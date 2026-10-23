import 'cause.dart';
import 'clock.dart';
import 'exit.dart';
import 'fiber.dart';
import 'option.dart';
import 'scope.dart';
import 'task.dart';

/// A bounded cache that shares each in-flight load among its callers.
///
/// The creating scope owns the cache and its loaders. A loader inherits the
/// services and task-local values of the request that starts it. Keys must
/// therefore include any tenant or other request differences that affect the
/// result. Keys follow Dart's equality and hash-code contract.
///
/// Capacity includes cached entries and unfinished loads, including invalidated
/// or cancelling generations. Completed entries are evicted in least recently
/// used order. If every slot is still loading, new entries wait in arrival
/// order. The number of waiting callers is controlled by application concurrency,
/// not by cache capacity.
///
/// A loader must not await its own entry, or recursively wait for capacity that
/// it occupies. Such dependencies cannot complete until the loader finishes.
///
/// Cached values have no resource lease. Use a resource pool for connections or
/// other values that must remain open while callers use them.
final class Cache<K, V, E> {
  Cache._(
    this._capacity,
    this._ttl,
    this._load,
    this._cacheFailures,
    this._scope,
    this._clock,
  );

  final int _capacity;
  final Duration _ttl;
  final Task<V, E> Function(K key) _load;
  final bool _cacheFailures;
  final Scope _scope;
  final Clock _clock;
  final _entries = <K, _Entry<K, V, E>>{};
  final _occupied = <_Entry<K, V, E>>{};
  final _waiting = <_Request<K, V, E>>{};
  var _closed = false;
  var _draining = false;

  /// Creates a cache owned by the current scope when this task executes.
  ///
  /// [capacity] must be positive and [ttl] nonnegative. Invalid arguments become
  /// argument defects. TTL starts when the loader and its cleanup have finished,
  /// using the clock available at creation. Zero TTL shares unfinished loads
  /// without retaining completed results.
  ///
  /// Only successful values are cached by default. [cacheFailures] also retains
  /// business failures and groups containing only business failures. Defects and
  /// cancellation are never cached. Each execution creates independent state.
  /// Expiration is checked on access and when a new entry needs capacity.
  /// Returning the cache from a completed Runtime.run does not keep it alive.
  static Task<Cache<K, V, E>, Never> create<K, V, E>({
    required int capacity,
    required Duration ttl,
    required Task<V, E> Function(K key) load,
    bool cacheFailures = false,
  }) => Task.service(Clock.key).flatMap(
    (clock) => accessTaskEnvironment(
      (scope, _) => Task.acquireRelease(
        Task.sync(() {
          if (capacity <= 0) {
            throw RangeError.range(capacity, 1, null, 'capacity');
          }
          if (ttl.isNegative) {
            throw ArgumentError.value(ttl, 'ttl', 'Must not be negative.');
          }
          return Cache<K, V, E>._(
            capacity,
            ttl,
            load,
            cacheFailures,
            scope,
            clock,
          );
        }),
        release: (cache, _) => cache._close(),
      ),
    ),
  );

  _Entry<K, V, E>? _lookup(K key) {
    _checkOpen();
    final entry = _entries[key];
    if (entry == null || entry.result == null) return entry;
    if (_clock.elapsed - entry.completedAt! >= _ttl) {
      _retire(entry);
      return null;
    }
    _entries.remove(key);
    _entries[key] = entry;
    return entry;
  }

  void _checkOpen() {
    if (_closed) throw StateError('The cache is closed.');
  }

  void _retire(_Entry<K, V, E> entry) {
    entry.retired = true;
    if (identical(_entries[entry.key], entry)) _entries.remove(entry.key);
    if (entry.result != null) _occupied.remove(entry);
  }

  bool _makeRoom() {
    if (_occupied.length < _capacity) return true;
    _Entry<K, V, E>? oldest;
    Duration? now;
    for (final entry in _entries.values) {
      if (entry.result != null) {
        oldest ??= entry;
        now ??= _clock.elapsed;
        if (now - entry.completedAt! >= _ttl) {
          _retire(entry);
          return true;
        }
      }
    }
    if (oldest == null) return false;
    _retire(oldest);
    return true;
  }

  void _drain() {
    if (_draining) return;
    _draining = true;
    try {
      // A later request may join an existing load without taking a slot.
      // Requests needing new slots still receive them in arrival order.
      for (final request in _waiting.toList()) {
        if (request.released) continue;
        try {
          var entry = _lookup(request.key);
          if (entry == null) {
            if (!_makeRoom()) continue;
            entry = _Entry(request.key);
            _entries[request.key] = entry;
            _occupied.add(entry);
          }
          _waiting.remove(request);
          request.entry = entry;
          entry.waiters.add(request);
          request.complete(Exit.success(entry));
        } catch (error, stackTrace) {
          _waiting.remove(request);
          request.complete(Exit.failure(Cause.defect(error, stackTrace)));
        }
      }
    } finally {
      _draining = false;
    }
  }

  Task<void, Never> _start(_Entry<K, V, E> entry) => Task.defer(() {
    if (entry.starting || entry.result != null) return Task.succeed(null);
    entry.starting = true;
    return Task.defer(() => _load(entry.key))
        .forkIn(_scope)
        .map((fiber) {
          entry.fiber = fiber;
          if (entry.cancelling) fiber.requestCancel(reason: entry.stopReason);
          listenFiber(fiber, (exit) {
            _finish(entry, TaskEvaluation(exit, fiberFailureEvents(fiber)));
          });
        })
        .recoverCause(
          (cause) => Task.sync(() {
            // A scope may close after admission but before the loader is registered.
            _finish(entry, TaskEvaluation.fresh(Exit.failure(cause)));
          }),
        );
  });

  void _finish(_Entry<K, V, E> entry, TaskEvaluation<V, E> result) {
    if (entry.result != null) return;
    var keep =
        !_closed &&
        !entry.retired &&
        _ttl > Duration.zero &&
        _cacheable(result.exit);
    if (keep) {
      try {
        entry.completedAt = _clock.elapsed;
      } catch (error, stackTrace) {
        final defect = CauseDefect(error, stackTrace);
        result = TaskEvaluation(
          Exit.failure(switch (result.exit) {
            ExitFailure(:final cause) => Cause.combine([cause, defect]),
            _ => defect,
          }),
          [...result.events, FailureEvent(defect, Object())],
        );
        keep = false;
      }
    }
    entry.result = result;
    if (keep) {
      _entries.remove(entry.key);
      _entries[entry.key] = entry;
    } else {
      _retire(entry);
    }
    final listeners = entry.listeners.toList();
    entry.listeners.clear();
    for (final listener in listeners) {
      listener(result);
    }
    _drain();
  }

  bool _cacheable(Exit<V, E> exit) => switch (exit) {
    ExitSuccess() => true,
    ExitFailure(:final cause) =>
      _cacheFailures &&
          switch (cause) {
            CauseFailure() => true,
            CauseGroup(:final causes) => causes.every(
              (cause) => cause is CauseFailure<E>,
            ),
            _ => false,
          },
  };

  Task<void, Never> _release(_Request<K, V, E> request) => Task.defer(() {
    if (request.released) return Task.succeed(null);
    request.released = true;
    _waiting.remove(request);
    final entry = request.entry;
    if (entry == null) {
      _drain();
      return Task.succeed(null);
    }
    entry.waiters.remove(request);
    if (entry.result == null && entry.waiters.isEmpty) {
      entry.cancelling = true;
      _retire(entry);
      if (!entry.starting) {
        _finish(
          entry,
          TaskEvaluation.fresh(Exit.failure(Cause.cancelled(entry.stopReason))),
        );
      } else {
        entry.fiber?.requestCancel(reason: entry.stopReason);
      }
    }
    _drain();
    if (entry.result == null && !entry.cancelling) return Task.succeed(null);
    return entry.awaitResult().flatMap((result) {
      final events = result.events;
      return taskFromEvaluation(
        TaskEvaluation<void, Never>(
          events.isEmpty
              ? const Exit.success(null)
              : Exit.failure(Cause.combine(events.map((event) => event.cause))),
          events,
        ),
      );
    });
  });

  Task<void, Never> _close() => Task.sync(() {
    _closed = true;
    for (final entry in _occupied.toList()) {
      _retire(entry);
      if (entry.result == null && entry.fiber == null) {
        _finish(
          entry,
          TaskEvaluation.fresh(
            Exit.failure(
              Cause.defect(
                StateError('The cache closed before its loader started.'),
                StackTrace.current,
              ),
            ),
          ),
        );
      }
    }
    _entries.clear();
    _drain();
  });

  Task<V, E> _get(K key) => Task.uninterruptibleMask((restore) {
    final request = _Request<K, V, E>(key);
    return Task.addFinalizer((_) => _release(request))
        .flatMap(
          (_) => restore(
            Task.fromCallback<_Entry<K, V, E>, Never>((complete) {
              request.complete = complete;
              _waiting.add(request);
              _drain();
              return () {
                _waiting.remove(request);
              };
            }),
          ),
        )
        .flatMap((entry) => _start(entry).map((_) => entry))
        .withError<E>()
        .flatMap(
          (entry) =>
              restore(entry.awaitResult())
                  .withError<E>()
                  .flatMap(taskFromEvaluation),
        );
  }).scoped();
}

/// Accesses a [Cache] using its statically known key, value, and error types.
extension CacheOperations<K, V, E> on Cache<K, V, E> {
  /// Gets a cached result or joins one shared load for [key].
  ///
  /// A miss waits for capacity if every slot is loading. Cancelling a caller
  /// removes only its wait. The last caller to leave an unfinished load cancels
  /// it and waits for its cleanup; a new request cannot join that generation.
  /// Closed caches fail with a [StateError] defect.
  Task<V, E> get(K key) => Task.defer(() => _get(key));

  /// Reads an unexpired completed entry without starting or waiting for a load.
  ///
  /// Returns [OptionNone] for a miss or unfinished load. A cached success is
  /// [OptionSome], including a nullable value. An explicitly cached business
  /// failure fails this task with its original cause. Hits update recency.
  Task<Option<V>, E> getOption(K key) => Task.defer(() {
    final entry = _lookup(key);
    _drain();
    final result = entry?.result;
    if (result == null) return Task.succeed(const Option.none());
    return taskFromEvaluation(result).map(Option.some);
  });

  /// Removes the current generation without cancelling its existing callers.
  ///
  /// Its result cannot be written back. A subsequent request gets a new
  /// generation and may wait for capacity until the old loader finishes.
  Task<void, Never> invalidate(K key) => Task.sync(() {
    _checkOpen();
    final entry = _entries[key];
    if (entry != null) _retire(entry);
    _drain();
  });

  /// Invalidates every current entry, retaining unfinished loads for callers.
  Task<void, Never> invalidateAll() => Task.sync(() {
    _checkOpen();
    for (final entry in _entries.values.toList()) {
      _retire(entry);
    }
    _drain();
  });

  /// Invalidates [key] and requests its next generation.
  ///
  /// This does not retain the old value while loading. The new request follows
  /// the same sharing, capacity, and cancellation rules as [get].
  Task<V, E> refresh(K key) =>
      invalidate(key).withError<E>().flatMap((_) => get(key));
}

final class _Entry<K, V, E> {
  _Entry(this.key);
  final K key;
  final stopReason = Object();
  final waiters = <_Request<K, V, E>>{};
  final listeners = <void Function(TaskEvaluation<V, E>)>{};
  Fiber<V, E>? fiber;
  TaskEvaluation<V, E>? result;
  Duration? completedAt;
  var starting = false;
  var retired = false;
  var cancelling = false;

  Task<TaskEvaluation<V, E>, Never> awaitResult() =>
      Task.fromCallback((complete) {
        void deliver(TaskEvaluation<V, E> result) =>
            complete(Exit.success(result));
        final ready = result;
        if (ready != null) {
          deliver(ready);
        } else {
          listeners.add(deliver);
        }
        return () {
          listeners.remove(deliver);
        };
      });
}

final class _Request<K, V, E> {
  _Request(this.key);
  final K key;
  late void Function(Exit<_Entry<K, V, E>, Never>) complete;
  _Entry<K, V, E>? entry;
  var released = false;
}
