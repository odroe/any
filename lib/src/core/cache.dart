import 'cause.dart';
import 'clock.dart';
import 'exit.dart';
import 'fiber.dart';
import 'option.dart';
import 'scope.dart';
import 'task.dart';

/// A bounded cache for reusable values and shared in-flight loads.
///
/// Use a cache for data such as lookup results that remain valid after loading
/// finishes. Loader resources are closed before a result is delivered; cached
/// values do not carry a resource lease. Use `SharedResource` or `Pool` for
/// connections and other objects that must stay open while in use.
///
/// Create and use the cache within one owning scope. Closing that scope stops
/// loaders and rejects further operations with a [StateError] defect.
///
/// ```dart
/// import 'package:any/any.dart';
///
/// Future<List<String>> loadNames() async {
///   final runtime = Runtime();
///   final program =
///       Cache.create<int, String, Never>(
///         capacity: 64,
///         ttl: const Duration(minutes: 1),
///         load: (id) => Task.succeed('Product $id'),
///       ).flatMap(
///         (cache) => Task.all([
///           cache.get(1),
///           cache.get(1),
///           cache.get(2),
///         ], concurrency: 3),
///       );
///   try {
///     return await runtime.runOrThrow(program);
///   } finally {
///     await runtime.close();
///   }
/// }
/// ```
///
/// Keys use Dart equality and must have stable hash codes. A load uses the
/// services and task-local values of the caller that starts it: include tenant,
/// authorization, or other result-changing context in the key.
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

  /// Creates an empty cache owned by the executing scope.
  ///
  /// [capacity] must be positive and [ttl] nonnegative; invalid arguments become
  /// defects when the task runs. Each execution creates a separate cache.
  ///
  /// Capacity includes stored results and loads still running or cleaning up,
  /// even after invalidation. Completed entries are evicted least recently used
  /// first. If all slots are loading, new loads wait in arrival order. Application
  /// concurrency determines the number of waiting callers.
  ///
  /// TTL uses the creation-time clock and starts after a load and its cleanup
  /// finish. Expiration is checked on access or when making room. Zero TTL shares
  /// in-flight work without retaining results.
  ///
  /// [cacheFailures] also retains business failures, including groups made only
  /// of business failures. Defects and cancellation are never cached. [load]
  /// must not wait for its own entry or for capacity occupied by that load.
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
    return forkTaskIn(
          Task.defer(() => _load(entry.key)).scoped(),
          _scope,
          deferStart: false,
        )
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

  Task<void, Never> _release(
    _Request<K, V, E> request,
    Exit<Object?, Object?> exit,
  ) => Task.defer(() {
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
      final delivered = switch (exit) {
        ExitFailure(:final cause) => _reasons(
          cause,
        ).whereType<CauseCancelled>().toList(),
        _ => <CauseCancelled>[],
      };
      final cleanup = <Cause<Never>>[];
      var removedStop = false;
      if (result.exit case ExitFailure(:final cause)) {
        for (final reason in _reasons(cause)) {
          switch (reason) {
            case CauseDefect():
              cleanup.add(reason);
            case CauseCancelled():
              final index = delivered.indexWhere(
                (value) => identical(value, reason),
              );
              if (index >= 0) {
                delivered.removeAt(index);
              } else if (!removedStop &&
                  identical(reason.reason, entry.stopReason)) {
                removedStop = true;
              } else {
                cleanup.add(reason);
              }
            default:
              // Business failures do not become cleanup defects when a caller
              // stops waiting. Explicit cleanup cancellations remain failures.
              break;
          }
        }
      }
      return taskFromEvaluation(
        TaskEvaluation<void, Never>(
          cleanup.isEmpty
              ? const Exit.success(null)
              : Exit.failure(Cause.combine(cleanup)),
          result.events,
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
    return Task.addFinalizer((exit) => _release(request, exit))
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

/// Reads, refreshes, and invalidates cached values.
extension CacheOperations<K, V, E> on Cache<K, V, E> {
  /// Returns the result for [key], loading it if necessary.
  ///
  /// Concurrent callers share the same unfinished load. A miss waits for a slot
  /// if all capacity is occupied by loads.
  ///
  /// Cancelling one caller leaves other callers waiting. The last caller cancels
  /// an unfinished load and waits for cleanup; subsequent callers start a new
  /// load. Cleanup defects and explicit cleanup cancellations are preserved.
  Task<V, E> get(K key) => Task.defer(() => _get(key));

  /// Looks up a completed result without starting or waiting for a load.
  ///
  /// Returns [OptionNone] for an absent, expired, or still-loading entry. A cached
  /// success returns [OptionSome], even when its value is `null`; a cached
  /// business failure fails this task with its original cause. A hit updates
  /// recency.
  Task<Option<V>, E> getOption(K key) => Task.defer(() {
    final entry = _lookup(key);
    _drain();
    final result = entry?.result;
    if (result == null) return Task.succeed(const Option.none());
    return taskFromEvaluation(result).map(Option.some);
  });

  /// Makes the next request for [key] start a new load.
  ///
  /// Existing callers still receive their original load's result, which cannot
  /// repopulate the cache. That load occupies capacity until it finishes, so a
  /// new request may need to wait. An absent key has no effect.
  Task<void, Never> invalidate(K key) => Task.sync(() {
    _checkOpen();
    final entry = _entries[key];
    if (entry != null) _retire(entry);
    _drain();
  });

  /// Invalidates all entries without cancelling their existing callers.
  ///
  /// Unfinished loads retain their capacity until they finish, as with
  /// [invalidate].
  Task<void, Never> invalidateAll() => Task.sync(() {
    _checkOpen();
    for (final entry in _entries.values.toList()) {
      _retire(entry);
    }
    _drain();
  });

  /// Invalidates [key] and returns a newly requested result.
  ///
  /// The previous result is removed immediately. Loading, capacity, and
  /// cancellation follow [get].
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

List<Cause<T>> _reasons<T>(Cause<T> cause) => switch (cause) {
  CauseGroup(:final causes) => causes,
  _ => [cause],
};
