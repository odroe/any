import 'dart:async';

import '../cause.dart';
import '../clock.dart';
import '../exit.dart';
import '../fiber.dart';
import '../scope.dart';
import '../task.dart';

// The single resource lifecycle used by SharedResource and SharedResourceMap.
final class SharedResourceState<K, T, E> {
  SharedResourceState._(
    this._acquire,
    this._idleTimeout,
    this._environment,
    this._clock,
    this._scope,
    this._capacity,
    this._onCapacityExceeded,
  );

  final Task<T, E> Function(K key) _acquire;
  final Duration _idleTimeout;
  final TaskEnvironment _environment;
  final Clock _clock;
  final Scope _scope;
  final int? _capacity;
  final E Function(K key)? _onCapacityExceeded;
  final _generations = <_Generation<K, T, E>>{};
  final _current = <K, _Generation<K, T, E>>{};
  var _closed = false;

  static Task<SharedResourceState<K, T, E>, Never> create<K, T, E>(
    Task<T, E> Function(K key) acquire, {
    int? capacity,
    E Function(K key)? onCapacityExceeded,
    Duration idleTimeout = Duration.zero,
  }) => accessTaskEnvironment(
    (scope, _) => captureTaskEnvironment().flatMap(
      (environment) => Task.service(Clock.key).flatMap(
        (clock) => Task.acquireRelease(
          Task.sync(() {
            if (capacity != null && capacity <= 0) {
              throw RangeError.range(capacity, 1, null, 'capacity');
            }
            if (capacity != null && onCapacityExceeded == null) {
              throw ArgumentError.notNull('onCapacityExceeded');
            }
            if (idleTimeout.isNegative) {
              throw ArgumentError.value(
                idleTimeout,
                'idleTimeout',
                'Must not be negative.',
              );
            }
            return SharedResourceState<K, T, E>._(
              acquire,
              idleTimeout,
              environment,
              clock,
              scope,
              capacity,
              onCapacityExceeded,
            );
          }),
          release: (resource, exit) => resource._close(exit),
        ),
      ),
    ),
  );

  void _checkOpen() {
    if (_closed) throw StateError('The shared resource is closed.');
  }

  Task<_Lease<K, T, E>, E> _reserve(K key, Scope borrowerScope) {
    _checkOpen();
    var generation = _current[key];
    if (generation == null) {
      final capacity = _capacity;
      if (capacity != null && _generations.length >= capacity) {
        return Task.fail(_onCapacityExceeded!(key));
      }
      generation = _Generation<K, T, E>(_scope, key);
      _current[key] = generation;
      _generations.add(generation);
      final lease = _Lease(generation, borrowerScope);
      generation.leases.add(lease);
      _start(generation);
      return Task.succeed(lease);
    }
    final lease = _Lease(generation, borrowerScope);
    generation.leases.add(lease);
    if (!_cancelIdle(generation)) {
      _beginClose(generation, const Exit.success(null));
    }
    return Task.succeed(lease);
  }

  void _start(_Generation<K, T, E> generation) {
    try {
      final fiber = startFiber(
        inTaskScope(
          Task.defer(() => _acquire(generation.key)),
          generation.scope,
        ),
        owner: generation.scope,
        context: _environment.context,
        locals: _environment.locals,
        report: _environment.report,
        inheritHost: false,
        deferStart: true,
      );
      listenFiber(fiber, (exit) {
        _loaded(generation, TaskEvaluation(exit, fiberFailureEvents(fiber)));
      });
    } catch (error, stackTrace) {
      _loaded(
        generation,
        TaskEvaluation.fresh(Exit.failure(Cause.defect(error, stackTrace))),
      );
    }
  }

  void _loaded(_Generation<K, T, E> generation, TaskEvaluation<T, E> result) {
    generation.loaded = result;
    if (generation.closing != null) return;
    if (result.exit is ExitSuccess<T, E>) {
      generation.publish(result);
    } else {
      _beginClose(generation, result.exit);
    }
  }

  void _retire(_Generation<K, T, E> generation) {
    generation.retired = true;
    if (identical(_current[generation.key], generation)) {
      _current.remove(generation.key);
    }
  }

  bool _cancelIdle(_Generation<K, T, E> generation) {
    final cancel = generation.cancelIdle;
    generation.cancelIdle = null;
    if (cancel == null) return true;
    try {
      cancel();
      return true;
    } catch (error, stackTrace) {
      generation.faults.add(
        FailureEvent(CauseDefect(error, stackTrace), Object()),
      );
      return false;
    }
  }

  Future<TaskEvaluation<void, Never>> _beginClose(
    _Generation<K, T, E> generation,
    Exit<Object?, Object?> exit, {
    bool background = false,
  }) {
    if (!background) generation.claimed = true;
    final existing = generation.closing;
    if (existing != null) return existing.future;
    final completion = Completer<TaskEvaluation<void, Never>>();
    generation.closing = completion;
    _retire(generation);
    _cancelIdle(generation);
    unawaited(_finishClose(generation, exit, completion, background));
    return completion.future;
  }

  Future<void> _finishClose(
    _Generation<K, T, E> generation,
    Exit<Object?, Object?> exit,
    Completer<TaskEvaluation<void, Never>> completion,
    bool background,
  ) async {
    TaskEvaluation<void, Never> closed;
    try {
      await closeScope(generation.scope, exit);
      closed = scopeCleanupFor(generation.scope);
    } catch (error, stackTrace) {
      closed = TaskEvaluation.fresh(
        Exit.failure(Cause.defect(error, stackTrace)),
      );
    }
    final cleanup = _append(
      _append(_defects(generation.loaded?.events ?? const []), closed),
      _defects(generation.faults),
    );
    final loaded = generation.loaded;
    if (generation.result == null) {
      final result = loaded?.exit is ExitFailure<T, E>
          ? loaded!
          : TaskEvaluation<T, E>.fresh(
              Exit.failure(Cause.cancelled(generation.stopReason)),
            );
      generation.publish(_append(result, cleanup));
    }
    _generations.remove(generation);
    generation.removeOwnerDependency();
    completion.complete(cleanup);
    if (background && !generation.claimed) {
      if (cleanup.exit case ExitFailure(:final cause)) {
        _environment.report(cause);
      }
    }
  }

  Task<void, Never> _release(
    _Lease<K, T, E> lease,
    Exit<Object?, Object?> exit,
  ) => Task.defer(() {
    if (lease.released) return Task.succeed(null);
    lease.released = true;
    lease.removeOwnerDependency?.call();
    lease.removeOwnerDependency = null;
    final generation = lease.generation;
    generation.leases.remove(lease);
    if (generation.leases.isNotEmpty) return Task.succeed(null);
    generation.empty?.complete();
    generation.empty = null;
    if (generation.closing != null ||
        generation.loaded == null ||
        generation.retired ||
        _closed ||
        _idleTimeout == Duration.zero) {
      return _await(_beginClose(generation, generation.ownerExit ?? exit));
    }
    try {
      generation.cancelIdle = _clock.schedule(_idleTimeout, () {
        generation.cancelIdle = null;
        _beginClose(generation, const Exit.success(null), background: true);
      });
      return Task.succeed(null);
    } catch (error, stackTrace) {
      generation.faults.add(
        FailureEvent(CauseDefect(error, stackTrace), Object()),
      );
      return _await(_beginClose(generation, exit));
    }
  });

  Task<void, Never> _close(Exit<Object?, Object?> exit) => Task.defer(() {
    _closed = true;
    final generations = _generations.toList();
    for (final generation in generations) {
      generation.claimed = true;
      generation.ownerExit = exit;
      _retire(generation);
      _cancelIdle(generation);
      // Waiting for acquisition is not a committed resource lease. Stop it on
      // owner closure; only callers already using a value can delay cleanup.
      for (final lease in generation.leases.toList()) {
        if (!lease.committed) {
          lease.released = true;
          generation.leases.remove(lease);
        }
      }
    }
    return Task.fromFuture<TaskEvaluation<void, Never>, Never>(() async {
      final results = <TaskEvaluation<void, Never>>[];
      await Future.wait([
        for (final generation in generations)
          _closeWhenUnused(generation, exit).then(results.add),
      ]);
      return results.fold<TaskEvaluation<void, Never>>(
        TaskEvaluation<void, Never>(const Exit.success(null), const []),
        _append<void, Never>,
      );
    }, onError: Error.throwWithStackTrace).flatMap(taskFromEvaluation);
  });

  Future<TaskEvaluation<void, Never>> _closeWhenUnused(
    _Generation<K, T, E> generation,
    Exit<Object?, Object?> exit,
  ) async {
    if (generation.leases.isNotEmpty) {
      generation.empty ??= Completer<void>();
      await generation.empty!.future;
    }
    return _beginClose(generation, exit);
  }

  Task<T, E> acquire(K key) => Task.uninterruptibleMask(
    (restore) =>
        accessTaskEnvironment(
              (scope, _) => Task.defer(() => _reserve(key, scope)),
            )
            .flatMap(
              (lease) =>
                  Task.addFinalizer((exit) => _release(lease, exit))
                      .map((_) => lease),
            )
            .flatMap(
              (lease) =>
                  restore(lease.generation.awaitResult())
                      .withError<E>()
                      .flatMap((result) => _confirm(lease, result)),
            ),
  );

  Task<T, E> _confirm(_Lease<K, T, E> lease, TaskEvaluation<T, E> result) {
    if (result.exit is ExitFailure<T, E>) {
      lease.released = true;
      lease.generation.leases.remove(lease);
      return taskFromEvaluation(result);
    }
    if (lease.released) {
      return Task.sync<T>(
        () => throw StateError(
          'The shared resource closed before the lease was acquired.',
        ),
      );
    }
    final closing = lease.generation.closing;
    if (closing != null) {
      return _await(closing.future).withError<E>().flatMap(
        (_) => Task.sync<T>(
          () => throw StateError('The shared resource is closing.'),
        ),
      );
    }
    lease.removeOwnerDependency = addScopeCloseDependency(_scope, lease.scope);
    lease.committed = true;
    return taskFromEvaluation(result);
  }

  Task<void, Never> invalidate(K key) => Task.uninterruptibleMask(
    (_) => Task.defer(() {
      _checkOpen();
      final generation = _current[key];
      if (generation == null) return Task.succeed(null);
      _retire(generation);
      _cancelIdle(generation);
      if (generation.leases.isNotEmpty) return Task.succeed(null);
      return _await(_beginClose(generation, const Exit.success(null)));
    }),
  );
}

final class _Generation<K, T, E> {
  _Generation(Scope owner, this.key) {
    removeOwnerDependency = addScopeCloseDependency(owner, scope);
  }

  final K key;
  final scope = createScope();
  late final void Function() removeOwnerDependency;
  final stopReason = Object();
  final leases = <_Lease<K, T, E>>{};
  final listeners = <void Function(TaskEvaluation<T, E>)>{};
  final faults = <FailureEvent>[];
  TaskEvaluation<T, E>? loaded;
  TaskEvaluation<T, E>? result;
  Completer<TaskEvaluation<void, Never>>? closing;
  Completer<void>? empty;
  Exit<Object?, Object?>? ownerExit;
  void Function()? cancelIdle;
  var retired = false;
  var claimed = false;

  void publish(TaskEvaluation<T, E> value) {
    result = value;
    final callbacks = listeners.toList();
    listeners.clear();
    for (final callback in callbacks) {
      callback(value);
    }
  }

  Task<TaskEvaluation<T, E>, Never> awaitResult() =>
      Task.fromCallback((complete) {
        void notify(TaskEvaluation<T, E> value) =>
            complete(Exit.success(value));
        final ready = result;
        if (ready == null) {
          listeners.add(notify);
        } else {
          notify(ready);
        }
        return () {
          listeners.remove(notify);
        };
      });
}

final class _Lease<K, T, E> {
  _Lease(this.generation, this.scope);
  final _Generation<K, T, E> generation;
  final Scope scope;
  void Function()? removeOwnerDependency;
  var committed = false;
  var released = false;
}

Task<void, Never> _await(Future<TaskEvaluation<void, Never>> future) =>
    Task.fromFuture<TaskEvaluation<void, Never>, Never>(
      () => future,
      onError: Error.throwWithStackTrace,
    ).flatMap(taskFromEvaluation);

TaskEvaluation<void, Never> _defects(Iterable<FailureEvent> events) {
  final seen = <Object>{};
  final unique = [
    for (final event in events)
      if (seen.add(event.source)) event,
  ];
  return TaskEvaluation(
    unique.isEmpty
        ? const Exit.success(null)
        : Exit.failure(Cause.combine(unique.map((event) => event.cause))),
    unique,
  );
}

TaskEvaluation<T, E> _append<T, E>(
  TaskEvaluation<T, E> result,
  TaskEvaluation<void, Never> cleanup,
) {
  if (cleanup.exit case ExitFailure(:final cause)) {
    final seen = {for (final event in result.events) event.source};
    final events = cleanup.events.iterator;
    final additional = <Cause<Never>>[];
    final additionalEvents = <FailureEvent>[];
    final causes = switch (cause) {
      CauseGroup(:final causes) => causes,
      _ => [cause],
    };
    for (final cause in causes) {
      if (cause is CauseDefect) {
        events.moveNext();
        final event = events.current;
        if (!seen.add(event.source)) continue;
        additionalEvents.add(event);
      }
      additional.add(cause);
    }
    if (additional.isEmpty) return result;
    return TaskEvaluation(
      Exit.failure(
        Cause.combine([
          if (result.exit case ExitFailure(:final cause)) cause,
          ...additional,
        ]),
      ),
      [...result.events, ...additionalEvents],
    );
  }
  return result;
}
