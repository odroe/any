import 'dart:async';

import 'cause.dart';
import 'clock.dart';
import 'exit.dart';
import 'fiber.dart';
import 'scope.dart';
import 'task.dart';

/// Shares a lazily acquired resource through scope-owned leases.
///
/// Each successful [acquire] keeps its generation alive until the calling scope
/// closes. Concurrent callers share one acquisition. Acquisition and cleanup
/// use the services and task-local values captured when this object is created,
/// rather than those of its borrowers.
///
/// The creating scope owns this object. Closing it rejects new requests,
/// cancels unfinished acquisitions, and waits for existing leases before
/// releasing their resources. Borrowers in another runtime must release their
/// leases; closing the owner cannot cancel those independent tasks. Returning
/// this object from a completed Runtime.run does not keep it alive.
///
/// Acquisition and cleanup must not wait for another lease on this same
/// resource. Waiting for the owning scope or runtime to close from acquisition,
/// cleanup, or an active lease fails with a [StateError] defect when closing
/// would wait for that caller.
final class SharedResource<T, E> {
  SharedResource._(
    this._acquire,
    this._idleTimeout,
    this._environment,
    this._clock,
    this._scope,
  );

  final Task<T, E> _acquire;
  final Duration _idleTimeout;
  final TaskEnvironment _environment;
  final Clock _clock;
  final Scope _scope;
  final _generations = <_Generation<T, E>>{};
  _Generation<T, E>? _current;
  var _closed = false;

  /// Creates a shared resource owned by the current scope when this task runs.
  ///
  /// [acquire] starts only when the first borrower arrives. Resources registered
  /// by it remain open for its leases unless explicitly scoped more narrowly.
  /// A failed acquisition is fully cleaned up before its failure is delivered
  /// and is not cached.
  ///
  /// After the last lease ends, [idleTimeout] keeps an unused generation ready
  /// for reuse. Zero releases it immediately. Negative durations produce an
  /// argument defect during execution. Idle timers use the creation-time clock.
  /// Background cleanup failures are reported to the creating runtime; cleanup
  /// explicitly awaited by a caller is included in that caller's exit instead.
  static Task<SharedResource<T, E>, Never> create<T, E>(
    Task<T, E> acquire, {
    Duration idleTimeout = Duration.zero,
  }) => accessTaskEnvironment(
    (scope, _) => captureTaskEnvironment().flatMap(
      (environment) => Task.service(Clock.key).flatMap(
        (clock) => Task.acquireRelease(
          Task.sync(() {
            if (idleTimeout.isNegative) {
              throw ArgumentError.value(
                idleTimeout,
                'idleTimeout',
                'Must not be negative.',
              );
            }
            return SharedResource<T, E>._(
              acquire,
              idleTimeout,
              environment,
              clock,
              scope,
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

  _Lease<T, E> _reserve(Scope borrowerScope) {
    _checkOpen();
    var generation = _current;
    if (generation == null) {
      generation = _Generation<T, E>(_scope);
      _current = generation;
      _generations.add(generation);
      final lease = _Lease(generation, borrowerScope);
      generation.leases.add(lease);
      _start(generation);
      return lease;
    }
    final lease = _Lease(generation, borrowerScope);
    generation.leases.add(lease);
    if (!_cancelIdle(generation)) {
      _beginClose(generation, const Exit.success(null));
    }
    return lease;
  }

  void _start(_Generation<T, E> generation) {
    try {
      final fiber = startFiber(
        inTaskScope(_acquire, generation.scope),
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

  void _loaded(_Generation<T, E> generation, TaskEvaluation<T, E> result) {
    generation.loaded = result;
    if (generation.closing != null) return;
    if (result.exit is ExitSuccess<T, E>) {
      generation.publish(result);
    } else {
      _beginClose(generation, result.exit);
    }
  }

  void _retire(_Generation<T, E> generation) {
    generation.retired = true;
    if (identical(_current, generation)) _current = null;
  }

  bool _cancelIdle(_Generation<T, E> generation) {
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
    _Generation<T, E> generation,
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
    _Generation<T, E> generation,
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

  Task<void, Never> _release(_Lease<T, E> lease, Exit<Object?, Object?> exit) =>
      Task.defer(() {
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
    _Generation<T, E> generation,
    Exit<Object?, Object?> exit,
  ) async {
    if (generation.leases.isNotEmpty) {
      generation.empty ??= Completer<void>();
      await generation.empty!.future;
    }
    return _beginClose(generation, exit);
  }

  Task<T, E> _borrow() => Task.uninterruptibleMask(
    (restore) =>
        accessTaskEnvironment((scope, _) => Task.sync(() => _reserve(scope)))
            .flatMap(
              (lease) =>
                  Task.addFinalizer((exit) => _release(lease, exit))
                      .map((_) => lease),
            )
            .withError<E>()
            .flatMap(
              (lease) =>
                  restore(lease.generation.awaitResult())
                      .withError<E>()
                      .flatMap((result) => _confirm(lease, result)),
            ),
  );

  Task<T, E> _confirm(_Lease<T, E> lease, TaskEvaluation<T, E> result) {
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
}

/// Acquires and invalidates a shared resource using its known value/error types.
extension SharedResourceOperations<T, E> on SharedResource<T, E> {
  /// Acquires a lease that lasts until the calling scope closes.
  ///
  /// Pending callers share the same acquisition. Cancelling one removes only
  /// its wait; the last waiting caller cancels acquisition and waits for cleanup.
  /// Cancellation during delivery cannot leave a lease without its finalizer.
  /// Use a scoped task to release the lease before the surrounding task ends.
  /// A closed owner produces a [StateError] defect.
  Task<T, E> acquire() => Task.defer(_borrow);

  /// Retires the current generation so the next caller starts a new one.
  ///
  /// Existing waiters and leases retain their generation. It is released as
  /// soon as its final lease ends, without the idle timeout. If already unused,
  /// this task waits for its cleanup and propagates cleanup failures. Otherwise
  /// its last borrower observes those failures. Closed owners reject this call.
  /// Once invalidation starts, cancellation waits for any cleanup it initiates.
  Task<void, Never> invalidate() => Task.uninterruptibleMask(
    (_) => Task.defer(() {
      _checkOpen();
      final generation = _current;
      if (generation == null) return Task.succeed(null);
      _retire(generation);
      _cancelIdle(generation);
      if (generation.leases.isNotEmpty) return Task.succeed(null);
      return _await(_beginClose(generation, const Exit.success(null)));
    }),
  );
}

final class _Generation<T, E> {
  _Generation(Scope owner) {
    removeOwnerDependency = addScopeCloseDependency(owner, scope);
  }

  final scope = createScope();
  late final void Function() removeOwnerDependency;
  final stopReason = Object();
  final leases = <_Lease<T, E>>{};
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

final class _Lease<T, E> {
  _Lease(this.generation, this.scope);
  final _Generation<T, E> generation;
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
