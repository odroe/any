import 'dart:async';

import 'cancellation.dart';
import 'cause.dart';
import 'context.dart';
import 'exit.dart';
import 'option.dart';
import 'scope.dart';
import 'task.dart';
import 'task_local.dart';

/// A running task whose owner waits for its work and cleanup to finish.
///
/// Start a fiber with Runtime.start, Task.fork, or Task.forkIn. Its final exit
/// includes resource cleanup and can be observed more than once. Cancelling a
/// waiter does not cancel the fiber; use [requestCancel] or [cancel] explicitly.
final class Fiber<T, E> {
  Fiber._(this._scope, this._report);

  final Scope _scope;
  final void Function(Cause<Object?>) _report;
  final _controller = CancellationController();
  final _completion = Completer<Exit<T, E>>();
  final _listeners = <Object, void Function(Exit<T, E>)>{};
  final _zone = Zone.current;
  late final List<FailureEvent> _finalEvents;
  Exit<T, E>? _exit;
  var _active = true;
  var _observed = false;
  var _defectsHandledByOwner = false;

  /// Returns the completed exit, or absence while work or cleanup is running.
  ///
  /// Returning a completed exit counts as observing it for failure reporting.
  Option<Exit<T, E>> poll() {
    final exit = _exit;
    if (exit == null) return const Option.none();
    _observed = true;
    return Option.some(exit);
  }

  /// Waits for this fiber's complete exit when the returned task runs.
  ///
  /// Waiting on yourself or a task that must wait for you fails with a
  /// [StateError] defect. Cancelling the wait removes its subscription without
  /// stopping this fiber or observing an exit that has not been delivered.
  Task<Exit<T, E>, Never> awaitExit() => awaitFiberTask(this);

  /// Waits for completion and propagates the successful value or full cause.
  ///
  /// This has the same waiting and cancellation rules as [awaitExit].
  Task<T, E> join() => joinFiberTask(this);

  /// Requests cancellation without waiting for work or cleanup to finish.
  ///
  /// The first request fixes [reason]. Requests after completion have no effect.
  void requestCancel({Object? reason}) {
    if (_active) _controller.requestCancel(reason: reason);
  }

  /// Requests cancellation and waits for the complete exit when executed.
  ///
  /// Resource cleanup and any in-flight, non-cancellable Future must finish
  /// before this operation can return the exit. Self and ancestor waits fail
  /// with a [StateError] defect before sending the cancellation request.
  Task<Exit<T, E>, Never> cancel({Object? reason}) =>
      awaitFiberTask(this, cancel: true, reason: reason);

  void Function() _listen(void Function(Exit<T, E>) listener) {
    final exit = _exit;
    if (exit != null) {
      _observed = true;
      listener(exit);
      return () {};
    }
    final token = Object();
    _listeners[token] = Zone.current.bindUnaryCallbackGuarded(listener);
    return () => _listeners.remove(token);
  }

  void _finish(TaskEvaluation<T, E> result) {
    final exit = result.exit;
    _finalEvents = result.events;
    _active = false;
    _exit = exit;
    _completion.complete(exit);
    for (final token in _listeners.keys.toList()) {
      final listener = _listeners.remove(token);
      if (listener != null) {
        _observed = true;
        listener(exit);
      }
    }

    if (exit case ExitFailure(:final cause)) {
      final reasons = _reasons(cause);
      if (!reasons.any((reason) => reason is! CauseCancelled)) return;
      _zone.run(() {
        Timer.run(() {
          if (_observed) return;
          final unhandled = _defectsHandledByOwner
              ? reasons.whereType<CauseFailure<E>>().toList()
              : reasons;
          if (unhandled.isEmpty) return;
          _zone.runGuarded(() => _report(Cause.combine(unhandled)));
        });
      });
    }
  }
}

// Internal execution and ownership operations, not exported by any.dart.
final _currentFibers = Object();

/// Starts an execution after synchronously registering it with [owner].
Fiber<T, E> startFiber<T, E>(
  Task<T, E> task, {
  required Scope owner,
  required Context context,
  Map<TaskLocal<Object?>, Object?> locals = const {},
  required void Function(Cause<Object?> cause) report,
  bool inheritHost = true,
  CancellationSignal? signal,
}) {
  final fiber = Fiber<T, E>._(createScope(), report);
  attachScopeFiber(owner, fiber);
  final unsubscribe = signal?.onCancel(
    (reason) => fiber.requestCancel(reason: reason),
  );
  final ancestors = <Fiber<Object?, Object?>>[
    if (inheritHost) ...currentFibers().where(fiberIsActive),
    fiber,
  ];

  unawaited(
    runZoned(
      () async {
        TaskEvaluation<T, E> result;
        try {
          result = await evaluateTask(
            task,
            signal: fiber._controller.signal,
            scope: fiber._scope,
            fiber: fiber,
            context: context,
            locals: locals,
          );
        } catch (error, stackTrace) {
          result = TaskEvaluation.fresh(
            Exit.failure(Cause.defect(error, stackTrace)),
          );
        }
        unsubscribe?.call();
        // Detach before publishing the exit: a joining parent must not collect
        // the same failure again when it subsequently closes its own scope.
        detachScopeFiber(owner, fiber);
        fiber._finish(result);
      },
      zoneValues: {
        _currentFibers: List<Fiber<Object?, Object?>>.unmodifiable(ancestors),
      },
    ),
  );
  return fiber;
}

/// Creates an already completed handle without starting or registering work.
Fiber<T, E> completedFiber<T, E>(
  Exit<T, E> exit, {
  required void Function(Cause<Object?> cause) report,
}) {
  final fiber = Fiber<T, E>._(createScope(), report);
  unawaited(closeScope(fiber._scope, exit));
  fiber._finish(TaskEvaluation.fresh(exit));
  return fiber;
}

Scope fiberScope(Fiber<Object?, Object?> fiber) => fiber._scope;

void Function(Cause<Object?>) fiberReport(Fiber<Object?, Object?> fiber) =>
    fiber._report;

bool fiberIsActive(Fiber<Object?, Object?> fiber) => fiber._active;

List<FailureEvent> fiberFailureEvents(Fiber<Object?, Object?> fiber) =>
    fiber._finalEvents;

List<Fiber<Object?, Object?>> currentFibers() =>
    Zone.current[_currentFibers] as List<Fiber<Object?, Object?>>? ?? const [];

/// Subscribes to delivery; removing the subscription does not observe the exit.
void Function() listenFiber<T, E>(
  Fiber<T, E> fiber,
  void Function(Exit<T, E> exit) listener,
) => fiber._listen(listener);

Future<Exit<T, E>> fiberCompletion<T, E>(
  Fiber<T, E> fiber, {
  bool observe = true,
}) {
  if (!observe) return fiber._completion.future;
  return fiber._completion.future.then((exit) {
    fiber._observed = true;
    return exit;
  });
}

/// Stops owned work and retains defects without converting business failures.
Future<List<FailureEvent>> stopFiber(Fiber<Object?, Object?> fiber) async {
  fiber._defectsHandledByOwner = true;
  fiber.requestCancel();
  await fiberCompletion(fiber, observe: false);
  return fiber._finalEvents;
}

/// One defect occurrence retained across internal ownership boundaries.
final class FailureEvent {
  const FailureEvent(this.cause, this.source);

  final CauseDefect cause;
  final Object source;
}

List<Cause<E>> _reasons<E>(Cause<E> cause) => switch (cause) {
  CauseGroup(:final causes) => causes,
  _ => [cause],
};
