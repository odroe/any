import 'dart:async';

import 'cancellation.dart';
import 'cause.dart';
import 'context.dart';
import 'exit.dart';
import 'option.dart';
import 'scope.dart';
import 'task.dart';
import 'task_local.dart';

/// A handle to one running [Task] and its eventual [Exit].
///
/// Create a fiber with `Runtime.start`, [TaskOperations.fork], or
/// [TaskOperations.forkIn]. Its owning [Scope] keeps resources alive until the
/// fiber stops and finishes cleanup.
///
/// Ownership and cancellation are registered before work starts. Deeply nested
/// starts may be deferred to keep execution stack safe; cancellation before startup
/// prevents the task body from running.
///
/// Use [join] to continue with its value or failure, [awaitExit] to inspect either
/// outcome, and [poll] to inspect without waiting. The same result can be observed
/// more than once. Cancelling a waiter leaves the fiber running; use [cancel] to
/// stop it and wait, or [requestCancel] to request cancellation without waiting.
final class Fiber<T, E> {
  Fiber._(this._scope, this._report);

  final Scope _scope;
  final void Function(Cause<Object?>) _report;
  final _controller = CancellationController();
  final _completion = Completer<Exit<T, E>>();
  final _listeners = <Object, void Function(Exit<T, E>)>{};
  final _zone = Zone.current;
  late final List<FailureEvent> _finalEvents;
  late final List<CauseCancelled> _cleanupCancellations;
  Exit<T, E>? _exit;
  var _active = true;
  var _observed = false;
  var _defectsHandledByOwner = false;

  /// Returns the completed exit, or [OptionNone] if the fiber is still running.
  ///
  /// A fiber remains running until its cleanup finishes. Returning [OptionSome]
  /// counts as observing the result for unhandled-failure reporting.
  Option<Exit<T, E>> poll() {
    final exit = _exit;
    if (exit == null) return const Option.none();
    _observed = true;
    return Option.some(exit);
  }

  /// Returns a task that waits for this fiber's value or failure as an [Exit].
  ///
  /// Cancelling the wait leaves the fiber running and does not mark an undelivered
  /// result as observed. Waiting on oneself, or on work that must wait for the
  /// caller, fails with a [StateError] defect instead of deadlocking.
  Task<Exit<T, E>, Never> awaitExit() => awaitFiberTask(this);

  /// Returns a task that waits for this fiber and adopts its value or failure.
  ///
  /// Use [awaitExit] to inspect a failure as a value instead. Both operations share
  /// the same cancellation and self-wait rules.
  Task<T, E> join() => joinFiberTask(this);

  /// Requests cancellation without waiting for work or cleanup to finish.
  ///
  /// The first request fixes [reason]. Requests after completion have no effect.
  void requestCancel({Object? reason}) {
    if (_active) _controller.requestCancel(reason: reason);
  }

  /// Returns a task that cancels this fiber and waits for its [Exit].
  ///
  /// Completion includes resource cleanup. An in-flight, non-cancellable [Future]
  /// can therefore delay the result. A self-wait or ancestor wait fails with a
  /// [StateError] defect before requesting cancellation.
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
    _cleanupCancellations = result.cleanupCancellations;
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
// Limit the synchronous call stack across separate fiber evaluators.
const _maxStartupDepth = 32;
var _startupDepth = 0;

// Starts an execution after synchronously registering it with [owner].
//
// Deeply nested starts are deferred so separate evaluators cannot recursively
// exhaust the stack. [deferStart] also lets owners finish installing workers
// before their first step. Ownership and cancellation are installed first.
Fiber<T, E> startFiber<T, E>(
  Task<T, E> task, {
  required Scope owner,
  required Context context,
  Map<TaskLocal<Object?>, Object?> locals = const {},
  required void Function(Cause<Object?> cause) report,
  bool inheritHost = true,
  bool deferStart = false,
  bool interruptible = true,
  CancellationSignal? signal,
  Scope? runScope,
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

  void start() {
    _startupDepth++;
    try {
      unawaited(
        runZoned(
          () async {
            TaskEvaluation<T, E> result;
            try {
              result = await evaluateTask(
                task,
                interruptible: interruptible,
                signal: fiber._controller.signal,
                scope: fiber._scope,
                runScope: runScope,
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
            _currentFibers: List<Fiber<Object?, Object?>>.unmodifiable(
              ancestors,
            ),
          },
        ),
      );
    } finally {
      _startupDepth--;
    }
  }

  if (deferStart || _startupDepth >= _maxStartupDepth) {
    scheduleMicrotask(start);
  } else {
    start();
  }
  return fiber;
}

// Creates an already completed handle without starting or registering work.
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

List<CauseCancelled> fiberCleanupCancellations(Fiber<Object?, Object?> fiber) =>
    fiber._cleanupCancellations;

List<Fiber<Object?, Object?>> currentFibers() =>
    Zone.current[_currentFibers] as List<Fiber<Object?, Object?>>? ?? const [];

// Subscribes to delivery; removing the subscription does not observe the exit.
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

// Stops owned work and retains defects without converting business failures.
Future<List<FailureEvent>> stopFiber(
  Fiber<Object?, Object?> fiber, {
  Object? reason,
}) async {
  fiber._defectsHandledByOwner = true;
  fiber.requestCancel(reason: reason);
  await fiberCompletion(fiber, observe: false);
  return fiber._finalEvents;
}

// One defect occurrence retained across internal ownership boundaries.
final class FailureEvent {
  const FailureEvent(this.cause, this.source);

  final CauseDefect cause;
  final Object source;
}

List<Cause<E>> _reasons<E>(Cause<E> cause) => switch (cause) {
  CauseGroup(:final causes) => causes,
  _ => [cause],
};
