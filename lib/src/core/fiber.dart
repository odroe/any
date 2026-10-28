import 'dart:async';

import 'cancellation.dart';
import 'cause.dart';
import 'context.dart';
import 'exit.dart';
import 'internal/completion.dart';
import 'internal/execution_mode.dart';
import 'option.dart';
import 'scope.dart';
import 'task.dart';

/// A handle to one running [Task] and its eventual [Exit].
///
/// Create a fiber with `Runtime.start`, [TaskOperations.fork], or
/// [TaskOperations.forkIn]. A child can borrow a resource [Scope] that outlives
/// its execution. Finishing the child does not close that borrowed scope.
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
  Fiber._(this._scope, this._report, this._reportFailure);

  final Scope _scope;
  final void Function(Cause<Object?>) _report;
  final _controller = CancellationController();
  final _completion = Completion<Exit<T, E>>();
  final _zone = Zone.current;
  late final List<FailureEvent> _finalEvents;
  late final List<CauseCancelled> _cleanupCancellations;
  Exit<T, E>? get _exit => _completion.value;
  var _active = true;
  bool _reportFailure;
  var _cleanupOwned = false;

  /// Returns the completed exit, or [OptionNone] if the fiber is still running.
  ///
  /// Completion includes owned cleanup. Polling does not change how the
  /// execution's owner propagates or reports failures.
  Option<Exit<T, E>> poll() => switch (_exit) {
    null => const Option.none(),
    final exit => Option.some(exit),
  };

  /// Calls [listener] once when execution and owned cleanup have finished.
  ///
  /// If this fiber has already completed, the callback runs immediately. The
  /// returned function removes only this registration and can be called more
  /// than once; it does not cancel the fiber.
  ///
  /// Callbacks run in their registration [Zone]. A callback exception is reported
  /// to that zone without changing the exit or preventing other callbacks. Like
  /// [poll], listening does not change the owner's failure handling.
  ///
  /// ```dart
  /// final stopWatching = fiber.onExit((exit) => print(exit));
  /// // Remove this listener when its UI or observer no longer needs updates.
  /// stopWatching();
  /// ```
  void Function() onExit(void Function(Exit<T, E>) listener) =>
      _completion.listen(listener);

  /// Returns a task that waits for this fiber's value or failure as an [Exit].
  ///
  /// Cancelling the wait leaves the fiber and its resource leases running.
  /// Waiting does not change the execution owner's failure reporting. Use
  /// [cancel] to stop the target itself.
  ///
  /// Waiting on oneself, or on work that must wait for the caller, fails with a
  /// [StateError] defect instead of deadlocking.
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
  /// Completion includes the fiber's own cleanup, without closing borrowed
  /// resources. An in-flight, non-cancellable [Future] can delay the result.
  /// A self-wait or ancestor wait fails with a [StateError] defect before
  /// requesting cancellation.
  Task<Exit<T, E>, Never> cancel({Object? reason}) =>
      awaitFiberTask(this, cancel: true, reason: reason);

  void _finish(TaskEvaluation<T, E> result) {
    _finalEvents = result.events;
    _cleanupCancellations = result.cleanupCancellations;
    _active = false;
    _completion.complete(result.exit);
    if (_reportFailure) reportFiberFailure(this);
  }

  void _reportIndependentFailure(Set<FailureSource> excluded) {
    final failures = <Cause<Object?>>[];
    final events = _finalEvents.iterator;
    final cause = (_exit as ExitFailure<T, E>).cause;
    var hasFailure = false;
    for (final reason in switch (cause) {
      CauseGroup(:final causes) => causes,
      _ => [cause],
    }) {
      if (reason is CauseCancelled) {
        if (!_cleanupOwned) failures.add(reason);
        continue;
      }
      events.moveNext();
      final event = events.current;
      if (excluded.contains(event.source)) continue;
      // Scope shutdown receives cleanup defects through its own result.
      if (_cleanupOwned && reason is CauseDefect) continue;
      if (!event.source.reportedTo.add(_report)) continue;
      hasFailure = true;
      failures.add(reason);
    }
    if (hasFailure) {
      _zone.runGuarded(() => _report(Cause.combine(failures)));
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
  required void Function(Cause<Object?> cause) report,
  required bool reportFailure,
  bool inheritHost = true,
  bool deferStart = false,
  bool interruptible = true,
  CancellationSignal? signal,
  Scope? runScope,
  Scope? resourceScope,
  ExecutionMode? mode,
  void Function(Fiber<T, E> fiber)? onRegistered,
}) {
  final fiber = Fiber<T, E>._(
    createScope(runtimeId: scopeRuntimeId(owner)),
    report,
    reportFailure,
  );
  attachScopeFiber(owner, fiber);
  final void Function()? releaseBorrow;
  try {
    releaseBorrow = resourceScope == null || identical(resourceScope, owner)
        ? null
        : borrowScopeFiber(resourceScope, fiber);
  } catch (_) {
    detachScopeFiber(owner, fiber);
    rethrow;
  }
  final unsubscribe = signal?.onCancel(
    (reason) => fiber.requestCancel(reason: reason),
  );
  final ancestors = <Fiber<Object?, Object?>>[
    if (inheritHost) ...currentFibers().where(fiberIsActive),
    fiber,
  ];

  void finish(TaskEvaluation<T, E> result) {
    unsubscribe?.call();
    // Detach before publishing the exit: a joining parent must not collect
    // the same failure again when it subsequently closes its own scope.
    detachScopeFiber(owner, fiber);
    releaseBorrow?.call();
    fiber._finish(result);
  }

  void failed(Object error, StackTrace stackTrace) => finish(
    TaskEvaluation.fresh(Exit.failure(Cause.defect(error, stackTrace))),
  );

  void start() {
    _startupDepth++;
    try {
      runZoned(
        () {
          final FutureOr<TaskEvaluation<T, E>> result;
          try {
            result = evaluateTask(
              task,
              interruptible: interruptible,
              signal: fiber._controller.signal,
              scope: fiber._scope,
              resourceScope: resourceScope,
              runScope: runScope,
              fiber: fiber,
              context: context,
              mode: mode,
            );
          } catch (error, stackTrace) {
            failed(error, stackTrace);
            return;
          }
          if (result is Future<TaskEvaluation<T, E>>) {
            unawaited(result.then(finish, onError: failed));
          } else {
            finish(result);
          }
        },
        zoneValues: {
          _currentFibers: List<Fiber<Object?, Object?>>.unmodifiable(ancestors),
        },
      );
    } finally {
      _startupDepth--;
    }
  }

  // Owners install membership and listeners before synchronous work can finish.
  onRegistered?.call(fiber);
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
  required bool reportFailure,
}) {
  final fiber = Fiber<T, E>._(createScope(), report, reportFailure);
  final closing = closeScope(fiber._scope, exit);
  if (closing is Future<Cause<Never>?>) unawaited(closing);
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

// Runtime.runSync transfers an unfinished execution to runtime supervision.
void superviseFiber(Fiber<Object?, Object?> fiber) {
  fiber._reportFailure = true;
}

// Owning combinators can report completed failures outside their result path.
// Use the same occurrence tracking and shutdown ownership as public forks.
void reportFiberFailure(
  Fiber<Object?, Object?> fiber, {
  Set<FailureSource> excluded = const {},
}) {
  if (fiber._finalEvents.isEmpty) return;
  fiber._zone.run(
    () => Timer.run(() => fiber._reportIndependentFailure(excluded)),
  );
}

Future<Exit<T, E>> fiberCompletion<T, E>(Fiber<T, E> fiber) {
  final completion = Completer<Exit<T, E>>();
  fiber.onExit(completion.complete);
  return completion.future;
}

// Stops owned work and retains defects without converting business failures.
FutureOr<List<FailureEvent>> stopFiber(
  Fiber<Object?, Object?> fiber, {
  Object? reason,
}) {
  fiber._cleanupOwned = true;
  fiber.requestCancel(reason: reason);
  List<FailureEvent> defects() => [
    for (final event in fiber._finalEvents)
      if (event.cause is CauseDefect) event,
  ];
  if (fiber._exit != null) return defects();
  return fiberCompletion(fiber).then((_) => defects());
}

// One failure occurrence retained across execution and ownership boundaries.
final class FailureEvent {
  const FailureEvent(this.cause, this.source);

  final Cause<Object?> cause;
  final FailureSource source;
}

// Only explicit reporting marks an occurrence. Reads and listeners never do.
final class FailureSource {
  final reportedTo = <void Function(Cause<Object?>)>{};
}
