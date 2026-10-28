import 'cause.dart';
import 'exit.dart';
import 'fiber.dart';
import 'internal/completion.dart';
import 'scope.dart';
import 'task.dart';

/// Dynamically submitted tasks owned by one [Scope].
///
/// Use [start] for jobs that arrive over time. A member failure becomes available
/// through [join] without stopping the other members or closing the group.
/// [awaitEmpty] waits only for the current members to finish, regardless of their
/// results. Both waits can be cancelled without cancelling the members.
///
/// The first qualifying failure is retained for current and future [join] calls.
/// Later independent failures use the submitting runtime's error handler. Reading
/// a member's result does not change this responsibility. Scope shutdown retains
/// cleanup defects through its own result instead of reporting them again.
///
/// ```dart
/// final runtime = Runtime();
/// final work = FiberGroup.create<Never>().flatMap((jobs) {
///   return jobs.start(Task.sync(() => 6 * 7)).flatMap((job) => job.join());
/// });
/// try {
///   final answer = await runtime.runOrThrow(work);
///   assert(answer == 42);
/// } finally {
///   await runtime.close();
/// }
/// ```
///
/// Keep the group inside its creating scope's lifetime. Scope closure rejects
/// submissions, cancels members and waits for their native work and cleanup.
/// The group does not retain successful member results; keep a returned [Fiber]
/// when its value is needed.
final class FiberGroup<E> {
  FiberGroup._(this._scope);

  final Scope _scope;
  final _members = <Fiber<Object?, E>>{};
  final _result = Completion<TaskEvaluation<void, E>>();
  var _empty = Completion<Exit<void, Never>>();

  /// Creates an empty group owned by the current scope on each execution.
  ///
  /// Returning a group from a finished `Runtime.run` does not keep it open.
  /// Use an application service or a longer enclosing task for long-lived jobs.
  static Task<FiberGroup<E>, Never> create<E>() => accessTaskEnvironment(
    (scope, _) => Task.acquireRelease(
      Task.sync(() => FiberGroup<E>._(scope)),
      release: (group, _) => Task.sync(() {
        group._result.complete(const TaskEvaluation(Exit.success(null), []));
      }),
    ),
  );

  /// Submits [task] and returns its typed handle without waiting for completion.
  ///
  /// The submitting task must belong to the same runtime. The member inherits
  /// that task's context, but cancelling the submitter does not cancel the member.
  /// Acquisitions belong to the group's scope, as with `Task.forkIn`; use
  /// `task.scoped()` for resources that should close when that member finishes.
  ///
  /// Pure cancellation is ignored by [join] unless [propagateCancellation] is
  /// true. Cancellation from scope shutdown never introduces a new group failure;
  /// independent errors and cleanup defects in mixed causes are still retained.
  /// A closed scope rejects the submission with a [StateError] defect.
  Task<Fiber<T, E>, Never> start<T>(
    Task<T, E> task, {
    bool propagateCancellation = false,
  }) => currentTaskRunScope().flatMap((caller) {
    if (!identical(scopeRuntimeId(caller), scopeRuntimeId(_scope))) {
      throw StateError(
        'Cannot submit to a fiber group owned by another runtime.',
      );
    }
    return forkTaskIn(
      task,
      _scope,
      deferStart: false,
      onRegistered: (fiber) {
        if (_members.isEmpty) _empty = Completion();
        _members.add(fiber);
        fiber.onExit((exit) => _finished(fiber, exit, propagateCancellation));
      },
    );
  });

  /// Waits for the first qualifying failure, or succeeds when the group closes.
  ///
  /// An open, empty group keeps waiting. Recovering this task or waiting again
  /// does not erase a recorded failure. Failure does not stop other members;
  /// letting it leave the owning scope triggers normal scope cleanup.
  Task<void, E> join() => Task.fromCallback<TaskEvaluation<void, E>, E>(
    (complete) => _result.listen((result) => complete(Exit.success(result))),
  ).flatMap(taskFromEvaluation);

  /// Waits until no members remain, including their owned cleanup.
  ///
  /// Returns immediately when empty, including after closure. This does not
  /// assert that members succeeded, close the group or reset its first failure.
  /// Future submissions can make the group nonempty again.
  Task<void, Never> awaitEmpty() => Task.defer(
    () => _members.isEmpty
        ? Task.succeed(null)
        : Task.fromCallback(_empty.listen),
  );

  void _finished<T>(
    Fiber<T, E> fiber,
    Exit<T, E> exit,
    bool propagateCancellation,
  ) {
    _members.remove(fiber);
    if (exit case ExitFailure(:final cause)) {
      final reasons = switch (cause) {
        CauseGroup(:final causes) => causes,
        _ => [cause],
      };
      final cancelledOnly = reasons.every((reason) => reason is CauseCancelled);
      if (!cancelledOnly ||
          (propagateCancellation && !scopeIsClosing(_scope))) {
        final result = TaskEvaluation<void, E>(
          Exit.failure(cause),
          fiberFailureEvents(fiber),
          cleanupCancellations: fiberCleanupCancellations(fiber),
        );
        if (!_result.complete(result)) {
          reportFiberFailure(
            fiber,
            excluded: {for (final event in _result.value!.events) event.source},
          );
        }
      }
    }
    if (_members.isEmpty) _empty.complete(const Exit.success(null));
  }
}
