import 'dart:async';

import 'package:any/any.dart';

/// Runs [body] with an owned resource scope and dynamically forked children.
///
/// [TaskGroup.context] shares the caller's dependencies and cancellation token,
/// with a fresh scope for the body. Each forked child owns another scope. When
/// the body ends, the group requests cancellation of every unfinished child
/// before awaiting any of them. All children and their cleanup finish before
/// the body's resources are released. Work that ignores cancellation can delay
/// exit; arbitrary futures are never interrupted forcibly.
///
/// Children do not automatically cancel the body or each other on failure.
/// Calling [TaskHandle.join] transfers responsibility for that child's outcome
/// to the caller. Unjoined failures are combined after the body's failure in
/// fork order, followed by body-scope cleanup failures. Internal cancellation
/// from normal group shutdown is omitted; cleanup defects are always retained
/// in the child's outcome. Explicit or parent cancellation keeps its reason.
///
/// Creation is lazy; each run creates a new group. Returned resources from the
/// body or children have been released by the time this task completes.
///
/// ```dart
/// final task = taskGroup<int, Never, ()>((group) async {
///   final child = group.fork(
///     Task<int, Never, ()>((_) => const Exit.success(42)),
///   );
///   return await child.join();
/// });
/// final exit = await task.run(());
/// ```
Task<T, E, R> taskGroup<T, E, R>(
  FutureOr<Exit<T, E>> Function(TaskGroup<E, R> group) body,
) => Task((parent) async {
  final scope = Scope();
  final group = TaskGroup<E, R>._(parent.withScope(scope));
  final removeListener = parent.cancellation.onCancel((reason) {
    for (final child in group._children.toList(growable: false)) {
      child.cancel(reason: reason);
    }
  });
  try {
    var exit = await Task<T, E, R>((_) => body(group)).runIn(group.context);
    final failures = await group._close();
    if (failures.isNotEmpty) {
      exit = Exit.failure(
        Cause.combine([
          if (exit case ExitFailure(:final cause)) cause,
          ...failures,
        ]),
      );
    }
    try {
      await scope.close(exit: exit);
    } on ScopeCloseException catch (error) {
      exit = Exit.failure(
        Cause.combine([
          if (exit case ExitFailure(:final cause)) cause,
          error.cause,
        ]),
      );
    }
    return exit;
  } finally {
    removeListener();
  }
});

/// A group of child executions owned by a [taskGroup] body.
///
/// Use [fork] to start children and [context] for sequential work and resources
/// shared with those children. Only the group closes this context's scope;
/// calling its [Scope.close] from the body can release resources too early.
/// A group cannot be reused after its body ends.
final class TaskGroup<E, R> {
  TaskGroup._(this.context);

  /// The body context, whose resources outlive all forked children.
  final Context<R> context;
  final _children = <TaskHandle<Object?, E>>{};
  bool _closed = false;

  /// Starts one execution of [task] and returns its handle immediately.
  ///
  /// The child starts in a microtask with the same dependency object, its own
  /// scope, and its own cancellation controller linked to the parent. Cancelling
  /// this child does not cancel its parent or siblings. A cancelled parent skips
  /// new child bodies. Repeated forks start independent executions.
  ///
  /// Throws a [StateError] after the group body has ended. Completed, unjoined
  /// outcomes are retained until joined or the group closes, so join children
  /// as they finish in long-lived groups to release their retained results.
  TaskHandle<T, E> fork<T>(Task<T, E, R> task) {
    if (_closed || context.scope.isClosed) {
      throw StateError('Cannot fork into a closed task group.');
    }
    final controller = CancellationController();
    if (context.cancellation.isCancelled) {
      controller.cancel(reason: context.cancellation.reason);
    }
    final handle = TaskHandle<T, E>._(controller);
    _children.add(handle);
    handle._onObserved = () {
      if (handle.isCompleted) _children.remove(handle);
    };
    handle._future = task
        .run(
          context.dependencies,
          cancellation: controller.token,
          yieldEvery: context.yieldEvery,
        )
        .then((exit) {
          handle._completed = true;
          if (handle._observed) _children.remove(handle);
          return exit;
        });
    return handle;
  }

  Future<List<Cause<E>>> _close() async {
    _closed = true;
    final children = _children.toList(growable: false);
    final reason = Object();
    for (final child in children) {
      child.cancel(reason: reason);
    }
    final failures = <Cause<E>>[];
    for (final child in children) {
      final exit = await child._future;
      child._onObserved = null;
      if (!child._observed && exit is ExitFailure<Object?, E>) {
        final cause = exit.cause;
        final causes = cause is CauseGroup<E> ? cause.causes : [cause];
        failures.addAll(
          causes.where(
            (cause) =>
                cause is! CauseCancelled<E> || !identical(cause.reason, reason),
          ),
        );
      }
    }
    _children.clear();
    return failures;
  }
}

/// One execution of a task, including its resource cleanup.
///
/// Obtain a handle from [TaskGroup.fork]. A task describes reusable work; a
/// handle refers to exactly one execution. The owning group always waits for
/// that execution to end, whether or not [join] is called.
final class TaskHandle<T, E> {
  TaskHandle._(this._controller);

  final CancellationController _controller;
  late final Future<Exit<T, E>> _future;
  bool _completed = false;
  bool _observed = false;
  void Function()? _onObserved;

  /// Whether the body and all of its resource cleanup have completed.
  bool get isCompleted => _completed;

  /// Waits for the body and cleanup, returning the same outcome on every call.
  ///
  /// Calling this method marks the outcome as observed: the group no longer
  /// automatically aggregates it. Await the returned future and handle or
  /// propagate its [Exit], including any cleanup failures. Merely cancelling
  /// or inspecting [isCompleted] does not mark an outcome as observed.
  Future<Exit<T, E>> join() {
    _observed = true;
    _onObserved?.call();
    _onObserved = null;
    return _future;
  }

  /// Requests cooperative cancellation without waiting for completion.
  ///
  /// The first reason wins. Finished executions are unchanged. Use [join] to
  /// wait for work and cleanup after requesting cancellation.
  void cancel({Object? reason}) {
    if (!_completed) _controller.cancel(reason: reason);
  }
}
