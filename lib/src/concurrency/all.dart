import 'dart:async';

import 'package:any/any.dart';

/// A task that runs [tasks] with at most [concurrency] children in progress.
///
/// Returns successful values in input order, regardless of completion order.
/// An empty input succeeds with an empty list. The input is iterated lazily,
/// once per run, as capacity becomes available. Creating this task does not
/// iterate the input or execute any children.
///
/// Each child receives the parent's dependencies by reference and owns a fresh
/// [Scope]. A concurrency slot remains occupied until that child's cleanup
/// finishes. Resources acquired by a child are released before its slot is
/// reused, even if its returned value references those resources. Resources in
/// a context borrowed through [Task.runIn] remain owned by that context's scope.
///
/// An unsuccessful body stops new work and requests cooperative cancellation
/// of the other children. A cleanup failure also stops new work once that
/// child's cleanup finishes. Parent cancellation is forwarded with its reason.
/// Running children and their cleanup are always awaited, including callbacks
/// that continue running after cancellation. Cleanup itself is not interrupted.
///
/// Failure causes are combined in input order, preserving each child's cause
/// order. Cancellation caused by stopping siblings after a failure is omitted;
/// other cancellation causes, expected errors, and cleanup defects are retained.
/// Iteration errors become defects after causes from previously started inputs.
/// Multiple causes form a flat [CauseGroup], including repeated parent
/// cancellation causes when several children observe the same request.
///
/// Throws a [RangeError] immediately if [concurrency] is less than one.
///
/// ```dart
/// import 'package:any/any.dart';
/// import 'package:any/concurrency.dart';
///
/// final tasks = [
///   for (final value in [1, 2, 3])
///     Task<int, Never, int>(
///       (context) => Exit.success(value * context.dependencies),
///     ),
/// ];
/// final exit = await all(tasks, concurrency: 2).run(10);
/// if (exit case ExitSuccess(:final value)) {
///   print(value); // [10, 20, 30]
/// }
/// ```
Task<List<T>, E, R> all<T, E, R>(
  Iterable<Task<T, E, R>> tasks, {
  required int concurrency,
}) {
  if (concurrency < 1) {
    throw RangeError.range(concurrency, 1, null, 'concurrency');
  }

  return Task((context) async {
    final controller = CancellationController();
    final siblingCancellation = Object();
    final removeListener = context.cancellation.onCancel(
      (reason) => controller.cancel(reason: reason),
    );
    final outcomes = <Exit<T, E>?>[];

    void stop() => controller.cancel(reason: siblingCancellation);

    try {
      final iterator = tasks.iterator;
      var exhausted = false;

      Future<void> worker() async {
        while (!exhausted && !controller.token.isCancelled) {
          int? index;
          try {
            if (!iterator.moveNext()) {
              exhausted = true;
              return;
            }
            if (controller.token.isCancelled) return;
            final task = iterator.current;
            if (controller.token.isCancelled) return;
            index = outcomes.length;
            outcomes.add(null);

            // Observe body failure before cleanup, while Task.run retains
            // ownership of the child's scope and combines cleanup failures.
            final exit =
                await Task<T, E, R>((child) async {
                  final exit = await task.runIn(child);
                  if (exit is ExitFailure<T, E>) stop();
                  return exit;
                }).run(
                  context.dependencies,
                  cancellation: controller.token,
                  yieldEvery: context.yieldEvery,
                );

            outcomes[index] = exit;
            if (exit is ExitFailure<T, E>) stop();
          } catch (error, stackTrace) {
            final exit = Exit<T, E>.failure(Cause.defect(error, stackTrace));
            if (index == null) {
              outcomes.add(exit);
            } else {
              outcomes[index] = exit;
            }
            stop();
          }
        }
      }

      final workers = <Future<void>>[];
      for (
        var i = 0;
        i < concurrency && !exhausted && !controller.token.isCancelled;
        i++
      ) {
        workers.add(worker());
      }
      await Future.wait(workers);

      List<Cause<E>>? failures;
      for (final exit in outcomes) {
        if (exit case ExitFailure(:final cause)) {
          final failure = _withoutCancellation(cause, siblingCancellation);
          if (failure != null) (failures ??= []).add(failure);
        }
      }
      if (failures != null) return Exit.failure(Cause.combine(failures));

      return Exit.success([
        for (final exit in outcomes) (exit as ExitSuccess<T, E>).value,
      ]);
    } finally {
      removeListener();
    }
  });
}

Cause<E>? _withoutCancellation<E>(Cause<E> cause, Object reason) {
  bool keep(Cause<E> cause) =>
      cause is! CauseCancelled<E> || !identical(cause.reason, reason);

  if (cause is! CauseGroup<E>) return keep(cause) ? cause : null;
  final remaining = cause.causes.where(keep).toList(growable: false);
  if (remaining.length == cause.causes.length) return cause;
  return remaining.isEmpty ? null : Cause.combine(remaining);
}
