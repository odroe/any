import 'dart:async';

import 'package:any/any.dart';

/// Runs [tasks] with a limit, collecting every completed [Exit] in input order.
///
/// Child failures are returned as data and do not stop other children. Each
/// child owns a fresh [Scope]; its exit includes cleanup failures, and its slot
/// is reused only after cleanup. Successful resource values are already
/// released when returned. Dependencies are shared by reference.
///
/// Input is read lazily once per run, as slots become available. Empty input
/// succeeds with an empty list. Parent cancellation or an iteration error stops
/// dispatch, cancels running work cooperatively, and awaits bodies and cleanup.
/// In that case the batch fails: failures from started children appear in input
/// order, followed by the iteration error and parent cancellation, if present.
/// Internal cancellation used to stop an invalid iterator is omitted. Partial
/// successful values are not returned from an aborted batch.
///
/// Throws a [RangeError] immediately if [concurrency] is less than one.
///
/// ```dart
/// final batch = allSettled([
///   Task<int, String, ()>((_) => const Exit.success(1)),
///   Task<int, String, ()>((_) => const Exit.failure(Cause.failure('missing'))),
/// ], concurrency: 2);
/// final exit = await batch.run(()); // Success containing two child exits.
/// ```
Task<List<Exit<T, E>>, E, R> allSettled<T, E, R>(
  Iterable<Task<T, E, R>> tasks, {
  required int concurrency,
}) {
  if (concurrency < 1) {
    throw RangeError.range(concurrency, 1, null, 'concurrency');
  }
  return Task((context) async {
    final controller = CancellationController();
    final stopReason = Object();
    final removeListener = context.cancellation.onCancel(
      (reason) => controller.cancel(reason: reason),
    );
    final outcomes = <Exit<T, E>?>[];
    Cause<E>? iterationFailure;
    try {
      try {
        final iterator = tasks.iterator;
        var exhausted = false;
        Future<void> worker() async {
          while (!exhausted && !controller.token.isCancelled) {
            Task<T, E, R> task;
            try {
              if (!iterator.moveNext()) {
                exhausted = true;
                return;
              }
              if (controller.token.isCancelled) return;
              task = iterator.current;
              if (controller.token.isCancelled) return;
            } catch (error, stackTrace) {
              iterationFailure = Cause.defect(error, stackTrace);
              controller.cancel(reason: stopReason);
              return;
            }
            final index = outcomes.length;
            outcomes.add(null);
            outcomes[index] = await task.run(
              context.dependencies,
              cancellation: controller.token,
            );
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
      } catch (error, stackTrace) {
        iterationFailure = Cause.defect(error, stackTrace);
      }

      if (iterationFailure != null || context.cancellation.isCancelled) {
        final failures = <Cause<E>>[];
        for (final exit in outcomes) {
          if (exit case ExitFailure(:final cause)) {
            final causes = cause is CauseGroup<E> ? cause.causes : [cause];
            failures.addAll(
              causes.where(
                (cause) =>
                    cause is! CauseCancelled<E> ||
                    !identical(cause.reason, stopReason),
              ),
            );
          }
        }
        if (iterationFailure != null) failures.add(iterationFailure!);
        if (context.cancellation.isCancelled) {
          failures.add(Cause.cancelled(reason: context.cancellation.reason));
        }
        return Exit.failure(Cause.combine(failures));
      }
      return Exit.success(outcomes.cast<Exit<T, E>>().toList(growable: false));
    } finally {
      removeListener();
    }
  });
}
