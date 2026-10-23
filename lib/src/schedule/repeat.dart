import 'package:any/any.dart';

import 'sleep.dart';

/// Repeats successful task executions with caller-supplied delays.
extension TaskRepeat<T, E, R> on Task<T, E, R> {
  /// A task that repeats successes according to [delays].
  ///
  /// Executes once immediately, then once per delay, returning the last value.
  /// An empty iterable executes once. Any failure stops the sequence and keeps
  /// its cause. Values from earlier runs are discarded rather than accumulated.
  ///
  /// Each execution has a fresh scope and finishes cleanup before waiting or
  /// returning. Resources returned by an execution are already released; put
  /// resource acquisition and use inside the repeated task. Dependencies are
  /// shared by reference, and the caller's scope remains caller-owned.
  ///
  /// A fresh delay iterator is created lazily after the first success on each
  /// run. Iteration errors become defects. Waits are cancellable, including
  /// zero-delay waits, which yield to the event queue. Cancellation stops future
  /// executions and returns a [CauseCancelled]. An infinite iterable continues
  /// until a failure or cancellation occurs.
  ///
  /// ```dart
  /// final polling = task.repeat(
  ///   delays: Iterable.generate(4, (_) => const Duration(seconds: 1)),
  /// ); // Five executions, with one second after each of the first four.
  /// ```
  Task<T, E, R> repeat({required Iterable<Duration> delays}) => Task((
    context,
  ) async {
    Iterator<Duration>? intervals;
    while (true) {
      final exit = await run(
        context.dependencies,
        cancellation: context.cancellation,
      );
      if (exit is ExitFailure<T, E>) return exit;
      if (context.cancellation.isCancelled) {
        return Exit.failure(
          Cause.cancelled(reason: context.cancellation.reason),
        );
      }
      final iterator = intervals ??= delays.iterator;
      if (!iterator.moveNext()) return exit;
      final waiting = await sleep<R>(iterator.current).runIn(context);
      if (waiting case ExitFailure(:final cause)) return Exit.failure(cause);
    }
  });
}
