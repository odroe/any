import 'package:any/any.dart';

import 'sleep.dart';

/// Retries expected task failures with caller-supplied delays.
extension TaskRetry<T, E, R> on Task<T, E, R> {
  /// A task that retries single expected failures according to [delays].
  ///
  /// The first attempt starts immediately. Each delay permits one additional
  /// attempt; an empty iterable permits none. Delays are read lazily, only after
  /// an eligible failure, with a new iterator for each run. Native iterables can
  /// express fixed intervals, backoff, or an unbounded retry policy.
  ///
  /// If supplied, [when] must accept the error before a delay is read. Defects,
  /// cancellation, and grouped causes are never retried. A cancelled parent
  /// stops new attempts. Waiting is cancellable, including zero-delay waits,
  /// which yield to the event queue. A cancelled wait returns cancellation;
  /// exhausted delays return the last attempt's failure. Earlier failures are
  /// discarded. Errors thrown by the policy become defects.
  ///
  /// Each attempt owns a fresh scope and finishes cleanup before the next
  /// attempt or the final result. Cleanup errors stop retries. Put acquisition
  /// and use of a resource inside the retried task; returned resources are
  /// already released. The caller's dependencies are shared by reference.
  ///
  /// ```dart
  /// final resilient = task.retry(
  ///   delays: const [Duration(milliseconds: 100), Duration(milliseconds: 200)],
  /// ); // At most three attempts.
  /// ```
  Task<T, E, R> retry({
    required Iterable<Duration> delays,
    bool Function(E error)? when,
  }) => Task((context) async {
    Iterator<Duration>? intervals;
    while (true) {
      final exit = await run(
        context.dependencies,
        cancellation: context.cancellation,
      );
      if (exit case ExitFailure(cause: CauseFailure(:final error))) {
        if (context.cancellation.isCancelled || when != null && !when(error)) {
          return exit;
        }
        final iterator = intervals ??= delays.iterator;
        if (!iterator.moveNext()) return exit;
        if (context.cancellation.isCancelled) return exit;
        final waiting = await sleep<R>(iterator.current).runIn(context);
        if (waiting case ExitFailure(:final cause)) return Exit.failure(cause);
      } else {
        return exit;
      }
    }
  });
}
