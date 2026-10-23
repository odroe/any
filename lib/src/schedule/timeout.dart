import 'dart:async';

import 'package:any/any.dart';

/// Adds a cancellation deadline to a task body.
extension TaskTimeout<T, E, R> on Task<T, E, R> {
  /// A task that requests cancellation after [duration].
  ///
  /// The deadline starts when execution begins. Expiry cancels a child token
  /// with a [TimeoutException] as its reason. Parent cancellation is forwarded;
  /// the first request wins. Zero and negative durations use a zero-delay timer.
  ///
  /// The body shares the caller's scope and dependencies. It must cooperate
  /// with cancellation; running work is always awaited. A late success becomes
  /// a [CauseCancelled], while a late failure keeps its cause. This is a deadline
  /// for requesting cancellation, not a limit on how long awaiting the task
  /// can take. Synchronous work cannot be preempted.
  ///
  /// The timer and listener are removed when the body finishes. Scope cleanup
  /// remains the owner's responsibility and is outside the deadline. The child
  /// token cannot cancel the parent or unrelated work in the parent's scope.
  ///
  /// ```dart
  /// final bounded = task.timeout(const Duration(seconds: 5));
  /// // A timeout is ExitFailure(CauseCancelled(reason: TimeoutException(...))).
  /// ```
  Task<T, E, R> timeout(Duration duration) => Task((context) async {
    final controller = CancellationController();
    Timer? timer;
    final removeListener = context.cancellation.onCancel((reason) {
      timer?.cancel();
      controller.cancel(reason: reason);
    });
    try {
      if (!controller.token.isCancelled) {
        timer = Timer(duration, () {
          controller.cancel(
            reason: TimeoutException('Task timed out.', duration),
          );
        });
      }
      return await runIn(
        Context(
          scope: context.scope,
          dependencies: context.dependencies,
          cancellation: controller.token,
        ),
      );
    } finally {
      timer?.cancel();
      removeListener();
    }
  });
}
