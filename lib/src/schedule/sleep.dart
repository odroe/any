import 'dart:async';

import 'package:any/any.dart';

/// A task that waits for [duration], or stops waiting when cancelled.
///
/// Uses a Dart [Timer], so zero and negative durations yield to the event queue.
/// Cancellation cancels the timer and returns a [CauseCancelled] with the
/// caller's reason. Each run owns its timer and removes its cancellation
/// listener when finished. Creating the task does not start a timer.
///
/// ```dart
/// await sleep<()>(const Duration(milliseconds: 100)).run(());
/// ```
Task<void, Never, R> sleep<R>(Duration duration) => Task((context) {
  final completion = Completer<Exit<void, Never>>();
  void finish(Exit<void, Never> exit) {
    if (!completion.isCompleted) completion.complete(exit);
  }

  final timer = Timer(duration, () => finish(const Exit.success(null)));
  final removeListener = context.cancellation.onCancel((reason) {
    timer.cancel();
    finish(Exit.failure(Cause.cancelled(reason: reason)));
  });
  return completion.future.whenComplete(removeListener);
});
