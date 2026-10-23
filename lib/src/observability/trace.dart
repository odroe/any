import 'dart:async';

import 'package:any/any.dart';

/// The completed body of a named task execution.
///
/// Payloads are retained by reference. The enclosing scope may still be open;
/// [exit] does not include cleanup performed later by that scope's owner.
final class TaskSpan<T, E> {
  /// Records a body outcome and the time spent awaiting it.
  const TaskSpan({
    required this.name,
    required this.elapsed,
    required this.exit,
  });

  /// The caller-supplied label for this operation.
  final String name;

  /// Monotonic elapsed time, including asynchronous waits within the body.
  final Duration elapsed;

  /// The body's outcome, including observed cancellation or defects.
  final Exit<T, E> exit;
}

/// Adds opt-in observation without changing task dependencies or ownership.
extension TaskTracing<T, E, R> on Task<T, E, R> {
  /// A task that reports one [TaskSpan] to [onEnd] after its body finishes.
  ///
  /// [name] labels the operation. Each run starts its own [Stopwatch] and awaits
  /// the task in the same context. The callback runs synchronously before the
  /// result is returned; asynchronous work started by the callback is not
  /// awaited. Synchronous callback errors are reported to the executing [Zone]
  /// as uncaught errors, without replacing the task's outcome.
  ///
  /// A task skipped because its context is already cancelled or closed emits
  /// no span. Otherwise successes, failures, defects, and cancellation are all
  /// reported. The enclosing scope's later cleanup is outside this span. Wrap
  /// a whole operation, such as a concurrency group, to include cleanup that
  /// the operation itself awaits.
  ///
  /// This wrapper installs no global state or backend. Use [onEnd] to integrate
  /// with a logger, metrics collector, or application tracing service. Tasks
  /// without this wrapper incur no tracing overhead.
  ///
  /// ```dart
  /// final observed = task.trace('load-user', onEnd: (span) {
  ///   print('${span.name}: ${span.elapsed.inMicroseconds} us, ${span.exit}');
  /// });
  /// ```
  Task<T, E, R> trace(
    String name, {
    required void Function(TaskSpan<T, E> span) onEnd,
  }) => Task((context) async {
    final stopwatch = Stopwatch()..start();
    final exit = await runIn(context);
    stopwatch.stop();
    Zone.current.runUnaryGuarded(
      onEnd,
      TaskSpan(name: name, elapsed: stopwatch.elapsed, exit: exit),
    );
    return exit;
  });
}
