import 'dart:async';

import 'package:any/any.dart';

import 'isolate_stub.dart'
    if (dart.library.io) 'isolate_native.dart'
    as platform;

/// A task that runs [computation] in a new native Dart isolate on every run.
///
/// Receives the caller's dependency value and returns an [Exit]. Dependencies,
/// the callback, and its result must be sendable between isolates. Mutable
/// dependency objects are copied rather than shared with the caller. Prefer a
/// top-level or static function to avoid closures capturing unrelated state.
/// The caller's [Context], scope, and cancellation token are not sent.
///
/// Expected failures returned by [computation] keep their error type. Thrown
/// errors, unsendable values, and worker failures become defects according to
/// native `Isolate.run` semantics; uncaught asynchronous worker errors may be
/// represented by a remote error with a remote stack trace.
///
/// Cancellation before execution skips spawning. Once spawned, execution is
/// awaited to completion; this entry point does not forward cancellation into
/// the worker or kill it. A late success is replaced with cancellation, while
/// an existing failure is retained. The worker must finish its own cleanup
/// before returning, typically with `try`/`finally` or a task run inside it.
///
/// [debugName] labels the worker for debugging. Web platforms return a defect
/// containing an [UnsupportedError] rather than running CPU work on the caller.
/// For small computations, isolate startup and data transfer can dominate cost.
///
/// ```dart
/// Exit<int, Never> sum(List<int> values) =>
///     Exit.success(values.fold(0, (sum, value) => sum + value));
///
/// final task = isolateTask(sum, debugName: 'sum');
/// final exit = await task.run([1, 2, 3]);
/// ```
Task<T, E, R> isolateTask<T, E, R>(
  FutureOr<Exit<T, E>> Function(R dependencies) computation, {
  String? debugName,
}) => Task(
  (context) => platform.executeIsolate(
    computation,
    context.dependencies,
    debugName: debugName,
  ),
);
