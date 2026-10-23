import 'dart:async';

import 'cancellation.dart';
import 'cause.dart';
import 'clock.dart';
import 'context.dart';
import 'exit.dart';
import 'fiber.dart';
import 'layer.dart';
import 'logger.dart';
import 'metric.dart';
import 'scope.dart';
import 'random_source.dart';
import 'task.dart';
import 'tracer.dart';

/// The application boundary for running tasks and owning their services.
///
/// Use [run] to obtain an [Exit], [runOrThrow] to integrate with exception-based
/// code, or [start] to keep a [Fiber] handle. Each call starts an independent
/// execution. [close] stops owned work and releases application resources.
///
/// ```dart
/// final runtime = Runtime();
/// try {
///   final result = await runtime.run(Task.sync(() => 6 * 7));
///   switch (result) {
///     case ExitSuccess(:final value):
///       print(value); // 42
///     case ExitFailure(:final cause):
///       print('Execution failed: $cause');
///   }
/// } finally {
///   final closed = await runtime.close();
///   if (closed case ExitFailure(:final cause)) {
///     print('Cleanup failed: $cause');
///   }
/// }
/// ```
///
/// Use [create] with a [Layer] when the runtime should also acquire and release
/// application services. Inspect the closing result at the host boundary: cleanup
/// can fail independently of the tasks that used those services.
final class Runtime {
  /// Creates a runtime with application services and failure reporting.
  ///
  /// [context] overrides matching default bindings for [Clock], [RandomSource],
  /// [Logger], [Tracer], and [MetricRegistry]. Unspecified defaults remain available;
  /// logging, tracing, and metrics default to no-ops. Supplied service objects keep
  /// their caller-managed lifetime.
  ///
  /// [onUnhandledError] receives business failures and defects that remain
  /// unobserved on the next event-loop turn. Each failure is reported once;
  /// cancellation alone is not reported. Without a handler, a [TaskException]
  /// is reported to the creating [Zone]. Exceptions thrown by the handler are
  /// also reported to that zone and cannot interrupt cleanup.
  Runtime({
    Context? context,
    void Function(Cause<Object?> cause)? onUnhandledError,
  }) : _context = overlayContexts(
         Context.from([
           Clock.key.bind(Clock()),
           RandomSource.key.bind(RandomSource()),
           Logger.key.bind(Logger.noop()),
           Tracer.key.bind(Tracer.noop()),
           MetricRegistry.key.bind(MetricRegistry.noop()),
         ]),
         context ?? Context.from(const []),
       ) {
    final zone = Zone.current;
    _report = (cause) {
      try {
        if (onUnhandledError != null) {
          onUnhandledError(cause);
        } else {
          zone.handleUncaughtError(TaskException(cause), _stackTrace(cause));
        }
      } catch (error, stackTrace) {
        zone.handleUncaughtError(error, stackTrace);
      }
    };
  }

  final _scope = createScope();
  Context _context;
  late final void Function(Cause<Object?>) _report;
  Future<Exit<void, Never>>? _closing;

  /// Creates a runtime that acquires and owns application [services].
  ///
  /// The layer uses [context] and the default services as inputs. Its output
  /// bindings override matching inputs for subsequent executions. Acquired
  /// resources live until [close], which stops tasks before releasing services.
  /// Objects supplied directly through [context] remain externally owned.
  ///
  /// Failure or startup cancellation returns an [ExitFailure] after releasing any
  /// acquired resources, preserving cleanup failures in its cause. [signal]
  /// affects startup only; close the returned runtime to stop it after startup.
  static Future<Exit<Runtime, E>> create<E>(
    Layer<E> services, {
    Context? context,
    CancellationSignal? signal,
    void Function(Cause<Object?> cause)? onUnhandledError,
  }) async {
    final runtime = Runtime(
      context: context,
      onUnhandledError: onUnhandledError,
    );
    final building = runtime.run(
      buildLayer(services, runtime._scope),
      signal: signal,
    );
    final unsubscribe = signal?.onCancel((reason) {
      unawaited(runtime._beginClose(Exit.failure(Cause.cancelled(reason))));
    });
    final built = await building;
    unsubscribe?.call();
    Cause<E>? failure;
    switch (built) {
      case ExitSuccess(:final value):
        if (signal?.isCancelled ?? false) {
          failure = Cause.cancelled(signal!.reason);
        } else {
          runtime._context = overlayContexts(runtime._context, value);
          return Exit.success(runtime);
        }
      case ExitFailure(:final cause):
        failure = cause;
    }
    final closed = await runtime._beginClose(Exit.failure(failure));
    return Exit.failure(switch (closed) {
      ExitSuccess() => failure,
      ExitFailure(:final cause) => Cause.combine([failure, cause]),
    });
  }

  /// Starts [task] and returns a handle to its independent execution.
  ///
  /// [signal] can cancel this execution without affecting other roots. The fiber
  /// completes after its child work and resource cleanup. Observe its result with
  /// [Fiber.join], [Fiber.awaitExit], or [Fiber.poll] to handle failures directly.
  ///
  /// A closing or closed runtime returns a completed fiber with a [RuntimeClosed]
  /// defect; the task does not start.
  Fiber<T, E> start<T, E>(Task<T, E> task, {CancellationSignal? signal}) {
    if (_closing != null) {
      return completedFiber(
        Exit<T, E>.failure(
          Cause.defect(const RuntimeClosed(), StackTrace.current),
        ),
        report: _report,
      );
    }
    return startFiber(
      task,
      owner: _scope,
      report: _report,
      signal: signal,
      context: _context,
    );
  }

  /// Runs [task] and returns its value or complete failure as an [Exit].
  ///
  /// The result includes child completion and resource cleanup, and counts as an
  /// observed fiber result for failure reporting.
  ///
  /// [signal] cancels only this execution. An already cancelled signal prevents
  /// the task from starting. Cancellation waits for any in-flight,
  /// non-cancellable [Future] to settle before returning.
  Future<Exit<T, E>> run<T, E>(Task<T, E> task, {CancellationSignal? signal}) =>
      fiberCompletion(start(task, signal: signal));

  /// Runs [task] and returns its value, throwing [TaskException] on failure.
  ///
  /// The exception retains the complete [Cause], including original defect stack
  /// traces. Execution, cancellation, and cleanup follow [run]. Prefer [run] when
  /// the caller can handle an [Exit] directly.
  Future<T> runOrThrow<T, E>(
    Task<T, E> task, {
    CancellationSignal? signal,
  }) async => switch (await run(task, signal: signal)) {
    ExitSuccess(:final value) => value,
    ExitFailure(:final cause) => throw TaskException(cause),
  };

  /// Stops this runtime and returns the outcome of shutting it down.
  ///
  /// New work is rejected. Active roots are cancelled and fully cleaned up before
  /// application services are released. Repeated calls share the same result.
  /// Non-cancellable [Future]s must settle before shutdown can finish.
  ///
  /// Root business failures remain in their executions' exits. Defects encountered
  /// while stopping active roots, including cleanup failures, are also included in
  /// the closing result.
  ///
  /// Call this from the host that owns the runtime. A task or finalizer whose
  /// completion the runtime must await cannot close it: the call returns a
  /// [StateError] defect. This includes tasks holding resource leases that the
  /// runtime must wait to release, even when those tasks run in another runtime.
  Future<Exit<void, Never>> close() {
    if (currentFibers().any(
          (fiber) => fiberIsActive(fiber) && scopeWaitsForFiber(_scope, fiber),
        ) ||
        currentFinalizingScopes().any(
          (scope) => scopeCloseWouldDeadlock(_scope, scope),
        )) {
      return Future.value(
        Exit.failure(
          Cause.defect(
            StateError('A task cannot close its own runtime.'),
            StackTrace.current,
          ),
        ),
      );
    }
    return _beginClose(const Exit.success(null));
  }

  Future<Exit<void, Never>> _beginClose(Exit<Object?, Object?> exit) {
    final closing = _closing;
    if (closing != null) return closing;
    final completion = Completer<Exit<void, Never>>();
    _closing = completion.future;
    unawaited(_close(completion, exit));
    return completion.future;
  }

  Future<void> _close(
    Completer<Exit<void, Never>> completion,
    Exit<Object?, Object?> exit,
  ) async {
    final cause = await closeScope(_scope, exit);
    completion.complete(
      cause == null ? const Exit.success(null) : Exit.failure(cause),
    );
  }
}

/// An execution rejected because its [Runtime] is closing or closed.
final class RuntimeClosed implements Exception {
  /// Creates a closed-runtime diagnostic.
  const RuntimeClosed();

  @override
  String toString() => 'RuntimeClosed: The runtime no longer accepts tasks.';
}

/// A task's complete failure represented as a Dart exception.
///
/// [Runtime.runOrThrow] throws this exception when a task fails. Inspect [cause]
/// for business errors, defects with their original stack traces, or cancellation.
final class TaskException implements Exception {
  /// Creates an exception retaining the complete [cause].
  const TaskException(this.cause);

  /// The original business failures, defects, and cancellation reasons.
  final Cause<Object?> cause;

  @override
  String toString() => 'TaskException: $cause';
}

StackTrace _stackTrace(Cause<Object?> cause) => switch (cause) {
  CauseDefect(:final stackTrace) => stackTrace,
  CauseGroup(:final causes) => _stackTrace(causes.first),
  _ => StackTrace.current,
};
