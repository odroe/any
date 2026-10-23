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

/// Owns root task executions and waits for them when closing.
///
/// Each [run] executes its task independently. Keep the runtime open while its
/// tasks are in use, and inspect the result of [close] at the host boundary.
final class Runtime {
  /// Creates a runtime with an optional unobserved-failure reporter.
  ///
  /// [context] supplies application services and overrides the default [Clock],
  /// [RandomSource], no-op [Logger], no-op [Tracer], and no-op [MetricRegistry]
  /// bindings when their keys are present. Other defaults remain available.
  /// Supplied objects retain their caller-managed lifetime.
  ///
  /// A failed fiber is reported once on the next event-loop turn if its result
  /// has not been observed. The default forwards a [TaskException] to the Zone
  /// that creates this runtime. Exceptions thrown by [onUnhandledError] are
  /// forwarded to that same Zone; they cannot interrupt task cleanup.
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

  /// Builds application [services] and returns a runtime that owns them.
  ///
  /// Construction uses [context] and the default services as its inputs. The
  /// resulting bindings override matching inputs for subsequent executions.
  /// Acquired resources remain alive until [close], after all owned tasks stop.
  /// Existing objects supplied through [context] keep their external ownership.
  ///
  /// A construction failure or startup cancellation releases acquired services
  /// before returning its complete cause, including cleanup defects. [signal]
  /// affects startup only; use [close] to stop a successfully created runtime.
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

  /// Starts an independent root fiber owned by this runtime.
  ///
  /// [signal] affects only this execution. The fiber completes after its child
  /// tasks and resource cleanup. A closing runtime returns a completed fiber
  /// containing a [RuntimeClosed] defect, without starting business work.
  /// Observe the result through the fiber, or let the runtime report failures.
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

  /// Runs [task] and returns its complete exit after resource cleanup.
  ///
  /// [signal] requests cancellation of this execution only. An already
  /// cancelled signal prevents business work from starting. Cancellation waits
  /// for any in-flight, non-cancellable Future before completing the exit.
  /// This host boundary observes the fiber's result, including failures.
  Future<Exit<T, E>> run<T, E>(Task<T, E> task, {CancellationSignal? signal}) =>
      fiberCompletion(start(task, signal: signal));

  /// Runs [task], throwing [TaskException] if its exit is a failure.
  ///
  /// The exception retains the complete cause, including original defect stack
  /// traces. Prefer [run] inside integrations that can handle an [Exit] directly.
  Future<T> runOrThrow<T, E>(
    Task<T, E> task, {
    CancellationSignal? signal,
  }) async => switch (await run(task, signal: signal)) {
    ExitSuccess(:final value) => value,
    ExitFailure(:final cause) => throw TaskException(cause),
  };

  /// Stops accepting work, requests cancellation, and waits for all root runs.
  ///
  /// Repeated calls share the same closing result. Closing does not turn root
  /// business failures into closing failures; each caller receives its own
  /// execution's [Exit]. Defects encountered while stopping active roots are
  /// included in the closing result, including resource cleanup failures.
  /// Non-cancellable Futures must settle before closing can finish.
  ///
  /// A task owned by this runtime cannot wait for the runtime to close itself.
  /// Such a call returns a [StateError] defect instead of starting a deadlock.
  Future<Exit<void, Never>> close() {
    if (currentFibers().any(
      (fiber) => fiberIsActive(fiber) && scopeOwnsFiber(_scope, fiber),
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

/// Indicates that an execution was rejected by a closing or closed [Runtime].
final class RuntimeClosed implements Exception {
  /// Creates a closed-runtime diagnostic.
  const RuntimeClosed();

  @override
  String toString() => 'RuntimeClosed: The runtime no longer accepts tasks.';
}

/// Bridges a complete task failure into a host API that expects exceptions.
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
