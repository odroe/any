import 'dart:async';

import 'cancellation.dart';
import 'cause.dart';
import 'clock.dart';
import 'context.dart';
import 'exit.dart';
import 'fiber.dart';
import 'internal/execution_mode.dart';
import 'layer.dart';
import 'logger.dart';
import 'metric.dart';
import 'option.dart';
import 'scope.dart';
import 'random_source.dart';
import 'task.dart';
import 'tracer.dart';

/// The application boundary for running tasks and owning their services.
///
/// Use [run] to obtain an [Exit], [runOrThrow] to integrate with exception-based
/// code, or [start] to keep a [Fiber] handle. Each call starts an independent
/// execution. [close] stops owned work and releases application resources.
/// Use [runSync] or [runSyncOrThrow] at interfaces that require a synchronous
/// result; asynchronous work is reported through [AsyncTaskException].
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
  /// [onUnhandledError] receives independent failures from [start] and public
  /// forks on the next event-loop turn. Reading their results does not suppress
  /// reporting. [run] and synchronous results go directly to their caller. Each
  /// failure occurrence is reported once;
  /// cancellation alone is not reported. Without a handler, a [TaskException]
  /// is reported to the creating [Zone]. Exceptions thrown by the handler are
  /// also reported to that zone and cannot interrupt cleanup.
  ///
  /// [onFiberStart] observes each accepted user root that remains unfinished
  /// after its initial evaluation, before the execution entry point returns.
  /// Synchronously completed work, rejected requests, child fibers, and internal
  /// service construction are excluded. The callback runs in the creating
  /// [Zone]; callback exceptions go to that zone without changing the task's
  /// result or cleanup. Observation does not change ownership or failure handling.
  ///
  /// Combine the hook with [Fiber.onExit] to track work through its cleanup:
  ///
  /// ```dart
  /// // Imports dart:io and package:any/any.dart.
  /// final pending = <Fiber<Object?, Object?>>{};
  /// final runtime = Runtime(onFiberStart: (fiber) {
  ///   pending.add(fiber);
  ///   fiber.onExit((_) => pending.remove(fiber));
  /// });
  /// try {
  ///   await runtime.runOrThrow(Task.fromFuture(
  ///     () => File('settings.json').readAsString(),
  ///     onError: (error, _) => error,
  ///   ));
  /// } finally {
  ///   await runtime.close();
  /// }
  /// ```
  Runtime({
    Context? context,
    void Function(Cause<Object?> cause)? onUnhandledError,
    void Function(Fiber<Object?, Object?> fiber)? onFiberStart,
  }) : _context = Context.from([
         Clock.key.bind(Clock()),
         RandomSource.key.bind(RandomSource()),
         Logger.key.bind(Logger.noop()),
         Tracer.key.bind(Tracer.noop()),
         MetricRegistry.key.bind(MetricRegistry.noop()),
       ]).merge(context ?? Context.from(const [])) {
    final zone = Zone.current;
    _onFiberStart = onFiberStart == null
        ? null
        : zone.bindUnaryCallbackGuarded(onFiberStart);
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
  late final void Function(Fiber<Object?, Object?>)? _onFiberStart;
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
  ///
  /// [onFiberStart] and [onUnhandledError] configure the returned runtime as in
  /// [Runtime.new]. The internal service-building fiber is not passed to
  /// [onFiberStart]; subsequent user roots follow the usual notification rule.
  static Future<Exit<Runtime, E>> create<E>(
    Layer<E> services, {
    Context? context,
    CancellationSignal? signal,
    void Function(Cause<Object?> cause)? onUnhandledError,
    void Function(Fiber<Object?, Object?> fiber)? onFiberStart,
  }) async {
    final runtime = Runtime(
      context: context,
      onUnhandledError: onUnhandledError,
      onFiberStart: onFiberStart,
    );
    final building = runtime._start(
      buildLayer(services, runtime._scope),
      reportFailure: false,
      signal: signal,
      notifyStart: false,
    );
    final unsubscribe = signal?.onCancel((reason) {
      unawaited(runtime._beginClose(Exit.failure(Cause.cancelled(reason))));
    });
    final built = await fiberCompletion(building);
    unsubscribe?.call();
    Cause<E>? failure;
    switch (built) {
      case ExitSuccess(:final value):
        if (signal?.isCancelled ?? false) {
          failure = Cause.cancelled(signal!.reason);
        } else {
          runtime._context = runtime._context.merge(value);
          return Exit.success(runtime);
        }
      case ExitFailure(:final cause):
        failure = cause;
    }
    await runtime._beginClose(Exit.failure(failure));
    // Stopping startup can deliver the same defect through its fiber and the
    // application scope. Keep each occurrence once, including equal defects
    // produced independently by service cleanup.
    final closed = scopeCleanupFor(runtime._scope, {
      for (final event in fiberFailureEvents(building)) event.source,
    }).exit;
    return Exit.failure(switch (closed) {
      ExitSuccess() => failure,
      ExitFailure(:final cause) => Cause.combine([failure, cause]),
    });
  }

  /// Starts [task] and returns a handle to its independent execution.
  ///
  /// [signal] can cancel this execution without affecting other roots. The fiber
  /// completes after its child work and resource cleanup. Failures go to this
  /// runtime's handler, independently of [Fiber.onExit], [Fiber.poll], or waiting
  /// for the result. Use [run] to receive a result without a background report.
  ///
  /// To let a page stop waiting without cancelling native work, pass the page's
  /// signal to a separate [run] of [Fiber.join]. Acquire leases inside the owned
  /// operation; they remain held until the operation actually finishes.
  ///
  /// For an application-owned `SharedResource<HttpClient, HttpException>` named
  /// `clients`, this keeps the client lease through the entire HTTP response:
  ///
  /// ```dart
  /// // Imports dart:convert, dart:io, and package:any/any.dart.
  /// Future<Exit<String, HttpException>> downloadForPage(
  ///   Runtime app,
  ///   SharedResource<HttpClient, HttpException> clients,
  ///   Uri uri,
  ///   CancellationSignal pageSignal,
  /// ) {
  ///   final download = clients.use(
  ///     (client) => Task.fromFuture(() async {
  ///       final request = await client.getUrl(uri);
  ///       final response = await request.close();
  ///       final body = await response.transform(utf8.decoder).join();
  ///       if (response.statusCode >= 400) {
  ///         throw HttpException('HTTP ${response.statusCode}', uri: uri);
  ///       }
  ///       return body;
  ///     }, onError: (error, _) => HttpException('$error', uri: uri)),
  ///   );
  ///   final operation = app.start(download);
  ///   return app.run(operation.join(), signal: pageSignal);
  /// }
  /// ```
  ///
  /// The application must eventually [close] this runtime. Shutdown requests
  /// cancellation and waits for pending native work before releasing resources.
  /// A page cancellation does not abort the HTTP request in this example.
  ///
  /// A closing or closed runtime returns a completed fiber with a [RuntimeClosed]
  /// defect; the task does not start.
  Fiber<T, E> start<T, E>(Task<T, E> task, {CancellationSignal? signal}) =>
      _start(task, signal: signal, reportFailure: true);

  Fiber<T, E> _start<T, E>(
    Task<T, E> task, {
    CancellationSignal? signal,
    ExecutionMode? mode,
    required bool reportFailure,
    bool notifyStart = true,
  }) {
    if (_closing != null) {
      return completedFiber(
        Exit<T, E>.failure(
          Cause.defect(const RuntimeClosed(), StackTrace.current),
        ),
        report: _report,
        reportFailure: reportFailure,
      );
    }
    final fiber = startFiber(
      task,
      owner: _scope,
      report: _report,
      reportFailure: reportFailure,
      signal: signal,
      context: _context,
      mode: mode,
    );
    if (notifyStart && _onFiberStart != null && fiber.poll() is OptionNone) {
      _onFiberStart(fiber);
    }
    return fiber;
  }

  /// Runs [task] and returns its value or complete failure as an [Exit].
  ///
  /// The result includes child completion and resource cleanup. This entry sends
  /// the result to its caller without also reporting it as a background failure.
  ///
  /// [signal] cancels only this execution. An already cancelled signal prevents
  /// the task from starting. Cancellation waits for any in-flight,
  /// non-cancellable [Future] to settle before returning.
  ///
  /// Applying [Future.timeout] to the returned Future does not cancel this task.
  /// The eventual exit remains on that Future, even if the host abandons it. Use [start] and a separately cancellable [Fiber.join] when work must
  /// remain supervised after its caller stops waiting.
  Future<Exit<T, E>> run<T, E>(Task<T, E> task, {CancellationSignal? signal}) =>
      fiberCompletion(_start(task, signal: signal, reportFailure: false));

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

  /// Runs [task] synchronously and returns its complete [Exit].
  ///
  /// Completion includes owned child work and cleanup. Business failures,
  /// defects, and cancellation remain in the exit, as with [run]. An already
  /// cancelled [signal] prevents the body from starting.
  ///
  /// Throws [AsyncTaskException] if execution must wait, including for a native
  /// [Future], an explicit yield, unfinished child work, or asynchronous cleanup.
  /// Even an already completed Future requires asynchronous continuation. A
  /// callback that delivers its result during registration can finish synchronously.
  ///
  /// At an asynchronous boundary, cancellation is requested and this runtime
  /// continues supervising and reporting failures from the original execution.
  /// Reading the exception's completion does not change that responsibility. Its
  /// [AsyncTaskException.completion] exposes its eventual exit. Effects already
  /// performed are not repeated or rolled back; cancellation still respects
  /// masks and waits for non-cancellable work and cleanup.
  ///
  /// Automatic scheduling yields are disabled during this call. Explicit yields
  /// and stack-safety deferrals still require asynchronous execution. Timers
  /// cannot interrupt synchronous Dart code; use [run] for long-running work.
  Exit<T, E> runSync<T, E>(Task<T, E> task, {CancellationSignal? signal}) {
    final mode = ExecutionMode(automaticYield: false);
    final Fiber<T, E> fiber;
    try {
      fiber = _start(task, signal: signal, mode: mode, reportFailure: false);
    } finally {
      mode.automaticYield = true;
    }
    if (fiber.poll() case OptionSome(:final value)) return value;
    superviseFiber(fiber);
    fiber.requestCancel();
    throw AsyncTaskException<T, E>._(fiber);
  }

  /// Runs [task] synchronously and returns its value.
  ///
  /// Throws [TaskException] on task failure, retaining the complete [Cause].
  /// Throws [AsyncTaskException] when synchronous completion is impossible.
  /// Execution, cancellation, and cleanup follow [runSync].
  ///
  /// ```dart
  /// final runtime = Runtime();
  /// final total = runtime.runSyncOrThrow(
  ///   Task.sync(() => [3, 5, 8]).map((values) => values.reduce((a, b) => a + b)),
  /// );
  /// assert(total == 16);
  /// await runtime.close();
  /// ```
  T runSyncOrThrow<T, E>(Task<T, E> task, {CancellationSignal? signal}) =>
      switch (runSync(task, signal: signal)) {
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

/// A synchronous run that requires asynchronous work to finish.
///
/// [Runtime.runSync] and [Runtime.runSyncOrThrow] request cancellation before
/// throwing this exception. The original runtime retains ownership of the work
/// and its cleanup. Use [completion] to wait for the final result; starting the
/// task again would repeat effects that already ran.
final class AsyncTaskException<T, E> implements Exception {
  AsyncTaskException._(this._fiber);

  final Fiber<T, E> _fiber;

  /// The original execution's exit after owned work and cleanup finish.
  ///
  /// The runtime reports independent failures whether or not this property is
  /// read. A non-cancellable operation can delay completion.
  Future<Exit<T, E>> get completion => fiberCompletion(_fiber);

  @override
  String toString() =>
      'AsyncTaskException: The task could not finish synchronously.';
}

/// A task's complete failure represented as a Dart exception.
///
/// [Runtime.runOrThrow] and [Runtime.runSyncOrThrow] throw this exception when a
/// task fails. Inspect [cause] for business errors, defects with their original
/// stack traces, or cancellation. Printing the exception describes every reason
/// using [Cause.toString].
final class TaskException implements Exception {
  /// Creates an exception retaining the complete [cause].
  const TaskException(this.cause);

  /// The original business failures, defects, and cancellation reasons.
  final Cause<Object?> cause;

  @override
  String toString() =>
      'TaskException:\n  ${cause.toString().replaceAll('\n', '\n  ')}';
}

StackTrace _stackTrace(Cause<Object?> cause) => switch (cause) {
  CauseDefect(:final stackTrace) => stackTrace,
  CauseGroup(:final causes) => _stackTrace(causes.first),
  _ => StackTrace.current,
};
