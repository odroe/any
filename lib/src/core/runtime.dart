import 'dart:async';

import 'cancellation.dart';
import 'cause.dart';
import 'exit.dart';
import 'task.dart';

/// Owns root task executions and waits for them when closing.
///
/// Each [run] executes its task independently. Keep the runtime open while its
/// tasks are in use, and inspect the result of [close] at the host boundary.
final class Runtime {
  /// Creates an open runtime.
  Runtime();

  static final _current = Object();
  final _runs = <_RootRun>{};
  Future<Exit<void, Never>>? _closing;

  /// Runs [task] and returns its complete exit.
  ///
  /// [signal] requests cancellation of this execution only. An already
  /// cancelled signal prevents business work from starting. Cancellation waits
  /// for any in-flight, non-cancellable Future before completing the exit.
  ///
  /// After closing starts, returns a [RuntimeClosed] defect without starting
  /// [task]. Task failures are returned as values, including thrown defects.
  Future<Exit<T, E>> run<T, E>(Task<T, E> task, {CancellationSignal? signal}) {
    if (_closing != null) {
      return Future.value(
        Exit.failure(Cause.defect(const RuntimeClosed(), StackTrace.current)),
      );
    }

    final root = _RootRun();
    final completion = Completer<Exit<T, E>>();
    _runs.add(root);
    final unsubscribe = signal?.onCancel(
      (reason) => root.controller.requestCancel(reason: reason),
    );
    final ancestors = Zone.current[_current] as List<_RootRun>?;

    unawaited(
      runZoned(
        () async {
          try {
            completion.complete(
              await evaluateTask(task, signal: root.controller.signal),
            );
          } catch (error, stackTrace) {
            completion.complete(Exit.failure(Cause.defect(error, stackTrace)));
          } finally {
            unsubscribe?.call();
            _runs.remove(root);
            root.done.complete();
          }
        },
        zoneValues: {
          _current: <_RootRun>[...?ancestors, root],
        },
      ),
    );
    return completion.future;
  }

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
  /// execution's [Exit]. Non-cancellable Futures must settle before closing can
  /// finish.
  ///
  /// A task owned by this runtime cannot wait for the runtime to close itself.
  /// Such a call returns a [StateError] defect instead of starting a deadlock.
  Future<Exit<void, Never>> close() {
    final ancestors = Zone.current[_current] as List<_RootRun>?;
    if (ancestors?.any(_runs.contains) ?? false) {
      return Future.value(
        Exit.failure(
          Cause.defect(
            StateError('A task cannot close its own runtime.'),
            StackTrace.current,
          ),
        ),
      );
    }
    final closing = _closing;
    if (closing != null) return closing;
    final completion = Completer<Exit<void, Never>>();
    _closing = completion.future;
    unawaited(_close(completion));
    return completion.future;
  }

  Future<void> _close(Completer<Exit<void, Never>> completion) async {
    final runs = _runs.toList();
    for (final root in runs) {
      root.controller.requestCancel();
    }
    await Future.wait(runs.map((root) => root.done.future));
    completion.complete(const Exit.success(null));
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

final class _RootRun {
  final controller = CancellationController();
  final done = Completer<void>();
}
