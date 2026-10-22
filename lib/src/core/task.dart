import 'dart:async';

import 'context.dart';
import 'exit.dart';
import 'scope.dart';

/// A lazy computation with value type [T], error type [E], and dependencies [R].
///
/// Creating or composing a task does not execute it. Each [run] invokes the
/// computation again with its own [Scope] and waits for resource cleanup.
/// [TaskComposition.map] and [TaskComposition.flatMap] compose steps within
/// that same scope.
///
/// Expected errors are returned as [ExitFailure] values. Thrown objects become
/// [ExitDefect] values with their original stack traces, even if they match [E].
/// Returning an [ExitCancelled] stops the composition; it does not cancel
/// work that the computation has started separately.
///
/// ```dart
/// final parse = Task<int, String, ({String input})>((context) {
///   final value = int.tryParse(context.dependencies.input);
///   return value == null
///       ? const Exit.failure('Expected an integer.')
///       : Exit.success(value);
/// });
///
/// final exit = await parse.map((value) => value * 2).run((input: '21'));
/// final message = switch (exit) {
///   ExitSuccess(:final value) => 'Parsed $value.',
///   ExitFailure(:final error) => error,
///   ExitDefect(:final error) => 'Unexpected: $error',
///   ExitCancelled() => 'Cancelled.',
/// };
/// ```
final class Task<T, E, R> {
  /// Creates a task that invokes [computation] when run.
  ///
  /// The callback receives a [Context] containing the supplied dependencies and
  /// the run's scope. It may return an [Exit] directly or a future of one.
  /// Register resource cleanup with [Scope.addFinalizer]; [run] owns and closes
  /// the scope. Await any work that uses those resources before returning.
  const Task(FutureOr<Exit<T, E>> Function(Context<R> context) computation)
    : _computation = computation;

  final FutureOr<Exit<T, E>> Function(Context<R>) _computation;

  /// Executes this task with [dependencies] and awaits its resource cleanup.
  ///
  /// Every call creates a fresh scope and starts the computation in a microtask.
  /// Dependencies are passed by reference; concurrent runs can share mutable
  /// dependency objects.
  ///
  /// Finalizers receive the task's exit and finish before the returned future
  /// completes. If cleanup fails, the result is an [ExitDefect] containing a
  /// [ScopeCloseException], which retains the original exit and cleanup errors.
  Future<Exit<T, E>> run(R dependencies) async {
    final scope = Scope();
    final context = Context(scope: scope, dependencies: dependencies);
    final exit = await _evaluate(context);

    try {
      await scope.close(exit: exit);
    } catch (error, stackTrace) {
      return Exit.defect(error, stackTrace);
    }
    return exit;
  }

  // Schedule each step and use Future.then in compositions to avoid growing
  // the call stack for long chains.
  Future<Exit<T, E>> _evaluate(Context<R> context) =>
      Future<Exit<T, E>>.microtask(
        () => _computation(context),
      ).onError<Object>((error, stackTrace) => Exit.defect(error, stackTrace));
}

/// Composes tasks while preserving their dependency and expected error types.
///
/// Extension methods use the receiver's static types. This allows a task with
/// error type [Never] to be widened before composing a step that can fail.
extension TaskComposition<T, E, R> on Task<T, E, R> {
  /// A task that transforms this task's successful value with [transform].
  ///
  /// Other outcomes skip [transform] and retain their error, stack trace, or
  /// cancellation reason. A thrown object becomes an [ExitDefect].
  ///
  /// The transformed value is used as-is, including when it is a [Future]. Use
  /// [flatMap] to sequence another task.
  Task<U, E, R> map<U>(U Function(T value) transform) => Task(
    (context) => _evaluate(context).then(
      (exit) => switch (exit) {
        ExitSuccess(:final value) => Exit.success(transform(value)),
        ExitFailure(:final error) => Exit.failure(error),
        ExitDefect(:final error, :final stackTrace) => Exit.defect(
          error,
          stackTrace,
        ),
        ExitCancelled(:final reason) => Exit.cancelled(reason: reason),
      },
    ),
  );

  /// A task that runs [next] after this task succeeds.
  ///
  /// [next] receives the successful value. Its task uses the same context and
  /// scope, so resources remain available until the whole composition finishes.
  /// Both tasks use the same dependency type [R] and expected error type [E].
  ///
  /// Other outcomes skip [next] and retain their error, stack trace, or
  /// cancellation reason. A thrown object becomes an [ExitDefect].
  Task<U, E, R> flatMap<U>(Task<U, E, R> Function(T value) next) => Task(
    (context) => _evaluate(context).then(
      (exit) => switch (exit) {
        ExitSuccess(:final value) => next(value)._evaluate(context),
        ExitFailure(:final error) => Exit.failure(error),
        ExitDefect(:final error, :final stackTrace) => Exit.defect(
          error,
          stackTrace,
        ),
        ExitCancelled(:final reason) => Exit.cancelled(reason: reason),
      },
    ),
  );
}
