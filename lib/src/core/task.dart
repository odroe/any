import 'dart:async';

import 'cancellation.dart';
import 'context.dart';
import 'exit.dart';
import 'scope.dart';

/// A lazy computation with value type [T], error type [E], and dependencies [R].
///
/// Creating or composing a task does not execute it. Each [run] invokes the
/// computation again with its own [Scope] and waits for resource cleanup.
/// [runIn] uses an existing [Context] and leaves cleanup to the scope's owner.
/// [TaskComposition.map] and [TaskComposition.flatMap] compose steps within
/// that same scope.
///
/// Expected errors are returned as [ExitFailure] values. Thrown objects become
/// [ExitDefect] values with their original stack traces, even if they match [E].
/// Returning an [ExitCancelled] stops the composition; it does not cancel
/// work that the computation has started separately.
///
/// Pass a [CancellationToken] to [run] to request cancellation from outside the
/// task. Running callbacks must cooperate through [Context.cancellation]; a run
/// still waits for them to finish before releasing resources.
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
  /// The callback receives the [Context] created by [run] or supplied to [runIn].
  /// It may return an [Exit] directly or a future of one. Register resource
  /// cleanup with [Scope.addFinalizer]; the scope's owner closes it. Await any
  /// work that uses those resources before returning.
  const Task(FutureOr<Exit<T, E>> Function(Context<R> context) computation)
    : _computation = computation;

  final FutureOr<Exit<T, E>> Function(Context<R>) _computation;

  /// Executes this task with [dependencies] and awaits its resource cleanup.
  ///
  /// Every call creates a fresh scope and starts the computation in a microtask.
  /// Dependencies are passed by reference; concurrent runs can share mutable
  /// dependency objects.
  ///
  /// [cancellation] is checked before each step and after successful completion.
  /// A request skips unstarted steps and replaces success with [ExitCancelled].
  /// Expected failures, defects, and explicit cancellation exits are preserved.
  /// A running callback is awaited even after a request; it can observe
  /// [Context.cancellation] to stop cooperatively.
  ///
  /// Finalizers receive the task's exit and finish before the returned future
  /// completes. Once cleanup begins, later requests do not change that exit or
  /// interrupt cleanup. If cleanup fails, the result is an [ExitDefect] containing a
  /// [ScopeCloseException], which retains the original exit and cleanup errors.
  Future<Exit<T, E>> run(
    R dependencies, {
    CancellationToken? cancellation,
  }) async {
    final scope = Scope();
    final context = Context(
      scope: scope,
      dependencies: dependencies,
      cancellation: cancellation,
    );
    final exit = _checkCancellation(await runIn(context), context.cancellation);

    try {
      await scope.close(exit: exit);
    } catch (error, stackTrace) {
      return Exit.defect(error, stackTrace);
    }
    return exit;
  }

  /// Executes this task in [context], leaving cleanup to the scope's owner.
  ///
  /// Every call starts the computation in a microtask using the same context,
  /// including its dependencies, scope, and cancellation token. Cancellation
  /// follows the same rules as [run]. Thrown objects become [ExitDefect] values
  /// with their original stack traces.
  ///
  /// Registered finalizers are left for the scope's owner to run. The owner
  /// must await all runs using the scope before calling [Scope.close], regardless
  /// of their outcomes. Finalizers receive the exit supplied by that owner.
  ///
  /// If [Scope.close] has been called before the computation starts, returns an
  /// [ExitDefect] containing a [StateError] without invoking the computation,
  /// even if cancellation has also been requested.
  ///
  /// Use this to call another task from a computation while sharing its resource
  /// lifetime. [Context.withDependencies] can supply different dependencies:
  ///
  /// ```dart
  /// final readLabel = Task<String, Never, ({String label})>(
  ///   (context) => Exit.success(context.dependencies.label),
  /// );
  /// final workflow = Task<String, Never, ()>(
  ///   (context) => readLabel.runIn(
  ///     context.withDependencies((label: 'local',)),
  ///   ),
  /// );
  ///
  /// final exit = await workflow.run(());
  /// ```
  Future<Exit<T, E>> runIn(Context<R> context) {
    // Schedule each step and use Future.then in compositions to avoid growing
    // the call stack for long chains.
    return Future<Exit<T, E>>.microtask(() {
      if (context.scope.isClosed) {
        throw StateError('Cannot run a task in a closed scope.');
      }
      final cancellation = context.cancellation;
      if (cancellation.isCancelled) {
        return Exit.cancelled(reason: cancellation.reason);
      }
      return _computation(context);
    }).then(
      (exit) => _checkCancellation(exit, context.cancellation),
      onError: (Object error, StackTrace stackTrace) =>
          Exit<T, E>.defect(error, stackTrace),
    );
  }
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
    (context) => runIn(context).then(
      (exit) => switch (_checkCancellation(exit, context.cancellation)) {
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
    (context) => runIn(context).then(
      (exit) => switch (_checkCancellation(exit, context.cancellation)) {
        ExitSuccess(:final value) => next(value).runIn(context),
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

Exit<T, E> _checkCancellation<T, E>(Exit<T, E> exit, CancellationToken token) =>
    exit is ExitSuccess<T, E> && token.isCancelled
    ? Exit.cancelled(reason: token.reason)
    : exit;
