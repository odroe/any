import 'dart:async';

import 'cancellation.dart';
import 'cause.dart';
import 'context.dart';
import 'exit.dart';
import 'scope.dart';

/// A lazy computation with value type [T], error type [E], and dependencies [R].
///
/// Creating or composing a task does not execute it. Each [run] invokes the
/// computation again with its own [Scope] and waits for resource cleanup.
/// [runIn] uses an existing [Context] and leaves cleanup to the scope's owner.
/// [TaskComposition.map] and [TaskComposition.flatMap] compose steps within
/// that same scope. Compositions are stack-safe, including long chains of
/// synchronously completing tasks.
///
/// Expected errors are represented by [CauseFailure]. Thrown objects become
/// [CauseDefect] values with their original stack traces, even if they match [E].
/// Returning an exit with a [CauseCancelled] stops the composition; it does not
/// cancel work that the computation has started separately.
///
/// Pass a [CancellationToken] to [run] to request cancellation from outside the
/// task. Running callbacks must cooperate through [Context.cancellation]; a run
/// still waits for them to finish before releasing resources.
///
/// ```dart
/// final parse = Task<int, String, ({String input})>((context) {
///   final value = int.tryParse(context.dependencies.input);
///   return value == null
///       ? const Exit.failure(Cause.failure('Expected an integer.'))
///       : Exit.success(value);
/// });
///
/// final exit = await parse.map((value) => value * 2).run((input: '21'));
/// final message = switch (exit) {
///   ExitSuccess(:final value) => 'Parsed $value.',
///   ExitFailure(cause: CauseFailure(:final error)) => error,
///   ExitFailure(cause: CauseDefect(:final error)) => 'Unexpected: $error',
///   ExitFailure(cause: CauseCancelled()) => 'Cancelled.',
///   ExitFailure(cause: CauseGroup(:final causes)) =>
///     '${causes.length} failure reasons.',
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
    : _computation = computation,
      _source = null,
      _transform = null,
      _next = null,
      _failure = null,
      _recover = null,
      _context = null;

  const Task._map(this._source, this._transform)
    : _computation = null,
      _next = null,
      _failure = null,
      _recover = null,
      _context = null;

  const Task._flatMap(this._source, this._next)
    : _computation = null,
      _transform = null,
      _failure = null,
      _recover = null,
      _context = null;

  const Task._mapError(this._source, this._transform, this._failure)
    : _computation = null,
      _next = null,
      _recover = null,
      _context = null;

  const Task._recover(this._source, this._recover)
    : _computation = null,
      _transform = null,
      _next = null,
      _failure = null,
      _context = null;

  const Task._adapt(this._source, this._transform, this._failure, this._context)
    : _computation = null,
      _next = null,
      _recover = null;

  // A leaf stores a computation; a composition stores a source and one callback.
  // These immutable nodes can be shared without copying the existing chain.
  final FutureOr<Exit<T, E>> Function(Context<R>)? _computation;
  final Task<Object?, Object?, Object?>? _source;
  final Exit<T, E> Function(Object?)? _transform;
  final Task<T, E, R> Function(Object?)? _next;
  final Exit<T, E> Function(Cause<Object?>)? _failure;
  final Task<T, E, R> Function(Object?)? _recover;
  final Context<Object?> Function(Context<Object?>)? _context;

  // Each leaf restores its own dependency type at the interpreter boundary.
  FutureOr<Exit<T, E>> _invoke(Context<Object?> context) =>
      _computation!(context as Context<R>);

  /// Executes this task with [dependencies] and awaits its resource cleanup.
  ///
  /// Every call creates a fresh scope and starts the computation in a microtask.
  /// Dependencies are passed by reference; concurrent runs can share mutable
  /// dependency objects.
  ///
  /// [cancellation] is checked before each step and after successful completion.
  /// A request skips unstarted steps and replaces success with an [ExitFailure]
  /// containing a [CauseCancelled]. Existing failure causes are preserved.
  /// A running callback is awaited even after a request; it can observe
  /// [Context.cancellation] to stop cooperatively.
  ///
  /// Synchronous steps can execute consecutively in one microtask. A queued
  /// cancellation request can be observed only after its callback runs.
  ///
  /// Finalizers receive the task's exit and finish before the returned future
  /// completes. Once cleanup begins, later requests do not change that exit or
  /// interrupt cleanup. Cleanup failures become [CauseDefect] values. If the
  /// computation also failed, its cause comes first, followed by cleanup causes
  /// in finalizer execution order. Multiple causes form a flat [CauseGroup].
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
    } on ScopeCloseException catch (error) {
      return Exit.failure(switch (exit) {
        ExitSuccess() => error.cause,
        ExitFailure(:final cause) => Cause.combine([cause, error.cause]),
      });
    } catch (error, stackTrace) {
      return Exit.failure(Cause.defect(error, stackTrace));
    }
    return exit;
  }

  /// Executes this task in [context], leaving cleanup to the scope's owner.
  ///
  /// Every call starts the computation in a microtask using the same context,
  /// including its dependencies, scope, and cancellation token. Scheduling and
  /// cancellation follow the same rules as [run]. Thrown objects become
  /// [ExitFailure] values containing a [CauseDefect] with the original stack trace.
  ///
  /// Registered finalizers are left for the scope's owner to run. The owner
  /// must await all runs using the scope before calling [Scope.close], regardless
  /// of their outcomes. Finalizers receive the exit supplied by that owner.
  ///
  /// If [Scope.close] has been called before the computation starts, returns an
  /// [ExitFailure] with a [CauseDefect] containing a [StateError], without invoking
  /// the computation, even if cancellation has also been requested.
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
  ///
  /// [TaskComposition.adapt] combines dependency projection and expected error
  /// translation when integrating a task into another workflow.
  Future<Exit<T, E>> runIn(Context<R> context) async {
    // Defer entry once. Only computations returning a Future suspend the loop.
    await Future<void>.value();
    Task<Object?, Object?, Object?> current = this;
    List<Task<Object?, Object?, Object?>>? pending;
    Context<Object?> activeContext = context;
    List<(int, Context<Object?>)>? contexts;
    var errorHandlers = 0;
    final cancellation = context.cancellation;

    try {
      while (true) {
        if (context.scope.isClosed) {
          throw StateError('Cannot run a task in a closed scope.');
        }
        if (cancellation.isCancelled) {
          return Exit.failure(Cause.cancelled(reason: cancellation.reason));
        }

        while (current._source != null) {
          (pending ??= []).add(current);
          if (current._failure != null || current._recover != null) {
            errorHandlers++;
          }
          final project = current._context;
          if (project != null) {
            (contexts ??= []).add((pending.length, activeContext));
            activeContext = project(activeContext);
            if (context.scope.isClosed) {
              throw StateError('Cannot run a task in a closed scope.');
            }
            if (cancellation.isCancelled) {
              return Exit.failure(Cause.cancelled(reason: cancellation.reason));
            }
          }
          current = current._source;
        }
        final result = current._invoke(activeContext);
        var exit = result is Future<Exit<Object?, Object?>>
            ? await result
            : result;

        while (true) {
          if (pending == null ||
              pending.isEmpty ||
              exit is ExitFailure<Object?, Object?> && errorHandlers == 0) {
            // A skipped success step can leave a different value type on a
            // failure. Preserve already compatible exits, including leaf exits.
            final typed = exit is Exit<T, E>
                ? exit
                : switch (exit) {
                    ExitSuccess(:final value) => Exit<T, E>.success(value as T),
                    ExitFailure(:final cause) => Exit<T, E>.failure(
                      cause as Cause<E>,
                    ),
                  };
            return _checkCancellation(typed, cancellation);
          }
          if (contexts != null &&
              contexts.isNotEmpty &&
              contexts.last.$1 == pending.length) {
            activeContext = contexts.removeLast().$2;
          }
          final step = pending.removeLast();
          if (step._failure != null || step._recover != null) errorHandlers--;
          if (exit is ExitFailure<Object?, Object?>) {
            final failure = step._failure;
            final recover = step._recover;
            if (failure != null) {
              exit = failure(exit.cause);
            } else if (recover != null && !cancellation.isCancelled) {
              if (exit.cause case CauseFailure(:final error)) {
                current = recover(error);
                break;
              }
            }
            continue;
          }
          if (cancellation.isCancelled) {
            return Exit.failure(Cause.cancelled(reason: cancellation.reason));
          }
          final value = (exit as ExitSuccess<Object?, Object?>).value;
          final transform = step._transform;
          if (transform != null) {
            exit = transform(value);
          } else if (step._next != null) {
            current = step._next(value);
            break;
          }
        }
      }
    } catch (error, stackTrace) {
      return Exit.failure(Cause.defect(error, stackTrace));
    }
  }
}

/// Composes task values, expected errors, and dependency views.
///
/// Extension methods use the receiver's static types. This allows a task with
/// error type [Never] to be widened before composing a step that can fail.
extension TaskComposition<T, E, R> on Task<T, E, R> {
  /// A task that transforms this task's successful value with [transform].
  ///
  /// Failure outcomes skip [transform] and retain the same [Cause], including
  /// groups. A thrown object becomes a [CauseDefect] in the resulting exit.
  ///
  /// The transformed value is used as-is, including when it is a [Future]. Use
  /// [flatMap] to sequence another task.
  Task<U, E, R> map<U>(U Function(T value) transform) =>
      Task._map(this, (value) => Exit.success(transform(value as T)));

  /// A task that runs [next] after this task succeeds.
  ///
  /// [next] receives the successful value. Its task uses the same context and
  /// scope, so resources remain available until the whole composition finishes.
  /// Both tasks use the same dependency type [R] and expected error type [E].
  ///
  /// Failure outcomes skip [next] and retain the same [Cause], including
  /// groups. A thrown object becomes a [CauseDefect] in the resulting exit.
  Task<U, E, R> flatMap<U>(Task<U, E, R> Function(T value) next) =>
      Task._flatMap(this, (value) => next(value as T));

  /// A task that translates expected errors with [transform].
  ///
  /// Applies [Cause.mapError] to failed outcomes, including groups. Defect
  /// objects, stack traces, cancellation reasons, and cause order are retained.
  /// Successful values are unchanged. A thrown object from [transform] becomes
  /// a defect. Resource cleanup happens later, when the shared scope closes.
  ///
  /// ```dart
  /// final task = Task<int, String, ()>(
  ///   (_) => const Exit.failure(Cause.failure('missing')),
  /// ).mapError((error) => (operation: 'load', error: error));
  /// ```
  Task<T, F, R> mapError<F>(F Function(E error) transform) => Task._mapError(
    this,
    (value) => Exit.success(value as T),
    (cause) => Exit.failure((cause as Cause<E>).mapError(transform)),
  );

  /// A task that runs [recover] for a single expected failure.
  ///
  /// Only a [CauseFailure] invokes [recover]. Success, defects, cancellation,
  /// and groups pass through unchanged. Groups are not partially recovered:
  /// doing so could hide other failures. The replacement task shares the same
  /// context and scope; resources stay alive until the entire workflow ends.
  ///
  /// A pending cancellation request skips recovery and preserves the original
  /// failure. A thrown object from [recover] becomes a defect. The error type
  /// remains [E] because groups and replacement tasks can still fail.
  Task<T, E, R> recover(Task<T, E, R> Function(E error) recover) =>
      Task._recover(this, (error) => recover(error as E));

  /// A task that projects outer dependencies and translates expected errors.
  ///
  /// [dependencies] selects this task's [R] from the caller's [R0], once per
  /// run. The projected context shares the caller's scope and cancellation
  /// token. Resource ownership stays with that scope; this does not create a
  /// new resource lifetime or cache dependencies.
  ///
  /// [error] translates every expected error through [Cause.mapError], including
  /// grouped causes. Defect objects, stack traces, cancellation reasons, and
  /// cause order are retained. Throws from either callback become defects.
  /// A pre-cancelled or closed context skips both callbacks and the source task.
  ///
  /// ```dart
  /// final read = Task<int, String, String>((context) {
  ///   final value = int.tryParse(context.dependencies);
  ///   return value == null
  ///       ? const Exit.failure(Cause.failure('Invalid port.'))
  ///       : Exit.success(value);
  /// });
  /// final configured = read.adapt<({String message}), ({String port})>(
  ///   dependencies: (config) => config.port,
  ///   error: (message) => (message: message),
  /// );
  /// final exit = await configured.run((port: '8080'));
  /// ```
  Task<T, F, R0> adapt<F, R0>({
    required R Function(R0 dependencies) dependencies,
    required F Function(E error) error,
  }) => Task._adapt(
    this,
    (value) => Exit.success(value as T),
    (cause) => Exit.failure((cause as Cause<E>).mapError(error)),
    (context) =>
        context.withDependencies<R>(dependencies(context.dependencies as R0)),
  );
}

Exit<T, E> _checkCancellation<T, E>(Exit<T, E> exit, CancellationToken token) =>
    exit is ExitSuccess<T, E> && token.isCancelled
    ? Exit.failure(Cause.cancelled(reason: token.reason))
    : exit;
