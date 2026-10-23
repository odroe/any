import 'dart:async';

import 'cancellation.dart';
import 'cause.dart';
import 'exit.dart';
import 'result.dart';
import 'scope.dart';

/// A lazy computation producing a value of type [T] or a business error [E].
///
/// Constructing and combining tasks does not run their callbacks. Each execution
/// starts with independent state. Unexpected exceptions become defects, and
/// cancellation remains separate from business errors, including when [E] is
/// [Never]. Cancellation is observed only while enabled; [uninterruptibleMask]
/// can defer it across a critical section.
final class Task<T, E> {
  const Task._(this._node);

  final _Node _node;

  /// Describes a computation that succeeds with [value].
  static Task<T, Never> succeed<T>(T value) => Task._(_Success(value));

  /// Describes a computation that fails with the business [error].
  static Task<Never, E> fail<E>(E error) =>
      Task._(_Failure(Cause.failure(error)));

  /// Describes a computation that fails with the complete [cause].
  static Task<Never, E> failCause<E>(Cause<E> cause) => Task._(_Failure(cause));

  /// Calls [body] on each execution, treating thrown objects as defects.
  static Task<T, Never> sync<T>(T Function() body) => Task._(_Sync(body));

  /// Calls [body] on each execution and maps its exceptions with [onError].
  ///
  /// If [onError] itself throws, its exception and stack trace become a defect.
  static Task<T, E> trySync<T, E>(
    T Function() body, {
    required E Function(Object error, StackTrace stackTrace) onError,
  }) => Task._(_Sync(body, onError));

  /// Calls [create] on each execution and continues with the returned task.
  ///
  /// Use this to defer a choice or describe recursion without building the
  /// entire computation up front. Exceptions from [create] become defects.
  static Task<T, E> defer<T, E>(Task<T, E> Function() create) =>
      Task._(_Suspend(() => create()._node));

  /// Defers cancellation while executing the task returned by [body].
  ///
  /// [body] runs lazily on each execution with cancellation disabled. Its
  /// `restore` function wraps a task with the cancellation state that was active
  /// before this mask. Nested masks therefore cannot enable cancellation that
  /// was already disabled. Leaving either region restores its enclosing state,
  /// including when a callback throws.
  ///
  /// Pending cancellation is observed when cancellation becomes enabled again.
  /// Defects already produced by the body are retained alongside cancellation.
  /// A task that never completes while masked can indefinitely delay
  /// cancellation; use `restore` around work that should remain cancellable.
  /// Cancellation observed before this mask starts prevents [body] from running.
  ///
  /// `restore` only captures a cancellation state, not an execution or signal.
  /// Tasks it returns are lazy and may be reused, including in another execution,
  /// where they use that execution's signal with the captured state.
  static Task<T, E> uninterruptibleMask<T, E>(
    Task<T, E> Function(RestoreCancellation restore) body,
  ) => Task._(
    _Mask(
      (interruptible) => body(
        <A, B>(Task<A, B> task) =>
            Task._(_Interruptibility(task._node, interruptible)),
      )._node,
    ),
  );

  /// Describes the already available business [result].
  static Task<T, E> fromResult<T, E>(Result<T, E> result) =>
      Task._(switch (result) {
        ResultSuccess(:final value) => _Success(value),
        ResultFailure(:final error) => _Failure(Cause.failure(error)),
      });

  /// Starts a new Future on each execution and maps exceptions with [onError].
  ///
  /// Both synchronous exceptions from [create] and errors completing its Future
  /// are mapped. An exception thrown by [onError] becomes a defect.
  ///
  /// A Future cannot be cancelled. If enabled cancellation arrives after it
  /// starts, execution waits for it to settle before returning. No subsequent
  /// business callback runs. A Future error observed after cancellation is
  /// retained as a defect with its original stack trace alongside cancellation;
  /// [onError] is skipped. This also applies to a synchronous exception from
  /// [create] if it requests cancellation before throwing. While cancellation is
  /// masked, [onError] and subsequent callbacks run normally.
  static Task<T, E> fromFuture<T, E>(
    Future<T> Function() create, {
    required E Function(Object error, StackTrace stackTrace) onError,
  }) => Task._(_FromFuture(create, onError));

  /// Registers a callback operation each time this task executes.
  ///
  /// [register] receives a completion callback and synchronously returns a
  /// cancellation function. The first completion wins; duplicate and late
  /// completions are ignored. A registration exception becomes a defect, even
  /// if completion was already called synchronously. If that completion failed,
  /// its cause is retained before the registration defect.
  ///
  /// Enabled cancellation waits for the returned function to stop using resources
  /// and finish cleanup. Cancellation during registration waits for [register] to
  /// return before calling that function. An operation that completed before
  /// cancellation does not receive a cancellation call. Cleanup exceptions keep
  /// their original object and stack trace alongside cancellation. Cleanup runs
  /// in the Zone where registration executed, including after asynchronous gaps.
  /// A masked operation waits for completion without calling its cancellation
  /// function.
  static Task<T, E> fromCallback<T, E>(
    FutureOr<void> Function() Function(void Function(Exit<T, E> exit) complete)
    register,
  ) => Task._(_FromCallback((complete) => register(complete)));

  /// Checks for owner cancellation when this task executes.
  ///
  /// Succeeds with no value if cancellation has not been requested or is masked.
  /// An enabled request terminates execution before the next business callback.
  static Task<void, Never> checkCancelled() => const Task._(_Success(null));

  /// Yields to the event loop before continuing with the next task.
  ///
  /// Timers and other ready work can advance. Enabled cancellation received
  /// while yielding terminates execution before the next business callback.
  static Task<void, Never> yieldNow() => const Task._(_Yield());

  /// Registers cleanup in the current scope when this task executes.
  ///
  /// Finalizers receive the scope's body exit and run in reverse registration
  /// order with cancellation masked. Every finalizer is attempted. Cleanup
  /// failures are appended to the body's cause before the scope returns.
  /// Cleanup has its own temporary scope, so it can register further cleanup
  /// and use scoped resources before the enclosing scope continues closing.
  static Task<void, Never> addFinalizer(
    Task<void, Never> Function(Exit<Object?, Object?> exit) finalizer,
  ) => Task._(_AddFinalizer(finalizer));

  /// Acquires a resource and registers its release in the current scope.
  ///
  /// Acquisition and registration are masked from cancellation. A pending
  /// request is handled only after a successful resource has its release
  /// registered. An acquisition that never finishes therefore cannot be
  /// cancelled promptly. A failed acquisition does not call [release].
  ///
  /// [release] runs with cancellation masked and observes the scope's exit.
  /// Use [using] for a local lifetime, or complete all uses within this scope.
  /// Returning a resource from Runtime.run does not extend its lifetime.
  static Task<T, E> acquireRelease<T, E>(
    Task<T, E> acquire, {
    required Task<void, Never> Function(T resource, Exit<Object?, Object?> exit)
    release,
  }) => uninterruptibleMask(
    (_) => acquire.flatMap(
      (resource) =>
          addFinalizer((exit) => release(resource, exit))
              .withError<E>()
              .map((_) => resource),
    ),
  );

  /// Acquires, uses, and releases a resource within one local scope.
  ///
  /// Returns [use]'s result after [release] finishes, including on failure or
  /// cancellation. Release observes the use exit; release failures follow body
  /// failures in the final cause. Thrown callback exceptions become defects.
  static Task<T, E> using<R, T, E>(
    Task<R, E> acquire, {
    required Task<T, E> Function(R resource) use,
    required Task<void, Never> Function(R resource, Exit<T, E> exit) release,
  }) => acquireRelease(
    acquire,
    release: (resource, exit) => release(resource, _typedExit<T, E>(exit)),
  ).flatMap(use).scoped();
}

/// Wraps a task with the cancellation state captured by an enclosing mask.
///
/// The wrapper is lazy. Repeated executions use their own cancellation signals
/// and the same captured state; no execution or signal is retained.
typedef RestoreCancellation = Task<T, E> Function<T, E>(Task<T, E> task);

/// Transforms and combines tasks using their static value and error types.
extension TaskOperations<T, E> on Task<T, E> {
  /// Runs this task in a child scope, waiting for its cleanup before continuing.
  ///
  /// Every execution creates a fresh scope. Child scopes close before parent
  /// resources, and cleanup failures follow the body's cause without wrapping
  /// or repeating that cause.
  Task<T, E> scoped() => Task._(_Scoped(_node));

  /// Runs [cleanup] after this task, including failure and cancellation.
  ///
  /// Cleanup is registered before starting this task and runs with cancellation
  /// masked. Its failures are retained alongside the body's failure.
  Task<T, E> ensuring(Task<void, Never> cleanup) => onExit((_) => cleanup);

  /// Runs [cleanup] with this task's exit before continuing to the caller.
  ///
  /// The callback is registered in a local scope before starting this task.
  /// Both the callback and returned cleanup task run in the registration Zone
  /// with cancellation masked. Their exceptions become cleanup defects.
  Task<T, E> onExit(Task<void, Never> Function(Exit<T, E> exit) cleanup) =>
      Task.addFinalizer((exit) => cleanup(_typedExit<T, E>(exit)))
          .withError<E>()
          .flatMap((_) => this)
          .scoped();

  /// Transforms the successful value when this task runs.
  ///
  /// Failures skip [transform]. Exceptions from [transform] become defects.
  Task<U, E> map<U>(U Function(T value) transform) => Task._(
    _Continue(
      _node,
      _Frame(success: (value) => _Success(transform(value as T))),
    ),
  );

  /// Continues with [next] after success, using the same business error type.
  ///
  /// Both tasks run in the same execution. A failure skips [next]; use
  /// [mapError] to adapt a different business error type before composing.
  Task<U, E> flatMap<U>(Task<U, E> Function(T value) next) => Task._(
    _Continue(_node, _Frame(success: (value) => next(value as T)._node)),
  );

  /// Runs [action] after success and retains the original successful value.
  ///
  /// A failure from [action] becomes the result of the combined task.
  Task<T, E> tap(Task<void, E> Function(T value) action) =>
      flatMap((value) => action(value).map((_) => value));

  /// Transforms every business failure, retaining other reasons and their order.
  ///
  /// Exceptions from [transform] become defects. External cancellation stops
  /// transformations that have not started, including later group members.
  Task<T, F> mapError<F>(F Function(E error) transform) => Task._(
    _Continue(
      _node,
      _Frame(
        failure: (cause, signal) => _Failure(
          _mapErrors(cause, (error) => transform(error as E), signal),
        ),
      ),
    ),
  );

  /// Recovers a single business failure using the same business error type.
  ///
  /// Defects, cancellation, and groups bypass [recover]. Use [recoverCause] to
  /// explicitly handle a complete cause. External cancellation stops recovery.
  Task<T, E> recover(Task<T, E> Function(E error) recover) => Task._(
    _Continue(
      _node,
      _Frame(
        failure: (cause, _) => switch (cause) {
          CauseFailure(:final error) => recover(error as E)._node,
          _ => _Failure(cause),
        },
      ),
    ),
  );

  /// Recovers a complete failure with [recover].
  ///
  /// [recover] receives all business failures, defects, and explicit
  /// cancellation reasons. It cannot override cancellation requested by the
  /// running task's owner. Exceptions from [recover] become defects.
  Task<T, F> recoverCause<F>(Task<T, F> Function(Cause<E> cause) recover) =>
      Task._(
        _Continue(
          _node,
          _Frame(failure: (cause, _) => recover(_typedCause<E>(cause))._node),
        ),
      );

  /// Transforms the complete failure cause when this task fails.
  ///
  /// External cancellation skips [transform]. Exceptions from [transform]
  /// become defects.
  Task<T, F> mapCause<F>(Cause<F> Function(Cause<E> cause) transform) => Task._(
    _Continue(
      _node,
      _Frame(failure: (cause, _) => _Failure(transform(_typedCause<E>(cause)))),
    ),
  );

  /// Captures this computation's complete [Exit] as a successful value.
  ///
  /// Explicit failure and cancellation causes can be inspected this way.
  /// Cancellation requested by the owner still terminates the enclosing task.
  Task<Exit<T, E>, Never> exit() => Task._(
    _Continue(
      _node,
      _Frame(
        success: (value) => _Success(Exit<T, E>.success(value as T)),
        failure: (cause, _) =>
            _Success(Exit<T, E>.failure(_typedCause<E>(cause))),
      ),
    ),
  );

  /// Converts every business failure into a defect at this conversion boundary.
  ///
  /// Non-null errors are retained unchanged. For a null business error, the
  /// original [CauseFailure] is retained as the defect's non-null error object.
  /// Existing defects, their stack traces, and cancellation remain unchanged.
  Task<T, Never> orDie() => Task._(
    _Continue(
      _node,
      _Frame(
        failure: (cause, _) => _Failure(_defects(cause, StackTrace.current)),
      ),
    ),
  );
}

/// Establishes a business error type for a task that has no business failures.
extension InfallibleTaskOperations<T> on Task<T, Never> {
  /// Gives this task error type [F] so it can begin a flow using that type.
  ///
  /// This does not run the task or change its behavior.
  Task<T, F> withError<F>() => this;
}

// Runtime's execution boundary. This declaration is not exported by any.dart.
Future<Exit<T, E>> evaluateTask<T, E>(
  Task<T, E> task, {
  CancellationSignal? signal,
  List<Scope> closingScopes = const [],
}) async {
  final ownedScope = createScope();
  final (exit, cancelled) = await _evaluate(
    task,
    signal: signal,
    scope: ownedScope,
    closingScopes: closingScopes,
  );
  var result = exit;
  var cancellationObserved = cancelled;
  if (!cancellationObserved && (signal?.isCancelled ?? false)) {
    result = _appendCleanup(result, Cause.cancelled(signal!.reason));
    cancellationObserved = true;
  }
  result = _appendCleanup(result, await closeScope(ownedScope, result));
  if (!cancellationObserved && (signal?.isCancelled ?? false)) {
    result = _appendCleanup(result, Cause.cancelled(signal!.reason));
  }
  return result;
}

// Scope's public operations create instructions without exposing runtime state.
Task<Scope, Never> openScopeTask() => const Task._(_OpenScope());

Task<void, Never> closeScopeTask(Scope scope, Exit<Object?, Object?> exit) =>
    Task._(_CloseScope(scope, exit));

Future<(Exit<T, E>, bool)> _evaluate<T, E>(
  Task<T, E> task, {
  required Scope scope,
  required List<Scope> closingScopes,
  CancellationSignal? signal,
}) async {
  var current = task._node;
  final frames = <_Frame>[];
  var evaluating = true;
  var interruptible = true;
  var currentScope = scope;
  Object? value;
  Cause<Object?>? failure;
  var remaining = 2048;

  while (true) {
    final effectiveSignal = interruptible ? signal : null;
    if (effectiveSignal?.isCancelled ?? false) {
      return (
        Exit<T, E>.failure(_cancelled(failure, effectiveSignal!.reason)),
        true,
      );
    }
    if (--remaining == 0) {
      remaining = 2048;
      await Future<void>.delayed(Duration.zero);
      continue;
    }

    try {
      if (evaluating) {
        switch (current) {
          case _Success(value: final result):
            value = result;
            failure = null;
            evaluating = false;
          case _Failure(:final cause):
            failure = cause;
            evaluating = false;
          case _Sync(:final body, :final onError):
            try {
              current = _Success(body());
            } catch (error, stackTrace) {
              failure =
                  onError == null || (effectiveSignal?.isCancelled ?? false)
                  ? Cause.defect(error, stackTrace)
                  : Cause.failure(onError(error, stackTrace));
              evaluating = false;
            }
          case _Suspend(:final create):
            current = create();
          case _Mask(:final create):
            final previous = interruptible;
            frames.add(_Frame(interruptible: previous));
            interruptible = false;
            current = create(previous);
          case _Interruptibility(:final source, interruptible: final restored):
            frames.add(_Frame(interruptible: interruptible));
            interruptible = restored;
            current = source;
          case _FromFuture(:final create, :final onError):
            try {
              value = await create();
              failure = null;
            } catch (error, stackTrace) {
              failure = effectiveSignal?.isCancelled ?? false
                  ? Cause.defect(error, stackTrace)
                  : Cause.failure(onError(error, stackTrace));
            }
            evaluating = false;
          case _FromCallback(:final register):
            final pending = _runCallback(register, effectiveSignal);
            final exit = pending is Future<Exit<Object?, Object?>>
                ? await pending
                : pending;
            switch (exit) {
              case ExitSuccess(value: final result):
                value = result;
                failure = null;
              case ExitFailure(:final cause):
                failure = cause;
            }
            evaluating = false;
          case _Yield():
            await Future<void>.delayed(Duration.zero);
            value = null;
            failure = null;
            evaluating = false;
          case _Continue(:final source, :final frame):
            frames.add(frame);
            current = source;
          case _OpenScope():
            value = createScope(parent: currentScope);
            failure = null;
            evaluating = false;
          case _AddFinalizer(:final finalizer):
            final zone = Zone.current;
            final pending = addScopeFinalizer(
              currentScope,
              (exit) => zone.run(() => finalizer(exit)),
              runFinalizer: (task, scope) => zone.run(
                () => evaluateTask(
                  task,
                  closingScopes: [...closingScopes, scope],
                ),
              ),
            );
            failure = pending is Future<Cause<Never>?>
                ? await pending
                : pending;
            value = null;
            evaluating = false;
          case _Scoped(:final source):
            final parent = currentScope;
            final child = createScope(parent: parent);
            currentScope = child;
            frames.add(
              _Frame(
                success: (value) {
                  currentScope = parent;
                  return _FinishScope(child, Exit.success(value));
                },
                failure: (cause, _) {
                  currentScope = parent;
                  return _FinishScope(child, Exit.failure(cause));
                },
              ),
            );
            current = source;
          case _FinishScope(:final scope, :final exit):
            final completed = _appendCleanup(
              exit,
              await closeScope(scope, exit),
            );
            switch (completed) {
              case ExitSuccess(value: final result):
                value = result;
                failure = null;
              case ExitFailure(:final cause):
                failure = cause;
            }
            evaluating = false;
          case _CloseScope(:final scope, :final exit):
            if (scopeCloseWouldDeadlock(scope, currentScope) ||
                closingScopes.any(
                  (owner) => scopeCloseWouldDeadlock(scope, owner),
                )) {
              throw StateError('Closing this scope would wait on the caller.');
            }
            failure = await closeScope(scope, exit);
            value = null;
            evaluating = false;
        }
        continue;
      }

      if (frames.isEmpty) {
        final Exit<T, E> exit = failure == null
            ? Exit.success(value as T)
            : Exit.failure(_typedCause<E>(failure));
        return (exit, false);
      }

      final frame = frames.removeLast();
      if (frame.interruptible case final restored?) {
        interruptible = restored;
        continue;
      }
      if (failure case final cause?) {
        final resume = frame.failure;
        if (resume == null) continue;
        current = resume(cause, effectiveSignal);
      } else {
        final resume = frame.success;
        if (resume == null) continue;
        current = resume(value);
      }
      // A continuation has already produced this failure. Record it before the
      // next cancellation check so defects cannot disappear between frames.
      if (current case _Failure(:final cause)) {
        failure = cause;
        evaluating = false;
      } else {
        failure = null;
        evaluating = true;
      }
    } catch (error, stackTrace) {
      failure = Cause.defect(error, stackTrace);
      evaluating = false;
    }
  }
}

FutureOr<Exit<Object?, Object?>> _runCallback(
  _RegisterCallback register,
  CancellationSignal? signal,
) {
  final zone = Zone.current;
  final completion = Completer<Exit<Object?, Object?>>();
  Exit<Object?, Object?>? completed;
  Exit<Object?, Object?>? settled;
  _CancelCallback? cancel;
  var registered = false;
  var cancelled = false;
  var cleaning = false;
  void Function()? unsubscribe;

  void settle(Exit<Object?, Object?> exit) {
    unsubscribe?.call();
    settled = exit;
    completion.complete(exit);
  }

  Future<void> cleanup() async {
    if (cleaning) return;
    cleaning = true;
    try {
      final pending = cancel!();
      if (pending is Future<void>) await pending;
      // The execution loop reports cancellation after cleanup has finished.
      settle(const Exit.success(null));
    } catch (error, stackTrace) {
      settle(Exit.failure(Cause.defect(error, stackTrace)));
    }
  }

  unsubscribe = signal?.onCancel((_) {
    if (completed != null || cancelled || completion.isCompleted) return;
    cancelled = true;
    if (registered) unawaited(zone.run(cleanup));
  });

  if (cancelled) {
    settle(const Exit.success(null));
    return settled ?? completion.future;
  }

  try {
    cancel = register((exit) {
      if (completed != null || cancelled || completion.isCompleted) return;
      completed = exit;
      if (registered) settle(exit);
    });
  } catch (error, stackTrace) {
    final defect = Cause<Object?>.defect(error, stackTrace);
    settle(
      Exit.failure(switch (completed) {
        ExitFailure(:final cause) => Cause.combine([cause, defect]),
        _ => defect,
      }),
    );
    return settled ?? completion.future;
  }

  registered = true;
  if (cancelled) {
    unawaited(zone.run(cleanup));
  } else if (completed case final exit?) {
    settle(exit);
  }
  return settled ?? completion.future;
}

typedef _CancelCallback = FutureOr<void> Function();
typedef _RegisterCallback = _CancelCallback Function(
  void Function(Exit<Object?, Object?>) complete,
);

Cause<E> _typedCause<E>(Cause<Object?> cause) =>
    cause.mapError((error) => error as E);

Exit<T, E> _typedExit<T, E>(Exit<Object?, Object?> exit) => switch (exit) {
  ExitSuccess(:final value) => Exit.success(value as T),
  ExitFailure(:final cause) => Exit.failure(_typedCause<E>(cause)),
};

Exit<T, E> _appendCleanup<T, E>(Exit<T, E> exit, Cause<Never>? cleanup) {
  if (cleanup == null) return exit;
  return switch (exit) {
    ExitSuccess() => Exit.failure(cleanup),
    ExitFailure(:final cause) => Exit.failure(Cause.combine([cause, cleanup])),
  };
}

Cause<Object?> _mapErrors(
  Cause<Object?> cause,
  Object? Function(Object?) transform,
  CancellationSignal? signal,
) {
  if (cause case CauseGroup(:final causes)) {
    final mapped = <Cause<Object?>>[];
    for (final reason in causes) {
      if (signal?.isCancelled ?? false) return cause;
      mapped.add(reason.mapError(transform));
    }
    return Cause.combine(mapped);
  }
  return cause.mapError(transform);
}

Cause<Never> _defects(Cause<Object?> cause, StackTrace stackTrace) =>
    switch (cause) {
      CauseFailure(:final error) => Cause.defect(error ?? cause, stackTrace),
      CauseDefect() => cause,
      CauseCancelled() => cause,
      CauseGroup(:final causes) => Cause.combine(
        causes.map((reason) => _defects(reason, stackTrace)),
      ),
    };

Cause<Never> _cancelled(Cause<Object?>? failure, Object? reason) {
  final retained = <Cause<Never>>[];
  final causes = switch (failure) {
    null => const <Cause<Object?>>[],
    CauseGroup(:final causes) => causes,
    _ => [failure],
  };
  for (final cause in causes) {
    switch (cause) {
      case CauseDefect():
        retained.add(cause);
      case CauseCancelled():
        retained.add(cause);
      default:
        break;
    }
  }
  retained.add(Cause.cancelled(reason));
  return Cause.combine(retained);
}

sealed class _Node {
  const _Node();
}

final class _Success extends _Node {
  const _Success(this.value);
  final Object? value;
}

final class _Failure extends _Node {
  const _Failure(this.cause);
  final Cause<Object?> cause;
}

final class _Sync extends _Node {
  const _Sync(this.body, [this.onError]);
  final Object? Function() body;
  final Object? Function(Object, StackTrace)? onError;
}

final class _Suspend extends _Node {
  const _Suspend(this.create);
  final _Node Function() create;
}

final class _Mask extends _Node {
  const _Mask(this.create);
  final _Node Function(bool interruptible) create;
}

final class _Interruptibility extends _Node {
  const _Interruptibility(this.source, this.interruptible);
  final _Node source;
  final bool interruptible;
}

final class _FromFuture extends _Node {
  const _FromFuture(this.create, this.onError);
  final Future<Object?> Function() create;
  final Object? Function(Object, StackTrace) onError;
}

final class _FromCallback extends _Node {
  const _FromCallback(this.register);
  final _RegisterCallback register;
}

final class _Yield extends _Node {
  const _Yield();
}

final class _Continue extends _Node {
  const _Continue(this.source, this.frame);
  final _Node source;
  final _Frame frame;
}

final class _OpenScope extends _Node {
  const _OpenScope();
}

final class _AddFinalizer extends _Node {
  const _AddFinalizer(this.finalizer);
  final Task<void, Never> Function(Exit<Object?, Object?>) finalizer;
}

final class _Scoped extends _Node {
  const _Scoped(this.source);
  final _Node source;
}

final class _FinishScope extends _Node {
  const _FinishScope(this.scope, this.exit);
  final Scope scope;
  final Exit<Object?, Object?> exit;
}

final class _CloseScope extends _Node {
  const _CloseScope(this.scope, this.exit);
  final Scope scope;
  final Exit<Object?, Object?> exit;
}

final class _Frame {
  const _Frame({this.success, this.failure, this.interruptible});
  final _Node Function(Object?)? success;
  final _Node Function(Cause<Object?>, CancellationSignal?)? failure;
  final bool? interruptible;
}
