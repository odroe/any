import 'dart:async';

import 'cancellation.dart';
import 'cause.dart';
import 'exit.dart';
import 'result.dart';

/// A lazy computation producing a value of type [T] or a business error [E].
///
/// Constructing and combining tasks does not run their callbacks. Each execution
/// starts with independent state. Unexpected exceptions become defects, and
/// cancellation remains separate from business errors, including when [E] is
/// [Never].
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
  /// A Future cannot be cancelled. If cancellation arrives after it starts,
  /// execution waits for it to settle before returning. No subsequent business
  /// callback runs. A Future error observed after cancellation is retained as a
  /// defect with its original stack trace alongside cancellation; [onError] is
  /// skipped. This also applies to a synchronous exception from [create] if it
  /// requests cancellation before throwing.
  static Task<T, E> fromFuture<T, E>(
    Future<T> Function() create, {
    required E Function(Object error, StackTrace stackTrace) onError,
  }) => Task._(_FromFuture(create, onError));
}

/// Transforms and combines tasks using their static value and error types.
extension TaskOperations<T, E> on Task<T, E> {
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
}) async {
  var current = task._node;
  final frames = <_Frame>[];
  var evaluating = true;
  Object? value;
  Cause<Object?>? failure;
  var remaining = 2048;

  while (true) {
    if (signal?.isCancelled ?? false) {
      return Exit.failure(_cancelled(failure, signal!.reason));
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
              failure = onError == null || (signal?.isCancelled ?? false)
                  ? Cause.defect(error, stackTrace)
                  : Cause.failure(onError(error, stackTrace));
              evaluating = false;
            }
          case _Suspend(:final create):
            current = create();
          case _FromFuture(:final create, :final onError):
            try {
              value = await create();
              failure = null;
            } catch (error, stackTrace) {
              failure = signal?.isCancelled ?? false
                  ? Cause.defect(error, stackTrace)
                  : Cause.failure(onError(error, stackTrace));
            }
            evaluating = false;
          case _Continue(:final source, :final frame):
            frames.add(frame);
            current = source;
        }
        continue;
      }

      if (frames.isEmpty) {
        return failure == null
            ? Exit.success(value as T)
            : Exit.failure(_typedCause<E>(failure));
      }

      final frame = frames.removeLast();
      if (failure case final cause?) {
        final resume = frame.failure;
        if (resume == null) continue;
        current = resume(cause, signal);
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

Cause<E> _typedCause<E>(Cause<Object?> cause) =>
    cause.mapError((error) => error as E);

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

final class _FromFuture extends _Node {
  const _FromFuture(this.create, this.onError);
  final Future<Object?> Function() create;
  final Object? Function(Object, StackTrace) onError;
}

final class _Continue extends _Node {
  const _Continue(this.source, this.frame);
  final _Node source;
  final _Frame frame;
}

final class _Frame {
  const _Frame({this.success, this.failure});
  final _Node Function(Object?)? success;
  final _Node Function(Cause<Object?>, CancellationSignal?)? failure;
}
