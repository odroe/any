import 'cause.dart';
import 'result.dart';

/// The terminal outcome of a task with value type [T] and expected error type [E].
///
/// An exit is either an [ExitSuccess] containing a value or an [ExitFailure]
/// containing a [Cause]. The cause distinguishes expected failures, unexpected
/// errors, cancellation, and combinations of these reasons.
///
/// An exit is data. Creating one does not execute a task, catch exceptions, or
/// request cancellation.
///
/// Use an exhaustive `switch` to inspect the outcome and its cause:
///
/// ```dart
/// const Exit<int, String> exit = Exit.failure(Cause.failure('Invalid input.'));
///
/// final message = switch (exit) {
///   ExitSuccess(:final value) => 'Completed: $value',
///   ExitFailure(cause: CauseFailure(:final error)) => 'Failed: $error',
///   ExitFailure(cause: CauseDefect(:final error)) => 'Unexpected: $error',
///   ExitFailure(cause: CauseCancelled(:final reason)) => 'Cancelled: $reason',
///   ExitFailure(cause: CauseGroup(:final causes)) =>
///     '${causes.length} failure reasons.',
/// };
/// ```
sealed class Exit<T, E> {
  const Exit._();

  /// Records successful completion with [value].
  const factory Exit.success(T value) = ExitSuccess<T, E>;

  /// Records unsuccessful completion with [cause].
  const factory Exit.failure(Cause<E> cause) = ExitFailure<T, E>;

  /// Converts [result] to a successful or expected-failure exit.
  ///
  /// A [Success] becomes an [ExitSuccess]. A [Failure] becomes an [ExitFailure]
  /// with a [CauseFailure], even when its error is an [Exception] or [Error].
  /// The original value or error is preserved without copying.
  factory Exit.fromResult(Result<T, E> result) => switch (result) {
    Success(:final value) => Exit.success(value),
    Failure(:final error) => Exit.failure(Cause.failure(error)),
  };
}

/// A task that completed successfully with a [value] of type [T].
final class ExitSuccess<T, E> extends Exit<T, E> {
  /// Records successful completion with [value].
  const ExitSuccess(this.value) : super._();

  /// The value produced by the task, which may be null if [T] allows it.
  final T value;
}

/// A task that ended with one or more reasons in [cause].
final class ExitFailure<T, E> extends Exit<T, E> {
  /// Records unsuccessful completion with [cause].
  const ExitFailure(this.cause) : super._();

  /// The complete failure cause, including any defects or cancellation.
  final Cause<E> cause;
}
