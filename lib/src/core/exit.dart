import 'result.dart';

/// The terminal outcome of a task with value type [T] and expected error type [E].
///
/// An exit is an [ExitSuccess], [ExitFailure], [ExitDefect], or [ExitCancelled].
/// Only [ExitFailure] contains an expected error of type [E]. Unexpected errors
/// retain their original stack traces, and cancellation is a separate outcome.
///
/// An exit describes an outcome. Creating one does not execute a task, catch
/// exceptions, or request cancellation.
///
/// Use an exhaustive `switch` to handle all four cases:
///
/// ```dart
/// const Exit<int, String> exit = Exit.success(42);
///
/// final message = switch (exit) {
///   ExitSuccess(:final value) => 'Completed: $value',
///   ExitFailure(:final error) => 'Failed: $error',
///   ExitDefect(:final error) => 'Unexpected: $error',
///   ExitCancelled(:final reason) => 'Cancelled: $reason',
/// };
/// ```
sealed class Exit<T, E> {
  const Exit._();

  /// Records successful completion with [value].
  const factory Exit.success(T value) = ExitSuccess<T, E>;

  /// Records an expected failure with [error].
  const factory Exit.failure(E error) = ExitFailure<T, E>;

  /// Records an unexpected [error] with its original [stackTrace].
  ///
  /// Pass the stack trace received by `catch (error, stackTrace)` to preserve
  /// the location of the failure.
  const factory Exit.defect(Object error, StackTrace stackTrace) =
      ExitDefect<T, E>;

  /// Records cancellation with an optional [reason].
  const factory Exit.cancelled({Object? reason}) = ExitCancelled<T, E>;

  /// Converts [result] to a successful or expected-failure exit.
  ///
  /// A [Success] becomes an [ExitSuccess]; a [Failure] becomes an [ExitFailure].
  /// The value or error is preserved without copying.
  factory Exit.fromResult(Result<T, E> result) => switch (result) {
    Success(:final value) => Exit.success(value),
    Failure(:final error) => Exit.failure(error),
  };
}

/// A task that completed successfully with a [value] of type [T].
final class ExitSuccess<T, E> extends Exit<T, E> {
  /// Records successful completion with [value].
  const ExitSuccess(this.value) : super._();

  /// The value produced by the task, which may be null if [T] allows it.
  final T value;
}

/// A task that ended with an expected [error] of type [E].
final class ExitFailure<T, E> extends Exit<T, E> {
  /// Records an expected failure with [error].
  const ExitFailure(this.error) : super._();

  /// The expected error, which may be null if [E] allows it.
  final E error;
}

/// A task that ended with an unexpected [error] and its original [stackTrace].
///
/// The error can be any non-null object, regardless of the expected error type
/// [E].
final class ExitDefect<T, E> extends Exit<T, E> {
  /// Records an unexpected [error] with its original [stackTrace].
  const ExitDefect(this.error, this.stackTrace) : super._();

  /// The original object thrown by the task.
  final Object error;

  /// The original stack trace associated with [error].
  final StackTrace stackTrace;
}

/// A task that ended in response to cancellation.
///
/// The optional [reason] is independent of the expected error type [E].
final class ExitCancelled<T, E> extends Exit<T, E> {
  /// Records cancellation with an optional [reason].
  const ExitCancelled({this.reason}) : super._();

  /// The reason for cancellation, or null when none was supplied.
  final Object? reason;
}
