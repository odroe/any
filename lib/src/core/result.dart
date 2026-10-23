/// A successful value of type [T] or a business error of type [E].
///
/// Use [ResultSuccess] and [ResultFailure] in an exhaustive switch to inspect
/// the result. Either branch can contain `null` when its type permits it.
///
/// A result is a value, not a computation. Its transformations run immediately
/// and propagate thrown objects rather than turning them into business errors.
sealed class Result<T, E> {
  const Result._();

  /// Creates a successful result containing [value].
  const factory Result.success(T value) = ResultSuccess<T, E>;

  /// Creates a failed result containing [error].
  const factory Result.failure(E error) = ResultFailure<T, E>;
}

/// A successful [Result] containing [value].
final class ResultSuccess<T, E> extends Result<T, E> {
  /// Creates a successful result containing [value].
  const ResultSuccess(this.value) : super._();

  /// The successful value.
  final T value;
}

/// A failed [Result] containing a business [error].
final class ResultFailure<T, E> extends Result<T, E> {
  /// Creates a failed result containing [error].
  const ResultFailure(this.error) : super._();

  /// The business error.
  final E error;
}

/// Transforms and combines results using their static value and error types.
extension ResultOperations<T, E> on Result<T, E> {
  /// Transforms a successful value, leaving a business error unchanged.
  ///
  /// Calls [transform] immediately for success and skips it for failure.
  /// Any object thrown by [transform] propagates to the caller.
  Result<U, E> map<U>(U Function(T value) transform) => switch (this) {
    ResultSuccess(:final value) => Result.success(transform(value)),
    ResultFailure(:final error) => Result.failure(error),
  };

  /// Transforms a business error, leaving a successful value unchanged.
  ///
  /// Calls [transform] immediately for failure and skips it for success.
  /// Any object thrown by [transform] propagates to the caller.
  Result<T, F> mapError<F>(F Function(E error) transform) => switch (this) {
    ResultSuccess(:final value) => Result.success(value),
    ResultFailure(:final error) => Result.failure(transform(error)),
  };

  /// Continues a successful result with [next], leaving a failure unchanged.
  ///
  /// Calls [next] immediately for success and returns its result. The next
  /// result uses the same error type [E]; use [mapError] to adapt other errors.
  /// Any object thrown by [next] propagates to the caller.
  Result<U, E> flatMap<U>(Result<U, E> Function(T value) next) =>
      switch (this) {
        ResultSuccess(:final value) => next(value),
        ResultFailure(:final error) => Result.failure(error),
      };
}
