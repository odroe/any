/// A successful value of type [T] or a business error of type [E].
///
/// Use a result to return an expected failure without throwing an exception.
/// Either branch can contain `null` when its type permits it.
///
/// Transformations run immediately. Callback exceptions propagate to the caller
/// and are never converted into [ResultFailure].
///
/// ```dart
/// Result<int, String> parsePort(String text) {
///   final port = int.tryParse(text);
///   return port != null && port > 0 && port <= 65535
///       ? Result.success(port)
///       : Result.failure('Expected a port between 1 and 65535.');
/// }
///
/// final message = switch (parsePort('8080')) {
///   ResultSuccess(:final value) => 'Listening on $value',
///   ResultFailure(:final error) => error,
/// };
/// assert(message == 'Listening on 8080');
/// ```
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

/// Transformations and sequential composition for results.
extension ResultOperations<T, E> on Result<T, E> {
  /// Transforms a successful value, leaving a business error unchanged.
  Result<U, E> map<U>(U Function(T value) transform) => switch (this) {
    ResultSuccess(:final value) => Result.success(transform(value)),
    ResultFailure(:final error) => Result.failure(error),
  };

  /// Transforms a business error, leaving a successful value unchanged.
  Result<T, F> mapError<F>(F Function(E error) transform) => switch (this) {
    ResultSuccess(:final value) => Result.success(value),
    ResultFailure(:final error) => Result.failure(transform(error)),
  };

  /// Continues a successful result with [next], leaving a failure unchanged.
  ///
  /// Use [mapError] first when the two results have different error types.
  Result<U, E> flatMap<U>(Result<U, E> Function(T value) next) =>
      switch (this) {
        ResultSuccess(:final value) => next(value),
        ResultFailure(:final error) => Result.failure(error),
      };
}
