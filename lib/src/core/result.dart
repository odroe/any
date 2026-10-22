/// The outcome of an operation that succeeds with [T] or fails with [E].
///
/// A result is either a [Success] containing a value or a [Failure] containing
/// an expected error. The error can be any type; it need not be an [Exception].
/// Creating a result does not catch exceptions.
///
/// Use [Result.success] or [Result.failure] to create a result, then handle both
/// cases with an exhaustive `switch`:
///
/// ```dart
/// Result<int, String> parseInteger(String input) {
///   final value = int.tryParse(input);
///   return value == null
///       ? const Result.failure('Expected an integer.')
///       : Result.success(value);
/// }
///
/// final message = switch (parseInteger('42')) {
///   Success(:final value) => 'Parsed $value.',
///   Failure(:final error) => error,
/// };
/// ```
sealed class Result<T, E> {
  const Result._();

  /// Creates a successful result containing [value].
  const factory Result.success(T value) = Success<T, E>;

  /// Creates a failed result containing [error].
  const factory Result.failure(E error) = Failure<T, E>;
}

/// A successful [Result] containing a [value] of type [T].
final class Success<T, E> extends Result<T, E> {
  /// Creates a successful result containing [value].
  const Success(this.value) : super._();

  /// The value produced by the operation, which may be null if [T] allows it.
  final T value;
}

/// A failed [Result] containing an [error] of type [E].
final class Failure<T, E> extends Result<T, E> {
  /// Creates a failed result containing [error].
  const Failure(this.error) : super._();

  /// The expected error, which may be null if [E] allows it.
  final E error;
}
