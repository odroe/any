import 'cause.dart';

/// The final result of an execution, including its complete failure cause.
///
/// [T] is the success value type and [E] is the business error type. An
/// [ExitFailure] can also contain defects or cancellation, even when [E] is
/// [Never]. Use pattern matching to distinguish success from failure.
sealed class Exit<T, E> {
  const Exit._();

  /// Creates a successful exit containing [value].
  const factory Exit.success(T value) = ExitSuccess<T, E>;

  /// Creates a failed exit containing the complete [cause].
  const factory Exit.failure(Cause<E> cause) = ExitFailure<T, E>;
}

/// A successful execution containing a value of type [T].
final class ExitSuccess<T, E> extends Exit<T, E> {
  /// Creates a successful exit containing [value].
  const ExitSuccess(this.value) : super._();

  /// The successful result, retained unchanged.
  final T value;
}

/// A failed execution containing all its failure reasons.
final class ExitFailure<T, E> extends Exit<T, E> {
  /// Creates a failed exit containing [cause].
  const ExitFailure(this.cause) : super._();

  /// The complete failure cause, retained unchanged.
  final Cause<E> cause;
}

/// Success and business error transformations for [Exit].
extension ExitMapping<T, E> on Exit<T, E> {
  /// Transforms a successful value, preserving a failed exit's cause.
  ///
  /// Errors thrown by [transform] propagate to the caller.
  Exit<U, E> map<U>(U Function(T value) transform) {
    final exit = this;
    return switch (exit) {
      ExitSuccess(:final value) => Exit<U, E>.success(transform(value)),
      ExitFailure(:final cause) => Exit<U, E>.failure(cause),
    };
  }

  /// Transforms business errors, preserving a successful exit's value.
  ///
  /// Delegates to [CauseMapping.mapError], preserving defects, cancellation,
  /// and the order of failure reasons. Errors thrown by [transform] propagate
  /// to the caller.
  Exit<T, F> mapError<F>(F Function(E error) transform) {
    final exit = this;
    return switch (exit) {
      ExitSuccess(:final value) => Exit<T, F>.success(value),
      ExitFailure(:final cause) => Exit<T, F>.failure(
        cause.mapError(transform),
      ),
    };
  }
}
