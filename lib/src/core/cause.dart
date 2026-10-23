/// Describes the reasons an execution failed.
///
/// A cause contains a business failure, an unexpected defect, a cancellation,
/// or an ordered group of these reasons. [E] is the business error type; defects
/// and cancellations do not add values to that type.
///
/// Use [Cause.combine] to preserve multiple failures, such as a business failure
/// followed by failures while releasing resources.
sealed class Cause<E> {
  const Cause._();

  /// Creates a business failure containing [error].
  const factory Cause.failure(E error) = CauseFailure<E>;

  /// Creates an unexpected failure with its original [error] and [stackTrace].
  const factory Cause.defect(Object error, StackTrace stackTrace) = CauseDefect;

  /// Creates a cancellation with an optional [reason].
  ///
  /// The reason is retained unchanged and may describe the request or its source.
  const factory Cause.cancelled([Object? reason]) = CauseCancelled;

  /// Combines [causes] in iteration order without removing duplicate reasons.
  ///
  /// Groups are flattened, and the resulting list cannot be modified. A single
  /// cause is returned unchanged, including when it is already a group.
  ///
  /// Throws an [ArgumentError] if [causes] is empty.
  factory Cause.combine(Iterable<Cause<E>> causes) {
    final reasons = <Cause<E>>[];
    Cause<E>? onlyCause;
    var count = 0;

    for (final cause in causes) {
      onlyCause = cause;
      count++;
      switch (cause) {
        case CauseGroup(:final causes):
          reasons.addAll(causes);
        default:
          reasons.add(cause);
      }
    }

    if (count == 0) {
      throw ArgumentError.value(causes, 'causes', 'Must not be empty.');
    }
    if (count == 1) return onlyCause!;
    return CauseGroup._(List.unmodifiable(reasons));
  }
}

/// A business failure containing an error of type [E].
final class CauseFailure<E> extends Cause<E> {
  /// Creates a business failure containing [error].
  const CauseFailure(this.error) : super._();

  /// The business error, retained unchanged.
  final E error;
}

/// An unexpected failure with its original error and stack trace.
final class CauseDefect extends Cause<Never> {
  /// Creates a defect with [error] and [stackTrace].
  const CauseDefect(this.error, this.stackTrace) : super._();

  /// The original thrown object.
  final Object error;

  /// The stack trace captured when [error] was thrown.
  final StackTrace stackTrace;
}

/// A cancellation, optionally carrying information about its request.
final class CauseCancelled extends Cause<Never> {
  /// Creates a cancellation with an optional [reason].
  const CauseCancelled([this.reason]) : super._();

  /// Information about the cancellation request, retained unchanged.
  final Object? reason;
}

/// An ordered, immutable group of at least two failure reasons.
///
/// Create groups with [Cause.combine]. Each member of [causes] is a failure,
/// defect, or cancellation; groups never contain other groups.
final class CauseGroup<E> extends Cause<E> {
  CauseGroup._(this.causes) : super._();

  /// The flattened reasons in their original order, including duplicates.
  final List<Cause<E>> causes;
}

/// Business error transformations for [Cause].
extension CauseMapping<E> on Cause<E> {
  /// Transforms every business error while preserving the other reasons.
  ///
  /// Calls [transform] once per [CauseFailure], in group order. Defects and
  /// cancellations are retained unchanged, including their object identities.
  /// Errors thrown by [transform] propagate to the caller.
  Cause<F> mapError<F>(F Function(E error) transform) {
    final cause = this;
    switch (cause) {
      case CauseFailure(:final error):
        return Cause<F>.failure(transform(error));
      case CauseDefect():
        return cause;
      case CauseCancelled():
        return cause;
      case CauseGroup(:final causes):
        return Cause<F>.combine(
          causes.map((reason) => reason.mapError(transform)),
        );
    }
  }
}
