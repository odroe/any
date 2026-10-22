/// One or more reasons why a computation did not succeed.
///
/// [CauseFailure] holds an expected error of type [E]. [CauseDefect] holds a
/// thrown object and its original stack trace. [CauseCancelled] records
/// cancellation independently of [E]. [CauseGroup] combines multiple reasons.
///
/// A cause is data. Creating one does not throw an exception or request
/// cancellation. Payloads are retained by reference.
///
/// ```dart
/// final cause = Cause<String>.combine([
///   const Cause.failure('Request failed.'),
///   Cause.defect(StateError('Cleanup failed.'), StackTrace.current),
/// ]);
///
/// final message = switch (cause) {
///   CauseFailure(:final error) => error,
///   CauseDefect(:final error) => 'Unexpected: $error',
///   CauseCancelled(:final reason) => 'Cancelled: $reason',
///   CauseGroup(:final causes) => '${causes.length} failure reasons.',
/// };
/// ```
sealed class Cause<E> {
  const Cause._();

  /// Records an expected failure with [error].
  const factory Cause.failure(E error) = CauseFailure<E>;

  /// Records a thrown [error] with its original [stackTrace].
  ///
  /// Pass the trace received by `catch (error, stackTrace)` to preserve where
  /// the error was thrown. The error need not be an [Exception].
  const factory Cause.defect(Object error, StackTrace stackTrace) =
      CauseDefect<E>;

  /// Records cancellation with an optional [reason].
  const factory Cause.cancelled({Object? reason}) = CauseCancelled<E>;

  /// Combines [causes] in iteration order, flattening any [CauseGroup] entries.
  ///
  /// A single entry is returned unchanged. Multiple entries produce a group
  /// with an unmodifiable snapshot of the individual causes. Duplicates are
  /// preserved; payloads are not copied. Iteration order does not imply that
  /// the failures happened sequentially or establish their relative timing.
  ///
  /// Throws an [ArgumentError] if [causes] is empty.
  factory Cause.combine(Iterable<Cause<E>> causes) {
    Cause<E>? first;
    List<Cause<E>>? combined;
    for (final cause in causes) {
      if (first == null) {
        first = cause;
        continue;
      }
      combined ??= first is CauseGroup<E>
          ? List<Cause<E>>.of(first.causes)
          : [first];
      if (cause is CauseGroup<E>) {
        combined.addAll(cause.causes);
      } else {
        combined.add(cause);
      }
    }
    if (first == null) {
      throw ArgumentError.value(causes, 'causes', 'Must not be empty.');
    }
    return combined == null ? first : CauseGroup._(combined);
  }

  @override
  String toString() => switch (this) {
    CauseFailure(:final error) => 'Cause.failure($error)',
    CauseDefect(:final error) => 'Cause.defect($error)',
    CauseCancelled(:final reason) => 'Cause.cancelled(reason: $reason)',
    CauseGroup(:final causes) => 'Cause.combine($causes)',
  };
}

/// An expected failure with an [error] of type [E].
final class CauseFailure<E> extends Cause<E> {
  /// Records an expected failure with [error].
  const CauseFailure(this.error) : super._();

  /// The expected error, which may be null if [E] allows it.
  final E error;
}

/// A thrown [error] and its original [stackTrace].
final class CauseDefect<E> extends Cause<E> {
  /// Records a thrown [error] with its original [stackTrace].
  const CauseDefect(this.error, this.stackTrace) : super._();

  /// The original thrown object, independent of the expected error type [E].
  final Object error;

  /// The original stack trace associated with [error].
  final StackTrace stackTrace;
}

/// Cancellation with an optional [reason].
final class CauseCancelled<E> extends Cause<E> {
  /// Records cancellation with an optional [reason].
  const CauseCancelled({this.reason}) : super._();

  /// The cancellation reason, independent of [E], or null if none was supplied.
  final Object? reason;
}

/// Multiple individual causes, created by [Cause.combine].
final class CauseGroup<E> extends Cause<E> {
  CauseGroup._(List<Cause<E>> causes)
    : causes = List.unmodifiable(causes),
      super._();

  /// The individual causes in combination order, including duplicates.
  ///
  /// This list is unmodifiable, contains at least two entries, and contains no
  /// nested [CauseGroup] values.
  final List<Cause<E>> causes;
}
