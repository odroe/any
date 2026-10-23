/// One validation problem at a field or list index.
///
/// Issues contain diagnostic metadata, without retaining the rejected input.
final class ValidationIssue {
  /// Creates an issue with an immutable snapshot of [path].
  ///
  /// Each path segment must be a string field name or a nonnegative integer
  /// list index. An empty path refers to the value being validated. Invalid
  /// segments throw an [ArgumentError].
  ValidationIssue({
    required this.code,
    required this.message,
    Iterable<Object> path = const [],
  }) : path = List.unmodifiable(path) {
    for (final segment in this.path) {
      if (segment is! String && (segment is! int || segment < 0)) {
        throw ArgumentError.value(
          segment,
          'path',
          'Expected a field name or nonnegative index.',
        );
      }
    }
  }

  /// A stable code suitable for programmatic handling or localization.
  final String code;

  /// A human-readable explanation of the constraint.
  final String message;

  /// Field names and list indices from the root to the invalid value.
  final List<Object> path;

  /// The same issue nested below [segment].
  ///
  /// The segment follows the same rules as the constructor's path.
  ValidationIssue at(Object segment) =>
      ValidationIssue(code: code, message: message, path: [segment, ...path]);

  @override
  String toString() => '$code: $message (path: $path)';
}

/// One or more validation issues in traversal order.
///
/// This is expected error data, rather than a thrown exception. Use its
/// structured [issues] to render field feedback or translate application errors.
final class ValidationError {
  /// Creates an immutable snapshot of [issues].
  ///
  /// Throws an [ArgumentError] for an empty input: a failure must have at least
  /// one issue. Order and duplicate issues are preserved.
  ValidationError(Iterable<ValidationIssue> issues)
    : issues = List.unmodifiable(issues) {
    if (this.issues.isEmpty) throw ArgumentError('issues must not be empty.');
  }

  /// The nonempty, unmodifiable list of validation issues.
  final List<ValidationIssue> issues;

  /// The same issues nested below a field name or list index [segment].
  ValidationError at(Object segment) =>
      ValidationError(issues.map((issue) => issue.at(segment)));

  @override
  String toString() => 'ValidationError(${issues.join(', ')})';
}
