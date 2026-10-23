import 'package:any/any.dart';

import 'validation_error.dart';

/// A synchronous parser from untrusted values to values of type [T].
///
/// [parse] returns expected validation errors through [Result]. Combinators
/// preserve static output types and accumulate independent field or list issues.
/// No reflection, code generation, implicit coercion, or global registry is used.
/// Custom parser and transformation exceptions propagate to the caller; they
/// represent programming failures, not invalid input. A surrounding [Task]
/// converts such throws into defects in the usual way.
///
/// ```dart
/// final user = Schema<String>.type().field('name')
///     .zip(Schema<int>.type().refine((age) => age >= 0,
///       message: 'Must be nonnegative.').field('age'))
///     .map((fields) => (name: fields.$1, age: fields.$2));
///
/// final result = user.parse({'name': 'Ada', 'age': 37});
/// if (result case Success(:final value)) {
///   print(value.name); // Ada
/// }
/// ```
final class Schema<T> {
  /// Creates a schema using [parse] for application-specific validation.
  ///
  /// Return a failure with [ValidationError] for invalid input. Thrown objects
  /// propagate unchanged. Creating a schema does not invoke the callback.
  const Schema(Result<T, ValidationError> Function(Object? input) parse)
    : _parse = parse;

  /// Accepts values satisfying Dart's `is T` type check, without conversion.
  ///
  /// [name] supplies a readable type name in diagnostics; it defaults to `T`'s
  /// runtime name. A mismatch has issue code `type`. This checks runtime types,
  /// not object fields; compose [field], [list], and [refine] for deeper checks.
  factory Schema.type({String? name}) => Schema(
    (input) => input is T
        ? Result.success(input)
        : Result.failure(_issue('type', 'Expected ${name ?? T}.')),
  );

  final Result<T, ValidationError> Function(Object?) _parse;

  /// Validates [input], returning the parsed value or structured issues.
  ///
  /// Does not catch exceptions. Payload copying depends on the schema: [type]
  /// retains values by reference, while [list] creates an unmodifiable list.
  Result<T, ValidationError> parse(Object? input) => _parse(input);

  /// Transforms a valid value with [transform], skipping invalid inputs.
  ///
  /// Throws from [transform] propagate unchanged. Use [refine] for additional
  /// validation, or the constructor for a conversion that can fail normally.
  Schema<U> map<U>(U Function(T value) transform) => Schema(
    (input) => switch (parse(input)) {
      Success(:final value) => Result.success(transform(value)),
      Failure(:final error) => Result.failure(error),
    },
  );

  /// Requires parsed values to satisfy [test].
  ///
  /// Failed inputs skip [test]. A rejected value produces one issue with [code]
  /// and [message] at the current path. Throws from [test] propagate unchanged.
  Schema<T> refine(
    bool Function(T value) test, {
    required String message,
    String code = 'refinement',
  }) => Schema((input) {
    final result = parse(input);
    if (result case Success(:final value)) {
      if (!test(value)) return Result.failure(_issue(code, message));
    }
    return result;
  });

  /// Also accepts null, without invoking this schema for null inputs.
  ///
  /// This permits a null value; [field] still requires the key to be present.
  Schema<T?> nullable() => Schema(
    (input) => input == null
        ? const Result.success(null)
        : switch (parse(input)) {
            Success(:final value) => Result.success(value),
            Failure(:final error) => Result.failure(error),
          },
  );

  /// Validates each list element, collecting all issues in index order.
  ///
  /// A non-list input has issue code `type`. Element issue paths are prefixed
  /// with their zero-based index. Successful output is an unmodifiable list
  /// snapshot; elements retain whatever ownership their individual schemas use.
  /// An empty list succeeds. No output list is returned when any element fails.
  Schema<List<T>> list() => Schema((input) {
    if (input is! List<Object?>) {
      return Result.failure(_issue('type', 'Expected a list.'));
    }
    List<T>? values = [];
    List<ValidationIssue>? issues;
    for (var index = 0; index < input.length; index++) {
      switch (parse(input[index])) {
        case Success(:final value):
          values?.add(value);
        case Failure(:final error):
          values = null;
          (issues ??= []).addAll(error.issues.map((issue) => issue.at(index)));
      }
    }
    return issues == null
        ? Result.success(List.unmodifiable(values!))
        : Result.failure(ValidationError(issues));
  });

  /// Reads and validates required field [name] from a map.
  ///
  /// A non-map input has issue code `type`; a missing key has code `missing`.
  /// A present null is passed to this schema, so null and absence stay distinct.
  /// Other keys are ignored. Validation paths are prefixed with [name].
  Schema<T> field(String name) => Schema((input) {
    if (input is! Map<Object?, Object?>) {
      return Result.failure(_issue('type', 'Expected a map.'));
    }
    if (!input.containsKey(name)) {
      return Result.failure(
        _issue('missing', 'Required field is missing.').at(name),
      );
    }
    return switch (parse(input[name])) {
      Success(:final value) => Result.success(value),
      Failure(:final error) => Result.failure(error.at(name)),
    };
  });

  /// Validates the same input with both schemas and pairs their output values.
  ///
  /// Both schemas run, left to right, even if the first returns a failure.
  /// Issues are combined in that order, preserving duplicates. Thrown objects
  /// stop evaluation immediately. Use [map] on the pair to construct a typed
  /// record or application object; this avoids lookup by runtime type or casts.
  Schema<(T, U)> zip<U>(Schema<U> other) => Schema((input) {
    final first = parse(input);
    final second = other.parse(input);
    return switch ((first, second)) {
      (Success(value: final left), Success(value: final right)) =>
        Result.success((left, right)),
      (Failure(error: final left), Failure(error: final right)) =>
        Result.failure(ValidationError([...left.issues, ...right.issues])),
      (Failure(:final error), _) ||
      (_, Failure(:final error)) => Result.failure(error),
    };
  });
}

ValidationError _issue(String code, String message) =>
    ValidationError([ValidationIssue(code: code, message: message)]);
