/// A value that is either present or absent.
///
/// [OptionSome] holds a value, including `null` when [T] is nullable.
/// [OptionNone] represents absence. Use a nullable type directly when there is
/// no need to distinguish a present `null` from an absent value.
///
/// Use an exhaustive switch to handle presence and absence:
///
/// ```dart
/// const Option<String?> name = Option.some(null);
/// final label = switch (name) {
///   OptionSome(:final value) => value ?? '(unnamed)',
///   OptionNone() => '(missing)',
/// };
/// ```
sealed class Option<T> {
  const Option._();

  /// Creates an option containing [value], even if it is `null`.
  const factory Option.some(T value) = OptionSome<T>;

  /// Creates an option with no value.
  const factory Option.none() = OptionNone<T>;
}

/// A present value in an [Option].
final class OptionSome<T> extends Option<T> {
  /// Creates an option containing [value], even if it is `null`.
  const OptionSome(this.value) : super._();

  /// The present value.
  final T value;
}

/// An [Option] with no value.
final class OptionNone<T> extends Option<T> {
  /// Creates an option with no value.
  const OptionNone() : super._();
}

/// Transformations and fallbacks for optional values.
extension OptionOperations<T> on Option<T> {
  /// Transforms a present value, preserving absence.
  ///
  /// A present `null` is passed to [transform]. Exceptions propagate to the
  /// caller.
  Option<R> map<R>(R Function(T value) transform) => switch (this) {
    OptionSome<T>(:final value) => Option<R>.some(transform(value)),
    OptionNone<T>() => const Option<Never>.none(),
  };

  /// Transforms a present value into another option, preserving absence.
  ///
  /// Use this when [transform] can itself return an absent value. Exceptions
  /// from the callback propagate to the caller.
  Option<R> flatMap<R>(Option<R> Function(T value) transform) => switch (this) {
    OptionSome<T>(:final value) => transform(value),
    OptionNone<T>() => const Option<Never>.none(),
  };

  /// Returns the present value, or calls [orElse] when no value is present.
  ///
  /// A present `null` is returned as-is. [orElse] is called only for
  /// [OptionNone], and any exception it throws propagates to the caller.
  T getOrElse(T Function() orElse) => switch (this) {
    OptionSome<T>(:final value) => value,
    OptionNone<T>() => orElse(),
  };
}
