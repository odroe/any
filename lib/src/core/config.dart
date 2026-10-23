import 'dart:core';
import 'dart:core' as core;

import 'cause.dart';
import 'config_provider.dart';
import 'option.dart';
import 'redacted.dart';
import 'result.dart';
import 'task.dart';

/// A lazy description of a value loaded from a [ConfigProvider].
///
/// [load] reads the provider bound to [ConfigProvider.key] on each execution.
/// Constructing and combining descriptions does not read or parse values.
/// Names are single path segments; use [ConfigOperations.nested] for prefixes.
final class Config<T> {
  Config._(this._read);

  final Task<T, ConfigError> Function(ConfigProvider, List<String>) _read;

  /// Reads a string unchanged, including whitespace and an empty string.
  static Config<String> string(String name) =>
      parse(name, (value) => Result.success(value));

  /// Reads a base-10 integer, allowing surrounding whitespace and a sign.
  ///
  /// Empty values, fractional values, and hexadecimal notation are invalid.
  static Config<core.int> int(String name) => parse(name, _parseInt);

  /// Reads a finite double, allowing surrounding whitespace.
  ///
  /// NaN, infinities, and empty values are invalid.
  static Config<core.double> double(String name) => parse(name, _parseDouble);

  /// Reads exactly `true` or `false` after trimming surrounding whitespace.
  ///
  /// Matching is case-sensitive; other spellings and numeric values are invalid.
  static Config<core.bool> bool(String name) => parse(name, _parseBool);

  /// Reads [name] and parses its value with [parse].
  ///
  /// Parser failures receive the full field path before their own relative
  /// paths. Source errors already contain their paths and pass through unchanged.
  /// Exceptions thrown by [parse] remain defects.
  static Config<T> parse<T>(
    String name,
    Result<T, ConfigError> Function(String value) parse,
  ) => Config._((provider, prefix) {
    final path = List<String>.unmodifiable([...prefix, name]);
    return provider
        .read(path)
        .flatMap(
          (option) => switch (option) {
            OptionNone() => Task.fail(ConfigError.missing(path: path)),
            OptionSome(:final value) => Task.fromResult(
              parse(value).mapError((error) => error.withPrefix(path)),
            ),
          },
        );
  });

  /// Reads a delimited list and parses each element with [parse].
  ///
  /// An empty raw string produces an empty list. Other values are split on the
  /// literal [separator], retaining empty elements. Each element is trimmed
  /// when [trim] is `true`; its parser determines whether an empty element is
  /// valid. All parser failures are accumulated with zero-based index paths.
  ///
  /// The result is immutable. An empty separator produces an [ArgumentError]
  /// defect on execution, before reading. Parser exceptions remain defects.
  static Config<List<T>> list<T>(
    String name, {
    required Result<T, ConfigError> Function(String value) parse,
    String separator = ',',
    core.bool trim = true,
  }) {
    final parsed = Config.parse<List<T>>(name, (value) {
      if (value.isEmpty) return const Result.success([]);
      final parts = value.split(separator);
      final values = <T>[];
      final errors = <ConfigError>[];
      for (var index = 0; index < parts.length; index++) {
        final part = trim ? parts[index].trim() : parts[index];
        switch (parse(part)) {
          case ResultSuccess(:final value):
            values.add(value);
          case ResultFailure(:final error):
            errors.add(error.withPrefix(['$index']));
        }
      }
      return errors.isEmpty
          ? Result.success(List.unmodifiable(values))
          : Result.failure(ConfigError.combine(errors));
    });
    return Config._(
      (provider, prefix) => Task.defer(() {
        if (separator.isEmpty) {
          throw ArgumentError.value(
            separator,
            'separator',
            'Must not be empty.',
          );
        }
        return parsed._read(provider, prefix);
      }),
    );
  }

  /// Loads [configs] in order, accumulating their expected configuration errors.
  ///
  /// Defects and cancellation stop loading and retain their original cause.
  /// The descriptions are copied when this method is called. Each execution
  /// reads them afresh, returning an immutable list, including for empty input.
  static Config<List<T>> all<T>(Iterable<Config<T>> configs) {
    final inputs = List<Config<T>>.unmodifiable(configs);
    return Config._(
      (provider, prefix) => Task.defer(() {
        final values = <T>[];
        final errors = <ConfigError>[];
        Task<void, ConfigError> collected = Task.succeed(null);
        for (final config in inputs) {
          collected = collected.flatMap(
            (_) => _capture(config._read(provider, prefix)).map((result) {
              switch (result) {
                case ResultSuccess(:final value):
                  values.add(value);
                case ResultFailure(:final error):
                  errors.add(error);
              }
            }),
          );
        }
        return collected.flatMap(
          (_) => errors.isEmpty
              ? Task.succeed(List<T>.unmodifiable(values))
              : Task.fail(ConfigError.combine(errors)),
        );
      }),
    );
  }

  /// Loads this description from the current configuration service.
  ///
  /// Reads and parsing repeat on each execution. A missing service binding is
  /// a defect; a missing configuration field is a [ConfigError].
  Task<T, ConfigError> load() =>
      Task.service(ConfigProvider.key)
          .withError<ConfigError>()
          .flatMap((provider) => _read(provider, const []));
}

/// Composes configurations using their statically known value types.
extension ConfigOperations<T> on Config<T> {
  /// Transforms a loaded value; exceptions from [transform] become defects.
  Config<U> map<U>(U Function(T value) transform) =>
      Config._((provider, prefix) => _read(provider, prefix).map(transform));

  /// Selects another configuration after successfully loading this value.
  ///
  /// Both descriptions use the same provider and enclosing path prefix.
  /// Exceptions from [next] become defects.
  Config<U> flatMap<U>(Config<U> Function(T value) next) => Config._(
    (provider, prefix) => _read(
      provider,
      prefix,
    ).flatMap((value) => next(value)._read(provider, prefix)),
  );

  /// Loads both descriptions in order and returns their values as a record.
  ///
  /// Expected configuration problems from both sides are accumulated in order.
  /// A defect or cancellation stops loading without becoming a ConfigError.
  Config<(T, U)> zip<U>(Config<U> other) => Config._(
    (provider, prefix) => _capture(_read(provider, prefix)).flatMap(
      (left) => _capture(other._read(provider, prefix)).flatMap(
        (right) => switch ((left, right)) {
          (ResultSuccess(value: final a), ResultSuccess(value: final b)) =>
            Task.succeed((a, b)),
          (ResultFailure(error: final a), ResultFailure(error: final b)) =>
            Task.fail(ConfigError.combine([a, b])),
          (ResultFailure(:final error), _) => Task.fail(error),
          (_, ResultFailure(:final error)) => Task.fail(error),
        },
      ),
    ),
  );

  /// Prepends one [name] segment to the paths read by this description.
  ///
  /// `Config.string('host').nested('db').nested('app')` reads
  /// `['app', 'db', 'host']`. Dots within a name are not split into segments.
  Config<T> nested(String name) =>
      Config._((provider, prefix) => _read(provider, [...prefix, name]));

  /// Returns absence only when every reported problem is a missing value.
  ///
  /// Invalid values, source errors, defects, and cancellation remain failures.
  /// A present nullable value remains [OptionSome].
  Config<Option<T>> optional() => Config._(
    (provider, prefix) => _read(provider, prefix)
        .map<Option<T>>(Option.some)
        .recover(
          (error) => error.isMissing
              ? Task.succeed(const Option.none())
              : Task.fail(error),
        ),
  );

  /// Uses [value] only when every reported problem is a missing value.
  ///
  /// Invalid values and unavailable sources do not fall back to this default.
  Config<T> withDefault(T value) => Config._(
    (provider, prefix) => _read(provider, prefix).recover(
      (error) => error.isMissing ? Task.succeed(value) : Task.fail(error),
    ),
  );

  /// Wraps the loaded value so its default string representation is redacted.
  Config<Redacted<T>> redacted() => map(Redacted<T>.new);
}

Task<Result<T, ConfigError>, ConfigError> _capture<T>(
  Task<T, ConfigError> task,
) => task.map<Result<T, ConfigError>>(Result.success).recoverCause((cause) {
  final reasons = switch (cause) {
    CauseGroup(:final causes) => causes,
    _ => [cause],
  };
  if (reasons.any((reason) => reason is! CauseFailure<ConfigError>)) {
    return Task.failCause(cause);
  }
  return Task.succeed(
    Result.failure(
      ConfigError.combine(
        reasons.whereType<CauseFailure<ConfigError>>().map(
          (reason) => reason.error,
        ),
      ),
    ),
  );
});

Result<int, ConfigError> _parseInt(String value) {
  final parsed = int.tryParse(value.trim(), radix: 10);
  return parsed == null
      ? Result.failure(ConfigError.invalid('Expected a base-10 integer.'))
      : Result.success(parsed);
}

Result<double, ConfigError> _parseDouble(String value) {
  final parsed = double.tryParse(value.trim());
  return parsed == null || !parsed.isFinite
      ? Result.failure(ConfigError.invalid('Expected a finite number.'))
      : Result.success(parsed);
}

Result<bool, ConfigError> _parseBool(String value) => switch (value.trim()) {
  'true' => const Result.success(true),
  'false' => const Result.success(false),
  _ => Result.failure(ConfigError.invalid('Expected true or false.')),
};
