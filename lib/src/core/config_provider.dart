import 'context.dart';
import 'option.dart';
import 'task.dart';

/// A source of configuration strings addressed by path segments.
///
/// Use [ConfigProvider.fromMap] for in-memory settings and
/// [ConfigProvider.fallback] to layer sources in priority order. Platform
/// adapters can implement this interface for environment variables or files.
///
/// Reads are deferred until the returned task executes. Absence is [OptionNone];
/// an empty string is still a present value. Expected source errors use
/// [ConfigError] with the full requested path; unexpected exceptions are defects.
abstract interface class ConfigProvider {
  /// Copies [values] into an immutable, in-memory configuration source.
  ///
  /// Paths are joined with [separator], which defaults to `'.'`. For example,
  /// `['server', 'port']` reads `'server.port'`. Throws [ArgumentError] for an
  /// empty separator. Later map changes do not affect the provider.
  factory ConfigProvider.fromMap(
    Map<String, String> values, {
    String separator,
  }) = _MapConfigProvider;

  /// Tries [providers] in order, consulting the next only for an absent value.
  ///
  /// The first present value wins, including an empty string. A read failure
  /// stops the search; it is never treated as absence. The provider list is
  /// copied, and an empty list always returns absence.
  factory ConfigProvider.fallback(Iterable<ConfigProvider> providers) =
      _FallbackConfigProvider;

  /// The service key used by `Config.load`.
  static final key = ServiceKey<ConfigProvider>('config');

  /// Reads the value at [path] when the returned task executes.
  ///
  /// Each string is one path segment. Sources define how segments map to their
  /// keys. Implementations must preserve missing values as absence and include
  /// the full path in any expected source error.
  Task<Option<String>, ConfigError> read(List<String> path);
}

final class _MapConfigProvider implements ConfigProvider {
  _MapConfigProvider(Map<String, String> values, {String separator = '.'})
    : _values = Map.unmodifiable(values),
      _separator = separator {
    if (separator.isEmpty) {
      throw ArgumentError.value(separator, 'separator', 'Must not be empty.');
    }
  }

  final Map<String, String> _values;
  final String _separator;

  @override
  Task<Option<String>, ConfigError> read(List<String> path) {
    final name = path.join(_separator);
    return Task.sync(
      () => switch (_values[name]) {
        final value? => Option.some(value),
        null => const Option.none(),
      },
    );
  }
}

final class _FallbackConfigProvider implements ConfigProvider {
  _FallbackConfigProvider(Iterable<ConfigProvider> providers)
    : _providers = List.unmodifiable(providers);

  final List<ConfigProvider> _providers;

  @override
  Task<Option<String>, ConfigError> read(List<String> path) {
    final segments = List<String>.unmodifiable(path);
    Task<Option<String>, ConfigError> next(int index) => Task.defer(() {
      if (index == _providers.length) {
        return Task.succeed(const Option.none());
      }
      return _providers[index]
          .read(segments)
          .flatMap(
            (value) => switch (value) {
              OptionSome() => Task.succeed(value),
              OptionNone() => next(index + 1),
            },
          );
    });

    return next(0);
  }
}

/// The category of a configuration problem.
enum ConfigIssueKind {
  /// No value exists at the requested path.
  missing,

  /// A present value cannot be parsed or validated.
  invalid,

  /// The provider could not read its source.
  source,
}

/// A configuration problem with a path and diagnostic message.
///
/// Instances are created by [ConfigError]'s factories. The default text excludes
/// [cause] and [stackTrace], which may contain sensitive source information.
final class ConfigIssue {
  ConfigIssue._(
    this.kind,
    Iterable<String> path,
    this.message, {
    this.cause,
    this.stackTrace,
  }) : path = List.unmodifiable(path);

  /// The category of this configuration problem.
  final ConfigIssueKind kind;

  /// The complete path, or a relative path from a custom value parser.
  final List<String> path;

  /// A diagnostic that should describe the expectation without secret values.
  final String message;

  /// The original source error, available for explicit diagnostic inspection.
  final Object? cause;

  /// The original source stack trace, when supplied.
  final StackTrace? stackTrace;

  @override
  String toString() => '${path.isEmpty ? '<root>' : path.join('.')}: $message';
}

/// One or more configuration problems in reporting order.
///
/// Built-in diagnostics never include raw configuration values. Custom parsers
/// should also omit such values from the messages they provide. Source errors
/// are retained for explicit inspection and excluded from [toString].
final class ConfigError implements Exception {
  ConfigError._(Iterable<ConfigIssue> issues)
    : issues = List.unmodifiable(issues);

  /// Describes a required value that is absent at [path].
  factory ConfigError.missing({Iterable<String> path = const []}) =>
      ConfigError._([
        ConfigIssue._(
          ConfigIssueKind.missing,
          path,
          'Required value is missing.',
        ),
      ]);

  /// Describes invalid content using a value-free [message].
  ///
  /// A custom `Config` parser supplies a relative [path]; its field path is
  /// prepended during loading.
  factory ConfigError.invalid(
    String message, {
    Iterable<String> path = const [],
  }) => ConfigError._([ConfigIssue._(ConfigIssueKind.invalid, path, message)]);

  /// Retains a source [cause] and [stackTrace] with the requested [path].
  ///
  /// The default diagnostic does not format either object.
  factory ConfigError.source(
    Object cause,
    StackTrace stackTrace, {
    Iterable<String> path = const [],
  }) => ConfigError._([
    ConfigIssue._(
      ConfigIssueKind.source,
      path,
      'Configuration source could not be read.',
      cause: cause,
      stackTrace: stackTrace,
    ),
  ]);

  /// Flattens [errors] in order without discarding duplicate issues.
  ///
  /// Throws [ArgumentError] if no errors are provided.
  factory ConfigError.combine(Iterable<ConfigError> errors) {
    final issues = errors.expand((error) => error.issues).toList();
    if (issues.isEmpty) {
      throw ArgumentError.value(errors, 'errors', 'Must not be empty.');
    }
    return ConfigError._(issues);
  }

  /// The ordered, immutable configuration problems.
  final List<ConfigIssue> issues;

  /// Whether every issue describes an absent value.
  bool get isMissing =>
      issues.every((issue) => issue.kind == ConfigIssueKind.missing);

  /// Prepends [prefix] to every issue path, retaining its diagnostic details.
  ConfigError withPrefix(Iterable<String> prefix) {
    final segments = prefix.toList();
    return ConfigError._(
      issues.map(
        (issue) => ConfigIssue._(
          issue.kind,
          [...segments, ...issue.path],
          issue.message,
          cause: issue.cause,
          stackTrace: issue.stackTrace,
        ),
      ),
    );
  }

  @override
  String toString() => 'ConfigError: ${issues.join('; ')}';
}
