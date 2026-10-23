import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  late Runtime runtime;
  setUp(() => runtime = Runtime());
  tearDown(() async {
    expect(await runtime.close(), isA<ExitSuccess<void, Never>>());
  });

  test('map provider distinguishes absence from an empty string', () async {
    final provider = ConfigProvider.fromMap({'empty': '', 'db.host': 'local'});

    expect(_some(await runtime.runOrThrow(provider.read(['empty']))), '');
    expect(
      _some(await runtime.runOrThrow(provider.read(['db', 'host']))),
      'local',
    );
    expect(
      await runtime.runOrThrow(provider.read(['missing'])),
      isA<OptionNone<String>>(),
    );
  });

  test('map values and path arguments are copied', () async {
    final values = {'app_port': '80'};
    final provider = ConfigProvider.fromMap(values, separator: '_');
    final path = ['app', 'port'];
    final read = provider.read(path);
    values['app_port'] = '90';
    path.clear();

    expect(_some(await runtime.runOrThrow(read)), '80');
    expect(_some(await runtime.runOrThrow(read)), '80');
  });

  test('map provider rejects an empty separator', () {
    expect(
      () => ConfigProvider.fromMap({}, separator: ''),
      throwsArgumentError,
    );
  });

  test(
    'fallback uses the first present value including empty strings',
    () async {
      final reads = <String>[];
      final provider = ConfigProvider.fallback([
        _Provider(
          (path) => Task.sync(() {
            reads.add('first');
            return const Option.none();
          }),
        ),
        _Provider(
          (path) => Task.sync(() {
            reads.add('second');
            return const Option.some('');
          }),
        ),
        _Provider(
          (path) => Task.sync(() {
            reads.add('third');
            return const Option.some('unused');
          }),
        ),
      ]);
      final read = provider.read(['key']);
      expect(reads, isEmpty);

      expect(_some(await runtime.runOrThrow(read)), '');
      expect(_some(await runtime.runOrThrow(read)), '');
      expect(reads, ['first', 'second', 'first', 'second']);
    },
  );

  test(
    'fallback snapshots providers and path without altering the path',
    () async {
      final observed = <List<String>>[];
      final providers = <ConfigProvider>[
        _Provider(
          (path) => Task.sync(() {
            observed.add(path);
            return const Option.none();
          }),
        ),
        ConfigProvider.fromMap({'db.host': 'local'}),
      ];
      final provider = ConfigProvider.fallback(providers);
      final path = ['db', 'host'];
      final read = provider.read(path);
      providers.clear();
      path.clear();

      expect(_some(await runtime.runOrThrow(read)), 'local');
      expect(observed.single, ['db', 'host']);
      expect(() => observed.single.add('other'), throwsUnsupportedError);
    },
  );

  test('empty fallback and absent values remain absent', () async {
    for (final provider in [
      ConfigProvider.fallback([]),
      ConfigProvider.fallback([ConfigProvider.fromMap({})]),
    ]) {
      expect(
        await runtime.runOrThrow(provider.read(['key'])),
        isA<OptionNone<String>>(),
      );
    }
  });

  test(
    'fallback preserves source failure and skips lower priority providers',
    () async {
      final error = ConfigError.source(
        StateError('private source detail'),
        StackTrace.current,
        path: ['db', 'host'],
      );
      var fallbackRead = false;
      final provider = ConfigProvider.fallback([
        _Provider((_) => Task.fail(error)),
        _Provider(
          (_) => Task.sync(() {
            fallbackRead = true;
            return const Option.some('fallback');
          }),
        ),
      ]);

      expect(
        _cause(await runtime.run(provider.read(['db', 'host']))),
        isA<CauseFailure<ConfigError>>().having(
          (cause) => cause.error,
          'error',
          same(error),
        ),
      );
      expect(fallbackRead, isFalse);
    },
  );

  test('unexpected provider exceptions remain defects', () async {
    final error = StateError('provider bug');
    final provider = ConfigProvider.fallback([
      _Provider((_) => throw error),
      ConfigProvider.fromMap({'key': 'unused'}),
    ]);

    expect(
      _cause(await runtime.run(provider.read(['key']))),
      isA<CauseDefect>().having((cause) => cause.error, 'error', same(error)),
    );
  });

  test('source error formatting does not inspect its original cause', () {
    final source = _PrivateError();
    final trace = StackTrace.fromString('private stack');
    final error = ConfigError.source(source, trace, path: ['db', 'secret']);

    expect(error.toString(), contains('db.secret'));
    expect(error.toString(), isNot(contains('private')));
    expect(error.issues.single.cause, same(source));
    expect(error.issues.single.stackTrace, same(trace));
    expect(error.issues.single.kind, ConfigIssueKind.source);
  });

  test('error paths and issue collections are immutable snapshots', () {
    final path = ['port'];
    final missing = ConfigError.missing(path: path);
    path.clear();
    final prefixed = missing.withPrefix(['app']);
    final invalid = ConfigError.invalid(
      'Expected an integer.',
      path: ['other'],
    );
    final combined = ConfigError.combine([prefixed, invalid, prefixed]);

    expect(missing.isMissing, isTrue);
    expect(combined.isMissing, isFalse);
    expect(combined.issues.map((issue) => issue.path), [
      ['app', 'port'],
      ['other'],
      ['app', 'port'],
    ]);
    expect(() => combined.issues.clear(), throwsUnsupportedError);
    expect(() => missing.issues.single.path.clear(), throwsUnsupportedError);
    expect(() => ConfigError.combine([]), throwsArgumentError);
  });
}

final class _Provider implements ConfigProvider {
  _Provider(this.readValue);
  final Task<Option<String>, ConfigError> Function(List<String>) readValue;

  @override
  Task<Option<String>, ConfigError> read(List<String> path) => readValue(path);
}

final class _PrivateError {
  @override
  String toString() => throw StateError('Source errors must not be formatted.');
}

T _some<T>(Option<T> option) => switch (option) {
  OptionSome(:final value) => value,
  OptionNone() => fail('Expected a present value.'),
};

Cause<E> _cause<T, E>(Exit<T, E> exit) => switch (exit) {
  ExitSuccess(:final value) => fail('Expected failure, got $value.'),
  ExitFailure(:final cause) => cause,
};
