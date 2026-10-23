import 'dart:async';

import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  Runtime runtime(ConfigProvider provider) {
    final runtime = Runtime(
      context: Context.from([ConfigProvider.key.bind(provider)]),
    );
    addTearDown(() async {
      expect(await runtime.close(), isA<ExitSuccess<void, Never>>());
    });
    return runtime;
  }

  Runtime fromMap(Map<String, String> values) =>
      runtime(ConfigProvider.fromMap(values));

  test('loads primitive values without changing raw strings', () async {
    final runner = fromMap({
      'text': '  raw text  ',
      'integer': ' +42 ',
      'number': ' 1.25e2 ',
      'yes': ' true ',
      'no': 'false',
      'empty': '',
    });

    expect(
      await runner.runOrThrow(Config.string('text').load()),
      '  raw text  ',
    );
    expect(await runner.runOrThrow(Config.string('empty').load()), '');
    expect(await runner.runOrThrow(Config.int('integer').load()), 42);
    expect(await runner.runOrThrow(Config.double('number').load()), 125.0);
    expect(await runner.runOrThrow(Config.bool('yes').load()), isTrue);
    expect(await runner.runOrThrow(Config.bool('no').load()), isFalse);
  });

  test(
    'primitive parsers reject invalid values without including them in errors',
    () async {
      for (final entry in <(String, Config<Object?>)>[
        ('0xFF', Config.int('key')),
        ('1.2', Config.int('key')),
        ('', Config.int('key')),
        ('NaN', Config.double('key')),
        ('Infinity', Config.double('key')),
        ('-Infinity', Config.double('key')),
        ('TRUE', Config.bool('key')),
        ('1', Config.bool('key')),
        ('private-token', Config.bool('key')),
      ]) {
        final runner = fromMap({'key': entry.$1});
        final error = _error(await runner.run(entry.$2.load()));
        expect(error.issues.single.kind, ConfigIssueKind.invalid);
        expect(error.issues.single.path, ['key']);
        if (entry.$1.isNotEmpty) {
          expect(error.toString(), isNot(contains(entry.$1)));
        }
      }
    },
  );

  test('loads and parses lazily on every execution', () async {
    var reads = 0;
    var parses = 0;
    final runner = runtime(
      _Provider((_) => Task.sync(() => Option.some('${++reads}'))),
    );
    final config = Config.parse('key', (value) {
      parses++;
      return Result<String, ConfigError>.success(value);
    });
    final task = config.load();
    expect(reads, 0);
    expect(parses, 0);

    expect(await runner.runOrThrow(task), '1');
    expect(await runner.runOrThrow(task), '2');
    expect(reads, 2);
    expect(parses, 2);
  });

  test('nested custom parser errors receive the full path once', () async {
    final runner = fromMap({'app.db.host': 'value'});
    final config = Config.parse<String>(
      'host',
      (_) => Result.failure(
        ConfigError.invalid('Expected a host.', path: ['detail']),
      ),
    ).nested('db').nested('app');

    final error = _error(await runner.run(config.load()));
    expect(error.issues.single.path, ['app', 'db', 'host', 'detail']);
  });

  test(
    'map and flatMap use static widened types and the enclosing path',
    () async {
      final runner = fromMap({'app.mode': 'local', 'app.local': '7'});
      final config = Config.string('mode')
          .flatMap((mode) => Config.int(mode))
          .nested('app');
      final Config<num> widened = config;

      expect(
        await runner.runOrThrow(widened.map((num value) => value + 0.5).load()),
        7.5,
      );
    },
  );

  test(
    'zip returns typed records and accumulates errors from both fields',
    () async {
      final runner = fromMap({'port': '80', 'host': 'local', 'bad': 'no'});
      final Config<(int, String)> config = Config.int('port')
          .zip(Config.string('host'));
      expect(await runner.runOrThrow(config.load()), (80, 'local'));

      final error = _error(
        await runner.run(
          Config.int('bad').zip(Config.string('missing')).load(),
        ),
      );
      expect(error.issues.map((issue) => issue.path), [
        ['bad'],
        ['missing'],
      ]);
      expect(error.issues.map((issue) => issue.kind), [
        ConfigIssueKind.invalid,
        ConfigIssueKind.missing,
      ]);
    },
  );

  test(
    'all snapshots descriptions and repeats collection independently',
    () async {
      final runner = fromMap({'one': '1', 'two': '2'});
      final inputs = [Config.int('one'), Config.int('two')];
      final config = Config.all(inputs);
      inputs.clear();

      final first = await runner.runOrThrow(config.load());
      final second = await runner.runOrThrow(config.load());
      expect(first, [1, 2]);
      expect(second, [1, 2]);
      expect(first, isNot(same(second)));
      expect(() => first.add(3), throwsUnsupportedError);
      expect(await runner.runOrThrow(Config.all<int>([]).load()), isEmpty);
    },
  );

  test('all accumulates field problems in declaration order', () async {
    final runner = fromMap({'app.bad': 'wrong', 'app.good': '7'});
    final config = Config.all([
      Config.int('missing'),
      Config.int('bad'),
      Config.int('good'),
    ]).nested('app');

    final error = _error(await runner.run(config.load()));
    expect(error.issues.map((issue) => issue.path), [
      ['app', 'missing'],
      ['app', 'bad'],
    ]);
  });

  test('optional and default only recover missing values', () async {
    final runner = fromMap({'bad': 'wrong', 'empty': ''});
    expect(
      await runner.runOrThrow(Config.int('missing').withDefault(7).load()),
      7,
    );
    expect(
      await runner.runOrThrow(Config.int('missing').optional().load()),
      isA<OptionNone<int>>(),
    );
    expect(
      _error(await runner.run(Config.int('bad').withDefault(7).load()))
          .issues
          .single
          .kind,
      ConfigIssueKind.invalid,
    );
    expect(
      _error(await runner.run(Config.int('bad').optional().load()))
          .issues
          .single
          .kind,
      ConfigIssueKind.invalid,
    );
    expect(
      _some(await runner.runOrThrow(Config.string('empty').optional().load())),
      '',
    );

    final nullable = Config.parse<String?>(
      'empty',
      (_) => const Result.success(null),
    );
    expect(_some(await runner.runOrThrow(nullable.optional().load())), isNull);
    expect(
      await runner.runOrThrow(
        Config.int('a').zip(Config.int('b')).optional().load(),
      ),
      isA<OptionNone<(int, int)>>(),
    );
    expect(
      _error(
        await runner.run(
          Config.int('a').zip(Config.int('bad')).optional().load(),
        ),
      ).issues,
      hasLength(2),
    );
  });

  test(
    'source failures retain their paths and do not activate defaults',
    () async {
      final source = StateError('private token');
      final runner = runtime(
        _Provider(
          (path) => Task.fail(
            ConfigError.source(source, StackTrace.current, path: path),
          ),
        ),
      );
      final config = Config.string('key').nested('app').withDefault('fallback');

      final error = _error(await runner.run(config.load()));
      expect(error.issues.single.kind, ConfigIssueKind.source);
      expect(error.issues.single.path, ['app', 'key']);
      expect(error.issues.single.cause, same(source));
      expect(error.toString(), isNot(contains('private token')));
    },
  );

  test(
    'lists support empty input, nullable values, and explicit trimming',
    () async {
      final runner = fromMap({'empty': '', 'parts': ' a | | b '});
      Result<String?, ConfigError> parse(String value) =>
          Result.success(value.isEmpty ? null : value);
      final list = Config.list('parts', parse: parse, separator: '|');

      final values = await runner.runOrThrow(list.load());
      expect(values, ['a', null, 'b']);
      expect(() => values.clear(), throwsUnsupportedError);
      expect(
        await runner.runOrThrow(Config.list('empty', parse: parse).load()),
        isEmpty,
      );
      expect(
        await runner.runOrThrow(
          Config.list(
            'parts',
            parse: parse,
            separator: '|',
            trim: false,
          ).load(),
        ),
        [' a ', ' ', ' b '],
      );
    },
  );

  test(
    'list errors accumulate indexed paths including empty elements',
    () async {
      final runner = fromMap({'app.ports': '1,,bad,4'});
      final config = Config.list('ports', parse: _parseInt).nested('app');

      final error = _error(await runner.run(config.load()));
      expect(error.issues.map((issue) => issue.path), [
        ['app', 'ports', '1'],
        ['app', 'ports', '2'],
      ]);
    },
  );

  test('empty list separators fail before provider access', () async {
    var reads = 0;
    final runner = runtime(
      _Provider(
        (_) => Task.sync(() {
          reads++;
          return const Option.none();
        }),
      ),
    );
    final config = Config.list('missing', parse: _parseInt, separator: '');

    expect(
      _cause(await runner.run(config.load())),
      isA<CauseDefect>().having(
        (cause) => cause.error,
        'error',
        isA<ArgumentError>(),
      ),
    );
    expect(reads, 0);
  });

  test(
    'redacted hides formatting while explicitly retaining the loaded value',
    () async {
      final runner = fromMap({'token': 'private-token'});
      final value = await runner.runOrThrow(
        Config.string('token').redacted().load(),
      );

      expect(value.toString(), 'Redacted(<redacted>)');
      expect(value.reveal(), 'private-token');
    },
  );

  test(
    'parser defects stop all without turning into configuration errors',
    () async {
      final reads = <String>[];
      final defect = StateError('parser bug');
      final runner = runtime(
        _Provider(
          (path) => Task.sync(() {
            reads.add(path.single);
            return const Option.some('value');
          }),
        ),
      );
      final config = Config.all([
        Config.parse<String>('first', (_) => throw defect),
        Config.string('later'),
      ]);

      expect(
        _cause(await runner.run(config.load())),
        isA<CauseDefect>().having(
          (cause) => cause.error,
          'error',
          same(defect),
        ),
      );
      expect(reads, ['first']);
    },
  );

  test(
    'all preserves mixed causes and accumulates pure business groups',
    () async {
      final missing = ConfigError.missing(path: ['first']);
      final invalid = ConfigError.invalid('Invalid field.', path: ['second']);
      final defect = StateError('provider bug');
      for (final mixed in [false, true]) {
        var reads = 0;
        final runner = runtime(
          _Provider((_) {
            reads++;
            return Task.failCause(
              Cause.combine([
                Cause.failure(missing),
                Cause.failure(invalid),
                if (mixed) Cause.defect(defect, StackTrace.current),
              ]),
            );
          }),
        );
        final exit = await runner.run(
          Config.all([Config.string('a'), Config.string('b')]).load(),
        );
        if (mixed) {
          final cause = _cause(exit);
          expect(cause, isA<CauseGroup<ConfigError>>());
          expect(
            (cause as CauseGroup<ConfigError>).causes.last,
            isA<CauseDefect>(),
          );
          expect(reads, 1);
        } else {
          expect(_error(exit).issues, hasLength(4));
          expect(reads, 2);
        }
      }
    },
  );

  test(
    'cancelling a provider read stops collection and cleans up its wait',
    () async {
      final entered = Completer<void>();
      var cancelled = 0;
      var reads = 0;
      final controller = CancellationController();
      final runner = runtime(
        _Provider(
          (_) => Task.fromCallback((_) {
            reads++;
            entered.complete();
            return () {
              cancelled++;
            };
          }),
        ),
      );
      final running = runner.run(
        Config.all([Config.string('one'), Config.string('two')]).load(),
        signal: controller.signal,
      );
      await entered.future;

      controller.requestCancel();

      expect(_cause(await running), isA<CauseCancelled>());
      expect(cancelled, 1);
      expect(reads, 1);
    },
  );
}

final class _Provider implements ConfigProvider {
  _Provider(this.readValue);
  final Task<Option<String>, ConfigError> Function(List<String>) readValue;

  @override
  Task<Option<String>, ConfigError> read(List<String> path) => readValue(path);
}

Result<int, ConfigError> _parseInt(String value) {
  final parsed = int.tryParse(value);
  return parsed == null
      ? Result.failure(ConfigError.invalid('Expected an integer.'))
      : Result.success(parsed);
}

ConfigError _error<T>(Exit<T, ConfigError> exit) {
  final cause = _cause(exit);
  expect(cause, isA<CauseFailure<ConfigError>>());
  return (cause as CauseFailure<ConfigError>).error;
}

T _some<T>(Option<T> option) => switch (option) {
  OptionSome(:final value) => value,
  OptionNone() => fail('Expected a present value.'),
};

Cause<E> _cause<T, E>(Exit<T, E> exit) => switch (exit) {
  ExitSuccess(:final value) => fail('Expected failure, got $value.'),
  ExitFailure(:final cause) => cause,
};
