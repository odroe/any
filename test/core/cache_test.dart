import 'dart:async';

import 'package:any/any.dart';
import 'package:any/testing.dart';
import 'package:test/test.dart';

void main() {
  late Runtime runtime;
  late TestClock clock;
  late List<Cause<Object?>> reports;

  setUp(() {
    clock = TestClock();
    reports = [];
    runtime = Runtime(
      context: Context.from([Clock.key.bind(clock)]),
      onUnhandledError: reports.add,
    );
  });
  tearDown(() async {
    await runtime.close();
  });

  const ttl = Duration(seconds: 10);

  test(
    'cache creation and operations are lazy; successes and null are retained',
    () async {
      var calls = 0;
      final cache = await _cache<String, int?, String>(
        runtime,
        capacity: 2,
        ttl: ttl,
        load: (key) => Task.sync(() {
          calls++;
          return key == 'null' ? null : 1;
        }),
      );
      final get = cache.get('value');
      expect(calls, 0);
      expect(
        _value(await runtime.run(cache.getOption('value'))),
        isA<OptionNone<int?>>(),
      );
      expect(_value(await runtime.run(get)), 1);
      expect(_value(await runtime.run(get)), 1);
      expect(_value(await runtime.run(cache.get('null'))), isNull);
      expect(
        (_value(
          await runtime.run(cache.getOption('null')),
        ) as OptionSome<int?>).value,
        isNull,
      );
      expect(calls, 2);
    },
  );

  test(
    'same-key callers share one load and getOption never waits for it',
    () async {
      var calls = 0;
      final gate = _Gate<int, String>();
      final cache = await _cache<String, int, String>(
        runtime,
        capacity: 1,
        ttl: ttl,
        load: (_) {
          calls++;
          return gate.task;
        },
      );
      final first = runtime.run(cache.get('a'));
      final second = runtime.run(cache.get('a'));
      expect(calls, 1);
      expect(
        _value(await runtime.run(cache.getOption('a'))),
        isA<OptionNone<int>>(),
      );
      gate.succeed(42);
      expect(_value(await first), 42);
      expect(_value(await second), 42);
      expect(calls, 1);
    },
  );

  test('TTL begins after the complete loader including cleanup', () async {
    var calls = 0;
    final gate = _Gate<int, String>();
    final release = Completer<void>();
    final entered = Completer<void>();
    addTearDown(() {
      if (!release.isCompleted) release.complete();
    });
    final cache = await _cache<String, int, String>(
      runtime,
      capacity: 1,
      ttl: ttl,
      load: (_) {
        calls++;
        if (calls > 1) return Task.succeed(calls);
        return gate.task.ensuring(
          Task.defer(() {
            entered.complete();
            return _future(release.future);
          }),
        );
      },
    );
    final first = runtime.run(cache.get('a'));
    gate.succeed(1);
    await entered.future;
    clock.advance(const Duration(seconds: 100));
    release.complete();
    expect(_value(await first), 1);
    clock.advance(const Duration(seconds: 9));
    expect(_value(await runtime.run(cache.get('a'))), 1);
    clock.advance(const Duration(seconds: 1));
    expect(
      _value(await runtime.run(cache.getOption('a'))),
      isA<OptionNone<int>>(),
    );
    expect(_value(await runtime.run(cache.get('a'))), 2);
  });

  test('zero TTL shares pending work without retaining its result', () async {
    final gates = [_Gate<int, Never>(), _Gate<int, Never>()];
    var calls = 0;
    final cache = await _cache<String, int, Never>(
      runtime,
      capacity: 1,
      ttl: Duration.zero,
      load: (_) => gates[calls++].task,
    );
    final first = runtime.run(cache.get('a'));
    final second = runtime.run(cache.get('a'));
    gates[0].succeed(1);
    expect(_value(await first), 1);
    expect(_value(await second), 1);
    expect(
      _value(await runtime.run(cache.getOption('a'))),
      isA<OptionNone<int>>(),
    );
    final third = runtime.run(cache.get('a'));
    expect(calls, 2);
    gates[1].succeed(2);
    expect(_value(await third), 2);
  });

  test(
    'completed entries use LRU eviction and getOption touches recency',
    () async {
      final loads = <String>[];
      final cache = await _cache<String, String, Never>(
        runtime,
        capacity: 2,
        ttl: ttl,
        load: (key) => Task.sync(() {
          loads.add(key);
          return key;
        }),
      );
      await runtime.run(cache.get('a'));
      await runtime.run(cache.get('b'));
      await runtime.run(cache.getOption('a'));
      await runtime.run(cache.get('c'));
      expect(
        _value(await runtime.run(cache.getOption('b'))),
        isA<OptionNone<String>>(),
      );
      expect(_value(await runtime.run(cache.get('a'))), 'a');
      expect(loads, ['a', 'b', 'c']);
    },
  );

  test(
    'expired entries are evicted before live least-recently-used entries',
    () async {
      final loads = <String>[];
      final cache = await _cache<String, String, Never>(
        runtime,
        capacity: 2,
        ttl: ttl,
        load: (key) => Task.sync(() {
          loads.add(key);
          return key;
        }),
      );
      await runtime.run(cache.get('old'));
      clock.advance(const Duration(seconds: 5));
      await runtime.run(cache.get('live'));
      await runtime.run(cache.get('old'));
      clock.advance(const Duration(seconds: 5));
      await runtime.run(cache.get('new'));
      expect(
        (_value(
          await runtime.run(cache.getOption('live')),
        ) as OptionSome<String>).value,
        'live',
      );
      expect(
        _value(await runtime.run(cache.getOption('old'))),
        isA<OptionNone<String>>(),
      );
      expect(loads, ['old', 'live', 'new']);
    },
  );

  test(
    'a thrown loader callback releases its slot and is never cached',
    () async {
      final error = StateError('load');
      var calls = 0;
      final cache = await _cache<String, int, Never>(
        runtime,
        capacity: 1,
        ttl: ttl,
        load: (_) {
          if (++calls == 1) throw error;
          return Task.succeed(2);
        },
      );
      expect(
        (_cause(await runtime.run(cache.get('a'))) as CauseDefect).error,
        same(error),
      );
      expect(_value(await runtime.run(cache.get('a'))), 2);
      expect(calls, 2);
    },
  );

  test('default failures reload; explicit cached failures propagate through getOption', () async {
    for (final cacheFailures in [false, true]) {
      var calls = 0;
      final cache = await _cache<String, int, String?>(
        runtime,
        capacity: 1,
        ttl: ttl,
        cacheFailures: cacheFailures,
        load: (_) => Task.defer(() {
          calls++;
          return Task.fail(null);
        }),
      );
      expect(
        (_cause(
          await runtime.run(cache.get('a')),
        ) as CauseFailure<String?>).error,
        isNull,
      );
      final option = await runtime.run(cache.getOption('a'));
      if (cacheFailures) {
        expect((_cause(option) as CauseFailure<String?>).error, isNull);
      } else {
        expect(_value(option), isA<OptionNone<int>>());
      }
      await runtime.run(cache.get('a'));
      expect(calls, cacheFailures ? 1 : 2);
    }
  });

  test(
    'only pure business-failure groups are eligible for failure caching',
    () async {
      final defect = Cause<String>.defect(Object(), StackTrace.current);
      final causes = <Cause<String>>[
        Cause.combine([const Cause.failure('a'), const Cause.failure('b')]),
        Cause.combine([const Cause.failure('a'), defect]),
        const Cause.cancelled('loader'),
        defect,
      ];
      for (var i = 0; i < causes.length; i++) {
        var calls = 0;
        final cache = await _cache<int, int, String>(
          runtime,
          capacity: 1,
          ttl: ttl,
          cacheFailures: true,
          load: (_) => Task.defer(() {
            calls++;
            return Task.failCause(causes[i]);
          }),
        );
        await runtime.run(cache.get(1));
        await runtime.run(cache.get(1));
        expect(calls, i == 0 ? 1 : 2);
      }
      await _turn();
      expect(reports, isEmpty);
    },
  );

  test(
    'capacity bounds pending loads and admits new keys in FIFO order',
    () async {
      final gates = [for (var i = 0; i < 4; i++) _Gate<int, Never>()];
      final started = <int>[];
      final cache = await _cache<int, int, Never>(
        runtime,
        capacity: 2,
        ttl: ttl,
        load: (key) {
          started.add(key);
          return gates[key].task;
        },
      );
      final runs = [for (var i = 0; i < 4; i++) runtime.run(cache.get(i))];
      expect(started, [0, 1]);
      gates[1].succeed(11);
      expect(_value(await runs[1]), 11);
      await _turn();
      expect(started, [0, 1, 2]);
      gates[0].succeed(10);
      expect(_value(await runs[0]), 10);
      await _turn();
      expect(started, [0, 1, 2, 3]);
      gates[2].succeed(12);
      gates[3].succeed(13);
      expect(_value(await runs[2]), 12);
      expect(_value(await runs[3]), 13);
    },
  );

  test(
    'same-key sharing bypasses a capacity wait without evicting active work',
    () async {
      final a = _Gate<int, Never>();
      final b = _Gate<int, Never>();
      final started = <String>[];
      final cache = await _cache<String, int, Never>(
        runtime,
        capacity: 1,
        ttl: ttl,
        load: (key) {
          started.add(key);
          return key == 'a' ? a.task : b.task;
        },
      );
      final first = runtime.run(cache.get('a'));
      final waiting = runtime.run(cache.get('b'));
      final shared = runtime.run(cache.get('a'));
      expect(started, ['a']);
      a.succeed(1);
      expect(_value(await first), 1);
      expect(_value(await shared), 1);
      await _turn();
      expect(started, ['a', 'b']);
      b.succeed(2);
      expect(_value(await waiting), 2);
    },
  );

  test(
    'invalidated old completion cannot overwrite the new generation',
    () async {
      final gates = [_Gate<int, Never>(), _Gate<int, Never>()];
      var calls = 0;
      final cache = await _cache<String, int, Never>(
        runtime,
        capacity: 2,
        ttl: ttl,
        load: (_) => gates[calls++].task,
      );
      final old = runtime.run(cache.get('a'));
      await runtime.run(cache.invalidate('a'));
      final fresh = runtime.run(cache.get('a'));
      expect(calls, 2);
      gates[1].succeed(2);
      expect(_value(await fresh), 2);
      gates[0].succeed(1);
      expect(_value(await old), 1);
      expect(_value(await runtime.run(cache.get('a'))), 2);
      expect(calls, 2);
    },
  );

  test(
    'invalidated active generations still consume capacity until fully closed',
    () async {
      var calls = 0;
      final old = _Gate<int, Never>();
      final release = Completer<void>();
      final entered = Completer<void>();
      addTearDown(() {
        if (!release.isCompleted) release.complete();
      });
      final cache = await _cache<String, int, Never>(
        runtime,
        capacity: 1,
        ttl: ttl,
        load: (_) {
          calls++;
          return calls == 1
              ? old.task.ensuring(
                  Task.defer(() {
                    entered.complete();
                    return _future(release.future);
                  }),
                )
              : Task.succeed(2);
        },
      );
      final first = runtime.run(cache.get('a'));
      await runtime.run(cache.invalidate('a'));
      final fresh = runtime.run(cache.get('a'));
      expect(calls, 1);
      old.succeed(1);
      await entered.future;
      expect(calls, 1);
      release.complete();
      expect(_value(await first), 1);
      expect(_value(await fresh), 2);
      expect(calls, 2);
    },
  );

  test(
    'invalidateAll discards completed entries and retires pending ones',
    () async {
      final gate = _Gate<int, Never>();
      final loads = <String>[];
      final cache = await _cache<String, int, Never>(
        runtime,
        capacity: 2,
        ttl: ttl,
        load: (key) {
          loads.add(key);
          return key == 'pending' && loads.length == 2
              ? gate.task
              : Task.succeed(loads.length);
        },
      );
      await runtime.run(cache.get('ready'));
      final pending = runtime.run(cache.get('pending'));
      await runtime.run(cache.invalidateAll());
      expect(
        _value(await runtime.run(cache.getOption('ready'))),
        isA<OptionNone<int>>(),
      );
      gate.succeed(2);
      expect(_value(await pending), 2);
      expect(
        _value(await runtime.run(cache.getOption('pending'))),
        isA<OptionNone<int>>(),
      );
      expect(_value(await runtime.run(cache.get('pending'))), 3);
    },
  );

  test(
    'refresh invalidates first and shares its new load with later get',
    () async {
      var calls = 0;
      final gate = _Gate<int, Never>();
      final cache = await _cache<String, int, Never>(
        runtime,
        capacity: 1,
        ttl: ttl,
        load: (_) => ++calls == 1 ? Task.succeed(1) : gate.task,
      );
      expect(_value(await runtime.run(cache.get('a'))), 1);
      final refresh = runtime.run(cache.refresh('a'));
      expect(
        _value(await runtime.run(cache.getOption('a'))),
        isA<OptionNone<int>>(),
      );
      final get = runtime.run(cache.get('a'));
      expect(calls, 2);
      gate.succeed(2);
      expect(_value(await refresh), 2);
      expect(_value(await get), 2);
    },
  );

  test(
    'one cancelled waiter does not cancel another caller shared load',
    () async {
      final cancel = CancellationController();
      final gate = _Gate<int, Never>();
      final cache = await _cache<String, int, Never>(
        runtime,
        capacity: 1,
        ttl: ttl,
        load: (_) => gate.task,
      );
      final first = runtime.run(cache.get('a'), signal: cancel.signal);
      final second = runtime.run(cache.get('a'));
      cancel.requestCancel(reason: 'first');
      expect((_cause(await first) as CauseCancelled).reason, 'first');
      expect(gate.cancelled, isFalse);
      gate.succeed(1);
      expect(_value(await second), 1);
    },
  );

  test('last cancelled waiter waits for cleanup; next get waits for a new generation', () async {
    final cancel = CancellationController();
    final release = Completer<void>();
    addTearDown(() {
      if (!release.isCompleted) release.complete();
    });
    final error = StateError('cleanup');
    final gate = _Gate<int, String>(
      onCancel: () async {
        await release.future;
        throw error;
      },
    );
    var calls = 0;
    final cache = await _cache<String, int, String>(
      runtime,
      capacity: 1,
      ttl: ttl,
      load: (_) => ++calls == 1 ? gate.task : Task.succeed(2),
    );
    var done = false;
    final first = runtime.run(cache.get('a'), signal: cancel.signal).then((
      exit,
    ) {
      done = true;
      return exit;
    });
    cancel.requestCancel(reason: 'owner');
    await _turn();
    expect(gate.cancelled, isTrue);
    expect(done, isFalse);
    final second = runtime.run(cache.get('a'));
    expect(calls, 1);
    release.complete();
    final causes = _reasons(_cause(await first));
    expect(causes.whereType<CauseCancelled>().map((cause) => cause.reason), [
      'owner',
    ]);
    expect(causes.whereType<CauseDefect>().map((cause) => cause.error), [
      error,
    ]);
    expect(_value(await second), 2);
    expect(calls, 2);
  });

  test(
    'last waiter cancellation retains a native Future slot until it settles',
    () async {
      final cancel = CancellationController();
      final future = Completer<int>();
      addTearDown(() {
        if (!future.isCompleted) future.complete(1);
      });
      var calls = 0;
      final cache = await _cache<String, int, Never>(
        runtime,
        capacity: 1,
        ttl: ttl,
        load: (_) => ++calls == 1
            ? Task.fromFuture<int, Never>(
                () => future.future,
                onError: (error, stack) =>
                    Error.throwWithStackTrace(error, stack),
              )
            : Task.succeed(2),
      );
      final first = runtime.run(cache.get('a'), signal: cancel.signal);
      cancel.requestCancel();
      await _turn();
      final second = runtime.run(cache.get('a'));
      expect(calls, 1);
      future.complete(1);
      expect(_cause(await first), isA<CauseCancelled>());
      expect(_value(await second), 2);
    },
  );

  test(
    'cancelled capacity waiters never start loaders or consume released slots',
    () async {
      final cancel = CancellationController();
      final gate = _Gate<int, Never>();
      final loads = <String>[];
      final cache = await _cache<String, int, Never>(
        runtime,
        capacity: 1,
        ttl: ttl,
        load: (key) {
          loads.add(key);
          return key == 'a' ? gate.task : Task.succeed(3);
        },
      );
      final first = runtime.run(cache.get('a'));
      final cancelled = runtime.run(cache.get('b'), signal: cancel.signal);
      final next = runtime.run(cache.get('c'));
      cancel.requestCancel();
      expect(_cause(await cancelled), isA<CauseCancelled>());
      gate.succeed(1);
      expect(_value(await first), 1);
      expect(_value(await next), 3);
      expect(loads, ['a', 'c']);
    },
  );

  test(
    'loaders inherit the first starting request services and task locals',
    () async {
      final key = ServiceKey<String>('tenant');
      final local = TaskLocal(0);
      final gate = _Gate<void, Never>();
      var calls = 0;
      final cache = await _cache<String, String, Never>(
        runtime,
        capacity: 1,
        ttl: ttl,
        load: (_) {
          calls++;
          return Task.service(key).flatMap(
            (service) => local.get().flatMap(
              (value) => gate.task.map((_) => '$service:$value'),
            ),
          );
        },
      );
      final first = runtime.run(
        local
            .locally(1, cache.get('shared'))
            .provideContext(Context.from([key.bind('first')])),
      );
      final second = runtime.run(
        local
            .locally(2, cache.get('shared'))
            .provideContext(Context.from([key.bind('second')])),
      );
      gate.succeed(null);
      expect(_value(await first), 'first:1');
      expect(_value(await second), 'first:1');
      expect(calls, 1);
    },
  );

  test(
    'widened caches can read valid original keys without narrowing callbacks',
    () async {
      final narrow = await _cache<String, int, String>(
        runtime,
        capacity: 1,
        ttl: ttl,
        load: (_) => Task.succeed(1),
      );
      final Cache<Object?, Object?, Object?> wide = narrow;
      expect(_value(await runtime.run(wide.get('a'))), 1);
      expect(
        (_value(
          await runtime.run(wide.getOption('a')),
        ) as OptionSome<Object?>).value,
        1,
      );
      expect(_value(await runtime.run(wide.refresh('a'))), 1);
    },
  );

  test(
    'closing the owning scope stops loads and rejects subsequent operations',
    () async {
      final ready = Completer<Cache<String, int, Never>>();
      final gate = _Gate<int, Never>();
      final owner = runtime.start(
        Cache.create<String, int, Never>(
          capacity: 1,
          ttl: ttl,
          load: (_) => gate.task,
        ).flatMap(
          (cache) =>
              Task.sync(() => ready.complete(cache))
                  .flatMap((_) => _pending<void>()),
        ),
      );
      final cache = await ready.future;
      final get = runtime.run(cache.get('a'));
      await runtime.run(owner.cancel());
      expect(_cause(await get), isA<CauseCancelled>());
      expect(gate.cancelled, isTrue);
      for (final task in <Task<Object?, Never>>[
        cache.get('a'),
        cache.getOption('a'),
        cache.invalidate('a'),
        cache.invalidateAll(),
        cache.refresh('a'),
      ]) {
        expect(
          (_cause(await runtime.run(task)) as CauseDefect).error,
          isStateError,
        );
      }
    },
  );

  test('clock failures during completion reach all waiters and do not cache the result', () async {
    final faulty = _FaultyClock(clock);
    final gate = _Gate<int, Never>();
    var calls = 0;
    final cache = await _cache<String, int, Never>(
      runtime,
      capacity: 1,
      ttl: ttl,
      clock: faulty,
      load: (_) => ++calls == 1 ? gate.task : Task.succeed(2),
    );
    final first = runtime.run(cache.get('a'));
    final second = runtime.run(cache.get('a'));
    faulty.fail = true;
    gate.succeed(1);
    expect((_cause(await first) as CauseDefect).error, same(faulty.error));
    expect((_cause(await second) as CauseDefect).error, same(faulty.error));
    faulty.fail = false;
    expect(_value(await runtime.run(cache.get('a'))), 2);
    expect(calls, 2);
    await _turn();
    expect(reports, isEmpty);
  });

  test('clock lookup failures are request defects and leave the cached entry intact', () async {
    final faulty = _FaultyClock(clock);
    var calls = 0;
    final cache = await _cache<String, int, Never>(
      runtime,
      capacity: 1,
      ttl: ttl,
      clock: faulty,
      load: (_) => Task.sync(() => ++calls),
    );
    expect(_value(await runtime.run(cache.get('a'))), 1);
    faulty.fail = true;
    expect(
      (_cause(await runtime.run(cache.get('a'))) as CauseDefect).error,
      same(faulty.error),
    );
    expect(
      (_cause(await runtime.run(cache.getOption('a'))) as CauseDefect).error,
      same(faulty.error),
    );
    faulty.fail = false;
    expect(_value(await runtime.run(cache.get('a'))), 1);
    expect(calls, 1);
  });

  test(
    'shared defect sources are not duplicated by waiting and cleanup',
    () async {
      final error = StateError('load cleanup');
      final gate = _Gate<int, Never>();
      final cache = await _cache<String, int, Never>(
        runtime,
        capacity: 1,
        ttl: ttl,
        load: (_) => gate.task.ensuring(Task.sync(() => throw error)),
      );
      final running = runtime.run(
        Task.all([cache.get('a'), cache.get('a')], concurrency: 2),
      );
      gate.succeed(1);
      expect(
        _reasons(_cause(await running))
            .whereType<CauseDefect>()
            .map((cause) => cause.error),
        [error],
      );
    },
  );

  test('invalid construction fails before invoking the loader', () async {
    var calls = 0;
    for (final config in [
      (0, ttl),
      (-1, ttl),
      (1, const Duration(seconds: -1)),
    ]) {
      final result = await runtime.run(
        Cache.create<String, int, Never>(
          capacity: config.$1,
          ttl: config.$2,
          load: (_) {
            calls++;
            return Task.succeed(1);
          },
        ),
      );
      expect((_cause(result) as CauseDefect).error, isArgumentError);
    }
    expect(calls, 0);
  });
}

Future<Cache<K, V, E>> _cache<K, V, E>(
  Runtime runtime, {
  required int capacity,
  required Duration ttl,
  required Task<V, E> Function(K) load,
  bool cacheFailures = false,
  Clock? clock,
}) {
  final ready = Completer<Cache<K, V, E>>();
  var task =
      Cache.create<K, V, E>(
        capacity: capacity,
        ttl: ttl,
        load: load,
        cacheFailures: cacheFailures,
      ).flatMap(
        (cache) =>
            Task.sync(() => ready.complete(cache))
                .flatMap((_) => _pending<void>()),
      );
  if (clock != null) {
    task = task.provideContext(Context.from([Clock.key.bind(clock)]));
  }
  runtime.start(task);
  return ready.future;
}

final class _Gate<T, E> {
  _Gate({this.onCancel});
  final FutureOr<void> Function()? onCancel;
  late void Function(Exit<T, E>) complete;
  var cancelled = false;
  Task<T, E> get task => Task.fromCallback((complete) {
    this.complete = complete;
    return () {
      cancelled = true;
      return onCancel?.call();
    };
  });
  void succeed(T value) => complete(Exit.success(value));
}

final class _FaultyClock implements Clock {
  _FaultyClock(this.clock);
  final Clock clock;
  final error = StateError('clock');
  bool fail = false;
  @override
  Duration get elapsed {
    if (fail) throw error;
    return clock.elapsed;
  }

  @override
  DateTime now() => clock.now();
  @override
  void Function() schedule(Duration delay, void Function() callback) =>
      clock.schedule(delay, callback);
}

Task<T, Never> _pending<T>() => Task.fromCallback((_) => () {});
Task<void, Never> _future(Future<void> future) => Task.fromFuture<void, Never>(
  () => future,
  onError: (error, stackTrace) => Error.throwWithStackTrace(error, stackTrace),
);
T _value<T, E>(Exit<T, E> exit) => (exit as ExitSuccess<T, E>).value;
Cause<E> _cause<T, E>(Exit<T, E> exit) => (exit as ExitFailure<T, E>).cause;
List<Cause<E>> _reasons<E>(Cause<E> cause) => switch (cause) {
  CauseGroup(:final causes) => causes,
  _ => [cause],
};
Future<void> _turn() => Future<void>.delayed(Duration.zero);
