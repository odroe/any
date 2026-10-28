import 'dart:async';

import 'package:any/any.dart';
import 'package:any/testing.dart';
import 'package:test/test.dart';

void main() {
  late TestClock clock;
  late Runtime runtime;
  final reports = <Cause<Object?>>[];
  setUp(() {
    clock = TestClock();
    reports.clear();
    runtime = Runtime(
      context: Context.from([Clock.key.bind(clock)]),
      onUnhandledError: reports.add,
    );
  });
  tearDown(() async {
    await runtime.close();
    await _turn();
    expect(reports, isEmpty);
    expect(clock.pendingCount, 0);
  });

  test(
    'retry is lazy and reopens production with a fresh budget per consumption',
    () async {
      var calls = 0;
      final flow = Flow.fromTask(
        Task.defer<int, String>(() {
          calls++;
          return calls.isOdd ? Task.fail('retry') : Task.succeed(calls);
        }),
      ).retry(Schedule.recurs(1));
      expect(calls, 0);
      expect(_value(await runtime.run(flow.toList())), [2]);
      expect(_value(await runtime.run(flow.toList())), [4]);
    },
  );

  test(
    'already delivered values can repeat and do not reset the budget',
    () async {
      var calls = 0;
      final values = <int>[];
      final flow = Flow.fromTask(Task.sync(() => ++calls))
          .withError<String>()
          .concat(Flow.fromTask(Task.fail('again')))
          .retry(Schedule.recurs(2));
      final exit = await runtime.run(
        flow.forEach((v) => Task.sync(() => values.add(v))),
      );
      expect((_cause(exit) as CauseFailure<String>).error, 'again');
      expect(values, [1, 2, 3]);
      expect(calls, 3);
    },
  );

  test('normal completion and consumer failure do not resubscribe', () async {
    var calls = 0;
    final flow = Flow.fromTask(Task.sync(() => ++calls))
        .withError<String>()
        .retry(Schedule.recurs(10));
    expect(_value(await runtime.run(flow.toList())), [1]);
    expect(
      (_cause(
        await runtime.run(flow.forEach((_) => Task.fail('consumer'))),
      ) as CauseFailure<String>).error,
      'consumer',
    );
    expect(calls, 2);
  });

  test('predicate rejects retry without closing ambient resources', () async {
    final log = <String>[];
    final flow =
        Flow.fromTask(
          Task.acquireRelease(
            Task.sync(() {
              log.add('acquire');
              return 1;
            }),
            release: (_, _) => Task.sync(() => log.add('release')),
          ).withError<String>().flatMap((_) => Task.fail('stop')),
        ).retry(
          Schedule.recurs(10),
          when: (error) {
            log.add(error);
            return false;
          },
        );
    expect(await runtime.run(flow.toList()), isA<ExitFailure>());
    expect(log, ['acquire', 'stop', 'release']);
  });

  test('explicit local cleanup failure prevents retry', () async {
    final defect = StateError('release');
    var predicates = 0;
    final flow =
        Flow.fromTask(
          Task.acquireRelease(
            Task.succeed(1),
            release: (_, _) => Task.sync(() => throw defect),
          ).withError<String>().flatMap((_) => Task.fail('body')).scoped(),
        ).retry(
          Schedule.recurs(1),
          when: (_) {
            predicates++;
            return true;
          },
        );
    final causes = _causes(_cause(await runtime.run(flow.toList())));
    expect(causes, hasLength(2));
    expect((causes.first as CauseFailure<String>).error, 'body');
    expect((causes.last as CauseDefect).error, same(defect));
    expect(predicates, 0);
  });

  test('defect-containing and cancellation-only causes bypass retry', () async {
    var predicates = 0;
    for (final cause in <Cause<String>>[
      Cause.combine([
        const Cause.failure('a'),
        const Cause.failure('b'),
        Cause.defect(StateError('grouped defect'), StackTrace.current),
      ]),
      Cause.defect(StateError('defect'), StackTrace.current),
      const Cause.cancelled('source'),
    ]) {
      final flow = Flow.fromTask(Task.failCause(cause)).retry(
        Schedule.recurs(1),
        when: (_) {
          predicates++;
          return true;
        },
      );
      expect(
        _causes(_cause(await runtime.run(flow.toList()))).map(_reason),
        _causes(cause).map(_reason),
      );
    }
    expect(predicates, 0);
  });

  test(
    'retry predicates select the first grouped error including null',
    () async {
      for (final first in <String?>['first', null]) {
        var attempts = 0;
        final seen = <String?>[];
        final flow =
            Flow.fromTask(
              Task.defer<int, String?>(() {
                if (++attempts == 2) return Task.succeed(42);
                return Task.failCause(
                  Cause<String?>.combine([
                    const Cause.cancelled('recorded'),
                    Cause.failure(first),
                    const Cause.failure('later'),
                  ]),
                );
              }),
            ).retry(
              Schedule.recurs(1),
              when: (error) {
                seen.add(error);
                return true;
              },
            );

        expect(_value(await runtime.run(flow.toList())), [42]);
        expect(seen, [first]);
        expect(attempts, 2);
      }
    },
  );

  test(
    'retry exhaustion returns the last attempt\'s first business error',
    () async {
      var attempts = 0;
      final flow = Flow.fromTask(
        Task.defer<int, String>(() {
          return Task.failCause(
            Cause.combine([
              Cause.failure('attempt ${++attempts}'),
              const Cause.failure('later'),
            ]),
          );
        }),
      ).retry(Schedule.recurs(2));

      final cause =
          _cause(await runtime.run(flow.toList())) as CauseFailure<String>;
      expect(cause.error, 'attempt 3');
      expect(attempts, 3);
    },
  );

  test(
    'resources retained by grouping survive failed producer replacement',
    () async {
      var calls = 0;
      final live = <int>{};
      final flow =
          Flow.fromTask(
                Task.acquireRelease(
                  Task.sync(() {
                    final value = ++calls;
                    live.add(value);
                    return value;
                  }),
                  release: (value, _) => Task.sync(() => live.remove(value)),
                ),
              )
              .withError<String>()
              .concat(Flow.fromTask(Task.fail('again')))
              .retry(Schedule.recurs(2))
              .take(3)
              .grouped(3);
      expect(
        await runtime.run(
          flow.forEach(
            (values) => Task.sync(() {
              expect(values, [1, 2, 3]);
              expect(live, {1, 2, 3});
            }),
          ),
        ),
        isA<ExitSuccess>(),
      );
      expect(live, isEmpty);
    },
  );

  test('cleanup deferred by retained elements is still reported', () async {
    var calls = 0;
    final defect = StateError('late release');
    final flow =
        Flow.fromTask(
              Task.acquireRelease(
                Task.sync(() => ++calls),
                release: (value, _) => value == 1
                    ? Task.sync(() => throw defect)
                    : Task.succeed(null),
              ),
            )
            .withError<String>()
            .concat(Flow.fromTask(Task.fail('again')))
            .retry(Schedule.recurs(2))
            .take(2)
            .grouped(2);
    final exit = await runtime.run(flow.drain());
    expect((_cause(exit) as CauseDefect).error, same(defect));
    expect(calls, 2);
  });

  test('retry delays use the clock and can be cancelled', () async {
    var calls = 0;
    final controller = CancellationController();
    final flow = Flow.fromTask(
      Task.defer<int, String>(() {
        calls++;
        return Task.fail('again');
      }),
    ).retry(Schedule.spaced(const Duration(seconds: 10)));
    final running = runtime.run(flow.toList(), signal: controller.signal);
    await _until(() => clock.pendingCount == 1);
    expect(calls, 1);
    clock.advance(const Duration(seconds: 10));
    await _until(() => calls == 2 && clock.pendingCount == 1);
    controller.requestCancel(reason: 'owner');
    expect((_cause(await running) as CauseCancelled).reason, 'owner');
  });

  test('zero delay remains stack safe over ten thousand retries', () async {
    var calls = 0;
    final flow = Flow.fromTask(
      Task.defer<int, String>(() {
        calls++;
        return calls <= 10000 ? Task.fail('again') : Task.succeed(calls);
      }),
    ).retry(Schedule.recurs(10000));
    expect(_value(await runtime.run(flow.toList())), [10001]);
    expect(clock.pendingCount, 0);
  });

  test('elapsed policy includes production and consumer time', () async {
    var calls = 0;
    final flow = Flow.fromTask(Task.sync(() => ++calls))
        .withError<String>()
        .concat(Flow.fromTask(Task.fail('again')))
        .retry(Schedule.recurs(10).whileElapsed(const Duration(seconds: 5)));
    final exit = await runtime.run(
      flow.forEach(
        (_) => Task.sync(() {
          clock.advance(const Duration(seconds: 5));
        }),
      ),
    );
    expect((_cause(exit) as CauseFailure<String>).error, 'again');
    expect(calls, 1);
  });

  test(
    'predicate exceptions become defects after resources are released',
    () async {
      var released = false;
      final defect = StateError('predicate');
      final flow = Flow.fromTask(
        Task.acquireRelease(
          Task.succeed(1),
          release: (_, _) => Task.sync(() => released = true),
        ).withError<String>().flatMap((_) => Task.fail('again')),
      ).retry(Schedule.recurs(1), when: (_) => throw defect);
      expect(
        (_cause(await runtime.run(flow.toList())) as CauseDefect).error,
        same(defect),
      );
      expect(released, isTrue);
    },
  );
}

T _value<T, E>(Exit<T, E> exit) => (exit as ExitSuccess<T, E>).value;
Cause<E> _cause<T, E>(Exit<T, E> exit) => (exit as ExitFailure<T, E>).cause;
List<Cause<E>> _causes<E>(Cause<E> cause) => switch (cause) {
  CauseGroup(:final causes) => causes,
  _ => [cause],
};
Object? _reason(Cause<Object?> cause) => switch (cause) {
  CauseFailure(:final error) => ('failure', error),
  CauseDefect(:final error, :final stackTrace) => ('defect', error, stackTrace),
  CauseCancelled(:final reason) => ('cancelled', reason),
  CauseGroup() => throw StateError('Expected a flat cause.'),
};
Future<void> _turn() => Future<void>.delayed(Duration.zero);
Future<void> _until(bool Function() ready) async {
  for (var i = 0; i < 100; i++) {
    if (ready()) return;
    await _turn();
  }
  fail('Expected state was not reached.');
}
