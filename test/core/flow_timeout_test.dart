import 'dart:async';

import 'package:any/any.dart';
import 'package:any/testing.dart';
import 'package:test/test.dart';

void main() {
  late TestClock clock;
  late Runtime runtime;
  final reports = <Cause<Object?>>[];
  const timeout = Duration(seconds: 1);
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
    'deadlines are lazy, per consumption, and cancelled after each pull',
    () async {
      var calls = 0;
      final flow = Flow.fromTask(Task.sync(() => ++calls))
          .withError<String>()
          .timeout(timeout, onTimeout: () => 'timeout');
      expect(calls, 0);
      expect(clock.pendingCount, 0);
      expect(_value(await runtime.run(flow.toList())), [1]);
      expect(_value(await runtime.run(flow.toList())), [2]);
    },
  );

  test('time spent in the consumer is not part of the next deadline', () async {
    final reached = Completer<void>();
    final finish = Completer<void>();
    final values = <int>[];
    final running = runtime.run(
      Flow.fromIterable([1, 2])
          .withError<String>()
          .timeout(timeout, onTimeout: () => 'timeout')
          .forEach(
            (value) => Task.fromFuture(() async {
              values.add(value);
              if (value == 1) {
                reached.complete();
                await finish.future;
              }
            }, onError: _defect),
          ),
    );
    await reached.future;
    expect(clock.pendingCount, 0);
    clock.advance(const Duration(days: 1));
    finish.complete();
    expect(await running, isA<ExitSuccess>());
    expect(values, [1, 2]);
  });

  test(
    'a blocked first pull times out and awaits callback cancellation',
    () async {
      final started = Completer<void>();
      final release = Completer<void>();
      final events = <String>[];
      final source = Task.fromCallback<int, String>((_) {
        started.complete();
        return () async {
          events.add('cancel');
          await release.future;
          events.add('released');
        };
      });
      var done = false;
      final running = runtime.run(
        Flow.fromTask(source)
            .timeout(
              timeout,
              onTimeout: () {
                events.add('timeout');
                return 'timeout';
              },
            )
            .toList(),
      )..then((_) => done = true);
      await started.future;
      clock.advance(timeout);
      await _until(() => events.isNotEmpty);
      expect(done, isFalse);
      release.complete();
      expect(_cause(await running), isA<CauseFailure<String>>());
      expect(events, ['cancel', 'released', 'timeout']);
    },
  );

  test('each next pull receives a fresh deadline', () async {
    final reached = Completer<void>();
    final values = <int>[];
    final flow = Flow.fromIterable([1])
        .withError<String>()
        .concat(
          Flow.fromTask(
            Task.fromCallback<int, String>((_) {
              reached.complete();
              return () {};
            }),
          ),
        )
        .timeout(timeout, onTimeout: () => 'timeout');
    final running = runtime.run(
      flow.forEach((v) => Task.sync(() => values.add(v))),
    );
    await reached.future;
    clock.advance(timeout);
    expect((_cause(await running) as CauseFailure<String>).error, 'timeout');
    expect(values, [1]);
  });

  test(
    'resources survive deadline fibers and downstream retained values',
    () async {
      final live = <int>{};
      final flow = Flow.fromIterable([1, 2, 3])
          .mapTask(
            (value) => Task.acquireRelease(
              Task.sync(() {
                live.add(value);
                return value;
              }),
              release: (_, _) => Task.sync(() => live.remove(value)),
            ),
          )
          .withError<String>()
          .timeout(timeout, onTimeout: () => 'timeout')
          .grouped(3);
      expect(
        await runtime.run(
          flow.forEach(
            (values) => Task.sync(() {
              expect(live, containsAll(values));
            }),
          ),
        ),
        isA<ExitSuccess>(),
      );
      expect(live, isEmpty);
    },
  );

  test(
    'timeout keeps cleanup defects and supports ordinary recovery',
    () async {
      final entered = Completer<void>();
      final defect = StateError('release');
      final source = Flow.fromTask(
        Task.acquireRelease(
          Task.succeed(1),
          release: (_, _) => Task.sync(() => throw defect),
        ).withError<String>().flatMap(
          (_) => Task.fromCallback<int, String>((_) {
            entered.complete();
            return () {};
          }),
        ),
      );
      final running = runtime.run(
        source
            .timeout(timeout, onTimeout: () => 'timeout')
            .recover((_) => Flow.fromIterable([9]))
            .toList(),
      );
      await entered.future;
      clock.advance(timeout);
      final causes = _causes(_cause(await running));
      expect(causes.whereType<CauseFailure>().map((c) => c.error), ['timeout']);
      expect(causes.whereType<CauseDefect>().map((c) => c.error), [
        same(defect),
      ]);
    },
  );

  test('owner cancellation keeps its reason and removes the timer', () async {
    final entered = Completer<void>();
    final controller = CancellationController();
    var mapped = false;
    final running = runtime.run(
      Flow.fromTask(
            Task.fromCallback<int, String>((_) {
              entered.complete();
              return () {};
            }),
          )
          .timeout(
            timeout,
            onTimeout: () {
              mapped = true;
              return 'timeout';
            },
          )
          .toList(),
      signal: controller.signal,
    );
    await entered.future;
    controller.requestCancel(reason: 'owner');
    expect((_cause(await running) as CauseCancelled).reason, 'owner');
    expect(mapped, isFalse);
  });

  test(
    'source errors and callback exceptions retain their failure kinds',
    () async {
      final failure = await runtime.run(
        Flow.fromTask(Task.fail('source'))
            .timeout(timeout, onTimeout: () => 'timeout')
            .toList(),
      );
      expect((_cause(failure) as CauseFailure<String>).error, 'source');
      final defect = StateError('source');
      final thrown = await runtime.run(
        Flow.fromTask(Task.sync<int>(() => throw defect))
            .withError<String>()
            .timeout(timeout, onTimeout: () => 'timeout')
            .toList(),
      );
      expect((_cause(thrown) as CauseDefect).error, same(defect));
    },
  );

  test('an uncancellable future is awaited after expiration', () async {
    final entered = Completer<void>();
    final pending = Completer<int>();
    var done = false;
    final running = runtime.run(
      Flow.fromTask(
        Task.fromFuture(() {
          entered.complete();
          return pending.future;
        }, onError: (error, _) => error.toString()),
      ).timeout(timeout, onTimeout: () => 'timeout').toList(),
    )..then((_) => done = true);
    await entered.future;
    clock.advance(timeout);
    await _turn();
    expect(done, isFalse);
    pending.complete(1);
    expect((_cause(await running) as CauseFailure<String>).error, 'timeout');
  });
}

T _value<T, E>(Exit<T, E> exit) => (exit as ExitSuccess<T, E>).value;
Cause<E> _cause<T, E>(Exit<T, E> exit) => (exit as ExitFailure<T, E>).cause;
List<Cause<E>> _causes<E>(Cause<E> cause) => switch (cause) {
  CauseGroup(:final causes) => causes,
  _ => [cause],
};
Never _defect(Object error, StackTrace stackTrace) =>
    Error.throwWithStackTrace(error, stackTrace);
Future<void> _turn() => Future<void>.delayed(Duration.zero);
Future<void> _until(bool Function() ready) async {
  for (var i = 0; i < 100; i++) {
    if (ready()) return;
    await _turn();
  }
  fail('Expected state was not reached.');
}
