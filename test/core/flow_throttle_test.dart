import 'dart:async';

import 'package:any/any.dart';
import 'package:any/testing.dart';
import 'package:test/test.dart';

void main() {
  late Runtime runtime;
  late TestClock clock;
  final reports = <Cause<Object?>>[];
  const interval = Duration(seconds: 10);
  setUp(() {
    reports.clear();
    clock = TestClock();
    runtime = Runtime(
      context: Context.from([Clock.key.bind(clock)]),
      onUnhandledError: reports.add,
    );
  });
  tearDown(() async {
    await runtime.close();
    await _turn();
    expect(clock.pendingCount, 0);
    expect(reports, isEmpty);
  });

  test('negative duration is checked lazily before source work', () async {
    var started = false;
    final source = Flow.fromTask(
      Task.sync(() {
        started = true;
        return 1;
      }),
    );
    final flow = source.throttle(const Duration(microseconds: -1));
    expect(started, isFalse);
    expect(
      _cause(await runtime.run(flow.toList())),
      isA<CauseDefect>().having(
        (cause) => cause.error,
        'error',
        isA<RangeError>(),
      ),
    );
    expect(started, isFalse);
    expect(
      _value(await runtime.run(source.throttle(interval).take(0).toList())),
      isEmpty,
    );
    expect(started, isFalse);
  });

  test(
    'zero duration preserves values without reading or scheduling the clock',
    () async {
      final probe = _ClockProbe(clock, failElapsedAt: 1);
      final Flow<int?, String> flow = Flow.fromIterable<int?>([null, 1, 2])
          .throttle(Duration.zero)
          .withError<String>();
      expect(
        _value(
          await runtime.run(
            flow.toList().provideContext(Context.from([Clock.key.bind(probe)])),
          ),
        ),
        [null, 1, 2],
      );
      expect(probe.reads, 0);
      expect(probe.schedules, 0);
    },
  );

  test('empty and single-element sources finish without a timer', () async {
    expect(
      _value(
        await runtime.run(
          Flow.fromIterable(<int>[]).throttle(interval).toList(),
        ),
      ),
      isEmpty,
    );
    expect(
      _value(
        await runtime.run(
          Flow.fromIterable<int?>([null]).throttle(interval).toList(),
        ),
      ),
      [null],
    );
    expect(clock.elapsed, Duration.zero);
  });

  test(
    'all values are delivered with at least one interval between them',
    () async {
      final delivered = <(int, Duration)>[];
      final running = runtime.run(
        Flow.fromIterable([1, 2, 3])
            .throttle(interval)
            .forEach(
              (value) => Task.sync(() => delivered.add((value, clock.elapsed))),
            ),
      );
      await _until(() => clock.pendingCount == 1);
      expect(delivered, [(1, Duration.zero)]);
      clock.advance(const Duration(seconds: 9));
      await _turn();
      expect(delivered, hasLength(1));
      clock.advance(const Duration(seconds: 1));
      await _until(() => delivered.length == 2 && clock.pendingCount == 1);
      expect(delivered.last, (2, interval));
      clock.advance(interval);
      expect(await running, isA<ExitSuccess>());
      expect(delivered, [(1, Duration.zero), (2, interval), (3, interval * 2)]);
    },
  );

  test(
    'consumer time counts toward the interval and no input is prefetched',
    () async {
      final pulled = <int>[];
      final delivered = <int>[];
      final hold = Completer<void>();
      Iterable<int> input() sync* {
        for (final value in [1, 2]) {
          pulled.add(value);
          yield value;
        }
      }

      final running = runtime.run(
        Flow.fromIterable(input()).throttle(interval).forEach((value) {
          delivered.add(value);
          return value == 1
              ? Task.fromFuture(() => hold.future, onError: _defect)
              : Task.succeed(null);
        }),
      );
      await _until(() => delivered.isNotEmpty);
      expect(pulled, [1]);
      expect(clock.pendingCount, 0);
      clock.advance(const Duration(seconds: 4));
      hold.complete();
      await _until(() => clock.pendingCount == 1);
      expect(pulled, [1, 2]);
      clock.advance(const Duration(seconds: 5));
      await _turn();
      expect(delivered, [1]);
      clock.advance(const Duration(seconds: 1));
      expect(await running, isA<ExitSuccess>());
      expect(delivered, [1, 2]);
      expect(clock.elapsed, interval);
    },
  );

  test('a consumer slower than the interval adds no extra wait', () async {
    final hold = Completer<void>();
    final delivered = <Duration>[];
    final running = runtime.run(
      Flow.fromIterable([1, 2]).throttle(interval).forEach((value) {
        delivered.add(clock.elapsed);
        return value == 1
            ? Task.fromFuture(() => hold.future, onError: _defect)
            : Task.succeed(null);
      }),
    );
    await _until(() => delivered.isNotEmpty);
    clock.advance(const Duration(seconds: 15));
    hold.complete();
    expect(await running, isA<ExitSuccess>());
    expect(delivered, [Duration.zero, const Duration(seconds: 15)]);
    expect(clock.pendingCount, 0);
  });

  test('time spent pulling upstream counts toward the interval', () async {
    final delivered = <Duration>[];
    final source = Flow.fromIterable([1]).concat(
      Flow.fromTask(Task.sleep(const Duration(seconds: 20)).map((_) => 2)),
    );
    final running = runtime.run(
      source
          .throttle(interval)
          .forEach((_) => Task.sync(() => delivered.add(clock.elapsed))),
    );
    await _until(() => clock.pendingCount == 1);
    expect(delivered, [Duration.zero]);
    clock.advance(const Duration(seconds: 20));
    expect(await running, isA<ExitSuccess>());
    expect(delivered, [Duration.zero, const Duration(seconds: 20)]);
  });

  test(
    'late wakeups establish a fresh interval instead of a catch-up burst',
    () async {
      final delivered = <Duration>[];
      final running = runtime.run(
        Flow.fromIterable([1, 2, 3])
            .throttle(interval)
            .forEach((_) => Task.sync(() => delivered.add(clock.elapsed))),
      );
      await _until(() => clock.pendingCount == 1);
      clock.advance(const Duration(days: 1));
      await _until(() => delivered.length == 2 && clock.pendingCount == 1);
      expect(delivered, [Duration.zero, const Duration(days: 1)]);
      clock.advance(interval);
      expect(await running, isA<ExitSuccess>());
      expect(delivered.last, const Duration(days: 1) + interval);
    },
  );

  test('deadlines use monotonic time despite wall-clock changes', () async {
    final values = <int>[];
    final running = runtime.run(
      Flow.fromIterable([1, 2])
          .throttle(interval)
          .forEach((value) => Task.sync(() => values.add(value))),
    );
    await _until(() => clock.pendingCount == 1);
    clock.setWallTime(DateTime.utc(1900));
    clock.advance(const Duration(seconds: 9));
    await _turn();
    expect(values, [1]);
    clock.setWallTime(DateTime.utc(2100));
    clock.advance(const Duration(seconds: 1));
    expect(await running, isA<ExitSuccess>());
    expect(values, [1, 2]);
  });

  test(
    'source failure is propagated immediately without another interval',
    () async {
      final values = <int>[];
      final flow = Flow.fromIterable([1])
          .withError<String>()
          .concat(Flow.fromTask(Task.fail('source')))
          .throttle(interval);
      final exit = await runtime.run(
        flow.forEach((value) => Task.sync(() => values.add(value))),
      );
      expect(
        _cause(exit),
        isA<CauseFailure<String>>().having(
          (cause) => cause.error,
          'error',
          'source',
        ),
      );
      expect(values, [1]);
      expect(clock.elapsed, Duration.zero);
      expect(clock.pendingCount, 0);
    },
  );

  test('each consumption starts with an immediate first value', () async {
    final flow = Flow.fromIterable([1, 2]).throttle(interval);
    for (var attempt = 0; attempt < 2; attempt++) {
      final values = <int>[];
      final running = runtime.run(
        flow.forEach((value) => Task.sync(() => values.add(value))),
      );
      await _until(() => clock.pendingCount == 1);
      expect(values, [1]);
      clock.advance(interval);
      expect(await running, isA<ExitSuccess>());
      expect(values, [1, 2]);
    }
  });

  test('consumer resources keep their original source scope', () async {
    final events = <String>[];
    final running = runtime.run(
      Flow.fromIterable([1, 2, 3])
          .throttle(interval)
          .forEach(
            (value) => Task.acquireRelease(
              Task.sync(() {
                expect(
                  events.where((event) => event.startsWith('release')),
                  isEmpty,
                );
                events.add('use $value');
              }),
              release: (_, _) => Task.sync(() => events.add('release $value')),
            ),
          ),
    );
    await _until(() => clock.pendingCount == 1);
    clock.advance(interval);
    await _until(() => events.length == 2 && clock.pendingCount == 1);
    clock.advance(interval);
    expect(await running, isA<ExitSuccess>());
    expect(events, [
      'use 1',
      'use 2',
      'use 3',
      'release 3',
      'release 2',
      'release 1',
    ]);
  });

  test(
    'input resources remain alive while waiting and in downstream groups',
    () async {
      final live = <int>{};
      Flow<int, Never> source(int value) => Flow.fromTask(
        Task.acquireRelease(
          Task.sync(() {
            live.add(value);
            return value;
          }),
          release: (_, _) => Task.sync(() => live.remove(value)),
        ),
      );
      final flow = source(1).concat(source(2)).throttle(interval).grouped(3);
      final running = runtime.run(
        flow.forEach(
          (values) => Task.sync(() {
            expect(values, [1, 2]);
            expect(live, {1, 2});
          }),
        ),
      );
      await _until(() => clock.pendingCount == 1);
      expect(live, {1, 2});
      clock.advance(interval);
      expect(await running, isA<ExitSuccess>());
      expect(live, isEmpty);
    },
  );

  test('take does not pull another value or create a timer', () async {
    final pulled = <int>[];
    Iterable<int> source() sync* {
      for (final value in [1, 2, 3]) {
        pulled.add(value);
        yield value;
      }
    }

    expect(
      _value(
        await runtime.run(
          Flow.fromIterable(source()).throttle(interval).take(1).toList(),
        ),
      ),
      [1],
    );
    expect(pulled, [1]);
    expect(clock.pendingCount, 0);
  });

  test(
    'cancelling a waiting value clears its timer and retains complete cleanup',
    () async {
      final controller = CancellationController();
      final released = <int>[];
      final flow = Flow.fromIterable([1, 2])
          .mapTask(
            (value) => Task.acquireRelease(
              Task.succeed(value),
              release: (_, _) => Task.defer(() {
                released.add(value);
                return value == 2
                    ? Task.failCause(
                        Cause.combine([
                          const Cause.cancelled(),
                          const Cause.cancelled(),
                        ]),
                      )
                    : Task.succeed(null);
              }),
            ),
          )
          .throttle(interval);
      final running = runtime.run(flow.toList(), signal: controller.signal);
      await _until(() => clock.pendingCount == 1);
      expect(released, [1]);
      controller.requestCancel(reason: 'consumer');
      expect(
        _causes(_cause(await running))
            .whereType<CauseCancelled>()
            .map((cause) => cause.reason),
        unorderedEquals(['consumer', null, null]),
      );
      expect(released, [1, 2]);
    },
  );

  test('cancellation waits for source cleanup before completing', () async {
    final controller = CancellationController();
    final releasing = Completer<void>();
    final gate = Completer<void>();
    var completed = false;
    final flow = Flow.fromIterable([1, 2])
        .mapTask(
          (value) => Task.acquireRelease(
            Task.succeed(value),
            release: (_, _) => value == 2
                ? Task.fromFuture(() {
                    releasing.complete();
                    return gate.future;
                  }, onError: _defect)
                : Task.succeed(null),
          ),
        )
        .throttle(interval);
    final running = runtime.run(flow.toList(), signal: controller.signal).then((
      exit,
    ) {
      completed = true;
      return exit;
    });
    await _until(() => clock.pendingCount == 1);
    controller.requestCancel(reason: 'consumer');
    await releasing.future;
    expect(completed, isFalse);
    expect(clock.pendingCount, 0);
    gate.complete();
    expect(
      _causes(_cause(await running)).whereType<CauseCancelled>().single.reason,
      'consumer',
    );
  });

  test('early timer wakeups are rechecked before delivery', () async {
    final probe = _ClockProbe(clock, firstTimerEarly: true);
    final values = <int>[];
    final running = runtime.run(
      Flow.fromIterable([1, 2])
          .throttle(interval)
          .forEach((value) => Task.sync(() => values.add(value)))
          .provideContext(Context.from([Clock.key.bind(probe)])),
    );
    await _until(() => clock.pendingCount == 1);
    clock.advance(const Duration(seconds: 5));
    await _until(() => probe.schedules == 2);
    expect(values, [1]);
    clock.advance(const Duration(seconds: 5));
    expect(await running, isA<ExitSuccess>());
    expect(values, [1, 2]);
  });

  for (final failure in ['elapsed', 'schedule']) {
    test(
      'clock $failure failure releases the value held for delivery',
      () async {
        final error = StateError(failure);
        final probe = _ClockProbe(
          clock,
          error: error,
          failElapsedAt: failure == 'elapsed' ? 2 : null,
          failSchedule: failure == 'schedule',
        );
        final live = <int>{};
        final flow = Flow.fromIterable([1, 2])
            .mapTask(
              (value) => Task.acquireRelease(
                Task.sync(() {
                  live.add(value);
                  return value;
                }),
                release: (_, _) => Task.sync(() => live.remove(value)),
              ),
            )
            .throttle(interval);
        final exit = await runtime.run(
          flow.toList().provideContext(Context.from([Clock.key.bind(probe)])),
        );
        expect(
          _causes(_cause(exit)).whereType<CauseDefect>().single.error,
          same(error),
        );
        expect(live, isEmpty);
      },
    );
  }

  test('timer cancellation defects are retained alongside resource cleanup defects', () async {
    final controller = CancellationController();
    final timerError = StateError('timer');
    final resourceError = StateError('resource');
    final probe = _ClockProbe(clock, error: timerError, failCancel: true);
    final flow = Flow.fromIterable([1, 2])
        .mapTask(
          (value) => Task.acquireRelease(
            Task.succeed(value),
            release: (_, _) => value == 2
                ? Task.sync(() => throw resourceError)
                : Task.succeed(null),
          ),
        )
        .throttle(interval);
    final running = runtime.run(
      flow.toList().provideContext(Context.from([Clock.key.bind(probe)])),
      signal: controller.signal,
    );
    await _until(() => clock.pendingCount == 1);
    controller.requestCancel(reason: 'consumer');
    final causes = _causes(_cause(await running));
    expect(causes.whereType<CauseCancelled>().single.reason, 'consumer');
    expect(
      causes.whereType<CauseDefect>().map((cause) => cause.error),
      unorderedEquals([timerError, resourceError]),
    );
  });
}

final class _ClockProbe implements Clock {
  _ClockProbe(
    this.delegate, {
    Object? error,
    this.failElapsedAt,
    this.failSchedule = false,
    this.failCancel = false,
    this.firstTimerEarly = false,
  }) : error = error ?? StateError('clock');
  final Clock delegate;
  final Object error;
  final int? failElapsedAt;
  final bool failSchedule;
  final bool failCancel;
  final bool firstTimerEarly;
  var reads = 0;
  var schedules = 0;
  @override
  DateTime now() => delegate.now();
  @override
  Duration get elapsed {
    if (++reads == failElapsedAt) throw error;
    return delegate.elapsed;
  }

  @override
  void Function() schedule(Duration delay, void Function() callback) {
    schedules++;
    if (failSchedule) throw error;
    final actual = firstTimerEarly && schedules == 1 ? delay ~/ 2 : delay;
    final cancel = delegate.schedule(actual, callback);
    return () {
      cancel();
      if (failCancel) throw error;
    };
  }
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
  fail('The expected state was not reached.');
}
