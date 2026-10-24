import 'dart:async';
import 'dart:collection';

import 'package:any/any.dart';
import 'package:any/testing.dart';
import 'package:test/test.dart';

void main() {
  late _ObservedClock clock;
  late Runtime runtime;
  setUp(() {
    clock = _ObservedClock();
    runtime = Runtime(context: Context.from([Clock.key.bind(clock)]));
    addTearDown(runtime.close);
  });

  test('sleep is lazy and uses the supplied clock', () async {
    final task = Task.sleep(const Duration(seconds: 2));
    expect(clock.clock.pendingCount, 0);
    final result = runtime.run(task);
    expect(await clock.registration(), const Duration(seconds: 2));
    clock.clock.advance(const Duration(seconds: 1));
    expect(clock.clock.pendingCount, 1);
    clock.clock.advance(const Duration(seconds: 1));
    expect(await result, isA<ExitSuccess<void, Never>>());
    expect(clock.clock.pendingCount, 0);
  });

  test('cancelling sleep removes its timer', () async {
    final cancel = CancellationController();
    final result = runtime.run(
      Task.sleep(const Duration(days: 1)),
      signal: cancel.signal,
    );
    await clock.registration();
    cancel.requestCancel(reason: 'stop');
    expect(_cause(await result), isA<CauseCancelled>());
    expect(clock.clock.pendingCount, 0);
  });

  test('delay waits before starting the computation', () async {
    var calls = 0;
    final result = runtime.run(
      Task.sync(() => ++calls).delay(const Duration(seconds: 1)),
    );
    await clock.registration();
    expect(calls, 0);
    clock.clock.advance(const Duration(seconds: 1));
    expect(_value(await result), 1);
  });

  test('nonpositive sleep still registers a cancellable timer', () async {
    final result = runtime.run(Task.sleep(const Duration(seconds: -1)));
    await clock.registration();
    expect(clock.clock.pendingCount, 1);
    clock.clock.advance(Duration.zero);
    expect(await result, isA<ExitSuccess<void, Never>>());
  });

  test(
    'recurs counts additional retries and returns the final business error',
    () async {
      var attempts = 0;
      final task = Task.defer<int, String>(
        () => Task.fail('try ${++attempts}'),
      );
      final result = await runtime.run(task.retry(Schedule.recurs(3)));
      expect(attempts, 4);
      expect((_cause(result) as CauseFailure<String>).error, 'try 4');
      expect(clock.clock.pendingCount, 0);
    },
  );

  test('explicitly scoped retry closes each attempt before another', () async {
    var attempts = 0;
    final events = <String>[];
    final task =
        Task.acquireRelease(
          Task.sync(() {
            final attempt = ++attempts;
            events.add('acquire $attempt');
            return attempt;
          }).withError<String>(),
          release: (attempt, _) =>
              Task.sync(() => events.add('release $attempt')),
        ).flatMap(
          (attempt) => attempt < 3 ? Task.fail('retry') : Task.succeed(attempt),
        );

    expect(
      _value(await runtime.run(task.scoped().retry(Schedule.recurs(4)))),
      3,
    );
    expect(events, [
      'acquire 1',
      'release 1',
      'acquire 2',
      'release 2',
      'acquire 3',
      'release 3',
    ]);
  });

  test('retry retains every attempt resource in the caller scope', () async {
    var attempts = 0;
    final released = <int>[];
    final task = Task.acquireRelease(
      Task.sync(() {
        expect(released, isEmpty);
        return ++attempts;
      }).withError<String>(),
      release: (value, _) => Task.sync(() => released.add(value)),
    ).flatMap((value) => value < 3 ? Task.fail('retry') : Task.succeed(value));

    expect(
      await runtime.runOrThrow(
        task.retry(Schedule.recurs(4)).map((value) {
          expect(released, isEmpty);
          return value;
        }),
      ),
      3,
    );
    expect(released, [3, 2, 1]);
  });

  test('retry predicates see only single business failures', () async {
    var attempts = 0;
    var predicates = 0;
    final defect = StateError('body');
    final task = Task.sync<int>(() {
      attempts++;
      throw defect;
    }).withError<String>();
    final result = await runtime.run(
      task.retry(
        Schedule.recurs(3),
        when: (_) {
          predicates++;
          return true;
        },
      ),
    );
    expect(attempts, 1);
    expect(predicates, 0);
    expect((_cause(result) as CauseDefect).error, same(defect));
  });

  test(
    'cleanup failure prevents retry and retains the business failure',
    () async {
      final defect = StateError('cleanup');
      var attempts = 0;
      final task = Task.defer<int, String>(() {
        attempts++;
        return Task.fail('body');
      }).ensuring(Task.sync(() => throw defect));
      final result = await runtime.run(task.retry(Schedule.recurs(3)));
      expect(attempts, 1);
      final causes = (_cause(result) as CauseGroup<String>).causes;
      expect((causes[0] as CauseFailure<String>).error, 'body');
      expect((causes[1] as CauseDefect).error, same(defect));
    },
  );

  test(
    'explicit cancellation causes and grouped failures are not retried',
    () async {
      for (final cause in <Cause<String>>[
        Cause.cancelled('cancelled'),
        Cause.combine([Cause.failure('a'), Cause.failure('b')]),
      ]) {
        var calls = 0;
        final result = await runtime.run(
          Task.defer<int, String>(() {
            calls++;
            return Task.failCause(cause);
          }).retry(Schedule.recurs(3)),
        );
        expect(calls, 1);
        if (cause case CauseGroup(:final causes)) {
          expect(
            (_cause(result) as CauseGroup<String>).causes.map(
              (cause) => (cause as CauseFailure<String>).error,
            ),
            causes.map((cause) => (cause as CauseFailure<String>).error),
          );
        } else {
          expect(_cause(result), same(cause));
        }
      }
    },
  );

  test(
    'a false retry predicate stops and a thrown predicate is a defect',
    () async {
      final task = Task.fail('failure');
      final stopped = await runtime.run(
        task.retry(Schedule.recurs(3), when: (_) => false),
      );
      expect((_cause(stopped) as CauseFailure<String>).error, 'failure');
      final defect = StateError('predicate');
      final failed = await runtime.run(
        task.retry(Schedule.recurs(3), when: (_) => throw defect),
      );
      expect((_cause(failed) as CauseDefect).error, same(defect));
    },
  );

  test(
    'retry waits using exponential delays after completed attempts',
    () async {
      var attempts = 0;
      final result = runtime.run(
        Task.defer<int, String>(() {
          attempts++;
          return attempts == 3 ? Task.succeed(attempts) : Task.fail('retry');
        }).retry(Schedule.exponential(const Duration(seconds: 1)).limit(3)),
      );

      expect(await clock.registration(), const Duration(seconds: 1));
      expect(attempts, 1);
      clock.clock.advance(const Duration(seconds: 1));
      expect(await clock.registration(), const Duration(seconds: 2));
      expect(attempts, 2);
      clock.clock.advance(const Duration(seconds: 2));
      expect(_value(await result), 3);
    },
  );

  test('cancelling retry during backoff stops future attempts', () async {
    var attempts = 0;
    final cancel = CancellationController();
    final result = runtime.run(
      Task.defer<int, String>(() {
        attempts++;
        return Task.fail('retry');
      }).retry(Schedule.spaced(const Duration(seconds: 1))),
      signal: cancel.signal,
    );
    await clock.registration();
    cancel.requestCancel();
    expect(_cause(await result), isA<CauseCancelled>());
    expect(attempts, 1);
    expect(clock.clock.pendingCount, 0);
  });

  test('elapsed policy includes attempts and their cleanup', () async {
    var attempts = 0;
    final result = runtime.run(
      Task.defer<int, String>(() {
            attempts++;
            return Task.fail('failure');
          })
          .ensuring(Task.sleep(const Duration(seconds: 1)))
          .retry(Schedule.recurs(10).whileElapsed(const Duration(seconds: 2))),
    );
    await clock.registration();
    clock.clock.advance(const Duration(seconds: 1));
    await clock.registration();
    clock.clock.advance(const Duration(seconds: 1));
    expect(_cause(await result), isA<CauseFailure<String>>());
    expect(attempts, 2);
  });

  test(
    'repeat returns the last value and uses independent policy state per run',
    () async {
      var calls = 0;
      final task = Task.sync(() => ++calls).repeat(Schedule.recurs(2));
      expect(_value(await runtime.run(task)), 3);
      expect(_value(await runtime.run(task)), 6);
    },
  );

  test(
    'repeat stops when its predicate succeeds or an attempt fails',
    () async {
      var calls = 0;
      final task = Task.sync(() => ++calls);
      expect(
        _value(
          await runtime.run(
            task.repeat(Schedule.recurs(10), until: (value) => value == 3),
          ),
        ),
        3,
      );
      calls = 0;
      final failure = Task.defer<int, String>(
        () => ++calls == 2 ? Task.fail('stop') : Task.succeed(calls),
      );
      expect(
        (_cause(
          await runtime.run(failure.repeat(Schedule.recurs(10))),
        ) as CauseFailure<String>).error,
        'stop',
      );
      expect(calls, 2);
    },
  );

  test(
    'repeat retains attempt resources until the caller scope closes',
    () async {
      var acquired = 0;
      var released = 0;
      final task = Task.acquireRelease(
        Task.sync(() => ++acquired),
        release: (_, _) => Task.sync(() => released++),
      ).repeat(Schedule.recurs(2));
      expect(
        _value(
          await runtime.run(
            task.map((value) {
              expect(released, 0);
              return value;
            }),
          ),
        ),
        3,
      );
      expect(released, 3);
    },
  );

  test(
    'explicitly scoped repeat releases each attempt before continuing',
    () async {
      var acquired = 0;
      var released = 0;
      final task = Task.acquireRelease(
        Task.sync(() {
          expect(released, acquired);
          return ++acquired;
        }),
        release: (_, _) => Task.sync(() => released++),
      ).scoped().repeat(Schedule.recurs(2));
      expect(
        await runtime.runOrThrow(
          task.map((value) {
            expect(released, 3);
            return value;
          }),
        ),
        3,
      );
    },
  );

  test(
    'large zero-delay repeats remain stack safe and cooperatively cancellable',
    () async {
      var calls = 0;
      expect(
        _value(
          await runtime.run(
            Task.sync(() => ++calls).repeat(Schedule.recurs(10000)),
          ),
        ),
        10001,
      );
      final cancel = CancellationController();
      final result = runtime.run(
        Task.succeed(null).repeat(Schedule.spaced(Duration.zero)),
        signal: cancel.signal,
      );
      Timer.run(cancel.requestCancel);
      expect(_cause(await result), isA<CauseCancelled>());
    },
  );

  test(
    'widened Never computations keep their error type through repetition',
    () async {
      final Task<int, String> task = Task.succeed(1);
      final repeated = task
          .repeat(Schedule.recurs(1))
          .flatMap<int>((_) => Task.fail('typed'));
      expect(
        (_cause(await runtime.run(repeated)) as CauseFailure<String>).error,
        'typed',
      );
    },
  );
}

T _value<T, E>(Exit<T, E> exit) => (exit as ExitSuccess<T, E>).value;
Cause<E> _cause<T, E>(Exit<T, E> exit) => (exit as ExitFailure<T, E>).cause;

final class _ObservedClock implements Clock {
  final clock = TestClock();
  final _registrations = Queue<Duration>();
  final _waiters = Queue<Completer<Duration>>();

  Future<Duration> registration() {
    if (_registrations.isNotEmpty) {
      return Future.value(_registrations.removeFirst());
    }
    final waiter = Completer<Duration>();
    _waiters.add(waiter);
    return waiter.future;
  }

  @override
  Duration get elapsed => clock.elapsed;

  @override
  DateTime now() => clock.now();

  @override
  void Function() schedule(Duration delay, void Function() callback) {
    final cancel = clock.schedule(delay, callback);
    if (_waiters.isEmpty) {
      _registrations.add(delay);
    } else {
      _waiters.removeFirst().complete(delay);
    }
    return cancel;
  }
}
