import 'dart:async';

import 'package:any/any.dart';
import 'package:any/testing.dart';
import 'package:test/test.dart';

void main() {
  late TestClock clock;
  late Runtime runtime;

  setUp(() {
    clock = TestClock();
    runtime = Runtime(context: Context.from([Clock.key.bind(clock)]));
  });
  tearDown(() async {
    expect(await runtime.close(), isA<ExitSuccess<void, Never>>());
    expect(clock.pendingCount, 0);
  });

  Future<RateLimiter> create({
    int permits = 1,
    Duration interval = const Duration(seconds: 1),
    int? burst,
  }) => runtime.runOrThrow(
    RateLimiter.create(permits: permits, interval: interval, burst: burst),
  );

  test('each execution creates an independent full bucket', () async {
    final task = RateLimiter.create(
      permits: 2,
      interval: const Duration(seconds: 1),
    );
    final first = await runtime.runOrThrow(task);
    final second = await runtime.runOrThrow(task);
    expect(first, isNot(same(second)));

    await runtime.runOrThrow(first.acquire(permits: 2));
    await runtime.runOrThrow(second.acquire(permits: 2));
    expect(clock.pendingCount, 0);
  });

  test('invalid creation parameters become lazy range defects', () async {
    final tasks = [
      RateLimiter.create(permits: 0, interval: const Duration(seconds: 1)),
      RateLimiter.create(permits: -1, interval: const Duration(seconds: 1)),
      RateLimiter.create(permits: 1, interval: Duration.zero),
      RateLimiter.create(
        permits: 1,
        interval: const Duration(microseconds: -1),
      ),
      RateLimiter.create(
        permits: 1,
        interval: const Duration(seconds: 1),
        burst: 0,
      ),
      RateLimiter.create(
        permits: 9007199254740992,
        interval: const Duration(seconds: 1),
      ),
      RateLimiter.create(
        permits: 1,
        interval: const Duration(seconds: 1),
        burst: 9007199254740992,
      ),
    ];
    for (final task in tasks) {
      expect(_cause(await runtime.run(task)), _rangeDefect);
    }
    expect(clock.pendingCount, 0);
  });

  test('invalid requests preserve the balance and do not start work', () async {
    final limiter = await create(burst: 2);
    var calls = 0;
    for (final count in [-1, 0, 3]) {
      final task = limiter.run(Task.sync(() => calls++), permits: count);
      expect(calls, 0);
      expect(_cause(await runtime.run(task)), _rangeDefect);
    }
    await runtime.runOrThrow(limiter.acquire(permits: 2));
    expect(calls, 0);
    expect(clock.pendingCount, 0);
  });

  test('reusing an acquisition makes a new request each time', () async {
    final limiter = await create();
    final acquire = limiter.acquire();
    await runtime.runOrThrow(acquire);
    final waiting = runtime.run(acquire);
    expect(clock.pendingCount, 1);

    clock.advance(const Duration(seconds: 1));
    expect(await waiting, isA<ExitSuccess<void, Never>>());
  });

  test('refill is continuous and work starts only after consumption', () async {
    final limiter = await create(permits: 4);
    await runtime.runOrThrow(limiter.acquire(permits: 4));
    var calls = 0;
    final waiting = runtime.run(limiter.run(Task.sync(() => ++calls)));
    expect(calls, 0);
    expect(clock.pendingCount, 1);

    clock.advance(const Duration(microseconds: 249999));
    expect(calls, 0);
    expect(clock.pendingCount, 1);
    clock.advance(const Duration(microseconds: 1));
    expect(await waiting, isA<ExitSuccess<int, Never>>());
    expect(calls, 1);
    expect(clock.pendingCount, 0);
  });

  test(
    'idle refill stops at the burst capacity and retains no timer',
    () async {
      final limiter = await create(permits: 2, burst: 3);
      await runtime.runOrThrow(limiter.acquire(permits: 3));
      expect(clock.pendingCount, 0);
      clock.advance(const Duration(days: 30));
      await runtime.runOrThrow(limiter.acquire(permits: 3));
      final waiting = runtime.run(limiter.acquire());
      expect(clock.pendingCount, 1);

      clock.advance(const Duration(milliseconds: 500));
      expect(await waiting, isA<ExitSuccess<void, Never>>());
    },
  );

  test('later service overrides do not replace the captured clock', () async {
    final limiter = await create();
    final otherClock = TestClock();
    await runtime.runOrThrow(limiter.acquire());
    final waiting = runtime.run(
      limiter.acquire().provideContext(
        Context.from([Clock.key.bind(otherClock)]),
      ),
    );
    expect(clock.pendingCount, 1);
    expect(otherClock.pendingCount, 0);
    otherClock.advance(const Duration(days: 1));
    expect(clock.pendingCount, 1);

    clock.advance(const Duration(seconds: 1));
    expect(await waiting, isA<ExitSuccess<void, Never>>());
  });

  test('wall-time adjustments do not change refill deadlines', () async {
    final limiter = await create();
    await runtime.runOrThrow(limiter.acquire());
    final waiting = runtime.run(limiter.acquire());
    clock.setWallTime(DateTime.utc(2100));
    expect(clock.pendingCount, 1);

    clock.advance(const Duration(seconds: 1));
    expect(await waiting, isA<ExitSuccess<void, Never>>());
  });

  test('small requests cannot bypass a larger FIFO head', () async {
    final limiter = await create(burst: 3);
    await runtime.runOrThrow(limiter.acquire(permits: 3));
    final order = <String>[];
    final large = runtime.run(
      limiter.run(Task.sync(() => order.add('large')), permits: 3),
    );
    final small = runtime.run(limiter.run(Task.sync(() => order.add('small'))));
    expect(clock.pendingCount, 1);
    clock.advance(const Duration(seconds: 1));
    expect(order, isEmpty);
    clock.advance(const Duration(seconds: 2));
    expect(await large, isA<ExitSuccess<void, Never>>());
    expect(order, ['large']);
    expect(clock.pendingCount, 1);

    clock.advance(const Duration(seconds: 1));
    expect(await small, isA<ExitSuccess<void, Never>>());
    expect(order, ['large', 'small']);
  });

  test('cancelling the head permits a fitting later request', () async {
    final limiter = await create(burst: 3);
    await runtime.runOrThrow(limiter.acquire(permits: 3));
    final controller = CancellationController();
    final large = runtime.run(
      limiter.acquire(permits: 3),
      signal: controller.signal,
    );
    final small = runtime.run(limiter.acquire());
    clock.advance(const Duration(seconds: 1));
    expect(clock.pendingCount, 1);

    controller.requestCancel(reason: 'skip head');
    expect(_cause(await large), isA<CauseCancelled>());
    expect(await small, isA<ExitSuccess<void, Never>>());
    expect(clock.pendingCount, 0);
  });

  test(
    'queued cancellations consume no tokens and remove the last timer',
    () async {
      final limiter = await create();
      await runtime.runOrThrow(limiter.acquire());
      final controllers = List.generate(8, (_) => CancellationController());
      final waiting = [
        for (final controller in controllers)
          runtime.run(limiter.acquire(), signal: controller.signal),
      ];
      expect(clock.pendingCount, 1);
      for (final controller in controllers) {
        controller.requestCancel();
      }
      for (final result in waiting) {
        expect(_cause(await result), isA<CauseCancelled>());
      }
      expect(clock.pendingCount, 0);

      clock.advance(const Duration(seconds: 1));
      await runtime.runOrThrow(limiter.acquire());
      expect(clock.pendingCount, 0);
    },
  );

  test('handoff cancellation leaves the token for the next waiter', () async {
    final limiter = await create();
    await runtime.runOrThrow(limiter.acquire());
    final controller = CancellationController();
    var armed = false;
    var intercepted = false;
    var calls = 0;
    final cancelled = runZoned(
      () => runtime.run(
        limiter.run(Task.sync(() => calls++)),
        signal: controller.signal,
      ),
      zoneSpecification: ZoneSpecification(
        scheduleMicrotask: (self, parent, zone, callback) {
          if (armed) {
            armed = false;
            intercepted = true;
            controller.requestCancel(reason: 'handoff');
          }
          parent.scheduleMicrotask(zone, callback);
        },
      ),
    );
    final next = runtime.run(limiter.acquire());

    armed = true;
    clock.advance(const Duration(seconds: 1));
    expect(_cause(await cancelled), isA<CauseCancelled>());
    expect(await next, isA<ExitSuccess<void, Never>>());
    expect(intercepted, isTrue);
    expect(calls, 0);
    expect(clock.pendingCount, 0);
  });

  test('body failures preserve their cause and do not refund tokens', () async {
    final limiter = await create();
    final defect = CauseDefect(StateError('defect'), StackTrace.current);
    final cause = Cause<String>.combine([
      const Cause.failure('failed'),
      defect,
    ]);
    final exit = await runtime.run(limiter.run(Task.failCause(cause)));
    expect(
      _cause(exit),
      isA<CauseGroup<String>>().having((cause) => cause.causes, 'causes', [
        isA<CauseFailure<String>>().having(
          (cause) => cause.error,
          'error',
          'failed',
        ),
        same(defect),
      ]),
    );
    final next = runtime.run(limiter.acquire());
    expect(clock.pendingCount, 1);

    clock.advance(const Duration(seconds: 1));
    expect(await next, isA<ExitSuccess<void, Never>>());
  });

  test(
    'cancelling an already started body does not refund its tokens',
    () async {
      final limiter = await create();
      final controller = CancellationController();
      final started = Completer<void>();
      var cancelCalls = 0;
      final running = runtime.run(
        limiter.run(
          Task.fromCallback<void, Never>((_) {
            started.complete();
            return () {
              cancelCalls++;
            };
          }),
        ),
        signal: controller.signal,
      );
      await started.future;
      controller.requestCancel();
      expect(_cause(await running), isA<CauseCancelled>());
      expect(cancelCalls, 1);
      final next = runtime.run(limiter.acquire());
      expect(clock.pendingCount, 1);

      clock.advance(const Duration(seconds: 1));
      expect(await next, isA<ExitSuccess<void, Never>>());
    },
  );

  test('fractional refill deadlines round up to whole microseconds', () async {
    final limiter = await create(permits: 3);
    await runtime.runOrThrow(limiter.acquire(permits: 3));
    var calls = 0;
    final waiting = runtime.run(limiter.run(Task.sync(() => calls++)));
    clock.advance(const Duration(microseconds: 333333));
    expect(calls, 0);
    expect(clock.pendingCount, 1);
    clock.advance(const Duration(microseconds: 1));
    expect(await waiting, isA<ExitSuccess<int, Never>>());
    expect(calls, 1);
  });

  test(
    'an early timer retries with a positive delay without consuming',
    () async {
      final probe = _ClockProbe(clock)..earlyOnce = true;
      final limiter = await runtime.runOrThrow(
        RateLimiter.create(
          permits: 1,
          interval: const Duration(microseconds: 2),
        ).provideContext(Context.from([Clock.key.bind(probe)])),
      );
      await runtime.runOrThrow(limiter.acquire());
      var calls = 0;
      final waiting = runtime.run(limiter.run(Task.sync(() => calls++)));
      clock.advance(const Duration(microseconds: 1));
      expect(calls, 0);
      expect(probe.delays, [
        const Duration(microseconds: 2),
        const Duration(microseconds: 1),
      ]);
      expect(clock.pendingCount, 1);

      clock.advance(const Duration(microseconds: 1));
      expect(await waiting, isA<ExitSuccess<int, Never>>());
      expect(calls, 1);
    },
  );

  test('timer callback clock failures wake all waiters as defects', () async {
    final probe = _ClockProbe(clock);
    final limiter = await runtime.runOrThrow(
      RateLimiter.create(
        permits: 1,
        interval: const Duration(seconds: 1),
      ).provideContext(Context.from([Clock.key.bind(probe)])),
    );
    await runtime.runOrThrow(limiter.acquire());
    final first = runtime.run(limiter.acquire());
    final second = runtime.run(limiter.acquire());
    final error = StateError('clock unavailable');
    probe.elapsedError = error;
    clock.advance(const Duration(seconds: 1));
    for (final result in [first, second]) {
      expect(_cause(await result), _defect(error));
    }
    expect(clock.pendingCount, 0);

    probe.elapsedError = null;
    await runtime.runOrThrow(limiter.acquire());
  });

  test('a backwards elapsed reading fails without consuming tokens', () async {
    final probe = _ClockProbe(clock);
    clock.advance(const Duration(seconds: 1));
    final limiter = await runtime.runOrThrow(
      RateLimiter.create(
        permits: 1,
        interval: const Duration(seconds: 1),
      ).provideContext(Context.from([Clock.key.bind(probe)])),
    );
    probe.elapsedOverride = Duration.zero;
    expect(
      _cause(await runtime.run(limiter.acquire())),
      isA<CauseDefect>().having(
        (cause) => cause.error,
        'error',
        isA<StateError>(),
      ),
    );

    probe.elapsedOverride = null;
    await runtime.runOrThrow(limiter.acquire());
  });

  test(
    'timer registration errors become defects and leave no waiter',
    () async {
      final probe = _ClockProbe(clock);
      final limiter = await runtime.runOrThrow(
        RateLimiter.create(
          permits: 1,
          interval: const Duration(seconds: 1),
        ).provideContext(Context.from([Clock.key.bind(probe)])),
      );
      await runtime.runOrThrow(limiter.acquire());
      final error = StateError('cannot schedule');
      probe.scheduleError = error;
      expect(_cause(await runtime.run(limiter.acquire())), _defect(error));
      expect(clock.pendingCount, 0);

      probe.scheduleError = null;
      clock.advance(const Duration(seconds: 1));
      await runtime.runOrThrow(limiter.acquire());
    },
  );

  test(
    'timer cancellation failures remain in the cancelling task cause',
    () async {
      final probe = _ClockProbe(clock);
      final limiter = await runtime.runOrThrow(
        RateLimiter.create(
          permits: 1,
          interval: const Duration(seconds: 1),
        ).provideContext(Context.from([Clock.key.bind(probe)])),
      );
      await runtime.runOrThrow(limiter.acquire());
      final controller = CancellationController();
      final waiting = runtime.run(limiter.acquire(), signal: controller.signal);
      final error = StateError('cancel failed');
      probe.cancelError = error;
      controller.requestCancel(reason: 'stop');
      expect(
        _cause(await waiting),
        isA<CauseGroup<Never>>().having(
          (cause) => cause.causes,
          'causes',
          unorderedEquals([isA<CauseCancelled>(), _defect(error)]),
        ),
      );
      expect(clock.pendingCount, 0);
    },
  );

  test(
    'a clock failure retains timer cleanup defects for every waiter',
    () async {
      final probe = _ClockProbe(clock);
      final limiter = await runtime.runOrThrow(
        RateLimiter.create(
          permits: 1,
          interval: const Duration(seconds: 1),
        ).provideContext(Context.from([Clock.key.bind(probe)])),
      );
      await runtime.runOrThrow(limiter.acquire());
      final first = runtime.run(limiter.acquire());
      final clockError = StateError('elapsed failed');
      final cancelError = StateError('cancel failed');
      probe.elapsedError = clockError;
      probe.cancelError = cancelError;
      final second = runtime.run(limiter.acquire());

      for (final waiting in [first, second]) {
        expect(
          _cause(await waiting),
          isA<CauseGroup<Never>>().having((cause) => cause.causes, 'causes', [
            _defect(clockError),
            _defect(cancelError),
          ]),
        );
      }
      expect(clock.pendingCount, 0);
    },
  );

  test('closing the runtime removes pending acquisition timers', () async {
    final limiter = await create();
    await runtime.runOrThrow(limiter.acquire());
    final first = runtime.run(limiter.acquire());
    final second = runtime.run(limiter.acquire());
    expect(clock.pendingCount, 1);

    expect(await runtime.close(), isA<ExitSuccess<void, Never>>());
    expect(_cause(await first), isA<CauseCancelled>());
    expect(_cause(await second), isA<CauseCancelled>());
    expect(clock.pendingCount, 0);
  });
}

Cause<E> _cause<T, E>(Exit<T, E> exit) => switch (exit) {
  ExitFailure(:final cause) => cause,
  ExitSuccess() => throw StateError('Expected failure, got $exit.'),
};

final _rangeDefect = isA<CauseDefect>().having(
  (cause) => cause.error,
  'error',
  isA<RangeError>(),
);

Matcher _defect(Object error) =>
    isA<CauseDefect>().having((cause) => cause.error, 'error', same(error));

final class _ClockProbe implements Clock {
  _ClockProbe(this.source);

  final TestClock source;
  final delays = <Duration>[];
  bool earlyOnce = false;
  Object? elapsedError;
  Object? scheduleError;
  Object? cancelError;
  Duration? elapsedOverride;

  @override
  Duration get elapsed {
    if (elapsedError case final error?) throw error;
    return elapsedOverride ?? source.elapsed;
  }

  @override
  DateTime now() => source.now();

  @override
  void Function() schedule(Duration delay, void Function() callback) {
    if (scheduleError case final error?) throw error;
    delays.add(delay);
    final actual = earlyOnce ? delay - const Duration(microseconds: 1) : delay;
    earlyOnce = false;
    final cancel = source.schedule(actual, callback);
    return () {
      cancel();
      if (cancelError case final error?) throw error;
    };
  }
}
