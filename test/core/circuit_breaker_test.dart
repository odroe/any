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

  Future<CircuitBreaker> create({int threshold = 2, int probes = 1}) =>
      runtime.runOrThrow(
        CircuitBreaker.create(
          failureThreshold: threshold,
          resetTimeout: const Duration(seconds: 1),
          maxConcurrentProbes: probes,
        ),
      );

  Future<void> fail(CircuitBreaker breaker) async {
    expect(
      _cause(
        await runtime.run(
          breaker.protect(Task.fail('failed'), onOpen: () => 'open'),
        ),
      ),
      _business('failed'),
    );
  }

  test('each execution creates a distinct initially closed breaker', () async {
    final task = CircuitBreaker.create(
      failureThreshold: 1,
      resetTimeout: const Duration(seconds: 1),
    );
    final first = await runtime.runOrThrow(task);
    final second = await runtime.runOrThrow(task);
    expect(first, isNot(same(second)));
    await fail(first);
    expect(first.snapshot.state, CircuitBreakerState.open);
    expect(second.snapshot, (
      state: CircuitBreakerState.closed,
      consecutiveFailures: 0,
      activeProbes: 0,
      retryAt: null,
    ));
  });

  test('invalid parameters produce lazy range defects', () async {
    final tasks = [
      CircuitBreaker.create(
        failureThreshold: 0,
        resetTimeout: const Duration(seconds: 1),
      ),
      CircuitBreaker.create(
        failureThreshold: -1,
        resetTimeout: const Duration(seconds: 1),
      ),
      CircuitBreaker.create(failureThreshold: 1, resetTimeout: Duration.zero),
      CircuitBreaker.create(
        failureThreshold: 1,
        resetTimeout: const Duration(microseconds: -1),
      ),
      CircuitBreaker.create(
        failureThreshold: 1,
        resetTimeout: const Duration(seconds: 1),
        maxConcurrentProbes: 0,
      ),
      CircuitBreaker.create(
        failureThreshold: 9007199254740992,
        resetTimeout: const Duration(seconds: 1),
      ),
      CircuitBreaker.create(
        failureThreshold: 1,
        resetTimeout: const Duration(seconds: 1),
        maxConcurrentProbes: 9007199254740992,
      ),
      CircuitBreaker.create(
        failureThreshold: 1,
        resetTimeout: const Duration(microseconds: 9007199254740992),
      ),
    ];
    for (final task in tasks) {
      expect(
        _cause(await runtime.run(task)),
        isA<CauseDefect>().having(
          (cause) => cause.error,
          'error',
          isA<RangeError>(),
        ),
      );
    }
  });

  test('success resets the consecutive failure count', () async {
    final breaker = await create();
    await fail(breaker);
    expect(breaker.snapshot.consecutiveFailures, 1);
    expect(
      await runtime.runOrThrow(
        breaker.protect(Task.succeed(7), onOpen: () => 'open'),
      ),
      7,
    );
    expect(breaker.snapshot.consecutiveFailures, 0);
    await fail(breaker);
    expect(breaker.snapshot.state, CircuitBreakerState.closed);
    await fail(breaker);
    expect(breaker.snapshot.state, CircuitBreakerState.open);
    expect(breaker.snapshot.retryAt, const Duration(seconds: 1));
  });

  test(
    'ignored business failures are neutral without resetting the count',
    () async {
      final breaker = await create();
      await fail(breaker);
      var policyCalls = 0;
      final result = await runtime.run(
        breaker.protect(
          Task.fail('ignored'),
          onOpen: () => 'open',
          isFailure: (error) {
            expect(error, 'ignored');
            policyCalls++;
            return false;
          },
        ),
      );
      expect(_cause(result), _business('ignored'));
      expect(policyCalls, 1);
      expect(breaker.snapshot.consecutiveFailures, 1);
      await fail(breaker);
      expect(breaker.snapshot.state, CircuitBreakerState.open);
    },
  );

  test(
    'open rejection skips work and invokes onOpen only for rejections',
    () async {
      final breaker = await create(threshold: 1);
      var bodyCalls = 0;
      var openCalls = 0;
      final task = breaker.protect(
        Task.sync(() => ++bodyCalls),
        onOpen: () {
          openCalls++;
          return 'open';
        },
      );
      expect(await runtime.runOrThrow(task), 1);
      expect(openCalls, 0);
      await fail(breaker);
      final before = breaker.snapshot;
      expect(_cause(await runtime.run(task)), _business('open'));
      expect(bodyCalls, 1);
      expect(openCalls, 1);
      expect(breaker.snapshot, before);
      expect(clock.pendingCount, 0);
    },
  );

  test('onOpen exceptions are defects and leave state untouched', () async {
    final breaker = await create(threshold: 1);
    await fail(breaker);
    final before = breaker.snapshot;
    final error = StateError('rejection error');
    final result = await runtime.run(
      breaker.protect<int, String>(Task.succeed(1), onOpen: () => throw error),
    );
    expect(_cause(result), _defect(error));
    expect(breaker.snapshot, before);
  });

  test(
    'defects count without invoking the business failure predicate',
    () async {
      final breaker = await create(threshold: 1);
      final error = StateError('defect');
      var calls = 0;
      final result = await runtime.run(
        breaker.protect<int, String>(
          Task.sync(() => throw error),
          onOpen: () => 'open',
          isFailure: (_) {
            calls++;
            return false;
          },
        ),
      );
      expect(_cause(result), _defect(error));
      expect(calls, 0);
      expect(breaker.snapshot.state, CircuitBreakerState.open);
    },
  );

  test(
    'groups count once and policies see business errors in cause order',
    () async {
      final breaker = await create();
      final defect = CauseDefect(StateError('defect'), StackTrace.current);
      final seen = <String>[];
      final cause = Cause<String>.combine([
        const Cause.failure('first'),
        defect,
        const Cause.cancelled('cancelled'),
        const Cause.failure('last'),
      ]);
      final result = await runtime.run(
        breaker.protect(
          Task.failCause(cause),
          onOpen: () => 'open',
          isFailure: (error) {
            seen.add(error);
            return false;
          },
        ),
      );
      expect(
        _cause(result),
        isA<CauseGroup<String>>().having((cause) => cause.causes, 'causes', [
          _business('first'),
          same(defect),
          isA<CauseCancelled>(),
          _business('last'),
        ]),
      );
      expect(seen, ['first', 'last']);
      expect(breaker.snapshot.consecutiveFailures, 1);
      expect(breaker.snapshot.state, CircuitBreakerState.closed);
    },
  );

  test(
    'predicate exceptions count once and follow the original cause',
    () async {
      final breaker = await create(threshold: 1);
      final error = StateError('policy failed');
      final result = await runtime.run(
        breaker.protect(
          Task.fail('original'),
          onOpen: () => 'open',
          isFailure: (_) => throw error,
        ),
      );
      expect(
        _cause(result),
        isA<CauseGroup<String>>().having((cause) => cause.causes, 'causes', [
          _business('original'),
          _defect(error),
        ]),
      );
      expect(breaker.snapshot.consecutiveFailures, 1);
      expect(breaker.snapshot.state, CircuitBreakerState.open);
    },
  );

  test('pure explicit cancellation is neutral', () async {
    final breaker = await create();
    await fail(breaker);
    final result = await runtime.run(
      breaker.protect<void, String>(
        Task.failCause(const Cause.cancelled('skip')),
        onOpen: () => 'open',
      ),
    );
    expect(_cause(result), isA<CauseCancelled>());
    expect(breaker.snapshot.consecutiveFailures, 1);
    expect(breaker.snapshot.state, CircuitBreakerState.closed);
  });

  test('owner cancellation is neutral and does not retry work', () async {
    final breaker = await create();
    await fail(breaker);
    final gate = _Gate<int, String>();
    final controller = CancellationController();
    final running = runtime.run(
      breaker.protect(gate.task, onOpen: () => 'open'),
      signal: controller.signal,
    );
    await gate.started.future;
    controller.requestCancel();
    expect(_cause(await running), isA<CauseCancelled>());
    expect(gate.cancelCalls, 1);
    expect(breaker.snapshot.consecutiveFailures, 1);
    expect(breaker.snapshot.state, CircuitBreakerState.closed);
  });

  test(
    'an enclosing mask protects work and following masked callbacks',
    () async {
      final breaker = await create(threshold: 1);
      await fail(breaker);
      clock.advance(const Duration(seconds: 1));
      final gate = _Gate<int, String>();
      final controller = CancellationController();
      var continued = false;
      final task = Task.uninterruptibleMask<int, String>(
        (_) => breaker.protect(gate.task, onOpen: () => 'open').map((value) {
          continued = true;
          return value;
        }),
      );
      final running = runtime.run(task, signal: controller.signal);
      await gate.started.future;
      controller.requestCancel();
      expect(gate.cancelCalls, 0);
      gate.finish(const Exit.success(1));
      expect(_cause(await running), isA<CauseCancelled>());
      expect(continued, isTrue);
      expect(breaker.snapshot.state, CircuitBreakerState.closed);
      expect(breaker.snapshot.activeProbes, 0);
    },
  );

  test('protected work inherits services and task-local bindings', () async {
    final breaker = await create();
    final key = ServiceKey<String>('label');
    final local = TaskLocal(0);
    final work = Task.service(key)
        .flatMap((label) => local.get().map((value) => (label, value)));
    final result = await runtime.runOrThrow(
      local
          .locally(9, breaker.protect(work, onOpen: () => 'open'))
          .provideContext(Context.from([key.bind('provided')])),
    );
    expect(result, ('provided', 9));
  });

  test('snapshot does not expire open state or read the clock', () async {
    final probe = _ClockProbe(clock);
    final breaker = await runtime.runOrThrow(
      CircuitBreaker.create(
        failureThreshold: 1,
        resetTimeout: const Duration(seconds: 1),
      ).provideContext(Context.from([Clock.key.bind(probe)])),
    );
    await fail(breaker);
    final reads = probe.reads;
    clock.advance(const Duration(seconds: 2));
    probe.error = StateError('clock should not be read');
    expect(breaker.snapshot.state, CircuitBreakerState.open);
    expect(breaker.snapshot.retryAt, const Duration(seconds: 1));
    expect(probe.reads, reads);
    expect(clock.pendingCount, 0);
  });

  test(
    'the first admission at the deadline probes and success closes',
    () async {
      final breaker = await create(threshold: 1);
      await fail(breaker);
      clock.advance(const Duration(microseconds: 999999));
      expect(
        _cause(
          await runtime.run(
            breaker.protect(Task.succeed(1), onOpen: () => 'open'),
          ),
        ),
        _business('open'),
      );
      clock.advance(const Duration(microseconds: 1));
      final gate = _Gate<int, String>();
      final running = runtime.run(
        breaker.protect(gate.task, onOpen: () => 'open'),
      );
      await gate.started.future;
      expect(breaker.snapshot.state, CircuitBreakerState.halfOpen);
      expect(breaker.snapshot.activeProbes, 1);
      expect(breaker.snapshot.retryAt, isNull);
      gate.finish(const Exit.success(9));
      expect(await running, isA<ExitSuccess<int, String>>());
      expect(breaker.snapshot.state, CircuitBreakerState.closed);
      expect(breaker.snapshot.activeProbes, 0);
      expect(breaker.snapshot.consecutiveFailures, 0);
    },
  );

  test('half-open admission rejects excess concurrent probes', () async {
    final breaker = await create(threshold: 1, probes: 2);
    await fail(breaker);
    clock.advance(const Duration(seconds: 1));
    final first = _Gate<int, String>();
    final second = _Gate<int, String>();
    final firstRun = runtime.run(
      breaker.protect(first.task, onOpen: () => 'open'),
    );
    final secondRun = runtime.run(
      breaker.protect(second.task, onOpen: () => 'open'),
    );
    await first.started.future;
    await second.started.future;
    expect(breaker.snapshot.activeProbes, 2);
    expect(
      _cause(
        await runtime.run(
          breaker.protect(Task.succeed(3), onOpen: () => 'open'),
        ),
      ),
      _business('open'),
    );
    first.finish(const Exit.success(1));
    second.finish(const Exit.success(2));
    expect(await firstRun, isA<ExitSuccess<int, String>>());
    expect(await secondRun, isA<ExitSuccess<int, String>>());
    expect(breaker.snapshot.activeProbes, 0);
  });

  test('a counted probe failure reopens with a fresh deadline', () async {
    final breaker = await create(threshold: 1);
    await fail(breaker);
    clock.advance(const Duration(seconds: 1));
    await fail(breaker);
    expect(breaker.snapshot.state, CircuitBreakerState.open);
    expect(breaker.snapshot.retryAt, const Duration(seconds: 2));
  });

  test('ignored probe errors release a slot without closing', () async {
    final breaker = await create(threshold: 1);
    await fail(breaker);
    clock.advance(const Duration(seconds: 1));
    expect(
      _cause(
        await runtime.run(
          breaker.protect(
            Task.fail('ignored'),
            onOpen: () => 'open',
            isFailure: (_) => false,
          ),
        ),
      ),
      _business('ignored'),
    );
    expect(breaker.snapshot.state, CircuitBreakerState.halfOpen);
    expect(breaker.snapshot.activeProbes, 0);
    expect(breaker.snapshot.consecutiveFailures, 1);
    expect(
      await runtime.runOrThrow(
        breaker.protect(Task.succeed(1), onOpen: () => 'open'),
      ),
      1,
    );
    expect(breaker.snapshot.state, CircuitBreakerState.closed);
  });

  test('cancelled probes release their slot without closing', () async {
    final breaker = await create(threshold: 1);
    await fail(breaker);
    clock.advance(const Duration(seconds: 1));
    final gate = _Gate<int, String>();
    final controller = CancellationController();
    final running = runtime.run(
      breaker.protect(gate.task, onOpen: () => 'open'),
      signal: controller.signal,
    );
    await gate.started.future;
    controller.requestCancel();
    expect(_cause(await running), isA<CauseCancelled>());
    expect(breaker.snapshot.state, CircuitBreakerState.halfOpen);
    expect(breaker.snapshot.activeProbes, 0);
    expect(
      await runtime.runOrThrow(
        breaker.protect(Task.succeed(1), onOpen: () => 'open'),
      ),
      1,
    );
    expect(breaker.snapshot.state, CircuitBreakerState.closed);
  });

  test(
    'cancellation during result handoff does not report probe success',
    () async {
      final breaker = await create(threshold: 1);
      await fail(breaker);
      clock.advance(const Duration(seconds: 1));
      final gate = _Gate<int, String>();
      final controller = CancellationController();
      var armed = false;
      var intercepted = false;
      final running = runZoned(
        () => runtime.run(
          breaker.protect(gate.task, onOpen: () => 'open'),
          signal: controller.signal,
        ),
        zoneSpecification: ZoneSpecification(
          scheduleMicrotask: (self, parent, zone, callback) {
            if (armed) {
              armed = false;
              intercepted = true;
              controller.requestCancel();
            }
            parent.scheduleMicrotask(zone, callback);
          },
        ),
      );
      await gate.started.future;
      armed = true;
      gate.finish(const Exit.success(1));
      expect(_cause(await running), isA<CauseCancelled>());
      expect(intercepted, isTrue);
      expect(breaker.snapshot.state, CircuitBreakerState.halfOpen);
      expect(breaker.snapshot.activeProbes, 0);
    },
  );

  test(
    'consecutive results use completion order within a closed generation',
    () async {
      final breaker = await create();
      final first = _Gate<int, String>();
      final second = _Gate<int, String>();
      final third = _Gate<int, String>();
      final firstRun = runtime.run(
        breaker.protect(first.task, onOpen: () => 'open'),
      );
      final secondRun = runtime.run(
        breaker.protect(second.task, onOpen: () => 'open'),
      );
      final thirdRun = runtime.run(
        breaker.protect(third.task, onOpen: () => 'open'),
      );
      await Future.wait([
        first.started.future,
        second.started.future,
        third.started.future,
      ]);
      first.finish(const Exit.failure(Cause.failure('first')));
      expect(_cause(await firstRun), _business('first'));
      expect(breaker.snapshot.consecutiveFailures, 1);
      third.finish(const Exit.success(3));
      expect(await thirdRun, isA<ExitSuccess<int, String>>());
      expect(breaker.snapshot.consecutiveFailures, 0);
      second.finish(const Exit.failure(Cause.failure('second')));
      expect(_cause(await secondRun), _business('second'));
      expect(breaker.snapshot.state, CircuitBreakerState.closed);
      expect(breaker.snapshot.consecutiveFailures, 1);
    },
  );

  test(
    'old closed results do not affect a recovered generation or call policy',
    () async {
      final breaker = await create(threshold: 1);
      final old = _Gate<int, String>();
      var calls = 0;
      final oldRun = runtime.run(
        breaker.protect(
          old.task,
          onOpen: () => 'open',
          isFailure: (_) {
            calls++;
            return true;
          },
        ),
      );
      await old.started.future;
      await fail(breaker);
      clock.advance(const Duration(seconds: 1));
      await runtime.runOrThrow(
        breaker.protect(Task.succeed(1), onOpen: () => 'open'),
      );
      old.finish(const Exit.failure(Cause.failure('old')));
      expect(_cause(await oldRun), _business('old'));
      expect(calls, 0);
      expect(breaker.snapshot.state, CircuitBreakerState.closed);
      expect(breaker.snapshot.consecutiveFailures, 0);
    },
  );

  test(
    'a late failed probe cannot reopen after another probe succeeded',
    () async {
      final breaker = await create(threshold: 1, probes: 2);
      await fail(breaker);
      clock.advance(const Duration(seconds: 1));
      final old = _Gate<int, String>();
      var calls = 0;
      final oldRun = runtime.run(
        breaker.protect(
          old.task,
          onOpen: () => 'open',
          isFailure: (_) {
            calls++;
            return true;
          },
        ),
      );
      await old.started.future;
      await runtime.runOrThrow(
        breaker.protect(Task.succeed(1), onOpen: () => 'open'),
      );
      old.finish(const Exit.failure(Cause.failure('old')));
      expect(_cause(await oldRun), _business('old'));
      expect(calls, 0);
      expect(breaker.snapshot.state, CircuitBreakerState.closed);
      expect(breaker.snapshot.activeProbes, 0);
    },
  );

  test(
    'a late successful probe cannot close after another probe failed',
    () async {
      final breaker = await create(threshold: 1, probes: 2);
      await fail(breaker);
      clock.advance(const Duration(seconds: 1));
      final old = _Gate<int, String>();
      final oldRun = runtime.run(
        breaker.protect(old.task, onOpen: () => 'open'),
      );
      await old.started.future;
      await fail(breaker);
      final before = breaker.snapshot;
      old.finish(const Exit.success(1));
      expect(await oldRun, isA<ExitSuccess<int, String>>());
      expect(breaker.snapshot, before);
    },
  );

  test('state changes wait for protected cleanup to finish', () async {
    final breaker = await create(threshold: 1);
    final cleanup = _Gate<void, Never>();
    final running = runtime.run(
      breaker.protect(
        Task.fail('body').ensuring(cleanup.task),
        onOpen: () => 'open',
      ),
    );
    await cleanup.started.future;
    expect(breaker.snapshot.state, CircuitBreakerState.closed);
    cleanup.finish(const Exit.success(null));
    expect(_cause(await running), _business('body'));
    expect(breaker.snapshot.state, CircuitBreakerState.open);
  });

  test(
    'protected resources remain available after the outcome is counted',
    () async {
      final breaker = await create(threshold: 1);
      final resource = Object();
      var released = false;
      final result = await runtime.runOrThrow(
        breaker
            .protect(
              Task.acquireRelease(
                Task.succeed(resource),
                release: (_, _) => Task.sync(() => released = true),
              ),
              onOpen: () => 'open',
            )
            .map((value) {
              expect(released, isFalse);
              expect(breaker.snapshot.state, CircuitBreakerState.closed);
              return value;
            }),
      );

      expect(result, same(resource));
      expect(released, isTrue);
    },
  );

  test('only explicitly scoped resource cleanup affects the breaker', () async {
    for (final scoped in [false, true]) {
      final breaker = await create(threshold: 1);
      final error = StateError('resource cleanup');
      final task = Task.addFinalizer((_) => Task.sync(() => throw error));
      final exit = await runtime.run(
        breaker.protect(scoped ? task.scoped() : task, onOpen: () => 'open'),
      );

      expect(_cause(exit), _defect(error));
      expect(
        breaker.snapshot.state,
        scoped ? CircuitBreakerState.open : CircuitBreakerState.closed,
      );
    }
  });

  test(
    'cleanup defects turn a successful body into a counted failure',
    () async {
      final breaker = await create(threshold: 1);
      final error = StateError('cleanup failed');
      final result = await runtime.run(
        breaker.protect(
          Task.succeed(1).ensuring(Task.sync(() => throw error)),
          onOpen: () => 'open',
        ),
      );
      expect(_cause(result), _defect(error));
      expect(breaker.snapshot.state, CircuitBreakerState.open);
    },
  );

  test('cleanup defects still count when the body was cancelled', () async {
    final breaker = await create(threshold: 1);
    final gate = _Gate<int, String>();
    final controller = CancellationController();
    final error = StateError('cleanup failed');
    final running = runtime.run(
      breaker.protect(
        gate.task.ensuring(Task.sync(() => throw error)),
        onOpen: () => 'open',
      ),
      signal: controller.signal,
    );
    await gate.started.future;
    controller.requestCancel();
    expect(
      _cause(await running),
      isA<CauseGroup<String>>().having(
        (cause) => cause.causes,
        'causes',
        unorderedEquals([isA<CauseCancelled>(), _defect(error)]),
      ),
    );
    expect(breaker.snapshot.state, CircuitBreakerState.open);
  });

  test(
    'wall time and later clock overrides do not change the reset deadline',
    () async {
      final breaker = await create(threshold: 1);
      await fail(breaker);
      final other = TestClock()..advance(const Duration(days: 1));
      clock.setWallTime(DateTime.utc(2100));
      final protected = breaker
          .protect(Task.succeed(1), onOpen: () => 'open')
          .provideContext(Context.from([Clock.key.bind(other)]));
      expect(_cause(await runtime.run(protected)), _business('open'));
      clock.advance(const Duration(seconds: 1));
      expect(await runtime.runOrThrow(protected), 1);
    },
  );

  test(
    'clock and policy errors are both retained alongside the original failure',
    () async {
      final probe = _ClockProbe(clock);
      final breaker = await runtime.runOrThrow(
        CircuitBreaker.create(
          failureThreshold: 1,
          resetTimeout: const Duration(seconds: 1),
        ).provideContext(Context.from([Clock.key.bind(probe)])),
      );
      final clockError = StateError('clock failed');
      final policyError = StateError('policy failed');
      probe.error = clockError;
      final result = await runtime.run(
        breaker.protect(
          Task.fail('original'),
          onOpen: () => 'open',
          isFailure: (_) => throw policyError,
        ),
      );
      expect(
        _cause(result),
        isA<CauseGroup<String>>().having((cause) => cause.causes, 'causes', [
          _business('original'),
          _defect(policyError),
          _defect(clockError),
        ]),
      );
      probe.error = null;
      await fail(breaker);
      expect(breaker.snapshot.state, CircuitBreakerState.open);
    },
  );

  test('runtime cancellation counts the cleanup defect exactly once', () async {
    final breaker = await create(threshold: 1);
    final owner = Runtime();
    final gate = _Gate<int, String>();
    final error = StateError('cleanup failed');
    final running = owner.run(
      breaker.protect(
        gate.task.ensuring(Task.sync(() => throw error)),
        onOpen: () => 'open',
      ),
    );
    await gate.started.future;
    final closed = owner.close();
    expect(
      _cause(await running),
      isA<CauseGroup<String>>().having(
        (cause) => cause.causes,
        'causes',
        unorderedEquals([isA<CauseCancelled>(), _defect(error)]),
      ),
    );
    expect(_cause(await closed), _defect(error));
    expect(breaker.snapshot.state, CircuitBreakerState.open);
    expect(breaker.snapshot.consecutiveFailures, 1);
  });

  test('nested runtimes retain the outer host self-close guard', () async {
    final breaker = await create();
    final inner = Runtime();
    addTearDown(() async {
      expect(await inner.close(), isA<ExitSuccess<void, Never>>());
    });
    var timedOut = false;
    final closeOuter = Task.fromFuture<Exit<void, Never>, String>(
      () => runtime.close().timeout(
        const Duration(seconds: 1),
        onTimeout: () {
          // Release the circular wait if host ancestry regresses, so both
          // runtimes can still finish and the test reports a bounded failure.
          timedOut = true;
          return const Exit.success(null);
        },
      ),
      onError: (error, _) => error.toString(),
    );
    final result = await runtime.run(
      Task.fromFuture(
        () =>
            inner.runOrThrow(breaker.protect(closeOuter, onOpen: () => 'open')),
        onError: (error, _) => error,
      ),
    );

    expect(timedOut, isFalse);
    expect(
      result,
      isA<ExitSuccess<Exit<void, Never>, Object>>().having(
        (exit) => _cause(exit.value),
        'rejected self-close',
        isA<CauseDefect>().having(
          (cause) => cause.error,
          'error',
          isA<StateError>(),
        ),
      ),
    );
  });

  test(
    'runtime closure releases a half-open probe without closing the breaker',
    () async {
      final breaker = await create(threshold: 1);
      await fail(breaker);
      clock.advance(const Duration(seconds: 1));
      final gate = _Gate<int, String>();
      final running = runtime.run(
        breaker.protect(gate.task, onOpen: () => 'open'),
      );
      await gate.started.future;
      expect(await runtime.close(), isA<ExitSuccess<void, Never>>());
      expect(_cause(await running), isA<CauseCancelled>());
      expect(breaker.snapshot.state, CircuitBreakerState.halfOpen);
      expect(breaker.snapshot.activeProbes, 0);
    },
  );
}

Cause<E> _cause<T, E>(Exit<T, E> exit) => switch (exit) {
  ExitFailure(:final cause) => cause,
  ExitSuccess() => throw StateError('Expected failure, got $exit.'),
};

Matcher _business(String error) =>
    isA<CauseFailure<String>>().having((cause) => cause.error, 'error', error);

Matcher _defect(Object error) =>
    isA<CauseDefect>().having((cause) => cause.error, 'error', same(error));

final class _Gate<T, E> {
  final started = Completer<void>();
  late void Function(Exit<T, E>) finish;
  var cancelCalls = 0;

  Task<T, E> get task => Task.fromCallback((complete) {
    finish = complete;
    started.complete();
    return () {
      cancelCalls++;
    };
  });
}

final class _ClockProbe implements Clock {
  _ClockProbe(this.source);

  final TestClock source;
  Object? error;
  var reads = 0;

  @override
  Duration get elapsed {
    reads++;
    if (error case final error?) throw error;
    return source.elapsed;
  }

  @override
  DateTime now() => source.now();

  @override
  void Function() schedule(Duration delay, void Function() callback) =>
      source.schedule(delay, callback);
}
