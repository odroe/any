import 'dart:async';

import 'package:any/any.dart';
import 'package:any/testing.dart';
import 'package:test/test.dart';

void main() {
  late Runtime runtime;
  late TestClock clock;
  late List<Cause<Object?>> reports;
  const ttl = Duration(seconds: 10);

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

  test(
    'default creation is lazy and keeps returned values for reuse',
    () async {
      var calls = 0;
      final pool = await _own(
        runtime,
        Pool.create(Task.sync(() => ++calls), maxSize: 1),
      );
      expect(calls, 0);
      expect(_value(await runtime.run(pool.acquire())), 1);
      expect(_value(await runtime.run(pool.acquire())), 1);
      expect(calls, 1);
      expect(clock.pendingCount, 0);
    },
  );

  test('invalid sizes and idle timeout fail during execution', () async {
    for (final options in [(0, 0), (1, -1), (1, 2)]) {
      final task = Pool.create(
        Task.succeed(1),
        maxSize: options.$1,
        initialSize: options.$2,
      );
      expect(_defects(await runtime.run(task)).single, isA<RangeError>());
    }
    final task = Pool.create(
      Task.succeed(1),
      maxSize: 1,
      idleTimeout: const Duration(microseconds: -1),
    );
    expect(_defects(await runtime.run(task)).single, isA<ArgumentError>());
  });

  test('a completed creation scope closes the pool', () async {
    final pool = _value(
      await runtime.run(Pool.create(Task.succeed(1), maxSize: 1)),
    );
    expect(
      _defects(await runtime.run(pool.acquire())).single,
      isA<StateError>(),
    );
    expect(
      _defects(await runtime.run(pool.invalidate(1))).single,
      isA<StateError>(),
    );
  });

  test('each lease is exclusive and callers wait in FIFO order', () async {
    var calls = 0;
    final pool = await _own(
      runtime,
      Pool.create(Task.sync(() => ++calls), maxSize: 1),
    );
    final first = _Borrow(runtime, pool.acquire());
    expect(await first.ready.future, 1);
    final second = _Borrow(runtime, pool.acquire());
    final third = _Borrow(runtime, pool.acquire());
    expect(second.ready.isCompleted, isFalse);
    expect(third.ready.isCompleted, isFalse);
    first.release();
    await first.done;
    expect(await second.ready.future, 1);
    expect(third.ready.isCompleted, isFalse);
    second.release();
    await second.done;
    expect(await third.ready.future, 1);
    third.release();
    await third.done;
    expect(calls, 1);
  });

  test(
    'cancelling a queued caller leaves later waiters and active leases intact',
    () async {
      final pool = await _own(
        runtime,
        Pool.create(Task.succeed(1), maxSize: 1),
      );
      final held = _Borrow(runtime, pool.acquire());
      await held.ready.future;
      final controller = CancellationController();
      final cancelled = runtime.run(pool.acquire(), signal: controller.signal);
      final next = _Borrow(runtime, pool.acquire());
      controller.requestCancel(reason: 'waiting');
      expect(
        _causes(await cancelled).whereType<CauseCancelled>().single.reason,
        'waiting',
      );
      expect(next.ready.isCompleted, isFalse);
      held.release();
      await held.done;
      expect(await next.ready.future, 1);
      next.release();
      await next.done;
    },
  );

  test(
    'maxSize allows different resources and reuses only a returned slot',
    () async {
      var calls = 0;
      final pool = await _own(
        runtime,
        Pool.create(Task.sync(() => ++calls), maxSize: 2),
      );
      final first = _Borrow(runtime, pool.acquire());
      final second = _Borrow(runtime, pool.acquire());
      expect(await first.ready.future, 1);
      expect(await second.ready.future, 2);
      final third = _Borrow(runtime, pool.acquire());
      expect(third.ready.isCompleted, isFalse);
      first.release();
      await first.done;
      expect(await third.ready.future, 1);
      second.release();
      third.release();
      await Future.wait([second.done, third.done]);
      expect(calls, 2);
    },
  );

  test('use waits for body finalizers before returning its slot', () async {
    final cleaning = Completer<void>();
    final release = Completer<void>();
    addTearDown(() {
      if (!release.isCompleted) release.complete();
    });
    final pool = await _own(runtime, Pool.create(Task.succeed(1), maxSize: 1));
    final first = runtime.run(
      pool.use(
        (value) => Task.addFinalizer(
          (_) =>
              Task.sync(() => cleaning.complete())
                  .flatMap((_) => _future(release.future)),
        ).map((_) => value + 1),
      ),
    );
    await cleaning.future;
    final next = _Borrow(runtime, pool.acquire());
    expect(next.ready.isCompleted, isFalse);
    release.complete();
    expect(_value(await first), 2);
    expect(await next.ready.future, 1);
    next.release();
    await next.done;
  });

  test(
    'zero idle timeout waits cleanup before starting another acquisition',
    () async {
      var calls = 0;
      final cleaning = Completer<void>();
      final release = Completer<void>();
      addTearDown(() {
        if (!release.isCompleted) release.complete();
      });
      final pool = await _own(
        runtime,
        Pool.create(
          Task.acquireRelease(
            Task.sync(() => ++calls),
            release: (value, _) => value == 1
                ? Task.sync(() => cleaning.complete())
                      .flatMap((_) => _future(release.future))
                : Task.succeed(null),
          ),
          maxSize: 1,
          idleTimeout: Duration.zero,
        ),
      );
      final first = runtime.run(pool.acquire());
      await cleaning.future;
      final next = _Borrow(runtime, pool.acquire());
      expect(calls, 1);
      expect(next.ready.isCompleted, isFalse);
      release.complete();
      expect(_value(await first), 1);
      expect(await next.ready.future, 2);
      next.release();
      await next.done;
    },
  );

  test(
    'idle TTL resets on reuse and cleanup holds capacity until completion',
    () async {
      var calls = 0;
      final cleaning = Completer<void>();
      final release = Completer<void>();
      addTearDown(() {
        if (!release.isCompleted) release.complete();
      });
      final pool = await _own(
        runtime,
        Pool.create(
          Task.acquireRelease(
            Task.sync(() => ++calls),
            release: (value, _) => value == 1
                ? Task.sync(() => cleaning.complete())
                      .flatMap((_) => _future(release.future))
                : Task.succeed(null),
          ),
          maxSize: 1,
          idleTimeout: ttl,
        ),
      );
      expect(_value(await runtime.run(pool.acquire())), 1);
      clock.advance(const Duration(seconds: 9));
      final held = _Borrow(runtime, pool.acquire());
      expect(await held.ready.future, 1);
      expect(clock.pendingCount, 0);
      clock.advance(ttl);
      expect(cleaning.isCompleted, isFalse);
      held.release();
      await held.done;
      clock.advance(ttl);
      await cleaning.future;
      final next = _Borrow(runtime, pool.acquire());
      expect(next.ready.isCompleted, isFalse);
      expect(calls, 1);
      release.complete();
      expect(await next.ready.future, 2);
      next.release();
      await next.done;
    },
  );

  test(
    'invalidating an active value keeps its slot until the lease returns',
    () async {
      var calls = 0;
      final released = <int>[];
      final pool = await _own(
        runtime,
        Pool.create(
          Task.acquireRelease(
            Task.sync(() => ++calls),
            release: (value, _) => Task.sync(() => released.add(value)),
          ),
          maxSize: 1,
        ),
      );
      final first = _Borrow(runtime, pool.acquire());
      final value = await first.ready.future;
      expect(await runtime.run(pool.invalidate(value)), isA<ExitSuccess>());
      final next = _Borrow(runtime, pool.acquire());
      expect(next.ready.isCompleted, isFalse);
      expect(released, isEmpty);
      first.release();
      await first.done;
      expect(released, [1]);
      expect(await next.ready.future, 2);
      next.release();
      await next.done;
    },
  );

  test(
    'an expiry during queued delivery preserves capacity and FIFO order',
    () async {
      var calls = 0;
      final cleaning = Completer<void>();
      final release = Completer<void>();
      addTearDown(() {
        if (!release.isCompleted) release.complete();
      });
      final pool = await _own(
        runtime,
        Pool.create(
          Task.acquireRelease(
            Task.sync(() => ++calls),
            release: (value, _) => value == 1
                ? Task.sync(() => cleaning.complete())
                      .flatMap((_) => _future(release.future))
                : Task.succeed(null),
          ),
          maxSize: 1,
          idleTimeout: ttl,
        ),
      );
      final first = _Borrow(runtime, pool.acquire());
      await first.ready.future;
      var armed = false;
      var expired = false;
      final second = runZoned(
        () => _Borrow(runtime, pool.acquire()),
        zoneSpecification: ZoneSpecification(
          scheduleMicrotask: (self, parent, zone, callback) {
            if (armed) {
              armed = false;
              expired = true;
              clock.advance(ttl);
            }
            parent.scheduleMicrotask(zone, callback);
          },
        ),
      );
      final third = _Borrow(runtime, pool.acquire());
      armed = true;
      first.release();
      await cleaning.future;
      expect(expired, isTrue);
      expect(calls, 1);
      expect(second.ready.isCompleted, isFalse);
      expect(third.ready.isCompleted, isFalse);
      release.complete();
      expect(await second.ready.future, 2);
      expect(third.ready.isCompleted, isFalse);
      second.release();
      await second.done;
      expect(await third.ready.future, 2);
      third.release();
      await Future.wait([first.done, third.done]);
      expect(calls, 2);
    },
  );

  test('cancellation at queued delivery returns the granted slot', () async {
    var calls = 0;
    final pool = await _own(
      runtime,
      Pool.create(Task.sync(() => ++calls), maxSize: 1),
    );
    final held = _Borrow(runtime, pool.acquire());
    await held.ready.future;
    final controller = CancellationController();
    var armed = false;
    var bodyCalls = 0;
    final cancelled = runZoned(
      () => runtime.run(
        pool.acquire().map((_) => bodyCalls++),
        signal: controller.signal,
      ),
      zoneSpecification: ZoneSpecification(
        scheduleMicrotask: (self, parent, zone, callback) {
          if (armed) {
            armed = false;
            controller.requestCancel(reason: 'delivery');
          }
          parent.scheduleMicrotask(zone, callback);
        },
      ),
    );
    final next = _Borrow(runtime, pool.acquire());
    armed = true;
    held.release();
    expect(
      _causes(await cancelled).whereType<CauseCancelled>().single.reason,
      'delivery',
    );
    expect(bodyCalls, 0);
    expect(await next.ready.future, 1);
    next.release();
    await Future.wait([held.done, next.done]);
    expect(calls, 1);
  });

  test(
    'cancellation before publishing an acquired value releases it',
    () async {
      final controller = CancellationController();
      var calls = 0;
      final released = <int>[];
      final pool = await _own(
        runtime,
        Pool.create(
          Task.acquireRelease(
            Task.sync(() => ++calls),
            release: (value, _) => Task.sync(() => released.add(value)),
          ).map((value) {
            if (value == 1) controller.requestCancel(reason: 'delivery');
            return value;
          }),
          maxSize: 1,
        ),
      );
      final exit = await runtime.run(pool.acquire(), signal: controller.signal);
      expect(
        _causes(exit).whereType<CauseCancelled>().single.reason,
        'delivery',
      );
      expect(released, [1]);
      expect(_value(await runtime.run(pool.acquire())), 2);
    },
  );

  test('invalidation matches identity rather than value equality', () async {
    var calls = 0;
    final released = <_Equal>[];
    final pool = await _own(
      runtime,
      Pool.create(
        Task.acquireRelease(
          Task.sync(() {
            calls++;
            return _Equal(1);
          }),
          release: (value, _) => Task.sync(() => released.add(value)),
        ),
        maxSize: 2,
      ),
    );
    final first = _Borrow(runtime, pool.acquire());
    final second = _Borrow(runtime, pool.acquire());
    final a = await first.ready.future;
    final b = await second.ready.future;
    expect(a == b, isTrue);
    expect(identical(a, b), isFalse);
    await runtime.run(pool.invalidate(_Equal(1)));
    await runtime.run(pool.invalidate(a));
    second.release();
    await second.done;
    expect(_value(await runtime.run(pool.acquire())), same(b));
    first.release();
    await first.done;
    expect(released, [same(a)]);
    expect(calls, 2);
  });

  test('invalidation cleans every slot containing the same object despite failures', () async {
    final value = Object();
    var acquired = 0;
    final entered = [Completer<void>(), Completer<void>()];
    final releases = [Completer<void>(), Completer<void>()];
    addTearDown(() {
      for (final release in releases) {
        if (!release.isCompleted) release.complete();
      }
    });
    final faults = [StateError('first'), StateError('second')];
    final acquire = Task.defer<Object, Never>(() {
      final index = acquired++;
      return Task.acquireRelease(
        Task.succeed(value),
        release: (_, _) =>
            Task.sync(() => entered[index].complete())
                .flatMap((_) => _future(releases[index].future))
                .flatMap((_) => Task.sync(() => throw faults[index])),
      );
    });
    final pool = await _own(
      runtime,
      Pool.create(acquire, maxSize: 2, initialSize: 2),
    );
    final invalidated = runtime.run(pool.invalidate(value));
    await Future.wait(entered.map((event) => event.future));
    releases[1].complete();
    await runtime.run(Task.yieldNow());
    releases[0].complete();
    expect(_defects(await invalidated), [same(faults[1]), same(faults[0])]);
    expect(reports, isEmpty);
    expect(await runtime.close(), isA<ExitSuccess>());
  });

  test(
    'invalidation attempts all selected cleanup after cancellation and failure',
    () async {
      final value = Object();
      final controller = CancellationController();
      final fault = StateError('cleanup');
      var calls = 0;
      final cleaned = <int>[];
      final pool = await _own(
        runtime,
        Pool.create(
          Task.defer(() {
            final index = calls++;
            return Task.acquireRelease(
              Task.succeed(value),
              release: (_, _) => Task.sync(() {
                cleaned.add(index);
                if (index == 0) {
                  controller.requestCancel(reason: 'invalidate');
                  throw fault;
                }
              }),
            );
          }),
          maxSize: 3,
          initialSize: 3,
        ),
      );
      final exit = await runtime.run(
        pool.invalidate(value),
        signal: controller.signal,
      );
      expect(cleaned.toSet(), {0, 1, 2});
      expect(_defects(exit), [same(fault)]);
      expect(
        _causes(exit).whereType<CauseCancelled>().single.reason,
        'invalidate',
      );
      expect(await runtime.close(), isA<ExitSuccess>());
      expect(reports, isEmpty);
    },
  );

  test('prewarm acquires distinct slots before publishing and returns them to idle', () async {
    var calls = 0;
    final pool = await _own(
      runtime,
      Pool.create(Task.sync(() => ++calls), maxSize: 3, initialSize: 3),
    );
    expect(calls, 3);
    final leases = [
      for (var i = 0; i < 3; i++) _Borrow(runtime, pool.acquire()),
    ];
    expect(
      (await Future.wait(leases.map((lease) => lease.ready.future))).toSet(),
      {1, 2, 3},
    );
    expect(calls, 3);
    for (final lease in leases) {
      lease.release();
    }
    await Future.wait(leases.map((lease) => lease.done));
  });

  test('prewarm with zero timeout follows the return policy', () async {
    var calls = 0;
    final released = <int>[];
    final pool = await _own(
      runtime,
      Pool.create(
        Task.acquireRelease(
          Task.sync(() => ++calls),
          release: (value, _) => Task.sync(() => released.add(value)),
        ),
        maxSize: 2,
        initialSize: 2,
        idleTimeout: Duration.zero,
      ),
    );
    expect(calls, 2);
    expect(released.toSet(), {1, 2});
    expect(_value(await runtime.run(pool.acquire())), 3);
  });

  test(
    'failed prewarm rolls back and retains full cleanup causes exactly once',
    () async {
      var calls = 0;
      final fault = StateError('rollback');
      final release = Completer<void>();
      final cleaning = Completer<void>();
      addTearDown(() {
        if (!release.isCompleted) release.complete();
      });
      final acquire = Task.defer<int, String>(() {
        if (++calls == 2) return Task.fail('acquire');
        return Task.acquireRelease(
          Task.succeed(calls),
          release: (_, _) =>
              Task.sync(() => cleaning.complete())
                  .flatMap((_) => _future(release.future))
                  .flatMap(
                    (_) => Task.failCause(
                      Cause<Never>.combine([
                        const Cause.cancelled('cleanup'),
                        Cause.defect(fault, StackTrace.current),
                      ]),
                    ),
                  ),
        );
      });
      var completed = false;
      final running = runtime
          .run(Pool.create(acquire, maxSize: 2, initialSize: 2).exit())
          .then((exit) {
            completed = true;
            return exit;
          });
      await cleaning.future;
      expect(completed, isFalse);
      release.complete();
      final exit = _value(await running);
      expect(
        _causes(exit).whereType<CauseFailure<String>>().single.error,
        'acquire',
      );
      expect(
        _causes(exit).whereType<CauseCancelled>().single.reason,
        'cleanup',
      );
      expect(_defects(exit), [same(fault)]);
      expect(reports, isEmpty);
    },
  );

  test(
    'prewarm waiting is cancellable after pool ownership is registered',
    () async {
      var calls = 0;
      final pending = _Gate<int, Never>();
      final released = <int>[];
      final acquire = Task.defer<int, Never>(
        () => ++calls == 1
            ? Task.acquireRelease(
                Task.succeed(1),
                release: (value, _) => Task.sync(() => released.add(value)),
              )
            : pending.task,
      );
      final controller = CancellationController();
      final running = runtime.run(
        Pool.create(acquire, maxSize: 2, initialSize: 2),
        signal: controller.signal,
      );
      await pending.started.future;
      controller.requestCancel(reason: 'startup');
      final exit = await running.timeout(const Duration(seconds: 1));
      expect(
        _causes(exit).whereType<CauseCancelled>().single.reason,
        'startup',
      );
      expect(pending.cancelled, isTrue);
      expect(released, [1]);
    },
  );

  test(
    'owner close rejects queued foreign callers and waits active leases',
    () async {
      final foreign = Runtime();
      addTearDown(foreign.close);
      var releases = 0;
      final pool = await _own(
        runtime,
        Pool.create(
          Task.acquireRelease(
            Task.succeed(1),
            release: (_, _) => Task.sync(() {
              releases++;
            }),
          ),
          maxSize: 1,
        ),
      );
      final held = _Borrow(foreign, pool.acquire());
      addTearDown(held.release);
      await held.ready.future;
      final waiting = foreign.run(pool.acquire());
      var finished = false;
      final closing = runtime.close().then((exit) {
        finished = true;
        return exit;
      });
      expect(_defects(await waiting).single, isA<StateError>());
      expect(finished, isFalse);
      expect(releases, 0);
      held.release();
      await held.done;
      expect(await closing, isA<ExitSuccess>());
      expect(releases, 1);
    },
  );

  test(
    'failed acquisition frees its slot only after cleanup finishes',
    () async {
      var calls = 0;
      final fail = _Gate<void, String>();
      final cleaning = Completer<void>();
      final finish = Completer<void>();
      final fault = StateError('cleanup');
      addTearDown(() {
        if (!finish.isCompleted) finish.complete();
      });
      final pool = await _own(
        runtime,
        Pool.create(
          Task.defer<int, String>(
            () => ++calls == 1
                ? Task.addFinalizer(
                    (_) =>
                        Task.sync(() => cleaning.complete())
                            .flatMap((_) => _future(finish.future))
                            .flatMap((_) => Task.sync(() => throw fault)),
                  ).withError<String>().flatMap((_) => fail.task).map((_) => 1)
                : Task.succeed(calls),
          ),
          maxSize: 1,
        ),
      );
      final first = runtime.run(pool.acquire());
      await fail.started.future;
      final next = _Borrow(runtime, pool.acquire());
      fail.complete(const Exit.failure(Cause.failure('acquire')));
      await cleaning.future;
      expect(next.ready.isCompleted, isFalse);
      expect(calls, 1);
      finish.complete();
      final exit = await first;
      expect(
        _causes(exit).whereType<CauseFailure<String>>().single.error,
        'acquire',
      );
      expect(_defects(exit), [same(fault)]);
      expect(await next.ready.future, 2);
      next.release();
      await next.done;
      expect(reports, isEmpty);
    },
  );

  test(
    'cancelled native acquisition waits settlement before reusing capacity',
    () async {
      var calls = 0;
      final started = Completer<void>();
      final finish = Completer<int>();
      final fault = StateError('native acquisition');
      addTearDown(() {
        if (!finish.isCompleted) finish.complete(1);
      });
      final pool = await _own(
        runtime,
        Pool.create(
          Task.defer<int, String>(() {
            if (++calls > 1) return Task.succeed(calls);
            return Task.fromFuture(() {
              started.complete();
              return finish.future;
            }, onError: (_, _) => 'mapped');
          }),
          maxSize: 1,
        ),
      );
      final controller = CancellationController();
      var finished = false;
      final first = runtime.run(pool.acquire(), signal: controller.signal).then(
        (exit) {
          finished = true;
          return exit;
        },
      );
      await started.future;
      final next = _Borrow(runtime, pool.acquire());
      controller.requestCancel(reason: 'native');
      await runtime.run(Task.yieldNow());
      expect(finished, isFalse);
      expect(next.ready.isCompleted, isFalse);
      expect(calls, 1);
      finish.completeError(fault, StackTrace.current);
      final exit = await first;
      expect(_causes(exit).whereType<CauseCancelled>().single.reason, 'native');
      expect(_causes(exit).whereType<CauseFailure>(), isEmpty);
      expect(_defects(exit), [same(fault)]);
      expect(await next.ready.future, 2);
      next.release();
      await next.done;
    },
  );

  test(
    'acquisition and cleanup retain creation services and task locals',
    () async {
      final key = ServiceKey<String>('service');
      final local = TaskLocal('default');
      final capturedClock = TestClock();
      final cleaned = <String>[];
      final acquire = Task.service(key).flatMap(
        (service) => local.get().flatMap(
          (value) => Task.acquireRelease(
            Task.succeed('$service/$value'),
            release: (_, _) => Task.service(key).flatMap(
              (service) =>
                  local.get().map((value) => cleaned.add('$service/$value')),
            ),
          ),
        ),
      );
      final pool = await _own(
        runtime,
        local
            .locally(
              'created',
              Pool.create(acquire, maxSize: 1, idleTimeout: ttl),
            )
            .provideContext(
              Context.from([
                key.bind('creator'),
                Clock.key.bind(capturedClock),
              ]),
            ),
      );
      final borrowing = local
          .locally('borrowed', pool.acquire())
          .provideContext(Context.from([key.bind('borrower')]));
      final value = _value(await runtime.run(borrowing));
      expect(value, 'creator/created');
      expect(clock.pendingCount, 0);
      expect(capturedClock.pendingCount, 1);
      await runtime.run(pool.invalidate(value));
      expect(cleaned, ['creator/created']);
    },
  );

  test('idle cleanup failure reports once and releases capacity', () async {
    var calls = 0;
    final fault = StateError('idle');
    final reported = Completer<void>();
    final owner = Runtime(
      context: Context.from([Clock.key.bind(clock)]),
      onUnhandledError: (cause) {
        reports.add(cause);
        reported.complete();
      },
    );
    addTearDown(owner.close);
    final pool = await _own(
      owner,
      Pool.create(
        Task.acquireRelease(
          Task.sync(() => ++calls),
          release: (value, _) =>
              value == 1 ? Task.sync(() => throw fault) : Task.succeed(null),
        ),
        maxSize: 1,
        idleTimeout: ttl,
      ),
    );
    await runtime.run(pool.acquire());
    clock.advance(ttl);
    await reported.future;
    expect(
      reports.single,
      isA<CauseDefect>().having((cause) => cause.error, 'error', same(fault)),
    );
    expect(_value(await runtime.run(pool.acquire())), 2);
    expect(await owner.close(), isA<ExitSuccess>());
    expect(reports, hasLength(1));
  });

  test('a foreign lease cannot wait for its owning runtime to close', () async {
    final foreign = Runtime();
    addTearDown(foreign.close);
    final pool = await _own(runtime, Pool.create(Task.succeed(1), maxSize: 1));
    final exit = await foreign
        .run(
          pool.use(
            (_) => Task.fromFuture(
              runtime.close,
              onError: Error.throwWithStackTrace,
            ),
          ),
        )
        .timeout(const Duration(seconds: 1));
    expect(_defects(_value(exit)).single, isA<StateError>());
    expect(_value(await runtime.run(pool.acquire())), 1);
  });

  test(
    'nullable and covariantly widened values preserve the pool API',
    () async {
      final Pool<Object?, Object?> pool = await _own(
        runtime,
        Pool.create(Task.succeed<int?>(null), maxSize: 1),
      );
      expect(
        _value(await runtime.run(pool.use((value) => Task.succeed('$value')))),
        'null',
      );
      expect(await runtime.run(pool.invalidate(null)), isA<ExitSuccess>());
      expect(_value(await runtime.run(pool.acquire())), isNull);
    },
  );
}

final class _Equal {
  _Equal(this.value);
  final int value;
  @override
  bool operator ==(Object other) => other is _Equal && other.value == value;
  @override
  int get hashCode => value;
}

Future<T> _own<T, E>(Runtime runtime, Task<T, E> create) {
  final ready = Completer<T>();
  unawaited(
    runtime
        .run(
          create.flatMap(
            (value) => Task.sync(() => ready.complete(value))
                .flatMap((_) => Task.fromCallback<void, Never>((_) => () {})),
          ),
        )
        .then((exit) {
          if (!ready.isCompleted) {
            ready.completeError(StateError('Creation failed: $exit'));
          }
        }),
  );
  return ready.future;
}

final class _Borrow<T, E> {
  _Borrow(Runtime runtime, Task<T, E> acquire) {
    done = runtime.run(
      acquire.flatMap(
        (value) =>
            Task.sync(() => ready.complete(value))
                .flatMap((_) => hold.task)
                .map((_) => value),
      ),
    );
  }
  final ready = Completer<T>();
  final hold = _Gate<void, Never>();
  late Future<Exit<T, E>> done;
  void release() => hold.succeed(null);
}

final class _Gate<T, E> {
  final started = Completer<void>();
  late void Function(Exit<T, E>) complete;
  var cancelled = false;
  Task<T, E> get task => Task.fromCallback((complete) {
    this.complete = complete;
    started.complete();
    return () {
      cancelled = true;
    };
  });
  void succeed(T value) => complete(Exit.success(value));
}

Task<void, Never> _future(Future<void> future) => Task.fromFuture<void, Never>(
  () => future,
  onError: Error.throwWithStackTrace,
);
T _value<T, E>(Exit<T, E> exit) => switch (exit) {
  ExitSuccess(:final value) => value,
  ExitFailure(:final cause) => throw StateError('Unexpected failure: $cause'),
};
List<Cause<E>> _causes<T, E>(Exit<T, E> exit) => switch (exit) {
  ExitSuccess() => [],
  ExitFailure(cause: CauseGroup(:final causes)) => causes,
  ExitFailure(:final cause) => [cause],
};
List<Object> _defects<T, E>(Exit<T, E> exit) =>
    _causes(exit).whereType<CauseDefect>().map((cause) => cause.error).toList();
