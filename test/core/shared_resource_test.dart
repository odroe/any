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
    'creation is lazy, independent, and does not start acquisition',
    () async {
      var acquisitions = 0;
      final create = SharedResource.create(Task.sync(() => ++acquisitions));
      expect(acquisitions, 0);
      final first = await _own(runtime, create);
      final second = await _own(runtime, create);
      expect(identical(first, second), isFalse);
      expect(acquisitions, 0);
      expect(_value(await runtime.run(first.acquire())), 1);
      expect(_value(await runtime.run(second.acquire())), 2);
    },
  );

  test('negative timeout is an execution defect', () async {
    final task = SharedResource.create(
      Task.succeed(1),
      idleTimeout: const Duration(microseconds: -1),
    );
    expect(_defects(await runtime.run(task)).single, isA<ArgumentError>());
  });

  test('a resource returned from a finished owner is closed', () async {
    final resource = _value(
      await runtime.run(SharedResource.create(Task.succeed(1))),
    );
    expect(
      _defects(await runtime.run(resource.acquire())).single,
      isA<StateError>(),
    );
    expect(
      _defects(await runtime.run(resource.invalidate())).single,
      isA<StateError>(),
    );
  });

  test(
    'overlapping leases share acquisition and final lease owns cleanup',
    () async {
      final events = <String>[];
      final acquiring = _Gate<int, Never>();
      final resource = await _own(
        runtime,
        SharedResource.create(
          Task.acquireRelease(
            Task.sync(() => events.add('acquire'))
                .flatMap((_) => acquiring.task),
            release: (_, _) => Task.sync(() => events.add('release')),
          ),
        ),
      );
      final first = _Borrow(runtime, resource.acquire());
      final second = _Borrow(runtime, resource.acquire());
      await acquiring.started.future;
      acquiring.succeed(42);
      expect(await first.ready.future, 42);
      expect(await second.ready.future, 42);
      first.release();
      expect(_value(await first.done), 42);
      expect(events, ['acquire']);
      second.release();
      expect(_value(await second.done), 42);
      expect(events, ['acquire', 'release']);
    },
  );

  test('nullable resources are valid and zero timeout reacquires', () async {
    var calls = 0;
    var releases = 0;
    final resource = await _own(
      runtime,
      SharedResource.create(
        Task.acquireRelease(
          Task.sync<int?>(() {
            calls++;
            return null;
          }),
          release: (_, _) => Task.sync(() {
            releases++;
          }),
        ),
      ),
    );
    expect(_value(await runtime.run(resource.acquire())), isNull);
    expect(_value(await runtime.run(resource.acquire())), isNull);
    expect((calls, releases), (2, 2));
  });

  test('leases last for the calling scope and cleanup is LIFO', () async {
    final events = <String>[];
    final resource = await _own(
      runtime,
      SharedResource.create(
        Task.acquireRelease(
          Task.succeed(1),
          release: (_, _) => Task.sync(() => events.add('first')),
        ).flatMap(
          (_) => Task.acquireRelease(
            Task.succeed(2),
            release: (_, _) => Task.sync(() => events.add('second')),
          ),
        ),
      ),
    );
    final exit = await runtime.run(
      resource.acquire().scoped().flatMap(
        (_) => Task.sync(() => events.add('after')),
      ),
    );
    expect(exit, isA<ExitSuccess>());
    expect(events, ['second', 'first', 'after']);
  });

  test(
    'failed acquisition waits cleanup, preserves causes once, and retries',
    () async {
      final entered = Completer<void>();
      final release = Completer<void>();
      addTearDown(() {
        if (!release.isCompleted) release.complete();
      });
      final defect = StateError('release');
      var calls = 0;
      final resource = await _own(
        runtime,
        SharedResource.create(
          Task.defer<int, String>(() {
            if (++calls > 1) return Task.succeed(2);
            return Task.addFinalizer(
              (_) =>
                  Task.sync(() => entered.complete())
                      .flatMap((_) => _future(release.future))
                      .flatMap((_) => Task.sync(() => throw defect)),
            ).withError<String>().flatMap((_) => Task.fail('acquire'));
          }),
        ),
      );
      var completed = false;
      final first = runtime.run(resource.acquire()).then((exit) {
        completed = true;
        return exit;
      });
      await entered.future;
      expect(completed, isFalse);
      release.complete();
      final exit = await first;
      expect(
        _causes(exit).whereType<CauseFailure<String>>().single.error,
        'acquire',
      );
      expect(_defects(exit), [same(defect)]);
      expect(_value(await runtime.run(resource.acquire())), 2);
    },
  );

  test('cancelling one waiter does not stop shared acquisition', () async {
    final gate = _Gate<int, Never>();
    final resource = await _own(runtime, SharedResource.create(gate.task));
    final controller = CancellationController();
    final first = runtime.run(resource.acquire(), signal: controller.signal);
    final second = _Borrow(runtime, resource.acquire());
    await gate.started.future;
    controller.requestCancel(reason: 'first');
    expect(
      _causes(await first).whereType<CauseCancelled>().single.reason,
      'first',
    );
    expect(gate.cancelled, isFalse);
    gate.succeed(7);
    expect(await second.ready.future, 7);
    second.release();
    await second.done;
  });

  test(
    'last waiter cancellation waits cleanup and next caller starts fresh',
    () async {
      final gate = _Gate<int, Never>();
      final cleaning = Completer<void>();
      final release = Completer<void>();
      addTearDown(() {
        if (!release.isCompleted) release.complete();
      });
      var calls = 0;
      final resource = await _own(
        runtime,
        SharedResource.create(
          Task.defer<int, Never>(() {
            if (++calls > 1) return Task.succeed(2);
            return Task.addFinalizer(
              (_) =>
                  Task.sync(() => cleaning.complete())
                      .flatMap((_) => _future(release.future)),
            ).flatMap((_) => gate.task);
          }),
        ),
      );
      final controller = CancellationController();
      var completed = false;
      final first = runtime
          .run(resource.acquire(), signal: controller.signal)
          .then((exit) {
            completed = true;
            return exit;
          });
      await gate.started.future;
      controller.requestCancel();
      await cleaning.future;
      expect(gate.cancelled, isTrue);
      expect(completed, isFalse);
      expect(_value(await runtime.run(resource.acquire())), 2);
      release.complete();
      expect(_causes(await first).whereType<CauseCancelled>(), hasLength(1));
    },
  );

  test(
    'cancelled acquisition preserves cleanup defect without reporting it',
    () async {
      final defect = StateError('cleanup');
      final gate = _Gate<int, Never>();
      final resource = await _own(
        runtime,
        SharedResource.create(
          Task.addFinalizer((_) => Task.sync(() => throw defect))
              .flatMap((_) => gate.task),
        ),
      );
      final controller = CancellationController();
      final result = runtime.run(resource.acquire(), signal: controller.signal);
      await gate.started.future;
      controller.requestCancel();
      expect(_defects(await result), [same(defect)]);
      expect(reports, isEmpty);
    },
  );

  test(
    'cancel after acquisition completion cannot leak a handed-off lease',
    () async {
      final controller = CancellationController();
      var releases = 0;
      var body = false;
      final resource = await _own(
        runtime,
        SharedResource.create(
          Task.acquireRelease(
            Task.succeed(1),
            release: (_, _) => Task.sync(() {
              releases++;
            }),
          ).map((value) {
            controller.requestCancel();
            return value;
          }),
        ),
      );
      final exit = await runtime.run(
        resource.acquire().map((_) {
          body = true;
        }),
        signal: controller.signal,
      );
      expect(_causes(exit).whereType<CauseCancelled>(), hasLength(1));
      expect(body, isFalse);
      expect(releases, 1);
      expect(clock.pendingCount, 0);
    },
  );

  test(
    'invalidation keeps active leases and starts an independent generation',
    () async {
      var calls = 0;
      final released = <int>[];
      final resource = await _own(
        runtime,
        SharedResource.create(
          Task.acquireRelease(
            Task.sync(() => ++calls),
            release: (value, _) => Task.sync(() => released.add(value)),
          ),
          idleTimeout: ttl,
        ),
      );
      final first = _Borrow(runtime, resource.acquire());
      expect(await first.ready.future, 1);
      expect(await runtime.run(resource.invalidate()), isA<ExitSuccess>());
      final second = _Borrow(runtime, resource.acquire());
      expect(await second.ready.future, 2);
      expect(released, isEmpty);
      first.release();
      await first.done;
      expect(released, [1]);
      expect(clock.pendingCount, 0);
      second.release();
      await second.done;
      expect(clock.pendingCount, 1);
    },
  );

  test(
    'cancellation while invalidating waits and retains cleanup defects',
    () async {
      final controller = CancellationController();
      final defect = StateError('invalidate cleanup');
      final resource = await _own(
        runtime,
        SharedResource.create(
          Task.acquireRelease(
            Task.succeed(1),
            release: (_, _) => Task.sync(() {
              controller.requestCancel();
              throw defect;
            }),
          ),
          idleTimeout: ttl,
        ),
      );
      await runtime.run(resource.acquire());
      final exit = await runtime.run(
        resource.invalidate(),
        signal: controller.signal,
      );
      expect(_causes(exit).whereType<CauseCancelled>(), hasLength(1));
      expect(_defects(exit), [same(defect)]);
      expect(reports, isEmpty);
    },
  );

  test(
    'cancellation during result delivery preserves another active lease',
    () async {
      final gate = _Gate<int, Never>();
      var releases = 0;
      var bodyCalls = 0;
      final resource = await _own(
        runtime,
        SharedResource.create(
          Task.acquireRelease(
            gate.task,
            release: (_, _) => Task.sync(() {
              releases++;
            }),
          ),
        ),
      );
      final keeper = _Borrow(runtime, resource.acquire());
      await gate.started.future;
      final controller = CancellationController();
      var armed = false;
      var intercepted = false;
      final cancelled = runZoned(
        () => runtime.run(
          resource.acquire().map((_) {
            bodyCalls++;
          }),
          signal: controller.signal,
        ),
        zoneSpecification: ZoneSpecification(
          scheduleMicrotask: (self, parent, zone, callback) {
            if (armed) {
              armed = false;
              intercepted = true;
              controller.requestCancel(reason: 'delivery');
            }
            parent.scheduleMicrotask(zone, callback);
          },
        ),
      );
      armed = true;
      gate.succeed(1);
      expect(
        _causes(await cancelled).whereType<CauseCancelled>(),
        hasLength(1),
      );
      expect(intercepted, isTrue);
      expect(bodyCalls, 0);
      expect(await keeper.ready.future, 1);
      expect(releases, 0);
      keeper.release();
      await keeper.done;
      expect(releases, 1);
    },
  );

  test(
    'invalidation preserves pending callers of the old generation',
    () async {
      final firstGate = _Gate<int, Never>();
      var calls = 0;
      final resource = await _own(
        runtime,
        SharedResource.create(
          Task.defer(() => ++calls == 1 ? firstGate.task : Task.succeed(2)),
        ),
      );
      final first = runtime.run(resource.acquire());
      await firstGate.started.future;
      await runtime.run(resource.invalidate());
      expect(_value(await runtime.run(resource.acquire())), 2);
      expect(firstGate.cancelled, isFalse);
      firstGate.succeed(1);
      expect(_value(await first), 1);
    },
  );

  test('idle reuse cancels the timer and resets the idle deadline', () async {
    var calls = 0;
    final released = <int>[];
    final cleaned = Completer<void>();
    final resource = await _own(
      runtime,
      SharedResource.create(
        Task.acquireRelease(
          Task.sync(() => ++calls),
          release: (value, _) => Task.sync(() {
            released.add(value);
            if (!cleaned.isCompleted) cleaned.complete();
          }),
        ),
        idleTimeout: ttl,
      ),
    );
    expect(_value(await runtime.run(resource.acquire())), 1);
    expect(clock.pendingCount, 1);
    clock.advance(const Duration(seconds: 9));
    final borrower = _Borrow(runtime, resource.acquire());
    expect(await borrower.ready.future, 1);
    expect(clock.pendingCount, 0);
    clock.advance(const Duration(seconds: 30));
    expect(released, isEmpty);
    borrower.release();
    await borrower.done;
    clock.advance(const Duration(seconds: 9));
    expect(released, isEmpty);
    clock.advance(const Duration(seconds: 1));
    await cleaned.future;
    expect(released, [1]);
    expect(_value(await runtime.run(resource.acquire())), 2);
  });

  test(
    'invalidating an idle generation waits cleanup and removes its timer',
    () async {
      final entered = Completer<void>();
      final release = Completer<void>();
      addTearDown(() {
        if (!release.isCompleted) release.complete();
      });
      final resource = await _own(
        runtime,
        SharedResource.create(
          Task.acquireRelease(
            Task.succeed(1),
            release: (_, _) =>
                Task.sync(() => entered.complete())
                    .flatMap((_) => _future(release.future)),
          ),
          idleTimeout: ttl,
        ),
      );
      await runtime.run(resource.acquire());
      var completed = false;
      final invalidated = runtime.run(resource.invalidate()).then((exit) {
        completed = true;
        return exit;
      });
      await entered.future;
      expect(clock.pendingCount, 0);
      expect(completed, isFalse);
      release.complete();
      expect(await invalidated, isA<ExitSuccess>());
    },
  );

  test('acquisition cannot await its owning runtime closing', () async {
    final foreign = Runtime();
    addTearDown(foreign.close);
    final escape = Completer<Exit<void, Never>>();
    addTearDown(() {
      if (!escape.isCompleted) escape.complete(const Exit.success(null));
    });
    final resource = await _own(
      runtime,
      SharedResource.create(
        Task.fromFuture<Exit<void, Never>, Never>(
          () => Future.any([runtime.close(), escape.future]),
          onError: Error.throwWithStackTrace,
        ),
      ),
    );
    final exit = await foreign
        .run(resource.acquire())
        .timeout(const Duration(seconds: 1));
    expect(_defects(_value(exit)).single, isA<StateError>());
    expect(_value(await runtime.run(Task.succeed(2))), 2);
  });

  test('a foreign lease cannot await its owning runtime closing', () async {
    final foreign = Runtime();
    addTearDown(foreign.close);
    final escape = Completer<Exit<void, Never>>();
    addTearDown(() {
      if (!escape.isCompleted) escape.complete(const Exit.success(null));
    });
    final resource = await _own(
      runtime,
      SharedResource.create(Task.succeed(1)),
    );
    final exit = await foreign
        .run(
          resource.acquire().flatMap(
            (_) => Task.fromFuture<Exit<void, Never>, Never>(
              () => Future.any([runtime.close(), escape.future]),
              onError: Error.throwWithStackTrace,
            ),
          ),
        )
        .timeout(const Duration(seconds: 1));
    expect(_defects(_value(exit)).single, isA<StateError>());
    expect(_value(await runtime.run(Task.succeed(2))), 2);
  });

  test(
    'releasing one lease does not remove another lease dependency',
    () async {
      final foreign = Runtime();
      addTearDown(foreign.close);
      final escape = Completer<Exit<void, Never>>();
      addTearDown(() {
        if (!escape.isCompleted) escape.complete(const Exit.success(null));
      });
      final attempted = <Exit<void, Never>>[];
      final resource = await _own(
        runtime,
        SharedResource.create(Task.succeed(1)),
      );
      final exit = await foreign
          .run(
            resource
                .acquire()
                .flatMap(
                  (_) => Task.addFinalizer(
                    (_) => Task.fromFuture<void, Never>(() async {
                      attempted.add(
                        await Future.any([runtime.close(), escape.future]),
                      );
                    }, onError: Error.throwWithStackTrace),
                  ),
                )
                .flatMap((_) => resource.acquire()),
          )
          .timeout(const Duration(seconds: 1));
      expect(_value(exit), 1);
      expect(_defects(attempted.single).single, isA<StateError>());
      final closed = await foreign.run(
        Task.fromFuture<Exit<void, Never>, Never>(
          runtime.close,
          onError: Error.throwWithStackTrace,
        ),
      );
      expect(_value(closed), isA<ExitSuccess>());
    },
  );

  test('cleanup cannot await the shared resource owner closing', () async {
    final foreign = Runtime();
    addTearDown(foreign.close);
    final escape = Completer<Exit<void, Never>>();
    addTearDown(() {
      if (!escape.isCompleted) escape.complete(const Exit.success(null));
    });
    final attempted = <Exit<void, Never>>[];
    final resource = await _own(
      runtime,
      SharedResource.create(
        Task.acquireRelease(
          Task.succeed(1),
          release: (_, _) => Task.fromFuture<void, Never>(() async {
            attempted.add(await Future.any([runtime.close(), escape.future]));
          }, onError: Error.throwWithStackTrace),
        ),
      ),
    );
    expect(
      _value(
        await foreign
            .run(resource.acquire())
            .timeout(const Duration(seconds: 1)),
      ),
      1,
    );
    expect(_defects(attempted.single).single, isA<StateError>());
    expect(_value(await runtime.run(Task.succeed(2))), 2);
  });

  test(
    'creation services, task locals, and clock remain fixed for borrowers',
    () async {
      final key = ServiceKey<String>('service');
      final local = TaskLocal('default');
      final capturedClock = TestClock();
      final seen = <String>[];
      final acquire = Task.service(key).flatMap(
        (service) => local.get().flatMap(
          (value) => Task.acquireRelease(
            Task.succeed('$service/$value'),
            release: (_, _) => Task.service(key).flatMap(
              (service) =>
                  local.get().map((value) => seen.add('$service/$value')),
            ),
          ),
        ),
      );
      final resource = await _own(
        runtime,
        local
            .locally(
              'created',
              SharedResource.create(acquire, idleTimeout: ttl),
            )
            .provideContext(
              Context.from([
                key.bind('creator'),
                Clock.key.bind(capturedClock),
              ]),
            ),
      );
      final borrowed = local
          .locally('borrowed', resource.acquire())
          .provideContext(Context.from([key.bind('borrower')]));
      expect(_value(await runtime.run(borrowed)), 'creator/created');
      expect(clock.pendingCount, 0);
      expect(capturedClock.pendingCount, 1);
      await runtime.run(resource.invalidate());
      expect(seen, ['creator/created']);
    },
  );

  test('owner close waits a foreign committed lease before releasing', () async {
    final borrowerRuntime = Runtime();
    addTearDown(borrowerRuntime.close);
    var released = false;
    final resource = await _own(
      runtime,
      SharedResource.create(
        Task.acquireRelease(
          Task.succeed(1),
          release: (_, _) => Task.sync(() {
            released = true;
          }),
        ),
      ),
    );
    final borrower = _Borrow(borrowerRuntime, resource.acquire());
    await borrower.ready.future;
    var closed = false;
    final closing = runtime.close().then((exit) {
      closed = true;
      return exit;
    });
    // This independent runtime execution is a deterministic scheduling barrier.
    await borrowerRuntime.run(Task.yieldNow());
    expect(closed, isFalse);
    expect(released, isFalse);
    expect(
      _defects(await borrowerRuntime.run(resource.acquire())).single,
      isA<StateError>(),
    );
    borrower.release();
    expect(_value(await borrower.done), 1);
    expect(await closing, isA<ExitSuccess>());
    expect(released, isTrue);
  });

  test('owner close stops pending foreign acquisition without waiting on its scope', () async {
    final borrowerRuntime = Runtime();
    addTearDown(borrowerRuntime.close);
    final gate = _Gate<int, Never>();
    var releases = 0;
    final resource = await _own(
      runtime,
      SharedResource.create(
        Task.addFinalizer(
          (_) => Task.sync(() {
            releases++;
          }),
        ).flatMap((_) => gate.task),
      ),
    );
    final borrow = borrowerRuntime.run(resource.acquire());
    await gate.started.future;
    expect(await runtime.close(), isA<ExitSuccess>());
    expect(await borrow, isA<ExitFailure>());
    expect(gate.cancelled, isTrue);
    expect(releases, 1);
  });

  test('runtime closes its borrowers before shared resource cleanup', () async {
    final events = <String>[];
    final resource = await _own(
      runtime,
      SharedResource.create(
        Task.acquireRelease(
          Task.succeed(1),
          release: (_, _) => Task.sync(() => events.add('resource')),
        ),
      ),
    );
    final entered = Completer<void>();
    final borrowing = runtime.run(
      resource.acquire().flatMap(
        (_) =>
            Task.addFinalizer((_) => Task.sync(() => events.add('borrower')))
                .flatMap((_) => Task.sync(() => entered.complete()))
                .flatMap((_) => _pending<void>()),
      ),
    );
    await entered.future;
    expect(await runtime.close(), isA<ExitSuccess>());
    await borrowing;
    expect(events, ['borrower', 'resource']);
    expect(clock.pendingCount, 0);
  });

  test(
    'foreground release defects propagate once without background reporting',
    () async {
      final defect = StateError('release');
      final resource = await _own(
        runtime,
        SharedResource.create(
          Task.acquireRelease(
            Task.succeed(1),
            release: (_, _) => Task.sync(() => throw defect),
          ),
        ),
      );
      expect(_defects(await runtime.run(resource.acquire())), [same(defect)]);
      expect(reports, isEmpty);
      expect(await runtime.close(), isA<ExitSuccess>());
    },
  );

  test(
    'explicit cancellation from cleanup remains a cleanup failure',
    () async {
      final resource = await _own(
        runtime,
        SharedResource.create(
          Task.acquireRelease(
            Task.succeed(1),
            release: (_, _) =>
                Task.failCause(const Cause<Never>.cancelled('cleanup')),
          ),
        ),
      );
      final exit = await runtime.run(resource.acquire());
      expect(
        _causes(exit).whereType<CauseCancelled>().single.reason,
        'cleanup',
      );
    },
  );

  test('failed acquisition preserves ordered mixed cleanup causes', () async {
    final fault = StateError('cleanup defect');
    final resource = await _own(
      runtime,
      SharedResource.create(
        Task.addFinalizer(
          (_) => Task.failCause(
            Cause<Never>.combine([
              const Cause.cancelled('cleanup'),
              Cause.defect(fault, StackTrace.current),
            ]),
          ),
        ).withError<String>().flatMap<int>((_) => Task.fail('acquire')),
      ),
    );
    final causes = _causes(await runtime.run(resource.acquire()));
    expect(causes, hasLength(3));
    expect((causes[0] as CauseFailure<String>).error, 'acquire');
    expect((causes[1] as CauseCancelled).reason, 'cleanup');
    expect((causes[2] as CauseDefect).error, same(fault));
    expect(reports, isEmpty);
  });

  test(
    'owner close keeps concurrent cleanup failures in completion order',
    () async {
      final foreign = Runtime();
      addTearDown(foreign.close);
      final entered = [Completer<void>(), Completer<void>()];
      final releases = [Completer<void>(), Completer<void>()];
      addTearDown(() {
        for (final release in releases) {
          if (!release.isCompleted) release.complete();
        }
      });
      final failures = [
        StateError('first generation'),
        StateError('second generation'),
      ];
      var next = 0;
      final resource = await _own(
        runtime,
        SharedResource.create(
          Task.acquireRelease(
            Task.sync(() => next++),
            release: (value, _) =>
                Task.sync(() => entered[value].complete())
                    .flatMap((_) => _future(releases[value].future))
                    .flatMap((_) => Task.sync(() => throw failures[value])),
          ),
        ),
      );
      final first = _Borrow(foreign, resource.acquire());
      expect(await first.ready.future, 0);
      await foreign.run(resource.invalidate());
      final second = _Borrow(foreign, resource.acquire());
      expect(await second.ready.future, 1);
      final closing = runtime.close();
      await foreign.run(Task.yieldNow());
      first.release();
      second.release();
      await Future.wait(entered.map((entry) => entry.future));
      releases[1].complete();
      expect(_defects(await second.done), [same(failures[1])]);
      releases[0].complete();
      expect(_defects(await first.done), [same(failures[0])]);
      expect(_defects(await closing), [same(failures[1]), same(failures[0])]);
      expect(reports, isEmpty);
    },
  );

  test('idle cleanup defects report once to the creation runtime', () async {
    final reported = Completer<Cause<Object?>>();
    final owner = Runtime(
      context: Context.from([Clock.key.bind(clock)]),
      onUnhandledError: (cause) {
        reports.add(cause);
        reported.complete(cause);
      },
    );
    addTearDown(owner.close);
    final defect = StateError('idle');
    final resource = await _own(
      owner,
      SharedResource.create(
        Task.acquireRelease(
          Task.succeed(1),
          release: (_, _) => Task.sync(() => throw defect),
        ),
        idleTimeout: ttl,
      ),
    );
    expect(_value(await runtime.run(resource.acquire())), 1);
    clock.advance(ttl);
    final cause = await reported.future;
    expect((cause as CauseDefect).error, same(defect));
    expect(await owner.close(), isA<ExitSuccess>());
    expect(reports, hasLength(1));
  });

  test(
    'owner close claims in-progress idle cleanup instead of double reporting',
    () async {
      final entered = Completer<void>();
      final release = Completer<void>();
      addTearDown(() {
        if (!release.isCompleted) release.complete();
      });
      final defect = StateError('idle cleanup');
      final resource = await _own(
        runtime,
        SharedResource.create(
          Task.acquireRelease(
            Task.succeed(1),
            release: (_, _) =>
                Task.sync(() => entered.complete())
                    .flatMap((_) => _future(release.future))
                    .flatMap((_) => Task.sync(() => throw defect)),
          ),
          idleTimeout: ttl,
        ),
      );
      await runtime.run(resource.acquire());
      clock.advance(ttl);
      await entered.future;
      final closing = runtime.close();
      // Let owner cancellation reach its registered finalizer before cleanup ends.
      final barrier = Runtime();
      await barrier.run(Task.yieldNow());
      await barrier.close();
      release.complete();
      expect(_defects(await closing), [same(defect)]);
      expect(reports, isEmpty);
    },
  );

  test(
    'covariantly widened resources keep acquisition and cleanup valid',
    () async {
      final SharedResource<Object?, Object?> resource = await _own(
        runtime,
        SharedResource.create(Task.succeed(1)),
      );
      expect(
        _value(await runtime.run(resource.acquire().map((value) => '$value'))),
        '1',
      );
    },
  );

  test(
    'last waiter waits a native Future and preserves its late failure',
    () async {
      final started = Completer<void>();
      final future = Completer<int>();
      addTearDown(() {
        if (!future.isCompleted) future.complete(1);
      });
      final lateFailure = StateError('native failure');
      final cleanupFailure = StateError('cleanup failure');
      final resource = await _own(
        runtime,
        SharedResource.create(
          Task.addFinalizer((_) => Task.sync(() => throw cleanupFailure))
              .withError<String>()
              .flatMap(
                (_) => Task.fromFuture<int, String>(() {
                  started.complete();
                  return future.future;
                }, onError: (_, _) => 'mapped'),
              ),
        ),
      );
      final controller = CancellationController();
      var finished = false;
      final result = runtime
          .run(resource.acquire(), signal: controller.signal)
          .then((exit) {
            finished = true;
            return exit;
          });
      await started.future;
      controller.requestCancel();
      await runtime.run(Task.yieldNow());
      expect(finished, isFalse);
      future.completeError(lateFailure);
      final exit = await result;
      expect(_causes(exit).whereType<CauseCancelled>(), hasLength(1));
      expect(_causes(exit).whereType<CauseFailure<String>>(), isEmpty);
      expect(_defects(exit), [same(lateFailure), same(cleanupFailure)]);
    },
  );

  test(
    'acquisition scoped cleanup finishes before a lease is published',
    () async {
      final entered = Completer<void>();
      final release = Completer<void>();
      addTearDown(() {
        if (!release.isCompleted) release.complete();
      });
      final resource = await _own(
        runtime,
        SharedResource.create(
          Task.acquireRelease(
            Task.succeed(1),
            release: (_, _) =>
                Task.sync(() => entered.complete())
                    .flatMap((_) => _future(release.future)),
          ).scoped(),
        ),
      );
      var finished = false;
      final result = runtime.run(resource.acquire()).then((exit) {
        finished = true;
        return exit;
      });
      await entered.future;
      expect(finished, isFalse);
      release.complete();
      expect(_value(await result), 1);
    },
  );

  test(
    'failed timer registration releases the resource and returns its defect',
    () async {
      final faulty = _FaultyClock(clock)..failSchedule = true;
      var releases = 0;
      final resource = await _own(
        runtime,
        SharedResource.create(
          Task.acquireRelease(
            Task.succeed(1),
            release: (_, _) => Task.sync(() {
              releases++;
            }),
          ),
          idleTimeout: ttl,
        ).provideContext(Context.from([Clock.key.bind(faulty)])),
      );
      expect(_defects(await runtime.run(resource.acquire())), [
        same(faulty.error),
      ]);
      expect(releases, 1);
      expect(clock.pendingCount, 0);
      expect(reports, isEmpty);
    },
  );

  test(
    'failed timer cancellation cannot deliver a closing generation',
    () async {
      final faulty = _FaultyClock(clock);
      var releases = 0;
      var uses = 0;
      final resource = await _own(
        runtime,
        SharedResource.create(
          Task.acquireRelease(
            Task.succeed(1),
            release: (_, _) => Task.sync(() {
              releases++;
            }),
          ),
          idleTimeout: ttl,
        ).provideContext(Context.from([Clock.key.bind(faulty)])),
      );
      await runtime.run(resource.acquire());
      faulty.failCancel = true;
      expect(
        _defects(
          await runtime.run(
            resource.acquire().map((_) {
              uses++;
            }),
          ),
        ),
        [same(faulty.error)],
      );
      expect(uses, 0);
      expect(releases, 1);
      expect(clock.pendingCount, 0);
      expect(reports, isEmpty);
    },
  );
}

final class _FaultyClock implements Clock {
  _FaultyClock(this.clock);
  final Clock clock;
  final error = StateError('timer');
  bool failSchedule = false;
  bool failCancel = false;
  @override
  DateTime now() => clock.now();
  @override
  Duration get elapsed => clock.elapsed;
  @override
  void Function() schedule(Duration delay, void Function() callback) {
    if (failSchedule) throw error;
    final cancel = clock.schedule(delay, callback);
    return () {
      cancel();
      if (failCancel) throw error;
    };
  }
}

Future<SharedResource<T, E>> _own<T, E>(
  Runtime runtime,
  Task<SharedResource<T, E>, Never> create,
) {
  final ready = Completer<SharedResource<T, E>>();
  unawaited(
    runtime.run(
      create.flatMap(
        (resource) =>
            Task.sync(() => ready.complete(resource))
                .flatMap((_) => _pending<void>()),
      ),
    ),
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

Task<T, Never> _pending<T>() => Task.fromCallback((_) => () {});
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
