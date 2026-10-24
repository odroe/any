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

  test('creation is lazy and each execution owns independent state', () async {
    var starts = 0;
    final create = SharedResourceMap.create<String, int, String>(
      capacity: 1,
      acquire: (_) => Task.sync(() => ++starts),
      onCapacityExceeded: (key) => 'full:$key',
    );
    expect(starts, 0);
    final first = await _own(runtime, create);
    final second = await _own(runtime, create);
    expect(identical(first, second), isFalse);
    expect(starts, 0);
    expect(_value(await runtime.run(first.acquire('a'))), 1);
    expect(_value(await runtime.run(second.acquire('a'))), 2);
  });

  test('use releases its lease and capacity before continuing', () async {
    final released = <String>[];
    final map = await _own(
      runtime,
      SharedResourceMap.create<String, String, String>(
        capacity: 1,
        acquire: (key) => Task.acquireRelease(
          Task.succeed(key),
          release: (value, _) => Task.sync(() => released.add(value)),
        ),
        onCapacityExceeded: (key) => 'full:$key',
      ),
    );

    final result = await runtime.run(
      map.use('a', (value) => Task.succeed('$value!')).flatMap((first) {
        expect(first, 'a!');
        expect(released, ['a']);
        return map.use('b', Task.succeed);
      }),
    );

    expect(_value(result), 'b');
    expect(released, ['a', 'b']);
  });

  test('widened use returns its lease before business recovery', () async {
    final released = <String>[];
    final original = await _own(
      runtime,
      SharedResourceMap.create<String, String, Never>(
        capacity: 1,
        acquire: (key) => Task.acquireRelease(
          Task.succeed(key),
          release: (value, _) => Task.sync(() => released.add(value)),
        ),
        onCapacityExceeded: (_) => throw StateError('unexpected capacity'),
      ),
    );
    final SharedResourceMap<String, String, String> map = original;

    final result = await runtime.run(
      map.use<String>('a', (_) => Task.fail('operation failed')).recover((
        error,
      ) {
        expect(error, 'operation failed');
        expect(released, ['a']);
        return map.use('b', Task.succeed);
      }),
    );

    expect(_value(result), 'b');
    expect(released, ['a', 'b']);
  });

  test(
    'overlapping uses share one resource and preserve idle retention',
    () async {
      var starts = 0;
      final released = <int>[];
      final hold = _Gate<int, String>();
      final map = await _own(
        runtime,
        SharedResourceMap.create<String, int, String>(
          capacity: 1,
          acquire: (_) => Task.acquireRelease(
            Task.sync(() => ++starts),
            release: (value, _) => Task.sync(() => released.add(value)),
          ),
          onCapacityExceeded: (key) => 'full:$key',
          idleTimeout: ttl,
        ),
      );
      final first = runtime.run(map.use('a', (_) => hold.task));
      await hold.started.future;

      expect(_value(await runtime.run(map.use('a', Task.succeed))), 1);
      expect(clock.pendingCount, 0);
      expect(released, isEmpty);
      hold.succeed(1);
      expect(_value(await first), 1);
      expect(clock.pendingCount, 1);
      expect(_value(await runtime.run(map.use('a', Task.succeed))), 1);
      expect(starts, 1);
      expect(released, isEmpty);

      await runtime.run(map.invalidate('a'));
      expect(released, [1]);
      expect(clock.pendingCount, 0);
    },
  );

  test('invalid capacity and timeout become execution defects', () async {
    for (final capacity in [0, -1]) {
      final create = SharedResourceMap.create<String, int, String>(
        capacity: capacity,
        acquire: (_) => Task.succeed(1),
        onCapacityExceeded: (key) => 'full:$key',
      );
      expect(_defects(await runtime.run(create)).single, isA<RangeError>());
    }
    final create = SharedResourceMap.create<String, int, String>(
      capacity: 1,
      acquire: (_) => Task.succeed(1),
      onCapacityExceeded: (key) => 'full:$key',
      idleTimeout: const Duration(microseconds: -1),
    );
    expect(_defects(await runtime.run(create)).single, isA<ArgumentError>());
  });

  test('a map returned from a finished owner rejects all operations', () async {
    final map = _value(
      await runtime.run(
        SharedResourceMap.create<String, int, String>(
          capacity: 1,
          acquire: (_) => Task.succeed(1),
          onCapacityExceeded: (key) => 'full:$key',
        ),
      ),
    );
    expect(
      _defects(await runtime.run(map.acquire('a'))).single,
      isA<StateError>(),
    );
    expect(
      _defects(await runtime.run(map.invalidate('missing'))).single,
      isA<StateError>(),
    );
  });

  test(
    'concurrent callers share a key while different keys start independently',
    () async {
      final gates = {'a': _Gate<int, String>(), 'b': _Gate<int, String>()};
      final starts = <String>[];
      final releases = <int>[];
      final map = await _own(
        runtime,
        SharedResourceMap.create<String, int, String>(
          capacity: 2,
          acquire: (key) {
            starts.add(key);
            return Task.acquireRelease(
              gates[key]!.task,
              release: (value, _) => Task.sync(() => releases.add(value)),
            );
          },
          onCapacityExceeded: (key) => 'full:$key',
        ),
      );
      final first = _Borrow(runtime, map.acquire('a'));
      final second = _Borrow(runtime, map.acquire('a'));
      final other = _Borrow(runtime, map.acquire('b'));
      await Future.wait(gates.values.map((gate) => gate.started.future));
      expect(starts, ['a', 'b']);
      gates['b']!.succeed(2);
      expect(await other.ready.future, 2);
      gates['a']!.succeed(1);
      expect(await first.ready.future, 1);
      expect(await second.ready.future, 1);
      first.release();
      await first.done;
      expect(releases, isEmpty);
      other.release();
      await other.done;
      second.release();
      await second.done;
      expect(releases, [2, 1]);
    },
  );

  test(
    'capacity rejects a new key without acquiring and permits existing leases',
    () async {
      final starts = <String>[];
      final rejected = <String>[];
      final map = await _own(
        runtime,
        SharedResourceMap.create<String, String, String>(
          capacity: 1,
          acquire: (key) {
            starts.add(key);
            return Task.succeed(key);
          },
          onCapacityExceeded: (key) {
            rejected.add(key);
            return 'full:$key';
          },
        ),
      );
      final held = _Borrow(runtime, map.acquire('a'));
      expect(await held.ready.future, 'a');
      expect(_error(await runtime.run(map.acquire('b'))), 'full:b');
      expect(_value(await runtime.run(map.acquire('a'))), 'a');
      expect(starts, ['a']);
      expect(rejected, ['b']);
      held.release();
      await held.done;
      expect(_value(await runtime.run(map.acquire('b'))), 'b');
      expect(starts, ['a', 'b']);
    },
  );

  test(
    'invalidated active generations retain capacity including for the same key',
    () async {
      var starts = 0;
      final released = <int>[];
      final map = await _own(
        runtime,
        SharedResourceMap.create<String, int, String>(
          capacity: 1,
          acquire: (_) => Task.acquireRelease(
            Task.sync(() => ++starts),
            release: (value, _) => Task.sync(() => released.add(value)),
          ),
          onCapacityExceeded: (key) => 'full:$key',
          idleTimeout: ttl,
        ),
      );
      final held = _Borrow(runtime, map.acquire('a'));
      expect(await held.ready.future, 1);
      for (var i = 0; i < 3; i++) {
        expect(await runtime.run(map.invalidate('a')), isA<ExitSuccess>());
        expect(_error(await runtime.run(map.acquire('a'))), 'full:a');
        expect(_error(await runtime.run(map.acquire('b'))), 'full:b');
      }
      expect(released, isEmpty);
      expect(starts, 1);
      held.release();
      await held.done;
      expect(released, [1]);
      expect(clock.pendingCount, 0);
      expect(_value(await runtime.run(map.acquire('a'))), 2);
    },
  );

  test('old and current generations of one key each occupy capacity', () async {
    var starts = 0;
    final map = await _own(
      runtime,
      SharedResourceMap.create<String, int, String>(
        capacity: 2,
        acquire: (_) => Task.sync(() => ++starts),
        onCapacityExceeded: (key) => 'full:$key',
      ),
    );
    final first = _Borrow(runtime, map.acquire('a'));
    expect(await first.ready.future, 1);
    await runtime.run(map.invalidate('a'));
    final second = _Borrow(runtime, map.acquire('a'));
    expect(await second.ready.future, 2);
    expect(_error(await runtime.run(map.acquire('b'))), 'full:b');
    first.release();
    await first.done;
    expect(_value(await runtime.run(map.acquire('b'))), 3);
    expect(_value(await runtime.run(map.acquire('a'))), 2);
    second.release();
    await second.done;
  });

  test(
    'invalidation preserves old waiters and reserves their acquisition slot',
    () async {
      final gate = _Gate<int, String>();
      var starts = 0;
      final map = await _own(
        runtime,
        SharedResourceMap.create<String, int, String>(
          capacity: 1,
          acquire: (_) => ++starts == 1 ? gate.task : Task.succeed(2),
          onCapacityExceeded: (key) => 'full:$key',
        ),
      );
      final first = runtime.run(map.acquire('a'));
      await gate.started.future;
      await runtime.run(map.invalidate('a'));
      expect(_error(await runtime.run(map.acquire('a'))), 'full:a');
      expect(gate.cancelled, isFalse);
      gate.succeed(1);
      expect(_value(await first), 1);
      expect(_value(await runtime.run(map.acquire('a'))), 2);
    },
  );

  test(
    'last waiter cancellation retains capacity until cleanup completes',
    () async {
      final gate = _Gate<int, String>();
      final cleaning = Completer<void>();
      final release = Completer<void>();
      addTearDown(() {
        if (!release.isCompleted) release.complete();
      });
      var starts = 0;
      final map = await _own(
        runtime,
        SharedResourceMap.create<String, int, String>(
          capacity: 1,
          acquire: (_) {
            if (++starts > 1) return Task.succeed(2);
            return Task.addFinalizer(
              (_) =>
                  Task.sync(() => cleaning.complete())
                      .flatMap((_) => _future(release.future)),
            ).withError<String>().flatMap((_) => gate.task);
          },
          onCapacityExceeded: (key) => 'full:$key',
        ),
      );
      final controller = CancellationController();
      final waiting = runtime.run(map.acquire('a'), signal: controller.signal);
      await gate.started.future;
      controller.requestCancel();
      await cleaning.future;
      expect(gate.cancelled, isTrue);
      expect(_error(await runtime.run(map.acquire('a'))), 'full:a');
      expect(_error(await runtime.run(map.acquire('b'))), 'full:b');
      release.complete();
      expect(_causes(await waiting).whereType<CauseCancelled>(), hasLength(1));
      expect(_value(await runtime.run(map.acquire('b'))), 2);
    },
  );

  test(
    'cancelling one caller preserves shared work and its capacity',
    () async {
      final gate = _Gate<int, String>();
      final map = await _own(
        runtime,
        SharedResourceMap.create<String, int, String>(
          capacity: 1,
          acquire: (_) => gate.task,
          onCapacityExceeded: (key) => 'full:$key',
        ),
      );
      final controller = CancellationController();
      final first = runtime.run(map.acquire('a'), signal: controller.signal);
      final second = _Borrow(runtime, map.acquire('a'));
      await gate.started.future;
      controller.requestCancel();
      expect(_causes(await first).whereType<CauseCancelled>(), hasLength(1));
      expect(gate.cancelled, isFalse);
      expect(_error(await runtime.run(map.acquire('b'))), 'full:b');
      gate.succeed(1);
      expect(await second.ready.future, 1);
      second.release();
      await second.done;
    },
  );

  test(
    'failed acquisition waits cleanup before releasing its capacity',
    () async {
      final cleaning = Completer<void>();
      final release = Completer<void>();
      addTearDown(() {
        if (!release.isCompleted) release.complete();
      });
      final fault = StateError('cleanup');
      var starts = 0;
      final map = await _own(
        runtime,
        SharedResourceMap.create<String, int, String>(
          capacity: 1,
          acquire: (_) {
            if (++starts > 1) return Task.succeed(2);
            return Task.addFinalizer(
              (_) =>
                  Task.sync(() => cleaning.complete())
                      .flatMap((_) => _future(release.future))
                      .flatMap((_) => Task.sync(() => throw fault)),
            ).withError<String>().flatMap((_) => Task.fail('acquire'));
          },
          onCapacityExceeded: (key) => 'full:$key',
        ),
      );
      final failed = runtime.run(map.acquire('a'));
      await cleaning.future;
      expect(_error(await runtime.run(map.acquire('b'))), 'full:b');
      release.complete();
      final exit = await failed;
      expect((_causes(exit).first as CauseFailure<String>).error, 'acquire');
      expect(_defects(exit), [same(fault)]);
      expect(_value(await runtime.run(map.acquire('b'))), 2);
      expect(reports, isEmpty);
    },
  );

  test(
    'idle entries occupy capacity and are not evicted for a new key',
    () async {
      var starts = 0;
      final map = await _own(
        runtime,
        SharedResourceMap.create<String, int, String>(
          capacity: 1,
          acquire: (_) => Task.sync(() => ++starts),
          onCapacityExceeded: (key) => 'full:$key',
          idleTimeout: ttl,
        ),
      );
      expect(_value(await runtime.run(map.acquire('a'))), 1);
      expect(_error(await runtime.run(map.acquire('b'))), 'full:b');
      expect(_value(await runtime.run(map.acquire('a'))), 1);
      expect(starts, 1);
      await runtime.run(map.invalidate('a'));
      expect(clock.pendingCount, 0);
      expect(_value(await runtime.run(map.acquire('b'))), 2);
    },
  );

  test(
    'idle expiration frees keys only after background cleanup completes',
    () async {
      final cleaning = Completer<void>();
      final release = Completer<void>();
      addTearDown(() {
        if (!release.isCompleted) release.complete();
      });
      var starts = 0;
      final map = await _own(
        runtime,
        SharedResourceMap.create<String, int, String>(
          capacity: 1,
          acquire: (_) => Task.acquireRelease(
            Task.sync(() => ++starts),
            release: (value, _) => value == 1
                ? Task.sync(() => cleaning.complete())
                      .flatMap((_) => _future(release.future))
                : Task.succeed(null),
          ),
          onCapacityExceeded: (key) => 'full:$key',
          idleTimeout: ttl,
        ),
      );
      expect(_value(await runtime.run(map.acquire('a'))), 1);
      clock.advance(ttl);
      await cleaning.future;
      expect(_error(await runtime.run(map.acquire('a'))), 'full:a');
      expect(_error(await runtime.run(map.acquire('b'))), 'full:b');
      release.complete();
      await runtime.run(Task.yieldNow());
      expect(_value(await runtime.run(map.acquire('b'))), 2);
      await runtime.run(map.invalidate('b'));
      expect(_value(await runtime.run(map.acquire('a'))), 3);
      expect(reports, isEmpty);
    },
  );

  test(
    'nullable keys and values and equal keys use ordinary Dart semantics',
    () async {
      var starts = 0;
      final map = await _own(
        runtime,
        SharedResourceMap.create<_Key?, int?, String>(
          capacity: 2,
          acquire: (_) {
            starts++;
            return Task.succeed(null);
          },
          onCapacityExceeded: (_) => 'full',
          idleTimeout: ttl,
        ),
      );
      expect(_value(await runtime.run(map.acquire(null))), isNull);
      expect(_value(await runtime.run(map.acquire(const _Key(1)))), isNull);
      expect(_value(await runtime.run(map.acquire(const _Key(1)))), isNull);
      expect(starts, 2);
      expect(_error(await runtime.run(map.acquire(const _Key(2)))), 'full');
    },
  );

  test(
    'a throwing capacity mapper is a defect without disturbing leases',
    () async {
      final fault = StateError('capacity mapping');
      final map = await _own(
        runtime,
        SharedResourceMap.create<String, int, Never>(
          capacity: 1,
          acquire: (_) => Task.succeed(1),
          onCapacityExceeded: (_) => throw fault,
        ),
      );
      final held = _Borrow(runtime, map.acquire('a'));
      await held.ready.future;
      expect(_defects(await runtime.run(map.acquire('b'))), [same(fault)]);
      expect(_value(await runtime.run(map.acquire('a'))), 1);
      held.release();
      await held.done;
      expect(_value(await runtime.run(map.acquire('b'))), 1);
    },
  );

  test(
    'a throwing acquisition factory releases its slot after failure',
    () async {
      final fault = StateError('acquire factory');
      var starts = 0;
      final map = await _own(
        runtime,
        SharedResourceMap.create<String, int, String>(
          capacity: 1,
          acquire: (_) {
            if (++starts == 1) throw fault;
            return Task.succeed(2);
          },
          onCapacityExceeded: (_) => 'full',
        ),
      );
      expect(_defects(await runtime.run(map.acquire('a'))), [same(fault)]);
      expect(_value(await runtime.run(map.acquire('b'))), 2);
    },
  );
  test(
    'acquisition factories and cleanup use the creation environment',
    () async {
      final key = ServiceKey<String>('tenant');
      final local = TaskLocal('default');
      final capturedClock = TestClock();
      final released = <String>[];
      final map = await _own(
        runtime,
        local
            .locally(
              'created',
              SharedResourceMap.create<String, String, String>(
                capacity: 1,
                acquire: (resourceKey) => Task.service(key).flatMap(
                  (tenant) => local.get().flatMap(
                    (value) => Task.acquireRelease(
                      Task.succeed('$resourceKey/$tenant/$value'),
                      release: (_, _) => Task.service(key).flatMap(
                        (tenant) => local.get().map(
                          (value) => released.add('$tenant/$value'),
                        ),
                      ),
                    ),
                  ),
                ),
                onCapacityExceeded: (_) => 'full',
                idleTimeout: ttl,
              ),
            )
            .provideContext(
              Context.from([key.bind('owner'), Clock.key.bind(capturedClock)]),
            ),
      );
      final task = local
          .locally('borrowed', map.acquire('a'))
          .provideContext(Context.from([key.bind('borrower')]));
      expect(_value(await runtime.run(task)), 'a/owner/created');
      expect(capturedClock.pendingCount, 1);
      expect(clock.pendingCount, 0);
      await runtime.run(map.invalidate('a'));
      expect(released, ['owner/created']);
    },
  );

  test(
    'owner closure stops all pending keys before waiting for foreign leases',
    () async {
      final foreign = Runtime();
      addTearDown(foreign.close);
      final gate = _Gate<int, String>();
      final released = <int>[];
      final map = await _own(
        runtime,
        SharedResourceMap.create<String, int, String>(
          capacity: 2,
          acquire: (key) => key == 'held'
              ? Task.acquireRelease(
                  Task.succeed(1),
                  release: (value, _) => Task.sync(() => released.add(value)),
                )
              : gate.task,
          onCapacityExceeded: (_) => 'full',
        ),
      );
      final held = _Borrow(foreign, map.acquire('held'));
      addTearDown(held.release);
      expect(await held.ready.future, 1);
      final pending = foreign.run(map.acquire('pending'));
      await gate.started.future;
      var completed = false;
      final closing = runtime.close().then((exit) {
        completed = true;
        return exit;
      });
      expect(
        await pending.timeout(const Duration(seconds: 1)),
        isA<ExitFailure>(),
      );
      expect(gate.cancelled, isTrue);
      expect(completed, isFalse);
      expect(released, isEmpty);
      expect(
        _defects(await foreign.run(map.acquire('new'))).single,
        isA<StateError>(),
      );
      held.release();
      expect(_value(await held.done), 1);
      expect(await closing, isA<ExitSuccess>());
      expect(released, [1]);
    },
  );

  test('background cleanup reports once and still releases capacity', () async {
    final reported = Completer<Cause<Object?>>();
    final owner = Runtime(
      context: Context.from([Clock.key.bind(clock)]),
      onUnhandledError: (cause) {
        reports.add(cause);
        reported.complete(cause);
      },
    );
    addTearDown(owner.close);
    final fault = StateError('idle cleanup');
    var starts = 0;
    final map = await _own(
      owner,
      SharedResourceMap.create<String, int, String>(
        capacity: 1,
        acquire: (_) => Task.acquireRelease(
          Task.sync(() => ++starts),
          release: (value, _) =>
              value == 1 ? Task.sync(() => throw fault) : Task.succeed(null),
        ),
        onCapacityExceeded: (_) => 'full',
        idleTimeout: ttl,
      ),
    );
    expect(_value(await runtime.run(map.acquire('a'))), 1);
    clock.advance(ttl);
    expect((await reported.future as CauseDefect).error, same(fault));
    expect(_value(await runtime.run(map.acquire('b'))), 2);
    expect(await owner.close(), isA<ExitSuccess>());
    expect(reports, hasLength(1));
  });

  test(
    'covariantly widened values and errors keep their static APIs',
    () async {
      final SharedResourceMap<String, Object?, Object?> map = await _own(
        runtime,
        SharedResourceMap.create<String, int, String>(
          capacity: 1,
          acquire: (_) => Task.succeed(1),
          onCapacityExceeded: (key) => 'full:$key',
          idleTimeout: ttl,
        ),
      );
      expect(
        _value(await runtime.run(map.acquire('a').map((value) => '$value'))),
        '1',
      );
      expect(_error(await runtime.run(map.acquire('b'))), 'full:b');
      await runtime.run(map.invalidate('a'));
      expect(_value(await runtime.run(map.acquire('b'))), 1);
    },
  );

  test(
    'capacity failures can be nullable without being mistaken for success',
    () async {
      final map = await _own(
        runtime,
        SharedResourceMap.create<String, int, String?>(
          capacity: 1,
          acquire: (_) => Task.succeed(1),
          onCapacityExceeded: (_) => null,
          idleTimeout: ttl,
        ),
      );
      expect(_value(await runtime.run(map.acquire('a'))), 1);
      expect(_error(await runtime.run(map.acquire('b'))), isNull);
    },
  );
}

final class _Key {
  const _Key(this.value);
  final int value;
  @override
  bool operator ==(Object other) => other is _Key && other.value == value;
  @override
  int get hashCode => value;
}

Future<T> _own<T>(Runtime runtime, Task<T, Never> create) {
  final ready = Completer<T>();
  unawaited(
    runtime.run(
      create.flatMap(
        (value) =>
            Task.sync(() => ready.complete(value))
                .flatMap((_) => Task.fromCallback<void, Never>((_) => () {})),
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

Task<void, Never> _future(Future<void> future) => Task.fromFuture<void, Never>(
  () => future,
  onError: Error.throwWithStackTrace,
);
T _value<T, E>(Exit<T, E> exit) => switch (exit) {
  ExitSuccess(:final value) => value,
  ExitFailure(:final cause) => throw StateError('Unexpected failure: $cause'),
};
E _error<T, E>(Exit<T, E> exit) => switch (exit) {
  ExitFailure(cause: CauseFailure(:final error)) => error,
  _ => throw StateError('Expected a business failure: $exit'),
};
List<Cause<E>> _causes<T, E>(Exit<T, E> exit) => switch (exit) {
  ExitSuccess() => [],
  ExitFailure(cause: CauseGroup(:final causes)) => causes,
  ExitFailure(:final cause) => [cause],
};
List<Object> _defects<T, E>(Exit<T, E> exit) =>
    _causes(exit).whereType<CauseDefect>().map((cause) => cause.error).toList();
