import 'dart:async';

import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  late Runtime runtime;

  setUp(() => runtime = Runtime());
  tearDown(() async {
    expect(await runtime.close(), isA<ExitSuccess<void, Never>>());
  });

  test('create and operations are lazy and reusable', () async {
    final create = SynchronizedRef.create(0);
    final first = await runtime.runOrThrow(create);
    final second = await runtime.runOrThrow(create);
    var calls = 0;
    final increment = first.update((value) {
      calls++;
      return value + 1;
    });
    final read = first.get();

    expect(first, isNot(same(second)));
    expect(calls, 0);
    expect(await runtime.runOrThrow(read), 0);
    await runtime.runOrThrow(increment);
    await runtime.runOrThrow(increment);
    expect(calls, 2);
    expect(await runtime.runOrThrow(read), 2);
    expect(await runtime.runOrThrow(second.get()), 0);
  });

  test(
    'all writes share one queue and read the preceding committed value',
    () async {
      final ref = await runtime.runOrThrow(SynchronizedRef.create(0));
      final first = _Gate<(String, int)>();
      final started = <String>[];
      final holding = runtime.run(ref.modifyTask((_) => first.task));
      await first.started.future;

      final setting = runtime.run(ref.set(20));
      final updating = runtime.run(
        ref.update((value) {
          started.add('update $value');
          return value + 1;
        }),
      );
      final modifying = runtime.run(
        ref.modify((value) {
          started.add('modify $value');
          return (value, value * 2);
        }),
      );
      final modifyingTask = runtime.run(
        ref.modifyTask((value) {
          started.add('modifyTask $value');
          return Task.succeed(('last', value + 1));
        }),
      );

      expect(await runtime.runOrThrow(ref.get()), 0);
      expect(started, isEmpty);
      first.open(('first', 10));

      expect(_value(await holding), 'first');
      expect(await setting, isA<ExitSuccess<void, Never>>());
      expect(await updating, isA<ExitSuccess<void, Never>>());
      expect(_value(await modifying), 21);
      expect(_value(await modifyingTask), 'last');
      expect(started, ['update 20', 'modify 21', 'modifyTask 42']);
      expect(await runtime.runOrThrow(ref.get()), 43);
    },
  );

  test('a reusable asynchronous modification can read its own ref', () async {
    final ref = await runtime.runOrThrow(SynchronizedRef.create<int?>(null));
    final change = ref.modifyTask<String?, Never>(
      (value) => ref.get().map((stored) {
        expect(stored, value);
        return (null, (value ?? 0) + 1);
      }),
    );

    expect(await runtime.runOrThrow(change), isNull);
    expect(await runtime.runOrThrow(change), isNull);
    expect(await runtime.runOrThrow(ref.get()), 2);
    await runtime.runOrThrow(ref.set(null));
    expect(await runtime.runOrThrow(ref.get()), isNull);
  });

  test(
    'candidate resources close before commitment and the next write',
    () async {
      final ref = await runtime.runOrThrow(SynchronizedRef.create(1));
      final cleanup = _Gate<void>();
      final events = <String>[];
      final running = runtime.run(
        ref.modifyTask(
          (value) =>
              Task.addFinalizer((_) => cleanup.task)
                  .map((_) => ('updated', value + 1)),
        ),
      );
      await cleanup.started.future;
      final waiting = runtime.run(
        ref.update((value) {
          events.add('next $value');
          return value * 10;
        }),
      );

      expect(await runtime.runOrThrow(ref.get()), 1);
      expect(events, isEmpty);
      cleanup.open(null);

      expect(_value(await running), 'updated');
      expect(await waiting, isA<ExitSuccess<void, Never>>());
      expect(events, ['next 2']);
      expect(await runtime.runOrThrow(ref.get()), 20);
    },
  );

  test('business failure and explicit cancellation do not commit', () async {
    final ref = await runtime.runOrThrow(SynchronizedRef.create(5));
    final failed = await runtime.run(
      ref.modifyTask<void, String>((_) => Task.fail('rejected')),
    );
    expect(
      _cause(failed),
      isA<CauseFailure<String>>().having(
        (cause) => cause.error,
        'error',
        'rejected',
      ),
    );
    final cancelled = await runtime.run(
      ref.modifyTask<void, Never>(
        (_) => Task.failCause(const Cause.cancelled('no candidate')),
      ),
    );
    expect(_cause(cancelled), isA<CauseCancelled>());
    expect(await runtime.runOrThrow(ref.get()), 5);
    await runtime.runOrThrow(ref.set(7));
    expect(await runtime.runOrThrow(ref.get()), 7);
  });

  test(
    'callback defects retain their stack and release the write permit',
    () async {
      final ref = await runtime.runOrThrow(SynchronizedRef.create(5));
      final error = StateError('update failed');
      final trace = StackTrace.fromString('original modification trace');
      final operations = <Task<Object?, Never>>[
        ref.update((_) => Error.throwWithStackTrace(error, trace)),
        ref.modify((_) => Error.throwWithStackTrace(error, trace)),
        ref.modifyTask((_) => Error.throwWithStackTrace(error, trace)),
        ref.modifyTask(
          (_) => Task.sync(() => Error.throwWithStackTrace(error, trace)),
        ),
      ];

      for (final operation in operations) {
        final cause = _cause(await runtime.run(operation)) as CauseDefect;
        expect(cause.error, same(error));
        expect(cause.stackTrace.toString(), '$trace');
        expect(
          await runtime.runOrThrow(ref.modify((value) => (value, value))),
          5,
        );
      }
    },
  );

  test(
    'candidate cleanup failure prevents committing its successful value',
    () async {
      final ref = await runtime.runOrThrow(SynchronizedRef.create(5));
      final error = StateError('cleanup failed');
      final exit = await runtime.run(
        ref.modifyTask(
          (_) =>
              Task.addFinalizer((_) => Task.sync(() => throw error))
                  .map((_) => ('candidate', 10)),
        ),
      );

      expect(
        _cause(exit),
        isA<CauseDefect>().having((cause) => cause.error, 'error', same(error)),
      );
      expect(await runtime.runOrThrow(ref.get()), 5);
      await runtime.runOrThrow(ref.set(7));
      expect(await runtime.runOrThrow(ref.get()), 7);
    },
  );

  test(
    'a cancelled queued write cannot start or release the active writer',
    () async {
      final ref = await runtime.runOrThrow(SynchronizedRef.create(0));
      final first = _Gate<(void, int)>();
      final controller = CancellationController();
      final holding = runtime.run(ref.modifyTask((_) => first.task));
      await first.started.future;
      var cancelledCalls = 0;
      var nextCalls = 0;
      final cancelled = runtime.run(
        ref.modifyTask((value) {
          cancelledCalls++;
          return Task.succeed((null, value + 100));
        }),
        signal: controller.signal,
      );
      final waiting = runtime.run(
        ref.update((value) {
          nextCalls++;
          return value + 1;
        }),
      );

      controller.requestCancel(reason: 'skip this write');
      expect(_cause(await cancelled), isA<CauseCancelled>());
      expect(cancelledCalls, 0);
      expect(nextCalls, 0);
      expect(await runtime.runOrThrow(ref.get()), 0);
      first.open((null, 10));

      expect(await holding, isA<ExitSuccess<void, Never>>());
      expect(await waiting, isA<ExitSuccess<void, Never>>());
      expect(nextCalls, 1);
      expect(await runtime.runOrThrow(ref.get()), 11);
    },
  );

  test(
    'cancelling a running modification waits for cancellation cleanup',
    () async {
      final ref = await runtime.runOrThrow(SynchronizedRef.create(5));
      final controller = CancellationController();
      final started = Completer<void>();
      final cleanupStarted = Completer<void>();
      final cleanup = Completer<void>();
      addTearDown(() {
        if (!cleanup.isCompleted) cleanup.complete();
      });
      final running = runtime.run(
        ref.modifyTask(
          (_) => Task.fromCallback<(void, int), Never>((_) {
            started.complete();
            return () {
              cleanupStarted.complete();
              return cleanup.future;
            };
          }),
        ),
        signal: controller.signal,
      );
      await started.future;
      var nextCalls = 0;
      final waiting = runtime.run(
        ref.update((value) {
          nextCalls++;
          return value + 1;
        }),
      );

      controller.requestCancel();
      await cleanupStarted.future;
      expect(nextCalls, 0);
      expect(await runtime.runOrThrow(ref.get()), 5);
      cleanup.complete();

      expect(_cause(await running), isA<CauseCancelled>());
      expect(await waiting, isA<ExitSuccess<void, Never>>());
      expect(await runtime.runOrThrow(ref.get()), 6);
    },
  );

  test('an uncancellable Future holds the permit until it settles', () async {
    final ref = await runtime.runOrThrow(SynchronizedRef.create(5));
    final controller = CancellationController();
    final started = Completer<void>();
    final work = Completer<(String, int)>();
    addTearDown(() {
      if (!work.isCompleted) work.complete(('candidate', 10));
    });
    final running = runtime.run(
      ref.modifyTask(
        (_) => Task.fromFuture(() {
          started.complete();
          return work.future;
        }, onError: (error, stackTrace) => error),
      ),
      signal: controller.signal,
    );
    await started.future;
    var nextCalls = 0;
    final waiting = runtime.run(
      ref.update((value) {
        nextCalls++;
        return value + 1;
      }),
    );

    controller.requestCancel();
    expect(await runtime.runOrThrow(ref.get()), 5);
    expect(nextCalls, 0);
    work.complete(('candidate', 10));

    expect(_cause(await running), isA<CauseCancelled>());
    expect(await waiting, isA<ExitSuccess<void, Never>>());
    expect(await runtime.runOrThrow(ref.get()), 6);
  });

  test('cancellation during candidate cleanup prevents commitment', () async {
    final ref = await runtime.runOrThrow(SynchronizedRef.create(5));
    final controller = CancellationController();
    final cleanup = _Gate<void>();
    final running = runtime.run(
      ref.modifyTask(
        (_) =>
            Task.addFinalizer((_) => cleanup.task)
                .map((_) => ('candidate', 10)),
      ),
      signal: controller.signal,
    );
    await cleanup.started.future;
    controller.requestCancel();

    expect(await runtime.runOrThrow(ref.get()), 5);
    expect(cleanup.cancelCalls, 0);
    cleanup.open(null);
    expect(_cause(await running), isA<CauseCancelled>());
    expect(await runtime.runOrThrow(ref.get()), 5);
    await runtime.runOrThrow(ref.set(7));
  });

  test(
    'enabled cancellation separates asynchronous candidates from sync commits',
    () async {
      final ref = await runtime.runOrThrow(SynchronizedRef.create(5));
      final syncCancel = CancellationController();
      final sync = await runtime.run(
        ref.update((value) {
          syncCancel.requestCancel();
          return value + 1;
        }),
        signal: syncCancel.signal,
      );
      expect(_cause(sync), isA<CauseCancelled>());
      expect(await runtime.runOrThrow(ref.get()), 6);

      final asyncCancel = CancellationController();
      final asynchronous = await runtime.run(
        ref.modifyTask(
          (value) => Task.sync(() {
            asyncCancel.requestCancel();
            return ('candidate', value + 1);
          }),
        ),
        signal: asyncCancel.signal,
      );
      expect(_cause(asynchronous), isA<CauseCancelled>());
      expect(await runtime.runOrThrow(ref.get()), 6);
    },
  );

  test(
    'an enclosing cancellation mask allows the candidate to commit',
    () async {
      final ref = await runtime.runOrThrow(SynchronizedRef.create(5));
      final controller = CancellationController();
      final exit = await runtime.run(
        Task.uninterruptibleMask(
          (_) => ref.modifyTask(
            (value) => Task.sync(() {
              controller.requestCancel();
              return ('candidate', value + 1);
            }),
          ),
        ),
        signal: controller.signal,
      );

      expect(_cause(exit), isA<CauseCancelled>());
      expect(await runtime.runOrThrow(ref.get()), 6);
    },
  );

  test(
    'widened callbacks work while incompatible writes fail lazily',
    () async {
      final narrow = await runtime.runOrThrow(SynchronizedRef.create(1));
      final SynchronizedRef<num> ref = narrow;
      await runtime.runOrThrow(ref.update((num value) => value.toInt() + 1));
      expect(
        await runtime.runOrThrow(
          ref.modifyTask(
            (num value) => Task.succeed(('previous $value', value.toInt() + 1)),
          ),
        ),
        'previous 2',
      );
      final incompatible = <Task<Object?, Never>>[
        ref.set(1.5),
        ref.update((_) => 1.5),
        ref.modify((_) => ('incompatible', 1.5)),
        ref.modifyTask((_) => Task.succeed(('incompatible', 1.5))),
      ];

      for (final operation in incompatible) {
        expect(
          _cause(await runtime.run(operation)),
          isA<CauseDefect>().having(
            (cause) => cause.error,
            'error',
            isA<TypeError>(),
          ),
        );
        expect(
          await runtime.runOrThrow(ref.modify((value) => (value, value))),
          3,
        );
      }
    },
  );

  test(
    'failed modifications do not roll back mutations of a stored object',
    () async {
      final original = <int>[];
      final ref = await runtime.runOrThrow(SynchronizedRef.create(original));
      final exit = await runtime.run(
        ref.modifyTask<void, String>((value) {
          value.add(1);
          return Task.fail('rejected');
        }),
      );

      expect(_cause(exit), isA<CauseFailure<String>>());
      expect(await runtime.runOrThrow(ref.get()), same(original));
      expect(original, [1]);
    },
  );
}

final class _Gate<T> {
  final started = Completer<void>();
  late void Function(Exit<T, Never>) _complete;
  var cancelCalls = 0;

  Task<T, Never> get task => Task.fromCallback((complete) {
    _complete = complete;
    started.complete();
    return () {
      cancelCalls++;
    };
  });

  void open(T value) => _complete(Exit.success(value));
}

T _value<T, E>(Exit<T, E> exit) => switch (exit) {
  ExitSuccess(:final value) => value,
  ExitFailure(:final cause) => fail('Expected success, got $cause.'),
};

Cause<E> _cause<T, E>(Exit<T, E> exit) => switch (exit) {
  ExitSuccess(:final value) => fail('Expected failure, got $value.'),
  ExitFailure(:final cause) => cause,
};
