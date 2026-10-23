import 'dart:async';

import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  late Runtime runtime;
  setUp(() => runtime = Runtime());
  tearDown(() async {
    expect(await runtime.close(), isA<ExitSuccess<void, Never>>());
  });

  test('creation and writes are lazy, reusable, and independent', () async {
    final create = SubscriptionRef.create<int?>(null);
    final first = await runtime.runOrThrow(create);
    final second = await runtime.runOrThrow(create);
    var calls = 0;
    final update = first.update((value) {
      calls++;
      return (value ?? 0) + 1;
    });
    expect(calls, 0);
    expect(first, isNot(same(second)));
    await runtime.runOrThrow(update);
    await runtime.runOrThrow(update);
    expect(await runtime.runOrThrow(first.get()), 2);
    expect(await runtime.runOrThrow(second.get()), isNull);
    expect(calls, 2);
    expect(await runtime.runOrThrow(first.modify((value) => (value, null))), 2);
    expect(await runtime.runOrThrow(first.get()), isNull);
  });

  test(
    'each consumption starts lazily with the current state and no history',
    () async {
      for (final overflow in SubscriptionOverflowStrategy.values) {
        final ref = await runtime.runOrThrow(
          SubscriptionRef.create(0, overflow: overflow),
        );
        final changes = ref.changes;
        for (var value = 1; value <= 20; value++) {
          await runtime.runOrThrow(ref.set(value));
        }
        expect(await runtime.runOrThrow(changes.take(1).toList()), [20]);
        await runtime.runOrThrow(ref.set(21));
        expect(await runtime.runOrThrow(changes.take(1).toList()), [21]);
      }
    },
  );

  test(
    'latest retains the initial snapshot and coalesces intermediate updates',
    () async {
      final ref = await runtime.runOrThrow(SubscriptionRef.create(0));
      final observer = _Observer(runtime, ref, 2);
      await observer.seen(0);
      await runtime.runOrThrow(ref.set(1));
      await runtime.runOrThrow(ref.set(2));
      await runtime.runOrThrow(ref.set(3));

      expect(observer.values, [0]);
      expect(await runtime.runOrThrow(ref.get()), 3);
      observer.release(0);
      await observer.seen(1);
      expect(observer.values, [0, 3]);
      observer.release(1);
      await observer.completed;
    },
  );

  test(
    'latest capacity bounds the recent updates shared by slow consumers',
    () async {
      final ref = await runtime.runOrThrow(
        SubscriptionRef.create(0, capacity: 2),
      );
      final first = _Observer(runtime, ref, 3);
      final second = _Observer(runtime, ref, 3);
      await first.seen(0);
      await second.seen(0);
      for (var value = 1; value <= 3; value++) {
        await runtime.runOrThrow(ref.set(value));
      }
      for (final observer in [first, second]) {
        observer.release(0);
        await observer.seen(1);
        observer.release(1);
        await observer.seen(2);
        expect(observer.values, [0, 2, 3]);
        observer.release(2);
        await observer.completed;
      }
    },
  );

  test(
    'equal values and nullable values still produce distinct updates',
    () async {
      final ref = await runtime.runOrThrow(
        SubscriptionRef.create<int?>(null, capacity: 2),
      );
      final observer = _Observer(runtime, ref, 3);
      await observer.seen(0);
      await runtime.runOrThrow(ref.set(null));
      await runtime.runOrThrow(ref.update((value) => value));
      observer.release(0);
      await observer.seen(1);
      observer.release(1);
      await observer.seen(2);
      expect(observer.values, [null, null, null]);
      observer.release(2);
      await observer.completed;
    },
  );

  test(
    'backpressure commits the value and every notification together',
    () async {
      final ref = await runtime.runOrThrow(_backPressure(0));
      final fast = _Observer(runtime, ref, 3);
      final slow = _Observer(runtime, ref, 3);
      await fast.seen(0);
      await slow.seen(0);
      await runtime.runOrThrow(ref.set(1));
      fast.release(0);
      await fast.seen(1);
      fast.release(1);
      var committed = false;
      final writing = runtime.run(ref.set(2).map((_) => committed = true));

      expect(await runtime.runOrThrow(ref.get()), 1);
      expect(fast.values, [0, 1]);
      expect(slow.values, [0]);
      expect(committed, isFalse);
      slow.release(0);
      await slow.seen(1);
      await writing;
      await fast.seen(2);
      expect(await runtime.runOrThrow(ref.get()), 2);
      slow.release(1);
      await slow.seen(2);
      expect(fast.values, [0, 1, 2]);
      expect(slow.values, [0, 1, 2]);
      fast.release(2);
      slow.release(2);
      await fast.completed;
      await slow.completed;
    },
  );

  test(
    'a new subscription sees old state then a still-pending commit',
    () async {
      final ref = await runtime.runOrThrow(_backPressure(0));
      final slow = _Observer(runtime, ref, 3);
      await slow.seen(0);
      await runtime.runOrThrow(ref.set(1));
      final writing = runtime.run(ref.set(2));
      final late = _Observer(runtime, ref, 2);
      await late.seen(0);
      expect(late.values, [1]);

      slow.release(0);
      await slow.seen(1);
      await writing;
      late.release(0);
      await late.seen(1);
      expect(late.values, [1, 2]);
      slow.release(1);
      await slow.seen(2);
      expect(slow.values, [0, 1, 2]);
      late.release(1);
      slow.release(2);
      await late.completed;
      await slow.completed;
    },
  );

  test(
    'cancelling an uncommitted write removes it from every subscription',
    () async {
      final ref = await runtime.runOrThrow(_backPressure(0));
      final first = _Observer(runtime, ref, 3);
      final second = _Observer(runtime, ref, 3);
      await first.seen(0);
      await second.seen(0);
      await runtime.runOrThrow(ref.set(1));
      final controller = CancellationController();
      final cancelled = runtime.run(ref.set(2), signal: controller.signal);
      final next = runtime.run(ref.set(3));
      controller.requestCancel(reason: 'discard candidate');
      expect(_cause(await cancelled), isA<CauseCancelled>());
      expect(await runtime.runOrThrow(ref.get()), 1);

      first.release(0);
      second.release(0);
      await first.seen(1);
      await second.seen(1);
      await next;
      first.release(1);
      second.release(1);
      await first.seen(2);
      await second.seen(2);
      expect(first.values, [0, 1, 3]);
      expect(second.values, [0, 1, 3]);
      first.release(2);
      second.release(2);
      await first.completed;
      await second.completed;
    },
  );

  test(
    'cancelling a slow consumer releases a pending commit for other consumers',
    () async {
      final ref = await runtime.runOrThrow(_backPressure(0));
      final fast = _Observer(runtime, ref, 3);
      final slow = _Observer(runtime, ref, 3);
      await fast.seen(0);
      await slow.seen(0);
      await runtime.runOrThrow(ref.set(1));
      fast.release(0);
      await fast.seen(1);
      fast.release(1);
      final writing = runtime.run(ref.set(2));
      expect(await runtime.runOrThrow(ref.get()), 1);

      slow.cancel();
      expect(_cause(await slow.completed), isA<CauseCancelled>());
      await writing;
      await fast.seen(2);
      expect(fast.values, [0, 1, 2]);
      expect(await runtime.runOrThrow(ref.get()), 2);
      fast.release(2);
      await fast.completed;
    },
  );

  test(
    'normal early consumption closes its subscription and releases writers',
    () async {
      final ref = await runtime.runOrThrow(_backPressure(0));
      final observer = _Observer(runtime, ref, 1);
      await observer.seen(0);
      await runtime.runOrThrow(ref.set(1));
      var committed = false;
      final writing = runtime.run(ref.set(2).map((_) => committed = true));
      expect(await runtime.runOrThrow(ref.get()), 1);
      expect(committed, isFalse);

      observer.release(0);
      expect(await observer.completed, isA<ExitSuccess<void, Never>>());
      await writing;
      expect(await runtime.runOrThrow(ref.get()), 2);
      expect(await runtime.runOrThrow(ref.changes.take(1).toList()), [2]);
    },
  );

  test(
    'candidate cleanup finishes before commit and the next queued write',
    () async {
      final ref = await runtime.runOrThrow(_backPressure(0, capacity: 2));
      final observer = _Observer(runtime, ref, 3);
      await observer.seen(0);
      final cleanupStarted = Completer<void>();
      final cleanup = Completer<void>();
      final events = <int>[];
      final writing = runtime.run(
        ref.modifyTask((value) {
          expect(value, 0);
          return Task.addFinalizer(
            (_) => Task.fromFuture<void, Never>(() {
              cleanupStarted.complete();
              return cleanup.future;
            }, onError: Error.throwWithStackTrace),
          ).map((_) => ('done', 1));
        }),
      );
      await cleanupStarted.future;
      final next = runtime.run(
        ref.update((value) {
          events.add(value);
          return value + 1;
        }),
      );
      expect(await runtime.runOrThrow(ref.get()), 0);
      expect(observer.values, [0]);
      expect(events, isEmpty);

      cleanup.complete();
      expect(_value(await writing), 'done');
      await next;
      expect(events, [1]);
      observer.release(0);
      await observer.seen(1);
      observer.release(1);
      await observer.seen(2);
      expect(observer.values, [0, 1, 2]);
      observer.release(2);
      await observer.completed;
    },
  );

  test(
    'all write forms share FIFO ordering and read the latest committed value',
    () async {
      final ref = await runtime.runOrThrow(SubscriptionRef.create(0));
      final gate = Completer<(String, int)>();
      final started = Completer<void>();
      final first = runtime.run(
        ref.modifyTask(
          (_) => Task.fromFuture(() {
            started.complete();
            return gate.future;
          }, onError: Error.throwWithStackTrace),
        ),
      );
      await started.future;
      final set = runtime.run(ref.set(2));
      final update = runtime.run(ref.update((value) => value + 1));
      final modify = runtime.run(ref.modify((value) => (value, value * 2)));
      final async = runtime.run(
        ref.modifyTask(
          (value) => ref.get().map((stored) {
            expect(stored, value);
            return ('last', value + 1);
          }),
        ),
      );
      gate.complete(('first', 1));
      expect(_value(await first), 'first');
      await set;
      await update;
      expect(_value(await modify), 3);
      expect(_value(await async), 'last');
      expect(await runtime.runOrThrow(ref.get()), 7);
    },
  );

  test(
    'business, defect, cancellation, and cleanup failures do not publish',
    () async {
      final ref = await runtime.runOrThrow(
        SubscriptionRef.create(0, capacity: 4),
      );
      final observer = _Observer(runtime, ref, 2);
      await observer.seen(0);
      final error = StateError('candidate');
      final failed = await runtime.run(
        ref.modifyTask<void, String>((_) => Task.fail('business')),
      );
      expect(_cause(failed), isA<CauseFailure<String>>());
      final defect = await runtime.run(ref.update((_) => throw error));
      expect(
        _cause(defect),
        isA<CauseDefect>().having((cause) => cause.error, 'error', same(error)),
      );
      final cleanup = await runtime.run(
        ref.modifyTask(
          (_) =>
              Task.succeed(('candidate', 1))
                  .ensuring(Task.sync(() => throw error)),
        ),
      );
      expect(_cause(cleanup), isA<CauseDefect>());
      final cancellation = await runtime.run(
        ref.modifyTask<void, Never>(
          (_) => Task.failCause(const Cause.cancelled('candidate')),
        ),
      );
      expect(_cause(cancellation), isA<CauseCancelled>());
      expect(await runtime.runOrThrow(ref.get()), 0);
      await runtime.runOrThrow(ref.set(2));
      observer.release(0);
      await observer.seen(1);
      expect(observer.values, [0, 2]);
      observer.release(1);
      await observer.completed;
    },
  );

  test('cancelling an asynchronous candidate waits for its Future and releases no partial value', () async {
    final ref = await runtime.runOrThrow(
      SubscriptionRef.create(0, capacity: 2),
    );
    final observer = _Observer(runtime, ref, 2);
    await observer.seen(0);
    final started = Completer<void>();
    final gate = Completer<(void, int)>();
    final controller = CancellationController();
    var cancelledSettled = false;
    final writing = runtime
        .run(
          ref.modifyTask<void, Never>(
            (_) => Task.fromFuture(() {
              started.complete();
              return gate.future;
            }, onError: Error.throwWithStackTrace),
          ),
          signal: controller.signal,
        )
        .then((exit) {
          cancelledSettled = true;
          return exit;
        });
    await started.future;
    controller.requestCancel();
    final next = runtime.run(ref.set(3));
    expect(await runtime.runOrThrow(ref.get()), 0);
    expect(cancelledSettled, isFalse);
    gate.complete((null, 2));
    expect(_cause(await writing), isA<CauseCancelled>());
    await next;
    observer.release(0);
    await observer.seen(1);
    expect(observer.values, [0, 3]);
    observer.release(1);
    await observer.completed;
  });

  test(
    'incompatible widened writes fail without changing state or broadcasting',
    () async {
      final original = await runtime.runOrThrow(
        SubscriptionRef.create(0, capacity: 5),
      );
      final SubscriptionRef<Object?> widened = original;
      final observer = _Observer(runtime, original, 2);
      await observer.seen(0);
      final attempts = <Task<Object?, Never>>[
        widened.set('invalid').map((_) => null),
        widened.update((_) => 'invalid').map((_) => null),
        widened.modify((_) => (null, 'invalid')),
        widened.modifyTask((_) => Task.succeed((null, 'invalid'))),
      ];
      for (final attempt in attempts) {
        expect(
          _cause(await runtime.run(attempt)),
          isA<CauseDefect>().having(
            (cause) => cause.error,
            'error',
            isA<TypeError>(),
          ),
        );
        expect(await runtime.runOrThrow(original.get()), 0);
      }
      await runtime.runOrThrow(widened.update((value) => (value as int) + 1));
      observer.release(0);
      await observer.seen(1);
      expect(observer.values, [0, 1]);
      observer.release(1);
      await observer.completed;
    },
  );

  test(
    'an idle cancelled subscription releases its wait without closing the ref',
    () async {
      final ref = await runtime.runOrThrow(_backPressure(0));
      final observer = _Observer(runtime, ref, 2);
      await observer.seen(0);
      observer.release(0);
      await runtime.runOrThrow(Task.yieldNow());
      observer.cancel();
      expect(_cause(await observer.completed), isA<CauseCancelled>());
      await runtime.runOrThrow(ref.set(1));
      await runtime.runOrThrow(ref.set(2));
      expect(await runtime.runOrThrow(ref.changes.take(1).toList()), [2]);
    },
  );

  test('invalid capacity is rejected only when the create task runs', () async {
    for (final capacity in [0, -1]) {
      final create = SubscriptionRef.create(0, capacity: capacity);
      expect(
        _cause(await runtime.run(create)),
        isA<CauseDefect>().having(
          (cause) => cause.error,
          'error',
          isA<RangeError>(),
        ),
      );
    }
  });
}

Task<SubscriptionRef<int>, Never> _backPressure(
  int initial, {
  int capacity = 1,
}) => SubscriptionRef.create(
  initial,
  capacity: capacity,
  overflow: SubscriptionOverflowStrategy.backPressure,
);

final class _Observer<T> {
  _Observer(Runtime runtime, SubscriptionRef<T> ref, int count)
    : _seen = List.generate(count, (_) => Completer<void>()),
      _gates = List.filled(count, null) {
    completed = runtime.run(
      ref.changes
          .take(count)
          .forEach(
            (value) => Task.fromCallback<void, Never>((complete) {
              final index = values.length;
              values.add(value);
              _gates[index] = complete;
              _seen[index].complete();
              return () => _gates[index] = null;
            }),
          ),
      signal: _controller.signal,
    );
    addTearDown(cancel);
  }

  final values = <T>[];
  final _controller = CancellationController();
  final List<Completer<void>> _seen;
  final List<void Function(Exit<void, Never>)?> _gates;
  late final Future<Exit<void, Never>> completed;

  Future<void> seen(int index) => _seen[index].future;

  void release(int index) {
    final complete = _gates[index];
    _gates[index] = null;
    complete?.call(const Exit.success(null));
  }

  void cancel() => _controller.requestCancel();
}

T _value<T, E>(Exit<T, E> exit) => switch (exit) {
  ExitSuccess(:final value) => value,
  ExitFailure(:final cause) => throw StateError('Expected success, got $cause'),
};

Cause<E> _cause<T, E>(Exit<T, E> exit) => switch (exit) {
  ExitFailure(:final cause) => cause,
  ExitSuccess() => throw StateError('Expected failure, got $exit'),
};
