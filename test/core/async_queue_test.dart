import 'dart:async';

import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  late Runtime runtime;

  setUp(() => runtime = Runtime());
  tearDown(() async {
    expect(await runtime.close(), isA<ExitSuccess<void, Never>>());
  });

  test('repeated creation produces independently scoped queues', () async {
    final create = AsyncQueue.bounded<int>(1);
    final first = await _queue(runtime, create);
    final second = await _queue(runtime, create);
    expect(first, isNot(same(second)));

    expect(await runtime.runOrThrow(first.offer(1)), isTrue);
    expect(await runtime.runOrThrow(second.offer(2)), isTrue);
    expect(_some(await runtime.runOrThrow(first.take())), 1);
    expect(_some(await runtime.runOrThrow(second.take())), 2);
  });

  test('invalid capacity is a defect when creation executes', () async {
    for (final capacity in [-1, 0]) {
      final task = AsyncQueue.bounded<int>(capacity);
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

  test('returning a queue from its owning run closes it', () async {
    final queue = await runtime.runOrThrow(AsyncQueue.unbounded<int>());

    expect(_cause(await runtime.run(queue.offer(1))), _closed);
    expect(_cause(await runtime.run(queue.take())), _closed);
  });

  test('owner cancellation closes the queue and wakes consumers', () async {
    final controller = CancellationController();
    final queue = await _queue(
      runtime,
      AsyncQueue.unbounded<int>(),
      controller: controller,
    );
    final taking = runtime.run(queue.take());

    controller.requestCancel();

    expect(_cause(await taking), _closed);
    expect(_cause(await runtime.run(queue.offer(1))), _closed);
  });

  test(
    'unbounded offers are lazy and preserve FIFO and nullable values',
    () async {
      final queue = await _queue(runtime, AsyncQueue.unbounded<int?>());
      final first = queue.offer(null);
      final second = queue.offer(2);

      expect(await runtime.runOrThrow(second), isTrue);
      expect(await runtime.runOrThrow(first), isTrue);
      expect(await runtime.runOrThrow(first), isTrue);
      await runtime.runOrThrow(queue.end());

      expect(_some(await runtime.runOrThrow(queue.take())), 2);
      expect(_some(await runtime.runOrThrow(queue.take())), isNull);
      expect(_some(await runtime.runOrThrow(queue.take())), isNull);
      expect(await runtime.runOrThrow(queue.take()), isA<OptionNone<int?>>());
    },
  );

  test('backpressure admits pending offers in FIFO order', () async {
    final queue = await _queue(runtime, AsyncQueue.bounded<int>(1));
    expect(await runtime.runOrThrow(queue.offer(1)), isTrue);
    var thirdAccepted = false;
    final second = runtime.run(queue.offer(2));
    final third = runtime.run(
      queue.offer(3).map((accepted) {
        thirdAccepted = accepted;
        return accepted;
      }),
    );

    expect(thirdAccepted, isFalse);
    expect(_some(await runtime.runOrThrow(queue.take())), 1);
    expect(_value(await second), isTrue);
    expect(thirdAccepted, isFalse);
    expect(_some(await runtime.runOrThrow(queue.take())), 2);
    expect(_value(await third), isTrue);
    expect(_some(await runtime.runOrThrow(queue.take())), 3);
  });

  test('waiting consumers receive values in FIFO order', () async {
    final queue = await _queue(runtime, AsyncQueue.unbounded<int>());
    final first = runtime.run(queue.take());
    final second = runtime.run(queue.take());
    final third = runtime.run(queue.take());

    expect(await runtime.runOrThrow(queue.offer(1)), isTrue);
    expect(await runtime.runOrThrow(queue.offer(2)), isTrue);
    expect(await runtime.runOrThrow(queue.offer(3)), isTrue);

    expect(_some(_value(await first)), 1);
    expect(_some(_value(await second)), 2);
    expect(_some(_value(await third)), 3);
  });

  test('cancelling a pending offer removes only that producer', () async {
    final queue = await _queue(runtime, AsyncQueue.bounded<int>(1));
    final controller = CancellationController();
    await runtime.runOrThrow(queue.offer(1));
    final cancelled = runtime.run(queue.offer(2), signal: controller.signal);
    final waiting = runtime.run(queue.offer(3));

    controller.requestCancel(reason: 'skip two');
    expect(_cause(await cancelled), isA<CauseCancelled>());
    expect(_some(await runtime.runOrThrow(queue.take())), 1);
    expect(_value(await waiting), isTrue);
    expect(_some(await runtime.runOrThrow(queue.take())), 3);
    await runtime.runOrThrow(queue.end());
    expect(await runtime.runOrThrow(queue.take()), isA<OptionNone<int>>());
  });

  test('cancelling a pending take leaves other consumers waiting', () async {
    final queue = await _queue(runtime, AsyncQueue.unbounded<int>());
    final controller = CancellationController();
    final cancelled = runtime.run(queue.take(), signal: controller.signal);
    final waiting = runtime.run(queue.take());

    controller.requestCancel();
    expect(_cause(await cancelled), isA<CauseCancelled>());
    expect(await runtime.runOrThrow(queue.offer(7)), isTrue);
    expect(_some(_value(await waiting)), 7);
  });

  test('cancellation during take handoff preserves the value', () async {
    final queue = await _queue(runtime, AsyncQueue.bounded<int>(1));
    final controller = CancellationController();
    var armed = false;
    var intercepted = false;
    final cancelled = runZoned(
      () => runtime.run(queue.take(), signal: controller.signal),
      zoneSpecification: ZoneSpecification(
        scheduleMicrotask: (self, parent, zone, callback) {
          if (armed) {
            armed = false;
            intercepted = true;
            controller.requestCancel(reason: 'before take confirmation');
          }
          parent.scheduleMicrotask(zone, callback);
        },
      ),
    );
    final waiting = runtime.run(queue.take());

    armed = true;
    expect(await runtime.runOrThrow(queue.offer(7)), isTrue);

    expect(intercepted, isTrue);
    expect(_cause(await cancelled), isA<CauseCancelled>());
    expect(_some(_value(await waiting)), 7);
    await runtime.runOrThrow(queue.end());
    expect(await runtime.runOrThrow(queue.take()), isA<OptionNone<int>>());
  });

  test('an unconfirmed take continues to occupy buffer capacity', () async {
    final queue = await _queue(runtime, AsyncQueue.bounded<int>(1));
    final controller = CancellationController();
    var armed = false;
    Future<Exit<bool, QueueClosed>>? secondOffer;
    final taking = runZoned(
      () => runtime.run(queue.take(), signal: controller.signal),
      zoneSpecification: ZoneSpecification(
        scheduleMicrotask: (self, parent, zone, callback) {
          if (armed) {
            armed = false;
            secondOffer = runtime.run(queue.offer(2));
            controller.requestCancel();
          }
          parent.scheduleMicrotask(zone, callback);
        },
      ),
    );

    armed = true;
    expect(await runtime.runOrThrow(queue.offer(1)), isTrue);
    expect(_cause(await taking), isA<CauseCancelled>());
    await runtime.runOrThrow(queue.end());

    expect(secondOffer, isNotNull);
    expect(_cause(await secondOffer!), _closed);
    expect(_some(await runtime.runOrThrow(queue.take())), 1);
    expect(await runtime.runOrThrow(queue.take()), isA<OptionNone<int>>());
  });

  test(
    'cancellation after offer acceptance keeps the accepted value',
    () async {
      final queue = await _queue(runtime, AsyncQueue.bounded<int>(1));
      final controller = CancellationController();
      await runtime.runOrThrow(queue.offer(1));
      var armed = false;
      var intercepted = false;
      final offering = runZoned(
        () => runtime.run(queue.offer(2), signal: controller.signal),
        zoneSpecification: ZoneSpecification(
          scheduleMicrotask: (self, parent, zone, callback) {
            if (armed) {
              armed = false;
              intercepted = true;
              controller.requestCancel(reason: 'after acceptance');
            }
            parent.scheduleMicrotask(zone, callback);
          },
        ),
      );

      armed = true;
      expect(_some(await runtime.runOrThrow(queue.take())), 1);
      expect(_cause(await offering), isA<CauseCancelled>());
      expect(intercepted, isTrue);
      expect(_some(await runtime.runOrThrow(queue.take())), 2);
    },
  );

  test('pre-cancelled operations do not alter buffered values', () async {
    final queue = await _queue(runtime, AsyncQueue.bounded<int>(1));
    final controller = CancellationController()..requestCancel();
    expect(
      _cause(await runtime.run(queue.offer(1), signal: controller.signal)),
      isA<CauseCancelled>(),
    );
    await runtime.runOrThrow(queue.offer(2));
    expect(
      _cause(await runtime.run(queue.take(), signal: controller.signal)),
      isA<CauseCancelled>(),
    );

    await runtime.runOrThrow(queue.end());
    expect(_some(await runtime.runOrThrow(queue.take())), 2);
    expect(await runtime.runOrThrow(queue.take()), isA<OptionNone<int>>());
  });

  test('end rejects unaccepted offers and drains accepted values', () async {
    final queue = await _queue(runtime, AsyncQueue.bounded<int>(1));
    await runtime.runOrThrow(queue.offer(1));
    final second = runtime.run(queue.offer(2));
    final third = runtime.run(queue.offer(3));

    await runtime.runOrThrow(queue.end());
    await runtime.runOrThrow(queue.end());

    expect(_cause(await second), _closed);
    expect(_cause(await third), _closed);
    expect(_cause(await runtime.run(queue.offer(4))), _closed);
    expect(_some(await runtime.runOrThrow(queue.take())), 1);
    expect(await runtime.runOrThrow(queue.take()), isA<OptionNone<int>>());
    expect(await runtime.runOrThrow(queue.take()), isA<OptionNone<int>>());
  });

  test('end wakes all consumers of an empty queue with absence', () async {
    final queue = await _queue(runtime, AsyncQueue.unbounded<int>());
    final first = runtime.run(queue.take());
    final second = runtime.run(queue.take());

    await runtime.runOrThrow(queue.end());

    expect(_value(await first), isA<OptionNone<int>>());
    expect(_value(await second), isA<OptionNone<int>>());
  });

  test('close discards buffered values and fails pending offers', () async {
    final queue = await _queue(runtime, AsyncQueue.bounded<int>(1));
    await runtime.runOrThrow(queue.offer(1));
    final offering = runtime.run(queue.offer(2));

    await runtime.runOrThrow(queue.close());
    await runtime.runOrThrow(queue.close());
    await runtime.runOrThrow(queue.end());

    expect(_cause(await offering), _closed);
    expect(_cause(await runtime.run(queue.take())), _closed);
    expect(_cause(await runtime.run(queue.offer(3))), _closed);
  });

  test('close wakes all pending consumers with QueueClosed', () async {
    final queue = await _queue(runtime, AsyncQueue.unbounded<int>());
    final first = runtime.run(queue.take());
    final second = runtime.run(queue.take());

    await runtime.runOrThrow(queue.close());

    expect(_cause(await first), _closed);
    expect(_cause(await second), _closed);
  });

  test(
    'close before take confirmation does not deliver a stale value',
    () async {
      final queue = await _queue(runtime, AsyncQueue.unbounded<int>());
      var armed = false;
      Future<Exit<void, Never>>? closing;
      final taking = runZoned(
        () => runtime.run(queue.take()),
        zoneSpecification: ZoneSpecification(
          scheduleMicrotask: (self, parent, zone, callback) {
            if (armed) {
              armed = false;
              closing = runtime.run(queue.close());
            }
            parent.scheduleMicrotask(zone, callback);
          },
        ),
      );

      armed = true;
      expect(await runtime.runOrThrow(queue.offer(7)), isTrue);

      expect(_cause(await taking), _closed);
      expect(closing, isNotNull);
      expect(await closing!, isA<ExitSuccess<void, Never>>());
    },
  );

  test('dropping rejects new values without evicting older ones', () async {
    final queue = await _queue(
      runtime,
      AsyncQueue.bounded<int>(2, overflow: QueueOverflowStrategy.dropping),
    );

    expect(await runtime.runOrThrow(queue.offer(1)), isTrue);
    expect(await runtime.runOrThrow(queue.offer(2)), isTrue);
    expect(await runtime.runOrThrow(queue.offer(3)), isFalse);
    expect(_some(await runtime.runOrThrow(queue.take())), 1);
    expect(_some(await runtime.runOrThrow(queue.take())), 2);
  });

  test(
    'sliding accepts new values and evicts the oldest buffer entry',
    () async {
      final queue = await _queue(
        runtime,
        AsyncQueue.bounded<int>(2, overflow: QueueOverflowStrategy.sliding),
      );

      expect(await runtime.runOrThrow(queue.offer(1)), isTrue);
      expect(await runtime.runOrThrow(queue.offer(2)), isTrue);
      expect(await runtime.runOrThrow(queue.offer(3)), isTrue);
      expect(_some(await runtime.runOrThrow(queue.take())), 2);
      expect(_some(await runtime.runOrThrow(queue.take())), 3);
    },
  );

  test('sliding can evict a value before a woken take confirms it', () async {
    final queue = await _queue(
      runtime,
      AsyncQueue.bounded<int>(1, overflow: QueueOverflowStrategy.sliding),
    );
    var armed = false;
    Future<Exit<bool, QueueClosed>>? secondOffer;
    final taking = runZoned(
      () => runtime.run(queue.take()),
      zoneSpecification: ZoneSpecification(
        scheduleMicrotask: (self, parent, zone, callback) {
          if (armed) {
            armed = false;
            secondOffer = runtime.run(queue.offer(2));
          }
          parent.scheduleMicrotask(zone, callback);
        },
      ),
    );

    armed = true;
    expect(await runtime.runOrThrow(queue.offer(1)), isTrue);

    expect(secondOffer, isNotNull);
    expect(_value(await secondOffer!), isTrue);
    expect(_some(_value(await taking)), 2);
  });

  test('widening a queue cannot insert an incompatible value', () async {
    final queue = await _queue(runtime, AsyncQueue.unbounded<int>());
    final AsyncQueue<Object?> widened = queue;

    expect(() => widened.offer('wrong type'), throwsA(isA<TypeError>()));
    expect(await runtime.runOrThrow(widened.offer(7)), isTrue);
    expect(_some(await runtime.runOrThrow(queue.take())), 7);
  });
}

Future<AsyncQueue<T>> _queue<T>(
  Runtime runtime,
  Task<AsyncQueue<T>, Never> create, {
  CancellationController? controller,
}) {
  final owner = controller ?? CancellationController();
  final ready = Completer<AsyncQueue<T>>();
  final running = runtime.run(
    create.flatMap(
      (queue) => Task.fromCallback<void, Never>((_) {
        ready.complete(queue);
        return () {};
      }),
    ),
    signal: owner.signal,
  );
  addTearDown(() async {
    owner.requestCancel();
    expect(_cause(await running), isA<CauseCancelled>());
  });
  return ready.future;
}

Matcher get _closed => isA<CauseFailure<QueueClosed>>().having(
  (cause) => cause.error,
  'error',
  isA<QueueClosed>(),
);

T _some<T>(Option<T> option) => switch (option) {
  OptionSome(:final value) => value,
  OptionNone() => fail('Expected a queued value, got normal completion.'),
};

T _value<T, E>(Exit<T, E> exit) => switch (exit) {
  ExitSuccess(:final value) => value,
  ExitFailure(:final cause) => fail('Expected success, got $cause.'),
};

Cause<E> _cause<T, E>(Exit<T, E> exit) => switch (exit) {
  ExitSuccess(:final value) => fail('Expected failure, got $value.'),
  ExitFailure(:final cause) => cause,
};
