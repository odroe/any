import 'dart:async';

import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  late Runtime runtime;

  setUp(() => runtime = Runtime());
  tearDown(() async {
    expect(await runtime.close(), isA<ExitSuccess<void, Never>>());
  });

  test(
    'each factory execution creates an independent scoped pub-sub',
    () async {
      final create = PubSub.bounded<int>(1);
      final first = await _owned(runtime, create);
      final second = await _owned(runtime, create);
      final firstReader = await _owned(runtime, first.subscribe());
      final secondReader = await _owned(runtime, second.subscribe());
      final publish = first.publish(1);

      expect(first, isNot(same(second)));
      await runtime.runOrThrow(second.publish(2));
      await runtime.runOrThrow(publish);
      expect(_some(await runtime.runOrThrow(firstReader.take())), 1);
      expect(_some(await runtime.runOrThrow(secondReader.take())), 2);
      await runtime.runOrThrow(publish);
      expect(_some(await runtime.runOrThrow(firstReader.take())), 1);
    },
  );

  test('invalid capacity and replay fail when the factory executes', () async {
    for (final (capacity, replay) in [(-1, 0), (0, 0), (1, -1), (1, 2)]) {
      final create = PubSub.bounded<int>(capacity, replay: replay);
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

  test('returning the pub-sub from its owner closes it', () async {
    final pubSub = await runtime.runOrThrow(PubSub.bounded<int>(1));

    expect(_cause(await runtime.run(pubSub.publish(1))), _closed);
    expect(_cause(await runtime.run(pubSub.subscribe())), _closed);
  });

  test(
    'a subscription closes with its own scope without closing its pub-sub',
    () async {
      final pubSub = await _owned(runtime, PubSub.bounded<int>(1));
      final closed = await runtime.runOrThrow(pubSub.subscribe());
      final active = await _owned(runtime, pubSub.subscribe());

      expect(_cause(await runtime.run(closed.take())), _closed);
      await runtime.runOrThrow(pubSub.publish(7));
      expect(_some(await runtime.runOrThrow(active.take())), 7);
    },
  );

  test(
    'each subscriber receives publications in order, including null',
    () async {
      final pubSub = await _owned(runtime, PubSub.bounded<int?>(2));
      final first = await _owned(runtime, pubSub.subscribe());
      final second = await _owned(runtime, pubSub.subscribe());
      final firstA = runtime.run(first.take());
      final firstB = runtime.run(first.take());
      final secondA = runtime.run(second.take());
      final secondB = runtime.run(second.take());

      await runtime.runOrThrow(pubSub.publish(null));
      await runtime.runOrThrow(pubSub.publish(2));

      expect(_some(_value(await firstA)), isNull);
      expect(_some(_value(await firstB)), 2);
      expect(_some(_value(await secondA)), isNull);
      expect(_some(_value(await secondB)), 2);
    },
  );

  test(
    'slow subscribers backpressure the entire publication in FIFO order',
    () async {
      final pubSub = await _owned(runtime, PubSub.bounded<int>(1));
      final fast = await _owned(runtime, pubSub.subscribe());
      final slow = await _owned(runtime, pubSub.subscribe());
      final accepted = <int>[];
      await runtime.runOrThrow(pubSub.publish(1));
      final second = runtime.run(pubSub.publish(2).map((_) => accepted.add(2)));
      final third = runtime.run(pubSub.publish(3).map((_) => accepted.add(3)));
      expect(_some(await runtime.runOrThrow(fast.take())), 1);
      var fastReceivedSecond = false;
      final fastSecond = runtime.run(
        fast.take().map((value) {
          fastReceivedSecond = true;
          return value;
        }),
      );

      expect(accepted, isEmpty);
      expect(fastReceivedSecond, isFalse);
      expect(_some(await runtime.runOrThrow(slow.take())), 1);
      expect(await second, isA<ExitSuccess<void, PubSubClosed>>());
      expect(_some(_value(await fastSecond)), 2);
      expect(accepted, [2]);

      expect(_some(await runtime.runOrThrow(slow.take())), 2);
      expect(await third, isA<ExitSuccess<void, PubSubClosed>>());
      expect(accepted, [2, 3]);
      expect(_some(await runtime.runOrThrow(fast.take())), 3);
      expect(_some(await runtime.runOrThrow(slow.take())), 3);
    },
  );

  test(
    'cancelling a blocked publication removes it for every subscriber',
    () async {
      final pubSub = await _owned(runtime, PubSub.bounded<int>(1));
      final first = await _owned(runtime, pubSub.subscribe());
      final second = await _owned(runtime, pubSub.subscribe());
      final controller = CancellationController();
      await runtime.runOrThrow(pubSub.publish(1));
      final cancelled = runtime.run(
        pubSub.publish(2),
        signal: controller.signal,
      );
      final waiting = runtime.run(pubSub.publish(3));

      controller.requestCancel();
      expect(_cause(await cancelled), isA<CauseCancelled>());
      expect(_some(await runtime.runOrThrow(first.take())), 1);
      expect(_some(await runtime.runOrThrow(second.take())), 1);
      expect(await waiting, isA<ExitSuccess<void, PubSubClosed>>());
      await runtime.runOrThrow(pubSub.end());

      for (final reader in [first, second]) {
        expect(_some(await runtime.runOrThrow(reader.take())), 3);
        expect(await runtime.runOrThrow(reader.take()), isA<OptionNone<int>>());
      }
    },
  );

  test('closing a slow subscription releases publisher backpressure', () async {
    final pubSub = await _owned(runtime, PubSub.bounded<int>(1));
    final fast = await _owned(runtime, pubSub.subscribe());
    final slow = await _owned(runtime, pubSub.subscribe());
    await runtime.runOrThrow(pubSub.publish(1));
    expect(_some(await runtime.runOrThrow(fast.take())), 1);
    final waiting = runtime.run(pubSub.publish(2));

    await runtime.runOrThrow(slow.close());
    await runtime.runOrThrow(slow.close());

    expect(await waiting, isA<ExitSuccess<void, PubSubClosed>>());
    expect(_some(await runtime.runOrThrow(fast.take())), 2);
    expect(_cause(await runtime.run(slow.take())), _closed);
    await runtime.runOrThrow(pubSub.publish(3));
    expect(_some(await runtime.runOrThrow(fast.take())), 3);
  });

  test('without subscribers, only configured replay is retained', () async {
    final pubSub = await _owned(runtime, PubSub.bounded<int>(3, replay: 2));
    final noReplay = await _owned(runtime, PubSub.bounded<int>(1));
    for (var value = 1; value <= 5; value++) {
      await runtime.runOrThrow(pubSub.publish(value));
      await runtime.runOrThrow(noReplay.publish(value));
    }
    final replay = await _owned(runtime, pubSub.subscribe());
    final fresh = await _owned(runtime, noReplay.subscribe());
    await runtime.runOrThrow(pubSub.publish(6));
    await runtime.runOrThrow(noReplay.publish(6));
    await runtime.runOrThrow(pubSub.end());
    await runtime.runOrThrow(noReplay.end());

    for (final value in [4, 5, 6]) {
      expect(_some(await runtime.runOrThrow(replay.take())), value);
    }
    expect(await runtime.runOrThrow(replay.take()), isA<OptionNone<int>>());
    expect(_some(await runtime.runOrThrow(fresh.take())), 6);
    expect(await runtime.runOrThrow(fresh.take()), isA<OptionNone<int>>());
  });

  test(
    'replay equal to capacity does not block replacing consumed history',
    () async {
      final pubSub = await _owned(runtime, PubSub.bounded<int>(2, replay: 2));
      for (final value in [1, 2, 3, 4]) {
        await runtime.runOrThrow(pubSub.publish(value));
      }
      final reader = await _owned(runtime, pubSub.subscribe());
      var fifthAccepted = false;
      final fifth = runtime.run(
        pubSub.publish(5).map((_) => fifthAccepted = true),
      );
      expect(fifthAccepted, isFalse);
      expect(_some(await runtime.runOrThrow(reader.take())), 3);
      expect(_value(await fifth), isTrue);
      final sixth = runtime.run(pubSub.publish(6));
      expect(_some(await runtime.runOrThrow(reader.take())), 4);
      expect(await sixth, isA<ExitSuccess<void, PubSubClosed>>());
      expect(_some(await runtime.runOrThrow(reader.take())), 5);
      expect(_some(await runtime.runOrThrow(reader.take())), 6);
    },
  );

  test(
    'a live-only subscription skips values retained for an older reader',
    () async {
      final pubSub = await _owned(runtime, PubSub.bounded<int>(1));
      final older = await _owned(runtime, pubSub.subscribe());
      await runtime.runOrThrow(pubSub.publish(1));
      final newer = await _owned(runtime, pubSub.subscribe());
      final reading = runtime.run(newer.take());
      final publishing = runtime.run(pubSub.publish(2));

      expect(_some(await runtime.runOrThrow(older.take())), 1);
      expect(await publishing, isA<ExitSuccess<void, PubSubClosed>>());
      expect(_some(_value(await reading)), 2);
      expect(_some(await runtime.runOrThrow(older.take())), 2);
    },
  );

  test(
    'a subscriber snapshot meets live publication without gaps or duplicates',
    () async {
      final pubSub = await _owned(runtime, PubSub.bounded<int>(3, replay: 2));
      await runtime.runOrThrow(pubSub.publish(1));
      final before = await _owned(
        runtime,
        pubSub.subscribe().tap((_) => pubSub.publish(2)),
      );
      final after = await _owned(runtime, pubSub.subscribe());
      await runtime.runOrThrow(pubSub.publish(3));
      await runtime.runOrThrow(pubSub.end());

      for (final reader in [before, after]) {
        for (final value in [1, 2, 3]) {
          expect(_some(await runtime.runOrThrow(reader.take())), value);
        }
        expect(await runtime.runOrThrow(reader.take()), isA<OptionNone<int>>());
      }
    },
  );

  test(
    'end rejects pending publications and lets every reader drain',
    () async {
      final pubSub = await _owned(runtime, PubSub.bounded<int>(1, replay: 1));
      final first = await _owned(runtime, pubSub.subscribe());
      final second = await _owned(runtime, pubSub.subscribe());
      await runtime.runOrThrow(pubSub.publish(1));
      final pending = runtime.run(pubSub.publish(2));

      await runtime.runOrThrow(pubSub.end());
      await runtime.runOrThrow(pubSub.end());
      expect(_cause(await pending), _closed);
      expect(_cause(await runtime.run(pubSub.publish(3))), _closed);
      final late = await _owned(runtime, pubSub.subscribe());

      for (final reader in [first, second, late]) {
        expect(_some(await runtime.runOrThrow(reader.take())), 1);
        expect(await runtime.runOrThrow(reader.take()), isA<OptionNone<int>>());
      }
    },
  );

  test('end wakes all pending takes on empty subscriptions', () async {
    final pubSub = await _owned(runtime, PubSub.bounded<int>(1));
    final first = await _owned(runtime, pubSub.subscribe());
    final second = await _owned(runtime, pubSub.subscribe());
    final waiting = [
      runtime.run(first.take()),
      runtime.run(first.take()),
      runtime.run(second.take()),
    ];

    await runtime.runOrThrow(pubSub.end());

    for (final take in waiting) {
      expect(_value(await take), isA<OptionNone<int>>());
    }
  });

  test(
    'close fails pending publishers and readers and discards unread values',
    () async {
      final pubSub = await _owned(runtime, PubSub.bounded<int>(1));
      final fast = await _owned(runtime, pubSub.subscribe());
      final slow = await _owned(runtime, pubSub.subscribe());
      await runtime.runOrThrow(pubSub.publish(1));
      expect(_some(await runtime.runOrThrow(fast.take())), 1);
      final reading = runtime.run(fast.take());
      final publishing = runtime.run(pubSub.publish(2));

      await runtime.runOrThrow(pubSub.close());
      await runtime.runOrThrow(pubSub.close());
      await runtime.runOrThrow(pubSub.end());

      expect(_cause(await reading), _closed);
      expect(_cause(await publishing), _closed);
      expect(_cause(await runtime.run(slow.take())), _closed);
      expect(_cause(await runtime.run(pubSub.subscribe())), _closed);
    },
  );

  test('cancelling a pending take does not cancel its subscription', () async {
    final pubSub = await _owned(runtime, PubSub.bounded<int>(1));
    final reader = await _owned(runtime, pubSub.subscribe());
    final controller = CancellationController();
    final cancelled = runtime.run(reader.take(), signal: controller.signal);
    final next = runtime.run(reader.take());

    controller.requestCancel();
    expect(_cause(await cancelled), isA<CauseCancelled>());
    await runtime.runOrThrow(pubSub.publish(7));
    expect(_some(_value(await next)), 7);
  });

  test(
    'cancelling during take handoff preserves that subscriber value',
    () async {
      final pubSub = await _owned(runtime, PubSub.bounded<int?>(1));
      final reader = await _owned(runtime, pubSub.subscribe());
      final other = await _owned(runtime, pubSub.subscribe());
      final controller = CancellationController();
      var armed = false;
      var intercepted = false;
      final cancelled = runZoned(
        () => runtime.run(reader.take(), signal: controller.signal),
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
      final next = runtime.run(reader.take());

      armed = true;
      await runtime.runOrThrow(pubSub.publish(null));

      expect(intercepted, isTrue);
      expect(_cause(await cancelled), isA<CauseCancelled>());
      expect(_some(_value(await next)), isNull);
      expect(_some(await runtime.runOrThrow(other.take())), isNull);
      await runtime.runOrThrow(pubSub.end());
      expect(await runtime.runOrThrow(reader.take()), isA<OptionNone<int?>>());
    },
  );

  test('an unconfirmed take still occupies the shared buffer', () async {
    final pubSub = await _owned(runtime, PubSub.bounded<int>(1));
    final reader = await _owned(runtime, pubSub.subscribe());
    final controller = CancellationController();
    var armed = false;
    Future<Exit<void, PubSubClosed>>? second;
    final taking = runZoned(
      () => runtime.run(reader.take(), signal: controller.signal),
      zoneSpecification: ZoneSpecification(
        scheduleMicrotask: (self, parent, zone, callback) {
          if (armed) {
            armed = false;
            second = runtime.run(pubSub.publish(2));
            controller.requestCancel();
          }
          parent.scheduleMicrotask(zone, callback);
        },
      ),
    );

    armed = true;
    await runtime.runOrThrow(pubSub.publish(1));
    expect(_cause(await taking), isA<CauseCancelled>());
    await runtime.runOrThrow(pubSub.end());

    expect(second, isNotNull);
    expect(_cause(await second!), _closed);
    expect(_some(await runtime.runOrThrow(reader.take())), 1);
  });

  test(
    'cancellation after publication acceptance does not retract broadcast',
    () async {
      final pubSub = await _owned(runtime, PubSub.bounded<int>(1));
      final first = await _owned(runtime, pubSub.subscribe());
      final second = await _owned(runtime, pubSub.subscribe());
      final controller = CancellationController();
      await runtime.runOrThrow(pubSub.publish(1));
      var armed = false;
      final publishing = runZoned(
        () => runtime.run(pubSub.publish(2), signal: controller.signal),
        zoneSpecification: ZoneSpecification(
          scheduleMicrotask: (self, parent, zone, callback) {
            if (armed) {
              armed = false;
              controller.requestCancel();
            }
            parent.scheduleMicrotask(zone, callback);
          },
        ),
      );
      expect(_some(await runtime.runOrThrow(first.take())), 1);
      armed = true;
      expect(_some(await runtime.runOrThrow(second.take())), 1);

      expect(_cause(await publishing), isA<CauseCancelled>());
      expect(_some(await runtime.runOrThrow(first.take())), 2);
      expect(_some(await runtime.runOrThrow(second.take())), 2);
    },
  );

  for (final closeHub in [false, true]) {
    test(
      '${closeHub ? 'hub' : 'subscription'} close before confirmation rejects a reserved take',
      () async {
        final pubSub = await _owned(runtime, PubSub.bounded<int>(1));
        final reader = await _owned(runtime, pubSub.subscribe());
        final other = await _owned(runtime, pubSub.subscribe());
        var armed = false;
        Future<Exit<void, Never>>? closing;
        final taking = runZoned(
          () => runtime.run(reader.take()),
          zoneSpecification: ZoneSpecification(
            scheduleMicrotask: (self, parent, zone, callback) {
              if (armed) {
                armed = false;
                closing = runtime.run(
                  closeHub ? pubSub.close() : reader.close(),
                );
              }
              parent.scheduleMicrotask(zone, callback);
            },
          ),
        );

        armed = true;
        await runtime.runOrThrow(pubSub.publish(1));

        expect(closing, isNotNull);
        expect(await closing!, isA<ExitSuccess<void, Never>>());
        expect(_cause(await taking), _closed);
        if (closeHub) {
          expect(_cause(await runtime.run(other.take())), _closed);
        } else {
          expect(_some(await runtime.runOrThrow(other.take())), 1);
        }
      },
    );
  }

  test(
    'subscription owner cancellation releases space without closing the hub',
    () async {
      final pubSub = await _owned(runtime, PubSub.bounded<int>(1));
      final controller = CancellationController();
      final slow = await _owned(
        runtime,
        pubSub.subscribe(),
        controller: controller,
      );
      final fast = await _owned(runtime, pubSub.subscribe());
      await runtime.runOrThrow(pubSub.publish(1));
      expect(_some(await runtime.runOrThrow(fast.take())), 1);
      final publishing = runtime.run(pubSub.publish(2));

      controller.requestCancel();

      expect(await publishing, isA<ExitSuccess<void, PubSubClosed>>());
      expect(_cause(await runtime.run(slow.take())), _closed);
      expect(_some(await runtime.runOrThrow(fast.take())), 2);
    },
  );

  test(
    'hub owner cancellation closes independent subscription scopes',
    () async {
      final controller = CancellationController();
      final pubSub = await _owned(
        runtime,
        PubSub.bounded<int>(1),
        controller: controller,
      );
      final fast = await _owned(runtime, pubSub.subscribe());
      final slow = await _owned(runtime, pubSub.subscribe());
      await runtime.runOrThrow(pubSub.publish(1));
      expect(_some(await runtime.runOrThrow(fast.take())), 1);
      final reading = runtime.run(fast.take());
      final publishing = runtime.run(pubSub.publish(2));

      controller.requestCancel();

      expect(_cause(await reading), _closed);
      expect(_cause(await publishing), _closed);
      expect(_cause(await runtime.run(slow.take())), _closed);
    },
  );

  test(
    'last unsubscribe allows pending publications without retaining history',
    () async {
      final pubSub = await _owned(runtime, PubSub.bounded<int>(1));
      final reader = await _owned(runtime, pubSub.subscribe());
      await runtime.runOrThrow(pubSub.publish(1));
      final second = runtime.run(pubSub.publish(2));
      final third = runtime.run(pubSub.publish(3));

      await runtime.runOrThrow(reader.close());
      expect(await second, isA<ExitSuccess<void, PubSubClosed>>());
      expect(await third, isA<ExitSuccess<void, PubSubClosed>>());
      final late = await _owned(runtime, pubSub.subscribe());
      await runtime.runOrThrow(pubSub.end());
      expect(await runtime.runOrThrow(late.take()), isA<OptionNone<int>>());
    },
  );
}

Future<T> _owned<T, E>(
  Runtime runtime,
  Task<T, E> create, {
  CancellationController? controller,
}) {
  final owner = controller ?? CancellationController();
  final ready = Completer<T>();
  final running = runtime.run(
    create.flatMap(
      (value) => Task.fromCallback<void, Never>((_) {
        ready.complete(value);
        return () {};
      }).withError<E>(),
    ),
    signal: owner.signal,
  );
  addTearDown(() async {
    owner.requestCancel();
    expect(_cause(await running), isA<CauseCancelled>());
  });
  return ready.future;
}

Matcher get _closed => isA<CauseFailure<PubSubClosed>>().having(
  (cause) => cause.error,
  'error',
  isA<PubSubClosed>(),
);

T _some<T>(Option<T> option) => switch (option) {
  OptionSome(:final value) => value,
  OptionNone() => fail('Expected a published value, got normal completion.'),
};

T _value<T, E>(Exit<T, E> exit) => switch (exit) {
  ExitSuccess(:final value) => value,
  ExitFailure(:final cause) => fail('Expected success, got $cause.'),
};

Cause<E> _cause<T, E>(Exit<T, E> exit) => switch (exit) {
  ExitSuccess(:final value) => fail('Expected failure, got $value.'),
  ExitFailure(:final cause) => cause,
};
