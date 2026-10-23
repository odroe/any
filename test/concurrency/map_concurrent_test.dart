import 'dart:async';

import 'package:any/any.dart';
import 'package:any/concurrency.dart';
import 'package:test/test.dart';

void main() {
  test(
    'cancelling before the first microtask never subscribes upstream',
    () async {
      var subscribed = false;
      final source = StreamController<int>(onListen: () => subscribed = true);
      final subscription = source.stream
          .mapConcurrent(
            (value) => Task<int, Never, ()>((_) => Exit.success(value)),
            dependencies: (),
            concurrency: 1,
          )
          .listen((_) => fail('No event expected.'));
      await subscription.cancel();
      expect(subscribed, isFalse);
      unawaited(source.close());
    },
  );

  test('cancelling paused output reports an undelivered failed exit', () async {
    final gates = [Completer<void>(), Completer<void>()];
    final ready = Completer<void>();
    var started = 0;
    var delivered = 0;
    late StreamSubscription<Exit<int, String>> subscription;
    subscription = Stream.fromIterable([0, 1])
        .mapConcurrent(
          (index) => Task<int, String, ()>((_) async {
            if (++started == 2) ready.complete();
            await gates[index].future;
            return index == 0
                ? const Exit.success(0)
                : const Exit.failure(Cause.failure('undelivered'));
          }),
          dependencies: (),
          concurrency: 2,
        )
        .listen((_) {
          delivered++;
          subscription.pause();
        });
    await ready.future;
    gates[0].complete();
    await Future<void>.delayed(Duration.zero);
    gates[1].complete();
    await Future<void>.delayed(Duration.zero);
    expect(delivered, 1);
    await expectLater(
      subscription.cancel(),
      throwsA(
        isA<TaskStreamException<String>>().having(
          (e) => (e.cause as CauseFailure).error,
          'error',
          'undelivered',
        ),
      ),
    );
  });

  test('is lazy, bounds active work, and emits in completion order', () async {
    final gates = List.generate(3, (_) => Completer<void>());
    final ready = Completer<void>();
    final third = Completer<void>();
    final events = <int>[];
    var started = 0;
    var active = 0;
    var peak = 0;
    final stream = Stream.fromIterable([0, 1, 2]).mapConcurrent(
      (index) => Task<int, Never, ()>((context) async {
        started++;
        active++;
        if (active > peak) peak = active;
        if (started == 2) ready.complete();
        if (index == 2) third.complete();
        context.scope.addFinalizer((_) => active--);
        await gates[index].future;
        return Exit.success(index);
      }),
      dependencies: (),
      concurrency: 2,
    );
    expect(started, 0);
    final done = stream.forEach(
      (e) => events.add((e as ExitSuccess<int, Never>).value),
    );
    await ready.future;
    expect(started, 2);
    gates[1].complete();
    await third.future;
    gates[2].complete();
    await Future<void>.delayed(Duration.zero);
    gates[0].complete();
    await done;
    expect(events, [1, 2, 0]);
    expect(peak, 2);
    expect(active, 0);
  });

  test(
    'paused output bounds completed exits and pauses upstream dispatch',
    () async {
      var started = 0;
      final ready = Completer<void>();
      final gate = Completer<void>();
      final done = Completer<void>();
      final events = <Exit<int, Never>>[];
      late StreamSubscription<Exit<int, Never>> subscription;
      subscription = Stream.fromIterable(List.generate(20, (i) => i))
          .mapConcurrent(
            (value) => Task<int, Never, ()>((_) async {
              if (++started == 2) ready.complete();
              await gate.future;
              return Exit.success(value);
            }),
            dependencies: (),
            concurrency: 2,
          )
          .listen((exit) {
            events.add(exit);
            if (events.length == 1) subscription.pause();
          }, onDone: done.complete);
      await ready.future;
      gate.complete();
      await Future<void>.delayed(Duration.zero);
      expect(events, hasLength(1));
      expect(started, 2);
      subscription.resume();
      await done.future;
      expect(events, hasLength(20));
    },
  );

  test(
    'body failures, thrown factories, and cleanup failures remain data',
    () async {
      final cleanup = StateError('cleanup');
      final outcomes = await Stream.fromIterable([0, 1, 2, 3])
          .mapConcurrent<int, String, ()>(
            (value) {
              if (value == 0) throw StateError('factory');
              return Task((context) {
                if (value == 1) {
                  return const Exit.failure(Cause.failure('body'));
                }
                if (value == 2) {
                  context.scope.addFinalizer((_) => throw cleanup);
                }
                return Exit.success(value);
              });
            },
            dependencies: (),
            concurrency: 1,
          )
          .toList();
      expect((outcomes[0] as ExitFailure).cause, isA<CauseDefect>());
      expect(
        ((outcomes[1] as ExitFailure).cause as CauseFailure).error,
        'body',
      );
      expect(
        ((outcomes[2] as ExitFailure).cause as CauseDefect).error,
        same(cleanup),
      );
      expect((outcomes[3] as ExitSuccess).value, 3);
    },
  );

  test(
    'subscription cancellation immediately cancels children and awaits cleanup',
    () async {
      final started = Completer<void>();
      final stopped = Completer<void>();
      final cleanup = Completer<void>();
      var sourceCancelled = false;
      var completed = false;
      final source = StreamController<int>(
        onCancel: () => sourceCancelled = true,
      );
      final subscription = source.stream
          .mapConcurrent(
            (_) => Task<int, Never, ()>((context) async {
              context.cancellation.onCancel((_) => stopped.complete());
              context.scope.addFinalizer((_) => cleanup.future);
              started.complete();
              await stopped.future;
              return const Exit.success(1);
            }),
            dependencies: (),
            concurrency: 1,
          )
          .listen((_) => fail('No delivery after cancellation.'));
      source.add(1);
      await started.future;
      final cancelled = subscription.cancel().then((_) => completed = true);
      await stopped.future;
      expect(completed, isFalse);
      cleanup.complete();
      await cancelled;
      expect(sourceCancelled, isTrue);
      await source.close();
    },
  );

  test(
    'cancellation reports undelivered cleanup and source cleanup failures',
    () async {
      final ready = Completer<void>();
      final stop = Completer<void>();
      final childError = StateError('child');
      final sourceError = StateError('source');
      final source = StreamController<int>(onCancel: () => throw sourceError);
      final subscription = source.stream
          .mapConcurrent(
            (_) => Task<int, Never, ()>((context) async {
              context.scope.addFinalizer((_) => throw childError);
              context.cancellation.onCancel((_) => stop.complete());
              ready.complete();
              await stop.future;
              return const Exit.success(1);
            }),
            dependencies: (),
            concurrency: 1,
          )
          .listen((_) {});
      source.add(1);
      await ready.future;
      await expectLater(
        subscription.cancel(),
        throwsA(
          isA<TaskStreamException<Never>>().having(
            (e) => (e.cause as CauseGroup).causes.map(
              (c) => (c as CauseDefect).error,
            ),
            'causes',
            [childError, sourceError],
          ),
        ),
      );
      await source.close();
    },
  );

  test(
    'source errors stop dispatch, drain children, and retain traces',
    () async {
      final source = StreamController<int>();
      final started = Completer<void>();
      final stop = Completer<void>();
      final error = StateError('input');
      final trace = StackTrace.current;
      final cleanup = StateError('cleanup');
      final future = source.stream
          .mapConcurrent(
            (_) => Task<int, Never, ()>((context) async {
              context.scope.addFinalizer((_) => throw cleanup);
              context.cancellation.onCancel((_) => stop.complete());
              started.complete();
              await stop.future;
              return const Exit.success(1);
            }),
            dependencies: (),
            concurrency: 2,
          )
          .toList();
      source.add(1);
      await started.future;
      source.addError(error, trace);
      final exits = await future;
      expect(exits, hasLength(2));
      expect(
        ((exits.first as ExitFailure).cause as CauseDefect).error,
        same(cleanup),
      );
      final terminal = (exits.last as ExitFailure).cause as CauseDefect;
      expect(terminal.error, same(error));
      expect(terminal.stackTrace, same(trace));
      await source.close();
    },
  );

  test(
    'parent cancellation stops an idle source and preserves reason',
    () async {
      final controller = CancellationController();
      final source = StreamController<int>();
      final future = source.stream
          .mapConcurrent(
            (value) => Task<int, Never, ()>((_) => Exit.success(value)),
            dependencies: (),
            concurrency: 2,
            cancellation: controller.token,
          )
          .toList();
      await Future<void>.delayed(Duration.zero);
      controller.cancel(reason: 'parent');
      final exit = (await future).single as ExitFailure;
      expect((exit.cause as CauseCancelled).reason, 'parent');
      await source.close();
    },
  );

  test('pre-cancellation skips subscription and validates the limit', () async {
    final source = StreamController<int>(onListen: () => fail('Skipped.'));
    final controller = CancellationController()..cancel();
    Task<int, Never, ()> task(int value) => Task((_) => Exit.success(value));
    expect(
      () => source.stream.mapConcurrent(task, dependencies: (), concurrency: 0),
      throwsRangeError,
    );
    final results = await source.stream
        .mapConcurrent(
          task,
          dependencies: (),
          concurrency: 1,
          cancellation: controller.token,
        )
        .toList();
    expect((results.single as ExitFailure).cause, isA<CauseCancelled>());
    unawaited(source.close());
  });

  test(
    'empty and nullable streams complete without retaining all inputs',
    () async {
      final outcomes = await const Stream<int?>.empty()
          .mapConcurrent(
            (value) => Task<int?, Never, ()>((_) => Exit.success(value)),
            dependencies: (),
            concurrency: 1,
          )
          .toList();
      expect(outcomes, isEmpty);
      final values = await Stream<int?>.fromIterable([null, 1])
          .mapConcurrent(
            (value) => Task<int?, Never, ()>((_) => Exit.success(value)),
            dependencies: (),
            concurrency: 1,
          )
          .toList();
      expect(values.cast<ExitSuccess>().map((e) => e.value), [null, 1]);
    },
  );
}
