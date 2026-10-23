import 'dart:async';

import 'package:any/any.dart';
import 'package:any/concurrency.dart';
import 'package:any/schedule.dart';
import 'package:test/test.dart';

void main() {
  test('ready futures do not reset the event-yield budget', () async {
    final controller = CancellationController();
    var calls = 0;
    var task = Task<int, Never, ()>((_) {
      Timer.run(() => controller.cancel());
      return const Exit.success(0);
    });
    for (var i = 0; i < 10000; i++) {
      task = task.flatMap(
        (value) => Task((_) {
          calls++;
          return Future.value(Exit.success(value + 1));
        }),
      );
    }
    final exit = await task.run(
      (),
      cancellation: controller.token,
      yieldEvery: 128,
    );
    expect((exit as ExitFailure).cause, isA<CauseCancelled>());
    expect(calls, lessThan(128));
  });

  test(
    'timeouts can cancel a long chain when voluntary yielding is enabled',
    () async {
      var task = Task<int, Never, ()>((_) => const Exit.success(0));
      for (var i = 0; i < 10000; i++) {
        task = task.map((value) => value + 1);
      }
      final exit =
          await task.timeout(Duration.zero).run((), yieldEvery: 128)
              as ExitFailure;
      expect((exit.cause as CauseCancelled).reason, isA<TimeoutException>());
    },
  );

  test('default execution retains the synchronous fast path', () async {
    var timerRan = false;
    final event = Completer<void>();
    Timer.run(() {
      timerRan = true;
      event.complete();
    });
    var task = Task<int, Never, ()>((_) => const Exit.success(0));
    for (var i = 0; i < 10000; i++) {
      task = task.map((n) => n + 1);
    }
    expect((await task.run(()) as ExitSuccess).value, 10000);
    expect(timerRan, isFalse);
    await event.future;
  });

  test(
    'yields during graph traversal and skips callbacks after cancellation',
    () async {
      final controller = CancellationController();
      var called = false;
      var task = Task<int, Never, ()>((_) {
        called = true;
        return const Exit.success(0);
      });
      for (var i = 0; i < 10000; i++) {
        task = task.map((n) => n + 1);
      }
      Timer.run(() => controller.cancel(reason: 'event'));
      final exit = await task.run(
        (),
        cancellation: controller.token,
        yieldEvery: 128,
      ) as ExitFailure;
      expect((exit.cause as CauseCancelled).reason, 'event');
      expect(called, isFalse);
    },
  );

  test('yields while applying synchronous map and flatMap steps', () async {
    for (final flat in [false, true]) {
      final controller = CancellationController();
      var calls = 0;
      var closed = false;
      var task = Task<int, Never, ()>((context) {
        context.scope.addFinalizer((_) => closed = true);
        Timer.run(() => controller.cancel(reason: 'during callbacks'));
        return const Exit.success(0);
      });
      for (var i = 0; i < 10000; i++) {
        task = flat
            ? task.flatMap((n) {
                calls++;
                return Task((_) => Exit.success(n + 1));
              })
            : task.map((n) {
                calls++;
                return n + 1;
              });
      }
      final exit = await task.run(
        (),
        cancellation: controller.token,
        yieldEvery: 128,
      ) as ExitFailure;
      expect(exit.cause, isA<CauseCancelled>());
      expect(calls, lessThan(128));
      expect(closed, isTrue);
    }
  });

  test(
    'cancelled error chains still translate errors and skip recovery',
    () async {
      final controller = CancellationController();
      var calls = 0;
      var task = Task<int, int, ()>((_) {
        Timer.run(() => controller.cancel());
        return const Exit.failure(Cause.failure(0));
      });
      for (var i = 0; i < 10000; i++) {
        task = task.mapError((error) {
          calls++;
          return error + 1;
        });
      }
      final converted = task
          .mapError((error) => '$error')
          .recover((_) => throw StateError('Must skip recovery.'));
      final exit = await converted.run(
        (),
        cancellation: controller.token,
        yieldEvery: 128,
      ) as ExitFailure;
      expect((exit.cause as CauseFailure).error, '10000');
      expect(calls, 10000);
    },
  );

  test('adapt and recover remain stack safe with event yields', () async {
    var task = Task<int, int, int>((_) => const Exit.failure(Cause.failure(0)));
    for (var i = 0; i < 10000; i++) {
      task = task.recover(
        (e) => Task((_) => Exit.failure(Cause.failure(e + 1))),
      );
      task = task.adapt<int, int>(dependencies: (n) => n, error: (e) => e);
    }
    final exit = await task.run(7, yieldEvery: 256) as ExitFailure;
    expect((exit.cause as CauseFailure).error, 10000);
  });

  test(
    'dependency projections and child lifetimes preserve the policy',
    () async {
      final scope = Scope();
      final context = Context(scope: scope, dependencies: 1, yieldEvery: 17);
      expect(context.withDependencies('one').yieldEvery, 17);
      expect(context.withScope(Scope()).yieldEvery, 17);
      final child = Task<int, Never, int>(
        (c) => Exit.success(c.yieldEvery ?? -1),
      );
      final wrappers = <Task<Object?, Never, int>>[
        Semaphore(1).withPermit(child),
        all([child], concurrency: 1).map((values) => values.single),
        allSettled(
          [child],
          concurrency: 1,
        ).map((values) => (values.single as ExitSuccess<int, Never>).value),
        race([child]),
        firstSuccess([child]),
        taskGroup<int, Never, int>((group) => group.fork(child).join()),
        child.retry(delays: const []),
        child.repeat(delays: const []),
        child.timeout(const Duration(days: 1)),
      ];
      for (final wrapper in wrappers) {
        expect((await wrapper.runIn(context) as ExitSuccess).value, 17);
      }
      await scope.close();
      final exits = await Stream.value(1)
          .mapConcurrent(
            (_) => child,
            dependencies: 1,
            concurrency: 1,
            yieldEvery: 17,
          )
          .toList();
      expect((exits.single as ExitSuccess).value, 17);
    },
  );

  test('validates budgets before invoking computations', () async {
    final scope = Scope();
    expect(
      () => Context(scope: scope, dependencies: (), yieldEvery: 0),
      throwsRangeError,
    );
    final task = Task<int, Never, ()>((_) => fail('Skipped.'));
    await expectLater(task.run((), yieldEvery: -1), throwsRangeError);
    await scope.close();
  });

  test('scope closure during a yield skips pending computations', () async {
    final scope = Scope();
    var task = Task<int, Never, ()>((_) => fail('Closed scope.'));
    for (var i = 0; i < 1000; i++) {
      task = task.map((n) => n);
    }
    Timer.run(() => unawaited(scope.close()));
    final exit = await task.runIn(
      Context(scope: scope, dependencies: (), yieldEvery: 128),
    ) as ExitFailure;
    expect((exit.cause as CauseDefect).error, isStateError);
  });
}
