import 'dart:async';

import 'package:any/any.dart';
import 'package:any/concurrency.dart';
import 'package:test/test.dart';

void main() {
  test(
    'validates capacity and performs lazy repeatable puts and takes',
    () async {
      expect(() => AsyncQueue<int>(capacity: 0), throwsRangeError);
      final queue = AsyncQueue<int>(capacity: 2);
      final put = queue.put<()>(1);
      expect(queue.length, 0);
      await put.run(());
      await put.run(());
      expect(queue.length, 2);
      final take = queue.take<()>();
      expect((await take.run(()) as ExitSuccess).value, 1);
      expect((await take.run(()) as ExitSuccess).value, 1);
      expect(queue.length, 0);
      queue.close();
    },
  );

  test('applies backpressure and serves producers in FIFO order', () async {
    final queue = AsyncQueue<int>(capacity: 1);
    await queue.put<()>(0).run(());
    var completed = 0;
    final producers = [
      for (var i = 1; i <= 3; i++)
        queue.put<()>(i).run(()).then((e) {
          completed++;
          return e;
        }),
    ];
    await Future<void>.delayed(Duration.zero);
    expect(completed, 0);
    expect(queue.length, 1);
    final values = <int>[];
    for (var i = 0; i < 4; i++) {
      values.add(
        (await queue.take<()>().run(()) as ExitSuccess<int, QueueClosed>).value,
      );
      expect(queue.length, lessThanOrEqualTo(1));
    }
    await Future.wait(producers);
    expect(values, [0, 1, 2, 3]);
    expect(completed, 3);
    queue.close();
  });

  test('consumers are FIFO and nullable values are ordinary data', () async {
    final queue = AsyncQueue<int?>(capacity: 1);
    final consumers = [for (var i = 0; i < 3; i++) queue.take<()>().run(())];
    await Future<void>.delayed(Duration.zero);
    for (final value in <int?>[1, null, 3]) {
      await queue.put<()>(value).run(());
    }
    final exits = await Future.wait(consumers);
    expect(exits.cast<ExitSuccess>().map((e) => e.value), [1, null, 3]);
    queue.close();
  });

  test('cancelling waiting producers drops their values', () async {
    final queue = AsyncQueue<int>(capacity: 1);
    final controller = CancellationController();
    await queue.put<()>(1).run(());
    final cancelled = queue.put<()>(2).run((), cancellation: controller.token);
    final next = queue.put<()>(3).run(());
    await Future<void>.delayed(Duration.zero);
    controller.cancel(reason: 'skip');
    expect(
      ((await cancelled as ExitFailure).cause as CauseCancelled).reason,
      'skip',
    );
    expect((await queue.take<()>().run(()) as ExitSuccess).value, 1);
    expect(await next, isA<ExitSuccess>());
    expect((await queue.take<()>().run(()) as ExitSuccess).value, 3);
    queue.close();
  });

  test('cancelling waiting consumers does not steal later values', () async {
    final queue = AsyncQueue<int>(capacity: 1);
    final controller = CancellationController();
    final cancelled = queue.take<()>().run((), cancellation: controller.token);
    final next = queue.take<()>().run(());
    await Future<void>.delayed(Duration.zero);
    controller.cancel();
    expect((await cancelled as ExitFailure).cause, isA<CauseCancelled>());
    await queue.put<()>(4).run(());
    expect((await next as ExitSuccess).value, 4);
    queue.close();
  });

  test(
    'close rejects pending puts, drains buffered values, and is idempotent',
    () async {
      final queue = AsyncQueue<int>(capacity: 1);
      await queue.put<()>(1).run(());
      final pending = queue.put<()>(2).run(());
      await Future<void>.delayed(Duration.zero);
      queue.close();
      queue.close();
      expect(queue.isClosed, isTrue);
      expect(
        (await pending as ExitFailure).cause,
        isA<CauseFailure<QueueClosed>>(),
      );
      expect((await queue.take<()>().run(()) as ExitSuccess).value, 1);
      expect(
        (await queue.take<()>().run(()) as ExitFailure).cause,
        isA<CauseFailure<QueueClosed>>(),
      );
      expect(
        (await queue.put<()>(3).run(()) as ExitFailure).cause,
        isA<CauseFailure<QueueClosed>>(),
      );
    },
  );

  test('close wakes every waiting consumer', () async {
    final queue = AsyncQueue<int>(capacity: 2);
    final pending = [for (var i = 0; i < 5; i++) queue.take<()>().run(())];
    await Future<void>.delayed(Duration.zero);
    queue.close();
    final outcomes = await Future.wait(pending);
    expect(outcomes, everyElement(isA<ExitFailure<int, QueueClosed>>()));
  });

  test('pre-cancellation does not change the buffer', () async {
    final queue = AsyncQueue<int>(capacity: 1);
    final controller = CancellationController()..cancel();
    await queue.put<()>(1).run((), cancellation: controller.token);
    expect(queue.length, 0);
    await queue.put<()>(2).run(());
    await queue.take<()>().run((), cancellation: controller.token);
    expect(queue.length, 1);
    queue.close();
  });

  test(
    'ten thousand values flow through a bounded structured producer/consumer',
    () async {
      final queue = AsyncQueue<int>(capacity: 8);
      final task = taskGroup<int, QueueClosed, ()>((group) async {
        group.context.scope.addFinalizer((_) => queue.close());
        final producer = group.fork(
          Task<void, QueueClosed, ()>((context) async {
            for (var i = 0; i < 10000; i++) {
              final exit = await queue.put<()>(i).runIn(context);
              if (exit is ExitFailure<void, QueueClosed>) return exit;
            }
            queue.close();
            return const Exit.success(null);
          }),
        );
        var sum = 0;
        while (true) {
          final exit = await queue.take<()>().runIn(group.context);
          if (exit case ExitSuccess(:final value)) {
            sum += value;
          } else {
            break;
          }
        }
        expect(await producer.join(), isA<ExitSuccess>());
        return Exit.success(sum);
      });
      expect((await task.run(()) as ExitSuccess).value, 49995000);
      expect(queue.length, 0);
    },
  );
}
