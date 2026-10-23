import 'dart:async';

import 'package:any/any.dart';
import 'package:any/concurrency.dart';
import 'package:test/test.dart';

void main() {
  test('continues after body and cleanup failures, preserving order', () async {
    final cleanup = StateError('close');
    final outcomes =
        (await allSettled([
              Task<int, String, ()>(
                (_) => const Exit.failure(Cause.failure('first')),
              ),
              Task<int, String, ()>((context) {
                context.scope.addFinalizer((_) => throw cleanup);
                return const Exit.success(2);
              }),
              Task<int, String, ()>((_) => const Exit.success(3)),
            ], concurrency: 1).run(()) as ExitSuccess).value
            as List<Exit<int, String>>;
    expect(((outcomes[0] as ExitFailure).cause as CauseFailure).error, 'first');
    expect(
      ((outcomes[1] as ExitFailure).cause as CauseDefect).error,
      same(cleanup),
    );
    expect((outcomes[2] as ExitSuccess).value, 3);
  });

  test(
    'bounds lazy dispatch through cleanup and preserves input order',
    () async {
      final gates = List.generate(3, (_) => Completer<void>());
      final ready = Completer<void>();
      var pulled = 0;
      var active = 0;
      var peak = 0;
      Iterable<Task<int, Never, ()>> tasks() sync* {
        for (var i = 0; i < 3; i++) {
          pulled++;
          yield Task((context) {
            active++;
            if (active > peak) peak = active;
            if (active == 2) ready.complete();
            context.scope.addFinalizer((_) async {
              await gates[i].future;
              active--;
            });
            return Exit.success(i);
          });
        }
      }

      final task = allSettled(tasks(), concurrency: 2);
      expect(pulled, 0);
      final future = task.run(());
      await ready.future;
      expect(pulled, 2);
      gates[1].complete();
      gates[2].complete();
      gates[0].complete();
      final exit = await future as ExitSuccess;
      expect((exit.value as List).cast<ExitSuccess>().map((e) => e.value), [
        0,
        1,
        2,
      ]);
      expect(active, 0);
      expect(peak, 2);
    },
  );

  test(
    'parent cancellation stops input and preserves cleanup failures',
    () async {
      final controller = CancellationController();
      final ready = Completer<void>();
      final release = Completer<void>();
      final defect = StateError('cleanup');
      var pulled = 0;
      var done = false;
      Iterable<Task<int, String, ()>> tasks() sync* {
        pulled++;
        yield Task((context) async {
          context.scope.addFinalizer((_) => throw defect);
          ready.complete();
          await release.future;
          return const Exit.failure(Cause.failure('late'));
        });
        pulled++;
        yield Task((_) => const Exit.success(2));
      }

      final future = allSettled(tasks(), concurrency: 1)
          .run((), cancellation: controller.token)
          .then((exit) {
            done = true;
            return exit;
          });
      await ready.future;
      controller.cancel(reason: 'stop');
      expect(done, isFalse);
      release.complete();
      final exit = await future as ExitFailure;
      final causes = (exit.cause as CauseGroup).causes;
      expect((causes[0] as CauseFailure).error, 'late');
      expect((causes[1] as CauseDefect).error, same(defect));
      expect((causes[2] as CauseCancelled).reason, 'stop');
      expect(pulled, 1);
    },
  );

  test(
    'iteration defects abort the batch without internal cancellation noise',
    () async {
      final error = StateError('iterator');
      final trace = StackTrace.current;
      Iterable<Task<int, Never, ()>> tasks() sync* {
        yield Task((_) => fail('Cancelled before start.'));
        Error.throwWithStackTrace(error, trace);
      }

      final exit =
          await allSettled(tasks(), concurrency: 2).run(()) as ExitFailure;
      final cause = exit.cause as CauseDefect;
      expect(cause.error, same(error));
      expect(cause.stackTrace, same(trace));
    },
  );

  test(
    'empty, repeated, nullable, and widened inputs retain their types',
    () async {
      expect(
        (await allSettled<int, Never, ()>(
          [],
          concurrency: 1000000,
        ).run(()) as ExitSuccess).value,
        isEmpty,
      );
      final Task<int?, Never, ()> narrow = Task(
        (_) => const Exit.success(null),
      );
      final Task<List<Exit<Object?, String>>, String, ()> task =
          allSettled<Object?, String, ()>([narrow], concurrency: 2);
      for (var i = 0; i < 2; i++) {
        final exit = await task.run(()) as ExitSuccess;
        expect((exit.value.single as ExitSuccess).value, isNull);
      }
    },
  );

  test('validates limit and pre-cancellation skips input', () async {
    expect(
      () => allSettled<int, Never, ()>([], concurrency: 0),
      throwsRangeError,
    );
    Iterable<Task<int, Never, ()>> tasks() sync* {
      fail('No iteration.');
    }

    final controller = CancellationController()..cancel();
    final exit = await allSettled(
      tasks(),
      concurrency: 1,
    ).run((), cancellation: controller.token) as ExitFailure;
    expect(exit.cause, isA<CauseCancelled>());
  });

  test(
    'ten thousand failures are collected without stopping dispatch',
    () async {
      final task = Task<int, String, ()>(
        (_) => const Exit.failure(Cause.failure('expected')),
      );
      final exit = await allSettled(
        List.filled(10000, task),
        concurrency: 8,
      ).run(()) as ExitSuccess;
      expect(exit.value, hasLength(10000));
      expect(exit.value, everyElement(isA<ExitFailure>()));
    },
  );
}
