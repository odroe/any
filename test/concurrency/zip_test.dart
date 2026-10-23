import 'dart:async';

import 'package:any/any.dart';
import 'package:any/concurrency.dart';
import 'package:test/test.dart';

void main() {
  test(
    'runs concurrently and retains record types and argument order',
    () async {
      final firstStarted = Completer<void>();
      final secondFinished = Completer<void>();
      final task = zip(
        Task<int, Never, ()>((_) async {
          firstStarted.complete();
          await secondFinished.future;
          return const Exit.success(42);
        }),
        Task<String, Never, ()>((_) async {
          await firstStarted.future;
          secondFinished.complete();
          return const Exit.success('Ada');
        }),
      );
      final Exit<(int, String), Never> exit = await task.run(());
      expect((exit as ExitSuccess).value, (42, 'Ada'));
    },
  );

  test(
    'aligns expected errors and dependencies without widening values',
    () async {
      final Task<(int, String), String, int> task = zip(
        Task<int, Never, int>((c) => Exit.success(c.dependencies)),
        Task<String, String, int>((c) => Exit.success('${c.dependencies}')),
      );
      expect((await task.run(7) as ExitSuccess).value, (7, '7'));
    },
  );

  test('failure cancels the sibling and waits for cleanup errors', () async {
    final ready = Completer<void>();
    final cancelled = Completer<void>();
    final release = Completer<void>();
    final error = StateError('cleanup');
    var done = false;
    final task = zip(
      Task<int, String, ()>((_) async {
        await ready.future;
        return const Exit.failure(Cause.failure('body'));
      }),
      Task<String, String, ()>((c) async {
        c.cancellation.onCancel((_) => cancelled.complete());
        c.scope.addFinalizer((_) async {
          await release.future;
          throw error;
        });
        ready.complete();
        await cancelled.future;
        return const Exit.success('ignored');
      }),
    );
    final future = task.run(()).then((exit) {
      done = true;
      return exit;
    });
    await cancelled.future;
    expect(done, isFalse);
    release.complete();
    final causes = ((await future as ExitFailure).cause as CauseGroup).causes;
    expect(causes, hasLength(2));
    expect((causes[0] as CauseFailure).error, 'body');
    expect((causes[1] as CauseDefect).error, same(error));
  });

  test('nullable values and Future values are not flattened', () async {
    final value = Completer<int>();
    final task = zip(
      Task<String?, Never, ()>((_) => const Exit.success(null)),
      Task<Future<int>, Never, ()>((_) => Exit.success(value.future)),
    );
    final exit = await task.run(()) as ExitSuccess;
    value.complete(1);
    final (String? label, Future<int> number) = exit.value;
    expect(label, isNull);
    expect(number, same(value.future));
    expect(await number, 1);
  });

  test('pre-cancellation skips both children', () async {
    final controller = CancellationController()..cancel(reason: 'stop');
    final task = zip(
      Task<int, Never, ()>((_) => fail('Skipped')),
      Task<String, Never, ()>((_) => fail('Skipped')),
    );
    final exit =
        await task.run((), cancellation: controller.token) as ExitFailure;
    expect((exit.cause as CauseCancelled).reason, 'stop');
  });
}
