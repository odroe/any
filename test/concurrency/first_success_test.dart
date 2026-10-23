import 'dart:async';

import 'package:any/any.dart';
import 'package:any/concurrency.dart';
import 'package:test/test.dart';

void main() {
  test(
    'finished candidates do not receive later winner cancellation',
    () async {
      CancellationToken? finishedToken;
      var cleaned = false;
      final task = firstSuccess([
        Task<int, String, ()>((context) {
          finishedToken = context.cancellation;
          context.cancellation.onCancel((_) => fail('Already finished.'));
          context.scope.addFinalizer((_) => cleaned = true);
          return const Exit.failure(Cause.failure('earlier'));
        }),
        Task<int, String, ()>((_) async {
          await Future<void>.delayed(Duration.zero);
          expect(cleaned, isTrue);
          return const Exit.success(1);
        }),
      ]);
      expect((await task.run(()) as ExitSuccess).value, 1);
      expect(finishedToken!.isCancelled, isFalse);
    },
  );

  test(
    'expected errors, defects, and child cancellations allow later success',
    () async {
      final task = firstSuccess<int, String, ()>([
        Task((_) => const Exit.failure(Cause.failure('missing'))),
        Task((_) => throw StateError('defect')),
        Task((_) => const Exit.failure(Cause.cancelled(reason: 'local'))),
        Task((_) => const Exit.success(42)),
      ]);
      expect((await task.run(()) as ExitSuccess).value, 42);
    },
  );

  test('first successful body wins and only losers are cancelled', () async {
    final ready = Completer<void>();
    final finish = Completer<void>();
    CancellationToken? winnerToken;
    final cleaned = <String>[];
    final task = firstSuccess([
      Task<String, Never, ()>((context) async {
        context.cancellation.onCancel((_) => finish.complete());
        context.scope.addFinalizer((_) => cleaned.add('loser'));
        ready.complete();
        await finish.future;
        return const Exit.success('late');
      }),
      Task<String, Never, ()>((context) async {
        winnerToken = context.cancellation;
        context.scope.addFinalizer((_) => cleaned.add('winner'));
        await ready.future;
        return const Exit.success('winner');
      }),
    ]);
    expect((await task.run(()) as ExitSuccess).value, 'winner');
    expect(winnerToken!.isCancelled, isFalse);
    expect(cleaned, containsAll(['winner', 'loser']));
  });

  test(
    'all failed causes retain input order, duplicates, and cleanup traces',
    () async {
      final error = StateError('cleanup');
      final trace = StackTrace.current;
      final task = firstSuccess([
        Task<int, String, ()>((c) async {
          c.scope.addFinalizer((_) => Error.throwWithStackTrace(error, trace));
          await Future<void>.delayed(Duration.zero);
          return const Exit.failure(Cause.failure('first'));
        }),
        Task<int, String, ()>(
          (_) => const Exit.failure(Cause.failure('second')),
        ),
      ]);
      final causes =
          ((await task.run(()) as ExitFailure).cause as CauseGroup).causes;
      expect((causes[0] as CauseFailure).error, 'first');
      expect((causes[1] as CauseDefect).error, same(error));
      expect((causes[1] as CauseDefect).stackTrace, same(trace));
      expect((causes[2] as CauseFailure).error, 'second');
    },
  );

  test('success does not hide a failed candidate cleanup', () async {
    final defect = StateError('close');
    final task = firstSuccess([
      Task<int, String, ()>((c) {
        c.scope.addFinalizer((_) => throw defect);
        return const Exit.failure(Cause.failure('ignored body'));
      }),
      Task<int, String, ()>((_) => const Exit.success(1)),
    ]);
    expect(
      ((await task.run(()) as ExitFailure).cause as CauseDefect).error,
      same(defect),
    );
  });

  test(
    'parent cancellation reaches all candidates and retains cleanup',
    () async {
      final parent = CancellationController();
      final ready = Completer<void>();
      final defect = StateError('cleanup');
      final task = firstSuccess([
        Task<int, Never, ()>((context) async {
          final stop = Completer<void>();
          context.cancellation.onCancel((reason) {
            expect(reason, 'parent');
            stop.complete();
          });
          context.scope.addFinalizer((_) => throw defect);
          ready.complete();
          await stop.future;
          return const Exit.success(1);
        }),
      ]);
      final future = task.run((), cancellation: parent.token);
      await ready.future;
      parent.cancel(reason: 'parent');
      final causes = ((await future as ExitFailure).cause as CauseGroup).causes;
      expect((causes.first as CauseCancelled).reason, 'parent');
      expect((causes.last as CauseDefect).error, same(defect));
    },
  );

  test(
    'empty and invalid input become defects and pre-cancellation skips input',
    () async {
      expect(
        ((await firstSuccess<int, Never, ()>([]).run(()) as ExitFailure).cause
                as CauseDefect)
            .error,
        isArgumentError,
      );
      final error = StateError('iterator');
      Iterable<Task<int, Never, ()>> tasks() sync* {
        yield Task((_) => fail('Must be cancelled before execution.'));
        throw error;
      }

      expect(
        ((await firstSuccess(tasks()).run(()) as ExitFailure).cause
                as CauseDefect)
            .error,
        same(error),
      );
      final controller = CancellationController()..cancel();
      expect(
        (await firstSuccess(
          tasks(),
        ).run((), cancellation: controller.token) as ExitFailure).cause,
        isA<CauseCancelled>(),
      );
    },
  );

  test(
    'repeated runs retain nullable values and widened error types',
    () async {
      final task = firstSuccess<Object?, String, ()>([
        Task<int?, Never, ()>((_) => const Exit.success(null)),
      ]);
      for (var i = 0; i < 2; i++) {
        expect((await task.run(()) as ExitSuccess).value, isNull);
      }
    },
  );
}
