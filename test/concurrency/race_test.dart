import 'dart:async';

import 'package:any/any.dart';
import 'package:any/concurrency.dart';
import 'package:test/test.dart';

void main() {
  test('selecting a winner cancels only the losing tokens', () async {
    final loserReady = Completer<void>();
    final release = Completer<void>();
    CancellationToken? winningToken;
    CancellationToken? losingToken;
    final tasks = [
      Task<int, Never, ()>((context) async {
        losingToken = context.cancellation;
        context.cancellation.onCancel((_) => release.complete());
        loserReady.complete();
        await release.future;
        return const Exit.success(0);
      }),
      Task<int, Never, ()>((context) async {
        winningToken = context.cancellation;
        await loserReady.future;
        return const Exit.success(1);
      }),
    ];
    expect((await race(tasks).run(()) as ExitSuccess).value, 1);
    expect(losingToken!.isCancelled, isTrue);
    expect(winningToken!.isCancelled, isFalse);
  });

  test(
    'first body wins, cancels losers, and waits for their cleanup',
    () async {
      final started = Completer<void>();
      final finish = Completer<void>();
      final cancelled = Completer<void>();
      final cleaned = <String>[];
      final loser = Task<String, String, ()>((context) async {
        context.cancellation.onCancel((_) => cancelled.complete());
        context.scope.addFinalizer((_) => cleaned.add('loser'));
        started.complete();
        await finish.future;
        return const Exit.failure(Cause.failure('late failure'));
      });
      final winner = Task<String, String, ()>((context) async {
        await started.future;
        context.scope.addFinalizer((exit) {
          expect(exit, isA<ExitSuccess>());
          cleaned.add('winner');
        });
        return const Exit.success('winner');
      });
      var done = false;
      final future = race([loser, winner]).run(()).then((exit) {
        done = true;
        return exit;
      });
      await cancelled.future;
      expect(done, isFalse);
      finish.complete();
      expect(
        await future,
        isA<ExitSuccess<String, String>>().having(
          (exit) => exit.value,
          'value',
          'winner',
        ),
      );
      expect(cleaned, containsAll(['winner', 'loser']));
    },
  );

  test('failure can win and skips composition', () async {
    const failure = Cause<String>.failure('unavailable');
    final task = race([
      Task<int, String, ()>((_) => const Exit.failure(failure)),
      Task<int, String, ()>((_) => const Exit.success(2)),
    ]).map((_) => fail('Must not map a failed race.'));
    final exit = await task.run(()) as ExitFailure;
    expect(exit.cause, same(failure));
  });

  test('winner failure precedes all cleanup failures in input order', () async {
    final release = Completer<void>();
    final failures = [StateError('zero'), StateError('one')];
    final trace = StackTrace.current;
    final tasks = List.generate(
      2,
      (index) => Task<int, String, ()>((context) async {
        context.scope.addFinalizer(
          (_) => Error.throwWithStackTrace(failures[index], trace),
        );
        if (index == 0) {
          context.cancellation.onCancel((_) => release.complete());
          await release.future;
          return const Exit.success(0);
        }
        return const Exit.failure(Cause.failure('winner'));
      }),
    );
    final exit = await race(tasks).run(()) as ExitFailure;
    final causes = (exit.cause as CauseGroup).causes;
    expect((causes[0] as CauseFailure).error, 'winner');
    expect(causes.skip(1).map((c) => (c as CauseDefect).error), failures);
    expect(
      causes.skip(1).map((c) => (c as CauseDefect).stackTrace),
      everyElement(same(trace)),
    );
  });

  test('winning success still reports losing cleanup failures', () async {
    final defect = StateError('close');
    final release = Completer<void>();
    final exit = await race([
      Task<int, Never, ()>((context) async {
        context.cancellation.onCancel((_) => release.complete());
        context.scope.addFinalizer((_) => throw defect);
        await release.future;
        return const Exit.success(2);
      }),
      Task<int, Never, ()>((_) => const Exit.success(1)),
    ]).run(()) as ExitFailure;
    expect((exit.cause as CauseDefect).error, same(defect));
  });

  test('child scopes are isolated and parent scope stays open', () async {
    final scope = Scope();
    final dependencies = Object();
    final scopes = <Scope>[];
    final started = Completer<void>();
    final context = Context(scope: scope, dependencies: dependencies);
    final exit = await race([
      for (var i = 0; i < 2; i++)
        Task<int, Never, Object>((child) async {
          expect(child.dependencies, same(dependencies));
          scopes.add(child.scope);
          if (scopes.length == 2) started.complete();
          await started.future;
          return Exit.success(i);
        }),
    ]).runIn(context);
    expect(exit, isA<ExitSuccess>());
    expect(scopes.toSet(), hasLength(2));
    expect(scopes.every((s) => s.isClosed), isTrue);
    expect(scope.isClosed, isFalse);
    await scope.close();
  });

  test('parent cancellation forwards reason and drains running work', () async {
    final controller = CancellationController();
    final started = Completer<void>();
    final release = Completer<void>();
    final reason = Object();
    final task = Task<int, Never, ()>((context) async {
      context.cancellation.onCancel((value) {
        expect(value, same(reason));
        release.complete();
      });
      started.complete();
      await release.future;
      return const Exit.success(1);
    });
    final future = race([task]).run((), cancellation: controller.token);
    await started.future;
    controller.cancel(reason: reason);
    final exit = await future as ExitFailure;
    expect((exit.cause as CauseCancelled).reason, same(reason));
  });

  test(
    'empty input becomes a defect; pre-cancelled input is untouched',
    () async {
      final exit = await race<int, Never, ()>([]).run(()) as ExitFailure;
      expect((exit.cause as CauseDefect).error, isArgumentError);
      final controller = CancellationController()..cancel();
      Iterable<Task<int, Never, ()>> input() sync* {
        fail('Must not iterate.');
      }

      final cancelled = await race(input())
          .run((), cancellation: controller.token);
      expect((cancelled as ExitFailure).cause, isA<CauseCancelled>());
    },
  );

  test(
    'iteration failure cancels queued work and keeps its stack trace',
    () async {
      final error = StateError('iterate');
      final trace = StackTrace.current;
      Iterable<Task<int, Never, ()>> input() sync* {
        yield Task((_) => fail('Queued body must not start.'));
        Error.throwWithStackTrace(error, trace);
      }

      final exit = await race(input()).run(()) as ExitFailure;
      final cause = exit.cause as CauseDefect;
      expect(cause.error, same(error));
      expect(cause.stackTrace, same(trace));
    },
  );

  test(
    'input and state are fresh on each run and nullable values work',
    () async {
      var iterations = 0;
      Iterable<Task<String?, Never, ()>> input() sync* {
        iterations++;
        yield Task((_) => const Exit.success(null));
      }

      final task = race(input());
      expect(iterations, 0);
      final exits = await Future.wait([task.run(()), task.run(())]);
      expect(iterations, 2);
      expect(exits.cast<ExitSuccess>().map((e) => e.value), [null, null]);
    },
  );
}
