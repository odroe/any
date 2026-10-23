import 'dart:async';

import 'package:any/any.dart';
import 'package:any/concurrency.dart';
import 'package:test/test.dart';

void main() {
  test(
    'fork starts once and repeated joins share the completed outcome',
    () async {
      var runs = 0;
      final task = taskGroup<int, String, int>((group) async {
        final handle = group.fork(
          Task<int, String, int>((context) {
            runs++;
            context.scope.addFinalizer((_) => runs++);
            return Exit.success(context.dependencies);
          }),
        );
        expect(handle.isCompleted, isFalse);
        final first = handle.join();
        expect(handle.join(), same(first));
        final exit = await first;
        expect(handle.isCompleted, isTrue);
        handle.cancel(reason: 'too late');
        expect(await handle.join(), same(exit));
        expect(runs, 2);
        return exit;
      });
      expect(runs, 0);
      expect((await task.run(42) as ExitSuccess).value, 42);
    },
  );

  test(
    'cancels every child before waiting and releases parent resources last',
    () async {
      final ready = Completer<void>();
      final cancellations = List.generate(2, (_) => Completer<void>());
      final events = <String>[];
      var started = 0;
      final task = taskGroup<int, Never, ()>((group) async {
        group.context.scope.addFinalizer((_) => events.add('parent close'));
        for (var i = 0; i < 2; i++) {
          group.fork(
            Task<void, Never, ()>((context) async {
              context.cancellation.onCancel((_) {
                events.add('cancel $i');
                cancellations[i].complete();
              });
              context.scope.addFinalizer((_) async {
                // Sequential cancel-then-join would deadlock here.
                await cancellations[1 - i].future;
                events.add('close $i');
              });
              if (++started == 2) ready.complete();
              await cancellations[i].future;
              return const Exit.success(null);
            }),
          );
        }
        await ready.future;
        return const Exit.success(1);
      });
      expect(await task.run(()), isA<ExitSuccess>());
      expect(events.take(2), ['cancel 0', 'cancel 1']);
      expect(events.last, 'parent close');
      expect(events, containsAll(['close 0', 'close 1']));
    },
  );

  test(
    'unjoined failures aggregate in fork order before body cleanup',
    () async {
      final ready = Completer<void>();
      var completed = 0;
      final errors = [
        StateError('child 0'),
        StateError('child 1'),
        StateError('body'),
      ];
      final task = taskGroup<int, String, ()>((group) async {
        group.context.scope.addFinalizer((exit) {
          expect(exit, isA<ExitFailure>());
          throw errors[2];
        });
        for (var i = 0; i < 2; i++) {
          group.fork(
            Task<void, String, ()>((context) {
              context.scope.addFinalizer((_) {
                if (++completed == 2) ready.complete();
                throw errors[i];
              });
              return Exit.failure(Cause.failure('child $i'));
            }),
          );
        }
        await ready.future;
        return const Exit.failure(Cause.failure('body'));
      });
      final exit = await task.run(()) as ExitFailure;
      final causes = (exit.cause as CauseGroup).causes;
      expect(causes, hasLength(6));
      expect(causes.whereType<CauseFailure>().map((c) => c.error), [
        'body',
        'child 0',
        'child 1',
      ]);
      expect(causes.whereType<CauseDefect>().map((c) => c.error), errors);
    },
  );

  test(
    'joined failures are handled by the caller without duplication',
    () async {
      const failure = Cause<String>.failure('child');
      final child = Task<int, String, ()>((_) => const Exit.failure(failure));
      final propagate = taskGroup<int, String, ()>((g) => g.fork(child).join());
      expect((await propagate.run(()) as ExitFailure).cause, same(failure));
      final recover = taskGroup<int, String, ()>((g) async {
        expect(await g.fork(child).join(), isA<ExitFailure>());
        return const Exit.success(7);
      });
      expect((await recover.run(()) as ExitSuccess).value, 7);
    },
  );

  test('individual cancellation leaves siblings and parent running', () async {
    final task = taskGroup<int, Never, ()>((group) async {
      final cancelled = Completer<void>();
      final ready = Completer<void>();
      final first = group.fork(
        Task<int, Never, ()>((context) async {
          context.cancellation.onCancel((_) => cancelled.complete());
          ready.complete();
          await cancelled.future;
          return const Exit.success(0);
        }),
      );
      await ready.future;
      first.cancel(reason: 'only first');
      first.cancel(reason: 'ignored');
      final exit = await first.join() as ExitFailure;
      expect((exit.cause as CauseCancelled).reason, 'only first');
      expect(group.context.cancellation.isCancelled, isFalse);
      return await group
          .fork(
            Task<int, Never, ()>((context) {
              expect(context.cancellation.isCancelled, isFalse);
              return const Exit.success(2);
            }),
          )
          .join();
    });
    expect((await task.run(()) as ExitSuccess).value, 2);
  });

  test(
    'parent cancellation forwards its reason and waits for noncooperative work',
    () async {
      final controller = CancellationController();
      final ready = Completer<void>();
      final finish = Completer<void>();
      final reason = Object();
      var done = false;
      final task = taskGroup<int, Never, ()>((group) async {
        final child = group.fork(
          Task<int, Never, ()>((context) async {
            context.cancellation.onCancel(
              (value) => expect(value, same(reason)),
            );
            ready.complete();
            await finish.future;
            return const Exit.success(1);
          }),
        );
        return child.join();
      });
      final future = task.run((), cancellation: controller.token).then((exit) {
        done = true;
        return exit;
      });
      await ready.future;
      controller.cancel(reason: reason);
      expect(done, isFalse);
      finish.complete();
      expect(
        ((await future as ExitFailure).cause as CauseCancelled).reason,
        same(reason),
      );
    },
  );

  test(
    'body throws still drain children and retain original stack traces',
    () async {
      final defect = StateError('body');
      final trace = StackTrace.current;
      final ready = Completer<void>();
      final finish = Completer<void>();
      var closed = false;
      final task = taskGroup<int, Never, ()>((group) async {
        group.fork(
          Task<void, Never, ()>((context) async {
            context.cancellation.onCancel((_) => finish.complete());
            context.scope.addFinalizer((_) => closed = true);
            ready.complete();
            await finish.future;
            return const Exit.success(null);
          }),
        );
        await ready.future;
        Error.throwWithStackTrace(defect, trace);
      });
      final cause = (await task.run(()) as ExitFailure).cause as CauseDefect;
      expect(cause.error, same(defect));
      expect(cause.stackTrace, same(trace));
      expect(closed, isTrue);
    },
  );

  test('cleanup defects survive automatic cancellation', () async {
    final ready = Completer<void>();
    final finish = Completer<void>();
    final defect = StateError('cleanup');
    final task = taskGroup<int, Never, ()>((group) async {
      group.fork(
        Task<void, Never, ()>((context) async {
          context.cancellation.onCancel((_) => finish.complete());
          context.scope.addFinalizer((_) => throw defect);
          ready.complete();
          await finish.future;
          return const Exit.success(null);
        }),
      );
      await ready.future;
      return const Exit.success(1);
    });
    expect(
      ((await task.run(()) as ExitFailure).cause as CauseDefect).error,
      same(defect),
    );
  });

  test(
    'nested groups close before ancestors and keep borrowed scopes open',
    () async {
      final events = <String>[];
      final scope = Scope();
      final task = taskGroup<int, Never, ()>((outer) async {
        outer.context.scope.addFinalizer((_) => events.add('outer'));
        return outer
            .fork(
              taskGroup<int, Never, ()>((inner) async {
                inner.context.scope.addFinalizer((_) => events.add('inner'));
                return inner
                    .fork(
                      Task<int, Never, ()>((c) {
                        c.scope.addFinalizer((_) => events.add('leaf'));
                        return const Exit.success(3);
                      }),
                    )
                    .join();
              }),
            )
            .join();
      });
      expect(
        await task.runIn(Context(scope: scope, dependencies: ())),
        isA<ExitSuccess>(),
      );
      expect(events, ['leaf', 'inner', 'outer']);
      expect(scope.isClosed, isFalse);
      await scope.close();
    },
  );

  test(
    'closed groups reject new children and each run has fresh state',
    () async {
      final groups = <TaskGroup<Never, ()>>[];
      final task = taskGroup<int, Never, ()>((group) {
        groups.add(group);
        return const Exit.success(1);
      });
      await task.run(());
      await task.run(());
      expect(groups.toSet(), hasLength(2));
      expect(
        () =>
            groups[0].fork(Task<int, Never, ()>((_) => const Exit.success(1))),
        throwsStateError,
      );
    },
  );

  test('pre-cancelled groups skip body and children', () async {
    final controller = CancellationController()..cancel();
    final task = taskGroup<int, Never, ()>((_) => fail('Not started.'));
    expect(
      (await task.run((), cancellation: controller.token) as ExitFailure).cause,
      isA<CauseCancelled>(),
    );
  });

  test('ten thousand forks can be joined without stack growth', () async {
    final child = Task<int, Never, ()>((_) => const Exit.success(1));
    final task = taskGroup<int, Never, ()>((group) async {
      final children = [for (var i = 0; i < 10000; i++) group.fork(child)];
      var total = 0;
      for (final handle in children) {
        total += (await handle.join() as ExitSuccess<int, Never>).value;
      }
      return Exit.success(total);
    });
    expect((await task.run(()) as ExitSuccess).value, 10000);
  });
}
