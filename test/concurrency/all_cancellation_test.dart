import 'dart:async';

import 'package:any/any.dart';
import 'package:any/concurrency.dart';
import 'package:any/resource.dart';
import 'package:test/test.dart';

void main() {
  group('all cancellation and failures', () {
    for (final beforeRun in [false, true]) {
      test(
        'skips input and children when cancelled before starting ($beforeRun)',
        () async {
          final controller = CancellationController();
          final reason = Object();
          var iterated = false;
          Iterable<Task<int, Never, ()>> tasks() sync* {
            iterated = true;
            yield Task((_) => fail('must not execute'));
          }

          final task = all(tasks(), concurrency: 2);
          if (beforeRun) controller.cancel(reason: reason);

          final running = task.run((), cancellation: controller.token);
          if (!beforeRun) controller.cancel(reason: reason);

          _expectCancelled(await running, reason);
          expect(iterated, isFalse);
        },
      );
    }

    final error = Object();
    final trace = StackTrace.fromString('original defect');
    final causes = <Cause<String?>>[
      const Cause.failure(null),
      Cause.defect(error, trace),
      const Cause.cancelled(),
      Cause.combine([
        const Cause.failure('expected'),
        Cause.defect(error, trace),
        const Cause.cancelled(reason: 'explicit'),
      ]),
    ];
    for (final cause in causes) {
      test(
        'preserves ${cause.runtimeType} and skips unstarted children',
        () async {
          var started = false;
          final task = all([
            Task<int, String?, ()>((_) => Exit.failure(cause)),
            Task<int, String?, ()>((_) {
              started = true;
              return const Exit.success(2);
            }),
          ], concurrency: 1);

          expect(
            (await task.run(()) as ExitFailure<List<int>, String?>).cause,
            same(cause),
          );
          expect(started, isFalse);
        },
      );
    }

    for (final failAfterRequest in [false, true]) {
      test(
        'forwards parent cancellation and waits for running bodies ($failAfterRequest)',
        () async {
          final controller = CancellationController();
          final reason = Object();
          final gates = List.generate(2, (_) => Completer<Exit<int, String>>());
          final started = List.generate(2, (_) => Completer<void>());
          final tokens = <CancellationToken>[];
          final cleanupExits = <Exit<void, Object?>>[];
          const original = Cause<String>.failure('after cancellation');
          var nextStarted = false;
          final tasks = [
            for (var i = 0; i < 2; i++)
              Task<int, String, ()>((context) {
                tokens.add(context.cancellation);
                context.scope.addFinalizer(cleanupExits.add);
                started[i].complete();
                return gates[i].future;
              }),
            Task<int, String, ()>((_) {
              nextStarted = true;
              return const Exit.success(3);
            }),
          ];
          var finished = false;
          final running = all(tasks, concurrency: 2)
              .run((), cancellation: controller.token)
              .then((exit) {
                finished = true;
                return exit;
              });
          await Future.wait(started.map((value) => value.future));

          controller.cancel(reason: reason);
          expect(tokens.every((token) => token.isCancelled), isTrue);
          expect(
            tokens.map((token) => token.reason),
            everyElement(same(reason)),
          );
          await Future<void>(() {});
          expect(finished, isFalse);
          expect(cleanupExits, isEmpty);
          gates[1].complete(const Exit.success(1));
          await Future<void>(() {});
          expect(finished, isFalse);
          gates[0].complete(
            failAfterRequest
                ? const Exit.failure(original)
                : const Exit.success(0),
          );

          final exit = await running as ExitFailure<List<int>, String>;
          final failures = (exit.cause as CauseGroup<String>).causes;
          expect(failures, hasLength(2));
          if (failAfterRequest) {
            expect(failures.first, same(original));
          } else {
            expect(
              (failures.first as CauseCancelled<String>).reason,
              same(reason),
            );
          }
          expect(
            (failures.last as CauseCancelled<String>).reason,
            same(reason),
          );
          expect(cleanupExits, hasLength(2));
          expect(nextStarted, isFalse);
        },
      );
    }

    test(
      'cancels siblings as soon as a body fails, before its cleanup finishes',
      () async {
        final started = Completer<void>();
        final body = Completer<Exit<int, String>>();
        final cleaning = Completer<void>();
        final cleanup = Completer<void>();
        late CancellationToken token;
        const original = ExitFailure<int, String>(Cause.failure('failed'));
        var nextStarted = false;
        var finished = false;
        final tasks = [
          Task<int, String, ()>((context) {
            token = context.cancellation;
            started.complete();
            return body.future;
          }),
          Task<int, String, ()>((context) async {
            context.scope.addFinalizer((exit) {
              expect(exit, same(original));
              cleaning.complete();
              return cleanup.future;
            });
            await started.future;
            return original;
          }),
          Task<int, String, ()>((_) {
            nextStarted = true;
            return const Exit.success(3);
          }),
        ];
        final running = all(tasks, concurrency: 2).run(()).then((exit) {
          finished = true;
          return exit;
        });
        await cleaning.future;
        expect(token.isCancelled, isTrue);
        expect(finished, isFalse);
        expect(nextStarted, isFalse);

        body.complete(const Exit.success(1));
        await Future<void>(() {});
        expect(finished, isFalse);
        cleanup.complete();

        expect(
          (await running as ExitFailure<List<int>, String>).cause,
          same(original.cause),
        );
        expect(nextStarted, isFalse);
      },
    );

    test(
      'keeps nested cleanup defects when a cancelled acquisition succeeds late',
      () async {
        final acquiring = Completer<void>();
        final acquired = Completer<Exit<int, String>>();
        final cancelled = Completer<void>();
        final cleanupErrors = [Object(), Object()];
        final cleanupTraces = [
          StackTrace.fromString('first cleanup'),
          StackTrace.fromString('second cleanup'),
        ];
        const original = Cause<String>.failure('other child failed');
        var releases = 0;
        final first = acquireRelease<int, String, ()>(
          acquire: (context) {
            context.cancellation.onCancel((_) => cancelled.complete());
            acquiring.complete();
            return acquired.future;
          },
          release: (value, exit) {
            expect(value, 42);
            expect(
              (exit as ExitFailure<void, Object?>).cause,
              isA<CauseCancelled<Object?>>(),
            );
            releases++;
            final nested = Scope();
            for (var i = 0; i < 2; i++) {
              nested.addFinalizer(
                (_) => Error.throwWithStackTrace(
                  cleanupErrors[i],
                  cleanupTraces[i],
                ),
              );
            }
            return nested.close(exit: exit);
          },
        );
        final second = Task<int, String, ()>((_) async {
          await acquiring.future;
          return const Exit.failure(original);
        });
        final running = all([first, second], concurrency: 2).run(());
        await cancelled.future;
        expect(releases, 0);

        acquired.complete(const Exit.success(42));
        final exit = await running as ExitFailure<List<int>, String>;
        final failures = (exit.cause as CauseGroup<String>).causes;

        expect(releases, 1);
        expect(failures, hasLength(3));
        for (var i = 0; i < 2; i++) {
          final defect = failures[i] as CauseDefect<String>;
          expect(defect.error, same(cleanupErrors[1 - i]));
          expect(defect.stackTrace, same(cleanupTraces[1 - i]));
        }
        expect(failures.last, same(original));
      },
    );

    for (final outcome in ['expected', 'throw', 'cancelled']) {
      test(
        'retains a sibling $outcome after cancellation in input order',
        () async {
          final started = Completer<void>();
          final cancelled = Completer<void>();
          final pending = Completer<Exit<int, String>>();
          const independent = Cause<String>.failure('independent failure');
          const ownCancellation = Cause<String>.cancelled();
          const primary = Cause<String>.failure('trigger');
          const additional = Cause<String>.cancelled(reason: 'child reason');
          final original = Cause<String>.combine([primary, additional]);
          final first = Task<int, String, ()>((context) {
            context.cancellation.onCancel((_) => cancelled.complete());
            started.complete();
            return pending.future;
          });
          final second = Task<int, String, ()>((_) async {
            await started.future;
            return Exit.failure(original);
          });
          final running = all([first, second], concurrency: 2).run(());
          await cancelled.future;

          switch (outcome) {
            case 'expected':
              pending.complete(const Exit.failure(independent));
            case 'throw':
              pending.completeError(error, trace);
            case 'cancelled':
              pending.complete(const Exit.failure(ownCancellation));
          }
          final exit = await running as ExitFailure<List<int>, String>;
          final failures = (exit.cause as CauseGroup<String>).causes;

          expect(failures, hasLength(3));
          switch (outcome) {
            case 'expected':
              expect(failures.first, same(independent));
            case 'throw':
              final defect = failures.first as CauseDefect<String>;
              expect(defect.error, same(error));
              expect(defect.stackTrace, same(trace));
            case 'cancelled':
              expect(failures.first, same(ownCancellation));
          }
          expect(failures[1], same(primary));
          expect(failures[2], same(additional));
        },
      );
    }

    test('stops queued work when only cleanup fails', () async {
      var nextStarted = false;
      final first = Task<int, Never, ()>((context) {
        context.scope.addFinalizer((_) => Future<void>.error(error, trace));
        return const Exit.success(1);
      });
      final next = Task<int, Never, ()>((_) {
        nextStarted = true;
        return const Exit.success(2);
      });

      final exit = await all([first, next], concurrency: 1).run(());

      final defect =
          (exit as ExitFailure<List<int>, Never>).cause as CauseDefect<Never>;
      expect(defect.error, same(error));
      expect(defect.stackTrace, same(trace));
      expect(nextStarted, isFalse);
    });

    test(
      'keeps the child cleanup exit fixed after late parent cancellation',
      () async {
        final controller = CancellationController();
        final cleaning = Completer<void>();
        final cleanup = Completer<void>();
        const original = Exit<int, Never>.success(42);
        Exit<void, Object?>? cleanupExit;
        var finished = false;
        final child = Task<int, Never, ()>((context) {
          context.scope.addFinalizer((exit) {
            cleanupExit = exit;
            cleaning.complete();
            return cleanup.future;
          });
          return original;
        });
        final running = all([child], concurrency: 1)
            .run((), cancellation: controller.token)
            .then((exit) {
              finished = true;
              return exit;
            });
        await cleaning.future;

        controller.cancel(reason: 'during cleanup');
        await Future<void>(() {});
        expect(finished, isFalse);
        expect(cleanupExit, same(original));
        cleanup.complete();

        _expectCancelled(await running, 'during cleanup');
        expect(cleanupExit, same(original));
      },
    );

    test(
      'isolates concurrent runs and removes completed parent subscriptions',
      () async {
        final firstController = CancellationController();
        final secondController = CancellationController();
        final firstGate = Completer<Exit<int, Never>>();
        final secondGate = Completer<Exit<int, Never>>();
        final bothStarted = Completer<void>();
        final contexts = <Context<Future<Exit<int, Never>>>>[];
        final child = Task<int, Never, Future<Exit<int, Never>>>((context) {
          contexts.add(context);
          if (contexts.length == 2) bothStarted.complete();
          return context.dependencies;
        });
        final task = all([child], concurrency: 1);
        final first = task.run(
          firstGate.future,
          cancellation: firstController.token,
        );
        final second = task.run(
          secondGate.future,
          cancellation: secondController.token,
        );
        await bothStarted.future;

        firstController.cancel(reason: 'first');
        expect(contexts[0].cancellation.isCancelled, isTrue);
        expect(contexts[1].cancellation.isCancelled, isFalse);
        expect(contexts[0].scope, isNot(same(contexts[1].scope)));
        secondGate.complete(const Exit.success(2));
        expect((await second as ExitSuccess<List<int>, Never>).value, [2]);
        expect(contexts[0].scope.isClosed, isFalse);
        expect(contexts[1].scope.isClosed, isTrue);

        secondController.cancel(reason: 'after completion');
        expect(contexts[1].cancellation.isCancelled, isFalse);
        firstGate.complete(const Exit.success(1));
        _expectCancelled(await first, 'first');
        expect(contexts[0].scope.isClosed, isTrue);
      },
    );

    test('drains nested groups and retains their cleanup defects', () async {
      final started = Completer<void>();
      final cancelled = Completer<void>();
      final body = Completer<Exit<int, String>>();
      const primary = Cause<String>.failure('outer failure');
      final child = Task<int, String, ()>((context) {
        context.cancellation.onCancel((_) => cancelled.complete());
        context.scope.addFinalizer(
          (_) => Error.throwWithStackTrace(error, trace),
        );
        started.complete();
        return body.future;
      });
      final nested = all([child], concurrency: 1);
      final failure = Task<List<int>, String, ()>((_) async {
        await started.future;
        return const Exit.failure(primary);
      });
      final running = all([nested, failure], concurrency: 2).run(());
      await cancelled.future;

      body.complete(const Exit.success(1));
      final exit = await running as ExitFailure<List<List<int>>, String>;
      final failures = (exit.cause as CauseGroup<String>).causes;

      expect(failures, hasLength(2));
      final defect = failures.first as CauseDefect<String>;
      expect(defect.error, same(error));
      expect(defect.stackTrace, same(trace));
      expect(failures.last, same(primary));
    });
  });
}

void _expectCancelled<T, E>(Exit<T, E> exit, Object? reason) {
  expect(
    (exit as ExitFailure<T, E>).cause,
    isA<CauseCancelled<E>>().having(
      (cause) => cause.reason,
      'reason',
      same(reason),
    ),
  );
}
