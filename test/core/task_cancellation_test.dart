import 'dart:async';

import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  group('Task cancellation', () {
    for (final reason in [null, Object()]) {
      test(
        'skips the whole computation when already cancelled (reason: ${reason != null})',
        () async {
          final controller = CancellationController()..cancel(reason: reason);
          var called = false;
          final task =
              Task<int, Never, ()>((_) {
                    called = true;
                    return const Exit.success(1);
                  })
                  .map((value) {
                    called = true;
                    return value + 1;
                  })
                  .flatMap((value) {
                    called = true;
                    return Task<int, Never, ()>((_) => Exit.success(value));
                  });

          final exit = await task.run((), cancellation: controller.token);

          _expectCancelled(exit, reason);
          expect(called, isFalse);
        },
      );
    }

    test('observes cancellation between run and its first microtask', () async {
      final controller = CancellationController();
      var called = false;
      final task = Task<int, Never, ()>((_) {
        called = true;
        return const Exit.success(1);
      });

      final running = task.run((), cancellation: controller.token);
      controller.cancel(reason: 'before start');

      _expectCancelled(await running, 'before start');
      expect(called, isFalse);
    });

    test('waits for running work before releasing resources', () async {
      final controller = CancellationController();
      final reason = Object();
      final started = Completer<void>();
      final body = Completer<Exit<int, Never>>();
      final cleanupExits = <Exit<void, Object?>>[];
      late Context<()> context;
      final task = Task<int, Never, ()>((current) {
        context = current;
        current.scope.addFinalizer(cleanupExits.add);
        started.complete();
        return body.future;
      });
      var finished = false;
      final running = task.run((), cancellation: controller.token).then((exit) {
        finished = true;
        return exit;
      });
      await started.future;

      controller.cancel(reason: reason);
      await Future<void>(() {});

      expect(context.cancellation, same(controller.token));
      expect(context.scope.isClosed, isFalse);
      expect(cleanupExits, isEmpty);
      expect(finished, isFalse);

      body.complete(const Exit.success(42));
      final exit = await running;

      _expectCancelled(exit, reason);
      expect(cleanupExits.single, same(exit));
      expect(context.scope.isClosed, isTrue);
    });

    test(
      'notifies a native operation and awaits its asynchronous shutdown',
      () async {
        final controller = CancellationController();
        final started = Completer<void>();
        final nativeCleanupStarted = Completer<void>();
        final nativeCleanup = Completer<void>();
        final events = <String>[];
        final stream = StreamController<int>(
          onCancel: () async {
            nativeCleanupStarted.complete();
            await nativeCleanup.future;
            events.add('native cleanup');
          },
        );
        addTearDown(() {
          if (!nativeCleanup.isCompleted) nativeCleanup.complete();
          return stream.close();
        });
        late Scope scope;
        final task = Task<void, Never, ()>((context) async {
          scope = context.scope;
          final subscription = stream.stream.listen((_) {});
          final stopped = Completer<void>();
          final removeListener = context.cancellation.onCancel((_) {
            stopped.complete(subscription.cancel());
          });
          context.scope.addFinalizer((_) async {
            await stream.close();
            events.add('scope cleanup');
          });
          try {
            started.complete();
            await stopped.future;
            return Exit.failure(
              Cause.cancelled(reason: context.cancellation.reason),
            );
          } finally {
            removeListener();
          }
        });
        final running = task.run((), cancellation: controller.token);
        await started.future;

        controller.cancel(reason: 'shutdown');
        await nativeCleanupStarted.future;
        expect(scope.isClosed, isFalse);
        expect(events, isEmpty);

        nativeCleanup.complete();
        _expectCancelled(await running, 'shutdown');
        expect(events, ['native cleanup', 'scope cleanup']);
      },
    );

    for (final operation in ['map', 'flatMap']) {
      test(
        '$operation skips its callback after cancellation in the previous step',
        () async {
          final controller = CancellationController();
          final task = Task<int, Never, ()>((_) {
            controller.cancel(reason: 'between steps');
            return const Exit.success(1);
          });
          var called = false;
          final composed = operation == 'map'
              ? task.map((value) {
                  called = true;
                  return value + 1;
                })
              : task.flatMap((value) {
                  called = true;
                  return Task<int, Never, ()>((_) => Exit.success(value + 1));
                });

          _expectCancelled(
            await composed.run((), cancellation: controller.token),
            'between steps',
          );
          expect(called, isFalse);
        },
      );
    }

    test(
      'observes cancellation from a map callback before further steps',
      () async {
        final controller = CancellationController();
        var called = false;
        final task = Task<int, Never, ()>((_) => const Exit.success(1))
            .map((value) {
              controller.cancel(reason: 'mapped');
              return value + 1;
            })
            .map((value) {
              called = true;
              return value + 1;
            });

        _expectCancelled(
          await task.run((), cancellation: controller.token),
          'mapped',
        );
        expect(called, isFalse);
      },
    );

    test(
      'does not start the next task if selecting it requests cancellation',
      () async {
        final controller = CancellationController();
        var called = false;
        final task = Task<int, Never, ()>((_) => const Exit.success(1))
            .flatMap((value) {
              controller.cancel(reason: 'selected');
              return Task<int, Never, ()>((_) {
                called = true;
                return Exit.success(value);
              });
            });

        _expectCancelled(
          await task.run((), cancellation: controller.token),
          'selected',
        );
        expect(called, isFalse);
      },
    );

    final error = StateError('original failure');
    final trace = StackTrace.fromString('original trace');
    final explicitReason = Object();
    final terminalOutcomes = [
      (name: 'failure', exit: Exit<int, Object?>.failure(Cause.failure(error))),
      (
        name: 'defect',
        exit: Exit<int, Object?>.failure(Cause.defect(error, trace)),
      ),
      (
        name: 'explicit cancellation',
        exit: Exit<int, Object?>.failure(
          Cause.cancelled(reason: explicitReason),
        ),
      ),
      (
        name: 'combined failure',
        exit: Exit<int, Object?>.failure(
          Cause.combine([
            Cause.failure(error),
            Cause.defect(error, trace),
            Cause.cancelled(reason: explicitReason),
          ]),
        ),
      ),
    ];

    for (final outcome in terminalOutcomes) {
      test('preserves ${outcome.name} returned after a request', () async {
        final controller = CancellationController();
        final started = Completer<void>();
        final body = Completer<Exit<int, Object?>>();
        late Exit<void, Object?> cleanupExit;
        final task = Task<int, Object?, ()>((context) {
          context.scope.addFinalizer((exit) => cleanupExit = exit);
          started.complete();
          return body.future;
        });
        final running = task.run((), cancellation: controller.token);
        await started.future;

        controller.cancel(reason: 'external request');
        body.complete(outcome.exit);

        expect(await running, same(outcome.exit));
        expect(cleanupExit, same(outcome.exit));
      });
    }

    for (final asynchronous in [false, true]) {
      test(
        'preserves a thrown error after cancellation (async: $asynchronous)',
        () async {
          final controller = CancellationController();
          late Exit<void, Object?> cleanupExit;
          final task = Task<int, StateError, ()>((context) {
            context.scope.addFinalizer((exit) => cleanupExit = exit);
            controller.cancel(reason: 'external request');
            if (asynchronous) {
              return Future<Exit<int, StateError>>.error(error, trace);
            }
            Error.throwWithStackTrace(error, trace);
          });

          final exit = await task.run((), cancellation: controller.token);

          final cause = (exit as ExitFailure<int, StateError>).cause;
          final defect = cause as CauseDefect<StateError>;
          expect(defect.error, same(error));
          expect(defect.stackTrace, same(trace));
          expect(cleanupExit, same(exit));
        },
      );
    }

    for (final outcome in [
      (name: 'success', exit: const Exit<int, Object?>.success(42)),
      ...terminalOutcomes,
    ]) {
      test(
        'keeps ${outcome.name} fixed and awaits cleanup after a late request',
        () async {
          final controller = CancellationController();
          final cleanupStarted = Completer<void>();
          final cleanup = Completer<void>();
          late Exit<void, Object?> cleanupExit;
          final task = Task<int, Object?, ()>((context) {
            context.scope.addFinalizer((exit) {
              cleanupExit = exit;
              cleanupStarted.complete();
              return cleanup.future;
            });
            return outcome.exit;
          });
          var finished = false;
          final running = task.run((), cancellation: controller.token).then((
            exit,
          ) {
            finished = true;
            return exit;
          });
          await cleanupStarted.future;

          controller.cancel(reason: 'during cleanup');
          await Future<void>(() {});
          expect(finished, isFalse);
          expect(cleanupExit, same(outcome.exit));

          cleanup.complete();
          expect(await running, same(outcome.exit));
        },
      );
    }

    test(
      'keeps cleanup failures together with the cancellation exit',
      () async {
        final controller = CancellationController();
        final reason = Object();
        var remainingCleanupRan = false;
        final task = Task<int, Never, ()>((context) {
          context.scope.addFinalizer((_) => remainingCleanupRan = true);
          context.scope.addFinalizer(
            (_) => Error.throwWithStackTrace(error, trace),
          );
          controller.cancel(reason: reason);
          return const Exit.success(1);
        });

        final exit = await task.run((), cancellation: controller.token);

        final cause =
            (exit as ExitFailure<int, Never>).cause as CauseGroup<Never>;
        expect(cause.causes, hasLength(2));
        expect(
          (cause.causes.first as CauseCancelled<Never>).reason,
          same(reason),
        );
        final cleanup = cause.causes.last as CauseDefect<Never>;
        expect(cleanup.error, same(error));
        expect(cleanup.stackTrace, same(trace));
        expect(remainingCleanupRan, isTrue);
      },
    );

    test(
      'removes a completed operation listener before later cancellation',
      () async {
        final controller = CancellationController();
        var calls = 0;
        final task = Task<int, Never, ()>((context) {
          final remove = context.cancellation.onCancel((_) => calls++);
          context.scope.addFinalizer((_) => remove());
          return const Exit.success(42);
        });

        final exit = await task.run((), cancellation: controller.token);
        controller.cancel(reason: 'after completion');

        expect((exit as ExitSuccess<int, Never>).value, 42);
        expect(calls, 0);
      },
    );

    test('keeps concurrent runs cancellation and scopes independent', () async {
      final firstController = CancellationController();
      final secondController = CancellationController();
      final firstGate = Completer<Exit<int, Never>>();
      final secondGate = Completer<Exit<int, Never>>();
      final bothStarted = Completer<void>();
      final contexts = <Context<Future<Exit<int, Never>>>>[];
      final task = Task<int, Never, Future<Exit<int, Never>>>((context) {
        contexts.add(context);
        if (contexts.length == 2) bothStarted.complete();
        return context.dependencies;
      });
      final first = task.run(
        firstGate.future,
        cancellation: firstController.token,
      );
      final second = task.run(
        secondGate.future,
        cancellation: secondController.token,
      );
      await bothStarted.future;

      firstController.cancel(reason: 'first only');
      secondGate.complete(const Exit.success(2));
      expect((await second as ExitSuccess<int, Never>).value, 2);
      expect(contexts[0].scope.isClosed, isFalse);
      expect(contexts[0].cancellation, same(firstController.token));
      expect(contexts[1].cancellation, same(secondController.token));
      expect(contexts[1].cancellation.isCancelled, isFalse);

      firstGate.complete(const Exit.success(1));
      _expectCancelled(await first, 'first only');
      expect(contexts[0].scope.isClosed, isTrue);
    });
  });
}

void _expectCancelled<T, E>(Exit<T, E> exit, Object? reason) {
  final cause = (exit as ExitFailure<T, E>).cause;
  expect(cause, isA<CauseCancelled<E>>());
  expect((cause as CauseCancelled<E>).reason, same(reason));
}
