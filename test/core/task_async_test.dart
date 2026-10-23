import 'dart:async';

import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  group('Task.fromCallback', () {
    test('registers lazily on every execution', () async {
      var registrations = 0;
      var cancellations = 0;
      final task = Task.fromCallback<int, String>((complete) {
        complete(Exit.success(++registrations));
        return () {
          cancellations++;
        };
      });

      expect(registrations, 0);
      expect(_value(await _run(task)), 1);
      expect(_value(await _run(task)), 2);
      expect(cancellations, 0);
    });

    test(
      'synchronous completion accepts null and ignores duplicates',
      () async {
        final task = Task.fromCallback<int?, String>((complete) {
          complete(const Exit.success(null));
          complete(const Exit.failure(Cause.failure('late')));
          return () => fail('Completed work must not be cancelled');
        });

        expect(_value(await _run(task)), isNull);
      },
    );

    test(
      'asynchronous completion preserves its complete typed failure',
      () async {
        late void Function(Exit<int, String>) complete;
        final defect = Cause<String>.defect(
          StateError('defect'),
          StackTrace.current,
        );
        final task = Task.fromCallback<int, String>((callback) {
          complete = callback;
          return () => fail('Completed work must not be cancelled');
        });
        final running = _run(task);
        complete(
          Exit.failure(
            Cause.combine([const Cause.failure('business'), defect]),
          ),
        );
        final cause = _cause(await running) as CauseGroup<String>;

        expect((cause.causes.first as CauseFailure<String>).error, 'business');
        expect(cause.causes.last, same(defect));
      },
    );

    test('explicit callback cancellation can be observed with exit', () async {
      final task = Task.fromCallback<int, Never>((complete) {
        complete(const Exit.failure(Cause.cancelled('explicit')));
        return () => fail('Completed work must not be cancelled');
      }).exit();

      expect(
        (_cause(_value(await _run(task))) as CauseCancelled).reason,
        'explicit',
      );
    });

    test(
      'a registration exception retains its original object and stack',
      () async {
        final error = StateError('registration failed');
        final stack = StackTrace.fromString('registration stack');
        final task = Task.fromCallback<int, String>((complete) {
          Error.throwWithStackTrace(error, stack);
        });
        final cause = _cause(await _run(task)) as CauseDefect;

        expect(cause.error, same(error));
        expect(cause.stackTrace.toString(), stack.toString());
      },
    );

    test(
      'synchronous success does not hide a later registration exception',
      () async {
        final error = StateError('after success');
        final task = Task.fromCallback<int, String>((complete) {
          complete(const Exit.success(42));
          throw error;
        });

        expect((_cause(await _run(task)) as CauseDefect).error, same(error));
      },
    );

    test(
      'synchronous failure is retained before a registration exception',
      () async {
        final error = StateError('after failure');
        final task = Task.fromCallback<int, String>((complete) {
          complete(const Exit.failure(Cause.failure('first')));
          complete(const Exit.failure(Cause.failure('ignored')));
          throw error;
        });
        final cause = _cause(await _run(task)) as CauseGroup<String>;

        expect(cause.causes.length, 2);
        expect((cause.causes.first as CauseFailure<String>).error, 'first');
        expect((cause.causes.last as CauseDefect).error, same(error));
      },
    );

    test('pre-cancellation does not register the operation', () async {
      final controller = CancellationController()
        ..requestCancel(reason: 'stop');
      final task = Task.fromCallback<int, String>(
        (_) => fail('Must not register'),
      );

      expect(
        (_cause(
          await _run(task, signal: controller.signal),
        ) as CauseCancelled).reason,
        'stop',
      );
    });

    test(
      'cancellation during registration waits for the returned cleanup',
      () async {
        final controller = CancellationController();
        final events = <String>[];
        final cleaned = Completer<void>();
        var finished = false;
        final task = Task.fromCallback<int, String>((complete) {
          events.add('register');
          controller.requestCancel(reason: 'stop');
          complete(const Exit.success(42));
          events.add('return cleanup');
          return () {
            events.add('cleanup');
            return cleaned.future;
          };
        });
        final running = _run(task, signal: controller.signal).then((exit) {
          finished = true;
          return exit;
        });
        expect(events, ['register', 'return cleanup', 'cleanup']);
        await Future<void>.delayed(Duration.zero);
        expect(finished, isFalse);
        cleaned.complete();

        expect((_cause(await running) as CauseCancelled).reason, 'stop');
      },
    );

    test(
      'synchronous completion before cancellation does not invoke cleanup',
      () async {
        final controller = CancellationController();
        var cleanups = 0;
        final task = Task.fromCallback<int, String>((complete) {
          complete(const Exit.success(42));
          controller.requestCancel();
          return () {
            cleanups++;
          };
        });

        expect(
          _cause(await _run(task, signal: controller.signal)),
          isA<CauseCancelled>(),
        );
        expect(cleanups, 0);
      },
    );

    test(
      'pending cancellation ignores completion and waits for cleanup once',
      () async {
        final controller = CancellationController();
        final cleaned = Completer<void>();
        late void Function(Exit<int, String>) complete;
        var cleanups = 0;
        var finished = false;
        final task = Task.fromCallback<int, String>((callback) {
          complete = callback;
          return () {
            cleanups++;
            complete(const Exit.success(99));
            return cleaned.future;
          };
        }).map((_) => fail('Continuation must not run'));
        final running = _run(task, signal: controller.signal).then((exit) {
          finished = true;
          return exit;
        });
        controller.requestCancel(reason: 'first');
        controller.requestCancel(reason: 'second');
        complete(const Exit.failure(Cause.failure('late')));
        await Future<void>.delayed(Duration.zero);
        expect(finished, isFalse);
        expect(cleanups, 1);
        cleaned.complete();

        expect((_cause(await running) as CauseCancelled).reason, 'first');
        complete(const Exit.success(100));
        expect(cleanups, 1);
      },
    );

    test(
      'cleanup failures preserve their original stack alongside cancellation',
      () async {
        for (final asynchronous in [false, true]) {
          final controller = CancellationController();
          final error = StateError('cleanup failed');
          final stack = StackTrace.fromString('cleanup stack');
          final task = Task.fromCallback<int, String>(
            (_) => () {
              if (asynchronous) return Future<void>.error(error, stack);
              Error.throwWithStackTrace(error, stack);
            },
          ).mapError<int>((_) => fail('Business mapping must not run')).exit();
          final running = _run(task, signal: controller.signal);
          controller.requestCancel(reason: 'stop');
          final cause = _cause(await running) as CauseGroup<Never>;

          expect(cause.causes.length, 2);
          expect((cause.causes.first as CauseDefect).error, same(error));
          expect(
            (cause.causes.first as CauseDefect).stackTrace.toString(),
            stack.toString(),
          );
          expect((cause.causes.last as CauseCancelled).reason, 'stop');
        }
      },
    );

    test('registration failure after cancellation is not lost', () async {
      final controller = CancellationController();
      final error = StateError('registration failed');
      final task = Task.fromCallback<int, String>((_) {
        controller.requestCancel();
        throw error;
      });
      final cause = _cause(
        await _run(task, signal: controller.signal),
      ) as CauseGroup<String>;

      expect((cause.causes.first as CauseDefect).error, same(error));
      expect(cause.causes.last, isA<CauseCancelled>());
    });

    test(
      'settled success and failure do not receive later cancellation',
      () async {
        for (final exit in <Exit<int, String>>[
          const Exit.success(42),
          const Exit.failure(Cause.failure('failed')),
        ]) {
          final controller = CancellationController();
          late void Function(Exit<int, String>) complete;
          var cleanups = 0;
          final task = Task.fromCallback<int, String>((callback) {
            complete = callback;
            return () {
              cleanups++;
            };
          });
          final running = _run(task, signal: controller.signal);
          complete(exit);
          await running;
          controller.requestCancel();
          complete(const Exit.success(100));

          expect(cleanups, 0);
        }
      },
    );

    test('runtime shutdown waits for callback cancellation cleanup', () async {
      final runtime = Runtime();
      final cleaned = Completer<void>();
      var cleanups = 0;
      var closed = false;
      final running = runtime.run(
        Task.fromCallback<int, Never>(
          (_) => () {
            cleanups++;
            return cleaned.future;
          },
        ),
      );
      final closing = runtime.close().then((exit) {
        closed = true;
        return exit;
      });
      await Future<void>.delayed(Duration.zero);
      expect(cleanups, 1);
      expect(closed, isFalse);
      cleaned.complete();

      expect(_cause(await running), isA<CauseCancelled>());
      expect(await closing, isA<ExitSuccess<void, Never>>());
    });

    test(
      'cancellation cleanup retains the registration execution zone',
      () async {
        for (final cancelDuringRegistration in [false, true]) {
          final runtime = Runtime();
          final controller = CancellationController();
          final cleanupClose = Completer<Exit<void, Never>>();
          late Zone registrationZone;
          final running = runtime.run(
            Task.fromCallback<int, Never>((_) {
              registrationZone = Zone.current;
              if (cancelDuringRegistration) controller.requestCancel();
              return () async {
                expect(Zone.current, same(registrationZone));
                await Future<void>.delayed(Duration.zero);
                expect(Zone.current, same(registrationZone));
                cleanupClose.complete(await runtime.close());
              };
            }),
            signal: controller.signal,
          );
          if (!cancelDuringRegistration) controller.requestCancel();

          expect(
            _cause(await running.timeout(const Duration(seconds: 1))),
            isA<CauseCancelled>(),
          );
          final closeFailure = _cause(await cleanupClose.future) as CauseDefect;
          expect(closeFailure.error, isA<StateError>());
          expect(_value(await runtime.run(Task.succeed(42))), 42);
          expect(await runtime.close(), isA<ExitSuccess<void, Never>>());
        }
      },
    );

    test('synchronously completed callback chains are stack safe', () async {
      var task = Task.succeed(0);
      for (var i = 0; i < 10000; i++) {
        task = task.flatMap(
          (value) => Task.fromCallback<int, Never>((complete) {
            complete(Exit.success(value + 1));
            return () => fail('Completed work must not be cancelled');
          }),
        );
      }

      expect(_value(await _run(task)), 10000);
    });
  });

  group('Task cancellation checkpoints', () {
    test(
      'checkCancelled succeeds when no cancellation has been requested',
      () async {
        final task = Task.checkCancelled().map((_) => 42);

        expect(_value(await _run(task)), 42);
      },
    );

    test('checkCancelled does not continue after cancellation', () async {
      final controller = CancellationController()
        ..requestCancel(reason: 'stop');
      final task = Task.checkCancelled().map((_) => fail('Must not continue'));

      expect(
        (_cause(
          await _run(task, signal: controller.signal),
        ) as CauseCancelled).reason,
        'stop',
      );
    });

    test(
      'yieldNow lets event-loop timers run before the continuation',
      () async {
        var timerRan = false;
        Timer.run(() {
          timerRan = true;
        });

        expect(
          _value(await _run(Task.yieldNow().map((_) => timerRan))),
          isTrue,
        );
      },
    );

    test('yieldNow checks cancellation before continuing', () async {
      final controller = CancellationController();
      Timer.run(() {
        controller.requestCancel(reason: 'stop');
      });
      final task = Task.yieldNow().map((_) => fail('Must not continue'));

      expect(
        (_cause(
          await _run(task, signal: controller.signal),
        ) as CauseCancelled).reason,
        'stop',
      );
    });
  });
}

Future<Exit<T, E>> _run<T, E>(
  Task<T, E> task, {
  CancellationSignal? signal,
}) async {
  final runtime = Runtime();
  try {
    return await runtime.run(task, signal: signal);
  } finally {
    await runtime.close();
  }
}

T _value<T, E>(Exit<T, E> exit) => switch (exit) {
  ExitSuccess(:final value) => value,
  ExitFailure(:final cause) => fail('Expected success, got $cause'),
};

Cause<E> _cause<T, E>(Exit<T, E> exit) => switch (exit) {
  ExitSuccess(:final value) => fail('Expected failure, got $value'),
  ExitFailure(:final cause) => cause,
};
