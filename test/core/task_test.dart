import 'dart:async';

import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  group('Task construction', () {
    test(
      'construction is lazy and each execution invokes callbacks again',
      () async {
        var executions = 0;
        final task = Task.sync(() => ++executions).map((value) => value * 2);

        expect(executions, 0);
        expect(_value(await _run(task)), 2);
        expect(_value(await _run(task)), 4);
        expect(executions, 2);
      },
    );

    test('succeed and fromResult preserve nullable values', () async {
      expect(_value(await _run(Task.succeed<String?>(null))), isNull);
      expect(
        _value(
          await _run(Task.fromResult(const Result<String?, int>.success(null))),
        ),
        isNull,
      );
      expect(
        (_cause(
          await _run(Task.fromResult(const Result<int, String?>.failure(null))),
        ) as CauseFailure<String?>).error,
        isNull,
      );
    });

    test('sync preserves the original thrown object and stack', () async {
      final error = StateError('failed');
      final stack = StackTrace.fromString('original sync stack');
      final task = Task.sync<int>(
        () => Error.throwWithStackTrace(error, stack),
      );
      final cause = _cause(await _run(task)) as CauseDefect;

      expect(cause.error, same(error));
      expect(cause.stackTrace.toString(), stack.toString());
    });

    test('trySync converts exceptions explicitly', () async {
      final error = StateError('failed');
      final stack = StackTrace.fromString('original trySync stack');
      final task = Task.trySync<int, String>(
        () => Error.throwWithStackTrace(error, stack),
        onError: (caught, trace) {
          expect(caught, same(error));
          expect(trace.toString(), stack.toString());
          return 'mapped';
        },
      );

      expect(
        (_cause(await _run(task)) as CauseFailure<String>).error,
        'mapped',
      );
    });

    test('exceptions from exception mappers become defects', () async {
      final mappingError = StateError('mapping failed');
      final tasks = [
        Task.trySync<int, String>(
          () => throw StateError('source'),
          onError: (_, _) => throw mappingError,
        ),
        Task.fromFuture<int, String>(
          () => Future.error(StateError('source')),
          onError: (_, _) => throw mappingError,
        ),
      ];

      for (final task in tasks) {
        expect(
          (_cause(await _run(task)) as CauseDefect).error,
          same(mappingError),
        );
      }
    });

    test('defer is lazy and recreates the task for every execution', () async {
      var created = 0;
      final task = Task.defer(() => Task.succeed(++created));

      expect(created, 0);
      expect(_value(await _run(task)), 1);
      expect(_value(await _run(task)), 2);
    });

    test('fromFuture starts a fresh Future for every execution', () async {
      var created = 0;
      final task = Task.fromFuture<int, String>(
        () => Future.value(++created),
        onError: (error, _) => error.toString(),
      );

      expect(created, 0);
      expect(_value(await _run(task)), 1);
      expect(_value(await _run(task)), 2);
    });

    test(
      'fromFuture maps synchronous factory errors and asynchronous errors',
      () async {
        final error = StateError('native failure');
        final stack = StackTrace.fromString('original Future stack');
        for (final create in <Future<int> Function()>[
          () => Error.throwWithStackTrace(error, stack),
          () => Future.error(error, stack),
        ]) {
          final task = Task.fromFuture<int, String>(
            create,
            onError: (caught, trace) {
              expect(caught, same(error));
              expect(trace.toString(), stack.toString());
              return 'mapped';
            },
          );
          expect(
            (_cause(await _run(task)) as CauseFailure<String>).error,
            'mapped',
          );
        }
      },
    );
  });

  group('Task composition', () {
    test('flatMap and tap use one ordered success path', () async {
      final observed = <int>[];
      final task = Task.succeed(1)
          .withError<String>()
          .flatMap((value) => Task.succeed(value + 1))
          .tap(
            (value) => Task.sync(() {
              observed.add(value);
            }),
          )
          .map((value) => value * 3);

      expect(_value(await _run(task)), 6);
      expect(observed, [2]);
    });

    test('failures skip success callbacks and tap can fail', () async {
      final Task<int, String> failure = Task.fail('failed');
      final task = failure
          .map((value) => fail('map must not run'))
          .flatMap((value) => throw StateError('flatMap must not run'))
          .tap((value) => throw StateError('tap must not run'));

      expect(
        (_cause(await _run(task)) as CauseFailure<String>).error,
        'failed',
      );
      final tapped = Task.succeed(1)
          .withError<String>()
          .tap((_) => Task.fail('tap'));
      expect((_cause(await _run(tapped)) as CauseFailure<String>).error, 'tap');
    });

    test(
      'callbacks that throw preserve their exception and stack as defects',
      () async {
        final error = StateError('callback failed');
        final stack = StackTrace.fromString('callback stack');
        Never throwing() => Error.throwWithStackTrace(error, stack);
        final Task<int, String> success = Task.succeed(1);
        final Task<int, String> failure = Task.fail('failed');
        final Task<int, String> grouped = Task.failCause(
          Cause.combine([
            const Cause.failure('first'),
            const Cause.cancelled('recorded'),
            const Cause.failure('second'),
          ]),
        );
        final tasks = <Task<Object?, Object?>>[
          success.map((_) => throwing()),
          success.flatMap((_) => throwing()),
          Task.defer<int, String>(throwing),
          failure.mapError((_) => throwing()),
          failure.recover((_) => throwing()),
          grouped.recover((_) => throwing()),
          failure.recoverCause((_) => throwing()),
          failure.mapCause((_) => throwing()),
        ];

        for (final task in tasks) {
          final cause = _cause(await _run(task)) as CauseDefect;
          expect(cause.error, same(error));
          expect(cause.stackTrace.toString(), stack.toString());
        }
      },
    );

    test(
      'Never widening preserves the statically selected callback types',
      () async {
        final Task<num, String> task = Task.succeed(1);
        final next = task.flatMap(
          (num value) => Task.succeed(value.toDouble() + 0.5),
        );

        expect(_value(await _run(next)), 1.5);
        expect(
          _value(
            await _run(
              Task.succeed(1)
                  .withError<String>()
                  .recover((_) => Task.succeed(2)),
            ),
          ),
          1,
        );
      },
    );

    test(
      'recover handles a business error but bypasses defects and cancellation',
      () async {
        final Task<int, String> task = Task.fail('failed');
        expect(
          _value(
            await _run(task.recover((error) => Task.succeed(error.length))),
          ),
          6,
        );

        final causes = <Cause<String>>[
          Cause.defect(StateError('defect'), StackTrace.current),
          const Cause.cancelled('explicit'),
          Cause.combine([
            const Cause.cancelled('first'),
            const Cause.cancelled('second'),
          ]),
        ];
        for (final cause in causes) {
          final Task<int, String> failed = Task.failCause(cause);
          final exit = await _run(
            failed.recover((_) => fail('Recovery must not run')),
          );
          expect(_cause(exit), same(cause));
        }
      },
    );

    test(
      'recover selects the first business value and handles the entire group',
      () async {
        for (final first in <String?>['first', null]) {
          final seen = <String?>[];
          final Task<int, String?> task = Task.failCause(
            Cause<String?>.combine([
              const Cause.cancelled('before'),
              Cause.failure(first),
              const Cause.failure('later'),
              const Cause.cancelled('after'),
            ]),
          );
          final recovered = task.recover((error) {
            seen.add(error);
            return Task.succeed(42);
          });

          expect(_value(await _run(recovered)), 42);
          expect(seen, [first]);
        }
      },
    );

    test(
      'a trailing defect, including cleanup, prevents group recovery',
      () async {
        final error = StateError('defect');
        final stack = StackTrace.fromString('preserved defect stack');
        final defect = CauseDefect(error, stack);
        final first = Object();
        final second = Object();
        final reasons = <Cause<Object>>[
          Cause.failure(first),
          const Cause.cancelled('recorded'),
          Cause.failure(second),
          defect,
        ];
        final cause = Cause.combine(reasons);
        final Task<int, Object> direct = Task.failCause(cause);
        final Task<int, Object> beforeCleanup = Task.failCause(
          Cause.combine(reasons.take(3)),
        );
        final tasks = [
          direct,
          beforeCleanup.ensuring(Task.failCause<Never>(defect)),
        ];

        for (final task in tasks) {
          final exit = await _run(
            task.recover((_) => fail('A defect must prevent recovery.')),
          );
          final retained = (_cause(exit) as CauseGroup<Object>).causes;
          expect(retained, hasLength(4));
          expect((retained[0] as CauseFailure<Object>).error, same(first));
          expect(retained[1], same(reasons[1]));
          expect((retained[2] as CauseFailure<Object>).error, same(second));
          expect(retained[3], same(defect));
          final last = retained.last as CauseDefect;
          expect(last.error, same(error));
          expect(last.stackTrace, same(stack));
        }
      },
    );

    test(
      'a failing recovery replaces the group without recovering itself',
      () async {
        var calls = 0;
        const fallback = Cause<String>.failure('fallback');
        final Task<int, String> task = Task.failCause(
          Cause.combine([
            const Cause.failure('first'),
            const Cause.cancelled('recorded'),
            const Cause.failure('second'),
          ]),
        );
        final exit = await _run(
          task.recover((error) {
            expect(error, 'first');
            calls++;
            return Task.failCause(fallback);
          }),
        );

        expect(calls, 1);
        expect(_cause(exit), same(fallback));
      },
    );

    test('recovery does not consume later outer cleanup failures', () async {
      final cleanup = Cause<Never>.defect(Object(), StackTrace.current);
      var calls = 0;
      final Task<int, String> failure = Task.failCause(
        Cause.combine([
          const Cause.failure('first'),
          const Cause.failure('second'),
        ]),
      );
      final task = Task.addFinalizer((_) => Task.failCause(cleanup))
          .withError<String>()
          .flatMap(
            (_) => failure.recover((_) {
              calls++;
              return Task.succeed(42);
            }),
          );

      expect(_cause(await _run(task)), same(cleanup));
      expect(calls, 1);
    });

    test(
      'owner cancellation before or during recovery stops the fallback',
      () async {
        for (final cancelInHandler in [false, true]) {
          final controller = CancellationController();
          final seen = <String>[];
          final task =
              Task.defer<int, String>(() {
                if (!cancelInHandler) controller.requestCancel(reason: 'owner');
                return Task.failCause(
                  Cause.combine([
                    const Cause.failure('first'),
                    const Cause.cancelled('recorded'),
                    const Cause.failure('second'),
                  ]),
                );
              }).recover((error) {
                seen.add(error);
                controller.requestCancel(reason: 'owner');
                return Task.sync(() => fail('The fallback must not start.'));
              });

          final cause = _cause(
            await _run(task, signal: controller.signal),
          ) as CauseCancelled;
          expect(cause.reason, 'owner');
          expect(seen, cancelInHandler ? ['first'] : isEmpty);
        }
      },
    );

    test('mapError and recoverCause restore typed grouped causes', () async {
      final defect = Cause<int>.defect(
        StateError('defect'),
        StackTrace.current,
      );
      const cancelled = Cause<int>.cancelled('explicit');
      final Task<int, int> task = Task.failCause(
        Cause<int>.combine([
          const Cause.failure(1),
          defect,
          const Cause.failure(2),
          cancelled,
        ]),
      );
      final adapted = task.mapError((error) => 'error $error');
      final captured = _value(await _run(adapted.exit()));
      final cause = _cause(captured) as CauseGroup<String>;

      expect((cause.causes[0] as CauseFailure<String>).error, 'error 1');
      expect(cause.causes[1], same(defect));
      expect((cause.causes[2] as CauseFailure<String>).error, 'error 2');
      expect(cause.causes[3], same(cancelled));
      expect(
        _value(
          await _run(
            adapted.recoverCause<bool>((cause) {
              expect(cause, isA<CauseGroup<String>>());
              return Task.succeed((cause as CauseGroup<String>).causes.length);
            }),
          ),
        ),
        4,
      );
    });

    test('mapCause can change the entire error type', () async {
      final Task<int, String> task = Task.fail('source');
      final mapped = task.mapCause<int>((cause) => Cause.failure(42));

      expect((_cause(await _run(mapped)) as CauseFailure<int>).error, 42);
    });

    test('exit captures explicit cancellation as a value', () async {
      final captured = Task.failCause<Never>(const Cause.cancelled('explicit'))
          .exit();
      final cause = _cause(_value(await _run(captured))) as CauseCancelled;

      expect(cause.reason, 'explicit');
    });

    test(
      'orDie transforms every business failure and retains other reasons',
      () async {
        final error = Object();
        final defect = Cause<Object?>.defect(
          StateError('original'),
          StackTrace.current,
        );
        const cancelled = Cause<Object?>.cancelled('explicit');
        const nullFailure = Cause<Object?>.failure(null);
        final task = Task.failCause(
          Cause<Object?>.combine([
            Cause.failure(error),
            nullFailure,
            defect,
            cancelled,
          ]),
        ).orDie();
        final causes = (_cause(await _run(task)) as CauseGroup<Never>).causes;

        expect((causes[0] as CauseDefect).error, same(error));
        expect((causes[1] as CauseDefect).error, same(nullFailure));
        expect(causes[2], same(defect));
        expect(causes[3], same(cancelled));
      },
    );
  });

  group('Task execution boundaries', () {
    test('ten thousand map and flatMap steps are stack safe', () async {
      var mapped = Task.succeed(0);
      var bound = Task.succeed(0);
      for (var i = 0; i < 10000; i++) {
        mapped = mapped.map((value) => value + 1);
        bound = bound.flatMap((value) => Task.succeed(value + 1));
      }

      expect(_value(await _run(mapped)), 10000);
      expect(_value(await _run(bound)), 10000);
    });

    test('deferred recursion and failure chains are stack safe', () async {
      Task<int, Never> count(int remaining) => remaining == 0
          ? Task.succeed(0)
          : Task.defer(() => count(remaining - 1)).map((value) => value + 1);
      Task<int, int> failed = Task.fail(0);
      for (var i = 0; i < 10000; i++) {
        failed = failed.mapError((error) => error + 1);
      }

      expect(_value(await _run(count(10000))), 10000);
      expect((_cause(await _run(failed)) as CauseFailure<int>).error, 10000);
    });

    test('long synchronous chains let event-loop timers advance', () async {
      var timerRan = false;
      var task = Task.succeed(0);
      for (var i = 0; i < 10000; i++) {
        task = task.map((value) => value + 1);
      }
      Timer.run(() {
        timerRan = true;
      });

      expect(_value(await _run(task)), 10000);
      expect(timerRan, isTrue);
    });

    test('pre-cancelled execution does not start business work', () async {
      final controller = CancellationController()
        ..requestCancel(reason: 'stop');
      final task = Task.sync<int>(() => fail('Task must not start')).exit();
      final cause =
          _cause(await _run(task, signal: controller.signal)) as CauseCancelled;

      expect(cause.reason, 'stop');
    });

    test(
      'cancellation in a callback skips later callbacks and recovery',
      () async {
        final controller = CancellationController();
        final task =
            Task.sync(() {
                  controller.requestCancel(reason: 'stop');
                  return 1;
                })
                .map((_) => fail('map must not run'))
                .recoverCause((_) => fail('recovery must not run'))
                .exit();

        expect(
          _cause(await _run(task, signal: controller.signal)),
          isA<CauseCancelled>(),
        );
      },
    );

    test(
      'cancellation stops later callbacks inside grouped error mapping',
      () async {
        final controller = CancellationController();
        final visited = <int>[];
        final task =
            Task.failCause(
              Cause<int>.combine([
                const Cause.failure(1),
                const Cause.failure(2),
              ]),
            ).mapError((error) {
              visited.add(error);
              controller.requestCancel();
              return error.toString();
            });

        expect(
          _cause(await _run(task, signal: controller.signal)),
          isA<CauseCancelled>(),
        );
        expect(visited, [1]);
      },
    );

    test(
      'cancellation during error mapping retains previously observed defects',
      () async {
        final error = StateError('already observed');
        final stack = StackTrace.fromString('original defect stack');
        final defect = Cause<int>.defect(error, stack);

        for (var position = 0; position < 3; position++) {
          final controller = CancellationController();
          final visited = <int>[];
          final reasons = <Cause<int>>[
            const Cause.failure(1),
            const Cause.failure(2),
          ]..insert(position, defect);
          final task = Task.failCause(Cause.combine(reasons)).mapError((error) {
            visited.add(error);
            controller.requestCancel(reason: 'cancel in mapping');
            return error.toString();
          }).exit();

          final cause = _cause(
            await _run(task, signal: controller.signal),
          ) as CauseGroup<Never>;
          expect(visited, [1]);
          expect(cause.causes.length, 2);
          expect(cause.causes.first, same(defect));
          expect((cause.causes.first as CauseDefect).error, same(error));
          expect((cause.causes.first as CauseDefect).stackTrace, same(stack));
          expect(
            (cause.causes.last as CauseCancelled).reason,
            'cancel in mapping',
          );
        }
      },
    );

    test(
      'cancellation during yielding does not leak an intermediate error type',
      () async {
        final controller = CancellationController();
        Task<Never, int> task = Task.fail('source').mapError((_) {
          Timer.run(() {
            controller.requestCancel(reason: 'stop');
          });
          return 0;
        });
        for (var i = 0; i < 10000; i++) {
          task = task.mapError((error) => error + 1);
        }
        final finalType = task.mapError((_) => 'final type').exit();

        expect(
          _cause(await _run(finalType, signal: controller.signal)),
          isA<CauseCancelled>(),
        );
      },
    );

    test(
      'a cancelled Future is awaited and its late error remains a defect',
      () async {
        final controller = CancellationController();
        final started = Completer<void>();
        final pending = Completer<int>();
        final error = StateError('late native failure');
        final stack = StackTrace.fromString('late native stack');
        var completed = false;
        final task =
            Task.fromFuture<int, String>(
                  () {
                    started.complete();
                    return pending.future;
                  },
                  onError: (_, _) =>
                      fail('onError must not run after cancellation'),
                )
                .mapError<int>((_) => fail('mapError must not run'))
                .recoverCause<bool>((_) => fail('recovery must not run'))
                .exit();
        final running = _run(task, signal: controller.signal).then((exit) {
          completed = true;
          return exit;
        });
        await started.future;
        controller.requestCancel(reason: 'stop');
        await Future<void>.delayed(Duration.zero);
        expect(completed, isFalse);

        pending.completeError(error, stack);
        final cause = _cause(await running) as CauseGroup<Never>;
        expect(cause.causes.length, 2);
        expect((cause.causes.first as CauseDefect).error, same(error));
        expect(
          (cause.causes.first as CauseDefect).stackTrace.toString(),
          stack.toString(),
        );
        expect((cause.causes.last as CauseCancelled).reason, 'stop');
      },
    );

    test('a cancelled Future cannot succeed after it settles', () async {
      final controller = CancellationController();
      final pending = Completer<int>();
      final task = Task.fromFuture<int, String>(
        () => pending.future,
        onError: (error, _) => error.toString(),
      ).map((_) => fail('Continuation must not run'));
      final running = _run(task, signal: controller.signal);
      controller.requestCancel();
      pending.complete(42);

      expect(_cause(await running), isA<CauseCancelled>());
    });

    test(
      'synchronous native failure after cancellation bypasses onError',
      () async {
        final controller = CancellationController();
        final error = StateError('native failure');
        final task = Task.fromFuture<int, String>(() {
          controller.requestCancel();
          throw error;
        }, onError: (_, _) => fail('onError must not run'));
        final cause = _cause(
          await _run(task, signal: controller.signal),
        ) as CauseGroup<String>;

        expect((cause.causes.first as CauseDefect).error, same(error));
        expect(cause.causes.last, isA<CauseCancelled>());
      },
    );
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
