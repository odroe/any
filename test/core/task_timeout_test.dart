import 'dart:async';

import 'package:any/any.dart';
import 'package:any/testing.dart';
import 'package:test/test.dart';

void main() {
  late TestClock clock;
  late Runtime runtime;
  late List<Cause<Object?>> reports;

  setUp(() {
    clock = TestClock();
    reports = [];
    runtime = Runtime(
      context: Context.from([Clock.key.bind(clock)]),
      onUnhandledError: reports.add,
    );
  });
  tearDown(() async {
    await runtime.close();
  });

  const deadline = Duration(seconds: 1);

  test(
    'timeout is lazy and cancels its timer when the operation succeeds',
    () async {
      var calls = 0;
      var errors = 0;
      final task =
          Task.sync(() {
            calls++;
            expect(clock.pendingCount, 1);
            return 42;
          }).withError<String>().timeout(
            deadline,
            onTimeout: () {
              errors++;
              return 'timeout';
            },
          );
      expect(calls, 0);
      expect(clock.pendingCount, 0);
      expect(_value(await runtime.run(task)), 42);
      expect(calls, 1);
      expect(errors, 0);
      expect(clock.pendingCount, 0);
      clock.advance(deadline);
      expect(errors, 0);
    },
  );

  test(
    'original business failures and cancellation do not become timeouts',
    () async {
      var calls = 0;
      for (final cause in <Cause<String>>[
        const Cause.failure('original'),
        const Cause.cancelled('operation'),
      ]) {
        final result = await runtime.run(
          Task.failCause(cause).timeout(
            deadline,
            onTimeout: () {
              calls++;
              return 'timeout';
            },
          ),
        );
        switch (_cause(result)) {
          case CauseFailure(:final error):
            expect(error, 'original');
          case CauseCancelled(:final reason):
            expect(reason, 'operation');
          default:
            fail('Expected the original reason.');
        }
        expect(clock.pendingCount, 0);
      }
      expect(calls, 0);
    },
  );

  test(
    'timeout error is computed only after cancellation cleanup finishes',
    () async {
      final cleanup = Completer<void>();
      addTearDown(() {
        if (!cleanup.isCompleted) cleanup.complete();
      });
      final log = <String>[];
      final gate = _Gate<int, String>(
        onCancel: () async {
          log.add('release start');
          await cleanup.future;
          log.add('release end');
        },
      );
      var done = false;
      final running = runtime
          .run(
            gate.task.timeout(
              deadline,
              onTimeout: () {
                log.add('timeout');
                return 'expired';
              },
            ),
          )
          .then((exit) {
            done = true;
            return exit;
          });
      clock.advance(deadline);
      await _turn();
      expect(log, ['release start']);
      expect(done, isFalse);
      cleanup.complete();
      expect((_cause(await running) as CauseFailure<String>).error, 'expired');
      expect(log, ['release start', 'release end', 'timeout']);
      expect(clock.pendingCount, 0);
    },
  );

  test('timeout combines the typed error with all cleanup defects', () async {
    final first = StateError('first cleanup');
    final second = StateError('second cleanup');
    final task = _Gate<int, String>().task
        .ensuring(Task.sync(() => throw first))
        .ensuring(Task.sync(() => throw second))
        .timeout(deadline, onTimeout: () => 'timeout');
    final running = runtime.run(task);
    clock.advance(deadline);
    final reasons = _reasons(_cause(await running));
    expect((reasons[0] as CauseFailure<String>).error, 'timeout');
    expect(reasons.whereType<CauseDefect>().map((cause) => cause.error), [
      first,
      second,
    ]);
    expect(reasons.whereType<CauseCancelled>(), isEmpty);
    await _turn();
    expect(reports, isEmpty);
  });

  test(
    'a thrown timeout callback retains its stack and cleanup failures',
    () async {
      final callbackError = StateError('onTimeout');
      final cleanupError = StateError('cleanup');
      final callbackTrace = StackTrace.current;
      final gate = _Gate<int, String>(onCancel: () => throw cleanupError);
      final running = runtime.run(
        gate.task.timeout(
          deadline,
          onTimeout: () {
            Error.throwWithStackTrace(callbackError, callbackTrace);
          },
        ),
      );
      clock.advance(deadline);
      final defects = _reasons(_cause(await running)).cast<CauseDefect>();
      expect(defects.map((cause) => cause.error), [
        callbackError,
        cleanupError,
      ]);
      expect(defects.first.stackTrace.toString(), callbackTrace.toString());
    },
  );

  test(
    'timeout preserves a defect produced in the same turn as expiration',
    () async {
      final error = StateError('operation');
      final gate = _Gate<int, String>();
      final running = runtime.run(
        gate.task.timeout(deadline, onTimeout: () => 'timeout'),
      );
      clock.advance(deadline);
      gate.complete(Exit.failure(Cause.defect(error, StackTrace.current)));
      final reasons = _reasons(_cause(await running));
      expect(
        reasons.whereType<CauseFailure<String>>().map((cause) => cause.error),
        ['timeout'],
      );
      expect(reasons.whereType<CauseDefect>().map((cause) => cause.error), [
        error,
      ]);
      expect(reasons.whereType<CauseCancelled>(), isEmpty);
    },
  );

  test(
    'deadline covers cleanup even when the body has already succeeded',
    () async {
      final cleanup = Completer<void>();
      final entered = Completer<void>();
      addTearDown(() {
        if (!cleanup.isCompleted) cleanup.complete();
      });
      final running = runtime.run(
        Task.succeed(1)
            .ensuring(
              Task.defer(() {
                entered.complete();
                return _future(cleanup.future);
              }),
            )
            .withError<String>()
            .timeout(deadline, onTimeout: () => 'timeout'),
      );
      await entered.future;
      clock.advance(deadline);
      await _turn();
      cleanup.complete();
      expect((_cause(await running) as CauseFailure<String>).error, 'timeout');
    },
  );

  test('a native Future must settle before a timeout can finish', () async {
    final future = Completer<int>();
    addTearDown(() {
      if (!future.isCompleted) future.complete(1);
    });
    var timeoutCalls = 0;
    var continued = false;
    var done = false;
    final running = runtime
        .run(
          Task.fromFuture<int, String>(
                () => future.future,
                onError: (_, _) => 'future',
              )
              .tap(
                (_) => Task.sync(() {
                  continued = true;
                }),
              )
              .timeout(
                deadline,
                onTimeout: () {
                  timeoutCalls++;
                  return 'timeout';
                },
              ),
        )
        .then((exit) {
          done = true;
          return exit;
        });
    clock.advance(deadline);
    await _turn();
    expect(done, isFalse);
    expect(timeoutCalls, 0);
    future.complete(1);
    expect((_cause(await running) as CauseFailure<String>).error, 'timeout');
    expect(continued, isFalse);
    expect(timeoutCalls, 1);
  });

  test('native Future errors after expiration remain defects', () async {
    final future = Completer<int>();
    final error = StateError('native Future');
    var mapped = false;
    final running = runtime.run(
      Task.fromFuture<int, String>(
        () => future.future,
        onError: (_, _) {
          mapped = true;
          return 'mapped';
        },
      ).timeout(deadline, onTimeout: () => 'timeout'),
    );
    clock.advance(deadline);
    await _turn();
    future.completeError(error);
    final reasons = _reasons(_cause(await running));
    expect(
      reasons.whereType<CauseFailure<String>>().map((cause) => cause.error),
      ['timeout'],
    );
    expect(reasons.whereType<CauseDefect>().map((cause) => cause.error), [
      error,
    ]);
    expect(mapped, isFalse);
  });

  test(
    'timeoutOrElse starts the fallback only after successful cleanup',
    () async {
      final cleanup = Completer<void>();
      addTearDown(() {
        if (!cleanup.isCompleted) cleanup.complete();
      });
      final log = <String>[];
      final gate = _Gate<int, String>(
        onCancel: () async {
          log.add('release start');
          await cleanup.future;
          log.add('release end');
        },
      );
      final running = runtime.run(
        gate.task.timeoutOrElse(deadline, () {
          log.add('fallback');
          return Task.succeed(2);
        }),
      );
      clock.advance(deadline);
      await _turn();
      expect(log, ['release start']);
      cleanup.complete();
      expect(_value(await running), 2);
      expect(log, ['release start', 'release end', 'fallback']);
      expect(clock.pendingCount, 0);
    },
  );

  test(
    'timeoutOrElse propagates cleanup defects without invoking fallback',
    () async {
      final error = StateError('cleanup');
      var calls = 0;
      final gate = _Gate<int, String>(onCancel: () => throw error);
      final running = runtime.run(
        gate.task.timeoutOrElse(deadline, () {
          calls++;
          return Task.succeed(2);
        }),
      );
      clock.advance(deadline);
      expect((_cause(await running) as CauseDefect).error, same(error));
      expect(calls, 0);
    },
  );

  test('timeoutOrElse does not recover ordinary failures or defects', () async {
    final error = StateError('body');
    var calls = 0;
    for (final cause in <Cause<String>>[
      const Cause.failure('business'),
      Cause.defect(error, StackTrace.current),
    ]) {
      final result = await runtime.run(
        Task.failCause(cause).timeoutOrElse(deadline, () {
          calls++;
          return Task.fail('fallback');
        }),
      );
      switch (_cause(result)) {
        case CauseFailure(:final error):
          expect(error, 'business');
        case CauseDefect(error: final defect):
          expect(defect, same(error));
        default:
          fail('Expected the original failure.');
      }
      expect(clock.pendingCount, 0);
    }
    expect(calls, 0);
  });

  test(
    'fallback exceptions become defects and typed fallback errors stay typed',
    () async {
      final error = StateError('fallback');
      for (final throws in [false, true]) {
        final gate = _Gate<int, String>();
        final running = runtime.run(
          gate.task.timeoutOrElse(deadline, () {
            if (throws) throw error;
            return Task.fail('fallback');
          }),
        );
        clock.advance(deadline);
        final cause = _cause(await running);
        if (throws) {
          expect((cause as CauseDefect).error, same(error));
        } else {
          expect((cause as CauseFailure<String>).error, 'fallback');
        }
      }
    },
  );

  test(
    'owner cancellation cancels the deadline and never chooses a fallback',
    () async {
      final controller = CancellationController();
      final cleanup = Completer<void>();
      addTearDown(() {
        if (!cleanup.isCompleted) cleanup.complete();
      });
      final error = StateError('cleanup');
      var calls = 0;
      final gate = _Gate<int, String>(
        onCancel: () async {
          await cleanup.future;
          throw error;
        },
      );
      var done = false;
      final running = runtime
          .run(
            gate.task.timeoutOrElse(deadline, () {
              calls++;
              return Task.succeed(2);
            }),
            signal: controller.signal,
          )
          .then((exit) {
            done = true;
            return exit;
          });
      controller.requestCancel(reason: 'owner');
      await _turn();
      expect(clock.pendingCount, 0);
      expect(done, isFalse);
      cleanup.complete();
      final reasons = _reasons(_cause(await running));
      expect(reasons.whereType<CauseCancelled>().map((cause) => cause.reason), [
        'owner',
      ]);
      expect(reasons.whereType<CauseDefect>().map((cause) => cause.error), [
        error,
      ]);
      expect(calls, 0);
    },
  );

  test(
    'owner cancellation during expired operation cleanup skips onTimeout',
    () async {
      final controller = CancellationController();
      final cleanup = Completer<void>();
      addTearDown(() {
        if (!cleanup.isCompleted) cleanup.complete();
      });
      var calls = 0;
      final gate = _Gate<int, String>(onCancel: () => cleanup.future);
      final running = runtime.run(
        gate.task.timeout(
          deadline,
          onTimeout: () {
            calls++;
            return 'timeout';
          },
        ),
        signal: controller.signal,
      );
      clock.advance(deadline);
      await _turn();
      controller.requestCancel(reason: 'owner');
      cleanup.complete();
      expect((_cause(await running) as CauseCancelled).reason, 'owner');
      expect(calls, 0);
    },
  );

  test('timeouts use the locally provided clock', () async {
    final localClock = TestClock();
    final running = runtime.run(
      _Gate<int, String>().task
          .timeout(deadline, onTimeout: () => 'timeout')
          .provideContext(Context.from([Clock.key.bind(localClock)])),
    );
    expect(clock.pendingCount, 0);
    expect(localClock.pendingCount, 1);
    localClock.advance(deadline);
    expect((_cause(await running) as CauseFailure<String>).error, 'timeout');
  });

  test('nonpositive deadlines remain asynchronous clock timers', () async {
    for (final duration in [Duration.zero, const Duration(seconds: -1)]) {
      final gate = _Gate<int, String>();
      final running = runtime.run(
        gate.task.timeout(duration, onTimeout: () => 'timeout'),
      );
      expect(gate.cancelled, isFalse);
      expect(clock.pendingCount, 1);
      clock.advance(Duration.zero);
      expect((_cause(await running) as CauseFailure<String>).error, 'timeout');
      expect(gate.cancelled, isTrue);
    }
    expect(
      _value(
        await runtime.run(
          Task.succeed(1)
              .withError<String>()
              .timeout(Duration.zero, onTimeout: () => 'timeout'),
        ),
      ),
      1,
    );
  });

  test(
    'repeated and concurrent executions use independent deadlines',
    () async {
      var calls = 0;
      final task = Task.fromCallback<int, String>((_) => () {})
          .timeout(deadline, onTimeout: () => 'timeout ${++calls}');
      final first = runtime.run(task);
      final second = runtime.run(task);
      expect(clock.pendingCount, 2);
      clock.advance(deadline);
      expect((_cause(await first) as CauseFailure<String>).error, 'timeout 1');
      expect((_cause(await second) as CauseFailure<String>).error, 'timeout 2');
      final third = runtime.run(task);
      expect(clock.pendingCount, 1);
      clock.advance(deadline);
      expect((_cause(await third) as CauseFailure<String>).error, 'timeout 3');
    },
  );

  test(
    'timer cancellation errors cannot be hidden by operation success',
    () async {
      final error = StateError('timer cancel');
      final faulty = _CancelErrorClock(clock, error);
      var calls = 0;
      final result = await runtime.run(
        Task.succeed(1)
            .withError<String>()
            .timeout(
              deadline,
              onTimeout: () {
                calls++;
                return 'timeout';
              },
            )
            .provideContext(Context.from([Clock.key.bind(faulty)])),
      );
      expect((_cause(result) as CauseDefect).error, same(error));
      expect(calls, 0);
      expect(clock.pendingCount, 0);
    },
  );

  test('nullable values and timeout errors are preserved', () async {
    expect(
      _value(
        await runtime.run(
          Task.succeed<int?>(null)
              .withError<String?>()
              .timeout(deadline, onTimeout: () => null),
        ),
      ),
      isNull,
    );
    final running = runtime.run(
      _Gate<int?, String?>().task.timeout(deadline, onTimeout: () => null),
    );
    clock.advance(deadline);
    expect((_cause(await running) as CauseFailure<String?>).error, isNull);
  });

  test(
    'fallback resources remain available to the calling task scope',
    () async {
      final log = <String>[];
      final gate = _Gate<int, String>(
        onCancel: () => log.add('operation released'),
      );
      final running = runtime.run(
        gate.task
            .timeoutOrElse(deadline, () {
              return Task.acquireRelease(
                Task.sync(() {
                  log.add('fallback acquired');
                  return 2;
                }).withError<String>(),
                release: (_, _) =>
                    Task.sync(() => log.add('fallback released')),
              );
            })
            .tap(
              (value) => Task.sync(() {
                expect(value, 2);
                log.add('fallback used');
              }),
            ),
      );
      clock.advance(deadline);
      expect(_value(await running), 2);
      expect(log, [
        'operation released',
        'fallback acquired',
        'fallback used',
        'fallback released',
      ]);
    },
  );

  test(
    'timeout preserves an occurrence source shared with another join',
    () async {
      final error = StateError('shared cleanup');
      final gate = _Gate<int, String>();
      final running = runtime.run(
        gate.task
            .ensuring(Task.sync(() => throw error))
            .fork()
            .withError<String>()
            .flatMap((fiber) {
              return Task.all([
                fiber.join().timeout(deadline, onTimeout: () => 'timeout'),
                fiber.join(),
              ], concurrency: 2);
            }),
      );
      gate.succeed(1);
      final reasons = _reasons(_cause(await running));
      expect(reasons.whereType<CauseDefect>().map((cause) => cause.error), [
        error,
      ]);
      expect(clock.pendingCount, 0);
    },
  );
}

final class _Gate<T, E> {
  _Gate({this.onCancel});
  final FutureOr<void> Function()? onCancel;
  late void Function(Exit<T, E>) complete;
  var cancelled = false;

  Task<T, E> get task => Task.fromCallback((complete) {
    this.complete = complete;
    return () {
      cancelled = true;
      return onCancel?.call();
    };
  });

  void succeed(T value) => complete(Exit.success(value));
}

final class _CancelErrorClock implements Clock {
  _CancelErrorClock(this.clock, this.error);
  final Clock clock;
  final Object error;

  @override
  Duration get elapsed => clock.elapsed;
  @override
  DateTime now() => clock.now();
  @override
  void Function() schedule(Duration delay, void Function() callback) {
    final cancel = clock.schedule(delay, callback);
    return () {
      cancel();
      throw error;
    };
  }
}

Task<void, Never> _future(Future<void> future) => Task.fromFuture<void, Never>(
  () => future,
  onError: (error, stackTrace) => Error.throwWithStackTrace(error, stackTrace),
);
T _value<T, E>(Exit<T, E> exit) => (exit as ExitSuccess<T, E>).value;
Cause<E> _cause<T, E>(Exit<T, E> exit) => (exit as ExitFailure<T, E>).cause;
List<Cause<E>> _reasons<E>(Cause<E> cause) => switch (cause) {
  CauseGroup(:final causes) => causes,
  _ => [cause],
};
Future<void> _turn() => Future<void>.delayed(Duration.zero);
