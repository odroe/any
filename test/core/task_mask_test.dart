import 'dart:async';

import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  group('Task.uninterruptibleMask', () {
    for (final concurrency in [1, 2]) {
      test('all preserves the mask with concurrency $concurrency', () async {
        final controller = CancellationController();
        final events = <String>[];
        final task = Task.uninterruptibleMask<void, Never>((_) {
          return Task.all([
            Task.sync(() {
              events.add('first');
              controller.requestCancel(reason: 'owner');
              return 1;
            }).map((value) {
              events.add('first continuation');
              return value;
            }),
            Task.sync(() {
              events.add('second');
              return 2;
            }),
          ], concurrency: concurrency).map((values) {
            expect(values, [1, 2]);
            events.add('collected');
          });
        });

        expect(
          _cancelReason(await _run(task, signal: controller.signal)),
          'owner',
        );
        expect(events, ['first', 'first continuation', 'second', 'collected']);
      });

      test(
        'all preserves worker restore with concurrency $concurrency',
        () async {
          final controller = CancellationController();
          var protected = false;
          final task = Task.uninterruptibleMask<List<int>, Never>((restore) {
            controller.requestCancel(reason: 'restored worker');
            return Task.all([
              Task.sync(() {
                protected = true;
                return 1;
              }),
              restore(
                Task.sync<int>(() => fail('Restored work must not start')),
              ),
            ], concurrency: concurrency);
          });

          final cause = _cause(await _run(task, signal: controller.signal));
          final causes = switch (cause) {
            CauseGroup(:final causes) => causes,
            _ => [cause],
          };
          expect(causes, isNotEmpty);
          expect(
            causes,
            everyElement(
              isA<CauseCancelled>().having(
                (cause) => cause.reason,
                'reason',
                'restored worker',
              ),
            ),
          );
          expect(protected, isTrue);
        },
      );
    }

    test('nested all and nested restore retain the inherited mask', () async {
      final controller = CancellationController();
      var collected = false;
      final task = Task.uninterruptibleMask<void, Never>((_) {
        return Task.all([
          Task.uninterruptibleMask<List<int>, Never>((restore) {
            controller.requestCancel(reason: 'nested all');
            return restore(
              Task.all([Task.succeed(1), Task.succeed(2)], concurrency: 2),
            );
          }),
        ]).map((values) {
          expect(values, [
            [1, 2],
          ]);
          collected = true;
        });
      });

      expect(
        _cancelReason(await _run(task, signal: controller.signal)),
        'nested all',
      );
      expect(collected, isTrue);
    });

    test('all collects after cancellation during worker cleanup', () async {
      final controller = CancellationController();
      final cleaning = Completer<void>();
      final release = Completer<void>();
      final events = <String>[];
      final worker = Task.addFinalizer((_) {
        return Task.fromFuture<void, Never>(() async {
          events.add('cleanup started');
          cleaning.complete();
          await release.future;
          events.add('cleanup finished');
        }, onError: Error.throwWithStackTrace);
      }).flatMap((_) => Task.succeed(1));
      final task = Task.uninterruptibleMask<void, Never>((_) {
        return Task.all([worker.scoped(), Task.succeed(2)], concurrency: 2).map(
          (values) {
            expect(values, [1, 2]);
            events.add('collected');
          },
        );
      }).map((_) => fail('Cancellation must be restored outside the mask'));

      final running = _run(task, signal: controller.signal);
      await cleaning.future;
      controller.requestCancel(reason: 'during cleanup');
      release.complete();

      expect(_cancelReason(await running), 'during cleanup');
      expect(events, ['cleanup started', 'cleanup finished', 'collected']);
    });

    test('all recovers a worker Future error after cancellation', () async {
      final controller = CancellationController();
      final started = Completer<void>();
      final pending = Completer<int>();
      final events = <String>[];
      final error = StateError('worker');
      final stack = StackTrace.fromString('worker Future stack');
      final worker =
          Task.fromFuture<int, String>(
            () {
              started.complete();
              return pending.future;
            },
            onError: (caught, caughtStack) {
              expect(caught, same(error));
              expect(caughtStack, same(stack));
              events.add('onError');
              return 'failed';
            },
          ).recover((failure) {
            expect(failure, 'failed');
            events.add('recovered');
            return Task.succeed(1);
          });
      final task = Task.uninterruptibleMask<void, String>((_) {
        return Task.all([
          worker,
          Task.succeed(2).withError<String>(),
        ], concurrency: 2).map((values) {
          expect(values, [1, 2]);
          events.add('collected');
        });
      }).map((_) => fail('Cancellation must be restored outside the mask'));

      final running = _run(task, signal: controller.signal);
      await started.future;
      controller.requestCancel(reason: 'pending Future');
      pending.completeError(error, stack);

      expect(_cancelReason(await running), 'pending Future');
      expect(events, ['onError', 'recovered', 'collected']);
    });

    for (final operation in ['allSettled', 'zip', 'forEach']) {
      test('$operation inherits the enclosing mask', () async {
        final controller = CancellationController();
        final events = <String>[];
        final workers = [
          Task.sync(() {
            events.add('first');
            controller.requestCancel(reason: operation);
            return 1;
          }),
          Task.checkCancelled().map((_) {
            events.add('second');
            return 2;
          }),
        ];
        final task = Task.uninterruptibleMask<void, Never>((_) {
          final Task<List<int>, Never> collected = switch (operation) {
            'allSettled' => Task.allSettled(
              workers,
              concurrency: 2,
            ).map((exits) => exits.map(_value).toList()),
            'zip' => Task.zip(
              workers[0],
              workers[1],
              concurrency: 2,
            ).map((pair) => [pair.$1, pair.$2]),
            _ => Task.forEach(workers, (worker) => worker, concurrency: 2),
          };
          return collected.map((values) {
            expect(values, [1, 2]);
            events.add('collected');
          });
        });

        expect(
          _cancelReason(await _run(task, signal: controller.signal)),
          operation,
        );
        expect(events, ['first', 'second', 'collected']);
      });
    }

    for (final operation in ['race', 'firstSuccess', 'timeout']) {
      test('$operation cancels pending work inside a mask', () async {
        final started = Completer<void>();
        void Function(Exit<int, String>)? finish;
        var cancellations = 0;
        final pending = Task.fromCallback<int, String>((complete) {
          finish = complete;
          started.complete();
          return () {
            cancellations++;
          };
        });
        final winner = Task.fromFuture<void, Never>(
          () => started.future,
          onError: Error.throwWithStackTrace,
        ).map((_) => 1).withError<String>();
        final task = Task.uninterruptibleMask<int, String>((_) {
          return switch (operation) {
            'race' => Task.race([pending, winner]),
            'firstSuccess' => Task.firstSuccess([pending, winner]),
            _ => pending.timeout(Duration.zero, onTimeout: () => 'deadline'),
          };
        });

        final running = _run(task);
        try {
          final exit = await running.timeout(const Duration(seconds: 2));
          if (operation == 'timeout') {
            expect((_cause(exit) as CauseFailure<String>).error, 'deadline');
          } else {
            expect(_value(exit), 1);
          }
          expect(cancellations, 1);
        } finally {
          finish?.call(Exit.success(-1));
          await running;
        }
      });
    }

    test('constructs its body lazily on every execution', () async {
      var bodies = 0;
      var restoredRuns = 0;
      late Task<int, Never> restored;
      final task = Task.uninterruptibleMask<int, Never>((restore) {
        restored = restore(Task.sync(() => ++restoredRuns));
        return Task.succeed(++bodies);
      });

      expect(bodies, 0);
      expect(_value(await _run(task)), 1);
      expect(_value(await _run(task)), 2);
      expect(restoredRuns, 0);
      expect(_value(await _run(restored)), 1);
      expect(_value(await _run(restored)), 2);
    });

    test('pre-cancellation prevents the mask from starting', () async {
      final controller = CancellationController()
        ..requestCancel(reason: 'before');
      final task = Task.uninterruptibleMask<int, Never>(
        (_) => fail('The body must not start'),
      );

      expect(
        _cancelReason(await _run(task, signal: controller.signal)),
        'before',
      );
    });

    test('defers a request through the entire body', () async {
      final controller = CancellationController();
      final events = <String>[];
      final task = Task.uninterruptibleMask<void, Never>((_) {
        controller.requestCancel(reason: 'stop');
        return Task.checkCancelled().flatMap((_) {
          events.add('masked');
          return Task.yieldNow().map((_) => events.add('after yield'));
        });
      }).map((_) => fail('The outer callback must not run'));

      expect(
        _cancelReason(await _run(task, signal: controller.signal)),
        'stop',
      );
      expect(events, ['masked', 'after yield']);
    });

    test(
      'restore observes a pending request before running its task',
      () async {
        final controller = CancellationController();
        final task = Task.uninterruptibleMask<int, Never>((restore) {
          controller.requestCancel(reason: 'restored');
          return restore(Task.sync(() => fail('Must not start')));
        });

        expect(
          _cancelReason(await _run(task, signal: controller.signal)),
          'restored',
        );
      },
    );

    test('a nested restore preserves already disabled cancellation', () async {
      final controller = CancellationController();
      final events = <String>[];
      final task = Task.uninterruptibleMask<void, Never>((_) {
        return Task.uninterruptibleMask<void, Never>((restore) {
          controller.requestCancel(reason: 'nested');
          return restore(Task.checkCancelled()).map((_) {
            events.add('inner restore');
          });
        }).map((_) => events.add('outer body'));
      });

      expect(
        _cancelReason(await _run(task, signal: controller.signal)),
        'nested',
      );
      expect(events, ['inner restore', 'outer body']);
    });

    test(
      'the outer restore can enable cancellation in a nested mask',
      () async {
        final controller = CancellationController();
        final task = Task.uninterruptibleMask<int, Never>((outerRestore) {
          return Task.uninterruptibleMask<int, Never>((_) {
            controller.requestCancel(reason: 'outer restore');
            return outerRestore(Task.sync(() => fail('Must not start')));
          });
        });

        expect(
          _cancelReason(await _run(task, signal: controller.signal)),
          'outer restore',
        );
      },
    );

    test('leaving restore reinstates the enclosing mask', () async {
      final controller = CancellationController();
      final events = <String>[];
      final task = Task.uninterruptibleMask<void, Never>((restore) {
        return restore(Task.succeed(1))
            .map((_) {
              controller.requestCancel(reason: 'after restore');
            })
            .flatMap((_) => Task.sync(() => events.add('still masked')));
      });

      expect(
        _cancelReason(await _run(task, signal: controller.signal)),
        'after restore',
      );
      expect(events, ['still masked']);
    });

    test('leaving a mask restores cancellation after success', () async {
      final controller = CancellationController();
      final task = Task.uninterruptibleMask<int, Never>((_) => Task.succeed(1))
          .map((_) => controller.requestCancel(reason: 'outside'))
          .map((_) => fail('Cancellation must be enabled again'));

      expect(
        _cancelReason(await _run(task, signal: controller.signal)),
        'outside',
      );
    });

    test(
      'body exceptions restore cancellation and retain their stack',
      () async {
        final controller = CancellationController();
        final error = StateError('body');
        final stack = StackTrace.fromString('mask body stack');
        final task = Task.uninterruptibleMask<int, Never>((_) {
          controller.requestCancel(reason: 'body failed');
          Error.throwWithStackTrace(error, stack);
        }).recoverCause<Never>((_) => fail('Outer recovery must not run'));
        final cause = _cause(
          await _run(task, signal: controller.signal),
        ) as CauseGroup<Never>;

        expect(cause.causes.length, 2);
        final defect = cause.causes.first as CauseDefect;
        expect(defect.error, same(error));
        expect(defect.stackTrace.toString(), stack.toString());
        expect((cause.causes.last as CauseCancelled).reason, 'body failed');
      },
    );

    test('failure in restore returns to the enclosing mask', () async {
      final controller = CancellationController();
      var recovered = false;
      final task = Task.uninterruptibleMask<int, String>((restore) {
        return restore<int, String>(
          Task.failCause(
            Cause.combine([
              const Cause.failure('expected'),
              const Cause.cancelled('recorded'),
              const Cause.failure('later'),
            ]),
          ),
        ).recover((error) {
          controller.requestCancel(reason: error);
          return Task.sync(() {
            recovered = true;
            return 42;
          });
        });
      });

      expect(
        _cancelReason(await _run(task, signal: controller.signal)),
        'expected',
      );
      expect(recovered, isTrue);
    });

    test(
      'masked synchronous error conversion ignores pending requests',
      () async {
        final controller = CancellationController();
        final error = StateError('native');
        final events = <String>[];
        final task = Task.uninterruptibleMask<int, String>((_) {
          return Task.trySync<int, int>(
                () {
                  controller.requestCancel(reason: 'sync');
                  throw error;
                },
                onError: (caught, _) {
                  expect(caught, same(error));
                  events.add('onError');
                  return 1;
                },
              )
              .mapError((error) {
                events.add('mapError');
                return '$error';
              })
              .recover(
                (_) => Task.sync(() {
                  events.add('recover');
                  return 42;
                }),
              );
        });

        expect(
          _cancelReason(await _run(task, signal: controller.signal)),
          'sync',
        );
        expect(events, ['onError', 'mapError', 'recover']);
      },
    );

    test('masked group error mapping visits every failure', () async {
      final controller = CancellationController();
      final mapped = <int>[];
      final task = Task.uninterruptibleMask<Never, String>((_) {
        return Task.failCause(
          Cause.combine([
            const Cause.failure(1),
            const Cause.failure(2),
            const Cause.failure(3),
          ]),
        ).mapError((error) {
          controller.requestCancel(reason: 'mapping');
          mapped.add(error);
          return '$error';
        });
      });

      expect(
        _cancelReason(await _run(task, signal: controller.signal)),
        'mapping',
      );
      expect(mapped, [1, 2, 3]);
    });

    test('a masked Future still maps and recovers its error', () async {
      final controller = CancellationController();
      final pending = Completer<int>();
      final error = StateError('future');
      final stack = StackTrace.fromString('masked future stack');
      final events = <String>[];
      final task = Task.uninterruptibleMask<int, String>((_) {
        return Task.fromFuture<int, int>(
              () => pending.future,
              onError: (caught, trace) {
                expect(caught, same(error));
                expect(trace.toString(), stack.toString());
                events.add('onError');
                return 1;
              },
            )
            .mapError((error) {
              events.add('mapError');
              return '$error';
            })
            .recover(
              (_) => Task.sync(() {
                events.add('recover');
                return 42;
              }),
            );
      });
      final running = _run(task, signal: controller.signal);
      controller.requestCancel(reason: 'future');
      pending.completeError(error, stack);

      expect(_cancelReason(await running), 'future');
      expect(events, ['onError', 'mapError', 'recover']);
    });

    test('a restored Future retains its late defect without mapping', () async {
      final controller = CancellationController();
      final pending = Completer<int>();
      final error = StateError('restored Future');
      final stack = StackTrace.fromString('restored future stack');
      final task = Task.uninterruptibleMask<int, String>((restore) {
        return restore(
          Task.fromFuture<int, String>(
            () => pending.future,
            onError: (_, _) => fail('Must not map after cancellation'),
          ),
        );
      });
      final running = _run(task, signal: controller.signal);
      controller.requestCancel(reason: 'restored Future');
      pending.completeError(error, stack);
      final cause = _cause(await running) as CauseGroup<String>;

      expect(cause.causes.length, 2);
      final defect = cause.causes.first as CauseDefect;
      expect(defect.error, same(error));
      expect(defect.stackTrace.toString(), stack.toString());
      expect((cause.causes.last as CauseCancelled).reason, 'restored Future');
    });

    test('a masked callback waits for completion without cancelling', () async {
      final controller = CancellationController();
      late void Function(Exit<int, String>) complete;
      var cancellations = 0;
      var mapped = false;
      var finished = false;
      final task = Task.uninterruptibleMask<int, String>((_) {
        return Task.fromCallback<int, String>((callback) {
          complete = callback;
          return () {
            cancellations++;
          };
        }).map((value) {
          mapped = true;
          return value;
        });
      });
      final running = _run(task, signal: controller.signal)
        ..then((_) => finished = true);
      controller.requestCancel(reason: 'callback');
      await Future<void>.delayed(Duration.zero);

      expect(finished, isFalse);
      expect(cancellations, 0);
      complete(const Exit.success(42));
      expect(_cancelReason(await running), 'callback');
      expect(mapped, isTrue);
      expect(cancellations, 0);
    });

    test('a restored callback waits for cancellation cleanup', () async {
      final controller = CancellationController();
      final cleaned = Completer<void>();
      var cancellations = 0;
      var finished = false;
      final task = Task.uninterruptibleMask<int, String>((restore) {
        return restore(
          Task.fromCallback<int, String>((_) {
            return () {
              cancellations++;
              return cleaned.future;
            };
          }),
        );
      });
      final running = _run(task, signal: controller.signal)
        ..then((_) => finished = true);
      controller.requestCancel(reason: 'restored callback');
      await Future<void>.delayed(Duration.zero);

      expect(finished, isFalse);
      expect(cancellations, 1);
      cleaned.complete();
      expect(_cancelReason(await running), 'restored callback');
    });

    test(
      'masked callback defects are retained before pending cancellation',
      () async {
        final controller = CancellationController();
        final error = StateError('callback');
        final task = Task.uninterruptibleMask<int, Never>((_) {
          return Task.fromCallback<int, Never>((_) {
            controller.requestCancel(reason: 'callback failed');
            throw error;
          });
        });
        final cause = _cause(
          await _run(task, signal: controller.signal),
        ) as CauseGroup<Never>;

        expect(cause.causes.length, 2);
        expect((cause.causes.first as CauseDefect).error, same(error));
        expect((cause.causes.last as CauseCancelled).reason, 'callback failed');
      },
    );

    test('captured restore uses the signal of its new execution', () async {
      late RestoreCancellation captured;
      await _run(
        Task.uninterruptibleMask<void, Never>((_) {
          return Task.uninterruptibleMask<void, Never>((restore) {
            captured = restore;
            return Task.succeed(null);
          });
        }),
      );
      final controller = CancellationController();
      var continued = false;
      final task = captured(
        Task.sync(() {
          controller.requestCancel(reason: 'new execution');
        }).map((_) => continued = true),
      );

      expect(
        _cancelReason(await _run(task, signal: controller.signal)),
        'new execution',
      );
      expect(continued, isTrue);
    });

    test(
      'captured enabled restore observes a different execution signal',
      () async {
        final original = CancellationController();
        late RestoreCancellation captured;
        await _run(
          Task.uninterruptibleMask<void, Never>((restore) {
            captured = restore;
            return Task.succeed(null);
          }),
          signal: original.signal,
        );
        final current = CancellationController();
        final task = Task.uninterruptibleMask<int, Never>((_) {
          current.requestCancel(reason: 'current signal');
          return captured(Task.sync(() => fail('Must use the current signal')));
        });

        expect(
          _cancelReason(await _run(task, signal: current.signal)),
          'current signal',
        );
        expect(original.signal.isCancelled, isFalse);
      },
    );

    test('concurrent executions do not share cancellation state', () async {
      final firstController = CancellationController();
      final pending = [Completer<int>(), Completer<int>()];
      var next = 0;
      final task = Task.uninterruptibleMask<int, Never>((_) {
        final index = next++;
        return Task.fromFuture(
          () => pending[index].future,
          onError: (_, _) => fail('Unexpected native failure'),
        );
      });
      final first = _run(task, signal: firstController.signal);
      final second = _run(task);
      firstController.requestCancel(reason: 'first');
      pending[1].complete(2);
      pending[0].complete(1);

      expect(_cancelReason(await first), 'first');
      expect(_value(await second), 2);
    });

    test('deep mask and restore nesting is stack safe', () async {
      Task<int, Never> task = Task.succeed(0);
      for (var index = 0; index < 10000; index++) {
        final previous = task;
        task = Task.uninterruptibleMask((restore) {
          return restore(previous).map((value) => value + 1);
        });
      }

      expect(_value(await _run(task)), 10000);
    });

    test('long masked chains yield while deferring cancellation', () async {
      final controller = CancellationController();
      var callbacks = 0;
      Task<int, Never> body = Task.succeed(0);
      for (var index = 0; index < 10000; index++) {
        body = body.map((value) {
          callbacks++;
          return value + 1;
        });
      }
      Timer.run(() => controller.requestCancel(reason: 'timer'));
      final task = Task.uninterruptibleMask((_) => body);

      expect(
        _cancelReason(await _run(task, signal: controller.signal)),
        'timer',
      );
      expect(callbacks, 10000);
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

Object? _cancelReason<T, E>(Exit<T, E> exit) =>
    (_cause(exit) as CauseCancelled).reason;
