import 'dart:async';

import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  late Runtime runtime;
  late List<Cause<Object?>> reports;

  setUp(() {
    reports = [];
    runtime = Runtime(onUnhandledError: reports.add);
  });
  tearDown(() async {
    await runtime.close();
  });

  test('poll waits for both work and resource cleanup', () async {
    late void Function(Exit<int?, Never>) complete;
    final cleaned = Completer<void>();
    addTearDown(() {
      if (!cleaned.isCompleted) cleaned.complete();
    });
    final fiber = runtime.start(
      Task.fromCallback<int?, Never>((callback) {
        complete = callback;
        return () {};
      }).ensuring(_future(cleaned.future)),
    );

    expect(fiber.poll(), isA<OptionNone<Exit<int?, Never>>>());
    complete(const Exit.success(null));
    await _turn();
    expect(fiber.poll(), isA<OptionNone<Exit<int?, Never>>>());
    cleaned.complete();
    final exit = _value(await runtime.run(fiber.awaitExit()));
    expect(_value(exit), isNull);
    expect((fiber.poll() as OptionSome<Exit<int?, Never>>).value, same(exit));
    fiber.requestCancel(reason: 'too late');
    expect((fiber.poll() as OptionSome<Exit<int?, Never>>).value, same(exit));
  });

  test('multiple and repeated waits share the complete result', () async {
    late void Function(Exit<int, Never>) complete;
    final fiber = runtime.start(
      Task.fromCallback<int, Never>((callback) {
        complete = callback;
        return () {};
      }),
    );
    final first = runtime.run(fiber.awaitExit());
    final second = runtime.run(fiber.awaitExit());
    complete(const Exit.success(42));

    expect(_value(await first), same(_value(await second)));
    expect(_value(await runtime.run(fiber.join())), 42);
    expect(_value(await runtime.run(fiber.join())), 42);
  });

  test('widened fibers can wait without narrowing callback types', () async {
    late void Function(Exit<int, String>) complete;
    final Fiber<Object?, Object?> fiber = runtime.start(
      Task.fromCallback<int, String>((callback) {
        complete = callback;
        return () {};
      }),
    );
    final waiting = runtime.run(fiber.join());
    complete(const Exit.success(42));

    expect(_value(await waiting), 42);
    expect(_value(_value(await runtime.run(fiber.awaitExit()))), 42);
  });

  test(
    'joining propagates the full typed cause without closing duplicates',
    () async {
      final defect = Cause<String>.defect(Object(), StackTrace.current);
      final child = Task.failCause(
        Cause.combine([const Cause.failure('business'), defect]),
      );
      final exit = await runtime.run(
        child.fork().withError<String>().flatMap((fiber) => fiber.join()),
      );
      final cause = _cause(exit) as CauseGroup<String>;

      expect(cause.causes.length, 2);
      expect((cause.causes.first as CauseFailure<String>).error, 'business');
      expect(cause.causes.last, same(defect));
      await _reportsTurn();
      expect(reports, isEmpty);
    },
  );

  for (final holdClosingFiber in [false, true]) {
    test('joining a child does not repeat its closing scope defect '
        '(closing fiber held: $holdClosingFiber)', () async {
      final error = StateError('child cleanup');
      final gate = Completer<void>();
      final holding = Completer<void>();
      addTearDown(() {
        if (!gate.isCompleted) gate.complete();
      });
      final child = Task.fromCallback<void, Never>(
        (_) =>
            () => throw error,
      );
      final running = runtime.run(
        Scope.open().flatMap((scope) {
          return child.forkIn(scope).flatMap((fiber) {
            var closing = scope.close(const Exit.success(null));
            if (holdClosingFiber) {
              closing = closing.ensuring(
                Task.defer(() {
                  holding.complete();
                  return _future(gate.future);
                }),
              );
            }
            return closing.fork().flatMap((_) => fiber.join());
          });
        }),
      );
      if (holdClosingFiber) {
        await holding.future;
        await _turn();
        gate.complete();
      }
      final exit = await running;
      final cause = _cause(exit);
      final reasons = switch (cause) {
        CauseGroup(:final causes) => causes,
        _ => [cause],
      };

      expect(
        reasons.whereType<CauseDefect>().where(
          (cause) => identical(cause.error, error),
        ),
        hasLength(1),
      );
    });
  }

  test('independent executions retain the same defect cause twice', () async {
    final defect = Cause<Never>.defect(Object(), StackTrace.current);
    final child = _pending<void>().ensuring(Task.failCause(defect));
    runtime.start(child);
    runtime.start(child);

    final cause = _cause(await runtime.close()) as CauseGroup<Never>;
    expect(cause.causes, [same(defect), same(defect)]);
  });

  test(
    'capturing an exit does not give later cleanup its old source',
    () async {
      final defect = Cause<Never>.defect(Object(), StackTrace.current);
      final cleanup = Task.failCause(defect);
      final original = runtime.start(_pending<void>().ensuring(cleanup));
      runtime.start(
        Task.uninterruptibleMask((_) => original.awaitExit().map((_) {}))
            .ensuring(cleanup),
      );

      final cause = _cause(await runtime.close()) as CauseGroup<Never>;
      expect(cause.causes, [same(defect), same(defect)]);
    },
  );

  test('explicit repeated scope closes return the same full failure', () async {
    final error = StateError('cleanup');
    final child = Task.fromCallback<void, Never>(
      (_) =>
          () => throw error,
    );
    final exits = _value(
      await runtime.run(
        Scope.open().flatMap(
          (scope) => child
              .forkIn(scope)
              .flatMap(
                (_) => scope
                    .close(const Exit.success(null))
                    .exit()
                    .flatMap(
                      (first) => scope
                          .close(const Exit.success(null))
                          .exit()
                          .map((second) => (first, second)),
                    ),
              ),
        ),
      ),
    );

    expect((_cause(exits.$1) as CauseDefect).error, same(error));
    expect(_cause(exits.$2), same(_cause(exits.$1)));
  });

  for (final capture in <String, Task<void, Never> Function(Task<void, Never>)>{
    'exit': (task) => task.exit().map((_) {}),
    'recoverCause': (task) => task.recoverCause((_) => Task.succeed(null)),
  }.entries) {
    test('${capture.key} ends the source before a new failure', () async {
      final defect = Cause<Never>.defect(Object(), StackTrace.current);
      final original = runtime.start(
        _pending<void>().ensuring(Task.failCause(defect)),
      );
      runtime.start(
        Task.uninterruptibleMask(
          (_) => capture
              .value(original.join())
              .flatMap((_) => Task.failCause(defect)),
        ),
      );

      final cause = _cause(await runtime.close()) as CauseGroup<Never>;
      expect(cause.causes, [same(defect), same(defect)]);
    });
  }

  test(
    'cause transformations preserve the current joined occurrence',
    () async {
      final defect = Cause<Never>.defect(Object(), StackTrace.current);
      final original = runtime.start(
        _pending<void>().ensuring(Task.failCause(defect)),
      );
      runtime.start(
        Task.uninterruptibleMask(
          (_) => original
              .join()
              .mapError<String>((error) => '$error')
              .orDie()
              .mapCause((cause) => Cause.combine([cause]))
              .scoped(),
        ),
      );

      expect(_cause(await runtime.close()), same(defect));
    },
  );

  test(
    'a cleanup joining a sibling forwards the original occurrence',
    () async {
      final defect = Cause<Never>.defect(Object(), StackTrace.current);
      final original = runtime.start(
        _pending<void>().ensuring(Task.failCause(defect)),
      );
      runtime.start(_pending<void>().ensuring(original.join()));

      expect(_cause(await runtime.close()), same(defect));
    },
  );

  test('separate finalizers preserve repeated use of the same cause', () async {
    final defect = Cause<Never>.defect(Object(), StackTrace.current);
    final cleanup = Task.failCause(defect);
    runtime.start(_pending<void>().ensuring(cleanup).ensuring(cleanup));

    final cause = _cause(await runtime.close()) as CauseGroup<Never>;
    expect(cause.causes, [same(defect), same(defect)]);
  });

  test('a cancelled waiter neither cancels nor observes its target', () async {
    late void Function(Exit<int, String>) complete;
    var targetCancellations = 0;
    final fiber = runtime.start(
      Task.fromCallback<int, String>((callback) {
        complete = callback;
        return () {
          targetCancellations++;
        };
      }),
    );
    final controller = CancellationController();
    final waiting = runtime.run(fiber.awaitExit(), signal: controller.signal);
    controller.requestCancel(reason: 'wait only');

    expect((_cause(await waiting) as CauseCancelled).reason, 'wait only');
    expect(fiber.poll(), isA<OptionNone<Exit<int, String>>>());
    expect(targetCancellations, 0);
    complete(const Exit.failure(Cause.failure('unobserved')));
    await _reportsTurn();
    expect(reports, hasLength(1));
    expect((reports.single as CauseFailure<Object?>).error, 'unobserved');
    expect(targetCancellations, 0);
  });

  test(
    'cancellation during exit handoff leaves its defect unobserved',
    () async {
      late void Function(Exit<int, Never>) complete;
      final defect = Cause<Never>.defect(Object(), StackTrace.current);
      final target = runtime.start(
        Task.fromCallback<int, Never>((callback) {
          complete = callback;
          return () {};
        }),
      );
      final controller = CancellationController();
      var armed = false;
      var intercepted = false;
      var received = false;
      final waiting = runZoned(
        () => runtime.run(
          target.awaitExit().map((_) => received = true),
          signal: controller.signal,
        ),
        zoneSpecification: ZoneSpecification(
          scheduleMicrotask: (self, parent, zone, callback) {
            if (armed) {
              armed = false;
              intercepted = true;
              controller.requestCancel(reason: 'handoff');
            }
            parent.scheduleMicrotask(zone, callback);
          },
        ),
      );
      armed = true;
      complete(Exit.failure(defect));

      expect((_cause(await waiting) as CauseCancelled).reason, 'handoff');
      expect(intercepted, isTrue);
      expect(received, isFalse);
      await _reportsTurn();
      expect(reports, [same(defect)]);
    },
  );

  for (final completed in [false, true]) {
    test('cancellation at a yield before exit consumption preserves reporting '
        '(already completed: $completed)', () async {
      late void Function(Exit<int, Never>) complete;
      final defect = Cause<Never>.defect(Object(), StackTrace.current);
      final target = runtime.start(
        Task.fromCallback<int, Never>((callback) {
          complete = callback;
          return () {};
        }),
      );
      if (completed) {
        complete(Exit.failure(defect));
        await _turn();
        expect(reports, isEmpty);
      }
      var task = target.awaitExit();
      final context = Context.from(const []);
      // Put the completed wait immediately before the executor's cooperative
      // yield, with only environment restoration before the consuming callback.
      for (var i = 0; i < 2045; i++) {
        task = task.provideContext(context);
      }
      final controller = CancellationController();
      var intercepted = false;
      var received = false;
      final waiting = runZoned(
        () => runtime.run(
          task.map((_) => received = true),
          signal: controller.signal,
        ),
        zoneSpecification: ZoneSpecification(
          createTimer: (self, parent, zone, duration, callback) {
            if (!intercepted && duration == Duration.zero) {
              intercepted = true;
              controller.requestCancel(reason: 'yield');
            }
            return parent.createTimer(zone, duration, callback);
          },
        ),
      );
      if (!completed) complete(Exit.failure(defect));

      expect((_cause(await waiting) as CauseCancelled).reason, 'yield');
      expect(intercepted, isTrue);
      expect(received, isFalse);
      await _reportsTurn();
      expect(reports, [same(defect)]);
    });
  }

  test('another waiter can observe an exit abandoned during handoff', () async {
    late void Function(Exit<int, Never>) complete;
    final defect = Cause<Never>.defect(Object(), StackTrace.current);
    final target = runtime.start(
      Task.fromCallback<int, Never>((callback) {
        complete = callback;
        return () {};
      }),
    );
    final controller = CancellationController();
    var armed = false;
    var received = false;
    final cancelled = runZoned(
      () => runtime.run(
        target.awaitExit().map((_) => received = true),
        signal: controller.signal,
      ),
      zoneSpecification: ZoneSpecification(
        scheduleMicrotask: (self, parent, zone, callback) {
          if (armed) {
            armed = false;
            controller.requestCancel();
          }
          parent.scheduleMicrotask(zone, callback);
        },
      ),
    );
    final observing = runtime.run(target.awaitExit());
    armed = true;
    complete(Exit.failure(defect));

    expect(_cause(await cancelled), isA<CauseCancelled>());
    expect(received, isFalse);
    expect(_cause(_value(await observing)), same(defect));
    await _reportsTurn();
    expect(reports, isEmpty);
  });

  test(
    'cancellation after exit consumption keeps the result observed',
    () async {
      final defect = Cause<Never>.defect(Object(), StackTrace.current);
      final target = runtime.start(Task.failCause(defect));
      final controller = CancellationController();
      final seen = <Exit<Never, Never>>[];
      final exit = await runtime.run(
        target.awaitExit().map((result) {
          seen.add(result);
          controller.requestCancel(reason: 'after consumption');
        }),
        signal: controller.signal,
      );

      expect(_cause(seen.single), same(defect));
      expect((_cause(exit) as CauseCancelled).reason, 'after consumption');
      await _reportsTurn();
      expect(reports, isEmpty);
    },
  );

  test(
    'cancellation during join handoff retains the adopted defect once',
    () async {
      late void Function(Exit<int, Never>) complete;
      final defect = Cause<Never>.defect(Object(), StackTrace.current);
      final target = runtime.start(
        Task.fromCallback<int, Never>((callback) {
          complete = callback;
          return () {};
        }),
      );
      final controller = CancellationController();
      var armed = false;
      final waiting = runZoned(
        () => runtime.run(target.join(), signal: controller.signal),
        zoneSpecification: ZoneSpecification(
          scheduleMicrotask: (self, parent, zone, callback) {
            if (armed) {
              armed = false;
              controller.requestCancel();
            }
            parent.scheduleMicrotask(zone, callback);
          },
        ),
      );
      armed = true;
      complete(Exit.failure(defect));

      final cause = _cause(await waiting) as CauseGroup<Never>;
      expect(cause.causes.whereType<CauseDefect>(), [same(defect)]);
      expect(cause.causes.whereType<CauseCancelled>(), hasLength(1));
      await _reportsTurn();
      expect(reports, isEmpty);
    },
  );

  for (final cancel in [false, true]) {
    test('direct exit delivery waits for execution cleanup '
        '(cancelled: $cancel)', () async {
      final defect = Cause<Never>.defect(Object(), StackTrace.current);
      final target = runtime.start(Task.failCause(defect));
      final controller = CancellationController();
      final cleaning = Completer<void>();
      final cleaned = Completer<void>();
      addTearDown(() {
        if (!cleaned.isCompleted) cleaned.complete();
      });
      final waiting = runtime.run(
        Task.addFinalizer((_) {
          cleaning.complete();
          return _future(cleaned.future);
        }).flatMap((_) => target.awaitExit()),
        signal: controller.signal,
      );
      await cleaning.future;
      if (cancel) controller.requestCancel(reason: 'cleanup');
      await _reportsTurn();
      expect(reports, isEmpty);
      cleaned.complete();

      final exit = await waiting;
      if (cancel) {
        expect((_cause(exit) as CauseCancelled).reason, 'cleanup');
      } else {
        expect(_cause(_value(exit)), same(defect));
      }
      await _reportsTurn();
      expect(reports, cancel ? [same(defect)] : isEmpty);
    });
  }

  for (final wrapper in ['scoped', 'exit', 'context', 'local', 'mask']) {
    test(
      '$wrapper does not consume an exit before cleanup completes',
      () async {
        final defect = Cause<Never>.defect(Object(), StackTrace.current);
        final target = runtime.start(Task.failCause(defect));
        final controller = CancellationController();
        final cleaning = Completer<void>();
        final cleaned = Completer<void>();
        addTearDown(() {
          if (!cleaned.isCompleted) cleaned.complete();
        });
        final source = Task.addFinalizer((_) {
          cleaning.complete();
          return _future(cleaned.future);
        }).flatMap((_) => target.awaitExit());
        final Task<Object?, Never> task = switch (wrapper) {
          'scoped' => source.scoped(),
          'exit' => source.exit(),
          'context' => source.provideContext(Context.from(const [])),
          'local' => TaskLocal(0).locally(1, source),
          _ => Task.uninterruptibleMask((restore) => restore(source)),
        };
        final waiting = runtime.run(task, signal: controller.signal);
        await cleaning.future;
        controller.requestCancel(reason: 'cleanup');
        await _reportsTurn();
        expect(reports, isEmpty);
        cleaned.complete();

        expect((_cause(await waiting) as CauseCancelled).reason, 'cleanup');
        await _reportsTurn();
        expect(reports, [same(defect)]);
      },
    );
  }

  for (final scoped in [false, true]) {
    test('directly returning a child exit excludes its owned cleanup defect '
        '(scoped: $scoped)', () async {
      final error = StateError('child cleanup');
      final cleaned = Completer<void>();
      final cleaning = Completer<void>();
      addTearDown(() {
        if (!cleaned.isCompleted) cleaned.complete();
      });
      final child = Task.fromCallback<void, Never>(
        (_) =>
            () => throw error,
      );
      var task = Scope.open().flatMap((scope) {
        return child.forkIn(scope).flatMap((fiber) {
          final closing = scope
              .close(const Exit.success(null))
              .ensuring(
                Task.defer(() {
                  cleaning.complete();
                  return _future(cleaned.future);
                }),
              );
          return closing.fork().flatMap((_) => fiber.awaitExit());
        });
      });
      if (scoped) task = task.scoped();
      final waiting = runtime.run(task);
      await cleaning.future;
      await _turn();
      cleaned.complete();
      final cause = _cause(_value(await waiting)) as CauseGroup<Never>;

      expect(cause.causes.whereType<CauseDefect>().single.error, same(error));
      expect(cause.causes.whereType<CauseCancelled>(), hasLength(1));
      await _reportsTurn();
      expect(reports, isEmpty);
    });
  }

  test(
    'recovering cleanup failure does not observe a discarded exit',
    () async {
      final defect = Cause<Never>.defect(Object(), StackTrace.current);
      final cleanup = Cause<Never>.defect(Object(), StackTrace.current);
      final target = runtime.start(Task.failCause(defect));
      final waiting = Task.addFinalizer((_) => Task.failCause(cleanup))
          .flatMap((_) => target.awaitExit())
          .scoped()
          .recoverCause((cause) {
            expect(cause, same(cleanup));
            return Task.succeed(
              const Exit<Never, Never>.failure(Cause.cancelled()),
            );
          });

      expect(_cause(_value(await runtime.run(waiting))), isA<CauseCancelled>());
      await _reportsTurn();
      expect(reports, [same(defect)]);
    },
  );

  for (final cancel in [false, true]) {
    test('a detached scope retains a pending exit through outer cleanup '
        '(cancelled: $cancel)', () async {
      final error = StateError('child cleanup');
      final child = Task.fromCallback<void, Never>(
        (_) =>
            () => throw error,
      );
      final controller = CancellationController();
      final cleaning = Completer<void>();
      final cleaned = Completer<void>();
      addTearDown(() {
        if (!cleaned.isCompleted) cleaned.complete();
      });
      final body = Scope.open().flatMap((scope) {
        return child.forkIn(scope).flatMap((fiber) {
          return scope
              .close(const Exit.success(null))
              .fork()
              .flatMap((_) => fiber.awaitExit());
        });
      }).scoped();
      final waiting = runtime.run(
        Task.addFinalizer((_) {
          cleaning.complete();
          return _future(cleaned.future);
        }).flatMap((_) => body),
        signal: controller.signal,
      );
      await cleaning.future;
      if (cancel) controller.requestCancel(reason: 'outer cleanup');
      cleaned.complete();
      final exit = await waiting;
      final cause =
          (cancel ? _cause(exit) : _cause(_value(exit))) as CauseGroup<Never>;

      expect(cause.causes.whereType<CauseDefect>().single.error, same(error));
      expect(cause.causes.whereType<CauseCancelled>(), hasLength(1));
      await _reportsTurn();
      expect(reports, isEmpty);
    });
  }

  for (final reuseCause in [false, true]) {
    test('abandoned cleanup preserves occurrence order '
        '(same cause reused: $reuseCause)', () async {
      final inner = Cause<Never>.defect(Object(), StackTrace.current);
      final outer = reuseCause
          ? inner
          : Cause<Never>.defect(Object(), StackTrace.current);
      final child = _pending<void>().ensuring(Task.failCause(inner));
      final body = Scope.open().flatMap((scope) {
        return child.forkIn(scope).flatMap((fiber) {
          return scope
              .close(const Exit.success(null))
              .fork()
              .flatMap((_) => fiber.awaitExit());
        });
      }).scoped();
      final exit = await runtime.run(
        Task.addFinalizer((_) => Task.failCause(outer)).flatMap((_) => body),
      );

      expect((_cause(exit) as CauseGroup<Never>).causes, [
        same(inner),
        same(outer),
      ]);
      await _reportsTurn();
      expect(reports, isEmpty);
    });
  }

  test(
    'masked workers can deliver captured exits after cancellation',
    () async {
      final controller = CancellationController();
      final error = StateError('child cleanup');
      final child = Task.fromCallback<void, Never>(
        (_) =>
            () => throw error,
      );
      final body = Scope.open().flatMap((scope) {
        return child.forkIn(scope).flatMap((fiber) {
          return scope
              .close(const Exit.success(null))
              .ensuring(Task.yieldNow())
              .fork()
              .flatMap((_) => fiber.awaitExit());
        });
      });
      final worker = Task.addFinalizer(
        (_) => Task.sync(() {
          controller.requestCancel(reason: 'masked cleanup');
        }),
      ).flatMap((_) => body);
      var received = false;
      final task = Task.uninterruptibleMask((_) {
        return Task.all([worker], concurrency: 2).map((exits) {
          final cause = _cause(exits.single) as CauseGroup<Never>;
          expect(
            cause.causes.whereType<CauseDefect>().single.error,
            same(error),
          );
          received = true;
        });
      });

      final exit = await runtime.run(task, signal: controller.signal);
      expect((_cause(exit) as CauseCancelled).reason, 'masked cleanup');
      expect(received, isTrue);
      await _reportsTurn();
      expect(reports, isEmpty);
    },
  );

  test('cancel is lazy and waits for cancellation cleanup', () async {
    var cancellations = 0;
    final cleaned = Completer<void>();
    addTearDown(() {
      if (!cleaned.isCompleted) cleaned.complete();
    });
    final fiber = runtime.start(
      Task.fromCallback<int, Never>((_) {
        return () {
          cancellations++;
          return cleaned.future;
        };
      }),
    );
    final cancel = fiber.cancel(reason: 'first');
    expect(cancellations, 0);
    final waiting = runtime.run(cancel);
    var finished = false;
    unawaited(waiting.then((_) => finished = true));
    fiber.requestCancel(reason: 'second');
    await _turn();

    expect(cancellations, 1);
    expect(finished, isFalse);
    cleaned.complete();
    final exit = _value(await waiting);
    expect((_cause(exit) as CauseCancelled).reason, 'first');
    expect(_value(await runtime.run(fiber.cancel())), same(exit));
  });

  test('a self-cancel wait fails before requesting cancellation', () async {
    late Fiber<int, Never> fiber;
    fiber = runtime.start(
      Task.yieldNow().flatMap((_) {
        return fiber.cancel().exit().map((exit) {
          expect((_cause(exit) as CauseDefect).error, isA<StateError>());
          return 42;
        });
      }),
    );

    expect(_value(await runtime.run(fiber.join())), 42);
  });

  test('a child cannot join the parent that owns it', () async {
    late Fiber<int, Never> parent;
    final child = Task.yieldNow().flatMap((_) {
      return parent.join().exit().map((exit) {
        expect((_cause(exit) as CauseDefect).error, isA<StateError>());
        return 42;
      });
    });
    parent = runtime.start(child.fork().flatMap((fiber) => fiber.join()));

    expect(_value(await runtime.run(parent.join())), 42);
  });

  test(
    'a fork retains the host dependency of nested runtime execution',
    () async {
      final inner = Runtime(onUnhandledError: reports.add);
      addTearDown(inner.close);
      final close = Task.fromFuture<Exit<void, Never>, Never>(
        runtime.close,
        onError: (error, stack) => Error.throwWithStackTrace(error, stack),
      );
      final exit = await runtime.run(
        Task.fromFuture<Exit<Exit<void, Never>, Never>, Never>(
          () => inner.run(close.fork().flatMap((fiber) => fiber.join())),
          onError: (error, stack) => Error.throwWithStackTrace(error, stack),
        ),
      );
      final closeExit = _value(_value(exit));

      expect((_cause(closeExit) as CauseDefect).error, isA<StateError>());
      expect(_value(await runtime.run(Task.succeed(42))), 42);
    },
  );

  test(
    'a lexical scope stops its children before releasing resources',
    () async {
      final events = <String>[];
      final child = Task.fromCallback<void, Never>((_) {
        events.add('child started');
        return () => events.add('child stopped');
      }).ensuring(Task.sync(() => events.add('child released')));
      final task = Task.addFinalizer(
        (_) => Task.sync(() => events.add('parent released')),
      ).flatMap((_) => child.fork()).scoped().map((_) => events.add('outside'));

      expect(await runtime.run(task), isA<ExitSuccess<void, Never>>());
      expect(events, [
        'child started',
        'child stopped',
        'child released',
        'parent released',
        'outside',
      ]);
    },
  );

  test(
    'forkIn survives its creator and remains owned by the chosen scope',
    () async {
      final scope = await _heldScope(runtime);
      late void Function(Exit<int, Never>) complete;
      var cancellations = 0;
      final fiber = _value(
        await runtime.run(
          Task.fromCallback<int, Never>((callback) {
            complete = callback;
            return () {
              cancellations++;
            };
          }).forkIn(scope),
        ),
      );

      expect(fiber.poll(), isA<OptionNone<Exit<int, Never>>>());
      expect(cancellations, 0);
      complete(const Exit.success(42));
      expect(_value(await runtime.run(fiber.join())), 42);
      expect(cancellations, 0);
    },
  );

  test(
    'forkIn can join its still running creator when owned elsewhere',
    () async {
      final scope = await _heldScope(runtime);
      late Fiber<int, Never> creator;
      final childReady = Completer<Fiber<int, Never>>();
      final creatorGate = Completer<int>();
      addTearDown(() {
        if (!creatorGate.isCompleted) creatorGate.complete(42);
      });
      final child = Task.yieldNow().flatMap((_) => creator.join());
      creator = runtime.start(
        child.forkIn(scope).flatMap((fiber) {
          childReady.complete(fiber);
          return _future(creatorGate.future);
        }),
      );
      final fiber = await childReady.future;
      await _turn();
      expect(fiber.poll(), isA<OptionNone<Exit<int, Never>>>());
      creatorGate.complete(42);

      expect(_value(await runtime.run(fiber.join())), 42);
    },
  );

  test(
    'forkIn still cannot close the runtime that indirectly owns it',
    () async {
      final scope = await _heldScope(runtime);
      final gate = Completer<void>();
      addTearDown(() {
        if (!gate.isCompleted) gate.complete();
      });
      final fiber = _value(
        await runtime.run(
          _future(gate.future)
              .flatMap((_) => _future(runtime.close()))
              .forkIn(scope),
        ),
      );
      gate.complete();
      final closeExit = _value(await runtime.run(fiber.join()));

      expect((_cause(closeExit) as CauseDefect).error, isA<StateError>());
      expect(_value(await runtime.run(Task.succeed(42))), 42);
    },
  );

  test('a closing scope rejects forkIn before business work starts', () async {
    final scope = await _heldScope(runtime);
    final cleaned = Completer<void>();
    final cleaning = Completer<void>();
    addTearDown(() {
      if (!cleaned.isCompleted) cleaned.complete();
    });
    await runtime.run(
      Task.fromCallback<void, Never>((_) {
        return () {
          cleaning.complete();
          return cleaned.future;
        };
      }).forkIn(scope),
    );
    final closing = runtime.run(scope.close(const Exit.success(null)));
    await cleaning.future;
    var started = false;
    final exit = await runtime.run(
      Task.sync(() => started = true).forkIn(scope),
    );

    expect((_cause(exit) as CauseDefect).error, isA<StateError>());
    expect(started, isFalse);
    cleaned.complete();
    expect(await closing, isA<ExitSuccess<void, Never>>());
  });

  test('a closed runtime returns a completed failed fiber', () async {
    await runtime.close();
    var started = false;
    final rejected = runtime.start(Task.sync(() => started = true));
    final exit = (rejected.poll() as OptionSome<Exit<bool, Never>>).value;

    expect((_cause(exit) as CauseDefect).error, isA<RuntimeClosed>());
    expect(started, isFalse);
    await _reportsTurn();
    expect(reports, isEmpty);
  });

  test(
    'unobserved business failures report once without waiting for shutdown',
    () async {
      final fiber = runtime.start(Task.fail('unobserved'));
      await _reportsTurn();

      expect(reports, hasLength(1));
      expect((reports.single as CauseFailure<Object?>).error, 'unobserved');
      expect(
        (_cause(
          _value(await runtime.run(fiber.awaitExit())),
        ) as CauseFailure<String>).error,
        'unobserved',
      );
      await _reportsTurn();
      expect(reports, hasLength(1));
    },
  );

  test('constructing a wait does not count as observing a failure', () async {
    final fiber = runtime.start(Task.fail('not executed'));
    fiber.awaitExit();
    fiber.join();
    await _reportsTurn();

    expect(reports, hasLength(1));
  });

  test('a completed poll suppresses an unhandled failure report', () async {
    final fiber = runtime.start(Task.fail('observed'));
    await _turn();
    expect(fiber.poll(), isA<OptionSome<Exit<Never, String>>>());
    await _reportsTurn();

    expect(reports, isEmpty);
  });

  test(
    'cancellation-only exits are not reported as unhandled errors',
    () async {
      runtime.start(Task.failCause(const Cause.cancelled('explicit')));
      final fiber = runtime.start(_pending<void>());
      fiber.requestCancel();
      await _reportsTurn();

      expect(reports, isEmpty);
    },
  );

  test(
    'owner shutdown handles defects while retaining unobserved business errors',
    () async {
      final gate = Completer<void>();
      addTearDown(() {
        if (!gate.isCompleted) gate.complete();
      });
      final cleaning = Completer<void>();
      final defect = Cause<Never>.defect(Object(), StackTrace.current);
      runtime.start(
        Task.addFinalizer((_) {
          cleaning.complete();
          return _future(gate.future).flatMap((_) => Task.failCause(defect));
        }).withError<String>().flatMap((_) => Task.fail('business')),
      );
      await cleaning.future;
      final closing = runtime.close();
      gate.complete();
      final closeCause = _cause(await closing);

      expect(closeCause, same(defect));
      await _reportsTurn();
      expect(reports, hasLength(1));
      expect((reports.single as CauseFailure<Object?>).error, 'business');
    },
  );
}

Future<Scope> _heldScope(Runtime runtime) {
  final ready = Completer<Scope>();
  runtime.start(
    Scope.open().flatMap((scope) {
      ready.complete(scope);
      return _pending<void>();
    }),
  );
  return ready.future;
}

Task<T, Never> _pending<T>() => Task.fromCallback<T, Never>((_) => () {});

Task<T, Never> _future<T>(Future<T> future) => Task.fromFuture<T, Never>(
  () => future,
  onError: (error, stackTrace) => Error.throwWithStackTrace(error, stackTrace),
);

Future<void> _turn() => Future<void>.delayed(Duration.zero);

Future<void> _reportsTurn() async {
  await _turn();
  await _turn();
}

T _value<T, E>(Exit<T, E> exit) => switch (exit) {
  ExitSuccess(:final value) => value,
  ExitFailure(:final cause) => fail('Expected success, got $cause'),
};

Cause<E> _cause<T, E>(Exit<T, E> exit) => switch (exit) {
  ExitSuccess(:final value) => fail('Expected failure, got $value'),
  ExitFailure(:final cause) => cause,
};
