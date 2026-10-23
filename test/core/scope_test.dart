import 'dart:async';

import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  late Runtime runtime;

  setUp(() => runtime = Runtime());
  tearDown(() async {
    expect(
      await runtime.close().timeout(const Duration(seconds: 2)),
      isA<ExitSuccess<void, Never>>(),
    );
  });

  test(
    'a root run releases its resource before returning the body value',
    () async {
      final resource = Object();
      final events = <String>[];
      final task =
          Task.acquireRelease(
            Task.sync(() {
              events.add('acquire');
              return resource;
            }),
            release: (acquired, exit) {
              expect(acquired, same(resource));
              expect((exit as ExitSuccess).value, 42);
              return Task.sync(() => events.add('release'));
            },
          ).map((acquired) {
            expect(acquired, same(resource));
            events.add('use');
            return 42;
          });

      expect(_value(await runtime.run(task)), 42);
      expect(events, ['acquire', 'use', 'release']);
    },
  );

  for (final outcome in ['failure', 'defect', 'cancellation']) {
    test('releases after $outcome and passes the original body exit', () async {
      final controller = CancellationController();
      final error = StateError('body defect');
      final trace = StackTrace.fromString('body trace');
      Exit<Object?, Object?>? releasedExit;
      var releases = 0;
      final task =
          Task.acquireRelease<Object, String>(
            Task.succeed(Object()),
            release: (_, exit) => Task.checkCancelled().map((_) {
              releasedExit = exit;
              releases++;
            }),
          ).flatMap<int>(
            (_) => switch (outcome) {
              'failure' => Task.fail('business'),
              'defect' => Task.sync(
                () => Error.throwWithStackTrace(error, trace),
              ),
              _ => Task.sync(() {
                controller.requestCancel(reason: 'stop');
                return 42;
              }),
            },
          );

      final exit = await runtime.run(task, signal: controller.signal);
      final bodyCause = _cause(releasedExit!);
      final finalCause = _cause(exit);
      expect(releases, 1);
      switch (outcome) {
        case 'failure':
          expect((bodyCause as CauseFailure).error, 'business');
          expect((finalCause as CauseFailure).error, 'business');
        case 'defect':
          expect((bodyCause as CauseDefect).error, same(error));
          expect((finalCause as CauseDefect).stackTrace.toString(), '$trace');
        default:
          expect((bodyCause as CauseCancelled).reason, 'stop');
          expect((finalCause as CauseCancelled).reason, 'stop');
      }
    });
  }

  test('pre-cancellation does not acquire or register a resource', () async {
    final controller = CancellationController()..requestCancel();
    final task = Task.acquireRelease(
      Task.sync(() => fail('Acquisition must not start.')),
      release: (_, _) => fail('An unacquired resource cannot be released.'),
    );

    expect(
      _cause(await runtime.run(task, signal: controller.signal)),
      isA<CauseCancelled>(),
    );
  });

  test('a failed acquisition never calls release', () async {
    final task = Task.acquireRelease<Object, String>(
      Task.fail('acquire failed'),
      release: (_, _) => fail('An unacquired resource cannot be released.'),
    );

    expect(
      (_cause(await runtime.run(task)) as CauseFailure).error,
      'acquire failed',
    );
  });

  test(
    'cancellation inside acquisition cannot lose its successful resource',
    () async {
      final controller = CancellationController();
      final resource = Object();
      final released = <Object>[];
      final task = Task.acquireRelease(
        Task.sync(() {
          controller.requestCancel(reason: 'during acquire');
          return resource;
        }),
        release: (value, exit) => Task.checkCancelled().map((_) {
          expect(_cause(exit), isA<CauseCancelled>());
          released.add(value);
        }),
      ).map((_) => fail('Use must not start after pending cancellation.'));

      final exit = await runtime.run(task, signal: controller.signal);

      expect((_cause(exit) as CauseCancelled).reason, 'during acquire');
      expect(released, [same(resource)]);
    },
  );

  test(
    'cancellation waits for asynchronous acquisition and releases the result',
    () async {
      final controller = CancellationController();
      final started = Completer<void>();
      final acquired = Completer<Object>();
      final resource = Object();
      final released = <Object>[];
      final task = Task.acquireRelease(
        Task.fromFuture<Object, Never>(() {
          started.complete();
          return acquired.future;
        }, onError: (error, stack) => Error.throwWithStackTrace(error, stack)),
        release: (value, _) => Task.sync(() => released.add(value)),
      ).map((_) => fail('Use must not start after pending cancellation.'));
      var finished = false;
      final running = runtime.run(task, signal: controller.signal).then((exit) {
        finished = true;
        return exit;
      });
      await started.future;
      controller.requestCancel(reason: 'during acquire');
      await Future<void>.delayed(Duration.zero);
      expect(finished, isFalse);
      expect(released, isEmpty);
      acquired.complete(resource);

      expect(_cause(await running), isA<CauseCancelled>());
      expect(released, [same(resource)]);
    },
  );

  test(
    'all LIFO cleanup failures follow the body failure without wrapping',
    () async {
      final first = Cause<Never>.defect(Object(), StackTrace.current);
      final lastA = Cause<Never>.defect(Object(), StackTrace.current);
      final lastB = Cause<Never>.defect(Object(), StackTrace.current);
      final events = <String>[];
      final task =
          Task.addFinalizer(
                (_) => Task.sync(() {
                  events.add('first');
                }).flatMap((_) => Task.failCause(first)),
              )
              .withError<String>()
              .flatMap(
                (_) => Task.addFinalizer(
                  (_) =>
                      Task.sync(() {
                        events.add('last');
                      }).flatMap(
                        (_) => Task.failCause(Cause.combine([lastA, lastB])),
                      ),
                ),
              )
              .flatMap<int>((_) => Task.fail('body'));

      final reasons = _reasons(_cause(await runtime.run(task)));

      expect(events, ['last', 'first']);
      expect(reasons, hasLength(4));
      expect((reasons[0] as CauseFailure).error, 'body');
      expect(reasons.sublist(1), [same(lastA), same(lastB), same(first)]);
    },
  );

  test(
    'nested scopes flatten cleanup causes without repeating the body failure',
    () async {
      final innerError = Cause<Never>.defect(Object(), StackTrace.current);
      final outerError = Cause<Never>.defect(Object(), StackTrace.current);
      final inner = Task.addFinalizer((_) => Task.failCause(innerError))
          .withError<String>()
          .flatMap<int>((_) => Task.fail('body'))
          .scoped();
      final task = Task.addFinalizer((_) => Task.failCause(outerError))
          .withError<String>()
          .flatMap((_) => inner);

      final reasons = _reasons(_cause(await runtime.run(task)));

      expect(reasons, hasLength(3));
      expect((reasons[0] as CauseFailure).error, 'body');
      expect(reasons.sublist(1), [same(innerError), same(outerError)]);
    },
  );

  test('a scoped resource closes before its parent continuation', () async {
    final events = <String>[];
    final task =
        Task.acquireRelease(
          Task.succeed('resource'),
          release: (_, _) => Task.sync(() => events.add('release child')),
        ).map((_) => 42).scoped().map((value) {
          events.add('parent continuation');
          return value;
        });

    expect(_value(await runtime.run(task)), 42);
    expect(events, ['release child', 'parent continuation']);
  });

  test(
    'a throwing release factory preserves its original error and stack',
    () async {
      final error = StateError('release factory');
      final trace = StackTrace.fromString('release factory stack');
      final task = Task.acquireRelease(
        Task.succeed(Object()),
        release: (_, _) => Error.throwWithStackTrace(error, trace),
      ).map((_) => 42);

      final cause = _cause(await runtime.run(task)) as CauseDefect;

      expect(cause.error, same(error));
      expect(cause.stackTrace.toString(), '$trace');
    },
  );

  test(
    'run waits for asynchronous release before exposing its result',
    () async {
      final started = Completer<void>();
      final released = Completer<void>();
      var finished = false;
      final task = Task.acquireRelease(
        Task.succeed(Object()),
        release: (_, _) => Task.fromFuture<void, Never>(() {
          started.complete();
          return released.future;
        }, onError: (error, stack) => Error.throwWithStackTrace(error, stack)),
      ).map((_) => 42);
      final running = runtime.run(task).then((exit) {
        finished = true;
        return exit;
      });
      await started.future;
      await Future<void>.delayed(Duration.zero);
      expect(finished, isFalse);
      released.complete();

      expect(_value(await running), 42);
    },
  );

  test(
    'a finalizer can register more cleanup in the scope being closed',
    () async {
      final events = <String>[];
      final task =
          Task.addFinalizer((_) => Task.sync(() => events.add('first')))
              .flatMap(
                (_) => Task.addFinalizer(
                  (_) => Task.sync(() => events.add('last')).flatMap(
                    (_) => Task.addFinalizer(
                      (_) => Task.sync(() => events.add('added')),
                    ),
                  ),
                ),
              );

      expect(await runtime.run(task), isA<ExitSuccess<void, Never>>());
      expect(events, ['last', 'added', 'first']);
    },
  );

  test(
    'cancellation during release waits for cleanup and replaces body success',
    () async {
      final controller = CancellationController();
      final started = Completer<void>();
      final gate = Completer<void>();
      var finished = false;
      var cleaned = false;
      final task = Task.acquireRelease(
        Task.succeed(Object()),
        release: (_, exit) {
          expect(_value(exit), 42);
          return Task.fromFuture<void, Never>(
            () {
              started.complete();
              return gate.future;
            },
            onError: (error, stack) => Error.throwWithStackTrace(error, stack),
          ).flatMap((_) => Task.checkCancelled()).map((_) => cleaned = true);
        },
      ).map((_) => 42);
      final running = runtime.run(task, signal: controller.signal).then((exit) {
        finished = true;
        return exit;
      });
      await started.future;
      controller.requestCancel(reason: 'during release');
      await Future<void>.delayed(Duration.zero);
      expect(finished, isFalse);
      expect(cleaned, isFalse);
      gate.complete();

      expect(
        (_cause(await running) as CauseCancelled).reason,
        'during release',
      );
      expect(cleaned, isTrue);
    },
  );

  test(
    'cancellation during release preserves the completed business failure',
    () async {
      final controller = CancellationController();
      final started = Completer<void>();
      final gate = Completer<void>();
      final task = Task.acquireRelease<Object, String>(
        Task.succeed(Object()),
        release: (_, exit) {
          expect((_cause(exit) as CauseFailure<String>).error, 'body failed');
          return Task.fromFuture<void, Never>(
            () {
              started.complete();
              return gate.future;
            },
            onError: (error, stack) => Error.throwWithStackTrace(error, stack),
          );
        },
      ).flatMap<int>((_) => Task.fail('body failed'));
      final running = runtime.run(task, signal: controller.signal);
      await started.future;
      controller.requestCancel(reason: 'during release');
      gate.complete();

      final reasons = (_cause(await running) as CauseGroup<String>).causes;
      expect(reasons, hasLength(2));
      expect((reasons[0] as CauseFailure<String>).error, 'body failed');
      expect((reasons[1] as CauseCancelled).reason, 'during release');
      expect(reasons.whereType<CauseDefect>(), isEmpty);
    },
  );

  test(
    'a cancellation already observed by the body is not repeated after release',
    () async {
      final controller = CancellationController();
      final reason = Object();
      final started = Completer<void>();
      final gate = Completer<void>();
      final task =
          Task.acquireRelease(
            Task.succeed(Object()),
            release: (_, exit) {
              expect((_cause(exit) as CauseCancelled).reason, same(reason));
              return Task.fromFuture<void, Never>(
                () {
                  started.complete();
                  return gate.future;
                },
                onError: (error, stack) =>
                    Error.throwWithStackTrace(error, stack),
              );
            },
          ).map((_) {
            controller.requestCancel(reason: reason);
            return 42;
          });
      final running = runtime.run(task, signal: controller.signal);
      await started.future;
      controller.requestCancel(reason: 'duplicate request');
      gate.complete();

      final reasons = _reasons(_cause(await running));
      expect(reasons, hasLength(1));
      expect((reasons.single as CauseCancelled).reason, same(reason));
    },
  );

  test(
    'using releases before continuation and preserves its typed use exit',
    () async {
      final resource = Object();
      final events = <String>[];
      Exit<int, String>? releasedExit;
      final task =
          Task.using<Object, int, String>(
            Task.succeed(resource),
            use: (value) => Task.sync(() {
              expect(value, same(resource));
              events.add('use');
              return 42;
            }),
            release: (value, exit) => Task.sync(() {
              expect(value, same(resource));
              releasedExit = exit;
              events.add('release');
            }),
          ).map((value) {
            events.add('continuation');
            return value + 1;
          });

      expect(_value(await runtime.run(task)), 43);
      expect(_value(releasedExit!), 42);
      expect(events, ['use', 'release', 'continuation']);
    },
  );

  test('using retains use failure before a release failure', () async {
    final cleanup = Cause<Never>.defect(Object(), StackTrace.current);
    var releases = 0;
    final task = Task.using<Object, int, String>(
      Task.succeed(Object()),
      use: (_) => Task.fail('use failed'),
      release: (_, exit) {
        releases++;
        expect((_cause(exit) as CauseFailure).error, 'use failed');
        return Task.failCause(cleanup);
      },
    );

    final reasons = _reasons(_cause(await runtime.run(task)));

    expect(releases, 1);
    expect(reasons, hasLength(2));
    expect((reasons[0] as CauseFailure).error, 'use failed');
    expect(reasons[1], same(cleanup));
  });

  test(
    'ensuring and onExit finish before following success callbacks',
    () async {
      final events = <String>[];
      final task = Task.succeed(42)
          .withError<String>()
          .ensuring(Task.sync(() => events.add('ensuring')))
          .onExit(
            (Exit<int, String> exit) => Task.sync(() {
              expect(_value(exit), 42);
              events.add('onExit');
            }),
          )
          .map((value) {
            events.add('continuation');
            return value;
          });

      expect(_value(await runtime.run(task)), 42);
      expect(events, ['ensuring', 'onExit', 'continuation']);
    },
  );

  test('onExit observes failure and does not recover it', () async {
    final Task<int, String> body = Task.fail('body');
    var calls = 0;
    final task = body.onExit(
      (exit) => Task.sync(() {
        expect((_cause(exit) as CauseFailure).error, 'body');
        calls++;
      }),
    );

    expect((_cause(await runtime.run(task)) as CauseFailure).error, 'body');
    expect(calls, 1);
  });

  test(
    'repeated executions acquire and release independent resources',
    () async {
      var nextId = 0;
      final released = <int>[];
      final task = Task.acquireRelease(
        Task.sync(() => ++nextId),
        release: (id, _) => Task.sync(() => released.add(id)),
      ).map((id) => id * 10);

      expect(_value(await runtime.run(task)), 10);
      expect(released, [1]);
      expect(_value(await runtime.run(task)), 20);
      expect(released, [1, 2]);
    },
  );

  test(
    'cancellation cleanup retains its execution zone across async gaps',
    () async {
      final key = Object();
      final controller = CancellationController();
      final started = Completer<void>();
      final observed = <Object?>[];
      final task =
          Task.acquireRelease(
            Task.succeed(Object()),
            release: (_, _) {
              observed.add(Zone.current[key]);
              return Task.sync(() => observed.add(Zone.current[key]))
                  .flatMap((_) => Task.yieldNow())
                  .map((_) => observed.add(Zone.current[key]));
            },
          ).flatMap(
            (_) => Task.fromCallback<void, Never>((complete) {
              started.complete();
              return () {};
            }),
          );
      final running = runZoned(
        () => runtime.run(task, signal: controller.signal),
        zoneValues: {key: 'execution'},
      );
      await started.future;
      runZoned(controller.requestCancel, zoneValues: {key: 'cancel caller'});

      expect(_cause(await running), isA<CauseCancelled>());
      expect(observed, ['execution', 'execution', 'execution']);
    },
  );

  test(
    'explicit child close reports cleanup rather than its supplied body exit',
    () async {
      const bodyExit = Exit<void, String>.failure(Cause.failure('child body'));
      final task = Scope.open().flatMap(
        (child) => child
            .close(bodyExit)
            .flatMap((_) => child.close(const Exit.success(null)))
            .map((_) => 'closed'),
      );

      expect(_value(await runtime.run(task)), 'closed');
    },
  );

  test('cleanup can open scopes and releases its temporary resources before continuing', () async {
    final events = <String>[];
    final task =
        Task.addFinalizer((_) => Task.sync(() => events.add('original first')))
            .flatMap(
              (_) => Task.addFinalizer(
                (_) => Scope.open().flatMap((child) {
                  events.add('opened cleanup scope');
                  return Task.acquireRelease(
                    Task.sync(() {
                      events.add('acquire temporary');
                      return Object();
                    }),
                    release: (_, _) =>
                        Task.sync(() => events.add('release temporary')),
                  ).map((_) => events.add('cleanup body finished'));
                }),
              ),
            );

    expect(await runtime.run(task), isA<ExitSuccess<void, Never>>());
    expect(events, [
      'opened cleanup scope',
      'acquire temporary',
      'cleanup body finished',
      'release temporary',
      'original first',
    ]);
  });

  test('release can compose ensuring cleanup', () async {
    final events = <String>[];
    final task =
        Task.acquireRelease(
          Task.succeed(Object()),
          release: (_, _) =>
              Task.sync(() => events.add('release'))
                  .ensuring(Task.sync(() => events.add('ensuring release'))),
        ).map((_) {
          events.add('body');
          return 42;
        });

    expect(_value(await runtime.run(task)), 42);
    expect(events, ['body', 'release', 'ensuring release']);
  });

  test(
    'release can use a temporary resource before the next original finalizer',
    () async {
      final events = <String>[];
      final temporary = Object();
      final task =
          Task.addFinalizer(
                (_) => Task.sync(() => events.add('original first')),
              )
              .flatMap(
                (_) => Task.acquireRelease(
                  Task.succeed(Object()),
                  release: (_, _) => Task.using<Object, void, Never>(
                    Task.sync(() {
                      events.add('acquire temporary');
                      return temporary;
                    }),
                    use: (resource) => Task.sync(() {
                      expect(resource, same(temporary));
                      events.add('use temporary');
                    }),
                    release: (resource, exit) => Task.sync(() {
                      expect(resource, same(temporary));
                      expect(exit, isA<ExitSuccess<void, Never>>());
                      events.add('release temporary');
                    }),
                  ),
                ),
              )
              .map((_) => 42);

      expect(_value(await runtime.run(task)), 42);
      expect(events, [
        'acquire temporary',
        'use temporary',
        'release temporary',
        'original first',
      ]);
    },
  );

  test(
    'nested release cleanup failures remain flat and occur exactly once',
    () async {
      final releaseFailure = Cause<Never>.defect(Object(), StackTrace.current);
      final ensuringFailure = Cause<Never>.defect(Object(), StackTrace.current);
      final temporaryFailure = Cause<Never>.defect(
        Object(),
        StackTrace.current,
      );
      final events = <String>[];
      final task = Task.acquireRelease<Object, String>(
        Task.succeed(Object()),
        release: (_, _) => Task.using<Object, void, Never>(
          Task.succeed(Object()),
          use: (_) =>
              Task.sync(() => events.add('release body'))
                  .flatMap<void>((_) => Task.failCause(releaseFailure))
                  .ensuring(
                    Task.sync(() => events.add('ensuring'))
                        .flatMap<void>((_) => Task.failCause(ensuringFailure)),
                  ),
          release: (_, exit) {
            final inner = _reasons(_cause(exit));
            expect(inner, [same(releaseFailure), same(ensuringFailure)]);
            return Task.sync(() => events.add('temporary release'))
                .flatMap<void>((_) => Task.failCause(temporaryFailure));
          },
        ),
      ).flatMap<int>((_) => Task.fail('body failed'));

      final reasons = _reasons(_cause(await runtime.run(task)));

      expect(events, ['release body', 'ensuring', 'temporary release']);
      expect(reasons, hasLength(4));
      expect((reasons[0] as CauseFailure<String>).error, 'body failed');
      expect(reasons.sublist(1), [
        same(releaseFailure),
        same(ensuringFailure),
        same(temporaryFailure),
      ]);
    },
  );

  test('a finalizer cannot wait for its own runtime to close', () async {
    Exit<void, Never>? attemptedClose;
    final task = Task.addFinalizer(
      (_) => Task.fromFuture<Exit<void, Never>, Never>(
        runtime.close,
        onError: (error, stack) => Error.throwWithStackTrace(error, stack),
      ).map((exit) => attemptedClose = exit),
    );

    expect(
      await runtime.run(task).timeout(const Duration(seconds: 1)),
      isA<ExitSuccess<void, Never>>(),
    );
    expect((_cause(attemptedClose!) as CauseDefect).error, isA<StateError>());
    expect(_value(await runtime.run(Task.succeed('still open'))), 'still open');
  });
}

T _value<T, E>(Exit<T, E> exit) => switch (exit) {
  ExitSuccess(:final value) => value,
  ExitFailure(:final cause) => fail('Expected success, got $cause'),
};

Cause<E> _cause<T, E>(Exit<T, E> exit) => switch (exit) {
  ExitSuccess(:final value) => fail('Expected failure, got $value'),
  ExitFailure(:final cause) => cause,
};

List<Cause<E>> _reasons<E>(Cause<E> cause) => switch (cause) {
  CauseGroup(:final causes) => causes,
  _ => [cause],
};
