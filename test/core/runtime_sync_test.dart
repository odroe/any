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
    expect(reports, isEmpty);
  });

  test('both synchronous entries execute each description once', () {
    var calls = 0;
    final task = Task.defer(() => Task.sync(() => ++calls))
        .map((value) => value * 2)
        .flatMap((value) => Task.succeed(value + 1));

    expect(_value(runtime.runSync(task)), 3);
    expect(runtime.runSyncOrThrow(task), 5);
    expect(calls, 2);
  });

  test('combinations preserve context and task-local bindings', () {
    final key = ServiceKey<String>('name');
    final local = TaskLocal('outside');
    final read = Task.service(key)
        .flatMap((name) => local.get().map((value) => '$name/$value'));
    final task = local
        .locally('inside', Task.all([read, read], concurrency: 2))
        .provideContext(Context.from([key.bind('provided')]));

    expect(_value(runtime.runSync(task)), [
      'provided/inside',
      'provided/inside',
    ]);
    expect(runtime.runSyncOrThrow(local.get()), 'outside');
    expect(
      _value(runtime.runSync(Task.zip(Task.succeed(1), Task.succeed('two')))),
      (1, 'two'),
    );
  });

  test('business recovery and error mapping stay synchronous', () {
    final Task<int, String> failed = Task.fail(1)
        .mapError((error) => 'error $error');
    final task = failed.recover((error) => Task.succeed(error.length));

    expect(_value(runtime.runSync(task)), 7);
    expect(_causes(runtime.runSync(Task.fail('failure'))), [
      isA<CauseFailure<String>>().having(
        (cause) => cause.error,
        'error',
        'failure',
      ),
    ]);
    expect(
      () => runtime.runSyncOrThrow(Task.fail('failure')),
      throwsA(
        isA<TaskException>().having(
          (error) => (error.cause as CauseFailure).error,
          'business error',
          'failure',
        ),
      ),
    );
  });

  test('runSyncOrThrow describes the body and cleanup failure', () {
    final defect = Cause<Never>.defect(
      StateError('Close failed'),
      StackTrace.fromString('original cleanup stack'),
    );
    final task = Task.fail('Save rejected').ensuring(Task.failCause(defect));

    expect(
      () => runtime.runSyncOrThrow(task),
      throwsA(
        isA<TaskException>()
            .having(
              (error) => (error.cause as CauseGroup).causes.last,
              'cleanup defect',
              same(defect),
            )
            .having(
              (error) => error.toString(),
              'diagnostic',
              'TaskException:\n'
                  '  Failure: Save rejected\n'
                  '  Defect: Bad state: Close failed\n'
                  '    original cleanup stack',
            ),
      ),
    );
  });

  test('cleanup is complete and ordered before the synchronous return', () {
    final events = <String>[];
    final fault = StateError('first cleanup');
    final stack = StackTrace.fromString('first cleanup stack');
    const cancellation = Cause<Never>.cancelled('second cleanup');

    Task<void, Never> cleanup(String name, Exit<Object?, Object?> exit) =>
        Task.sync(() {
          expect((exit as ExitFailure).cause, isA<CauseFailure<String>>());
          events.add(name);
        });
    final task =
        Task.addFinalizer(
              (exit) => cleanup(
                'first',
                exit,
              ).flatMap((_) => Task.failCause(Cause.defect(fault, stack))),
            )
            .flatMap(
              (_) => Task.addFinalizer(
                (exit) => cleanup(
                  'second',
                  exit,
                ).flatMap((_) => Task.failCause(cancellation)),
              ),
            )
            .withError<String>()
            .flatMap((_) => Task.fail('body'));

    final causes = _causes(runtime.runSync(task));
    expect(events, ['second', 'first']);
    expect(causes, hasLength(3));
    expect((causes[0] as CauseFailure).error, 'body');
    expect(causes[1], same(cancellation));
    expect((causes[2] as CauseDefect).error, same(fault));
    expect((causes[2] as CauseDefect).stackTrace, same(stack));
  });

  test(
    'synchronous callbacks complete without invoking cancellation cleanup',
    () {
      var cancelled = 0;
      final task = Task.fromCallback<int?, Never>((complete) {
        complete(const Exit.success(null));
        complete(const Exit.success(2));
        return () {
          cancelled++;
        };
      });

      expect(runtime.runSyncOrThrow(task), isNull);
      expect(cancelled, 0);
    },
  );

  test('callback registration exceptions remain defects after completion', () {
    final fault = StateError('registration');
    final task = Task.fromCallback<int, Never>((complete) {
      complete(const Exit.success(1));
      throw fault;
    });

    expect(
      (_causes(runtime.runSync(task)).single as CauseDefect).error,
      same(fault),
    );
  });

  test('a cancelled signal prevents even masked work from starting', () {
    final controller = CancellationController()
      ..requestCancel(reason: 'before');
    var called = false;
    final task = Task.uninterruptibleMask(
      (_) => Task.sync(() => called = true),
    );

    final result = runtime.runSync(task, signal: controller.signal);

    expect(called, isFalse);
    expect((_causes(result).single as CauseCancelled).reason, 'before');
  });

  test('a synchronous mask finishes protected work before cancellation', () {
    final controller = CancellationController();
    final events = <String>[];
    final task = Task.uninterruptibleMask(
      (_) =>
          Task.all([
            Task.sync(() {
              controller.requestCancel(reason: 'masked');
              events.add('first');
              return 1;
            }),
            Task.sync(() {
              events.add('second');
              return 2;
            }),
          ], concurrency: 2).map((values) {
            expect(values, [1, 2]);
            events.add('collected');
          }),
    ).map((_) => events.add('outside'));

    final result = runtime.runSync(task, signal: controller.signal);

    expect(events, ['first', 'second', 'collected']);
    expect((_causes(result).single as CauseCancelled).reason, 'masked');
  });

  for (final throwing in [false, true]) {
    test(
      'an already completed native Future is asynchronous: $throwing',
      () async {
        final settled = Completer<int>.sync()..complete(7);
        var calls = 0;
        var continued = false;
        final task =
            Task.fromFuture<int, String>(() {
              calls++;
              return settled.future;
            }, onError: (error, _) => '$error').map((value) {
              continued = true;
              return value;
            });

        final pending = _pending<int, String>(() {
          if (throwing) {
            runtime.runSyncOrThrow(task);
          } else {
            runtime.runSync(task);
          }
        });

        expect(calls, 1);
        _expectCancelled(await pending.completion);
        expect(calls, 1);
        expect(continued, isFalse);
      },
    );
  }

  test('a Future with synchronous then still suspends exactly once', () async {
    var calls = 0;
    var continued = false;
    final task =
        Task.fromFuture<int, Never>(() {
          calls++;
          return const _SynchronousFuture(7);
        }, onError: (error, _) => throw error).map((value) {
          continued = true;
          return value;
        });

    final pending = _pending<int, Never>(() => runtime.runSync(task));

    expect(calls, 1);
    expect(continued, isFalse);
    _expectCancelled(await pending.completion);
    expect(calls, 1);
    expect(continued, isFalse);
  });

  test('a Future returned as an ordinary value is not awaited', () async {
    final future = Future.value(7);
    final synchronousFuture = const _SynchronousFuture(8);

    expect(runtime.runSyncOrThrow(Task.sync(() => future)), same(future));
    expect(
      runtime.runSyncOrThrow(Task.succeed(synchronousFuture)),
      same(synchronousFuture),
    );
    expect(await future, 7);
  });

  test(
    'a synchronously throwing Future factory needs no asynchronous handoff',
    () {
      final fault = StateError('factory');
      final result = runtime.runSync(
        Task.fromFuture<int, Object>(
          () => throw fault,
          onError: (error, _) => error,
        ),
      );

      expect((_causes(result).single as CauseFailure).error, same(fault));
    },
  );

  test(
    'native handoff remains owned until its late failure and cleanup finish',
    () async {
      final gate = Completer<int>();
      final fault = StateError('late native failure');
      final stack = StackTrace.fromString('late native stack');
      var calls = 0;
      var cleaned = false;
      var mapped = false;
      final task = Task.addFinalizer((_) => Task.sync(() => cleaned = true))
          .withError<String>()
          .flatMap(
            (_) => Task.fromFuture<int, String>(
              () {
                calls++;
                return gate.future;
              },
              onError: (error, _) {
                mapped = true;
                return '$error';
              },
            ),
          );

      final pending = _pending<int, String>(() => runtime.runSync(task));
      var closed = false;
      final closing = runtime.close().then((exit) {
        closed = true;
        return exit;
      });
      await Future<void>.delayed(Duration.zero);
      expect(calls, 1);
      expect(closed, isFalse);
      expect(cleaned, isFalse);

      gate.completeError(fault, stack);
      final result = await pending.completion;
      _expectCancelled(result);
      final defect = _causes(result).whereType<CauseDefect>().single;
      expect(defect.error, same(fault));
      expect(defect.stackTrace, same(stack));
      expect(mapped, isFalse);
      expect(cleaned, isTrue);
      expect(calls, 1);
      expect(
        _causes(await closing).whereType<CauseDefect>().single.error,
        same(fault),
      );
    },
  );

  test(
    'masked asynchronous acquisition registers and releases its result once',
    () async {
      final gate = Completer<Object>();
      final resource = Object();
      var acquired = 0;
      var released = 0;
      var used = false;
      final task = Task.acquireRelease(
        Task.fromFuture<Object, Never>(() {
          acquired++;
          return gate.future;
        }, onError: Error.throwWithStackTrace),
        release: (value, _) => Task.sync(() {
          expect(value, same(resource));
          released++;
        }),
      ).map((_) => used = true);

      final pending = _pending<bool, Never>(() => runtime.runSync(task));
      expect(acquired, 1);
      expect(released, 0);
      gate.complete(resource);

      _expectCancelled(await pending.completion);
      expect(acquired, 1);
      expect(released, 1);
      expect(used, isFalse);
    },
  );

  test('asynchronous finalizer failure preserves remaining cleanup', () async {
    final gate = Completer<void>();
    final events = <String>[];
    final fault = StateError('cleanup');
    final task = Task.addFinalizer((_) => Task.sync(() => events.add('last')))
        .flatMap(
          (_) => Task.addFinalizer(
            (_) => Task.fromFuture<void, Never>(() {
              events.add('pending');
              return gate.future;
            }, onError: Error.throwWithStackTrace),
          ),
        )
        .flatMap((_) => Task.succeed(1));

    final pending = _pending<int, Never>(() => runtime.runSync(task));
    expect(events, ['pending']);
    gate.completeError(fault, StackTrace.current);
    final result = await pending.completion;

    _expectCancelled(result);
    expect(_causes(result).whereType<CauseDefect>().single.error, same(fault));
    expect(events, ['pending', 'last']);
  });

  test(
    'synchronous children and their borrowed resources finish before return',
    () {
      final events = <String>[];
      final child = Task.acquireRelease(
        Task.sync(() {
          events.add('acquire');
          return 4;
        }),
        release: (_, _) => Task.sync(() => events.add('release')),
      );
      final task = child.fork().flatMap((fiber) => fiber.join()).map((value) {
        events.add('parent');
        return value + 1;
      });

      expect(runtime.runSyncOrThrow(task), 5);
      expect(events, ['acquire', 'parent', 'release']);
    },
  );

  test('a suspended callback child requires a handoff even with synchronous cleanup', () async {
    var stopped = 0;
    var cleaned = 0;
    final child = Task.fromCallback<void, Never>(
      (_) => () {
        stopped++;
      },
    ).ensuring(Task.sync(() => cleaned++));

    final pending = _pending<int, Never>(() {
      runtime.runSyncOrThrow(child.fork().map((_) => 1));
    });
    _expectCancelled(await pending.completion);
    expect(stopped, 1);
    expect(cleaned, 1);
  });

  test(
    'asynchronous child cancellation cleanup requires a managed handoff',
    () async {
      final gate = Completer<void>();
      var stopped = 0;
      var cleaned = false;
      final child = Task.fromCallback<void, Never>(
        (_) => () {
          stopped++;
          return gate.future;
        },
      ).ensuring(Task.sync(() => cleaned = true));
      final pending = _pending<int, Never>(
        () => runtime.runSync(child.fork().map((_) => 1)),
      );

      expect(stopped, 1);
      expect(cleaned, isFalse);
      gate.complete();
      _expectCancelled(await pending.completion);
      expect(stopped, 1);
      expect(cleaned, isTrue);
    },
  );

  test(
    'deferred deep child startup remains an asynchronous boundary',
    () async {
      var started = 0;
      var cleaned = 0;
      Task<int, Never> nested(int depth) => Task.defer(() {
        started++;
        return depth == 0
            ? Task.succeed(1)
            : nested(depth - 1).fork().flatMap((child) => child.join());
      }).ensuring(Task.sync(() => cleaned++));

      final pending = _pending<int, Never>(() => runtime.runSync(nested(100)));
      final startedAtHandoff = started;
      expect(startedAtHandoff, greaterThan(0));
      expect(startedAtHandoff, lessThan(101));
      _expectCancelled(await pending.completion);
      expect(started, startedAtHandoff);
      expect(cleaned, started);
    },
  );

  test(
    'explicit yielding remains asynchronous even with no other work',
    () async {
      var continued = false;
      final pending = _pending<bool, Never>(
        () => runtime.runSync(Task.yieldNow().map((_) => continued = true)),
      );

      _expectCancelled(await pending.completion);
      expect(continued, isFalse);
    },
  );

  test(
    'automatic fairness does not interrupt a synchronous long chain',
    () async {
      var timerRan = false;
      final timer = Completer<void>();
      Timer.run(() {
        timerRan = true;
        timer.complete();
      });

      expect(runtime.runSyncOrThrow(_count()), 10000);
      expect(timerRan, isFalse);
      await timer.future;
    },
  );

  test(
    'asynchronous handoff restores fairness inside an ongoing mask',
    () async {
      final gate = Completer<void>();
      var timerRan = false;
      var counted = 0;
      final task = Task.uninterruptibleMask(
        (_) =>
            Task.fromFuture<void, Never>(
              () => gate.future,
              onError: Error.throwWithStackTrace,
            ).flatMap((_) => _count()).map((value) {
              expect(timerRan, isTrue);
              counted = value;
            }),
      );
      final pending = _pending<void, Never>(() => runtime.runSync(task));
      Timer.run(() => timerRan = true);
      gate.complete();

      _expectCancelled(await pending.completion);
      expect(counted, 10000);
    },
  );

  for (final syncRegistration in [false, true]) {
    test(
      'scope closure uses its current execution mode: registered sync=$syncRegistration',
      () async {
        final key = ServiceKey<Scope>('resource owner');
        final built = await Runtime.create(key.layer(Scope.open()));
        final owner = _value(built);
        addTearDown(owner.close);
        var timerRan = false;
        var finalized = 0;
        final register = Task.service(key).flatMap(
          (scope) => Task.addFinalizer(
            (_) => _count().map((value) {
              finalized = value;
            }),
          ).forkIn(scope).flatMap((fiber) => fiber.join()),
        );
        if (syncRegistration) {
          owner.runSyncOrThrow(register);
        } else {
          await owner.runOrThrow(register);
        }
        final timer = Completer<void>();
        Timer.run(() {
          timerRan = true;
          timer.complete();
        });
        final close = Task.service(key)
            .flatMap((scope) => scope.close(const Exit.success(null)));

        if (syncRegistration) {
          await owner.runOrThrow(close);
          expect(timerRan, isTrue);
        } else {
          owner.runSyncOrThrow(close);
          expect(timerRan, isFalse);
        }
        expect(finalized, 10000);
        await timer.future;
      },
    );
  }

  test(
    'closed runtimes reject both synchronous entries without work',
    () async {
      await runtime.close();
      var called = false;
      final task = Task.sync(() => called = true);

      expect(
        (_causes(runtime.runSync(task)).single as CauseDefect).error,
        isA<RuntimeClosed>(),
      );
      expect(
        () => runtime.runSyncOrThrow(task),
        throwsA(
          isA<TaskException>().having(
            (error) => (error.cause as CauseDefect).error,
            'closed runtime',
            isA<RuntimeClosed>(),
          ),
        ),
      );
      expect(called, isFalse);
    },
  );
}

Task<int, Never> _count() => Task.loop(
  0,
  whileTrue: (value) => value < 10000,
  step: (value) => Task.succeed(value + 1),
);

AsyncTaskException<T, E> _pending<T, E>(void Function() run) {
  try {
    run();
  } on AsyncTaskException<T, E> catch (error) {
    return error;
  }
  fail('Expected asynchronous work to require a managed handoff.');
}

T _value<T, E>(Exit<T, E> exit) => switch (exit) {
  ExitSuccess(:final value) => value,
  ExitFailure(:final cause) => throw StateError('Unexpected failure: $cause'),
};

List<Cause<E>> _causes<T, E>(Exit<T, E> exit) => switch (exit) {
  ExitSuccess() => [],
  ExitFailure(cause: CauseGroup(:final causes)) => causes,
  ExitFailure(:final cause) => [cause],
};

void _expectCancelled<T, E>(Exit<T, E> exit) =>
    expect(_causes(exit).whereType<CauseCancelled>(), isNotEmpty);

// Models Future implementations that deliver then callbacks immediately.
final class _SynchronousFuture<T> implements Future<T> {
  const _SynchronousFuture(this.value);

  final T value;

  @override
  Future<R> then<R>(FutureOr<R> Function(T) onValue, {Function? onError}) {
    final result = onValue(value);
    return result is Future<R> ? result : _SynchronousFuture(result);
  }

  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}
