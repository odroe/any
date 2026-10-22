import 'dart:async';

import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  group('Task.runIn', () {
    test('defers each run and reuses the supplied context', () async {
      final scope = Scope();
      final dependencies = <int>[];
      final controller = CancellationController();
      final context = Context(
        scope: scope,
        dependencies: dependencies,
        cancellation: controller.token,
      );
      var cleanedUp = 0;
      final task = Task<int, Never, List<int>>((current) {
        expect(current, same(context));
        current.dependencies.add(1);
        current.scope.addFinalizer((_) => cleanedUp++);
        return Exit.success(current.dependencies.length);
      });

      final first = task.runIn(context);
      expect(dependencies, isEmpty);
      expect((await first as ExitSuccess<int, Never>).value, 1);
      expect((await task.runIn(context) as ExitSuccess<int, Never>).value, 2);
      expect(scope.isClosed, isFalse);
      expect(cleanedUp, 0);

      await scope.close();
      expect(cleanedUp, 2);
    });

    test('nested tasks keep resources until the owning run finishes', () async {
      final events = <String>[];
      final cleanupExits = <Exit<void, Object?>>[];
      final controller = CancellationController();
      late Scope scope;
      final child = Task<String, Never, ({String label})>((context) {
        expect(context.scope, same(scope));
        expect(context.cancellation, same(controller.token));
        final label = context.dependencies.label;
        events.add('use $label');
        context.scope.addFinalizer((exit) {
          events.add('close $label');
          cleanupExits.add(exit);
        });
        return Exit.success(label);
      });
      const parentExit = Exit<String, String>.failure(
        Cause.failure('parent failed'),
      );
      final workflow = Task<String, String, ()>((context) async {
        scope = context.scope;
        context.scope.addFinalizer((exit) {
          events.add('close parent');
          cleanupExits.add(exit);
        });
        final first = await child.runIn(
          context.withDependencies((label: 'first')),
        );
        final second = await child.runIn(
          context.withDependencies((label: 'second')),
        );

        expect((first as ExitSuccess<String, Never>).value, 'first');
        expect((second as ExitSuccess<String, Never>).value, 'second');
        expect(scope.isClosed, isFalse);
        expect(cleanupExits, isEmpty);
        return parentExit;
      });

      expect(
        await workflow.run((), cancellation: controller.token),
        same(parentExit),
      );
      expect(events, [
        'use first',
        'use second',
        'close second',
        'close first',
        'close parent',
      ]);
      expect(cleanupExits, hasLength(3));
      expect(cleanupExits.every((exit) => identical(exit, parentExit)), isTrue);
      expect(scope.isClosed, isTrue);
    });

    final error = StateError('computation failed');
    final trace = StackTrace.fromString('original computation trace');
    final reason = Object();
    final outcomes = [
      (name: 'success', exit: const Exit<int, Object?>.success(42)),
      (name: 'failure', exit: Exit<int, Object?>.failure(Cause.failure(error))),
      (
        name: 'defect',
        exit: Exit<int, Object?>.failure(Cause.defect(error, trace)),
      ),
      (
        name: 'cancellation',
        exit: Exit<int, Object?>.failure(Cause.cancelled(reason: reason)),
      ),
      (
        name: 'combined failure',
        exit: Exit<int, Object?>.failure(
          Cause.combine([
            Cause.failure(error),
            Cause.defect(error, trace),
            Cause.cancelled(reason: reason),
          ]),
        ),
      ),
    ];

    for (final outcome in outcomes) {
      test('leaves cleanup to the owner after ${outcome.name}', () async {
        final scope = Scope();
        final context = Context(scope: scope, dependencies: ());
        final cleanupExits = <Exit<void, Object?>>[];
        final task = Task<int, Object?, ()>((context) {
          context.scope.addFinalizer(cleanupExits.add);
          return outcome.exit;
        });

        expect(await task.runIn(context), same(outcome.exit));
        expect(scope.isClosed, isFalse);
        expect(cleanupExits, isEmpty);

        const ownerExit = Exit<void, Object?>.failure(
          Cause.failure('owner outcome'),
        );
        await scope.close(exit: ownerExit);
        expect(cleanupExits.single, same(ownerExit));
      });
    }

    for (final asynchronous in [false, true]) {
      test('captures thrown errors (async: $asynchronous)', () async {
        final scope = Scope();
        final context = Context(scope: scope, dependencies: ());
        final task = Task<int, StateError, ()>((_) {
          if (asynchronous) {
            return Future<Exit<int, StateError>>.error(error, trace);
          }
          Error.throwWithStackTrace(error, trace);
        });

        final exit = await task.runIn(context);

        final cause = (exit as ExitFailure<int, StateError>).cause;
        final defect = cause as CauseDefect<StateError>;
        expect(defect.error, same(error));
        expect(defect.stackTrace, same(trace));
        expect(scope.isClosed, isFalse);
        await scope.close(exit: exit);
      });
    }

    for (final beforeRun in [false, true]) {
      test('skips cancelled work (before run: $beforeRun)', () async {
        final scope = Scope();
        final controller = CancellationController();
        final context = Context(
          scope: scope,
          dependencies: (),
          cancellation: controller.token,
        );
        var called = false;
        final task = Task<int, Never, ()>((_) {
          called = true;
          return const Exit.success(1);
        });
        if (beforeRun) controller.cancel(reason: reason);

        final running = task.runIn(context);
        if (!beforeRun) controller.cancel(reason: reason);
        final exit = await running;

        final cause = (exit as ExitFailure<int, Never>).cause;
        expect((cause as CauseCancelled<Never>).reason, same(reason));
        expect(called, isFalse);
        expect(scope.isClosed, isFalse);
        await scope.close(exit: exit);
      });
    }

    test('awaits running work after a cancellation request', () async {
      final scope = Scope();
      final controller = CancellationController();
      final context = Context(
        scope: scope,
        dependencies: (),
        cancellation: controller.token,
      );
      final started = Completer<void>();
      final body = Completer<Exit<int, Never>>();
      final cleanupExits = <Exit<void, Object?>>[];
      final task = Task<int, Never, ()>((context) {
        context.scope.addFinalizer(cleanupExits.add);
        started.complete();
        return body.future;
      });
      var finished = false;
      final running = task.runIn(context).then((exit) {
        finished = true;
        return exit;
      });
      await started.future;

      controller.cancel(reason: reason);
      await Future<void>(() {});
      expect(finished, isFalse);
      expect(scope.isClosed, isFalse);
      expect(cleanupExits, isEmpty);

      body.complete(const Exit.success(42));
      final exit = await running;
      final cause = (exit as ExitFailure<int, Never>).cause;
      expect((cause as CauseCancelled<Never>).reason, same(reason));
      expect(scope.isClosed, isFalse);
      expect(cleanupExits, isEmpty);

      await scope.close(exit: exit);
      expect(cleanupExits.single, same(exit));
    });

    for (final cancelled in [false, true]) {
      test('rejects a closed scope (cancelled: $cancelled)', () async {
        final scope = Scope();
        final controller = CancellationController();
        final context = Context(
          scope: scope,
          dependencies: (),
          cancellation: controller.token,
        );
        await scope.close();
        if (cancelled) controller.cancel();
        var called = false;
        final task = Task<int, Never, ()>((_) {
          called = true;
          return const Exit.success(1);
        });

        _expectClosedScope(await task.runIn(context));
        expect(called, isFalse);
      });
    }

    test('rejects a scope while its cleanup is still running', () async {
      final scope = Scope();
      final context = Context(scope: scope, dependencies: ());
      final cleanupStarted = Completer<void>();
      final cleanup = Completer<void>();
      scope.addFinalizer((_) {
        cleanupStarted.complete();
        return cleanup.future;
      });
      final closing = scope.close();
      await cleanupStarted.future;
      var called = false;
      final task = Task<int, Never, ()>((_) {
        called = true;
        return const Exit.success(1);
      });

      _expectClosedScope(await task.runIn(context));
      expect(called, isFalse);
      expect(cleanup.isCompleted, isFalse);

      cleanup.complete();
      await closing;
    });

    test('checks the scope when the computation starts', () async {
      final scope = Scope();
      final context = Context(scope: scope, dependencies: ());
      var called = false;
      final task = Task<int, Never, ()>((_) {
        called = true;
        return const Exit.success(1);
      });

      final running = task.runIn(context);
      final closing = scope.close();

      _expectClosedScope(await running);
      expect(called, isFalse);
      await closing;
    });

    test('reports cleanup failures to the scope owner', () async {
      final scope = Scope();
      final context = Context(scope: scope, dependencies: ());
      const result = Exit<int, Never>.success(42);
      final task = Task<int, Never, ()>((context) {
        context.scope.addFinalizer(
          (_) => Error.throwWithStackTrace(error, trace),
        );
        return result;
      });

      final exit = await task.runIn(context);
      expect(exit, same(result));
      expect(scope.isClosed, isFalse);

      await expectLater(
        scope.close(exit: exit),
        throwsA(
          isA<ScopeCloseException>()
              .having((failure) => failure.exit, 'exit', same(exit))
              .having(
                (failure) => (failure.cause as CauseDefect<Never>).error,
                'cleanup error',
                same(error),
              )
              .having(
                (failure) => (failure.cause as CauseDefect<Never>).stackTrace,
                'cleanup trace',
                same(trace),
              ),
        ),
      );
    });
  });
}

void _expectClosedScope<T, E>(Exit<T, E> exit) {
  expect(
    exit,
    isA<ExitFailure<T, E>>().having(
      (exit) => exit.cause,
      'cause',
      isA<CauseDefect<E>>().having(
        (cause) => cause.error,
        'error',
        isA<StateError>(),
      ),
    ),
  );
}
