import 'dart:async';

import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  group('Scope', () {
    test('closes an empty scope and reuses its completion', () async {
      final scope = Scope();
      expect(scope.isClosed, isFalse);

      final closing = scope.close();
      expect(scope.isClosed, isTrue);
      expect(scope.close(), same(closing));

      await closing;
      expect(scope.close(), same(closing));
    });

    test('awaits finalizers one at a time in reverse order', () async {
      final scope = Scope();
      final started = Completer<void>();
      final release = Completer<void>();
      final events = <String>[];
      scope.addFinalizer((_) => events.add('first'));
      scope.addFinalizer((_) async {
        events.add('second:start');
        started.complete();
        await release.future;
        events.add('second:end');
      });

      final closing = scope.close();
      await started.future;
      expect(scope.isClosed, isTrue);
      expect(events, ['second:start']);

      release.complete();
      await closing;
      expect(events, ['second:start', 'second:end', 'first']);

      await scope.close();
      expect(events, ['second:start', 'second:end', 'first']);
    });

    test(
      'passes the first exit to every finalizer without copying it',
      () async {
        final scope = Scope();
        final seen = <Exit<void, Object?>>[];
        const exit = Exit<int, String>.failure(
          Cause.failure('original failure'),
        );
        scope.addFinalizer(seen.add);
        scope.addFinalizer(seen.add);

        final closing = scope.close(exit: exit);
        expect(
          scope.close(
            exit: const Exit.failure(Cause.cancelled(reason: 'too late')),
          ),
          same(closing),
        );
        await closing;

        expect(seen, hasLength(2));
        expect(seen, everyElement(same(exit)));
      },
    );

    test('awaits futures whose result type is not void', () async {
      final scope = Scope();
      final started = Completer<void>();
      final release = Completer<int>();
      var earlierFinalizerRan = false;
      scope.addFinalizer((_) => earlierFinalizerRan = true);
      Future<int> finalizer(Exit<void, Object?> _) {
        started.complete();
        return release.future;
      }

      scope.addFinalizer(finalizer);
      final closing = scope.close();
      await started.future;
      expect(earlierFinalizerRan, isFalse);

      release.complete(42);
      await closing;
      expect(earlierFinalizerRan, isTrue);
    });

    test('defaults to successful completion without a value', () async {
      final scope = Scope();
      scope.addFinalizer((exit) {
        expect(exit, isA<ExitSuccess<void, Object?>>());
      });

      await scope.close();
    });

    test('passes a cancellation reason to cleanup', () async {
      final scope = Scope();
      final reason = Object();
      scope.addFinalizer((exit) {
        expect(
          exit,
          isA<ExitFailure<void, Object?>>().having(
            (exit) => exit.cause,
            'cause',
            isA<CauseCancelled<Object?>>().having(
              (cause) => cause.reason,
              'reason',
              same(reason),
            ),
          ),
        );
      });

      await scope.close(
        exit: Exit<Never, Never>.failure(Cause.cancelled(reason: reason)),
      );
    });

    test('a reentrant close sees the same completion', () async {
      final scope = Scope();
      late Future<void> reentrant;
      scope.addFinalizer((_) {
        reentrant = scope.close();
      });

      final closing = scope.close();
      await closing;
      expect(reentrant, same(closing));
    });

    test('rejects new finalizers as soon as closing starts', () async {
      final scope = Scope();
      var calls = 0;
      void register() => scope.addFinalizer((_) => calls++);
      scope.addFinalizer((_) {
        expect(register, throwsStateError);
      });

      final closing = scope.close();
      expect(register, throwsStateError);
      await closing;
      expect(register, throwsStateError);
      expect(calls, 0);
    });

    test('attempts all cleanup and retains every error and trace', () async {
      final scope = Scope();
      final events = <String>[];
      final syncError = StateError('synchronous cleanup failed');
      const asyncError = 'asynchronous cleanup failed';
      final syncTrace = StackTrace.fromString('original synchronous trace');
      final asyncTrace = StackTrace.fromString('original asynchronous trace');
      scope.addFinalizer((_) {
        events.add('first');
        Error.throwWithStackTrace(syncError, syncTrace);
      });
      scope.addFinalizer((_) => events.add('second'));
      scope.addFinalizer((_) {
        events.add('third');
        return Future<void>.error(asyncError, asyncTrace);
      });

      final error = await _closeError(scope.close());

      expect(events, ['third', 'second', 'first']);
      final causes = (error.cause as CauseGroup<Never>).causes;
      expect(causes, hasLength(2));
      final first = causes[0] as CauseDefect<Never>;
      final second = causes[1] as CauseDefect<Never>;
      expect(first.error, same(asyncError));
      expect(first.stackTrace, same(asyncTrace));
      expect(second.error, same(syncError));
      expect(second.stackTrace, same(syncTrace));
      expect(error.toString(), contains(asyncError));
      expect(error.toString(), contains('synchronous cleanup failed'));
      expect(() => causes.clear(), throwsUnsupportedError);
    });

    for (final (name, exit) in <(String, Exit<void, Object?>)>[
      ('success', const Exit<int, String>.success(42)),
      (
        'failure',
        const Exit<int, String>.failure(Cause.failure('original failure')),
      ),
      (
        'nullable failure',
        const Exit<int, String?>.failure(Cause.failure(null)),
      ),
      (
        'defect',
        Exit<Never, Never>.failure(
          Cause.defect(
            StateError('original defect'),
            StackTrace.fromString('original task trace'),
          ),
        ),
      ),
      (
        'cancellation',
        const Exit<Never, Never>.failure(Cause.cancelled(reason: 'shutdown')),
      ),
    ]) {
      test('retains the original $name alongside cleanup failures', () async {
        final scope = Scope();
        final cleanupError = StateError('cleanup failed');
        scope.addFinalizer((_) => throw cleanupError);

        final error = await _closeError(scope.close(exit: exit));

        expect(error.exit, same(exit));
        expect((error.cause as CauseDefect<Never>).error, same(cleanupError));
      });
    }

    test(
      'repeated close preserves failure and does not retry cleanup',
      () async {
        final scope = Scope();
        var calls = 0;
        scope.addFinalizer((_) {
          calls++;
          throw StateError('cleanup failed');
        });

        final closing = scope.close();
        expect(scope.close(), same(closing));
        final error = await _closeError(closing);
        expect(await _closeError(scope.close()), same(error));
        expect(calls, 1);
        expect(scope.isClosed, isTrue);
        expect(() => scope.addFinalizer((_) {}), throwsStateError);
      },
    );

    test('awaits native stream subscription cleanup', () async {
      final scope = Scope();
      final cancelStarted = Completer<void>();
      final release = Completer<void>();
      final controller = StreamController<int>(
        onCancel: () {
          cancelStarted.complete();
          return release.future;
        },
      );
      final subscription = controller.stream.listen((_) {});
      scope.addFinalizer((_) => subscription.cancel());
      var finished = false;
      final closing = scope.close().then((_) => finished = true);

      await cancelStarted.future;
      expect(finished, isFalse);
      release.complete();
      await closing;
      expect(finished, isTrue);
      await controller.close();
    });

    test(
      'nested scopes compose cleanup and pass through cancellation',
      () async {
        final parent = Scope();
        final child = Scope();
        final events = <String>[];
        const exit = Exit<Never, Never>.failure(
          Cause.cancelled(reason: 'shutdown'),
        );
        parent.addFinalizer((_) => events.add('parent'));
        parent.addFinalizer((exit) => child.close(exit: exit));
        child.addFinalizer((childExit) {
          expect(childExit, same(exit));
          events.add('child');
        });

        await parent.close(exit: exit);
        expect(events, ['child', 'parent']);
        expect(child.isClosed, isTrue);
      },
    );

    for (final failureCount in [1, 2]) {
      test(
        'extracts $failureCount cleanup causes from a synchronous throw',
        () async {
          final child = Scope();
          const childExit = Exit<void, String>.failure(Cause.failure('child'));
          for (var index = 0; index < failureCount; index++) {
            child.addFinalizer((_) => throw StateError('cleanup $index'));
          }
          final childError = await _closeError(child.close(exit: childExit));
          final parent = Scope();
          const parentExit = Exit<void, String>.failure(
            Cause.failure('parent'),
          );
          parent.addFinalizer((_) => throw childError);

          final error = await _closeError(parent.close(exit: parentExit));

          expect(error.exit, same(parentExit));
          expect(error.cause, same(childError.cause));
          expect(childError.exit, same(childExit));
          expect(await _closeError(parent.close()), same(error));
        },
      );
    }

    test('flattens multiple scope levels in cleanup execution order', () async {
      final parent = Scope();
      final child = Scope();
      final grandchild = Scope();
      final errors = List.generate(4, (index) => StateError('cleanup $index'));
      final traces = List.generate(
        4,
        (index) => StackTrace.fromString('trace $index'),
      );
      final events = <int>[];
      final cleanupStarted = Completer<void>();
      final cleanupFinished = Completer<void>();
      var finished = false;
      const exit = Exit<void, String>.failure(Cause.failure('original'));
      parent.addFinalizer((_) {
        events.add(3);
        Error.throwWithStackTrace(errors[3], traces[3]);
      });
      parent.addFinalizer((exit) => child.close(exit: exit));
      parent.addFinalizer((_) {
        events.add(0);
        Error.throwWithStackTrace(errors[0], traces[0]);
      });
      child.addFinalizer((_) {
        events.add(2);
        Error.throwWithStackTrace(errors[2], traces[2]);
      });
      child.addFinalizer((exit) => grandchild.close(exit: exit));
      grandchild.addFinalizer((_) async {
        cleanupStarted.complete();
        await cleanupFinished.future;
        events.add(1);
        Error.throwWithStackTrace(errors[1], traces[1]);
      });

      final closing = _closeError(parent.close(exit: exit)).then((error) {
        finished = true;
        return error;
      });
      await cleanupStarted.future;
      expect(finished, isFalse);
      expect(events, [0]);
      cleanupFinished.complete();
      final error = await closing;

      expect(error.exit, same(exit));
      expect(events, [0, 1, 2, 3]);
      final causes = (error.cause as CauseGroup<Never>).causes;
      expect(causes, hasLength(4));
      for (var index = 0; index < causes.length; index++) {
        final defect = causes[index] as CauseDefect<Never>;
        expect(defect.error, same(errors[index]));
        expect(defect.stackTrace, same(traces[index]));
      }
    });
  });
}

Future<ScopeCloseException> _closeError(Future<void> closing) async {
  try {
    await closing;
  } on ScopeCloseException catch (error) {
    return error;
  }
  fail('Expected a ScopeCloseException.');
}
