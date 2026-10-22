import 'dart:async';

import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  group('Task', () {
    test('defers execution and repeats the composition on each run', () async {
      var computations = 0;
      var transformations = 0;
      final scopes = <Scope>[];
      final closedScopes = <Scope>[];
      final task =
          Task<int, Never, int>((context) {
            computations++;
            scopes.add(context.scope);
            context.scope.addFinalizer((_) => closedScopes.add(context.scope));
            return Exit.success(context.dependencies);
          }).map((value) {
            transformations++;
            return value * 2;
          });

      expect(computations, 0);
      expect(transformations, 0);
      expect((await task.run(21) as ExitSuccess<int, Never>).value, 42);
      expect((await task.run(10) as ExitSuccess<int, Never>).value, 20);
      expect(computations, 2);
      expect(transformations, 2);
      expect(scopes[0], isNot(same(scopes[1])));
      expect(closedScopes, scopes);
      expect(scopes.every((scope) => scope.isClosed), isTrue);
    });

    test('supports the documented parsing workflow', () async {
      final parse = Task<int, String, ({String input})>((context) {
        final value = int.tryParse(context.dependencies.input);
        return value == null
            ? const Exit.failure('Expected an integer.')
            : Exit.success(value);
      });

      final exit = await parse.map((value) => value * 2).run((input: '21'));
      final message = switch (exit) {
        ExitSuccess(:final value) => 'Parsed $value.',
        ExitFailure(:final error) => error,
        ExitDefect(:final error) => 'Unexpected: $error',
        ExitCancelled() => 'Cancelled.',
      };

      expect(message, 'Parsed 42.');
      final failure = await parse.run((input: 'invalid'));
      expect(
        (failure as ExitFailure<int, String>).error,
        'Expected an integer.',
      );
    });

    test(
      'awaits the computation and asynchronous cleanup before returning',
      () async {
        final body = Completer<Exit<int, Never>>();
        final cleanupStarted = Completer<void>();
        final cleanup = Completer<void>();
        late Scope scope;
        final task = Task<int, Never, ()>((context) {
          scope = context.scope;
          context.scope.addFinalizer((_) async {
            cleanupStarted.complete();
            await cleanup.future;
          });
          return body.future;
        });
        var finished = false;
        final running = task.run(()).then((exit) {
          finished = true;
          return exit;
        });
        const original = Exit<int, Never>.success(42);

        expect(cleanupStarted.isCompleted, isFalse);
        expect(finished, isFalse);
        body.complete(original);
        await cleanupStarted.future;
        expect(scope.isClosed, isTrue);
        expect(finished, isFalse);

        cleanup.complete();
        expect(await running, same(original));
        expect(finished, isTrue);
      },
    );

    for (final asynchronous in [false, true]) {
      test(
        'preserves thrown errors and traces (async: $asynchronous)',
        () async {
          final error = StateError('body failed');
          final trace = StackTrace.fromString('original task trace');
          late Exit<void, Object?> cleanupExit;
          final task = Task<int, StateError, ()>((context) {
            context.scope.addFinalizer((exit) => cleanupExit = exit);
            if (asynchronous) {
              return Future<Exit<int, StateError>>.error(error, trace);
            }
            Error.throwWithStackTrace(error, trace);
          });

          final exit = await task.run(());

          _expectDefect(exit, error, trace);
          expect(cleanupExit, same(exit));
        },
      );
    }

    test('treats a thrown non-Exception object as a defect', () async {
      final error = Object();
      final trace = StackTrace.fromString('non-Exception trace');
      final task = Task<int, Object, ()>(
        (_) => Error.throwWithStackTrace(error, trace),
      );

      _expectDefect(await task.run(()), error, trace);
    });

    final expectedError = StateError('expected failure');
    final defect = Object();
    final defectTrace = StackTrace.fromString('returned defect trace');
    final reason = Object();
    final outcomes = [
      (name: 'success', exit: const Exit<int, Object?>.success(42)),
      (name: 'failure', exit: Exit<int, Object?>.failure(expectedError)),
      (name: 'defect', exit: Exit<int, Object?>.defect(defect, defectTrace)),
      (
        name: 'cancellation',
        exit: Exit<int, Object?>.cancelled(reason: reason),
      ),
    ];

    for (final outcome in outcomes) {
      test(
        'passes ${outcome.name} to cleanup and preserves the exit',
        () async {
          late Exit<void, Object?> cleanupExit;
          final task = Task<int, Object?, ()>((context) {
            context.scope.addFinalizer((exit) => cleanupExit = exit);
            return outcome.exit;
          });

          expect(await task.run(()), same(outcome.exit));
          expect(cleanupExit, same(outcome.exit));
        },
      );

      test('preserves ${outcome.name} when cleanup fails', () async {
        final cleanupError = StateError('cleanup failed');
        final cleanupTrace = StackTrace.fromString('cleanup trace');
        final events = <String>[];
        final task = Task<int, Object?, ()>((context) {
          context.scope.addFinalizer((_) => events.add('remaining'));
          context.scope.addFinalizer((_) async {
            await Future<void>.value();
            events.add('failing');
            Error.throwWithStackTrace(cleanupError, cleanupTrace);
          });
          return outcome.exit;
        });

        final exit = await task.run(());

        expect(exit, isA<ExitDefect<int, Object?>>());
        final error = (exit as ExitDefect<int, Object?>).error;
        expect(error, isA<ScopeCloseException>());
        final failure = error as ScopeCloseException;
        expect(failure.exit, same(outcome.exit));
        expect(failure.errors, hasLength(1));
        expect(failure.errors.single.error, same(cleanupError));
        expect(failure.errors.single.stackTrace, same(cleanupTrace));
        expect(events, ['failing', 'remaining']);
      });
    }

    test(
      'runs concurrently with separate scopes and original dependencies',
      () async {
        final firstGate = Completer<void>();
        final secondGate = Completer<void>();
        final bothStarted = Completer<void>();
        final contexts = <Context<({String name, Future<void> gate})>>[];
        final closed = <String>[];
        final task = Task<String, Never, ({String name, Future<void> gate})>((
          context,
        ) async {
          contexts.add(context);
          if (contexts.length == 2) bothStarted.complete();
          context.scope.addFinalizer(
            (_) => closed.add(context.dependencies.name),
          );
          await context.dependencies.gate;
          return Exit.success(context.dependencies.name);
        });
        final firstDependencies = (name: 'first', gate: firstGate.future);
        final secondDependencies = (name: 'second', gate: secondGate.future);
        final first = task.run(firstDependencies);
        final second = task.run(secondDependencies);

        await bothStarted.future;
        expect(contexts, hasLength(2));
        expect(contexts[0].dependencies, same(firstDependencies));
        expect(contexts[1].dependencies, same(secondDependencies));
        expect(contexts[0].scope, isNot(same(contexts[1].scope)));

        secondGate.complete();
        expect((await second as ExitSuccess<String, Never>).value, 'second');
        expect(closed, ['second']);
        expect(contexts[0].scope.isClosed, isFalse);

        firstGate.complete();
        expect((await first as ExitSuccess<String, Never>).value, 'first');
        expect(closed, ['second', 'first']);
      },
    );

    test(
      'supports nullable values, nullable failures, and void tasks',
      () async {
        final success = Task<int?, Never, ()>((_) => const Exit.success(null));
        final failure = Task<int, String?, ()>((_) => const Exit.failure(null));
        final empty = Task<void, Never, ()>((_) => const Exit.success(null));

        expect(
          (await success.run(()) as ExitSuccess<int?, Never>).value,
          isNull,
        );
        expect(
          (await failure.run(()) as ExitFailure<int, String?>).error,
          isNull,
        );
        expect(await empty.run(()), isA<ExitSuccess<void, Never>>());
      },
    );

    test(
      'accepts an existing Result without reclassifying its error',
      () async {
        final error = StateError('expected');
        final result = Result<int, StateError>.failure(error);
        final task = Task<int, StateError, ()>((_) => Exit.fromResult(result));

        expect(
          (await task.run(()) as ExitFailure<int, StateError>).error,
          same(error),
        );
      },
    );

    for (final operation in ['map', 'flatMap']) {
      test(
        'runs 10000 $operation steps without overflowing the stack',
        () async {
          var cleanupCalls = 0;
          var task = Task<int, Never, ()>((context) {
            context.scope.addFinalizer((_) => cleanupCalls++);
            return const Exit.success(0);
          });
          for (var i = 0; i < 10000; i++) {
            task = operation == 'map'
                ? task.map((value) => value + 1)
                : task.flatMap((value) => Task((_) => Exit.success(value + 1)));
          }

          final exit = await task.run(());

          expect((exit as ExitSuccess<int, Never>).value, 10000);
          expect(cleanupCalls, 1);
        },
      );
    }

    group('map', () {
      test(
        'keeps a future as the transformed value without awaiting it',
        () async {
          final pending = Completer<int>();
          final task = Task<int, Never, ()>((_) => const Exit.success(1))
              .map((_) => pending.future);

          final exit = await task.run(());

          expect(
            (exit as ExitSuccess<Future<int>, Never>).value,
            same(pending.future),
          );
          expect(pending.isCompleted, isFalse);
          pending.complete(42);
          expect(await exit.value, 42);
        },
      );

      test(
        'turns a thrown transformation into the exit used by cleanup',
        () async {
          final error = Object();
          final trace = StackTrace.fromString('map trace');
          late Exit<void, Object?> cleanupExit;
          final task = Task<int, Never, ()>((context) {
            context.scope.addFinalizer((exit) => cleanupExit = exit);
            return const Exit.success(1);
          }).map<String>((_) => Error.throwWithStackTrace(error, trace));

          final exit = await task.run(());

          _expectDefect(exit, error, trace);
          expect(cleanupExit, same(exit));
        },
      );
    });

    group('flatMap', () {
      test(
        'shares the context and keeps resources until the whole task exits',
        () async {
          final events = <String>[];
          final cleanupExits = <Exit<void, Object?>>[];
          late Context<({int offset})> originalContext;
          final task =
              Task<int, String, ({int offset})>((context) {
                    originalContext = context;
                    context.scope.addFinalizer((exit) {
                      cleanupExits.add(exit);
                      events.add('close first');
                    });
                    events.add('first');
                    return const Exit.success(40);
                  })
                  .flatMap((value) {
                    return Task<String, String, ({int offset})>((
                      context,
                    ) async {
                      expect(context, same(originalContext));
                      expect(context.scope.isClosed, isFalse);
                      context.scope.addFinalizer((exit) {
                        cleanupExits.add(exit);
                        events.add('close second');
                      });
                      await Future<void>.value();
                      events.add('second');
                      return Exit.success(
                        '${value + context.dependencies.offset}',
                      );
                    });
                  })
                  .map((value) {
                    expect(originalContext.scope.isClosed, isFalse);
                    events.add('map');
                    return 'value:$value';
                  });

          expect(events, isEmpty);
          final exit = await task.run((offset: 2));

          expect((exit as ExitSuccess<String, String>).value, 'value:42');
          expect(events, [
            'first',
            'second',
            'map',
            'close second',
            'close first',
          ]);
          expect(cleanupExits, hasLength(2));
          expect(
            cleanupExits.every((cleanupExit) => identical(cleanupExit, exit)),
            isTrue,
          );
        },
      );

      test('captures a throw while choosing the next task', () async {
        final error = Object();
        final trace = StackTrace.fromString('next task trace');
        final task = Task<int, Never, ()>((_) => const Exit.success(1))
            .flatMap<String>((_) => Error.throwWithStackTrace(error, trace));

        _expectDefect(await task.run(()), error, trace);
      });

      test(
        'captures asynchronous failures in the next task and cleans up',
        () async {
          final error = Object();
          final trace = StackTrace.fromString('second task trace');
          var closed = false;
          final task = Task<int, Never, ()>((_) => const Exit.success(1))
              .flatMap(
                (_) => Task<String, Never, ()>((context) {
                  context.scope.addFinalizer((_) => closed = true);
                  return Future<Exit<String, Never>>.error(error, trace);
                }),
              );

          _expectDefect(await task.run(()), error, trace);
          expect(closed, isTrue);
        },
      );

      test(
        'allows an infallible task to be widened for a fallible next step',
        () async {
          final Task<num, String, ()> first = Task<int, Never, ()>(
            (_) => const Exit.success(1),
          );
          final task = first.flatMap(
            (value) => Task<double, String, ()>(
              (_) => Exit.failure('Cannot use $value'),
            ),
          );

          final exit = await task.run(());

          expect((exit as ExitFailure<double, String>).error, 'Cannot use 1');
        },
      );
    });

    final terminalOutcomes = [
      (
        exit: Exit<int, Object?>.failure(expectedError),
        name: 'failure',
        matcher: isA<ExitFailure<String, Object?>>().having(
          (exit) => exit.error,
          'error',
          same(expectedError),
        ),
      ),
      (
        exit: Exit<int, Object?>.defect(defect, defectTrace),
        name: 'defect',
        matcher: isA<ExitDefect<String, Object?>>()
            .having((exit) => exit.error, 'error', same(defect))
            .having(
              (exit) => exit.stackTrace,
              'stack trace',
              same(defectTrace),
            ),
      ),
      (
        exit: Exit<int, Object?>.cancelled(reason: reason),
        name: 'cancellation',
        matcher: isA<ExitCancelled<String, Object?>>().having(
          (exit) => exit.reason,
          'reason',
          same(reason),
        ),
      ),
    ];

    for (final outcome in terminalOutcomes) {
      test('map and flatMap skip callbacks after ${outcome.name}', () async {
        final task = Task<int, Object?, ()>((_) => outcome.exit);
        var called = false;
        final mapped = task.map((_) {
          called = true;
          return 'unexpected';
        });
        final chained = task.flatMap((_) {
          called = true;
          return Task<String, Object?, ()>(
            (_) => const Exit.success('unexpected'),
          );
        });

        expect(await mapped.run(()), outcome.matcher);
        expect(await chained.run(()), outcome.matcher);
        expect(called, isFalse);
      });

      test('flatMap retains ${outcome.name} from the next task', () async {
        final task = Task<void, Object?, ()>((_) => const Exit.success(null))
            .flatMap((_) => Task<int, Object?, ()>((_) => outcome.exit))
            .map((value) => 'unexpected:$value');

        expect(await task.run(()), outcome.matcher);
      });
    }
  });
}

void _expectDefect<T, E>(Exit<T, E> exit, Object error, StackTrace trace) {
  expect(exit, isA<ExitDefect<T, E>>());
  final defect = exit as ExitDefect<T, E>;
  expect(defect.error, same(error));
  expect(defect.stackTrace, same(trace));
}
