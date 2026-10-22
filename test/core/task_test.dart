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
            ? const Exit.failure(Cause.failure('Expected an integer.'))
            : Exit.success(value);
      });

      final exit = await parse.map((value) => value * 2).run((input: '21'));
      final message = switch (exit) {
        ExitSuccess(:final value) => 'Parsed $value.',
        ExitFailure(cause: CauseFailure(:final error)) => error,
        ExitFailure(cause: CauseDefect(:final error)) => 'Unexpected: $error',
        ExitFailure(cause: CauseCancelled()) => 'Cancelled.',
        ExitFailure(cause: CauseGroup(:final causes)) =>
          '${causes.length} failure reasons.',
      };

      expect(message, 'Parsed 42.');
      final failure = await parse.run((input: 'invalid'));
      expect(_expectedError(failure), 'Expected an integer.');
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
      (
        name: 'failure',
        exit: Exit<int, Object?>.failure(Cause.failure(expectedError)),
      ),
      (
        name: 'defect',
        exit: Exit<int, Object?>.failure(Cause.defect(defect, defectTrace)),
      ),
      (
        name: 'cancellation',
        exit: Exit<int, Object?>.failure(Cause.cancelled(reason: reason)),
      ),
      (
        name: 'combined failure',
        exit: Exit<int, Object?>.failure(
          Cause.combine([
            Cause.failure(expectedError),
            Cause.defect(defect, defectTrace),
            Cause.cancelled(reason: reason),
          ]),
        ),
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

        final cause = (exit as ExitFailure<int, Object?>).cause;
        final originalCauses = switch (outcome.exit) {
          ExitSuccess() => <Cause<Object?>>[],
          ExitFailure(cause: CauseGroup(:final causes)) => causes,
          ExitFailure(:final cause) => [cause],
        };
        final CauseDefect<Object?> cleanup;
        if (originalCauses.isEmpty) {
          cleanup = cause as CauseDefect<Object?>;
        } else {
          final causes = (cause as CauseGroup<Object?>).causes;
          expect(causes, hasLength(originalCauses.length + 1));
          expect(causes.take(originalCauses.length), originalCauses);
          cleanup = causes.last as CauseDefect<Object?>;
        }
        expect(cleanup.error, same(cleanupError));
        expect(cleanup.stackTrace, same(cleanupTrace));
        expect(events, ['failing', 'remaining']);
      });
    }

    test('combines typed causes and all cleanup defects in order', () async {
      const primary = Cause<({String code})>.failure((code: 'invalid'));
      final cancelled = Cause<Never>.cancelled(reason: Object());
      final original = Exit<int, ({String code})>.failure(
        Cause.combine([primary, cancelled]),
      );
      final syncError = StateError('synchronous cleanup failed');
      final asyncError = Object();
      final syncTrace = StackTrace.fromString('synchronous cleanup trace');
      final asyncTrace = StackTrace.fromString('asynchronous cleanup trace');
      final cleanupExits = <Exit<void, Object?>>[];
      final task = Task<int, ({String code}), ()>((context) {
        context.scope.addFinalizer((exit) {
          cleanupExits.add(exit);
          Error.throwWithStackTrace(syncError, syncTrace);
        });
        context.scope.addFinalizer((exit) {
          cleanupExits.add(exit);
          return Future<void>.error(asyncError, asyncTrace);
        });
        return original;
      });

      final exit = await task.run(()) as ExitFailure<int, ({String code})>;
      final causes = (exit.cause as CauseGroup<({String code})>).causes;

      expect(causes, hasLength(4));
      expect(causes[0], same(primary));
      expect(
        (causes[0] as CauseFailure<({String code})>).error.code,
        'invalid',
      );
      expect(causes[1], same(cancelled));
      final asyncFailure = causes[2] as CauseDefect<({String code})>;
      final syncFailure = causes[3] as CauseDefect<({String code})>;
      expect(asyncFailure.error, same(asyncError));
      expect(asyncFailure.stackTrace, same(asyncTrace));
      expect(syncFailure.error, same(syncError));
      expect(syncFailure.stackTrace, same(syncTrace));
      expect(cleanupExits, [same(original), same(original)]);
    });

    test(
      'keeps a cleanup exception thrown by the computation as a defect',
      () async {
        final otherScope = Scope();
        otherScope.addFinalizer(
          (_) => throw StateError('other cleanup failed'),
        );
        late ScopeCloseException error;
        try {
          await otherScope.close();
        } on ScopeCloseException catch (failure) {
          error = failure;
        }
        final trace = StackTrace.fromString('computation trace');
        final task = Task<int, Never, ()>(
          (_) => Error.throwWithStackTrace(error, trace),
        );

        _expectDefect(await task.run(()), error, trace);
      },
    );

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
        final failure = Task<int, String?, ()>(
          (_) => const Exit.failure(Cause.failure(null)),
        );
        final empty = Task<void, Never, ()>((_) => const Exit.success(null));

        expect(
          (await success.run(()) as ExitSuccess<int?, Never>).value,
          isNull,
        );
        expect(_expectedError(await failure.run(())), isNull);
        expect(await empty.run(()), isA<ExitSuccess<void, Never>>());
      },
    );

    test(
      'accepts an existing Result without reclassifying its error',
      () async {
        final error = StateError('expected');
        final result = Result<int, StateError>.failure(error);
        final task = Task<int, StateError, ()>((_) => Exit.fromResult(result));

        expect(_expectedError(await task.run(())), same(error));
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
              (_) => Exit.failure(Cause.failure('Cannot use $value')),
            ),
          );

          final exit = await task.run(());

          expect(_expectedError(exit), 'Cannot use 1');
        },
      );
    });

    final terminalOutcomes = outcomes.where(
      (outcome) => outcome.exit is ExitFailure,
    );

    for (final outcome in terminalOutcomes) {
      final cause = (outcome.exit as ExitFailure<int, Object?>).cause;
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

        expect(
          (await mapped.run(()) as ExitFailure<String, Object?>).cause,
          same(cause),
        );
        expect(
          (await chained.run(()) as ExitFailure<String, Object?>).cause,
          same(cause),
        );
        expect(called, isFalse);
      });

      test('flatMap retains ${outcome.name} from the next task', () async {
        final task = Task<void, Object?, ()>((_) => const Exit.success(null))
            .flatMap((_) => Task<int, Object?, ()>((_) => outcome.exit))
            .map((value) => 'unexpected:$value');

        expect(
          (await task.run(()) as ExitFailure<String, Object?>).cause,
          same(cause),
        );
      });
    }
  });
}

void _expectDefect<T, E>(Exit<T, E> exit, Object error, StackTrace trace) {
  final cause = (exit as ExitFailure<T, E>).cause;
  final defect = cause as CauseDefect<E>;
  expect(defect.error, same(error));
  expect(defect.stackTrace, same(trace));
}

E _expectedError<T, E>(Exit<T, E> exit) =>
    ((exit as ExitFailure<T, E>).cause as CauseFailure<E>).error;
