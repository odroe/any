import 'dart:async';
import 'dart:io';

import 'package:any/any.dart';
import 'package:any/resource.dart';
import 'package:test/test.dart';

void main() {
  group('acquireRelease', () {
    test('is lazy and leaves borrowed resources to the scope owner', () async {
      final scope = Scope();
      final dependencies = <Object>[];
      final controller = CancellationController();
      final context = Context(
        scope: scope,
        dependencies: dependencies,
        cancellation: controller.token,
      );
      final released = <Object>[];
      final cleanupExits = <Exit<void, Object?>>[];
      final task = acquireRelease<Object, Never, List<Object>>(
        acquire: (current) {
          expect(current, same(context));
          final resource = Object();
          current.dependencies.add(resource);
          return Exit.success(resource);
        },
        release: (resource, exit) {
          released.add(resource);
          cleanupExits.add(exit);
        },
      );

      expect(dependencies, isEmpty);
      final first = task.runIn(context);
      expect(dependencies, isEmpty);
      expect(
        (await first as ExitSuccess<Object, Never>).value,
        same(dependencies.first),
      );
      expect(
        (await task.runIn(context) as ExitSuccess<Object, Never>).value,
        same(dependencies.last),
      );
      expect(dependencies, hasLength(2));
      expect(released, isEmpty);
      expect(scope.isClosed, isFalse);

      const ownerExit = Exit<void, Object?>.failure(
        Cause.failure('owner exit'),
      );
      await scope.close(exit: ownerExit);
      await scope.close();

      expect(released, dependencies.reversed.toList());
      expect(cleanupExits, everyElement(same(ownerExit)));
    });

    test(
      'keeps a native file open for use and closes it before completion',
      () async {
        final directory = await Directory.systemTemp.createTemp(
          'any-resource-',
        );
        addTearDown(() => directory.delete(recursive: true));
        final source = await File('${directory.path}/example.txt')
            .writeAsString('hello');
        late RandomAccessFile resource;
        final length =
            acquireRelease<RandomAccessFile, Never, File>(
              acquire: (context) async {
                resource = await context.dependencies.open();
                return Exit.success(resource);
              },
              release: (file, _) async {
                await file.close();
              },
            ).flatMap(
              (file) => Task<int, Never, File>(
                (_) async => Exit.success(await file.length()),
              ),
            );

        final exit = await length.run(source);

        expect((exit as ExitSuccess<int, Never>).value, 5);
        await expectLater(
          resource.length(),
          throwsA(isA<FileSystemException>()),
        );
      },
    );

    test('reacquires resources in a fresh scope on each run', () async {
      final scopes = <Scope>[];
      final acquired = <Object>[];
      final released = <Object>[];
      final task = acquireRelease<Object, Never, ()>(
        acquire: (context) {
          scopes.add(context.scope);
          final resource = Object();
          acquired.add(resource);
          return Exit.success(resource);
        },
        release: (resource, _) => released.add(resource),
      );

      final first = await task.run(());
      final second = await task.run(());

      expect((first as ExitSuccess<Object, Never>).value, same(acquired.first));
      expect((second as ExitSuccess<Object, Never>).value, same(acquired.last));
      expect(acquired.first, isNot(same(acquired.last)));
      expect(scopes.first, isNot(same(scopes.last)));
      expect(scopes.every((scope) => scope.isClosed), isTrue);
      expect(released, acquired);
    });

    test('treats a nullable success as an acquired resource', () async {
      var releases = 0;
      const acquired = Exit<Object?, Never>.success(null);
      final task = acquireRelease<Object?, Never, ()>(
        acquire: (_) => acquired,
        release: (resource, exit) {
          expect(resource, isNull);
          expect(exit, same(acquired));
          releases++;
        },
      );

      expect(await task.run(()), same(acquired));
      expect(releases, 1);
    });

    test('accepts a wider failure type from later steps', () async {
      final cleanupExits = <Exit<void, Object?>>[];
      final Task<num, String, ()> task = acquireRelease<int, Never, ()>(
        acquire: (_) => const Exit.success(42),
        release: (value, exit) {
          expect(value, 42);
          cleanupExits.add(exit);
        },
      );
      const useExit = Exit<void, String>.failure(Cause.failure('use failed'));
      final workflow = task.flatMap(
        (_) => Task<void, String, ()>((_) => useExit),
      );

      expect(await workflow.run(()), same(useExit));
      expect(cleanupExits.single, same(useExit));
    });

    final error = StateError('acquisition failed');
    final trace = StackTrace.fromString('acquisition trace');
    final reason = Object();
    final failures = [
      (name: 'expected failure', cause: Cause<Object?>.failure(null)),
      (name: 'defect', cause: Cause<Object?>.defect(error, trace)),
      (name: 'cancellation', cause: Cause<Object?>.cancelled(reason: reason)),
      (
        name: 'combined failure',
        cause: Cause<Object?>.combine([
          Cause.failure(error),
          Cause.defect(error, trace),
          Cause.cancelled(reason: reason),
        ]),
      ),
    ];

    for (final failure in failures) {
      test('preserves ${failure.name} without releasing a resource', () async {
        final controller = CancellationController();
        final acquired = Exit<Object, Object?>.failure(failure.cause);
        var released = false;
        final task = acquireRelease<Object, Object?, ()>(
          acquire: (_) async {
            controller.cancel(reason: 'during acquisition');
            return acquired;
          },
          release: (_, _) => released = true,
        );

        expect(
          await task.run((), cancellation: controller.token),
          same(acquired),
        );
        expect(released, isFalse);
      });
    }

    for (final asynchronous in [false, true]) {
      test('captures acquisition throws (async: $asynchronous)', () async {
        var released = false;
        final task = acquireRelease<Object, StateError, ()>(
          acquire: (_) {
            if (asynchronous) {
              return Future<Exit<Object, StateError>>.error(error, trace);
            }
            Error.throwWithStackTrace(error, trace);
          },
          release: (_, _) => released = true,
        );

        final exit = await task.run(());

        final cause =
            (exit as ExitFailure<Object, StateError>).cause
                as CauseDefect<StateError>;
        expect(cause.error, same(error));
        expect(cause.stackTrace, same(trace));
        expect(released, isFalse);
      });
    }

    for (final beforeRun in [false, true]) {
      test(
        'skips acquisition cancelled before start (before run: $beforeRun)',
        () async {
          final controller = CancellationController();
          var called = false;
          final task = acquireRelease<Object, Never, ()>(
            acquire: (_) {
              called = true;
              return Exit.success(Object());
            },
            release: (_, _) => called = true,
          );
          if (beforeRun) controller.cancel(reason: reason);

          final running = task.run((), cancellation: controller.token);
          if (!beforeRun) controller.cancel(reason: reason);
          final exit = await running;

          final cause =
              (exit as ExitFailure<Object, Never>).cause
                  as CauseCancelled<Never>;
          expect(cause.reason, same(reason));
          expect(called, isFalse);
        },
      );
    }

    test(
      'registers a synchronous acquisition even when it requests cancellation',
      () async {
        final controller = CancellationController();
        final resource = Object();
        final cleanupExits = <Exit<void, Object?>>[];
        var used = false;
        final task =
            acquireRelease<Object, Never, ()>(
              acquire: (context) {
                expect(context.cancellation, same(controller.token));
                controller.cancel(reason: reason);
                return Exit.success(resource);
              },
              release: (value, exit) {
                expect(value, same(resource));
                cleanupExits.add(exit);
              },
            ).map((value) {
              used = true;
              return value;
            });

        final exit = await task.run((), cancellation: controller.token);

        final cause =
            (exit as ExitFailure<Object, Never>).cause as CauseCancelled<Never>;
        expect(cause.reason, same(reason));
        expect(cleanupExits.single, same(exit));
        expect(used, isFalse);
      },
    );

    test(
      'awaits a cancelled acquisition and its asynchronous release',
      () async {
        final controller = CancellationController();
        final resource = Object();
        final started = Completer<void>();
        final acquired = Completer<Exit<Object, Never>>();
        final releaseStarted = Completer<void>();
        final releaseFinished = Completer<void>();
        final cleanupExits = <Exit<void, Object?>>[];
        var used = false;
        var finished = false;
        final task =
            acquireRelease<Object, Never, ()>(
              acquire: (context) {
                expect(context.cancellation, same(controller.token));
                started.complete();
                return acquired.future;
              },
              release: (value, exit) async {
                expect(value, same(resource));
                cleanupExits.add(exit);
                releaseStarted.complete();
                await releaseFinished.future;
              },
            ).map((value) {
              used = true;
              return value;
            });
        final running = task.run((), cancellation: controller.token).then((
          exit,
        ) {
          finished = true;
          return exit;
        });
        await started.future;

        controller.cancel(reason: reason);
        await Future<void>(() {});
        expect(finished, isFalse);
        expect(cleanupExits, isEmpty);

        acquired.complete(Exit.success(resource));
        await releaseStarted.future;
        expect(finished, isFalse);
        expect(used, isFalse);

        releaseFinished.complete();
        final exit = await running;
        final cause =
            (exit as ExitFailure<Object, Never>).cause as CauseCancelled<Never>;
        expect(cause.reason, same(reason));
        expect(cleanupExits.single, same(exit));
      },
    );

    test(
      'retains the closing exit when cancellation arrives during release',
      () async {
        final controller = CancellationController();
        const acquired = Exit<int, Never>.success(42);
        final releaseStarted = Completer<void>();
        final releaseFinished = Completer<void>();
        var finished = false;
        final cleanupExits = <Exit<void, Object?>>[];
        final task = acquireRelease<int, Never, ()>(
          acquire: (_) => acquired,
          release: (_, exit) async {
            cleanupExits.add(exit);
            releaseStarted.complete();
            await releaseFinished.future;
          },
        );
        final running = task.run((), cancellation: controller.token).then((
          exit,
        ) {
          finished = true;
          return exit;
        });
        await releaseStarted.future;

        controller.cancel(reason: reason);
        await Future<void>(() {});
        expect(finished, isFalse);

        releaseFinished.complete();
        expect(await running, same(acquired));
        expect(cleanupExits.single, same(acquired));
      },
    );

    test('releases nested resources in order and preserves use and cleanup failures', () async {
      final events = <String>[];
      final cleanupExits = <Exit<void, Object?>>[];
      const useExit = Exit<void, String>.failure(Cause.failure('use failed'));
      final firstError = StateError('first cleanup');
      final secondError = StateError('second cleanup');
      final firstTrace = StackTrace.fromString('first cleanup trace');
      final secondTrace = StackTrace.fromString('second cleanup trace');
      final task =
          acquireRelease<String, String, ()>(
            acquire: (context) {
              context.scope.addFinalizer((_) => events.add('outer cleanup'));
              events.add('acquire first');
              return const Exit.success('first');
            },
            release: (resource, exit) {
              events.add('release $resource');
              cleanupExits.add(exit);
              Error.throwWithStackTrace(firstError, firstTrace);
            },
          ).flatMap(
            (first) =>
                acquireRelease<String, String, ()>(
                  acquire: (_) {
                    events.add('acquire second');
                    return const Exit.success('second');
                  },
                  release: (resource, exit) async {
                    events.add('release $resource');
                    cleanupExits.add(exit);
                    await Future<void>.value();
                    Error.throwWithStackTrace(secondError, secondTrace);
                  },
                ).flatMap(
                  (second) => Task<void, String, ()>((_) {
                    events.add('use $first and $second');
                    return useExit;
                  }),
                ),
          );

      final exit = await task.run(());

      expect(events, [
        'acquire first',
        'acquire second',
        'use first and second',
        'release second',
        'release first',
        'outer cleanup',
      ]);
      expect(cleanupExits, [same(useExit), same(useExit)]);
      final causes =
          ((exit as ExitFailure<void, String>).cause as CauseGroup<String>)
              .causes;
      expect(causes, hasLength(3));
      expect(causes.first, same((useExit as ExitFailure<void, String>).cause));
      final second = causes[1] as CauseDefect<String>;
      final first = causes[2] as CauseDefect<String>;
      expect(second.error, same(secondError));
      expect(second.stackTrace, same(secondTrace));
      expect(first.error, same(firstError));
      expect(first.stackTrace, same(firstTrace));
    });

    test('combines cancellation and release failure', () async {
      final controller = CancellationController();
      final cleanupError = StateError('cleanup failed');
      final cleanupTrace = StackTrace.fromString('cleanup trace');
      final task = acquireRelease<Object, Never, ()>(
        acquire: (_) {
          controller.cancel(reason: reason);
          return Exit.success(Object());
        },
        release: (_, _) =>
            Error.throwWithStackTrace(cleanupError, cleanupTrace),
      );

      final exit = await task.run((), cancellation: controller.token);

      final causes =
          ((exit as ExitFailure<Object, Never>).cause as CauseGroup<Never>)
              .causes;
      expect(causes, hasLength(2));
      expect((causes.first as CauseCancelled<Never>).reason, same(reason));
      final defect = causes.last as CauseDefect<Never>;
      expect(defect.error, same(cleanupError));
      expect(defect.stackTrace, same(cleanupTrace));
    });

    test('does not acquire resources in a closed scope', () async {
      final scope = Scope();
      await scope.close();
      var called = false;
      final task = acquireRelease<Object, Never, ()>(
        acquire: (_) {
          called = true;
          return Exit.success(Object());
        },
        release: (_, _) => called = true,
      );

      final exit = await task.runIn(Context(scope: scope, dependencies: ()));

      final cause =
          (exit as ExitFailure<Object, Never>).cause as CauseDefect<Never>;
      expect(cause.error, isA<StateError>());
      expect(called, isFalse);
    });

    test(
      'releases before returning if the scope closes during acquisition',
      () async {
        final scope = Scope();
        final started = Completer<void>();
        final acquired = Completer<Exit<Object, Never>>();
        final releaseStarted = Completer<void>();
        final releaseFinished = Completer<void>();
        final resource = Object();
        final cleanupExits = <Exit<void, Object?>>[];
        var finished = false;
        final task = acquireRelease<Object, Never, ()>(
          acquire: (_) {
            started.complete();
            return acquired.future;
          },
          release: (value, exit) async {
            expect(value, same(resource));
            cleanupExits.add(exit);
            releaseStarted.complete();
            await releaseFinished.future;
          },
        );
        final running = task
            .runIn(Context(scope: scope, dependencies: ()))
            .then((exit) {
              finished = true;
              return exit;
            });
        await started.future;
        await scope.close();

        acquired.complete(Exit.success(resource));
        await releaseStarted.future;
        expect(finished, isFalse);

        releaseFinished.complete();
        final exit = await running;
        final cause =
            (exit as ExitFailure<Object, Never>).cause as CauseDefect<Never>;
        expect(cause.error, isA<StateError>());
        expect(cleanupExits.single, same(exit));
        await scope.close();
        expect(cleanupExits, hasLength(1));
      },
    );

    for (final asynchronous in [false, true]) {
      test(
        'preserves registration and release errors (async: $asynchronous)',
        () async {
          final scope = Scope();
          final cleanupError = StateError('cleanup failed');
          final cleanupTrace = StackTrace.fromString('cleanup trace');
          late Exit<void, Object?> cleanupExit;
          var releases = 0;
          final task = acquireRelease<Object, Never, ()>(
            acquire: (_) {
              unawaited(scope.close());
              return Exit.success(Object());
            },
            release: (_, exit) {
              cleanupExit = exit;
              releases++;
              if (asynchronous) {
                return Future<void>.error(cleanupError, cleanupTrace);
              }
              Error.throwWithStackTrace(cleanupError, cleanupTrace);
            },
          );

          final exit = await task.runIn(
            Context(scope: scope, dependencies: ()),
          );

          final causes =
              ((exit as ExitFailure<Object, Never>).cause as CauseGroup<Never>)
                  .causes;
          expect(causes, hasLength(2));
          expect(
            causes.first,
            same((cleanupExit as ExitFailure<void, Object?>).cause),
          );
          expect((causes.first as CauseDefect<Never>).error, isA<StateError>());
          final defect = causes.last as CauseDefect<Never>;
          expect(defect.error, same(cleanupError));
          expect(defect.stackTrace, same(cleanupTrace));
          expect(releases, 1);
          await scope.close();
          expect(releases, 1);
        },
      );
    }
  });
}
