import 'dart:async';

import 'package:any/any.dart';
import 'package:any/concurrency.dart';
import 'package:any/resource.dart';
import 'package:test/test.dart';

void main() {
  group('all', () {
    for (final concurrency in [0, -1]) {
      test('rejects concurrency $concurrency before reading input', () {
        final tasks = _Iterable<Task<int, Never, ()>>(
          () => fail('must not read input'),
        );

        expect(
          () => all(tasks, concurrency: concurrency),
          throwsA(
            isA<RangeError>().having(
              (error) => error.name,
              'name',
              'concurrency',
            ),
          ),
        );
      });
    }

    for (final concurrency in [1, 1000000]) {
      test('handles empty input with concurrency $concurrency', () async {
        final task = all<int, Never, ()>([], concurrency: concurrency);

        expect(
          (await task.run(()) as ExitSuccess<List<int>, Never>).value,
          isEmpty,
        );
      });
    }

    test(
      'supports the documented workflow and a limit larger than the input',
      () async {
        final tasks = [
          for (final value in [1, 2, 3])
            Task<int, Never, int>(
              (context) => Exit.success(value * context.dependencies),
            ),
        ];

        final exit = await all(tasks, concurrency: 1000000).run(10);

        expect((exit as ExitSuccess<List<int>, Never>).value, [10, 20, 30]);
      },
    );

    test('iterates lazily and starts each run with fresh state', () async {
      final gate = Completer<Exit<int, Never>>();
      final started = Completer<void>();
      var iterations = 0;
      var reads = 0;
      var bodies = 0;
      Iterable<Task<int, Never, ()>> tasks() sync* {
        iterations++;
        for (var i = 0; i < 3; i++) {
          reads++;
          yield Task((_) {
            bodies++;
            if (i == 0) {
              if (!started.isCompleted) started.complete();
              return gate.future;
            }
            return Exit.success(i);
          });
        }
      }

      final task = all(tasks(), concurrency: 1);
      expect(iterations, 0);
      final first = task.run(());
      expect(iterations, 0);
      await started.future;
      expect(iterations, 1);
      expect(reads, 1);
      expect(bodies, 1);

      gate.complete(const Exit.success(0));
      expect((await first as ExitSuccess<List<int>, Never>).value, [0, 1, 2]);
      expect((await task.run(()) as ExitSuccess<List<int>, Never>).value, [
        0,
        1,
        2,
      ]);
      expect(iterations, 2);
      expect(reads, 6);
      expect(bodies, 6);
    });

    test('bounds active children and returns out-of-order completions in input order', () async {
      final gates = List.generate(4, (_) => Completer<Exit<int, Never>>());
      final started = List.generate(4, (_) => Completer<void>());
      var active = 0;
      var maximum = 0;
      var reads = 0;
      final tasks = Iterable.generate(4, (index) {
        reads++;
        return Task<int, Never, ()>((context) {
          active++;
          if (active > maximum) maximum = active;
          context.scope.addFinalizer((_) => active--);
          started[index].complete();
          return gates[index].future;
        });
      });
      var finished = false;
      final running = all(tasks, concurrency: 2).run(()).then((exit) {
        finished = true;
        return exit;
      });
      await Future.wait([started[0].future, started[1].future]);
      expect(reads, 2);
      expect(active, 2);

      gates[1].complete(const Exit.success(1));
      await started[2].future;
      expect(reads, 3);
      gates[2].complete(const Exit.success(2));
      await started[3].future;
      gates[3].complete(const Exit.success(3));
      await Future<void>(() {});
      expect(finished, isFalse);
      expect(active, 1);

      gates[0].complete(const Exit.success(0));
      expect((await running as ExitSuccess<List<int>, Never>).value, [
        0,
        1,
        2,
        3,
      ]);
      expect(maximum, 2);
      expect(active, 0);
    });

    test(
      'keeps a concurrency slot occupied until resource cleanup finishes',
      () async {
        final cleaning = Completer<void>();
        final cleanup = Completer<void>();
        var nextStarted = false;
        final first = acquireRelease<int, Never, ()>(
          acquire: (_) => const Exit.success(1),
          release: (value, exit) {
            expect(value, 1);
            expect((exit as ExitSuccess<int, Never>).value, 1);
            cleaning.complete();
            return cleanup.future;
          },
        );
        final next = Task<int, Never, ()>((_) {
          nextStarted = true;
          return const Exit.success(2);
        });
        final running = all([first, next], concurrency: 1).run(());
        await cleaning.future;
        await Future<void>(() {});
        expect(nextStarted, isFalse);

        cleanup.complete();
        expect((await running as ExitSuccess<List<int>, Never>).value, [1, 2]);
        expect(nextStarted, isTrue);
      },
    );

    test(
      'closes child scopes while leaving a borrowed parent scope open',
      () async {
        final scope = Scope();
        final dependencies = <String>[];
        final context = Context(scope: scope, dependencies: dependencies);
        final children = <Scope>[];
        final cleanupExits = <Exit<void, Object?>>[];
        var parentClosed = false;
        scope.addFinalizer((_) => parentClosed = true);
        final tasks = [
          for (var i = 0; i < 2; i++)
            Task<int, Never, List<String>>((child) {
              expect(child.dependencies, same(dependencies));
              expect(child.scope, isNot(same(scope)));
              children.add(child.scope);
              child.scope.addFinalizer(cleanupExits.add);
              return Exit.success(i);
            }),
        ];

        final exit = await all(tasks, concurrency: 2).runIn(context);

        expect((exit as ExitSuccess<List<int>, Never>).value, [0, 1]);
        expect(children[0], isNot(same(children[1])));
        expect(children.every((child) => child.isClosed), isTrue);
        expect(cleanupExits, hasLength(2));
        expect(
          cleanupExits.every((exit) => exit is ExitSuccess<int, Never>),
          isTrue,
        );
        expect(scope.isClosed, isFalse);
        expect(parentClosed, isFalse);
        await scope.close();
        expect(parentClosed, isTrue);
      },
    );

    test(
      'supports nullable values, void, future values, and widened types',
      () async {
        final nullable = all([
          Task<int?, Never, ()>((_) => const Exit.success(null)),
          Task<int?, Never, ()>((_) => const Exit.success(1)),
        ], concurrency: 2);
        expect(
          (await nullable.run(()) as ExitSuccess<List<int?>, Never>).value,
          [null, 1],
        );

        final empty = all([
          Task<void, Never, ()>((_) => const Exit.success(null)),
        ], concurrency: 1);
        expect(
          (await empty.run(()) as ExitSuccess<List<void>, Never>).value,
          hasLength(1),
        );

        final pending = Completer<int>();
        final futures = all([
          Task<Future<int>, Never, ()>((_) => Exit.success(pending.future)),
        ], concurrency: 1);
        final values = (await futures.run(
          (),
        ) as ExitSuccess<List<Future<int>>, Never>).value;
        expect(values.single, same(pending.future));
        expect(pending.isCompleted, isFalse);
        pending.complete(42);
        expect(await values.single, 42);

        final widened = all<num, String?, ()>([
          Task<int, Never, ()>((_) => const Exit.success(1)),
          Task<double, String?, ()>((_) => const Exit.success(2.5)),
        ], concurrency: 2);
        expect(
          (await widened.run(()) as ExitSuccess<List<num>, String?>).value,
          [1, 2.5],
        );
      },
    );

    test('captures a thrown iterator getter with its original trace', () async {
      final error = Object();
      final trace = StackTrace.fromString('iterator getter');
      final tasks = _Iterable<Task<int, Never, ()>>(
        () => Error.throwWithStackTrace(error, trace),
      );

      final exit = await all(tasks, concurrency: 2).run(());

      _expectDefect(exit, error, trace);
    });

    for (final generated in [false, true]) {
      test(
        'cancels and drains started children after iteration throws (generate: $generated)',
        () async {
          final error = Object();
          final trace = StackTrace.fromString('input access');
          final gates = List.generate(2, (_) => Completer<Exit<int, Never>>());
          final started = List.generate(2, (_) => Completer<void>());
          final cancelled = Completer<void>();
          var closed = 0;
          final children = [
            for (var i = 0; i < 2; i++)
              Task<int, Never, ()>((context) {
                context.scope.addFinalizer((_) => closed++);
                if (i == 0) {
                  context.cancellation.onCancel((_) => cancelled.complete());
                }
                started[i].complete();
                return gates[i].future;
              }),
          ];
          Iterable<Task<int, Never, ()>> source() sync* {
            yield* children;
            Error.throwWithStackTrace(error, trace);
          }

          final tasks = generated
              ? Iterable.generate(
                  3,
                  (index) => index < 2
                      ? children[index]
                      : Error.throwWithStackTrace(error, trace),
                )
              : source();
          var finished = false;
          final running = all(tasks, concurrency: 2).run(()).then((exit) {
            finished = true;
            return exit;
          });
          await Future.wait(started.map((value) => value.future));
          gates[1].complete(const Exit.success(1));
          await cancelled.future;
          expect(finished, isFalse);
          expect(closed, 1);

          gates[0].complete(const Exit.success(0));
          _expectDefect(await running, error, trace);
          expect(closed, 2);
        },
      );
    }

    test(
      'does not start work when input access requests parent cancellation',
      () async {
        final controller = CancellationController();
        var started = false;
        final tasks = _Iterable<Task<int, Never, ()>>(() {
          controller.cancel(reason: 'during iteration');
          return [
            Task<int, Never, ()>((_) {
              started = true;
              return const Exit.success(1);
            }),
          ].iterator;
        });

        final exit = await all(
          tasks,
          concurrency: 1,
        ).run((), cancellation: controller.token);

        expect(
          (exit as ExitFailure<List<int>, Never>).cause,
          isA<CauseCancelled<Never>>().having(
            (cause) => cause.reason,
            'reason',
            'during iteration',
          ),
        );
        expect(started, isFalse);
      },
    );

    for (final cancelOnAdvance in [false, true]) {
      test(
        'stops reading input after cancellation during iteration ($cancelOnAdvance)',
        () async {
          final controller = CancellationController();
          var reads = 0;
          var starts = 0;
          final tasks = _Iterable<Task<int, Never, ()>>(
            () => _Iterator(
              advance: () {
                if (cancelOnAdvance) controller.cancel(reason: 'input');
                return true;
              },
              read: () {
                reads++;
                controller.cancel(reason: 'input');
                return Task((_) {
                  starts++;
                  return const Exit.success(1);
                });
              },
            ),
          );

          final exit = await all(
            tasks,
            concurrency: 2,
          ).run((), cancellation: controller.token);

          expect(
            (exit as ExitFailure<List<int>, Never>).cause,
            isA<CauseCancelled<Never>>().having(
              (cause) => cause.reason,
              'reason',
              'input',
            ),
          );
          expect(reads, cancelOnAdvance ? 0 : 1);
          expect(starts, 0);
        },
      );
    }

    test(
      'processes 10000 children with bounded resource use and no stack growth',
      () async {
        var active = 0;
        var maximum = 0;
        final tasks = Iterable.generate(
          10000,
          (index) => Task<int, Never, ()>((context) async {
            active++;
            if (active > maximum) maximum = active;
            context.scope.addFinalizer((_) => active--);
            await Future<void>.value();
            return Exit.success(index);
          }),
        );

        final exit = await all(tasks, concurrency: 4).run(());

        expect(
          (exit as ExitSuccess<List<int>, Never>).value,
          orderedEquals(Iterable.generate(10000, (index) => index)),
        );
        expect(maximum, lessThanOrEqualTo(4));
        expect(active, 0);
      },
    );
  });
}

final class _Iterable<T> extends Iterable<T> {
  _Iterable(this._iterator);

  final Iterator<T> Function() _iterator;

  @override
  Iterator<T> get iterator => _iterator();
}

final class _Iterator<T> implements Iterator<T> {
  _Iterator({required this.advance, required this.read});

  final bool Function() advance;
  final T Function() read;

  @override
  bool moveNext() => advance();

  @override
  T get current => read();
}

void _expectDefect<T, E>(Exit<T, E> exit, Object error, StackTrace trace) {
  final defect = (exit as ExitFailure<T, E>).cause as CauseDefect<E>;
  expect(defect.error, same(error));
  expect(defect.stackTrace, same(trace));
}
