import 'dart:async';

import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  group('Task evaluation', () {
    test('resumes a deep mixed chain across futures and value types', () async {
      var task = Task<int, Never, ()>((_) => const Exit.success(0));
      for (var i = 0; i < 10000; i++) {
        final asynchronous = i % 100 == 99;
        task = task
            .map((value) => (number: value))
            .flatMap(
              (value) => Task<String, Never, ()>((_) {
                final exit = Exit<String, Never>.success('${value.number + 1}');
                return asynchronous ? Future.value(exit) : exit;
              }).map(int.parse),
            );
      }

      expect((await task.run(()) as ExitSuccess<int, Never>).value, 10000);
    });

    test(
      'runs 10000 dynamically nested continuations without recursion',
      () async {
        Task<int, Never, ()> descend(int depth) =>
            Task<int, Never, ()>((_) => Exit.success(depth)).flatMap(
              (value) => value == 10000
                  ? Task((_) => Exit.success(value))
                  : descend(value + 1).map((result) => result - 1),
            );

        expect((await descend(0).run(()) as ExitSuccess<int, Never>).value, 0);
      },
    );

    for (final failAt in [0, 5000, 10000]) {
      test('short-circuits a deep chain at step $failAt', () async {
        final cause = Cause<String>.combine([
          const Cause.failure('expected'),
          Cause.defect(Object(), StackTrace.fromString('original trace')),
          const Cause.cancelled(reason: 'original reason'),
        ]);
        var called = 0;
        var task = Task<int, String, ()>(
          (_) => failAt == 0 ? Exit.failure(cause) : const Exit.success(0),
        );
        for (var i = 0; i < 10000; i++) {
          task = task.flatMap((value) {
            called++;
            return Task((_) {
              final next = value + 1;
              return next == failAt ? Exit.failure(cause) : Exit.success(next);
            });
          });
        }
        final mapped = task.map<String>((_) => fail('failure must skip map'));

        final exit = await mapped.run(()) as ExitFailure<String, String>;

        expect(exit.cause, same(cause));
        expect(called, failAt);
      });
    }

    test('defers a deep chain so cancellation can prevent its start', () async {
      final controller = CancellationController();
      var called = false;
      var task = Task<int, Never, ()>((_) {
        called = true;
        return const Exit.success(0);
      });
      for (var i = 0; i < 10000; i++) {
        task = task.map((value) => value + 1);
      }

      final running = task.run((), cancellation: controller.token);
      expect(called, isFalse);
      controller.cancel(reason: 'before start');

      _expectCancelled(await running, 'before start');
      expect(called, isFalse);
    });

    for (final operation in ['map', 'flatMap']) {
      test('observes cancellation inside a deep $operation chain', () async {
        final controller = CancellationController();
        var callbacks = 0;
        var bodies = 0;
        late Exit<void, Object?> cleanupExit;
        var task = Task<int, Never, ()>((context) {
          context.scope.addFinalizer((exit) => cleanupExit = exit);
          return const Exit.success(0);
        });
        for (var i = 0; i < 10000; i++) {
          int increment(int value) {
            callbacks++;
            if (value == 5000) controller.cancel(reason: 'inside step');
            return value + 1;
          }

          task = operation == 'map'
              ? task.map(increment)
              : task.flatMap((value) {
                  final next = increment(value);
                  return Task((_) {
                    bodies++;
                    return Exit.success(next);
                  });
                });
        }

        final exit = await task.run((), cancellation: controller.token);

        _expectCancelled(exit, 'inside step');
        expect(callbacks, 5001);
        expect(bodies, operation == 'map' ? 0 : 5000);
        expect(cleanupExit, same(exit));
      });
    }

    for (final outcome in ['success', 'failure', 'throw']) {
      test('waits for a suspended deep chain ending in $outcome', () async {
        final controller = CancellationController();
        final started = Completer<void>();
        final body = Completer<Exit<int, String>>();
        final error = Object();
        final trace = StackTrace.fromString('suspended step trace');
        const cause = Cause<String>.failure('original failure');
        final cleanupExits = <Exit<void, Object?>>[];
        var called = false;
        var task = Task<int, String, ()>((context) {
          context.scope.addFinalizer(cleanupExits.add);
          return const Exit.success(0);
        });
        for (var i = 0; i < 5000; i++) {
          task = task.map((value) => value + 1);
        }
        task = task.flatMap(
          (_) => Task((_) {
            started.complete();
            return body.future;
          }),
        );
        for (var i = 0; i < 5000; i++) {
          task = task.map((value) {
            called = true;
            return value + 1;
          });
        }
        var finished = false;
        final running = task.run((), cancellation: controller.token).then((
          exit,
        ) {
          finished = true;
          return exit;
        });
        await started.future;

        controller.cancel(reason: 'while suspended');
        await Future<void>(() {});
        expect(finished, isFalse);
        expect(cleanupExits, isEmpty);
        switch (outcome) {
          case 'success':
            body.complete(const Exit.success(42));
          case 'failure':
            body.complete(const Exit.failure(cause));
          case 'throw':
            body.completeError(error, trace);
        }
        final exit = await running;

        final failure = (exit as ExitFailure<int, String>).cause;
        switch (outcome) {
          case 'success':
            _expectCancelled(exit, 'while suspended');
          case 'failure':
            expect(failure, same(cause));
          case 'throw':
            final defect = failure as CauseDefect<String>;
            expect(defect.error, same(error));
            expect(defect.stackTrace, same(trace));
        }
        expect(called, isFalse);
        expect(cleanupExits.single, same(exit));
      });
    }

    test(
      'keeps shared prefixes independent across concurrent and repeated runs',
      () async {
        final firstGate = Completer<Exit<int, Never>>();
        final secondGate = Completer<Exit<int, Never>>();
        final bothStarted = Completer<void>();
        var starts = 0;
        var prefix = Task<int, Never, Future<Exit<int, Never>>>((context) {
          if (++starts == 2) bothStarted.complete();
          return context.dependencies;
        });
        for (var i = 0; i < 1000; i++) {
          prefix = prefix.map((value) => value + 1);
        }
        final left = prefix.flatMap(
          (value) => Task((_) => Exit.success(value * 2)),
        );
        final right = prefix.map((value) => '$value');
        final first = left.run(firstGate.future);
        final second = right.run(secondGate.future);
        await bothStarted.future;

        secondGate.complete(const Exit.success(2));
        expect((await second as ExitSuccess<String, Never>).value, '1002');
        firstGate.complete(const Exit.success(1));
        expect((await first as ExitSuccess<int, Never>).value, 2002);
        expect(
          (await left.run(
            Future.value(const Exit.success(3)),
          ) as ExitSuccess<int, Never>).value,
          2006,
        );
      },
    );

    test(
      'composes widened value and error types after an existing chain',
      () async {
        final Task<num, String?, ()> first =
            Task<int, Never, ()>((_) => const Exit.success(1))
                .map((value) => value + 1)
                .flatMap((value) => Task((_) => Exit.success(value + 1)));
        final task = first
            .map((value) => value + 0.5)
            .flatMap(
              (value) => Task<String?, String?, ()>(
                (_) => value == 3.5
                    ? const Exit.success(null)
                    : const Exit.failure(Cause.failure(null)),
              ),
            )
            .map((value) => value ?? 'empty');

        expect(
          (await task.run(()) as ExitSuccess<String, String?>).value,
          'empty',
        );

        final Task<num, String?, ()> impossible = Task<Never, Never, ()>(
          (_) => const Exit.failure(Cause.cancelled(reason: 'no value')),
        );
        _expectCancelled(
          await impossible.map((value) => '$value').run(()),
          'no value',
        );
      },
    );

    test('keeps tasks and exits returned by map as values', () async {
      var called = false;
      final value = Task<int, Never, ()>((_) {
        called = true;
        return const Exit.success(42);
      });
      final task = Task<void, Never, ()>((_) => const Exit.success(null))
          .map((_) => value);
      expect(
        (await task.run(()) as ExitSuccess<Task<int, Never, ()>, Never>).value,
        same(value),
      );
      expect(called, isFalse);

      const failure = Exit<int, String>.failure(Cause.failure('data'));
      final data = task.map((_) => failure);
      expect(
        (await data.run(()) as ExitSuccess<Exit<int, String>, Never>).value,
        same(failure),
      );
    });

    test(
      'runs callbacks in the calling zone before and after suspension',
      () async {
        final key = Object();
        final zones = <Object?>[];
        final task =
            Task<int, Never, ()>((_) {
                  zones.add(Zone.current[key]);
                  return const Exit.success(1);
                })
                .map((value) {
                  zones.add(Zone.current[key]);
                  return value + 1;
                })
                .flatMap(
                  (value) => Task((_) async {
                    zones.add(Zone.current[key]);
                    await Future<void>.value();
                    zones.add(Zone.current[key]);
                    return Exit.success(value + 1);
                  }),
                )
                .map((value) {
                  zones.add(Zone.current[key]);
                  return value + 1;
                });

        final exit = await runZoned(
          () => task.run(()),
          zoneValues: {key: 'run'},
        );

        expect((exit as ExitSuccess<int, Never>).value, 4);
        expect(zones, List.filled(5, 'run'));
      },
    );
  });
}

void _expectCancelled<T, E>(Exit<T, E> exit, Object? reason) {
  expect(
    (exit as ExitFailure<T, E>).cause,
    isA<CauseCancelled<E>>().having(
      (cause) => cause.reason,
      'reason',
      same(reason),
    ),
  );
}
