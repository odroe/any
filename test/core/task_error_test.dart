import 'dart:async';

import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  test(
    'mapError converts groups without changing defect or cancellation data',
    () async {
      final defect = Object();
      final trace = StackTrace.current;
      final reason = Object();
      final cause = Cause<String>.combine([
        const Cause.failure('missing'),
        Cause.defect(defect, trace),
        Cause.cancelled(reason: reason),
        const Cause.failure('other'),
      ]);
      final calls = <String>[];
      final task = Task<int, String, ()>((_) => Exit.failure(cause))
          .mapError((error) {
            calls.add(error);
            return error.length;
          });
      expect(calls, isEmpty);
      final exit = await task.run(()) as ExitFailure<int, int>;
      final causes = (exit.cause as CauseGroup<int>).causes;
      expect(calls, ['missing', 'other']);
      expect((causes[0] as CauseFailure).error, 7);
      expect((causes[1] as CauseDefect).error, same(defect));
      expect((causes[1] as CauseDefect).stackTrace, same(trace));
      expect((causes[2] as CauseCancelled).reason, same(reason));
      expect((causes[3] as CauseFailure).error, 5);
    },
  );

  test('mapError preserves nullable and future successful values', () async {
    final future = Future.value(1);
    final task = Task<Future<int>?, String, ()>((_) => Exit.success(future))
        .mapError((error) => error.length);
    expect((await task.run(()) as ExitSuccess).value, same(future));
    final empty = Task<int?, String, ()>((_) => const Exit.success(null))
        .mapError((error) => error.length);
    expect((await empty.run(()) as ExitSuccess).value, isNull);
  });

  test(
    'recover shares context and keeps resources until the workflow closes',
    () async {
      final events = <String>[];
      Context<()>? original;
      final task =
          Task<int, String, ()>((context) {
                original = context;
                context.scope.addFinalizer((exit) {
                  expect(exit, isA<ExitSuccess>());
                  events.add('source cleanup');
                });
                return const Exit.failure(Cause.failure('retry elsewhere'));
              })
              .recover(
                (error) => Task((context) {
                  expect(context, same(original));
                  expect(error, 'retry elsewhere');
                  expect(events, isEmpty);
                  context.scope.addFinalizer(
                    (_) => events.add('replacement cleanup'),
                  );
                  return const Exit.success(7);
                }),
              )
              .map((value) => value + 1);
      expect((await task.run(()) as ExitSuccess).value, 8);
      expect(events, ['replacement cleanup', 'source cleanup']);
    },
  );

  test('recover skips success, defects, cancellation, and groups', () async {
    final outcomes = <Exit<int, String>>[
      const Exit.success(1),
      Exit.failure(Cause.defect(Object(), StackTrace.current)),
      const Exit.failure(Cause.cancelled()),
      Exit.failure(
        Cause.combine([const Cause.failure('a'), const Cause.failure('b')]),
      ),
    ];
    for (final original in outcomes) {
      final exit = await Task<int, String, ()>((_) => original)
          .recover((_) => fail('Must not recover.'))
          .run(());
      expect(exit, same(original));
    }
  });

  test('cancelled failures are mapped but are not recovered', () async {
    final controller = CancellationController();
    final task = Task<int, String, ()>((_) {
      controller.cancel();
      return const Exit.failure(Cause.failure('bad'));
    }).mapError((e) => e.length).recover((_) => fail('Must not recover.'));
    final exit =
        await task.run((), cancellation: controller.token) as ExitFailure;
    expect((exit.cause as CauseFailure).error, 3);
  });

  test(
    'callback throws preserve identity and stack trace as defects',
    () async {
      final error = Object();
      final trace = StackTrace.current;
      final source = Task<int, String, ()>(
        (_) => const Exit.failure(Cause.failure('bad')),
      );
      final tasks = [
        source.mapError<String>((_) => Error.throwWithStackTrace(error, trace)),
        source.recover((_) => Error.throwWithStackTrace(error, trace)),
      ];
      for (final task in tasks) {
        final exit = await task.run(()) as ExitFailure;
        expect((exit.cause as CauseDefect).error, same(error));
        expect((exit.cause as CauseDefect).stackTrace, same(trace));
      }
    },
  );

  test(
    'widened tasks and changed error types compose without runtime casts',
    () async {
      final Task<int, Object, ()> widened = Task<int, Never, ()>(
        (_) => const Exit.success(1),
      );
      final result = widened
          .mapError((e) => '$e')
          .flatMap(
            (value) => Task<int, String, ()>(
              (_) => const Exit.failure(Cause.failure('missing')),
            ),
          )
          .recover((error) => Task((_) => Exit.success(error.length)));
      expect((await result.run(()) as ExitSuccess).value, 7);
    },
  );

  test(
    '10000 alternating error transforms and recoveries stay stack safe',
    () async {
      var task = Task<int, int, ()>(
        (_) => const Exit.failure(Cause.failure(0)),
      );
      for (var i = 0; i < 10000; i++) {
        task = task
            .mapError((value) => '$value')
            .mapError(int.parse)
            .recover(
              (error) => Task((_) => Exit.failure(Cause.failure(error + 1))),
            );
      }
      var microtasks = 0;
      final exit = await runZoned(
        () => task.run(()),
        zoneSpecification: ZoneSpecification(
          scheduleMicrotask: (self, parent, zone, callback) {
            microtasks++;
            parent.scheduleMicrotask(zone, callback);
          },
        ),
      ) as ExitFailure;
      expect((exit.cause as CauseFailure).error, 10000);
      expect(microtasks, lessThan(10));
    },
  );

  test('error handlers do not intercept later scope cleanup', () async {
    final error = Object();
    final exit = await Task<int, String, ()>((context) {
      context.scope.addFinalizer((_) => throw error);
      return const Exit.success(1);
    }).recover((_) => fail('Must not recover cleanup.')).run(()) as ExitFailure;
    expect((exit.cause as CauseDefect).error, same(error));
  });
}
