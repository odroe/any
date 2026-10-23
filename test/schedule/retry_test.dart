import 'dart:async';

import 'package:any/any.dart';
import 'package:any/schedule.dart';
import 'package:test/test.dart';

void main() {
  test(
    'retries expected errors, releases each attempt, and stops at success',
    () async {
      var attempts = 0;
      var intervals = 0;
      final events = <String>[];
      final scopes = <Scope>[];
      Iterable<Duration> delays() sync* {
        for (var i = 0; i < 5; i++) {
          intervals++;
          yield Duration.zero;
        }
      }

      final task = Task<int, String, Object>((context) {
        final attempt = ++attempts;
        scopes.add(context.scope);
        events.add('run $attempt');
        context.scope.addFinalizer((_) async {
          await Future<void>.value();
          events.add('close $attempt');
        });
        return attempt == 3
            ? Exit.success(attempt)
            : const Exit.failure(Cause.failure('busy'));
      }).retry(delays: delays());
      expect(attempts, 0);
      expect((await task.run(Object()) as ExitSuccess).value, 3);
      expect(intervals, 2);
      expect(events, [
        'run 1',
        'close 1',
        'run 2',
        'close 2',
        'run 3',
        'close 3',
      ]);
      expect(scopes.toSet(), hasLength(3));
      expect(scopes.every((scope) => scope.isClosed), isTrue);
    },
  );

  test(
    'exhaustion retains the final failure and rereads the policy each run',
    () async {
      var attempts = 0;
      final task = Task<int, int, ()>(
        (_) => Exit.failure(Cause.failure(++attempts)),
      ).retry(delays: const [Duration.zero, Duration.zero]);
      expect(
        ((await task.run(()) as ExitFailure).cause as CauseFailure).error,
        3,
      );
      expect(
        ((await task.run(()) as ExitFailure).cause as CauseFailure).error,
        6,
      );
    },
  );

  test('when can reject without touching the delay iterable', () async {
    Iterable<Duration> delays() sync* {
      fail('No delay expected.');
    }

    final task = Task<int, String, ()>(
      (_) => const Exit.failure(Cause.failure('fatal')),
    ).retry(delays: delays(), when: (error) => error != 'fatal');
    expect(
      ((await task.run(()) as ExitFailure).cause as CauseFailure).error,
      'fatal',
    );
  });

  test('defects, cancellation, and groups are not retried', () async {
    final causes = <Cause<String>>[
      Cause.defect(Object(), StackTrace.current),
      const Cause.cancelled(),
      Cause.combine([const Cause.failure('a'), const Cause.failure('b')]),
    ];
    for (final cause in causes) {
      var calls = 0;
      final task =
          Task<int, String, ()>((_) {
            calls++;
            return Exit.failure(cause);
          }).retry(
            delays: const [Duration.zero],
            when: (_) => fail('No predicate expected.'),
          );
      expect((await task.run(()) as ExitFailure).cause, same(cause));
      expect(calls, 1);
    }
  });

  test(
    'cleanup failure prevents retry and follows the attempt failure',
    () async {
      final error = Object();
      var attempts = 0;
      final task = Task<int, String, ()>((context) {
        attempts++;
        context.scope.addFinalizer((_) => throw error);
        return const Exit.failure(Cause.failure('busy'));
      }).retry(delays: const [Duration.zero]);
      final cause = (await task.run(()) as ExitFailure).cause as CauseGroup;
      expect((cause.causes[0] as CauseFailure).error, 'busy');
      expect((cause.causes[1] as CauseDefect).error, same(error));
      expect(attempts, 1);
    },
  );

  test(
    'cancellation during a delay cancels its timer and skips the next attempt',
    () async {
      final waiting = Completer<void>();
      final controller = CancellationController();
      Timer? timer;
      var attempts = 0;
      final task = Task<int, String, ()>((_) {
        attempts++;
        return const Exit.failure(Cause.failure('busy'));
      }).retry(delays: const [Duration(days: 1)]);
      final running = runZoned(
        () => task.run((), cancellation: controller.token),
        zoneSpecification: ZoneSpecification(
          createTimer: (self, parent, zone, duration, callback) {
            timer = parent.createTimer(zone, duration, callback);
            waiting.complete();
            return timer!;
          },
        ),
      );
      await waiting.future;
      controller.cancel(reason: 'stop');
      expect(
        ((await running as ExitFailure).cause as CauseCancelled).reason,
        'stop',
      );
      expect(attempts, 1);
      expect(timer!.isActive, isFalse);
    },
  );

  test('policy throws become defects with their original trace', () async {
    final error = Object();
    final trace = StackTrace.current;
    Iterable<Duration> delays() sync* {
      Error.throwWithStackTrace(error, trace);
    }

    final source = Task<int, String, ()>(
      (_) => const Exit.failure(Cause.failure('busy')),
    );
    for (final task in [
      source.retry(delays: delays()),
      source.retry(
        delays: [],
        when: (_) => Error.throwWithStackTrace(error, trace),
      ),
    ]) {
      final cause = (await task.run(()) as ExitFailure).cause as CauseDefect;
      expect(cause.error, same(error));
      expect(cause.stackTrace, same(trace));
    }
  });

  test('cancellation inside policy does not start another attempt', () async {
    final controller = CancellationController();
    var calls = 0;
    final task =
        Task<int, String, ()>((_) {
          calls++;
          return const Exit.failure(Cause.failure('busy'));
        }).retry(
          delays: const [Duration.zero],
          when: (_) {
            controller.cancel();
            return true;
          },
        );
    final exit = await task.run((), cancellation: controller.token);
    expect((exit as ExitFailure).cause, isA<CauseFailure>());
    expect(calls, 1);
  });
}
