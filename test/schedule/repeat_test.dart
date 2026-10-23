import 'dart:async';

import 'package:any/any.dart';
import 'package:any/schedule.dart';
import 'package:test/test.dart';

void main() {
  test('executes once plus each delay and returns the final value', () async {
    var attempts = 0;
    final events = <String>[];
    final task = Task<int, Never, ()>((context) {
      final attempt = ++attempts;
      events.add('run $attempt');
      context.scope.addFinalizer((_) => events.add('close $attempt'));
      return Exit.success(attempt);
    }).repeat(delays: const [Duration.zero, Duration.zero]);
    expect(attempts, 0);
    expect((await task.run(()) as ExitSuccess).value, 3);
    expect(events, [
      'run 1',
      'close 1',
      'run 2',
      'close 2',
      'run 3',
      'close 3',
    ]);
    expect((await task.run(()) as ExitSuccess).value, 6);
  });

  test('empty policy still runs once and handles nullable values', () async {
    var attempts = 0;
    final exit = await Task<int?, Never, ()>((_) {
      attempts++;
      return const Exit.success(null);
    }).repeat(delays: []).run(());
    expect((exit as ExitSuccess).value, isNull);
    expect(attempts, 1);
  });

  test('failure stops repetitions without reading the next delay', () async {
    var attempts = 0;
    var read = 0;
    Iterable<Duration> delays() sync* {
      for (var i = 0; i < 10; i++) {
        read++;
        yield Duration.zero;
      }
    }

    final exit = await Task<int, String, ()>((_) {
      attempts++;
      return attempts == 3
          ? const Exit.failure(Cause.failure('stop'))
          : Exit.success(attempts);
    }).repeat(delays: delays()).run(());
    expect(((exit as ExitFailure).cause as CauseFailure).error, 'stop');
    expect(read, 2);
    expect(attempts, 3);
  });

  test('cleanup failure stops repetition', () async {
    final error = Object();
    var attempts = 0;
    final exit = await Task<int, Never, ()>((context) {
      attempts++;
      context.scope.addFinalizer((_) => throw error);
      return const Exit.success(1);
    }).repeat(delays: const [Duration.zero]).run(());
    expect(((exit as ExitFailure).cause as CauseDefect).error, same(error));
    expect(attempts, 1);
  });

  test(
    'infinite zero-delay policy yields so cancellation can stop it',
    () async {
      final controller = CancellationController();
      var attempts = 0;
      Iterable<Duration> forever() sync* {
        while (true) {
          yield Duration.zero;
        }
      }

      final task = Task<int, Never, ()>((_) {
        attempts++;
        if (attempts == 3) Timer.run(() => controller.cancel(reason: 'done'));
        return Exit.success(attempts);
      }).repeat(delays: forever());
      final exit = await task.run((), cancellation: controller.token);
      expect(((exit as ExitFailure).cause as CauseCancelled).reason, 'done');
      expect(attempts, 3);
    },
  );

  test('iteration errors preserve the thrown object and trace', () async {
    final error = Object();
    final trace = StackTrace.current;
    Iterable<Duration> delays() sync* {
      Error.throwWithStackTrace(error, trace);
    }

    final exit = await Task<int, Never, ()>((_) => const Exit.success(1))
        .repeat(delays: delays())
        .run(());
    final cause = (exit as ExitFailure).cause as CauseDefect;
    expect(cause.error, same(error));
    expect(cause.stackTrace, same(trace));
  });

  test(
    'many executions do not accumulate stack or keep child scopes open',
    () async {
      var runs = 0;
      var closes = 0;
      final exit = await Task<int, Never, ()>((context) {
        expect(closes, runs);
        runs++;
        context.scope.addFinalizer((_) => closes++);
        return Exit.success(runs);
      }).repeat(delays: Iterable.generate(9999, (_) => Duration.zero)).run(());
      expect((exit as ExitSuccess).value, 10000);
      expect(closes, 10000);
    },
  );
}
