import 'dart:async';

import 'package:any/any.dart';
import 'package:any/concurrency.dart';
import 'package:test/test.dart';

import '../../example/concurrency.dart' as example;

void main() {
  test('independent producer/consumer groups share a resource limit', () async {
    final limit = Semaphore(2);
    var active = 0;
    var peak = 0;
    var closed = 0;
    final task = example.processJobs(
      Iterable.generate(20),
      semaphore: limit,
      capacity: 2,
      process: (job) => Task((context) async {
        active++;
        if (active > peak) peak = active;
        context.scope.addFinalizer((_) {
          active--;
          closed++;
        });
        await Future<void>.delayed(Duration.zero);
        return Exit.success(job * 2);
      }),
    );
    final results = await Future.wait([task.run(()), task.run(())]);
    for (final result in results) {
      expect(
        (result as ExitSuccess).value,
        unorderedEquals(Iterable.generate(20, (i) => i * 2)),
      );
    }
    expect(peak, 2);
    expect(closed, 40);
    expect(active, 0);
  });

  test(
    'worker failure stops a backpressured producer and retains its error',
    () async {
      var processed = 0;
      final task = example.processJobs(
        Iterable.generate(1000),
        semaphore: Semaphore(1),
        workers: 1,
        capacity: 1,
        process: (_) => Task((_) {
          processed++;
          return const Exit.failure(Cause.failure('job failed'));
        }),
      );
      final exit = await task.run(()) as ExitFailure;
      expect((exit.cause as CauseFailure).error, 'job failed');
      expect(processed, 1);
    },
  );

  test(
    'producer iteration failure drains buffered jobs and is reported',
    () async {
      final error = StateError('input');
      final cleaned = <int>[];
      Iterable<int> jobs() sync* {
        yield 1;
        yield 2;
        throw error;
      }

      final task = example.processJobs(
        jobs(),
        semaphore: Semaphore(1),
        process: (job) => Task((c) {
          c.scope.addFinalizer((_) => cleaned.add(job));
          return Exit.success(job);
        }),
      );
      final exit = await task.run(()) as ExitFailure;
      expect((exit.cause as CauseDefect).error, same(error));
      expect(cleaned, unorderedEquals([1, 2]));
    },
  );

  test('parent cancellation drains active cleanup before returning', () async {
    final controller = CancellationController();
    final ready = Completer<void>();
    final finish = Completer<void>();
    var cleaned = false;
    var done = false;
    final task = example.processJobs(
      Iterable.generate(1000),
      semaphore: Semaphore(1),
      capacity: 1,
      process: (job) => Task((c) async {
        c.scope.addFinalizer((_) {
          cleaned = true;
        });
        ready.complete();
        await finish.future;
        return Exit.success(job);
      }),
    );
    final future = task.run((), cancellation: controller.token).then((e) {
      done = true;
      return e;
    });
    await ready.future;
    controller.cancel(reason: 'stop');
    expect(done, isFalse);
    expect(cleaned, isFalse);
    finish.complete();
    final exit = await future as ExitFailure;
    final causes = exit.cause is CauseGroup
        ? (exit.cause as CauseGroup).causes
        : [exit.cause];
    expect(
      causes.whereType<CauseCancelled>().map((c) => c.reason),
      everyElement('stop'),
    );
    expect(causes.whereType<CauseCancelled>(), isNotEmpty);
    expect(cleaned, isTrue);
  });

  test('cleanup defects stop dispatch and remain visible', () async {
    final error = StateError('release');
    var jobs = 0;
    final task = example.processJobs(
      Iterable.generate(1000),
      semaphore: Semaphore(1),
      workers: 1,
      capacity: 1,
      process: (job) => Task((c) {
        jobs++;
        c.scope.addFinalizer((_) => throw error);
        return Exit.success(job);
      }),
    );
    final exit = await task.run(()) as ExitFailure;
    expect((exit.cause as CauseDefect).error, same(error));
    expect(jobs, 1);
  });
}
