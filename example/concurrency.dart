import 'package:any/any.dart';
import 'package:any/concurrency.dart';
import 'package:any/resource.dart';

/// Processes a bounded queue with scoped workers and a shared concurrency limit.
///
/// Each processor owns its resource scope. The returned values are grouped by
/// worker, with unspecified order across workers. A failed worker stops its
/// siblings; the enclosing group then stops the producer and awaits cleanup.
/// A producer failure closes the queue and is reported after workers drain it.
Task<List<int>, Object, ()> processJobs(
  Iterable<int> jobs, {
  required Task<int, Object, ()> Function(int job) process,
  required Semaphore semaphore,
  int workers = 3,
  int capacity = 8,
}) {
  if (workers < 1) throw RangeError.range(workers, 1, null, 'workers');
  if (capacity < 1) throw RangeError.range(capacity, 1, null, 'capacity');
  return taskGroup((group) async {
    final queue = AsyncQueue<int>(capacity: capacity);
    group.context.scope.addFinalizer((_) => queue.close());
    final producer = group.fork(
      Task<void, Object, ()>((context) async {
        try {
          for (final job in jobs) {
            final exit = await queue.put<()>(job).runIn(context);
            if (exit case ExitFailure(:final cause)) return Exit.failure(cause);
          }
          return const Exit.success(null);
        } finally {
          queue.close();
        }
      }),
    );
    final consumers = group.fork(
      all([
        for (var i = 0; i < workers; i++)
          Task<List<int>, Object, ()>((context) async {
            final values = <int>[];
            while (true) {
              final next = await queue.take<()>().runIn(context);
              switch (next) {
                case ExitFailure(cause: CauseFailure(error: QueueClosed())):
                  return Exit.success(values);
                case ExitFailure(:final cause):
                  return Exit.failure(cause);
                case ExitSuccess(:final value):
                  final exit = await semaphore
                      .withPermit(scoped(process(value)))
                      .runIn(context);
                  switch (exit) {
                    case ExitSuccess(:final value):
                      values.add(value);
                    case ExitFailure(:final cause):
                      return Exit.failure(cause);
                  }
              }
            }
          }),
      ], concurrency: workers),
    );
    final results = await consumers.join();
    if (results case ExitFailure(:final cause)) return Exit.failure(cause);
    final produced = await producer.join();
    if (produced case ExitFailure(:final cause)) return Exit.failure(cause);
    return Exit.success([
      for (final values
          in (results as ExitSuccess<List<List<int>>, Object>).value)
        ...values,
    ]);
  });
}

Future<void> main() async {
  final closed = <int>[];
  final task = processJobs(
    Iterable.generate(6, (index) => index + 1),
    semaphore: Semaphore(2),
    capacity: 2,
    process: (job) => Task((context) async {
      context.scope.addFinalizer((_) => closed.add(job));
      await Future<void>.delayed(const Duration(milliseconds: 1));
      return Exit.success(job * job);
    }),
  );
  final exit = await task.run((), yieldEvery: 2048);
  switch (exit) {
    case ExitSuccess(:final value):
      print(value..sort()); // [1, 4, 9, 16, 25, 36]
      print('Released ${closed.length} job scopes.');
    case ExitFailure(:final cause):
      print('Processing failed: $cause');
  }
}
