import 'dart:async';
import 'dart:io';

import 'package:any/any.dart';
import 'package:any/concurrency.dart';

/// Measures median microseconds per completed task across five warmed samples.
///
/// Run with `dart run benchmark/concurrency_benchmark.dart [count]`, or compile
/// to an executable for AOT comparisons. The default count is 2000. Results
/// include task scopes and cleanup, and exclude construction of the task itself.
/// These synthetic cases measure scheduling overhead, not application latency.
Future<void> main(List<String> arguments) async {
  final count = arguments.isEmpty ? 2000 : int.parse(arguments.single);
  if (count < 1) throw ArgumentError.value(count, 'count');
  final task = Task<int, Never, ()>((_) => const Exit.success(1));
  final semaphore = Semaphore(1);
  final limited = semaphore.withPermit(task);
  final cases = <String, Future<void> Function()>{
    'task_sequential': () async {
      for (var i = 0; i < count; i++) {
        _check(await task.run(()));
      }
    },
    'semaphore_uncontended': () async {
      for (var i = 0; i < count; i++) {
        _check(await limited.run(()));
      }
    },
    'semaphore_contended': () async {
      final outcomes = await Future.wait([
        for (var i = 0; i < count; i++) limited.run(()),
      ]);
      outcomes.forEach(_check);
    },
    'all_8': () async {
      _check(await all(List.filled(count, task), concurrency: 8).run(()));
    },
    'all_settled_8': () async {
      _check(
        await allSettled(List.filled(count, task), concurrency: 8).run(()),
      );
    },
    'fork_join': () async {
      _check(
        await taskGroup<void, Never, ()>((group) async {
          final children = [for (var i = 0; i < count; i++) group.fork(task)];
          for (final child in children) {
            _check(await child.join());
          }
          return const Exit.success(null);
        }).run(()),
      );
    },
    'queue_roundtrip': () async {
      final queue = AsyncQueue<int>(capacity: 1);
      final roundtrip = queue.put<()>(1).flatMap((_) => queue.take<()>());
      for (var i = 0; i < count; i++) {
        _check(await roundtrip.run(()));
      }
      queue.close();
    },
    'queue_contended': () async {
      final queue = AsyncQueue<int>(capacity: 1);
      final puts = [for (var i = 0; i < count; i++) queue.put<()>(i).run(())];
      for (var i = 0; i < count; i++) {
        _check(await queue.take<()>().run(()));
      }
      (await Future.wait(puts)).forEach(_check);
      queue.close();
    },
    'stream_8': () async {
      await for (final exit in Stream.fromIterable(
        List.filled(count, 1),
      ).mapConcurrent((_) => task, dependencies: (), concurrency: 8)) {
        _check(exit);
      }
    },
  };
  print(
    'Dart ${Platform.version.split(' ').first}, ${Platform.operatingSystem}',
  );
  print('case,count,us_per_task');
  for (final entry in cases.entries) {
    for (var i = 0; i < 3; i++) {
      await entry.value();
    }
    final samples = <double>[];
    for (var i = 0; i < 5; i++) {
      final watch = Stopwatch()..start();
      await entry.value();
      watch.stop();
      samples.add(watch.elapsedMicroseconds / count);
    }
    samples.sort();
    print('${entry.key},$count,${samples[2].toStringAsFixed(3)}');
  }
}

void _check(Exit<Object?, Object?> exit) {
  if (exit is ExitFailure) throw StateError('Unexpected failure: $exit');
}
