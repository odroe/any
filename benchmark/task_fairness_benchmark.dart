import 'dart:async';
import 'dart:io';

import 'package:any/any.dart';

/// Measures timer responsiveness and total latency during a synchronous chain.
///
/// Run with `dart run benchmark/task_fairness_benchmark.dart [length]`, or
/// compile to an executable. Defaults to 100000 map steps with a small amount
/// of synchronous work in each callback. Construction is outside the timer.
/// Each row is one run; repeat on the same machine before comparing latency.
/// Timer latency depends on OS load. The cancellation rows also show how much
/// work was skipped when an event-loop cancellation request was processed.
Future<void> main(List<String> arguments) async {
  final length = arguments.isEmpty ? 100000 : int.parse(arguments.single);
  if (length < 1) throw ArgumentError.value(length, 'length');
  print(
    'Dart ${Platform.version.split(' ').first}, ${Platform.operatingSystem}',
  );
  print(
    'steps,yield_every,cancel,total_us,timer_us,timer_during_run,callbacks,exit',
  );
  for (final cancel in [false, true]) {
    for (final budget in <int?>[null, 1024, 8192]) {
      final controller = CancellationController();
      final event = Completer<void>();
      var running = true;
      var duringRun = false;
      var timerMicros = 0;
      var callbacks = 0;
      final timerWatch = Stopwatch();
      var task = Task<int, Never, ()>((_) {
        timerWatch.start();
        Timer.run(() {
          timerMicros = timerWatch.elapsedMicroseconds;
          duringRun = running;
          if (cancel) controller.cancel();
          event.complete();
        });
        return const Exit.success(0);
      });
      for (var i = 0; i < length; i++) {
        task = task.map((value) {
          callbacks++;
          var result = value;
          for (var n = 0; n < 100; n++) {
            result = (result * 31 + n) & 0x7fffffff;
          }
          return result;
        });
      }
      final watch = Stopwatch()..start();
      final exit = await task.run(
        (),
        cancellation: controller.token,
        yieldEvery: budget,
      );
      watch.stop();
      running = false;
      await event.future;
      print(
        '$length,${budget ?? 'disabled'},$cancel,${watch.elapsedMicroseconds},'
        '$timerMicros,$duringRun,$callbacks,${exit.runtimeType}',
      );
    }
  }
}
