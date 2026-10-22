import 'dart:async';
import 'dart:io';
import 'dart:math' as math;

import 'package:any/any.dart';

/// Measures task construction, execution, and microtask scheduling separately.
///
/// Run with `dart run benchmark/task_benchmark.dart`, optionally followed by
/// chain lengths. Defaults to 1, 1000, and 10000 steps, plus a primitive task.
/// For comparisons without JIT compilation, compile this file with
/// `dart compile exe benchmark/task_benchmark.dart -o /tmp/task_benchmark`.
/// Run both revisions on the same SDK and machine with the same arguments.
///
/// Times are medians of five warmed samples in microseconds per operation.
/// Execution reuses an already-built task and includes scope cleanup. Microtasks
/// are counted in a separate run so instrumentation does not affect timings.
/// These synthetic workloads do not measure allocation or application latency.
Future<void> main(List<String> arguments) async {
  final lengths = arguments.isEmpty
      ? [1, 1000, 10000]
      : arguments.map(int.parse).toList();
  if (lengths.any((length) => length < 0)) {
    throw ArgumentError('Chain lengths must be non-negative.');
  }

  print(
    'Dart ${Platform.version.split(' ').first}, ${Platform.operatingSystem}',
  );
  print('case,steps,build_us,run_us,microtasks');
  await _measure('primitive', 0, _Chain.map, false);
  for (final length in lengths) {
    for (final chain in _Chain.values) {
      await _measure(chain.name, length, chain, false);
      if (chain != _Chain.mixed) {
        await _measure('${chain.name}_failure', length, chain, true);
      }
    }
  }
}

enum _Chain { map, flatMap, mixed }

Task<int, String, ()> _build(int length, _Chain chain, bool failed) {
  var task = Task<int, String, ()>(
    (_) => failed
        ? const Exit.failure(Cause.failure('failed'))
        : const Exit.success(0),
  );
  for (var i = 0; i < length; i++) {
    if (chain == _Chain.map || chain == _Chain.mixed && i.isEven) {
      task = task.map((value) => value + 1);
    } else {
      final asynchronous = chain == _Chain.mixed && i % 100 == 99;
      task = task.flatMap(
        (value) => Task((_) {
          final exit = Exit<int, String>.success(value + 1);
          return asynchronous ? Future.value(exit) : exit;
        }),
      );
    }
  }
  return task;
}

// Keep the constructed graph observable without executing it in the timer.
late Task<int, String, ()> _built;

Future<void> _measure(
  String name,
  int length,
  _Chain chain,
  bool failed,
) async {
  final iterations = math.max(20, 200000 ~/ math.max(20, length));
  final task = _build(length, chain, failed);
  for (var i = 0; i < iterations; i++) {
    _built = _build(length, chain, failed);
    _check(await task.run(()), length, failed);
  }

  final construction = <double>[];
  final execution = <double>[];
  final timer = Stopwatch();
  for (var sample = 0; sample < 5; sample++) {
    timer
      ..reset()
      ..start();
    for (var i = 0; i < iterations; i++) {
      _built = _build(length, chain, failed);
    }
    timer.stop();
    construction.add(timer.elapsedMicroseconds / iterations);
    _check(await _built.run(()), length, failed);

    timer
      ..reset()
      ..start();
    for (var i = 0; i < iterations; i++) {
      _check(await task.run(()), length, failed);
    }
    timer.stop();
    execution.add(timer.elapsedMicroseconds / iterations);
  }

  var microtasks = 0;
  await runZoned(
    () async => _check(await task.run(()), length, failed),
    zoneSpecification: ZoneSpecification(
      scheduleMicrotask: (self, parent, zone, callback) {
        microtasks++;
        parent.scheduleMicrotask(zone, callback);
      },
    ),
  );
  construction.sort();
  execution.sort();
  print(
    '$name,$length,${construction[2].toStringAsFixed(2)},'
    '${execution[2].toStringAsFixed(2)},$microtasks',
  );
}

void _check(Exit<int, String> exit, int length, bool failed) {
  final valid = switch (exit) {
    ExitSuccess(:final value) => !failed && value == length,
    ExitFailure(cause: CauseFailure(error: 'failed')) => failed,
    ExitFailure() => false,
  };
  if (!valid) throw StateError('Unexpected benchmark outcome: $exit');
}
