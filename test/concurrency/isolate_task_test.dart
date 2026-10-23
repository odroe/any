@TestOn('vm')
library;

import 'dart:async';
import 'dart:isolate';

import 'package:any/any.dart';
import 'package:any/concurrency.dart';
import 'package:test/test.dart';

void main() {
  test('runs in a fresh named isolate and retains typed outcomes', () async {
    final task = isolateTask(_identity, debugName: 'any-test-worker');
    for (var i = 0; i < 2; i++) {
      final exit = await task.run(42) as ExitSuccess;
      expect(exit.value, ('any-test-worker', 42));
    }
    final failure = await isolateTask(_failure).run('expected') as ExitFailure;
    expect((failure.cause as CauseFailure).error, 'expected');
  });

  test('mutable dependencies are isolated and copied for each run', () async {
    final values = [1];
    final task = isolateTask(_append);
    expect((await task.run(values) as ExitSuccess).value, [1, 2]);
    expect((await task.run(values) as ExitSuccess).value, [1, 2]);
    expect(values, [1]);
  });

  test('worker throws retain diagnostic stack traces as defects', () async {
    final exit = await isolateTask(_throws).run(()) as ExitFailure;
    final cause = exit.cause as CauseDefect;
    expect(cause.error, isStateError);
    expect(cause.stackTrace.toString(), contains('_throws'));
  });

  test('unsendable dependencies become defects', () async {
    final port = ReceivePort();
    try {
      final exit = await isolateTask(_unsendable).run(port) as ExitFailure;
      expect(exit.cause, isA<CauseDefect>());
    } finally {
      port.close();
    }
  });

  test(
    'pre-cancellation skips spawning even with unsendable dependencies',
    () async {
      final controller = CancellationController()..cancel(reason: 'skip');
      final port = ReceivePort();
      try {
        final exit = await isolateTask(
          _unsendable,
        ).run(port, cancellation: controller.token) as ExitFailure;
        expect((exit.cause as CauseCancelled).reason, 'skip');
      } finally {
        port.close();
      }
    },
  );

  test('cancellation waits for the worker and its cleanup', () async {
    final controller = CancellationController();
    final port = ReceivePort();
    var done = false;
    final future = isolateTask(_waitForMessage)
        .run((port.sendPort, false), cancellation: controller.token)
        .then((exit) {
          done = true;
          return exit;
        });
    final sender = await port.first as SendPort;
    controller.cancel(reason: 'stop');
    expect(done, isFalse);
    sender.send(null);
    final exit = await future as ExitFailure;
    expect((exit.cause as CauseCancelled).reason, 'stop');
    port.close();
  });

  test('late worker failures remain failures after cancellation', () async {
    final controller = CancellationController();
    final port = ReceivePort();
    final future = isolateTask(_waitForMessage)
        .run((port.sendPort, true), cancellation: controller.token);
    final sender = await port.first as SendPort;
    controller.cancel();
    sender.send(null);
    expect(((await future as ExitFailure).cause as CauseFailure).error, 'late');
    port.close();
  });
}

Exit<(String?, int), Never> _identity(int value) =>
    Exit.success((Isolate.current.debugName, value));

Exit<int, String> _failure(String error) => Exit.failure(Cause.failure(error));

Exit<List<int>, Never> _append(List<int> values) =>
    Exit.success(values..add(2));

Exit<int, Never> _throws(() _) => throw StateError('worker');

Exit<int, Never> _unsendable(ReceivePort port) => const Exit.success(1);

Future<Exit<int, String>> _waitForMessage((SendPort, bool) dependencies) async {
  final inbox = ReceivePort();
  try {
    dependencies.$1.send(inbox.sendPort);
    await inbox.first;
    return dependencies.$2
        ? const Exit.failure(Cause.failure('late'))
        : const Exit.success(1);
  } finally {
    inbox.close();
  }
}
