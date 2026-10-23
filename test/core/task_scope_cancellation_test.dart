import 'dart:async';

import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  for (final depth in [10000, 10001]) {
    test('cancellation retains the body defect across $depth scopes', () async {
      final runtime = Runtime();
      final controller = CancellationController();
      final error = StateError('body failed before cancellation');
      final trace = StackTrace.fromString('body stack');
      var bodyFailed = false;
      Task<void, Never> task = Task.sync(() {
        Timer.run(() => controller.requestCancel(reason: 'timer'));
        bodyFailed = true;
        Error.throwWithStackTrace(error, trace);
      });
      for (var index = 0; index < depth; index++) {
        task = task.scoped();
      }

      final exit = await runtime.run(task, signal: controller.signal);
      expect(bodyFailed, isTrue);
      expect(controller.signal.isCancelled, isTrue);
      final cause = (exit as ExitFailure<void, Never>).cause;
      final reasons = switch (cause) {
        CauseGroup(:final causes) => causes,
        _ => [cause],
      };
      final defects = reasons.whereType<CauseDefect>().toList();
      expect(defects, hasLength(1));
      expect(defects.single.error, same(error));
      expect(defects.single.stackTrace, same(trace));
      expect(reasons.whereType<CauseCancelled>().single.reason, 'timer');
      expect(await runtime.close(), isA<ExitSuccess<void, Never>>());
    });
  }
}
