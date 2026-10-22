import 'dart:async';

import 'package:any/any.dart';
import 'package:any/schedule.dart';
import 'package:test/test.dart';

void main() {
  test('sleep is lazy and uses a fresh timer per run', () async {
    final timers = <Timer>[];
    await runZoned(
      () async {
        final task = sleep<()>(Duration.zero);
        expect(timers, isEmpty);
        expect(await task.run(()), isA<ExitSuccess>());
        expect(await task.run(()), isA<ExitSuccess>());
      },
      zoneSpecification: ZoneSpecification(
        createTimer: (self, parent, zone, duration, callback) {
          final timer = parent.createTimer(zone, duration, callback);
          timers.add(timer);
          return timer;
        },
      ),
    );
    expect(timers, hasLength(2));
    expect(timers.every((timer) => !timer.isActive), isTrue);
  });

  test('cancellation cancels the timer and preserves its reason', () async {
    final controller = CancellationController();
    final started = Completer<void>();
    Timer? timer;
    final reason = Object();
    final future = runZoned(
      () =>
          sleep<()>(const Duration(days: 1))
              .run((), cancellation: controller.token),
      zoneSpecification: ZoneSpecification(
        createTimer: (self, parent, zone, duration, callback) {
          timer = parent.createTimer(zone, duration, callback);
          started.complete();
          return timer!;
        },
      ),
    );
    await started.future;
    controller.cancel(reason: reason);
    final exit = await future as ExitFailure;
    expect((exit.cause as CauseCancelled).reason, same(reason));
    expect(timer!.isActive, isFalse);
  });

  test('pre-cancelled sleep creates no timer', () async {
    final controller = CancellationController()..cancel();
    final exit = await runZoned(
      () =>
          sleep<()>(const Duration(days: 1))
              .run((), cancellation: controller.token),
      zoneSpecification: ZoneSpecification(
        createTimer: (_, _, _, _, _) => fail('No timer expected.'),
      ),
    );
    expect((exit as ExitFailure).cause, isA<CauseCancelled>());
  });

  test(
    'nonpositive waits yield to the event queue and then allow composition',
    () async {
      for (final duration in [
        Duration.zero,
        const Duration(microseconds: -1),
      ]) {
        var eventRan = false;
        Timer.run(() => eventRan = true);
        final task = sleep<()>(duration).map((_) => eventRan);
        expect((await task.run(()) as ExitSuccess).value, isTrue);
      }
    },
  );

  test('cancel after completion does not affect another run', () async {
    final controller = CancellationController();
    final task = sleep<()>(Duration.zero);
    expect(
      await task.run((), cancellation: controller.token),
      isA<ExitSuccess>(),
    );
    controller.cancel();
    expect(await task.run(()), isA<ExitSuccess>());
  });
}
