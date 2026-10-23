import 'dart:async';

import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  group('Clock', () {
    test('provides UTC wall time', () {
      final clock = Clock();

      expect(clock.now().isUtc, isTrue);
    });

    test('monotonic readings do not decrease', () async {
      final clock = Clock();
      final before = clock.elapsed;
      final completed = Completer<void>();
      final cancel = clock.schedule(Duration.zero, completed.complete);
      addTearDown(cancel);

      await completed.future;

      expect(before.isNegative, isFalse);
      expect(clock.elapsed.compareTo(before), greaterThanOrEqualTo(0));
    });

    test('can be bound using the standard service key', () {
      final clock = Clock();
      final context = Context.from([Clock.key.bind(clock)]);

      expect(Clock.key.name, 'clock');
      expect(context.read(Clock.key), same(clock));
    });

    for (final delay in [
      Duration.zero,
      const Duration(microseconds: -1),
      const Duration(milliseconds: 1),
    ]) {
      test('schedules $delay without running the callback inline', () async {
        final clock = Clock();
        final completed = Completer<bool>();
        var returned = false;
        final cancel = clock.schedule(delay, () {
          completed.complete(returned);
        });
        addTearDown(cancel);
        returned = true;

        expect(await completed.future, isTrue);

        cancel();
        cancel();
      });
    }

    test('cancelling prevents a pending callback and is idempotent', () async {
      final clock = Clock();
      var calls = 0;
      final cancel = clock.schedule(Duration.zero, () => calls++);
      addTearDown(cancel);

      cancel();
      cancel();

      final nextTimer = Completer<void>();
      final cancelNext = clock.schedule(Duration.zero, nextTimer.complete);
      addTearDown(cancelNext);
      await nextTimer.future;

      expect(calls, 0);
    });

    test('uses the registration zone for callbacks and their errors', () async {
      final clock = Clock();
      final zoneKey = Object();
      final zoneValue = Object();
      final error = StateError('timer callback');
      final reported = Completer<Object>();
      Object? callbackZoneValue;
      late void Function() cancel;

      runZonedGuarded(
        () {
          cancel = clock.schedule(Duration.zero, () {
            callbackZoneValue = Zone.current[zoneKey];
            throw error;
          });
        },
        (error, stackTrace) => reported.complete(error),
        zoneValues: {zoneKey: zoneValue},
      );
      addTearDown(cancel);

      expect(await reported.future, same(error));
      expect(callbackZoneValue, same(zoneValue));
    });
  });
}
