import 'dart:async';

import 'package:any/any.dart';
import 'package:any/testing.dart';
import 'package:test/test.dart';

void main() {
  test('starts at the Unix epoch or the supplied UTC instant', () {
    final clock = TestClock();
    expect(clock.now(), DateTime.utc(1970));
    expect(clock.elapsed, Duration.zero);
    expect(clock.pendingCount, 0);

    final start = DateTime(2026, 10, 24, 12);
    expect(TestClock(start: start).now(), start.toUtc());
    expect(TestClock(start: start).now().isUtc, isTrue);
  });

  test(
    'timers observe deadline time and equal deadlines retain FIFO order',
    () {
      final clock = TestClock();
      final events = <(String, Duration)>[];
      void record(String name) => events.add((name, clock.elapsed));
      clock.schedule(const Duration(seconds: 2), () => record('second'));
      clock.schedule(const Duration(seconds: 1), () => record('first'));
      clock.schedule(const Duration(seconds: 2), () => record('third'));

      clock.advance(const Duration(seconds: 3));

      expect(events, [
        ('first', const Duration(seconds: 1)),
        ('second', const Duration(seconds: 2)),
        ('third', const Duration(seconds: 2)),
      ]);
      expect(clock.elapsed, const Duration(seconds: 3));
      expect(clock.now(), DateTime.utc(1970).add(const Duration(seconds: 3)));
      expect(clock.pendingCount, 0);
    },
  );

  test('zero and negative delays wait for an explicit advance', () {
    final clock = TestClock();
    var calls = 0;
    clock.schedule(Duration.zero, () => calls++);
    clock.schedule(const Duration(seconds: -1), () => calls++);
    expect(calls, 0);
    clock.advance(Duration.zero);
    expect(calls, 2);
  });

  test('cancellation is idempotent before and after a deadline', () {
    final clock = TestClock();
    var calls = 0;
    final cancel = clock.schedule(Duration.zero, () => calls++);
    cancel();
    cancel();
    expect(clock.pendingCount, 0);
    clock.advance(Duration.zero);
    expect(calls, 0);

    final cancelCompleted = clock.schedule(Duration.zero, () => calls++);
    clock.advance(Duration.zero);
    cancelCompleted();
    expect(calls, 1);
    expect(clock.pendingCount, 0);
  });

  test('callbacks can cancel peers and register due or future timers', () {
    final clock = TestClock();
    final events = <String>[];
    late void Function() cancel;
    clock.schedule(const Duration(seconds: 1), () {
      events.add('first');
      cancel();
      clock.schedule(Duration.zero, () => events.add('nested'));
      clock.schedule(const Duration(seconds: 2), () => events.add('future'));
    });
    cancel = clock.schedule(const Duration(seconds: 1), () {
      events.add('cancelled');
    });
    clock.schedule(const Duration(seconds: 1), () => events.add('peer'));

    clock.advance(const Duration(seconds: 2));
    expect(events, ['first', 'peer', 'nested']);
    expect(clock.pendingCount, 1);
    clock.advance(const Duration(seconds: 1));
    expect(events.last, 'future');
  });

  test('wall-clock changes do not move deadlines or monotonic time', () {
    final clock = TestClock();
    var fired = false;
    clock.schedule(const Duration(seconds: 2), () => fired = true);
    clock.advance(const Duration(seconds: 1));
    clock.setWallTime(DateTime.utc(2020));
    expect(clock.elapsed, const Duration(seconds: 1));
    expect(fired, isFalse);
    clock.advance(const Duration(seconds: 1));
    expect(fired, isTrue);
    expect(clock.now(), DateTime.utc(2020).add(const Duration(seconds: 1)));
  });

  test(
    'callback errors reach their registration zone and other timers run',
    () {
      final clock = TestClock();
      final zoneKey = Object();
      final errors = <Object>[];
      final error = StateError('callback');
      var reachedNext = false;
      runZonedGuarded(
        () {
          clock.schedule(Duration.zero, () {
            expect(Zone.current[zoneKey], 'registered');
            throw error;
          });
        },
        (error, _) => errors.add(error),
        zoneValues: {zoneKey: 'registered'},
      );
      clock.schedule(Duration.zero, () => reachedNext = true);

      clock.advance(Duration.zero);
      expect(errors, [same(error)]);
      expect(reachedNext, isTrue);
    },
  );

  test('advance does not run asynchronous continuations', () async {
    final clock = TestClock();
    final completed = Completer<void>();
    var continued = false;
    final continuation = completed.future.then((_) => continued = true);
    clock.schedule(Duration.zero, completed.complete);

    clock.advance(Duration.zero);
    expect(completed.isCompleted, isTrue);
    expect(continued, isFalse);
    await continuation;
    expect(continued, isTrue);
  });

  test('rejects negative and unrepresentable advances without mutation', () {
    final clock = TestClock();
    clock.schedule(Duration.zero, () {});
    expect(
      () => clock.advance(const Duration(microseconds: -1)),
      throwsArgumentError,
    );
    expect(
      () => clock.advance(const Duration(microseconds: 9007199254740992)),
      throwsRangeError,
    );
    expect(clock.elapsed, Duration.zero);
    expect(clock.pendingCount, 1);
    clock.advance(const Duration(microseconds: 9007199254740991));
    expect(
      () => clock.schedule(const Duration(microseconds: 1), () {}),
      throwsRangeError,
    );
  });

  test('rejects reentrant advance without changing outer target', () {
    final clock = TestClock();
    clock.schedule(const Duration(seconds: 1), () {
      expect(() => clock.advance(Duration.zero), throwsStateError);
    });
    clock.advance(const Duration(seconds: 2));
    expect(clock.elapsed, const Duration(seconds: 2));
  });

  test(
    'can replace the runtime clock through the public entrypoints',
    () async {
      final clock = TestClock();
      final runtime = Runtime(context: Context.from([Clock.key.bind(clock)]));
      addTearDown(runtime.close);
      expect(await runtime.runOrThrow(Task.service(Clock.key)), same(clock));
    },
  );
}
