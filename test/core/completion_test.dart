import 'dart:async';

import 'package:any/src/core/internal/completion.dart';
import 'package:test/test.dart';

void main() {
  test('first publication retains its payload and delivers synchronously', () {
    final completion = Completion<Object>();
    final first = Object();
    final second = Object();
    final delivered = <Object>[];

    expect(completion.value, isNull);
    completion.listen(delivered.add);

    expect(completion.complete(first), isTrue);
    expect(completion.value, same(first));
    expect(delivered, [same(first)]);
    expect(completion.complete(second), isFalse);

    final unsubscribe = completion.listen(delivered.add);
    expect(delivered, [same(first), same(first)]);
    unsubscribe();
    unsubscribe();
    expect(completion.value, same(first));
  });

  test('each registration can be unsubscribed independently', () {
    final completion = Completion<Object>();
    final payload = Object();
    final delivered = <Object>[];
    final listener = delivered.add;
    final unsubscribe = completion.listen(listener);
    completion.listen(listener);

    unsubscribe();
    unsubscribe();
    expect(completion.complete(payload), isTrue);
    expect(delivered, [same(payload)]);
    unsubscribe();
  });

  test('unsubscribing during publication skips an undelivered listener', () {
    final completion = Completion<Object>();
    final events = <String>[];
    late void Function() unsubscribeSecond;
    completion.listen((_) {
      events.add('first');
      unsubscribeSecond();
      unsubscribeSecond();
    });
    unsubscribeSecond = completion.listen((_) => events.add('second'));
    completion.listen((_) => events.add('third'));

    expect(completion.complete(Object()), isTrue);
    expect(events, ['first', 'third']);
    unsubscribeSecond();
  });

  test(
    'listener failures use the registration zone and do not stop delivery',
    () {
      final completion = Completion<Object>();
      final payload = Object();
      final failure = StateError('listener');
      final lateFailure = StateError('late listener');
      final zoneKey = Object();
      final listenerZones = <Object?>[];
      final errors = <Object>[];
      final delivered = <Object>[];

      runZonedGuarded(
        () {
          completion.listen((_) {
            listenerZones.add(Zone.current[zoneKey]);
            throw failure;
          });
        },
        (error, _) => errors.add(error),
        zoneValues: {zoneKey: 'registration'},
      );
      completion.listen(delivered.add);

      expect(
        runZoned(
          () => completion.complete(payload),
          zoneValues: {zoneKey: 'publication'},
        ),
        isTrue,
      );
      expect(listenerZones, ['registration']);
      expect(errors, [same(failure)]);
      expect(delivered, [same(payload)]);

      var registrationReturned = false;
      runZonedGuarded(() {
        completion.listen((_) => throw lateFailure);
        registrationReturned = true;
      }, (error, _) => errors.add(error));
      completion.listen(delivered.add);

      expect(registrationReturned, isTrue);
      expect(errors, [same(failure), same(lateFailure)]);
      expect(delivered, [same(payload), same(payload)]);
      expect(completion.value, same(payload));
    },
  );

  test('publication is committed before listeners reenter the completion', () {
    final completion = Completion<Object>();
    final payload = Object();
    final events = <String>[];
    completion.listen((value) {
      expect(value, same(payload));
      expect(completion.value, same(payload));
      events.add('first');
      expect(completion.complete(Object()), isFalse);
      completion.listen((lateValue) {
        expect(lateValue, same(payload));
        events.add('late');
      });
    });
    completion.listen((_) => events.add('second'));

    expect(completion.complete(payload), isTrue);
    expect(events, ['first', 'late', 'second']);
    expect(completion.value, same(payload));
  });
}
