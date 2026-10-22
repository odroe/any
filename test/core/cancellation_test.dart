import 'dart:async';

import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  group('CancellationController', () {
    test('publishes the first request before notifying listeners', () {
      final controller = CancellationController();
      final token = controller.token;
      final reason = Object();
      var calls = 0;
      token.onCancel((received) {
        calls++;
        expect(token.isCancelled, isTrue);
        expect(token.reason, same(reason));
        expect(received, same(reason));
      });

      expect(token.isCancelled, isFalse);
      expect(token.reason, isNull);
      controller.cancel(reason: reason);
      controller.cancel(reason: 'ignored');

      expect(controller.token, same(token));
      expect(token.reason, same(reason));
      expect(calls, 1);
    });

    test('preserves null as the first cancellation reason', () {
      final controller = CancellationController();
      controller.cancel();
      controller.cancel(reason: 'ignored');

      expect(controller.token.isCancelled, isTrue);
      expect(controller.token.reason, isNull);
      var called = false;
      controller.token.onCancel((reason) {
        called = true;
        expect(reason, isNull);
      });
      expect(called, isTrue);
    });

    test('notifies duplicate registrations independently in order', () {
      final controller = CancellationController();
      final events = <String>[];
      void listener(Object? _) => events.add('duplicate');
      controller.token.onCancel(listener);
      controller.token.onCancel((_) => events.add('middle'));
      controller.token.onCancel(listener);

      controller.cancel();

      expect(events, ['duplicate', 'middle', 'duplicate']);
    });

    test(
      'removes only the selected registration and tolerates repeated removal',
      () {
        final controller = CancellationController();
        var calls = 0;
        void listener(Object? _) => calls++;
        final removeFirst = controller.token.onCancel(listener);
        final removeSecond = controller.token.onCancel(listener);

        removeFirst();
        removeFirst();
        controller.cancel();
        removeSecond();
        removeSecond();

        expect(calls, 1);
      },
    );

    test('can register again after removing the last listener', () {
      final controller = CancellationController();
      final events = <String>[];
      final remove = controller.token.onCancel((_) => events.add('removed'));
      remove();
      controller.token.onCancel((_) => events.add('remaining'));

      controller.cancel();

      expect(events, ['remaining']);
    });

    test(
      'allows removal during notification and ignores reentrant requests',
      () {
        final controller = CancellationController();
        final reason = Object();
        final events = <String>[];
        late void Function() removeFirst;
        late void Function() removeSecond;
        removeFirst = controller.token.onCancel((_) {
          events.add('first');
          removeFirst();
          removeSecond();
          controller.cancel(reason: 'ignored');
        });
        removeSecond = controller.token.onCancel((_) => events.add('second'));
        controller.token.onCancel((_) => events.add('third'));

        controller.cancel(reason: reason);

        expect(events, ['first', 'third']);
        expect(controller.token.reason, same(reason));
      },
    );

    test(
      'immediately notifies late listeners, including during cancellation',
      () {
        final controller = CancellationController();
        final reason = Object();
        final events = <String>[];
        controller.token.onCancel((_) {
          events.add('first');
          final remove = controller.token.onCancel((received) {
            expect(received, same(reason));
            events.add('late');
          });
          remove();
          remove();
          events.add('after late registration');
        });
        controller.token.onCancel((_) => events.add('second'));

        controller.cancel(reason: reason);
        final remove = controller.token.onCancel((_) => events.add('last'));
        remove();

        expect(events, [
          'first',
          'late',
          'after late registration',
          'second',
          'last',
        ]);
      },
    );

    for (final alreadyCancelled in [false, true]) {
      test(
        'reports listener errors in their registration zone (late: $alreadyCancelled)',
        () {
          final controller = CancellationController();
          final error = Object();
          final trace = StackTrace.fromString('listener trace');
          final errors = <AsyncError>[];
          final marker = Object();
          var followed = false;
          if (alreadyCancelled) controller.cancel();

          runZonedGuarded(
            () {
              controller.token.onCancel((_) {
                expect(Zone.current[marker], 'registration');
                Error.throwWithStackTrace(error, trace);
              });
            },
            (error, stackTrace) => errors.add(AsyncError(error, stackTrace)),
            zoneValues: {marker: 'registration'},
          );
          controller.token.onCancel((_) => followed = true);
          controller.cancel();

          expect(errors, hasLength(1));
          expect(errors.single.error, same(error));
          expect(errors.single.stackTrace, same(trace));
          expect(followed, isTrue);
        },
      );
    }

    test('supports the documented timer cancellation workflow', () {
      final controller = CancellationController();
      final timer = Timer(
        const Duration(seconds: 1),
        () => fail('Timer fired.'),
      );
      addTearDown(timer.cancel);
      final removeListener = controller.token.onCancel((_) => timer.cancel());

      controller.cancel(reason: 'No longer needed.');
      removeListener();

      expect(timer.isActive, isFalse);
    });
  });

  test('CancellationToken.none stays inactive and never invokes listeners', () {
    final token = CancellationToken.none;
    final remove = token.onCancel((_) => fail('Unexpected cancellation.'));
    CancellationController().cancel(reason: 'unrelated');
    remove();
    remove();

    expect(token, same(CancellationToken.none));
    expect(token.isCancelled, isFalse);
    expect(token.reason, isNull);
  });
}
