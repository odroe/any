import 'dart:async';

import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  group('CancellationController', () {
    test('starts with a stable, uncancelled signal', () {
      final controller = CancellationController();

      expect(controller.signal, same(controller.signal));
      expect(controller.signal.isCancelled, isFalse);
      expect(controller.signal.reason, isNull);
    });

    test('publishes the first reason before notifying listeners', () {
      final controller = CancellationController();
      final reason = Object();
      final seen = <Object?>[];
      controller.signal.onCancel((value) {
        expect(controller.signal.isCancelled, isTrue);
        expect(controller.signal.reason, same(reason));
        seen.add(value);
      });

      controller.requestCancel(reason: reason);
      controller.requestCancel(reason: 'later');

      expect(controller.signal.reason, same(reason));
      expect(seen, [same(reason)]);
    });

    test('preserves a first null reason', () {
      final controller = CancellationController();
      final seen = <Object?>[];
      controller.signal.onCancel(seen.add);

      controller.requestCancel();
      controller.requestCancel(reason: 'later');

      expect(controller.signal.isCancelled, isTrue);
      expect(controller.signal.reason, isNull);
      expect(seen, [null]);
    });

    test('notifies synchronously in registration order', () {
      final controller = CancellationController();
      final events = <String>[];
      controller.signal.onCancel((_) => events.add('first'));
      controller.signal.onCancel((_) => events.add('second'));

      controller.requestCancel();
      events.add('returned');

      expect(events, ['first', 'second', 'returned']);
    });

    test(
      'reentrant cancellation cannot replace the reason or notify twice',
      () {
        final controller = CancellationController();
        final seen = <Object?>[];
        controller.signal.onCancel((reason) {
          seen.add(reason);
          controller.requestCancel(reason: 'reentrant');
        });
        controller.signal.onCancel(seen.add);

        controller.requestCancel(reason: 'first');

        expect(seen, ['first', 'first']);
        expect(controller.signal.reason, 'first');
      },
    );

    test('does not schedule asynchronous notifications', () {
      var microtasks = 0;
      var calls = 0;
      runZoned(
        () {
          final controller = CancellationController();
          controller.signal.onCancel((_) => calls++);
          controller.requestCancel();
          controller.signal.onCancel((_) => calls++);
        },
        zoneSpecification: ZoneSpecification(
          scheduleMicrotask: (self, parent, zone, callback) {
            microtasks++;
            parent.scheduleMicrotask(zone, callback);
          },
        ),
      );

      expect(calls, 2);
      expect(microtasks, 0);
    });
  });

  group('onCancel', () {
    test('removing a subscription prevents notification and is idempotent', () {
      final controller = CancellationController();
      var calls = 0;
      final remove = controller.signal.onCancel((_) => calls++);

      remove();
      remove();
      controller.requestCancel();
      remove();

      expect(calls, 0);
    });

    test('registers the same callback independently', () {
      final controller = CancellationController();
      var calls = 0;
      void listener(Object? reason) => calls++;
      final removeFirst = controller.signal.onCancel(listener);
      controller.signal.onCancel(listener);
      controller.signal.onCancel(listener);

      removeFirst();
      controller.requestCancel();

      expect(calls, 2);
    });

    test('can remove a later listener while notifying', () {
      final controller = CancellationController();
      final events = <String>[];
      late void Function() removeSecond;
      controller.signal.onCancel((_) {
        events.add('first');
        removeSecond();
      });
      removeSecond = controller.signal.onCancel((_) => events.add('second'));
      controller.signal.onCancel((_) => events.add('third'));

      controller.requestCancel();

      expect(events, ['first', 'third']);
    });

    test('can remove itself while notifying', () {
      final controller = CancellationController();
      var calls = 0;
      late void Function() remove;
      remove = controller.signal.onCancel((_) {
        calls++;
        remove();
      });

      controller.requestCancel();
      remove();
      controller.requestCancel();

      expect(calls, 1);
    });

    test('notifies a late listener before returning its removal function', () {
      final controller = CancellationController();
      controller.requestCancel(reason: 'done');
      final events = <Object?>[];

      final remove = controller.signal.onCancel(events.add);
      events.add('returned');
      remove();
      remove();
      controller.requestCancel();

      expect(events, ['done', 'returned']);
    });

    test('notifies reentrant subscriptions immediately and only once', () {
      final controller = CancellationController();
      final events = <String>[];
      controller.signal.onCancel((_) {
        events.add('first');
        final remove = controller.signal.onCancel((reason) {
          expect(reason, 'initial');
          events.add('nested');
        });
        remove();
        events.add('first returned');
      });
      controller.signal.onCancel((_) => events.add('second'));

      controller.requestCancel(reason: 'initial');

      expect(events, ['first', 'nested', 'first returned', 'second']);
    });
  });

  group('listener errors', () {
    test('reports the original error and stack without stopping listeners', () {
      final errors = <(Object, StackTrace)>[];
      final controller = CancellationController(
        onError: (error, stackTrace) => errors.add((error, stackTrace)),
      );
      final error = StateError('listener failed');
      final stackTrace = StackTrace.fromString('listener stack');
      var laterCalls = 0;
      controller.signal.onCancel((_) {
        Error.throwWithStackTrace(error, stackTrace);
      });
      controller.signal.onCancel((_) => laterCalls++);

      controller.requestCancel();

      expect(errors, hasLength(1));
      expect(errors.single.$1, same(error));
      expect(errors.single.$2.toString(), stackTrace.toString());
      expect(laterCalls, 1);
    });

    test('reports errors from late listeners through the same handler', () {
      final errors = <Object>[];
      final controller = CancellationController(
        onError: (error, _) => errors.add(error),
      );
      final error = StateError('late listener failed');
      controller.requestCancel();

      final remove = controller.signal.onCancel((_) => throw error);
      remove();

      expect(errors, [same(error)]);
    });

    test('uses the creating zone when no error handler is supplied', () {
      late CancellationController controller;
      final errors = <(Object, StackTrace)>[];
      runZoned(
        () => controller = CancellationController(),
        zoneSpecification: ZoneSpecification(
          handleUncaughtError: (self, parent, zone, error, stackTrace) {
            errors.add((error, stackTrace));
          },
        ),
      );
      final error = StateError('listener failed');
      final stackTrace = StackTrace.fromString('creating zone stack');
      var laterCalls = 0;
      controller.signal.onCancel((_) {
        Error.throwWithStackTrace(error, stackTrace);
      });
      controller.signal.onCancel((_) => laterCalls++);

      controller.requestCancel();

      expect(errors, hasLength(1));
      expect(errors.single.$1, same(error));
      expect(errors.single.$2.toString(), stackTrace.toString());
      expect(laterCalls, 1);
    });

    test('reports a throwing error handler and continues notifying', () {
      late CancellationController controller;
      final listenerError = StateError('listener failed');
      final reporterError = StateError('reporter failed');
      final reported = <Object>[];
      final uncaught = <Object>[];
      runZoned(
        () {
          controller = CancellationController(
            onError: (error, _) {
              reported.add(error);
              throw reporterError;
            },
          );
        },
        zoneSpecification: ZoneSpecification(
          handleUncaughtError: (self, parent, zone, error, stackTrace) {
            uncaught.add(error);
          },
        ),
      );
      var laterCalls = 0;
      controller.signal.onCancel((_) => throw listenerError);
      controller.signal.onCancel((_) => laterCalls++);

      controller.requestCancel();

      expect(reported, [same(listenerError)]);
      expect(uncaught, [same(reporterError)]);
      expect(laterCalls, 1);
    });

    test('allows the error handler to subscribe and request cancellation', () {
      late CancellationController controller;
      final events = <Object?>[];
      controller = CancellationController(
        onError: (_, _) {
          controller.requestCancel(reason: 'replacement');
          controller.signal.onCancel(events.add);
        },
      );
      controller.signal.onCancel((_) => throw StateError('listener failed'));
      controller.signal.onCancel(events.add);

      controller.requestCancel(reason: 'original');

      expect(events, ['original', 'original']);
      expect(controller.signal.reason, 'original');
    });
  });
}
