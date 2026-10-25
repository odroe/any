import 'dart:async';

import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  late Runtime runtime;

  setUp(() {
    runtime = Runtime();
    addTearDown(runtime.close);
  });

  test('notifies after execution and owned cleanup finish', () async {
    late void Function(Exit<int, Never>) complete;
    final cleaning = Completer<void>();
    final release = Completer<void>();
    addTearDown(() {
      if (!release.isCompleted) release.complete();
    });
    final events = <String>[];
    final seen = <Exit<int, Never>>[];
    final fiber = runtime.start(
      Task.fromCallback<int, Never>((callback) {
        complete = callback;
        return () {};
      }).ensuring(
        Task.fromFuture<void, Never>(
          () {
            events.add('cleaning');
            cleaning.complete();
            return release.future;
          },
          onError: Error.throwWithStackTrace,
        ).map((_) => events.add('cleaned')),
      ),
    );
    fiber.onExit((exit) {
      seen.add(exit);
      expect((fiber.poll() as OptionSome<Exit<int, Never>>).value, same(exit));
      events.add('notified');
    });

    complete(const Exit.success(42));
    await cleaning.future;
    expect(seen, isEmpty);
    expect(fiber.poll(), isA<OptionNone<Exit<int, Never>>>());
    release.complete();
    final exit = await runtime.runOrThrow(fiber.awaitExit());

    expect(events, ['cleaning', 'cleaned', 'notified']);
    expect(seen, [same(exit)]);
    expect((exit as ExitSuccess<int, Never>).value, 42);
  });

  test('late registration notifies before returning and only once', () {
    final fiber = runtime.start(Task.succeed(42));
    final exit = (fiber.poll() as OptionSome<Exit<int, Never>>).value;
    final events = <String>[];
    final seen = <Exit<int, Never>>[];

    final remove = fiber.onExit((result) {
      seen.add(result);
      events.add('notified');
    });
    events.add('returned');
    remove();
    remove();
    fiber.requestCancel();

    expect(events, ['notified', 'returned']);
    expect(seen, [same(exit)]);
    expect((fiber.poll() as OptionSome<Exit<int, Never>>).value, same(exit));
  });

  test(
    'unsubscribe is idempotent and leaves other registrations and work',
    () async {
      late void Function(Exit<int, Never>) complete;
      var cancellations = 0;
      var notifications = 0;
      final fiber = runtime.start(
        Task.fromCallback<int, Never>((callback) {
          complete = callback;
          return () {
            cancellations++;
          };
        }),
      );
      void listener(Exit<int, Never> exit) => notifications++;
      final remove = fiber.onExit(listener);
      fiber.onExit(listener);

      remove();
      remove();
      expect(fiber.poll(), isA<OptionNone<Exit<int, Never>>>());
      expect(cancellations, 0);
      complete(const Exit.success(42));
      expect(await runtime.runOrThrow(fiber.join()), 42);
      remove();

      expect(notifications, 1);
      expect(cancellations, 0);
    },
  );

  test(
    'a callback can remove another and register an immediate listener',
    () async {
      late void Function(Exit<int, Never>) complete;
      late void Function() removeSecond;
      final events = <String>[];
      final fiber = runtime.start(
        Task.fromCallback<int, Never>((callback) {
          complete = callback;
          return () {};
        }),
      );
      fiber.onExit((exit) {
        events.add('first');
        removeSecond();
        final removeNested = fiber.onExit((nested) {
          expect(nested, same(exit));
          events.add('nested');
        });
        removeNested();
        events.add('first returned');
      });
      removeSecond = fiber.onExit((_) => events.add('second'));
      fiber.onExit((_) => events.add('third'));

      complete(const Exit.success(42));
      await runtime.runOrThrow(fiber.awaitExit());

      expect(events, ['first', 'nested', 'first returned', 'third']);
    },
  );

  for (final completed in [false, true]) {
    test('callbacks use their registration Zone and isolate errors '
        '(already completed: $completed)', () async {
      late void Function(Exit<int, Never>) complete;
      final fiber = completed
          ? runtime.start(Task.succeed(42))
          : runtime.start(
              Task.fromCallback<int, Never>((callback) {
                complete = callback;
                return () {};
              }),
            );
      final key = Object();
      final zones = <Object?>[];
      final errors = <(Object, StackTrace)>[];
      final seen = <Exit<int, Never>>[];
      final error = StateError('listener failed');
      final stack = StackTrace.fromString('listener stack');
      late void Function() remove;

      runZoned(
        () {
          remove = fiber.onExit((exit) {
            zones.add(Zone.current[key]);
            seen.add(exit);
            Error.throwWithStackTrace(error, stack);
          });
        },
        zoneValues: {key: 'first'},
        zoneSpecification: ZoneSpecification(
          handleUncaughtError: (self, parent, zone, error, stack) {
            errors.add((error, stack));
          },
        ),
      );
      runZoned(
        () => fiber.onExit((exit) {
          zones.add(Zone.current[key]);
          seen.add(exit);
        }),
        zoneValues: {key: 'second'},
      );

      if (!completed) {
        expect(seen, isEmpty);
        complete(const Exit.success(42));
      } else {
        expect(zones, ['first', 'second']);
      }
      final exit = await runtime.runOrThrow(fiber.awaitExit());
      remove();
      remove();

      expect(zones, ['first', 'second']);
      expect(seen, [same(exit), same(exit)]);
      expect(errors, hasLength(1));
      expect(errors.single.$1, same(error));
      expect(errors.single.$2.toString(), stack.toString());
      expect((exit as ExitSuccess<int, Never>).value, 42);
    });
  }
}
