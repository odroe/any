import 'dart:async';

import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  test('deeply nested forks finish every join and finalizer', () async {
    final runtime = Runtime();
    final cleaned = <int>[];
    const depth = 1000;

    Task<int, Never> nested(int remaining) => Task.defer(() {
      if (remaining == 0) return Task.succeed(42);
      return nested(remaining - 1)
          .fork()
          .flatMap((child) => child.join())
          .ensuring(
            Task.sync(() {
              cleaned.add(remaining);
            }),
          );
    });

    expect(await runtime.runOrThrow(nested(depth)), 42);
    expect(cleaned, List.generate(depth, (index) => index + 1));
    expect(await runtime.close(), isA<ExitSuccess<void, Never>>());
  });

  test(
    'deeply nested runtime starts complete without recursive startup',
    () async {
      final runtime = Runtime();

      Task<int, Never> nested(int remaining) => Task.defer(() {
        if (remaining == 0) return Task.succeed(42);
        return Task.fromFuture<Exit<int, Never>, Never>(
          () => runtime.run(nested(remaining - 1)),
          onError: Error.throwWithStackTrace,
        ).flatMap(
          (exit) => switch (exit) {
            ExitSuccess(:final value) => Task.succeed(value),
            ExitFailure(:final cause) => Task.failCause(cause),
          },
        );
      });

      expect(await runtime.runOrThrow(nested(1000)), 42);
      expect(await runtime.close(), isA<ExitSuccess<void, Never>>());
    },
  );

  test(
    'cancellation stops a queued descendant and releases started work',
    () async {
      final runtime = Runtime();
      var started = 0;
      var cleaned = 0;
      var reachedLeaf = false;
      Task<void, Never> nested(int remaining) => Task.defer(() {
        started++;
        if (remaining == 0) {
          reachedLeaf = true;
          return Task.succeed(null);
        }
        return nested(remaining - 1)
            .fork()
            .flatMap((child) => child.join())
            .ensuring(
              Task.sync(() {
                cleaned++;
              }),
            );
      });

      final fiber = runtime.start(nested(1000));
      expect(started, greaterThan(0));
      expect(reachedLeaf, isFalse);
      fiber.requestCancel(reason: 'before leaf');
      final exit = await runtime.runOrThrow(fiber.awaitExit());

      expect(reachedLeaf, isFalse);
      expect(cleaned, started);
      final cause = (exit as ExitFailure).cause as CauseCancelled;
      expect(cause.reason, 'before leaf');
      expect(await runtime.close(), isA<ExitSuccess<void, Never>>());
    },
  );

  test('deferred nested work retains its creation zone', () async {
    final runtime = Runtime();
    final key = Object();
    final observed = <Object?>[];
    Task<Object?, Never> nested(int remaining) => remaining == 0
        ? Task.sync(() => Zone.current[key])
        : Task.defer(
            () => nested(remaining - 1).fork().flatMap((child) => child.join()),
          );
    final task = nested(1000)
        .ensuring(
          Task.sync(() {
            observed.add(Zone.current[key]);
          }),
        )
        .fork()
        .flatMap((child) => child.join());
    final result = runZoned(
      () => runtime.runOrThrow(task),
      zoneValues: {key: 'creation'},
    );

    expect(await result, 'creation');
    expect(observed, ['creation']);
    expect(await runtime.close(), isA<ExitSuccess<void, Never>>());
  });
}
