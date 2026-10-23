import 'dart:async';

import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  late Runtime runtime;

  setUp(() => runtime = Runtime());
  tearDown(() async {
    expect(await runtime.close(), isA<ExitSuccess<void, Never>>());
  });

  test(
    'create produces independent closed gates on repeated execution',
    () async {
      final create = Latch.create();
      final first = await runtime.runOrThrow(create);
      final second = await runtime.runOrThrow(create);
      final started = Completer<void>();
      var secondPassed = false;
      final waiting = runtime.run(
        Task.sync(started.complete)
            .flatMap((_) => second.awaitOpen())
            .map((_) => secondPassed = true),
      );
      await started.future;

      expect(first, isNot(same(second)));
      await runtime.runOrThrow(first.open());
      expect(secondPassed, isFalse);

      await runtime.runOrThrow(second.open());
      expect(_value(await waiting), isTrue);
    },
  );

  test('open and reset are lazy and reusable', () async {
    final latch = await runtime.runOrThrow(Latch.create(open: true));
    final open = latch.open();
    final reset = latch.reset();

    await runtime.runOrThrow(latch.awaitOpen());
    await runtime.runOrThrow(reset);

    for (var cycle = 0; cycle < 2; cycle++) {
      final started = Completer<void>();
      var passed = false;
      final waiting = runtime.run(
        Task.sync(started.complete)
            .flatMap((_) => latch.awaitOpen())
            .map((_) => passed = true),
      );
      await started.future;
      expect(passed, isFalse);

      await runtime.runOrThrow(open);
      expect(_value(await waiting), isTrue);
      await runtime.runOrThrow(latch.awaitOpen());
      await runtime.runOrThrow(reset);
    }
  });

  test('one open releases every execution of the same waiting task', () async {
    final latch = await runtime.runOrThrow(Latch.create());
    final waiting = latch.awaitOpen();
    final first = runtime.run(waiting);
    final second = runtime.run(waiting);
    final third = runtime.run(waiting);

    await runtime.runOrThrow(latch.open());

    expect(await first, isA<ExitSuccess<void, Never>>());
    expect(await second, isA<ExitSuccess<void, Never>>());
    expect(await third, isA<ExitSuccess<void, Never>>());
    await runtime.runOrThrow(waiting);
  });

  test(
    'reset does not retract releases delivered by the preceding open',
    () async {
      final latch = await runtime.runOrThrow(Latch.create());
      final first = runtime.run(latch.awaitOpen());
      final second = runtime.run(latch.awaitOpen());

      await runtime.runOrThrow(latch.open().flatMap((_) => latch.reset()));

      expect(await first, isA<ExitSuccess<void, Never>>());
      expect(await second, isA<ExitSuccess<void, Never>>());

      final started = Completer<void>();
      var passed = false;
      final next = runtime.run(
        Task.sync(started.complete)
            .flatMap((_) => latch.awaitOpen())
            .map((_) => passed = true),
      );
      await started.future;
      expect(passed, isFalse);

      await runtime.runOrThrow(latch.open());
      expect(_value(await next), isTrue);
    },
  );

  test('resetting a closed latch preserves its existing waiters', () async {
    final latch = await runtime.runOrThrow(Latch.create());
    var passed = false;
    final waiting = runtime.run(latch.awaitOpen().map((_) => passed = true));

    await runtime.runOrThrow(latch.reset());
    await runtime.runOrThrow(latch.reset());
    expect(passed, isFalse);

    await runtime.runOrThrow(latch.open());
    expect(_value(await waiting), isTrue);
  });

  test(
    'repeated opens do not accumulate releases for the next cycle',
    () async {
      final latch = await runtime.runOrThrow(Latch.create());
      await runtime.runOrThrow(latch.open());
      await runtime.runOrThrow(latch.open());
      await runtime.runOrThrow(latch.reset());

      final started = Completer<void>();
      var passed = false;
      final waiting = runtime.run(
        Task.sync(started.complete)
            .flatMap((_) => latch.awaitOpen())
            .map((_) => passed = true),
      );
      await started.future;
      expect(passed, isFalse);

      await runtime.runOrThrow(latch.open());
      expect(_value(await waiting), isTrue);
    },
  );

  test(
    'cancelling one waiter leaves the gate and other waiters alone',
    () async {
      final latch = await runtime.runOrThrow(Latch.create());
      final controller = CancellationController();
      final waiting = latch.awaitOpen();
      var cancelledPassed = false;
      var otherPassed = false;
      final cancelled = runtime.run(
        waiting.map((_) => cancelledPassed = true),
        signal: controller.signal,
      );
      final other = runtime.run(waiting.map((_) => otherPassed = true));

      controller.requestCancel(reason: 'stop this wait');

      expect(
        _cause(await cancelled),
        isA<CauseCancelled>().having(
          (cause) => cause.reason,
          'reason',
          'stop this wait',
        ),
      );
      expect(otherPassed, isFalse);
      await runtime.runOrThrow(latch.open());
      expect(_value(await other), isTrue);
      expect(cancelledPassed, isFalse);
      await runtime.runOrThrow(waiting);
    },
  );

  test('cancellation after release can still stop a continuation', () async {
    final latch = await runtime.runOrThrow(Latch.create());
    final controller = CancellationController();
    var passed = false;
    final waiting = runtime.run(
      latch.awaitOpen().map((_) => passed = true),
      signal: controller.signal,
    );

    await runtime.runOrThrow(
      latch.open().flatMap((_) => Task.sync(controller.requestCancel)),
    );

    expect(_cause(await waiting), isA<CauseCancelled>());
    expect(passed, isFalse);
    await runtime.runOrThrow(latch.awaitOpen());
  });

  test('cancelled open and reset tasks do not change the gate', () async {
    final latch = await runtime.runOrThrow(Latch.create());
    final controller = CancellationController()..requestCancel();
    var passed = false;
    final waiting = runtime.run(latch.awaitOpen().map((_) => passed = true));

    expect(
      _cause(await runtime.run(latch.open(), signal: controller.signal)),
      isA<CauseCancelled>(),
    );
    expect(passed, isFalse);

    await runtime.runOrThrow(latch.open());
    expect(_value(await waiting), isTrue);
    expect(
      _cause(await runtime.run(latch.reset(), signal: controller.signal)),
      isA<CauseCancelled>(),
    );
    await runtime.runOrThrow(latch.awaitOpen());
  });
}

T _value<T, E>(Exit<T, E> exit) => switch (exit) {
  ExitSuccess(:final value) => value,
  ExitFailure(:final cause) => fail('Expected success, got $cause.'),
};

Cause<E> _cause<T, E>(Exit<T, E> exit) => switch (exit) {
  ExitSuccess(:final value) => fail('Expected failure, got $value.'),
  ExitFailure(:final cause) => cause,
};
