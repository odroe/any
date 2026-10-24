import 'dart:async';

import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  late Runtime runtime;

  setUp(() => runtime = Runtime());
  tearDown(() async {
    expect(await runtime.close(), isA<ExitSuccess<void, Never>>());
  });

  test('create produces independent semaphores on each execution', () async {
    final create = Semaphore.create(1);
    final first = await runtime.runOrThrow(create);
    final second = await runtime.runOrThrow(create);
    final firstGate = _Gate();
    final secondGate = _Gate();

    expect(first, isNot(same(second)));
    final firstRun = runtime.run(first.withPermits(1, firstGate.task));
    final secondRun = runtime.run(second.withPermits(1, secondGate.task));
    await firstGate.started.future;
    await secondGate.started.future;

    firstGate.open();
    secondGate.open();
    expect(await firstRun, isA<ExitSuccess<void, Never>>());
    expect(await secondRun, isA<ExitSuccess<void, Never>>());
  });

  test(
    'permits return while body resources remain in the caller scope',
    () async {
      final semaphore = await runtime.runOrThrow(Semaphore.create(1));
      var released = false;
      final resource = Object();
      final acquired = Task.acquireRelease(
        Task.succeed(resource),
        release: (_, _) => Task.sync(() => released = true),
      );

      final result = await runtime.runOrThrow(
        semaphore
            .withPermits(1, acquired)
            .flatMap(
              (value) => semaphore.withPermits(
                1,
                Task.sync(() {
                  expect(released, isFalse);
                  return value;
                }),
              ),
            ),
      );

      expect(result, same(resource));
      expect(released, isTrue);
    },
  );

  test('invalid capacity fails lazily with a range defect', () async {
    for (final permits in [-1, 0]) {
      final create = Semaphore.create(permits);
      expect(
        _cause(await runtime.run(create)),
        isA<CauseDefect>().having(
          (cause) => cause.error,
          'error',
          isA<RangeError>(),
        ),
      );
    }
  });

  test('invalid requests leave capacity and task body untouched', () async {
    final semaphore = await runtime.runOrThrow(Semaphore.create(2));
    var calls = 0;
    for (final count in [-1, 0, 3]) {
      final request = semaphore.withPermits(count, Task.sync(() => calls++));
      expect(calls, 0);
      expect(
        _cause(await runtime.run(request)),
        isA<CauseDefect>().having(
          (cause) => cause.error,
          'error',
          isA<RangeError>(),
        ),
      );
    }

    expect(calls, 0);
    expect(
      await runtime.runOrThrow(semaphore.withPermits(2, Task.succeed(7))),
      7,
    );
  });

  test('one lazy task makes independent requests on repeated runs', () async {
    final semaphore = await runtime.runOrThrow(Semaphore.create(1));
    final gates = <_Gate>[];
    final secondStarted = Completer<_Gate>();
    final task = semaphore.withPermits(
      1,
      Task.defer(() {
        final gate = _Gate();
        gates.add(gate);
        if (gates.length == 2) secondStarted.complete(gate);
        return gate.task;
      }),
    );

    expect(gates, isEmpty);
    final first = runtime.run(task);
    await gates.single.started.future;
    final second = runtime.run(task);
    expect(gates, hasLength(1));

    gates.first.open();
    expect(await first, isA<ExitSuccess<void, Never>>());
    final secondGate = await secondStarted.future;
    await secondGate.started.future;
    secondGate.open();
    expect(await second, isA<ExitSuccess<void, Never>>());
  });

  test('a smaller request cannot bypass a larger queue head', () async {
    final semaphore = await runtime.runOrThrow(Semaphore.create(3));
    final holder = _Gate();
    final large = _Gate();
    final small = _Gate();
    final holding = runtime.run(semaphore.withPermits(2, holder.task));
    await holder.started.future;

    final largeRun = runtime.run(semaphore.withPermits(3, large.task));
    final smallRun = runtime.run(semaphore.withPermits(1, small.task));
    expect(large.started.isCompleted, isFalse);
    expect(small.started.isCompleted, isFalse);

    holder.open();
    await large.started.future;
    expect(small.started.isCompleted, isFalse);
    large.open();
    await small.started.future;
    small.open();

    expect(await holding, isA<ExitSuccess<void, Never>>());
    expect(await largeRun, isA<ExitSuccess<void, Never>>());
    expect(await smallRun, isA<ExitSuccess<void, Never>>());
  });

  test('cancelling the queue head admits a fitting later request', () async {
    final semaphore = await runtime.runOrThrow(Semaphore.create(2));
    final holder = _Gate();
    final large = _Gate();
    final small = _Gate();
    final controller = CancellationController();
    final holding = runtime.run(semaphore.withPermits(1, holder.task));
    await holder.started.future;
    final largeRun = runtime.run(
      semaphore.withPermits(2, large.task),
      signal: controller.signal,
    );
    final smallRun = runtime.run(semaphore.withPermits(1, small.task));
    expect(small.started.isCompleted, isFalse);

    controller.requestCancel(reason: 'skip large');
    await small.started.future;
    expect(large.started.isCompleted, isFalse);
    expect(
      _cause(await largeRun),
      isA<CauseCancelled>().having(
        (cause) => cause.reason,
        'reason',
        'skip large',
      ),
    );

    holder.open();
    small.open();
    expect(await holding, isA<ExitSuccess<void, Never>>());
    expect(await smallRun, isA<ExitSuccess<void, Never>>());
    expect(
      await runtime.runOrThrow(semaphore.withPermits(2, Task.succeed(7))),
      7,
    );
  });

  test('removing a cancelled waiter does not return unowned permits', () async {
    final semaphore = await runtime.runOrThrow(Semaphore.create(1));
    final holder = _Gate();
    final cancelled = _Gate();
    final next = _Gate();
    final controller = CancellationController();
    final holding = runtime.run(semaphore.withPermits(1, holder.task));
    await holder.started.future;
    final cancelledRun = runtime.run(
      semaphore.withPermits(1, cancelled.task),
      signal: controller.signal,
    );
    final nextRun = runtime.run(semaphore.withPermits(1, next.task));

    controller.requestCancel();
    expect(_cause(await cancelledRun), isA<CauseCancelled>());
    expect(cancelled.started.isCompleted, isFalse);
    expect(next.started.isCompleted, isFalse);

    holder.open();
    await next.started.future;
    next.open();
    expect(await holding, isA<ExitSuccess<void, Never>>());
    expect(await nextRun, isA<ExitSuccess<void, Never>>());
  });

  test('cancelling the body releases its permits', () async {
    final semaphore = await runtime.runOrThrow(Semaphore.create(1));
    final body = _Gate();
    final next = _Gate();
    final controller = CancellationController();
    final running = runtime.run(
      semaphore.withPermits(1, body.task),
      signal: controller.signal,
    );
    await body.started.future;
    final waiting = runtime.run(semaphore.withPermits(1, next.task));

    controller.requestCancel(reason: 'stop body');
    await next.started.future;
    expect(body.cancelCalls, 1);
    expect(_cause(await running), isA<CauseCancelled>());
    next.open();
    expect(await waiting, isA<ExitSuccess<void, Never>>());
  });

  test('cancellation during grant handoff cannot leak permits', () async {
    final semaphore = await runtime.runOrThrow(Semaphore.create(1));
    final holder = _Gate();
    final controller = CancellationController();
    var armed = false;
    var intercepted = false;
    var bodyCalls = 0;
    final holding = runtime.run(semaphore.withPermits(1, holder.task));
    await holder.started.future;

    final waiting = runZoned(
      () => runtime.run(
        semaphore.withPermits(1, Task.sync(() => bodyCalls++)),
        signal: controller.signal,
      ),
      zoneSpecification: ZoneSpecification(
        scheduleMicrotask: (self, parent, zone, callback) {
          // Cancel as the granted wait is scheduled, before its continuation
          // can enter the body or register any later cleanup.
          if (armed) {
            armed = false;
            intercepted = true;
            controller.requestCancel(reason: 'handoff');
          }
          parent.scheduleMicrotask(zone, callback);
        },
      ),
    );

    armed = true;
    holder.open();
    expect(await holding, isA<ExitSuccess<void, Never>>());
    expect(_cause(await waiting), isA<CauseCancelled>());
    expect(intercepted, isTrue);
    expect(bodyCalls, 0);
    expect(
      await runtime.runOrThrow(semaphore.withPermits(1, Task.succeed(7))),
      7,
    );
  });

  test('business failure and thrown defects release permits', () async {
    final semaphore = await runtime.runOrThrow(Semaphore.create(2));
    final error = StateError('body failed');
    final tasks = <Task<int, String>>[
      Task.fail('missing'),
      Task.sync(() => throw error),
    ];

    final failure = await runtime.run(semaphore.withPermits(2, tasks.first));
    expect(
      _cause(failure),
      isA<CauseFailure<String>>().having(
        (cause) => cause.error,
        'error',
        'missing',
      ),
    );
    final defect = await runtime.run(semaphore.withPermits(2, tasks.last));
    expect(
      _cause(defect),
      isA<CauseDefect>().having((cause) => cause.error, 'error', same(error)),
    );
    expect(
      await runtime.runOrThrow(semaphore.withPermits(2, Task.succeed(7))),
      7,
    );
  });

  test('failing body cleanup still releases permits', () async {
    final semaphore = await runtime.runOrThrow(Semaphore.create(1));
    final error = StateError('cleanup failed');
    final body = Task.fail('body failed')
        .ensuring(Task.sync(() => throw error));
    final exit = await runtime.run(semaphore.withPermits(1, body));

    final cause = _cause(exit);
    expect(cause, isA<CauseGroup<String>>());
    final reasons = (cause as CauseGroup<String>).causes;
    expect(reasons, hasLength(2));
    expect(reasons.first, isA<CauseFailure<String>>());
    expect(
      reasons.last,
      isA<CauseDefect>().having((cause) => cause.error, 'error', same(error)),
    );
    expect(
      await runtime.runOrThrow(semaphore.withPermits(1, Task.succeed(7))),
      7,
    );
  });

  test('nested scope resources finish closing before permits return', () async {
    final semaphore = await runtime.runOrThrow(Semaphore.create(1));
    final cleanup = _Gate();
    final events = <String>[];
    final body =
        Task.addFinalizer((_) => Task.sync(() => events.add('outer cleanup')))
            .flatMap(
              (_) => Task.addFinalizer(
                (_) =>
                    Task.sync(() => events.add('inner cleanup'))
                        .flatMap((_) => cleanup.task),
              ).scoped(),
            );
    final running = runtime.run(semaphore.withPermits(1, body.scoped()));
    await cleanup.started.future;
    final waiting = runtime.run(
      semaphore.withPermits(1, Task.sync(() => events.add('next body'))),
    );
    expect(events, ['inner cleanup']);

    cleanup.open();
    expect(await running, isA<ExitSuccess<void, Never>>());
    expect(await waiting, isA<ExitSuccess<void, Never>>());
    expect(events, ['inner cleanup', 'outer cleanup', 'next body']);
  });

  test(
    'cancellation waits for body cleanup before returning permits',
    () async {
      final semaphore = await runtime.runOrThrow(Semaphore.create(1));
      final body = _Gate();
      final cleanup = _Gate();
      final next = _Gate();
      final controller = CancellationController();
      final running = runtime.run(
        semaphore.withPermits(1, body.task.ensuring(cleanup.task)),
        signal: controller.signal,
      );
      await body.started.future;
      final waiting = runtime.run(semaphore.withPermits(1, next.task));

      controller.requestCancel();
      await cleanup.started.future;
      expect(next.started.isCompleted, isFalse);
      expect(cleanup.cancelCalls, 0);

      cleanup.open();
      await next.started.future;
      expect(_cause(await running), isA<CauseCancelled>());
      next.open();
      expect(await waiting, isA<ExitSuccess<void, Never>>());
    },
  );

  test('an already cancelled execution does not acquire permits', () async {
    final semaphore = await runtime.runOrThrow(Semaphore.create(1));
    final controller = CancellationController()..requestCancel();
    var calls = 0;
    final exit = await runtime.run(
      semaphore.withPermits(1, Task.sync(() => calls++)),
      signal: controller.signal,
    );

    expect(_cause(exit), isA<CauseCancelled>());
    expect(calls, 0);
    expect(
      await runtime.runOrThrow(semaphore.withPermits(1, Task.succeed(7))),
      7,
    );
  });

  test(
    'an enclosing mask keeps waiting and the body uninterruptible',
    () async {
      final semaphore = await runtime.runOrThrow(Semaphore.create(1));
      final holder = _Gate();
      final controller = CancellationController();
      var calls = 0;
      final holding = runtime.run(semaphore.withPermits(1, holder.task));
      await holder.started.future;
      final waiting = runtime.run(
        Task.uninterruptibleMask(
          (_) => semaphore.withPermits(1, Task.sync(() => calls++)),
        ),
        signal: controller.signal,
      );

      controller.requestCancel();
      expect(calls, 0);
      holder.open();
      expect(await holding, isA<ExitSuccess<void, Never>>());
      expect(_cause(await waiting), isA<CauseCancelled>());
      expect(calls, 1);
      expect(
        await runtime.runOrThrow(semaphore.withPermits(1, Task.succeed(7))),
        7,
      );
    },
  );
}

final class _Gate {
  final started = Completer<void>();
  late void Function(Exit<void, Never>) _complete;
  var cancelCalls = 0;

  Task<void, Never> get task => Task.fromCallback((complete) {
    _complete = complete;
    started.complete();
    return () {
      cancelCalls++;
    };
  });

  void open() => _complete(const Exit.success(null));
}

Cause<E> _cause<T, E>(Exit<T, E> exit) => switch (exit) {
  ExitSuccess(:final value) => fail('Expected failure, got $value.'),
  ExitFailure(:final cause) => cause,
};
