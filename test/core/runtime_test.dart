import 'dart:async';

import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  test('run repeats a description with independent execution state', () async {
    final runtime = Runtime();
    var calls = 0;
    final task = Task.sync(() => ++calls);

    expect(await runtime.run(task), isA<ExitSuccess<int, Never>>());
    expect(await runtime.run(task), isA<ExitSuccess<int, Never>>());
    expect(calls, 2);
    expect(await runtime.close(), isA<ExitSuccess<void, Never>>());
  });

  test(
    'run returns failures and runOrThrow bridges the entire cause',
    () async {
      final runtime = Runtime();
      final defect = StateError('unexpected');
      final stack = StackTrace.fromString('original defect stack');
      final cause = Cause<String>.combine([
        const Cause.failure('business'),
        Cause.defect(defect, stack),
      ]);
      final task = Task.failCause(cause);

      final exit = await runtime.run(task);
      expect(exit, isA<ExitFailure<Never, String>>());
      await expectLater(
        runtime.runOrThrow(task),
        throwsA(
          isA<TaskException>()
              .having(
                (error) => switch (error.cause) {
                  CauseGroup(:final causes) => causes.length,
                  _ => 0,
                },
                'complete causes',
                2,
              )
              .having(
                (error) => error.toString(),
                'diagnostic',
                'TaskException:\n'
                    '  Failure: business\n'
                    '  Defect: Bad state: unexpected\n'
                    '    original defect stack',
              ),
        ),
      );
      expect(await runtime.runOrThrow(Task.succeed(42)), 42);
      await runtime.close();
    },
  );

  test('an already cancelled signal prevents business work', () async {
    final runtime = Runtime();
    final controller = CancellationController()..requestCancel(reason: 'stop');
    var called = false;
    final exit = await runtime.run(
      Task.sync(() => called = true),
      signal: controller.signal,
    );

    expect(called, isFalse);
    expect(exit, isA<ExitFailure<bool, Never>>());
    expect((exit as ExitFailure).cause, isA<CauseCancelled>());
    await runtime.close();
  });

  test('external cancellation affects only its own root', () async {
    final runtime = Runtime();
    final controller = CancellationController();
    final first = Completer<int>();
    final second = Completer<int>();
    final firstStarted = Completer<void>();
    final secondStarted = Completer<void>();
    Task<int, String> pending(Completer<int> work, Completer<void> started) =>
        Task.fromFuture(() {
          started.complete();
          return work.future;
        }, onError: (error, stack) => '$error');

    final a = runtime.run(
      pending(first, firstStarted),
      signal: controller.signal,
    );
    final b = runtime.run(pending(second, secondStarted));
    await Future.wait([firstStarted.future, secondStarted.future]);
    controller.requestCancel(reason: 'first only');
    first.complete(1);
    second.complete(2);

    expect(await a, isA<ExitFailure<int, String>>());
    expect((await b as ExitSuccess<int, String>).value, 2);
    await runtime.close();
  });

  test(
    'close waits for in-flight work and rejects new runs immediately',
    () async {
      final runtime = Runtime();
      final work = Completer<int>();
      final started = Completer<void>();
      final run = runtime.run(
        Task.fromFuture(() {
          started.complete();
          return work.future;
        }, onError: (error, stack) => '$error'),
      );
      await started.future;

      final closing = runtime.close();
      expect(runtime.close(), same(closing));
      var finished = false;
      closing.then((_) => finished = true);
      var called = false;
      final rejected = await runtime.run(Task.sync(() => called = true));
      expect(called, isFalse);
      expect(finished, isFalse);
      final rejectedCause = (rejected as ExitFailure).cause as CauseDefect;
      expect(rejectedCause.error, isA<RuntimeClosed>());

      work.complete(1);
      expect(await run, isA<ExitFailure<int, String>>());
      expect(await closing, isA<ExitSuccess<void, Never>>());
      expect(runtime.close(), same(closing));
    },
  );

  test('a run cannot wait for its own runtime to close', () async {
    final runtime = Runtime();
    final result = await runtime.run(
      Task.fromFuture(runtime.close, onError: (error, stack) => error),
    );
    final closed = (result as ExitSuccess<Exit<void, Never>, Object>).value;
    final cause = (closed as ExitFailure<void, Never>).cause as CauseDefect;
    expect(cause.error, isA<StateError>());
    expect(await runtime.runOrThrow(Task.succeed('still open')), 'still open');
    await runtime.close();
  });

  for (final finalizer in [false, true]) {
    test(
      'cleanup cannot close a runtime awaiting its lease: $finalizer',
      () async {
        final owner = Runtime();
        final borrower = Runtime();
        addTearDown(owner.close);
        addTearDown(borrower.close);
        final ready = Completer<SharedResource<int, Never>>();
        var acquisitions = 0;
        var releases = 0;
        owner.start(
          SharedResource.create(
            Task.acquireRelease(
              Task.sync(() => ++acquisitions),
              release: (_, _) => Task.sync(() => releases++),
            ),
          ).flatMap((resource) {
            ready.complete(resource);
            return Task.fromCallback<void, Never>((_) => () {});
          }),
        );
        final resource = await ready.future;
        Task<void, Never> cleanup(Task<void, Never> use) => finalizer
            ? Task.addFinalizer((_) => use)
            : Task.succeed(null).ensuring(use);

        await borrower.runOrThrow(
          cleanup(resource.acquire().map((value) => expect(value, 1))),
        );
        expect(releases, 1);

        await borrower.runOrThrow(
          cleanup(
            resource.acquire().flatMap(
              (_) =>
                  Task.fromFuture(
                    () => owner.close().timeout(const Duration(seconds: 2)),
                    onError: Error.throwWithStackTrace,
                  ).map((exit) {
                    final cause = (exit as ExitFailure).cause as CauseDefect;
                    expect(cause.error, isA<StateError>());
                  }),
            ),
          ),
        );
        expect(acquisitions, 2);
        expect(releases, 2);
        expect(await owner.runOrThrow(Task.succeed(42)), 42);
      },
    );
  }

  test('completion removes the external cancellation listener', () async {
    final runtime = Runtime();
    final controller = CancellationController();
    expect(
      await runtime.run(Task.succeed(7), signal: controller.signal),
      isA<ExitSuccess<int, Never>>(),
    );
    controller.requestCancel();
    expect(await runtime.runOrThrow(Task.succeed(8)), 8);
    await runtime.close();
  });

  test(
    'a callback inherited from a completed root can close the runtime',
    () async {
      final runtime = Runtime();
      final closed = Completer<Exit<void, Never>>();
      await runtime.run(
        Task.sync(() {
          Timer.run(() => closed.complete(runtime.close()));
        }),
      );

      expect(await closed.future, isA<ExitSuccess<void, Never>>());
      final rejected = await runtime.run(Task.succeed(1));
      expect((rejected as ExitFailure).cause, isA<CauseDefect>());
    },
  );

  test('nested runs cannot close an active ancestor runtime', () async {
    final outer = Runtime();
    final inner = Runtime();
    final result = await outer
        .run(
          Task.fromFuture(
            () => inner.run(
              Task.fromFuture(outer.close, onError: (error, stack) => error),
            ),
            onError: (error, stack) => error,
          ),
        )
        .timeout(const Duration(seconds: 1));

    final innerExit = (result as ExitSuccess).value as ExitSuccess;
    final closeExit = innerExit.value as ExitFailure;
    expect((closeExit.cause as CauseDefect).error, isA<StateError>());
    expect(await outer.runOrThrow(Task.succeed(1)), 1);
    await inner.close();
    await outer.close();
  });
}
