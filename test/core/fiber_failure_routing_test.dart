import 'dart:async';

import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  late Runtime runtime;
  late List<Cause<Object?>> reports;

  setUp(() {
    reports = [];
    runtime = Runtime(onUnhandledError: reports.add);
  });
  tearDown(() => runtime.close());

  for (final read in ['none', 'poll', 'listener', 'awaitExit', 'join']) {
    test('$read does not change an independent failure report', () async {
      final fiber = runtime.start(Task.fail('failed'));
      switch (read) {
        case 'poll':
          expect(fiber.poll(), isA<OptionSome<Exit<Never, String>>>());
          fiber.poll();
        case 'listener':
          fiber.onExit((exit) => expect(exit, isA<ExitFailure>()));
        case 'awaitExit':
          await runtime.run(fiber.awaitExit());
          await runtime.run(fiber.awaitExit());
        case 'join':
          await runtime.run(fiber.join());
          await runtime.run(fiber.join());
      }
      await _reportsTurn();
      expect(reports, hasLength(1));
      expect((reports.single as CauseFailure).error, 'failed');
    });
  }

  test('result-returning entries do not add a fallback report', () async {
    expect(await runtime.run(Task.fail('run')), isA<ExitFailure>());
    expect(runtime.runSync(Task.fail('sync')), isA<ExitFailure>());
    await expectLater(
      runtime.runOrThrow(Task.fail('throw')),
      throwsA(isA<TaskException>()),
    );
    await _reportsTurn();
    expect(reports, isEmpty);
  });

  for (final observerFirst in [false, true]) {
    test('joining from another runtime preserves both reporting routes '
        '(observer reports first: $observerFirst)', () async {
      final order = <String>[];
      final ownerReports = <Cause<Object?>>[];
      final observerReports = <Cause<Object?>>[];
      final ownerReported = Completer<void>();
      final observerReported = Completer<void>();
      final owner = Runtime(
        onUnhandledError: (cause) {
          ownerReports.add(cause);
          order.add('owner');
          if (!ownerReported.isCompleted) ownerReported.complete();
        },
      );
      final observer = Runtime(
        onUnhandledError: (cause) {
          observerReports.add(cause);
          order.add('observer');
          if (!observerReported.isCompleted) observerReported.complete();
        },
      );
      addTearDown(owner.close);
      addTearDown(observer.close);
      late void Function(Exit<int, String>) complete;
      final fiber = owner.start(
        Task.fromCallback<int, String>((callback) {
          complete = callback;
          return () {};
        }),
      );
      late Fiber<int, String> joined;
      if (observerFirst) {
        // The reentrant root completes before the owner queues its report.
        fiber.onExit((_) => joined = observer.start(fiber.join()));
      }
      complete(const Exit.failure(Cause.failure('owned failure')));
      if (!observerFirst) {
        await ownerReported.future;
        joined = observer.start(fiber.join());
      }
      await Future.wait([ownerReported.future, observerReported.future]);

      expect(
        order,
        observerFirst ? ['observer', 'owner'] : ['owner', 'observer'],
      );
      for (var i = 0; i < 2; i++) {
        expect(fiber.poll(), isA<OptionSome<Exit<int, String>>>());
        expect(joined.poll(), isA<OptionSome<Exit<int, String>>>());
        fiber.onExit((exit) => expect(exit, isA<ExitFailure<int, String>>()));
        await owner.run(fiber.awaitExit());
        await observer.run(fiber.join());
        await owner.run(joined.join());
        await observer.run(joined.awaitExit());
      }
      await _reportsTurn();

      expect(ownerReports, hasLength(1));
      expect(observerReports, hasLength(1));
      expect((ownerReports.single as CauseFailure).error, 'owned failure');
      expect((observerReports.single as CauseFailure).error, 'owned failure');
    });
  }

  for (final conversion in ['none', 'mapError', 'orDie']) {
    test(
      'a public child joined into a reported root reports once (conversion: $conversion)',
      () async {
        final task = Task.fail('child')
            .fork()
            .withError<String>()
            .flatMap((fiber) {
              final joined = fiber.join();
              return switch (conversion) {
                'mapError' => joined.mapError((error) => 'mapped $error'),
                'orDie' => joined.orDie(),
                _ => joined,
              };
            });
        final root = runtime.start(task);
        await runtime.run(root.awaitExit());
        await _reportsTurn();
        expect(reports, hasLength(1));
      },
    );
  }

  test('independent executions of the same cause still report twice', () async {
    const cause = Cause<String>.failure('same object');
    runtime.start(Task.failCause(cause));
    runtime.start(Task.failCause(cause));
    await _reportsTurn();
    expect(reports, hasLength(2));
  });

  test(
    'group recovery reports only independent and replacement failures',
    () async {
      final reported = Completer<void>();
      final owner = Runtime(
        onUnhandledError: expectAsync1((cause) {
          reports.add(cause);
          if (reports.length == 2) reported.complete();
        }, count: 2),
      );
      addTearDown(owner.close);
      final group = Cause<String>.combine([
        const Cause.failure('first'),
        const Cause.failure('second'),
      ]);
      const replacement = Cause<String>.failure('replacement');
      final Task<int, String> failed = Task.failCause(group);
      var recovered = 0;
      var replaced = 0;

      final independent = owner.start(failed);
      final success = owner.start(
        failed.recover((_) {
          recovered++;
          return Task.succeed(42);
        }),
      );
      final fallback = owner.start(
        failed.recover((_) {
          replaced++;
          return Task.failCause(replacement);
        }),
      );

      expect(
        await owner.runOrThrow(independent.awaitExit()),
        isA<ExitFailure>(),
      );
      expect(await owner.runOrThrow(success.join()), 42);
      final exit = await owner.runOrThrow(fallback.awaitExit());
      expect((exit as ExitFailure<int, String>).cause, same(replacement));
      await reported.future;

      expect(recovered, 1);
      expect(replaced, 1);
      expect(reports, hasLength(2));
      final original = reports.whereType<CauseGroup<Object?>>().single;
      expect(original.causes, (group as CauseGroup<String>).causes.map(same));
      expect(
        reports.whereType<CauseFailure<String>>().single,
        same(replacement),
      );
    },
  );

  test('owning combinators receive failures without worker reports', () async {
    final all = await runtime.run(
      Task.all([Task.fail('first'), Task.fail('second')], concurrency: 2),
    );
    expect(all, isA<ExitFailure>());
    final settled = await runtime.runOrThrow(
      Task.allSettled([Task.fail('settled')], concurrency: 1),
    );
    expect(settled.single, isA<ExitFailure>());
    await _reportsTurn();
    expect(reports, isEmpty);
  });

  for (final read in [false, true]) {
    test(
      'runSync async failure remains supervised (completion read: $read)',
      () async {
        final release = Completer<void>();
        addTearDown(() {
          if (!release.isCompleted) release.complete();
        });
        final defect = Cause<Never>.defect(
          StateError('cleanup'),
          StackTrace.current,
        );
        final task = Task.fromCallback<void, Never>((_) => () {}).ensuring(
          Task.fromFuture<void, Never>(
            () => release.future,
            onError: Error.throwWithStackTrace,
          ).flatMap((_) => Task.failCause(defect)),
        );
        late AsyncTaskException<void, Never> pending;
        try {
          runtime.runSync(task);
          fail('The pending callback requires an asynchronous boundary.');
        } on AsyncTaskException<void, Never> catch (error) {
          pending = error;
        }
        final completion = read ? pending.completion : null;
        release.complete();
        if (completion != null) expect(await completion, isA<ExitFailure>());
        await _reportsTurn();
        expect((reports.single as CauseGroup).causes.whereType<CauseDefect>(), [
          same(defect),
        ]);
        expect(
          (reports.single as CauseGroup).causes.whereType<CauseCancelled>(),
          hasLength(1),
        );
      },
    );
  }
}

Future<void> _reportsTurn() async {
  await Future<void>.delayed(Duration.zero);
  await Future<void>.delayed(Duration.zero);
}
