import 'dart:async';

import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  for (final entry in ['start', 'run', 'runOrThrow']) {
    test('$entry tracks its root once through owned cleanup', () async {
      final events = <String>[];
      final pending = <Fiber<Object?, Object?>>{};
      final runtime = Runtime(
        onFiberStart: (fiber) {
          events.add('hook');
          pending.add(fiber);
          fiber.onExit((_) => pending.remove(fiber));
        },
      );
      addTearDown(runtime.close);
      final work = Completer<int>();
      final cleaning = Completer<void>();
      final cleaned = Completer<void>();
      final task =
          Task.fromFuture(() {
            events.add('evaluate');
            return work.future;
          }, onError: Error.throwWithStackTrace).ensuring(
            Task.fromFuture(() {
              cleaning.complete();
              return cleaned.future;
            }, onError: Error.throwWithStackTrace),
          );

      final result = switch (entry) {
        'start' => _completion(runtime.start(task)),
        'run' => runtime.run(task),
        _ => runtime.runOrThrow(task).then(Exit<int, Never>.success),
      };
      events.add('returned');
      expect(events, ['evaluate', 'hook', 'returned']);
      expect(pending, hasLength(1));

      work.complete(42);
      await cleaning.future;
      expect(pending, hasLength(1));
      cleaned.complete();
      expect((await result as ExitSuccess<int, Never>).value, 42);
      expect(pending, isEmpty);
    });
  }

  test('all entry points skip synchronously completed roots', () async {
    final runtime = Runtime(
      onFiberStart: (_) => fail('Synchronous completion must not be tracked.'),
    );
    addTearDown(runtime.close);
    final task = Task.succeed(42);

    expect(runtime.start(task).poll(), isA<OptionSome>());
    expect(await runtime.run(task), isA<ExitSuccess>());
    expect(await runtime.runOrThrow(task), 42);
    expect(runtime.runSync(task), isA<ExitSuccess>());
    expect(runtime.runSyncOrThrow(task), 42);
    expect(await runtime.run(Task.fail('business')), isA<ExitFailure>());
    final controller = CancellationController()..requestCancel();
    expect(
      await runtime.run(task, signal: controller.signal),
      isA<ExitFailure>(),
    );
  });

  test('closing and closed runtimes exclude rejected executions', () async {
    final seen = <Fiber<Object?, Object?>>[];
    final runtime = Runtime(onFiberStart: seen.add, onUnhandledError: (_) {});
    final work = Completer<void>();
    runtime.start(_future(work.future));
    final closing = runtime.close();
    final rejected = Task.sync(() => fail('Rejected work must not run.'));

    Future<void> checkRejected() async {
      expect(runtime.start(rejected).poll(), isA<OptionSome>());
      expect(await runtime.run(rejected), isA<ExitFailure>());
      await expectLater(
        runtime.runOrThrow(rejected),
        throwsA(isA<TaskException>()),
      );
      expect(runtime.runSync(rejected), isA<ExitFailure>());
      expect(
        () => runtime.runSyncOrThrow(rejected),
        throwsA(isA<TaskException>()),
      );
      expect(seen, hasLength(1));
    }

    await checkRejected();
    work.complete();
    await closing;
    await checkRejected();
  });

  for (final throwing in [false, true]) {
    test('tracks unfinished synchronous runs (throwing: $throwing)', () async {
      final pending = <Fiber<Object?, Object?>>{};
      var notifications = 0;
      final runtime = Runtime(
        onFiberStart: (fiber) {
          notifications++;
          pending.add(fiber);
          fiber.onExit((_) => pending.remove(fiber));
        },
      );
      addTearDown(runtime.close);
      final work = Completer<int>();
      late AsyncTaskException<int, Never> suspended;
      try {
        if (throwing) {
          runtime.runSyncOrThrow(_future(work.future));
        } else {
          runtime.runSync(_future(work.future));
        }
        fail('Expected asynchronous work.');
      } on AsyncTaskException<int, Never> catch (error) {
        suspended = error;
      }
      expect(notifications, 1);
      expect(pending, hasLength(1));

      work.complete(42);
      expect(await suspended.completion, isA<ExitFailure<int, Never>>());
      expect(pending, isEmpty);
    });
  }

  test(
    'create skips service construction and retains the hook for user roots',
    () async {
      final key = ServiceKey<int>('value');
      final service = Completer<int>();
      final work = Completer<void>();
      final seen = <Fiber<Object?, Object?>>[];
      var released = false;
      final created = Runtime.create(
        key.layer(
          Task.acquireRelease(
            _future(service.future),
            release: (_, _) => Task.sync(() => released = true),
          ),
        ),
        onFiberStart: seen.add,
      );
      expect(seen, isEmpty);
      service.complete(42);
      final runtime = (await created as ExitSuccess<Runtime, Never>).value;
      addTearDown(runtime.close);
      expect(seen, isEmpty);

      final result = runtime.runOrThrow(
        _future(work.future).flatMap((_) => Task.service(key)),
      );
      expect(seen, hasLength(1));
      work.complete();
      expect(await result, 42);
      expect(released, isFalse);
      await runtime.close();
      expect(released, isTrue);
    },
  );

  test(
    'observes the user root without recursively observing children',
    () async {
      final seen = <Fiber<Object?, Object?>>[];
      final runtime = Runtime(onFiberStart: seen.add);
      addTearDown(runtime.close);
      final work = Completer<int>();
      late Fiber<int, Never> child;
      final parent = runtime.start(
        _future(work.future).fork().flatMap((fiber) {
          child = fiber;
          return fiber.join();
        }),
      );

      expect(seen, [same(parent)]);
      expect(seen, isNot(contains(same(child))));
      work.complete(42);
      expect(await _completion(parent), isA<ExitSuccess<int, Never>>());
    },
  );

  test(
    'hook exceptions reach the configuration Zone without changing the task',
    () async {
      final error = StateError('observer failed');
      final errors = <Object>[];
      final callbackZones = <Zone>[];
      late Runtime runtime;
      late Zone configurationZone;
      runZonedGuarded(() {
        configurationZone = Zone.current;
        runtime = Runtime(
          onFiberStart: (fiber) {
            callbackZones.add(Zone.current);
            fiber.onExit((_) => callbackZones.add(Zone.current));
            throw error;
          },
        );
      }, (error, _) => errors.add(error));
      addTearDown(runtime.close);
      final work = Completer<int>();
      var cleaned = false;
      final result = runtime.runOrThrow(
        _future(work.future).ensuring(Task.sync(() => cleaned = true)),
      );
      expect(errors, [same(error)]);
      expect(callbackZones, [same(configurationZone)]);

      work.complete(42);
      expect(await result, 42);
      expect(cleaned, isTrue);
      expect(callbackZones, [same(configurationZone), same(configurationZone)]);
    },
  );

  test(
    'observation preserves direct and background failure responsibility',
    () async {
      final reported = Completer<Cause<Object?>>();
      final exits = <Exit<Object?, Object?>>[];
      final runtime = Runtime(
        onFiberStart: (fiber) => fiber.onExit(exits.add),
        onUnhandledError: expectAsync1(reported.complete),
      );
      addTearDown(runtime.close);
      final directGate = Completer<void>();
      final direct = runtime.run(
        _future(directGate.future)
            .withError<String>()
            .flatMap((_) => Task.fail('direct')),
      );
      directGate.complete();
      expect(await direct, isA<ExitFailure>());

      final backgroundGate = Completer<void>();
      runtime.start(
        _future(backgroundGate.future)
            .withError<String>()
            .flatMap((_) => Task.fail('background')),
      );
      backgroundGate.complete();
      expect((await reported.future as CauseFailure).error, 'background');
      expect(exits, hasLength(2));
    },
  );

  test(
    'explicit hook cancellation finishes cleanup before removing the root',
    () async {
      final pending = <Fiber<Object?, Object?>>{};
      var cleaned = false;
      final runtime = Runtime(
        onFiberStart: (fiber) {
          fiber.requestCancel(reason: 'stop');
          pending.add(fiber);
          fiber.onExit((_) => pending.remove(fiber));
        },
      );
      addTearDown(runtime.close);
      final result = await runtime.run(
        Task.fromCallback<int, Never>((_) => () {})
            .ensuring(Task.sync(() => cleaned = true)),
      );

      expect(((result as ExitFailure).cause as CauseCancelled).reason, 'stop');
      expect(cleaned, isTrue);
      expect(pending, isEmpty);
    },
  );

  test('a reentrant submission is a distinct root notified once', () async {
    final seen = <Fiber<Object?, Object?>>[];
    final work = Completer<int>();
    late Runtime runtime;
    late Fiber<int, Never> nested;
    runtime = Runtime(
      onFiberStart: (fiber) {
        seen.add(fiber);
        if (seen.length == 1) nested = runtime.start(_future(work.future));
      },
    );
    addTearDown(runtime.close);
    final outer = runtime.start(_future(work.future));

    expect(seen, [same(outer), same(nested)]);
    work.complete(42);
    expect(await _completion(outer), isA<ExitSuccess<int, Never>>());
    expect(await _completion(nested), isA<ExitSuccess<int, Never>>());
    expect(seen, hasLength(2));
  });
}

Task<T, Never> _future<T>(Future<T> future) =>
    Task.fromFuture(() => future, onError: Error.throwWithStackTrace);

Future<Exit<T, E>> _completion<T, E>(Fiber<T, E> fiber) {
  final completed = Completer<Exit<T, E>>();
  fiber.onExit(completed.complete);
  return completed.future;
}
