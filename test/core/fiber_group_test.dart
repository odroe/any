import 'dart:async';

import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  test(
    'typed synchronous members finish their local cleanup before removal',
    () async {
      final (runtime, group) = await _group<Never>();
      var released = false;
      final Fiber<int, Never> member = runtime.runSyncOrThrow(
        group.start(
          Task.acquireRelease(
            Task.succeed(42),
            release: (_, _) => Task.sync(() => released = true),
          ).scoped(),
        ),
      );

      expect(released, isTrue);
      expect(runtime.runSyncOrThrow(member.join()), 42);
      expect(runtime.runSync(group.awaitEmpty()), isA<ExitSuccess>());
    },
  );

  test(
    'an empty open group keeps join pending across successful submissions',
    () async {
      final (runtime, group) = await _group<Never>();
      final controller = CancellationController();
      var joined = false;
      final waiting = runtime.run(group.join(), signal: controller.signal).then(
        (exit) {
          joined = true;
          return exit;
        },
      );
      final number = Completer<int>();
      final text = Completer<String>();
      final Fiber<int, Never> first = await runtime.runOrThrow(
        group.start(_future(number.future)),
      );
      final Fiber<String, Never> second = await runtime.runOrThrow(
        group.start(_future(text.future)),
      );
      number.complete(42);
      text.complete('done');
      await runtime.runOrThrow(group.awaitEmpty());

      expect(await runtime.runOrThrow(first.join()), 42);
      expect(await runtime.runOrThrow(second.join()), 'done');
      expect(joined, isFalse);
      controller.requestCancel(reason: 'stop waiting');
      expect(_cause(await waiting), isA<CauseCancelled>());
    },
  );

  test(
    'retains a nullable first failure through recovery and repeated waits',
    () async {
      final (runtime, group) = await _group<String?>();
      final first = await runtime.runOrThrow(
        group.start(Task.fail<String?>(null)),
      );
      final exit = await runtime.run(group.join());
      expect((_cause(exit) as CauseFailure<String?>).error, isNull);
      expect(
        await runtime.run(group.join().recover((_) => Task.succeed(null))),
        isA<ExitSuccess>(),
      );

      await runtime.runOrThrow(group.start(Task.succeed('later success')));
      await runtime.runOrThrow(group.awaitEmpty());
      expect(
        (_cause(await runtime.run(group.join())) as CauseFailure).error,
        isNull,
      );
      expect(
        (_cause(await runtime.run(first.join())) as CauseFailure).error,
        isNull,
      );
    },
  );

  test(
    'failure does not cancel siblings or reject later submissions',
    () async {
      final (runtime, group) = await _group<String>();
      final work = Completer<int>();
      var cleaned = false;
      final sibling = await runtime.runOrThrow(
        group.start(
          _future(work.future)
              .withError<String>()
              .ensuring(Task.sync(() => cleaned = true)),
        ),
      );
      await runtime.runOrThrow(group.start(Task.fail('failed')));
      expect(
        (_cause(await runtime.run(group.join())) as CauseFailure).error,
        'failed',
      );
      expect(sibling.poll(), isA<OptionNone>());
      expect(cleaned, isFalse);

      final later = await runtime.runOrThrow(group.start(Task.succeed(7)));
      expect(await runtime.runOrThrow(later.join()), 7);
      work.complete(42);
      await runtime.runOrThrow(group.awaitEmpty());
      expect(cleaned, isTrue);
    },
  );

  test(
    'later independent occurrences report once without re-reporting the first',
    () async {
      final reported = Completer<Cause<Object?>>();
      final (runtime, group) = await _group<Object>(
        onUnhandledError: expectAsync1(reported.complete),
      );
      final error = StateError('same object, separate occurrences');
      final first = await runtime.runOrThrow(
        group.start(Task.fail<Object>(error)),
      );
      final observed = <Exit<Never, Object>>[];
      first.onExit(observed.add);
      first.poll();
      await runtime.runOrThrow(first.awaitExit());
      await runtime.run(first.join());
      await runtime.run(group.join());

      await runtime.runOrThrow(group.start(first.join()));
      final later = await runtime.runOrThrow(
        group.start(Task.fail<Object>(error)),
      );
      await runtime.runOrThrow(later.awaitExit());
      await runtime.run(later.join());
      expect((await reported.future as CauseFailure).error, same(error));
      expect(observed, hasLength(1));
      expect(
        (_cause(await runtime.run(group.join())) as CauseFailure).error,
        same(error),
      );
    },
  );

  for (final propagate in [false, true]) {
    test(
      'member cancellation policy is chosen per submission: $propagate',
      () async {
        final (runtime, group) = await _group<Never>();
        final member = await runtime.runOrThrow(
          group.start(
            Task.fromCallback<int, Never>((_) => () {}),
            propagateCancellation: propagate,
          ),
        );
        member.requestCancel(reason: 'member stop');
        await runtime.runOrThrow(member.awaitExit());
        await runtime.runOrThrow(group.awaitEmpty());

        if (propagate) {
          expect(
            (_cause(await runtime.run(group.join())) as CauseCancelled).reason,
            'member stop',
          );
        } else {
          final waiter = Runtime();
          addTearDown(waiter.close);
          final waiting = waiter.run(group.join());
          await runtime.close();
          expect(await waiting, isA<ExitSuccess>());
        }
      },
    );
  }

  test(
    'mixed cancellation retains business errors and cleanup defects',
    () async {
      final (runtime, group) = await _group<String>();
      final fault = Cause<Never>.defect(
        StateError('cleanup'),
        StackTrace.fromString('cleanup stack'),
      );
      final original = Cause<String>.combine([
        const Cause.cancelled('member stop'),
        const Cause.failure('business'),
      ]);
      await runtime.runOrThrow(
        group.start(Task.failCause(original).ensuring(Task.failCause(fault))),
      );

      final reasons =
          (_cause(await runtime.run(group.join())) as CauseGroup).causes;
      expect(reasons, [
        isA<CauseCancelled>(),
        isA<CauseFailure>(),
        same(fault),
      ]);
      expect((reasons[1] as CauseFailure).error, 'business');
    },
  );

  for (final reason in [null, 'explicit stop']) {
    test(
      'explicit cancellation before shutdown stays a group failure: $reason',
      () async {
        final (runtime, group) = await _group<Never>();
        final waiter = Runtime();
        addTearDown(waiter.close);
        final work = Completer<int>();
        final member = await runtime.runOrThrow(
          group.start(_future(work.future), propagateCancellation: true),
        );
        member.requestCancel(reason: reason);
        final waiting = waiter.run(group.join());
        final closing = runtime.close();
        work.complete(42);

        expect(await closing, isA<ExitSuccess>());
        expect((_cause(await waiting) as CauseCancelled).reason, reason);
      },
    );
  }

  test(
    'shutdown retains cancellation already recorded before pending cleanup',
    () async {
      final (runtime, group) = await _group<Never>();
      final waiter = Runtime();
      addTearDown(waiter.close);
      final cleanup = Completer<void>();
      const original = Cause<Never>.cancelled();
      await runtime.runOrThrow(
        group.start(
          Task.failCause(original).ensuring(_future(cleanup.future)),
          propagateCancellation: true,
        ),
      );
      final waiting = waiter.run(group.join());
      final closing = runtime.close();
      cleanup.complete();

      expect(await closing, isA<ExitSuccess>());
      final reasons = (_cause(await waiting) as CauseGroup).causes;
      expect(reasons, [same(original), isA<CauseCancelled>()]);
    },
  );

  for (final waitForEmpty in [false, true]) {
    test(
      'cancelling a group wait leaves members owned: empty=$waitForEmpty',
      () async {
        final (runtime, group) = await _group<String>();
        final work = Completer<int>();
        var cleaned = false;
        final member = await runtime.runOrThrow(
          group.start(
            _future(work.future)
                .withError<String>()
                .flatMap((_) => Task.fail('eventual failure'))
                .ensuring(Task.sync(() => cleaned = true)),
          ),
        );
        final controller = CancellationController();
        final waiting = runtime.run(
          waitForEmpty ? group.awaitEmpty().withError<String>() : group.join(),
          signal: controller.signal,
        );
        controller.requestCancel();
        expect(_cause(await waiting), isA<CauseCancelled>());
        expect(member.poll(), isA<OptionNone>());
        expect(cleaned, isFalse);

        work.complete(42);
        await runtime.runOrThrow(group.awaitEmpty());
        expect(cleaned, isTrue);
        expect(
          (_cause(await runtime.run(group.join())) as CauseFailure).error,
          'eventual failure',
        );
      },
    );
  }

  test('shutdown excludes its cancellation and rejects submissions before cleanup finishes', () async {
    final runtime = Runtime(onUnhandledError: _unexpected);
    addTearDown(runtime.close);
    final ready = Completer<FiberGroup<Never>>();
    final finish = Completer<void>();
    final owner = runtime.run(
      FiberGroup.create<Never>().flatMap((group) {
        ready.complete(group);
        return _future(finish.future);
      }),
    );
    final group = await ready.future;
    final cleaning = Completer<void>();
    final cleaned = Completer<void>();
    final member = await runtime.runOrThrow(
      group.start(
        Task.fromCallback<int, Never>((_) => () {}).ensuring(
          Task.fromFuture(() {
            cleaning.complete();
            return cleaned.future;
          }, onError: Error.throwWithStackTrace),
        ),
        propagateCancellation: true,
      ),
    );
    var joined = false;
    final joinedExit = runtime.run(group.join()).then((exit) {
      joined = true;
      return exit;
    });
    final empty = runtime.run(group.awaitEmpty());
    finish.complete();
    await cleaning.future;
    var started = false;
    final rejected = await runtime.run(
      group.start(Task.sync(() => started = true)),
    );
    expect((_cause(rejected) as CauseDefect).error, isA<StateError>());
    expect(started, isFalse);
    expect(member.poll(), isA<OptionNone>());
    expect(joined, isFalse);
    cleaned.complete();

    expect(await owner, isA<ExitSuccess>());
    expect(await joinedExit, isA<ExitSuccess>());
    expect(await empty, isA<ExitSuccess>());
    expect(
      _cause(await runtime.runOrThrow(member.awaitExit())),
      isA<CauseCancelled>(),
    );
  });

  test(
    'shutdown waits for native work and propagates cleanup defects once',
    () async {
      final runtime = Runtime(onUnhandledError: _unexpected);
      addTearDown(runtime.close);
      final ready = Completer<FiberGroup<String>>();
      final finish = Completer<void>();
      final owner = runtime.run(
        FiberGroup.create<String>().flatMap((group) {
          ready.complete(group);
          return _future(finish.future);
        }),
      );
      final group = await ready.future;
      await runtime.runOrThrow(group.start(Task.fail('first')));
      var ownerFinished = false;
      owner.then((_) => ownerFinished = true);
      final work = Completer<void>();
      final stopping = Completer<void>();
      final defect = Cause<Never>.defect(
        StateError('close failed'),
        StackTrace.fromString('close stack'),
      );
      await runtime.runOrThrow(
        group.start(
          _future(work.future)
              .withError<String>()
              .ensuring(Task.failCause(defect)),
        ),
      );
      await runtime.runOrThrow(
        group.start(Task.fromCallback<void, String>((_) => stopping.complete)),
      );

      finish.complete();
      await stopping.future;
      expect(ownerFinished, isFalse);
      work.complete();
      expect(_cause(await owner), same(defect));
      expect(
        (_cause(await runtime.run(group.join())) as CauseFailure).error,
        'first',
      );
    },
  );

  test(
    'synchronous finalizers see the running member and can submit another',
    () async {
      final (runtime, group) = await _group<Never>();
      final work = Completer<void>();
      late Future<Exit<void, Never>> empty;
      var wasEmpty = false;
      final task = Task.succeed(42).ensuring(
        Task.sync(() {
          empty = runtime.run(group.awaitEmpty()).then((exit) {
            wasEmpty = true;
            return exit;
          });
        }).flatMap((_) => group.start(_future(work.future)).map((_) {})),
      );
      final first = await runtime.runOrThrow(group.start(task));
      expect(await runtime.runOrThrow(first.join()), 42);
      expect(wasEmpty, isFalse);
      work.complete();
      expect(await empty, isA<ExitSuccess>());
    },
  );

  test(
    'each creation has its own scope lifetime and rejects foreign submission',
    () async {
      final (runtime, group) = await _group<Never>();
      final other = Runtime();
      addTearDown(other.close);
      var started = false;
      final rejected = await other.run(
        group.start(Task.sync(() => started = true)),
      );
      expect((_cause(rejected) as CauseDefect).error, isA<StateError>());
      expect(started, isFalse);

      final create = FiberGroup.create<Never>();
      final a = await runtime.runOrThrow(create);
      final b = await runtime.runOrThrow(create);
      expect(a, isNot(same(b)));
      expect(await runtime.run(a.join()), isA<ExitSuccess>());
      expect(
        (_cause(
          await runtime.run(a.start(Task.succeed(1))),
        ) as CauseDefect).error,
        isA<StateError>(),
      );
    },
  );
}

Future<(Runtime, FiberGroup<E>)> _group<E>({
  void Function(Cause<Object?>)? onUnhandledError,
}) async {
  final key = ServiceKey<FiberGroup<E>>('jobs');
  final created = await Runtime.create(
    key.layer(FiberGroup.create<E>()),
    onUnhandledError: onUnhandledError ?? _unexpected,
  );
  final runtime = (created as ExitSuccess<Runtime, Never>).value;
  addTearDown(runtime.close);
  return (runtime, await runtime.runOrThrow(Task.service(key)));
}

void _unexpected(Cause<Object?> cause) =>
    fail('Unexpected background failure: $cause');

Task<T, Never> _future<T>(Future<T> future) =>
    Task.fromFuture(() => future, onError: Error.throwWithStackTrace);

Cause<E> _cause<T, E>(Exit<T, E> exit) => (exit as ExitFailure<T, E>).cause;
