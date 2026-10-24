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
  tearDown(() async {
    await runtime.close();
  });

  test('all is lazy, defaults to sequential, and can be reused', () async {
    final log = <String>[];
    Iterable<Task<int, Never>> tasks() sync* {
      for (var value = 0; value < 3; value++) {
        log.add('iterate $value');
        yield Task.sync(() {
          log.add('run $value');
          return value;
        }).ensuring(Task.sync(() => log.add('release $value')));
      }
    }

    final task = Task.all(tasks());
    expect(log, isEmpty);
    expect(_value(await runtime.run(task)), [0, 1, 2]);
    expect(log, [
      for (var value = 0; value < 3; value++) ...[
        'iterate $value',
        'run $value',
        'release $value',
      ],
    ]);
    log.clear();
    expect(_value(await runtime.run(task)), [0, 1, 2]);
    expect(log, hasLength(9));
  });

  for (final mode in [
    'all',
    'forEach',
    'allSettled',
    'zip',
    'race',
    'firstSuccess',
  ]) {
    test('$mode leaves worker resources in the caller scope', () async {
      final acquired = <int>[];
      final released = <int>[];
      Task<int, Never> acquire(int value) => Task.acquireRelease(
        Task.sync(() {
          acquired.add(value);
          return value;
        }),
        release: (value, _) => Task.sync(() => released.add(value)),
      );
      final tasks = [acquire(1), acquire(2)];
      final Task<List<int>, Never> combined = switch (mode) {
        'all' => Task.all(tasks, concurrency: 2),
        'forEach' => Task.forEach([1, 2], acquire, concurrency: 2),
        'allSettled' => Task.allSettled(
          tasks,
          concurrency: 2,
        ).map((exits) => [for (final exit in exits) _value(exit)]),
        'zip' => Task.zip(
          tasks.first,
          tasks.last,
          concurrency: 2,
        ).map((values) => [values.$1, values.$2]),
        'race' => Task.race(tasks).map((value) => [value]),
        _ => Task.firstSuccess(tasks).map((value) => [value]),
      };

      final values = await runtime.runOrThrow(
        combined.map((values) {
          expect(released, isEmpty);
          return values;
        }),
      );

      expect(values, isNotEmpty);
      expect(released, unorderedEquals(acquired));
      expect(reports, isEmpty);
    });
  }

  test(
    'explicitly scoped workers close their resources before delivery',
    () async {
      final released = <int>[];
      final task = Task.forEach(
        [1, 2],
        (value) => Task.acquireRelease(
          Task.succeed(value),
          release: (value, _) => Task.sync(() => released.add(value)),
        ).scoped(),
        concurrency: 2,
      );

      expect(
        await runtime.runOrThrow(
          task.map((values) {
            expect(released, unorderedEquals([1, 2]));
            return values;
          }),
        ),
        [1, 2],
      );
    },
  );

  test('failed workers retain resources while the caller recovers', () async {
    var released = false;
    final worker = Task.acquireRelease(
      Task.succeed(1).withError<String>(),
      release: (_, _) => Task.sync(() => released = true),
    ).flatMap<int>((_) => Task.fail('failed'));

    final result = await runtime.runOrThrow(
      Task.all([worker]).recover((error) {
        expect(error, 'failed');
        expect(released, isFalse);
        return Task.succeed(<int>[]);
      }),
    );

    expect(result, isEmpty);
    expect(released, isTrue);
  });

  test(
    'race stops its loser without closing the loser resource scope',
    () async {
      var released = false;
      final loser = _Gate<int, Never>();
      final winner = _Gate<int, Never>();
      final acquired = Task.acquireRelease(
        Task.succeed(1),
        release: (_, _) => Task.sync(() => released = true),
      ).flatMap((_) => loser.task);
      final running = runtime.runOrThrow(
        Task.race([acquired, winner.task]).map((value) {
          expect(loser.cancelled, isTrue);
          expect(released, isFalse);
          return value;
        }),
      );

      await loser.ready.future;
      await winner.ready.future;
      winner.complete(const Exit.success(2));
      expect(await running, 2);
      expect(released, isTrue);
    },
  );

  test(
    'forEach admits only available slots and preserves input order',
    () async {
      final gates = List.generate(4, (_) => _Gate<int, String>());
      final iterated = <int>[];
      final started = <int>[];
      Iterable<int> values() sync* {
        for (var i = 0; i < gates.length; i++) {
          iterated.add(i);
          yield i;
        }
      }

      final running = runtime.run(
        Task.forEach(values(), (i) {
          started.add(i);
          return gates[i].task;
        }, concurrency: 2),
      );
      expect(iterated, [0, 1]);
      expect(started, [0, 1]);
      gates[1].succeed(11);
      await _turn();
      expect(iterated, [0, 1, 2]);
      gates[2].succeed(12);
      await _turn();
      expect(iterated, [0, 1, 2, 3]);
      gates[3].succeed(13);
      gates[0].succeed(10);
      expect(_value(await running), [10, 11, 12, 13]);
    },
  );

  test(
    'all short circuits without invoking or iterating later input',
    () async {
      final touched = <int>[];
      Iterable<int> values() sync* {
        for (var i = 0; i < 10; i++) {
          touched.add(i);
          yield i;
        }
      }

      final cause = _cause(
        await runtime.run(Task.forEach(values(), (_) => Task.fail('failed'))),
      );
      expect((cause as CauseFailure<String>).error, 'failed');
      expect(touched, [0]);
    },
  );

  test(
    'all waits for cancelled peers and retains cleanup defects once',
    () async {
      final cleanup = Completer<void>();
      addTearDown(() {
        if (!cleanup.isCompleted) cleanup.complete();
      });
      final defect = StateError('peer release');
      final first = _Gate<int, String>();
      final peer = _Gate<int, String>(
        onCancel: () async {
          await cleanup.future;
          throw defect;
        },
      );
      var done = false;
      final running = runtime
          .run(Task.all([first.task, peer.task], concurrency: 2))
          .then((exit) {
            done = true;
            return exit;
          });
      first.fail('first');
      await _turn();
      expect(peer.cancelled, isTrue);
      expect(done, isFalse);
      cleanup.complete();
      final reasons = _reasons(_cause(await running));
      expect(reasons, hasLength(2));
      expect((reasons[0] as CauseFailure<String>).error, 'first');
      expect((reasons[1] as CauseDefect).error, same(defect));
      await _turn();
      expect(reports, isEmpty);
    },
  );

  test('all retains masked sibling failures in observation order', () async {
    final first = _Gate<int, String>();
    final second = _Gate<int, String>();
    final running = runtime.run(
      Task.uninterruptibleMask(
        (_) => Task.all([first.task, second.task], concurrency: 2),
      ),
    );
    second.fail('second');
    first.fail('first');

    final reasons = _reasons(_cause(await running));
    expect(reasons.map((cause) => (cause as CauseFailure<String>).error), [
      'second',
      'first',
    ]);
  });

  test(
    'independent fibers keep identical defect objects as two events',
    () async {
      final defect = Cause<Never>.defect(Object(), StackTrace.current);
      final first = _Gate<int, Never>();
      final second = _Gate<int, Never>();
      final running = runtime.run(
        Task.all([first.task, second.task], concurrency: 2),
      );
      first.complete(Exit.failure(defect));
      second.complete(Exit.failure(defect));
      expect(_reasons(_cause(await running)), [same(defect), same(defect)]);
    },
  );

  test(
    'forwarding one fiber failure does not duplicate its defect event',
    () async {
      final defect = Cause<Never>.defect(Object(), StackTrace.current);
      final gate = _Gate<int, Never>();
      final running = runtime.run(
        gate.task.ensuring(Task.failCause(defect)).fork().flatMap((fiber) {
          final both = Task.all([fiber.join(), fiber.join()], concurrency: 2);
          return Task.sync(() => gate.succeed(1)).flatMap((_) => both);
        }),
      );
      expect(_reasons(_cause(await running)), [same(defect)]);
    },
  );

  test('iterator failures stop and clean all already started work', () async {
    final iteratorError = StateError('iterator');
    final cleanupError = StateError('release');
    final peer = _Gate<int, Never>(onCancel: () => throw cleanupError);
    Iterable<Task<int, Never>> tasks() sync* {
      yield peer.task;
      throw iteratorError;
    }

    final reasons = _reasons(
      _cause(await runtime.run(Task.all(tasks(), concurrency: 2))),
    );
    expect(peer.cancelled, isTrue);
    expect(reasons.map((cause) => (cause as CauseDefect).error), [
      iteratorError,
      cleanupError,
    ]);
  });

  test('iterator getter errors remain lazy defects', () async {
    final error = StateError('iterator getter');
    final task = Task.all(_BadIterable<Task<int, Never>>(error));
    expect((_cause(await runtime.run(task)) as CauseDefect).error, same(error));
  });

  test('forEach callback exceptions are defects and release peers', () async {
    final error = StateError('action');
    final gate = _Gate<int, Never>();
    final result = await runtime.run(
      Task.forEach([0, 1, 2], (value) {
        if (value == 1) throw error;
        return gate.task;
      }, concurrency: 2),
    );

    expect((_cause(result) as CauseDefect).error, same(error));
    expect(gate.cancelled, isTrue);
  });

  test(
    'empty collections succeed and invalid limits fail without iteration',
    () async {
      expect(_value(await runtime.run(Task.all<int, Never>([]))), isEmpty);
      expect(
        _value(await runtime.run(Task.allSettled<int, String>([]))),
        isEmpty,
      );
      for (final limit in [0, -1]) {
        final result = await runtime.run(
          Task.all(
            _BadIterable<Task<int, Never>>(StateError('must not iterate')),
            concurrency: limit,
          ),
        );
        expect((_cause(result) as CauseDefect).error, isArgumentError);
      }
    },
  );

  test(
    'allSettled preserves each complete exit without failing early',
    () async {
      final defect = StateError('cleanup');
      final task = Task.allSettled<int?, String>([
        Task.succeed(null),
        Task.fail('business'),
        Task.succeed(2).ensuring(Task.sync(() => throw defect)),
        Task.failCause(const Cause.cancelled('participant')),
      ], concurrency: 2);
      final exits = _value(await runtime.run(task));

      expect(_value(exits[0]), isNull);
      expect((_cause(exits[1]) as CauseFailure<String>).error, 'business');
      expect((_cause(exits[2]) as CauseDefect).error, same(defect));
      expect((_cause(exits[3]) as CauseCancelled).reason, 'participant');
      await _turn();
      expect(reports, isEmpty);
    },
  );

  test(
    'allSettled does not turn iteration errors into participant exits',
    () async {
      final error = StateError('iterator');
      Iterable<Task<int, Never>> tasks() sync* {
        yield Task.succeed(1);
        throw error;
      }

      expect(
        (_cause(
          await runtime.run(Task.allSettled(tasks())),
        ) as CauseDefect).error,
        same(error),
      );
    },
  );

  test('allSettled retains cleanup defects when iteration aborts', () async {
    final iteratorError = StateError('iterator');
    final cleanupError = StateError('cleanup');
    final gate = _Gate<int, Never>(onCancel: () => throw cleanupError);
    Iterable<Task<int, Never>> tasks() sync* {
      yield gate.task;
      throw iteratorError;
    }

    final reasons = _reasons(
      _cause(await runtime.run(Task.allSettled(tasks(), concurrency: 2))),
    );
    expect(reasons.map((cause) => (cause as CauseDefect).error), [
      iteratorError,
      cleanupError,
    ]);
  });

  for (final settled in [false, true]) {
    test(
      'owner cancellation waits for every cleanup and retains defects (settled: $settled)',
      () async {
        final controller = CancellationController();
        final cleanup = Completer<void>();
        addTearDown(() {
          if (!cleanup.isCompleted) cleanup.complete();
        });
        final error = StateError('release');
        final first = _Gate<int, String>(
          onCancel: () async {
            await cleanup.future;
            throw error;
          },
        );
        final second = _Gate<int, String>();
        final Task<Object?, Object?> task = settled
            ? Task.allSettled([first.task, second.task], concurrency: 2)
            : Task.all([first.task, second.task], concurrency: 2);
        var done = false;
        final running = runtime.run(task, signal: controller.signal).then((
          exit,
        ) {
          done = true;
          return exit;
        });
        controller.requestCancel(reason: 'owner');
        await _turn();
        expect(first.cancelled, isTrue);
        expect(second.cancelled, isTrue);
        expect(done, isFalse);
        cleanup.complete();
        final reasons = _reasons(_cause(await running));
        expect(
          reasons.whereType<CauseCancelled>().map((cause) => cause.reason),
          ['owner'],
        );
        expect(reasons.whereType<CauseDefect>().map((cause) => cause.error), [
          error,
        ]);
        expect(reasons.whereType<CauseFailure>(), isEmpty);
      },
    );
  }

  test('all waits for an uncancellable Future after short circuit', () async {
    final future = Completer<int>();
    addTearDown(() {
      if (!future.isCompleted) future.complete(1);
    });
    final first = _Gate<int, String>();
    var done = false;
    final running = runtime
        .run(
          Task.all([
            first.task,
            Task.fromFuture(() => future.future, onError: (_, _) => 'future'),
          ], concurrency: 2),
        )
        .then((exit) {
          done = true;
          return exit;
        });
    first.fail('first');
    await _turn();
    expect(done, isFalse);
    future.complete(1);
    expect((_cause(await running) as CauseFailure<String>).error, 'first');
  });

  test('race returns the first complete exit after loser cleanup', () async {
    final cleanup = Completer<void>();
    addTearDown(() {
      if (!cleanup.isCompleted) cleanup.complete();
    });
    final first = _Gate<int, String>(onCancel: () => cleanup.future);
    final second = _Gate<int, String>();
    var done = false;
    final running = runtime.run(Task.race([first.task, second.task])).then((
      exit,
    ) {
      done = true;
      return exit;
    });
    second.succeed(2);
    await _turn();
    expect(first.cancelled, isTrue);
    expect(done, isFalse);
    cleanup.complete();
    expect(_value(await running), 2);
  });

  test(
    'race uses full cleanup completion rather than body completion',
    () async {
      final cleanup = Completer<void>();
      addTearDown(() {
        if (!cleanup.isCompleted) cleanup.complete();
      });
      final first = _Gate<int, Never>();
      final second = _Gate<int, Never>();
      final running = runtime.run(
        Task.race([first.task.ensuring(_future(cleanup.future)), second.task]),
      );
      first.succeed(1);
      await _turn();
      second.succeed(2);
      await _turn();
      cleanup.complete();
      expect(_value(await running), 2);
    },
  );

  test(
    'race keeps winner failure and ignores loser business failures',
    () async {
      final first = _Gate<int, String>();
      final second = _Gate<int, String>();
      final running = runtime.run(Task.race([first.task, second.task]));
      second.fail('winner');
      first.fail('loser');
      expect((_cause(await running) as CauseFailure<String>).error, 'winner');
    },
  );

  test('race propagates loser defects even when the winner succeeds', () async {
    final error = StateError('loser');
    final first = _Gate<int, String>();
    final second = _Gate<int, String>();
    final running = runtime.run(Task.race([first.task, second.task]));
    first.succeed(1);
    second.complete(Exit.failure(Cause.defect(error, StackTrace.current)));
    expect((_cause(await running) as CauseDefect).error, same(error));
  });

  test(
    'race preserves explicit cancellation of the winning participant',
    () async {
      final winner = _Gate<int, Never>();
      final loser = _Gate<int, Never>();
      final running = runtime.run(Task.race([winner.task, loser.task]));
      winner.complete(const Exit.failure(Cause.cancelled('participant')));
      expect((_cause(await running) as CauseCancelled).reason, 'participant');
      expect(loser.cancelled, isTrue);
    },
  );

  for (final firstSuccess in [false, true]) {
    test(
      'owner cancellation stops every competition participant (firstSuccess: $firstSuccess)',
      () async {
        final controller = CancellationController();
        final release = Completer<void>();
        addTearDown(() {
          if (!release.isCompleted) release.complete();
        });
        final error = StateError('cleanup');
        final first = _Gate<int, String>(
          onCancel: () async {
            await release.future;
            throw error;
          },
        );
        final second = _Gate<int, String>();
        final tasks = [first.task, second.task];
        var done = false;
        final running = runtime
            .run(
              firstSuccess ? Task.firstSuccess(tasks) : Task.race(tasks),
              signal: controller.signal,
            )
            .then((exit) {
              done = true;
              return exit;
            });
        controller.requestCancel(reason: 'owner');
        await _turn();
        expect(done, isFalse);
        expect(first.cancelled, isTrue);
        expect(second.cancelled, isTrue);
        release.complete();
        final reasons = _reasons(_cause(await running));
        expect(
          reasons.whereType<CauseCancelled>().map((cause) => cause.reason),
          ['owner'],
        );
        expect(reasons.whereType<CauseDefect>().map((cause) => cause.error), [
          error,
        ]);
      },
    );
  }

  test('firstSuccess recovers business failures until a success', () async {
    final first = _Gate<int, String>();
    final second = _Gate<int, String>();
    final third = _Gate<int, String>();
    final running = runtime.run(
      Task.firstSuccess([first.task, second.task, third.task]),
    );
    first.fail('first');
    await _turn();
    expect(second.cancelled, isFalse);
    expect(third.cancelled, isFalse);
    second.succeed(2);
    expect(_value(await running), 2);
    expect(third.cancelled, isTrue);
  });

  test(
    'firstSuccess retains all causes when every participant fails',
    () async {
      final first = _Gate<int, String>();
      final second = _Gate<int, String>();
      final third = _Gate<int, String>();
      final running = runtime.run(
        Task.firstSuccess([first.task, second.task, third.task]),
      );
      second.fail('second');
      first.complete(const Exit.failure(Cause.cancelled('self')));
      third.fail('third');
      final reasons = _reasons(_cause(await running));
      expect(reasons, hasLength(3));
      expect((reasons[0] as CauseFailure<String>).error, 'second');
      expect((reasons[1] as CauseCancelled).reason, 'self');
      expect((reasons[2] as CauseFailure<String>).error, 'third');
    },
  );

  test('firstSuccess does not silently recover earlier defects', () async {
    final error = StateError('first');
    final first = _Gate<int, String>();
    final second = _Gate<int, String>();
    final running = runtime.run(Task.firstSuccess([first.task, second.task]));
    first.complete(Exit.failure(Cause.defect(error, StackTrace.current)));
    await _turn();
    second.succeed(2);
    expect((_cause(await running) as CauseDefect).error, same(error));
  });

  test('empty races and firstSuccess computations fail lazily', () async {
    for (final task in [
      Task.race<int, Never>([]),
      Task.firstSuccess<int, Never>([]),
    ]) {
      expect(
        (_cause(await runtime.run(task)) as CauseDefect).error,
        isArgumentError,
      );
    }
  });

  test(
    'zip retains heterogeneous value types and sequential default',
    () async {
      final log = <String>[];
      final Task<(int, String?), Never> task = Task.zip(
        Task.sync(() {
          log.add('left');
          return 1;
        }),
        Task.sync(() {
          log.add('right');
          return null;
        }),
      );
      expect(_value(await runtime.run(task)), (1, null));
      expect(log, ['left', 'right']);
    },
  );

  test(
    'zip starts both sides only with a concurrency of at least two',
    () async {
      final left = _Gate<int, String>();
      final right = _Gate<String, String>();
      final running = runtime.run(
        Task.zip(left.task, right.task, concurrency: 2),
      );
      expect(left.started, isTrue);
      expect(right.started, isTrue);
      right.succeed('right');
      left.succeed(1);
      expect(_value(await running), (1, 'right'));
    },
  );

  test(
    'children inherit services and locals without merging local changes',
    () async {
      final key = ServiceKey<String>('name');
      final local = TaskLocal(0);
      final task = local
          .locally(
            7,
            Task.all([
              Task.service(key).flatMap(
                (service) => local.get().map((value) => '$service:$value'),
              ),
              local.locally(8, local.get().map((value) => 'local:$value')),
              local.get().map((value) => 'parent:$value'),
            ], concurrency: 3),
          )
          .provideContext(Context.from([key.bind('service')]));
      expect(_value(await runtime.run(task)), [
        'service:7',
        'local:8',
        'parent:7',
      ]);
    },
  );

  test(
    'cancellation during result handoff retains undelivered defects',
    () async {
      final controller = CancellationController();
      final error = StateError('worker');
      final cleaning = Completer<void>();
      final release = Completer<void>();
      addTearDown(() {
        if (!release.isCompleted) release.complete();
      });
      var delivered = false;
      final task = Task.allSettled([Task.sync<int>(() => throw error)])
          .ensuring(
            Task.sync(() => cleaning.complete())
                .flatMap((_) => _future(release.future)),
          )
          .map((_) {
            delivered = true;
          });

      final running = runtime.run(task, signal: controller.signal);
      await cleaning.future;
      controller.requestCancel(reason: 'owner');
      release.complete();
      final reasons = _reasons(_cause(await running));
      expect(delivered, isFalse);
      expect(reasons.whereType<CauseCancelled>().map((cause) => cause.reason), [
        'owner',
      ]);
      expect(reasons.whereType<CauseDefect>().map((cause) => cause.error), [
        error,
      ]);
    },
  );

  test('large sequential traversal is stack safe', () async {
    final result = await runtime.run(
      Task.forEach(Iterable.generate(10000), (value) => Task.succeed(value)),
    );
    final values = _value(result);
    expect(values, hasLength(10000));
    expect(values.first, 0);
    expect(values.last, 9999);
  });
}

final class _Gate<T, E> {
  _Gate({this.onCancel});

  final FutureOr<void> Function()? onCancel;
  final ready = Completer<void>();
  late void Function(Exit<T, E>) complete;
  var started = false;
  var cancelled = false;

  Task<T, E> get task => Task.fromCallback((complete) {
    started = true;
    this.complete = complete;
    if (!ready.isCompleted) ready.complete();
    return () {
      cancelled = true;
      return onCancel?.call();
    };
  });

  void succeed(T value) => complete(Exit.success(value));
  void fail(E error) => complete(Exit.failure(Cause.failure(error)));
}

final class _BadIterable<T> extends Iterable<T> {
  _BadIterable(this.error);
  final Object error;

  @override
  Iterator<T> get iterator => throw error;
}

Task<void, Never> _future(Future<void> future) => Task.fromFuture<void, Never>(
  () => future,
  onError: (error, stackTrace) => Error.throwWithStackTrace(error, stackTrace),
);

T _value<T, E>(Exit<T, E> exit) => (exit as ExitSuccess<T, E>).value;
Cause<E> _cause<T, E>(Exit<T, E> exit) => (exit as ExitFailure<T, E>).cause;
List<Cause<E>> _reasons<E>(Cause<E> cause) => switch (cause) {
  CauseGroup(:final causes) => causes,
  _ => [cause],
};
Future<void> _turn() => Future<void>.delayed(Duration.zero);
