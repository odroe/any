import 'dart:async';

import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  Runtime runtime() {
    final value = Runtime();
    addTearDown(value.close);
    return value;
  }

  test('batch is lazy, ordered, deduplicated, and immutable', () async {
    final calls = <List<int>>[];
    final resolver = RequestResolver<int, int, String>((keys) {
      calls.add(keys);
      expect(() => keys.clear(), throwsUnsupportedError);
      return Task.succeed([for (final key in keys) Result.success(key * 2)]);
    });
    final input = [2, 1, 2];
    final task = resolver.batch(input);
    expect(calls, isEmpty);
    input.add(3);
    final results = await runtime().runOrThrow(task);
    expect(_values(results), [4, 2, 4, 6]);
    expect(calls, [
      [2, 1, 3],
    ]);
    expect(() => results.clear(), throwsUnsupportedError);
  });

  test('concurrent requests coalesce without an explicit batch', () async {
    final calls = <List<int>>[];
    final resolver = RequestResolver<int, int, Never>((keys) {
      calls.add(keys);
      return Task.succeed([for (final key in keys) Result.success(key)]);
    });
    expect(
      await runtime().runOrThrow(
        Task.all([
          resolver.request(1),
          resolver.request(2),
          resolver.request(1),
        ], concurrency: 3),
      ),
      [1, 2, 1],
    );
    expect(calls, [
      [1, 2],
    ]);
  });

  test(
    'explicit submission groups do not absorb later distinct keys',
    () async {
      final calls = <List<int>>[];
      final resolver = RequestResolver<int, int, Never>((keys) {
        calls.add(keys);
        return Task.succeed([for (final key in keys) Result.success(key)]);
      }, concurrency: 2);
      await runtime().runOrThrow(
        Task.all([
          resolver.batch([1, 2]),
          resolver.batch([3, 4]),
        ], concurrency: 2),
      );
      expect(calls, [
        [1, 2],
        [3, 4],
      ]);
    },
  );

  test('successful request caching is limited to one root execution', () async {
    var loads = 0;
    final resolver = RequestResolver<int, int, Never>((keys) {
      loads++;
      return Task.succeed([for (final key in keys) Result.success(key)]);
    });
    final task = resolver.request(1).flatMap((_) => resolver.request(1));
    final host = runtime();
    expect(await host.runOrThrow(task), 1);
    expect(loads, 1);
    expect(await host.runOrThrow(task), 1);
    expect(loads, 2);
  });

  test('disabled caching retains only unfinished deduplication', () async {
    var loads = 0;
    final resolver = RequestResolver<int, int, Never>((keys) {
      loads++;
      return Task.succeed([for (final key in keys) Result.success(key)]);
    }, cache: false);
    await runtime().runOrThrow(
      Task.all([
        resolver.request(1),
        resolver.request(1),
      ], concurrency: 2).flatMap((_) => resolver.request(1)),
    );
    expect(loads, 2);
  });

  test('LRU capacity is shared across environment partitions', () async {
    final calls = <List<int>>[];
    final tag = TaskLocal<Object?>(null);
    final first = Object();
    final second = Object();
    final resolver = RequestResolver<int, int, Never>((keys) {
      calls.add(keys);
      return Task.succeed([for (final key in keys) Result.success(key)]);
    }, cacheCapacity: 2);
    await runtime().runOrThrow(
      tag
          .locally(first, resolver.request(1))
          .flatMap((_) => tag.locally(second, resolver.request(2)))
          .flatMap((_) => tag.locally(first, resolver.request(1)))
          .flatMap((_) => tag.locally(second, resolver.request(3)))
          .flatMap((_) => tag.locally(second, resolver.request(2))),
    );
    expect(calls, [
      [1],
      [2],
      [3],
      [2],
    ]);
  });

  test('per-key errors remain Results and are not cached', () async {
    var loads = 0;
    final resolver = RequestResolver<int, int, String>((keys) {
      loads++;
      return Task.succeed([
        for (final key in keys)
          if (key == 1)
            const Result.failure('missing')
          else
            Result.success(key),
      ]);
    });
    final host = runtime();
    await host.runOrThrow(
      resolver.batch([1, 2, 1]).flatMap((results) {
        expect(results[0], isA<ResultFailure<int, String>>());
        expect(results[1], isA<ResultSuccess<int, String>>());
        return resolver.request(1).recover((error) {
          expect(error, 'missing');
          return Task.succeed(0);
        });
      }),
    );
    expect(loads, 2);
  });

  test(
    'empty batches do not start work and invalid limits fail early',
    () async {
      Task<List<Result<int, Never>>, Never> load(List<int> keys) =>
          throw StateError('must not run');
      final resolver = RequestResolver<int, int, Never>(load);
      expect(await runtime().runOrThrow(resolver.batch([])), isEmpty);
      expect(() => RequestResolver(load, maxBatchSize: 0), throwsRangeError);
      expect(() => RequestResolver(load, concurrency: 0), throwsRangeError);
      expect(() => RequestResolver(load, cacheCapacity: 0), throwsRangeError);
    },
  );

  test('input iteration fails before any key is submitted', () async {
    var called = false;
    final error = StateError('iterator');
    final resolver = RequestResolver<int, int, Never>((keys) {
      called = true;
      return Task.succeed([]);
    });
    final exit = await runtime().run(resolver.batch(_brokenKeys(error)));
    expect(_defects(exit).single.error, same(error));
    expect(called, isFalse);
  });

  test(
    'effective context compares binding identity, never service equality',
    () async {
      final tenant = ServiceKey<_Tenant>('tenant');
      final a = _Tenant('a');
      final b = _Tenant('b');
      final calls = <(String, List<int>)>[];
      final resolver = RequestResolver<int, String, Never>(
        (keys) => Task.service(tenant).map((value) {
          calls.add((value.name, keys));
          return [for (final key in keys) Result.success('${value.name}:$key')];
        }),
      );
      final result = await runtime().runOrThrow(
        Task.all([
          resolver.request(1).provideContext(Context.from([tenant.bind(a)])),
          resolver.request(2).provideContext(Context.from([tenant.bind(a)])),
          resolver.request(1).provideContext(Context.from([tenant.bind(b)])),
        ], concurrency: 3),
      );
      expect(result, ['a:1', 'a:2', 'b:1']);
      expect(calls.map((call) => call.$1), ['a', 'b']);
      expect(calls.map((call) => call.$2), [
        [1, 2],
        [1],
      ]);
    },
  );

  test('task-local overrides isolate both batches and cached values', () async {
    final tenant = TaskLocal<Object?>(null);
    final a = Object();
    final b = Object();
    final calls = <(Object?, List<int>)>[];
    final resolver = RequestResolver<int, Object?, Never>(
      (keys) => tenant.get().map((value) {
        calls.add((value, keys));
        return [for (final _ in keys) Result.success(value)];
      }),
    );
    await runtime().runOrThrow(
      Task.all([
        tenant.locally(a, resolver.request(1)),
        tenant.locally(a, resolver.request(2)),
        tenant.locally(b, resolver.request(1)),
      ], concurrency: 3).flatMap((results) {
        expect(results, [same(a), same(a), same(b)]);
        return tenant.locally(b, resolver.request(1));
      }),
    );
    expect(calls.map((call) => call.$1), [same(a), same(b)]);
    expect(calls.map((call) => call.$2), [
      [1, 2],
      [1],
    ]);
  });

  test('batch size and concurrency bound active loaders', () async {
    final calls = <List<int>>[];
    final gates = <Completer<void>>[];
    final firstTwo = Completer<void>();
    final third = Completer<void>();
    var active = 0;
    var maximum = 0;
    final resolver = RequestResolver<int, int, Never>(
      (keys) {
        calls.add(keys);
        final gate = Completer<void>();
        gates.add(gate);
        active++;
        if (active > maximum) maximum = active;
        if (calls.length == 2) firstTwo.complete();
        if (calls.length == 3) third.complete();
        return _wait(gate.future)
            .map(
              (_) => [for (final key in keys) Result<int, Never>.success(key)],
            )
            .ensuring(
              Task.sync(() {
                active--;
              }),
            );
      },
      maxBatchSize: 2,
      concurrency: 2,
    );
    final running = runtime().runOrThrow(resolver.batch([1, 2, 3, 4, 5]));
    await firstTwo.future;
    expect(calls, [
      [1, 2],
      [3, 4],
    ]);
    gates[0].complete();
    await third.future;
    expect(calls[2], [5]);
    expect(maximum, 2);
    gates[1].complete();
    gates[2].complete();
    expect(_values(await running), [1, 2, 3, 4, 5]);
    expect(active, 0);
  });

  test('result-count mismatch settles every request with one defect', () async {
    final resolver = RequestResolver<int, int, Never>(
      (keys) => Task.succeed([]),
    );
    final exit = await runtime().run(
      Task.all([
        resolver.request(1),
        resolver.request(2),
        resolver.request(3),
      ], concurrency: 3),
    );
    expect(_defects(exit), hasLength(1));
    expect(_defects(exit).single.error, isA<StateError>());
  });

  test(
    'loader cleanup completes before values and cleanup defects propagate',
    () async {
      final error = StateError('release');
      final cleaned = Completer<void>();
      final resolver = RequestResolver<int, int, Never>(
        (keys) => Task.addFinalizer(
          (_) => Task.sync(() {
            cleaned.complete();
            throw error;
          }),
        ).map((_) => [for (final key in keys) Result<int, Never>.success(key)]),
      );
      final exit = await runtime().run(resolver.batch([1, 2]));
      expect(cleaned.isCompleted, isTrue);
      expect(_defects(exit).single.error, same(error));
    },
  );

  test(
    'whole-batch failures are combined once per batch in completion order',
    () async {
      final gates = [Completer<void>(), Completer<void>()];
      final ready = Completer<void>();
      var started = 0;
      final resolver = RequestResolver<int, int, String>(
        (keys) {
          if (++started == 2) ready.complete();
          final index = keys.first == 1 ? 0 : 1;
          return _wait(gates[index].future)
              .withError<String>()
              .flatMap((_) => Task.fail('batch $index'));
        },
        maxBatchSize: 2,
        concurrency: 2,
      );
      final running = runtime().run(resolver.batch([1, 2, 3, 4]));
      await ready.future;
      gates[1].complete();
      await Future<void>.delayed(Duration.zero);
      gates[0].complete();
      expect(
        _causes(await running)
            .whereType<CauseFailure<String>>()
            .map((c) => c.error),
        ['batch 1', 'batch 0'],
      );
    },
  );

  test(
    'cancelling one duplicate waiter leaves the shared load alive',
    () async {
      final started = Completer<void>();
      final gate = Completer<void>();
      var loads = 0;
      var stopped = false;
      final resolver = RequestResolver<int, int, Never>((keys) {
        loads++;
        started.complete();
        return _wait(gate.future)
            .map(
              (_) => [for (final key in keys) Result<int, Never>.success(key)],
            )
            .ensuring(Task.sync(() => stopped = true).map((_) {}));
      });
      final host = runtime();
      final running = host.runOrThrow(
        resolver
            .request(1)
            .fork()
            .flatMap(
              (first) => resolver
                  .request(1)
                  .fork()
                  .flatMap(
                    (second) => _wait(started.future)
                        .flatMap((_) => first.cancel())
                        .flatMap((exit) {
                          expect(_causes(exit).single, isA<CauseCancelled>());
                          expect(stopped, isFalse);
                          gate.complete();
                          return second.join();
                        }),
                  ),
            ),
      );
      expect(await running, 1);
      expect(loads, 1);
      expect(stopped, isTrue);
    },
  );

  test(
    'last waiter cancellation waits for cleanup and preserves defects',
    () async {
      final started = Completer<void>();
      final cleaning = Completer<void>();
      final gate = Completer<void>();
      final error = StateError('cancel cleanup');
      final resolver = RequestResolver<int, int, Never>(
        (keys) =>
            Task.addFinalizer(
              (_) =>
                  Task.sync(cleaning.complete)
                      .flatMap((_) => _wait(gate.future))
                      .flatMap((_) => Task.sync<void>(() => throw error)),
            ).flatMap(
              (_) => Task.fromCallback<List<Result<int, Never>>, Never>((
                complete,
              ) {
                started.complete();
                return () {};
              }),
            ),
      );
      final controller = CancellationController();
      final running = runtime().run(
        resolver.request(1),
        signal: controller.signal,
      );
      await started.future;
      controller.requestCancel(reason: 'stop');
      await cleaning.future;
      var finished = false;
      unawaited(running.then((_) => finished = true));
      expect(finished, isFalse);
      gate.complete();
      final exit = await running;
      expect(_defects(exit).single.error, same(error));
      expect(_causes(exit).whereType<CauseCancelled>().single.reason, 'stop');
    },
  );

  test(
    'invalidate isolates generations while previous callers finish',
    () async {
      final firstStarted = Completer<void>();
      final firstGate = Completer<void>();
      var loads = 0;
      final resolver = RequestResolver<int, int, Never>((keys) {
        final version = ++loads;
        if (version == 1) {
          firstStarted.complete();
          return _wait(firstGate.future).map((_) => [Result.success(version)]);
        }
        return Task.succeed([Result.success(version)]);
      }, concurrency: 2);
      final result = await runtime().runOrThrow(
        resolver
            .request(1)
            .fork()
            .flatMap(
              (old) =>
                  _wait(firstStarted.future)
                      .flatMap((_) => resolver.invalidate(1))
                      .flatMap((_) => resolver.request(1))
                      .flatMap((fresh) {
                        expect(fresh, 2);
                        firstGate.complete();
                        return old.join();
                      })
                      .flatMap((previous) {
                        expect(previous, 1);
                        return resolver.request(1);
                      }),
            ),
      );
      expect(result, 2);
      expect(loads, 2);
    },
  );

  test(
    'recursive resolution fails instead of occupying its own only slot',
    () async {
      late RequestResolver<int, int, Never> resolver;
      resolver = RequestResolver(
        (keys) => resolver
            .request(2)
            .map((value) => [for (final _ in keys) Result.success(value)]),
      );
      final exit = await runtime().run(resolver.request(1));
      expect(_defects(exit).single.error, isA<StateError>());
    },
  );

  test('a waiter cancelled before dispatch does not start a loader', () async {
    var loads = 0;
    final resolver = RequestResolver<int, int, Never>((keys) {
      loads++;
      return Task.succeed([for (final key in keys) Result.success(key)]);
    });
    final exit = await runtime().runOrThrow(
      resolver.request(1).fork().flatMap((fiber) => fiber.cancel()),
    );
    expect(_causes(exit).single, isA<CauseCancelled>());
    expect(loads, 0);
  });

  test('forkIn within the request tree shares its cached entries', () async {
    var loads = 0;
    final resolver = RequestResolver<int, int, Never>((keys) {
      loads++;
      return Task.succeed([for (final key in keys) Result.success(key)]);
    });
    expect(
      await runtime().runOrThrow(
        Scope.open().flatMap(
          (scope) => resolver
              .request(1)
              .flatMap((_) => resolver.request(1).forkIn(scope))
              .flatMap((fiber) => fiber.join()),
        ),
      ),
      1,
    );
    expect(loads, 1);
  });

  test(
    'background forks use a new domain after their creator has finished',
    () async {
      var loads = 0;
      final resolver = RequestResolver<int, int, Never>((keys) {
        final version = ++loads;
        return Task.succeed([for (final _ in keys) Result.success(version)]);
      });
      final scopeKey = ServiceKey<Scope>('background');
      final built = await Runtime.create(scopeKey.layer(Scope.open()));
      final host = (built as ExitSuccess<Runtime, Never>).value;
      addTearDown(host.close);
      final gate = Completer<void>();
      final background = await host.runOrThrow(
        Task.service(scopeKey).flatMap(
          (scope) => resolver.request(1).flatMap((first) {
            expect(first, 1);
            return _wait(gate.future)
                .flatMap((_) => resolver.request(1))
                .forkIn(scope);
          }),
        ),
      );
      gate.complete();
      expect(await host.runOrThrow(background.join()), 2);
      expect(loads, 2);
    },
  );

  test('root finalizers resolve requests in their own live scope', () async {
    var loads = 0;
    final observed = <int>[];
    final resolver = RequestResolver<int, int, Never>((keys) {
      final version = ++loads;
      return Task.succeed([for (final _ in keys) Result.success(version)]);
    });
    await runtime().runOrThrow(
      Task.addFinalizer((_) => resolver.request(1).map(observed.add))
          .flatMap((_) => resolver.request(1))
          .map(observed.add),
    );
    expect(observed, [1, 2]);
  });

  test('cancelling one key lets its batch peers finish and admits a fresh generation', () async {
    final firstStarted = Completer<void>();
    final firstGate = Completer<void>();
    final calls = <List<int>>[];
    var loads = 0;
    final resolver = RequestResolver<int, int, Never>((keys) {
      calls.add(keys);
      final version = ++loads;
      if (version == 1) {
        firstStarted.complete();
        return _wait(firstGate.future).map(
          (_) => [for (final _ in keys) Result<int, Never>.success(version)],
        );
      }
      return Task.succeed([for (final _ in keys) Result.success(version)]);
    }, concurrency: 2);
    await runtime().runOrThrow(
      resolver
          .request(1)
          .fork()
          .flatMap(
            (first) => resolver
                .request(2)
                .fork()
                .flatMap(
                  (second) =>
                      _wait(firstStarted.future)
                          .flatMap((_) => first.cancel())
                          .flatMap((_) => resolver.request(1))
                          .flatMap((fresh) {
                            expect(fresh, 2);
                            firstGate.complete();
                            return second.join();
                          })
                          .flatMap((oldPeer) {
                            expect(oldPeer, 1);
                            return resolver.request(1);
                          })
                          .map((cached) => expect(cached, 2)),
                ),
          ),
    );
    expect(calls, [
      [1, 2],
      [1],
    ]);
  });

  test(
    'cancellation at result delivery retains a loader defect once',
    () async {
      final error = StateError('handoff');
      final controller = CancellationController();
      final resolver = RequestResolver<int, int, Never>(
        (keys) =>
            Task.addFinalizer(
              (_) => Task.sync(() {
                controller.requestCancel(reason: 'at completion');
              }),
            ).flatMap(
              (_) => Task.sync<List<Result<int, Never>>>(() => throw error),
            ),
      );
      final exit = await runtime().run(
        resolver.request(1),
        signal: controller.signal,
      );
      expect(_defects(exit).map((cause) => cause.error), [same(error)]);
      expect(_causes(exit).whereType<CauseCancelled>(), hasLength(1));
    },
  );

  test('last waiter preserves explicit cleanup cancellation, even with the outer reason', () async {
    for (final cleanupReason in ['cleanup', 'stop']) {
      final started = Completer<void>();
      final controller = CancellationController();
      final resolver = RequestResolver<int, int, Never>(
        (keys) =>
            Task.addFinalizer(
              (_) => Task.failCause(Cause.cancelled(cleanupReason)),
            ).flatMap(
              (_) => Task.fromCallback<List<Result<int, Never>>, Never>((
                complete,
              ) {
                started.complete();
                return () {};
              }),
            ),
      );
      final running = runtime().run(
        resolver.request(1),
        signal: controller.signal,
      );
      await started.future;
      controller.requestCancel(reason: 'stop');
      final exit = await running;
      expect(
        _causes(exit).whereType<CauseCancelled>().map((cause) => cause.reason),
        ['stop', cleanupReason],
      );
    }
  });

  test(
    'delivered cleanup cancellation is not repeated by request cleanup',
    () async {
      final resolver = RequestResolver<int, int, Never>(
        (keys) => Task.addFinalizer(
          (_) => Task.failCause(Cause.cancelled('cleanup')),
        ).map((_) => [for (final key in keys) Result<int, Never>.success(key)]),
      );
      final exit = await runtime().run(resolver.batch([1, 2]));
      expect(
        _causes(exit).whereType<CauseCancelled>().map((cause) => cause.reason),
        ['cleanup'],
      );
    },
  );

  test('queued requests cancelled behind a busy batch are removed', () async {
    final started = Completer<void>();
    final gate = Completer<void>();
    final calls = <List<int>>[];
    final resolver = RequestResolver<int, int, Never>((keys) {
      calls.add(keys);
      if (keys.single == 1) {
        started.complete();
        return _wait(gate.future).map((_) => [const Result.success(1)]);
      }
      return Task.succeed([Result.success(keys.single)]);
    });
    await runtime().runOrThrow(
      resolver
          .request(1)
          .fork()
          .flatMap(
            (first) =>
                _wait(started.future)
                    .flatMap((_) => resolver.request(2).fork())
                    .flatMap((queued) => queued.cancel())
                    .flatMap((cancelled) {
                      expect(_causes(cancelled).single, isA<CauseCancelled>());
                      expect(calls, [
                        [1],
                      ]);
                      gate.complete();
                      return first.join();
                    })
                    .flatMap((_) => resolver.request(3)),
          ),
    );
    expect(calls, [
      [1],
      [3],
    ]);
  });

  test(
    'throwing loader callbacks retain the original defect and stack',
    () async {
      final error = StateError('loader');
      final stack = StackTrace.fromString('original loader stack');
      final resolver = RequestResolver<int, int, Never>(
        (_) => Error.throwWithStackTrace(error, stack),
      );
      final exit = await runtime().run(resolver.batch([1, 2]));
      final defect = _defects(exit).single;
      expect(defect.error, same(error));
      expect(defect.stackTrace.toString(), stack.toString());
    },
  );

  test('overlapping root runs never share an unfinished load', () async {
    final started = Completer<void>();
    final gate = Completer<void>();
    var loads = 0;
    final resolver = RequestResolver<int, int, Never>((keys) {
      final version = ++loads;
      if (loads == 2) started.complete();
      return _wait(gate.future).map((_) => [Result.success(version)]);
    });
    final host = runtime();
    final first = host.runOrThrow(resolver.request(1));
    final second = host.runOrThrow(resolver.request(1));
    await started.future;
    gate.complete();
    expect(await Future.wait([first, second]), [1, 2]);
  });

  test('widened resolvers preserve static error composition', () async {
    final RequestResolver<int, int, String> resolver =
        RequestResolver<int, int, Never>(
          (keys) => Task.succeed([for (final key in keys) Result.success(key)]),
        );
    final exit = await runtime().run(
      resolver.request(1).flatMap((_) => Task.fail('later')),
    );
    expect(_causes(exit).single, isA<CauseFailure<String>>());
  });
}

Task<void, Never> _wait(Future<void> future) =>
    Task.fromFuture(() => future, onError: Error.throwWithStackTrace);
List<V> _values<V, E>(List<Result<V, E>> results) => [
  for (final result in results) (result as ResultSuccess<V, E>).value,
];
List<Cause<Object?>> _causes(Exit<Object?, Object?> exit) => switch (exit) {
  ExitSuccess() => const [],
  ExitFailure(cause: CauseGroup(:final causes)) => causes,
  ExitFailure(:final cause) => [cause],
};
List<CauseDefect> _defects(Exit<Object?, Object?> exit) =>
    _causes(exit).whereType<CauseDefect>().toList();
Iterable<int> _brokenKeys(Object error) sync* {
  yield 1;
  throw error;
}

final class _Tenant {
  _Tenant(this.name);
  final String name;
  @override
  bool operator ==(Object other) =>
      throw StateError('Do not compare services.');
  @override
  int get hashCode => throw StateError('Do not hash services.');
}
