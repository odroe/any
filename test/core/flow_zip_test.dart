import 'dart:async';

import 'package:any/any.dart';
import 'package:any/src/core/flow.dart' show flowFromCursor;
import 'package:any/src/core/internal/flow_cursor.dart';
import 'package:test/test.dart';

void main() {
  late Runtime runtime;
  final reports = <Cause<Object?>>[];
  setUp(() {
    reports.clear();
    runtime = Runtime(onUnhandledError: reports.add);
  });
  tearDown(() async {
    await runtime.close();
    await _turn();
    expect(reports, isEmpty);
  });

  test(
    'zip is lazy and repeated consumption produces typed nullable records',
    () async {
      var calls = 0;
      final Flow<num?, Object?> left = Flow.fromTask(
        Task.sync<int?>(() {
          calls++;
          return null;
        }),
      ).concat(Flow.fromIterable([1]));
      final Flow<(num?, String), Object?> zipped = left.zip(
        Flow.fromIterable(['a', 'b', 'c']),
      );
      expect(calls, 0);
      expect(_value(await runtime.run(zipped.take(0).toList())), isEmpty);
      expect(calls, 0);
      for (var run = 0; run < 2; run++) {
        expect(_value(await runtime.run(zipped.toList())), [
          (null, 'a'),
          (1, 'b'),
        ]);
      }
      expect(calls, 2);
    },
  );

  test(
    'the shorter side determines the number of pairs in either position',
    () async {
      final left = Flow.fromIterable([1, 2]);
      final right = Flow.fromIterable(['a', 'b', 'c']);
      expect(_value(await runtime.run(left.zip(right).toList())), [
        (1, 'a'),
        (2, 'b'),
      ]);
      expect(_value(await runtime.run(right.zip(left).toList())), [
        ('a', 1),
        ('b', 2),
      ]);
      expect(
        _value(
          await runtime.run(Flow.fromIterable(<int>[]).zip(right).toList()),
        ),
        isEmpty,
      );
      expect(
        _value(
          await runtime.run(left.zip(Flow.fromIterable(<String>[])).toList()),
        ),
        isEmpty,
      );
    },
  );

  test(
    'both reads begin concurrently and neither value is emitted alone',
    () async {
      final left = _Gate<int, Never>();
      final right = _Gate<String, Never>();
      var delivered = false;
      final running = runtime.run(
        Flow.fromTask(left.task).zip(Flow.fromTask(right.task)).map((pair) {
          delivered = true;
          return pair;
        }).toList(),
      );
      await Future.wait([left.started.future, right.started.future]);
      right.succeed('a');
      await _turn();
      expect(delivered, isFalse);
      left.succeed(1);
      expect(_value(await running), [(1, 'a')]);
    },
  );

  test(
    'an empty side stops a pending read and waits its cancellation cleanup',
    () async {
      for (final emptyOnLeft in [true, false]) {
        final started = Completer<void>();
        final stopping = Completer<void>();
        final finish = Completer<void>();
        addTearDown(() {
          if (!finish.isCompleted) finish.complete();
        });
        final pending = Flow.fromTask(
          Task.fromCallback<int, Never>((_) {
            started.complete();
            return () {
              stopping.complete();
              return finish.future;
            };
          }),
        );
        final empty = Flow.fromIterable(<int>[]);
        var completed = false;
        final running = runtime
            .run(
              (emptyOnLeft ? empty.zip(pending) : pending.zip(empty)).toList(),
            )
            .then((exit) {
              completed = true;
              return exit;
            });
        await Future.wait([started.future, stopping.future])
            .timeout(const Duration(seconds: 1));
        expect(completed, isFalse);
        finish.complete();
        expect(_value(await running), isEmpty);
      }
    },
  );

  test(
    'an empty side can stop the other side while it is still opening',
    () async {
      final opening = _Gate<FlowCursor<int, Never>, Never>();
      final running = runtime.run(
        flowFromCursor((_) => opening.task)
            .zip(Flow.fromIterable(<String>[]))
            .toList(),
      );
      await opening.started.future;
      expect(
        _value(await running.timeout(const Duration(seconds: 1))),
        isEmpty,
      );
      expect(opening.cancelled, isTrue);
    },
  );

  test(
    'an empty side waits a native Future and retains its late defect',
    () async {
      final started = Completer<void>();
      final finish = Completer<int>();
      final fault = StateError('native');
      addTearDown(() {
        if (!finish.isCompleted) finish.complete(1);
      });
      final pending = Flow.fromTask(
        Task.fromFuture<int, String>(() {
          started.complete();
          return finish.future;
        }, onError: (_, _) => 'mapped'),
      );
      var completed = false;
      final running = runtime
          .run(pending.zip(Flow.fromIterable(<String>[])).toList())
          .then((exit) {
            completed = true;
            return exit;
          });
      await started.future;
      await _turn();
      expect(completed, isFalse);
      finish.completeError(fault, StackTrace.current);
      final exit = await running;
      expect(_defects(exit), [same(fault)]);
      expect(_causes(exit).whereType<CauseFailure>(), isEmpty);
    },
  );

  test(
    'an unpaired resource is released on the shorter input ending',
    () async {
      var released = 0;
      final acquired = Completer<void>();
      final resource = Flow.fromTask(
        Task.acquireRelease(
          Task.sync(() {
            acquired.complete();
            return 1;
          }),
          release: (_, _) => Task.sync(() => released++),
        ),
      );
      final running = runtime.run(
        resource.zip(Flow.fromIterable(<String>[])).toList(),
      );
      await acquired.future;
      expect(_value(await running), isEmpty);
      expect(released, 1);
    },
  );

  test(
    'source reads stay bounded while the consuming callback is blocked',
    () async {
      final pulls = [<int>[], <int>[]];
      final entered = Completer<void>();
      final finish = Completer<void>();
      addTearDown(() {
        if (!finish.isCompleted) finish.complete();
      });
      Flow<int, Never> source(int side) => Flow.unfold<int, int, Never>(
        0,
        (state) => Task.sync(() {
          pulls[side].add(state);
          return state == 3
              ? const Option.none()
              : Option.some((state, state + 1));
        }),
      );
      final running = runtime.run(
        source(0).zip(source(1)).forEach((pair) {
          if (pair.$1 != 0) return Task.succeed(null);
          entered.complete();
          return _future(finish.future);
        }),
      );
      await entered.future;
      await _turn();
      expect(pulls, [
        [0],
        [0],
      ]);
      finish.complete();
      expect(await running, isA<ExitSuccess>());
      expect(pulls, [
        [0, 1, 2, 3],
        [0, 1, 2, 3],
      ]);
    },
  );

  test(
    'failure stops its sibling and preserves cancellation cleanup defects',
    () async {
      final left = _Gate<int, String>();
      final started = Completer<void>();
      final fault = StateError('stop');
      final pending = Task.fromCallback<String, String>((_) {
        started.complete();
        return () => throw fault;
      });
      final running = runtime.run(
        Flow.fromTask(left.task).zip(Flow.fromTask(pending)).toList(),
      );
      await Future.wait([left.started.future, started.future]);
      left.complete(const Exit.failure(Cause.failure('source')));
      final exit = await running;
      expect(
        _causes(exit).whereType<CauseFailure<String>>().single.error,
        'source',
      );
      expect(_defects(exit), [same(fault)]);
    },
  );

  test(
    'cancellation between the two results releases the acquired side',
    () async {
      final controller = CancellationController();
      final pending = _Gate<String, Never>();
      final acquired = Completer<void>();
      var released = false;
      final left = Flow.fromTask(
        Task.acquireRelease(
          Task.sync(() {
            acquired.complete();
            return 1;
          }),
          release: (_, _) => Task.sync(() => released = true),
        ),
      );
      final running = runtime.run(
        left.zip(Flow.fromTask(pending.task)).toList(),
        signal: controller.signal,
      );
      await Future.wait([acquired.future, pending.started.future]);
      controller.requestCancel(reason: 'owner');
      final exit = await running;
      expect(pending.cancelled, isTrue);
      expect(released, isTrue);
      expect(_causes(exit).whereType<CauseCancelled>().single.reason, 'owner');
    },
  );

  test(
    'downstream concurrent mapping and buffering hold both resource leases',
    () async {
      final all = <_Resource>[];
      final mapped = [Completer<void>(), Completer<void>()];
      final finish = [Completer<void>(), Completer<void>()];
      addTearDown(() {
        for (final gate in finish) {
          if (!gate.isCompleted) gate.complete();
        }
      });
      Flow<_Resource, Never> source(String side) =>
          Flow.fromIterable([0, 1]).flatMap(
            (index) => Flow.fromTask(
              Task.acquireRelease(
                Task.sync(() {
                  final value = _Resource(side, index);
                  all.add(value);
                  return value;
                }),
                release: (value, _) => Task.sync(() => value.closed = true),
              ),
            ),
          );
      final running = runtime.run(
        source('a')
            .zip(source('b'))
            .buffer(2)
            .mapTask(
              (pair) {
                mapped[pair.$1.index].complete();
                return _future(finish[pair.$1.index].future).map((_) {
                  expect(pair.$1.closed, isFalse);
                  expect(pair.$2.closed, isFalse);
                  return (pair.$1.side, pair.$2.side, pair.$1.index);
                });
              },
              concurrency: 2,
              preserveOrder: false,
            )
            .toList(),
      );
      await Future.wait(mapped.map((event) => event.future));
      finish[1].complete();
      await _turn();
      expect(
        all
            .where((resource) => resource.index == 0)
            .every((resource) => !resource.closed),
        isTrue,
      );
      finish[0].complete();
      expect(_value(await running), [('a', 'b', 1), ('a', 'b', 0)]);
      expect(all.every((resource) => resource.closed), isTrue);
    },
  );

  test(
    'early termination keeps cleanup cancellation and defects without replay',
    () async {
      final fault = StateError('release');
      final left = Flow.fromTask(
        Task.acquireRelease(
          Task.succeed(1),
          release: (_, _) => Task.failCause(
            Cause<Never>.combine([
              const Cause.cancelled('cleanup'),
              Cause.defect(fault, StackTrace.current),
            ]),
          ),
        ),
      );
      final exit = _value(
        await runtime.run(
          left
              .zip(Flow.fromIterable(['a', 'b']))
              .take(1)
              .toList()
              .scoped()
              .exit(),
        ),
      );
      expect(
        _causes(exit).whereType<CauseCancelled>().single.reason,
        'cleanup',
      );
      expect(_defects(exit), [same(fault)]);
    },
  );

  test(
    'consumer cleanup runs before either input resource is released',
    () async {
      final events = <String>[];
      Flow<int, Never> source(String name) => Flow.fromTask(
        Task.acquireRelease(
          Task.succeed(1),
          release: (_, _) => Task.sync(() => events.add(name)),
        ),
      );
      final exit = await runtime.run(
        source('left')
            .zip(source('right'))
            .forEach(
              (_) => Task.addFinalizer(
                (_) => Task.sync(() => events.add('consumer')),
              ),
            ),
      );
      expect(exit, isA<ExitSuccess>());
      expect(events.first, 'consumer');
      expect(events.toSet(), {'consumer', 'left', 'right'});
    },
  );
}

final class _Resource {
  _Resource(this.side, this.index);
  final String side;
  final int index;
  var closed = false;
}

final class _Gate<T, E> {
  final started = Completer<void>();
  late void Function(Exit<T, E>) complete;
  var cancelled = false;
  Task<T, E> get task => Task.fromCallback((complete) {
    this.complete = complete;
    started.complete();
    return () {
      cancelled = true;
    };
  });
  void succeed(T value) => complete(Exit.success(value));
}

Future<void> _turn() => Future<void>.delayed(Duration.zero);
Task<void, Never> _future(Future<void> future) =>
    Task.fromFuture(() => future, onError: Error.throwWithStackTrace);
T _value<T, E>(Exit<T, E> exit) => switch (exit) {
  ExitSuccess(:final value) => value,
  ExitFailure(:final cause) => throw StateError('Unexpected failure: $cause'),
};
List<Cause<E>> _causes<T, E>(Exit<T, E> exit) => switch (exit) {
  ExitSuccess() => [],
  ExitFailure(cause: CauseGroup(:final causes)) => causes,
  ExitFailure(:final cause) => [cause],
};
List<Object> _defects<T, E>(Exit<T, E> exit) =>
    _causes(exit).whereType<CauseDefect>().map((cause) => cause.error).toList();
