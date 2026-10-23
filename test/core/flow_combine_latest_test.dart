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

  test('construction is lazy and widened nullable records are independently consumed', () async {
    var calls = 0;
    final Flow<num?, Object?> left = Flow.fromTask(
      Task.sync<int?>(() {
        calls++;
        return null;
      }),
    );
    final Flow<(num?, String?), Object?> combined = left.combineLatest(
      Flow.fromTask(Task.succeed<String?>(null)),
    );
    expect(calls, 0);
    expect(_value(await runtime.run(combined.take(0).toList())), isEmpty);
    expect(calls, 0);
    for (var run = 0; run < 2; run++) {
      expect(_value(await runtime.run(combined.toList())), [(null, null)]);
    }
    expect(calls, 2);
  });

  test(
    'initial updates retain only the latest until both inputs have values',
    () async {
      final left = _Source<int>(4);
      final right = _Source<String>(2);
      final values = <(int, String)>[];
      final running = runtime.run(
        left.flow
            .combineLatest(right.flow)
            .forEach((pair) => Task.sync(() => values.add(pair))),
      );
      await Future.wait([
        left.gates[0].started.future,
        right.gates[0].started.future,
      ]);
      await left.emit(0, 1);
      await left.emit(1, 2);
      await left.gates[2].started.future;
      expect(values, isEmpty);
      await right.emit(0, 'a');
      await right.gates[1].started.future;
      expect(values, [(2, 'a')]);
      await left.emit(2, 3);
      await left.gates[3].started.future;
      expect(values, [(2, 'a'), (3, 'a')]);
      await left.end(3);
      await right.end(1);
      expect(await running, isA<ExitSuccess>());
    },
  );

  test(
    'each update after initialization emits in observed completion order',
    () async {
      final left = _Source<int>(3);
      final right = _Source<String>(3);
      final values = <(int, String)>[];
      final running = runtime.run(
        left.flow
            .combineLatest(right.flow)
            .forEach((pair) => Task.sync(() => values.add(pair))),
      );
      await left.emit(0, 1);
      await right.emit(0, 'a');
      await Future.wait([
        left.gates[1].started.future,
        right.gates[1].started.future,
      ]);
      await right.emit(1, 'b');
      await right.gates[2].started.future;
      await left.emit(1, 2);
      await left.gates[2].started.future;
      expect(values, [(1, 'a'), (1, 'b'), (2, 'b')]);
      await left.end(2);
      await right.end(2);
      expect(await running, isA<ExitSuccess>());
    },
  );

  test(
    'a completed input keeps its last value while the other continues',
    () async {
      final right = _Source<String>(4);
      final values = <(int, String)>[];
      final running = runtime.run(
        Flow.fromIterable([1])
            .combineLatest(right.flow)
            .forEach((pair) => Task.sync(() => values.add(pair))),
      );
      for (var index = 0; index < 3; index++) {
        await right.emit(index, 'value$index');
      }
      await right.end(3);
      expect(await running, isA<ExitSuccess>());
      expect(values, [(1, 'value0'), (1, 'value1'), (1, 'value2')]);
    },
  );

  test(
    'an input with no value stops a pending sibling in either position',
    () async {
      for (final emptyOnLeft in [true, false]) {
        final pending = _Gate<int, Never>();
        final empty = Flow.fromIterable(<int>[]);
        final source = Flow.fromTask(pending.task);
        final running = runtime.run(
          (emptyOnLeft
                  ? empty.combineLatest(source)
                  : source.combineLatest(empty))
              .toList(),
        );
        await pending.started.future;
        expect(
          _value(await running.timeout(const Duration(seconds: 1))),
          isEmpty,
        );
        expect(pending.cancelled, isTrue);
      }
    },
  );

  test('an empty input stops a sibling that is still opening', () async {
    final pending = _Gate<FlowCursor<int, Never>, Never>();
    final running = runtime.run(
      flowFromCursor(pending.task)
          .combineLatest(Flow.fromIterable(<String>[]))
          .toList(),
    );
    await pending.started.future;
    expect(_value(await running.timeout(const Duration(seconds: 1))), isEmpty);
    expect(pending.cancelled, isTrue);
  });

  test('an empty input releases an already acquired latest value', () async {
    final empty = _Source<String>(1);
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
    final running = runtime.run(left.combineLatest(empty.flow).toList());
    await acquired.future;
    await empty.end(0);
    expect(_value(await running), isEmpty);
    expect(released, isTrue);
  });

  test(
    'latest resources from a completed side stay alive for later combinations',
    () async {
      final right = _Source<int>(3);
      late _Resource resource;
      final left = Flow.fromTask(
        Task.acquireRelease(
          Task.sync(() => resource = _Resource(1)),
          release: (value, _) => Task.sync(value.close),
        ),
      );
      final values = <int>[];
      final running = runtime.run(
        left
            .combineLatest(right.flow)
            .forEach(
              (pair) => Task.sync(() {
                expect(pair.$1.closed, isFalse);
                values.add(pair.$2);
              }),
            ),
      );
      await right.emit(0, 10);
      await right.gates[1].started.future;
      expect(resource.closed, isFalse);
      await right.emit(1, 20);
      await right.end(2);
      expect(await running, isA<ExitSuccess>());
      expect(values, [10, 20]);
      expect(resource.closed, isTrue);
    },
  );

  test(
    'replacing an initial latest releases its resources before the first pair',
    () async {
      final left = _Source<int>(3);
      final right = _Source<String>(1);
      final resources = <_Resource>[];
      final source = left.flow.flatMap(
        (index) => Flow.fromTask(
          Task.acquireRelease(
            Task.sync(() {
              final value = _Resource(index);
              resources.add(value);
              return value;
            }),
            release: (value, _) => Task.sync(value.close),
          ),
        ),
      );
      final running = runtime.run(
        source.combineLatest(right.flow).take(1).map((pair) {
          expect(pair.$1.closed, isFalse);
          return (pair.$1.index, pair.$2);
        }).toList(),
      );
      await left.emit(0, 0);
      await left.emit(1, 1);
      await left.gates[2].started.future;
      expect(resources[0].closed, isTrue);
      expect(resources[1].closed, isFalse);
      await right.emit(0, 'a');
      expect(_value(await running), [(1, 'a')]);
      expect(resources.every((resource) => resource.closed), isTrue);
    },
  );

  test(
    'older outputs retain independent leases after a latest value advances',
    () async {
      final left = _Source<int>(3);
      final right = _Source<int>(2);
      final leftResources = <_Resource>[];
      final rightResources = <_Resource>[];
      Flow<_Resource, Never> resourceFlow(
        _Source<int> source,
        List<_Resource> resources,
      ) => source.flow.flatMap(
        (index) => Flow.fromTask(
          Task.acquireRelease(
            Task.sync(() {
              final value = _Resource(index);
              resources.add(value);
              return value;
            }),
            release: (value, _) => Task.sync(value.close),
          ),
        ),
      );
      final mapped = [Completer<void>(), Completer<void>()];
      final finish = [Completer<void>(), Completer<void>()];
      addTearDown(() {
        for (final gate in finish) {
          if (!gate.isCompleted) gate.complete();
        }
      });
      final running = runtime.run(
        resourceFlow(
          left,
          leftResources,
        ).combineLatest(resourceFlow(right, rightResources)).mapTask((pair) {
          mapped[pair.$1.index].complete();
          return _future(finish[pair.$1.index].future).map((_) {
            expect(pair.$1.closed, isFalse);
            expect(pair.$2.closed, isFalse);
            return pair.$1.index;
          });
        }, concurrency: 2).toList(),
      );
      await left.emit(0, 0);
      await right.emit(0, 0);
      await mapped[0].future;
      await right.end(1);
      await left.emit(1, 1);
      await mapped[1].future;
      expect(leftResources[0].closed, isFalse);
      expect(rightResources.single.closed, isFalse);
      finish[0].complete();
      await leftResources[0].released.future;
      expect(leftResources[1].closed, isFalse);
      expect(rightResources.single.closed, isFalse);
      await left.end(2);
      finish[1].complete();
      expect(_value(await running), [0, 1]);
      expect(
        [
          ...leftResources,
          ...rightResources,
        ].every((resource) => resource.closed),
        isTrue,
      );
    },
  );

  test(
    'downstream blocking allows at most one pending update per input',
    () async {
      final left = _Source<int>(4);
      final right = _Source<int>(4);
      final entered = Completer<void>();
      final finish = Completer<void>();
      addTearDown(() {
        if (!finish.isCompleted) finish.complete();
      });
      final running = runtime.run(
        left.flow.combineLatest(right.flow).take(1).forEach((_) {
          entered.complete();
          return _future(finish.future);
        }),
      );
      await left.emit(0, 0);
      await right.emit(0, 0);
      await entered.future;
      await _turn();
      expect(left.gates[2].started.isCompleted, isFalse);
      expect(right.gates[2].started.isCompleted, isFalse);
      finish.complete();
      expect(await running, isA<ExitSuccess>());
    },
  );

  test(
    'buffered outputs keep both old values alive as both latest values advance',
    () async {
      final left = _Source<int>(3);
      final right = _Source<int>(3);
      final a = <_Resource>[];
      final b = <_Resource>[];
      Flow<_Resource, Never> resourceFlow(
        _Source<int> source,
        List<_Resource> values,
      ) => source.flow.flatMap(
        (index) => Flow.fromTask(
          Task.acquireRelease(
            Task.sync(() {
              final value = _Resource(index);
              values.add(value);
              return value;
            }),
            release: (value, _) => Task.sync(value.close),
          ),
        ),
      );
      final entered = Completer<void>();
      final finish = Completer<void>();
      addTearDown(() {
        if (!finish.isCompleted) finish.complete();
      });
      final values = <(int, int)>[];
      final buffered = Completer<void>();
      var produced = 0;
      final running = runtime.run(
        resourceFlow(left, a)
            .combineLatest(resourceFlow(right, b))
            .map((pair) {
              if (++produced == 3) buffered.complete();
              return pair;
            })
            .buffer(2)
            .forEach((pair) {
              expect(pair.$1.closed, isFalse);
              expect(pair.$2.closed, isFalse);
              values.add((pair.$1.index, pair.$2.index));
              if (values.length != 1) return Task.succeed(null);
              entered.complete();
              return _future(finish.future);
            }),
      );
      await left.emit(0, 0);
      await right.emit(0, 0);
      await entered.future;
      await left.emit(1, 1);
      await left.gates[2].started.future;
      await right.emit(1, 1);
      await buffered.future;
      expect(a.first.closed, isFalse);
      expect(b.first.closed, isFalse);
      final endings = Future.wait([left.end(2), right.end(2)]);
      finish.complete();
      await endings;
      expect(await running, isA<ExitSuccess>());
      expect(values, [(0, 0), (1, 0), (1, 1)]);
      expect([...a, ...b].every((value) => value.closed), isTrue);
    },
  );

  test(
    'replacement cleanup failure stops production and releases the new latest',
    () async {
      final left = _Source<int>(3);
      final right = _Source<String>(1);
      final closed = <int>[];
      final fault = StateError('replacement');
      final resources = left.flow.flatMap(
        (value) => Flow.fromTask(
          Task.acquireRelease(
            Task.succeed(value),
            release: (value, _) => Task.sync(() => closed.add(value)).flatMap(
              (_) => value == 0
                  ? Task.failCause(
                      Cause<Never>.combine([
                        const Cause.cancelled('cleanup'),
                        Cause.defect(fault, StackTrace.current),
                      ]),
                    )
                  : Task.succeed(null),
            ),
          ),
        ),
      );
      final running = runtime.run(
        resources.combineLatest(right.flow).toList().exit(),
      );
      await left.emit(0, 0);
      await left.emit(1, 1);
      final exit = _value(await running);
      expect(_defects(exit), [same(fault)]);
      expect(
        _causes(exit).whereType<CauseCancelled>().single.reason,
        'cleanup',
      );
      expect(closed, [0, 1]);
      expect(right.gates[0].cancelled, isTrue);
    },
  );

  test('failure after initialization stops the other input and releases latest values', () async {
    final left = _Source<int>(2);
    final right = _Source<int>(2);
    final first = Completer<void>();
    var releases = 0;
    final resources = left.flow.flatMap(
      (value) => Flow.fromTask(
        Task.acquireRelease(
          Task.succeed(value),
          release: (_, _) => Task.sync(() => releases++),
        ),
      ),
    );
    final running = runtime.run(
      resources
          .combineLatest(right.flow)
          .forEach((_) => Task.sync(() => first.complete())),
    );
    await left.emit(0, 1);
    await right.emit(0, 2);
    await first.future;
    await Future.wait([
      left.gates[1].started.future,
      right.gates[1].started.future,
    ]);
    final fault = StateError('source');
    left.gates[1].complete(
      Exit.failure(Cause.defect(fault, StackTrace.current)),
    );
    final exit = await running;
    expect(_defects(exit), [same(fault)]);
    expect(right.gates[1].cancelled, isTrue);
    expect(releases, 1);
  });

  test('owner cancellation releases a latest value while waiting for the other first value', () async {
    final controller = CancellationController();
    final left = _Source<int>(2);
    final right = _Source<String>(1);
    var released = false;
    final source = left.flow.flatMap(
      (value) => Flow.fromTask(
        Task.acquireRelease(
          Task.succeed(value),
          release: (_, _) => Task.sync(() => released = true),
        ),
      ),
    );
    final running = runtime.run(
      source.combineLatest(right.flow).toList(),
      signal: controller.signal,
    );
    await left.emit(0, 1);
    await left.gates[1].started.future;
    controller.requestCancel(reason: 'owner');
    final exit = await running;
    expect(_causes(exit).whereType<CauseCancelled>().single.reason, 'owner');
    expect(released, isTrue);
    expect(right.gates[0].cancelled, isTrue);
  });

  test(
    'early termination retains complete cleanup causes exactly once',
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
              .combineLatest(Flow.fromIterable(['a', 'b']))
              .take(1)
              .toList()
              .exit(),
        ),
      );
      expect(_defects(exit), [same(fault)]);
      expect(
        _causes(exit).whereType<CauseCancelled>().single.reason,
        'cleanup',
      );
    },
  );

  test(
    'empty input waits for native work and preserves a late failure',
    () async {
      final started = Completer<void>();
      final finish = Completer<int>();
      final fault = StateError('native');
      addTearDown(() {
        if (!finish.isCompleted) finish.complete(1);
      });
      final left = Flow.fromTask(
        Task.fromFuture<int, String>(() {
          started.complete();
          return finish.future;
        }, onError: (_, _) => 'mapped'),
      );
      var completed = false;
      final running = runtime
          .run(left.combineLatest(Flow.fromIterable(<String>[])).toList())
          .then((exit) {
            completed = true;
            return exit;
          });
      await started.future;
      await _turn();
      expect(completed, isFalse);
      finish.completeError(fault, StackTrace.current);
      expect(_defects(await running), [same(fault)]);
    },
  );
}

final class _Resource {
  _Resource(this.index);
  final int index;
  final released = Completer<void>();
  var closed = false;
  void close() {
    closed = true;
    released.complete();
  }
}

final class _Source<T> {
  _Source(int count)
    : gates = List.generate(count, (_) => _Gate<Option<T>, Never>());
  final List<_Gate<Option<T>, Never>> gates;
  Flow<T, Never> get flow => Flow.unfold<int, T, Never>(
    0,
    (index) => gates[index].task.map(
      (option) => option.map((value) => (value, index + 1)),
    ),
  );
  Future<void> emit(int index, T value) async {
    await gates[index].started.future;
    gates[index].succeed(Option.some(value));
  }

  Future<void> end(int index) async {
    await gates[index].started.future;
    gates[index].succeed(const Option.none());
  }
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
