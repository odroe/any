import 'dart:async';

import 'package:any/any.dart';
import 'package:any/src/core/flow.dart' show flowFromCursor, openFlowCursor;
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

  test('merge is lazy, independent, nullable, and covariantly typed', () async {
    var opens = 0;
    final Flow<num?, Object?> left = Flow.fromTask(
      Task.sync<int?>(() {
        opens++;
        return null;
      }),
    );
    final flow = left.merge(Flow.fromIterable([1, 2]));
    expect(opens, 0);
    expect(_value(await runtime.run(flow.take(0).toList())), isEmpty);
    expect(opens, 0);
    expect(
      _value(await runtime.run(flow.toList())),
      unorderedEquals([null, 1, 2]),
    );
    expect(
      _value(await runtime.run(flow.toList())),
      unorderedEquals([null, 1, 2]),
    );
    expect(opens, 2);
  });

  test('empty inputs end independently and input order is preserved', () async {
    final empty = Flow.fromIterable(<int>[]);
    expect(_value(await runtime.run(empty.merge(empty).toList())), isEmpty);
    final values = Flow.fromIterable([1, 2, 3]);
    expect(_value(await runtime.run(empty.merge(values).toList())), [1, 2, 3]);
    expect(_value(await runtime.run(values.merge(empty).toList())), [1, 2, 3]);
    final merged = _value(
      await runtime.run(values.merge(Flow.fromIterable([4, 5])).toList()),
    );
    expect(merged.where((value) => value < 4), [1, 2, 3]);
    expect(merged.where((value) => value >= 4), [4, 5]);
  });

  test(
    'both sources start and values follow observed completion order',
    () async {
      final left = _Gate<int, Never>();
      final right = _Gate<int, Never>();
      final received = <int>[];
      final delivered = Completer<void>();
      final running = runtime.run(
        Flow.fromTask(left.task)
            .merge(Flow.fromTask(right.task))
            .forEach(
              (value) => Task.sync(() {
                received.add(value);
                if (value == 2) delivered.complete();
              }),
            ),
      );
      await Future.wait([left.started.future, right.started.future]);
      right.succeed(2);
      await delivered.future;
      expect(received, [2]);
      left.succeed(1);
      expect(await running, isA<ExitSuccess>());
      expect(received, [2, 1]);
    },
  );

  test(
    'a pending input open does not block the other input from failing',
    () async {
      final opening = _Gate<FlowCursor<int, String>, String>();
      final rightStarted = Completer<void>();
      final left = flowFromCursor((_) => opening.task);
      final right = Flow.fromTask(
        Task.defer<int, String>(() {
          rightStarted.complete();
          return Task.fail('right');
        }),
      );
      final running = runtime.run(left.merge(right).toList());
      await Future.wait([opening.started.future, rightStarted.future])
          .timeout(const Duration(seconds: 1));
      final exit = await running;
      expect(opening.cancelled, isTrue);
      expect(
        _causes(exit).whereType<CauseFailure<String>>().single.error,
        'right',
      );
    },
  );

  test(
    'resources acquired while opening belong to the whole consumption',
    () async {
      var released = false;
      final left = flowFromCursor(
        (_) =>
            Task.addFinalizer((_) => Task.sync(() => released = true))
                .flatMap((_) => openFlowCursor(Flow.fromIterable([1, 2]))),
      );
      final received = <int>[];
      final exit = await runtime.run(
        left
            .merge(Flow.fromIterable(<int>[]))
            .forEach(
              (value) => Task.sync(() {
                expect(released, isFalse);
                received.add(value);
              }),
            ),
      );
      expect(exit, isA<ExitSuccess>());
      expect(received, [1, 2]);
      expect(released, isTrue);
    },
  );

  test(
    'the other input starts while a native Future open is pending',
    () async {
      final opening = Completer<void>();
      final finish = Completer<void>();
      final rightStarted = Completer<void>();
      addTearDown(() {
        if (!finish.isCompleted) finish.complete();
      });
      final left = flowFromCursor(
        (_) => Task.fromFuture<void, String>(
          () {
            opening.complete();
            return finish.future;
          },
          onError: (_, _) => 'open',
        ).flatMap((_) => openFlowCursor(Flow.fromIterable([1]))),
      );
      final right = Flow.fromTask(
        Task.defer<int, String>(() {
          rightStarted.complete();
          return Task.fail('right');
        }),
      );
      var completed = false;
      final running = runtime.run(left.merge(right).toList()).then((exit) {
        completed = true;
        return exit;
      });
      await Future.wait([opening.future, rightStarted.future])
          .timeout(const Duration(seconds: 1));
      await _turn();
      expect(completed, isFalse);
      finish.complete();
      expect(
        _causes(await running).whereType<CauseFailure<String>>().single.error,
        'right',
      );
    },
  );

  test(
    'each source has one pending result while downstream is blocked',
    () async {
      final pulls = [<int>[], <int>[]];
      final consuming = Completer<void>();
      final finish = Completer<void>();
      addTearDown(() {
        if (!finish.isCompleted) finish.complete();
      });
      Flow<int, Never> source(int side) => Flow.unfold<int, int, Never>(
        0,
        (int state) => Task.sync(() {
          pulls[side].add(state);
          return state == 3
              ? const Option.none()
              : Option.some((side * 10 + state, state + 1));
        }),
      );
      final running = runtime.run(
        source(0).merge(source(1)).forEach((value) {
          if (!consuming.isCompleted) {
            consuming.complete();
            return _future(finish.future);
          }
          return Task.succeed(null);
        }),
      );
      await consuming.future;
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
    'failure stops a pending sibling and waits cancellation cleanup',
    () async {
      final fail = _Gate<int, String>();
      final started = Completer<void>();
      final stopping = Completer<void>();
      final finish = Completer<void>();
      addTearDown(() {
        if (!finish.isCompleted) finish.complete();
      });
      final pending = Task.fromCallback<int, String>((_) {
        started.complete();
        return () {
          stopping.complete();
          return finish.future;
        };
      });
      var completed = false;
      final running = runtime
          .run(Flow.fromTask(fail.task).merge(Flow.fromTask(pending)).toList())
          .then((exit) {
            completed = true;
            return exit;
          });
      await Future.wait([fail.started.future, started.future]);
      fail.complete(const Exit.failure(Cause.failure('failed')));
      await stopping.future;
      expect(completed, isFalse);
      finish.complete();
      expect(
        _causes(await running).whereType<CauseFailure<String>>().single.error,
        'failed',
      );
    },
  );

  test(
    'early termination stops unused pulls and retains cleanup causes',
    () async {
      final left = _Gate<int, Never>();
      final right = _Gate<int, Never>();
      final fault = StateError('cleanup');
      final pending = Task.addFinalizer(
        (_) => Task.failCause(
          Cause<Never>.combine([
            const Cause.cancelled('cleanup'),
            Cause.defect(fault, StackTrace.current),
          ]),
        ),
      ).flatMap((_) => right.task);
      final running = runtime.run(
        Flow.fromTask(left.task).merge(Flow.fromTask(pending)).take(1).toList(),
      );
      await Future.wait([left.started.future, right.started.future]);
      left.succeed(1);
      final exit = await running;
      expect(right.cancelled, isTrue);
      expect(
        _causes(exit).whereType<CauseCancelled>().single.reason,
        'cleanup',
      );
      expect(_defects(exit), [same(fault)]);
    },
  );

  test(
    'early termination waits native Future and retains its late defect',
    () async {
      final started = Completer<void>();
      final finish = Completer<int>();
      final left = _Gate<int, String>();
      final fault = StateError('native');
      addTearDown(() {
        if (!finish.isCompleted) finish.complete(2);
      });
      final right = Task.fromFuture(() {
        started.complete();
        return finish.future;
      }, onError: (_, _) => 'mapped');
      var completed = false;
      final running = runtime
          .run(
            Flow.fromTask(left.task)
                .merge(Flow.fromTask(right))
                .take(1)
                .toList(),
          )
          .then((exit) {
            completed = true;
            return exit;
          });
      await Future.wait([started.future, left.started.future]);
      left.succeed(1);
      await _turn();
      expect(completed, isFalse);
      finish.completeError(fault, StackTrace.current);
      final exit = await running;
      expect(_defects(exit), [same(fault)]);
      expect(_causes(exit).whereType<CauseFailure>(), isEmpty);
    },
  );

  test('owner cancellation discards late source business and explicit cancellation', () async {
    for (final cause in [
      const Cause<String>.failure('late'),
      const Cause<String>.cancelled('late'),
    ]) {
      final source = _Gate<int, String>();
      final controller = CancellationController();
      final running = runtime.run(
        Flow.fromTask(Task.uninterruptibleMask<int, String>((_) => source.task))
            .merge(Flow.fromIterable(<int>[]))
            .toList(),
        signal: controller.signal,
      );
      await source.started.future;
      controller.requestCancel(reason: 'owner');
      source.complete(Exit.failure(cause));
      final exit = await running;
      expect(_causes(exit), hasLength(1));
      expect(_causes(exit).whereType<CauseCancelled>().single.reason, 'owner');
    }
  });

  test(
    'downstream concurrent mapping retains each delivered resource',
    () async {
      final releases = <int>[];
      final mapped = [Completer<void>(), Completer<void>()];
      final finish = [Completer<void>(), Completer<void>()];
      addTearDown(() {
        for (final gate in finish) {
          if (!gate.isCompleted) gate.complete();
        }
      });
      Flow<_Resource, Never> source(int value) => Flow.fromTask(
        Task.acquireRelease(
          Task.sync(() => _Resource(value)),
          release: (resource, _) => Task.sync(() {
            resource.closed = true;
            releases.add(value);
          }),
        ),
      );
      final running = runtime.run(
        source(0)
            .merge(source(1))
            .mapTask(
              (resource) {
                mapped[resource.value].complete();
                return _future(finish[resource.value].future).map((_) {
                  expect(resource.closed, isFalse);
                  return resource.value;
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
      expect(releases, isNot(contains(0)));
      finish[0].complete();
      expect(_value(await running), [1, 0]);
      expect(releases.toSet(), {0, 1});
    },
  );

  test('downstream cleanup precedes both input resource releases', () async {
    final events = <String>[];
    Flow<int, Never> source(int value) => Flow.fromTask(
      Task.acquireRelease(
        Task.succeed(value),
        release: (_, _) => Task.sync(() => events.add('source$value')),
      ),
    );
    final exit = await runtime.run(
      source(0)
          .merge(source(1))
          .buffer(2)
          .forEach(
            (value) => Task.addFinalizer(
              (_) => Task.sync(() => events.add('consumer$value')),
            ),
          ),
    );
    expect(exit, isA<ExitSuccess>());
    for (final value in [0, 1]) {
      expect(
        events.indexOf('consumer$value'),
        lessThan(events.indexOf('source$value')),
      );
    }
  });

  test(
    'explicit source cleanup cancellation survives merging and take',
    () async {
      final cleaned = Completer<void>();
      final left = Flow.fromTask(
        Task.acquireRelease(
          Task.succeed(1),
          release: (_, _) => Task.sync(() => cleaned.complete()).flatMap(
            (_) => Task.failCause(const Cause<Never>.cancelled('cleanup')),
          ),
        ).scoped(),
      );
      final right = Flow.fromIterable([2, 3]);
      final exit = await runtime.run(
        left
            .merge(right)
            .take(2)
            .forEach(
              (value) => value == 2
                  ? _future(cleaned.future).flatMap((_) => _future(_turn()))
                  : Task.succeed(null),
            ),
      );
      expect(
        _causes(exit).whereType<CauseCancelled>().single.reason,
        'cleanup',
      );
    },
  );

  test('delivered explicit cancellation is not replayed by cleanup', () async {
    final flow = Flow.fromTask(
      Task.failCause<String>(const Cause.cancelled('source')),
    ).merge(Flow.fromIterable(<Never>[]));
    final exit = _value(await runtime.run(flow.toList().exit()));
    expect(_causes(exit), hasLength(1));
    expect(_causes(exit).whereType<CauseCancelled>().single.reason, 'source');
  });

  test(
    'failure keeps sibling cancellation cleanup defects in completion order',
    () async {
      final failed = _Gate<int, Never>();
      final started = Completer<void>();
      final first = StateError('source');
      final second = StateError('stop');
      final stack = StackTrace.fromString('source failure');
      final right = Task.fromCallback<int, Never>((_) {
        started.complete();
        return () => throw second;
      });
      final running = runtime.run(
        Flow.fromTask(failed.task).merge(Flow.fromTask(right)).toList(),
      );
      await Future.wait([failed.started.future, started.future]);
      failed.complete(Exit.failure(Cause.defect(first, stack)));
      final exit = await running;
      expect(_defects(exit), [same(first), same(second)]);
      expect(
        _causes(exit).whereType<CauseDefect>().first.stackTrace,
        same(stack),
      );
    },
  );

  test('separate occurrences of the same defect are retained', () async {
    final left = _Gate<int, Never>();
    final right = _Gate<int, Never>();
    final fault = StateError('shared error object');
    final running = runtime.run(
      Flow.fromTask(left.task)
          .merge(
            Flow.fromTask(
              Task.uninterruptibleMask<int, Never>((_) => right.task),
            ),
          )
          .toList(),
    );
    await Future.wait([left.started.future, right.started.future]);
    left.complete(Exit.failure(Cause.defect(fault, StackTrace.current)));
    right.complete(Exit.failure(Cause.defect(fault, StackTrace.current)));
    expect(_defects(await running), [same(fault), same(fault)]);
  });

  test(
    'cancellation during resource result handoff cannot hide a lease',
    () async {
      final controller = CancellationController();
      var armed = false;
      var intercepted = false;
      var released = 0;
      var consumed = 0;
      final left = Flow.fromTask(
        Task.acquireRelease(
          Task.succeed(1),
          release: (_, _) => Task.sync(() => released++),
        ).map((value) {
          armed = true;
          return value;
        }),
      );
      final running = runZoned(
        () => runtime.run(
          left
              .merge(Flow.fromIterable(<int>[]))
              .forEach((_) => Task.sync(() => consumed++)),
          signal: controller.signal,
        ),
        zoneSpecification: ZoneSpecification(
          scheduleMicrotask: (self, parent, zone, callback) {
            if (armed) {
              armed = false;
              intercepted = true;
              controller.requestCancel(reason: 'handoff');
            }
            parent.scheduleMicrotask(zone, callback);
          },
        ),
      );
      final exit = await running;
      expect(intercepted, isTrue);
      expect(consumed, 0);
      expect(released, 1);
      expect(
        _causes(exit).whereType<CauseCancelled>().single.reason,
        'handoff',
      );
    },
  );

  test(
    'nested merge and downstream buffering preserve every resource lease',
    () async {
      final resources = <_Resource>[];
      Flow<_Resource, Never> source(int id) => Flow.fromTask(
        Task.acquireRelease(
          Task.sync(() {
            final resource = _Resource(id);
            resources.add(resource);
            return resource;
          }),
          release: (resource, _) => Task.sync(() => resource.closed = true),
        ),
      );
      final received = _value(
        await runtime.run(
          source(1)
              .merge(source(2))
              .merge(source(3))
              .buffer(3)
              .mapTask(
                (resource) => Task.yieldNow().map((_) {
                  expect(resource.closed, isFalse);
                  return resource.value;
                }),
                concurrency: 3,
              )
              .toList(),
        ),
      );
      expect(received, unorderedEquals([1, 2, 3]));
      expect(resources.every((resource) => resource.closed), isTrue);
    },
  );

  test('consumer failure keeps its cleanup before source cleanup', () async {
    final bodyFault = StateError('body');
    final consumerFault = StateError('consumer cleanup');
    final sourceFault = StateError('source cleanup');
    final left = Flow.fromTask(
      Task.acquireRelease(
        Task.succeed(1),
        release: (_, _) => Task.sync(() => throw sourceFault),
      ),
    );
    final exit = await runtime.run(
      left
          .merge(Flow.fromIterable(<int>[]))
          .forEach(
            (_) =>
                Task.addFinalizer((_) => Task.sync(() => throw consumerFault))
                    .flatMap((_) => Task.sync(() => throw bodyFault)),
          ),
    );
    expect(_defects(exit), [
      same(bodyFault),
      same(consumerFault),
      same(sourceFault),
    ]);
  });
}

final class _Resource {
  _Resource(this.value);
  final int value;
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
