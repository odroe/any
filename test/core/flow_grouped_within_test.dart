import 'dart:async';

import 'package:any/any.dart';
import 'package:any/testing.dart';
import 'package:test/test.dart';

void main() {
  late Runtime runtime;
  late TestClock clock;
  final reports = <Cause<Object?>>[];
  const window = Duration(seconds: 10);
  setUp(() {
    reports.clear();
    clock = TestClock();
    runtime = Runtime(
      context: Context.from([Clock.key.bind(clock)]),
      onUnhandledError: reports.add,
    );
  });
  tearDown(() async {
    await runtime.close();
    await _turn();
    expect(clock.pendingCount, 0);
    expect(reports, isEmpty);
  });

  test('size and duration are validated lazily before source work', () async {
    var started = false;
    final source = Flow.fromTask(
      Task.sync(() {
        started = true;
        return 1;
      }),
    );
    for (final (size, duration) in [
      (0, window),
      (-1, window),
      (2, Duration.zero),
      (2, const Duration(seconds: -1)),
    ]) {
      final flow = source.groupedWithin(size, duration);
      expect(started, isFalse);
      expect(
        _cause(await runtime.run(flow.toList())),
        isA<CauseDefect>().having(
          (cause) => cause.error,
          'error',
          isA<RangeError>(),
        ),
      );
    }
    expect(started, isFalse);
    expect(
      _value(
        await runtime.run(source.groupedWithin(2, window).take(0).toList()),
      ),
      isEmpty,
    );
    expect(started, isFalse);
  });

  test('groups are immutable, nullable and repeat independently', () async {
    final Flow<List<int?>, String> flow = Flow.fromIterable<int?>([null, 1, 2])
        .groupedWithin(2, window)
        .withError<String>();
    for (var i = 0; i < 2; i++) {
      final values = _value(await runtime.run(flow.toList()));
      expect(values, [
        [null, 1],
        [2],
      ]);
      expect(() => values.first.add(3), throwsUnsupportedError);
    }
    expect(
      _value(
        await runtime.run(
          Flow.fromIterable(<int>[]).groupedWithin(2, window).toList(),
        ),
      ),
      isEmpty,
    );
  });

  test(
    'size emits immediately and pauses further pulls during consumption',
    () async {
      final source = _Source<int, Never>();
      final hold = Completer<void>();
      final groups = <List<int>>[];
      final running = runtime.run(
        source.flow.groupedWithin(2, window).forEach((group) {
          groups.add(group);
          return group.first == 1
              ? Task.fromFuture(() => hold.future, onError: _defect)
              : Task.succeed(null);
        }),
      );
      await _until(() => source.requested == 1);
      expect(clock.pendingCount, 0);
      source.emit(0, 1);
      await _until(() => source.requested == 2);
      expect(clock.pendingCount, 1);
      source.emit(1, 2);
      await _until(() => groups.isNotEmpty);
      expect(groups, [
        [1, 2],
      ]);
      expect(clock.pendingCount, 0);
      await _turn();
      expect(source.requested, 2);
      hold.complete();
      await _until(() => source.requested == 3);
      source.end(2);
      expect(await running, isA<ExitSuccess>());
      expect(source.cancelled, isEmpty);
    },
  );

  test(
    'first value starts a monotonic deadline without empty groups',
    () async {
      final source = _Source<int, Never>();
      final groups = <List<int>>[];
      final running = runtime.run(
        source.flow
            .groupedWithin(5, window)
            .forEach((group) => Task.sync(() => groups.add(group))),
      );
      await _until(() => source.requested == 1);
      clock.advance(const Duration(days: 1));
      expect(clock.pendingCount, 0);
      expect(groups, isEmpty);
      source.emit(0, 1);
      await _until(() => source.requested == 2 && clock.pendingCount == 1);
      clock.setWallTime(DateTime.utc(1990));
      clock.advance(const Duration(seconds: 9));
      await _turn();
      expect(groups, isEmpty);
      clock.advance(const Duration(seconds: 1));
      await _until(() => groups.isNotEmpty);
      expect(groups, [
        [1],
      ]);
      expect(source.requested, 2);
      expect(source.cancelled, isEmpty);
      clock.advance(const Duration(days: 1));
      await _turn();
      expect(groups, [
        [1],
      ]);
      expect(clock.pendingCount, 0);
      source.emit(1, 2);
      await _until(() => source.requested == 3);
      source.end(2);
      expect(await running, isA<ExitSuccess>());
      expect(groups, [
        [1],
        [2],
      ]);
    },
  );

  test(
    'a pending pull crosses the deadline without cancellation or loss',
    () async {
      final source = _Source<int, Never>();
      final hold = Completer<void>();
      final groups = <List<int>>[];
      final running = runtime.run(
        source.flow.groupedWithin(3, window).forEach((group) {
          groups.add(group);
          return groups.length == 1
              ? Task.fromFuture(() => hold.future, onError: _defect)
              : Task.succeed(null);
        }),
      );
      await _until(() => source.requested == 1);
      source.emit(0, 1);
      await _until(() => source.requested == 2 && clock.pendingCount == 1);
      clock.advance(window);
      await _until(() => groups.length == 1);
      expect(source.cancelled, isEmpty);
      source.emit(1, 2);
      await _turn();
      clock.advance(window);
      expect(source.requested, 2);
      expect(clock.pendingCount, 0);
      hold.complete();
      await _until(() => groups.length == 2 && source.requested == 3);
      expect(groups, [
        [1],
        [2],
      ]);
      source.end(2);
      expect(await running, isA<ExitSuccess>());
      expect(source.cancelled, isEmpty);
    },
  );

  test(
    'normal end emits the trailing group without waiting for its timer',
    () async {
      final source = _Source<int, Never>();
      final running = runtime.run(
        source.flow.groupedWithin(3, window).toList(),
      );
      await _until(() => source.requested == 1);
      source.emit(0, 1);
      await _until(() => source.requested == 2 && clock.pendingCount == 1);
      source.end(1);
      expect(_value(await running), [
        [1],
      ]);
      expect(clock.elapsed, Duration.zero);
    },
  );

  test(
    'source failure discards a partial group and releases its inputs',
    () async {
      final source = _Source<int, String>();
      final live = <int>{};
      final flow = source.flow
          .mapTask(
            (value) => Task.acquireRelease(
              Task.sync(() {
                live.add(value);
                return value;
              }),
              release: (_, _) => Task.sync(() => live.remove(value)),
            ),
          )
          .groupedWithin(3, window);
      final groups = <List<int>>[];
      final running = runtime.run(
        flow.forEach((group) => Task.sync(() => groups.add(group))),
      );
      await _until(() => source.requested == 1);
      source.emit(0, 1);
      await _until(() => source.requested == 2 && clock.pendingCount == 1);
      expect(live, {1});
      source.fail(1, const Cause.failure('source'));
      expect(
        _cause(await running),
        isA<CauseFailure<String>>().having(
          (cause) => cause.error,
          'error',
          'source',
        ),
      );
      expect(groups, isEmpty);
      expect(live, isEmpty);
    },
  );

  test(
    'already delivered groups remain delivered when the pending pull fails',
    () async {
      final source = _Source<int, String>();
      final groups = <List<int>>[];
      final running = runtime.run(
        source.flow
            .groupedWithin(3, window)
            .forEach((group) => Task.sync(() => groups.add(group))),
      );
      await _until(() => source.requested == 1);
      source.emit(0, 1);
      await _until(() => source.requested == 2 && clock.pendingCount == 1);
      clock.advance(window);
      await _until(() => groups.length == 1);
      source.fail(1, const Cause.failure('source'));
      expect(_cause(await running), isA<CauseFailure<String>>());
      expect(groups, [
        [1],
      ]);
    },
  );

  test('groups retain concat and flatMap resources through EOF and downstream grouping', () async {
    final live = <String>{};
    Task<int, Never> resource(String name, int value) => Task.acquireRelease(
      Task.sync(() {
        live.add(name);
        return value;
      }),
      release: (_, _) => Task.sync(() => live.remove(name)),
    );
    Flow<int, Never> source(int value) =>
        Flow.fromTask(resource('outer$value', value))
            .flatMap((value) => Flow.fromTask(resource('inner$value', value)));
    final flow = source(1)
        .concat(source(2))
        .concat(source(3))
        .groupedWithin(2, window)
        .grouped(10);
    expect(
      await runtime.run(
        flow.forEach(
          (groups) => Task.sync(() {
            expect(groups, [
              [1, 2],
              [3],
            ]);
            expect(live, {
              'outer1',
              'inner1',
              'outer2',
              'inner2',
              'outer3',
              'inner3',
            });
          }),
        ),
      ),
      isA<ExitSuccess>(),
    );
    expect(live, isEmpty);
  });

  test('nested buffered and concurrent groups keep inputs alive through consumption', () async {
    final live = <int>{};
    final flow = Flow.fromIterable(List.generate(7, (index) => index))
        .mapTask(
          (value) => Task.acquireRelease(
            Task.sync(() {
              live.add(value);
              return value;
            }),
            release: (_, _) => Task.sync(() => live.remove(value)),
          ),
          concurrency: 2,
        )
        .buffer(2)
        .groupedWithin(3, window)
        .mapTask(Task.succeed, concurrency: 2)
        .grouped(2);
    final values = <int>[];
    expect(
      await runtime.run(
        flow.forEach(
          (groups) => Task.sync(() {
            final retained = groups.expand((group) => group).toList();
            expect(live, containsAll(retained));
            values.addAll(retained);
          }),
        ),
      ),
      isA<ExitSuccess>(),
    );
    expect(values, List.generate(7, (index) => index));
    expect(live, isEmpty);
  });

  test(
    'take stops a crossing pull and waits before releasing group resources',
    () async {
      final stopped = Completer<void>();
      final stopGate = Completer<void>();
      final events = <String>[];
      final source = _Source<int, Never>(
        onCancel: (_) {
          events.add('stop pull');
          stopped.complete();
          return stopGate.future;
        },
      );
      final flow = source.flow
          .mapTask(
            (value) => Task.acquireRelease(
              Task.succeed(value),
              release: (_, _) => Task.sync(() => events.add('release $value')),
            ),
          )
          .groupedWithin(3, window)
          .take(1);
      var completed = false;
      final running = runtime.run(flow.toList()).then((exit) {
        completed = true;
        return exit;
      });
      await _until(() => source.requested == 1);
      source.emit(0, 1);
      await _until(() => source.requested == 2 && clock.pendingCount == 1);
      clock.advance(window);
      await stopped.future;
      expect(completed, isFalse);
      expect(events, ['stop pull']);
      expect(clock.pendingCount, 0);
      stopGate.complete();
      expect(_value(await running), [
        [1],
      ]);
      expect(events, ['stop pull', 'release 1']);
    },
  );

  test(
    'take retains a pending Future defect with its original stack once',
    () async {
      final started = Completer<void>();
      final gate = Completer<int>();
      final error = StateError('late source');
      final flow = Flow.fromIterable([1])
          .concat(
            Flow.fromTask(
              Task.fromFuture(() {
                started.complete();
                return gate.future;
              }, onError: _defect),
            ),
          )
          .groupedWithin(3, window)
          .take(1);
      var completed = false;
      final running = runtime.run(flow.toList()).then((exit) {
        completed = true;
        return exit;
      });
      await started.future;
      await _until(() => clock.pendingCount == 1);
      clock.advance(window);
      await _turn();
      expect(completed, isFalse);
      gate.completeError(error, StackTrace.fromString('timed grouping'));
      final defect = _causes(_cause(await running))
          .whereType<CauseDefect>()
          .single;
      expect(defect.error, same(error));
      expect(defect.stackTrace.toString(), 'timed grouping');
    },
  );

  test(
    'external cancellation retains pull cleanup and complete resource causes',
    () async {
      final controller = CancellationController();
      final pullError = StateError('pull cleanup');
      final source = _Source<int, Never>(onCancel: (_) => throw pullError);
      final flow = source.flow
          .mapTask(
            (value) => Task.acquireRelease(
              Task.succeed(value),
              release: (_, _) => Task.failCause(
                Cause.combine([
                  const Cause.cancelled(),
                  const Cause.cancelled(),
                ]),
              ),
            ),
          )
          .groupedWithin(3, window);
      final running = runtime.run(flow.toList(), signal: controller.signal);
      await _until(() => source.requested == 1);
      source.emit(0, 1);
      await _until(() => source.requested == 2 && clock.pendingCount == 1);
      controller.requestCancel(reason: 'consumer');
      final causes = _causes(_cause(await running));
      expect(
        causes.whereType<CauseCancelled>().map((cause) => cause.reason),
        unorderedEquals(['consumer', null, null]),
      );
      expect(causes.whereType<CauseDefect>().single.error, same(pullError));
    },
  );

  test(
    'group cleanup runs before releasing retained source resources',
    () async {
      final events = <String>[];
      final flow = Flow.fromIterable([1, 2])
          .mapTask(
            (value) => Task.acquireRelease(
              Task.succeed(value),
              release: (_, _) => Task.sync(() => events.add('input $value')),
            ),
          )
          .groupedWithin(2, window);
      expect(
        await runtime.run(
          flow.forEach(
            (group) => Task.acquireRelease(
              Task.succeed(null),
              release: (_, _) => Task.sync(() => events.add('group')),
            ),
          ),
        ),
        isA<ExitSuccess>(),
      );
      expect(events.first, 'group');
      expect(events.skip(1), unorderedEquals(['input 1', 'input 2']));
    },
  );

  test('source pulls inherit the consumer context and task locals', () async {
    final key = ServiceKey<String>('value');
    final local = TaskLocal<int>(0);
    final flow = Flow.fromTask(
      Task.service(key)
          .flatMap((value) => local.get().map((number) => '$value:$number')),
    ).groupedWithin(2, window);
    expect(
      _value(
        await runtime.run(
          local
              .locally(4, flow.toList())
              .provideContext(Context.from([key.bind('bound')])),
        ),
      ),
      [
        ['bound:4'],
      ],
    );
  });

  for (final failure in ['elapsed', 'schedule', 'cancel']) {
    test('a throwing clock $failure does not leak inputs or timers', () async {
      final error = StateError(failure);
      var live = 0;
      final faulty = _FaultyClock(clock, failure, error);
      final flow = Flow.fromIterable([1, 2])
          .mapTask(
            (value) => Task.acquireRelease(
              Task.sync(() {
                live++;
                return value;
              }),
              release: (_, _) => Task.sync(() => live--),
            ),
          )
          .groupedWithin(2, window);
      final exit = await runtime.run(
        flow.toList().provideContext(Context.from([Clock.key.bind(faulty)])),
      );
      expect(
        _causes(_cause(exit)).whereType<CauseDefect>().single.error,
        same(error),
      );
      expect(live, 0);
    });
  }
  test('take retains cancellation from source cleanup that finishes after the deadline', () async {
    final started = Completer<void>();
    final gate = Completer<void>();
    final flow = Flow.fromIterable([1])
        .concat(
          Flow.fromTask(
            Task.acquireRelease(
              Task.succeed(2),
              release: (_, _) => Task.fromFuture(
                () {
                  started.complete();
                  return gate.future;
                },
                onError: _defect,
              ).flatMap((_) => Task.failCause(const Cause.cancelled())),
            ).scoped(),
          ).filter((_) => false),
        )
        .groupedWithin(3, window)
        .take(1);
    final running = runtime.run(flow.toList());
    await started.future;
    expect(clock.pendingCount, 1);
    clock.advance(window);
    await _turn();
    gate.complete();
    expect(
      _causes(_cause(await running))
          .whereType<CauseCancelled>()
          .map((cause) => cause.reason),
      [null],
    );
  });

  test(
    'consumer failure stops the crossing pull and retains its cleanup defect',
    () async {
      final error = StateError('pull cleanup');
      final source = _Source<int, String>(onCancel: (_) => throw error);
      final flow = source.flow.groupedWithin(3, window);
      final running = runtime.run(flow.forEach((_) => Task.fail('consumer')));
      await _until(() => source.requested == 1);
      source.emit(0, 1);
      await _until(() => source.requested == 2 && clock.pendingCount == 1);
      clock.advance(window);
      final causes = _causes(_cause(await running));
      expect(causes.whereType<CauseFailure<String>>().single.error, 'consumer');
      expect(causes.whereType<CauseDefect>().single.error, same(error));
      expect(causes.whereType<CauseCancelled>(), isEmpty);
      expect(source.cancelled, [1]);
    },
  );
}

final class _Source<T, E> {
  _Source({this.onCancel});
  final FutureOr<void> Function(int index)? onCancel;
  final _completions = <void Function(Exit<Option<(T, int)>, E>)>[];
  final cancelled = <int>[];
  int get requested => _completions.length;
  late final flow = Flow.unfold<int, T, E>(
    0,
    (index) => Task.fromCallback((complete) {
      expect(index, requested);
      _completions.add(complete);
      return () {
        cancelled.add(index);
        return onCancel?.call(index);
      };
    }),
  );
  void emit(int index, T value) =>
      _completions[index](Exit.success(Option.some((value, index + 1))));
  void end(int index) => _completions[index](const Exit.success(Option.none()));
  void fail(int index, Cause<E> cause) =>
      _completions[index](Exit.failure(cause));
}

final class _FaultyClock implements Clock {
  _FaultyClock(this.delegate, this.failure, this.error);
  final TestClock delegate;
  final String failure;
  final Object error;
  @override
  DateTime now() => delegate.now();
  @override
  Duration get elapsed => failure == 'elapsed' ? throw error : delegate.elapsed;
  @override
  void Function() schedule(Duration delay, void Function() callback) {
    if (failure == 'schedule') throw error;
    final cancel = delegate.schedule(delay, callback);
    return () {
      cancel();
      if (failure == 'cancel') throw error;
    };
  }
}

T _value<T, E>(Exit<T, E> exit) => (exit as ExitSuccess<T, E>).value;
Cause<E> _cause<T, E>(Exit<T, E> exit) => (exit as ExitFailure<T, E>).cause;
List<Cause<E>> _causes<E>(Cause<E> cause) => switch (cause) {
  CauseGroup(:final causes) => causes,
  _ => [cause],
};
Never _defect(Object error, StackTrace stackTrace) =>
    Error.throwWithStackTrace(error, stackTrace);
Future<void> _turn() => Future<void>.delayed(Duration.zero);
Future<void> _until(bool Function() ready) async {
  for (var i = 0; i < 100; i++) {
    if (ready()) return;
    await _turn();
  }
  fail('The expected state was not reached.');
}
