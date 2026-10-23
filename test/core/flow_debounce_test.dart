import 'dart:async';

import 'package:any/any.dart';
import 'package:any/testing.dart';
import 'package:test/test.dart';

void main() {
  late Runtime runtime;
  late TestClock clock;
  final reports = <Cause<Object?>>[];
  const quiet = Duration(seconds: 10);
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

  test('duration is validated lazily and zero preserves every value', () async {
    var started = false;
    final source = Flow.fromTask(
      Task.sync(() {
        started = true;
        return 1;
      }),
    );
    final invalid = source.debounce(const Duration(microseconds: -1));
    expect(started, isFalse);
    expect(
      _cause(await runtime.run(invalid.toList())),
      isA<CauseDefect>().having(
        (cause) => cause.error,
        'error',
        isA<RangeError>(),
      ),
    );
    expect(started, isFalse);
    expect(
      _value(await runtime.run(source.debounce(quiet).take(0).toList())),
      isEmpty,
    );
    expect(started, isFalse);
    expect(
      _value(
        await runtime.run(
          Flow.fromIterable([1, 2, 3]).debounce(Duration.zero).toList(),
        ),
      ),
      [1, 2, 3],
    );
    expect(clock.elapsed, Duration.zero);
  });

  test(
    'empty and nullable sources retain their types and independent state',
    () async {
      final Flow<int?, String> flow = Flow.fromIterable<int?>([1, 2, null])
          .debounce(quiet)
          .withError<String>();
      expect(_value(await runtime.run(flow.toList())), [null]);
      expect(_value(await runtime.run(flow.toList())), [null]);
      expect(
        _value(
          await runtime.run(
            Flow.fromIterable(<int>[]).debounce(quiet).toList(),
          ),
        ),
        isEmpty,
      );
    },
  );

  test('every new value restarts one monotonic quiet deadline', () async {
    final source = _Source<int, Never>();
    final values = <int>[];
    final running = runtime.run(
      source.flow
          .debounce(quiet)
          .forEach((value) => Task.sync(() => values.add(value))),
    );
    await _until(() => source.requested == 1);
    expect(clock.pendingCount, 0);
    source.emit(0, 1);
    await _until(() => source.requested == 2 && clock.pendingCount == 1);
    clock.advance(const Duration(seconds: 4));
    source.emit(1, 2);
    await _until(() => source.requested == 3);
    expect(clock.pendingCount, 1);
    clock.setWallTime(DateTime.utc(2030));
    clock.advance(const Duration(seconds: 9));
    await _turn();
    expect(values, isEmpty);
    clock.advance(const Duration(seconds: 1));
    await _until(() => values.isNotEmpty);
    expect(values, [2]);
    expect(clock.pendingCount, 0);
    expect(source.cancelled, isEmpty);
    source.emit(2, 3);
    await _until(() => source.requested == 4);
    source.end(3);
    expect(await running, isA<ExitSuccess>());
    expect(values, [2, 3]);
  });

  test(
    'normal end emits the latest value immediately without advancing time',
    () async {
      final source = _Source<int, Never>();
      final running = runtime.run(source.flow.debounce(quiet).toList());
      await _until(() => source.requested == 1);
      source.emit(0, 1);
      await _until(() => source.requested == 2);
      source.emit(1, 2);
      await _until(() => source.requested == 3);
      source.end(2);
      expect(_value(await running), [2]);
      expect(clock.elapsed, Duration.zero);
    },
  );

  test('a crossing pull survives delivery and consumer backpressure', () async {
    final source = _Source<int, Never>();
    final hold = Completer<void>();
    final values = <int>[];
    final running = runtime.run(
      source.flow.debounce(quiet).forEach((value) {
        values.add(value);
        return value == 1
            ? Task.fromFuture(() => hold.future, onError: _defect)
            : Task.succeed(null);
      }),
    );
    await _until(() => source.requested == 1);
    source.emit(0, 1);
    await _until(() => source.requested == 2 && clock.pendingCount == 1);
    clock.advance(quiet);
    await _until(() => values.length == 1);
    source.emit(1, 2);
    await _turn();
    clock.advance(quiet);
    expect(values, [1]);
    expect(source.requested, 2);
    expect(source.cancelled, isEmpty);
    expect(clock.pendingCount, 0);
    hold.complete();
    await _until(() => values.length == 2 && source.requested == 3);
    source.end(2);
    expect(await running, isA<ExitSuccess>());
    expect(values, [1, 2]);
  });

  test(
    'source failure discards the candidate and preserves its complete cause',
    () async {
      final source = _Source<int, String>();
      final values = <int>[];
      final error = StateError('source defect');
      final stack = StackTrace.fromString('debounce source');
      final running = runtime.run(
        source.flow
            .debounce(quiet)
            .forEach((value) => Task.sync(() => values.add(value))),
      );
      await _until(() => source.requested == 1);
      source.emit(0, 1);
      await _until(() => source.requested == 2);
      source.fail(
        1,
        Cause.combine([
          const Cause.failure('source'),
          Cause.defect(error, stack),
        ]),
      );
      final causes = _causes(_cause(await running));
      expect(values, isEmpty);
      expect(causes.whereType<CauseFailure<String>>().single.error, 'source');
      final defect = causes.whereType<CauseDefect>().single;
      expect(defect.error, same(error));
      expect(defect.stackTrace.toString(), 'debounce source');
    },
  );

  test(
    'replaced input resources close before the next source pull starts',
    () async {
      final source = _Source<int, Never>();
      final live = <int>{};
      final released = <int>[];
      final flow = source.flow
          .mapTask(
            (value) => Task.acquireRelease(
              Task.sync(() {
                live.add(value);
                return value;
              }),
              release: (_, _) => Task.sync(() {
                live.remove(value);
                released.add(value);
              }),
            ),
          )
          .debounce(quiet);
      final running = runtime.run(flow.toList());
      await _until(() => source.requested == 1);
      source.emit(0, 1);
      await _until(() => source.requested == 2);
      expect(live, {1});
      source.emit(1, 2);
      await _until(() => source.requested == 3);
      expect(released, [1]);
      expect(live, {2});
      source.emit(2, 3);
      await _until(() => source.requested == 4);
      expect(released, [1, 2]);
      expect(live, {3});
      source.end(3);
      expect(_value(await running), [3]);
      expect(released, [1, 2, 3]);
      expect(live, isEmpty);
    },
  );

  test(
    'replacement waits for old cleanup while retaining the new candidate',
    () async {
      final source = _Source<int, Never>();
      final releasing = Completer<void>();
      final release = Completer<void>();
      final live = <int>{};
      final flow = source.flow
          .mapTask(
            (value) => Task.acquireRelease(
              Task.sync(() {
                live.add(value);
                return value;
              }),
              release: (_, _) => value == 1
                  ? Task.fromFuture(() {
                      releasing.complete();
                      return release.future;
                    }, onError: _defect).map((_) => live.remove(value))
                  : Task.sync(() => live.remove(value)),
            ),
          )
          .debounce(quiet);
      final running = runtime.run(flow.toList());
      await _until(() => source.requested == 1);
      source.emit(0, 1);
      await _until(() => source.requested == 2);
      source.emit(1, 2);
      await releasing.future;
      expect(live, {1, 2});
      expect(source.requested, 2);
      expect(clock.pendingCount, 0);
      release.complete();
      await _until(() => source.requested == 3);
      expect(live, {2});
      source.end(2);
      expect(_value(await running), [2]);
      expect(live, isEmpty);
    },
  );

  test(
    'replacement cleanup failure also releases the accepted new input',
    () async {
      final oldError = StateError('old cleanup');
      final newError = StateError('new cleanup');
      final live = <int>{};
      final flow = Flow.fromIterable([1, 2])
          .mapTask(
            (value) => Task.acquireRelease(
              Task.sync(() {
                live.add(value);
                return value;
              }),
              release: (_, _) => Task.sync(() {
                live.remove(value);
                throw value == 1 ? oldError : newError;
              }),
            ),
          )
          .debounce(quiet);
      final causes = _causes(_cause(await runtime.run(flow.toList())));
      expect(
        causes.whereType<CauseDefect>().map((cause) => cause.error),
        unorderedEquals([oldError, newError]),
      );
      expect(live, isEmpty);
    },
  );

  test(
    'explicit cancellation during replacement cleanup is retained',
    () async {
      final released = <int>[];
      final flow = Flow.fromIterable([1, 2])
          .mapTask(
            (value) => Task.acquireRelease(
              Task.succeed(value),
              release: (_, _) => Task.defer(() {
                released.add(value);
                return value == 1
                    ? Task.failCause(const Cause.cancelled())
                    : Task.succeed(null);
              }),
            ),
          )
          .debounce(quiet);
      expect(
        _causes(_cause(await runtime.run(flow.toList())))
            .whereType<CauseCancelled>()
            .map((cause) => cause.reason),
        [null],
      );
      expect(released, [1, 2]);
    },
  );

  test(
    'zero debounce keeps concat and flatMap resources for downstream groups',
    () async {
      final live = <String>{};
      Task<int, Never> resource(String name, int value) => Task.acquireRelease(
        Task.sync(() {
          live.add(name);
          return value;
        }),
        release: (_, _) => Task.sync(() => live.remove(name)),
      );
      Flow<int, Never> source(int value) => Flow.fromTask(
        resource('outer$value', value),
      ).flatMap((value) => Flow.fromTask(resource('inner$value', value)));
      final flow = source(1)
          .concat(source(2))
          .debounce(Duration.zero)
          .buffer(1)
          .mapTask(Task.succeed, concurrency: 2)
          .grouped(3);
      expect(
        await runtime.run(
          flow.forEach(
            (values) => Task.sync(() {
              expect(values, [1, 2]);
              expect(live, {'outer1', 'inner1', 'outer2', 'inner2'});
            }),
          ),
        ),
        isA<ExitSuccess>(),
      );
      expect(live, isEmpty);
    },
  );

  test('downstream cleanup precedes the retained candidate cleanup', () async {
    final events = <String>[];
    final flow = Flow.fromIterable([1, 2])
        .mapTask(
          (value) => Task.acquireRelease(
            Task.succeed(value),
            release: (_, _) => Task.sync(() => events.add('input $value')),
          ),
        )
        .debounce(quiet);
    expect(
      await runtime.run(
        flow.forEach(
          (value) => Task.acquireRelease(
            Task.succeed(null),
            release: (_, _) => Task.sync(() => events.add('consumer $value')),
          ),
        ),
      ),
      isA<ExitSuccess>(),
    );
    expect(events, ['input 1', 'consumer 2', 'input 2']);
  });

  test(
    'take waits for a crossing Future and preserves its late defect once',
    () async {
      final started = Completer<void>();
      final gate = Completer<int>();
      final error = StateError('late');
      final flow = Flow.fromIterable([1])
          .concat(
            Flow.fromTask(
              Task.fromFuture(() {
                started.complete();
                return gate.future;
              }, onError: _defect),
            ),
          )
          .debounce(quiet)
          .take(1);
      var completed = false;
      final running = runtime.run(flow.toList()).then((exit) {
        completed = true;
        return exit;
      });
      await started.future;
      clock.advance(quiet);
      await _turn();
      expect(completed, isFalse);
      gate.completeError(error, StackTrace.fromString('late debounce'));
      final defect = _causes(_cause(await running))
          .whereType<CauseDefect>()
          .single;
      expect(defect.error, same(error));
      expect(defect.stackTrace.toString(), 'late debounce');
    },
  );

  test(
    'owner cancellation retains complete candidate and pull cleanup failures',
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
          .debounce(quiet);
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
    'consumer failure stops the pending pull after an output is delivered',
    () async {
      final source = _Source<int, String>();
      final running = runtime.run(
        source.flow.debounce(quiet).forEach((_) => Task.fail('consumer')),
      );
      await _until(() => source.requested == 1);
      source.emit(0, 1);
      await _until(() => source.requested == 2 && clock.pendingCount == 1);
      clock.advance(quiet);
      expect(
        _cause(await running),
        isA<CauseFailure<String>>().having(
          (cause) => cause.error,
          'error',
          'consumer',
        ),
      );
      expect(source.cancelled, [1]);
    },
  );

  test(
    'clock cancellation failure during replacement releases both values',
    () async {
      final error = StateError('cancel timer');
      final clockWithFailure = _CancelFailingClock(clock, error);
      final live = <int>{};
      final flow = Flow.fromIterable([1, 2])
          .mapTask(
            (value) => Task.acquireRelease(
              Task.sync(() {
                live.add(value);
                return value;
              }),
              release: (_, _) => Task.sync(() => live.remove(value)),
            ),
          )
          .debounce(quiet);
      final exit = await runtime.run(
        flow.toList().provideContext(
          Context.from([Clock.key.bind(clockWithFailure)]),
        ),
      );
      expect(
        _causes(_cause(exit)).whereType<CauseDefect>().single.error,
        same(error),
      );
      expect(live, isEmpty);
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

final class _CancelFailingClock implements Clock {
  _CancelFailingClock(this.delegate, this.error);
  final Clock delegate;
  final Object error;
  @override
  DateTime now() => delegate.now();
  @override
  Duration get elapsed => delegate.elapsed;
  @override
  void Function() schedule(Duration delay, void Function() callback) {
    final cancel = delegate.schedule(delay, callback);
    return () {
      cancel();
      throw error;
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
