import 'dart:async';

import 'package:any/any.dart';
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
    await Future<void>.delayed(Duration.zero);
    expect(reports, isEmpty);
  });

  test('group size is validated before opening the source', () async {
    var started = false;
    final source = Flow.fromTask(
      Task.sync(() {
        started = true;
        return 1;
      }),
    );
    for (final size in [0, -1]) {
      final task = source.grouped(size).toList();
      expect(started, isFalse);
      expect(
        _causes(_cause(await runtime.run(task))).single,
        isA<CauseDefect>().having(
          (cause) => cause.error,
          'error',
          isA<RangeError>(),
        ),
      );
    }
    expect(started, isFalse);
  });

  test('groups are immutable and an empty source emits nothing', () async {
    final result = _value(
      await runtime.run(Flow.fromIterable([1, 2, 3]).grouped(2).toList()),
    );
    expect(result, [
      [1, 2],
      [3],
    ]);
    expect(() => result.first.add(4), throwsUnsupportedError);
    expect(
      _value(await runtime.run(Flow.fromIterable(<int>[]).grouped(2).toList())),
      isEmpty,
    );
  });

  test(
    'grouping can pull beyond sequential mapping without releasing inputs',
    () async {
      final alive = <int>{};
      final releases = <int>[];
      final flow = Flow.fromIterable([1, 2, 3, 4])
          .mapTask(
            (value) => Task.acquireRelease(
              Task.sync(() {
                alive.add(value);
                return value;
              }),
              release: (_, _) => Task.sync(() {
                alive.remove(value);
                releases.add(value);
              }),
            ),
          )
          .grouped(2);
      final groups = <List<int>>[];
      expect(
        await runtime.run(
          flow.forEach(
            (group) => Task.sync(() {
              expect(alive, containsAll(group));
              groups.add(group);
            }),
          ),
        ),
        isA<ExitSuccess>(),
      );
      expect(groups, [
        [1, 2],
        [3, 4],
      ]);
      expect(releases, unorderedEquals([1, 2, 3, 4]));
      expect(alive, isEmpty);
    },
  );

  test(
    'the last partial group holds mapped resources across upstream EOF',
    () async {
      final alive = <int>{};
      final flow = Flow.fromIterable([1, 2, 3])
          .mapTask(
            (value) => Task.acquireRelease(
              Task.sync(() {
                alive.add(value);
                return value;
              }),
              release: (_, _) => Task.sync(() => alive.remove(value)),
            ),
            concurrency: 1,
          )
          .grouped(2);
      final values = <List<int>>[];
      expect(
        await runtime.run(
          flow.forEach(
            (group) => Task.sync(() {
              expect(alive, containsAll(group));
              values.add(group);
            }),
          ),
        ),
        isA<ExitSuccess>(),
      );
      expect(values, [
        [1, 2],
        [3],
      ]);
      expect(alive, isEmpty);
    },
  );

  test(
    'a group larger than all input does not wait on upstream resource leases',
    () async {
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
            concurrency: 2,
          )
          .grouped(10);
      expect(
        await runtime.run(
          flow.forEach(
            (group) => Task.sync(() {
              expect(group, [1, 2]);
              expect(live, {1, 2});
            }),
          ),
        ),
        isA<ExitSuccess>(),
      );
      expect(live, isEmpty);
    },
  );

  test(
    'nested groups and mappings retain every original value until use',
    () async {
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
          )
          .grouped(2)
          .mapTask(Task.succeed, concurrency: 2)
          .grouped(2);
      final collected = <List<List<int>>>[];
      expect(
        await runtime.run(
          flow.forEach(
            (groups) => Task.sync(() {
              expect(live, containsAll(groups.expand((group) => group)));
              collected.add(groups);
            }),
          ),
        ),
        isA<ExitSuccess>(),
      );
      expect(collected, [
        [
          [0, 1],
          [2, 3],
        ],
        [
          [4, 5],
          [6],
        ],
      ]);
      expect(live, isEmpty);
    },
  );

  test('groups span concat and flatMap producer lifetimes safely', () async {
    final live = <int>{};
    Flow<int, Never> item(int value) => Flow.fromTask(
      Task.acquireRelease(
        Task.sync(() {
          live.add(value);
          return value;
        }),
        release: (_, _) => Task.sync(() => live.remove(value)),
      ),
    );
    final source = Flow.fromIterable([1, 2]).flatMap(item).concat(item(3));
    final groups = <List<int>>[];
    expect(
      await runtime.run(
        source
            .grouped(2)
            .forEach(
              (group) => Task.sync(() {
                expect(live, containsAll(group));
                groups.add(group);
              }),
            ),
      ),
      isA<ExitSuccess>(),
    );
    expect(groups, [
      [1, 2],
      [3],
    ]);
    expect(live, isEmpty);
  });

  test(
    'recovery can supply the rest of a group while old inputs remain alive',
    () async {
      var oldAlive = false;
      final events = <String>[];
      final source =
          Flow.unfold<int, int, String>(
            0,
            (state) => state == 0
                ? Task.acquireRelease(
                    Task.sync(() {
                      oldAlive = true;
                      return const Option.some((1, 1));
                    }),
                    release: (_, _) => Task.sync(() {
                      oldAlive = false;
                      events.add('old release');
                    }),
                  )
                : Task.fail('broken'),
          ).recover((_) {
            events.add('recover');
            return Flow.fromIterable([2]);
          });
      expect(
        await runtime.run(
          source
              .grouped(2)
              .forEach(
                (group) => Task.sync(() {
                  expect(oldAlive, isTrue);
                  expect(group, [1, 2]);
                  events.add('consume');
                }),
              ),
        ),
        isA<ExitSuccess>(),
      );
      expect(events, ['recover', 'consume', 'old release']);
      expect(oldAlive, isFalse);
    },
  );

  test(
    'deferred cleanup defect after recovery cannot be recovered or lost',
    () async {
      final error = StateError('old release');
      final stack = StackTrace.fromString('old resource');
      var recoveries = 0;
      var consumed = false;
      final source =
          Flow.unfold<int, int, String>(
            0,
            (state) => state == 0
                ? Task.acquireRelease(
                    Task.succeed(const Option.some((1, 1))),
                    release: (_, _) => Task.sync(
                      () => Error.throwWithStackTrace(error, stack),
                    ),
                  )
                : Task.fail('broken'),
          ).recover((_) {
            recoveries++;
            return Flow.fromIterable([2]);
          });
      final exit = await runtime.run(
        source.grouped(2).forEach((group) => Task.sync(() => consumed = true)),
      );
      final defects = _causes(_cause(exit)).whereType<CauseDefect>().toList();
      expect(defects, hasLength(1));
      expect(defects.single.error, same(error));
      expect(defects.single.stackTrace.toString(), 'old resource');
      expect(recoveries, 1);
      expect(consumed, isTrue);
    },
  );

  test(
    'failed partial groups release inputs and preserve the complete cause',
    () async {
      final cleanup = StateError('cleanup');
      var consumed = false;
      final source = Flow.unfold<int, int, String>(
        0,
        (state) => state == 0
            ? Task.acquireRelease(
                Task.succeed(const Option.some((1, 1))),
                release: (_, _) => Task.sync(() => throw cleanup),
              )
            : Task.fail('business'),
      );
      final reasons = _causes(
        _cause(
          await runtime.run(
            source.grouped(2).forEach((_) => Task.sync(() => consumed = true)),
          ),
        ),
      );
      expect(consumed, isFalse);
      expect(
        reasons.whereType<CauseFailure<String>>().single.error,
        'business',
      );
      expect(reasons.whereType<CauseDefect>().single.error, same(cleanup));
    },
  );

  test(
    'take releases a complete group without pulling another input',
    () async {
      var pulls = 0;
      final released = <int>[];
      final flow =
          Flow.unfold<int, int, Never>(0, (state) {
                pulls++;
                return Task.succeed(Option.some((state, state + 1)));
              })
              .mapTask(
                (value) => Task.acquireRelease(
                  Task.succeed(value),
                  release: (_, _) => Task.sync(() => released.add(value)),
                ),
              )
              .grouped(2)
              .take(1);
      expect(_value(await runtime.run(flow.toList())), [
        [0, 1],
      ]);
      expect(pulls, 2);
      expect(released, [1, 0]);
    },
  );

  test(
    'cancelling a partial group stops mapping before releasing buffered inputs',
    () async {
      final controller = CancellationController();
      final started = Completer<void>();
      final stopped = Completer<void>();
      final stopGate = Completer<void>();
      final events = <String>[];
      final flow = Flow.fromIterable([1, 2])
          .mapTask(
            (value) => value == 1
                ? Task.acquireRelease(
                    Task.succeed(1),
                    release: (_, _) =>
                        Task.sync(() => events.add('release first')),
                  )
                : Task.fromCallback<int, Never>((complete) {
                    started.complete();
                    return () {
                      events.add('stop second');
                      stopped.complete();
                      return stopGate.future;
                    };
                  }),
          )
          .grouped(2);
      final running = runtime.run(flow.toList(), signal: controller.signal);
      await started.future;
      controller.requestCancel(reason: 'stop');
      await stopped.future;
      expect(events, ['stop second']);
      stopGate.complete();
      expect(
        _causes(_cause(await running))
            .whereType<CauseCancelled>()
            .single
            .reason,
        'stop',
      );
      expect(events, ['stop second', 'release first']);
    },
  );

  test(
    'cancellation preserves mapper and buffered resource cleanup defects',
    () async {
      final controller = CancellationController();
      final started = Completer<void>();
      final mapperError = StateError('stop mapper');
      final inputError = StateError('release buffered');
      final flow = Flow.fromIterable([1, 2])
          .mapTask(
            (value) => value == 1
                ? Task.acquireRelease(
                    Task.succeed(1),
                    release: (_, _) => Task.sync(() => throw inputError),
                  )
                : Task.fromCallback<int, Never>((complete) {
                    started.complete();
                    return () => throw mapperError;
                  }),
          )
          .grouped(2);
      final running = runtime.run(flow.toList(), signal: controller.signal);
      await started.future;
      controller.requestCancel(reason: 'stop');
      final reasons = _causes(_cause(await running));
      expect(reasons.whereType<CauseCancelled>().single.reason, 'stop');
      expect(
        reasons.whereType<CauseDefect>().map((cause) => cause.error),
        unorderedEquals([mapperError, inputError]),
      );
    },
  );

  test(
    'group callback cleanup precedes every retained input cleanup',
    () async {
      final events = <String>[];
      final flow = Flow.fromIterable([1, 2])
          .mapTask(
            (value) => Task.acquireRelease(
              Task.succeed(value),
              release: (_, _) => Task.sync(() => events.add('input $value')),
            ),
          )
          .grouped(2);
      expect(
        await runtime.run(
          flow.forEach(
            (_) => Task.acquireRelease(
              Task.succeed(null),
              release: (_, _) => Task.sync(() => events.add('consumer')),
            ),
          ),
        ),
        isA<ExitSuccess>(),
      );
      expect(events.first, 'consumer');
      expect(events.skip(1), unorderedEquals(['input 1', 'input 2']));
    },
  );

  test('explicit cleanup cancellation survives grouped release', () async {
    final flow = Flow.fromIterable([1])
        .mapTask(
          (value) => Task.acquireRelease(
            Task.succeed(value),
            release: (_, _) => Task.failCause(Cause.cancelled('cleanup')),
          ),
        )
        .grouped(2);
    expect(
      _causes(_cause(await runtime.run(flow.toList())))
          .whereType<CauseCancelled>()
          .single
          .reason,
      'cleanup',
    );
  });
}

T _value<T, E>(Exit<T, E> exit) => (exit as ExitSuccess<T, E>).value;
Cause<E> _cause<T, E>(Exit<T, E> exit) => (exit as ExitFailure<T, E>).cause;
List<Cause<E>> _causes<E>(Cause<E> cause) => switch (cause) {
  CauseGroup(:final causes) => causes,
  _ => [cause],
};
