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

  test('validates concurrency lazily before opening input', () async {
    var opened = false;
    Iterable<int> values() sync* {
      opened = true;
      yield 1;
    }

    for (final limit in [0, -1]) {
      final flow = Flow.fromIterable(values())
          .mapTask(Task.succeed, concurrency: limit);
      expect(opened, isFalse);
      expect(
        _causes(_cause(await runtime.run(flow.toList()))).single,
        isA<CauseDefect>().having(
          (cause) => cause.error,
          'error',
          isA<RangeError>(),
        ),
      );
      expect(opened, isFalse);
    }
  });

  test(
    'default mapping shares callback resources until the consumer scope closes',
    () async {
      final events = <String>[];
      Iterable<int> values() sync* {
        for (var value = 1; value <= 2; value++) {
          events.add('pull $value');
          yield value;
        }
      }

      final flow = Flow.fromIterable(values()).mapTask(
        (value) => Task.acquireRelease(
          Task.sync(() {
            events.add('map $value');
            return value;
          }),
          release: (_, _) => Task.sync(() => events.add('release map $value')),
        ),
      );
      final result = await runtime.run(
        flow.forEach(
          (value) => Task.acquireRelease(
            Task.sync(() => events.add('consume $value')),
            release: (_, _) =>
                Task.sync(() => events.add('release consume $value')),
          ),
        ),
      );
      expect(result, isA<ExitSuccess>());
      expect(events, [
        'pull 1',
        'map 1',
        'consume 1',
        'pull 2',
        'map 2',
        'consume 2',
        'release consume 2',
        'release map 2',
        'release consume 1',
        'release map 1',
      ]);
    },
  );

  test(
    'ordered mapping bounds running, completed and consumed slots together',
    () async {
      final gates = List.generate(6, (_) => Completer<int>());
      final started = <int>[];
      final consumed = <int>[];
      final holdFirst = Completer<void>();
      final task = Flow.fromIterable([0, 1, 2, 3, 4, 5])
          .mapTask((value) {
            started.add(value);
            return Task.fromFuture(() => gates[value].future, onError: _defect);
          }, concurrency: 3)
          .forEach((value) {
            consumed.add(value);
            return value == 0
                ? Task.fromFuture(() => holdFirst.future, onError: _defect)
                : Task.succeed(null);
          });
      final running = runtime.run(task);
      await _until(() => started.length == 3);
      gates[2].complete(2);
      gates[1].complete(1);
      await _turn();
      expect(started, [0, 1, 2]);
      expect(consumed, isEmpty);
      gates[0].complete(0);
      await _until(() => consumed.isNotEmpty);
      expect(started, [0, 1, 2]);
      holdFirst.complete();
      await _until(() => started.length == 6);
      for (var value = 3; value < 6; value++) {
        gates[value].complete(value);
      }
      expect(await running, isA<ExitSuccess>());
      expect(consumed, [0, 1, 2, 3, 4, 5]);
    },
  );

  test('unordered mapping emits in completion order', () async {
    final gates = List.generate(3, (_) => Completer<int>());
    var started = 0;
    final consumed = <int>[];
    final running = runtime.run(
      Flow.fromIterable([0, 1, 2])
          .mapTask(
            (value) {
              started++;
              return Task.fromFuture(
                () => gates[value].future,
                onError: _defect,
              );
            },
            concurrency: 3,
            preserveOrder: false,
          )
          .forEach((value) => Task.sync(() => consumed.add(value))),
    );
    await _until(() => started == 3);
    for (final value in [2, 0, 1]) {
      gates[value].complete(value);
      await _until(() => consumed.contains(value));
    }
    expect(await running, isA<ExitSuccess>());
    expect(consumed, [2, 0, 1]);
  });

  test(
    'widened Never inputs preserve nullable values and independent runs',
    () async {
      final Flow<int?, String> flow = Flow.fromIterable([null, 2]);
      final mapped = flow.mapTask<String?>(
        (value) => Task.succeed(value?.toString()),
        concurrency: 2,
      );
      expect(_value(await runtime.run(mapped.toList())), [null, '2']);
      expect(_value(await runtime.run(mapped.toList())), [null, '2']);
      expect(
        _value(
          await runtime.run(
            Flow.fromIterable(<int>[])
                .mapTask(Task.succeed, concurrency: 3)
                .toList(),
          ),
        ),
        isEmpty,
      );
    },
  );

  test(
    'mapper failure stops input and waits for running callback cleanup',
    () async {
      final starts = <int>[];
      final cleanupStarted = Completer<void>();
      final cleanupGate = Completer<void>();
      void Function(Exit<int, String>)? fail;
      final running = runtime.run(
        Flow.fromIterable([1, 2, 3, 4]).withError<String>().mapTask((value) {
          starts.add(value);
          return Task.fromCallback<int, String>((complete) {
            if (value == 1) fail = complete;
            return () async {
              if (value != 2) return;
              cleanupStarted.complete();
              await cleanupGate.future;
            };
          });
        }, concurrency: 2).toList(),
      );
      await _until(() => starts.length == 2);
      fail!(const Exit.failure(Cause.failure('failed')));
      await cleanupStarted.future;
      var finished = false;
      unawaited(running.then((_) => finished = true));
      await _turn();
      expect(finished, isFalse);
      expect(starts, [1, 2]);
      cleanupGate.complete();
      expect(_causes(_cause(await running)), [
        isA<CauseFailure<String>>().having(
          (cause) => cause.error,
          'error',
          'failed',
        ),
      ]);
    },
  );

  test('take retains an already produced prefetched defect', () async {
    final consumed = Completer<void>();
    final releaseConsumer = Completer<void>();
    final defect = StateError('prefetched');
    void Function(Exit<int, Never>)? fail;
    final flow = Flow.fromIterable([1, 2])
        .mapTask(
          (value) => value == 1
              ? Task.succeed(1)
              : Task.fromCallback<int, Never>((complete) {
                  fail = complete;
                  return () {};
                }),
          concurrency: 2,
        )
        .take(1);
    final running = runtime.run(
      flow.forEach(
        (_) => Task.fromFuture(() {
          consumed.complete();
          return releaseConsumer.future;
        }, onError: _defect),
      ),
    );
    await consumed.future;
    await _until(() => fail != null);
    fail!(Exit.failure(Cause.defect(defect, StackTrace.current)));
    await _turn();
    releaseConsumer.complete();
    expect(
      _causes(_cause(await running)).whereType<CauseDefect>().single.error,
      same(defect),
    );
  });

  test(
    'cancellation waits for mappers before releasing their inputs',
    () async {
      final events = <String>[];
      final controller = CancellationController();
      final started = Completer<void>();
      final stopped = Completer<void>();
      final finishStop = Completer<void>();
      final flow =
          Flow.fromTask(
            Task.acquireRelease(
              Task.succeed(1),
              release: (_, _) => Task.sync(() => events.add('input release')),
            ),
          ).mapTask(
            (value) => Task.fromCallback<int, Never>((complete) {
              started.complete();
              return () {
                events.add('mapper stop');
                stopped.complete();
                return finishStop.future;
              };
            }),
            concurrency: 2,
          );
      final running = runtime.run(flow.toList(), signal: controller.signal);
      await started.future;
      controller.requestCancel(reason: 'owner');
      await stopped.future;
      expect(events, ['mapper stop']);
      finishStop.complete();
      expect(
        _causes(_cause(await running))
            .whereType<CauseCancelled>()
            .single
            .reason,
        'owner',
      );
      expect(events, ['mapper stop', 'input release']);
    },
  );

  test(
    'concat keeps earlier input resources until their outputs are consumed',
    () async {
      final alive = <int>{};
      final events = <String>[];
      final gates = [Completer<int>(), Completer<int>()];
      final started = <int>[];
      Flow<int, Never> input(int value) => Flow.fromTask(
        Task.acquireRelease(
          Task.sync(() {
            alive.add(value);
            events.add('open $value');
            return value;
          }),
          release: (_, _) => Task.sync(() {
            alive.remove(value);
            events.add('input $value');
          }),
        ),
      );
      final running = runtime.run(
        input(0)
            .concat(input(1))
            .mapTask((value) {
              started.add(value);
              return Task.acquireRelease(
                Task.fromFuture(() => gates[value].future, onError: _defect),
                release: (_, _) => Task.sync(() => events.add('mapped $value')),
              );
            }, concurrency: 2)
            .forEach(
              (value) => Task.acquireRelease(
                Task.sync(() {
                  expect(alive, contains(value));
                  events.add('consume $value');
                }),
                release: (_, _) => Task.sync(() {
                  expect(alive, contains(value));
                  events.add('consumer $value');
                }),
              ),
            ),
      );
      await _until(() => started.length == 2);
      expect(alive, {0, 1});
      gates[1].complete(1);
      gates[0].complete(0);
      expect(await running, isA<ExitSuccess>());
      expect(
        events.indexOf('consumer 0'),
        lessThan(events.indexOf('mapped 0')),
      );
      expect(events.indexOf('mapped 0'), lessThan(events.indexOf('input 0')));
      expect(
        events.indexOf('consumer 1'),
        lessThan(events.indexOf('mapped 1')),
      );
      expect(events.indexOf('mapped 1'), lessThan(events.indexOf('input 1')));
      expect(alive, isEmpty);
    },
  );

  test(
    'flatMap leases both outer and inner producers across prefetch',
    () async {
      final alive = <String>{};
      final gates = [Completer<int>(), Completer<int>()];
      var started = 0;
      Task<int, Never> resource(String name, int value) => Task.acquireRelease(
        Task.sync(() {
          alive.add(name);
          return value;
        }),
        release: (_, _) => Task.sync(() => alive.remove(name)),
      );
      final flow = Flow.fromIterable([0, 1]).flatMap(
        (value) => Flow.fromTask(resource('outer$value', value))
            .flatMap((value) => Flow.fromTask(resource('inner$value', value))),
      );
      final running = runtime.run(
        flow
            .mapTask((value) {
              started++;
              return Task.fromFuture(
                () => gates[value].future,
                onError: _defect,
              );
            }, concurrency: 2)
            .forEach(
              (value) => Task.sync(() {
                expect(alive, containsAll(['outer$value', 'inner$value']));
              }),
            ),
      );
      await _until(() => started == 2);
      expect(alive, {'outer0', 'inner0', 'outer1', 'inner1'});
      gates[0].complete(0);
      gates[1].complete(1);
      expect(await running, isA<ExitSuccess>());
      expect(alive, isEmpty);
    },
  );

  test(
    'nested concurrent stages preserve inputs through final consumption',
    () async {
      final alive = <int>{};
      final flow = Flow.fromIterable(List.generate(12, (index) => index))
          .mapTask(
            (value) => Task.acquireRelease(
              Task.sync(() {
                alive.add(value);
                return value;
              }),
              release: (_, _) => Task.sync(() => alive.remove(value)),
            ),
            concurrency: 2,
          )
          .mapTask(
            (value) => Task.yieldNow().map((_) {
              expect(alive, contains(value));
              return value;
            }),
            concurrency: 3,
            preserveOrder: false,
          )
          .mapTask((value) => Task.succeed(value), concurrency: 1);
      final output = <int>[];
      expect(
        await runtime.run(
          flow.forEach(
            (value) => Task.sync(() {
              expect(alive, contains(value));
              output.add(value);
            }),
          ),
        ),
        isA<ExitSuccess>(),
      );
      expect(output, unorderedEquals(List.generate(12, (index) => index)));
      expect(alive, isEmpty);
    },
  );

  test(
    'recovery retires a producer while completed output holds its lease',
    () async {
      final events = <String>[];
      final mapperGate = Completer<int>();
      var mapperStarted = false;
      var pulls = 0;
      final failed =
          Flow.unfold<int, int, String>(0, (state) {
            pulls++;
            return state == 0
                ? Task.acquireRelease(
                    Task.succeed(const Option.some((1, 1))),
                    release: (_, _) => Task.sync(() => events.add('release')),
                  )
                : Task.fail('broken');
          }).recover((_) {
            events.add('recover');
            return Flow.fromIterable([2]);
          });
      final running = runtime.run(
        failed
            .mapTask((value) {
              if (value == 1) {
                mapperStarted = true;
                return Task.fromFuture(
                  () => mapperGate.future,
                  onError: _defect,
                );
              }
              return Task.succeed(value);
            }, concurrency: 2)
            .forEach((value) => Task.sync(() => events.add('consume $value'))),
      );
      await _until(() => mapperStarted && pulls == 2);
      expect(events, ['recover']);
      mapperGate.complete(1);
      expect(await running, isA<ExitSuccess>());
      expect(events, ['recover', 'consume 1', 'consume 2', 'release']);
    },
  );

  test(
    'cancelling after recovery releases all retired producer resources',
    () async {
      final controller = CancellationController();
      var pulls = 0;
      var started = false;
      var recovered = false;
      var released = false;
      final input =
          Flow.fromTask(
                Task.acquireRelease(
                  Task.succeed(null).withError<String>(),
                  release: (_, _) => Task.sync(() => released = true),
                ),
              )
              .flatMap(
                (_) => Flow.unfold<int, int, String>(0, (state) {
                  pulls++;
                  return state == 0
                      ? Task.succeed(const Option.some((1, 1)))
                      : Task.fail('failed');
                }),
              )
              .recover((_) {
                recovered = true;
                return Flow.fromIterable([2]);
              });
      final running = runtime.run(
        input
            .mapTask(
              (_) => Task.fromCallback<int, String>((complete) {
                started = true;
                return () {};
              }),
              concurrency: 2,
            )
            .toList(),
        signal: controller.signal,
      );
      await _until(() => started && pulls == 2 && recovered);
      controller.requestCancel(reason: 'cancel recovery');
      expect(
        _causes(_cause(await running))
            .whereType<CauseCancelled>()
            .single
            .reason,
        'cancel recovery',
      );
      expect(released, isTrue);
      expect(recovered, isTrue);
    },
  );

  test('mapper callbacks inherit services and task locals', () async {
    final key = ServiceKey<int>('factor');
    final local = TaskLocal<String>('root');
    final flow = Flow.fromIterable([1, 2, 3]).mapTask(
      (value) => Task.service(key).flatMap(
        (factor) => local.get().map((name) => '$name:${value * factor}'),
      ),
      concurrency: 2,
    );
    final task = local
        .locally('flow', flow.toList())
        .provideContext(Context.from([key.bind(3)]));
    expect(_value(await runtime.run(task)), ['flow:3', 'flow:6', 'flow:9']);
    expect(_value(await runtime.run(local.get())), 'root');
  });

  test(
    'two independent failures retain two occurrences of the same defect',
    () async {
      final gates = [Completer<void>(), Completer<void>()];
      var started = 0;
      final error = StateError('same object');
      final cause = Cause<Never>.defect(error, StackTrace.current);
      final flow = Flow.fromIterable([0, 1]).mapTask(
        (value) => Task.uninterruptibleMask(
          (_) => Task.fromFuture(() {
            started++;
            return gates[value].future;
          }, onError: _defect).flatMap<int>((_) => Task.failCause(cause)),
        ),
        concurrency: 2,
      );
      final running = runtime.run(flow.toList());
      await _until(() => started == 2);
      gates[0].complete();
      gates[1].complete();
      final defects = _causes(_cause(await running))
          .whereType<CauseDefect>()
          .toList();
      expect(defects, hasLength(2));
      expect(defects.map((cause) => cause.error), everyElement(same(error)));
    },
  );

  test(
    'mapper and output cleanup failures keep original stack traces once',
    () async {
      final bodyError = StateError('body');
      final cleanupError = StateError('cleanup');
      final bodyStack = StackTrace.fromString('mapping call');
      final cleanupStack = StackTrace.fromString('mapping release');
      final flow = Flow.fromIterable([1]).mapTask(
        (_) =>
            Task.acquireRelease(
              Task.succeed(null),
              release: (_, _) => Task.sync(
                () => Error.throwWithStackTrace(cleanupError, cleanupStack),
              ),
            ).flatMap<int>(
              (_) => Task.sync(
                () => Error.throwWithStackTrace(bodyError, bodyStack),
              ),
            ),
        concurrency: 2,
      );
      final defects = _causes(_cause(await runtime.run(flow.toList())))
          .whereType<CauseDefect>()
          .toList();
      expect(defects.map((cause) => cause.error), [
        same(bodyError),
        same(cleanupError),
      ]);
      expect(defects.map((cause) => cause.stackTrace.toString()), [
        'mapping call',
        'mapping release',
      ]);
    },
  );

  test(
    'upstream failure cancels pending mapping and preserves cleanup defect',
    () async {
      final cleanupError = StateError('cleanup');
      var mapped = false;
      var released = false;
      final input = Flow.unfold<int, int, String>(
        0,
        (state) => state == 0
            ? Task.succeed(const Option.some((1, 1)))
            : Task.fromFuture(
                () async {
                  await _until(() => mapped);
                  return const Option<(int, int)>.none();
                },
                onError: _defect,
              ).withError<String>().flatMap((_) => Task.fail('source')),
      );
      final flow = input.mapTask(
        (_) =>
            Task.acquireRelease(
              Task.sync(() {
                mapped = true;
                return 1;
              }),
              release: (_, _) => Task.sync(() {
                released = true;
                throw cleanupError;
              }),
            ).withError<String>().flatMap(
              (_) => Task.fromCallback<int, String>((complete) => () {}),
            ),
        concurrency: 2,
      );
      final reasons = _causes(_cause(await runtime.run(flow.toList())));
      expect(reasons.whereType<CauseFailure<String>>().single.error, 'source');
      expect(reasons.whereType<CauseDefect>().single.error, same(cleanupError));
      expect(released, isTrue);
    },
  );

  test(
    'early take waits for cleanup without exposing coordinator cancellation',
    () async {
      final closed = <int>[];
      var started = 0;
      final first = Completer<int>();
      final flow = Flow.fromIterable([0, 1, 2])
          .mapTask(
            (value) =>
                Task.acquireRelease(
                  Task.sync(() {
                    started++;
                    return value;
                  }),
                  release: (_, _) => Task.sync(() => closed.add(value)),
                ).flatMap(
                  (value) => value == 0
                      ? Task.fromFuture(() => first.future, onError: _defect)
                      : Task.fromCallback<int, Never>((complete) => () {}),
                ),
            concurrency: 3,
          )
          .take(1);
      final running = runtime.run(flow.toList());
      await _until(() => started == 3);
      first.complete(0);
      expect(_value(await running), [0]);
      expect(closed, unorderedEquals([0, 1, 2]));
    },
  );

  test('many sequential mapTask stages remain stack safe', () async {
    var flow = Flow.fromIterable([0]);
    for (var index = 0; index < 2000; index++) {
      flow = flow.mapTask((value) => Task.succeed(value + 1));
    }
    expect(_value(await runtime.run(flow.toList())), [2000]);
  });

  for (final fromMapper in [true, false]) {
    for (final preserveOrder in [true, false]) {
      test('early take retains ${fromMapper ? 'mapper' : 'source'} cleanup '
          'cancellation (preserveOrder: $preserveOrder)', () async {
        final first = Completer<int>();
        final started = Completer<void>();
        final cleaning = Completer<void>();
        final release = Completer<void>();
        final defect = StateError('cleanup');
        final stack = StackTrace.fromString('cleanup stack');
        const cancellation = Cause<Never>.cancelled('cleanup');
        final pending =
            Task.fromCallback<int, Never>((complete) {
              started.complete();
              return () {};
            }).ensuring(
              Task.fromFuture(() {
                cleaning.complete();
                return release.future;
              }, onError: _defect).flatMap(
                (_) => Task.failCause(
                  Cause.combine([
                    cancellation,
                    cancellation,
                    Cause.defect(defect, stack),
                  ]),
                ),
              ),
            );
        final source = fromMapper
            ? Flow.fromIterable([1, 2])
            : Flow.fromIterable([1]).concat(Flow.fromTask(pending));
        var completed = false;
        final running = runtime
            .run(
              source
                  .mapTask(
                    (value) => value == 1
                        ? Task.fromFuture(() => first.future, onError: _defect)
                        : pending,
                    concurrency: 2,
                    preserveOrder: preserveOrder,
                  )
                  .take(1)
                  .toList(),
            )
            .then((exit) {
              completed = true;
              return exit;
            });
        await started.future;
        first.complete(1);
        await cleaning.future;
        expect(completed, isFalse);
        release.complete();

        final causes = _causes(_cause(await running));
        expect(causes, [
          same(cancellation),
          same(cancellation),
          isA<CauseDefect>()
              .having((cause) => cause.error, 'error', same(defect))
              .having((cause) => cause.stackTrace, 'stack', same(stack)),
        ]);
      });
    }
  }

  test(
    'delivered mapper cancellation is not repeated during cleanup',
    () async {
      const cancellation = Cause<Never>.cancelled('mapper');
      final flow = Flow.fromIterable([1]).mapTask(
        (_) =>
            Task.failCause<Never>(Cause.combine([cancellation, cancellation])),
        concurrency: 2,
      );
      expect(_causes(_cause(await runtime.run(flow.toList()))), [
        same(cancellation),
        same(cancellation),
      ]);
    },
  );

  test('owner cancellation retains mapper cleanup cancellation once', () async {
    final controller = CancellationController();
    final started = Completer<void>();
    const cleanup = Cause<Never>.cancelled('cleanup');
    final flow = Flow.fromIterable([1]).mapTask(
      (_) => Task.fromCallback<int, Never>((complete) {
        started.complete();
        return () {};
      }).ensuring(Task.failCause(Cause.combine([cleanup, cleanup]))),
      concurrency: 2,
    );
    final running = runtime.run(flow.toList(), signal: controller.signal);
    await started.future;
    controller.requestCancel(reason: 'owner');
    expect(_causes(_cause(await running)), [
      isA<CauseCancelled>().having((cause) => cause.reason, 'reason', 'owner'),
      same(cleanup),
      same(cleanup),
    ]);
  });

  for (final fromMapper in [true, false]) {
    test('${fromMapper ? 'mapper' : 'source'} cleanup can request owner '
        'cancellation without losing its own failure', () async {
      final controller = CancellationController();
      const cancellation = Cause<Never>.cancelled('cleanup');
      final defect = StateError('cleanup defect');
      final stack = StackTrace.fromString('cleanup stack');
      final task = Task.succeed(1).ensuring(
        Task.defer(() {
          controller.requestCancel(reason: 'owner');
          return Task.failCause(
            Cause.combine([
              cancellation,
              cancellation,
              Cause.defect(defect, stack),
            ]),
          );
        }),
      );
      final source = fromMapper ? Flow.fromIterable([1]) : Flow.fromTask(task);
      final flow = source.mapTask(
        (value) => fromMapper ? task : Task.succeed(value),
        concurrency: 2,
      );

      expect(
        _causes(
          _cause(await runtime.run(flow.toList(), signal: controller.signal)),
        ),
        [
          isA<CauseCancelled>().having(
            (cause) => cause.reason,
            'reason',
            'owner',
          ),
          isA<CauseDefect>()
              .having((cause) => cause.error, 'error', same(defect))
              .having((cause) => cause.stackTrace, 'stack', same(stack)),
          same(cancellation),
          same(cancellation),
        ],
      );
    });
  }

  for (final capture in [true, false]) {
    test('${capture ? 'captured' : 'recovered'} cleanup cancellation is not '
        'retained when a later body reuses its cause', () async {
      final controller = CancellationController();
      const cancellation = Cause<Never>.cancelled('reused');
      final flow = Flow.fromIterable([1]).mapTask(
        (value) => Task.uninterruptibleMask<int, Never>((_) {
          final failed = Task.succeed(value)
              .ensuring(Task.failCause(cancellation));
          final handled = capture
              ? failed.exit().map((_) => value)
              : failed.recoverCause<Never>((_) => Task.succeed(value));
          return handled.flatMap((_) {
            controller.requestCancel(reason: 'owner');
            return Task.yieldNow().flatMap((_) => Task.failCause(cancellation));
          });
        }),
        concurrency: 2,
      );

      expect(
        _causes(
          _cause(await runtime.run(flow.toList(), signal: controller.signal)),
        ),
        [
          isA<CauseCancelled>().having(
            (cause) => cause.reason,
            'reason',
            'owner',
          ),
        ],
      );
    });
  }

  test('cause mapping preserves cleanup before owner cancellation', () async {
    final controller = CancellationController();
    const cancellation = Cause<Never>.cancelled('cleanup');
    final flow = Flow.fromIterable([1]).mapTask(
      (value) =>
          Task.succeed(value)
              .ensuring(Task.failCause(cancellation))
              .mapCause<Never>((cause) => cause)
              .ensuring(
                Task.defer(() {
                  controller.requestCancel(reason: 'owner');
                  return Task.yieldNow();
                }),
              ),
      concurrency: 2,
    );

    expect(
      _causes(
        _cause(await runtime.run(flow.toList(), signal: controller.signal)),
      ),
      [
        isA<CauseCancelled>().having(
          (cause) => cause.reason,
          'reason',
          'owner',
        ),
        same(cancellation),
      ],
    );
  });

  for (final reuseInCleanup in [false, true]) {
    test('owner cancellation discards late masked mapper cancellation '
        '(reused in cleanup: $reuseInCleanup)', () async {
      final controller = CancellationController();
      final started = Completer<void>();
      final release = Completer<void>();
      const cancellation = Cause<Never>.cancelled('late mapper');
      final flow = Flow.fromIterable([1]).mapTask(
        (_) =>
            Task.uninterruptibleMask<int, Never>(
              (_) => Task.fromFuture(() {
                started.complete();
                return release.future;
              }, onError: _defect).flatMap((_) => Task.failCause(cancellation)),
            ).ensuring(
              reuseInCleanup
                  ? Task.failCause(cancellation)
                  : Task.succeed(null),
            ),
        concurrency: 2,
      );
      final running = runtime.run(flow.toList(), signal: controller.signal);
      await started.future;
      controller.requestCancel(reason: 'owner');
      release.complete();
      expect(_causes(_cause(await running)), [
        isA<CauseCancelled>().having(
          (cause) => cause.reason,
          'reason',
          'owner',
        ),
        if (reuseInCleanup) same(cancellation),
      ]);
    });
  }

  test('explicit cancellation from output cleanup is retained', () async {
    final flow = Flow.fromIterable([1]).mapTask(
      (value) => Task.acquireRelease(
        Task.succeed(value),
        release: (_, _) => Task.failCause(Cause.cancelled('cleanup')),
      ),
      concurrency: 2,
    );
    expect(
      _causes(_cause(await runtime.run(flow.toList())))
          .whereType<CauseCancelled>()
          .single
          .reason,
      'cleanup',
    );
  });
}

Never _defect(Object error, StackTrace stackTrace) =>
    Error.throwWithStackTrace(error, stackTrace);
T _value<T, E>(Exit<T, E> exit) => (exit as ExitSuccess<T, E>).value;
Cause<E> _cause<T, E>(Exit<T, E> exit) => (exit as ExitFailure<T, E>).cause;
List<Cause<E>> _causes<E>(Cause<E> cause) => switch (cause) {
  CauseGroup(:final causes) => causes,
  _ => [cause],
};
Future<void> _turn() => Future<void>.delayed(Duration.zero);
Future<void> _until(bool Function() ready) async {
  for (var attempt = 0; attempt < 100; attempt++) {
    if (ready()) return;
    await _turn();
  }
  fail('Expected progress did not arrive.');
}
