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

  test(
    'construction and transformations are lazy and repeat independently',
    () async {
      var iterated = 0;
      var transformed = 0;
      Iterable<int?> values() sync* {
        iterated++;
        yield null;
        yield 2;
      }

      final flow = Flow.fromIterable(values()).map((value) {
        transformed++;
        return value == null ? null : value * 2;
      });
      final consume = flow.toList();
      expect(iterated, 0);
      expect(transformed, 0);
      expect(_value(await runtime.run(consume)), [null, 4]);
      expect(_value(await runtime.run(consume)), [null, 4]);
      expect(iterated, 2);
      expect(transformed, 4);
    },
  );

  test('iterable is read at execution and output is immutable', () async {
    final input = [1];
    final flow = Flow.fromIterable(input);
    input.add(2);
    final result = _value(await runtime.run(flow.toList()));
    expect(result, [1, 2]);
    expect(() => result.add(3), throwsUnsupportedError);
  });

  test('fromTask emits once per consumption', () async {
    var calls = 0;
    final flow = Flow.fromTask(Task.sync(() => ++calls));
    expect(_value(await runtime.run(flow.toList())), [1]);
    expect(_value(await runtime.run(flow.toList())), [2]);
  });

  test('unfold preserves nullable values and resets its state', () async {
    final flow = Flow.unfold<int, int?, Never>(
      0,
      (state) => Task.succeed(
        state == 3
            ? const Option.none()
            : Option.some((state == 0 ? null : state, state + 1)),
      ),
    );
    expect(_value(await runtime.run(flow.toList())), [null, 1, 2]);
    expect(_value(await runtime.run(flow.toList())), [null, 1, 2]);
  });

  test('mapTask and forEach request one element at a time', () async {
    final events = <String>[];
    final mapped = Completer<int>();
    final consumed = Completer<void>();
    Iterable<int> values() sync* {
      for (var i = 1; i <= 2; i++) {
        events.add('pull $i');
        yield i;
      }
    }

    final task = Flow.fromIterable(values())
        .mapTask((value) {
          events.add('map $value');
          return value == 1
              ? Task.fromFuture(() => mapped.future, onError: _defect)
              : Task.succeed(value);
        })
        .forEach((value) {
          events.add('consume $value');
          return value == 1
              ? Task.fromFuture(() => consumed.future, onError: _defect)
              : Task.succeed(null);
        });
    final running = runtime.run(task);
    await _until(() => events.contains('map 1'));
    expect(events, ['pull 1', 'map 1']);
    mapped.complete(1);
    await _until(() => events.contains('consume 1'));
    expect(events, ['pull 1', 'map 1', 'consume 1']);
    consumed.complete();
    expect(await running, isA<ExitSuccess>());
    expect(events, [
      'pull 1',
      'map 1',
      'consume 1',
      'pull 2',
      'map 2',
      'consume 2',
    ]);
  });

  test('filter, filterTask, scan, drop, and fold preserve order', () async {
    final flow = Flow.fromIterable([1, 2, 3, 4, 5, 6])
        .filter((value) => value.isEven)
        .filterTask((value) => Task.succeed(value != 4))
        .scan(10, (sum, value) => sum + value);
    expect(_value(await runtime.run(flow.toList())), [12, 18]);
    expect(
      _value(
        await runtime.run(flow.drop(1).fold(0, (sum, value) => sum + value)),
      ),
      18,
    );
    expect(
      _value(
        await runtime.run(
          Flow.fromIterable(<int>[]).fold(7, (sum, value) => sum + value),
        ),
      ),
      7,
    );
  });

  test(
    'take bounds an infinite flow without requesting another element',
    () async {
      var pulls = 0;
      final flow = Flow.unfold<int, int, Never>(0, (state) {
        pulls++;
        return Task.succeed(Option.some((state, state + 1)));
      });
      expect(_value(await runtime.run(flow.take(3).toList())), [0, 1, 2]);
      expect(pulls, 3);
    },
  );

  test('take zero does not open the upstream iterable', () async {
    var calls = 0;
    Iterable<int> values() sync* {
      calls++;
      throw StateError('must not iterate');
    }

    expect(
      _value(await runtime.run(Flow.fromIterable(values()).take(0).toList())),
      isEmpty,
    );
    expect(calls, 0);
  });

  test('negative take and drop fail lazily before opening upstream', () async {
    var calls = 0;
    final flow = Flow.fromTask(Task.sync(() => ++calls));
    for (final bounded in [flow.take(-1), flow.drop(-1)]) {
      expect(
        _defects(_cause(await runtime.run(bounded.drain()))).single.error,
        isA<RangeError>(),
      );
    }
    expect(calls, 0);
  });

  test(
    'takeWhile excludes and takeUntil includes their stopping element',
    () async {
      final flow = Flow.fromIterable([1, 2, 3, 4]);
      expect(
        _value(
          await runtime.run(flow.takeWhile((value) => value < 3).toList()),
        ),
        [1, 2],
      );
      expect(
        _value(
          await runtime.run(flow.takeUntil((value) => value == 3).toList()),
        ),
        [1, 2, 3],
      );
      expect(_value(await runtime.run(flow.drop(20).toList())), isEmpty);
    },
  );

  test(
    'flatMap shares user resource ownership across sequential inner flows',
    () async {
      final events = <String>[];
      final flow = Flow.fromIterable([1, 2]).flatMap(
        (value) => Flow.fromTask(
          Task.acquireRelease(
            Task.sync(() {
              events.add('open $value');
              return value;
            }),
            release: (_, _) => Task.sync(() => events.add('close $value')),
          ),
        ).concat(Flow.fromIterable([value * 10])),
      );
      expect(_value(await runtime.run(flow.toList())), [1, 10, 2, 20]);
      expect(events, ['open 1', 'open 2', 'close 2', 'close 1']);
    },
  );

  test('resource remains alive in consuming callbacks after take', () async {
    var alive = false;
    final events = <String>[];
    final flow = Flow.fromTask(
      Task.acquireRelease(
        Task.sync(() {
          alive = true;
          return Object();
        }),
        release: (_, _) => Task.sync(() {
          alive = false;
          events.add('close');
        }),
      ),
    ).take(1);
    expect(
      await runtime.run(
        flow.forEach(
          (_) => Task.sync(() {
            expect(alive, isTrue);
            events.add('consume');
          }),
        ),
      ),
      isA<ExitSuccess>(),
    );
    expect(alive, isFalse);
    expect(events, ['consume', 'close']);
  });

  test(
    'concat preserves user resources while starting the second source',
    () async {
      final events = <String>[];
      final first = Flow.fromTask(
        Task.acquireRelease(
          Task.succeed(1),
          release: (_, _) => Task.sync(() => events.add('close first')),
        ),
      );
      final second = Flow.fromTask(
        Task.sync(() {
          events.add('start second');
          return 2;
        }),
      );
      expect(_value(await runtime.run(first.concat(second).toList())), [1, 2]);
      expect(events, ['start second', 'close first']);
    },
  );

  test(
    'body and cleanup defects retain their identity and order once',
    () async {
      final body = StateError('body');
      final cleanup = StateError('cleanup');
      final flow = Flow.fromTask(
        Task.acquireRelease(
          Task.succeed(1),
          release: (_, _) => Task.sync(() => throw cleanup),
        ),
      ).map<int>((_) => throw body);
      expect(
        _defects(_cause(await runtime.run(flow.drain())))
            .map((cause) => cause.error),
        [body, cleanup],
      );
    },
  );

  test('recover preserves user resources while opening replacement', () async {
    final events = <String>[];
    final failing = Task.acquireRelease(
      Task.succeed(1).withError<String>(),
      release: (_, _) => Task.sync(() => events.add('close failed')),
    ).flatMap<int>((_) => Task.fail('broken'));
    final flow = Flow.fromTask(failing).recover((error) {
      expect(error, 'broken');
      events.add('replace');
      return Flow.fromIterable([2, 3]);
    });
    expect(_value(await runtime.run(flow.toList())), [2, 3]);
    expect(events, ['replace', 'close failed']);
  });

  test(
    'recover preserves prior output and does not recover replacement again',
    () async {
      var recovered = 0;
      final replacementFailure = Cause<String>.combine([
        const Cause.failure('second'),
        const Cause.cancelled('replacement stopped'),
        const Cause.failure('third'),
      ]);
      final flow = Flow.fromIterable([1])
          .withError<String>()
          .concat(Flow.fromTask<int, String>(Task.fail('first')))
          .recover((_) {
            recovered++;
            return Flow.fromIterable([2]).withError<String>().concat(
              Flow.fromTask<int, String>(Task.failCause(replacementFailure)),
            );
          });
      final values = <int>[];
      final cause = _cause(
        await runtime.run(
          flow.forEach((value) => Task.sync(() => values.add(value))),
        ),
      );
      final reasons = _reasons(cause);
      expect(reasons, hasLength(3));
      expect((reasons[0] as CauseFailure<String>).error, 'second');
      expect((reasons[1] as CauseCancelled).reason, 'replacement stopped');
      expect((reasons[2] as CauseFailure<String>).error, 'third');
      expect(values, [1, 2]);
      expect(recovered, 1);
    },
  );

  test(
    'explicit local cleanup prevents recovery and concat from continuing',
    () async {
      final cleanup = StateError('cleanup');
      var recovered = false;
      var nextStarted = false;
      final failed =
          Flow.fromTask(
            Task.acquireRelease(
              Task.succeed(1).withError<String>(),
              release: (_, _) => Task.sync(() => throw cleanup),
            ).flatMap<int>((_) => Task.fail('business')).scoped(),
          ).recover((_) {
            recovered = true;
            return Flow.fromIterable([2]);
          });
      final reasons = _reasons(_cause(await runtime.run(failed.drain())));
      expect(reasons, hasLength(2));
      expect(reasons.first, isA<CauseFailure<String>>());
      expect((reasons.last as CauseDefect).error, same(cleanup));
      expect(recovered, isFalse);
      final success = Flow.fromTask(
        Task.acquireRelease(
          Task.succeed(1),
          release: (_, _) => Task.sync(() => throw cleanup),
        ).scoped(),
      );
      final next = Flow.fromTask(
        Task.sync(() {
          nextStarted = true;
          return 2;
        }),
      );
      expect(
        _defects(_cause(await runtime.run(success.concat(next).drain())))
            .single
            .error,
        same(cleanup),
      );
      expect(nextStarted, isFalse);
    },
  );

  test(
    'mapError transforms producer failures while preserving defects',
    () async {
      final flow = Flow.fromTask<int, int>(Task.fail(7))
          .mapError((error) => 'E$error');
      expect(
        _cause(await runtime.run(flow.drain())),
        isA<CauseFailure<String>>().having(
          (cause) => cause.error,
          'error',
          'E7',
        ),
      );
      final error = StateError('mapping');
      final thrown = Flow.fromTask<int, int>(Task.fail(7))
          .mapError<String>((_) => throw error);
      expect(
        _defects(_cause(await runtime.run(thrown.drain()))).single.error,
        same(error),
      );
    },
  );

  test('widened Never flows compose through static extensions', () async {
    final Flow<int, String> flow = Flow.fromIterable([1, 2]);
    final result = flow
        .mapTask((value) => Task.succeed(value + 1).withError<String>())
        .flatMap(
          (value) => Flow.fromTask(Task.succeed(value).withError<String>()),
        )
        .filterTask((value) => Task.succeed(value > 1).withError<String>())
        .mapError((error) => error.length)
        .recover((_) => Flow.fromIterable([0]));
    expect(_value(await runtime.run(result.toList())), [2, 3]);
  });

  test(
    'queue end drains values and early consumption does not close it',
    () async {
      final task = AsyncQueue.unbounded<int?>()
          .withError<QueueClosed>()
          .flatMap(
            (queue) => queue
                .offer(null)
                .flatMap((_) => queue.offer(2))
                .flatMap(
                  (_) => Flow.fromQueue(queue).take(1).toList().flatMap((
                    first,
                  ) {
                    expect(first, [null]);
                    return queue
                        .offer(3)
                        .flatMap((_) => queue.end().withError<QueueClosed>())
                        .flatMap((_) => Flow.fromQueue(queue).toList());
                  }),
                ),
          );
      expect(_value(await runtime.run(task)), [2, 3]);
    },
  );

  test('closed queue is a business failure', () async {
    final task = AsyncQueue.unbounded<int>()
        .flatMap((queue) => queue.close().map((_) => queue))
        .withError<QueueClosed>()
        .flatMap((queue) => Flow.fromQueue(queue).drain());
    expect(_cause(await runtime.run(task)), isA<CauseFailure<QueueClosed>>());
  });

  test(
    'cancellation closes the producer without secondary guard defects',
    () async {
      final ready = Completer<void>();
      final controller = CancellationController();
      final events = <String>[];
      final flow =
          Flow.fromTask(
            Task.acquireRelease(
              Task.succeed(1),
              release: (_, _) => Task.sync(() => events.add('release')),
            ),
          ).mapTask(
            (_) => Task.fromCallback<int, Never>((_) {
              ready.complete();
              return () => events.add('cancel work');
            }),
          );
      final running = runtime.run(flow.drain(), signal: controller.signal);
      await ready.future;
      controller.requestCancel(reason: 'stop');
      final reason = _cause(await running);
      expect(
        reason,
        isA<CauseCancelled>().having((c) => c.reason, 'reason', 'stop'),
      );
      expect(events, ['cancel work', 'release']);
    },
  );

  test(
    'downstream resources release before their source dependencies',
    () async {
      final events = <String>[];
      var sourceAlive = false;
      final source = Flow.fromTask(
        Task.acquireRelease(
          Task.sync(() {
            sourceAlive = true;
            return 1;
          }),
          release: (_, _) => Task.sync(() {
            sourceAlive = false;
            events.add('source');
          }),
        ),
      );
      final flow = source
          .map((value) => value)
          .mapTask(
            (value) => Task.acquireRelease(
              Task.succeed(value),
              release: (_, _) => Task.sync(() {
                expect(sourceAlive, isTrue);
                events.add('map');
              }),
            ),
          )
          .filterTask(
            (_) => Task.acquireRelease(
              Task.succeed(true),
              release: (_, _) => Task.sync(() {
                expect(sourceAlive, isTrue);
                events.add('filter');
              }),
            ),
          );
      expect(
        await runtime.run(
          flow.forEach(
            (_) => Task.acquireRelease(
              Task.succeed(null),
              release: (_, _) => Task.sync(() {
                expect(sourceAlive, isTrue);
                events.add('consumer');
              }),
            ),
          ),
        ),
        isA<ExitSuccess>(),
      );
      expect(events, ['consumer', 'filter', 'map', 'source']);
    },
  );

  test(
    'concatenated user tasks register resources in one consuming scope',
    () async {
      final events = <String>[];
      Flow<int, Never> source(int value) => Flow.fromTask(
        Task.acquireRelease(
          Task.succeed(value),
          release: (_, _) => Task.sync(() => events.add('source $value')),
        ),
      );
      final flow = source(1)
          .concat(source(2))
          .mapTask(
            (value) => Task.acquireRelease(
              Task.succeed(value),
              release: (_, _) => Task.sync(() => events.add('mapped $value')),
            ),
          );
      expect(_value(await runtime.run(flow.toList())), [1, 2]);
      expect(events, ['mapped 2', 'source 2', 'mapped 1', 'source 1']);
    },
  );

  test('take stops nested production without closing user resources', () async {
    final events = <String>[];
    Flow<int, Never> source(int value) => Flow.fromTask(
      Task.acquireRelease(
        Task.succeed(value),
        release: (_, _) => Task.sync(() => events.add('close $value')),
      ),
    );
    final flow = source(0)
        .flatMap((_) => source(1).concat(source(2)))
        .take(2)
        .concat(
          Flow.fromTask(
            Task.sync(() {
              events.add('next');
              return 3;
            }),
          ),
        );
    expect(_value(await runtime.run(flow.toList())), [1, 2, 3]);
    expect(events, ['next', 'close 2', 'close 1', 'close 0']);
  });

  test('early termination waits for asynchronous producer cleanup', () async {
    final releasing = Completer<void>();
    final released = Completer<void>();
    var nextStarted = false;
    late StreamController<int> source;
    source = StreamController<int>(
      onListen: () => source.add(1),
      onCancel: () {
        releasing.complete();
        return released.future;
      },
    );
    addTearDown(source.close);
    final first = Flow.fromStream<int, Never>(
      () => source.stream,
      onError: _defect,
    );
    final running = runtime.run(
      first
          .takeUntil((_) => true)
          .concat(
            Flow.fromTask(
              Task.sync(() {
                nextStarted = true;
                return 2;
              }),
            ),
          )
          .toList(),
    );
    await releasing.future;
    expect(nextStarted, isFalse);
    released.complete();
    expect(_value(await running), [1, 2]);
  });

  test('consumer failure closes source with the original failure', () async {
    Exit<Object?, Object?>? released;
    final flow = Flow.fromTask(
      Task.acquireRelease(
        Task.succeed(1).withError<String>(),
        release: (_, exit) => Task.sync(() {
          released = exit;
        }),
      ),
    );
    final result = await runtime.run(
      flow.forEach((_) => Task.fail('consumer')),
    );
    expect((_cause(result) as CauseFailure).error, 'consumer');
    expect((_cause(released!) as CauseFailure).error, 'consumer');
  });

  test(
    'early termination stops producer children before releasing inputs',
    () async {
      final events = <String>[];
      final acquired =
          Task.acquireRelease(
            Task.succeed(1),
            release: (_, _) => Task.sync(() => events.add('release')),
          ).flatMap(
            (value) => Task.fromCallback<void, Never>((_) {
              events.add('start child');
              return () => events.add('stop child');
            }).fork().map((_) => value),
          );
      expect(
        _value(await runtime.run(Flow.fromTask(acquired).take(1).toList())),
        [1],
      );
      expect(events, ['start child', 'stop child', 'release']);
    },
  );

  test('producer cleanup preserves context and task-local values', () async {
    final key = ServiceKey<String>('label');
    final local = TaskLocal<int>(0);
    final values = <String>[];
    final source = Task.acquireRelease(
      Task.service(key),
      release: (_, _) => Task.service(key).flatMap(
        (label) => local.get().map((value) => values.add('$label $value')),
      ),
    );
    final task = local.locally(
      7,
      Flow.fromTask(source)
          .take(1)
          .toList()
          .provideContext(Context.from([key.bind('local')])),
    );
    expect(_value(await runtime.run(task)), ['local']);
    expect(values, ['local 7']);
  });

  test('iterator and callback defects retain their original stacks', () async {
    final error = StateError('source');
    final stack = StackTrace.fromString('original flow stack');
    Iterable<int> values() sync* {
      yield 1;
      Error.throwWithStackTrace(error, stack);
    }

    var recovered = false;
    final flow = Flow.fromIterable(values()).withError<String>().recover((_) {
      recovered = true;
      return Flow.fromIterable([2]);
    });
    final cause = _defects(_cause(await runtime.run(flow.drain()))).single;
    expect(cause.error, same(error));
    expect(cause.stackTrace.toString(), stack.toString());
    expect(recovered, isFalse);
    final mapped = Flow.fromIterable([1])
        .map<int>((_) => Error.throwWithStackTrace(error, stack));
    final mappedCause = _defects(_cause(await runtime.run(mapped.drain())))
        .single;
    expect(mappedCause.error, same(error));
    expect(mappedCause.stackTrace.toString(), stack.toString());
  });

  test('cancel during recovery cleanup prevents replacement', () async {
    final started = Completer<void>();
    final gate = Completer<void>();
    final controller = CancellationController();
    var recovered = false;
    final cleanupError = StateError('cleanup');
    final flow =
        Flow.fromTask(
          Task.acquireRelease(
            Task.succeed(1).withError<String>(),
            release: (_, _) => Task.fromFuture(
              () {
                started.complete();
                return gate.future;
              },
              onError: _defect,
            ).flatMap((_) => Task.sync(() => throw cleanupError)),
          ).flatMap<int>((_) => Task.fail('business')).scoped(),
        ).recover((_) {
          recovered = true;
          return Flow.fromIterable([2]);
        });
    final running = runtime.run(flow.drain(), signal: controller.signal);
    await started.future;
    controller.requestCancel(reason: 'stop');
    gate.complete();
    final reasons = _reasons(_cause(await running));
    expect(reasons.whereType<CauseCancelled>().single.reason, 'stop');
    expect(reasons.whereType<CauseDefect>().single.error, same(cleanupError));
    expect(recovered, isFalse);
  });

  test(
    'queue waiting cancellation leaves queue usable by another consumer',
    () async {
      final task = AsyncQueue.unbounded<int>().withError<QueueClosed>().flatMap(
        (queue) =>
            Flow.fromQueue(queue)
                .drain()
                .fork()
                .withError<QueueClosed>()
                .flatMap(
                  (waiting) => Task.yieldNow()
                      .withError<QueueClosed>()
                      .flatMap((_) => waiting.cancel().withError<QueueClosed>())
                      .flatMap((_) => queue.offer(7))
                      .flatMap((_) => queue.end().withError<QueueClosed>())
                      .flatMap((_) => Flow.fromQueue(queue).toList()),
                ),
      );
      expect(_value(await runtime.run(task)), [7]);
    },
  );

  test('pre-cancellation prevents producer and recovery callbacks', () async {
    final controller = CancellationController()..requestCancel();
    var started = false;
    var recovered = false;
    final flow =
        Flow.fromTask(
          Task.sync(() {
            started = true;
            return 1;
          }),
        ).withError<String>().recover((_) {
          recovered = true;
          return Flow.fromIterable([2]);
        });
    expect(
      _cause(await runtime.run(flow.drain(), signal: controller.signal)),
      isA<CauseCancelled>(),
    );
    expect(started, isFalse);
    expect(recovered, isFalse);
  });

  test('recover bypasses defects and cancellation-only causes', () async {
    var recovered = false;
    final defect = Cause<String>.defect(
      StateError('defect'),
      StackTrace.current,
    );
    for (final cause in <Cause<String>>[
      Cause.combine([
        const Cause.failure('a'),
        const Cause.failure('b'),
        defect,
      ]),
      const Cause.cancelled('explicit'),
      Cause.combine([
        const Cause.cancelled('first'),
        const Cause.cancelled('second'),
      ]),
    ]) {
      final flow = Flow.fromTask<int, String>(Task.failCause(cause))
          .recover((_) {
            recovered = true;
            return Flow.fromIterable([2]);
          });
      final actual = _cause(await runtime.run(flow.drain()));
      expect(_reasons(actual), _reasons(cause).map(same));
    }
    expect(recovered, isFalse);
  });

  test(
    'recover selects the first grouped error and preserves prior values',
    () async {
      for (final first in <String?>['first', null]) {
        final seen = <String?>[];
        final flow = Flow.fromIterable([1, 2])
            .withError<String?>()
            .concat(
              Flow.fromTask(
                Task.failCause(
                  Cause<String?>.combine([
                    const Cause.cancelled('recorded'),
                    Cause.failure(first),
                    const Cause.failure('later'),
                  ]),
                ),
              ),
            )
            .recover((error) {
              seen.add(error);
              return Flow.fromIterable([99]);
            });

        expect(_value(await runtime.run(flow.toList())), [1, 2, 99]);
        expect(seen, [first]);
      }
    },
  );

  test(
    'source cleanup preserves the full group and prevents recovery',
    () async {
      final first = Object();
      final second = Object();
      final cleanup = StateError('stream cleanup');
      final stack = StackTrace.fromString('stream cleanup stack');
      late StreamController<int> source;
      source = StreamController<int>(
        onListen: () => source.add(1),
        onCancel: () => Error.throwWithStackTrace(cleanup, stack),
      );
      addTearDown(source.close);
      final cause = Cause<Object>.combine([
        Cause.failure(first),
        const Cause.cancelled('recorded'),
        Cause.failure(second),
      ]);
      final flow =
          Flow.fromStream<int, Never>(() => source.stream, onError: _defect)
              .withError<Object>()
              .mapTask((_) => Task.failCause(cause))
              .recover(
                (_) => fail('The cleanup defect must prevent recovery.'),
              );

      final reasons = _reasons(_cause(await runtime.run(flow.drain())));
      expect(reasons, hasLength(4));
      expect((reasons[0] as CauseFailure<Object>).error, same(first));
      expect((reasons[1] as CauseCancelled).reason, 'recorded');
      expect((reasons[2] as CauseFailure<Object>).error, same(second));
      expect((reasons[3] as CauseDefect).error, same(cleanup));
      expect((reasons[3] as CauseDefect).stackTrace, same(stack));
    },
  );

  test(
    'cancellation during acquisition still registers and releases the value',
    () async {
      final acquiring = Completer<void>();
      final acquired = Completer<int>();
      final controller = CancellationController();
      final released = <int>[];
      var consumed = false;
      final flow = Flow.fromTask(
        Task.acquireRelease(
          Task.fromFuture(() {
            acquiring.complete();
            return acquired.future;
          }, onError: _defect),
          release: (value, _) => Task.sync(() => released.add(value)),
        ),
      );
      final running = runtime.run(
        flow.forEach(
          (_) => Task.sync(() {
            consumed = true;
          }),
        ),
        signal: controller.signal,
      );
      await acquiring.future;
      controller.requestCancel();
      acquired.complete(7);
      expect(_cause(await running), isA<CauseCancelled>());
      expect(consumed, isFalse);
      expect(released, [7]);
    },
  );

  test('ten thousand maps remain stack safe', () async {
    var flow = Flow.fromIterable([0]);
    for (var index = 0; index < 10000; index++) {
      flow = flow.map((value) => value + 1);
    }
    expect(_value(await runtime.run(flow.toList())), [10000]);
  });

  test('long rejected and empty-inner sequences remain stack safe', () async {
    final values = List.generate(10000, (index) => index);
    expect(
      _value(
        await runtime.run(
          Flow.fromIterable(values).filter((_) => false).toList(),
        ),
      ),
      isEmpty,
    );
    expect(
      _value(
        await runtime.run(
          Flow.fromIterable(values)
              .flatMap((_) => Flow.fromIterable(<int>[]))
              .toList(),
        ),
      ),
      isEmpty,
    );
  });
}

Never _defect(Object error, StackTrace stack) =>
    Error.throwWithStackTrace(error, stack);
T _value<T, E>(Exit<T, E> exit) => switch (exit) {
  ExitSuccess(:final value) => value,
  ExitFailure(:final cause) => fail('Expected success, got ${_reasons(cause)}'),
};
Cause<E> _cause<T, E>(Exit<T, E> exit) => switch (exit) {
  ExitSuccess(:final value) => fail('Expected failure, got $value'),
  ExitFailure(:final cause) => cause,
};
List<Cause<E>> _reasons<E>(Cause<E> cause) => switch (cause) {
  CauseGroup(:final causes) => causes,
  _ => [cause],
};
Iterable<CauseDefect> _defects(Cause<Object?> cause) =>
    _reasons(cause).whereType<CauseDefect>();
Future<void> _until(bool Function() condition) async {
  for (var attempt = 0; attempt < 100 && !condition(); attempt++) {
    await Future<void>.delayed(Duration.zero);
  }
  expect(condition(), isTrue);
}
