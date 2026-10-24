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

  test('capacity is checked lazily before opening the source', () async {
    var opened = false;
    final source = Flow.fromTask(
      Task.sync(() {
        opened = true;
        return 1;
      }),
    );
    for (final capacity in [0, -1]) {
      final flow = source.buffer(capacity);
      expect(opened, isFalse);
      expect(
        _cause(await runtime.run(flow.toList())),
        isA<CauseDefect>().having(
          (cause) => cause.error,
          'error',
          isA<RangeError>(),
        ),
      );
    }
    expect(opened, isFalse);
    expect(
      _value(await runtime.run(source.buffer(1).take(0).toList())),
      isEmpty,
    );
    expect(opened, isFalse);
  });

  test(
    'empty, nullable and widened values support independent consumption',
    () async {
      final Flow<int?, String> flow = Flow.fromIterable<int?>([null, 1, 2])
          .buffer(2)
          .withError<String>();
      expect(_value(await runtime.run(flow.toList())), [null, 1, 2]);
      expect(_value(await runtime.run(flow.toList())), [null, 1, 2]);
      expect(
        _value(
          await runtime.run(Flow.fromIterable(<int>[]).buffer(1).toList()),
        ),
        isEmpty,
      );
    },
  );

  for (final capacity in [1, 3]) {
    test(
      'capacity $capacity excludes current delivery and bounds prefetch',
      () async {
        final pulled = <int>[];
        final consumed = <int>[];
        final gate = Completer<void>();
        Iterable<int> values() sync* {
          for (var value = 0; value < 8; value++) {
            pulled.add(value);
            yield value;
          }
        }

        final flow = Flow.fromIterable(values()).buffer(capacity);
        expect(pulled, isEmpty);
        final running = runtime.run(
          flow.forEach((value) {
            consumed.add(value);
            return value == 0
                ? Task.fromFuture(() => gate.future, onError: _defect)
                : Task.succeed(null);
          }),
        );
        await _until(() => pulled.length == capacity + 1);
        expect(consumed, [0]);
        await _turn();
        expect(pulled, List.generate(capacity + 1, (index) => index));
        gate.complete();
        expect(await running, isA<ExitSuccess>());
        expect(consumed, List.generate(8, (index) => index));
      },
    );
  }

  test(
    'one in-flight pull reserves a buffer slot and pulls are sequential',
    () async {
      final gate = Completer<void>();
      final first = Completer<void>();
      final pulled = <int>[];
      var active = 0;
      final flow = Flow.unfold<int, int, Never>(0, (state) {
        pulled.add(state);
        expect(active++, 0);
        final Task<void, Never> wait = state == 1
            ? Task.fromFuture(() => gate.future, onError: _defect)
            : Task.succeed(null);
        return wait.map((_) {
          active--;
          return state == 3
              ? const Option.none()
              : Option.some((state, state + 1));
        });
      }).buffer(1);
      final running = runtime.run(
        flow.forEach(
          (value) => value == 0
              ? Task.fromFuture(() => first.future, onError: _defect)
              : Task.succeed(null),
        ),
      );
      await _until(() => pulled.length == 2);
      expect(active, 1);
      gate.complete();
      await _turn();
      expect(pulled, [0, 1]);
      first.complete();
      expect(await running, isA<ExitSuccess>());
      expect(pulled, [0, 1, 2, 3]);
    },
  );

  test('business failure is delivered after all queued values', () async {
    final failed = Completer<void>();
    final delivered = <int>[];
    final flow = Flow.fromIterable([1, 2, 3])
        .withError<String>()
        .concat(
          Flow.fromTask(
            Task.defer<int, String>(() {
              failed.complete();
              return Task.fail('source');
            }),
          ),
        )
        .buffer(4);
    final exit = await runtime.run(
      flow.forEach((value) {
        delivered.add(value);
        return value == 1
            ? Task.fromFuture(() => failed.future, onError: _defect)
            : Task.succeed(null);
      }),
    );
    expect(delivered, [1, 2, 3]);
    expect(
      _cause(exit),
      isA<CauseFailure<String>>().having(
        (cause) => cause.error,
        'error',
        'source',
      ),
    );
  });

  test(
    'defects follow queued values and keep their occurrence and stack',
    () async {
      final error = StateError('source');
      final stack = StackTrace.fromString('buffer source');
      final failed = Completer<void>();
      final delivered = <int>[];
      final flow = Flow.fromIterable([1, 2, 3])
          .concat(
            Flow.fromTask(
              Task.sync<int>(() {
                failed.complete();
                Error.throwWithStackTrace(error, stack);
              }),
            ),
          )
          .buffer(4);
      final exit = await runtime.run(
        flow.forEach((value) {
          delivered.add(value);
          return value == 1
              ? Task.fromFuture(() => failed.future, onError: _defect)
              : Task.succeed(null);
        }),
      );
      expect(delivered, [1, 2, 3]);
      final defect = _causes(_cause(exit)).whereType<CauseDefect>().single;
      expect(defect.error, same(error));
      expect(defect.stackTrace.toString(), 'buffer source');
    },
  );

  test(
    'downstream callbacks keep the original source resource scope',
    () async {
      final events = <String>[];
      final flow = Flow.fromIterable([1, 2, 3]).buffer(1);
      expect(
        await runtime.run(
          flow.forEach(
            (value) => Task.acquireRelease(
              Task.sync(() {
                expect(
                  events.where((event) => event.startsWith('release')),
                  isEmpty,
                );
                events.add('use $value');
              }),
              release: (_, _) => Task.sync(() => events.add('release $value')),
            ),
          ),
        ),
        isA<ExitSuccess>(),
      );
      expect(events, [
        'use 1',
        'use 2',
        'use 3',
        'release 3',
        'release 2',
        'release 1',
      ]);
    },
  );

  test('producer and consumer inherit services and task locals', () async {
    final key = ServiceKey<String>('name');
    final local = TaskLocal<String>('root');
    final observed = <String>[];
    Task<String, Never> read() =>
        Task.service(key)
            .flatMap((service) => local.get().map((name) => '$service:$name'));
    final flow = Flow.fromTask(read()).buffer(1);
    final task = flow.forEach(
      (value) => local.locally(
        'consumer',
        read().map((current) {
          observed.addAll([value, current]);
        }),
      ),
    );
    expect(
      await runtime.run(
        local
            .locally('producer', task)
            .provideContext(Context.from([key.bind('bound')])),
      ),
      isA<ExitSuccess>(),
    );
    expect(observed, ['bound:producer', 'bound:consumer']);
  });

  test(
    'buffered concat and flatMap inputs stay alive across producer switches',
    () async {
      final live = <String>{};
      final consumed = <int>[];
      final ready = Completer<void>();
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
          .concat(
            Flow.fromTask(
              Task.sync(() {
                ready.complete();
                return 3;
              }),
            ),
          )
          .buffer(3);
      expect(
        await runtime.run(
          flow.forEach(
            (value) => Task.defer(() {
              consumed.add(value);
              if (value == 3) return Task.succeed(null);
              expect(live, containsAll(['outer$value', 'inner$value']));
              return value == 1
                  ? Task.fromFuture(() => ready.future, onError: _defect).map(
                      (_) => expect(live, containsAll(['outer1', 'inner1'])),
                    )
                  : Task.succeed(null);
            }),
          ),
        ),
        isA<ExitSuccess>(),
      );
      expect(consumed, [1, 2, 3]);
      expect(live, isEmpty);
    },
  );

  for (final size in [2, 10]) {
    test('grouped($size) retains mapped inputs through buffered EOF', () async {
      final live = <int>{};
      final flow = Flow.fromIterable([1, 2, 3])
          .mapTask(
            (value) => Task.acquireRelease(
              Task.sync(() {
                live.add(value);
                return value;
              }),
              release: (_, _) => Task.sync(() => live.remove(value)),
            ),
          )
          .buffer(1)
          .grouped(size);
      final groups = <List<int>>[];
      expect(
        await runtime.run(
          flow.forEach(
            (group) => Task.sync(() {
              expect(live, containsAll(group));
              groups.add(group);
            }),
          ),
        ),
        isA<ExitSuccess>(),
      );
      expect(
        groups,
        size == 2
            ? [
                [1, 2],
                [3],
              ]
            : [
                [1, 2, 3],
              ],
      );
      expect(live, isEmpty);
    });
  }

  test(
    'nested buffers and concurrent mappings retain resources until use',
    () async {
      final live = <int>{};
      final flow = Flow.fromIterable(List.generate(12, (index) => index))
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
          .buffer(1)
          .mapTask((value) => Task.yieldNow().map((_) => value), concurrency: 3)
          .grouped(5);
      final consumed = <int>[];
      expect(
        await runtime.run(
          flow.forEach(
            (group) => Task.sync(() {
              expect(live, containsAll(group));
              consumed.addAll(group);
            }),
          ),
        ),
        isA<ExitSuccess>(),
      );
      expect(consumed, List.generate(12, (index) => index));
      expect(live, isEmpty);
    },
  );

  test(
    'recovery supplies a partially grouped buffer without waiting on leases',
    () async {
      var live = false;
      final flow =
          Flow.fromTask(
                Task.acquireRelease(
                  Task.sync(() {
                    live = true;
                    return 1;
                  }),
                  release: (_, _) => Task.sync(() => live = false),
                ),
              )
              .withError<String>()
              .concat(Flow.fromTask(Task.fail('recover')))
              .buffer(1)
              .recover((_) => Flow.fromIterable([2]))
              .grouped(2);
      expect(
        await runtime.run(
          flow.forEach(
            (values) => Task.sync(() {
              expect(values, [1, 2]);
              expect(live, isTrue);
            }),
          ),
        ),
        isA<ExitSuccess>(),
      );
      expect(live, isFalse);
    },
  );

  test(
    'take waits for an in-flight Future and retains its eventual defect',
    () async {
      final started = Completer<void>();
      final gate = Completer<int>();
      final error = StateError('late Future');
      var finished = false;
      final flow = Flow.fromIterable([1])
          .concat(
            Flow.fromTask(
              Task.fromFuture(() {
                started.complete();
                return gate.future;
              }, onError: _defect),
            ),
          )
          .buffer(1);
      final running = runtime
          .run(
            flow
                .take(1)
                .forEach(
                  (_) =>
                      Task.fromFuture(() => started.future, onError: _defect),
                ),
          )
          .then((exit) {
            finished = true;
            return exit;
          });
      await started.future;
      await _turn();
      expect(finished, isFalse);
      gate.completeError(error, StackTrace.fromString('late prefetch'));
      final reasons = _causes(_cause(await running));
      expect(reasons, hasLength(1));
      final defect = reasons.single as CauseDefect;
      expect(defect.error, same(error));
      expect(defect.stackTrace.toString(), 'late prefetch');
    },
  );

  test(
    'take preserves a defect already observed ahead of the consumer',
    () async {
      final failed = Completer<void>();
      final error = StateError('prefetched');
      final flow = Flow.fromIterable([1, 2])
          .concat(
            Flow.fromTask(
              Task.sync<int>(() {
                failed.complete();
                throw error;
              }),
            ),
          )
          .buffer(3);
      final exit = await runtime.run(
        flow
            .take(1)
            .forEach(
              (_) => Task.fromFuture(() => failed.future, onError: _defect),
            ),
      );
      expect(
        _causes(_cause(exit)).single,
        isA<CauseDefect>().having((cause) => cause.error, 'error', same(error)),
      );
    },
  );

  test(
    'cancellation stops the pull before releasing queued input resources',
    () async {
      final controller = CancellationController();
      final started = Completer<void>();
      final stopped = Completer<void>();
      final gate = Completer<void>();
      final events = <String>[];
      final first = Flow.fromTask(
        Task.acquireRelease(
          Task.succeed(1),
          release: (_, _) => Task.sync(() => events.add('release input')),
        ),
      );
      final pending = Flow.fromTask(
        Task.fromCallback<int, Never>((complete) {
          started.complete();
          return () {
            events.add('stop pull');
            stopped.complete();
            return gate.future;
          };
        }),
      );
      final flow = first.concat(pending).buffer(2);
      final running = runtime.run(
        flow.forEach((_) => Task.fromCallback((_) => () {})),
        signal: controller.signal,
      );
      await started.future;
      controller.requestCancel(reason: 'consumer');
      await stopped.future;
      expect(events, ['stop pull']);
      gate.complete();
      final reasons = _causes(_cause(await running));
      expect(reasons.whereType<CauseCancelled>().single.reason, 'consumer');
      expect(events, ['stop pull', 'release input']);
    },
  );

  test(
    'external cancellation retains pull and input cleanup defects once',
    () async {
      final controller = CancellationController();
      final started = Completer<void>();
      final pullError = StateError('pull cleanup');
      final inputError = StateError('input cleanup');
      final flow =
          Flow.fromTask(
                Task.acquireRelease(
                  Task.succeed(1),
                  release: (_, _) => Task.sync(() => throw inputError),
                ),
              )
              .concat(
                Flow.fromTask(
                  Task.fromCallback<int, Never>((complete) {
                    started.complete();
                    return () => throw pullError;
                  }),
                ),
              )
              .buffer(2);
      final running = runtime.run(
        flow.forEach((_) => Task.fromCallback((_) => () {})),
        signal: controller.signal,
      );
      await started.future;
      controller.requestCancel(reason: 'consumer');
      final reasons = _causes(_cause(await running));
      expect(reasons.whereType<CauseCancelled>().single.reason, 'consumer');
      expect(
        reasons.whereType<CauseDefect>().map((cause) => cause.error),
        unorderedEquals([pullError, inputError]),
      );
    },
  );

  for (final cancelExternally in [false, true]) {
    test(
      'explicit source null cancellation survives ${cancelExternally ? 'external cancel' : 'take'}',
      () async {
        final controller = CancellationController();
        final cancelled = Completer<void>();
        final flow = Flow.fromIterable([1])
            .concat(
              Flow.fromTask(
                Task.defer<int, Never>(() {
                  cancelled.complete();
                  return Task.failCause(const Cause.cancelled());
                }),
              ),
            )
            .buffer(2);
        final running = runtime.run(
          (cancelExternally ? flow : flow.take(1)).forEach(
            (_) => cancelExternally
                ? Task.fromCallback((_) => () {})
                : Task.fromFuture(() => cancelled.future, onError: _defect),
          ),
          signal: controller.signal,
        );
        await cancelled.future;
        await _turn();
        if (cancelExternally) controller.requestCancel(reason: 'consumer');
        final reasons = _causes(_cause(await running))
            .whereType<CauseCancelled>()
            .map((cause) => cause.reason)
            .toList();
        expect(
          reasons,
          cancelExternally ? unorderedEquals([null, 'consumer']) : [null],
        );
      },
    );

    test(
      'explicit resource null cancellation survives ${cancelExternally ? 'external cancel' : 'take'}',
      () async {
        final controller = CancellationController();
        final started = Completer<void>();
        final flow =
            Flow.fromTask(
                  Task.acquireRelease(
                    Task.succeed(1),
                    release: (_, _) => Task.failCause(const Cause.cancelled()),
                  ),
                )
                .concat(
                  Flow.fromTask(
                    Task.sync(() {
                      started.complete();
                      return 2;
                    }),
                  ),
                )
                .buffer(1);
        final running = runtime.run(
          (cancelExternally ? flow : flow.take(1)).forEach(
            (_) => cancelExternally
                ? Task.fromCallback((_) => () {})
                : Task.fromFuture(() => started.future, onError: _defect),
          ),
          signal: controller.signal,
        );
        await started.future;
        if (cancelExternally) controller.requestCancel(reason: 'consumer');
        final reasons = _causes(_cause(await running))
            .whereType<CauseCancelled>()
            .map((cause) => cause.reason)
            .toList();
        expect(
          reasons,
          cancelExternally ? unorderedEquals([null, 'consumer']) : [null],
        );
      },
    );
  }

  test(
    'explicit null cancellation delivered normally is not duplicated',
    () async {
      final flow = Flow.fromIterable([1])
          .concat(Flow.fromTask(Task.failCause<Never>(const Cause.cancelled())))
          .buffer(2);
      final exit = await runtime.run(flow.toList());
      expect(_causes(_cause(exit)), [
        isA<CauseCancelled>().having((cause) => cause.reason, 'reason', isNull),
      ]);
    },
  );
  test(
    'take does not duplicate already completed source cleanup cancellation',
    () async {
      final ended = Completer<void>();
      final consumed = Completer<void>();
      final flow = Flow.fromIterable([1])
          .concat(
            Flow.fromTask(
              Task.acquireRelease(
                Task.succeed(2),
                release: (_, _) => Task.defer(() {
                  ended.complete();
                  return Task.failCause(const Cause.cancelled());
                }),
              ).scoped(),
            ).filter((_) => false),
          )
          .buffer(2);
      final running = runtime.run(
        flow
            .take(1)
            .forEach(
              (_) => Task.fromFuture(() => consumed.future, onError: _defect),
            ),
      );
      await ended.future;
      await _turn();
      consumed.complete();
      expect(
        _causes(_cause(await running)).whereType<CauseCancelled>(),
        hasLength(1),
      );
    },
  );
  test(
    'take retains cancellation from cleanup finishing after driver stop',
    () async {
      final started = Completer<void>();
      final gate = Completer<void>();
      final consumed = Completer<void>();
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
          .buffer(2);
      final running = runtime.run(
        flow
            .take(1)
            .forEach(
              (_) => Task.fromFuture(() => consumed.future, onError: _defect),
            ),
      );
      await started.future;
      consumed.complete();
      await _turn();
      gate.complete();
      expect(
        _causes(_cause(await running)).whereType<CauseCancelled>(),
        hasLength(1),
      );
    },
  );
  test('external cancellation keeps a resource cancellation that finishes during shutdown', () async {
    final controller = CancellationController();
    final started = Completer<void>();
    final gate = Completer<void>();
    final flow = Flow.fromIterable([1])
        .concat(
          Flow.fromTask(
            Task.acquireRelease(
              Task.succeed(2),
              release: (_, _) =>
                  Task.fromFuture(() {
                    started.complete();
                    return gate.future;
                  }, onError: _defect).flatMap(
                    (_) => Task.failCause(
                      Cause.combine([
                        const Cause.cancelled(),
                        const Cause.cancelled(),
                      ]),
                    ),
                  ),
            ).scoped(),
          ).filter((_) => false),
        )
        .buffer(2);
    final running = runtime.run(
      flow.forEach((_) => Task.fromCallback((_) => () {})),
      signal: controller.signal,
    );
    await started.future;
    controller.requestCancel(reason: 'consumer');
    await _turn();
    gate.complete();
    expect(
      _causes(_cause(await running))
          .whereType<CauseCancelled>()
          .map((cause) => cause.reason),
      unorderedEquals(['consumer', null, null]),
    );
  });
  test(
    'owner cancellation discards undelivered cancellation from a masked source',
    () async {
      final controller = CancellationController();
      final started = Completer<void>();
      final gate = Completer<void>();
      final flow = Flow.fromIterable([1])
          .concat(
            Flow.fromTask(
              Task.uninterruptibleMask<int, Never>(
                (_) => Task.fromFuture(
                  () {
                    started.complete();
                    return gate.future;
                  },
                  onError: _defect,
                ).flatMap((_) => Task.failCause(const Cause.cancelled())),
              ),
            ),
          )
          .buffer(2);
      final running = runtime.run(
        flow.forEach((_) => Task.fromCallback((_) => () {})),
        signal: controller.signal,
      );
      await started.future;
      controller.requestCancel(reason: 'consumer');
      await _turn();
      gate.complete();
      expect(
        _causes(_cause(await running))
            .whereType<CauseCancelled>()
            .map((cause) => cause.reason),
        ['consumer'],
      );
    },
  );
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
