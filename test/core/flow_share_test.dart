import 'dart:async';

import 'package:any/any.dart';
import 'package:any/src/core/flow.dart' show openFlowCursor;
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

  test('creation is lazy and validates before starting the source', () async {
    var starts = 0;
    final source = Flow.fromTask(Task.sync(() => ++starts));
    for (final (capacity, replay) in [(0, 0), (-1, 0), (1, -1), (1, 2)]) {
      expect(
        _causes(
          await runtime.run(source.share(capacity: capacity, replay: replay)),
        ).single,
        isA<CauseDefect>().having(
          (cause) => cause.error,
          'error',
          isA<RangeError>(),
        ),
      );
    }
    final shared = await _own(runtime, source.share(capacity: 1));
    expect(starts, 0);
    expect(_value(await runtime.run(shared.take(0).toList())), isEmpty);
    expect(starts, 0);
    expect(_value(await runtime.run(shared.toList())), [1]);
    expect(_value(await runtime.run(shared.toList())), isEmpty);
    expect(starts, 1);
  });

  test(
    'a finished owner rejects consumption without starting the source',
    () async {
      var starts = 0;
      final shared = _value(
        await runtime.run(
          Flow.fromTask(Task.sync(() => ++starts)).share(capacity: 1),
        ),
      );
      expect(
        _causes(await runtime.run(shared.toList())).single,
        isA<CauseDefect>(),
      );
      expect(starts, 0);
    },
  );

  test(
    'overlapping consumers share one production including null values',
    () async {
      final gate = Completer<void>();
      var starts = 0;
      final shared = await _own(
        runtime,
        Flow.fromTask(
          Task.sync(() => starts++)
              .flatMap((_) => _future(gate.future))
              .map<int?>((_) => null),
        ).concat(Flow.fromIterable<int?>([1, 2])).share(capacity: 2),
      );
      final first = runtime.run(shared.toList());
      final second = runtime.run(shared.toList());
      await _until(() => starts == 1);
      gate.complete();
      expect(_value(await first), [null, 1, 2]);
      expect(_value(await second), [null, 1, 2]);
      expect(starts, 1);
    },
  );

  test(
    'a slow callback bounds prefetch and backpressures the source',
    () async {
      final gate = Completer<void>();
      final entered = Completer<void>();
      final pulled = <int>[];
      final consumed = <int>[];
      Iterable<int> values() sync* {
        for (var i = 0; i < 6; i++) {
          pulled.add(i);
          yield i;
        }
      }

      final shared = await _own(
        runtime,
        Flow.fromIterable(values()).share(capacity: 3),
      );
      final running = runtime.run(
        shared.forEach((value) {
          consumed.add(value);
          if (value != 0) return Task.succeed(null);
          entered.complete();
          return _future(gate.future);
        }),
      );
      await entered.future;
      await _until(() => pulled.length == 3);
      await _turn();
      expect(pulled, [0, 1, 2]);
      expect(consumed, [0]);
      gate.complete();
      expect(await running, isA<ExitSuccess>());
      expect(consumed, [0, 1, 2, 3, 4, 5]);
    },
  );

  test(
    'completion preserves a full replay buffer for late subscribers',
    () async {
      final shared = await _own(
        runtime,
        Flow.fromIterable([1, 2, 3]).share(capacity: 2, replay: 2),
      );
      expect(_value(await runtime.run(shared.toList())), [1, 2, 3]);
      expect(_value(await runtime.run(shared.toList())), [2, 3]);
      expect(_value(await runtime.run(shared.toList())), [2, 3]);
    },
  );

  test('business failure follows buffered elements and is replayed', () async {
    final shared = await _own(
      runtime,
      Flow.fromIterable([1, 2])
          .withError<String>()
          .concat(Flow.fromTask<int, String>(Task.fail('source')))
          .share(capacity: 2, replay: 1),
    );
    final first = <int>[];
    final exit = await runtime.run(
      shared.forEach((value) => Task.sync(() => first.add(value))),
    );
    expect(first, [1, 2]);
    expect(
      _causes(exit).single,
      isA<CauseFailure<String>>().having(
        (cause) => cause.error,
        'error',
        'source',
      ),
    );
    final late = <int>[];
    final next = await runtime.run(
      shared.forEach((value) => Task.sync(() => late.add(value))),
    );
    expect(late, [2]);
    expect(_causes(next).single, isA<CauseFailure<String>>());
  });

  test('last departure stops production once and retains replay', () async {
    var starts = 0;
    var stopped = false;
    final shared = await _own(
      runtime,
      Flow.fromTask(Task.sync(() => ++starts))
          .concat(
            Flow.fromTask(
              Task.fromCallback<int, Never>(
                (_) => () {
                  stopped = true;
                },
              ),
            ),
          )
          .share(capacity: 2, replay: 1),
    );
    expect(_value(await runtime.run(shared.take(1).toList())), [1]);
    expect(stopped, isTrue);
    expect(_value(await runtime.run(shared.toList())), [1]);
    expect(starts, 1);
  });

  test('cancelling one subscriber does not cancel another', () async {
    final gate = Completer<void>();
    var started = false;
    var cancelled = false;
    final shared = await _own(
      runtime,
      Flow.fromTask(
        Task.fromCallback<int, Never>((complete) {
          started = true;
          gate.future.then((_) => complete(const Exit.success(9)));
          return () {
            cancelled = true;
          };
        }),
      ).share(capacity: 1),
    );
    final controller = CancellationController();
    final first = runtime.run(shared.toList(), signal: controller.signal);
    final second = runtime.run(shared.toList());
    await _until(() => started);
    controller.requestCancel(reason: 'first');
    expect((_causes(await first).single as CauseCancelled).reason, 'first');
    expect(cancelled, isFalse);
    gate.complete();
    expect(_value(await second), [9]);
  });

  test('replay owns input resources until the owner closes', () async {
    var open = false;
    final owner = _Owner(
      runtime,
      Flow.fromTask(
        Task.acquireRelease(
          Task.sync(() {
            open = true;
            return 1;
          }),
          release: (_, _) => Task.sync(() {
            open = false;
          }),
        ),
      ).share(capacity: 1, replay: 1),
    );
    final shared = await owner.ready.future;
    expect(
      await runtime.run(
        shared.forEach((_) => Task.sync(() => expect(open, isTrue))),
      ),
      isA<ExitSuccess>(),
    );
    expect(open, isTrue);
    expect(_value(await runtime.run(shared.toList())), [1]);
    expect(open, isTrue);
    owner.release();
    expect(await owner.done, isA<ExitSuccess>());
    expect(open, isFalse);
  });

  test('owner close waits for a delivered value in another runtime', () async {
    final foreign = Runtime();
    addTearDown(foreign.close);
    final entered = Completer<void>();
    final gate = Completer<void>();
    var open = false;
    final owner = _Owner(
      runtime,
      Flow.fromTask(
        Task.acquireRelease(
          Task.sync(() {
            open = true;
            return 1;
          }),
          release: (_, _) => Task.sync(() {
            open = false;
          }),
        ),
      ).share(capacity: 1),
    );
    final shared = await owner.ready.future;
    final consuming = foreign.run(
      shared.forEach((_) {
        entered.complete();
        return _future(gate.future).map((_) => expect(open, isTrue));
      }),
    );
    await entered.future;
    var closed = false;
    owner.done.then((_) => closed = true);
    owner.release();
    await _turn();
    expect(closed, isFalse);
    expect(open, isTrue);
    gate.complete();
    expect(
      _causes(await consuming).single,
      isA<CauseDefect>().having(
        (cause) => cause.error,
        'error',
        isA<StateError>(),
      ),
    );
    expect(await owner.done, isA<ExitSuccess>());
    expect(open, isFalse);
  });

  test('cleanup cancellation remains complete and is not repeated', () async {
    const cancellation = Cause<Never>.cancelled('cleanup');
    final defect = StateError('cleanup');
    final shared = await _own(
      runtime,
      Flow.fromTask(
        Task.acquireRelease(
          Task.succeed(1),
          release: (_, _) => Task.failCause(
            Cause.combine([
              cancellation,
              Cause.defect(defect, StackTrace.current),
              cancellation,
            ]),
          ),
        ),
      ).share(capacity: 1),
    );
    final causes = _causes(await runtime.run(shared.toList()));
    expect(causes, hasLength(3));
    expect(causes[0], same(cancellation));
    expect((causes[1] as CauseDefect).error, same(defect));
    expect(causes[2], same(cancellation));
    expect(_causes(await runtime.run(shared.toList())), hasLength(3));
  });

  test('downstream buffering keeps earlier inner producers alive', () async {
    final gate = Completer<void>();
    final open = <int>{};
    final released = <int>[];
    final shared = await _own(
      runtime,
      Flow.fromIterable([1, 2, 3])
          .flatMap(
            (value) => Flow.fromTask(
              Task.acquireRelease(
                Task.sync(() {
                  open.add(value);
                  return value;
                }),
                release: (value, _) => Task.sync(() {
                  open.remove(value);
                  released.add(value);
                }),
              ),
            ),
          )
          .share(capacity: 2),
    );
    final running = runtime.run(
      shared.buffer(3).forEach((value) {
        expect(open, contains(value));
        return value == 1
            ? _future(gate.future).map((_) => expect(open, contains(1)))
            : Task.succeed(null);
      }),
    );
    await _until(() => open.length == 3);
    expect(released, isEmpty);
    gate.complete();
    expect(await running, isA<ExitSuccess>());
    expect(released, [1, 2, 3]);
    expect(open, isEmpty);
  });

  test('the slowest subscriber applies backpressure until it leaves', () async {
    final start = Completer<void>();
    final slowEntered = Completer<void>();
    final slowCancellation = CancellationController();
    final pulled = <int>[];
    final fast = <int>[];
    final shared = await _own(
      runtime,
      Flow.fromTask(_future(start.future).map((_) => 0))
          .concat(Flow.fromIterable([1, 2, 3, 4]))
          .map((value) {
            pulled.add(value);
            return value;
          })
          .share(capacity: 2),
    );
    final slow = runtime.run(
      shared.forEach((_) {
        slowEntered.complete();
        return Task.fromCallback<void, Never>((_) => () {});
      }),
      signal: slowCancellation.signal,
    );
    final quick = runtime.run(
      shared.forEach((value) => Task.sync(() => fast.add(value))),
    );
    start.complete();
    await slowEntered.future;
    await _until(() => fast.length == 2);
    await _turn();
    expect(pulled, [0, 1]);
    slowCancellation.requestCancel(reason: 'slow');
    expect(_causes(await slow).single, isA<CauseCancelled>());
    expect(await quick, isA<ExitSuccess>());
    expect(fast, [0, 1, 2, 3, 4]);
  });

  test('source and consumer keep their respective environments', () async {
    final key = ServiceKey<String>('source');
    final local = TaskLocal('default');
    final cleanup = <String>[];
    final observed = <String>[];
    final acquire = Task.service(key).flatMap(
      (service) => local.get().flatMap(
        (value) => Task.acquireRelease(
          Task.succeed('$service/$value'),
          release: (_, _) => Task.service(key).flatMap(
            (service) =>
                local.get().map((value) => cleanup.add('$service/$value')),
          ),
        ),
      ),
    );
    final owner = _Owner(
      runtime,
      local
          .locally(
            'created',
            Flow.fromTask(acquire).share(capacity: 1, replay: 1),
          )
          .provideContext(Context.from([key.bind('owner')])),
    );
    final shared = await owner.ready.future;
    expect(
      await runtime.run(
        local
            .locally(
              'reading',
              shared.forEach(
                (value) => Task.service(key).flatMap(
                  (service) => local.get().map(
                    (local) => observed.add('$value $service/$local'),
                  ),
                ),
              ),
            )
            .provideContext(Context.from([key.bind('consumer')])),
      ),
      isA<ExitSuccess>(),
    );
    expect(observed, ['owner/created consumer/reading']);
    owner.release();
    expect(await owner.done, isA<ExitSuccess>());
    expect(cleanup, ['owner/created']);
  });

  test(
    'last cancellation awaits cleanup and keeps explicit cancellation',
    () async {
      final started = Completer<void>();
      final cleaning = Completer<void>();
      final release = Completer<void>();
      final cancellation = CancellationController();
      const cleanup = Cause<Never>.cancelled('stop');
      final shared = await _own(
        runtime,
        Flow.fromTask(
          Task.acquireRelease(
            Task.sync(() {
              started.complete();
              return 1;
            }),
            release: (_, _) =>
                Task.sync(() => cleaning.complete())
                    .flatMap((_) => _future(release.future))
                    .flatMap((_) => Task.failCause(cleanup)),
          ).flatMap((_) => Task.fromCallback<int, Never>((_) => () {})),
        ).share(capacity: 1),
      );
      var done = false;
      final running = runtime.run(shared.toList(), signal: cancellation.signal);
      running.then((_) => done = true);
      await started.future;
      cancellation.requestCancel(reason: 'stop');
      await cleaning.future;
      expect(done, isFalse);
      release.complete();
      final causes = _causes(await running);
      expect(causes, hasLength(2));
      expect((causes[0] as CauseCancelled).reason, 'stop');
      expect(causes[1], same(cleanup));
    },
  );

  test('consumer callback failure stays local to that subscription', () async {
    final start = Completer<void>();
    final shared = await _own(
      runtime,
      Flow.fromTask(_future(start.future).map((_) => 1))
          .concat(Flow.fromIterable([2, 3]))
          .share(capacity: 2)
          .map((flow) => flow.withError<String>()),
    );
    final first = runtime.run(shared.forEach((_) => Task.fail('consumer')));
    final other = runtime.run(shared.toList());
    start.complete();
    expect(
      (_causes(await first).single as CauseFailure<String>).error,
      'consumer',
    );
    expect(_value(await other), [1, 2, 3]);
    expect(_value(await runtime.run(shared.toList())), isEmpty);
  });

  test(
    'source and consumer cannot close the owning runtime from active use',
    () async {
      final foreign = Runtime();
      addTearDown(foreign.close);
      final sourceAttempt = <Exit<void, Never>>[];
      final consumerAttempt = <Exit<void, Never>>[];
      final shared = await _own(
        runtime,
        Flow.fromTask(
          Task.fromFuture(() async {
            sourceAttempt.add(await runtime.close());
            return 1;
          }, onError: Error.throwWithStackTrace),
        ).share(capacity: 1),
      );
      expect(
        await foreign.run(
          shared.forEach(
            (_) => Task.fromFuture(() async {
              consumerAttempt.add(await runtime.close());
            }, onError: Error.throwWithStackTrace),
          ),
        ),
        isA<ExitSuccess>(),
      );
      for (final exit in [...sourceAttempt, ...consumerAttempt]) {
        expect(
          _causes(exit).single,
          isA<CauseDefect>().having(
            (cause) => cause.error,
            'error',
            isA<StateError>(),
          ),
        );
      }
    },
  );

  test(
    'replay cleanup failure belongs to the owner when no consumer holds it',
    () async {
      const cleanup = Cause<Never>.cancelled('replay cleanup');
      final owner = _Owner(
        runtime,
        Flow.fromTask(
          Task.acquireRelease(
            Task.succeed(1),
            release: (_, _) => Task.failCause(cleanup),
          ),
        ).share(capacity: 1, replay: 1),
      );
      final shared = await owner.ready.future;
      expect(_value(await runtime.run(shared.toList())), [1]);
      owner.release();
      expect(_causes(await owner.done), [same(cleanup)]);
    },
  );

  test('a long synchronous source and late replay remain stack safe', () async {
    final shared = await _own(
      runtime,
      Flow.fromIterable(List.generate(10000, (index) => index))
          .share(capacity: 3, replay: 2),
    );
    expect(
      _value(await runtime.run(shared.fold(0, (sum, value) => sum + value))),
      49995000,
    );
    expect(_value(await runtime.run(shared.toList())), [9998, 9999]);
  });

  for (final operation in ['mapTask', 'buffer', 'grouped', 'groupedWithin']) {
    for (final cancelled in [false, true]) {
      test(
        '$operation releases shared leases on ${cancelled ? 'cancellation' : 'failure'}',
        () async {
          final acquired = <int>{};
          final released = <int>{};
          final entered = Completer<void>();
          final controller = CancellationController();
          final owner = _Owner(
            runtime,
            Flow.fromIterable([1, 2, 3, 4])
                .flatMap(
                  (value) => Flow.fromTask(
                    Task.acquireRelease(
                      Task.sync(() {
                        acquired.add(value);
                        return value;
                      }),
                      release: (value, _) => Task.sync(() {
                        released.add(value);
                      }),
                    ),
                  ),
                )
                .share(capacity: 2),
          );
          final shared = (await owner.ready.future).withError<String>();
          final Flow<Object?, String> flow = switch (operation) {
            'mapTask' => shared.mapTask(Task.succeed, concurrency: 3),
            'buffer' => shared.buffer(3),
            'grouped' => shared.grouped(2),
            _ => shared.groupedWithin(2, const Duration(seconds: 5)),
          };
          final running = runtime.run(
            flow.forEach((_) {
              entered.complete();
              return cancelled
                  ? Task.fromCallback<void, String>((_) => () {})
                  : Task.fail('consumer');
            }),
            signal: controller.signal,
          );
          await entered.future;
          if (cancelled) controller.requestCancel(reason: 'consumer');
          final causes = _causes(await running);
          expect(
            causes,
            hasLength(1),
            reason: causes
                .whereType<CauseDefect>()
                .map((cause) => '${cause.error}\n${cause.stackTrace}')
                .join('\n'),
          );
          expect(
            causes.single,
            cancelled ? isA<CauseCancelled>() : isA<CauseFailure<String>>(),
          );
          expect(released, acquired);
          owner.release();
          expect(await owner.done, isA<ExitSuccess>());
        },
      );
    }

    test(
      '$operation keeps an external value alive during owner close',
      () async {
        final foreign = Runtime();
        addTearDown(foreign.close);
        final active = <int>{};
        final entered = Completer<void>();
        final release = Completer<void>();
        final owner = _Owner(
          runtime,
          Flow.fromIterable([1, 2, 3])
              .flatMap(
                (value) => Flow.fromTask(
                  Task.acquireRelease(
                    Task.sync(() {
                      active.add(value);
                      return value;
                    }),
                    release: (value, _) => Task.sync(() {
                      active.remove(value);
                    }),
                  ),
                ),
              )
              .share(capacity: 2),
        );
        final shared = await owner.ready.future;
        final Flow<List<int>, Never> flow = switch (operation) {
          'mapTask' => shared.mapTask(
            (value) => Task.succeed([value]),
            concurrency: 3,
          ),
          'buffer' => shared.buffer(3).map((value) => [value]),
          'grouped' => shared.grouped(2),
          _ => shared.groupedWithin(2, const Duration(seconds: 5)),
        };
        final consuming = foreign.run(
          flow.take(1).forEach((values) {
            entered.complete();
            return _future(release.future)
                .map((_) => expect(active, containsAll(values)));
          }),
        );
        await entered.future;
        var closed = false;
        owner.done.then((_) => closed = true);
        owner.release();
        await _turn();
        expect(closed, isFalse);
        release.complete();
        await consuming;
        expect(await owner.done, isA<ExitSuccess>());
        expect(active, isEmpty);
      },
    );
  }

  test(
    'cancelling before the deferred driver starts closes empty roots',
    () async {
      var starts = 0;
      final owner = _Owner(
        runtime,
        Flow.fromTask(Task.sync(() => ++starts)).share(capacity: 1),
      );
      final shared = await owner.ready.future;
      final controller = CancellationController();
      void Function()? start;
      var captured = false;
      final running = runZoned(
        () => runtime.run(shared.toList(), signal: controller.signal),
        zoneSpecification: ZoneSpecification(
          scheduleMicrotask: (self, parent, zone, callback) {
            if (!captured) {
              captured = true;
              start = () => parent.scheduleMicrotask(zone, callback);
            } else {
              parent.scheduleMicrotask(zone, callback);
            }
          },
        ),
      );
      await _until(() => start != null);
      controller.requestCancel(reason: 'before start');
      await _turn();
      start!();
      expect(_causes(await running).single, isA<CauseCancelled>());
      expect(starts, 0);
      owner.release();
      expect(await owner.done, isA<ExitSuccess>());
    },
  );
  test(
    'a late replay subscriber backpressures an already pending pull',
    () async {
      final second = Completer<int>();
      final pulling = Completer<void>();
      final reading = Completer<void>();
      final registered = Completer<void>();
      var candidateOpen = false;
      final shared = await _own(
        runtime,
        Flow.fromIterable([1])
            .concat(
              Flow.fromTask(
                Task.acquireRelease(
                  Task.sync(() {
                    candidateOpen = true;
                    pulling.complete();
                  }).flatMap(
                    (_) => Task.fromFuture(
                      () => second.future,
                      onError: Error.throwWithStackTrace,
                    ),
                  ),
                  release: (_, _) => Task.sync(() {
                    candidateOpen = false;
                  }),
                ),
              ),
            )
            .share(capacity: 1, replay: 1),
      );
      final first = runtime.run(shared.toList());
      await pulling.future;
      final late = runtime.run(
        openFlowCursor(shared)
            .flatMap(
              (cursor) =>
                  Task.sync(() => registered.complete())
                      .flatMap((_) => _future(reading.future))
                      .flatMap(
                        (_) => cursor.pull().flatMap(
                          (one) => cursor.pull().flatMap(
                            (two) =>
                                cursor.pull().map((end) => [one, two, end]),
                          ),
                        ),
                      ),
            )
            .scoped(),
      );
      await registered.future;
      second.complete(2);
      await _turn();
      expect(candidateOpen, isTrue);
      reading.complete();
      final items = _value(await late);
      expect((items[0] as OptionSome<int>).value, 1);
      expect((items[1] as OptionSome<int>).value, 2);
      expect(items[2], isA<OptionNone>());
      expect(_value(await first), [1, 2]);
    },
  );

  test(
    'a late waiter can cancel while the last consumer is cleaning up',
    () async {
      final started = Completer<void>();
      final cleaning = Completer<void>();
      final release = Completer<void>();
      final firstCancel = CancellationController();
      final lateCancel = CancellationController();
      final shared = await _own(
        runtime,
        Flow.fromTask(
          Task.acquireRelease(
            Task.sync(() => started.complete()),
            release: (_, _) =>
                Task.sync(() => cleaning.complete())
                    .flatMap((_) => _future(release.future)),
          ).flatMap((_) => Task.fromCallback<int, Never>((_) => () {})),
        ).share(capacity: 1),
      );
      final first = runtime.run(shared.toList(), signal: firstCancel.signal);
      await started.future;
      firstCancel.requestCancel();
      await cleaning.future;
      final late = runtime.run(shared.toList(), signal: lateCancel.signal);
      await _turn();
      lateCancel.requestCancel(reason: 'late');
      expect((_causes(await late).single as CauseCancelled).reason, 'late');
      release.complete();
      expect(_causes(await first).single, isA<CauseCancelled>());
    },
  );

  test(
    'cancellation during producer cleanup retains its cancellation cause',
    () async {
      final cleaning = Completer<void>();
      final release = Completer<void>();
      final controller = CancellationController();
      const cleanup = Cause<Never>.cancelled('cleanup handoff');
      final shared = await _own(
        runtime,
        Flow.fromTask(
          Task.acquireRelease(
            Task.succeed(1),
            release: (_, _) =>
                Task.sync(() => cleaning.complete())
                    .flatMap((_) => _future(release.future))
                    .flatMap((_) => Task.failCause(cleanup)),
          ),
        ).share(capacity: 1),
      );
      final running = runtime.run(shared.toList(), signal: controller.signal);
      await cleaning.future;
      controller.requestCancel(reason: 'consumer');
      release.complete();
      final causes = _causes(await running);
      expect(causes, hasLength(2));
      expect(causes, contains(same(cleanup)));
      expect(
        causes.whereType<CauseCancelled>().map((cause) => cause.reason),
        containsAll(['consumer', 'cleanup handoff']),
      );
    },
  );
  test(
    'owner close includes cleanup completed by an external final lease',
    () async {
      final foreign = Runtime();
      addTearDown(foreign.close);
      final entered = Completer<void>();
      final release = Completer<void>();
      const cleanup = Cause<Never>.cancelled('external cleanup');
      final owner = _Owner(
        runtime,
        Flow.fromTask(
          Task.acquireRelease(
            Task.succeed(1),
            release: (_, _) => Task.failCause(cleanup),
          ),
        ).share(capacity: 1),
      );
      final shared = await owner.ready.future;
      final consuming = foreign.run(
        shared.take(1).forEach((_) {
          entered.complete();
          return _future(release.future);
        }),
      );
      await entered.future;
      owner.release();
      await _turn();
      release.complete();
      expect(_causes(await consuming), [same(cleanup)]);
      expect(_causes(await owner.done), [same(cleanup)]);
    },
  );
}

final class _Owner<T, E> {
  _Owner(Runtime runtime, Task<T, E> create) {
    done = runtime.run(
      create.flatMap(
        (value) =>
            Task.sync(() => ready.complete(value))
                .flatMap((_) => _future(_release.future)),
      ),
    );
  }
  final ready = Completer<T>();
  final _release = Completer<void>();
  late final Future<Exit<void, E>> done;
  void release() => _release.complete();
}

Future<T> _own<T>(Runtime runtime, Task<T, Never> create) async {
  final ready = Completer<T>();
  unawaited(
    runtime.run(
      create.flatMap(
        (value) =>
            Task.sync(() => ready.complete(value))
                .flatMap((_) => Task.fromCallback<void, Never>((_) => () {})),
      ),
    ),
  );
  return ready.future;
}

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
Future<void> _turn() => Future<void>.delayed(Duration.zero);
Future<void> _until(bool Function() condition) async {
  for (var i = 0; i < 2000; i++) {
    if (condition()) return;
    await _turn();
  }
  fail('Condition was not reached.');
}
