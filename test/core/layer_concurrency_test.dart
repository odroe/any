import 'dart:async';

import 'package:any/any.dart';
import 'package:any/testing.dart';
import 'package:test/test.dart';

void main() {
  final reports = <Cause<Object?>>[];
  setUp(reports.clear);
  tearDown(() async {
    await _turn();
    expect(reports, isEmpty);
  });

  test(
    'provision timeout waits for construction cleanup before fallback',
    () async {
      final clock = TestClock();
      final runtime = Runtime(
        context: Context.from([Clock.key.bind(clock)]),
        onUnhandledError: reports.add,
      );
      final started = Completer<void>();
      final cleanup = Completer<void>();
      var stopped = false;
      var cleaned = false;
      var fallbackRan = false;
      var releases = 0;
      addTearDown(() async {
        if (!cleanup.isCompleted) cleanup.complete();
        await runtime.close();
      });
      final key = ServiceKey<int>('pending');
      final layer = key.layer(
        Task.acquireRelease(
          Task.succeed(1),
          release: (_, _) => Task.sync(() => releases++),
        ).flatMap(
          (_) => Task.fromCallback<int, Never>((_) {
            started.complete();
            return () async {
              stopped = true;
              await cleanup.future;
              cleaned = true;
            };
          }),
        ),
      );
      const deadline = Duration(seconds: 1);
      final running = runtime.run(
        Task.service(key)
            .provide(layer)
            .timeoutOrElse(
              deadline,
              () => Task.sync(() {
                fallbackRan = true;
                expect(stopped, isTrue);
                expect(cleaned, isTrue);
                expect(releases, 0);
                return 2;
              }),
            ),
      );
      await started.future;
      clock.advance(deadline);
      await _until(() => stopped || fallbackRan);
      expect(stopped, isTrue);
      expect(fallbackRan, isFalse);
      expect(releases, 0);
      cleanup.complete();
      expect(_value(await running), 2);
      expect(releases, 1);
    },
  );

  test(
    'a losing provision stops its build while ambient resources remain',
    () async {
      final runtime = Runtime(onUnhandledError: reports.add);
      addTearDown(runtime.close);
      final started = Completer<void>();
      var stopped = false;
      var releases = 0;
      final key = ServiceKey<int>('pending');
      final layer = key.layer(
        Task.acquireRelease(
          Task.succeed(1),
          release: (_, _) => Task.sync(() => releases++),
        ).flatMap(
          (_) => Task.fromCallback<int, Never>((_) {
            started.complete();
            return () {
              stopped = true;
            };
          }),
        ),
      );
      final result = await runtime.run(
        Task.race([
          Task.service(key).provide(layer),
          Task.fromFuture<void, Never>(
            () => started.future,
            onError: Error.throwWithStackTrace,
          ).map((_) => 2),
        ]).map((value) {
          expect(stopped, isTrue);
          expect(releases, 0);
          return value;
        }),
      );
      expect(_value(result), 2);
      expect(releases, 1);
    },
  );

  test('failed local construction waits for sibling cleanup and retains its defect', () async {
    final runtime = Runtime(onUnhandledError: reports.add);
    addTearDown(runtime.close);
    final pending = ServiceKey<int>('pending');
    final failed = ServiceKey<int>('failed');
    final started = Completer<void>();
    final defect = StateError('cancel construction');
    var stopped = false;
    final layer = pending
        .layer(
          Task.fromCallback<int, String>((_) {
            started.complete();
            return () {
              stopped = true;
              throw defect;
            };
          }),
        )
        .merge(
          failed.layer(
            Task.fromFuture<void, String>(
              () => started.future,
              onError: Error.throwWithStackTrace,
            ).flatMap((_) => Task.fail('build failed')),
          ),
          concurrency: 2,
        );
    final result = await runtime.run(
      Task.service(pending)
          .withError<String>()
          .provide(layer)
          .exit()
          .map((exit) {
            expect(stopped, isTrue);
            final reasons = _reasons(_cause(exit));
            expect(
              reasons.whereType<CauseFailure<String>>().single.error,
              'build failed',
            );
            expect(
              reasons.whereType<CauseDefect>().map((cause) => cause.error),
              [same(defect)],
            );
          }),
    );
    expect(result, isA<ExitSuccess>());
  });

  test('local construction shares its caller request domain', () async {
    final runtime = Runtime(onUnhandledError: reports.add);
    addTearDown(runtime.close);
    var loads = 0;
    final resolver = RequestResolver<int, int, Never>((keys) {
      loads++;
      return Task.succeed([for (final key in keys) Result.success(key)]);
    });
    final service = ServiceKey<int>('service');
    final result = await runtime.runOrThrow(
      resolver
          .request(1)
          .flatMap(
            (_) =>
                Task.service(service)
                    .provide(service.layer(resolver.request(1)))
                    .flatMap((_) => resolver.request(1)),
          ),
    );
    expect(result, 1);
    expect(loads, 1);
  });

  test(
    'parallel construction bounds active branches and keeps their outputs',
    () async {
      final keys = List.generate(
        5,
        (index) => ServiceKey<int>('service $index'),
      );
      final pending = <int, void Function(Exit<int, Never>)>{};
      var active = 0;
      var maximum = 0;
      final started = <int>[];
      final layers = [
        for (var index = 0; index < keys.length; index++)
          keys[index].layer(
            Task.fromCallback<int, Never>((complete) {
              active++;
              if (active > maximum) maximum = active;
              started.add(index);
              pending[index] = (exit) {
                active--;
                complete(exit);
              };
              return () {
                active--;
              };
            }),
          ),
      ];
      final opening = Runtime.create(
        Layer.mergeAll(layers, concurrency: 2),
        onUnhandledError: reports.add,
      );
      await _until(() => started.length == 2);
      expect(started, [0, 1]);
      pending[1]!(const Exit.success(10));
      await _until(() => started.length == 3);
      pending[2]!(const Exit.success(20));
      await _until(() => started.length == 4);
      pending[0]!(const Exit.success(0));
      await _until(() => started.length == 5);
      pending[4]!(const Exit.success(40));
      pending[3]!(const Exit.success(30));
      final runtime = _value(await opening);
      expect(maximum, 2);
      for (var index = 0; index < keys.length; index++) {
        expect(
          _value(await runtime.run(Task.service(keys[index]))),
          index * 10,
        );
      }
      expect(active, 0);
      await runtime.close();
    },
  );

  test(
    'parallel requests share one pending acquisition and one release',
    () async {
      final shared = ServiceKey<Object>('shared');
      final a = ServiceKey<Object>('a');
      final b = ServiceKey<Object>('b');
      final acquiring = Completer<void>();
      final gate = Completer<Object>();
      var calls = 0;
      var releases = 0;
      final dependency = shared.layer(
        Task.acquireRelease(
          Task.fromFuture(
            () {
              calls++;
              if (!acquiring.isCompleted) acquiring.complete();
              return gate.future;
            },
            onError: (error, stack) => Error.throwWithStackTrace(error, stack),
          ),
          release: (_, _) => Task.sync(() => releases++),
        ),
      );
      final opening = Runtime.create(
        a
            .layer(Task.service(shared))
            .provide(dependency)
            .merge(
              b.layer(Task.service(shared)).provide(dependency),
              concurrency: 2,
            ),
        onUnhandledError: reports.add,
      );
      await acquiring.future;
      await _turn();
      expect(calls, 1);
      final resource = Object();
      gate.complete(resource);
      final runtime = _value(await opening);
      expect(_value(await runtime.run(Task.service(a))), same(resource));
      expect(_value(await runtime.run(Task.service(b))), same(resource));
      await runtime.close();
      expect(releases, 1);
    },
  );

  test('a later failing branch stops a pending earlier branch and waits for cleanup', () async {
    final pending = ServiceKey<int>('pending');
    final failed = ServiceKey<int>('failed');
    final stopped = Completer<void>();
    final cleaned = Completer<void>();
    final defect = StateError('cancellation cleanup');
    final trace = StackTrace.fromString('cleanup');
    var cancellations = 0;
    final a = pending.layer(
      Task.fromCallback<int, String>(
        (_) => () async {
          cancellations++;
          stopped.complete();
          await cleaned.future;
          Error.throwWithStackTrace(defect, trace);
        },
      ),
    );
    final b = failed.layer(Task.fail('failed'));
    final opening = Runtime.create(
      a.merge(b, concurrency: 2),
      onUnhandledError: reports.add,
    );
    await stopped.future.timeout(const Duration(seconds: 2));
    var returned = false;
    unawaited(opening.then((_) => returned = true));
    await _turn();
    expect(returned, isFalse);
    cleaned.complete();
    final reasons = _reasons(_cause(await opening));
    expect(reasons.whereType<CauseFailure<String>>().single.error, 'failed');
    final errors = reasons.whereType<CauseDefect>().toList();
    expect(errors, hasLength(1));
    expect(errors.single.error, same(defect));
    expect(errors.single.stackTrace.toString(), trace.toString());
    expect(cancellations, 1);
  });

  test(
    'simultaneous business failures are retained in observation order',
    () async {
      final first = ServiceKey<int>('first');
      final second = ServiceKey<int>('second');
      final exit = await Runtime.create(
        first
            .layer(Task.fail('first'))
            .merge(second.layer(Task.fail('second')), concurrency: 2),
        onUnhandledError: reports.add,
      );
      expect(
        _reasons(_cause(exit))
            .whereType<CauseFailure<String>>()
            .map((cause) => cause.error),
        ['first', 'second'],
      );
      expect(_reasons(_cause(exit)).whereType<CauseCancelled>(), isEmpty);
    },
  );

  test('parallel shared defects preserve one occurrence', () async {
    final dependency = ServiceKey<int>('dependency');
    final a = ServiceKey<int>('a');
    final b = ServiceKey<int>('b');
    final defect = Cause<Never>.defect(Object(), StackTrace.current);
    final shared = dependency.layer(Task.failCause(defect));
    final exit = await Runtime.create(
      a
          .layer(Task.service(dependency))
          .provide(shared)
          .merge(
            b.layer(Task.service(dependency)).provide(shared),
            concurrency: 2,
          ),
      onUnhandledError: reports.add,
    );
    expect(_reasons(_cause(exit)).whereType<CauseDefect>(), [same(defect)]);
  });

  test(
    'independent parallel executions retain equal defect objects separately',
    () async {
      final a = ServiceKey<int>('a');
      final b = ServiceKey<int>('b');
      final defect = Cause<Never>.defect(Object(), StackTrace.current);
      final exit = await Runtime.create(
        a
            .layer(Task.failCause(defect))
            .merge(b.layer(Task.failCause(defect)), concurrency: 2),
        onUnhandledError: reports.add,
      );
      expect(_reasons(_cause(exit)).whereType<CauseDefect>(), [
        same(defect),
        same(defect),
      ]);
    },
  );

  test(
    'external cancellation preserves every active branch cleanup defect',
    () async {
      final controller = CancellationController();
      final started = Completer<void>();
      final errors = [StateError('a'), StateError('b')];
      var count = 0;
      final layers = [
        for (var index = 0; index < 2; index++)
          ServiceKey<int>('service $index').layer(
            Task.fromCallback<int, Never>((_) {
              if (++count == 2) started.complete();
              return () => throw errors[index];
            }),
          ),
      ];
      final opening = Runtime.create(
        Layer.mergeAll(layers, concurrency: 2),
        signal: controller.signal,
        onUnhandledError: reports.add,
      );
      await started.future;
      controller.requestCancel(reason: 'external');
      final reasons = _reasons(_cause(await opening));
      expect(reasons.whereType<CauseCancelled>().first.reason, 'external');
      expect(
        reasons.whereType<CauseDefect>().map((cause) => cause.error),
        unorderedEquals(errors),
      );
    },
  );

  test(
    'a locally scoped acquisition can unwind while its application closes',
    () async {
      final controller = CancellationController();
      final started = Completer<void>();
      final events = <String>[];
      final key = ServiceKey<int>('service');
      final acquire =
          Task.acquireRelease(
                Task.succeed(1),
                release: (_, _) => Task.sync(() => events.add('release')),
              )
              .flatMap(
                (_) => Task.fromCallback<int, Never>((_) {
                  started.complete();
                  return () => events.add('stop');
                }),
              )
              .scoped();
      final opening = Runtime.create(
        key.layer(acquire),
        signal: controller.signal,
        onUnhandledError: reports.add,
      );
      await started.future;
      controller.requestCancel(reason: 'stop startup');
      final exit = await opening.timeout(const Duration(seconds: 2));
      expect(_cause(exit), isA<CauseCancelled>());
      expect(events, ['stop', 'release']);
    },
  );

  test(
    'startup preserves separate cleanup occurrences with the same cause',
    () async {
      final controller = CancellationController();
      final started = Completer<void>();
      final defect = Cause<Never>.defect(Object(), StackTrace.current);
      final key = ServiceKey<int>('service');
      final opening = Runtime.create(
        key.layer(
          Task.acquireRelease(
            Task.succeed(1),
            release: (_, _) => Task.failCause(defect),
          ).flatMap(
            (_) => Task.fromCallback<int, Never>((_) {
              started.complete();
              return () {};
            }).ensuring(Task.failCause(defect)),
          ),
        ),
        signal: controller.signal,
        onUnhandledError: reports.add,
      );
      await started.future;
      controller.requestCancel(reason: 'cancel startup');
      final reasons = _reasons(_cause(await opening));
      expect(reasons.whereType<CauseDefect>(), [same(defect), same(defect)]);
      expect(
        reasons.whereType<CauseCancelled>().first.reason,
        'cancel startup',
      );
    },
  );

  test('startup cancellation drains nested local construction', () async {
    final controller = CancellationController();
    final started = Completer<void>();
    final inner = ServiceKey<int>('inner');
    final outer = ServiceKey<int>('outer');
    var stopped = false;
    final layer = outer.layer(
      Task.service(inner).provide(
        inner.layer(
          Task.fromCallback<int, Never>((_) {
            started.complete();
            return () {
              stopped = true;
            };
          }),
        ),
      ),
    );
    final opening = Runtime.create(
      layer,
      signal: controller.signal,
      onUnhandledError: reports.add,
    );
    await started.future;
    controller.requestCancel(reason: 'nested startup');
    final exit = await opening.timeout(const Duration(seconds: 2));
    expect((_cause(exit) as CauseCancelled).reason, 'nested startup');
    expect(stopped, isTrue);
  });

  test('parallel duplicate detection follows declaration order after out-of-order builds', () async {
    final first = ServiceKey<int>('first duplicate');
    final second = ServiceKey<int>('second duplicate');
    final gate = Completer<int>();
    final layers = [
      first.layer(
        Task.fromFuture(
          () => gate.future,
          onError: (e, s) => Error.throwWithStackTrace(e, s),
        ),
      ),
      Layer.value(first.bind(2)),
      Layer.value(second.bind(1)),
      Layer.value(second.bind(2)),
    ];
    final opening = Runtime.create(
      Layer.mergeAll(layers, concurrency: 4),
      onUnhandledError: reports.add,
    );
    await _turn();
    gate.complete(1);
    final errors = _reasons(_cause(await opening)).whereType<CauseDefect>();
    expect((errors.single.error as DuplicateService).key, same(first));
  });
}

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
Future<void> _turn() => Future<void>.delayed(Duration.zero);
Future<void> _until(bool Function() condition) async {
  for (var attempt = 0; attempt < 100 && !condition(); attempt++) {
    await _turn();
  }
  expect(condition(), isTrue);
}
