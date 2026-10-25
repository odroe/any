import 'dart:async';

import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  final reports = <Cause<Object?>>[];
  setUp(reports.clear);
  tearDown(() async {
    await _turn();
    expect(reports, isEmpty);
  });

  Future<Exit<Runtime, E>> create<E>(
    Layer<E> layer, {
    Context? context,
    CancellationSignal? signal,
  }) => Runtime.create(
    layer,
    context: context,
    signal: signal,
    onUnhandledError: reports.add,
  );

  test(
    'value supplies external services without taking their lifetime',
    () async {
      final key = ServiceKey<Object?>('value');
      final runtime = _value(await create(Layer.value(key.bind(null))));
      expect(_value(await runtime.run(Task.service(key))), isNull);
      expect(_value(await runtime.run(Task.service(Clock.key))), isA<Clock>());
      expect(await runtime.close(), isA<ExitSuccess<void, Never>>());
    },
  );

  test(
    'service construction is lazy and each application owns its resources',
    () async {
      final key = ServiceKey<Object>('resource');
      var acquired = 0;
      var released = 0;
      final layer = key.layer(
        Task.acquireRelease(
          Task.sync(() {
            acquired++;
            return Object();
          }),
          release: (_, _) => Task.sync(() => released++),
        ),
      );
      expect(acquired, 0);
      final first = _value(await create(layer));
      final second = _value(await create(layer));
      final a = _value(await first.run(Task.service(key)));
      final b = _value(await second.run(Task.service(key)));
      expect(identical(a, b), isFalse);
      expect(acquired, 2);
      expect(released, 0);
      expect(_value(await first.run(Task.service(key))), same(a));
      await first.close();
      expect(released, 1);
      await second.close();
      expect(released, 2);
    },
  );

  for (final concurrency in [1, 2]) {
    test(
      'diamond dependency shares acquisition with concurrency $concurrency',
      () async {
        final dependency = ServiceKey<Object>('dependency');
        final left = ServiceKey<Object>('left');
        final right = ServiceKey<Object>('right');
        var acquired = 0;
        var released = 0;
        final shared = dependency.layer(
          Task.acquireRelease(
            Task.sync(() {
              acquired++;
              return Object();
            }),
            release: (_, _) => Task.sync(() => released++),
          ),
        );
        final graph = left
            .layer(Task.service(dependency))
            .provide(shared)
            .merge(
              right.layer(Task.service(dependency)).provide(shared),
              concurrency: concurrency,
            );
        final runtime = _value(await create(graph));
        expect(
          _value(await runtime.run(Task.service(left))),
          same(_value(await runtime.run(Task.service(right)))),
        );
        expect(acquired, 1);
        expect(
          _defects(_cause(await runtime.run(Task.service(dependency))))
              .single
              .error,
          isA<MissingService>(),
        );
        await runtime.close();
        expect(released, 1);
      },
    );
  }

  test(
    'dependencies release after their users and retain build context',
    () async {
      final logger = ServiceKey<String>('logger');
      final database = ServiceKey<String>('database');
      final events = <String>[];
      final logs = logger.layer(
        Task.acquireRelease(
          Task.succeed('logger'),
          release: (_, _) => Task.sync(() => events.add('release logger')),
        ),
      );
      final db = database
          .layer(
            Task.acquireRelease(
              Task.service(logger).map((value) {
                events.add('acquire with $value');
                return 'db';
              }),
              release: (_, _) =>
                  Task.service(logger)
                      .map((value) => events.add('release db with $value')),
            ),
          )
          .provide(logs);
      final runtime = _value(await create(db));
      expect(events, ['acquire with logger']);
      await runtime.close();
      expect(events, [
        'acquire with logger',
        'release db with logger',
        'release logger',
      ]);
    },
  );

  test(
    'identity cache uses the first input and fresh isolates a subtree',
    () async {
      final input = ServiceKey<String>('input');
      final dependency = ServiceKey<String>('dependency');
      final a = ServiceKey<String>('a');
      final b = ServiceKey<String>('b');
      var builds = 0;
      final shared = dependency.layer(
        Task.service(input).map((value) {
          builds++;
          return value;
        }),
      );
      Layer<Never> branch(
        ServiceKey<String> key,
        String value,
        Layer<Never> base,
      ) => key
          .layer(Task.service(dependency))
          .provide(base.provide(Layer.value(input.bind(value))));
      final first = _value(
        await create(
          branch(a, 'first', shared).merge(branch(b, 'second', shared)),
        ),
      );
      expect(_value(await first.run(Task.service(a))), 'first');
      expect(_value(await first.run(Task.service(b))), 'first');
      expect(builds, 1);
      await first.close();
      final second = _value(
        await create(
          branch(a, 'first', shared).merge(branch(b, 'second', shared.fresh())),
        ),
      );
      expect(_value(await second.run(Task.service(a))), 'first');
      expect(_value(await second.run(Task.service(b))), 'second');
      expect(builds, 3);
      await second.close();
    },
  );

  test(
    'each encounter of one fresh wrapper acquires an independent subtree',
    () async {
      final dependency = ServiceKey<Object>('dependency');
      final a = ServiceKey<Object>('a');
      final b = ServiceKey<Object>('b');
      var acquired = 0;
      var released = 0;
      final fresh = dependency
          .layer(
            Task.acquireRelease(
              Task.sync(() {
                acquired++;
                return Object();
              }),
              release: (_, _) => Task.sync(() => released++),
            ),
          )
          .fresh();
      final runtime = _value(
        await create(
          a
              .layer(Task.service(dependency))
              .provide(fresh)
              .merge(
                b.layer(Task.service(dependency)).provide(fresh),
                concurrency: 2,
              ),
        ),
      );
      expect(acquired, 2);
      expect(
        identical(
          _value(await runtime.run(Task.service(a))),
          _value(await runtime.run(Task.service(b))),
        ),
        isFalse,
      );
      await runtime.close();
      expect(released, 2);
    },
  );

  test(
    'merge snapshots its inputs and an empty merge retains default services',
    () async {
      final key = ServiceKey<int>('input');
      final inputs = <Layer<Never>>[Layer.value(key.bind(1))];
      final merged = Layer.mergeAll(inputs);
      inputs.clear();
      final first = _value(await create(merged));
      expect(_value(await first.run(Task.service(key))), 1);
      await first.close();
      final empty = _value(await create(Layer.mergeAll<Never>([])));
      expect(_value(await empty.run(Task.service(Clock.key))), isA<Clock>());
      await empty.close();
    },
  );

  test(
    'output replacement keeps resources and existing dependants alive',
    () async {
      final port = ServiceKey<int>('port');
      final clientKey = ServiceKey<({int port})>('client');
      final released = <int>[];
      var builds = 0;
      Layer<Never> item(int value) => port.layer(
        Task.acquireRelease(
          Task.succeed(value),
          release: (_, _) => Task.sync(() => released.add(value)),
        ),
      );
      final live = item(443);
      final testing = item(8080);
      final client = clientKey.layer(
        Task.service(port).map((value) {
          builds++;
          return (port: value);
        }),
      );
      final runtime = _value(
        await create(client.provide(live).merge(live).merge(testing)),
      );
      expect(_value(await runtime.run(Task.service(port))), 8080);
      expect(_value(await runtime.run(Task.service(clientKey))).port, 443);
      expect(builds, 1);
      expect(released, isEmpty);
      await runtime.close();
      expect(released, [8080, 443]);

      final testRuntime = _value(
        await create(client.provide(testing).merge(testing)),
      );
      expect(_value(await testRuntime.run(Task.service(port))), 8080);
      expect(_value(await testRuntime.run(Task.service(clientKey))).port, 8080);
      expect(builds, 2);
      expect(released, [8080, 443]);
      await testRuntime.close();
      expect(released, [8080, 443, 8080]);
    },
  );

  test(
    'repeating an identical output shares one successful construction',
    () async {
      final key = ServiceKey<int>('shared');
      var builds = 0;
      final shared = key.layer(Task.sync(() => ++builds));
      final runtime = _value(
        await create(shared.merge(shared, concurrency: 2)),
      );
      expect(_value(await runtime.run(Task.service(key))), 1);
      expect(builds, 1);
      await runtime.close();
    },
  );

  test(
    'nested layer provision uses the innermost bindings and keeps other inputs',
    () async {
      final input = ServiceKey<int>('input');
      final inherited = ServiceKey<String>('inherited');
      final output = ServiceKey<(int, String)>('output');
      final layer = output
          .layer(
            Task.service(input).flatMap(
              (value) => Task.service(inherited).map((other) => (value, other)),
            ),
          )
          .provide(Layer.value(input.bind(2)))
          .provide(Layer.value(input.bind(3)));
      final runtime = _value(
        await create(
          layer,
          context: Context.from([input.bind(1), inherited.bind('kept')]),
        ),
      );
      expect(_value(await runtime.run(Task.service(output))), (2, 'kept'));
      expect(_value(await runtime.run(Task.service(input))), 1);
      await runtime.close();
    },
  );

  test('merged siblings do not implicitly provide dependencies', () async {
    final a = ServiceKey<int>('a');
    final b = ServiceKey<int>('b');
    final exit = await create(
      Layer.value(a.bind(1)).merge(b.layer(Task.service(a))),
    );
    expect(
      (_defects(_cause(exit)).single.error as MissingService).key,
      same(a),
    );
  });

  test('invalid concurrency fails lazily before starting any branch', () async {
    var calls = 0;
    final key = ServiceKey<int>('value');
    final layer = Layer.mergeAll([
      key.layer(Task.sync(() => ++calls)),
    ], concurrency: 0);
    expect(calls, 0);
    expect(
      _defects(_cause(await create(layer))).single.error,
      isA<RangeError>(),
    );
    expect(calls, 0);
  });

  test(
    'mapError transforms business failures and callbacks throw defects',
    () async {
      final key = ServiceKey<int>('value');
      final failed = key.layer(Task.fail('failed'));
      expect(
        (_cause(
          await create(failed.mapError((value) => value.length)),
        ) as CauseFailure<int>).error,
        6,
      );
      final error = StateError('mapping');
      expect(
        _defects(_cause(await create(failed.mapError<int>((_) => throw error))))
            .single
            .error,
        same(error),
      );
    },
  );

  test('widened infallible layers compose with fallible layers', () async {
    final a = ServiceKey<int>('a');
    final b = ServiceKey<int>('b');
    final Layer<Object?> widened = Layer.value(a.bind(1));
    final failed = b.layer(Task.fail<Object?>('failure'));
    final exit = await create(widened.merge(failed));
    expect((_cause(exit) as CauseFailure<Object?>).error, 'failure');
    final provided = b
        .layer(Task.service(a))
        .withError<Object?>()
        .provide(widened);
    final runtime = _value(
      await create(provided.mapError((error) => '$error')),
    );
    expect(_value(await runtime.run(Task.service(b))), 1);
    await runtime.close();
  });

  test(
    'widened service keys reject a mismatched acquired value and release it',
    () async {
      final narrow = ServiceKey<int>('int');
      final ServiceKey<Object> widened = narrow;
      var released = false;
      final layer = widened.layer(
        Task.acquireRelease(
          Task.succeed<Object>('wrong'),
          release: (_, _) => Task.sync(() => released = true),
        ),
      );
      expect(
        _defects(_cause(await create(layer))).single.error,
        isA<TypeError>(),
      );
      expect(released, isTrue);
    },
  );

  test('failed construction retains its cause and cleanup defects', () async {
    final first = ServiceKey<int>('first');
    final failed = ServiceKey<int>('failed');
    final error = StateError('cleanup');
    final trace = StackTrace.fromString('cleanup trace');
    Exit<Object?, Object?>? closingExit;
    final acquired = first
        .layer(
          Task.acquireRelease(
            Task.succeed(1),
            release: (_, exit) => Task.sync(() {
              closingExit = exit;
              Error.throwWithStackTrace(error, trace);
            }),
          ),
        )
        .withError<String>();
    final exit = await create(
      acquired.merge(failed.layer(Task.fail('startup'))),
    );
    final reasons = (_cause(exit) as CauseGroup<String>).causes;
    expect((reasons.first as CauseFailure<String>).error, 'startup');
    expect((reasons.last as CauseDefect).error, same(error));
    expect(
      (reasons.last as CauseDefect).stackTrace.toString(),
      trace.toString(),
    );
    expect((_cause(closingExit!) as CauseFailure<Object?>).error, 'startup');
  });

  test('pre-cancelled startup never begins acquiring services', () async {
    final controller = CancellationController()
      ..requestCancel(reason: 'before');
    var started = false;
    final key = ServiceKey<int>('value');
    final exit = await create(
      key.layer(
        Task.sync(() {
          started = true;
          return 1;
        }),
      ),
      signal: controller.signal,
    );
    expect((_cause(exit) as CauseCancelled).reason, 'before');
    expect(started, isFalse);
  });

  test(
    'startup cancellation waits for masked acquisition and releases its result',
    () async {
      final controller = CancellationController();
      final started = Completer<void>();
      final acquired = Completer<int>();
      var released = false;
      final key = ServiceKey<int>('resource');
      final opening = create(
        key.layer(
          Task.acquireRelease(
            Task.fromFuture(
              () {
                started.complete();
                return acquired.future;
              },
              onError: (error, stack) =>
                  Error.throwWithStackTrace(error, stack),
            ),
            release: (_, _) => Task.sync(() => released = true),
          ),
        ),
        signal: controller.signal,
      );
      await started.future;
      controller.requestCancel(reason: 'startup');
      var finished = false;
      unawaited(opening.then((_) => finished = true));
      await _turn();
      expect(finished, isFalse);
      expect(released, isFalse);
      acquired.complete(1);
      expect(
        _reasons(_cause(await opening))
            .whereType<CauseCancelled>()
            .first
            .reason,
        'startup',
      );
      expect(released, isTrue);
    },
  );

  test(
    'cancellation during acquisition cannot skip release registration',
    () async {
      final controller = CancellationController();
      final key = ServiceKey<int>('resource');
      var released = 0;
      final exit = await create(
        key.layer(
          Task.acquireRelease(
            Task.sync(() {
              controller.requestCancel(reason: 'during acquire');
              return 1;
            }),
            release: (_, _) => Task.sync(() => released++),
          ),
        ),
        signal: controller.signal,
      );
      expect(
        _reasons(_cause(exit)).whereType<CauseCancelled>().first.reason,
        'during acquire',
      );
      expect(released, 1);
    },
  );

  test('startup signal no longer controls a returned runtime', () async {
    final controller = CancellationController();
    final key = ServiceKey<int>('value');
    final runtime = _value(
      await create(Layer.value(key.bind(1)), signal: controller.signal),
    );
    controller.requestCancel();
    expect(_value(await runtime.run(Task.service(key))), 1);
    await runtime.close();
  });

  test(
    'runtime closes active requests and background work before service release',
    () async {
      final key = ServiceKey<int>('resource');
      final events = <String>[];
      final acquire =
          Task.acquireRelease(
            Task.succeed(1),
            release: (_, _) => Task.sync(() => events.add('resource')),
          ).flatMap(
            (value) => Scope.open().flatMap(
              (scope) =>
                  Task.fromCallback<void, Never>(
                        (_) =>
                            () => events.add('background stopped'),
                      )
                      .ensuring(
                        Task.sync(() => events.add('background cleanup')),
                      )
                      .forkIn(scope)
                      .map((_) => value),
            ),
          );
      final runtime = _value(await create(key.layer(acquire)));
      expect(events, isEmpty);
      final request = runtime.run(
        Task.service(key).flatMap(
          (_) => Task.fromCallback<void, Never>(
            (_) =>
                () => events.add('request stopped'),
          ).ensuring(Task.sync(() => events.add('request cleanup'))),
        ),
      );
      await runtime.close();
      expect(_cause(await request), isA<CauseCancelled>());
      expect(events.last, 'resource');
      expect(
        events.sublist(0, 4),
        unorderedEquals([
          'background stopped',
          'background cleanup',
          'request stopped',
          'request cleanup',
        ]),
      );
      expect(
        _defects(_cause(await runtime.run(Task.service(key)))).single.error,
        isA<RuntimeClosed>(),
      );
      expect(await runtime.close(), isA<ExitSuccess<void, Never>>());
    },
  );

  test(
    'provided resources remain available after bindings are restored',
    () async {
      final key = ServiceKey<Object>('resource');
      final resource = Object();
      var released = false;
      final runtime = Runtime(onUnhandledError: reports.add);
      final layer = key.layer(
        Task.acquireRelease(
          Task.succeed(resource),
          release: (_, _) => Task.sync(() => released = true),
        ),
      );

      final result = await runtime.runOrThrow(
        Task.service(key).provide(layer).map((value) {
          expect(released, isFalse);
          return value;
        }),
      );
      expect(result, same(resource));
      expect(released, isTrue);
      expect(await runtime.close(), isA<ExitSuccess<void, Never>>());
    },
  );

  test(
    'explicitly scoped provision releases services before continuing',
    () async {
      final key = ServiceKey<int>('scoped');
      var released = false;
      final runtime = Runtime(onUnhandledError: reports.add);
      final layer = key.layer(
        Task.acquireRelease(
          Task.succeed(1),
          release: (_, _) => Task.sync(() => released = true),
        ),
      );

      expect(
        await runtime.runOrThrow(
          Task.service(key).provide(layer).scoped().map((value) {
            expect(released, isTrue);
            return value;
          }),
        ),
        1,
      );
      expect(await runtime.close(), isA<ExitSuccess<void, Never>>());
    },
  );

  test('local provision inherits services, restores inputs, and releases after children', () async {
    final key = ServiceKey<int>('local');
    final inherited = ServiceKey<String>('inherited');
    final runtime = Runtime(
      context: Context.from([key.bind(1), inherited.bind('parent')]),
      onUnhandledError: reports.add,
    );
    final events = <String>[];
    final layer = key.layer(
      Task.acquireRelease(
        Task.succeed(2),
        release: (_, _) => Task.sync(() => events.add('release')),
      ),
    );
    final local = Task.service(key).flatMap(
      (value) => Task.service(inherited).flatMap(
        (parent) => Task.fromCallback<void, Never>(
          (_) =>
              () => events.add('child stopped'),
        ).fork().map((_) => (value, parent)),
      ),
    );
    final result = _value(
      await runtime.run(
        local
            .provide(layer)
            .flatMap(
              (inside) => Task.service(key).map((outside) => (inside, outside)),
            ),
      ),
    );
    expect(result, ((2, 'parent'), 1));
    expect(events, ['child stopped', 'release']);
    await runtime.close();
  });

  test(
    'local construction failure leaves partial resources in the caller scope',
    () async {
      final runtime = Runtime(onUnhandledError: reports.add);
      final a = ServiceKey<int>('a');
      final b = ServiceKey<int>('b');
      var bodyRan = false;
      var released = false;
      final layer = a
          .layer(
            Task.acquireRelease(
              Task.succeed(1),
              release: (_, _) => Task.sync(() => released = true),
            ),
          )
          .withError<String>()
          .merge(b.layer(Task.fail('failed')));
      final exit = await runtime.run(
        Task.sync(() => bodyRan = true)
            .withError<String>()
            .provide(layer)
            .onExit((_) => Task.sync(() => expect(released, isFalse))),
      );
      expect((_cause(exit) as CauseFailure<String>).error, 'failed');
      expect(bodyRan, isFalse);
      expect(released, isTrue);
      await runtime.close();
    },
  );

  test('a thousand nested layer dependencies are stack safe', () async {
    Layer<Never> layer = Layer.mergeAll([]);
    for (var depth = 0; depth < 1000; depth++) {
      layer = Layer.mergeAll<Never>([]).provide(layer);
    }
    final runtime = _value(
      await create(layer).timeout(const Duration(seconds: 5)),
    );
    expect(await runtime.close(), isA<ExitSuccess<void, Never>>());
  });

  test(
    'immediate startup cancellation prevents deferred acquisition',
    () async {
      final controller = CancellationController();
      final key = ServiceKey<int>('not started');
      var started = false;
      final opening = create(
        key.layer(
          Task.sync(() {
            started = true;
            return 1;
          }),
        ),
        signal: controller.signal,
      );
      controller.requestCancel(reason: 'before worker start');
      expect(
        (_cause(await opening) as CauseCancelled).reason,
        'before worker start',
      );
      expect(started, isFalse);
    },
  );

  test(
    'local builds and task-local values are independent per execution',
    () async {
      final runtime = Runtime(onUnhandledError: reports.add);
      final local = TaskLocal(0);
      final service = ServiceKey<int>('local value');
      var builds = 0;
      final layer = service.layer(
        local.get().map((value) {
          builds++;
          return value;
        }),
      );
      final task = Task.service(service).provide(layer);
      expect(_value(await runtime.run(local.locally(1, task))), 1);
      expect(_value(await runtime.run(local.locally(2, task))), 2);
      expect(builds, 2);
      await runtime.close();
    },
  );
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
List<CauseDefect> _defects(Cause<Object?> cause) =>
    _reasons(cause).whereType<CauseDefect>().toList();
Future<void> _turn() => Future<void>.delayed(Duration.zero);
