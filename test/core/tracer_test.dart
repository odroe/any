import 'dart:async';

import 'package:any/any.dart';
import 'package:any/testing.dart';
import 'package:test/test.dart';

void main() {
  Runtime setup(
    Tracer tracer, {
    Clock? clock,
    Logger? logger,
    RandomSource? random,
  }) {
    final runtime = Runtime(
      context: Context.from([
        Tracer.key.bind(tracer),
        if (clock != null) Clock.key.bind(clock),
        if (logger != null) Logger.key.bind(logger),
        if (random != null) RandomSource.key.bind(random),
      ]),
    );
    addTearDown(runtime.close);
    return runtime;
  }

  test(
    'spans are lazy and independent and discard successful values',
    () async {
      final records = <SpanRecord>[];
      final runtime = setup(Tracer(records.add));
      final task = Task.succeed(42).withSpan('read');
      expect(records, isEmpty);
      expect(await runtime.runOrThrow(task), 42);
      expect(await runtime.runOrThrow(task), 42);
      expect(records.map((record) => record.name), ['read', 'read']);
      expect(records.first.context.traceId, hasLength(32));
      expect(records.first.context.spanId, hasLength(16));
      expect(
        records.last.context.traceId,
        isNot(records.first.context.traceId),
      );
      expect(records.every((record) => record.parent == null), isTrue);
      expect(records.first.exit, isA<ExitSuccess<void, Object?>>());
    },
  );

  test('nested span parentage and logs restore to the outer span', () async {
    final records = <SpanRecord>[];
    final logs = <LogRecord>[];
    final runtime = setup(Tracer(records.add), logger: Logger(logs.add));
    await runtime.runOrThrow(
      Task.log('inner')
          .withSpan('inner')
          .flatMap((_) => Task.log('outer'))
          .withSpan('outer'),
    );
    await runtime.runOrThrow(Task.log('outside'));
    final [inner, outer] = records;
    expect(inner.parent, same(outer.context));
    expect(inner.context.traceId, outer.context.traceId);
    expect(inner.context.spanId, isNot(outer.context.spanId));
    expect(logs[0].spanId, inner.context.spanId);
    expect(logs[1].spanId, outer.context.spanId);
    expect(logs[0].traceId, outer.context.traceId);
    expect(logs[2].traceId, isNull);
    expect(await runtime.runOrThrow(Span.current()), isA<OptionNone<Span>>());
  });

  test('a span ends without closing resources acquired by its body', () async {
    final records = <SpanRecord>[];
    final runtime = setup(Tracer(records.add));
    var released = false;
    final resource = Object();
    final result = await runtime.runOrThrow(
      Task.acquireRelease(
        Task.succeed(resource),
        release: (_, _) => Task.sync(() => released = true),
      ).withSpan('acquire').map((value) {
        expect(released, isFalse);
        expect(records.single.exit, isA<ExitSuccess<void, Object?>>());
        return value;
      }),
    );

    expect(result, same(resource));
    expect(released, isTrue);
  });

  test(
    'cleanup outside the expression does not change the recorded exit',
    () async {
      final records = <SpanRecord>[];
      final runtime = setup(Tracer(records.add));
      final error = StateError('outer cleanup');
      final exit = await runtime.run(
        Task.addFinalizer((_) => Task.sync(() => throw error)).withSpan('body'),
      );

      expect(_causes(exit).whereType<CauseDefect>().single.error, same(error));
      expect(records.single.exit, isA<ExitSuccess<void, Object?>>());
    },
  );

  test(
    'an explicit remote parent overrides the locally active parent',
    () async {
      const remote = SpanContext(
        traceId: 'remote-trace',
        spanId: 'remote-span',
      );
      final records = <SpanRecord>[];
      final runtime = setup(Tracer(records.add));
      await runtime.runOrThrow(
        Task.succeed(1).withSpan('remote', parent: remote).withSpan('local'),
      );
      expect(records.first.parent, same(remote));
      expect(records.first.context.traceId, 'remote-trace');
      expect(records.last.parent, isNull);
    },
  );

  test(
    'span duration uses monotonic time while timestamps use wall time',
    () async {
      final clock = TestClock(start: DateTime.utc(2024));
      final records = <SpanRecord>[];
      final runtime = setup(Tracer(records.add), clock: clock);
      await runtime.runOrThrow(
        Task.sync(() {
          clock.advance(const Duration(seconds: 3));
          clock.setWallTime(DateTime.utc(2020));
        }).withSpan('wall change'),
      );
      expect(records.single.duration, const Duration(seconds: 3));
      expect(records.single.startedAt, DateTime.utc(2024));
      expect(records.single.endedAt, DateTime.utc(2020));
    },
  );

  test(
    'attributes copy at construction and bounded updates retain existing keys',
    () async {
      final records = <SpanRecord>[];
      final runtime = setup(
        Tracer(records.add, maxAttributes: 2, maxEvents: 1),
      );
      final attributes = <String, Object?>{'first': 1, 'second': 2};
      late Span retained;
      final task = _span()
          .flatMap((span) {
            retained = span;
            return span
                .setAttributes({'first': null, 'third': 3})
                .flatMap(
                  (_) => span.addEvent(
                    'one',
                    attributes: {'a': 1, 'b': 2, 'c': 3},
                  ),
                )
                .flatMap((_) => span.addEvent('two'));
          })
          .withSpan('bounded', attributes: attributes);
      attributes['first'] = 'changed';
      await runtime.runOrThrow(task);
      await runtime.runOrThrow(retained.setAttributes({'second': 'late'}));
      await runtime.runOrThrow(retained.addEvent('late'));
      final record = records.single;
      expect(record.attributes, {'first': null, 'second': 2});
      expect(record.events.single.name, 'one');
      expect(record.events.single.attributes, {'a': 1, 'b': 2});
      expect(record.droppedAttributes, 2);
      expect(record.droppedEvents, 1);
      expect(() => record.attributes.clear(), throwsUnsupportedError);
      expect(() => record.events.clear(), throwsUnsupportedError);
      expect(
        () => record.events.single.attributes.clear(),
        throwsUnsupportedError,
      );
    },
  );

  test(
    'zero capacities discard events and attributes without failing work',
    () async {
      final records = <SpanRecord>[];
      final runtime = setup(
        Tracer(records.add, maxAttributes: 0, maxEvents: 0),
      );
      await runtime.runOrThrow(
        _span()
            .flatMap((span) => span.addEvent('discarded'))
            .withSpan('zero', attributes: {'a': 1}),
      );
      expect(records.single.events, isEmpty);
      expect(records.single.attributes, isEmpty);
      expect(records.single.droppedEvents, 1);
      expect(records.single.droppedAttributes, 1);
      expect(() => Tracer(records.add, maxEvents: -1), throwsRangeError);
      expect(() => Tracer(records.add, maxAttributes: -1), throwsRangeError);
    },
  );

  test(
    'finalizers record events and their failures appear in the final span',
    () async {
      final records = <SpanRecord>[];
      final error = StateError('release');
      final stack = StackTrace.fromString('release stack');
      final runtime = setup(Tracer(records.add));
      final exit = await runtime.run(
        Task.addFinalizer(
              (_) => _span()
                  .flatMap((span) => span.addEvent('cleanup'))
                  .flatMap((_) => Task.failCause(Cause.defect(error, stack))),
            )
            .withError<String>()
            .flatMap((_) => Task.fail('business'))
            .scoped()
            .withSpan('fails'),
      );
      expect(exit, isA<ExitFailure<Never, String>>());
      final record = records.single;
      expect(record.events.single.name, 'cleanup');
      final causes = _causes(record.exit);
      expect(causes, hasLength(2));
      expect(
        causes[0],
        isA<CauseFailure<String>>().having((c) => c.error, 'error', 'business'),
      );
      expect(
        causes[1],
        isA<CauseDefect>()
            .having((c) => c.error, 'error', same(error))
            .having((c) => c.stackTrace, 'stack', same(stack)),
      );
    },
  );

  test('cancellation waits for cleanup and records its defect once', () async {
    final records = <SpanRecord>[];
    final runtime = setup(Tracer(records.add));
    final ready = Completer<void>();
    final cleaning = Completer<void>();
    final gate = Completer<void>();
    final error = StateError('cleanup');
    final controller = CancellationController();
    final running = runtime.run(
      Task.addFinalizer(
            (_) =>
                Task.sync(cleaning.complete)
                    .flatMap((_) => _wait(gate.future))
                    .flatMap((_) => Task.sync<void>(() => throw error)),
          )
          .flatMap(
            (_) => Task.fromCallback<void, Never>((complete) {
              ready.complete();
              return () {};
            }),
          )
          .scoped()
          .withSpan('cancel'),
      signal: controller.signal,
    );
    await ready.future;
    controller.requestCancel(reason: 'stop');
    await cleaning.future;
    expect(records, isEmpty);
    gate.complete();
    final exit = await running;
    expect(_causes(exit).whereType<CauseDefect>().map((c) => c.error), [
      same(error),
    ]);
    expect(records, hasLength(1));
    expect(
      _causes(records.single.exit).whereType<CauseCancelled>(),
      hasLength(1),
    );
    expect(
      _causes(records.single.exit).whereType<CauseCancelled>().single.reason,
      'stop',
    );
    expect(
      _causes(records.single.exit).whereType<CauseDefect>().map((c) => c.error),
      [same(error)],
    );
  });

  test('an enclosing mask remains in force inside a traced child', () async {
    final records = <SpanRecord>[];
    final runtime = setup(Tracer(records.add));
    final ready = Completer<void>();
    final gate = Completer<void>();
    final controller = CancellationController();
    var continued = false;
    final running = runtime.run(
      Task.uninterruptibleMask<void, Never>(
        (_) =>
            Task.sync(ready.complete)
                .flatMap((_) => _wait(gate.future))
                .flatMap((_) => Task.sync(() => continued = true))
                .map((_) {})
                .withSpan('masked'),
      ),
      signal: controller.signal,
    );
    await ready.future;
    controller.requestCancel();
    gate.complete();
    await running;
    expect(continued, isTrue);
    expect(records.single.exit, isA<ExitSuccess<void, Object?>>());
  });

  test('backend failure cannot change success or business failure', () async {
    final reported = <Object>[];
    final backend = StateError('output');
    final runtime = setup(
      Tracer(
        (_) async => throw backend,
        onError: (error, _) => reported.add(error),
      ),
    );
    expect(await runtime.runOrThrow(Task.succeed(5).withSpan('success')), 5);
    final exit = await runtime.run(Task.fail('business').withSpan('failure'));
    expect(
      _causes(exit).single,
      isA<CauseFailure<String>>().having((c) => c.error, 'error', 'business'),
    );
    expect(reported, [same(backend), same(backend)]);
  });

  test(
    'flush is an ordered barrier and failures preserve subsequent output',
    () async {
      final events = <String>[];
      final reported = <Object>[];
      final gate = Completer<void>();
      final started = Completer<void>();
      final flushError = StateError('flush');
      final tracer = Tracer(
        (record) async {
          events.add(record.name);
          if (record.name == 'first') {
            started.complete();
            await gate.future;
          }
        },
        flush: () {
          events.add('flush');
          throw flushError;
        },
        onError: (error, _) => reported.add(error),
      );
      final runtime = setup(tracer);
      final first = runtime.run(Task.succeed(1).withSpan('first'));
      await started.future;
      final flushing = tracer.flush();
      final second = runtime.run(Task.succeed(2).withSpan('second'));
      expect(events, ['first']);
      gate.complete();
      await Future.wait([first, flushing, second]);
      expect(events, ['first', 'flush', 'second']);
      expect(reported, [same(flushError)]);
    },
  );

  test(
    'a no-op tracer preserves the caller scope without reading clocks',
    () async {
      final clock = _FaultClock();
      final runtime = setup(
        Tracer.noop(),
        clock: clock,
        random: _FaultRandom(),
      );
      var released = false;
      await runtime.runOrThrow(
        Task.addFinalizer((_) => Task.sync(() => released = true).map((_) {}))
            .withSpan('noop')
            .flatMap((_) => Task.sync(() => expect(released, isFalse))),
      );
      expect(released, isTrue);
      expect(clock.calls, 0);
      expect(await runtime.runOrThrow(Span.current()), isA<OptionNone<Span>>());
    },
  );

  test('the default runtime supplies a no-op tracer', () async {
    final runtime = Runtime();
    addTearDown(runtime.close);
    expect(await runtime.runOrThrow(Task.succeed(7).withSpan('default')), 7);
    await (await runtime.runOrThrow(Task.service(Tracer.key))).flush();
  });

  test(
    'clock and random failures report independently and still run work',
    () async {
      for (final clockFails in [true, false]) {
        final reported = <Object>[];
        final records = <SpanRecord>[];
        final runtime = setup(
          Tracer(records.add, onError: (error, _) => reported.add(error)),
          clock: clockFails ? _FaultClock() : null,
          random: clockFails ? null : _FaultRandom(),
        );
        expect(
          await runtime.runOrThrow(Task.succeed(9).withSpan('broken provider')),
          9,
        );
        expect(records, isEmpty);
        expect(reported.single, isA<StateError>());
      }
    },
  );

  test(
    'clock faults at end still deliver one record with a safe duration',
    () async {
      final clock = _FaultClock()..fails = false;
      final records = <SpanRecord>[];
      final reported = <Object>[];
      final runtime = setup(
        Tracer(records.add, onError: (error, _) => reported.add(error)),
        clock: clock,
      );
      expect(
        await runtime.runOrThrow(
          Task.sync(() {
            clock.fails = true;
            return 3;
          }).withSpan('end fault'),
        ),
        3,
      );
      expect(records.single.duration, Duration.zero);
      expect(records.single.exit, isA<ExitSuccess<void, Object?>>());
      expect(reported, hasLength(1));
    },
  );

  test(
    'forks inherit the same active span and nested spans remain separate',
    () async {
      final records = <SpanRecord>[];
      final runtime = setup(Tracer(records.add));
      final seen = <Span>[];
      final child = _span().flatMap((span) => Task.sync(() => seen.add(span)));
      await runtime.runOrThrow(
        Task.all(
          [child, child],
          concurrency: 2,
        ).flatMap((_) => child.withSpan('nested')).withSpan('parent'),
      );
      expect(seen[0], same(seen[1]));
      expect(seen[2], isNot(same(seen[0])));
      expect(records.map((r) => r.name), ['nested', 'parent']);
    },
  );

  test('traced callbacks keep a widened business error type', () async {
    final runtime = setup(Tracer((_) {}));
    final Task<int, String> widened = Task.succeed(1);
    final exit = await runtime.run(
      widened.withSpan('widened').flatMap((_) => Task.fail('later')),
    );
    expect(_causes(exit).single, isA<CauseFailure<String>>());
  });

  test(
    'span output uses an independent creation-zone error reporter',
    () async {
      final errors = <Object>[];
      final backend = StateError('backend');
      final reporter = StateError('reporter');
      late Tracer tracer;
      runZoned(
        () {
          tracer = Tracer(
            (_) => throw backend,
            onError: (error, _) {
              expect(Zone.current[#origin], 'creation');
              expect(error, same(backend));
              throw reporter;
            },
          );
        },
        zoneValues: {#origin: 'creation'},
        zoneSpecification: ZoneSpecification(
          handleUncaughtError: (self, parent, zone, error, stack) =>
              errors.add(error),
        ),
      );
      final runtime = setup(tracer);
      expect(await runtime.runOrThrow(Task.succeed(1).withSpan('report')), 1);
      expect(errors, [same(reporter)]);
    },
  );
}

Task<Span, Never> _span() =>
    Span.current().map((value) => (value as OptionSome<Span>).value);
Task<void, Never> _wait(Future<void> future) =>
    Task.fromFuture(() => future, onError: Error.throwWithStackTrace);
List<Cause<Object?>> _causes(Exit<Object?, Object?> exit) => switch (exit) {
  ExitSuccess() => const [],
  ExitFailure(cause: CauseGroup(:final causes)) => causes,
  ExitFailure(:final cause) => [cause],
};

final class _FaultClock implements Clock {
  bool fails = true;
  int calls = 0;
  @override
  DateTime now() {
    calls++;
    if (fails) throw StateError('clock');
    return DateTime.utc(2024);
  }

  @override
  Duration get elapsed => Duration.zero;
  @override
  void Function() schedule(Duration delay, void Function() callback) =>
      throw UnimplementedError();
}

final class _FaultRandom implements RandomSource {
  @override
  int nextInt(int max) => throw StateError('random');
  @override
  bool nextBool() => throw UnimplementedError();
  @override
  double nextDouble() => throw UnimplementedError();
}
