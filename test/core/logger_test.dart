import 'dart:async';

import 'package:any/any.dart';
import 'package:any/testing.dart';
import 'package:test/test.dart';

void main() {
  Runtime runtimeWith(Logger logger, {Clock? clock}) {
    final runtime = Runtime(
      context: Context.from([
        Logger.key.bind(logger),
        if (clock != null) Clock.key.bind(clock),
      ]),
    );
    addTearDown(() async {
      expect(await runtime.close(), isA<ExitSuccess<void, Never>>());
    });
    return runtime;
  }

  test('records normalize time and copy only the field map', () {
    final nested = <String>['original'];
    final fields = <String, Object?>{'nested': nested, 'nullable': null};
    final timestamp = DateTime(2024, 2, 3, 4, 5);
    final record = LogRecord(
      level: LogLevel.warning,
      timestamp: timestamp,
      message: 'message',
      fields: fields,
      traceId: 'trace',
      spanId: 'span',
    );
    fields['added'] = true;
    nested.add('retained');

    expect(record.timestamp, timestamp.toUtc());
    expect(record.timestamp.isUtc, isTrue);
    expect(record.fields, {'nested': nested, 'nullable': null});
    expect(record.fields['nested'], same(nested));
    expect(record.traceId, 'trace');
    expect(record.spanId, 'span');
    expect(() => record.fields['change'] = true, throwsUnsupportedError);
  });

  test('minimum severity is inclusive and defaults to info', () async {
    final records = <LogRecord>[];
    final logger = Logger(records.add);
    for (final level in LogLevel.values) {
      await logger.write(_record('$level', level: level));
    }
    expect(records.map((record) => record.level), [
      LogLevel.info,
      LogLevel.warning,
      LogLevel.error,
      LogLevel.fatal,
    ]);
    final trace = Logger(records.add, minLevel: LogLevel.trace);
    await trace.write(_record('trace', level: LogLevel.trace));
    expect(records.last.level, LogLevel.trace);
  });

  test('writes and flushes are barriers in invocation order', () async {
    final firstStarted = Completer<void>();
    final firstGate = Completer<void>();
    final flushStarted = Completer<void>();
    final flushGate = Completer<void>();
    final events = <String>[];
    final logger = Logger(
      (record) async {
        events.add('${record.message} start');
        if (record.message == 'first') {
          firstStarted.complete();
          await firstGate.future;
        }
        events.add('${record.message} end');
      },
      flush: () async {
        events.add('flush start');
        flushStarted.complete();
        await flushGate.future;
        events.add('flush end');
      },
    );
    final first = logger.write(_record('first'));
    final flush = logger.flush();
    final last = logger.write(_record('last'));
    await firstStarted.future;
    expect(events, ['first start']);
    firstGate.complete();
    await first;
    await flushStarted.future;
    expect(events, ['first start', 'first end', 'flush start']);
    flushGate.complete();
    await flush;
    await last;
    expect(events, [
      'first start',
      'first end',
      'flush start',
      'flush end',
      'last start',
      'last end',
    ]);
  });

  test('flush without a backend callback still waits for writes', () async {
    final started = Completer<void>();
    final gate = Completer<void>();
    final logger = Logger((_) {
      started.complete();
      return gate.future;
    });
    final writing = logger.write(_record('pending'));
    var flushed = false;
    final flushing = logger.flush().then((_) => flushed = true);
    await started.future;
    expect(flushed, isFalse);
    gate.complete();
    await writing;
    await flushing;
    expect(flushed, isTrue);
  });

  test(
    'sync and async failures report original errors and preserve the queue',
    () async {
      final syncError = StateError('sync output');
      final asyncError = StateError('async output');
      final flushError = StateError('flush');
      final syncStack = StackTrace.fromString('sync stack');
      final asyncStack = StackTrace.fromString('async stack');
      final reported = <(Object, StackTrace)>[];
      final delivered = <String>[];
      final logger = Logger(
        (record) {
          switch (record.message) {
            case 'sync':
              Error.throwWithStackTrace(syncError, syncStack);
            case 'async':
              return Future<void>.error(asyncError, asyncStack);
            default:
              delivered.add(record.message);
          }
        },
        flush: () => throw flushError,
        onError: (error, stackTrace) => reported.add((error, stackTrace)),
      );

      await logger.write(_record('sync'));
      await logger.write(_record('async'));
      await logger.flush();
      await logger.write(_record('after'));
      expect(reported.map((entry) => entry.$1), [
        same(syncError),
        same(asyncError),
        same(flushError),
      ]);
      expect(reported[0].$2.toString(), syncStack.toString());
      expect(reported[1].$2.toString(), asyncStack.toString());
      expect(delivered, ['after']);
    },
  );

  test(
    'backend errors go to the creation zone from a different caller zone',
    () async {
      final reported = <Object>[];
      final error = StateError('backend');
      late Logger logger;
      runZoned(
        () => logger = Logger((_) async => throw error),
        zoneSpecification: ZoneSpecification(
          handleUncaughtError: (self, parent, zone, error, stack) {
            reported.add(error);
          },
        ),
      );

      await logger.write(_record('outside'));
      expect(reported, [same(error)]);
    },
  );

  test(
    'onError uses the creation zone and cannot poison later writes',
    () async {
      final backendError = StateError('backend');
      final reporterError = StateError('reporter');
      final reported = <Object>[];
      final zones = <Object?>[];
      var calls = 0;
      late Logger logger;
      runZoned(
        () {
          logger = Logger(
            (_) {
              if (++calls == 1) throw backendError;
            },
            onError: (error, stack) {
              expect(error, same(backendError));
              zones.add(Zone.current[#logger]);
              throw reporterError;
            },
          );
        },
        zoneValues: {#logger: 'creation'},
        zoneSpecification: ZoneSpecification(
          handleUncaughtError: (self, parent, zone, error, stack) {
            reported.add(error);
          },
        ),
      );

      await logger.write(_record('fails'));
      await logger.write(_record('succeeds'));
      expect(zones, ['creation']);
      expect(reported, [same(reporterError)]);
      expect(calls, 2);
    },
  );

  test(
    'combined outputs are copied, ordered, independently filtered and flushed',
    () async {
      final events = <String>[];
      final errors = <Object>[];
      final error = StateError('first output');
      final flushError = StateError('first flush');
      final first = Logger(
        (_) {
          events.add('first');
          throw error;
        },
        onError: (error, stack) => errors.add(error),
        flush: () {
          events.add('first flush');
          throw flushError;
        },
      );
      final second = Logger(
        (_) => events.add('second'),
        minLevel: LogLevel.warning,
        flush: () => events.add('second flush'),
      );
      final outputs = [first, second];
      final combined = Logger.combine(outputs, minLevel: LogLevel.info);
      outputs.clear();

      await combined.write(_record('filtered', level: LogLevel.debug));
      expect(events, isEmpty);
      await combined.write(_record('info'));
      await combined.write(_record('warning', level: LogLevel.warning));
      await combined.flush();
      expect(events, [
        'first',
        'first',
        'second',
        'first flush',
        'second flush',
      ]);
      expect(errors, [same(error), same(error), same(flushError)]);
    },
  );

  test('combined outputs wait for each preceding backend', () async {
    final started = Completer<void>();
    final gate = Completer<void>();
    var secondCalls = 0;
    final combined = Logger.combine([
      Logger((_) {
        started.complete();
        return gate.future;
      }),
      Logger((_) => secondCalls++),
    ]);
    final writing = combined.write(_record('event'));
    await started.future;
    expect(secondCalls, 0);
    gate.complete();
    await writing;
    expect(secondCalls, 1);
  });

  test('the default runtime supplies a no-op logger', () async {
    final runtime = Runtime();
    addTearDown(runtime.close);
    await runtime.runOrThrow(Task.log('discarded'));
    final logger = await runtime.runOrThrow(Task.service(Logger.key));
    await logger.write(_record('also discarded'));
    await logger.flush();
  });

  test('no-op and filtered tasks never read the clock', () async {
    final clock = _ThrowingClock(StateError('clock must not be read'));
    for (final logger in [
      Logger.noop(),
      Logger.combine([]),
      Logger(
        (_) => fail('filtered record reached output'),
        minLevel: LogLevel.error,
      ),
    ]) {
      final runtime = runtimeWith(logger, clock: clock);
      await runtime.runOrThrow(Task.log('discarded'));
    }
    expect(clock.calls, 0);
  });

  test(
    'log tasks are lazy, reusable, and use the current clock and logger',
    () async {
      final firstRecords = <LogRecord>[];
      final otherRecords = <LogRecord>[];
      final clock = TestClock(start: DateTime.utc(2024));
      final runtime = runtimeWith(Logger(firstRecords.add), clock: clock);
      final task = Task.log('event', level: LogLevel.warning);
      expect(firstRecords, isEmpty);

      await runtime.runOrThrow(task);
      clock.advance(const Duration(seconds: 2));
      await runtime.runOrThrow(
        task.provideContext(
          Context.from([Logger.key.bind(Logger(otherRecords.add))]),
        ),
      );

      expect(firstRecords.single.timestamp, DateTime.utc(2024));
      expect(otherRecords.single.timestamp, DateTime.utc(2024, 1, 1, 0, 0, 2));
      expect(otherRecords.single.level, LogLevel.warning);
      expect(otherRecords.single.message, 'event');
    },
  );

  test(
    'log fields copy on construction and explicit fields override locals',
    () async {
      final records = <LogRecord>[];
      final runtime = runtimeWith(Logger(records.add));
      final explicit = <String, Object?>{'shared': null, 'explicit': true};
      final local = <String, Object?>{'shared': 'local', 'local': true};
      final task = Task.log('event', fields: explicit).withLogFields(local);
      explicit['explicit'] = false;
      local['local'] = false;

      await runtime.runOrThrow(
        task.withLogFields({'outer': true, 'shared': 'outer'}),
      );
      expect(records.single.fields, {
        'outer': true,
        'shared': null,
        'local': true,
        'explicit': true,
      });
      expect(() => records.single.fields.clear(), throwsUnsupportedError);
    },
  );

  test('nested fields restore after success and business failure', () async {
    final records = <LogRecord>[];
    final runtime = runtimeWith(Logger(records.add));
    final task = Task.log('inner')
        .withLogFields({'request': 'inner'})
        .flatMap((_) => Task.log('outer'))
        .withError<String>()
        .flatMap<void>(
          (_) => Task.fail<String>('failed').withLogFields({'leak': true}),
        )
        .recover((error) => Task.log('recovered').withError<String>())
        .withLogFields({'request': 'outer'});

    await runtime.runOrThrow(task);
    await runtime.runOrThrow(Task.log('independent'));
    expect(records.map((record) => record.message), [
      'inner',
      'outer',
      'recovered',
      'independent',
    ]);
    expect(records.map((record) => record.fields), [
      {'request': 'inner'},
      {'request': 'outer'},
      {'request': 'outer'},
      <String, Object?>{},
    ]);
  });

  test(
    'fork inherits fields and child changes do not reach the parent',
    () async {
      final records = <LogRecord>[];
      final runtime = runtimeWith(Logger(records.add));
      final gate = Completer<void>();
      final child = _wait(gate.future)
          .flatMap((_) => Task.log('inherited'))
          .flatMap(
            (_) => Task.log('child').withLogFields({'request': 'child'}),
          );
      final task = child
          .fork()
          .withLogFields({'request': 'launch'})
          .flatMap(
            (fiber) =>
                Task.sync(gate.complete)
                    .flatMap((_) => fiber.join())
                    .flatMap((_) => Task.log('parent'))
                    .withLogFields({'request': 'parent'}),
          );

      await runtime.runOrThrow(task);
      expect(records.map((record) => record.message), [
        'inherited',
        'child',
        'parent',
      ]);
      expect(records.map((record) => record.fields), [
        {'request': 'launch'},
        {'request': 'child'},
        {'request': 'parent'},
      ]);
    },
  );

  test('finalizers use registration fields after the override ends', () async {
    final records = <LogRecord>[];
    final runtime = runtimeWith(Logger(records.add));
    final task = Task.addFinalizer((_) => Task.log('release'))
        .withLogFields({'resource': 'registered'})
        .flatMap((_) => Task.log('body'));

    await runtime.runOrThrow(task);
    expect(records.map((record) => record.message), ['body', 'release']);
    expect(records.map((record) => record.fields), [
      <String, Object?>{},
      {'resource': 'registered'},
    ]);
  });

  test(
    'logging failures preserve successful and failed business results',
    () async {
      final errors = <Object>[];
      final failure = StateError('output');
      final logger = Logger(
        (_) async => throw failure,
        onError: (error, stack) => errors.add(error),
      );
      final runtime = runtimeWith(logger);

      expect(await runtime.runOrThrow(Task.log('success').map((_) => 42)), 42);
      final result = await runtime.run(
        Task.fail<String>('business').ensuring(Task.log('cleanup')),
      );
      expect(
        _cause(result),
        isA<CauseFailure<String>>().having(
          (cause) => cause.error,
          'error',
          'business',
        ),
      );
      expect(errors, [same(failure), same(failure)]);
    },
  );

  test(
    'clock failures are independently reported and skip the record',
    () async {
      final records = <LogRecord>[];
      final errors = <Object>[];
      final failure = StateError('clock');
      final clock = _ThrowingClock(failure);
      final runtime = runtimeWith(
        Logger(records.add, onError: (error, stack) => errors.add(error)),
        clock: clock,
      );

      expect(await runtime.runOrThrow(Task.log('event').map((_) => 42)), 42);
      expect(records, isEmpty);
      expect(errors, [same(failure)]);
      expect(clock.calls, 1);
    },
  );

  test('cancellation waits for an in-flight write and reports backend failure independently', () async {
    final started = Completer<void>();
    final gate = Completer<void>();
    final errors = <Object>[];
    final error = StateError('backend failure after cancellation');
    final runtime = runtimeWith(
      Logger((_) {
        started.complete();
        return gate.future;
      }, onError: (error, stack) => errors.add(error)),
    );
    final controller = CancellationController();
    var settled = false;
    var continued = false;
    final running = runtime
        .run(
          Task.log('event').map((_) => continued = true),
          signal: controller.signal,
        )
        .then((exit) {
          settled = true;
          return exit;
        });
    await started.future;
    controller.requestCancel(reason: 'stop');
    expect(settled, isFalse);
    gate.completeError(error);
    final result = await running;

    expect(
      _cause(result),
      isA<CauseCancelled>().having((cause) => cause.reason, 'reason', 'stop'),
    );
    expect(errors, [same(error)]);
    expect(continued, isFalse);
  });

  test('an already cancelled log task does not enqueue output', () async {
    final records = <LogRecord>[];
    final runtime = runtimeWith(Logger(records.add));
    final controller = CancellationController()..requestCancel();
    final result = await runtime.run(
      Task.log('event'),
      signal: controller.signal,
    );
    expect(_cause(result), isA<CauseCancelled>());
    expect(records, isEmpty);
  });
}

LogRecord _record(String message, {LogLevel level = LogLevel.info}) =>
    LogRecord(level: level, timestamp: DateTime.utc(2024), message: message);

Task<void, Never> _wait(Future<void> future) =>
    Task.fromFuture(() => future, onError: Error.throwWithStackTrace);

Cause<E> _cause<T, E>(Exit<T, E> exit) => switch (exit) {
  ExitFailure(:final cause) => cause,
  ExitSuccess() => throw StateError('Expected failure, got $exit'),
};

final class _ThrowingClock implements Clock {
  _ThrowingClock(this.error);

  final Object error;
  int calls = 0;

  @override
  DateTime now() {
    calls++;
    throw error;
  }

  @override
  Duration get elapsed => Duration.zero;

  @override
  void Function() schedule(Duration delay, void Function() callback) =>
      throw StateError('Unexpected timer');
}
