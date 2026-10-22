import 'dart:async';

import 'package:any/any.dart';
import 'package:test/test.dart';

import '../../example/workflow.dart';

final _endpoint = Uri.parse('https://example.test/users/7');

void main() {
  group('Task workflow', () {
    test(
      'composes distinct dependency and error types in one owning run',
      () async {
        final events = <String>[];
        final tokens = <CancellationToken>[];
        final controller = CancellationController();
        final task = loadAndRecord(7);

        final exit = await task.run(
          _dependencies(events, tokens: tokens),
          cancellation: controller.token,
        );

        expect((exit as ExitSuccess<String, FlowError>).value, 'Ada');
        expect(events, [
          'open database',
          'read database',
          'open api',
          'request',
          'record Ada',
          'close api',
          'close database',
        ]);
        expect(tokens, [same(controller.token), same(controller.token)]);
      },
    );

    test(
      'leaves all resources in the borrowed scope until its owner closes it',
      () async {
        final events = <String>[];
        final scope = Scope();
        scope.addFinalizer((_) => events.add('outer cleanup'));
        final context = Context(
          scope: scope,
          dependencies: _dependencies(events),
        );

        final exit = await loadAndRecord(7).runIn(context);

        expect((exit as ExitSuccess<String, FlowError>).value, 'Ada');
        expect(scope.isClosed, isFalse);
        expect(events, [
          'open database',
          'read database',
          'open api',
          'request',
          'record Ada',
        ]);
        await scope.close(exit: exit);
        expect(events.sublist(5), [
          'close api',
          'close database',
          'outer cleanup',
        ]);
      },
    );

    test('maps a database failure and skips request and recording', () async {
      final events = <String>[];
      final exit = await loadAndRecord(7).run(
        _dependencies(
          events,
          lookup: (_, _) => const Result.failure(DbError.notFound),
        ),
      );

      final cause =
          (exit as ExitFailure<String, FlowError>).cause
              as CauseFailure<FlowError>;
      expect((cause.error as LoadFailed).error, DbError.notFound);
      expect(events, ['open database', 'read database', 'close database']);
    });

    test('maps a request failure and skips recording', () async {
      final events = <String>[];
      final exit = await loadAndRecord(7).run(
        _dependencies(
          events,
          get: (_, _) async => const Result.failure(HttpError.unavailable),
        ),
      );

      final cause =
          (exit as ExitFailure<String, FlowError>).cause
              as CauseFailure<FlowError>;
      expect((cause.error as RequestFailed).error, HttpError.unavailable);
      expect(events, [
        'open database',
        'read database',
        'open api',
        'request',
        'close api',
        'close database',
      ]);
    });

    for (final step in ['database', 'request', 'record']) {
      test('preserves a defect from $step and stops subsequent work', () async {
        final events = <String>[];
        final error = StateError('$step failed');
        final trace = StackTrace.fromString('$step trace');
        final dependencies = _dependencies(
          events,
          lookup: (_, _) {
            if (step == 'database') Error.throwWithStackTrace(error, trace);
            return Result.success(_endpoint);
          },
          get: (_, _) async {
            if (step == 'request') Error.throwWithStackTrace(error, trace);
            return const Result.success('Ada');
          },
          log: (_) {
            if (step == 'record') Error.throwWithStackTrace(error, trace);
          },
        );

        final exit = await loadAndRecord(7).run(dependencies);

        final defect =
            (exit as ExitFailure<String, FlowError>).cause
                as CauseDefect<FlowError>;
        expect(defect.error, same(error));
        expect(defect.stackTrace, same(trace));
        expect(events, [
          'open database',
          'read database',
          if (step != 'database') ...['open api', 'request'],
          if (step == 'record') 'record Ada',
          if (step != 'database') 'close api',
          'close database',
        ]);
      });
    }

    test(
      'does not open resources when cancellation precedes the workflow',
      () async {
        final events = <String>[];
        final reason = Object();
        final controller = CancellationController()..cancel(reason: reason);

        final exit = await loadAndRecord(7)
            .run(_dependencies(events), cancellation: controller.token);

        final cause =
            (exit as ExitFailure<String, FlowError>).cause
                as CauseCancelled<FlowError>;
        expect(cause.reason, same(reason));
        expect(events, isEmpty);
      },
    );

    for (final step in ['database', 'request']) {
      test('waits for cancellation during $step before cleanup', () async {
        final events = <String>[];
        final tokens = <CancellationToken>[];
        final controller = CancellationController();
        final reason = Object();
        final started = Completer<void>();
        final cancelled = Completer<void>();
        final stopped = Completer<void>();
        var finished = false;
        Future<void> waitForCancellation(CancellationToken token) async {
          final removeListener = token.onCancel((value) {
            expect(value, same(reason));
            cancelled.complete();
          });
          try {
            started.complete();
            await cancelled.future;
            await stopped.future;
          } finally {
            removeListener();
          }
        }

        final dependencies = _dependencies(
          events,
          tokens: tokens,
          lookup: (_, token) async {
            if (step == 'database') await waitForCancellation(token);
            return Result.success(_endpoint);
          },
          get: (_, token) async {
            await waitForCancellation(token);
            return const Result.success('Ada');
          },
        );
        final running = loadAndRecord(7)
            .run(dependencies, cancellation: controller.token)
            .then((exit) {
              finished = true;
              return exit;
            });
        await started.future;

        controller.cancel(reason: reason);
        await cancelled.future;
        await Future<void>(() {});
        expect(finished, isFalse);
        expect(events, [
          'open database',
          'read database',
          if (step == 'request') ...['open api', 'request'],
        ]);
        expect(tokens, everyElement(same(controller.token)));

        stopped.complete();
        final exit = await running;
        final cause =
            (exit as ExitFailure<String, FlowError>).cause
                as CauseCancelled<FlowError>;
        expect(cause.reason, same(reason));
        expect(events, [
          'open database',
          'read database',
          if (step == 'request') ...['open api', 'request', 'close api'],
          'close database',
        ]);
      });
    }

    test(
      'awaits recording and serial resource cleanup before completing',
      () async {
        final events = <String>[];
        final recordStarted = Completer<void>();
        final recordFinished = Completer<void>();
        final apiCloseStarted = Completer<void>();
        final apiCloseFinished = Completer<void>();
        var finished = false;
        final dependencies = _dependencies(
          events,
          log: (_) {
            recordStarted.complete();
            return recordFinished.future;
          },
          closeApi: () {
            apiCloseStarted.complete();
            return apiCloseFinished.future;
          },
        );
        final running = loadAndRecord(7).run(dependencies).then((exit) {
          finished = true;
          return exit;
        });
        await recordStarted.future;
        expect(finished, isFalse);
        expect(events.last, 'record Ada');

        recordFinished.complete();
        await apiCloseStarted.future;
        expect(finished, isFalse);
        expect(events.last, 'close api');
        expect(events, isNot(contains('close database')));

        apiCloseFinished.complete();
        expect(await running, isA<ExitSuccess<String, FlowError>>());
        expect(events.last, 'close database');
      },
    );

    test(
      'combines the mapped business failure and nested cleanup defects once',
      () async {
        final events = <String>[];
        final database = Scope();
        final api = Scope();
        final errors = List.generate(
          3,
          (index) => StateError('cleanup $index'),
        );
        final traces = List.generate(
          3,
          (index) => StackTrace.fromString('trace $index'),
        );
        database.addFinalizer(
          (_) => Error.throwWithStackTrace(errors[2], traces[2]),
        );
        api.addFinalizer(
          (_) => Error.throwWithStackTrace(errors[1], traces[1]),
        );
        api.addFinalizer((_) => Future<void>.error(errors[0], traces[0]));
        const childExit = Exit<void, String>.failure(
          Cause.failure('saved child exit'),
        );

        final exit = await loadAndRecord(7).run(
          _dependencies(
            events,
            get: (_, _) => const Result.failure(HttpError.unavailable),
            closeApi: () => api.close(exit: childExit),
            closeDatabase: () => database.close(exit: childExit),
          ),
        ) as ExitFailure<String, FlowError>;

        final causes = (exit.cause as CauseGroup<FlowError>).causes;
        expect(causes, hasLength(4));
        final failure =
            (causes.first as CauseFailure<FlowError>).error as RequestFailed;
        expect(failure.error, HttpError.unavailable);
        for (var index = 0; index < errors.length; index++) {
          final defect = causes[index + 1] as CauseDefect<FlowError>;
          expect(defect.error, same(errors[index]));
          expect(defect.stackTrace, same(traces[index]));
        }
        expect(events, [
          'open database',
          'read database',
          'open api',
          'request',
          'close api',
          'close database',
        ]);
      },
    );

    for (final boundary in ['dependency projection', 'error mapping']) {
      test('captures a throw from $boundary in a manual wrapper', () async {
        final events = <String>[];
        final error = Object();
        final trace = StackTrace.fromString('$boundary trace');
        OpenDatabase project(FlowDependencies dependencies) {
          if (boundary == 'dependency projection') {
            Error.throwWithStackTrace(error, trace);
          }
          return dependencies.db;
        }

        FlowError translate(DbError failure) =>
            Error.throwWithStackTrace(error, trace);
        final task = Task<Uri, FlowError, FlowDependencies>((context) async {
          final exit = await load(7)
              .runIn(context.withDependencies(project(context.dependencies)));
          return switch (exit) {
            ExitSuccess(:final value) => Exit.success(value),
            ExitFailure(:final cause) => Exit.failure(
              cause.mapError(translate),
            ),
          };
        });

        final exit = await task.run(
          _dependencies(
            events,
            lookup: (_, _) => const Result.failure(DbError.notFound),
          ),
        );

        final defect =
            (exit as ExitFailure<Uri, FlowError>).cause
                as CauseDefect<FlowError>;
        expect(defect.error, same(error));
        expect(defect.stackTrace, same(trace));
        expect(
          events,
          boundary == 'dependency projection'
              ? isEmpty
              : ['open database', 'read database', 'close database'],
        );
      });
    }
  });
}

FlowDependencies _dependencies(
  List<String> events, {
  List<CancellationToken>? tokens,
  FutureOr<Result<Uri, DbError>> Function(int, CancellationToken)? lookup,
  FutureOr<Result<String, HttpError>> Function(Uri, CancellationToken)? get,
  WriteLog? log,
  FutureOr<void> Function()? closeDatabase,
  FutureOr<void> Function()? closeApi,
}) => (
  db: () {
    events.add('open database');
    return Database(
      lookup: (userId, token) {
        expect(userId, 7);
        events.add('read database');
        tokens?.add(token);
        return lookup?.call(userId, token) ?? Result.success(_endpoint);
      },
      close: () {
        events.add('close database');
        if (closeDatabase != null) return closeDatabase();
      },
    );
  },
  api: () {
    events.add('open api');
    return ApiClient(
      get: (endpoint, token) {
        expect(endpoint, _endpoint);
        events.add('request');
        tokens?.add(token);
        return get?.call(endpoint, token) ?? const Result.success('Ada');
      },
      close: () {
        events.add('close api');
        if (closeApi != null) return closeApi();
      },
    );
  },
  log: (message) {
    events.add('record $message');
    if (log != null) return log(message);
  },
);
