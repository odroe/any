import 'dart:async';

import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  test(
    'application services support bounded work, tracing, and ordered shutdown',
    () async {
      final app = await _Application.create();
      addTearDown(app.runtime.close);
      final running = app.runtime.run(
        _loadReports().withLogFields({'requestId': 'one'}),
      );
      await Future.wait([app.api.started(1), app.api.started(2)]);
      expect(app.api.active, 2);
      expect(app.api.hasStarted(3), isFalse);
      expect(app.events, contains('query released'));

      app.api.succeed(2);
      await app.api.started(3);
      expect(app.api.active, 2);
      app.api.succeed(3);
      app.api.succeed(1);
      final exit = await running;
      expect(_value(exit).map((report) => report.user), [1, 2, 3]);
      expect(app.api.active, 0);
      expect(
        app.events.where((event) => event == 'open database'),
        hasLength(1),
      );
      expect(app.events.where((event) => event == 'open api'), hasLength(1));
      expect(app.events, isNot(contains('close database')));

      expect(await app.runtime.close(), isA<ExitSuccess<void, Never>>());
      final root = app.spans.singleWhere((span) => span.name == 'load reports');
      expect(root.exit, isA<ExitSuccess<void, Object?>>());
      final children = app.spans.where(
        (span) => span.name.startsWith('report '),
      );
      expect(children, hasLength(3));
      expect(
        children.every((span) => identical(span.parent, root.context)),
        isTrue,
      );
      final recorded = app.logs.singleWhere(
        (record) => record.message == 'reports loaded',
      );
      expect(recorded.fields, {'requestId': 'one', 'count': 3});
      expect(recorded.traceId, root.context.traceId);
      expect(recorded.spanId, root.context.spanId);
      _expectShutdownOrder(app);
      expect(app.unhandled, isEmpty);
    },
  );

  test('business failure stops sibling work and retains cleanup defects in the root span', () async {
    final cleanupError = StateError('request cleanup');
    final app = await _Application.create(cleanupFailure: cleanupError);
    addTearDown(app.runtime.close);
    final running = app.runtime.run(_loadReports());
    await Future.wait([app.api.started(1), app.api.started(2)]);
    app.api.fail(2);
    final exit = await running;
    final causes = _causes(exit);
    expect(
      causes.whereType<CauseFailure<_LoadError>>().single.error,
      isA<_ReportFailed>(),
    );
    expect(causes.whereType<CauseDefect>().map((cause) => cause.error), [
      cleanupError,
    ]);
    expect(app.api.hasStarted(3), isFalse);
    expect(app.api.active, 0);
    expect(app.events, contains('cancel request 1'));
    expect(
      app.logs.any((record) => record.message == 'reports loaded'),
      isFalse,
    );
    final root = app.spans.singleWhere((span) => span.name == 'load reports');
    expect(
      _causes(root.exit).whereType<CauseDefect>().single.error,
      same(cleanupError),
    );
    expect(await app.runtime.close(), isA<ExitSuccess<void, Never>>());
    _expectShutdownOrder(app);
    expect(app.unhandled, isEmpty);
  });

  test(
    'runtime shutdown waits for root cleanup before services and backend flush',
    () async {
      final app = await _Application.create();
      addTearDown(app.runtime.close);
      final running = app.runtime.run(_loadReports());
      await Future.wait([app.api.started(1), app.api.started(2)]);
      final closed = app.runtime.close();
      expect(await running, isA<ExitFailure<List<_Report>, _LoadError>>());
      expect(await closed, isA<ExitSuccess<void, Never>>());
      expect(app.api.active, 0);
      expect(app.api.hasStarted(3), isFalse);
      for (final id in [1, 2]) {
        expect(
          app.events.indexOf('release request $id'),
          lessThan(app.events.indexOf('close api')),
        );
      }
      final root = app.spans.singleWhere((span) => span.name == 'load reports');
      expect(_causes(root.exit).whereType<CauseCancelled>(), isNotEmpty);
      _expectShutdownOrder(app);
      expect(app.unhandled, isEmpty);
    },
  );
}

final _database = ServiceKey<_Database>('database');
final _api = ServiceKey<_Api>('api');

Task<List<_Report>, _LoadError> _loadReports() => Task.service(_database)
    .withError<_LoadError>()
    .flatMap(
      (database) => database.users().mapError<_LoadError>(_UsersFailed.new),
    )
    .flatMap(
      (users) => Task.service(_api).withError<_LoadError>().flatMap(
        (api) => Task.forEach(
          users,
          (user) => api
              .report(user)
              .mapError<_LoadError>(_ReportFailed.new)
              .withSpan('report $user'),
          concurrency: 2,
        ),
      ),
    )
    .tap(
      (reports) =>
          Task.log('reports loaded', fields: {'count': reports.length}),
    )
    .withSpan('load reports');

final class _Application {
  final events = <String>[];
  final logs = <LogRecord>[];
  final spans = <SpanRecord>[];
  final unhandled = <Cause<Object?>>[];
  late final Runtime runtime;
  late final _Api api;

  static Future<_Application> create({Object? cleanupFailure}) async {
    final app = _Application();
    app.api = _Api(app.events, cleanupFailure);
    final logger = Logger(
      app.logs.add,
      flush: () => app.events.add('flush logs'),
    );
    final tracer = Tracer(
      app.spans.add,
      flush: () => app.events.add('flush traces'),
    );
    final observers = Layer.mergeAll([
      Logger.key.layer(
        Task.acquireRelease(
          Task.succeed(logger),
          release: (logger, _) => Task.fromFuture<void, Never>(
            logger.flush,
            onError: Error.throwWithStackTrace,
          ),
        ),
      ),
      Tracer.key.layer(
        Task.acquireRelease(
          Task.succeed(tracer),
          release: (tracer, _) => Task.fromFuture<void, Never>(
            tracer.flush,
            onError: Error.throwWithStackTrace,
          ),
        ),
      ),
    ]);
    final database = _database
        .layer(
          Task.acquireRelease(
            Task.sync(() {
              app.events.add('open database');
              return _Database(app.events);
            }),
            release: (_, _) => Task.log(
              'database closed',
            ).flatMap((_) => Task.sync(() => app.events.add('close database'))),
          ),
        )
        .provide(observers);
    final api = _api
        .layer(
          Task.acquireRelease(
            Task.sync(() {
              app.events.add('open api');
              return app.api;
            }),
            release: (_, _) => Task.log('api closed')
                .flatMap((_) => Task.sync(() => app.events.add('close api'))),
          ),
        )
        .provide(observers);
    app.runtime = _value(
      await Runtime.create(
        Layer.mergeAll([observers, database, api]),
        onUnhandledError: app.unhandled.add,
      ),
    );
    return app;
  }
}

final class _Database {
  _Database(this.events);
  final List<String> events;

  Task<List<int>, _DbError> users() => Task.using(
    Task.succeed([1, 2, 3]).withError<_DbError>(),
    use: Task.succeed,
    release: (_, _) => Task.sync(() => events.add('query released')),
  );
}

final class _Api {
  _Api(this.events, this.cleanupFailure);
  final List<String> events;
  final Object? cleanupFailure;
  final _started = <int, Completer<void>>{};
  final _complete = <int, void Function(Exit<_Report, _ApiError>)>{};
  var active = 0;

  Future<void> started(int id) => (_started[id] ??= Completer()).future;
  bool hasStarted(int id) => _started[id]?.isCompleted ?? false;
  void succeed(int id) => _complete[id]!(Exit.success(_Report(id)));
  void fail(int id) =>
      _complete[id]!(const Exit.failure(Cause.failure(_ApiError())));

  Task<_Report, _ApiError> report(int id) => Task.using(
    Task.sync(() => active++).withError<_ApiError>(),
    use: (_) => Task.fromCallback((complete) {
      _complete[id] = complete;
      (_started[id] ??= Completer()).complete();
      return () {
        events.add('cancel request $id');
      };
    }),
    release: (_, _) => Task.sync(() {
      active--;
      events.add('release request $id');
      if (id == 1 && cleanupFailure != null) throw cleanupFailure!;
    }),
  );
}

sealed class _LoadError {}

final class _UsersFailed extends _LoadError {
  _UsersFailed(this.error);
  final _DbError error;
}

final class _ReportFailed extends _LoadError {
  _ReportFailed(this.error);
  final _ApiError error;
}

final class _DbError {}

final class _ApiError {
  const _ApiError();
}

final class _Report {
  const _Report(this.user);
  final int user;
}

T _value<T, E>(Exit<T, E> exit) => switch (exit) {
  ExitSuccess(:final value) => value,
  ExitFailure(:final cause) => throw StateError('$cause'),
};

List<Cause<E>> _causes<E>(Exit<Object?, E> exit) => switch (exit) {
  ExitFailure(cause: CauseGroup(:final causes)) => causes,
  ExitFailure(:final cause) => [cause],
  _ => [],
};

void _expectShutdownOrder(_Application app) {
  for (final closed in ['close database', 'close api']) {
    expect(app.events.where((event) => event == closed), hasLength(1));
    for (final flushed in ['flush logs', 'flush traces']) {
      expect(app.events.where((event) => event == flushed), hasLength(1));
      expect(app.events.indexOf(closed), lessThan(app.events.indexOf(flushed)));
    }
  }
  expect(
    app.logs.map((record) => record.message),
    containsAll(['api closed', 'database closed']),
  );
}
