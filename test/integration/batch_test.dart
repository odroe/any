import 'dart:async';

import 'package:any/any.dart';
import 'package:any/concurrency.dart';
import 'package:any/observability.dart';
import 'package:any/schedule.dart';
import 'package:test/test.dart';

import '../../example/batch.dart';
import '../../example/workflow.dart';

void main() {
  test('validation, provisioning, retries, bounds, adaptation, and tracing compose', () async {
    final events = <String>[];
    final spans = <TaskSpan<String, BatchError>>[];
    final exit = await runBatch(
      {
        'ids': [3, 2, 1],
        'concurrency': 2,
      },
      dependencies: sampleDependencies(events),
      onItem: spans.add,
    ).run(());
    expect((exit as ExitSuccess).value, ['User 3', 'User 2', 'User 1']);
    expect(events.where((e) => e.startsWith('request 2/')), [
      'request 2/1',
      'request 2/2',
    ]);
    expect(events.where((e) => e == 'close api'), hasLength(4));
    expect(events.where((e) => e == 'close database'), hasLength(4));
    expect(events.first, 'open audit');
    expect(events.last, 'close audit');
    expect(
      spans.map((span) => span.name),
      containsAll(['user/1', 'user/2', 'user/3']),
    );
    expect(spans.every((span) => span.exit is ExitSuccess), isTrue);
  });

  test(
    'invalid fields are reported together before dependencies are built',
    () async {
      final events = <String>[];
      final exit = await runBatch({
        'ids': [1, 'bad'],
        'concurrency': 0,
      }, dependencies: sampleDependencies(events)).run(()) as ExitFailure;
      final error = (exit.cause as CauseFailure).error as InvalidBatch;
      expect(error.error.issues.map((issue) => issue.path), [
        ['ids', 1],
        ['concurrency'],
      ]);
      expect(events, isEmpty);
    },
  );

  test(
    'a database error is mapped once, stops queued items, and is not retried',
    () async {
      final events = <String>[];
      final exit = await runBatch(
        {
          'ids': [1, 2],
          'concurrency': 1,
        },
        dependencies: _dependencies(
          events,
          lookup: (_, _) => const Result.failure(DbError.notFound),
        ),
      ).run(()) as ExitFailure;
      final error = (exit.cause as CauseFailure).error as ItemFailed;
      expect(error.id, 1);
      expect((error.error as LoadFailed).error, DbError.notFound);
      expect(events, [
        'open dependencies',
        'lookup 1',
        'close database',
        'close dependencies',
      ]);
    },
  );

  test(
    'cancellation drains two active items before releasing their dependencies',
    () async {
      final events = <String>[];
      final started = Completer<void>();
      final pending = <Completer<Result<String, HttpError>>>[];
      final tokens = <CancellationToken>[];
      final controller = CancellationController();
      final reason = Object();
      final task = runBatch(
        {
          'ids': [1, 2, 3],
          'concurrency': 2,
        },
        dependencies: _dependencies(
          events,
          request: (_, token) {
            tokens.add(token);
            final result = Completer<Result<String, HttpError>>();
            pending.add(result);
            if (pending.length == 2) started.complete();
            return result.future;
          },
        ),
      );
      var done = false;
      final running = task.run((), cancellation: controller.token).then((exit) {
        done = true;
        return exit;
      });
      await started.future;
      controller.cancel(reason: reason);
      expect(tokens.every((token) => identical(token.reason, reason)), isTrue);
      expect(events.where((e) => e.startsWith('close')), isEmpty);
      expect(done, isFalse);
      for (final result in pending) {
        result.complete(const Result.success('late'));
      }
      final cause = (await running as ExitFailure).cause as CauseGroup;
      expect(cause.causes, hasLength(2));
      expect(
        cause.causes.cast<CauseCancelled>().map((c) => c.reason),
        everyElement(same(reason)),
      );
      expect(events.where((e) => e == 'close api'), hasLength(2));
      expect(events.where((e) => e == 'close database'), hasLength(2));
      expect(events, isNot(contains('lookup 3')));
      expect(events.last, 'close dependencies');
    },
  );

  test(
    'cleanup failure prevents retry and appears in the completed item span',
    () async {
      final events = <String>[];
      final spans = <TaskSpan<String, BatchError>>[];
      final cleanupError = Object();
      final exit = await runBatch(
        {
          'ids': [1, 2],
          'concurrency': 1,
        },
        dependencies: _dependencies(
          events,
          request: (_, _) => const Result.failure(HttpError.unavailable),
          closeApi: () => throw cleanupError,
        ),
        onItem: spans.add,
      ).run(()) as ExitFailure;
      final causes = (exit.cause as CauseGroup).causes;
      expect((causes[0] as CauseFailure).error, isA<ItemFailed>());
      expect((causes[1] as CauseDefect).error, same(cleanupError));
      expect(events.where((e) => e == 'lookup 1'), hasLength(1));
      expect(events, isNot(contains('lookup 2')));
      expect(spans, hasLength(1));
      expect((spans.single.exit as ExitFailure).cause, same(exit.cause));
      expect(events.last, 'close dependencies');
    },
  );

  test(
    'an item deadline reaches native work and preserves orderly cleanup',
    () async {
      final events = <String>[];
      Object? reason;
      final exit = await runBatch(
        {
          'ids': [1],
          'concurrency': 1,
        },
        timeout: Duration.zero,
        dependencies: _dependencies(
          events,
          request: (_, cancellation) {
            final result = Completer<Result<String, HttpError>>();
            cancellation.onCancel((value) {
              reason = value;
              result.complete(const Result.success('late'));
            });
            return result.future;
          },
        ),
      ).run(()) as ExitFailure;
      expect(reason, isA<TimeoutException>());
      expect((exit.cause as CauseCancelled).reason, same(reason));
      expect(events, [
        'open dependencies',
        'lookup 1',
        'request 1',
        'close api',
        'close database',
        'close dependencies',
      ]);
    },
  );

  test(
    'repeating a batch rebuilds and closes its shared resources each time',
    () async {
      final events = <String>[];
      final task = runBatch(
        {
          'ids': [1],
          'concurrency': 1,
        },
        dependencies: sampleDependencies(events),
      ).repeat(delays: const [Duration.zero]);
      expect((await task.run(()) as ExitSuccess).value, ['User 1']);
      expect(events.where((e) => e == 'open audit'), hasLength(2));
      expect(events.where((e) => e == 'close audit'), hasLength(2));
      expect(
        events.indexOf('close audit'),
        lessThan(events.lastIndexOf('open audit')),
      );
    },
  );

  test(
    'recovery can handle validation errors without building services',
    () async {
      final events = <String>[];
      final task = runBatch(null, dependencies: sampleDependencies(events))
          .recover(
            (error) => Task(
              (_) => error is InvalidBatch
                  ? const Exit.success(<String>[])
                  : Exit.failure(Cause.failure(error)),
            ),
          );
      expect((await task.run(()) as ExitSuccess).value, isEmpty);
      expect(events, isEmpty);
    },
  );

  test('a batch can win a race while the losing wait is cancelled', () async {
    final events = <String>[];
    final batch = runBatch({
      'ids': [1],
      'concurrency': 1,
    }, dependencies: sampleDependencies(events));
    final Task<List<String>, BatchError, ()> waiting = sleep<()>(
      const Duration(days: 1),
    ).map((_) => <String>[]);
    final exit = await race([waiting, batch]).run(());
    expect((exit as ExitSuccess).value, ['User 1']);
    expect(events.last, 'close audit');
  });
}

Task<FlowDependencies, BatchError, ()> _dependencies(
  List<String> events, {
  FutureOr<Result<Uri, DbError>> Function(int, CancellationToken)? lookup,
  FutureOr<Result<String, HttpError>> Function(Uri, CancellationToken)? request,
  void Function()? closeApi,
}) => Task((context) {
  var open = true;
  void checkOpen() => expect(
    open,
    isTrue,
    reason: 'Shared services must outlive all consumers.',
  );
  events.add('open dependencies');
  context.scope.addFinalizer((_) {
    open = false;
    events.add('close dependencies');
  });
  return Exit.success((
    db: () => Database(
      lookup: (id, cancellation) {
        checkOpen();
        events.add('lookup $id');
        return lookup?.call(id, cancellation) ??
            Result.success(Uri.parse('https://example.test/$id'));
      },
      close: () {
        checkOpen();
        events.add('close database');
      },
    ),
    api: () => ApiClient(
      get: (uri, cancellation) {
        checkOpen();
        events.add('request ${uri.pathSegments.last}');
        return request?.call(uri, cancellation) ?? const Result.success('ok');
      },
      close: () {
        checkOpen();
        events.add('close api');
        closeApi?.call();
      },
    ),
    log: (message) {
      checkOpen();
      events.add('record $message');
    },
  ));
});
