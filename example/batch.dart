import 'dart:async';

import 'package:any/any.dart';
import 'package:any/concurrency.dart';
import 'package:any/dependency.dart';
import 'package:any/observability.dart';
import 'package:any/resource.dart';
import 'package:any/schedule.dart';
import 'package:any/schema.dart';

import 'workflow.dart';

typedef BatchInput = ({List<int> ids, int concurrency});

sealed class BatchError {
  const BatchError();
}

final class InvalidBatch extends BatchError {
  const InvalidBatch(this.error);
  final ValidationError error;
}

final class ItemFailed extends BatchError {
  const ItemFailed(this.id, this.error);
  final int id;
  final FlowError error;
}

final batchInput = Schema<int>.type()
    .refine((id) => id > 0, message: 'Must be positive.')
    .list()
    .field('ids')
    .zip(
      Schema<int>.type()
          .refine((limit) => limit > 0, message: 'Must be positive.')
          .field('concurrency'),
    )
    .map((fields) => (ids: fields.$1, concurrency: fields.$2));

/// Validates input, provides services, and runs ordered, bounded workflows.
///
/// An HTTP availability error permits two retries. Each attempt completes its
/// database and client cleanup before another starts. The deadline requests
/// cancellation of the whole item, including retry waits. Traces include those
/// attempt cleanups. Shared dependencies stay alive until the batch scope closes.
Task<List<String>, BatchError, ()> runBatch(
  Object? input, {
  required Task<FlowDependencies, BatchError, ()> dependencies,
  Duration timeout = const Duration(seconds: 5),
  void Function(TaskSpan<String, BatchError> span)? onItem,
}) =>
    Task<BatchInput, ValidationError, ()>(
      (_) => Exit.fromResult(batchInput.parse(input)),
    ).mapError<BatchError>(InvalidBatch.new).flatMap((config) {
      final tasks = config.ids.map((id) {
        final task = loadAndRecord(id)
            .mapError<BatchError>((error) => ItemFailed(id, error))
            .retry(
              delays: const [
                Duration(milliseconds: 10),
                Duration(milliseconds: 20),
              ],
              when: (error) => switch (error) {
                ItemFailed(
                  error: RequestFailed(error: HttpError.unavailable),
                ) =>
                  true,
                _ => false,
              },
            )
            .timeout(timeout);
        return onItem == null ? task : task.trace('user/$id', onEnd: onItem);
      });
      return all(tasks, concurrency: config.concurrency).provide(dependencies);
    });

/// In-memory services: user 2's first request fails, then its retry succeeds.
Task<FlowDependencies, Never, ()> sampleDependencies(List<String> events) =>
    acquireRelease<_Audit, Never, ()>(
      acquire: (_) {
        events.add('open audit');
        return Exit.success(_Audit(events));
      },
      release: (audit, _) => audit.close(),
    ).map<FlowDependencies>((audit) {
      final attempts = <int, int>{};
      return (
        db: () => Database(
          lookup: (id, _) =>
              Result.success(Uri.parse('https://example.test/users/$id')),
          close: () => events.add('close database'),
        ),
        api: () => ApiClient(
          get: (uri, _) async {
            final id = int.parse(uri.pathSegments.last);
            final attempt = attempts.update(
              id,
              (value) => value + 1,
              ifAbsent: () => 1,
            );
            events.add('request $id/$attempt');
            await Future<void>.delayed(Duration.zero);
            return id == 2 && attempt == 1
                ? const Result.failure(HttpError.unavailable)
                : Result.success('User $id');
          },
          close: () => events.add('close api'),
        ),
        log: audit.write,
      );
    });

final class _Audit {
  _Audit(this.events);
  final List<String> events;
  var closed = false;

  void write(String message) {
    if (closed) throw StateError('Audit log is closed.');
    events.add('record $message');
  }

  void close() {
    closed = true;
    events.add('close audit');
  }
}

Future<void> main() async {
  final events = <String>[];
  final spans = <TaskSpan<String, BatchError>>[];
  final exit = await runBatch(
    {
      'ids': [1, 2, 3],
      'concurrency': 2,
    },
    dependencies: sampleDependencies(events),
    onItem: spans.add,
  ).run(());
  print(switch (exit) {
    ExitSuccess(:final value) => value.join(', '),
    ExitFailure(:final cause) => 'Failed: $cause',
  });
  print('${spans.length} item spans');
  print(events.join(' → '));
}
