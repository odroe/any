import 'dart:async';

import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  test(
    'flow requests share batches within a run and reuse an application pool',
    () async {
      final connections = <_Connection>[];
      final runtime = await _runtime(connections);
      addTearDown(runtime.close);
      final batches = <List<int>>[];
      final resolver = RequestResolver<int, int, String>(
        (keys) => Task.service(_connections).withError<String>().flatMap(
          (pool) => pool.use(
            (connection) => Task.sync(() {
              expect(connection.closed, isFalse);
              batches.add(keys);
              return [
                for (final key in keys) Result<int, String>.success(key * 10),
              ];
            }),
          ),
        ),
        maxBatchSize: 2,
        concurrency: 2,
      );
      final task = Flow.fromIterable([3, 1, 3, 2])
          .withError<String>()
          .mapTask(resolver.request, concurrency: 4)
          .grouped(3)
          .toList();
      for (var run = 0; run < 2; run++) {
        batches.clear();
        expect(await runtime.runOrThrow(task), [
          [30, 10, 30],
          [20],
        ]);
        expect(batches.expand((batch) => batch), unorderedEquals([1, 2, 3]));
        expect(batches.every((batch) => batch.length <= 2), isTrue);
        expect(connections, hasLength(1));
        expect(connections.single.closed, isFalse);
      }
      expect(await runtime.close(), isA<ExitSuccess<void, Never>>());
      expect(connections.single.closed, isTrue);
    },
  );

  test(
    'cancelling a flow request returns its pool slot to another root run',
    () async {
      final connections = <_Connection>[];
      final runtime = await _runtime(connections);
      addTearDown(runtime.close);
      final started = [Completer<void>(), Completer<void>()];
      final replies =
          <int, void Function(Exit<List<Result<int, String>>, String>)>{};
      final cancelled = <int>[];
      final resolver = RequestResolver<int, int, String>(
        (keys) => Task.service(_connections).withError<String>().flatMap(
          (pool) => pool.use(
            (connection) => Task.fromCallback((complete) {
              expect(connection.closed, isFalse);
              final key = keys.single;
              replies[key] = complete;
              started[key - 1].complete();
              return () => cancelled.add(key);
            }),
          ),
        ),
      );
      final controller = CancellationController();
      final first = runtime.run(
        Flow.fromTask(resolver.request(1)).toList(),
        signal: controller.signal,
      );
      await started[0].future;
      final second = runtime.run(Flow.fromTask(resolver.request(2)).toList());
      controller.requestCancel(reason: 'caller left');
      final stopped = await first;
      expect(stopped, isA<ExitFailure<List<int>, String>>());
      expect(cancelled, [1]);
      await started[1].future;
      expect(connections, hasLength(1));
      expect(connections.single.closed, isFalse);
      replies[2]!(const Exit.success([Result.success(20)]));
      expect(_value(await second), [20]);
      expect(await runtime.close(), isA<ExitSuccess<void, Never>>());
      expect(connections.single.closed, isTrue);
    },
  );
}

final _connections = ServiceKey<Pool<_Connection, String>>('connections');

Future<Runtime> _runtime(List<_Connection> connections) async => _value(
  await Runtime.create(
    _connections.layer(
      Pool.create(
        Task.acquireRelease(
          Task.sync(() {
            final connection = _Connection();
            connections.add(connection);
            return connection;
          }).withError<String>(),
          release: (connection, _) => Task.sync(() {
            expect(connection.closed, isFalse);
            connection.closed = true;
          }),
        ),
        maxSize: 1,
      ),
    ),
  ),
);

final class _Connection {
  var closed = false;
}

T _value<T, E>(Exit<T, E> exit) => switch (exit) {
  ExitSuccess(:final value) => value,
  ExitFailure(:final cause) => throw StateError('$cause'),
};
