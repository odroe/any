import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  late Runtime runtime;

  setUp(() => runtime = Runtime());
  tearDown(() => runtime.close());

  test('cache loaders close their resources before publishing data', () async {
    final loaded = <_Connection>[];
    final program = _connect().flatMap(
      (caller) =>
          Cache.create<int, int, Never>(
            capacity: 1,
            ttl: const Duration(minutes: 1),
            load: (_) => Task.all([
              _connect().map((connection) => loaded.add(connection)),
              _connect().map((connection) => loaded.add(connection)),
            ], concurrency: 2).map((_) => loaded.length),
          ).flatMap(
            (cache) => cache.get(1).map((count) {
              expect(count, 2);
              expect(loaded.every((connection) => connection.closed), isTrue);
              expect(caller.closed, isFalse);
            }),
          ),
    );

    await runtime.runOrThrow(program);
    expect(loaded.every((connection) => connection.releases == 1), isTrue);
  });

  test(
    'request batches close loader resources without closing the caller',
    () async {
      final loaded = <_Connection>[];
      final resolver = RequestResolver<int, int, Never>(
        (keys) => Task.forEach(
          keys,
          (key) => _connect().map((connection) {
            loaded.add(connection);
            return Result<int, Never>.success(key);
          }),
          concurrency: 2,
        ),
      );
      await runtime.runOrThrow(
        _connect().flatMap(
          (caller) => resolver.batch([1, 2]).map((results) {
            expect(results, hasLength(2));
            expect(loaded.every((connection) => connection.closed), isTrue);
            expect(caller.closed, isFalse);
          }),
        ),
      );
      expect(loaded.every((connection) => connection.releases == 1), isTrue);
    },
  );

  test(
    'shared acquisition keeps concurrent resources alive for the lease',
    () async {
      final loaded = <_Connection>[];
      await runtime.runOrThrow(
        SharedResource.create(
          Task.all([_connect(), _connect()], concurrency: 2)
              .map((connections) {
                loaded.addAll(connections);
                return connections;
              })
              .ensuring(Task.succeed(null)),
        ).flatMap(
          (shared) => shared.acquire().map((connections) {
            expect(
              connections.every((connection) => !connection.closed),
              isTrue,
            );
          }),
        ),
      );
      expect(loaded, hasLength(2));
      expect(loaded.every((connection) => connection.releases == 1), isTrue);
    },
  );

  test(
    'pool acquisitions inherit the owner runtime and keep slot resources',
    () async {
      late _Connection connection;
      final acquire = Scope.open().flatMap(
        (scope) => _connect().forkIn(scope).flatMap((fiber) => fiber.join()),
      );
      await runtime.runOrThrow(
        Pool.create(acquire, maxSize: 1).flatMap(
          (pool) => pool.use(
            (value) => Task.sync(() {
              connection = value;
              expect(value.closed, isFalse);
            }),
          ),
        ),
      );
      expect(connection.releases, 1);
    },
  );
}

Task<_Connection, Never> _connect() => Task.acquireRelease(
  Task.sync(_Connection.new),
  release: (connection, _) => Task.sync(() => connection.releases++),
);

final class _Connection {
  var releases = 0;
  bool get closed => releases > 0;
}
