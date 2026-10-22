import 'dart:async';

import 'package:any/any.dart';
import 'package:any/resource.dart';

/// Opens a database connection owned by the running task's scope.
typedef OpenDatabase = FutureOr<Database> Function();

/// Opens an API client owned by the running task's scope.
typedef OpenApi = FutureOr<ApiClient> Function();

/// Writes one audit entry, optionally asynchronously.
typedef WriteLog = FutureOr<void> Function(String message);

/// The workflow's dependencies, supplied as a flat record.
typedef FlowDependencies = ({OpenDatabase db, OpenApi api, WriteLog log});

enum DbError { notFound }

enum HttpError { unavailable }

/// The expected errors exposed by this workflow.
sealed class FlowError {
  const FlowError();

  @override
  String toString() => switch (this) {
    LoadFailed(:final error) => 'Database lookup failed: $error',
    RequestFailed(:final error) => 'Request failed: $error',
  };
}

final class LoadFailed extends FlowError {
  const LoadFailed(this.error);

  final DbError error;
}

final class RequestFailed extends FlowError {
  const RequestFailed(this.error);

  final HttpError error;
}

/// Example database operations; callbacks stand in for a database driver.
final class Database {
  const Database({required this.lookup, required this.close});

  final FutureOr<Result<Uri, DbError>> Function(
    int userId,
    CancellationToken cancellation,
  )
  lookup;
  final FutureOr<void> Function() close;
}

/// Example HTTP operations; callbacks stand in for an HTTP client.
final class ApiClient {
  const ApiClient({required this.get, required this.close});

  final FutureOr<Result<String, HttpError>> Function(
    Uri endpoint,
    CancellationToken cancellation,
  )
  get;
  final FutureOr<void> Function() close;
}

/// Looks up [userId], retaining the connection until the owning scope closes.
Task<Uri, DbError, OpenDatabase> load(int userId) =>
    acquireRelease<Database, DbError, OpenDatabase>(
      acquire: (context) async => Exit.success(await context.dependencies()),
      release: (database, _) => database.close(),
    ).flatMap(
      (database) => Task((context) async {
        final result = await database.lookup(userId, context.cancellation);
        return Exit.fromResult(result);
      }),
    );

/// Fetches [endpoint], retaining the client until the owning scope closes.
Task<String, HttpError, OpenApi> request(Uri endpoint) =>
    acquireRelease<ApiClient, HttpError, OpenApi>(
      acquire: (context) async => Exit.success(await context.dependencies()),
      release: (client, _) => client.close(),
    ).flatMap(
      (client) => Task((context) async {
        final result = await client.get(endpoint, context.cancellation);
        return Exit.fromResult(result);
      }),
    );

/// Records [message]; a thrown logging error becomes a defect.
Task<void, Never, WriteLog> record(String message) => Task((context) async {
  await context.dependencies(message);
  return const Exit.success(null);
});

/// Looks up a user's endpoint, fetches its contents, then records the response.
///
/// Each step keeps its own dependency and error types. Local wrappers project
/// dependencies and translate expected failures to [FlowError]. They use
/// [Task.runIn] so the whole workflow shares a scope and cancellation signal.
/// A failed or cancelled step skips subsequent steps. The API client closes
/// before the database when the owning scope closes, including after failure.
Task<String, FlowError, FlowDependencies> loadAndRecord(int userId) {
  // TODO: Replace these local wrappers with Task.adapt when it is available.
  final lookup = Task<Uri, FlowError, FlowDependencies>((context) async {
    final exit = await load(userId)
        .runIn(context.withDependencies(context.dependencies.db));
    return switch (exit) {
      ExitSuccess(:final value) => Exit.success(value),
      ExitFailure(:final cause) => Exit.failure(cause.mapError(LoadFailed.new)),
    };
  });

  return lookup
      .flatMap(
        (endpoint) =>
            Task<String, FlowError, FlowDependencies>((context) async {
              final exit = await request(endpoint)
                  .runIn(context.withDependencies(context.dependencies.api));
              return switch (exit) {
                ExitSuccess(:final value) => Exit.success(value),
                ExitFailure(:final cause) => Exit.failure(
                  cause.mapError(RequestFailed.new),
                ),
              };
            }),
      )
      .flatMap(
        (body) => Task<String, FlowError, FlowDependencies>((context) async {
          final exit = await record(body)
              .runIn(context.withDependencies(context.dependencies.log));
          return switch (exit) {
            ExitSuccess() => Exit.success(body),
            // Cause<Never> also fits the workflow's expected error type.
            ExitFailure(:final cause) => Exit.failure(cause),
          };
        }),
      );
}

Future<void> main() async {
  final endpoint = Uri.parse('https://example.test/users/7');
  final users = {7: endpoint};
  final responses = {endpoint: 'Ada'};
  final events = <String>[];

  // All I/O is simulated in memory. Each factory creates a fresh client.
  final exit = await loadAndRecord(7).run((
    db: () => Database(
      lookup: (userId, _) {
        final endpoint = users[userId];
        return endpoint == null
            ? const Result.failure(DbError.notFound)
            : Result.success(endpoint);
      },
      close: () => events.add('close database'),
    ),
    api: () => ApiClient(
      get: (endpoint, _) async {
        final body = responses[endpoint];
        return body == null
            ? const Result.failure(HttpError.unavailable)
            : Result.success(body);
      },
      close: () => events.add('close api'),
    ),
    log: (message) => events.add('record $message'),
  ));

  print(switch (exit) {
    ExitSuccess(:final value) => 'Loaded $value',
    ExitFailure(:final cause) => 'Failed: $cause',
  });
  print(events.join(' → '));
}
