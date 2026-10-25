import 'dart:async';

import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  late Runtime runtime;
  late SharedResource<_Client, String> clients;
  late Scope owner;
  late _Client client;
  late List<Cause<Object?>> reports;
  late Completer<Cause<Object?>> reported;

  setUp(() async {
    client = _Client();
    reports = [];
    reported = Completer();
    final clientsKey = ServiceKey<SharedResource<_Client, String>>('clients');
    final ownerKey = ServiceKey<Scope>('operation owner');
    final services = clientsKey
        .layer(
          SharedResource.create<_Client, String>(
            Task.acquireRelease(
              Task.succeed(client),
              release: (client, _) => Task.sync(client.close),
            ),
          ),
        )
        .merge(ownerKey.layer(Scope.open()));
    runtime = _value(
      await Runtime.create(
        services,
        onUnhandledError: (cause) {
          reports.add(cause);
          if (!reported.isCompleted) reported.complete(cause);
        },
      ),
    );
    clients = await runtime.runOrThrow(Task.service(clientsKey));
    owner = await runtime.runOrThrow(Task.service(ownerKey));
  });

  tearDown(() async {
    await runtime.close();
  });

  test(
    'cancelling one waiter retains the native operation and its lease',
    () async {
      final native = _NativeCall();
      addTearDown(native.finish);
      final operation = runtime.start(native.run(clients));
      await native.started.future;
      final page = CancellationController();
      final first = runtime.run(operation.join(), signal: page.signal);
      final second = runtime.run(operation.join());

      page.requestCancel(reason: 'page left');
      expect(
        _causes(await first).whereType<CauseCancelled>().single.reason,
        'page left',
      );
      expect(operation.poll(), isA<OptionNone<Exit<int, String>>>());
      expect(client.releases, 0);
      expect(native.calls, 1);

      native.result.complete(42);
      expect(_value(await second), 42);
      expect(client.releases, 1);
      expect(native.mappings, 0);
      expect(reports, isEmpty);
    },
  );

  test(
    'a native failure after its sole waiter leaves is reported once',
    () async {
      final native = _NativeCall();
      addTearDown(native.finish);
      final operation = runtime.start(native.run(clients));
      await native.started.future;
      final page = CancellationController();
      final waiting = runtime.run(operation.join(), signal: page.signal);
      page.requestCancel(reason: 'page left');
      expect(_causes(await waiting).whereType<CauseCancelled>(), hasLength(1));
      expect(client.releases, 0);

      final error = StateError('connection failed');
      final stack = StackTrace.current;
      native.result.completeError(error, stack);
      final cause = await reported.future;
      expect((cause as CauseFailure<String>).error, 'native failure');
      expect(native.mappings, 1);
      expect(native.mappedError, same(error));
      expect(native.mappedStack, same(stack));
      expect(client.releases, 1);

      expect(await runtime.close(), isA<ExitSuccess<void, Never>>());
      await Future<void>.delayed(Duration.zero);
      expect(reports, hasLength(1));
    },
  );

  test(
    'runtime close retains a native lease and preserves a late defect',
    () async {
      final native = _NativeCall();
      addTearDown(native.finish);
      final operation = runtime.start(native.run(clients));
      await native.started.future;
      var closed = false;
      final closing = runtime.close().then((exit) {
        closed = true;
        return exit;
      });
      await Future<void>.delayed(Duration.zero);
      expect(closed, isFalse);
      expect(client.releases, 0);
      expect(operation.poll(), isA<OptionNone<Exit<int, String>>>());

      final error = StateError('failed while stopping');
      final stack = StackTrace.current;
      native.result.completeError(error, stack);
      final defect = _causes(await closing).whereType<CauseDefect>().single;
      expect(defect.error, same(error));
      expect(defect.stackTrace, same(stack));
      expect(native.mappings, 0);
      expect(client.releases, 1);
      final stopped = (operation.poll() as OptionSome<Exit<int, String>>).value;
      expect(_causes(stopped).whereType<CauseCancelled>(), hasLength(1));
      await Future<void>.delayed(Duration.zero);
      expect(reports, isEmpty);
    },
  );

  test(
    'forkIn keeps a native operation alive after its creator finishes',
    () async {
      final native = _NativeCall();
      addTearDown(native.finish);
      final operation = await runtime.runOrThrow(
        native.run(clients).forkIn(owner),
      );
      await native.started.future;
      expect(client.releases, 0);
      expect(operation.poll(), isA<OptionNone<Exit<int, String>>>());

      var closed = false;
      final closing = runtime.run(owner.close(const Exit.success(null))).then((
        exit,
      ) {
        closed = true;
        return exit;
      });
      await Future<void>.delayed(Duration.zero);
      expect(closed, isFalse);
      expect(client.releases, 0);
      native.result.complete(42);

      expect(await closing, isA<ExitSuccess<void, Never>>());
      expect(client.releases, 1);
      final stopped = (operation.poll() as OptionSome<Exit<int, String>>).value;
      expect(_causes(stopped).whereType<CauseCancelled>(), hasLength(1));
      expect(
        await runtime.runOrThrow(Task.succeed('still open')),
        'still open',
      );
      expect(reports, isEmpty);
    },
  );

  test(
    'a host Future timeout leaves Runtime.run as the result receiver',
    () async {
      final native = _NativeCall();
      addTearDown(native.finish);
      final running = runtime.run(native.run(clients));
      await native.started.future;

      await expectLater(
        running.timeout(Duration.zero),
        throwsA(isA<TimeoutException>()),
      );
      expect(client.releases, 0);
      native.result.completeError(
        StateError('late host result'),
        StackTrace.current,
      );
      await client.closed.future;
      await Future<void>.delayed(Duration.zero);
      expect(reports, isEmpty);
      final cause = _causes(await running).single as CauseFailure<String>;
      expect(cause.error, 'native failure');
      expect(native.mappings, 1);
      expect(client.releases, 1);
      await Future<void>.delayed(Duration.zero);
      expect(reports, isEmpty);
    },
  );

  for (final stopFails in [false, true]) {
    test(
      'a native cancellation ${stopFails ? 'failure' : 'acknowledgment'} precedes lease release',
      () async {
        final started = Completer<void>();
        final stopping = Completer<void>();
        final acknowledgment = Completer<void>();
        addTearDown(() {
          if (!acknowledgment.isCompleted) acknowledgment.complete();
        });
        var cancellations = 0;
        final operation = runtime.start(
          clients.use(
            (client) => Task.fromCallback<int, String>((complete) {
              client.checkOpen();
              started.complete();
              return () {
                cancellations++;
                client.checkOpen();
                stopping.complete();
                return acknowledgment.future.whenComplete(client.checkOpen);
              };
            }),
          ),
        );
        await started.future;
        final stopped = runtime.run(
          operation.cancel(reason: 'stop native work'),
        );
        await stopping.future;
        operation.requestCancel(reason: 'duplicate request');
        expect(client.releases, 0);
        expect(cancellations, 1);
        expect(operation.poll(), isA<OptionNone<Exit<int, String>>>());

        final error = StateError('native stop failed');
        final stack = StackTrace.current;
        if (stopFails) {
          acknowledgment.completeError(error, stack);
        } else {
          acknowledgment.complete();
        }
        final causes = _causes(_value(await stopped));
        expect(
          causes.whereType<CauseCancelled>().single.reason,
          'stop native work',
        );
        if (stopFails) {
          final defect = causes.whereType<CauseDefect>().single;
          expect(defect.error, same(error));
          expect(defect.stackTrace, same(stack));
        } else {
          expect(causes, hasLength(1));
        }
        expect(client.releases, 1);
        expect(cancellations, 1);
        await Future<void>.delayed(Duration.zero);
        if (stopFails) {
          expect(
            (reports.single as CauseGroup).causes
                .whereType<CauseDefect>()
                .single
                .error,
            same(error),
          );
        } else {
          expect(reports, isEmpty);
        }
      },
    );
  }
}

final class _Client {
  final closed = Completer<void>();
  var releases = 0;

  void checkOpen() {
    if (releases != 0) {
      throw StateError('Client was released during native work.');
    }
  }

  void close() {
    releases++;
    if (!closed.isCompleted) closed.complete();
  }
}

final class _NativeCall {
  final started = Completer<void>();
  final result = Completer<int>();
  var calls = 0;
  var mappings = 0;
  Object? mappedError;
  StackTrace? mappedStack;

  Task<int, String> run(SharedResource<_Client, String> clients) => clients.use(
    (client) => Task.fromFuture(
      () async {
        calls++;
        client.checkOpen();
        started.complete();
        try {
          return await result.future;
        } finally {
          client.checkOpen();
        }
      },
      onError: (error, stack) {
        mappings++;
        mappedError = error;
        mappedStack = stack;
        return 'native failure';
      },
    ),
  );

  void finish() {
    if (!result.isCompleted) result.complete(0);
  }
}

T _value<T, E>(Exit<T, E> exit) => switch (exit) {
  ExitSuccess(:final value) => value,
  ExitFailure(:final cause) => throw StateError('Unexpected failure: $cause'),
};

List<Cause<E>> _causes<T, E>(Exit<T, E> exit) => switch (exit) {
  ExitSuccess() => [],
  ExitFailure(cause: CauseGroup(:final causes)) => causes,
  ExitFailure(:final cause) => [cause],
};
