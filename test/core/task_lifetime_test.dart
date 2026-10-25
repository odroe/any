import 'dart:async';

import 'package:any/any.dart';
import 'package:any/src/core/task.dart' show inTaskScope;
import 'package:test/test.dart';

void main() {
  late Runtime runtime;
  setUp(() {
    runtime = Runtime();
    addTearDown(runtime.close);
  });

  test(
    'completion callbacks leave body resources in the caller scope',
    () async {
      final body = _Resource();
      final temporary = _Resource();
      final value = await runtime.runOrThrow(
        body.acquire
            .onExit((exit) {
              expect(exit, isA<ExitSuccess<_Resource, Never>>());
              expect(body.closed, isFalse);
              return temporary.acquire.map((_) {});
            })
            .flatMap(
              (value) => Task.sync(() {
                expect(value, same(body));
                expect(body.closed, isFalse);
                expect(temporary.closed, isTrue);
                return 42;
              }),
            ),
      );
      expect(value, 42);
      expect(body.releases, 1);
      expect(temporary.releases, 1);
    },
  );

  test('joining a child does not release its borrowed resources', () async {
    final resource = _Resource();
    await runtime.runOrThrow(
      resource.acquire.fork().flatMap((child) => child.join()).map((value) {
        expect(value, same(resource));
        expect(resource.closed, isFalse);
      }),
    );
    expect(resource.releases, 1);
  });

  test(
    'onExit runs before child draining and ambient resource release',
    () async {
      final events = <String>[];
      final child = Task.fromCallback<void, Never>((complete) {
        events.add('child started');
        return () => events.add('child stopped');
      }).ensuring(Task.sync(() => events.add('child cleanup')));
      await runtime.runOrThrow(
        Task.addFinalizer((_) => Task.sync(() => events.add('released')))
            .flatMap((_) => child.fork())
            .onExit(
              (exit) => Task.sync(() {
                expect((exit as ExitSuccess).value.poll(), isA<OptionNone>());
                events.add('hook');
              }),
            ),
      );
      expect(events, [
        'child started',
        'hook',
        'child stopped',
        'child cleanup',
        'released',
      ]);
    },
  );

  test(
    'a local scope stops borrowers even when their parent continues',
    () async {
      final events = <String>[];
      final child = Task.fromCallback<void, Never>((complete) {
        events.add('started');
        return () => events.add('stopped');
      });
      await runtime.runOrThrow(
        Task.addFinalizer((_) => Task.sync(() => events.add('released')))
            .flatMap((_) => child.fork())
            .scoped()
            .map((fiber) {
              expect(fiber.poll(), isA<OptionSome>());
              expect(events, ['started', 'stopped', 'released']);
              events.add('parent continued');
            }),
      );
      expect(events.last, 'parent continued');
    },
  );

  test(
    'a child parent stops its children while borrowed resources survive',
    () async {
      final resource = _Resource();
      var stopped = false;
      final child = Task.fromCallback<void, Never>((complete) {
        return () {
          stopped = true;
        };
      });
      final parent = resource.acquire.flatMap((_) => child.fork());
      await runtime.runOrThrow(
        parent.fork().flatMap((fiber) => fiber.join()).map((child) {
          expect(stopped, isTrue);
          expect(child.poll(), isA<OptionSome>());
          expect(resource.closed, isFalse);
        }),
      );
      expect(resource.closed, isTrue);
    },
  );

  test('onExit inside and outside scoped observe different exits', () async {
    final events = <String>[];
    final defect = Cause<Never>.defect(Object(), StackTrace.current);
    final body = Task.addFinalizer(
      (_) => Task.sync(() {
        events.add('released');
      }).flatMap((_) => Task.failCause(defect)),
    );
    final exit = await runtime.run(
      body
          .onExit(
            (exit) => Task.sync(() {
              expect(exit, isA<ExitSuccess>());
              events.add('inside');
            }),
          )
          .scoped()
          .onExit(
            (exit) => Task.sync(() {
              expect((exit as ExitFailure).cause, same(defect));
              events.add('outside');
            }),
          ),
    );
    expect((exit as ExitFailure).cause, same(defect));
    expect(events, ['inside', 'released', 'outside']);
  });

  test('cancelled completion callbacks run in reverse nesting order', () async {
    final started = Completer<void>();
    final controller = CancellationController();
    final events = <String>[];
    final task =
        Task.fromCallback<void, Never>((complete) {
              started.complete();
              return () {};
            })
            .ensuring(Task.sync(() => events.add('inner')))
            .ensuring(Task.sync(() => events.add('outer')))
            .map((_) => fail('Cancelled work must not continue'));
    final running = runtime.run(task, signal: controller.signal);
    await started.future;
    controller.requestCancel(reason: 'stop');
    expect((await running as ExitFailure).cause, isA<CauseCancelled>());
    expect(events, ['inner', 'outer']);
  });

  test(
    'forkIn resources survive the worker until their explicit owner closes',
    () async {
      final resource = _Resource();
      await runtime.runOrThrow(
        Scope.open().flatMap((owner) {
          return resource.acquire
              .forkIn(owner)
              .fork()
              .flatMap((creator) => creator.join())
              .flatMap((worker) => worker.join())
              .flatMap((value) {
                expect(resource.closed, isFalse);
                return owner.close(const Exit.success(null));
              })
              .map((_) => expect(resource.releases, 1));
        }),
      );
      expect(resource.releases, 1);
    },
  );

  test(
    'public forkIn rejects another runtime before starting the body',
    () async {
      final other = Runtime();
      addTearDown(other.close);
      final ready = Completer<Scope>();
      final root = runtime.start(
        Scope.open().flatMap((scope) {
          return Task.fromCallback<void, Never>((complete) {
            ready.complete(scope);
            return () {};
          });
        }),
      );
      final scope = await ready.future;
      var ran = false;
      final result = await other.run(Task.sync(() => ran = true).forkIn(scope));
      expect(
        ((result as ExitFailure).cause as CauseDefect).error,
        isA<StateError>(),
      );
      expect(ran, isFalse);
      root.requestCancel();
      await runtime.run(root.awaitExit());
    },
  );

  test(
    'scope closing waits for protected acquisition and release registration',
    () async {
      final started = Completer<void>();
      final acquired = Completer<_Resource>();
      final resource = _Resource();
      final ready = Completer<Scope>();
      var used = false;
      final task = Task.acquireRelease(
        Task.fromFuture(() {
          started.complete();
          return acquired.future;
        }, onError: Error.throwWithStackTrace),
        release: (resource, _) => resource.release,
      ).map((_) => used = true);
      final root = runtime.start(
        Scope.open().flatMap((scope) {
          ready.complete(scope);
          return task.forkIn(scope).flatMap((worker) => worker.awaitExit());
        }),
      );
      final scope = await ready.future;
      await started.future;
      final closing = runtime.start(scope.close(const Exit.success(null)));
      expect(closing.poll(), isA<OptionNone>());
      expect(resource.releases, 0);
      acquired.complete(resource);
      await runtime.runOrThrow(closing.join());
      await runtime.run(root.awaitExit());
      expect(used, isFalse);
      expect(resource.releases, 1);
    },
  );

  test('a closing scope rejects acquisition that has not started', () async {
    final started = Completer<void>();
    final proceed = Completer<void>();
    final ready = Completer<Scope>();
    var acquired = false;
    final worker = Task.uninterruptibleMask<void, Never>((_) {
      return Task.fromFuture<void, Never>(() {
        started.complete();
        return proceed.future;
      }, onError: Error.throwWithStackTrace).flatMap((_) {
        return Task.acquireRelease(
          Task.sync(() {
            acquired = true;
          }),
          release: (_, _) => Task.sync(() => fail('Nothing was acquired')),
        );
      });
    });
    final root = runtime.start(
      Scope.open().flatMap((scope) {
        ready.complete(scope);
        return worker.forkIn(scope).flatMap((fiber) => fiber.awaitExit());
      }),
    );
    final scope = await ready.future;
    await started.future;
    final closing = runtime.run(scope.close(const Exit.success(null)));
    proceed.complete();
    final closed = await closing as ExitFailure<void, Never>;
    expect((closed.cause as CauseDefect).error, isA<StateError>());
    await runtime.run(root.awaitExit());
    expect(acquired, isFalse);
  });

  test('ten thousand completion boundaries remain stack safe', () async {
    var completed = 0;
    var task = Task.succeed(42);
    for (var i = 0; i < 10000; i++) {
      task = task.ensuring(
        Task.sync(() {
          completed++;
        }),
      );
    }
    expect(await runtime.runOrThrow(task), 42);
    expect(completed, 10000);
  });

  test(
    'closing a borrowed child waits for its nested completion frames',
    () async {
      final ready = Completer<Scope>();
      final started = Completer<void>();
      final cleaning = Completer<void>();
      final finishCleanup = Completer<void>();
      final events = <String>[];
      final worker = runtime.start(
        Scope.open().flatMap((borrowed) {
          ready.complete(borrowed);
          final pending = Task.fromCallback<void, Never>((complete) {
            started.complete();
            return () => events.add('stopped');
          });
          return inTaskScope(
            Task.addFinalizer((_) => Task.sync(() => events.add('released')))
                .flatMap(
                  (_) => Task.addFinalizer(
                    (_) => Task.sync(() => events.add('local cleanup')),
                  ).flatMap((_) => inTaskScope(pending, borrowed)).scoped(),
                )
                .ensuring(
                  Task.fromFuture<void, Never>(() {
                    events.add('hook started');
                    cleaning.complete();
                    return finishCleanup.future;
                  }, onError: Error.throwWithStackTrace),
                ),
            borrowed,
          ).map((_) => fail('Cancellation must skip the outer continuation'));
        }),
      );
      final borrowed = await ready.future;
      await started.future;
      final closing = runtime.start(borrowed.close(const Exit.success(null)));
      await cleaning.future;
      expect(closing.poll(), isA<OptionNone>());
      expect(events, ['stopped', 'local cleanup', 'hook started']);
      finishCleanup.complete();
      await runtime.runOrThrow(closing.join());
      final exit = await runtime.runOrThrow(worker.awaitExit());
      expect((exit as ExitFailure).cause, isA<CauseCancelled>());
      expect(events, ['stopped', 'local cleanup', 'hook started', 'released']);
    },
  );

  test('borrowed child and lexical cleanup failures remain distinct', () async {
    final reports = <Cause<Object?>>[];
    final runtime = Runtime(onUnhandledError: reports.add);
    addTearDown(runtime.close);
    final ready = Completer<Scope>();
    final started = Completer<void>();
    final defect = Cause<Never>.defect(Object(), StackTrace.current);
    final worker = runtime.start(
      Scope.open().flatMap((borrowed) {
        ready.complete(borrowed);
        return inTaskScope(
          Task.addFinalizer((_) => Task.failCause(defect)).flatMap(
            (_) => Task.addFinalizer((_) => Task.failCause(defect))
                .flatMap(
                  (_) => Task.fromCallback<void, Never>((complete) {
                    started.complete();
                    return () {};
                  }),
                )
                .scoped(),
          ),
          borrowed,
        );
      }),
    );
    final borrowed = await ready.future;
    await started.future;
    final closed = await runtime.run(borrowed.close(const Exit.success(null)));
    expect((closed as ExitFailure).cause, same(defect));
    final result = await runtime.runOrThrow(worker.awaitExit()) as ExitFailure;
    final causes = (result.cause as CauseGroup).causes;
    expect(causes.whereType<CauseCancelled>(), hasLength(1));
    expect(causes.whereType<CauseDefect>(), [same(defect), same(defect)]);
    await Future<void>.delayed(Duration.zero);
    expect((reports.single as CauseGroup).causes.whereType<CauseDefect>(), [
      same(defect),
      same(defect),
    ]);
  });

  test(
    'scoped owns forks while resources are bound to another scope',
    () async {
      var stopped = false;
      final child = Task.fromCallback<void, Never>((complete) {
        return () {
          stopped = true;
        };
      });
      await runtime.runOrThrow(
        Scope.open().flatMap(
          (resources) =>
              inTaskScope(child.fork(), resources).scoped().map((fiber) {
                expect(stopped, isTrue);
                expect(fiber.poll(), isA<OptionSome>());
              }),
        ),
      );
    },
  );
}

final class _Resource {
  var releases = 0;
  bool get closed => releases != 0;
  Task<void, Never> get release => Task.sync(() {
    releases++;
  });
  Task<_Resource, Never> get acquire => Task.acquireRelease(
    Task.succeed(this),
    release: (resource, _) => resource.release,
  );
}
