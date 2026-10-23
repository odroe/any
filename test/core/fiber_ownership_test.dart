import 'dart:async';

import 'package:any/any.dart';
import 'package:test/test.dart';

List<Cause<Object?>> reasons(Cause<Object?> cause) => switch (cause) {
  CauseGroup(:final causes) => causes,
  _ => [cause],
};

void main() {
  test(
    'scope cancellation forwards its original reason to owned fibers',
    () async {
      final runtime = Runtime();
      addTearDown(runtime.close);
      final reason = Object();
      Exit<Object?, Object?>? cleanupExit;
      final started = Completer<void>();
      final child =
          Task.addFinalizer(
            (exit) => Task.sync(() {
              cleanupExit = exit;
            }),
          ).flatMap(
            (_) => Task.fromCallback<void, Never>((complete) {
              started.complete();
              return () {};
            }),
          );
      final exit = await runtime.runOrThrow(
        Scope.open().flatMap(
          (scope) => child.forkIn(scope).flatMap((fiber) {
            return Task.fromFuture<void, Never>(
                  () => started.future,
                  onError: Error.throwWithStackTrace,
                )
                .flatMap(
                  (_) => scope.close(
                    Exit.failure(
                      Cause.combine([
                        Cause.failure('body'),
                        Cause.cancelled(reason),
                      ]),
                    ),
                  ),
                )
                .flatMap((_) => fiber.awaitExit());
          }),
        ),
      );
      expect(
        reasons((exit as ExitFailure).cause)
            .whereType<CauseCancelled>()
            .single
            .reason,
        same(reason),
      );
      expect(
        reasons((cleanupExit as ExitFailure).cause)
            .whereType<CauseCancelled>()
            .single
            .reason,
        same(reason),
      );
    },
  );

  test(
    'runtime close includes cleanup defects and waits for their release',
    () async {
      final reports = <Cause<Object?>>[];
      final runtime = Runtime(onUnhandledError: reports.add);
      final started = Completer<void>();
      final releasing = Completer<void>();
      final released = Completer<void>();
      final error = StateError('release');
      final work = runtime.run(
        Task.fromCallback<void, Never>((complete) {
          started.complete();
          return () {};
        }).ensuring(
          Task.fromFuture(() {
            releasing.complete();
            return released.future;
          }, onError: (error, stack) => error).orDie().flatMap(
            (_) => Task.sync(() {
              throw error;
            }),
          ),
        ),
      );
      await started.future;
      var closed = false;
      final closing = runtime.close().then((exit) {
        closed = true;
        return exit;
      });
      await releasing.future;
      expect(closed, isFalse);
      released.complete();
      final exit = await closing;
      final cause = (exit as ExitFailure<void, Never>).cause;
      expect(reasons(cause).whereType<CauseDefect>().single.error, same(error));
      expect(
        reasons((await work as ExitFailure).cause)
            .whereType<CauseDefect>()
            .single
            .error,
        same(error),
      );
      await Future<void>.delayed(Duration.zero);
      expect(reports, isEmpty);
    },
  );

  test(
    'parent cancellation reaches children while its native Future is pending',
    () async {
      final runtime = Runtime();
      final controller = CancellationController();
      final entered = Completer<void>();
      final parentGate = Completer<void>();
      final childStopped = Completer<void>();
      final child = Task.fromCallback<void, Never>((complete) {
        return childStopped.complete;
      });
      final work = runtime.run(
        child.fork().flatMap(
          (_) => Task.fromFuture(() {
            entered.complete();
            return parentGate.future;
          }, onError: (error, stack) => error).orDie(),
        ),
        signal: controller.signal,
      );
      await entered.future;
      controller.requestCancel(reason: 'parent');
      await childStopped.future.timeout(const Duration(seconds: 1));
      parentGate.complete();
      expect(await work, isA<ExitFailure<void, Never>>());
      await runtime.close();
    },
  );

  test(
    'scope stops all descendant fibers before starting resource finalizers',
    () async {
      final runtime = Runtime();
      final events = <String>[];
      final firstStopped = Completer<void>();
      final secondStopped = Completer<void>();
      final bothStarted = Completer<void>();
      var starts = 0;
      var stops = 0;
      final bothStopping = Completer<void>();
      Task<void, Never> work(String name, Completer<void> gate) =>
          Task.fromCallback<void, Never>((complete) {
            if (++starts == 2) bothStarted.complete();
            return () async {
              events.add('stop $name');
              if (++stops == 2) bothStopping.complete();
              await gate.future;
            };
          });
      final root = runtime.start(
        Task.addFinalizer(
              (_) => Task.sync(() {
                events.add('release parent');
              }),
            )
            .flatMap((_) => Scope.open())
            .flatMap(
              (scope) => work('first', firstStopped)
                  .forkIn(scope)
                  .flatMap((_) => work('second', secondStopped).fork())
                  .flatMap(
                    (_) => Task.fromCallback<void, Never>((complete) => () {}),
                  ),
            ),
      );
      await bothStarted.future;
      final closing = runtime.close();
      await bothStopping.future;
      expect(events, containsAll(['stop first', 'stop second']));
      expect(events, isNot(contains('release parent')));
      firstStopped.complete();
      await Future<void>.delayed(Duration.zero);
      expect(events, isNot(contains('release parent')));
      secondStopped.complete();
      expect(await closing, isA<ExitSuccess<void, Never>>());
      expect(events.last, 'release parent');
      expect(root.poll(), isA<OptionSome<Exit<void, Never>>>());
    },
  );

  test('cleanup can fork and join work after owner cancellation', () async {
    final runtime = Runtime();
    final started = Completer<void>();
    var cleaned = false;
    final work = runtime.run(
      Task.fromCallback<void, Never>((complete) {
        started.complete();
        return () {};
      }).ensuring(
        Task.sync(() {
          cleaned = true;
        }).fork().flatMap((fiber) => fiber.join()),
      ),
    );
    await started.future;
    await runtime.close();
    expect(await work, isA<ExitFailure<void, Never>>());
    expect(cleaned, isTrue);
  });
}
