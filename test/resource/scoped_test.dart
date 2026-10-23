import 'dart:async';

import 'package:any/any.dart';
import 'package:any/concurrency.dart';
import 'package:any/resource.dart';
import 'package:test/test.dart';

void main() {
  test('each run owns a scope and inherits the execution context', () async {
    final owner = Scope();
    final controller = CancellationController();
    final dependencies = Object();
    final context = Context(
      scope: owner,
      dependencies: dependencies,
      cancellation: controller.token,
      yieldEvery: 32,
    );
    final scopes = <Scope>[];
    final task = scoped(
      Task<int, Never, Object>((child) {
        expect(child.dependencies, same(dependencies));
        expect(child.cancellation, same(controller.token));
        expect(child.yieldEvery, 32);
        scopes.add(child.scope);
        return const Exit.success(42);
      }),
    );
    for (var i = 0; i < 2; i++) {
      expect((await task.runIn(context) as ExitSuccess).value, 42);
    }
    expect(scopes.toSet(), hasLength(2));
    expect(scopes.every((scope) => scope.isClosed), isTrue);
    expect(owner.isClosed, isFalse);
    await owner.close();
  });

  test('nested scopes release in order before the next step', () async {
    final events = <String>[];
    final task = scoped(
      Task<void, Never, ()>((context) {
        context.scope.addFinalizer((_) => events.add('outer'));
        return scoped(
          Task<void, Never, ()>((child) {
            child.scope.addFinalizer((_) => events.add('first'));
            child.scope.addFinalizer((_) => events.add('second'));
            return const Exit.success(null);
          }),
        ).runIn(context);
      }),
    ).map((_) => events.add('next'));
    expect(await task.run(()), isA<ExitSuccess>());
    expect(events, ['second', 'first', 'outer', 'next']);
  });

  test('preserves body failure followed by cleanup defects', () async {
    final cleanup = StateError('cleanup');
    final task = scoped(
      Task<void, String, ()>((context) {
        context.scope.addFinalizer((_) => throw cleanup);
        return const Exit.failure(Cause.failure('body'));
      }),
    );
    final exit = await task.run(()) as ExitFailure;
    final causes = (exit.cause as CauseGroup).causes;
    expect(causes, hasLength(2));
    expect((causes.first as CauseFailure).error, 'body');
    expect((causes.last as CauseDefect).error, same(cleanup));
  });

  test('cancellation waits for the body and asynchronous cleanup', () async {
    final controller = CancellationController();
    final started = Completer<void>();
    final body = Completer<void>();
    final closing = Completer<void>();
    final cleanup = Completer<void>();
    var done = false;
    final task = scoped(
      Task<void, Never, ()>((context) async {
        context.scope.addFinalizer((exit) {
          expect((exit as ExitFailure).cause, isA<CauseCancelled>());
          closing.complete();
          return cleanup.future;
        });
        started.complete();
        await body.future;
        return const Exit.success(null);
      }),
    );
    final result = task.run((), cancellation: controller.token).then((exit) {
      done = true;
      return exit;
    });
    await started.future;
    controller.cancel(reason: 'stop');
    expect(closing.isCompleted, isFalse);
    body.complete();
    await closing.future;
    expect(done, isFalse);
    cleanup.complete();
    expect((await result as ExitFailure).cause, isA<CauseCancelled>());
  });

  test('pre-cancellation skips acquisition and scoped body', () async {
    final controller = CancellationController()..cancel();
    var ran = false;
    final task = scoped(
      Task<void, Never, ()>((_) {
        ran = true;
        return const Exit.success(null);
      }),
    );
    expect(
      (await task.run((), cancellation: controller.token) as ExitFailure).cause,
      isA<CauseCancelled>(),
    );
    expect(ran, isFalse);
  });

  test(
    'a permit preserves acquired resources until the explicit boundary',
    () async {
      final events = <String>[];
      final acquire = acquireRelease<List<String>, Never, ()>(
        acquire: (_) => Exit.success(events),
        release: (value, _) => value.add('released'),
      );
      final task =
          scoped(
            Semaphore(1).withPermit(acquire).map((value) {
              expect(value, isEmpty);
              value.add('used');
              return value;
            }),
          ).map((value) {
            expect(value, ['used', 'released']);
            return value.length;
          });
      expect((await task.run(()) as ExitSuccess).value, 2);
    },
  );

  test('sequential iterations release resources before reacquiring', () async {
    var active = 0;
    var peak = 0;
    final step = scoped(
      acquireRelease<void, Never, ()>(
        acquire: (_) {
          active++;
          if (active > peak) peak = active;
          return const Exit.success(null);
        },
        release: (_, _) => active--,
      ),
    );
    Task<void, Never, ()> task = Task((_) => const Exit.success(null));
    for (var i = 0; i < 1000; i++) {
      task = task.flatMap((_) => step);
    }
    expect(await task.run(()), isA<ExitSuccess>());
    expect(active, 0);
    expect(peak, 1);
  });
}
