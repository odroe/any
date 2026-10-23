import 'dart:async';

import 'package:any/any.dart';
import 'package:any/concurrency.dart';
import 'package:any/resource.dart';
import 'package:test/test.dart';

void main() {
  test('validates capacity', () {
    expect(() => Semaphore(0), throwsRangeError);
    expect(() => Semaphore(-1), throwsRangeError);
  });

  test('explicit scopes retain shared permits through cleanup', () async {
    final semaphore = Semaphore(2);
    final release = Completer<void>();
    final started = Completer<void>();
    var active = 0;
    var peak = 0;
    var completed = 0;
    final task = semaphore.withPermit(
      scoped(
        Task<int, Never, ()>((context) {
          active++;
          if (active > peak) peak = active;
          if (active == 2 && !started.isCompleted) started.complete();
          context.scope.addFinalizer((_) async {
            await release.future;
            active--;
            completed++;
          });
          return const Exit.success(1);
        }),
      ),
    );
    final batches = [
      for (var i = 0; i < 2; i++)
        all(List.filled(5, task), concurrency: 5).run(()),
    ];
    await started.future;
    await Future<void>.delayed(Duration.zero);
    expect(active, 2);
    expect(completed, 0);
    release.complete();
    expect(await Future.wait(batches), everyElement(isA<ExitSuccess>()));
    expect(peak, 2);
    expect(completed, 10);
    expect(active, 0);
  });

  test('waiters are FIFO and cancellation removes a queued waiter', () async {
    final semaphore = Semaphore(1);
    final gate = Completer<void>();
    final started = Completer<void>();
    final order = <int>[];
    Task<int, Never, ()> task(int id) => semaphore.withPermit(
      Task((_) async {
        order.add(id);
        if (id == 0) {
          started.complete();
          await gate.future;
        }
        return Exit.success(id);
      }),
    );
    final first = task(0).run(());
    await started.future;
    final controller = CancellationController();
    final cancelled = task(1).run((), cancellation: controller.token);
    final second = task(2).run(());
    final third = task(3).run(());
    await Future<void>.delayed(Duration.zero);
    controller.cancel(reason: 'skip');
    final exit = await cancelled as ExitFailure;
    expect((exit.cause as CauseCancelled).reason, 'skip');
    gate.complete();
    await Future.wait([first, second, third]);
    expect(order, [0, 2, 3]);
  });

  test('failures and cleanup defects release the permit', () async {
    final semaphore = Semaphore(1);
    final defect = StateError('close');
    final bad = semaphore.withPermit(
      Task<int, String, ()>((context) {
        context.scope.addFinalizer((_) => throw defect);
        return const Exit.failure(Cause.failure('body'));
      }),
    );
    final exit = await bad.run(()) as ExitFailure;
    final causes = (exit.cause as CauseGroup).causes;
    expect((causes[0] as CauseFailure).error, 'body');
    expect((causes[1] as CauseDefect).error, same(defect));
    final good = semaphore.withPermit(
      Task<int, Never, ()>((_) => const Exit.success(2)),
    );
    expect((await good.run(()) as ExitSuccess).value, 2);
  });

  test('granted then cancelled waiter releases its permit', () async {
    final semaphore = Semaphore(1);
    final controller = CancellationController();
    final gate = Completer<void>();
    final started = Completer<void>();
    var cancelAtHandoff = false;
    final first = semaphore
        .withPermit(
          scoped(
            Task<void, Never, ()>((context) async {
              context.scope.addFinalizer((_) => cancelAtHandoff = true);
              started.complete();
              await gate.future;
              return const Exit.success(null);
            }),
          ),
        )
        .run(());
    await started.future;
    final next = semaphore.withPermit(
      Task<void, Never, ()>((_) => const Exit.success(null)),
    );
    final cancelled = runZoned(
      () => next.run((), cancellation: controller.token),
      zoneSpecification: ZoneSpecification(
        scheduleMicrotask: (self, parent, zone, callback) {
          if (cancelAtHandoff) {
            cancelAtHandoff = false;
            controller.cancel();
          }
          parent.scheduleMicrotask(zone, callback);
        },
      ),
    );
    await Future<void>.delayed(Duration.zero);
    gate.complete();
    await first;
    expect((await cancelled as ExitFailure).cause, isA<CauseCancelled>());
    expect(await next.run(()), isA<ExitSuccess>());
  });

  test(
    'preserves the context and leaves resource ownership with the caller',
    () async {
      final semaphore = Semaphore(1);
      final scope = Scope();
      final dependencies = Object();
      final children = <Context<Object>>[];
      final task = semaphore.withPermit(
        Task<Object, Never, Object>((context) {
          children.add(context);
          return Exit.success(context.dependencies);
        }),
      );
      final context = Context(scope: scope, dependencies: dependencies);
      for (var i = 0; i < 2; i++) {
        expect(
          (await task.runIn(context) as ExitSuccess).value,
          same(dependencies),
        );
      }
      expect(children, everyElement(same(context)));
      expect(scope.isClosed, isFalse);
      await scope.close();
    },
  );

  test('ten thousand queued runs complete without leaking permits', () async {
    final semaphore = Semaphore(1);
    final task = semaphore.withPermit(
      Task<int?, Never, ()>((_) => const Exit.success(null)),
    );
    final outcomes = await Future.wait(
      List.generate(10000, (_) => task.run(())),
    );
    expect(outcomes, everyElement(isA<ExitSuccess<int?, Never>>()));
    expect(await task.run(()), isA<ExitSuccess>());
  });
}
