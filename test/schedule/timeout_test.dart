import 'dart:async';

import 'package:any/any.dart';
import 'package:any/schedule.dart';
import 'package:test/test.dart';

void main() {
  test(
    'expiry requests cancellation, drains body, and then cleans up',
    () async {
      final started = Completer<void>();
      final finish = Completer<void>();
      final controller = CancellationController();
      final timers = <_Timer>[];
      Object? reason;
      var cleaned = false;
      var done = false;
      final task = Task<int, String, ()>((context) async {
        context.cancellation.onCancel((value) => reason = value);
        context.scope.addFinalizer((_) => cleaned = true);
        started.complete();
        await finish.future;
        return const Exit.success(1);
      }).timeout(const Duration(seconds: 5));
      final running =
          _withTimers(
            timers,
            () => task.run((), cancellation: controller.token),
          ).then((exit) {
            done = true;
            return exit;
          });
      await started.future;
      timers.single.fire();
      expect(
        reason,
        isA<TimeoutException>().having(
          (e) => e.duration,
          'duration',
          const Duration(seconds: 5),
        ),
      );
      expect(controller.token.isCancelled, isFalse);
      expect(cleaned, isFalse);
      expect(done, isFalse);
      finish.complete();
      final exit = await running as ExitFailure;
      expect((exit.cause as CauseCancelled).reason, same(reason));
      expect(cleaned, isTrue);
      expect(timers.single.isActive, isFalse);
    },
  );

  test('late body failure and cleanup errors survive expiry', () async {
    final started = Completer<void>();
    final body = Completer<Exit<int, String>>();
    final timers = <_Timer>[];
    final defect = Object();
    final task = Task<int, String, ()>((context) {
      context.scope.addFinalizer((_) => throw defect);
      started.complete();
      return body.future;
    }).timeout(Duration.zero);
    final running = _withTimers(timers, () => task.run(()));
    await started.future;
    timers.single.fire();
    body.complete(const Exit.failure(Cause.failure('actual failure')));
    final cause = (await running as ExitFailure).cause as CauseGroup;
    expect((cause.causes[0] as CauseFailure).error, 'actual failure');
    expect((cause.causes[1] as CauseDefect).error, same(defect));
  });

  test('parent cancellation wins and cancels the deadline timer', () async {
    final started = Completer<void>();
    final body = Completer<Exit<int, Never>>();
    final controller = CancellationController();
    final reason = Object();
    final timers = <_Timer>[];
    final task = Task<int, Never, ()>((_) {
      started.complete();
      return body.future;
    }).timeout(const Duration(days: 1));
    final running = _withTimers(
      timers,
      () => task.run((), cancellation: controller.token),
    );
    await started.future;
    controller.cancel(reason: reason);
    expect(timers.single.isActive, isFalse);
    timers.single.fire();
    body.complete(const Exit.success(1));
    expect(
      ((await running as ExitFailure).cause as CauseCancelled).reason,
      same(reason),
    );
  });

  test('completion removes timer and leaves borrowed scope open', () async {
    final timers = <_Timer>[];
    final scope = Scope();
    final dependency = Object();
    final controller = CancellationController();
    CancellationToken? child;
    var cleaned = false;
    final task = Task<int, Never, Object>((context) {
      expect(context.scope, same(scope));
      expect(context.dependencies, same(dependency));
      child = context.cancellation;
      context.scope.addFinalizer((_) => cleaned = true);
      return const Exit.success(1);
    }).timeout(const Duration(days: 1));
    final exit = await _withTimers(
      timers,
      () => task.runIn(
        Context(
          scope: scope,
          dependencies: dependency,
          cancellation: controller.token,
        ),
      ),
    );
    expect(exit, isA<ExitSuccess>());
    expect(timers.single.isActive, isFalse);
    controller.cancel();
    expect(child!.isCancelled, isFalse);
    expect(scope.isClosed, isFalse);
    expect(cleaned, isFalse);
    await scope.close();
    expect(cleaned, isTrue);
  });

  test('pre-cancellation starts neither timer nor body', () async {
    final timers = <_Timer>[];
    final controller = CancellationController()..cancel();
    final exit = await _withTimers(
      timers,
      () =>
          Task<int, Never, ()>((_) => fail('No body.'))
              .timeout(Duration.zero)
              .run((), cancellation: controller.token),
    );
    expect((exit as ExitFailure).cause, isA<CauseCancelled>());
    expect(timers, isEmpty);
  });

  test('cleanup is outside the deadline', () async {
    final timers = <_Timer>[];
    final started = Completer<void>();
    final cleanup = Completer<void>();
    final task = Task<int, Never, ()>((context) {
      context.scope.addFinalizer((exit) async {
        started.complete();
        await cleanup.future;
        expect(exit, isA<ExitSuccess>());
      });
      return const Exit.success(1);
    }).timeout(Duration.zero);
    final running = _withTimers(timers, () => task.run(()));
    await started.future;
    expect(timers.single.isActive, isFalse);
    timers.single.fire();
    cleanup.complete();
    expect(await running, isA<ExitSuccess>());
  });
}

T _withTimers<T>(List<_Timer> timers, T Function() body) => runZoned(
  body,
  zoneSpecification: ZoneSpecification(
    createTimer: (_, _, _, _, callback) {
      final timer = _Timer(callback);
      timers.add(timer);
      return timer;
    },
  ),
);

final class _Timer implements Timer {
  _Timer(this.callback);
  final void Function() callback;
  @override
  bool isActive = true;
  @override
  int tick = 0;
  @override
  void cancel() => isActive = false;
  void fire() {
    if (!isActive) return;
    isActive = false;
    tick++;
    callback();
  }
}
