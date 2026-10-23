import 'package:any/any.dart';
import 'package:any/src/core/fiber.dart' as fibers;
import 'package:any/src/core/scope.dart' as scopes;
import 'package:test/test.dart';

void main() {
  test(
    'deferred construction registers cancellation before it starts',
    () async {
      final owner = scopes.createScope();
      final controller = CancellationController();
      var started = false;
      final fiber = fibers.startFiber(
        Task.sync(() => started = true),
        owner: owner,
        context: Context.from([]),
        deferStart: true,
        signal: controller.signal,
        report: (_) => fail('Unexpected report'),
      );
      expect(started, isFalse);
      controller.requestCancel(reason: 'before start');
      final exit = await fibers.fiberCompletion(fiber);
      expect(
        ((exit as ExitFailure).cause as CauseCancelled).reason,
        'before start',
      );
      expect(started, isFalse);
      expect(await scopes.closeScope(owner, const Exit.success(null)), isNull);
    },
  );

  test(
    'an owner can close a deferred worker before its business starts',
    () async {
      final owner = scopes.createScope();
      var started = false;
      final fiber = fibers.startFiber(
        Task.sync(() => started = true),
        owner: owner,
        context: Context.from([]),
        deferStart: true,
        report: (_) => fail('Unexpected report'),
      );
      final closed = scopes.closeScope(owner, const Exit.success(null));
      expect(await closed, isNull);
      expect(
        (await fibers.fiberCompletion(fiber) as ExitFailure).cause,
        isA<CauseCancelled>(),
      );
      expect(started, isFalse);
    },
  );
}
