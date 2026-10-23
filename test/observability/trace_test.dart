import 'dart:async';

import 'package:any/any.dart';
import 'package:any/observability.dart';
import 'package:test/test.dart';

void main() {
  test('tracing is lazy and retains each typed outcome', () async {
    final spans = <TaskSpan<int, String>>[];
    const original = Exit<int, String>.success(1);
    final task = Task<int, String, ()>((_) => original)
        .trace('read', onEnd: spans.add);
    expect(spans, isEmpty);
    expect(await task.run(()), same(original));
    expect(await task.run(()), same(original));
    expect(spans, hasLength(2));
    expect(spans[0], isNot(same(spans[1])));
    for (final span in spans) {
      expect(span.name, 'read');
      expect(span.exit, same(original));
      expect(span.elapsed.isNegative, isFalse);
    }
  });

  test('observes defects, expected errors, cancellation, and groups', () async {
    final causes = <Cause<String>>[
      const Cause.failure('missing'),
      Cause.defect(Object(), StackTrace.current),
      const Cause.cancelled(reason: 'stop'),
      Cause.combine([const Cause.failure('one'), const Cause.failure('two')]),
    ];
    for (final cause in causes) {
      TaskSpan<int, String>? span;
      final exit = await Task<int, String, ()>((_) => Exit.failure(cause))
          .trace('work', onEnd: (value) => span = value)
          .run(());
      expect(span!.exit, same(exit));
      expect((span!.exit as ExitFailure).cause, same(cause));
    }
  });

  test(
    'observer errors go to its execution zone and keep the outcome',
    () async {
      final error = Object();
      final trace = StackTrace.current;
      final errors = <(Object, StackTrace)>[];
      final task = Task<int, Never, ()>((_) => const Exit.success(3))
          .trace('work', onEnd: (_) => Error.throwWithStackTrace(error, trace));
      final exit = await runZonedGuarded(
        () => task.run(()),
        (e, s) => errors.add((e, s)),
      )!;
      expect((exit as ExitSuccess).value, 3);
      expect(errors, hasLength(1));
      expect(errors.single.$1, same(error));
      expect(errors.single.$2, same(trace));
    },
  );

  test('observation awaits the body and excludes enclosing cleanup', () async {
    final started = Completer<void>();
    final finish = Completer<void>();
    final spans = <TaskSpan<int, Never>>[];
    final cleanupError = Object();
    final task = Task<int, Never, ()>((context) async {
      context.scope.addFinalizer((_) {
        expect(spans, hasLength(1));
        throw cleanupError;
      });
      started.complete();
      await finish.future;
      return const Exit.success(1);
    }).trace('work', onEnd: spans.add);
    final running = task.run(());
    await started.future;
    expect(spans, isEmpty);
    finish.complete();
    final exit = await running as ExitFailure;
    expect(spans.single.exit, isA<ExitSuccess>());
    expect((exit.cause as CauseDefect).error, same(cleanupError));
  });

  test(
    'nested traces retain the same context and complete inside out',
    () async {
      final scope = Scope();
      final context = Context(scope: scope, dependencies: Object());
      final names = <String>[];
      final task =
          Task<int, Never, Object>((current) {
                expect(current, same(context));
                return const Exit.success(1);
              })
              .trace('inner', onEnd: (span) => names.add(span.name))
              .trace('outer', onEnd: (span) => names.add(span.name));
      expect(await task.runIn(context), isA<ExitSuccess>());
      expect(names, ['inner', 'outer']);
      expect(scope.isClosed, isFalse);
      await scope.close();
    },
  );

  test('pre-cancelled and closed contexts do not emit spans', () async {
    final controller = CancellationController()..cancel();
    final task = Task<int, Never, ()>((_) => fail('No body.'))
        .trace('work', onEnd: (_) => fail('No span.'));
    expect(
      (await task.run((), cancellation: controller.token) as ExitFailure).cause,
      isA<CauseCancelled>(),
    );
    final scope = Scope();
    await scope.close();
    final exit = await task.runIn(
      Context(scope: scope, dependencies: ()),
    ) as ExitFailure;
    expect((exit.cause as CauseDefect).error, isStateError);
  });
}
