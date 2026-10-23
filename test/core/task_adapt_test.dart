import 'dart:async';

import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  test(
    'projects dependencies lazily, once per run, sharing scope and token',
    () async {
      final scope = Scope();
      final controller = CancellationController();
      final values = <String>[];
      final task =
          Task<int, String, String>((context) {
            expect(context.scope, same(scope));
            expect(context.cancellation, same(controller.token));
            return Exit.success(int.parse(context.dependencies));
          }).adapt<int, ({String input})>(
            dependencies: (deps) {
              values.add(deps.input);
              return deps.input;
            },
            error: (error) => error.length,
          );
      expect(values, isEmpty);
      for (final value in ['1', '2']) {
        final exit = await task.runIn(
          Context(
            scope: scope,
            dependencies: (input: value),
            cancellation: controller.token,
          ),
        );
        expect((exit as ExitSuccess).value, int.parse(value));
      }
      expect(values, ['1', '2']);
      expect(scope.isClosed, isFalse);
      await scope.close();
    },
  );

  test(
    'maps grouped failures while preserving defect and cancellation payloads',
    () async {
      final object = Object();
      final trace = StackTrace.current;
      final reason = Object();
      final task = Task<int, String, String>(
        (_) => Exit.failure(
          Cause.combine([
            const Cause.failure('bad'),
            Cause.defect(object, trace),
            Cause.cancelled(reason: reason),
          ]),
        ),
      ).adapt<int, ()>(dependencies: (_) => '', error: (error) => error.length);
      final causes =
          ((await task.run(()) as ExitFailure).cause as CauseGroup).causes;
      expect((causes[0] as CauseFailure).error, 3);
      expect((causes[1] as CauseDefect).error, same(object));
      expect((causes[1] as CauseDefect).stackTrace, same(trace));
      expect((causes[2] as CauseCancelled).reason, same(reason));
    },
  );

  test(
    'throws from projections and translators become original defects',
    () async {
      final error = Object();
      final trace = StackTrace.current;
      final source = Task<int, String, int>(
        (_) => const Exit.failure(Cause.failure('bad')),
      );
      for (final task in [
        source.adapt<int, ()>(
          dependencies: (_) => Error.throwWithStackTrace(error, trace),
          error: (e) => e.length,
        ),
        source.adapt<int, ()>(
          dependencies: (_) => 0,
          error: (_) => Error.throwWithStackTrace(error, trace),
        ),
      ]) {
        final cause = (await task.run(()) as ExitFailure).cause as CauseDefect;
        expect(cause.error, same(error));
        expect(cause.stackTrace, same(trace));
      }
    },
  );

  test(
    'pre-cancellation and closed scopes skip projection and execution',
    () async {
      final task = Task<int, String, String>((_) => fail('No body.'))
          .adapt<int, ()>(
            dependencies: (_) => fail('No projection.'),
            error: (_) => fail('No mapping.'),
          );
      final controller = CancellationController()..cancel();
      expect(
        (await task.run(
          (),
          cancellation: controller.token,
        ) as ExitFailure).cause,
        isA<CauseCancelled>(),
      );
      final scope = Scope();
      await scope.close();
      expect(
        (await task.runIn(
          Context(scope: scope, dependencies: ()),
        ) as ExitFailure).cause,
        isA<CauseDefect>(),
      );
    },
  );

  test('cancellation from projection skips source computation', () async {
    final controller = CancellationController();
    final task = Task<int, Never, int>((_) => fail('No body.'))
        .adapt<String, ()>(
          dependencies: (_) {
            controller.cancel(reason: 'project');
            return 0;
          },
          error: (Never error) => error,
        );
    final cause =
        (await task.run(
              (),
              cancellation: controller.token,
            ) as ExitFailure).cause
            as CauseCancelled;
    expect(cause.reason, 'project');
  });

  test(
    'adapted asynchronous work is still awaited after cancellation',
    () async {
      final started = Completer<void>();
      final finish = Completer<Exit<int, String>>();
      final controller = CancellationController();
      final task = Task<int, String, int>((_) {
        started.complete();
        return finish.future;
      }).adapt<int, ()>(dependencies: (_) => 0, error: (e) => e.length);
      var done = false;
      final running = task.run((), cancellation: controller.token).then((exit) {
        done = true;
        return exit;
      });
      await started.future;
      controller.cancel();
      expect(done, isFalse);
      finish.complete(const Exit.failure(Cause.failure('actual')));
      expect(((await running as ExitFailure).cause as CauseFailure).error, 6);
    },
  );

  test(
    'Never sources and nullable successes adapt without runtime type loss',
    () async {
      final Task<int?, Object, int> source = Task<int?, Never, int>(
        (_) => const Exit.success(null),
      );
      final task = source.adapt<String, String>(
        dependencies: int.parse,
        error: (e) => '$e',
      );
      expect((await task.run('1') as ExitSuccess).value, isNull);
    },
  );

  test('deep dependency views remain stack safe', () async {
    var task = Task<int, String, int>(
      (context) => Exit.success(context.dependencies),
    );
    for (var i = 0; i < 10000; i++) {
      task = task.adapt<String, int>(
        dependencies: (value) => value + 1,
        error: (error) => error,
      );
    }
    var microtasks = 0;
    final exit = await runZoned(
      () => task.run(0),
      zoneSpecification: ZoneSpecification(
        scheduleMicrotask: (self, parent, zone, callback) {
          microtasks++;
          parent.scheduleMicrotask(zone, callback);
        },
      ),
    );
    expect((exit as ExitSuccess).value, 10000);
    expect(microtasks, lessThan(10));
  });

  test('restores outer dependencies before later steps and recovery', () async {
    final inner = Task<int, int, int>(
      (context) => context.dependencies < 0
          ? Exit.failure(Cause.failure(context.dependencies))
          : Exit.success(context.dependencies),
    );
    final adapted = inner.adapt<String, String>(
      dependencies: int.parse,
      error: (e) => '$e',
    );
    final success = adapted.flatMap(
      (value) => Task<String, String, String>(
        (context) => Exit.success('${context.dependencies}:$value'),
      ),
    );
    expect((await success.run('42') as ExitSuccess).value, '42:42');
    final recovered = adapted
        .map((value) => '$value')
        .recover(
          (error) =>
              Task((context) => Exit.success('${context.dependencies}:$error')),
        );
    expect((await recovered.run('-1') as ExitSuccess).value, '-1:-1');
  });

  test(
    'dynamic nested adaptations restore every context after asynchronous work',
    () async {
      final task =
          Task<String, String, String>(
                (context) => Exit.success(context.dependencies),
              )
              .flatMap(
                (outer) =>
                    Task<int, int, int>(
                          (context) async => Exit.success(context.dependencies),
                        )
                        .adapt<String, String>(
                          dependencies: int.parse,
                          error: (e) => '$e',
                        )
                        .flatMap(
                          (inner) => Task(
                            (context) => Exit.success(
                              '$outer/$inner/${context.dependencies}',
                            ),
                          ),
                        ),
              )
              .adapt<String, ({String text})>(
                dependencies: (deps) => deps.text,
                error: (e) => e,
              )
              .flatMap(
                (value) => Task(
                  (context) =>
                      Exit.success('$value/${context.dependencies.text}'),
                ),
              );
      expect((await task.run((text: '2')) as ExitSuccess).value, '2/2/2/2');
    },
  );

  test(
    'closing the scope in a projection takes priority over cancellation',
    () async {
      final scope = Scope();
      final controller = CancellationController();
      final task = Task<int, String, int>((_) => fail('No body.'))
          .adapt<String, ()>(
            dependencies: (_) {
              unawaited(scope.close());
              controller.cancel();
              return 0;
            },
            error: (error) => error,
          );
      final exit = await task.runIn(
        Context(scope: scope, dependencies: (), cancellation: controller.token),
      ) as ExitFailure;
      expect((exit.cause as CauseDefect).error, isStateError);
      await scope.close();
    },
  );
}
