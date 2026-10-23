import 'dart:async';

import 'package:any/any.dart';
import 'package:any/dependency.dart';
import 'package:any/resource.dart';
import 'package:test/test.dart';

void main() {
  test('typed records flow from a lazy builder into its consumer', () async {
    var builds = 0;
    final build = Task<({int port}), Never, String>((context) {
      builds++;
      return Exit.success((port: int.parse(context.dependencies)));
    });
    final read = Task<int, Never, ({int port})>(
      (context) => Exit.success(context.dependencies.port),
    );
    final task = read.provide(build);
    expect(builds, 0);
    expect((await task.run('8080') as ExitSuccess).value, 8080);
    expect((await task.run('9090') as ExitSuccess).value, 9090);
    expect(builds, 2);
  });

  test(
    'resource dependencies live through the workflow and release in order',
    () async {
      final events = <String>[];
      var open = false;
      Scope? builderScope;
      CancellationToken? token;
      final build = acquireRelease<({Object service}), String, ()>(
        acquire: (context) {
          builderScope = context.scope;
          token = context.cancellation;
          open = true;
          return Exit.success((service: Object()));
        },
        release: (_, exit) {
          expect(exit, isA<ExitSuccess>());
          events.add('close service');
          open = false;
        },
      );
      final use = Task<int, String, ({Object service})>((context) {
        expect(context.scope, same(builderScope));
        expect(context.cancellation, same(token));
        expect(open, isTrue);
        context.scope.addFinalizer((_) {
          expect(open, isTrue);
          events.add('close consumer');
        });
        return const Exit.success(1);
      });
      final exit = await use
          .provide(build)
          .map((value) {
            expect(open, isTrue);
            events.add('after consumer');
            return value;
          })
          .run(());
      expect(exit, isA<ExitSuccess>());
      expect(open, isFalse);
      expect(events, ['after consumer', 'close consumer', 'close service']);
    },
  );

  test(
    'builder failure skips the consumer and releases partial resources',
    () async {
      var closed = false;
      final build = Task<int, String, ()>((context) {
        context.scope.addFinalizer((_) => closed = true);
        return const Exit.failure(Cause.failure('configuration'));
      });
      final exit = await Task<int, String, int>((_) => fail('No consumer.'))
          .provide(build)
          .run(());
      expect(
        ((exit as ExitFailure).cause as CauseFailure).error,
        'configuration',
      );
      expect(closed, isTrue);
    },
  );

  test(
    'late acquisition is released after cancellation without calling consumer',
    () async {
      final started = Completer<void>();
      final acquired = Completer<Exit<Object, Never>>();
      final controller = CancellationController();
      var released = false;
      final build = acquireRelease<Object, Never, ()>(
        acquire: (_) {
          started.complete();
          return acquired.future;
        },
        release: (_, _) => released = true,
      );
      final task = Task<int, Never, Object>((_) => fail('No consumer.'))
          .provide(build);
      final running = task.run((), cancellation: controller.token);
      await started.future;
      controller.cancel(reason: 'stop');
      acquired.complete(Exit.success(Object()));
      expect(
        ((await running as ExitFailure).cause as CauseCancelled).reason,
        'stop',
      );
      expect(released, isTrue);
    },
  );

  test(
    'borrowed scope stays open and concurrent builders stay independent',
    () async {
      final firstScope = Scope();
      final secondScope = Scope();
      final built = <Object>[];
      var closed = 0;
      final build = acquireRelease<Object, Never, ()>(
        acquire: (_) {
          final value = Object();
          built.add(value);
          return Exit.success(value);
        },
        release: (_, _) => closed++,
      );
      final use = Task<Object, Never, Object>(
        (context) => Exit.success(context.dependencies),
      );
      final task = use.provide(build);
      final exits = await Future.wait([
        task.runIn(Context(scope: firstScope, dependencies: ())),
        task.runIn(Context(scope: secondScope, dependencies: ())),
      ]);
      expect(
        exits.cast<ExitSuccess>().map((e) => e.value).toSet(),
        built.toSet(),
      );
      expect(built.toSet(), hasLength(2));
      expect(closed, 0);
      await firstScope.close();
      await secondScope.close();
      expect(closed, 2);
    },
  );

  test('a Never builder can supply a fallible consumer', () async {
    final build = Task<int, Never, ()>((_) => const Exit.success(1));
    final consumer = Task<String, String, int>(
      (_) => const Exit.failure(Cause.failure('missing')),
    );
    final exit = await consumer.provide(build).run(());
    expect(((exit as ExitFailure).cause as CauseFailure).error, 'missing');
  });
}
