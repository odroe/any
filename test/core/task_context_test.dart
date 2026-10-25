import 'dart:async';

import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  test(
    'service reads are lazy and runtime roots share installed instances',
    () async {
      final key = ServiceKey<Object>('service');
      final service = Object();
      final task = Task.service(key);
      final runtime = Runtime(context: Context.from([key.bind(service)]));
      expect(await runtime.runOrThrow(task), same(service));
      expect(await runtime.runOrThrow(task), same(service));
      await runtime.close();
    },
  );

  test('missing services are defects identifying the required key', () async {
    final runtime = Runtime();
    final key = ServiceKey<String>('required');
    final result = await runtime.run(Task.service(key));
    final defect = (result as ExitFailure).cause as CauseDefect;
    expect((defect.error as MissingService).key, same(key));
    await runtime.close();
  });

  test(
    'local contexts overlay matching services and inherit other services',
    () async {
      final first = ServiceKey<int>('first');
      final second = ServiceKey<String?>('second');
      final runtime = Runtime(
        context: Context.from([first.bind(1), second.bind(null)]),
      );
      final result = await runtime.runOrThrow(
        Task.service(first)
            .flatMap((a) => Task.service(second).map((b) => (a, b)))
            .provideContext(Context.from([first.bind(2)]))
            .flatMap(
              (local) => Task.service(first).map((outer) => (local, outer)),
            ),
      );
      expect(result, ((2, null), 1));
      await runtime.close();
    },
  );

  test(
    'the innermost provider wins and inherits other outer services',
    () async {
      final port = ServiceKey<int>('port');
      final host = ServiceKey<String>('host');
      final runtime = Runtime(context: Context.from([port.bind(443)]));
      addTearDown(runtime.close);

      final result = await runtime.runOrThrow(
        Task.service(port)
            .flatMap((value) => Task.service(host).map((host) => (host, value)))
            .provideContext(Context.from([port.bind(8080)]))
            .provideContext(
              Context.from([port.bind(80), host.bind('localhost')]),
            ),
      );

      expect(result, ('localhost', 8080));
      expect(await runtime.runOrThrow(Task.service(port)), 443);
    },
  );

  test(
    'failure recovery outside a local provider sees the outer context',
    () async {
      final key = ServiceKey<int>('service');
      final runtime = Runtime(context: Context.from([key.bind(1)]));
      final Task<int, String> failure = Task.fail('failed');
      final result = await runtime.runOrThrow(
        failure
            .provideContext(Context.from([key.bind(2)]))
            .recoverCause<Never>((_) => Task.service(key)),
      );
      expect(result, 1);
      await runtime.close();
    },
  );

  test('finalizers retain services from their registration context', () async {
    final key = ServiceKey<int>('service');
    final observed = <int>[];
    final runtime = Runtime(context: Context.from([key.bind(1)]));
    await runtime.runOrThrow(
      Task.addFinalizer(
        (_) => Task.service(key).map((value) {
          observed.add(value);
        }),
      ).provideContext(Context.from([key.bind(2)])),
    );
    expect(observed, [2]);
    await runtime.close();
  });

  test(
    'child fibers inherit their launch context without changing the parent',
    () async {
      final key = ServiceKey<int>('service');
      final runtime = Runtime(context: Context.from([key.bind(1)]));
      final result = await runtime.runOrThrow(
        Task.service(key)
            .fork()
            .provideContext(Context.from([key.bind(2)]))
            .flatMap(
              (child) => child.join().flatMap(
                (value) => Task.service(key).map((outer) => (value, outer)),
              ),
            ),
      );
      expect(result, (2, 1));
      await runtime.close();
    },
  );

  test('independent asynchronous roots cannot leak local providers', () async {
    final key = ServiceKey<int>('service');
    final gate = Completer<void>();
    final started = Completer<void>();
    final runtime = Runtime(context: Context.from([key.bind(1)]));
    final local = runtime.runOrThrow(
      Task.fromFuture(() {
            started.complete();
            return gate.future;
          }, onError: (error, stack) => error)
          .orDie()
          .flatMap((_) => Task.service(key))
          .provideContext(Context.from([key.bind(2)])),
    );
    await started.future;
    expect(await runtime.runOrThrow(Task.service(key)), 1);
    gate.complete();
    expect(await local, 2);
    await runtime.close();
  });

  test(
    'runtime installs clock and random defaults and accepts replacements',
    () async {
      final runtime = Runtime();
      final clock = await runtime.runOrThrow(Task.service(Clock.key));
      final random = await runtime.runOrThrow(Task.service(RandomSource.key));
      expect(await runtime.runOrThrow(Task.service(Clock.key)), same(clock));
      expect(
        await runtime.runOrThrow(Task.service(RandomSource.key)),
        same(random),
      );
      final replacement = RandomSource(seed: 1);
      final other = Runtime(
        context: Context.from([RandomSource.key.bind(replacement)]),
      );
      expect(
        await other.runOrThrow(Task.service(RandomSource.key)),
        same(replacement),
      );
      expect(
        await other.runOrThrow(Task.service(Clock.key)),
        isNot(same(clock)),
      );
      await runtime.close();
      await other.close();
    },
  );
}
