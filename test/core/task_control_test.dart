import 'dart:async';

import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  late Runtime runtime;
  setUp(() {
    runtime = Runtime();
    addTearDown(runtime.close);
  });

  test('when evaluates its condition lazily on every execution', () async {
    var enabled = false;
    var conditions = 0;
    var bodies = 0;
    final task = Task.when(() {
      conditions++;
      return enabled;
    }, Task.sync(() => ++bodies));
    expect(conditions, 0);
    expect(await runtime.runOrThrow(task), isA<OptionNone<int>>());
    enabled = true;
    expect((await runtime.runOrThrow(task) as OptionSome<int>).value, 1);
    expect(conditions, 2);
    expect(bodies, 1);
  });

  test('when distinguishes a present null and propagates failures', () async {
    expect(
      await runtime.runOrThrow(Task.when(() => true, Task.succeed<int?>(null))),
      isA<OptionSome<int?>>(),
    );
    final exit = await runtime.run(Task.when(() => true, Task.fail('failure')));
    expect((_cause(exit) as CauseFailure<String>).error, 'failure');
  });

  test('a thrown condition never starts the body', () async {
    final error = StateError('condition');
    var bodies = 0;
    final exit = await runtime.run(
      Task.when(() => throw error, Task.sync(() => bodies++)),
    );
    expect((_cause(exit) as CauseDefect).error, same(error));
    expect(bodies, 0);
  });

  test(
    'loop tests before each step and returns the first rejected state',
    () async {
      final states = <int>[];
      final task = Task.loop<int, Never>(
        0,
        whileTrue: (state) {
          states.add(state);
          return state < 3;
        },
        step: (state) => Task.succeed(state + 1),
      );
      expect(states, isEmpty);
      expect(await runtime.runOrThrow(task), 3);
      expect(states, [0, 1, 2, 3]);
      states.clear();
      expect(await runtime.runOrThrow(task), 3);
      expect(states, [0, 1, 2, 3]);
    },
  );

  test('loop failure stops later conditions and steps', () async {
    final states = <int>[];
    final exit = await runtime.run(
      Task.loop<int, String>(
        0,
        whileTrue: (_) => true,
        step: (state) {
          states.add(state);
          return state == 2 ? Task.fail('stop') : Task.succeed(state + 1);
        },
      ),
    );
    expect((_cause(exit) as CauseFailure<String>).error, 'stop');
    expect(states, [0, 1, 2]);
  });

  test('loop callback exceptions retain their original stack', () async {
    final error = StateError('loop');
    final stack = StackTrace.fromString('original loop stack');
    final exit = await runtime.run(
      Task.loop<int, Never>(
        0,
        whileTrue: (_) => true,
        step: (_) => Error.throwWithStackTrace(error, stack),
      ),
    );
    final cause = _cause(exit) as CauseDefect;
    expect(cause.error, same(error));
    expect(cause.stackTrace.toString(), stack.toString());
  });

  test('large loops remain stack safe and cooperative', () async {
    expect(
      await runtime.runOrThrow(
        Task.loop<int, Never>(
          0,
          whileTrue: (value) => value < 10000,
          step: (value) => Task.succeed(value + 1),
        ),
      ),
      10000,
    );
    final cancel = CancellationController();
    final running = runtime.run(
      Task.loop<int, Never>(
        0,
        whileTrue: (_) => true,
        step: (value) => Task.succeed(value + 1),
      ),
      signal: cancel.signal,
    );
    Timer.run(cancel.requestCancel);
    expect(_cause(await running), isA<CauseCancelled>());
  });

  test(
    'fold iterates lazily, sequentially, and independently per run',
    () async {
      final seen = <int>[];
      Iterable<int> values() sync* {
        for (var i = 1; i <= 3; i++) {
          seen.add(i);
          yield i;
        }
      }

      final task = Task.fold<int, int, Never>(
        values(),
        0,
        (sum, value) => Task.sync(() {
          expect(seen.last, value);
          return sum + value;
        }),
      );
      expect(seen, isEmpty);
      expect(await runtime.runOrThrow(task), 6);
      expect(await runtime.runOrThrow(task), 6);
      expect(seen, [1, 2, 3, 1, 2, 3]);
      expect(
        await runtime.runOrThrow(
          Task.fold<int, int, Never>(
            [],
            5,
            (sum, value) => Task.succeed(sum + value),
          ),
        ),
        5,
      );
    },
  );

  test('fold stops reading inputs after a business failure', () async {
    final seen = <int>[];
    Iterable<int> values() sync* {
      for (var i = 0; i < 4; i++) {
        seen.add(i);
        yield i;
      }
    }

    final exit = await runtime.run(
      Task.fold<int, int, String>(
        values(),
        0,
        (sum, value) =>
            value == 1 ? Task.fail('stop') : Task.succeed(sum + value),
      ),
    );
    expect((_cause(exit) as CauseFailure<String>).error, 'stop');
    expect(seen, [0, 1]);
  });

  test('fold iteration errors retain previously registered cleanup', () async {
    final error = StateError('iterator');
    final events = <int>[];
    Iterable<int> values() sync* {
      yield 1;
      throw error;
    }

    final task = Task.fold<int, int, Never>(
      values(),
      0,
      (sum, value) =>
          Task.addFinalizer((_) => Task.sync(() => events.add(value)))
              .map((_) => sum + value),
    );
    expect((_cause(await runtime.run(task)) as CauseDefect).error, same(error));
    expect(events, [1]);
  });

  test('large folds are stack safe and keep widened error types', () async {
    final Task<int, String> seed = Task.succeed(1);
    final task = Task.fold<int, int, String>(
      List.generate(10000, (i) => i),
      0,
      (sum, _) => seed.map((value) => sum + value),
    );
    expect(await runtime.runOrThrow(task), 10000);
    final failed = task.flatMap<int>((_) => Task.fail('typed'));
    expect(
      (_cause(await runtime.run(failed)) as CauseFailure<String>).error,
      'typed',
    );
  });
}

Cause<E> _cause<T, E>(Exit<T, E> exit) => (exit as ExitFailure<T, E>).cause;
