import 'dart:async';

import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  late Runtime runtime;

  setUp(() => runtime = Runtime());
  tearDown(() async {
    expect(await runtime.close(), isA<ExitSuccess<void, Never>>());
  });

  test(
    'read descriptions use the current binding on every execution',
    () async {
      final local = TaskLocal('default', name: 'request');
      final read = local.get();
      final provided = local.locally('request', read);

      expect(await runtime.runOrThrow(read), 'default');
      expect(await runtime.runOrThrow(provided), 'request');
      expect(await runtime.runOrThrow(read), 'default');
      expect(await runtime.runOrThrow(provided), 'request');
    },
  );

  test('keys with the same name and type remain independent', () async {
    final first = TaskLocal('default', name: 'same');
    final second = TaskLocal('default', name: 'same');
    final task = first.locally(
      'first',
      first.get().flatMap((a) => second.get().map((b) => (a, b))),
    );

    expect(first, isNot(same(second)));
    expect(await runtime.runOrThrow(task), ('first', 'default'));
  });

  test('nested overrides restore each enclosing binding', () async {
    final local = TaskLocal(0);
    final other = TaskLocal('outer');
    final task = local
        .locally(
          1,
          local.get().flatMap(
            (before) => local
                .locally(2, local.get())
                .flatMap(
                  (inside) => local.get().flatMap(
                    (after) => other.get().map(
                      (unrelated) => (before, inside, after, unrelated),
                    ),
                  ),
                ),
          ),
        )
        .flatMap((values) => local.get().map((outside) => (values, outside)));

    expect(await runtime.runOrThrow(task), ((1, 2, 1, 'outer'), 0));
  });

  test('an explicit null overrides a non-null nullable default', () async {
    final local = TaskLocal<String?>('default');
    final absent = TaskLocal<int?>(null);

    expect(await runtime.runOrThrow(local.locally(null, local.get())), isNull);
    expect(await runtime.runOrThrow(local.get()), 'default');
    expect(await runtime.runOrThrow(absent.get()), isNull);
    expect(await runtime.runOrThrow(absent.locally(2, absent.get())), 2);
  });

  test('business failure restores the binding before recovery', () async {
    final local = TaskLocal('outer');
    final task = local
        .locally<String, String>(
          'inner',
          local.get().withError<String>().flatMap((value) => Task.fail(value)),
        )
        .recover((error) {
          expect(error, 'inner');
          return local.get().withError<String>();
        });

    expect(await runtime.runOrThrow(task), 'outer');
  });

  test('a defect restores the binding before cause recovery', () async {
    final local = TaskLocal('outer');
    final error = StateError('failed');
    final trace = StackTrace.fromString('local callback');
    final task = local
        .locally<String, Never>(
          'inner',
          Task.sync(() => Error.throwWithStackTrace(error, trace)),
        )
        .recoverCause<Never>((cause) {
          expect((cause as CauseDefect).error, same(error));
          expect(cause.stackTrace.toString(), '$trace');
          return local.get();
        });

    expect(await runtime.runOrThrow(task), 'outer');
  });

  test('a local binding survives a native Future boundary', () async {
    final local = TaskLocal('default');
    final started = Completer<void>();
    final work = Completer<void>();
    addTearDown(() {
      if (!work.isCompleted) work.complete();
    });
    final running = runtime.run(
      local.locally(
        'inside',
        Task.fromFuture<void, Never>(() {
          started.complete();
          return work.future;
        }, onError: Error.throwWithStackTrace).flatMap((_) => local.get()),
      ),
    );
    await started.future;

    expect(await runtime.runOrThrow(local.get()), 'default');
    work.complete();
    expect(_value(await running), 'inside');
  });

  test('concurrent runtime roots keep separate overrides', () async {
    final local = TaskLocal('default');
    final first = _Gate();
    final second = _Gate();
    final firstRun = runtime.run(
      local.locally('first', first.task.flatMap((_) => local.get())),
    );
    final secondRun = runtime.run(
      local.locally('second', second.task.flatMap((_) => local.get())),
    );
    await first.started.future;
    await second.started.future;

    second.open();
    expect(_value(await secondRun), 'second');
    expect(await runtime.runOrThrow(local.get()), 'default');
    first.open();
    expect(_value(await firstRun), 'first');
  });

  test(
    'a host-started nested root does not inherit the enclosing task local',
    () async {
      final local = TaskLocal('default');
      final task = local.locally(
        'outer root',
        Task.fromFuture(
          () => runtime.run(local.get()),
          onError: Error.throwWithStackTrace,
        ),
      );

      expect(_value(await runtime.runOrThrow(task)), 'default');
    },
  );

  test(
    'fork inherits a snapshot and child overrides never merge into its parent',
    () async {
      final local = TaskLocal('default');
      final gate = _Gate();
      final child = gate.task
          .flatMap((_) => local.get())
          .flatMap(
            (inherited) => local
                .locally('child', local.get())
                .flatMap(
                  (changed) => local.get().map(
                    (restored) => (inherited, changed, restored),
                  ),
                ),
          );
      final task = local
          .locally('launch', child.fork())
          .flatMap(
            (fiber) => local.locally(
              'parent later',
              Task.sync(gate.open).flatMap(
                (_) => fiber.join().flatMap(
                  (childValues) => local.get().map(
                    (parentValue) => (childValues, parentValue),
                  ),
                ),
              ),
            ),
          );

      expect(await runtime.runOrThrow(task), (
        ('launch', 'child', 'launch'),
        'parent later',
      ));
      expect(await runtime.runOrThrow(local.get()), 'default');
    },
  );

  test('forkIn inherits the launch binding from its creating task', () async {
    final local = TaskLocal('default');
    final gate = _Gate();
    final child = gate.task.flatMap((_) => local.get());
    final task = Scope.open().flatMap(
      (scope) => local
          .locally('launch', child.forkIn(scope))
          .flatMap(
            (fiber) => Task.sync(gate.open)
                .flatMap((_) => fiber.join())
                .flatMap(
                  (value) => local.get().map((parent) => (value, parent)),
                ),
          ),
    );

    expect(await runtime.runOrThrow(task), ('launch', 'default'));
  });

  test(
    'sibling overrides remain independent of each other and their parent',
    () async {
      final local = TaskLocal('default');
      final first = local.locally(
        'first',
        Task.yieldNow().flatMap((_) => local.get()),
      );
      final second = local.locally(
        'second',
        Task.yieldNow().flatMap((_) => local.get()),
      );
      final task = local.locally(
        'parent',
        first.fork().flatMap(
          (a) => second.fork().flatMap(
            (b) => a.join().flatMap(
              (av) => b.join().flatMap(
                (bv) => local.get().map((parent) => (av, bv, parent)),
              ),
            ),
          ),
        ),
      );

      expect(await runtime.runOrThrow(task), ('first', 'second', 'parent'));
    },
  );

  test('finalizers retain the value at each registration point', () async {
    final local = TaskLocal('default');
    final seen = <String>[];
    Task<void, Never> register() => Task.addFinalizer(
      (_) => Task.yieldNow().flatMap((_) => local.get()).map(seen.add),
    );
    final task = local
        .locally('first', register())
        .flatMap((_) => local.locally('second', register()))
        .flatMap((_) => local.get());

    expect(await runtime.runOrThrow(task), 'default');
    expect(seen, ['second', 'first']);
  });

  test(
    'onExit observes where cleanup was registered, not the body override',
    () async {
      final local = TaskLocal('default');
      final seen = <String>[];
      Task<void, Never> cleanup() => local.get().map(seen.add);
      final outside = local
          .locally('body', Task.succeed(1))
          .onExit((_) => cleanup());
      final inside = local.locally(
        'body',
        Task.succeed(1).onExit((_) => cleanup()),
      );

      expect(await runtime.runOrThrow(outside), 1);
      expect(await runtime.runOrThrow(inside), 1);
      expect(seen, ['default', 'body']);
    },
  );

  test(
    'cancellation cleanup uses its registration binding without leaking it',
    () async {
      final local = TaskLocal('default');
      final gate = _Gate();
      final controller = CancellationController();
      final seen = <String>[];
      final task = local
          .locally(
            'inner',
            Task.addFinalizer(
              (_) => Task.yieldNow().flatMap((_) => local.get()).map(seen.add),
            ).flatMap((_) => gate.task),
          )
          .ensuring(local.get().map(seen.add));
      final running = runtime.run(task, signal: controller.signal);
      await gate.started.future;

      controller.requestCancel();

      expect(_cause(await running), isA<CauseCancelled>());
      expect(seen, ['inner', 'default']);
      expect(await runtime.runOrThrow(local.get()), 'default');
    },
  );

  test('widening a key cannot change its original binding type', () async {
    final narrow = TaskLocal(1);
    final TaskLocal<num> widened = narrow;
    final read = widened.get();
    final Task<String, String> provided = widened.locally(
      2,
      Task.succeed('value'),
    );

    expect(await runtime.runOrThrow(provided), 'value');
    expect(await runtime.runOrThrow(widened.locally(2, read)), 2);
    expect(() => widened.locally(1.5, read), throwsA(isA<TypeError>()));
    final dynamic erased = widened;
    expect(() => erased.locally('wrong', read), throwsA(isA<TypeError>()));
    expect(await runtime.runOrThrow(narrow.get()), 1);
  });

  test(
    'binding isolation does not copy mutable values inherited by children',
    () async {
      final initial = <int>[];
      final shared = <int>[];
      final local = TaskLocal(initial);
      final child = local.get().map((value) => value.add(1));
      final task = local.locally(
        shared,
        child
            .fork()
            .flatMap((fiber) => fiber.join())
            .flatMap((_) => local.get()),
      );

      expect(await runtime.runOrThrow(task), same(shared));
      expect(shared, [1]);
      expect(await runtime.runOrThrow(local.get()), same(initial));
      expect(initial, isEmpty);
    },
  );
}

final class _Gate {
  final started = Completer<void>();
  late void Function(Exit<void, Never>) _complete;

  Task<void, Never> get task => Task.fromCallback((complete) {
    _complete = complete;
    started.complete();
    return () {};
  });

  void open() => _complete(const Exit.success(null));
}

T _value<T, E>(Exit<T, E> exit) => switch (exit) {
  ExitSuccess(:final value) => value,
  ExitFailure(:final cause) => fail('Expected success, got $cause.'),
};

Cause<E> _cause<T, E>(Exit<T, E> exit) => switch (exit) {
  ExitSuccess(:final value) => fail('Expected failure, got $value.'),
  ExitFailure(:final cause) => cause,
};
