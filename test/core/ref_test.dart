import 'dart:async';

import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  late Runtime runtime;

  setUp(() => runtime = Runtime());
  tearDown(() async {
    expect(await runtime.close(), isA<ExitSuccess<void, Never>>());
  });

  test('each execution of create owns an independent cell', () async {
    final create = Ref.create(0);
    final first = _value(await runtime.run(create));
    final second = _value(await runtime.run(create));

    expect(first, isNot(same(second)));
    expect(await runtime.run(first.set(7)), isA<ExitSuccess<void, Never>>());
    expect(_value(await runtime.run(first.get())), 7);
    expect(_value(await runtime.run(second.get())), 0);
  });

  test(
    'get and set are lazy and repeated reads see the latest value',
    () async {
      final ref = _value(await runtime.run(Ref.create(0)));
      final read = ref.get();
      final write = ref.set(42);

      expect(_value(await runtime.run(read)), 0);
      await runtime.run(write);
      expect(_value(await runtime.run(read)), 42);
      await runtime.run(ref.set(7));
      expect(_value(await runtime.run(read)), 7);
    },
  );

  test(
    'a reused update reads the current state once on every execution',
    () async {
      final ref = _value(await runtime.run(Ref.create(0)));
      var calls = 0;
      final increment = ref.update((value) {
        calls++;
        return value + 1;
      });

      expect(calls, 0);
      await runtime.run(increment);
      await runtime.run(increment);
      expect(calls, 2);
      expect(_value(await runtime.run(ref.get())), 2);
    },
  );

  test(
    'modify returns a result and commits its new value before continuing',
    () async {
      final ref = _value(await runtime.run(Ref.create(1)));
      final next = ref
          .modify((value) => ('previous $value', value + 1))
          .flatMap(
            (previous) => ref.get().map((current) => (previous, current)),
          );

      expect(_value(await runtime.run(next)), ('previous 1', 2));
      expect(_value(await runtime.run(next)), ('previous 2', 3));
    },
  );

  test('nullable values and nullable modify results remain valid', () async {
    final ref = _value(await runtime.run(Ref.create<int?>(null)));
    expect(_value(await runtime.run(ref.get())), isNull);
    final updated = await runtime.run(
      ref.modify<String?>((value) {
        expect(value, isNull);
        return (null, 2);
      }),
    );

    expect(_value(updated), isNull);
    expect(_value(await runtime.run(ref.get())), 2);
    await runtime.run(ref.set(null));
    expect(_value(await runtime.run(ref.get())), isNull);
  });

  test(
    'throwing callbacks preserve the stored value and original defect',
    () async {
      final ref = _value(await runtime.run(Ref.create(5)));
      final error = StateError('update failed');
      final trace = StackTrace.fromString('original update trace');
      final operations = <Task<Object?, Never>>[
        ref.update((_) => Error.throwWithStackTrace(error, trace)),
        ref.modify<String>((_) => Error.throwWithStackTrace(error, trace)),
      ];

      for (final operation in operations) {
        final cause = _cause(await runtime.run(operation)) as CauseDefect;
        expect(cause.error, same(error));
        expect(cause.stackTrace.toString(), '$trace');
        expect(_value(await runtime.run(ref.get())), 5);
      }
    },
  );

  test(
    'independent runs modify one cell without losing synchronous updates',
    () async {
      final ref = _value(await runtime.run(Ref.create(0)));
      final increment = Task.yieldNow().flatMap(
        (_) => ref.modify((value) => (value, value + 1)),
      );
      final exits = await Future.wait([
        for (var i = 0; i < 20; i++) runtime.run(increment),
      ]);

      final previous = exits.map(_value).toList()..sort();
      expect(previous, List.generate(20, (index) => index));
      expect(_value(await runtime.run(ref.get())), 20);
    },
  );

  test(
    'pre-cancellation skips callbacks and leaves the value untouched',
    () async {
      final ref = _value(await runtime.run(Ref.create(5)));
      final controller = CancellationController()..requestCancel();
      final operation = ref.update((_) => fail('The callback must not run.'));

      expect(
        _cause(await runtime.run(operation, signal: controller.signal)),
        isA<CauseCancelled>(),
      );
      expect(_value(await runtime.run(ref.get())), 5);
    },
  );

  test(
    'cancellation inside a synchronous update does not undo its commit',
    () async {
      final ref = _value(await runtime.run(Ref.create(5)));
      final controller = CancellationController();
      final operation = ref.update((value) {
        controller.requestCancel(reason: 'inside update');
        return value + 1;
      });

      expect(
        (_cause(
          await runtime.run(operation, signal: controller.signal),
        ) as CauseCancelled).reason,
        'inside update',
      );
      expect(_value(await runtime.run(ref.get())), 6);
    },
  );

  test('widened receivers accept callbacks using their static type', () async {
    final narrow = _value(await runtime.run(Ref.create(1)));
    final Ref<num> widened = narrow;

    await runtime.run(widened.update((num value) => value.toInt() + 1));
    final result = await runtime.run(
      widened.modify<String>(
        (num value) => ('previous $value', value.toInt() + 1),
      ),
    );
    expect(_value(result), 'previous 2');
    expect(_value(await runtime.run(narrow.get())), 3);
  });

  test(
    'incompatible widened writes fail lazily without corrupting the cell',
    () async {
      final narrow = _value(await runtime.run(Ref.create(1)));
      final Ref<num> widened = narrow;
      final operations = <Task<Object?, Never>>[
        widened.set(1.5),
        widened.update((_) => 1.5),
        widened.modify<String>((_) => ('not returned', 1.5)),
      ];

      for (final operation in operations) {
        expect(
          (_cause(await runtime.run(operation)) as CauseDefect).error,
          isA<TypeError>(),
        );
        expect(_value(await runtime.run(narrow.get())), 1);
      }
    },
  );

  test(
    'refs retain mutable objects rather than cloning or rolling them back',
    () async {
      final original = <int>[1];
      final create = Ref.create(original);
      final first = _value(await runtime.run(create));
      final second = _value(await runtime.run(create));
      final error = StateError('after mutation');
      final operation = first.update((value) {
        value.add(2);
        throw error;
      });

      expect(
        (_cause(await runtime.run(operation)) as CauseDefect).error,
        same(error),
      );
      expect(_value(await runtime.run(first.get())), same(original));
      expect(_value(await runtime.run(second.get())), [1, 2]);
    },
  );
}

T _value<T, E>(Exit<T, E> exit) => switch (exit) {
  ExitSuccess(:final value) => value,
  ExitFailure(:final cause) => fail('Expected success, got $cause'),
};

Cause<E> _cause<T, E>(Exit<T, E> exit) => switch (exit) {
  ExitSuccess(:final value) => fail('Expected failure, got $value'),
  ExitFailure(:final cause) => cause,
};
