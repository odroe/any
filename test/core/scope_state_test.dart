import 'dart:async';

import 'package:any/any.dart';
import 'package:any/src/core/scope.dart' as scopes;
import 'package:test/test.dart';

void main() {
  late Runtime runtime;

  Future<Exit<void, Never>> runFinalizer(
    Task<void, Never> task,
    scopes.Scope scope,
  ) => runtime.run(task);

  FutureOr<Cause<Never>?> add(
    scopes.Scope scope,
    Task<void, Never> Function(Exit<Object?, Object?>) finalizer,
  ) => scopes.addScopeFinalizer(scope, finalizer, runFinalizer: runFinalizer);

  setUp(() => runtime = Runtime());
  tearDown(() async {
    expect(await runtime.close(), isA<ExitSuccess<void, Never>>());
  });

  test('finalizers run in LIFO order and all are attempted', () async {
    final scope = scopes.createScope();
    final events = <int>[];
    final firstError = StateError('last registered');
    final secondError = StateError('middle registered');
    final firstTrace = StackTrace.fromString('first cleanup trace');
    final secondTrace = StackTrace.fromString('second cleanup trace');
    add(scope, (_) => Task.sync(() => events.add(1)));
    add(scope, (_) {
      events.add(2);
      Error.throwWithStackTrace(secondError, secondTrace);
    });
    add(
      scope,
      (_) => Task.sync<void>(() {
        events.add(3);
        Error.throwWithStackTrace(firstError, firstTrace);
      }),
    );

    final cause = await scopes.closeScope(scope, const Exit.success(null));

    expect(events, [3, 2, 1]);
    final reasons = (cause as CauseGroup<Never>).causes;
    expect(reasons, hasLength(2));
    expect((reasons[0] as CauseDefect).error, same(firstError));
    expect((reasons[0] as CauseDefect).stackTrace.toString(), '$firstTrace');
    expect((reasons[1] as CauseDefect).error, same(secondError));
    expect((reasons[1] as CauseDefect).stackTrace.toString(), '$secondTrace');
  });

  test('children close before parent in reverse registration order', () async {
    final parent = scopes.createScope();
    final first = scopes.createScope(parent: parent);
    final second = scopes.createScope(parent: parent);
    final grandchild = scopes.createScope(parent: first);
    final events = <String>[];
    add(parent, (_) => Task.sync(() => events.add('parent')));
    add(first, (_) => Task.sync(() => events.add('first')));
    add(second, (_) => Task.sync(() => events.add('second')));
    add(grandchild, (_) => Task.sync(() => events.add('grandchild')));

    expect(await scopes.closeScope(parent, const Exit.success(null)), isNull);
    expect(events, ['second', 'grandchild', 'first', 'parent']);
  });

  test('cleanup causes stay flat without repeating the body exit', () async {
    final parent = scopes.createScope();
    final child = scopes.createScope(parent: parent);
    final bodyExit = Exit<void, String>.failure(const Cause.failure('body'));
    final childDefect = Cause<Never>.defect(Object(), StackTrace.current);
    const childCancelled = Cause<Never>.cancelled('cleanup cancellation');
    final parentDefect = Cause<Never>.defect(Object(), StackTrace.current);
    add(child, (exit) {
      expect(exit, same(bodyExit));
      return Task.failCause(Cause.combine([childDefect, childCancelled]));
    });
    add(parent, (exit) {
      expect(exit, same(bodyExit));
      return Task.failCause(parentDefect);
    });

    final cause = await scopes.closeScope(parent, bodyExit);
    final reasons = (cause as CauseGroup<Never>).causes;

    expect(reasons, [
      same(childDefect),
      same(childCancelled),
      same(parentDefect),
    ]);
    final childCause = await scopes.closeScope(child, const Exit.success(null));
    expect((childCause as CauseGroup<Never>).causes, [
      same(childDefect),
      same(childCancelled),
    ]);
  });

  test(
    'concurrent and repeated close share their Future and first exit',
    () async {
      final scope = scopes.createScope();
      final gate = Completer<void>();
      final firstExit = Exit<void, String>.failure(
        const Cause.failure('first'),
      );
      var calls = 0;
      add(scope, (exit) {
        calls++;
        expect(exit, same(firstExit));
        return _wait(gate.future);
      });

      final first = scopes.closeScope(scope, firstExit);
      final second = scopes.closeScope(scope, const Exit.success(null));
      expect(second, same(first));
      expect(calls, 1);
      gate.complete();
      expect(await first, isNull);
      expect(scopes.closeScope(scope, const Exit.success(null)), same(first));
    },
  );

  test(
    'registration while closing joins the same LIFO cleanup queue',
    () async {
      final scope = scopes.createScope();
      final events = <String>[];
      add(scope, (_) => Task.sync(() => events.add('first')));
      add(
        scope,
        (_) => Task.sync(() {
          events.add('last');
          expect(
            add(scope, (_) => Task.sync(() => events.add('added'))),
            isNull,
          );
        }),
      );

      expect(await scopes.closeScope(scope, const Exit.success(null)), isNull);
      expect(events, ['last', 'added', 'first']);
    },
  );

  test(
    'late registration runs immediately without changing close result',
    () async {
      final scope = scopes.createScope();
      final firstExit = Exit<void, String>.failure(const Cause.failure('body'));
      final closing = scopes.closeScope(scope, firstExit);
      expect(await closing, isNull);
      var called = false;
      final cleanupCause = Cause<Never>.defect(Object(), StackTrace.current);

      final late = add(scope, (exit) {
        called = true;
        expect(exit, same(firstExit));
        return Task.failCause(cleanupCause);
      });

      expect(called, isTrue);
      expect(await late, same(cleanupCause));
      expect(scopes.closeScope(scope, const Exit.success(null)), same(closing));
      expect(await closing, isNull);
    },
  );

  test('a previously closed child is detached and not merged again', () async {
    final parent = scopes.createScope();
    final child = scopes.createScope(parent: parent);
    final cleanupCause = Cause<Never>.defect(Object(), StackTrace.current);
    var parentCalls = 0;
    add(child, (_) => Task.failCause(cleanupCause));
    add(parent, (_) => Task.sync(() => parentCalls++));

    expect(
      await scopes.closeScope(child, const Exit.success(null)),
      same(cleanupCause),
    );
    expect(scopes.scopeContains(parent, child), isFalse);
    expect(await scopes.closeScope(parent, const Exit.success(null)), isNull);
    expect(parentCalls, 1);
  });

  test(
    'parent waits for an independently closing child and retains failure',
    () async {
      final parent = scopes.createScope();
      final child = scopes.createScope(parent: parent);
      final gate = Completer<void>();
      final cleanupCause = Cause<Never>.defect(Object(), StackTrace.current);
      var parentCleaned = false;
      add(
        child,
        (_) => _wait(gate.future).flatMap((_) => Task.failCause(cleanupCause)),
      );
      add(parent, (_) => Task.sync(() => parentCleaned = true));

      final childClosing = scopes.closeScope(child, const Exit.success(null));
      final parentClosing = scopes.closeScope(parent, const Exit.success(null));
      await Future<void>.delayed(Duration.zero);
      expect(parentCleaned, isFalse);
      gate.complete();

      expect(await childClosing, same(cleanupCause));
      expect(await parentClosing, same(cleanupCause));
      expect(parentCleaned, isTrue);
    },
  );

  test(
    'an independent child cannot disappear while a sibling is closing',
    () async {
      final parent = scopes.createScope();
      final independent = scopes.createScope(parent: parent);
      final sibling = scopes.createScope(parent: parent);
      final childGate = Completer<void>();
      final siblingGate = Completer<void>();
      final cleanupCause = Cause<Never>.defect(Object(), StackTrace.current);
      add(
        independent,
        (_) =>
            _wait(childGate.future)
                .flatMap((_) => Task.failCause(cleanupCause)),
      );
      add(sibling, (_) => _wait(siblingGate.future));

      final childClosing = scopes.closeScope(
        independent,
        const Exit.success(null),
      );
      final parentClosing = scopes.closeScope(parent, const Exit.success(null));
      childGate.complete();
      expect(await childClosing, same(cleanupCause));
      expect(scopes.scopeContains(parent, independent), isFalse);
      siblingGate.complete();

      expect(await parentClosing, same(cleanupCause));
    },
  );

  test(
    'an independently closing newer child finishes before older siblings',
    () async {
      final parent = scopes.createScope();
      final first = scopes.createScope(parent: parent);
      final second = scopes.createScope(parent: parent);
      final gate = Completer<void>();
      final events = <String>[];
      final firstFailure = Cause<Never>.defect('first', StackTrace.current);
      final secondFailure = Cause<Never>.defect('second', StackTrace.current);
      add(first, (_) {
        events.add('first');
        return Task.failCause(firstFailure);
      });
      add(second, (_) {
        events.add('second started');
        return _wait(gate.future).flatMap((_) {
          events.add('second finished');
          return Task.failCause(secondFailure);
        });
      });
      add(parent, (_) => Task.sync(() => events.add('parent')));

      final secondClosing = scopes.closeScope(second, const Exit.success(null));
      final parentClosing = scopes.closeScope(parent, const Exit.success(null));
      try {
        await Future<void>.delayed(Duration.zero);
        expect(events, ['second started']);
      } finally {
        gate.complete();
      }

      expect(await secondClosing, same(secondFailure));
      final cause = await parentClosing as CauseGroup<Never>;
      expect(events, ['second started', 'second finished', 'first', 'parent']);
      expect(cause.causes, [same(secondFailure), same(firstFailure)]);
    },
  );

  test(
    'close guard rejects siblings queued behind the current finalizer',
    () async {
      final parent = scopes.createScope();
      final first = scopes.createScope(parent: parent);
      final second = scopes.createScope(parent: parent);
      final unrelated = scopes.createScope();
      final checked = <String>[];
      add(
        second,
        (_) => Task.sync(() {
          expect(scopes.scopeContains(first, second), isFalse);
          expect(scopes.scopeCloseWouldDeadlock(first, second), isTrue);
          expect(scopes.scopeCloseWouldDeadlock(parent, second), isTrue);
          expect(scopes.scopeCloseWouldDeadlock(second, second), isTrue);
          expect(scopes.scopeCloseWouldDeadlock(unrelated, second), isFalse);
          checked.add('queued sibling');
        }),
      );
      add(
        first,
        (_) => Task.sync(() {
          expect(scopes.scopeCloseWouldDeadlock(second, first), isFalse);
          checked.add('finished sibling');
        }),
      );

      expect(scopes.scopeCloseWouldDeadlock(first, second), isFalse);
      expect(await scopes.closeScope(parent, const Exit.success(null)), isNull);
      expect(
        await scopes.closeScope(unrelated, const Exit.success(null)),
        isNull,
      );
      expect(scopes.scopeCloseWouldDeadlock(parent, second), isFalse);
      expect(checked, ['queued sibling', 'finished sibling']);
    },
  );

  test(
    'close guard follows ancestors waiting for an independent child',
    () async {
      final parent = scopes.createScope();
      final first = scopes.createScope(parent: parent);
      final second = scopes.createScope(parent: parent);
      final gate = Completer<void>();
      add(second, (_) => _wait(gate.future));

      final secondClosing = scopes.closeScope(second, const Exit.success(null));
      final parentClosing = scopes.closeScope(parent, const Exit.success(null));
      try {
        expect(scopes.scopeContains(first, second), isFalse);
        expect(scopes.scopeCloseWouldDeadlock(first, second), isTrue);
      } finally {
        gate.complete();
        await secondClosing;
        await parentClosing;
      }
    },
  );

  test('the entire tree rejects new children before cleanup begins', () async {
    final parent = scopes.createScope();
    final child = scopes.createScope(parent: parent);
    add(
      child,
      (_) => Task.sync(() {
        expect(() => scopes.createScope(parent: parent), throwsStateError);
        expect(() => scopes.createScope(parent: child), throwsStateError);
      }),
    );

    expect(await scopes.closeScope(parent, const Exit.success(null)), isNull);
    expect(() => scopes.createScope(parent: parent), throwsStateError);
    expect(() => scopes.createScope(parent: child), throwsStateError);
  });

  test('the runner receives the finalizer registration scope', () async {
    final parent = scopes.createScope();
    final child = scopes.createScope(parent: parent);
    final seen = <scopes.Scope>[];
    scopes.addScopeFinalizer(
      child,
      (_) => Task.succeed(null),
      runFinalizer: (task, scope) {
        seen.add(scope);
        return runtime.run(task);
      },
    );

    expect(await scopes.closeScope(parent, const Exit.success(null)), isNull);
    expect(seen, [same(child)]);
  });

  test('ten thousand nested empty scopes close without recursion', () async {
    final root = scopes.createScope();
    var child = root;
    for (var i = 0; i < 10000; i++) {
      child = scopes.createScope(parent: child);
    }
    expect(scopes.scopeContains(root, child), isTrue);

    expect(await scopes.closeScope(root, const Exit.success(null)), isNull);
    expect(await scopes.closeScope(child, const Exit.success(null)), isNull);
    expect(scopes.scopeContains(root, child), isFalse);
  });
}

Task<void, Never> _wait(Future<void> future) => Task.fromFuture(
  () => future,
  onError: (error, stackTrace) => Error.throwWithStackTrace(error, stackTrace),
);
