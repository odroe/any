import 'dart:async';

import 'package:any/any.dart';
import 'package:any/src/core/context.dart';
import 'package:test/test.dart';

void main() {
  group('Context', () {
    test('retains the caller-owned scope and dependency instances', () {
      final scope = _scope();
      final dependencies = Object();
      final context = Context(scope: scope, dependencies: dependencies);

      expect(context.scope, same(scope));
      expect(context.dependencies, same(dependencies));
      expect(scope.isClosed, isFalse);
    });

    test('infers record fields and callable dependency types', () {
      final context = Context(
        scope: _scope(),
        dependencies: (format: (int value) => 'item:$value'),
      );

      String render(Context<({String Function(int) format})> context) =>
          context.dependencies.format(42);

      expect(render(context), 'item:42');
    });

    test(
      'overrides dependencies locally while sharing resource lifetime',
      () async {
        final scope = _scope();
        final context = Context(
          scope: scope,
          dependencies: (label: 'production'),
        );
        final local = context.withDependencies((label: 'test'));
        var cleanupCalls = 0;
        local.scope.addFinalizer((_) => cleanupCalls++);

        expect(context.dependencies.label, 'production');
        expect(local.dependencies.label, 'test');
        expect(local.scope, same(context.scope));
        expect(cleanupCalls, 0);
        expect(scope.isClosed, isFalse);

        await context.scope.close();
        expect(cleanupCalls, 1);
        expect(local.scope.isClosed, isTrue);
      },
    );

    test('can replace dependencies with a different type', () {
      final context = Context(scope: _scope(), dependencies: 'original');
      final Context<({int count})> local = context.withDependencies((
        count: 42,
      ));

      expect(local.dependencies.count, 42);
      expect(context.dependencies.length, 8);
      expect(local.scope, same(context.scope));
    });

    test(
      'allows nullable dependencies to be explicitly replaced with null',
      () {
        final context = Context<String?>(
          scope: _scope(),
          dependencies: 'original',
        );
        final Context<String?> local = context.withDependencies(null);

        expect(local.dependencies, isNull);
        expect(context.dependencies, 'original');
        expect(local.scope, same(context.scope));
      },
    );

    test('accepts an empty record when no dependencies are needed', () {
      final Context<()> context = Context(scope: _scope(), dependencies: ());

      expect(context.dependencies, ());
    });

    test('replacing a scope keeps its lifetime independent', () async {
      final originalScope = _scope();
      final replacementScope = _scope();
      final dependencies = Object();
      final context = Context(scope: originalScope, dependencies: dependencies);
      final local = context.withScope(replacementScope);
      final events = <String>[];
      originalScope.addFinalizer((_) => events.add('original'));
      replacementScope.addFinalizer((_) => events.add('replacement'));

      expect(context.scope, same(originalScope));
      expect(local.scope, same(replacementScope));
      expect(local.dependencies, same(dependencies));
      expect(originalScope.isClosed, isFalse);
      expect(replacementScope.isClosed, isFalse);

      await local.scope.close();
      expect(events, ['replacement']);
      expect(originalScope.isClosed, isFalse);

      await context.scope.close();
      expect(events, ['replacement', 'original']);
    });

    test('shares dependency objects without cloning or freezing them', () {
      final values = <int>[1];
      final context = Context(scope: _scope(), dependencies: values);
      final local = context.withScope(_scope());

      local.dependencies.add(2);

      expect(local.dependencies, same(values));
      expect(context.dependencies, [1, 2]);
    });

    test(
      'supports explicitly nested scopes with inherited dependencies',
      () async {
        final parentScope = _scope();
        final childScope = _scope();
        parentScope.addFinalizer((exit) => childScope.close(exit: exit));
        final context = Context(
          scope: parentScope,
          dependencies: (traceId: 'request-1'),
        );
        final child = context.withScope(childScope);
        const exit = Exit<Never, Never>.cancelled(reason: 'shutdown');
        child.scope.addFinalizer((childExit) {
          expect(child.dependencies.traceId, 'request-1');
          expect(childExit, same(exit));
        });

        await context.scope.close(exit: exit);

        expect(context.scope.isClosed, isTrue);
        expect(child.scope.isClosed, isTrue);
      },
    );

    test(
      'keeps concurrent local dependencies isolated across awaits',
      () async {
        final context = Context(
          scope: _scope(),
          dependencies: (name: 'original'),
        );
        final first = context.withDependencies((name: 'first'));
        final second = context.withDependencies((name: 'second'));
        final firstGate = Completer<void>();
        final secondGate = Completer<void>();
        var firstFinished = false;
        final firstRun = _readName(first, firstGate.future).then((value) {
          firstFinished = true;
          return value;
        });
        final secondRun = _readName(second, secondGate.future);

        secondGate.complete();
        expect(await secondRun, 'second');
        expect(firstFinished, isFalse);
        firstGate.complete();
        expect(await firstRun, 'first');
        expect(context.dependencies.name, 'original');
      },
    );

    test('retains dependency types when a context is widened', () {
      final dependencies = <int>[1, 2];
      final original = Context(scope: _scope(), dependencies: dependencies);
      final Context<Iterable<num>> widened = original;
      final local = widened.withScope(_scope());
      final Context<String> replacement = widened.withDependencies(
        'replacement',
      );

      expect(local.dependencies, same(dependencies));
      expect(local.dependencies.fold<num>(0, (sum, value) => sum + value), 3);
      expect(replacement.dependencies.length, 11);
      expect(original.dependencies, same(dependencies));
    });
  });
}

Scope _scope() {
  final scope = Scope();
  addTearDown(scope.close);
  return scope;
}

Future<String> _readName(
  Context<({String name})> context,
  Future<void> gate,
) async {
  await gate;
  return context.dependencies.name;
}
