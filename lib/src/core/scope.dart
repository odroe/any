import 'dart:async';

import 'cause.dart';
import 'exit.dart';
import 'fiber.dart';
import 'task.dart';

/// A lifetime boundary for resources and child fibers.
///
/// Register cleanup with [Task.addFinalizer] or [Task.acquireRelease]. Prefer
/// [TaskOperations.scoped] to give a computation a local lifetime. Use [open] and
/// [close] when work needs an explicit owner, such as [TaskOperations.forkIn].
///
/// Closing a scope stops and waits for its child fibers before releasing
/// resources. Child scopes close before their parent's finalizers; sibling
/// scopes and finalizers close in reverse registration order. Every finalizer
/// runs, with cancellation masked, even if another finalizer fails.
///
/// ```dart
/// final events = <String>[];
/// final program = Task.addFinalizer(
///   (_) => Task.sync(() => events.add('released')),
/// ).flatMap((_) => Task.sync(() => events.add('used'))).scoped();
/// final runtime = Runtime();
/// try {
///   await runtime.runOrThrow(program);
///   assert(events.join(', ') == 'used, released');
/// } finally {
///   await runtime.close();
/// }
/// ```
///
/// Cleanup failures join the computation's cause. They do not prevent remaining
/// resources from being released.
final class Scope {
  Scope._(this._parent);

  /// Returns a task that opens a child of the current scope.
  ///
  /// The parent closes the child automatically if it is still open. Opening a child
  /// under a closing or closed scope fails with a [StateError] defect.
  static Task<Scope, Never> open() => openScopeTask();

  /// Returns a task that closes this scope and waits for its cleanup.
  ///
  /// [exit] is the outcome passed to finalizers. The first close fixes this outcome;
  /// subsequent closes return the same cleanup result. The returned task propagates
  /// cleanup failures only, not the supplied [exit].
  ///
  /// A scope cannot be closed by work whose completion it must await. Closing the
  /// caller's active scope, an ancestor, or a scope waiting on that caller's cleanup
  /// fails with a [StateError] defect instead of deadlocking.
  Task<void, Never> close(Exit<Object?, Object?> exit) =>
      closeScopeTask(this, exit);

  Scope? _parent;
  final _children = <Scope>{};
  final _fibers = <Fiber<Object?, Object?>>{};
  final _closeDependencies = <Object, Scope>{};
  final _finalizers = <_Finalizer>[];
  _Closing? _closing;
}

// Internal lifecycle operations. These declarations are not exported by any.dart.

void attachScopeFiber(Scope scope, Fiber<Object?, Object?> fiber) {
  if (scope._closing != null) {
    throw StateError('Cannot start a fiber in a closing or closed scope.');
  }
  scope._fibers.add(fiber);
}

void detachScopeFiber(Scope scope, Fiber<Object?, Object?> fiber) {
  scope._fibers.remove(fiber);
}

// Fiber roots and leased resources are separate cleanup trees. Their lifetime
// dependencies still participate in self-wait detection without transferring
// cancellation or resource ownership.
void Function() addScopeCloseDependency(Scope owner, Scope dependency) {
  if (owner._closing != null) {
    throw StateError('Cannot add a dependency to a closing or closed scope.');
  }
  final token = Object();
  owner._closeDependencies[token] = dependency;
  return () => owner._closeDependencies.remove(token);
}

bool scopeWaitsForFiber(Scope scope, Fiber<Object?, Object?> target) {
  final pending = <Scope>[scope];
  final visited = <Scope>{};
  while (pending.isNotEmpty) {
    final current = pending.removeLast();
    if (!visited.add(current)) continue;
    if (current._fibers.contains(target)) return true;
    pending.addAll(current._children);
    pending.addAll(current._fibers.map(fiberScope));
    for (final dependency in current._closeDependencies.values) {
      if (scopeContains(fiberScope(target), dependency)) return true;
      pending.add(dependency);
    }
  }
  return false;
}

// Explicit closes read the full stable result. Automatic boundaries exclude
// occurrences already observed or carried by their current failure path.
TaskEvaluation<void, Never> scopeCleanupFor(
  Scope scope, [
  Set<Object> observed = const {},
]) {
  final closing = scope._closing!;
  if (!closing.completion.isCompleted) {
    throw StateError('Scope cleanup is not complete.');
  }
  final events = closing.events
      .where((event) => !observed.contains(event.source))
      .toList();
  return TaskEvaluation(
    events.isEmpty
        ? const Exit.success(null)
        : Exit.failure(Cause.combine(events.map((event) => event.cause))),
    List.unmodifiable([
      for (final event in events)
        if (event.cause case final CauseDefect cause)
          FailureEvent(cause, event.source),
    ]),
    cleanupCancellations: List.unmodifiable([
      for (final event in events)
        if (event.cause case final CauseCancelled cause) cause,
    ]),
  );
}

// Creates a root scope, or registers a child of [parent].
//
// Throws a [StateError] when [parent] is closing or closed.
Scope createScope({Scope? parent}) {
  if (parent?._closing != null) {
    throw StateError('Cannot create a child in a closing or closed scope.');
  }
  final scope = Scope._(parent);
  parent?._children.add(scope);
  return scope;
}

// Whether [descendant] belongs to the live subtree rooted at [ancestor].
//
// Includes [ancestor] itself until it has closed. The executor uses this to
// reject self and ancestor waits before requesting closure.
bool scopeContains(Scope ancestor, Scope descendant) {
  if (ancestor._closing?.completion.isCompleted ?? false) return false;
  for (Scope? scope = descendant; scope != null; scope = scope._parent) {
    if (identical(scope, ancestor)) return true;
  }
  return false;
}

// Actual ownership only: close dependencies do not transfer request domains.
bool scopeOwnsScope(Scope owner, Scope target) {
  final pending = <Scope>[owner];
  while (pending.isNotEmpty) {
    final scope = pending.removeLast();
    if (scopeContains(scope, target)) return true;
    pending.addAll(scope._children);
    pending.addAll(scope._fibers.map(fiberScope));
  }
  return false;
}

bool scopeIsClosing(Scope scope) => scope._closing != null;

// A finalizer also cannot await a sibling queued behind it by the same driver.
bool scopeCloseWouldDeadlock(Scope target, Scope current) {
  if (target._closing?.completion.isCompleted ?? false) return false;
  final pending = <Scope>[target];
  final visited = <Scope>{};
  while (pending.isNotEmpty) {
    final candidate = pending.removeLast();
    if (!visited.add(candidate)) continue;
    if (scopeContains(candidate, current)) return true;
    final owner = candidate._closing?.owner;
    if (owner != null) {
      for (Scope? scope = current; scope != null; scope = scope._parent) {
        if (identical(scope._closing?.owner, owner)) return true;
      }
    }
    pending.addAll(candidate._children);
    pending.addAll(candidate._fibers.map(fiberScope));
    pending.addAll(candidate._closeDependencies.values);
  }
  return false;
}

final _finalizingScopes = Object();

List<Scope> currentFinalizingScopes() =>
    Zone.current[_finalizingScopes] as List<Scope>? ?? const [];

T inScopeFinalizer<T>(Scope scope, T Function() run) => runZoned(
  run,
  zoneValues: {
    _finalizingScopes: List<Scope>.unmodifiable([
      ...currentFinalizingScopes(),
      scope,
    ]),
  },
);

// Registers [finalizer], or runs it immediately if [scope] has closed.
//
// Open and closing scopes queue the finalizer synchronously and return `null`.
// A closed scope uses its original closing exit and returns the late
// finalizer's cleanup cause without changing the completed close result.
//
// [runFinalizer] executes with cancellation masked and a temporary scope for
// resources used by cleanup. The supplied owning scope identifies self-waits.
// The executor captures the registration-time execution environment; Scope
// only manages ordering and failure accumulation.
FutureOr<TaskEvaluation<void, Never>?> addScopeFinalizer(
  Scope scope,
  Task<void, Never> Function(Exit<Object?, Object?> exit) finalizer, {
  required Future<TaskEvaluation<void, Never>> Function(
    Task<void, Never> task,
    Scope scope,
  )
  runFinalizer,
}) {
  final entry = _Finalizer(finalizer, runFinalizer);
  final closing = scope._closing;
  if (closing == null || !closing.completion.isCompleted) {
    scope._finalizers.add(entry);
    return null;
  }
  return _runFinalizer(scope, entry, closing.exit);
}

// Closes [scope], returning only its cleanup failures, or `null` on success.
//
// The first call fixes [exit] and synchronously marks all open descendants as
// closing. Repeated calls share the same Future and result. Children finish
// before parent finalizers; sibling scopes and finalizers use reverse
// registration order. Every finalizer is attempted even after failures.
//
// A child already closing independently is awaited, and its cleanup cause is
// included once. A child that closed before this call has detached and is not
// included again. The body's [exit] is passed to finalizers but never merged
// into the cleanup result.
Future<Cause<Never>?> closeScope(Scope scope, Exit<Object?, Object?> exit) {
  final existing = scope._closing;
  if (existing != null) return existing.completion.future;

  final owner = Object();
  final claimed = <Scope>[];
  final pending = <Scope>[scope];
  while (pending.isNotEmpty) {
    final current = pending.removeLast();
    if (current._closing != null) continue;
    final children = current._children.toList();
    current._closing = _Closing(
      exit,
      owner,
      children,
      current._fibers.toList(),
    );
    claimed.add(current);
    pending.addAll(children.reversed);
  }
  unawaited(_closeClaimed(claimed, owner));
  return scope._closing!.completion.future;
}

Future<void> _closeClaimed(List<Scope> claimed, Object owner) async {
  try {
    // Stop every running member before releasing any resource in this tree.
    // Fiber roots are separate scopes: each member finishes its own cleanup
    // before its completion can unblock the owning scope.
    final stopping = <Future<void>>[];
    for (final scope in claimed) {
      final reason = _cancellationReason(scope._closing!.exit);
      for (final fiber in scope._closing!.fibers) {
        stopping.add(
          stopFiber(fiber, reason: reason).then((events) {
            for (final event in events) {
              _record(scope, event.cause, owner, source: event.source);
            }
          }),
        );
      }
    }
    if (stopping.isNotEmpty) await Future.wait(stopping);
    final pending = <Scope>[claimed.first];
    while (pending.isNotEmpty) {
      final scope = pending.last;
      final closing = scope._closing!;
      // This snapshot also retains independently closing children that detach
      // while the parent waits elsewhere. Process each child to completion in
      // reverse registration order, including independently closing children.
      if (closing.children.isNotEmpty) {
        final child = closing.children.removeLast();
        final childClosing = child._closing!;
        if (identical(childClosing.owner, owner)) {
          pending.add(child);
        } else {
          final cause = await childClosing.completion.future;
          if (cause != null) {
            for (final event in childClosing.events) {
              _record(scope, event.cause, owner, source: event.source);
            }
          }
        }
        continue;
      }
      while (scope._finalizers.isNotEmpty) {
        final result = await _runFinalizer(
          scope,
          scope._finalizers.removeLast(),
          closing.exit,
        );
        _recordEvaluation(scope, result, owner);
      }
      _finish(scope);
      pending.removeLast();
    }
  } catch (error, stackTrace) {
    // Callback failures are contained below. An internal invariant failure
    // must still settle the close Futures rather than leave callers suspended.
    for (final scope in claimed.reversed) {
      if (!scope._closing!.completion.isCompleted) {
        _record(scope, Cause.defect(error, stackTrace), owner);
        _finish(scope);
      }
    }
  }
}

Object? _cancellationReason(Exit<Object?, Object?> exit) {
  if (exit case ExitFailure(:final cause)) {
    final causes = switch (cause) {
      CauseGroup(:final causes) => causes,
      _ => [cause],
    };
    for (final cause in causes) {
      if (cause case CauseCancelled(:final reason)) return reason;
    }
  }
  return null;
}

Future<TaskEvaluation<void, Never>> _runFinalizer(
  Scope scope,
  _Finalizer finalizer,
  Exit<Object?, Object?> exit,
) async {
  try {
    return await finalizer.run(finalizer.create(exit), scope);
  } catch (error, stackTrace) {
    return TaskEvaluation.fresh(Exit.failure(Cause.defect(error, stackTrace)));
  }
}

void _recordEvaluation(
  Scope scope,
  TaskEvaluation<void, Never> result,
  Object owner,
) {
  if (result.exit case ExitFailure(:final cause)) {
    final reasons = switch (cause) {
      CauseGroup(:final causes) => causes,
      _ => [cause],
    };
    final defects = result.events.iterator;
    for (final reason in reasons) {
      Object? source;
      if (reason is CauseDefect) {
        defects.moveNext();
        source = defects.current.source;
      }
      _record(scope, reason, owner, source: source);
    }
  }
}

// Record each cleanup event once in every ancestor owned by the same close.
// Child completion does not merge the same cause into those ancestors again.
void _record(Scope scope, Cause<Never> cause, Object owner, {Object? source}) {
  final events = switch (cause) {
    CauseGroup(:final causes) => [
      for (final item in causes) _CleanupEvent(item),
    ],
    _ => [_CleanupEvent(cause, source)],
  };
  for (Scope? current = scope; current != null; current = current._parent) {
    final closing = current._closing;
    if (closing == null || !identical(closing.owner, owner)) break;
    for (final event in events) {
      if (closing.sources.add(event.source)) closing.events.add(event);
    }
  }
}

void _finish(Scope scope) {
  final closing = scope._closing!;
  final cause = closing.events.isEmpty
      ? null
      : Cause<Never>.combine(closing.events.map((event) => event.cause));
  closing.children.clear();
  closing.fibers.clear();
  scope._parent?._children.remove(scope);
  scope._parent = null;
  closing.completion.complete(cause);
}

final class _Closing {
  _Closing(this.exit, this.owner, this.children, this.fibers);

  final Exit<Object?, Object?> exit;
  final Object owner;
  final List<Scope> children;
  final List<Fiber<Object?, Object?>> fibers;
  final completion = Completer<Cause<Never>?>();
  final events = <_CleanupEvent>[];
  final sources = <Object>{};
}

final class _CleanupEvent {
  _CleanupEvent(this.cause, [Object? source]) : source = source ?? Object();
  final Cause<Never> cause;
  final Object source;
}

final class _Finalizer {
  _Finalizer(this.create, this.run);

  final Task<void, Never> Function(Exit<Object?, Object?>) create;
  final Future<TaskEvaluation<void, Never>> Function(Task<void, Never>, Scope)
  run;
}
