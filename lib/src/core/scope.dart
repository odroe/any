import 'dart:async';

import 'cause.dart';
import 'exit.dart';
import 'task.dart';

/// Owns child scopes and resource finalizers.
///
/// Scopes are created and closed by the task runtime. Their lifecycle state is
/// private; finalizers are executed by the runtime that registered them.
final class Scope {
  Scope._(this._parent);

  /// Creates a child of the current task scope when the returned task runs.
  ///
  /// The parent closes the child if it has not already closed. Creating a child
  /// in a closing scope fails with a [StateError] defect.
  static Task<Scope, Never> open() => openScopeTask();

  /// Closes this scope with [exit], waiting for all registered cleanup.
  ///
  /// The first close fixes the exit passed to finalizers; later closes observe
  /// the same cleanup result. Only cleanup failures propagate from this task,
  /// not the supplied body exit. Waiting on the task's active scope, an ancestor,
  /// or an unfinished scope sharing its cleanup driver produces a [StateError]
  /// defect instead of a self-wait.
  Task<void, Never> close(Exit<Object?, Object?> exit) =>
      closeScopeTask(this, exit);

  Scope? _parent;
  final _children = <Scope>{};
  final _finalizers = <_Finalizer>[];
  _Closing? _closing;
}

// Internal lifecycle operations. These declarations are not exported by any.dart.

/// Creates a root scope, or registers a child of [parent].
///
/// Throws a [StateError] when [parent] is closing or closed.
Scope createScope({Scope? parent}) {
  if (parent?._closing != null) {
    throw StateError('Cannot create a child in a closing or closed scope.');
  }
  final scope = Scope._(parent);
  parent?._children.add(scope);
  return scope;
}

/// Whether [descendant] belongs to the live subtree rooted at [ancestor].
///
/// Includes [ancestor] itself until it has closed. The executor uses this to
/// reject self and ancestor waits before requesting closure.
bool scopeContains(Scope ancestor, Scope descendant) {
  if (ancestor._closing?.completion.isCompleted ?? false) return false;
  for (Scope? scope = descendant; scope != null; scope = scope._parent) {
    if (identical(scope, ancestor)) return true;
  }
  return false;
}

// A finalizer also cannot await a sibling queued behind it by the same driver.
bool scopeCloseWouldDeadlock(Scope target, Scope current) {
  if (target._closing?.completion.isCompleted ?? false) return false;
  if (scopeContains(target, current)) return true;
  final targetOwner = target._closing?.owner;
  if (targetOwner == null) return false;
  for (Scope? scope = current; scope != null; scope = scope._parent) {
    if (identical(scope._closing?.owner, targetOwner)) return true;
  }
  return false;
}

/// Registers [finalizer], or runs it immediately if [scope] has closed.
///
/// Open and closing scopes queue the finalizer synchronously and return `null`.
/// A closed scope uses its original closing exit and returns the late
/// finalizer's cleanup cause without changing the completed close result.
///
/// [runFinalizer] executes with cancellation masked and a temporary scope for
/// resources used by cleanup. The supplied owning scope identifies self-waits.
/// The executor captures the registration-time execution environment; Scope
/// only manages ordering and failure accumulation.
FutureOr<Cause<Never>?> addScopeFinalizer(
  Scope scope,
  Task<void, Never> Function(Exit<Object?, Object?> exit) finalizer, {
  required Future<Exit<void, Never>> Function(
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

/// Closes [scope], returning only its cleanup failures, or `null` on success.
///
/// The first call fixes [exit] and synchronously marks all open descendants as
/// closing. Repeated calls share the same Future and result. Children finish
/// before parent finalizers; sibling scopes and finalizers use reverse
/// registration order. Every finalizer is attempted even after failures.
///
/// A child already closing independently is awaited, and its cleanup cause is
/// included once. A child that closed before this call has detached and is not
/// included again. The body's [exit] is passed to finalizers but never merged
/// into the cleanup result.
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
    current._closing = _Closing(exit, owner, children);
    claimed.add(current);
    pending.addAll(children.reversed);
  }
  unawaited(_closeClaimed(claimed, owner));
  return scope._closing!.completion.future;
}

Future<void> _closeClaimed(List<Scope> claimed, Object owner) async {
  try {
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
          if (cause != null) _record(scope, cause, owner);
        }
        continue;
      }
      while (scope._finalizers.isNotEmpty) {
        final cause = await _runFinalizer(
          scope,
          scope._finalizers.removeLast(),
          closing.exit,
        );
        if (cause != null) _record(scope, cause, owner);
      }
      _finish(scope);
      pending.removeLast();
    }
  } catch (error, stackTrace) {
    // Callback failures are contained below. An internal invariant failure
    // must still settle the close Futures rather than leave callers suspended.
    for (final scope in claimed.reversed) {
      if (!scope._closing!.completion.isCompleted) {
        scope._closing!.causes.add(Cause.defect(error, stackTrace));
        _finish(scope);
      }
    }
  }
}

Future<Cause<Never>?> _runFinalizer(
  Scope scope,
  _Finalizer finalizer,
  Exit<Object?, Object?> exit,
) async {
  try {
    final result = await finalizer.run(finalizer.create(exit), scope);
    return switch (result) {
      ExitFailure(:final cause) => cause,
      _ => null,
    };
  } catch (error, stackTrace) {
    return Cause.defect(error, stackTrace);
  }
}

// Record each cleanup event once in every ancestor owned by the same close.
// Child completion does not merge the same cause into those ancestors again.
void _record(Scope scope, Cause<Never> cause, Object owner) {
  for (Scope? current = scope; current != null; current = current._parent) {
    final closing = current._closing;
    if (closing == null || !identical(closing.owner, owner)) break;
    closing.causes.add(cause);
  }
}

void _finish(Scope scope) {
  final closing = scope._closing!;
  final cause = closing.causes.isEmpty
      ? null
      : Cause<Never>.combine(closing.causes);
  closing.causes.clear();
  closing.children.clear();
  scope._parent?._children.remove(scope);
  scope._parent = null;
  closing.completion.complete(cause);
}

final class _Closing {
  _Closing(this.exit, this.owner, this.children);

  final Exit<Object?, Object?> exit;
  final Object owner;
  final List<Scope> children;
  final completion = Completer<Cause<Never>?>();
  final causes = <Cause<Never>>[];
}

final class _Finalizer {
  _Finalizer(this.create, this.run);

  final Task<void, Never> Function(Exit<Object?, Object?>) create;
  final Future<Exit<void, Never>> Function(Task<void, Never>, Scope) run;
}
