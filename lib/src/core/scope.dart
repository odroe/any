import 'dart:async';

import 'cause.dart';
import 'exit.dart';

/// A lifetime for resources with synchronous or asynchronous cleanup.
///
/// Register cleanup with [addFinalizer], then await [close] when the lifetime
/// ends. Finalizers run one at a time in reverse registration order. Every
/// finalizer is attempted, even when an earlier finalizer throws.
///
/// The first call to [close] fixes the [Exit] passed to every finalizer. A
/// [ScopeCloseException] preserves this exit alongside a [Cause] describing
/// cleanup failures, if any.
///
/// ```dart
/// final scope = Scope();
/// final subscription = const Stream<int>.empty().listen((_) {});
/// scope.addFinalizer((_) => subscription.cancel());
///
/// await scope.close();
/// ```
final class Scope {
  /// Creates an open scope with no registered finalizers.
  Scope();

  List<FutureOr<void> Function(Exit<void, Object?>)>? _finalizers;
  Future<void>? _closeFuture;

  /// Whether [close] has been called, preventing further registrations.
  ///
  /// Cleanup may still be running. Await [close] to wait for it to finish.
  bool get isClosed => _closeFuture != null;

  /// Registers [finalizer] to run when this scope closes.
  ///
  /// The callback receives the exit supplied to the first call to [close]. Its
  /// returned future, if any, is awaited before the next finalizer runs.
  /// Each registration is invoked once.
  ///
  /// Throws a [StateError] if [close] has already been called.
  void addFinalizer(
    FutureOr<void> Function(Exit<void, Object?> exit) finalizer,
  ) {
    if (isClosed) {
      throw StateError('Cannot add a finalizer to a closed scope.');
    }
    (_finalizers ??= []).add(finalizer);
  }

  /// Closes this scope and awaits its finalizers with the supplied [exit].
  ///
  /// Defaults to successful completion without a value. Registrations stop
  /// immediately; cleanup begins in a microtask. Repeated calls return the same
  /// future and keep the original exit, including after cleanup has failed.
  ///
  /// If any finalizer throws, the future completes with a [ScopeCloseException]
  /// after all remaining finalizers have been attempted. That exception retains
  /// [exit] alongside a [CauseDefect] for each thrown object and its original
  /// stack trace. Multiple cleanup failures form a [CauseGroup] in finalizer
  /// execution order. The original exit is kept separate from the cleanup cause.
  ///
  /// A finalizer must not await [close] on this scope, since closure waits for
  /// that finalizer to finish.
  Future<void> close({Exit<void, Object?> exit = const Exit.success(null)}) =>
      _closeFuture ??= Future<void>.microtask(() => _runFinalizers(exit));

  Future<void> _runFinalizers(Exit<void, Object?> exit) async {
    final finalizers = _finalizers;
    _finalizers = null;
    if (finalizers == null) return;

    Cause<Never>? firstFailure;
    List<Cause<Never>>? failures;
    while (finalizers.isNotEmpty) {
      final finalizer = finalizers.removeLast();
      try {
        final result = finalizer(exit);
        if (result is Future<void>) await result;
      } catch (error, stackTrace) {
        final cause = Cause<Never>.defect(error, stackTrace);
        if (firstFailure == null) {
          firstFailure = cause;
        } else {
          (failures ??= [firstFailure]).add(cause);
        }
      }
    }

    if (firstFailure != null) {
      throw ScopeCloseException._(
        exit,
        failures == null ? firstFailure : Cause.combine(failures),
      );
    }
  }
}

/// A cleanup [cause] encountered while closing a [Scope], with its original exit.
///
/// Every finalizer has been attempted before this exception is reported. The
/// original task outcome remains available through [exit], even when it was
/// already a failure or cancellation.
final class ScopeCloseException implements Exception {
  ScopeCloseException._(this.exit, this.cause);

  /// The outcome supplied to the first call to [Scope.close].
  final Exit<void, Object?> exit;

  /// The cleanup failures, independent of the original [exit].
  ///
  /// A single failure is a [CauseDefect]. Multiple failures form a [CauseGroup]
  /// of defects in finalizer execution order. Every original thrown object and
  /// stack trace is retained. Cleanup does not introduce expected errors.
  final Cause<Never> cause;

  @override
  String toString() => 'ScopeCloseException: $cause';
}
