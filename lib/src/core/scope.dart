import 'dart:async';

import 'exit.dart';

/// A lifetime for resources with synchronous or asynchronous cleanup.
///
/// Register cleanup with [addFinalizer], then await [close] when the lifetime
/// ends. Finalizers run one at a time in reverse registration order. Every
/// finalizer is attempted, even when an earlier finalizer throws.
///
/// The first call to [close] fixes the [Exit] passed to every finalizer. A
/// [ScopeCloseException] preserves this exit and all cleanup errors if cleanup
/// fails.
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
  /// [exit] alongside every cleanup error and its original stack trace.
  ///
  /// A finalizer must not await [close] on this scope, since closure waits for
  /// that finalizer to finish.
  Future<void> close({Exit<void, Object?> exit = const Exit.success(null)}) =>
      _closeFuture ??= Future<void>.microtask(() => _runFinalizers(exit));

  Future<void> _runFinalizers(Exit<void, Object?> exit) async {
    final finalizers = _finalizers;
    _finalizers = null;
    if (finalizers == null) return;

    List<AsyncError>? errors;
    while (finalizers.isNotEmpty) {
      final finalizer = finalizers.removeLast();
      try {
        final result = finalizer(exit);
        if (result is Future<void>) await result;
      } catch (error, stackTrace) {
        (errors ??= []).add(AsyncError(error, stackTrace));
      }
    }

    if (errors != null) {
      throw ScopeCloseException._(exit, errors);
    }
  }
}

/// Errors encountered while closing a [Scope], together with its original exit.
///
/// Every finalizer has been attempted before this exception is reported. The
/// original task outcome remains available through [exit], even when it was
/// already a failure or cancellation.
final class ScopeCloseException implements Exception {
  ScopeCloseException._(this.exit, List<AsyncError> errors)
    : errors = List.unmodifiable(errors);

  /// The outcome supplied to the first call to [Scope.close].
  final Exit<void, Object?> exit;

  /// Cleanup errors and their original stack traces, in execution order.
  ///
  /// This list is nonempty and unmodifiable.
  final List<AsyncError> errors;

  @override
  String toString() =>
      'ScopeCloseException: ${errors.map((error) => error.error).join('; ')}';
}
