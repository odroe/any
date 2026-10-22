import 'dart:async';

/// A controller that requests cooperative cancellation through its [token].
///
/// Pass [token] to the work that should observe cancellation, and retain the
/// controller to request it. A request notifies listeners; it does not wait for
/// the work to stop or release its resources.
///
/// ```dart
/// final controller = CancellationController();
/// final timer = Timer(const Duration(seconds: 1), () => print('Finished.'));
/// final removeListener = controller.token.onCancel((_) => timer.cancel());
///
/// controller.cancel(reason: 'No longer needed.');
/// removeListener();
/// ```
final class CancellationController {
  /// Creates a controller with a token that has not been cancelled.
  CancellationController();

  /// The read-only signal shared with work that supports cancellation.
  final CancellationToken token = CancellationToken._();

  /// Requests cancellation with an optional [reason].
  ///
  /// The first call sets the token's state and synchronously notifies its
  /// listeners in registration order. Later calls do nothing and preserve the
  /// first reason, including null.
  void cancel({Object? reason}) => token._cancel(reason);
}

/// A read-only signal that work can observe to stop cooperatively.
///
/// Obtain a token from [CancellationController.token]. Check [isCancelled]
/// between steps, or use [onCancel] to notify an operation that can be stopped.
/// Cancellation does not interrupt arbitrary synchronous code or futures.
final class CancellationToken {
  CancellationToken._();

  /// A shared token that never requests cancellation or retains listeners.
  static final CancellationToken none = CancellationToken._();

  bool _isCancelled = false;
  Object? _reason;
  Map<Object, void Function(Object?)>? _listeners;

  /// Whether cancellation has been requested, even if work is still running.
  bool get isCancelled => _isCancelled;

  /// The reason supplied to the first cancellation request, if any.
  ///
  /// Null can mean either no request or a request without a reason. Use
  /// [isCancelled] to distinguish those cases.
  Object? get reason => _reason;

  /// Registers [listener] and returns a function that removes the registration.
  ///
  /// Each registration is called once with [reason] when cancellation is
  /// requested, unless removed first. If already cancelled, [listener] is called
  /// synchronously before this method returns. Removing a registration is safe
  /// to repeat, including during notification or after cancellation.
  ///
  /// Remove the listener when its operation finishes to release captured
  /// resources. A listener must be synchronous; signal the operation to stop,
  /// then wait for that operation separately before releasing its resources.
  ///
  /// Listeners run in the zone where they were registered. Thrown errors are
  /// reported as uncaught errors to that zone without skipping other listeners.
  void Function() onCancel(void Function(Object? reason) listener) {
    if (identical(this, none)) return _ignore;

    final notify = Zone.current.bindUnaryCallbackGuarded(listener);
    if (_isCancelled) {
      notify(_reason);
      return _ignore;
    }

    final key = Object();
    (_listeners ??= {})[key] = notify;
    return () {
      _listeners?.remove(key);
      if (_listeners?.isEmpty ?? false) _listeners = null;
    };
  }

  void _cancel(Object? reason) {
    if (_isCancelled) return;
    _isCancelled = true;
    _reason = reason;

    final listeners = _listeners;
    if (listeners == null) return;
    for (final key in listeners.keys.toList(growable: false)) {
      listeners.remove(key)?.call(reason);
    }
    _listeners = null;
  }
}

void _ignore() {}
