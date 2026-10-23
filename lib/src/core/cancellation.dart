import 'dart:async';

/// The authority to request cancellation through a shared [signal].
///
/// Pass [signal] to work that should respond to cancellation, and keep the
/// controller with the caller that decides when to stop. A request is cooperative:
/// listeners are notified synchronously, but the request does not wait for work
/// or resource cleanup to finish.
final class CancellationController {
  /// Creates a controller with an uncancelled signal.
  ///
  /// [onError] receives synchronous exceptions from cancellation listeners. By
  /// default, they are reported to the creating [Zone]. An exception thrown by
  /// [onError] is also reported to that zone; it does not prevent other listeners
  /// from being notified.
  CancellationController({
    void Function(Object error, StackTrace stackTrace)? onError,
  }) : _signal = _CancellationSignal(onError, Zone.current);

  final _CancellationSignal _signal;

  /// The read-only signal associated with this controller.
  CancellationSignal get signal => _signal;

  /// Requests cancellation with an optional diagnostic [reason].
  ///
  /// Only the first request takes effect, including when its reason is `null`.
  /// The signal is marked cancelled before listeners are notified synchronously.
  /// Listener exceptions are reported through this controller's error handler;
  /// remaining listeners still run.
  void requestCancel({Object? reason}) => _signal._requestCancel(reason);
}

/// A read-only view of a cancellation request.
///
/// Obtain a signal from [CancellationController.signal]. Consumers can inspect
/// [isCancelled] or subscribe with [onCancel]; only the controller can request
/// cancellation.
sealed class CancellationSignal {
  const CancellationSignal._();

  /// Whether cancellation has been requested.
  bool get isCancelled;

  /// The reason supplied with the first cancellation request, if any.
  ///
  /// A `null` reason may mean no request or a request without a reason. Check
  /// [isCancelled] to distinguish them.
  Object? get reason;

  /// Subscribes to cancellation and returns an unsubscribe function.
  ///
  /// [listener] is called synchronously in registration order. For an already
  /// cancelled signal, it runs before this method returns. Each registration is
  /// notified at most once, including duplicate registrations of one callback.
  ///
  /// The unsubscribe function is safe to call repeatedly. It prevents any
  /// notification that has not started, even when called by an earlier listener.
  /// It cannot stop a listener that is already running.
  ///
  /// Listener exceptions go to the controller's error handler without preventing
  /// other notifications. Asynchronous work started by a listener is not awaited.
  void Function() onCancel(void Function(Object? reason) listener);
}

final class _CancellationSignal extends CancellationSignal {
  _CancellationSignal(this._onError, this._zone) : super._();

  final void Function(Object, StackTrace)? _onError;
  final Zone _zone;
  var _isCancelled = false;
  Object? _reason;
  List<_CancellationListener>? _listeners = [];

  @override
  bool get isCancelled => _isCancelled;

  @override
  Object? get reason => _reason;

  @override
  void Function() onCancel(void Function(Object? reason) listener) {
    if (_isCancelled) {
      _notify(listener);
      return () {};
    }

    final subscription = _CancellationListener(listener);
    _listeners!.add(subscription);
    return () {
      subscription.callback = null;
      _listeners?.remove(subscription);
    };
  }

  void _requestCancel(Object? reason) {
    if (_isCancelled) return;

    _isCancelled = true;
    _reason = reason;
    final listeners = _listeners!;
    _listeners = null;
    for (final subscription in listeners) {
      final callback = subscription.callback;
      subscription.callback = null;
      if (callback != null) _notify(callback);
    }
  }

  void _notify(void Function(Object?) listener) {
    try {
      listener(_reason);
    } catch (error, stackTrace) {
      final onError = _onError;
      if (onError == null) {
        _zone.handleUncaughtError(error, stackTrace);
      } else {
        _zone.runGuarded(() => onError(error, stackTrace));
      }
    }
  }
}

final class _CancellationListener {
  _CancellationListener(this.callback);

  void Function(Object?)? callback;
}
