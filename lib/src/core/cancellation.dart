import 'dart:async';

/// Sends a cancellation request to a read-only [signal].
///
/// Cancellation is synchronous and cooperative. Requesting it notifies listeners;
/// it does not wait for their work or resource cleanup to finish.
final class CancellationController {
  /// Creates a controller that has not been cancelled.
  ///
  /// Synchronous listener exceptions are passed to [onError]. When it is omitted,
  /// the creating zone reports them through [Zone.handleUncaughtError]. If
  /// [onError] throws, its exception is also reported to the creating zone.
  CancellationController({
    void Function(Object error, StackTrace stackTrace)? onError,
  }) : _signal = _CancellationSignal(onError, Zone.current);

  final _CancellationSignal _signal;

  /// The read-only signal associated with this controller.
  CancellationSignal get signal => _signal;

  /// Requests cancellation, notifying listeners synchronously.
  ///
  /// The first request fixes [reason], including `null`. Later requests have
  /// no effect. The signal is marked cancelled before any listener is called.
  /// Listener exceptions are reported without preventing later notifications.
  void requestCancel({Object? reason}) => _signal._requestCancel(reason);
}

/// A read-only cancellation signal created by a [CancellationController].
///
/// A `null` [reason] can mean either no cancellation or cancellation without a
/// reason. Check [isCancelled] to distinguish them.
sealed class CancellationSignal {
  const CancellationSignal._();

  /// Whether cancellation has been requested.
  bool get isCancelled;

  /// The first cancellation reason, or `null` when no reason is available.
  ///
  /// Use [isCancelled] to determine whether cancellation has been requested.
  Object? get reason;

  /// Registers [listener] and returns a function that removes this subscription.
  ///
  /// Pending listeners are notified synchronously in registration order. If this
  /// signal is already cancelled, [listener] is called before this method
  /// returns. Each registration receives at most one notification, even when
  /// the same callback is registered more than once.
  ///
  /// Removing a subscription is idempotent. It prevents a notification that
  /// has not started, including removal by an earlier listener during
  /// cancellation. It cannot stop a callback that is already running.
  ///
  /// Synchronous listener exceptions are reported through the controller's
  /// error handler and do not prevent other notifications. Asynchronous work
  /// started by [listener] is not awaited.
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
