import 'dart:async';

/// Retains one payload and notifies listeners in their registration zones.
///
/// Publication is synchronous. A listener can unsubscribe another listener
/// before its turn; callbacks are removed before delivery so completion and
/// cancellation release their references even during reentrant notification.
final class Completion<T extends Object> {
  T? _value;
  final _listeners = <Object, void Function(T)>{};

  T? get value => _value;

  bool complete(T value) {
    if (_value != null) return false;
    _value = value;
    for (final token in _listeners.keys.toList()) {
      _listeners.remove(token)?.call(value);
    }
    return true;
  }

  void Function() listen(void Function(T) listener) {
    final notify = Zone.current.bindUnaryCallbackGuarded(listener);
    if (_value case final value?) {
      notify(value);
      return () {};
    }
    final token = Object();
    _listeners[token] = notify;
    return () {
      _listeners.remove(token);
    };
  }
}
