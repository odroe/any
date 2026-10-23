import 'dart:async';
import 'dart:collection';

import 'package:any/any.dart';

/// A clock whose time and timers advance only when the test requests it.
///
/// Bind an instance to [Clock.key] when creating a [Runtime] or providing a
/// [Context]. Timers run during [advance], in deadline order, with equal
/// deadlines ordered by registration. Advancing does not wait for asynchronous
/// work started by a timer; await the relevant task or registration separately.
final class TestClock implements Clock {
  /// Creates a clock at [start], or the Unix epoch, with zero elapsed time.
  TestClock({DateTime? start})
    : _now = (start ?? DateTime.fromMillisecondsSinceEpoch(0, isUtc: true))
          .toUtc();

  static const _maxMicroseconds = 9007199254740991;

  final _timers = SplayTreeMap<int, Set<_TestTimer>>();
  DateTime _now;
  int _elapsed = 0;
  int _pendingCount = 0;
  bool _advancing = false;

  @override
  DateTime now() => _now;

  @override
  Duration get elapsed => Duration(microseconds: _elapsed);

  /// The number of registered timers that have not fired or been cancelled.
  int get pendingCount => _pendingCount;

  /// Changes wall time without advancing monotonic time or running timers.
  ///
  /// Use this to verify that deadlines do not depend on wall-clock changes.
  void setWallTime(DateTime time) => _now = time.toUtc();

  @override
  void Function() schedule(Duration delay, void Function() callback) {
    final micros = delay.isNegative ? 0 : delay.inMicroseconds;
    _checkAdvance(micros);
    final deadline = _elapsed + micros;
    final timer = _TestTimer(Zone.current.bindCallbackGuarded(callback));
    (_timers[deadline] ??= {}).add(timer);
    _pendingCount++;

    return () {
      final bucket = _timers[deadline];
      if (bucket == null || !bucket.remove(timer)) return;
      _pendingCount--;
      if (bucket.isEmpty) _timers.remove(deadline);
    };
  }

  /// Advances time by [duration] and synchronously fires every due timer.
  ///
  /// Each callback observes its own deadline as the current time. Timers
  /// registered by callbacks also fire if their deadline falls within this
  /// advance. A zero duration fires timers already due, including zero-delay
  /// timers; [schedule] itself never invokes a callback inline.
  ///
  /// Throws [ArgumentError] for a negative duration, [RangeError] if elapsed
  /// time would exceed the largest exactly representable JavaScript integer
  /// (2^53 - 1 microseconds), or [StateError] for a reentrant call. Wall time
  /// must remain within [DateTime]'s supported range. Callback exceptions go
  /// to the registration zone and do not prevent other timers from firing.
  void advance(Duration duration) {
    if (_advancing) throw StateError('TestClock.advance is not reentrant.');
    if (duration.isNegative) {
      throw ArgumentError.value(duration, 'duration', 'Must not be negative.');
    }
    _checkAdvance(duration.inMicroseconds);
    final target = _elapsed + duration.inMicroseconds;
    // Check the range before changing either clock or consuming timers.
    _now.add(duration);
    _advancing = true;
    try {
      while (_timers.isNotEmpty) {
        final deadline = _timers.firstKey()!;
        if (deadline > target) break;
        _moveTo(deadline);
        final bucket = _timers[deadline]!;
        final timer = bucket.first;
        bucket.remove(timer);
        if (bucket.isEmpty) _timers.remove(deadline);
        _pendingCount--;
        timer.callback();
      }
      _moveTo(target);
    } finally {
      _advancing = false;
    }
  }

  void _moveTo(int micros) {
    _now = _now.add(Duration(microseconds: micros - _elapsed));
    _elapsed = micros;
  }

  void _checkAdvance(int micros) {
    if (micros > _maxMicroseconds - _elapsed) {
      throw RangeError('TestClock time exceeds 2^53 - 1 microseconds.');
    }
  }
}

final class _TestTimer {
  _TestTimer(this.callback);

  final void Function() callback;
}
