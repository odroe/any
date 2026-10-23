import 'dart:async';

import 'context.dart';

/// A replaceable source of wall time, monotonic time, and one-shot timers.
///
/// Use [now] for timestamps and [elapsed] for deadlines and durations. Wall time
/// can change when the system clock is adjusted; monotonic time does not.
abstract interface class Clock {
  /// Creates a system clock backed by [DateTime], [Stopwatch], and [Timer].
  ///
  /// Each instance has its own monotonic origin, starting at construction.
  factory Clock() = _SystemClock;

  /// The standard service key for the clock used by an application.
  static final key = ServiceKey<Clock>('clock');

  /// Returns the current wall time in UTC.
  DateTime now();

  /// The nondecreasing duration since this clock's monotonic origin.
  ///
  /// Only compare readings from the same clock. System wall-time adjustments
  /// do not affect this value.
  Duration get elapsed;

  /// Schedules [callback] once after [delay] and returns a cancellation function.
  ///
  /// Nonpositive delays are treated as zero. The callback never runs before
  /// this method returns; it executes in the zone where it was registered.
  /// That zone also receives any exception thrown by the callback. Asynchronous
  /// work started by the callback is not awaited.
  ///
  /// The returned function synchronously cancels a callback that has not
  /// started. Calling it repeatedly or after completion has no effect; it
  /// cannot stop a callback that is already running. The caller owns the timer
  /// and must cancel it if it is no longer needed.
  void Function() schedule(Duration delay, void Function() callback);
}

final class _SystemClock implements Clock {
  final Stopwatch _stopwatch = Stopwatch()..start();

  @override
  DateTime now() => DateTime.now().toUtc();

  @override
  Duration get elapsed => _stopwatch.elapsed;

  @override
  void Function() schedule(Duration delay, void Function() callback) =>
      Timer(delay, callback).cancel;
}
