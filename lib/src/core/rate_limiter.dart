import 'dart:collection';
import 'dart:math' as math;

import 'cause.dart';
import 'clock.dart';
import 'exit.dart';
import 'task.dart';

/// A FIFO token bucket using the clock captured when it is created.
///
/// Tokens refill continuously at the configured rate, up to the burst capacity.
/// The bucket starts full. Acquiring tokens consumes them permanently; task
/// completion, failure, and cancellation do not return committed tokens.
///
/// This object is not scope-owned and needs no close operation. Each waiting
/// acquisition owns its cancellation cleanup. At most one timer exists while
/// requests wait for tokens, and idle limiters retain no timer.
final class RateLimiter {
  RateLimiter._(this._clock, this._rate, this._interval, this._burst)
    : _tokens = _burst.toDouble(),
      _updated = _clock.elapsed.inMicroseconds;

  final Clock _clock;
  final int _rate;
  final int _interval;
  final int _burst;
  double _tokens;
  int _updated;
  final _waiting = Queue<_Request>();
  _Request? _reserved;
  void Function()? _cancelTimer;
  int? _deadline;
  var _draining = false;

  /// Creates an independent, initially full bucket on each execution.
  ///
  /// [permits] tokens refill over each [interval], continuously rather than in
  /// batches. [burst] is the maximum stored balance and defaults to [permits].
  /// The current [Clock] service is captured and remains in use even when later
  /// acquisitions execute with a different clock service.
  ///
  /// Counts must be positive and at most `9007199254740991`; [interval] must
  /// contain at least one microsecond. Invalid parameters become [RangeError]
  /// defects when executed. The limiter outlives the scope that creates it.
  static Task<RateLimiter, Never> create({
    required int permits,
    required Duration interval,
    int? burst,
  }) => Task.defer(() {
    RangeError.checkValueInInterval(permits, 1, _maxExactInteger, 'permits');
    final capacity = burst ?? permits;
    RangeError.checkValueInInterval(capacity, 1, _maxExactInteger, 'burst');
    if (interval.inMicroseconds <= 0) {
      throw RangeError.value(
        interval.inMicroseconds,
        'interval',
        'Must be positive.',
      );
    }
    return Task.service(Clock.key).map(
      (clock) =>
          RateLimiter._(clock, permits, interval.inMicroseconds, capacity),
    );
  });

  /// Waits for and consumes [permits] tokens when executed.
  ///
  /// Requests are served in arrival order, including when a smaller request
  /// could fit before the queue head. A wake-up reserves a turn; consumption is
  /// committed only after this task resumes with cancellation masked. Cancelling
  /// before that confirmation leaves the balance unchanged. Once committed,
  /// cancellation does not refund tokens or guarantee a later callback runs.
  ///
  /// [permits] must be positive and no greater than the burst capacity; invalid
  /// counts become [RangeError] defects. Clock failures retain their original
  /// defect information and wake affected waiters. Timer delays round upward to
  /// microseconds, with a minimum positive delay when a retry is needed.
  Task<void, Never> acquire({int permits = 1}) =>
      Task.uninterruptibleMask<void, Never>((restore) {
        RangeError.checkValueInInterval(permits, 1, _burst, 'permits');
        final request = _Request(permits);
        return Task.addFinalizer((_) => Task.sync(() => _remove(request)))
            .flatMap(
              (_) => restore(
                Task.fromCallback<void, Never>((complete) {
                  request.complete = complete;
                  _waiting.addLast(request);
                  _drain();
                  return () => _remove(request);
                }),
              ),
            )
            .flatMap((_) => _confirm(request));
      }).scoped();

  Task<void, Never> _confirm(_Request request) {
    if (request.failure case final failure?) {
      return Task.failCause(failure);
    }
    _refill();
    _tokens -= request.permits;
    request.finished = true;
    _reserved = null;
    _drain();
    return Task.succeed(null);
  }

  void _remove(_Request request) {
    if (request.finished) return;
    request.finished = true;
    if (identical(_reserved, request)) {
      _reserved = null;
    } else {
      _waiting.remove(request);
    }
    _drain();
  }

  void _refill() {
    final now = _clock.elapsed.inMicroseconds;
    if (now < _updated) {
      throw StateError('The limiter clock moved backwards.');
    }
    _tokens = math.min(
      _burst.toDouble(),
      _tokens + (now - _updated) / _interval * _rate,
    );
    _updated = now;
  }

  void _stopTimer() {
    final cancel = _cancelTimer;
    _cancelTimer = null;
    _deadline = null;
    cancel?.call();
  }

  void _drain() {
    if (_draining) return;
    _draining = true;
    try {
      while (_reserved == null && _waiting.isNotEmpty) {
        _refill();
        final request = _waiting.first;
        if (_tokens < request.permits) {
          final delay = math.max(
            1,
            math
                .min(
                  _maxExactInteger.toDouble(),
                  (request.permits - _tokens) / _rate * _interval,
                )
                .ceil(),
          );
          final deadline = _updated + delay;
          if (_deadline != deadline) {
            _stopTimer();
            _deadline = deadline;
            _cancelTimer = _clock.schedule(Duration(microseconds: delay), () {
              _cancelTimer = null;
              _deadline = null;
              _drain();
            });
          }
          return;
        }
        _stopTimer();
        _waiting.removeFirst();
        _reserved = request;
        request.complete(const Exit.success(null));
      }
      _stopTimer();
    } catch (error, stackTrace) {
      _failWaiting(error, stackTrace);
    } finally {
      _draining = false;
    }
  }

  void _failWaiting(Object error, StackTrace stackTrace) {
    final failure = CauseDefect(error, stackTrace);
    final requests = [?_reserved, ..._waiting];
    _reserved = null;
    _waiting.clear();
    // A timer is cleared before invoking its cancellation function. If that
    // function was the failure, there is no second cancellation to attempt.
    final cancel = _cancelTimer;
    _cancelTimer = null;
    _deadline = null;
    if (requests.isEmpty) Error.throwWithStackTrace(error, stackTrace);
    Cause<Never> cause = failure;
    try {
      cancel?.call();
    } catch (cleanupError, cleanupStackTrace) {
      cause = Cause.combine([
        failure,
        Cause.defect(cleanupError, cleanupStackTrace),
      ]);
    }
    for (final request in requests) {
      request.finished = true;
      request.failure = cause;
      request.complete(Exit.failure(cause));
    }
  }
}

/// Starts tasks after acquiring tokens from a [RateLimiter].
extension RateLimiterOperations on RateLimiter {
  /// Consumes [permits] tokens before starting [task].
  ///
  /// The task's result and complete failure cause are preserved. Completion,
  /// failure, and cancellation do not return tokens already consumed.
  Task<T, E> run<T, E>(Task<T, E> task, {int permits = 1}) =>
      acquire(permits: permits).withError<E>().flatMap((_) => task);
}

final class _Request {
  _Request(this.permits);

  final int permits;
  late final void Function(Exit<void, Never>) complete;
  var finished = false;
  Cause<Never>? failure;
}

const _maxExactInteger = 9007199254740991;
