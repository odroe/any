import 'dart:math' as math;

import 'random_source.dart';

/// An immutable policy for deciding whether and when to run another attempt.
///
/// Use a schedule with a task's `retry` or `repeat` operation. It governs attempts
/// after the initial execution and can be reused without sharing counters.
///
/// For example, allow five retries with exponential backoff, capped at one
/// second and randomized to avoid retrying clients waking together:
///
/// ```dart
/// final policy = Schedule.exponential(const Duration(milliseconds: 100))
///     .cap(const Duration(seconds: 1))
///     .jitter()
///     .limit(5);
/// ```
///
/// Combinators apply in call order. Delays are rounded to the nearest microsecond
/// after all combinators have run. A final delay must be finite and no greater
/// than `2^53 - 1` microseconds; otherwise evaluation throws [RangeError]. Use
/// [cap] to bound exponential growth before this conversion.
final class Schedule {
  const Schedule._(this._delay);

  final num? Function(int iteration, Duration elapsed, RandomSource random)
  _delay;

  /// Allows [count] additional executions with no delay between them.
  ///
  /// `Schedule.recurs(3)` permits three executions after the initial attempt.
  /// Zero stops immediately. A negative [count] throws [RangeError].
  factory Schedule.recurs(int count) {
    RangeError.checkNotNegative(count, 'count');
    return Schedule._((iteration, _, _) => iteration < count ? 0 : null);
  }

  /// Continues indefinitely, waiting [delay] before each additional execution.
  ///
  /// The delay starts after the preceding attempt finishes; this is not a
  /// fixed-rate interval. A negative [delay] throws [RangeError].
  factory Schedule.spaced(Duration delay) {
    _checkDuration(delay, 'delay');
    return Schedule._((_, _, _) => delay.inMicroseconds);
  }

  /// Continues with delays `base`, `base * factor`, `base * factor^2`, and so on.
  ///
  /// [base] must be nonnegative, and [factor] must be finite and positive.
  /// Invalid arguments throw [RangeError]. A factor below one decreases the
  /// delay; one keeps it constant. Use [cap] to limit exponential growth.
  factory Schedule.exponential(Duration base, {double factor = 2}) {
    _checkDuration(base, 'base');
    if (!factor.isFinite || factor <= 0) {
      throw RangeError.value(factor, 'factor', 'Must be finite and positive.');
    }
    return Schedule._((iteration, _, _) {
      if (base == Duration.zero || iteration == 0 || factor == 1) {
        return base.inMicroseconds;
      }
      return base.inMicroseconds * math.pow(factor, iteration);
    });
  }

  /// Continues only while both policies continue, using the longer delay.
  ///
  /// Both policies receive the same iteration and elapsed time. If this policy
  /// stops, [other] is not evaluated.
  Schedule and(Schedule other) => Schedule._((iteration, elapsed, random) {
    final left = _delay(iteration, elapsed, random);
    if (left == null) return null;
    final right = other._delay(iteration, elapsed, random);
    return right == null ? null : math.max(left, right);
  });

  /// Continues while either policy continues, using the shorter active delay.
  ///
  /// Both policies are evaluated from left to right with the same iteration
  /// and elapsed time. If one stops, the other's delay is used. An evaluation
  /// error still propagates even when the other policy could continue.
  Schedule or(Schedule other) => Schedule._((iteration, elapsed, random) {
    final left = _delay(iteration, elapsed, random);
    final right = other._delay(iteration, elapsed, random);
    if (left == null) return right;
    if (right == null) return left;
    return math.min(left, right);
  });

  /// Permits at most [count] additional executions under this policy.
  ///
  /// A policy that stops earlier is not extended. Once the limit is reached,
  /// the underlying policy is not evaluated. Negative counts throw [RangeError].
  Schedule limit(int count) {
    RangeError.checkNotNegative(count, 'count');
    return Schedule._(
      (iteration, elapsed, random) =>
          iteration < count ? _delay(iteration, elapsed, random) : null,
    );
  }

  /// Limits this policy's delay to [maximum] without changing when it stops.
  ///
  /// Also bounds exponential overflow. Combinator order matters: a later
  /// [jitter] can increase the delay beyond this cap when its multiplier exceeds
  /// one. A negative [maximum] throws [RangeError].
  Schedule cap(Duration maximum) {
    _checkDuration(maximum, 'maximum');
    return Schedule._((iteration, elapsed, random) {
      final delay = _delay(iteration, elapsed, random);
      return delay == null ? null : math.min(delay, maximum.inMicroseconds);
    });
  }

  /// Continues only when elapsed time at the decision is below [maximum].
  ///
  /// This is an admission limit, not a timeout: an allowed delay or attempt can
  /// extend past [maximum]. Once the limit is reached, the underlying policy is
  /// not evaluated. Zero allows no additional executions; negative durations
  /// throw [RangeError].
  Schedule whileElapsed(Duration maximum) {
    _checkDuration(maximum, 'maximum');
    return Schedule._(
      (iteration, elapsed, random) =>
          elapsed < maximum ? _delay(iteration, elapsed, random) : null,
    );
  }

  /// Multiplies each continuing delay by a random factor between [min] and [max].
  ///
  /// The default spreads retries from zero up to the original delay. Bounds
  /// must be finite and satisfy `0 <= min <= max`, or this throws [RangeError].
  /// The multiplier is sampled from `[min, max)`; floating-point arithmetic and
  /// microsecond rounding can produce the rounded upper endpoint.
  ///
  /// Stopped policies, zero delays, and equal bounds consume no random draw.
  /// Equal bounds apply a fixed multiplier. An invalid random draw throws
  /// [StateError] when the policy is evaluated.
  Schedule jitter({double min = 0, double max = 1}) {
    if (!min.isFinite || min < 0) {
      throw RangeError.value(min, 'min', 'Must be finite and nonnegative.');
    }
    if (!max.isFinite || max < min) {
      throw RangeError.value(max, 'max', 'Must be finite and at least min.');
    }
    return Schedule._((iteration, elapsed, random) {
      final delay = _delay(iteration, elapsed, random);
      if (delay == null || delay == 0) return delay;
      if (min == max) return min == 0 ? 0 : delay * min;
      final draw = random.nextDouble();
      if (!draw.isFinite || draw < 0 || draw >= 1) {
        throw StateError(
          'RandomSource.nextDouble() must return a value in [0, 1).',
        );
      }
      final multiplier = min + (max - min) * draw;
      return multiplier == 0 ? 0 : delay * multiplier;
    });
  }
}

// Internal decision boundary shared by retry and repeat, not exported by any.dart.
// Iteration zero is the first execution after the initial attempt. The caller
// owns its counter, monotonic start time, and random source for each run.
Duration? scheduleDelay(
  Schedule schedule, {
  required int iteration,
  required Duration elapsed,
  required RandomSource random,
}) {
  RangeError.checkNotNegative(iteration, 'iteration');
  _checkDuration(elapsed, 'elapsed');
  final delay = schedule._delay(iteration, elapsed, random);
  if (delay == null) return null;
  if (!delay.isFinite || delay < 0 || delay > _maxMicroseconds) {
    throw RangeError.value(
      delay,
      'delay',
      'Must be finite and between 0 and $_maxMicroseconds microseconds.',
    );
  }
  return Duration(microseconds: delay.round());
}

const _maxMicroseconds = 9007199254740991;

void _checkDuration(Duration duration, String name) {
  if (duration.isNegative) {
    throw RangeError.value(
      duration.inMicroseconds,
      name,
      'Must be nonnegative.',
    );
  }
}
