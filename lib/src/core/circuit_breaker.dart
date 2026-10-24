import 'cause.dart';
import 'clock.dart';
import 'exit.dart';
import 'task.dart';

/// The admission state of a [CircuitBreaker].
enum CircuitBreakerState {
  /// Tasks are admitted and consecutive failures are counted.
  closed,

  /// Tasks are rejected until a later admission reaches the reset deadline.
  open,

  /// A limited number of tasks can probe whether the operation has recovered.
  halfOpen,
}

/// A circuit breaker that rejects work while a failing dependency recovers.
///
/// Protect calls with [CircuitBreakerOperations.protect]. Consecutive failures
/// open the breaker. After the reset timeout, a limited number of calls may
/// probe the dependency: the first success closes the breaker, while a counted
/// failure reopens it. Results from an earlier open/closed period do not change
/// the current state.
///
/// Share one instance among calls to the same dependency. It uses the clock
/// captured at creation, needs no close operation, and never retries work
/// automatically.
///
/// ```dart
/// final program =
///     CircuitBreaker.create(
///       failureThreshold: 3,
///       resetTimeout: const Duration(seconds: 30),
///     ).withError<String>().flatMap(
///       (breaker) => breaker.protect(
///         Task.fail<String>('Service unavailable'),
///         onOpen: () => 'Circuit open',
///       ),
///     );
/// final runtime = Runtime();
/// try {
///   final exit = await runtime.run(program);
///   assert(exit is ExitFailure<String, String>);
/// } finally {
///   await runtime.close();
/// }
/// ```
final class CircuitBreaker {
  CircuitBreaker._(
    this._clock,
    this._failureThreshold,
    this._resetTimeout,
    this._maxConcurrentProbes,
  );

  final Clock _clock;
  final int _failureThreshold;
  final Duration _resetTimeout;
  final int _maxConcurrentProbes;
  var _state = CircuitBreakerState.closed;
  var _generation = 0;
  var _consecutiveFailures = 0;
  var _activeProbes = 0;
  Duration? _retryAt;

  /// Creates an independent, initially closed breaker on each execution.
  ///
  /// [failureThreshold] is the number of consecutive counted failures that
  /// opens the breaker. [resetTimeout] starts when it opens.
  /// [maxConcurrentProbes] limits simultaneous admissions while half-open.
  ///
  /// Counts and timeout microseconds must be positive and at most
  /// `9007199254740991`. Invalid values become [RangeError] defects when executed.
  /// The current [Clock] service remains in use for this breaker's lifetime.
  static Task<CircuitBreaker, Never> create({
    required int failureThreshold,
    required Duration resetTimeout,
    int maxConcurrentProbes = 1,
  }) => Task.defer(() {
    RangeError.checkValueInInterval(
      failureThreshold,
      1,
      _maxExactInteger,
      'failureThreshold',
    );
    RangeError.checkValueInInterval(
      resetTimeout.inMicroseconds,
      1,
      _maxExactInteger,
      'resetTimeout',
    );
    RangeError.checkValueInInterval(
      maxConcurrentProbes,
      1,
      _maxExactInteger,
      'maxConcurrentProbes',
    );
    return Task.service(Clock.key).map(
      (clock) => CircuitBreaker._(
        clock,
        failureThreshold,
        resetTimeout,
        maxConcurrentProbes,
      ),
    );
  });

  /// The current admission state and failure counts.
  ///
  /// `retryAt` is a deadline on the captured clock's [Clock.elapsed], present
  /// only while open. Reading this snapshot does not advance the state; the next
  /// admission after the deadline starts probing.
  ///
  /// `consecutiveFailures` survives neutral outcomes and open/half-open periods,
  /// and resets on a success in the current period. `activeProbes` counts only
  /// probes in the current half-open period.
  ({
    CircuitBreakerState state,
    int consecutiveFailures,
    int activeProbes,
    Duration? retryAt,
  })
  get snapshot => (
    state: _state,
    consecutiveFailures: _consecutiveFailures,
    activeProbes: _activeProbes,
    retryAt: _retryAt,
  );

  _Admission? _admit() {
    if (_state == CircuitBreakerState.open) {
      if (_clock.elapsed < _retryAt!) return null;
      _state = CircuitBreakerState.halfOpen;
      _generation++;
      _retryAt = null;
    }
    final probe = _state == CircuitBreakerState.halfOpen;
    if (probe) {
      if (_activeProbes == _maxConcurrentProbes) return null;
      _activeProbes++;
    }
    return _Admission(_generation, probe);
  }

  Cause<Never>? _record<T, E>(
    _Admission admission,
    Exit<T, E> exit,
    bool Function(E)? isFailure,
  ) {
    if (admission.generation != _generation) return null;
    if (admission.probe) _activeProbes--;
    if (exit is ExitSuccess<T, E>) {
      _consecutiveFailures = 0;
      if (_state == CircuitBreakerState.halfOpen) {
        _state = CircuitBreakerState.closed;
        _generation++;
        _activeProbes = 0;
      }
      return null;
    }

    var counted = false;
    Cause<Never>? policyFailure;
    try {
      counted = _counts((exit as ExitFailure<T, E>).cause, isFailure);
    } catch (error, stackTrace) {
      counted = true;
      policyFailure = Cause.defect(error, stackTrace);
    }
    if (counted) {
      try {
        _recordFailure();
      } catch (error, stackTrace) {
        return Cause.combine([?policyFailure, Cause.defect(error, stackTrace)]);
      }
    }
    return policyFailure;
  }

  void _recordFailure() {
    if (_consecutiveFailures < _failureThreshold) _consecutiveFailures++;
    if (_state != CircuitBreakerState.halfOpen &&
        _consecutiveFailures < _failureThreshold) {
      return;
    }
    final elapsed = _clock.elapsed.inMicroseconds;
    RangeError.checkValueInInterval(
      elapsed,
      0,
      _maxExactInteger - _resetTimeout.inMicroseconds,
      'clock.elapsed',
    );
    _retryAt = Duration(microseconds: elapsed) + _resetTimeout;
    _state = CircuitBreakerState.open;
    _generation++;
    _activeProbes = 0;
  }
}

/// Runs tasks through a [CircuitBreaker]'s admission and failure policy.
extension CircuitBreakerOperations on CircuitBreaker {
  /// Admits [task], or fails with [onOpen] when no admission is available.
  ///
  /// [onOpen] runs only when the breaker is open or all probe slots are in use.
  /// Its exception becomes a defect; rejected tasks do not affect the state.
  ///
  /// Admitted work inherits the caller's context, resource scope, and
  /// cancellation state. Its outcome is counted when the expression completes:
  ///
  /// * Success clears consecutive failures and closes a half-open breaker.
  /// * Business errors count unless [isFailure] returns false; defects always
  ///   count. A group counts once if any of its reasons counts.
  /// * Cancellation and ignored errors are neutral. They do not reset failures
  ///   or close the breaker. A cleanup defect still counts after cancellation.
  ///
  /// [isFailure] is called in cause order. If it throws, the execution counts as
  /// a failure and the defect is appended to the original cause. Results from
  /// earlier open/closed periods neither invoke the policy nor update state.
  /// Cancelling a probe releases its slot without reporting success.
  ///
  /// Use `protect(task.scoped(), ...)` to include resource cleanup in the
  /// protected outcome. Otherwise those resources outlive this operation.
  Task<T, E> protect<T, E>(
    Task<T, E> task, {
    required E Function() onOpen,
    bool Function(E error)? isFailure,
  }) => Task.uninterruptibleMask<T, E>((restore) {
    final admission = _admit();
    if (admission == null) return Task.fail(onOpen());
    return restore(task).onExit((exit) {
      final failure = _record(admission, exit, isFailure);
      return failure == null ? Task.succeed(null) : Task.failCause(failure);
    });
  });
}

bool _counts<E>(Cause<E> cause, bool Function(E)? isFailure) {
  switch (cause) {
    case CauseFailure(:final error):
      return isFailure?.call(error) ?? true;
    case CauseDefect():
      return true;
    case CauseCancelled():
      return false;
    case CauseGroup(:final causes):
      var counted = false;
      for (final reason in causes) {
        if (_counts(reason, isFailure)) counted = true;
      }
      return counted;
  }
}

final class _Admission {
  const _Admission(this.generation, this.probe);

  final int generation;
  final bool probe;
}

const _maxExactInteger = 9007199254740991;
