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

/// Stops admitting work after repeated failures in the same isolate.
///
/// The breaker starts closed. Reaching the consecutive failure threshold opens
/// it. After the reset timeout, the next admission starts a half-open period
/// with limited probes. The first successful probe closes the breaker; a counted
/// failure opens it again. Results from a previous state generation do not
/// change the current state or invoke its failure policy.
///
/// The clock is captured at creation. There are no background timers or owned
/// resources, so the breaker does not need to be closed. Protected work is never
/// retried automatically.
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

  /// Returns the current state without reading the clock or admitting a probe.
  ///
  /// `retryAt` is the captured clock's monotonic elapsed deadline, not wall time.
  /// It is non-null only while open. An expired deadline remains in this
  /// snapshot until an admission starts the half-open period.
  /// `consecutiveFailures` is retained across neutral outcomes and while open
  /// or half-open, and resets when a successful current-generation task closes
  /// the breaker. `activeProbes` counts only the current half-open generation.
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
  /// Admitted work runs in an owned child fiber, inheriting the caller's
  /// services, task-local values, and cancellation state. Results are recorded
  /// from its final exit after work and resource cleanup finish. A successful
  /// task clears consecutive failures. Business failures count unless
  /// [isFailure] returns false, and defects always count. A group counts once if
  /// any reason counts. Cancellation and ignored business failures are neutral;
  /// they neither reset the count nor close a half-open breaker. Cancellation
  /// accompanied by a cleanup defect still counts as a failure.
  ///
  /// [isFailure] is called in cause order for each business error in a current
  /// generation. If it throws, that execution counts as one failure and its
  /// defect is appended to the original cause. Old-generation results retain
  /// their task outcome but do not invoke [isFailure] or update the breaker.
  /// Cancelling a probe releases its slot without reporting success.
  Task<T, E> protect<T, E>(
    Task<T, E> task, {
    required E Function() onOpen,
    bool Function(E error)? isFailure,
  }) => Task.uninterruptibleMask<T, E>((restore) {
    final admission = _admit();
    if (admission == null) return Task.fail(onOpen());
    // The local scope owns cancellation. A direct signal subscription would
    // cancel the child even while the caller's enclosing mask remains active.
    return accessTaskEnvironment(
      (scope, _) => forkTaskIn(restore(task), scope, inheritHost: true),
    ).withError<E>().flatMap(
      (fiber) => restore(fiber.join()).onExit(
        (_) => fiber.awaitExit().flatMap((exit) {
          final failure = _record(admission, exit, isFailure);
          return failure == null ? Task.succeed(null) : Task.failCause(failure);
        }),
      ),
    );
  }).scoped();
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
