import 'cause.dart';
import 'clock.dart';
import 'exit.dart';
import 'fiber.dart';
import 'flow.dart';
import 'internal/flow_cursor.dart';
import 'option.dart';
import 'scope.dart';
import 'task.dart';

/// Groups and regulates flow elements using the current [Clock].
extension FlowAggregationOperations<T, E> on Flow<T, E> {
  /// Collects elements into immutable groups bounded by [size] and [duration].
  ///
  /// The first element starts a monotonic deadline. A full group or an expired
  /// deadline emits a nonempty group; an empty source emits nothing. Normal end
  /// emits the last partial group. Failure discards a group still being built.
  ///
  /// At most [size] values and one source pull are retained, in addition to
  /// groups retained downstream. A deadline never cancels a pending source pull:
  /// its result belongs to the next group. No new pulls start while downstream
  /// consumes a group. Input resources remain available until its group is
  /// consumed, including when a pending pull advances to another producer.
  ///
  /// Consumer cancellation stops pending work and waits for cleanup. Unfinished
  /// source business failures and explicit cancellation are discarded; defects
  /// and resource cleanup failures are retained. Both [size] and [duration] must
  /// be positive; invalid values become [RangeError] defects before source work.
  Flow<List<T>, E> groupedWithin(int size, Duration duration) => flowFromCursor(
    Task.defer(() {
      if (size < 1) throw RangeError.range(size, 1, null, 'size');
      if (duration <= Duration.zero) {
        throw RangeError.value(
          duration.inMicroseconds,
          'duration',
          'Must be positive.',
        );
      }
      return Task.service(Clock.key).withError<E>().flatMap(
        (clock) => Scope.open().withError<E>().flatMap(
          (owner) => inTaskScope(openFlowCursor(this), owner).flatMap((source) {
            final grouping = _TimedGrouping(
              source,
              owner,
              clock,
              size,
              duration,
            );
            return inTaskScope(
              Task.addFinalizer(grouping.cleanup),
              owner,
            ).withError<E>().map((_) => grouping.cursor);
          }),
        ),
      );
    }),
  );
}

final class _TimedValue<T> {
  _TimedValue(this.value, this.receivedAt, this.producers);

  final T value;
  final Duration receivedAt;
  final List<FlowProducer> producers;
}

final class _TimedGrouping<T, E> {
  _TimedGrouping(this.source, this.owner, this.clock, this.size, this.duration);

  final FlowCursor<T, E> source;
  final Scope owner;
  final Clock clock;
  final int size;
  final Duration duration;
  final _stopReason = Object();
  late final _lifetime = FlowProducer(owner);
  final _values = <T>[];
  final _inputs = <FlowProducer>[];
  FlowProducer? _current;
  _TimedValue<T>? _pendingValue;
  Fiber<Option<_TimedValue<T>>, E>? _active;
  TaskEvaluation<Option<_TimedValue<T>>, E>? _result;
  TaskEvaluation<Option<_TimedValue<T>>, E>? _failure;
  List<CauseCancelled> _explicitCancellation = const [];
  Duration? _deadline;
  void Function()? _cancelTimer;
  void Function()? _wake;
  var _expired = false;
  var _ended = false;
  var _stopping = false;
  var _stoppedByUs = false;
  var _failureDelivered = false;

  late final cursor = FlowCursor<List<T>, E>(
    _pull(),
    () => _current?.scope ?? owner,
    _finish,
    () => [?_current],
  );

  Task<Option<List<T>>, E> _pull() => Task.defer(() {
    final previous = _current;
    if (previous != null) {
      _current = null;
      return previous
          .finish(const Exit.success(null))
          .withError<E>()
          .flatMap((_) => _pull());
    }
    if (_ended) {
      return _lifetime
          .finish(const Exit.success(null))
          .withError<E>()
          .map((_) => const Option.none());
    }
    final result = _result;
    if (result != null) {
      switch (result.exit) {
        case ExitFailure(:final cause):
          return taskFromEvaluation(
            TaskEvaluation<Option<List<T>>, E>(
              Exit.failure(cause),
              result.events,
            ),
          ).mapCause((cause) {
            _failureDelivered = true;
            return cause;
          });
        case ExitSuccess(value: OptionNone()):
          _result = null;
          _ended = true;
          _clearTimer();
          return _values.isEmpty ? _pull() : Task.succeed(Option.some(_emit()));
        case ExitSuccess(value: OptionSome(:final value)):
          // A completed pull keeps its receipt time even while the consumer is
          // busy. It cannot be folded into a group whose deadline it missed.
          if (_deadline case final deadline?
              when value.receivedAt >= deadline) {
            return Task.succeed(Option.some(_emit()));
          }
          _result = null;
          _pendingValue = null;
          _values.add(value.value);
          _inputs.addAll(value.producers);
          if (_values.length == 1) _deadline = value.receivedAt + duration;
          if (_values.length == size || clock.elapsed >= _deadline!) {
            return Task.succeed(Option.some(_emit()));
          }
          _armTimer();
          return _pull();
      }
    }
    if (_expired) return Task.succeed(Option.some(_emit()));
    if (_active == null) {
      return _startPull().withError<E>().flatMap((_) => _pull());
    }
    return _wait().withError<E>().flatMap((_) => _pull());
  });

  Task<void, Never> _startPull() =>
      forkTaskIn(
        Task.uninterruptibleMask<Option<_TimedValue<T>>, E>(
          (restore) => restore(source.pull()).map((item) {
            if (item is OptionNone<T>) {
              return const Option<_TimedValue<Never>>.none();
            }
            final receivedAt = clock.elapsed;
            final producers = source.producers().toSet().toList();
            for (final producer in producers) {
              producer.retain();
            }
            final entry = _TimedValue(
              (item as OptionSome<T>).value,
              receivedAt,
              producers,
            );
            // Cancellation can replace the fiber's successful exit before its
            // listener runs. The retained snapshot needs an independent owner.
            _pendingValue = entry;
            return Option.some(entry);
          }),
        ).mapCause((cause) {
          _explicitCancellation = _causes(cause)
              .whereType<CauseCancelled>()
              .toList();
          return cause;
        }),
        owner,
        inheritHost: true,
      ).map((fiber) {
        _active = fiber;
        listenFiber(fiber, (exit) {
          _active = null;
          final result = TaskEvaluation(exit, fiberFailureEvents(fiber));
          _result = result;
          if (exit case ExitFailure(:final cause)) {
            _failure = result;
            if (_stoppedByUs) {
              _explicitCancellation = _causes(cause)
                  .whereType<CauseCancelled>()
                  .where((cause) => !identical(cause.reason, _stopReason))
                  .toList();
            }
          }
          _wake?.call();
        });
      });

  void _armTimer() {
    if (_cancelTimer != null) return;
    final delay = _deadline! - clock.elapsed;
    _cancelTimer = clock.schedule(delay, () {
      _cancelTimer = null;
      _expired = true;
      _wake?.call();
    });
  }

  void _clearTimer() {
    final cancel = _cancelTimer;
    _cancelTimer = null;
    _deadline = null;
    _expired = false;
    cancel?.call();
  }

  List<T> _emit() {
    _clearTimer();
    final values = List<T>.unmodifiable(_values);
    final inputs = _inputs.toList();
    _values.clear();
    _inputs.clear();
    _lifetime.retain();
    _current = FlowProducer(
      createScope(parent: owner),
      afterClose: (_) async => mergeFlowEvaluations([
        await releaseFlowProducers(inputs),
        await _lifetime.release(),
      ]),
    );
    return values;
  }

  Task<void, Never> _wait() => Task.fromCallback((complete) {
    void notify() {
      _wake = null;
      complete(const Exit.success(null));
    }

    _wake = notify;
    if (_result != null || (!_stopping && _expired)) notify();
    return () {
      if (identical(_wake, notify)) _wake = null;
    };
  });

  void _stop() {
    if (_stopping) return;
    _stopping = true;
    // Scope marks descendants before cancelling their fibers, so its shutdown
    // owns resource cleanup; an earlier explicit stop owns the driver's exit.
    _stoppedByUs = !scopeIsClosing(owner);
    try {
      _clearTimer();
    } finally {
      _active?.requestCancel(reason: _stopReason);
    }
  }

  Task<void, Never> _finish(Exit<Object?, Object?> exit) =>
      Task.sync(_stop).ensuring(_finishStopped(exit));

  Task<void, Never> _finishStopped(Exit<Object?, Object?> exit) => Task.defer(
    () {
      if (_active != null) return _wait().flatMap((_) => _finishStopped(exit));
      final current = _current;
      _current = null;
      final inputs = _inputs.toList();
      if (_pendingValue case final value?) {
        inputs.addAll(value.producers);
      }
      _pendingValue = null;
      _inputs.clear();
      _values.clear();
      _result = null;
      _ended = true;
      final release = Task.uninterruptibleMask<void, Never>(
        (_) => Task.fromFuture(
          () => releaseFlowProducers(inputs),
          onError: Error.throwWithStackTrace,
        ).flatMap(taskFromEvaluation),
      );
      return source
          .finish(exit)
          .ensuring(current?.finish(exit) ?? Task.succeed(null))
          .ensuring(release)
          .ensuring(_lifetime.finish(exit));
    },
  );

  Task<void, Never> cleanup(
    Exit<Object?, Object?> exit,
  ) => Task.sync(_stop).ensuring(
    Task.defer(() {
      // Owner shutdown has already stopped the pull and closed input scopes.
      _values.clear();
      _inputs.clear();
      _current = null;
      _pendingValue = null;
      _result = null;
      final causes = <Cause<Never>>[];
      if (_failure?.exit case ExitFailure(:final cause)) {
        for (final reason in _causes(cause)) {
          if (reason is CauseDefect ||
              (reason is CauseCancelled &&
                  !_failureDelivered &&
                  _explicitCancellation.contains(reason))) {
            causes.add(reason as Cause<Never>);
          }
        }
      }
      return taskFromEvaluation(
        TaskEvaluation(
          causes.isEmpty
              ? const Exit.success(null)
              : Exit.failure(Cause.combine(causes)),
          _failure?.events ?? const [],
        ),
      );
    }),
  );
}

List<Cause<E>> _causes<E>(Cause<E> cause) => switch (cause) {
  CauseGroup(:final causes) => causes,
  _ => [cause],
};
