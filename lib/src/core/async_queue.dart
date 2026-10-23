import 'dart:collection';

import 'cause.dart';
import 'exit.dart';
import 'option.dart';
import 'task.dart';

/// Determines how a bounded [AsyncQueue] handles a full buffer.
enum QueueOverflowStrategy {
  /// Waits for space, serving pending offers in arrival order.
  backPressure,

  /// Rejects the new value, making [AsyncQueue.offer] return `false`.
  dropping,

  /// Accepts the new value and discards the oldest buffered value.
  sliding,
}

/// A FIFO queue owned by the scope in which it is created.
///
/// [end] rejects new offers and lets consumers drain accepted values. [close]
/// discards the buffer and fails pending operations. The creating scope closes
/// the queue automatically; returning it from a completed runtime execution
/// does not extend its lifetime.
///
/// Values may be nullable. [take] distinguishes a present `null` from normal
/// completion through [Option].
final class AsyncQueue<T> {
  AsyncQueue._(this._capacity, this._overflow);

  final int? _capacity;
  final QueueOverflowStrategy _overflow;
  final _values = Queue<T>();
  final _offers = Queue<_OfferRequest<T>>();
  final _takes = Queue<_TakeRequest>();
  _TakeRequest? _reserved;
  var _ended = false;
  var _closed = false;
  var _draining = false;

  /// Creates an independently scoped queue with at most [capacity] values.
  ///
  /// [capacity] must be positive; an invalid value becomes a [RangeError] defect
  /// when the task executes. The default [overflow] waits for available space.
  static Task<AsyncQueue<T>, Never> bounded<T>(
    int capacity, {
    QueueOverflowStrategy overflow = QueueOverflowStrategy.backPressure,
  }) => _create(capacity, overflow);

  /// Creates an independently scoped queue without a buffer limit.
  ///
  /// Offers never wait for space. The caller must bound production when
  /// consumers may be slower, or use [bounded] for backpressure.
  static Task<AsyncQueue<T>, Never> unbounded<T>() =>
      _create(null, QueueOverflowStrategy.backPressure);

  static Task<AsyncQueue<T>, Never> _create<T>(
    int? capacity,
    QueueOverflowStrategy overflow,
  ) => Task.acquireRelease(
    Task.sync(() {
      if (capacity != null && capacity <= 0) {
        throw RangeError.range(capacity, 1, null, 'capacity');
      }
      return AsyncQueue<T>._(capacity, overflow);
    }),
    release: (queue, _) => queue.close(),
  );

  /// Offers [value] when executed, returning whether the queue accepted it.
  ///
  /// Only the dropping strategy returns `false`. Backpressure waits in FIFO
  /// order; cancelling a pending offer removes only that offer. Once accepted,
  /// the value remains queued even if the producer is cancelled before seeing
  /// the result. Sliding may subsequently discard it as the oldest value.
  ///
  /// An ended or closed queue fails with [QueueClosed].
  Task<bool, QueueClosed> offer(T value) => Task.fromCallback((complete) {
    final request = _OfferRequest(value, complete);
    _offers.addLast(request);
    _drain();
    return () {
      if (request.finished) return;
      request.finished = true;
      _offers.remove(request);
      _drain();
    };
  });

  /// Takes the oldest value, waiting when an open queue is empty.
  ///
  /// Returns [OptionNone] after [end] and all accepted values have been taken.
  /// [close] instead fails pending and subsequent takes with [QueueClosed].
  /// Waiting consumers are served in FIFO order. A wake-up reserves a turn,
  /// and the value stays buffered until this task confirms the take. Cancelling
  /// before confirmation leaves the value available to another consumer.
  /// After confirmation, consumption is committed even if cancellation prevents
  /// the caller's next task from running. Sliding may discard buffered values
  /// before confirmation, following its usual overflow policy.
  Task<Option<T>, QueueClosed> take() =>
      Task.uninterruptibleMask<Option<T>, QueueClosed>((restore) {
        final request = _TakeRequest();
        return Task.addFinalizer((_) => Task.sync(() => _cancelTake(request)))
            .withError<QueueClosed>()
            .flatMap(
              (_) => restore(
                Task.fromCallback<void, QueueClosed>((complete) {
                  request.complete = complete;
                  _takes.addLast(request);
                  _drain();
                  return () => _cancelTake(request);
                }),
              ),
            )
            .flatMap((_) => _confirmTake(request));
      }).scoped();

  /// Stops accepting offers while preserving buffered values for consumers.
  ///
  /// Pending offers that have not been accepted fail with [QueueClosed]. Takes
  /// return [OptionNone] once the buffer drains. Repeated calls have no effect.
  Task<void, Never> end() => Task.sync(() {
    _ended = true;
    _drain();
  });

  /// Discards buffered values and fails all unfinished queue operations.
  ///
  /// Takes that have not confirmed consumption fail with [QueueClosed], even
  /// if they were already woken. Repeated calls have no effect. This is also
  /// performed automatically when the queue's owning scope closes.
  Task<void, Never> close() => Task.sync(() {
    if (_closed) return;
    _closed = true;
    _ended = true;
    _values.clear();
    _reserved?.finished = true;
    _reserved = null;
    _drain();
  });

  Task<Option<T>, QueueClosed> _confirmTake(_TakeRequest request) {
    if (_closed) return Task.fail(const QueueClosed());
    request.finished = true;
    if (!identical(_reserved, request)) {
      return Task.succeed(const Option<Never>.none());
    }
    _reserved = null;
    final value = _values.removeFirst();
    _drain();
    return Task.succeed(Option.some(value));
  }

  void _cancelTake(_TakeRequest request) {
    if (request.finished) return;
    request.finished = true;
    if (identical(_reserved, request)) {
      _reserved = null;
    } else {
      _takes.remove(request);
    }
    _drain();
  }

  void _drain() {
    if (_draining) return;
    _draining = true;
    try {
      while (true) {
        if (_offers.isNotEmpty) {
          if (_ended) {
            final request = _offers.removeFirst()..finished = true;
            request.complete(const Exit.failure(Cause.failure(QueueClosed())));
            continue;
          }
          if (_capacity == null || _values.length < _capacity) {
            final request = _offers.removeFirst()..finished = true;
            _values.addLast(request.value);
            request.complete(const Exit.success(true));
            continue;
          }
          if (_overflow != QueueOverflowStrategy.backPressure) {
            final request = _offers.removeFirst()..finished = true;
            final accepted = _overflow == QueueOverflowStrategy.sliding;
            if (accepted) {
              _values.removeFirst();
              _values.addLast(request.value);
            }
            request.complete(Exit.success(accepted));
            continue;
          }
        }
        if (_takes.isNotEmpty && _reserved == null) {
          if (_closed) {
            final request = _takes.removeFirst()..finished = true;
            request.complete(const Exit.failure(Cause.failure(QueueClosed())));
            continue;
          }
          if (_values.isNotEmpty) {
            final request = _takes.removeFirst();
            // Keep the value and its capacity until the consumer confirms.
            _reserved = request;
            request.complete(const Exit.success(null));
            continue;
          }
          if (_ended) {
            final request = _takes.removeFirst()..finished = true;
            request.complete(const Exit.success(null));
            continue;
          }
        }
        break;
      }
    } finally {
      _draining = false;
    }
  }
}

/// An operation rejected because its queue has ended or closed.
final class QueueClosed implements Exception {
  /// Creates a queue lifecycle error.
  const QueueClosed();

  @override
  String toString() =>
      'QueueClosed: The queue no longer accepts this operation.';
}

final class _OfferRequest<T> {
  _OfferRequest(this.value, this.complete);

  final T value;
  final void Function(Exit<bool, QueueClosed>) complete;
  var finished = false;
}

final class _TakeRequest {
  late final void Function(Exit<void, QueueClosed>) complete;
  var finished = false;
}
