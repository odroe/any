import 'dart:collection';

import 'cause.dart';
import 'exit.dart';
import 'option.dart';
import 'task.dart';

/// The policy used when a bounded [AsyncQueue] has no free space.
enum QueueOverflowStrategy {
  /// Waits for space, serving pending offers in arrival order.
  backPressure,

  /// Rejects the new value, making [AsyncQueue.offer] return `false`.
  dropping,

  /// Accepts the new value and discards the oldest buffered value.
  sliding,
}

/// A scoped queue that delivers each value to one waiting consumer.
///
/// Use a queue to distribute work among competing consumers. Values and waiting
/// operations are served in arrival order. For nullable [T], [OptionSome] can
/// contain `null`; [OptionNone] means normal completion.
///
/// ```dart
/// import 'package:any/any.dart';
///
/// Future<void> main() async {
///   final runtime = Runtime();
///   try {
///     final work = AsyncQueue.bounded<int>(2)
///         .withError<QueueClosed>()
///         .flatMap((queue) {
///           return queue
///               .offer(42)
///               .flatMap((_) => queue.end())
///               .flatMap((_) => queue.take())
///               .map((value) => value.getOrElse(() => 0));
///         });
///     print(await runtime.runOrThrow(work)); // 42
///   } finally {
///     await runtime.close();
///   }
/// }
/// ```
///
/// [end] lets consumers drain accepted values; [close] discards them and fails
/// unfinished operations. The creating scope closes the queue automatically,
/// so use it within that scope rather than returning it from a completed
/// runtime execution.
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

  /// Creates a queue that buffers at most [capacity] values.
  ///
  /// The default [overflow] waits for space; other policies drop the new or oldest
  /// value. [capacity] must be positive or execution fails with a [RangeError]
  /// defect. Each execution creates a queue owned by its current scope.
  static Task<AsyncQueue<T>, Never> bounded<T>(
    int capacity, {
    QueueOverflowStrategy overflow = QueueOverflowStrategy.backPressure,
  }) => _create(capacity, overflow);

  /// Creates a scoped queue whose offers never wait for buffer space.
  ///
  /// Buffered values can grow without limit when consumers fall behind. Prefer
  /// [bounded] when production must slow to match consumption.
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

  /// Adds [value], returning whether it was accepted.
  ///
  /// [QueueOverflowStrategy.dropping] returns `false` when full. Backpressure waits
  /// for space in arrival order; sliding accepts the value and removes the oldest.
  /// An ended or closed queue fails with [QueueClosed].
  ///
  /// Cancellation removes a waiting offer. Once accepted, a value is not retracted
  /// even if cancellation prevents the caller from observing success.
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

  /// Removes the oldest value, waiting if the queue is open and empty.
  ///
  /// Waiting consumers are served in arrival order. After [end], returns
  /// [OptionNone] once accepted values drain; after [close], fails with
  /// [QueueClosed].
  ///
  /// Cancelling a waiting take leaves the value available. Once a value has been
  /// removed, cancellation cannot put it back, even if the caller's next task never
  /// runs. With [QueueOverflowStrategy.sliding], a buffered value can be replaced
  /// until it is removed.
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

  /// Stops new offers while allowing accepted values to drain.
  ///
  /// Pending offers fail with [QueueClosed]. Takes return [OptionNone] after the
  /// buffer empties. Repeated calls have no effect; use [close] to discard values.
  Task<void, Never> end() => Task.sync(() {
    _ended = true;
    _drain();
  });

  /// Discards buffered values and stops all queue operations.
  ///
  /// Unfinished and subsequent offers and takes fail with [QueueClosed]. Repeated
  /// calls have no effect. The owning scope also calls this automatically.
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

/// A business failure for an operation on an ended or closed queue.
///
/// [AsyncQueue.end] rejects offers but lets takes drain normally.
/// [AsyncQueue.close] rejects both.
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
