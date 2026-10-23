import 'dart:async';
import 'dart:collection';

import 'package:any/any.dart';

/// A bounded FIFO queue with cancellable producer and consumer tasks.
///
/// [put] waits when the buffer is full; [take] waits when it is empty. Waiters
/// are served in registration order. Await each put before producing more work
/// to apply backpressure: [capacity] bounds buffered values, not the values
/// retained by an arbitrary number of waiting producers.
///
/// Closing rejects pending and future puts. Buffered values remain available
/// until drained, after which takes fail with [QueueClosed]. Values and their
/// resources remain caller-owned; closing the queue does not dispose them.
///
/// A transfer takes effect before its task completes. Cancellation removes a
/// waiting operation, but does not roll back a transfer that already happened.
/// In a cancellation race, a put may have stored its value or a take may have
/// removed one even though the task ultimately reports cancellation.
///
/// ```dart
/// final queue = AsyncQueue<String>(capacity: 2);
/// await queue.put<()>('message').run(());
/// queue.close();
/// final exit = await queue.take<()>().run(()); // Success('message').
/// final end = await queue.take<()>().run(()); // Failure(QueueClosed()).
/// ```
final class AsyncQueue<T> {
  /// Creates an empty queue with the given positive [capacity].
  ///
  /// Throws a [RangeError] if [capacity] is less than one.
  AsyncQueue({required this.capacity}) {
    if (capacity < 1) {
      throw RangeError.range(capacity, 1, null, 'capacity');
    }
  }

  /// The maximum number of buffered values.
  final int capacity;
  final _values = ListQueue<T>();
  final _readers = LinkedList<_Reader<T>>();
  final _writers = LinkedList<_Writer<T>>();
  bool _closed = false;

  /// Whether [close] has been called, preventing any further puts.
  bool get isClosed => _closed;

  /// The number of buffered values, excluding waiting producers.
  int get length => _values.length;

  /// A task that stores [value], waiting for capacity if necessary.
  ///
  /// Creation is lazy. Each run performs another put using the same value.
  /// Cancellation while waiting removes the producer and its retained value.
  /// A closed queue returns an expected [QueueClosed] failure.
  Task<void, QueueClosed, R> put<R>(T value) => Task((context) {
    if (_closed) return const Exit.failure(Cause.failure(QueueClosed()));
    if (_readers.isNotEmpty) {
      final reader = _readers.first..unlink();
      reader.ready.complete(Exit.success(value));
    } else if (_values.length < capacity) {
      _values.addLast(value);
    } else {
      final writer = _Writer(value);
      _writers.add(writer);
      return _wait(writer.ready, writer.unlink, context.cancellation);
    }
    return const Exit.success(null);
  });

  /// A task that removes the oldest value, waiting for a put if necessary.
  ///
  /// Each run performs a new take. Cancellation removes a waiting consumer.
  /// Once closed and empty, returns an expected [QueueClosed] failure. Null is
  /// an ordinary value when [T] is nullable and never signals closure.
  Task<T, QueueClosed, R> take<R>() => Task((context) {
    if (_values.isNotEmpty) {
      final value = _values.removeFirst();
      if (_writers.isNotEmpty) {
        final writer = _writers.first..unlink();
        _values.addLast(writer.value);
        writer.ready.complete(const Exit.success(null));
      }
      return Exit.success(value);
    }
    if (_closed) return const Exit.failure(Cause.failure(QueueClosed()));
    final reader = _Reader<T>();
    _readers.add(reader);
    return _wait(reader.ready, reader.unlink, context.cancellation);
  });

  /// Stops accepting puts and wakes waiting operations that cannot proceed.
  ///
  /// Pending puts fail without storing their values. Existing buffered values
  /// can still be taken. This method does not wait for the buffer to be drained
  /// and is safe to repeat, including as a scope finalizer.
  void close() {
    if (_closed) return;
    _closed = true;
    while (_writers.isNotEmpty) {
      final writer = _writers.first..unlink();
      writer.ready.complete(const Exit.failure(Cause.failure(QueueClosed())));
    }
    while (_readers.isNotEmpty) {
      final reader = _readers.first..unlink();
      reader.ready.complete(const Exit.failure(Cause.failure(QueueClosed())));
    }
  }
}

/// An expected failure when an operation cannot proceed on a closed queue.
final class QueueClosed {
  /// Creates a queue-closure failure value.
  const QueueClosed();

  @override
  String toString() => 'QueueClosed';
}

final class _Reader<T> extends LinkedListEntry<_Reader<T>> {
  final ready = Completer<Exit<T, QueueClosed>>();
}

final class _Writer<T> extends LinkedListEntry<_Writer<T>> {
  _Writer(this.value);
  final T value;
  final ready = Completer<Exit<void, QueueClosed>>();
}

Future<Exit<T, QueueClosed>> _wait<T>(
  Completer<Exit<T, QueueClosed>> ready,
  void Function() remove,
  CancellationToken cancellation,
) {
  final removeListener = cancellation.onCancel((reason) {
    if (!ready.isCompleted) {
      remove();
      ready.complete(Exit.failure(Cause.cancelled(reason: reason)));
    }
  });
  return ready.future.whenComplete(removeListener);
}
