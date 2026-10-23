import 'dart:collection';

import 'cause.dart';
import 'exit.dart';
import 'option.dart';
import 'task.dart';

/// A scoped broadcast buffer that sends each value to every subscription.
///
/// Use a pub-sub when readers must each receive the publications, rather than
/// compete for them. Subscriptions receive values in order, and a slow reader
/// holds up publishers when the shared buffer is full. With no readers, only
/// configured replay history is retained.
///
/// ```dart
/// import 'package:any/any.dart';
///
/// Future<void> main() async {
///   final runtime = Runtime();
///   try {
///     final work = PubSub.bounded<int>(2, replay: 1)
///         .withError<PubSubClosed>()
///         .flatMap((messages) {
///           return messages
///               .publish(7)
///               .flatMap((_) => messages.subscribe())
///               .flatMap((subscription) => subscription.take())
///               .map((value) => value.getOrElse(() => 0));
///         });
///     print(await runtime.runOrThrow(work)); // 7, from replay history.
///   } finally {
///     await runtime.close();
///   }
/// }
/// ```
///
/// [end] lets subscriptions drain; [close] discards buffered values and fails
/// unfinished operations. The creating scope closes the pub-sub automatically.
/// A publication is accepted for all active subscriptions together.
final class PubSub<T> {
  PubSub._(this._capacity, this._replay);

  final int _capacity;
  final int _replay;
  final _values = <int, T>{};
  final _subscriptions = <PubSubSubscription<T>>{};
  final _publishes = Queue<_PublishRequest<T>>();
  var _first = 0;
  var _next = 0;
  var _ended = false;
  var _closed = false;
  var _draining = false;

  /// Creates a pub-sub with room for [capacity] buffered values.
  ///
  /// New subscribers receive up to [replay] recent publications before live values.
  /// History shares the buffer with unread values; only history already consumed
  /// by every subscriber can be replaced to make space.
  ///
  /// [capacity] must be positive and [replay] must be between zero and [capacity],
  /// or execution fails with a [RangeError] defect. Each execution creates a
  /// pub-sub owned by its current scope; it cannot outlive that scope.
  static Task<PubSub<T>, Never> bounded<T>(int capacity, {int replay = 0}) =>
      Task.acquireRelease(
        Task.sync(() {
          if (capacity <= 0) {
            throw RangeError.range(capacity, 1, null, 'capacity');
          }
          RangeError.checkValueInInterval(replay, 0, capacity, 'replay');
          return PubSub<T>._(capacity, replay);
        }),
        release: (pubSub, _) => pubSub.close(),
      );

  /// Publishes [value] to all active subscriptions.
  ///
  /// Waits for space in arrival order when a reader's unread buffer is full. With
  /// no readers and no replay, succeeds without retaining the value. After [end]
  /// or [close], fails with [PubSubClosed].
  ///
  /// Cancellation removes a waiting publication. An accepted publication cannot be
  /// retracted, even if cancellation prevents the caller from observing success.
  Task<void, PubSubClosed> publish(T value) => Task.fromCallback((complete) {
    final request = _PublishRequest(value, complete);
    _publishes.addLast(request);
    _drain();
    return () {
      if (request.finished) return;
      request.finished = true;
      _publishes.remove(request);
      _drain();
    };
  });

  /// Creates an independently positioned, scoped subscription.
  ///
  /// The replay snapshot and registration are atomic, so no publication is lost
  /// between history and live values. The executing scope closes the subscription
  /// automatically.
  ///
  /// After [end], a new subscription can still read replay history before receiving
  /// [OptionNone]. After [close], fails with [PubSubClosed].
  Task<PubSubSubscription<T>, PubSubClosed> subscribe() => Task.acquireRelease(
    Task.defer(() {
      if (_closed) return Task.fail(const PubSubClosed());
      final start = _next < _replay ? 0 : _next - _replay;
      final subscription = PubSubSubscription<T>._(this, start);
      _subscriptions.add(subscription);
      return Task.succeed(subscription);
    }),
    release: (subscription, _) => subscription.close(),
  );

  /// Stops new publications while letting subscriptions drain accepted values.
  ///
  /// Pending publications fail with [PubSubClosed]. Each subscription receives
  /// [OptionNone] once its unread values drain. Repeated calls have no effect.
  Task<void, Never> end() => Task.sync(() {
    _ended = true;
    _drain();
  });

  /// Discards the buffer and closes all subscriptions.
  ///
  /// Unfinished and subsequent operations fail with [PubSubClosed]. Repeated calls
  /// have no effect. The owning scope also calls this automatically.
  Task<void, Never> close() => Task.sync(() {
    if (_closed) return;
    _closed = true;
    _ended = true;
    _values.clear();
    _first = _next;
    for (final subscription in _subscriptions.toList()) {
      subscription._close();
    }
    _drain();
  });

  void _trim() {
    var retainFrom = _next < _replay ? 0 : _next - _replay;
    for (final subscription in _subscriptions) {
      if (subscription._cursor < retainFrom) {
        retainFrom = subscription._cursor;
      }
    }
    while (_first < retainFrom) {
      _values.remove(_first++);
    }
  }

  bool _makeSpace() {
    if (_values.length < _capacity) return true;
    for (final subscription in _subscriptions) {
      if (subscription._cursor == _first) return false;
    }
    // Only consumed replay history can occupy a full buffer at this point.
    _values.remove(_first++);
    return true;
  }

  void _drain() {
    if (_draining) return;
    _draining = true;
    try {
      while (true) {
        var progressed = false;
        _trim();
        while (_publishes.isNotEmpty) {
          if (_ended) {
            final request = _publishes.removeFirst()..finished = true;
            request.complete(const Exit.failure(Cause.failure(PubSubClosed())));
          } else {
            if (!_makeSpace()) break;
            final request = _publishes.removeFirst()..finished = true;
            _values[_next++] = request.value;
            _trim();
            request.complete(const Exit.success(null));
          }
          progressed = true;
        }
        for (final subscription in _subscriptions.toList()) {
          if (subscription._wake()) progressed = true;
        }
        if (!progressed) break;
      }
    } finally {
      _draining = false;
    }
  }
}

/// An independent reader of a [PubSub]'s publications.
///
/// Obtain a subscription with [PubSub.subscribe]. Each subscription receives its
/// own copy of the sequence; concurrent [take] calls on the same subscription
/// instead divide that sequence in arrival order.
///
/// Closing this subscription, or its creating scope, releases unread buffer space
/// without closing the pub-sub or affecting other subscriptions.
final class PubSubSubscription<T> {
  PubSubSubscription._(this._pubSub, this._cursor);

  final PubSub<T> _pubSub;
  int _cursor;
  final _takes = Queue<_TakeRequest>();
  _TakeRequest? _reserved;
  var _closed = false;

  /// Removes this subscription's next value, waiting when necessary.
  ///
  /// After [PubSub.end], returns [OptionNone] once unread values drain. A present
  /// `null` is [OptionSome] when [T] allows it. Closing the subscription or pub-sub
  /// fails this task with [PubSubClosed].
  ///
  /// Cancelling while waiting leaves the value for the next take. Once removed,
  /// a value cannot be restored by cancellation, even if the caller's next task
  /// never runs.
  Task<Option<T>, PubSubClosed> take() =>
      Task.uninterruptibleMask<Option<T>, PubSubClosed>((restore) {
        final request = _TakeRequest();
        return Task.addFinalizer((_) => Task.sync(() => _cancelTake(request)))
            .withError<PubSubClosed>()
            .flatMap(
              (_) => restore(
                Task.fromCallback<void, PubSubClosed>((complete) {
                  if (_closed || _pubSub._closed) {
                    request.finished = true;
                    complete(const Exit.failure(Cause.failure(PubSubClosed())));
                    return () {};
                  }
                  request.complete = complete;
                  _takes.addLast(request);
                  _pubSub._drain();
                  return () => _cancelTake(request);
                }),
              ),
            )
            .flatMap((_) => _confirmTake(request));
      }).scoped();

  /// Closes this subscription and frees its unread buffer space.
  ///
  /// Unfinished and subsequent takes fail with [PubSubClosed]. Other subscriptions
  /// and the pub-sub stay open. Repeated calls have no effect.
  Task<void, Never> close() => Task.sync(_close);

  Task<Option<T>, PubSubClosed> _confirmTake(_TakeRequest request) {
    if (_closed || _pubSub._closed) return Task.fail(const PubSubClosed());
    request.finished = true;
    if (!identical(_reserved, request)) {
      return Task.succeed(const Option<Never>.none());
    }
    _reserved = null;
    final value = _pubSub._values[_cursor++] as T;
    _pubSub._drain();
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
    _pubSub._drain();
  }

  bool _wake() {
    var progressed = false;
    while (_takes.isNotEmpty && _reserved == null) {
      if (_closed || _pubSub._closed) {
        final request = _takes.removeFirst()..finished = true;
        request.complete(const Exit.failure(Cause.failure(PubSubClosed())));
      } else if (_cursor < _pubSub._next) {
        final request = _takes.removeFirst();
        _reserved = request;
        request.complete(const Exit.success(null));
      } else if (_pubSub._ended) {
        final request = _takes.removeFirst()..finished = true;
        request.complete(const Exit.success(null));
      } else {
        break;
      }
      progressed = true;
    }
    return progressed;
  }

  void _close() {
    if (_closed) return;
    _closed = true;
    _pubSub._subscriptions.remove(this);
    _reserved?.finished = true;
    _reserved = null;
    _wake();
    _pubSub._drain();
  }
}

/// An operation rejected because its pub-sub or subscription has closed.
///
/// Publication also fails with this error after normal [PubSub.end].
final class PubSubClosed implements Exception {
  /// Creates a pub-sub lifecycle error.
  const PubSubClosed();

  @override
  String toString() =>
      'PubSubClosed: The pub-sub no longer accepts this operation.';
}

final class _PublishRequest<T> {
  _PublishRequest(this.value, this.complete);

  final T value;
  final void Function(Exit<void, PubSubClosed>) complete;
  var finished = false;
}

final class _TakeRequest {
  late final void Function(Exit<void, PubSubClosed>) complete;
  var finished = false;
}
