import 'dart:collection';

import 'cause.dart';
import 'exit.dart';
import 'option.dart';
import 'task.dart';

/// A bounded broadcast buffer owned by its creating scope.
///
/// Each active subscription receives every accepted publication in order. A
/// slow subscription applies backpressure to publishers; closing it releases
/// its share of the buffer. Publications are accepted atomically for all
/// subscriptions, rather than delivered to individual queues one at a time.
///
/// With no subscriptions, only the configured replay history is retained.
/// [end] allows accepted values to drain, while [close] discards them and fails
/// unfinished operations. The owning scope automatically closes this pub-sub.
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

  /// Creates a scoped pub-sub with at most [capacity] buffered values.
  ///
  /// New subscriptions first receive up to [replay] recently accepted values,
  /// followed by subsequent publications. History and unread values share the
  /// same buffer. Consumed history can be replaced by a new publication;
  /// unread values cannot.
  ///
  /// [capacity] must be positive, and [replay] must be between zero and
  /// [capacity]. Invalid arguments become [RangeError] defects when executed.
  /// Each execution creates an independent pub-sub. Returning it from a
  /// completed runtime execution does not extend its lifetime.
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

  /// Publishes [value], waiting in FIFO order if an unread buffer is full.
  ///
  /// Cancellation before acceptance removes only this publisher. After
  /// acceptance, cancellation does not retract the publication, even if it
  /// prevents the caller from receiving success. With no subscriptions and no
  /// replay, publication succeeds without retaining the value.
  ///
  /// An ended or closed pub-sub fails with [PubSubClosed].
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

  /// Creates a subscription owned by the scope executing this task.
  ///
  /// Registration and the replay snapshot form one synchronous step, so a
  /// publication cannot fall between the history and live subscription. The
  /// subscription automatically closes when its owning scope closes.
  ///
  /// After [end], new subscriptions can still consume replay history before
  /// receiving [OptionNone]. After [close], this fails with [PubSubClosed].
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

  /// Rejects new publications and lets subscriptions drain accepted values.
  ///
  /// Publications still waiting for space fail with [PubSubClosed]. Each
  /// subscription receives [OptionNone] after its own unread values drain.
  /// Repeated calls have no effect.
  Task<void, Never> end() => Task.sync(() {
    _ended = true;
    _drain();
  });

  /// Discards the buffer and closes all subscriptions and pending operations.
  ///
  /// A take that has not confirmed consumption fails with [PubSubClosed], even
  /// if already woken. Repeated calls have no effect. The owning scope also
  /// performs this operation automatically.
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

/// An independently positioned reader of a [PubSub].
///
/// A subscription belongs to the scope that called [PubSub.subscribe]. Closing
/// it releases unread values for other publishers without closing the pub-sub
/// or affecting other subscriptions. Concurrent takes share this subscription's
/// position and are served in arrival order.
final class PubSubSubscription<T> {
  PubSubSubscription._(this._pubSub, this._cursor);

  final PubSub<T> _pubSub;
  int _cursor;
  final _takes = Queue<_TakeRequest>();
  _TakeRequest? _reserved;
  var _closed = false;

  /// Takes this subscription's next value, waiting if necessary.
  ///
  /// Returns [OptionNone] after [PubSub.end] and this subscription's accepted
  /// values drain. A present `null` is returned as [OptionSome], when [T] allows
  /// it. Closing the subscription or pub-sub fails with [PubSubClosed].
  ///
  /// A wake-up reserves a turn without advancing the subscription. Cancelling
  /// before confirmation leaves that value available to the next take on this
  /// subscription. After confirmation, consumption is committed even if later
  /// cancellation prevents the caller's continuation from running.
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

  /// Closes this subscription and releases its unread buffer occupancy.
  ///
  /// Pending and subsequent takes fail with [PubSubClosed]. Other subscriptions
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
