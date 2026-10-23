import 'exit.dart';
import 'flow.dart';
import 'option.dart';
import 'semaphore.dart';
import 'task.dart';

/// Handles updates that would exceed a subscription ref's shared buffer.
enum SubscriptionOverflowStrategy {
  /// Keeps the most recent updates, skipping older unread updates as needed.
  latest,

  /// Waits for every subscription to free space before committing an update.
  backPressure,
}

/// A serialized mutable value with a scoped flow of committed changes.
///
/// Reads return the latest committed value without waiting for pending writes.
/// Every consumption of [changes] first receives its registration-time value,
/// followed by updates committed after registration. Registration and that
/// snapshot are atomic; no update can fall between them.
///
/// Writes share one FIFO semaphore. A candidate and its resource cleanup must
/// succeed before it can commit. The value and all subscription notifications
/// commit together; a backpressured candidate has changed neither. Even setting
/// the same value publishes an update. Values are retained by reference, so
/// mutations to their fields and external side effects are not rolled back.
///
/// The ref has no independent resource lifetime. Each flow consumption owns its
/// subscription and releases it on completion, failure, or cancellation. Writes
/// are not reentrant: a modification must not wait for another write to this ref.
final class SubscriptionRef<T> {
  SubscriptionRef._(
    this._value,
    this._semaphore,
    this._capacity,
    this._overflow,
  );

  T _value;
  final Semaphore _semaphore;
  final int _capacity;
  final SubscriptionOverflowStrategy _overflow;
  final _values = <int, T>{};
  final _subscriptions = <_Subscription<T>>{};
  var _first = 0;
  var _next = 0;
  _Commit<T>? _pending;

  /// Creates an independent ref containing [initial] on each execution.
  ///
  /// [capacity] bounds the shared buffer of updates, not the number of
  /// subscriptions. Each subscription additionally retains its initial snapshot
  /// until it is consumed. Invalid capacity becomes a [RangeError] defect.
  ///
  /// The default retains only the latest update for a slow subscription. With
  /// a larger capacity, [SubscriptionOverflowStrategy.latest] retains up to
  /// that many recent updates. [SubscriptionOverflowStrategy.backPressure]
  /// instead waits before committing whenever an active subscription still
  /// needs the oldest buffered update. No history is kept without subscribers.
  static Task<SubscriptionRef<T>, Never> create<T>(
    T initial, {
    int capacity = 1,
    SubscriptionOverflowStrategy overflow = SubscriptionOverflowStrategy.latest,
  }) => Task.defer(() {
    if (capacity < 1) throw RangeError.range(capacity, 1, null, 'capacity');
    return Semaphore.create(1).map(
      (semaphore) => SubscriptionRef._(initial, semaphore, capacity, overflow),
    );
  });

  /// Reads the latest committed value without waiting for a pending write.
  Task<T, Never> get() => Task.sync(() => _value);

  /// Observes the current state followed by updates, with an independent cursor.
  ///
  /// Each consumption registers lazily and always receives its initial snapshot
  /// first, even when later updates overwrite the shared buffer before the
  /// first pull. Stopping consumption unsubscribes and frees buffer occupancy
  /// for waiting writers. Other subscriptions and the ref remain usable.
  ///
  /// This flow has no natural end. Bound consumption with an operator such as
  /// `take`, or cancel its consuming task. In backpressure mode, a consumer that
  /// waits for a write while keeping the buffer full can deadlock that write.
  Flow<T, Never> get changes =>
      Flow.fromTask(
        Task.acquireRelease(
          Task.sync(() {
            final subscription = _Subscription<T>(
              this,
              _next,
              Option.some(_value),
            );
            _subscriptions.add(subscription);
            return subscription;
          }),
          release: (subscription, _) => Task.sync(subscription.close),
        ),
      ).flatMap(
        (subscription) => Flow.unfold<void, T, Never>(
          null,
          (_) => subscription.take().map(
            (item) => item.map((value) => (value, null)),
          ),
        ),
      );

  // The receiver retains its original T at runtime, rejecting incompatible
  // values before either state or subscription buffers can be changed.
  Task<void, Never> _commit(T value) => Task.fromCallback((complete) {
    final pending = _Commit(value, complete);
    _pending = pending;
    _drain();
    return () {
      if (identical(_pending, pending)) _pending = null;
    };
  });

  void _trim() {
    var retainFrom = _next;
    for (final subscription in _subscriptions) {
      if (subscription.cursor < retainFrom) retainFrom = subscription.cursor;
    }
    while (_first < retainFrom) {
      _values.remove(_first++);
    }
  }

  void _drain() {
    _trim();
    final pending = _pending;
    if (pending != null) {
      if (_values.length == _capacity) {
        if (_overflow == SubscriptionOverflowStrategy.backPressure) return;
        _values.remove(_first++);
        for (final subscription in _subscriptions) {
          if (subscription.cursor < _first) subscription.cursor = _first;
        }
      }
      _pending = null;
      _value = pending.value;
      _values[_next++] = pending.value;
      _trim();
      for (final subscription in _subscriptions) {
        subscription.wake();
      }
      pending.complete(const Exit.success(null));
    }
  }
}

/// Writes using a subscription ref's statically known value type.
///
/// A widened ref retains its original runtime value type. An incompatible
/// replacement produces a [TypeError] defect without changing the value or
/// notifying any subscription.
extension SubscriptionRefOperations<T> on SubscriptionRef<T> {
  /// Stores [value] and publishes it atomically after preceding writes.
  ///
  /// Cancellation before commitment removes this write without publishing.
  /// Cancellation after commitment cannot undo the value or its notifications.
  Task<void, Never> set(T value) =>
      _semaphore.withPermits(1, Task.defer(() => _commit(value)));

  /// Computes and publishes a replacement after preceding writes.
  ///
  /// [transform] runs once with the latest committed value. Exceptions and
  /// cancellation before commitment leave the value and notifications unchanged.
  /// In backpressure mode, the candidate can wait for space after computation.
  Task<void, Never> update(T Function(T value) transform) =>
      _semaphore.withPermits(1, Task.defer(() => _commit(transform(_value))));

  /// Computes `(result, newValue)`, commits the value, then returns the result.
  ///
  /// The same ordering, exception, and cancellation rules as [update] apply.
  Task<R, Never> modify<R>((R result, T value) Function(T value) change) =>
      _semaphore.withPermits(
        1,
        Task.defer(() {
          final (result, value) = change(_value);
          return _commit(value).map((_) => result);
        }),
      );

  /// Runs a candidate task while excluding other writes, then publishes it.
  ///
  /// [change] receives the latest committed value and returns a task producing
  /// `(result, newValue)`. Its local scope and resources close before commitment.
  /// Business failure, defects, cleanup failure, or enabled cancellation before
  /// the commit leave the stored value and all subscriptions unchanged.
  ///
  /// Cancellation waits for any in-flight non-cancellable Future and resource
  /// cleanup before releasing the write permit. Reads remain available while a
  /// candidate is running or waiting for buffer space. External side effects
  /// and mutations to retained objects are not undone.
  Task<R, E> modifyTask<R, E>(
    Task<(R result, T value), E> Function(T value) change,
  ) => _semaphore.withPermits(
    1,
    Task.defer(() => change(_value)).scoped().flatMap((replacement) {
      final (result, value) = replacement;
      return _commit(value).withError<E>().map((_) => result);
    }),
  );
}

final class _Commit<T> {
  _Commit(this.value, this.complete);

  final T value;
  final void Function(Exit<void, Never>) complete;
}

final class _Subscription<T> {
  _Subscription(this.ref, this.cursor, this.initial);

  final SubscriptionRef<T> ref;
  int cursor;
  Option<T> initial;
  void Function(Exit<void, Never>)? _waiting;
  var _closed = false;

  Task<Option<T>, Never> take() => Task.defer(() {
    if (_closed) return Task.succeed(const Option.none());
    if (initial case OptionSome<T>()) {
      final value = initial;
      initial = const Option.none();
      return Task.succeed(value);
    }
    if (cursor < ref._next) {
      final value = ref._values[cursor++] as T;
      ref._drain();
      return Task.succeed(Option.some(value));
    }
    return Task.fromCallback<void, Never>((complete) {
      _waiting = complete;
      wake();
      return () => _waiting = null;
    }).flatMap((_) => take());
  });

  void wake() {
    if (!_closed && initial is OptionNone<T> && cursor == ref._next) return;
    final waiting = _waiting;
    _waiting = null;
    waiting?.call(const Exit.success(null));
  }

  void close() {
    if (_closed) return;
    _closed = true;
    initial = const Option.none();
    ref._subscriptions.remove(this);
    wake();
    ref._drain();
  }
}
