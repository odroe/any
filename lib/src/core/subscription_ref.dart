import 'exit.dart';
import 'flow.dart';
import 'option.dart';
import 'semaphore.dart';
import 'task.dart';

/// The policy for updates that fill a [SubscriptionRef]'s shared buffer.
enum SubscriptionOverflowStrategy {
  /// Keeps the most recent updates, skipping older unread updates as needed.
  latest,

  /// Waits for every subscription to free space before committing an update.
  backPressure,
}

/// A mutable value with serialized writes and a flow of committed updates.
///
/// Use a subscription ref when consumers need both the current state and future
/// changes. [get] reads the latest committed value without waiting for a writer.
/// Each consumption of [changes] starts with a current snapshot, then receives
/// updates without a gap between the snapshot and registration.
///
/// ```dart
/// import 'package:any/any.dart';
///
/// Future<void> main() async {
///   final runtime = Runtime();
///   try {
///     final work = SubscriptionRef.create(0).flatMap((count) {
///       return count
///           .update((value) => value + 1)
///           .flatMap((_) => count.changes.take(1).toList());
///     });
///     print(await runtime.runOrThrow(work)); // [1]
///   } finally {
///     await runtime.close();
///   }
/// }
/// ```
///
/// Writes commit the value and notifications together, in arrival order. Setting
/// an equal value still publishes an update. Values are held by reference;
/// mutating their fields or performing external effects cannot be rolled back.
/// A write must not wait for another write to the same ref.
///
/// The ref has no independent resource lifetime. Each consumption owns and closes
/// its subscription when it ends or is cancelled.
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

  /// Creates a new ref holding [initial].
  ///
  /// [capacity] bounds shared updates, not subscriptions or their initial
  /// snapshots. It must be positive or execution fails with a [RangeError] defect.
  /// Each execution creates an independent ref without copying [initial].
  ///
  /// By default, slow readers skip older updates and receive the latest.
  /// Increasing [capacity] retains more recent updates. Choose
  /// [SubscriptionOverflowStrategy.backPressure] to wait for slow readers before
  /// committing writes instead. No update history is kept without subscribers.
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

  /// The current state followed by committed updates.
  ///
  /// Each consumption registers independently and receives its initial snapshot
  /// first, even if later updates replace buffered history before it is read.
  /// Stopping consumption unsubscribes and frees space for waiting writers.
  ///
  /// The flow never ends on its own; use [FlowOperations.take] or cancel its task.
  /// With [SubscriptionOverflowStrategy.backPressure], a consumer must not wait
  /// for a write that needs it to free buffer space.
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

/// Atomic writes to a [SubscriptionRef].
///
/// A widened ref still enforces its original runtime value type. An incompatible
/// replacement fails with a [TypeError] defect without changing the value or
/// publishing an update.
extension SubscriptionRefOperations<T> on SubscriptionRef<T> {
  /// Stores [value] and publishes an update after preceding writes.
  ///
  /// Cancellation before commitment leaves the state unchanged. After commitment,
  /// cancellation cannot undo the value or notification.
  Task<void, Never> set(T value) =>
      _semaphore.withPermits(1, Task.defer(() => _commit(value)));

  /// Replaces the value with [transform]'s result and publishes an update.
  ///
  /// Runs [transform] once with the latest value after preceding writes finish.
  /// Exceptions or cancellation before commitment leave state and notifications
  /// unchanged. Backpressure can delay commitment after computation.
  Task<void, Never> update(T Function(T value) transform) =>
      _semaphore.withPermits(1, Task.defer(() => _commit(transform(_value))));

  /// Commits the new value from [change] and returns its result.
  ///
  /// [change] returns `(result, newValue)` and follows [update]'s ordering and
  /// failure rules.
  Task<R, Never> modify<R>((R result, T value) Function(T value) change) =>
      _semaphore.withPermits(
        1,
        Task.defer(() {
          final (result, value) = change(_value);
          return _commit(value).map((_) => result);
        }),
      );

  /// Runs [change], commits its new value, and returns its result.
  ///
  /// [change] receives the latest value and returns `(result, newValue)`. Other
  /// writes wait until this task finishes, while reads remain available. The
  /// candidate's resources close before commitment; failure or enabled
  /// cancellation leaves stored state and notifications unchanged.
  ///
  /// Cancellation waits for pending non-cancellable Futures and cleanup before
  /// allowing the next writer. External effects and in-place mutations to retained
  /// objects cannot be undone.
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
