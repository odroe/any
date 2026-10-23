import 'semaphore.dart';
import 'task.dart';

/// A mutable value whose updates run one at a time within an isolate.
///
/// Use [SynchronizedRefOperations.modifyTask] when computing the next value
/// requires asynchronous work. Writes wait in arrival order, while [get] always
/// reads the latest committed value without waiting for a write. Prefer `Ref`
/// when every update is synchronous.
///
/// A modification must not wait for another write to this ref: that write
/// cannot start until the modification finishes. Reading the ref is safe.
/// Values are stored by reference; mutations to their fields and other side
/// effects are not rolled back if an update fails.
final class SynchronizedRef<T> {
  SynchronizedRef._(this._value, this._semaphore);

  T _value;
  final Semaphore _semaphore;

  /// Creates a ref containing [initial].
  ///
  /// Each execution creates a separate ref. The initial value is not copied, so
  /// refs created from the same mutable object initially share that object.
  static Task<SynchronizedRef<T>, Never> create<T>(T initial) =>
      Semaphore.create(1)
          .map((semaphore) => SynchronizedRef._(initial, semaphore));

  /// Reads the latest committed value without waiting for pending writes.
  ///
  /// Until an update and its cleanup succeed, reads return the previous value.
  Task<T, Never> get() => Task.sync(() => _value);
}

/// Serializes writes and commits successful replacements.
///
/// A wider static type does not change the ref's runtime value type. Storing an
/// incompatible value fails with a [TypeError] defect and keeps the old value.
extension SynchronizedRefOperations<T> on SynchronizedRef<T> {
  /// Waits for preceding writes, then stores [value].
  ///
  /// Cancellation while waiting removes only this write.
  Task<void, Never> set(T value) => _semaphore.withPermits(
    1,
    Task.sync(() {
      _value = value;
    }),
  );

  /// Waits for preceding writes, then replaces the value using [transform].
  ///
  /// The callback runs once, synchronously. A thrown exception becomes a defect
  /// without replacing the value. Cancellation cannot interrupt the callback or
  /// undo a completed commit.
  Task<void, Never> update(T Function(T value) transform) =>
      _semaphore.withPermits(
        1,
        Task.sync(() {
          _value = transform(_value);
        }),
      );

  /// Changes the value and returns a result after preceding writes finish.
  ///
  /// [change] runs synchronously and returns `(result, newValue)`. The value is
  /// stored before this task succeeds with `result`. Exceptions become defects
  /// without replacing the value; cancellation cannot undo a completed commit.
  Task<R, Never> modify<R>((R result, T value) Function(T value) change) =>
      _semaphore.withPermits(
        1,
        Task.sync(() {
          final (result, value) = change(_value);
          _value = value;
          return result;
        }),
      );

  /// Computes and commits a replacement while excluding other writes.
  ///
  /// [change] receives the latest value and returns a task producing
  /// `(result, newValue)`. The task runs in a local scope. Only success after its
  /// cleanup stores the new value and returns `result`.
  ///
  /// Failure or cancellation before the commit keeps the previous value. Thrown
  /// callback exceptions become defects. Mutations to the old value's fields and
  /// other side effects are not undone.
  ///
  /// Cancellation follows the caller's cancellation state. In-flight
  /// non-cancellable futures and cleanup must finish before another write can
  /// start. Cancellation received after a commit cannot undo it.
  Task<R, E> modifyTask<R, E>(
    Task<(R result, T value), E> Function(T value) change,
  ) => _semaphore.withPermits(
    1,
    Task.defer(() => change(_value)).scoped().map((replacement) {
      final (result, value) = replacement;
      _value = value;
      return result;
    }),
  );
}
