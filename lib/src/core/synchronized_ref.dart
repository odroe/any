import 'semaphore.dart';
import 'task.dart';

/// A mutable value whose writes run one at a time within an isolate.
///
/// All writes share one FIFO semaphore. [get] reads the latest committed value
/// without waiting for an in-progress write. Values are retained by reference;
/// mutations to their fields and external side effects are not rolled back.
///
/// Writes are not reentrant. A modification that waits for another write to
/// this same ref can deadlock. Reading this ref during a modification is safe.
final class SynchronizedRef<T> {
  SynchronizedRef._(this._value, this._semaphore);

  T _value;
  final Semaphore _semaphore;

  /// Creates an independent ref containing [initial] on each execution.
  ///
  /// The initial value is retained unchanged. Refs created from the same
  /// mutable object share that object until a different value is stored.
  static Task<SynchronizedRef<T>, Never> create<T>(T initial) =>
      Semaphore.create(1)
          .map((semaphore) => SynchronizedRef._(initial, semaphore));

  /// Reads the latest committed value when the returned task executes.
  ///
  /// This does not wait for a pending modification or its cleanup. Until that
  /// modification commits, reads continue to return the previous value.
  Task<T, Never> get() => Task.sync(() => _value);
}

/// Writes using a synchronized ref's statically known value type.
///
/// A ref retains its original runtime type when viewed through a wider type.
/// Storing an incompatible value produces a [TypeError] defect when executed,
/// leaves the previous value in place, and releases the write permit.
extension SynchronizedRefOperations<T> on SynchronizedRef<T> {
  /// Waits for preceding writes, then stores [value].
  ///
  /// Cancellation while waiting removes only this write from the queue.
  Task<void, Never> set(T value) => _semaphore.withPermits(
    1,
    Task.sync(() {
      _value = value;
    }),
  );

  /// Waits for preceding writes, then synchronously replaces the current value.
  ///
  /// [transform] runs once after obtaining the write permit. A thrown exception
  /// becomes a defect without replacing the value. The synchronous callback and
  /// commit cannot be interrupted; cancellation cannot undo a completed commit.
  Task<void, Never> update(T Function(T value) transform) =>
      _semaphore.withPermits(
        1,
        Task.sync(() {
          _value = transform(_value);
        }),
      );

  /// Synchronously changes the value and returns a separate result.
  ///
  /// [change] runs once after preceding writes and returns `(result, newValue)`.
  /// The new value is committed before this task succeeds with `result`.
  /// Exceptions leave the value unchanged. Like [update], the callback and
  /// commit form one synchronous operation that cancellation cannot undo.
  Task<R, Never> modify<R>((R result, T value) Function(T value) change) =>
      _semaphore.withPermits(
        1,
        Task.sync(() {
          final (result, value) = change(_value);
          _value = value;
          return result;
        }),
      );

  /// Computes a replacement asynchronously while excluding other writes.
  ///
  /// [change] receives the latest value after obtaining the write permit. Its
  /// task returns `(result, newValue)` and runs in a local scope. Only success
  /// after that scope's cleanup commits the new value and returns `result`.
  /// Business failure, defects, cleanup failure, or enabled cancellation before
  /// the commit leave the stored value unchanged. Thrown callback exceptions
  /// become defects. External side effects and mutations to the old value's
  /// fields are not undone.
  ///
  /// Cancellation respects the enclosing task's cancellation state. An
  /// in-flight non-cancellable Future and all cleanup must finish before the
  /// write permit is released. Cancellation received after a commit cannot undo
  /// it. Waiting for another write to this ref from [change] is not reentrant.
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
