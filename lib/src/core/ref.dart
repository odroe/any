import 'task.dart';

/// A mutable value accessed through tasks within one isolate.
///
/// [RefOperations.update] and [RefOperations.modify] perform a synchronous
/// read and write without suspending between them. A separate [get] followed
/// by [RefOperations.set] does not form one atomic operation.
///
/// Values are retained by reference. Mutating a stored object's fields is not
/// tracked, copied, or rolled back by this ref.
final class Ref<T> {
  Ref._(this._value);

  T _value;

  /// Creates a new ref containing [initial] whenever the returned task runs.
  ///
  /// Repeated executions create independent refs. The initial value itself is
  /// retained unchanged, so refs created from one mutable object share that
  /// object until one of them stores a different value.
  static Task<Ref<T>, Never> create<T>(T initial) =>
      Task.sync(() => Ref._(initial));

  /// Reads the value when the returned task runs.
  ///
  /// Reusing the task reads the latest value on each execution.
  Task<T, Never> get() => Task.sync(() => _value);
}

/// Writes and transformations using a ref's statically known value type.
///
/// A ref keeps its original runtime type when viewed through a wider type.
/// Attempting to store an incompatible value produces a [TypeError] defect
/// when the task runs, and leaves the previous stored value in place.
extension RefOperations<T> on Ref<T> {
  /// Stores [value] when the returned task runs.
  Task<void, Never> set(T value) => Task.sync(() {
    _value = value;
  });

  /// Replaces the current value with the result of [transform].
  ///
  /// [transform] runs once per execution, synchronously with no suspension
  /// between reading and committing. If it throws, its exception becomes a
  /// defect and the stored value is not replaced. Mutations to the old value's
  /// own fields or other side effects are not undone.
  ///
  /// Cancellation is checked around this operation; it cannot interrupt the
  /// callback or undo a value committed by it.
  Task<void, Never> update(T Function(T value) transform) => Task.sync(() {
    _value = transform(_value);
  });

  /// Changes the value and returns a separate result in one synchronous step.
  ///
  /// [change] returns `(result, newValue)`. The new value is committed before
  /// the task succeeds with `result`. Exceptions become defects without
  /// replacing the stored value; other side effects are not rolled back.
  /// Cancellation cannot interrupt the callback or undo the commit.
  ///
  /// ```dart
  /// final nextId = counter.modify((value) => (value, value + 1));
  /// ```
  Task<R, Never> modify<R>((R result, T value) Function(T value) change) =>
      Task.sync(() {
        final (result, value) = change(_value);
        _value = value;
        return result;
      });
}
