import 'task.dart';

/// A mutable value read and updated through tasks within one isolate.
///
/// Use a ref for shared counters or application state. [RefOperations.update]
/// and [RefOperations.modify] read and write in one synchronous operation;
/// separate [get] and [RefOperations.set] calls are not atomic together. For
/// example, `counter.modify((value) => (value, value + 1))` allocates an ID and
/// advances the counter.
///
/// Use `SynchronizedRef` when computing a replacement requires a task. Values
/// are stored by reference: changes to their fields are not tracked or rolled
/// back. Cancellation cannot interrupt a synchronous update or undo its commit.
final class Ref<T> {
  Ref._(this._value);

  T _value;

  /// Creates a ref containing [initial].
  ///
  /// Each execution creates a separate ref. The initial value is not copied, so
  /// refs created from the same mutable object initially share that object.
  static Task<Ref<T>, Never> create<T>(T initial) =>
      Task.sync(() => Ref._(initial));

  /// Reads the current value each time the task executes.
  Task<T, Never> get() => Task.sync(() => _value);
}

/// Updates a ref without suspending between reading and writing.
///
/// A wider static type does not change the ref's runtime value type. Storing an
/// incompatible value fails with a [TypeError] defect and keeps the old value.
extension RefOperations<T> on Ref<T> {
  /// Replaces the stored value with [value].
  Task<void, Never> set(T value) => Task.sync(() {
    _value = value;
  });

  /// Replaces the value with the result of [transform].
  ///
  /// Calls [transform] once per execution. A thrown exception becomes a defect
  /// without replacing the stored value; other side effects are not undone.
  Task<void, Never> update(T Function(T value) transform) => Task.sync(() {
    _value = transform(_value);
  });

  /// Changes the value and returns a result in one synchronous operation.
  ///
  /// [change] returns `(result, newValue)`. The value is stored before this task
  /// succeeds with `result`. A thrown exception becomes a defect without replacing
  /// the stored value; other side effects are not undone.
  Task<R, Never> modify<R>((R result, T value) Function(T value) change) =>
      Task.sync(() {
        final (result, value) = change(_value);
        _value = value;
        return result;
      });
}
