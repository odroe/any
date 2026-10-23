import 'task.dart';

/// An identity key for a value local to a task execution.
///
/// Use [locally] to override a value for one computation and [get] to read it.
/// Forked tasks inherit the bindings present when they start. Their overrides
/// do not change the parent's bindings or merge back when the child finishes.
/// Independently started runtime executions begin with the defaults.
///
/// Bindings are isolated, but their values are retained by reference. Mutable
/// objects are not copied. Two keys with the same type, default, and [name] still
/// identify distinct values.
final class TaskLocal<T> {
  /// Creates a distinct key with [defaultValue] and an optional diagnostic name.
  ///
  /// [defaultValue] is used whenever the running task has no override for this
  /// key. It is retained unchanged and may be `null` when [T] permits it.
  TaskLocal(T defaultValue, {this.name}) : _defaultValue = defaultValue;

  final T _defaultValue;

  /// An optional label for diagnostics; it does not determine key identity.
  final String? name;

  /// Reads this execution's current value when the returned task runs.
  ///
  /// Without a local override, returns the default supplied to the constructor.
  /// An explicit `null` override remains distinct from the absence of a binding.
  Task<T, Never> get() => readTaskLocal<T>(this, _defaultValue);

  /// Runs [task] with this key bound to [value], then restores the outer binding.
  ///
  /// The override is lazy and lasts across asynchronous boundaries. It is
  /// restored on success or failure; cancellation does not leak the override
  /// into another execution. Finalizers registered inside the override retain
  /// that registration-time value, even when they run after it has ended.
  ///
  /// [value] must match this key's original type, including when the key is
  /// accessed through a wider type or `dynamic`. A mismatch throws [TypeError]
  /// when this method is called. The task's value and error types are unchanged.
  Task<R, E> locally<R, E>(T value, Task<R, E> task) =>
      provideTaskLocal<T, R, E>(this, value, task);
}
