import 'task.dart';

/// A value binding that follows one task execution and its child fibers.
///
/// Use [get] to read the current value and [locally] to override it for a task.
/// Children inherit the bindings present when they start; changes in a child do
/// not change the parent. Independent runtime executions use the default value.
///
/// Each [TaskLocal] has its own identity, regardless of its type, default value,
/// or [name]. Bindings are isolated, but mutable values are shared by reference.
///
/// ```dart
/// final requestId = TaskLocal('unknown', name: 'requestId');
/// final program = requestId.locally('request-42', requestId.get());
/// final runtime = Runtime();
/// try {
///   final local = await runtime.runOrThrow(program);
///   final outside = await runtime.runOrThrow(requestId.get());
///   assert(local == 'request-42');
///   assert(outside == 'unknown');
/// } finally {
///   await runtime.close();
/// }
/// ```
final class TaskLocal<T> {
  /// Creates a local binding with [defaultValue] as its fallback.
  ///
  /// The default is retained by reference and is returned whenever no override is
  /// active. [name] is an optional diagnostic label, not the binding's identity.
  TaskLocal(T defaultValue, {this.name}) : _defaultValue = defaultValue;

  final T _defaultValue;

  /// An optional label for diagnostics; it does not determine key identity.
  final String? name;

  /// Returns a task that reads the current value of this binding.
  ///
  /// Uses the constructor's default when no [locally] override is active. An
  /// explicit `null` override is a value, not an absent binding.
  Task<T, Never> get() => readTaskLocal<T>(this, _defaultValue);

  /// Runs [task] with a local [value] for this binding.
  ///
  /// The override lasts across asynchronous work and ends on success, failure, or
  /// cancellation. Finalizers registered within it retain that value even when
  /// cleanup happens after the override ends.
  ///
  /// [value] must match this key's original type, even through a wider type or
  /// `dynamic`. A mismatch throws [TypeError] when [locally] is called.
  Task<R, E> locally<R, E>(T value, Task<R, E> task) =>
      provideTaskLocal<T, R, E>(this, value, task);
}
