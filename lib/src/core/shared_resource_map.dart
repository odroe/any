import 'internal/shared_resource_state.dart';
import 'task.dart';

/// A bounded collection of shared resources selected by key.
///
/// Use this when each tenant, endpoint, or configuration needs its own reusable
/// resource. Callers of [SharedResourceMapOperations.acquire] with the same key
/// share one resource; each lease lasts until its caller's scope closes. Keys
/// use Dart equality and must have stable hash codes.
///
/// The creating scope owns the map and supplies the services and task-local
/// values for acquisition and cleanup. Closing it rejects new requests, stops
/// pending acquisitions, and waits for active leases. Borrowers in another
/// runtime must release their own leases.
///
/// Keep the map inside its owner's lifetime. For one operation, use
/// `resources.use(key, use)` to release the lease promptly.
/// Returning the map from a finished `Runtime.run` does not keep it open.
final class SharedResourceMap<K, T, E> {
  SharedResourceMap._(this._state);

  final SharedResourceState<K, T, E> _state;

  /// Creates an empty resource map owned by the executing scope.
  ///
  /// [capacity] must be positive and [idleTimeout] nonnegative. Invalid arguments
  /// become defects when the task runs. [acquire] runs only when a new resource
  /// can be admitted; each execution of this task creates a separate map.
  ///
  /// Capacity includes acquisitions, idle resources, invalidated resources still
  /// in use, and cleanup in progress. Nothing is evicted to admit a different
  /// key. [onCapacityExceeded] produces the business error for a rejected key;
  /// a caller can still share an existing resource when the map is full.
  /// Exceptions from either callback become defects.
  ///
  /// [idleTimeout] keeps an unused resource available after its last lease ends.
  /// Zero releases it immediately. A resource frees its slot only after cleanup,
  /// including when acquisition fails. Idle cleanup failures are reported to the
  /// creating runtime; explicitly awaited cleanup failures belong to the caller.
  static Task<SharedResourceMap<K, T, E>, Never> create<K, T, E>({
    required int capacity,
    required Task<T, E> Function(K key) acquire,
    required E Function(K key) onCapacityExceeded,
    Duration idleTimeout = Duration.zero,
  }) => SharedResourceState.create(
    acquire,
    capacity: capacity,
    onCapacityExceeded: onCapacityExceeded,
    idleTimeout: idleTimeout,
  ).map(SharedResourceMap._);
}

/// Borrows or replaces resources by key.
extension SharedResourceMapOperations<K, T, E> on SharedResourceMap<K, T, E> {
  /// Borrows the resource for [key] until the calling scope closes.
  ///
  /// If no resource is available for [key], acquisition either starts or fails
  /// immediately with the configured capacity error. Cancelling one waiting
  /// caller leaves the others waiting. The last waiter cancels an unfinished
  /// acquisition and waits for cleanup.
  ///
  /// A closed map fails with a [StateError] defect. Waiting for the owner to close
  /// while holding a lease also fails if closing would wait for the caller.
  /// Use [use] to release the lease after one operation.
  Task<T, E> acquire(K key) => Task.defer(() => _state.acquire(key));

  /// Borrows the resource for [key] and releases its lease before completing.
  ///
  /// Runs [use] in a child scope and returns the callback's value on success.
  /// Success, failure, and cancellation wait for the scope to close and release
  /// the lease. If the last lease closes the resource, cleanup finishes first.
  /// Other leases and the configured idle timeout can keep the resource open
  /// and occupying capacity.
  ///
  /// Callback and cleanup failures are both preserved; thrown callback exceptions
  /// become defects. Return values must remain usable after the lease ends.
  Task<U, E> use<U>(K key, Task<U, E> Function(T value) use) =>
      acquire(key).flatMap(use).scoped();

  /// Makes future callers of [key] acquire a replacement resource.
  ///
  /// Existing callers keep their resource. It is released after the last lease,
  /// without an idle delay, and occupies capacity until cleanup finishes. The
  /// next acquisition can therefore fail for capacity. A missing key has no
  /// effect.
  ///
  /// For an unused resource, this task waits for cleanup and propagates failures;
  /// otherwise its last borrower receives them. Cancellation waits for cleanup
  /// already started. A closed map fails with a [StateError] defect.
  Task<void, Never> invalidate(K key) =>
      Task.defer(() => _state.invalidate(key));
}
