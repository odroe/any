import 'internal/shared_resource_state.dart';
import 'task.dart';

/// Shares scope-owned resource leases by key within a fixed capacity.
///
/// Callers for the same key share one acquisition. Each successful [acquire]
/// keeps that generation alive until its caller's scope closes. Keys follow
/// Dart's equality and hash-code contract and must remain stable while in use.
///
/// Capacity counts resource generations, including unfinished acquisitions,
/// idle resources, invalidated resources with active leases, and cleanup in
/// progress. An existing current generation can be shared at capacity; creating
/// another fails with the configured business error. Entries are not evicted to
/// admit a different key.
///
/// The creating scope owns this map. Acquisition and cleanup use its captured
/// services and task-local values. Closing rejects new requests, stops pending
/// acquisitions, and waits for active leases before releasing their resources.
/// Borrowers in another runtime must release their own leases. Returning this
/// map from a completed Runtime.run does not keep it alive.
final class SharedResourceMap<K, T, E> {
  SharedResourceMap._(this._state);

  final SharedResourceState<K, T, E> _state;

  /// Creates a lazily populated map owned by the current scope.
  ///
  /// [capacity] must be positive and [idleTimeout] nonnegative. Invalid values
  /// become argument defects when this task executes. Each execution creates an
  /// independent map; [acquire] is called only when a new generation can start.
  /// A failed acquisition releases its capacity after cleanup finishes.
  ///
  /// [onCapacityExceeded] maps a rejected key into [E], without starting an
  /// acquisition or changing an existing generation. Exceptions thrown by this
  /// callback or [acquire] become defects.
  ///
  /// After the last lease ends, [idleTimeout] keeps its generation available
  /// for reuse. Zero releases it immediately. Capacity becomes available only
  /// after cleanup finishes. Background cleanup failures are reported to the
  /// creating runtime; explicitly awaited cleanup failures belong to that
  /// caller's exit instead.
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

/// Uses the statically known key, value, and error types of a shared resource map.
extension SharedResourceMapOperations<K, T, E> on SharedResourceMap<K, T, E> {
  /// Acquires [key] with a lease lasting until the calling scope closes.
  ///
  /// A current generation is shared even when capacity is full. Otherwise the
  /// configured capacity error is returned immediately if no slot is free.
  /// Cancelling one waiting caller does not affect others. The last waiter to
  /// leave an unfinished acquisition stops it and waits for cleanup.
  ///
  /// Use a scoped task to release a lease early. A closed map fails with a
  /// [StateError] defect. Waiting for the owner to close while holding a lease
  /// also fails when closing would wait for the caller.
  Task<T, E> acquire(K key) => Task.defer(() => _state.acquire(key));

  /// Retires [key]'s current generation without stopping its existing callers.
  ///
  /// A missing key does nothing. Retired generations are released after their
  /// last lease, without the idle timeout. They keep their capacity until that
  /// cleanup finishes, so the next acquisition of [key] can fail for capacity.
  /// Invalidating repeatedly cannot free a generation that is still in use.
  ///
  /// An unused generation is cleaned up before this task finishes, and cleanup
  /// failures propagate to this caller. Otherwise its final borrower receives
  /// them. Cancellation waits for cleanup already initiated by this operation.
  /// A closed map fails with a [StateError] defect.
  Task<void, Never> invalidate(K key) =>
      Task.defer(() => _state.invalidate(key));
}
