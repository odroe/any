import 'internal/shared_resource_state.dart';
import 'task.dart';

/// Shares a lazily acquired resource through scope-owned leases.
///
/// Each successful [acquire] keeps its generation alive until the calling scope
/// closes. Concurrent callers share one acquisition. Acquisition and cleanup
/// use the services and task-local values captured when this object is created,
/// rather than those of its borrowers.
///
/// The creating scope owns this object. Closing it rejects new requests,
/// cancels unfinished acquisitions, and waits for existing leases before
/// releasing their resources. Borrowers in another runtime must release their
/// leases; closing the owner cannot cancel those independent tasks. Returning
/// this object from a completed Runtime.run does not keep it alive.
///
/// Acquisition and cleanup must not wait for another lease on this same
/// resource. Waiting for the owning scope or runtime to close from acquisition,
/// cleanup, or an active lease fails with a [StateError] defect when closing
/// would wait for that caller.
final class SharedResource<T, E> {
  SharedResource._(this._state);

  final SharedResourceState<Null, T, E> _state;

  /// Creates a shared resource owned by the current scope when this task runs.
  ///
  /// [acquire] starts only when the first borrower arrives. Resources registered
  /// by it remain open for its leases unless explicitly scoped more narrowly.
  /// A failed acquisition is fully cleaned up before its failure is delivered
  /// and is not cached.
  ///
  /// After the last lease ends, [idleTimeout] keeps an unused generation ready
  /// for reuse. Zero releases it immediately. Negative durations produce an
  /// argument defect during execution. Idle timers use the creation-time clock.
  /// Background cleanup failures are reported to the creating runtime; cleanup
  /// explicitly awaited by a caller is included in that caller's exit instead.
  static Task<SharedResource<T, E>, Never> create<T, E>(
    Task<T, E> acquire, {
    Duration idleTimeout = Duration.zero,
  }) => SharedResourceState.create<Null, T, E>(
    (_) => acquire,
    idleTimeout: idleTimeout,
  ).map(SharedResource._);
}

/// Acquires and invalidates a shared resource using its known value/error types.
extension SharedResourceOperations<T, E> on SharedResource<T, E> {
  /// Acquires a lease that lasts until the calling scope closes.
  ///
  /// Pending callers share the same acquisition. Cancelling one removes only
  /// its wait; the last waiting caller cancels acquisition and waits for cleanup.
  /// Cancellation during delivery cannot leave a lease without its finalizer.
  /// Use a scoped task to release the lease before the surrounding task ends.
  /// A closed owner produces a [StateError] defect.
  Task<T, E> acquire() => Task.defer(() => _state.acquire(null));

  /// Retires the current generation so the next caller starts a new one.
  ///
  /// Existing waiters and leases retain their generation. It is released as
  /// soon as its final lease ends, without the idle timeout. If already unused,
  /// this task waits for its cleanup and propagates cleanup failures. Otherwise
  /// its last borrower observes those failures. Closed owners reject this call.
  /// Once invalidation starts, cancellation waits for any cleanup it initiates.
  Task<void, Never> invalidate() => Task.defer(() => _state.invalidate(null));
}
