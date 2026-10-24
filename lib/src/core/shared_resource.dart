import 'internal/shared_resource_state.dart';
import 'task.dart';

/// A resource shared by callers whose scopes keep it alive.
///
/// Use this for a client or connection that supports concurrent use. Each
/// [SharedResourceOperations.acquire] returns the same resource while it is
/// available and holds a lease until the caller's scope closes. For exclusive
/// use of interchangeable resources, use `Pool` instead.
///
/// The creating scope owns the shared resource. It captures the services and
/// task-local values used for acquisition and cleanup. Closing the owner
/// rejects new leases, stops pending acquisitions, and waits for active leases;
/// borrowers in another runtime must finish their own scopes.
///
/// This example keeps one HTTP client available throughout the requests. Each
/// request uses [SharedResourceOperations.use] to release its lease promptly.
///
/// ```dart
/// import 'dart:io';
///
/// import 'package:any/any.dart';
///
/// Future<List<int>> statusCodes(List<Uri> urls) async {
///   final runtime = Runtime();
///   final client = Task.acquireRelease(
///     Task.sync(HttpClient.new),
///     release: (client, _) => Task.sync(() => client.close(force: true)),
///   );
///   final program =
///       SharedResource.create<HttpClient, String>(
///         client,
///         idleTimeout: const Duration(seconds: 30),
///       ).withError<String>().flatMap(
///         (shared) => Task.forEach(
///           urls,
///           (url) => shared.use(
///             (client) => Task.fromFuture(() async {
///               final response = await (await client.getUrl(url)).close();
///               await response.drain<void>();
///               return response.statusCode;
///             }, onError: (error, _) => error.toString()),
///           ),
///           concurrency: 4,
///         ),
///       );
///   try {
///     return await runtime.runOrThrow(program);
///   } finally {
///     await runtime.close();
///   }
/// }
/// ```
///
/// Keep all use inside the owner's lifetime; returning this object from a
/// finished `Runtime.run` does not keep it open. Acquisition and cleanup must
/// not wait for another lease on the same resource. Waiting for the owner to
/// close from an active lease, acquisition, or cleanup fails with a [StateError]
/// defect if that close would wait for the caller.
final class SharedResource<T, E> {
  SharedResource._(this._state);

  final SharedResourceState<Null, T, E> _state;

  /// Creates a shared resource that acquires on its first use.
  ///
  /// The returned task creates an independent owner-bound resource each time it
  /// runs. Resources registered by [acquire] stay open for borrowers unless
  /// scoped more narrowly. A failed acquisition finishes cleanup before
  /// delivering its failure and is not kept for later callers.
  ///
  /// [idleTimeout] keeps an unused resource available after its last lease ends.
  /// Zero, the default, releases it immediately. Negative durations become
  /// argument defects when the task runs. The creation-time clock measures idle
  /// time.
  ///
  /// Idle cleanup failures are reported to the creating runtime. A caller that
  /// explicitly waits for cleanup receives those failures in its own exit.
  static Task<SharedResource<T, E>, Never> create<T, E>(
    Task<T, E> acquire, {
    Duration idleTimeout = Duration.zero,
  }) => SharedResourceState.create<Null, T, E>(
    (_) => acquire,
    idleTimeout: idleTimeout,
  ).map(SharedResource._);
}

/// Borrows or replaces a shared resource.
extension SharedResourceOperations<T, E> on SharedResource<T, E> {
  /// Borrows the resource until the calling scope closes.
  ///
  /// Use [use] to release the lease after one operation.
  /// Concurrent callers share an unfinished acquisition. Cancelling one waiter
  /// does not stop the others; the last waiter cancels acquisition and waits for
  /// its cleanup. A closed owner fails with a [StateError] defect.
  Task<T, E> acquire() => Task.defer(() => _state.acquire(null));

  /// Borrows the resource for [use] and releases its lease before completing.
  ///
  /// Runs [use] in a child scope and returns the callback's value on success.
  /// Success, failure, and cancellation wait for the scope to close and release
  /// the lease. If the last lease closes the resource, cleanup finishes first.
  /// Other leases and the configured idle timeout can keep the resource open.
  ///
  /// Callback and cleanup failures are both preserved; thrown callback exceptions
  /// become defects. Return values must remain usable after the lease ends.
  Task<U, E> use<U>(Task<U, E> Function(T value) use) =>
      acquire().flatMap(use).scoped();

  /// Makes future callers acquire a replacement resource.
  ///
  /// Existing callers keep the old resource until their last lease ends, at
  /// which point it is released without an idle delay. If it is already unused,
  /// this task waits for cleanup and propagates cleanup failures. Otherwise the
  /// last borrower receives them.
  ///
  /// Cancellation waits for cleanup started by this call. A closed owner fails
  /// with a [StateError] defect.
  Task<void, Never> invalidate() => Task.defer(() => _state.invalidate(null));
}
