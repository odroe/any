import 'exit.dart';
import 'task.dart';

/// A reusable gate that tasks can wait to pass through.
///
/// Use a latch to pause work until a readiness condition is met. [open]
/// releases every current waiter and lets later waiters pass; [reset] closes
/// the gate for future waits. Use a `Deferred` when waiting for a one-time
/// result instead of a reusable signal.
///
/// Resetting cannot retract a release already delivered to a waiter.
/// Cancelling a wait affects neither the gate nor other waiters.
final class Latch {
  Latch._(this._open);

  bool _open;
  Set<void Function()> _waiters = {};

  /// Creates a latch that starts closed unless [open] is `true`.
  ///
  /// Each execution creates an independent gate.
  static Task<Latch, Never> create({bool open = false}) =>
      Task.sync(() => Latch._(open));

  /// Waits until the gate opens, or completes immediately if already open.
  ///
  /// An open followed immediately by a reset still releases existing waiters.
  /// Each execution waits independently and can be cancelled.
  Task<void, Never> awaitOpen() => Task.fromCallback((complete) {
    if (_open) {
      complete(const Exit.success(null));
      return () {};
    }
    final waiters = _waiters;
    void release() => complete(const Exit.success(null));
    waiters.add(release);
    return () {
      waiters.remove(release);
    };
  });

  /// Opens the gate and releases all current waiters.
  ///
  /// Later waits pass until [reset]. Opening an already open gate has no extra
  /// effect and does not accumulate releases.
  Task<void, Never> open() => Task.sync(() {
    _open = true;
    final waiters = _waiters;
    _waiters = {};
    for (final release in waiters) {
      release();
    }
  });

  /// Closes the gate for future waits.
  ///
  /// Already released waiters remain released. Resetting a closed gate preserves
  /// its waiting tasks.
  Task<void, Never> reset() => Task.sync(() {
    _open = false;
  });
}
