import 'exit.dart';
import 'task.dart';

/// A reusable gate that lets tasks wait until it opens.
///
/// [open] releases all current waiters and lets later waiters pass until [reset]
/// closes the gate. Resetting does not retract a release already delivered to a
/// waiter. Cancelling a wait removes only that waiter and leaves the gate alone.
final class Latch {
  Latch._(this._open);

  bool _open;
  Set<void Function()> _waiters = {};

  /// Creates an independent latch whenever the returned task executes.
  ///
  /// The gate starts closed unless [open] is `true`.
  static Task<Latch, Never> create({bool open = false}) =>
      Task.sync(() => Latch._(open));

  /// Waits for the gate to open, or passes immediately if it is already open.
  ///
  /// Each execution registers its own wait. Cancellation stops only that wait;
  /// it does not open or reset the latch. An open followed immediately by a reset
  /// still releases waiters that were registered before the open.
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

  /// Opens the gate and releases all registered waiters when executed.
  ///
  /// Later waits pass until [reset] executes. Opening an already open gate has
  /// no additional effect; it does not accumulate releases for future resets.
  Task<void, Never> open() => Task.sync(() {
    _open = true;
    final waiters = _waiters;
    _waiters = {};
    for (final release in waiters) {
      release();
    }
  });

  /// Closes the gate for subsequent waits when executed.
  ///
  /// Waiters already released by [open] remain released. Resetting an already
  /// closed gate preserves its waiting tasks until a later open or cancellation.
  Task<void, Never> reset() => Task.sync(() {
    _open = false;
  });
}
