import 'cause.dart';
import 'exit.dart';
import 'option.dart';
import 'task.dart';

/// A result that can be completed once and observed by multiple tasks.
///
/// The first executed completion wins. All current and future observers receive
/// that result; cancelling an observer removes only its own wait. A deferred
/// stores an [Exit], not a task that is executed again for each observer.
final class Deferred<T, E> {
  Deferred._();

  Exit<T, E>? _exit;
  Set<void Function(Exit<T, E>)>? _waiters = {};

  /// Creates a new, incomplete deferred each time the returned task executes.
  static Task<Deferred<T, E>, Never> create<T, E>() =>
      Task.sync(Deferred<T, E>._);

  /// Completes this deferred with [exit] when the returned task executes.
  ///
  /// Returns `true` if this completion won, or `false` if a result was already
  /// stored. The original exit is retained unchanged. Constructing this task
  /// does not complete the deferred.
  Task<bool, Never> complete(Exit<T, E> exit) => Task.sync(() {
    if (_exit != null) return false;
    _exit = exit;
    final waiters = _waiters!;
    _waiters = null;
    for (final waiter in waiters) {
      waiter(exit);
    }
    return true;
  });

  /// Completes this deferred successfully with [value] when executed.
  ///
  /// Returns whether this completion won, following [complete]'s rules.
  Task<bool, Never> succeed(T value) => complete(Exit.success(value));

  /// Completes this deferred with the business [error] when executed.
  ///
  /// Returns whether this completion won. Use [complete] to publish an exit
  /// containing defects, cancellation, or multiple failure reasons.
  Task<bool, Never> fail(E error) =>
      complete(Exit.failure(Cause.failure(error)));

  /// Waits for the stored exit and returns it as a successful value.
  ///
  /// A failed stored exit does not fail this task. Cancellation of the waiting
  /// task still terminates its wait without changing the deferred or other
  /// observers.
  Task<Exit<T, E>, Never> awaitExit() => Task.fromCallback(
    (complete) => _listen((exit) => complete(Exit.success(exit))),
  );

  /// Waits for success or propagates the stored exit's complete failure cause.
  ///
  /// Cancellation terminates only this task's wait. It does not complete the
  /// deferred or affect other observers.
  Task<T, E> join() => Task.fromCallback(_listen);

  /// Returns the stored exit, or [OptionNone] while still incomplete.
  ///
  /// This synchronous snapshot does not register an observer or start work.
  Option<Exit<T, E>> poll() => switch (_exit) {
    final exit? => Option.some(exit),
    null => const Option.none(),
  };

  void Function() _listen(void Function(Exit<T, E>) listener) {
    if (_exit case final exit?) {
      listener(exit);
      return () {};
    }
    _waiters!.add(listener);
    return () {
      _waiters?.remove(listener);
    };
  }
}
