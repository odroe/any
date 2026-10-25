import 'cause.dart';
import 'exit.dart';
import 'internal/completion.dart';
import 'option.dart';
import 'task.dart';

/// A result that one task completes and any number of tasks can await.
///
/// Use a deferred to hand a value or failure from a producer to its consumers.
/// Run [succeed] or [fail] to publish a result, then [join] to consume it. Use
/// [awaitExit] when the consumer needs to inspect failure as a value.
///
/// The first executed completion wins. Current and future observers receive
/// that stored [Exit]; observing it does not rerun any work. Cancelling an
/// observer only stops its own wait.
final class Deferred<T, E> {
  Deferred._();

  final _completion = Completion<Exit<T, E>>();

  /// Creates an incomplete deferred, independent on each execution.
  static Task<Deferred<T, E>, Never> create<T, E>() =>
      Task.sync(Deferred<T, E>._);

  /// Publishes [exit] if no result has been stored yet.
  ///
  /// Returns `true` if this completion wins, or `false` if already completed. The
  /// original exit is retained unchanged. Completion happens when the task runs.
  Task<bool, Never> complete(Exit<T, E> exit) =>
      Task.sync(() => _completion.complete(exit));

  /// Publishes a successful [value], returning whether this completion wins.
  Task<bool, Never> succeed(T value) => complete(Exit.success(value));

  /// Publishes a business [error], returning whether this completion wins.
  ///
  /// Use [complete] for defects, cancellation, or combined failure causes.
  Task<bool, Never> fail(E error) =>
      complete(Exit.failure(Cause.failure(error)));

  /// Waits for the result and returns its [Exit] as a successful value.
  ///
  /// A stored failure does not fail this task. Cancelling the waiting task still
  /// ends its wait without changing the deferred.
  Task<Exit<T, E>, Never> awaitExit() => Task.fromCallback(
    (complete) => _completion.listen((exit) => complete(Exit.success(exit))),
  );

  /// Waits for a value or propagates the stored failure's complete cause.
  Task<T, E> join() => Task.fromCallback(_completion.listen);

  /// Returns the stored exit, or [OptionNone] if not yet completed.
  ///
  /// This synchronous snapshot does not wait or register an observer.
  Option<Exit<T, E>> poll() => switch (_completion.value) {
    final exit? => Option.some(exit),
    null => const Option.none(),
  };
}
