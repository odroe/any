import '../cause.dart';
import '../task.dart';
import 'task_concurrency.dart';

Task<T, E> timeoutTask<T, E>(
  Task<T, E> task,
  Duration duration, {
  required E Function() onTimeout,
}) => Task.defer(() {
  final expired = Object();
  return _race(task, duration, expired)
      .mapCause((cause) => _translate(cause, expired, onTimeout: onTimeout))
      .mapError((error) => error as E);
});

Task<T, E> timeoutOrElseTask<T, E>(
  Task<T, E> task,
  Duration duration,
  Task<T, E> Function() fallback,
) => Task.defer(() {
  final expired = Object();
  return recoverTaskCause(
        _race(task, duration, expired),
        (cause) => switch (cause) {
          CauseFailure(:final error) when identical(error, expired) =>
            Task.defer(fallback),
          _ => null,
        },
      )
      .mapCause((cause) => _translate(cause, expired))
      .mapError((error) => error as E);
});

Task<T, Object?> _race<T, E>(
  Task<T, E> task,
  Duration duration,
  Object expired,
) => computeRace([
  // Register the deadline before starting the computation. The race stops and
  // waits for both fibers; user resources retain the caller's scope.
  Task.sleep(duration).withError<Object?>().flatMap((_) => Task.fail(expired)),
  task,
]);

Cause<Object?> _translate(
  Cause<Object?> cause,
  Object expired, {
  Object? Function()? onTimeout,
}) {
  final reasons = <Cause<Object?>>[];
  for (final reason in switch (cause) {
    CauseGroup(:final causes) => causes,
    _ => [cause],
  }) {
    switch (reason) {
      case CauseFailure(:final error):
        if (!identical(error, expired)) {
          reasons.add(reason);
        } else if (onTimeout != null) {
          try {
            reasons.add(Cause.failure(onTimeout()));
          } catch (error, stackTrace) {
            // Keep release defects already collected by the race even when
            // constructing the timeout error itself fails.
            reasons.add(Cause.defect(error, stackTrace));
          }
        }
      case CauseDefect():
        reasons.add(reason);
      case CauseCancelled():
        reasons.add(reason);
      case CauseGroup():
        throw StateError('Cause groups must be flat.');
    }
  }
  // timeoutOrElse recovers a lone expiration marker before reaching here.
  // If cleanup failed, only that marker is removed; defects retain identity
  // and their occurrence sources through Task.mapCause. Narrow business errors
  // separately with mapError so their occurrence sources are retained too.
  return Cause.combine(reasons);
}
