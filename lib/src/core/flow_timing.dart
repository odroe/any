import 'cause.dart';
import 'clock.dart';
import 'exit.dart';
import 'flow.dart';
import 'internal/flow_cursor.dart';
import 'option.dart';
import 'random_source.dart';
import 'schedule.dart';
import 'task.dart';

/// Applies deadlines and resubscription policies to flows.
extension FlowTimingOperations<T, E> on Flow<T, E> {
  /// Resubscribes after a single business failure allowed by [schedule].
  ///
  /// The schedule counts resubscriptions after the initial attempt. Its counter
  /// and monotonic start time are fresh for every consumption; emitted values
  /// do not reset them. [when] can reject a retry before consulting the schedule.
  /// Already delivered elements may be delivered again, and consumer effects
  /// are not rolled back.
  ///
  /// Failed production is retired before checking the policy. Resources retained
  /// by downstream elements remain alive until those elements are released.
  /// Immediate cleanup failure prevents retry; deferred cleanup failure still
  /// fails consumption. Defects, cancellation, and grouped causes bypass retry.
  /// Consumer failures are not retried. Delays use the consumption's Clock and
  /// RandomSource services and are cancellable; zero delay needs no timer.
  Flow<T, E> retry(
    Schedule schedule, {
    bool Function(E error)? when,
  }) => flowFromCursor(
    accessTaskEnvironment(
      (owner, _) => Task.service(Clock.key).withError<E>().flatMap(
        (clock) => Task.service(RandomSource.key).withError<E>().map((random) {
          final started = clock.elapsed;
          var iteration = 0;
          FlowCursor<T, E>? current;

          Task<Option<T>, E> pull() => Task.defer(() {
            final source = current;
            final next = source != null
                ? source.pull()
                : inTaskScope(openFlowCursor(this), owner).flatMap((opened) {
                    current = opened;
                    return opened.pull();
                  });
            return next.recover((error) {
              final exit = Exit<Object?, E>.failure(Cause.failure(error));
              final retire = current?.finish(exit) ?? Task.succeed(null);
              return retire
                  .withError<E>()
                  .mapCause(
                    (cause) => Cause.combine([Cause.failure(error), cause]),
                  )
                  .flatMap<Option<T>>((_) => Task.fail(error))
                  .recover(
                    (error) => Task.defer(() {
                      if (when != null && !when(error)) return Task.fail(error);
                      final delay = scheduleDelay(
                        schedule,
                        iteration: iteration++,
                        elapsed: clock.elapsed - started,
                        random: random,
                      );
                      if (delay == null) return Task.fail(error);
                      current = null;
                      if (delay == Duration.zero) return pull();
                      return Task.fromCallback<void, Never>(
                        (complete) => clock.schedule(
                          delay,
                          () => complete(const Exit.success(null)),
                        ),
                      ).withError<E>().flatMap((_) => pull());
                    }),
                  );
            });
          });

          return FlowCursor(
            pull(),
            () => current?.scope ?? owner,
            (exit) =>
                Task.defer(() => current?.finish(exit) ?? Task.succeed(null)),
            () => current?.producers() ?? [],
          );
        }),
      ),
    ),
  );

  /// Limits how long each request for the next element may wait.
  ///
  /// The first deadline includes opening the producer. Each later pull starts
  /// a new deadline; time spent processing a delivered value is excluded. Use
  /// a deadline on the consuming Task to limit the entire consumption instead.
  ///
  /// Expiration cancels the pending pull and waits for it to stop before
  /// producing [onTimeout]'s business error. Resources close when consumption
  /// exits or recovery retires this producer. Cleanup failures are preserved.
  /// An uncancellable Future can delay timeout completion until it settles.
  /// Uses the current clock and Task.timeout's error and duration rules.
  Flow<T, E> timeout(Duration duration, {required E Function() onTimeout}) =>
      flowFromCursor(
        accessTaskEnvironment((owner, _) {
          FlowCursor<T, E>? current;
          final pull = Task.defer(() {
            final source = current;
            if (source != null) return source.pull();
            return inTaskScope(openFlowCursor(this), owner).flatMap((opened) {
              current = opened;
              return opened.pull();
            });
          }).timeout(duration, onTimeout: onTimeout);
          return Task.succeed(
            FlowCursor(
              pull,
              () => current?.scope ?? owner,
              (exit) =>
                  Task.defer(() => current?.finish(exit) ?? Task.succeed(null)),
              () => current?.producers() ?? [],
            ),
          );
        }),
      );
}
