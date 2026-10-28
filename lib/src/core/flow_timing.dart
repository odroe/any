import 'clock.dart';
import 'exit.dart';
import 'flow.dart';
import 'internal/flow_cursor.dart';
import 'option.dart';
import 'random_source.dart';
import 'schedule.dart';
import 'task.dart';

/// Retry policies and per-element deadlines for flows.
extension FlowTimingOperations<T, E> on Flow<T, E> {
  /// Restarts this flow after a business failure allowed by [schedule].
  ///
  /// The policy counts retries after the initial attempt and resets for each
  /// consumption, not after each element. [when] can reject a retry before the
  /// schedule is checked. Previously emitted values may appear again; consumer
  /// effects are not undone.
  ///
  /// Uses [TaskOperations.recover] to select the first business error when the
  /// cause contains no defect, including causes with recorded cancellations.
  /// [when] receives that error; rejection or policy exhaustion returns it as the
  /// final failure. Defects and pure cancellation propagate unchanged. Consumer
  /// failures do not retry the source. Immediate cleanup defects prevent retry;
  /// resources still used downstream remain alive and later cleanup failures
  /// still fail consumption.
  ///
  /// Retry delays use [Clock] and [RandomSource] from the consuming task and are
  /// cancellable.
  Flow<T, E> retry(
    Schedule schedule, {
    bool Function(E error)? when,
  }) => flowFromCursor(
    (resources) => accessTaskEnvironment(
      (owner, _) => Task.service(Clock.key).withError<E>().flatMap(
        (clock) => Task.service(RandomSource.key).withError<E>().map((random) {
          final started = clock.elapsed;
          var iteration = 0;
          FlowCursor<T, E>? current;

          Task<Option<T>, E> pull() => Task.defer(() {
            final source = current;
            final next = source != null
                ? source.pull()
                : inTaskScope(
                    openFlowCursor(this, resourceScope: resources),
                    owner,
                  ).flatMap((opened) {
                    current = opened;
                    return opened.pull();
                  });
            return next
                .onExit(
                  (exit) => exit is ExitFailure
                      ? current?.finish(exit) ?? Task.succeed(null)
                      : Task.succeed(null),
                )
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

          return FlowCursor(
            pull(),
            () => current?.scope ?? owner,
            (exit) =>
                Task.defer(() => current?.finish(exit) ?? Task.succeed(null)),
            () => current?.producers() ?? [],
            resourceScope: resources,
          );
        }),
      ),
    ),
  );

  /// Fails if a request for the next element exceeds [duration].
  ///
  /// The first request includes opening the source. Each later request gets a new
  /// deadline; downstream processing time is excluded. To limit the entire
  /// consumption, apply [TaskOperations.timeout] to its consuming task instead.
  ///
  /// Expiration cancels the pending read and waits for it to stop, then fails with
  /// [onTimeout]'s error. Cleanup failures are preserved. A non-cancellable
  /// [Future] may delay completion beyond the deadline. Uses the current [Clock]
  /// and [TaskOperations.timeout]'s duration and error rules.
  Flow<T, E> timeout(Duration duration, {required E Function() onTimeout}) =>
      flowFromCursor(
        (resources) => accessTaskEnvironment((owner, _) {
          FlowCursor<T, E>? current;
          final pull = Task.defer(() {
            final source = current;
            if (source != null) return source.pull();
            return inTaskScope(
              openFlowCursor(this, resourceScope: resources),
              owner,
            ).flatMap((opened) {
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
              resourceScope: resources,
            ),
          );
        }),
      );
}
