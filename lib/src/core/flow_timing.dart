import 'flow.dart';
import 'internal/flow_cursor.dart';
import 'task.dart';

/// Applies deadlines and resubscription policies to flows.
extension FlowTimingOperations<T, E> on Flow<T, E> {
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
