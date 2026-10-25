import 'dart:collection';

import '../cause.dart';
import '../exit.dart';
import '../fiber.dart';
import '../option.dart';
import '../scope.dart';
import '../task.dart';

// A pull acknowledges the preceding delivery; its transport slot can be reused.
// Operators retaining values retain their transport producers before pulling
// again. Retirement waits on downstream leases, independently of user resources.
final class FlowCursor<T, E> {
  FlowCursor(
    this._pull,
    this.scopeForValue,
    this.finish,
    this.producers, {
    required this.resourceScope,
  });

  // User tasks borrow the consumer's resources. Producer scopes below only own
  // transport work and cleanup, and may retire before the consumer continues.
  final Scope resourceScope;

  final Task<Option<T>, E> _pull;
  final Scope Function() scopeForValue;
  final Task<void, Never> Function(Exit<Object?, Object?> exit) finish;
  final List<FlowProducer> Function() producers;

  Scope get scope => scopeForValue();
  Task<Option<T>, E> pull() => _pull;
}

// Leases keep an emitted value's producer alive while downstream work uses it.
// Normal completion may advance to another producer before those leases end.
final class FlowProducer {
  FlowProducer(this.scope, {this.afterClose});

  final Scope scope;
  final Future<TaskEvaluation<void, Never>> Function(Exit<Object?, Object?>)?
  afterClose;
  var _leases = 0;
  Exit<Object?, Object?>? _exit;
  Future<TaskEvaluation<void, Never>>? _closing;

  void retain() {
    if (_closing != null) throw StateError('The producer has already closed.');
    _leases++;
  }

  Task<void, Never> finish(Exit<Object?, Object?> exit) => Task.defer(() {
    _exit ??= exit;
    // An enclosing Scope close already owns this cleanup. Waiting from one of
    // its finalizers could wait on a sibling queued behind that same finalizer.
    if (_closing == null && scopeIsClosing(scope)) return Task.succeed(null);
    return _leases == 0 ? _closeTask() : Task.succeed(null);
  });

  Task<void, Never> _closeTask() => Task.uninterruptibleMask(
    (_) => Task.fromFuture(
      _closeNow,
      onError: (Object error, StackTrace stackTrace) =>
          Error.throwWithStackTrace(error, stackTrace),
    ).flatMap(taskFromEvaluation),
  );

  Future<TaskEvaluation<void, Never>> release() {
    if (_leases <= 0) {
      throw StateError('The producer lease was already released.');
    }
    _leases--;
    if (_leases != 0) {
      return Future.value(const TaskEvaluation(Exit.success(null), []));
    }
    return _exit == null || (_closing == null && scopeIsClosing(scope))
        ? Future.value(const TaskEvaluation(Exit.success(null), []))
        : _closeNow();
  }

  Future<TaskEvaluation<void, Never>> _closeNow() {
    final closing = _closing;
    // A producer has one delivery path for cleanup failure: the call that
    // initiates closure. Later retirement waits without replaying that result.
    return closing == null
        ? _closing = _close()
        : closing.then((_) => const TaskEvaluation(Exit.success(null), []));
  }

  Future<TaskEvaluation<void, Never>> _close() async {
    final exit = _exit!;
    await closeScope(scope, exit);
    final own = scopeCleanupFor(scope);
    final after = afterClose;
    return after == null ? own : mergeFlowEvaluations([own, await after(exit)]);
  }
}

TaskEvaluation<void, Never> mergeFlowEvaluations(
  Iterable<TaskEvaluation<void, Never>> results,
) {
  final causes = <Cause<Never>>[];
  final events = <FailureEvent>[];
  final cleanupCancellations = <CauseCancelled>[];
  final seen = <FailureSource>{};
  for (final result in results) {
    if (result.exit case ExitFailure(:final cause)) {
      final sources = Map<Cause<Object?>, Queue<FailureEvent>>.identity();
      for (final event in result.events) {
        (sources[event.cause] ??= Queue()).addLast(event);
      }
      final cancellations = [...result.cleanupCancellations];
      for (final reason in _causes(cause)) {
        if (reason is CauseDefect) {
          final event = sources[reason]!.removeFirst();
          if (!seen.add(event.source)) continue;
          events.add(event);
        }
        causes.add(reason);
        if (reason is CauseCancelled && cancellations.remove(reason)) {
          cleanupCancellations.add(reason);
        }
      }
    }
  }
  return TaskEvaluation(
    causes.isEmpty
        ? const Exit.success(null)
        : Exit.failure(Cause.combine(causes)),
    List.unmodifiable(events),
    cleanupCancellations: List.unmodifiable(cleanupCancellations),
  );
}

Future<TaskEvaluation<void, Never>> releaseFlowProducers(
  List<FlowProducer> producers,
) async {
  final results = <TaskEvaluation<void, Never>>[];
  for (final producer in producers.reversed) {
    results.add(await producer.release());
  }
  return mergeFlowEvaluations(results);
}

List<Cause<E>> _causes<E>(Cause<E> cause) => switch (cause) {
  CauseGroup(:final causes) => causes,
  _ => [cause],
};
