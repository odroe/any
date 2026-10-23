import '../cause.dart';
import '../exit.dart';
import '../fiber.dart';
import '../task.dart';

// Shared implementations for Task's public concurrency operations.
Task<List<T>, E> computeAll<T, E>(
  Iterable<Task<T, E>> tasks, {
  int concurrency = 1,
}) => computeForEach(tasks, (task) => task, concurrency: concurrency);

Task<List<T>, E> computeForEach<A, T, E>(
  Iterable<A> values,
  Task<T, E> Function(A value) action, {
  int concurrency = 1,
}) => _compute(
  values,
  action,
  concurrency: concurrency,
  mode: _Mode.all,
  finish: (run) => run.result(
    () => List<T>.unmodifiable([
      for (final exit in run.exits) (exit as ExitSuccess<T, E>).value,
    ]),
    include: (_) => true,
  ),
);

Task<List<Exit<T, E>>, Never> computeAllSettled<T, E>(
  Iterable<Task<T, E>> tasks, {
  int concurrency = 1,
}) => _compute(
  tasks,
  (task) => task,
  concurrency: concurrency,
  mode: _Mode.settled,
  finish: (run) => run.result(
    () => List<Exit<T, E>>.unmodifiable(run.exits.cast<Exit<T, E>>()),
    include: (completion) => completion.index == null,
  ),
);

Task<T, E> computeRace<T, E>(Iterable<Task<T, E>> tasks) => _compute(
  tasks,
  (task) => task,
  mode: _Mode.race,
  finish: (run) => run.result(
    () => (run.winner!.exit as ExitSuccess<T, E>).value,
    include: (completion) => identical(completion, run.winner),
  ),
);

Task<T, E> computeFirstSuccess<T, E>(Iterable<Task<T, E>> tasks) => _compute(
  tasks,
  (task) => task,
  mode: _Mode.firstSuccess,
  finish: (run) => run.result(
    () => (run.winner!.exit as ExitSuccess<T, E>).value,
    include: (_) => run.winner == null,
  ),
);

Task<(A, B), E> computeZip<A, B, E>(
  Task<A, E> left,
  Task<B, E> right, {
  int concurrency = 1,
}) => computeAll<Object?, E>([
  left,
  right,
], concurrency: concurrency).map((values) => (values[0] as A, values[1] as B));

Task<R, F> _compute<A, T, E, R, F>(
  Iterable<A> values,
  Task<T, E> Function(A value) action, {
  int? concurrency,
  required _Mode mode,
  required TaskEvaluation<R, F> Function(_Run<A, T, E> run) finish,
}) => Task.defer(() {
  if (concurrency != null && concurrency < 1) {
    throw ArgumentError.value(concurrency, 'concurrency', 'Must be positive.');
  }
  final run = _Run(values, action, concurrency, mode);
  return run
      .advance()
      .withError<F>()
      .flatMap((_) => taskFromEvaluation(finish(run)))
      .onExit(run.cleanup);
});

enum _Mode { all, settled, race, firstSuccess }

final class _Run<A, T, E> {
  _Run(this.values, this.action, this.concurrency, this.mode);

  final Iterable<A> values;
  final Task<T, E> Function(A value) action;
  final int? concurrency;
  final _Mode mode;
  final stopReason = Object();
  final active = <int, Fiber<T, E>>{};
  final exits = <Exit<T, E>?>[];
  final completed = <_Completion<T, E>>[];
  Iterator<A>? iterator;
  _Completion<T, E>? winner;
  void Function()? wake;
  var exhausted = false;
  var stopping = false;

  Task<void, Never> advance() => Task.defer(() {
    if (stopping || exhausted) {
      return active.isEmpty
          ? Task.succeed(null)
          : wait().flatMap((_) => advance());
    }
    if (concurrency != null && active.length >= concurrency!) {
      return wait().flatMap((_) => advance());
    }

    final A value;
    try {
      final source = iterator ??= values.iterator;
      if (!source.moveNext()) {
        exhausted = true;
        if (exits.isEmpty &&
            (mode == _Mode.race || mode == _Mode.firstSuccess)) {
          throw ArgumentError.value(values, 'tasks', 'Must not be empty.');
        }
        return advance();
      }
      value = source.current;
    } catch (error, stackTrace) {
      completed.add(
        _Completion(
          null,
          TaskEvaluation.fresh(Exit.failure(Cause.defect(error, stackTrace))),
        ),
      );
      stop();
      return advance();
    }

    final index = exits.length;
    exits.add(null);
    return Task.defer(
      () => stopping
          ? Task.failCause<E>(Cause.cancelled(stopReason))
          : action(value),
    ).fork().flatMap((fiber) {
      active[index] = fiber;
      if (stopping) fiber.requestCancel(reason: stopReason);
      listenFiber(fiber, (exit) {
        active.remove(index);
        exits[index] = exit;
        final completion = _Completion(
          index,
          TaskEvaluation(exit, fiberFailureEvents(fiber)),
        );
        completed.add(completion);
        if (!stopping &&
            switch (mode) {
              _Mode.all => exit is ExitFailure<T, E>,
              _Mode.race => true,
              _Mode.firstSuccess => exit is ExitSuccess<T, E>,
              _Mode.settled => false,
            }) {
          winner = completion;
          stop();
        }
        wake?.call();
      });
      return advance();
    });
  });

  Task<void, Never> wait() => Task.fromCallback((complete) {
    void notify() {
      wake = null;
      complete(const Exit.success(null));
    }

    wake = notify;
    // Advancing and registration run in one isolate. The recheck also handles
    // completion before this callback instruction is reached.
    if (active.isEmpty ||
        (!stopping &&
            !exhausted &&
            (concurrency == null || active.length < concurrency!))) {
      notify();
    }
    return () {
      if (identical(wake, notify)) wake = null;
    };
  });

  void stop() {
    if (stopping) return;
    stopping = true;
    for (final fiber in active.values.toList()) {
      fiber.requestCancel(reason: stopReason);
    }
  }

  Task<void, Never> cleanup(Exit<Object?, Object?> exit) => Task.defer(() {
    stop();
    if (active.isNotEmpty) return wait().flatMap((_) => cleanup(exit));
    if (exit is ExitSuccess) return Task.succeed(null);
    // Owner cancellation follows Task's cancellation rules. Business errors
    // cannot escape through this Never-typed finalizer, but defects must.
    return taskFromEvaluation(
      result<void, Never>(() {}, include: (_) => false, defectsOnly: true),
    );
  });

  TaskEvaluation<R, F> result<R, F>(
    R Function() success, {
    required bool Function(_Completion<T, E>) include,
    bool defectsOnly = false,
  }) {
    final reasons = <Cause<F>>[];
    final events = <FailureEvent>[];
    final seen = <Object>{};
    for (final completion in completed) {
      if (completion.exit case ExitFailure(:final cause)) {
        final keepCause = !defectsOnly && include(completion);
        final keepDefects =
            defectsOnly || mode != _Mode.settled || stopping || keepCause;
        final sources = Map<CauseDefect, List<FailureEvent>>.identity();
        for (final event in completion.result.events.reversed) {
          (sources[event.cause] ??= []).add(event);
        }
        for (final reason in switch (cause) {
          CauseGroup(:final causes) => causes,
          _ => [cause],
        }) {
          switch (reason) {
            case CauseDefect():
              if (!keepDefects) continue;
              final event = sources[reason]!.removeLast();
              if (!seen.add(event.source)) continue;
              reasons.add(reason);
              events.add(event);
            case CauseCancelled(:final reason):
              if (keepCause && !identical(reason, stopReason)) {
                reasons.add(Cause.cancelled(reason));
              }
            case CauseFailure(:final error):
              if (keepCause) reasons.add(Cause<F>.failure(error as F));
            case CauseGroup():
              throw StateError('Cause groups must be flat.');
          }
        }
      }
    }
    return TaskEvaluation(
      reasons.isEmpty
          ? Exit.success(success())
          : Exit.failure(Cause.combine(reasons)),
      List.unmodifiable(events),
    );
  }
}

final class _Completion<T, E> {
  const _Completion(this.index, this.result);

  final int? index;
  final TaskEvaluation<T, E> result;

  Exit<T, E> get exit => result.exit;
}
