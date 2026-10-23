import 'dart:async';

import 'package:any/any.dart';

// Shared lifecycle rules for first-outcome and first-success competition.
Task<T, E, R> raceTasks<T, E, R>(
  Iterable<Task<T, E, R>> tasks, {
  required bool successOnly,
}) => Task((context) async {
  final controllers = <CancellationController>[];
  final removeListener = context.cancellation.onCancel((reason) {
    for (final controller in controllers) {
      controller.cancel(reason: reason);
    }
  });
  Exit<T, E>? winner;
  final failures = successOnly ? <Cause<E>?>[] : null;
  final cleanup = <Cause<E>?>[];
  final running = <Future<void>>[];

  void win(Exit<T, E> exit, [int? winnerIndex]) {
    if (winner != null) return;
    winner = exit;
    for (var index = 0; index < controllers.length; index++) {
      if (index != winnerIndex) controllers[index].cancel();
    }
  }

  Future<void> runChild(Task<T, E, R> task, int index) async {
    final scope = Scope();
    final exit = await task.runIn(
      Context(
        scope: scope,
        dependencies: context.dependencies,
        cancellation: controllers[index].token,
      ),
    );
    if (successOnly && exit is ExitFailure<T, E>) {
      failures![index] = exit.cause;
    } else {
      win(exit, index);
    }
    try {
      await scope.close(exit: exit);
    } on ScopeCloseException catch (error) {
      cleanup[index] = error.cause;
    } catch (error, stackTrace) {
      cleanup[index] = Cause.defect(error, stackTrace);
    }
  }

  try {
    try {
      final iterator = tasks.iterator;
      while (!context.cancellation.isCancelled && iterator.moveNext()) {
        if (context.cancellation.isCancelled) break;
        final task = iterator.current;
        if (context.cancellation.isCancelled) break;
        final index = cleanup.length;
        cleanup.add(null);
        failures?.add(null);
        controllers.add(CancellationController());
        running.add(runChild(task, index));
      }
      if (running.isEmpty && !context.cancellation.isCancelled) {
        throw ArgumentError('tasks must contain at least one task.');
      }
    } catch (error, stackTrace) {
      win(Exit.failure(Cause.defect(error, stackTrace)));
    }

    await Future.wait(running);
    if (winner == null && failures != null && failures.isNotEmpty) {
      return Exit.failure(
        Cause.combine([
          for (var i = 0; i < failures.length; i++) ...[
            ?failures[i],
            ?cleanup[i],
          ],
        ]),
      );
    }
    final exit =
        winner ??
        Exit<T, E>.failure(
          Cause.cancelled(reason: context.cancellation.reason),
        );
    final causes = [
      if (exit case ExitFailure(:final cause)) cause,
      for (final cause in cleanup) ?cause,
    ];
    return causes.isEmpty ? exit : Exit.failure(Cause.combine(causes));
  } finally {
    removeListener();
  }
});
