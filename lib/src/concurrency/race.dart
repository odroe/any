import 'dart:async';

import 'package:any/any.dart';

/// A task that returns the first completed body outcome from [tasks].
///
/// Success, expected failure, defects, and cancellation can all win. Losing
/// body outcomes are discarded. Each child owns a fresh [Scope] and shares the
/// parent's dependencies by reference. The winner requests cancellation of the
/// other children; all started bodies and their cleanup are awaited before
/// returning. A body that ignores cancellation can therefore delay the result.
///
/// Cleanup errors are never discarded. They follow the winning failure, if
/// any, in input order; a winning success becomes a failure if cleanup fails.
/// Finalizers receive their own child's outcome. Parent cancellation is
/// forwarded to every child, and the parent's scope remains caller-owned.
///
/// The finite input is enumerated once per run, before any child body starts.
/// Creating the task does not access the input. An empty input or a thrown
/// iteration error becomes a defect; children already started are cancelled
/// and drained. All children may run concurrently; use `all` when work needs a
/// concurrency limit.
///
/// ```dart
/// final fastest = race([
///   Task<String, Never, ()>((_) async {
///     await Future<void>.delayed(const Duration(milliseconds: 10));
///     return const Exit.success('first');
///   }),
///   Task<String, Never, ()>((_) => const Exit.success('second')),
/// ]);
/// final exit = await fastest.run(()); // ExitSuccess('second') after both finish.
/// ```
Task<T, E, R> race<T, E, R>(Iterable<Task<T, E, R>> tasks) =>
    Task((context) async {
      final controllers = <CancellationController>[];
      final removeListener = context.cancellation.onCancel((reason) {
        for (final controller in controllers) {
          controller.cancel(reason: reason);
        }
      });
      Exit<T, E>? winner;
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
        win(exit, index);
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
