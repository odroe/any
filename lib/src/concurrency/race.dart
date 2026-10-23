import 'package:any/any.dart';

import 'race_tasks.dart';

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
/// forwarded to unfinished children, and the parent's scope remains caller-owned.
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
    raceTasks(tasks, successOnly: false);
