import 'package:any/any.dart';

import 'race_tasks.dart';

/// Races [tasks] until one body succeeds or every body has failed.
///
/// An expected error, defect, or child cancellation does not stop the other
/// candidates. The first successful body requests cancellation of the remaining
/// children. All started bodies and cleanup are awaited, so noncooperative work
/// can delay completion. Parent cancellation is forwarded to every child.
///
/// Each child owns a fresh [Scope] and shares dependencies by reference. After
/// a success, unsuccessful body outcomes are discarded, but cleanup failures
/// from any child still make the result fail. If no body succeeds, body and
/// cleanup causes are combined in input order, body before cleanup per child.
/// Values referring to acquired resources have already been released on return.
///
/// The finite input is enumerated once per run. Every candidate may start;
/// this operation has no concurrency limit. Empty input or a thrown iterator
/// becomes a defect, cancelling and draining any queued children.
///
/// ```dart
/// final task = firstSuccess([
///   Task<String, String, ()>((_) => const Exit.failure(Cause.failure('offline'))),
///   Task<String, String, ()>((_) => const Exit.success('available')),
/// ]);
/// final exit = await task.run(()); // Success('available').
/// ```
Task<T, E, R> firstSuccess<T, E, R>(Iterable<Task<T, E, R>> tasks) =>
    raceTasks(tasks, successOnly: true);
