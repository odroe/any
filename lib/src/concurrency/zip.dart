import 'package:any/any.dart';

import 'all.dart';

/// Runs [first] and [second] concurrently, returning their values as a record.
///
/// The record preserves each task's value type and argument order. Both tasks
/// use the same dependency and expected-error types; use [TaskComposition.adapt]
/// to align independently defined tasks before combining them.
///
/// Execution follows [all]: each child owns a scope, failure requests sibling
/// cancellation, and both bodies and cleanup finish before the result is
/// returned. Cleanup errors are retained. Values referencing acquired resources
/// have already been released; use those resources inside the child task.
///
/// ```dart
/// final task = zip(
///   Task<int, Never, ()>((_) => const Exit.success(42)),
///   Task<String, Never, ()>((_) => const Exit.success('Ada')),
/// );
/// final exit = await task.run(()); // ExitSuccess<(int, String), Never>.
/// ```
Task<(A, B), E, R> zip<A, B, E, R>(Task<A, E, R> first, Task<B, E, R> second) =>
    all<Object?, E, R>([
      first,
      second,
    ], concurrency: 2).map((values) => (values[0] as A, values[1] as B));
