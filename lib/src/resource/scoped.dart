import 'package:any/any.dart';

/// Runs [task] within a new resource lifetime and waits for its cleanup.
///
/// Dependencies, cancellation, and scheduling policy are inherited from the
/// caller. Each run creates a fresh [Scope]; the caller's scope stays open.
/// Body and cleanup failures are combined as in [Task.run]. Cancellation does
/// not interrupt cleanup.
///
/// Acquire and use resources inside [task]. They have been released before
/// subsequent steps run, even if [task] returns a resource as its value. Use
/// this around one iteration of a long-lived workflow to release resources
/// before starting the next iteration.
///
/// ```dart
/// final events = <String>[];
/// final task = scoped(Task<int, Never, ()>((context) {
///   context.scope.addFinalizer((_) => events.add('released'));
///   return const Exit.success(42);
/// })).map((value) {
///   assert(events.single == 'released');
///   return value;
/// });
/// final exit = await task.run(());
/// ```
Task<T, E, R> scoped<T, E, R>(Task<T, E, R> task) => Task(
  (context) => task.run(
    context.dependencies,
    cancellation: context.cancellation,
    yieldEvery: context.yieldEvery,
  ),
);
