import 'package:any/any.dart';

/// Supplies task dependencies using an ordinary typed task.
extension TaskDependencies<T, E, R> on Task<T, E, R> {
  /// A task that builds its dependencies with [dependencies] before executing.
  ///
  /// The builder receives the outer dependencies [R0]. Its successful value of
  /// type [R] becomes this task's dependencies. Both tasks share the caller's
  /// scope and cancellation token. A failed or cancelled builder skips this
  /// task. Use [TaskComposition.mapError] when the builder and consumer expose
  /// different expected error types.
  ///
  /// The builder runs once per execution, without global caching or lookup by
  /// runtime type. Records and application classes can combine several services.
  /// A builder using `acquireRelease` retains those resources until the shared
  /// scope closes, including through later workflow steps. Consumer finalizers
  /// run before builder finalizers in normal reverse registration order.
  ///
  /// [Task.runIn] leaves that scope open for its owner; [Task.run] closes it
  /// before returning. Keep all use of resource-backed values within the scope.
  ///
  /// ```dart
  /// final readPort = Task<int, Never, ({int port})>(
  ///   (context) => Exit.success(context.dependencies.port),
  /// );
  /// final configuration = Task<({int port}), Never, ()>(
  ///   (_) => const Exit.success((port: 8080)),
  /// );
  /// final exit = await readPort.provide(configuration).run(());
  /// ```
  Task<T, E, R0> provide<R0>(Task<R, E, R0> dependencies) =>
      dependencies.flatMap(
        (value) => Task((context) => runIn(context.withDependencies(value))),
      );
}
