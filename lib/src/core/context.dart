import 'scope.dart';

/// The scope and dependencies used by a computation.
///
/// [R] describes the dependencies, typically a record or application class.
/// Their fields and methods retain their static types through [dependencies].
///
/// [withDependencies] creates a local view sharing the same [scope]. Use
/// [withScope] to associate a view with a separately managed lifetime. The caller
/// owns each scope and is responsible for closing it.
///
/// Context fields are immutable. Dependency objects are shared by reference and
/// may themselves be mutable.
///
/// ```dart
/// final scope = Scope();
/// final context = Context(
///   scope: scope,
///   dependencies: (label: 'production',),
/// );
/// final local = context.withDependencies((label: 'test',));
///
/// print(context.dependencies.label); // production
/// print(local.dependencies.label); // test
///
/// await scope.close();
/// ```
final class Context<R> {
  /// Creates a context referencing [scope] and [dependencies].
  Context({required this.scope, required this.dependencies});

  /// The resource lifetime used by this computation.
  final Scope scope;

  /// The dependencies available to this computation.
  final R dependencies;

  /// Creates a context with [dependencies] and the same [scope].
  ///
  /// The dependency type [D] may differ from [R]. This context and its
  /// dependencies are left unchanged.
  Context<D> withDependencies<D>(D dependencies) =>
      Context(scope: scope, dependencies: dependencies);

  /// Creates a context with [scope] and the same [dependencies].
  ///
  /// Dependencies are shared without copying. Both scopes retain their existing
  /// lifetimes; creating this view does not close or link them.
  Context<R> withScope(Scope scope) =>
      Context(scope: scope, dependencies: dependencies);
}
