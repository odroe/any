import 'cancellation.dart';
import 'scope.dart';

/// The scope, dependencies, and cancellation signal used by a computation.
///
/// [R] describes the dependencies, typically a record or application class.
/// Their fields and methods retain their static types through [dependencies].
///
/// [withDependencies] creates a local view sharing the same [scope]. Use
/// [withScope] to associate a view with a separately managed lifetime. The caller
/// owns each scope and is responsible for closing it. Both methods preserve the
/// same [cancellation] signal.
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
  /// Creates a context referencing [scope], [dependencies], and [cancellation].
  ///
  /// Defaults to [CancellationToken.none] when no cancellation token is supplied.
  Context({
    required this.scope,
    required this.dependencies,
    CancellationToken? cancellation,
  }) : cancellation = cancellation ?? CancellationToken.none;

  /// The resource lifetime used by this computation.
  final Scope scope;

  /// The dependencies available to this computation.
  final R dependencies;

  /// The cooperative cancellation signal shared by this computation's steps.
  final CancellationToken cancellation;

  /// Creates a context with [dependencies] and the same scope and cancellation.
  ///
  /// The dependency type [D] may differ from [R]. This context and its
  /// dependencies are left unchanged.
  Context<D> withDependencies<D>(D dependencies) => Context(
    scope: scope,
    dependencies: dependencies,
    cancellation: cancellation,
  );

  /// Creates a context with [scope] and the same dependencies and cancellation.
  ///
  /// Dependencies are shared without copying. Both scopes retain their existing
  /// lifetimes; creating this view does not close or link them.
  Context<R> withScope(Scope scope) => Context(
    scope: scope,
    dependencies: dependencies,
    cancellation: cancellation,
  );
}
