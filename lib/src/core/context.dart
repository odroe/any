import 'option.dart';

/// A typed identity for a service in a [Context].
///
/// Declare a key once and share it between service providers and consumers. Each
/// new key identifies a different service, even if its type and [name] match
/// another key. Use [bind] to associate a value with the key.
final class ServiceKey<T> {
  /// Creates a distinct service key with a diagnostic [name].
  ServiceKey(this.name);

  /// The name used to identify this key in diagnostics.
  final String name;

  /// Binds [value] to this key for use in a [Context].
  ///
  /// The value must match the key's original type, even when the key is accessed
  /// through a wider type or `dynamic`. A mismatch throws a [TypeError].
  ServiceBinding<T> bind(T value) => ServiceBinding._(this, value);
}

/// A service value associated with a [ServiceKey].
///
/// Create a binding with [ServiceKey.bind], then add it to a [Context]. The key
/// checks the value's type before the binding can be created.
final class ServiceBinding<T> {
  const ServiceBinding._(this.key, this.value);

  /// The key that identifies this service.
  final ServiceKey<T> key;

  /// The service associated with [key].
  final T value;
}

/// An immutable collection of services identified by typed keys.
///
/// Use [read] for required services and [find] for optional ones. [merge] combines
/// contexts with distinct keys; [replace] changes an existing binding. These
/// operations return new contexts and leave the original bindings intact.
///
/// ```dart
/// final endpoint = ServiceKey<Uri>('endpoint');
/// final production = Context.from([
///   endpoint.bind(Uri.parse('https://api.example.com')),
/// ]);
/// final testing = production.replace(
///   endpoint.bind(Uri.parse('http://localhost:8080')),
/// );
/// assert(production.read(endpoint).host == 'api.example.com');
/// assert(testing.read(endpoint).port == 8080);
/// ```
///
/// A context stores service objects by reference. It neither copies mutable
/// objects nor manages their cleanup. Supply it to `Runtime` or
/// `Task.provideContext` to make its bindings available to tasks.
final class Context {
  Context._(Map<ServiceKey<Object?>, Object?> services)
    : _services = Map.unmodifiable(services);

  /// Creates a context containing a snapshot of [bindings].
  ///
  /// Throws [DuplicateService] if a key occurs more than once, even if the values
  /// are identical. Later changes to the input collection do not affect this
  /// context.
  factory Context.from(Iterable<ServiceBinding<Object?>> bindings) {
    final services = <ServiceKey<Object?>, Object?>{};
    for (final binding in bindings) {
      if (services.containsKey(binding.key)) {
        throw DuplicateService(binding.key);
      }
      services[binding.key] = binding.value;
    }
    return Context._(services);
  }

  final Map<ServiceKey<Object?>, Object?> _services;

  /// Returns the service bound to [key].
  ///
  /// Throws [MissingService] if no binding exists. A bound `null` is returned
  /// normally when [T] permits it.
  T read<T>(ServiceKey<T> key) {
    if (!_services.containsKey(key)) throw MissingService(key);
    return _services[key] as T;
  }

  /// Returns the service bound to [key], or [OptionNone] if it is absent.
  ///
  /// A bound `null` is represented by [OptionSome], not by [OptionNone].
  Option<T> find<T>(ServiceKey<T> key) => _services.containsKey(key)
      ? Option.some(_services[key] as T)
      : const Option.none();

  /// Returns a new context containing this context's and [other]'s services.
  ///
  /// Throws [DuplicateService] if both contain the same key. Use [replace] when
  /// changing an existing service is intentional.
  Context merge(Context other) {
    final services = Map<ServiceKey<Object?>, Object?>.of(_services);
    for (final entry in other._services.entries) {
      if (services.containsKey(entry.key)) throw DuplicateService(entry.key);
      services[entry.key] = entry.value;
    }
    return Context._(services);
  }

  /// Returns a context with [binding] replacing its existing service.
  ///
  /// Throws [MissingService] if this context does not contain the binding's key.
  Context replace(ServiceBinding<Object?> binding) {
    if (!_services.containsKey(binding.key)) throw MissingService(binding.key);
    return Context._({..._services, binding.key: binding.value});
  }
}

// Explicit local providers replace matching bindings and inherit all others.
// Keep this operation internal so Context.merge continues to diagnose mistakes.
Context overlayContexts(Context base, Context provided) =>
    Context._({...base._services, ...provided._services});

// Service objects can define value equality unrelated to authorization or
// transaction identity. Request sharing compares bindings by identity only.
bool contextBindingsIdentical(Context left, Context right) {
  if (identical(left, right)) return true;
  if (left._services.length != right._services.length) return false;
  for (final entry in left._services.entries) {
    if (!right._services.containsKey(entry.key) ||
        !identical(right._services[entry.key], entry.value)) {
      return false;
    }
  }
  return true;
}

/// A missing required service in a [Context].
///
/// Thrown by [Context.read] and [Context.replace] when [key] has no binding.
final class MissingService implements Exception {
  /// Creates a diagnostic for the absent [key].
  const MissingService(this.key);

  /// The key whose binding was required.
  final ServiceKey<Object?> key;

  @override
  String toString() => 'MissingService: No service is bound to "${key.name}".';
}

/// A duplicate service key in a [Context] operation.
///
/// Thrown by [Context.from] and [Context.merge] instead of silently replacing a
/// binding.
final class DuplicateService implements Exception {
  /// Creates a diagnostic for the duplicate [key].
  const DuplicateService(this.key);

  /// The key that already has a binding.
  final ServiceKey<Object?> key;

  @override
  String toString() =>
      'DuplicateService: A service is already bound to "${key.name}".';
}
