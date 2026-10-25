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
/// contexts, and [add] inserts or replaces one binding. Later bindings replace
/// earlier ones for the same key. These operations leave the inputs intact.
///
/// ```dart
/// final endpoint = ServiceKey<Uri>('endpoint');
/// final production = Context.from([
///   endpoint.bind(Uri.parse('https://api.example.com')),
/// ]);
/// final testing = production.add(
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
  /// The last binding for each key wins. Later changes to the input collection
  /// do not affect this context.
  factory Context.from(Iterable<ServiceBinding<Object?>> bindings) {
    final services = <ServiceKey<Object?>, Object?>{};
    for (final binding in bindings) {
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
  /// Bindings in [other] replace matching keys in this context. Neither input is
  /// modified, and service objects retain their identity and existing lifetime.
  Context merge(Context other) => Context._({..._services, ...other._services});

  /// Returns a new context with [binding] inserted or replacing the same key.
  ///
  /// Other bindings and this context remain unchanged. A permitted `null` value
  /// replaces the previous value; it does not remove the binding.
  Context add(ServiceBinding<Object?> binding) =>
      Context._({..._services, binding.key: binding.value});
}

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
/// Thrown by [Context.read] when [key] has no binding.
final class MissingService implements Exception {
  /// Creates a diagnostic for the absent [key].
  const MissingService(this.key);

  /// The key whose binding was required.
  final ServiceKey<Object?> key;

  @override
  String toString() => 'MissingService: No service is bound to "${key.name}".';
}
