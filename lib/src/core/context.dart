import 'option.dart';

/// Identifies a service of type [T] in a [Context].
///
/// Each key has its own identity. Two keys with the same [name] and type still
/// identify different services; the name is only used for diagnostics.
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

/// A service [value] paired with its typed [key].
///
/// Create bindings with [ServiceKey.bind] so that the key checks the value's
/// type before the binding can enter a [Context].
final class ServiceBinding<T> {
  const ServiceBinding._(this.key, this.value);

  /// The key that identifies this service.
  final ServiceKey<T> key;

  /// The service associated with [key].
  final T value;
}

/// An immutable collection of typed service bindings.
///
/// Use [read] when a service is required and [find] when its absence is allowed.
/// [merge] adds distinct bindings; [replace] explicitly changes an existing
/// binding. Neither operation modifies its inputs.
///
/// Immutability applies to the bindings, not to the service objects themselves.
final class Context {
  Context._(Map<ServiceKey<Object?>, Object?> services)
    : _services = Map.unmodifiable(services);

  /// Copies [bindings] into a new context.
  ///
  /// Throws [DuplicateService] if the same key appears more than once, even if
  /// both bindings contain the same value. Later changes to [bindings] do not
  /// affect the context.
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

  /// Combines this context with the distinct services from [other].
  ///
  /// Throws [DuplicateService] if both contexts contain the same key. Use
  /// [replace] to explicitly change an existing binding instead.
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

/// Indicates that a required service [key] is absent from a [Context].
final class MissingService implements Exception {
  /// Creates a diagnostic for the absent [key].
  const MissingService(this.key);

  /// The key whose binding was required.
  final ServiceKey<Object?> key;

  @override
  String toString() => 'MissingService: No service is bound to "${key.name}".';
}

/// Indicates that a [Context] operation encountered the same [key] twice.
final class DuplicateService implements Exception {
  /// Creates a diagnostic for the duplicate [key].
  const DuplicateService(this.key);

  /// The key that already has a binding.
  final ServiceKey<Object?> key;

  @override
  String toString() =>
      'DuplicateService: A service is already bound to "${key.name}".';
}
