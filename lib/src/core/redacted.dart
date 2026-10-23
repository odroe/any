/// A value whose string representation hides its contents.
///
/// Use [reveal] to explicitly access the original value. This wrapper does not
/// encrypt or erase the value, and it does not prevent callers from exposing
/// the value after revealing it.
final class Redacted<T> {
  /// Wraps [value] without copying or modifying it.
  const Redacted(T value) : _value = value;

  final T _value;

  /// Returns the original value.
  T reveal() => _value;

  /// Returns a fixed placeholder without formatting the wrapped value.
  @override
  String toString() => 'Redacted(<redacted>)';
}
