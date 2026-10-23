/// A value whose string representation hides its contents.
///
/// Use this wrapper to keep secrets out of string interpolation and diagnostics.
/// Access the original value explicitly with [reveal]. It is still stored in
/// memory unchanged; the wrapper provides no encryption or secure erasure.
///
/// ```dart
/// const token = Redacted('example-token');
/// assert('$token' == 'Redacted(<redacted>)');
/// assert(token.reveal() == 'example-token');
/// ```
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
