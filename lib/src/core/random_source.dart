import 'dart:math' as math;

import 'context.dart';

/// A replaceable source of pseudorandom integers, doubles, and booleans.
///
/// The default implementation uses [math.Random] and is not suitable for
/// cryptographic purposes. Implement this interface to control random choices
/// in tests or to supply another source.
abstract interface class RandomSource {
  /// Identifies the pseudorandom source installed in a runtime or local context.
  static final key = ServiceKey<RandomSource>('random');

  /// Creates a source backed by [math.Random], optionally initialized with [seed].
  ///
  /// A seed controls the generator's initial state. The generated sequence is
  /// not guaranteed to remain the same across Dart SDK releases.
  factory RandomSource({int? seed}) = _RandomSource;

  /// Returns an integer from zero, inclusive, to [max], exclusive.
  ///
  /// The default source accepts [max] from 1 through `1 << 32`, inclusive, and
  /// preserves [math.Random.nextInt]'s behavior for invalid bounds.
  int nextInt(int max);

  /// Returns a double from zero, inclusive, to one, exclusive.
  double nextDouble();

  /// Returns a random boolean.
  bool nextBool();
}

final class _RandomSource implements RandomSource {
  _RandomSource({int? seed}) : _random = math.Random(seed);

  final math.Random _random;

  @override
  int nextInt(int max) => _random.nextInt(max);

  @override
  double nextDouble() => _random.nextDouble();

  @override
  bool nextBool() => _random.nextBool();
}
