import 'dart:math' as math;

import 'context.dart';

/// A replaceable source of pseudorandom integers, doubles, and booleans.
///
/// Supply a seeded source through [key] for reproducible tests, or implement
/// this interface to control individual choices. The default uses [math.Random]
/// and is unsuitable for cryptographic purposes.
abstract interface class RandomSource {
  /// The service key for the application's pseudorandom source.
  static final key = ServiceKey<RandomSource>('random');

  /// Creates a source backed by [math.Random], optionally initialized with [seed].
  ///
  /// A seed controls the generator's initial state. The generated sequence is
  /// not guaranteed to remain the same across Dart SDK releases.
  factory RandomSource({int? seed}) = _RandomSource;

  /// Returns an integer from zero, inclusive, to [max], exclusive.
  ///
  /// The default source accepts [max] from 1 through `2^32`, inclusive, and
  /// throws [RangeError] for other bounds, as specified by [math.Random.nextInt].
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
