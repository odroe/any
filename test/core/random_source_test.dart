import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  group('RandomSource', () {
    test('seeded sources reproduce the same sequence independently', () {
      final first = RandomSource(seed: 42);
      final second = RandomSource(seed: 42);
      final sequence = [
        for (var i = 0; i < 20; i++)
          (first.nextInt(97), first.nextDouble(), first.nextBool()),
      ];

      final repeated = [
        for (var i = 0; i < 20; i++)
          (second.nextInt(97), second.nextDouble(), second.nextBool()),
      ];

      expect(repeated, sequence);
    });

    test('default sources keep values within their bounds', () {
      final source = RandomSource();

      for (var i = 0; i < 20; i++) {
        expect(source.nextInt(17), inInclusiveRange(0, 16));
        expect(
          source.nextDouble(),
          allOf(greaterThanOrEqualTo(0), lessThan(1)),
        );
      }
    });

    test('accepts the smallest and largest supported integer bounds', () {
      final source = RandomSource(seed: 42);

      expect(source.nextInt(1), 0);
      expect(source.nextInt(1 << 32), inInclusiveRange(0, (1 << 32) - 1));
    });

    test('rejects invalid integer bounds', () {
      final source = RandomSource(seed: 42);

      for (final max in [-1, 0, (1 << 32) + 1]) {
        expect(() => source.nextInt(max), throwsArgumentError);
      }
    });

    test('can be implemented by a controlled source', () {
      final RandomSource source = _FixedRandomSource();

      expect(source.nextInt(17), 16);
      expect(source.nextDouble(), 0.25);
      expect(source.nextBool(), isTrue);
    });
  });
}

final class _FixedRandomSource implements RandomSource {
  @override
  int nextInt(int max) => max - 1;

  @override
  double nextDouble() => 0.25;

  @override
  bool nextBool() => true;
}
