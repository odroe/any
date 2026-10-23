import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  group('Option', () {
    test('some and none have const constructors', () {
      const some = Option<int>.some(1);
      const none = Option<int>.none();

      expect(some, same(const OptionSome(1)));
      expect(none, same(const OptionNone<int>()));
    });

    test('pattern matching distinguishes a present null from absence', () {
      String describe(Option<String?> option) => switch (option) {
        OptionSome(:final value) => value ?? 'present null',
        OptionNone() => 'absent',
      };

      expect(describe(const Option.some('name')), 'name');
      expect(describe(const Option.some(null)), 'present null');
      expect(describe(const Option.none()), 'absent');
    });
  });

  group('map', () {
    test('transforms a present value exactly once', () {
      var calls = 0;
      final result = const Option.some(2).map((value) {
        calls++;
        return value.toString();
      });

      expect(result, isA<OptionSome<String>>());
      expect(result.getOrElse(() => 'missing'), '2');
      expect(calls, 1);
    });

    test('preserves absence without calling the transform', () {
      final result = const Option<int>.none().map<String>(
        (_) => fail('The transform must not run.'),
      );

      expect(result, isA<OptionNone<String>>());
    });

    test('passes a present null to the transform', () {
      final result = const Option<int?>.some(null)
          .map((value) => value == null ? 'present null' : '$value');

      expect(result.getOrElse(() => 'absent'), 'present null');
    });

    test('keeps a null transform result present', () {
      final result = const Option.some(1).map<int?>((_) => null);

      expect(result, isA<OptionSome<int?>>());
      expect(result.getOrElse(() => 42), isNull);
    });

    test('propagates the original callback exception', () {
      final error = StateError('transform failed');

      expect(
        () => const Option.some(1).map<int>((_) => throw error),
        throwsA(same(error)),
      );
    });

    test('uses the widened receiver type for callbacks', () {
      const Option<num> option = Option<int>.some(2);
      final result = option.map<num>((num value) => value / 2);

      expect(result.getOrElse(() => -1), 1.0);
    });
  });

  group('flatMap', () {
    test('returns the transformed option unchanged', () {
      final next = Option.some(StringBuffer('value'));
      var calls = 0;
      final result = const Option.some(1).flatMap((value) {
        calls++;
        expect(value, 1);
        return next;
      });

      expect(result, same(next));
      expect(calls, 1);
    });

    test('can replace a present value with absence', () {
      final result = const Option.some(1)
          .flatMap<String>((_) => const Option.none());

      expect(result, isA<OptionNone<String>>());
    });

    test('preserves absence without calling the transform', () {
      final result = const Option<int>.none().flatMap<String>(
        (_) => fail('The transform must not run.'),
      );

      expect(result, isA<OptionNone<String>>());
    });

    test('passes a present null to the transform', () {
      final result = const Option<int?>.some(null).flatMap<String>((value) {
        expect(value, isNull);
        return const Option.some('present null');
      });

      expect(result.getOrElse(() => 'absent'), 'present null');
    });

    test('propagates the original callback exception', () {
      final error = StateError('transform failed');

      expect(
        () => const Option.some(1).flatMap<int>((_) => throw error),
        throwsA(same(error)),
      );
    });

    test('accepts a wider callback result after receiver widening', () {
      const Option<num> option = Option<int>.some(2);
      final result = option.flatMap<num>((num value) => Option.some(value / 2));

      expect(result.getOrElse(() => -1), 1.0);
    });
  });

  group('getOrElse', () {
    test('returns a present value without evaluating the default', () {
      final result = const Option.some(1)
          .getOrElse(() => fail('The default must not run.'));

      expect(result, 1);
    });

    test('evaluates the default once for absence', () {
      var calls = 0;
      final result = const Option<int>.none().getOrElse(() {
        calls++;
        return 42;
      });

      expect(result, 42);
      expect(calls, 1);
    });

    test('does not replace a present null', () {
      final result = const Option<int?>.some(null)
          .getOrElse(() => fail('A present null must not use the default.'));

      expect(result, isNull);
    });

    test('accepts a nullable default', () {
      final result = const Option<int?>.none().getOrElse(() => null);

      expect(result, isNull);
    });

    test('propagates the original default exception', () {
      final error = StateError('default failed');

      expect(
        () => const Option<int>.none().getOrElse(() => throw error),
        throwsA(same(error)),
      );
    });

    test('uses the widened type of an absent Never option', () {
      const Option<num> option = Option<Never>.none();

      expect(option.getOrElse(() => 1.5), 1.5);
      expect(option.map((value) => '$value'), isA<OptionNone<String>>());
      expect(
        option.flatMap((value) => Option.some('$value')),
        isA<OptionNone<String>>(),
      );
    });
  });
}
