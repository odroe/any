import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  group('Result', () {
    test('supports exhaustive matching in a parsing workflow', () {
      expect(_describe(_parseInteger('42')), 'Parsed 42.');
      expect(_describe(_parseInteger('invalid')), 'Expected an integer.');
    });

    test('named constructors create const results of the correct case', () {
      const Result<int, String> success = Result.success(42);
      const Result<int, String> failure = Result.failure('invalid');

      expect(success, same(const Success<int, String>(42)));
      expect(failure, same(const Failure<int, String>('invalid')));
    });

    test('success can be reused with different error types', () {
      const success = Success<int, Never>(42);
      const Result<num, String> first = success;
      const Result<int, FormatException> second = success;

      expect(first, same(success));
      expect(second, same(success));
      expect(switch (first) {
        Success(:final value) => value.isFinite,
        Failure() => false,
      }, isTrue);
    });

    test('failure can be reused with different success types', () {
      const failure = Failure<Never, String>('invalid');
      const Result<int, Object> first = failure;
      const Result<String, String> second = failure;

      expect(first, same(failure));
      expect(second, same(failure));
    });

    test(
      'nullable payloads preserve the distinction between the two cases',
      () {
        const Result<int?, String?> success = Result.success(null);
        const Result<int?, String?> failure = Result.failure(null);

        expect(
          switch (success) {
            Success(:final value) => ('success', value),
            Failure(:final error) => ('failure', error),
          },
          ('success', null),
        );
        expect(
          switch (failure) {
            Success(:final value) => ('success', value),
            Failure(:final error) => ('failure', error),
          },
          ('failure', null),
        );
      },
    );

    test('supports operations without a return value', () {
      const Result<void, String> result = Result.success(null);

      expect(result, isA<Success<void, String>>());
    });

    test('preserves structured error types without requiring exceptions', () {
      const Result<int, ({String field, String message})> result =
          Result.failure((field: 'count', message: 'Expected an integer.'));

      expect(switch (result) {
        Success(:final value) => '$value',
        Failure(:final error) => '${error.field}: ${error.message}',
      }, 'count: Expected an integer.');
    });

    test('keeps payloads without copying or awaiting them', () async {
      final value = Object();
      final error = FormatException('invalid');
      final future = Future.value(42);
      final Result<Object, String> success = Result.success(value);
      final Result<int, FormatException> failure = Result.failure(error);
      final Result<Future<int>, String> pending = Result.success(future);

      expect(switch (success) {
        Success(:final value) => value,
        Failure() => null,
      }, same(value));
      expect(switch (failure) {
        Success() => null,
        Failure(:final error) => error,
      }, same(error));
      expect(switch (pending) {
        Success(:final value) => value,
        Failure() => null,
      }, same(future));
      expect(await future, 42);
    });
  });
}

Result<int, String> _parseInteger(String input) {
  final value = int.tryParse(input);
  return value == null
      ? const Result.failure('Expected an integer.')
      : Result.success(value);
}

String _describe(Result<int, String> result) => switch (result) {
  Success(:final value) => 'Parsed $value.',
  Failure(:final error) => error,
};
