import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  group('Result', () {
    test('supports constant construction and exhaustive matching', () {
      const success = Result<int, String>.success(7);
      const failure = Result<int, String>.failure('missing');

      String describe(Result<int, String> result) => switch (result) {
        ResultSuccess(:final value) => 'value: $value',
        ResultFailure(:final error) => 'error: $error',
      };

      expect(describe(success), 'value: 7');
      expect(describe(failure), 'error: missing');
    });

    test('distinguishes nullable success from nullable failure', () {
      const success = Result<String?, String?>.success(null);
      const failure = Result<String?, String?>.failure(null);

      expect(success, isA<ResultSuccess<String?, String?>>());
      expect(failure, isA<ResultFailure<String?, String?>>());
      expect((success as ResultSuccess<String?, String?>).value, isNull);
      expect((failure as ResultFailure<String?, String?>).error, isNull);
    });
  });

  group('map', () {
    test('transforms success immediately and exactly once', () {
      var calls = 0;
      final result = const Result<int, String>.success(7).map((value) {
        calls++;
        return value.toString();
      });

      expect(calls, 1);
      expect(
        result,
        isA<ResultSuccess<String, String>>().having(
          (result) => result.value,
          'value',
          '7',
        ),
      );
    });

    test('preserves the error and skips the callback on failure', () {
      final error = Object();
      final result = Result<int, Object>.failure(error)
          .map<String>((_) => fail('A failure must not invoke map.'));

      expect(
        result,
        isA<ResultFailure<String, Object>>().having(
          (result) => result.error,
          'error',
          same(error),
        ),
      );
    });

    test('propagates callback exceptions unchanged', () {
      final error = StateError('map');

      expect(
        () => const Result<int, String>.success(7).map<int>((_) => throw error),
        throwsA(same(error)),
      );
    });
  });

  group('mapError', () {
    test('transforms failure immediately and exactly once', () {
      var calls = 0;
      final result = const Result<int, String>.failure('missing')
          .mapError((error) {
            calls++;
            return error.length;
          });

      expect(calls, 1);
      expect(
        result,
        isA<ResultFailure<int, int>>().having(
          (result) => result.error,
          'error',
          7,
        ),
      );
    });

    test('preserves the value and skips the callback on success', () {
      final value = Object();
      final result = Result<Object, String>.success(value)
          .mapError<int>((_) => fail('A success must not invoke mapError.'));

      expect(
        result,
        isA<ResultSuccess<Object, int>>().having(
          (result) => result.value,
          'value',
          same(value),
        ),
      );
    });

    test('propagates callback exceptions unchanged', () {
      final error = StateError('mapError');

      expect(
        () =>
            const Result<int, String>.failure('missing')
                .mapError<int>((_) => throw error),
        throwsA(same(error)),
      );
    });
  });

  group('flatMap', () {
    test('continues with a success', () {
      final result = const Result<int, String>.success(7)
          .flatMap((value) => Result.success(value.toString()));

      expect(
        result,
        isA<ResultSuccess<String, String>>().having(
          (result) => result.value,
          'value',
          '7',
        ),
      );
    });

    test('continues with a failure and skips later callbacks', () {
      final error = Object();
      final result = const Result<int, Object>.success(7)
          .flatMap<String>((_) => Result.failure(error))
          .flatMap<bool>((_) => fail('A failure must stop the chain.'));

      expect(
        result,
        isA<ResultFailure<bool, Object>>().having(
          (result) => result.error,
          'error',
          same(error),
        ),
      );
    });

    test('preserves an existing failure without invoking next', () {
      final error = Object();
      final result = Result<int, Object>.failure(error)
          .flatMap<String>((_) => fail('A failure must not invoke flatMap.'));

      expect(
        result,
        isA<ResultFailure<String, Object>>().having(
          (result) => result.error,
          'error',
          same(error),
        ),
      );
    });

    test('propagates callback exceptions unchanged', () {
      final error = StateError('flatMap');

      expect(
        () =>
            const Result<int, String>.success(7)
                .flatMap<int>((_) => throw error),
        throwsA(same(error)),
      );
    });
  });

  group('widened type parameters', () {
    test('flatMap accepts the widened error type after Never', () {
      const Result<int, String> source = Result<int, Never>.success(7);
      final result = source.flatMap<String>(
        (_) => const Result<String, String>.failure('missing'),
      );

      expect(
        result,
        isA<ResultFailure<String, String>>().having(
          (result) => result.error,
          'error',
          'missing',
        ),
      );
    });

    test('map and flatMap use both widened types', () {
      const Result<num, Object> source = Result<int, Never>.success(7);
      final mapped = source.map((num value) => value / 2);
      final chained = source.flatMap<double>(
        (num value) => Result<double, Object>.success(value / 2),
      );

      expect(
        mapped,
        isA<ResultSuccess<double, Object>>().having(
          (result) => result.value,
          'value',
          3.5,
        ),
      );
      expect(
        chained,
        isA<ResultSuccess<double, Object>>().having(
          (result) => result.value,
          'value',
          3.5,
        ),
      );
    });

    test('mapError uses the widened success and error types', () {
      const Result<num, Object> source = Result<Never, String>.failure(
        'missing',
      );
      final result = source.mapError((Object error) => error.toString().length);

      expect(
        result,
        isA<ResultFailure<num, int>>().having(
          (result) => result.error,
          'error',
          7,
        ),
      );
    });
  });
}
