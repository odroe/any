import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  group('Exit', () {
    test('const factories support exhaustive generic matching', () {
      const success = Exit<int, String>.success(42);
      const cause = Cause<String>.failure('failed');
      const failure = Exit<int, String>.failure(cause);

      expect(_valueOrCause(success), 42);
      expect(_valueOrCause(failure), same(cause));
      expect(success, same(const Exit<int, String>.success(42)));
      expect(failure, same(const Exit<int, String>.failure(cause)));
    });

    test('a successful value may be null', () {
      const success = Exit<int?, String>.success(null);

      expect(success, isA<ExitSuccess<int?, String>>());
      expect(_valueOrCause(success), isNull);
    });

    test('Never business errors still allow defects and cancellation', () {
      final defect = Cause<Never>.defect(
        StateError('unexpected'),
        StackTrace.current,
      );
      const cancelled = Cause<Never>.cancelled('requested');

      expect(_valueOrCause(Exit<int, Never>.failure(defect)), same(defect));
      expect(
        _valueOrCause(const Exit<int, Never>.failure(cancelled)),
        same(cancelled),
      );
    });

    test('retains a complete grouped failure', () {
      final cause = Cause<String>.combine([
        const Cause.failure('business failure'),
        Cause.defect(StateError('cleanup failure'), StackTrace.current),
        const Cause.cancelled('requested'),
      ]);
      final exit = Exit<int, String>.failure(cause);

      expect((exit as ExitFailure<int, String>).cause, same(cause));
    });
  });

  group('Exit.map', () {
    test('transforms successful values', () {
      const exit = Exit<int, String>.success(42);
      final mapped = exit.map((value) => value.toString());

      expect((mapped as ExitSuccess<String, String>).value, '42');
    });

    test('preserves the complete cause without calling the transform', () {
      final cause = Cause<String>.combine([
        const Cause.failure('failed'),
        const Cause.cancelled('requested'),
      ]);
      final exit = Exit<int, String>.failure(cause);
      final mapped = exit.map<String>(
        (value) => fail('Unexpected value: $value'),
      );

      expect((mapped as ExitFailure<String, String>).cause, same(cause));
    });

    test('uses widened success and business error types', () {
      const Exit<num, String> exit = Exit<int, Never>.success(1);
      final mapped = exit.map((num value) => value.toDouble() + 0.5);

      expect((mapped as ExitSuccess<double, String>).value, 1.5);
    });

    test('propagates errors thrown by the transform', () {
      final exception = StateError('mapping failed');
      const exit = Exit<int, String>.success(42);

      expect(
        () => exit.map<int>((value) => throw exception),
        throwsA(same(exception)),
      );
    });
  });

  group('Exit.mapError', () {
    test('maps every business error and preserves other reasons', () {
      final defect = Cause<int>.defect(
        StateError('unexpected'),
        StackTrace.current,
      );
      const cancelled = Cause<int>.cancelled('requested');
      final cause = Cause<int>.combine([
        const Cause.failure(1),
        defect,
        const Cause.failure(2),
        cancelled,
      ]);
      final exit = Exit<bool, int>.failure(cause);
      final visited = <int>[];
      final mapped = exit.mapError((error) {
        visited.add(error);
        return 'error $error';
      });
      final reasons =
          (mapped as ExitFailure<bool, String>).cause as CauseGroup<String>;

      expect(visited, [1, 2]);
      expect((reasons.causes[0] as CauseFailure<String>).error, 'error 1');
      expect(reasons.causes[1], same(defect));
      expect((reasons.causes[2] as CauseFailure<String>).error, 'error 2');
      expect(reasons.causes[3], same(cancelled));
    });

    test('retains successful values without calling the transform', () {
      final value = Object();
      final exit = Exit<Object, String>.success(value);
      final mapped = exit.mapError<int>(
        (error) => fail('Unexpected error: $error'),
      );

      expect((mapped as ExitSuccess<Object, int>).value, same(value));
    });

    test('uses widened types for failure transformations', () {
      const Exit<String, num> exit = Exit<Never, int>.failure(Cause.failure(1));
      final mapped = exit.mapError((num error) => error.toDouble() + 0.5);
      final cause = (mapped as ExitFailure<String, double>).cause;

      expect((cause as CauseFailure<double>).error, 1.5);
    });

    test('propagates errors thrown by the transform', () {
      final exception = StateError('mapping failed');
      const exit = Exit<int, String>.failure(Cause.failure('failed'));

      expect(
        () => exit.mapError<int>((error) => throw exception),
        throwsA(same(exception)),
      );
    });
  });
}

Object? _valueOrCause<T, E>(Exit<T, E> exit) => switch (exit) {
  ExitSuccess(:final value) => value,
  ExitFailure(:final cause) => cause,
};
