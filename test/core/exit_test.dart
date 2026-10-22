import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  group('Exit', () {
    test('supports exhaustive matching across all four terminal outcomes', () {
      expect(_describe(const Exit<int, String>.success(42)), 'Completed: 42');
      expect(
        _describe(const Exit<int, String>.failure('invalid')),
        'Failed: invalid',
      );
      expect(
        _describe(
          const Exit<int, String>.defect('unexpected', StackTrace.empty),
        ),
        'Unexpected: unexpected',
      );
      expect(
        _describe(const Exit<int, String>.cancelled(reason: 'shutdown')),
        'Cancelled: shutdown',
      );
    });

    test('named constructors support const values for all four cases', () {
      const Exit<int, String> success = Exit.success(42);
      const Exit<int, String> failure = Exit.failure('invalid');
      const Exit<int, String> defect = Exit.defect(
        'unexpected',
        StackTrace.empty,
      );
      const Exit<int, String> cancelled = Exit.cancelled();

      expect(success, same(const ExitSuccess<int, String>(42)));
      expect(failure, same(const ExitFailure<int, String>('invalid')));
      expect(
        defect,
        same(const ExitDefect<int, String>('unexpected', StackTrace.empty)),
      );
      expect(cancelled, same(const ExitCancelled<int, String>()));
    });

    for (final originalError in <Object>[
      StateError('unexpected state'),
      'a thrown value that is not an Exception',
    ]) {
      test('preserves a caught ${originalError.runtimeType} and its trace', () {
        try {
          throw originalError;
        } catch (error, stackTrace) {
          final Exit<int, String> exit = Exit.defect(error, stackTrace);

          expect(
            exit,
            isA<ExitDefect<int, String>>()
                .having((exit) => exit.error, 'error', same(originalError))
                .having(
                  (exit) => exit.stackTrace,
                  'stackTrace',
                  same(stackTrace),
                ),
          );
        }
      });
    }

    test('converts a successful Result without copying its value', () {
      final value = Object();
      final Result<Object, String> result = Result.success(value);
      final Exit<Object, String> exit = Exit.fromResult(result);

      expect(
        exit,
        isA<ExitSuccess<Object, String>>().having(
          (exit) => exit.value,
          'value',
          same(value),
        ),
      );
    });

    test('converts a failed Result without reclassifying its error', () {
      final error = StateError('an explicitly expected failure');
      final Result<int, StateError> result = Result.failure(error);
      final Exit<int, StateError> exit = Exit.fromResult(result);

      expect(
        exit,
        isA<ExitFailure<int, StateError>>().having(
          (exit) => exit.error,
          'error',
          same(error),
        ),
      );
    });

    test('preserves structured expected errors through Result conversion', () {
      const Result<int, ({String field, String message})> result =
          Result.failure((field: 'count', message: 'Expected an integer.'));
      final exit = Exit.fromResult(result);

      expect(switch (exit) {
        ExitSuccess(:final value) => '$value',
        ExitFailure(:final error) => '${error.field}: ${error.message}',
        ExitDefect() => 'unexpected failure',
        ExitCancelled() => 'cancelled',
      }, 'count: Expected an integer.');
    });

    test('preserves nullable payloads and their cases through conversion', () {
      const Result<int?, String?> success = Result.success(null);
      const Result<int?, String?> failure = Result.failure(null);

      expect(_describe(Exit.fromResult(success)), 'Completed: null');
      expect(_describe(Exit.fromResult(failure)), 'Failed: null');
      expect(
        _describe(const Exit<int?, String?>.success(null)),
        'Completed: null',
      );
      expect(
        _describe(const Exit<int?, String?>.failure(null)),
        'Failed: null',
      );
    });

    test('supports successful operations without a return value', () {
      const Result<void, Never> result = Result.success(null);
      final Exit<void, Never> exit = Exit.fromResult(result);

      expect(exit, isA<ExitSuccess<void, Never>>());
    });

    test(
      'keeps cancellation reasons separate from the expected error type',
      () {
        final reason = Object();
        final Exit<Never, Never> cancelled = Exit.cancelled(reason: reason);

        expect(
          cancelled,
          isA<ExitCancelled<Never, Never>>().having(
            (exit) => exit.reason,
            'reason',
            same(reason),
          ),
        );
        expect(
          const Exit<Never, Never>.cancelled(),
          isA<ExitCancelled<Never, Never>>().having(
            (exit) => exit.reason,
            'reason',
            isNull,
          ),
        );
      },
    );

    test('supports defects even when no expected failure is possible', () {
      const Exit<Never, Never> exit = Exit.defect(
        'unexpected',
        StackTrace.empty,
      );

      expect(_describe(exit), 'Unexpected: unexpected');
    });

    test('preserves all cases when generic types are widened', () {
      const success = ExitSuccess<int, Never>(42);
      const failure = ExitFailure<Never, String>('invalid');
      const defect = ExitDefect<Never, Never>('unexpected', StackTrace.empty);
      const cancelled = ExitCancelled<Never, Never>();
      const Exit<num, Object> widenedSuccess = success;
      const Exit<int, Object> widenedFailure = failure;
      const Exit<int, String> widenedDefect = defect;
      const Exit<int, String> widenedCancellation = cancelled;

      expect(widenedSuccess, same(success));
      expect(widenedFailure, same(failure));
      expect(widenedDefect, same(defect));
      expect(widenedCancellation, same(cancelled));
      expect(_describe(widenedSuccess), 'Completed: 42');
      expect(_describe(widenedFailure), 'Failed: invalid');
      expect(_describe(widenedDefect), 'Unexpected: unexpected');
      expect(_describe(widenedCancellation), 'Cancelled: null');
    });
  });
}

String _describe<T, E>(Exit<T, E> exit) => switch (exit) {
  ExitSuccess(:final value) => 'Completed: $value',
  ExitFailure(:final error) => 'Failed: $error',
  ExitDefect(:final error) => 'Unexpected: $error',
  ExitCancelled(:final reason) => 'Cancelled: $reason',
};
