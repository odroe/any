import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  group('Exit', () {
    test('supports exhaustive matching of outcomes and their causes', () {
      expect(_describe(const Exit<int, String>.success(42)), 'Completed: 42');
      expect(
        _describe(const Exit<int, String>.failure(Cause.failure('invalid'))),
        'Failed: invalid',
      );
      expect(
        _describe(
          const Exit<int, String>.failure(
            Cause.defect('unexpected', StackTrace.empty),
          ),
        ),
        'Unexpected: unexpected',
      );
      expect(
        _describe(
          const Exit<int, String>.failure(Cause.cancelled(reason: 'shutdown')),
        ),
        'Cancelled: shutdown',
      );
      final cause = Cause<String>.combine([
        const Cause.failure('invalid'),
        const Cause.defect('cleanup failed', StackTrace.empty),
      ]);
      expect(_describe(Exit<int, String>.failure(cause)), '2 failure reasons.');
    });

    test('supports const successful and unsuccessful exits', () {
      const Exit<int, String> success = Exit.success(42);
      const Exit<int, String> failure = Exit.failure(Cause.failure('invalid'));

      expect(success, same(const ExitSuccess<int, String>(42)));
      expect(
        failure,
        same(const ExitFailure<int, String>(Cause.failure('invalid'))),
      );
    });

    test('preserves every cause by reference', () {
      final causes = <Cause<String>>[
        const Cause.failure('invalid'),
        Cause.defect(StateError('unexpected'), StackTrace.current),
        Cause.cancelled(reason: Object()),
      ];
      for (final cause in [...causes, Cause<String>.combine(causes)]) {
        final exit =
            Exit<int, String>.failure(cause) as ExitFailure<int, String>;
        expect(exit.cause, same(cause));
      }
    });

    test('converts a successful Result without copying its value', () {
      final value = Object();
      final Result<Object, String> result = Result.success(value);
      final Exit<Object, String> exit = Exit.fromResult(result);

      expect((exit as ExitSuccess<Object, String>).value, same(value));
    });

    test('converts a failed Result without reclassifying its error', () {
      final error = StateError('an explicitly expected failure');
      final Result<int, StateError> result = Result.failure(error);
      final exit = Exit.fromResult(result) as ExitFailure<int, StateError>;

      expect((exit.cause as CauseFailure<StateError>).error, same(error));
    });

    test('preserves structured expected errors through Result conversion', () {
      const Result<int, ({String field, String message})> result =
          Result.failure((field: 'count', message: 'Expected an integer.'));
      final exit = Exit.fromResult(result);

      expect(switch (exit) {
        ExitSuccess(:final value) => '$value',
        ExitFailure(cause: CauseFailure(:final error)) =>
          '${error.field}: ${error.message}',
        ExitFailure() => 'unexpected outcome',
      }, 'count: Expected an integer.');
    });

    test('preserves nullable payloads and their cases through conversion', () {
      const Result<int?, String?> success = Result.success(null);
      const Result<int?, String?> failure = Result.failure(null);

      expect(_describe(Exit.fromResult(success)), 'Completed: null');
      expect(_describe(Exit.fromResult(failure)), 'Failed: null');
    });

    test('supports successful operations without a return value', () {
      const Result<void, Never> result = Result.success(null);
      final Exit<void, Never> exit = Exit.fromResult(result);

      expect(exit, isA<ExitSuccess<void, Never>>());
    });

    test('supports defects and cancellation with no expected error type', () {
      const Exit<Never, Never> defect = Exit.failure(
        Cause.defect('unexpected', StackTrace.empty),
      );
      const Exit<Never, Never> cancelled = Exit.failure(Cause.cancelled());

      expect(_describe(defect), 'Unexpected: unexpected');
      expect(_describe(cancelled), 'Cancelled: null');
    });

    test('preserves all cases when generic types are widened', () {
      const success = ExitSuccess<int, Never>(42);
      const failure = ExitFailure<Never, String>(Cause.failure('invalid'));
      const defect = ExitFailure<Never, Never>(
        Cause.defect('unexpected', StackTrace.empty),
      );
      const cancelled = ExitFailure<Never, Never>(Cause.cancelled());
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
      final cause = Cause<Never>.combine([
        const Cause.defect('cleanup', StackTrace.empty),
        const Cause.cancelled(),
      ]);
      final Exit<int, String> widenedGroup = Exit<Never, Never>.failure(cause);
      expect((widenedGroup as ExitFailure<int, String>).cause, same(cause));
      expect(_describe(widenedGroup), '2 failure reasons.');
    });
  });
}

String _describe<T, E>(Exit<T, E> exit) => switch (exit) {
  ExitSuccess(:final value) => 'Completed: $value',
  ExitFailure(cause: CauseFailure(:final error)) => 'Failed: $error',
  ExitFailure(cause: CauseDefect(:final error)) => 'Unexpected: $error',
  ExitFailure(cause: CauseCancelled(:final reason)) => 'Cancelled: $reason',
  ExitFailure(cause: CauseGroup(:final causes)) =>
    '${causes.length} failure reasons.',
};
