import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  group('Cause', () {
    test('supports exhaustive matching for individual and combined causes', () {
      expect(
        _describe(const Cause<String>.failure('invalid')),
        'Failed: invalid',
      );
      expect(
        _describe(const Cause<String>.defect('unexpected', StackTrace.empty)),
        'Unexpected: unexpected',
      );
      expect(
        _describe(const Cause<String>.cancelled(reason: 'shutdown')),
        'Cancelled: shutdown',
      );
      expect(
        _describe(
          Cause<String>.combine([
            const Cause.failure('Request failed.'),
            Cause.defect(StateError('Cleanup failed.'), StackTrace.current),
          ]),
        ),
        '2 failure reasons.',
      );
    });

    test('supports const individual causes', () {
      expect(
        const Cause<String>.failure('invalid'),
        same(const CauseFailure<String>('invalid')),
      );
      expect(
        const Cause<Never>.defect('unexpected', StackTrace.empty),
        same(const CauseDefect<Never>('unexpected', StackTrace.empty)),
      );
      expect(
        const Cause<Never>.cancelled(),
        same(const CauseCancelled<Never>()),
      );
    });

    test('keeps expected errors by reference without reclassifying them', () {
      final error = StateError('expected');
      final cause = Cause<StateError>.failure(error);

      expect((cause as CauseFailure<StateError>).error, same(error));
      expect(_describe(const Cause<String?>.failure(null)), 'Failed: null');
    });

    for (final original in <Object>[StateError('unexpected'), 'thrown value']) {
      test('preserves a caught ${original.runtimeType} and its trace', () {
        try {
          throw original;
        } catch (error, stackTrace) {
          final cause =
              Cause<Never>.defect(error, stackTrace) as CauseDefect<Never>;

          expect(cause.error, same(original));
          expect(cause.stackTrace, same(stackTrace));
        }
      });
    }

    test('keeps cancellation independent of the expected error type', () {
      final reason = Object();
      final cause =
          Cause<Never>.cancelled(reason: reason) as CauseCancelled<Never>;

      expect(cause.reason, same(reason));
      expect(_describe(const Cause<Never>.cancelled()), 'Cancelled: null');
    });

    test('preserves causes when their expected error types are widened', () {
      const failure = CauseFailure<String>('invalid');
      const defect = CauseDefect<Never>('unexpected', StackTrace.empty);
      const cancelled = CauseCancelled<Never>();
      const Cause<Object?> widenedFailure = failure;
      const Cause<String> widenedDefect = defect;
      const Cause<String> widenedCancellation = cancelled;

      expect(widenedFailure, same(failure));
      expect(widenedDefect, same(defect));
      expect(widenedCancellation, same(cancelled));
      expect(_describe(widenedFailure), 'Failed: invalid');
      expect(_describe(widenedDefect), 'Unexpected: unexpected');
      expect(_describe(widenedCancellation), 'Cancelled: null');
    });

    group('combine', () {
      test('rejects an empty input', () {
        expect(() => Cause<Never>.combine([]), throwsArgumentError);
      });

      final individual = <Cause<String>>[
        const Cause.failure('invalid'),
        const Cause.defect('unexpected', StackTrace.empty),
        const Cause.cancelled(),
      ];
      final group = Cause<String>.combine(individual);
      for (final cause in [...individual, group]) {
        test('returns a single ${cause.runtimeType} unchanged', () {
          expect(Cause<String>.combine([cause]), same(cause));
        });
      }

      test('flattens groups in order and preserves duplicate references', () {
        final failure = Cause<String>.failure('invalid');
        final defect = Cause<Never>.defect(Object(), StackTrace.current);
        final cancelled = Cause<Never>.cancelled(reason: Object());
        final first = Cause<String>.combine([failure, defect]);
        final second = Cause<String>.combine([defect, first]);
        final input = <Cause<String>>[first, cancelled, second];
        final cause = Cause<String>.combine(input) as CauseGroup<String>;

        expect(cause.causes, [
          failure,
          defect,
          cancelled,
          defect,
          failure,
          defect,
        ]);
        expect(cause.causes.whereType<CauseGroup<String>>(), isEmpty);
        input.clear();
        expect(cause.causes, hasLength(6));
        expect(() => cause.causes.clear(), throwsUnsupportedError);
        expect(() => cause.causes[0] = cancelled, throwsUnsupportedError);
      });

      test('combines widened groups with new error types', () {
        final original = Cause<int>.combine([
          const Cause.failure(1),
          const Cause.failure(2),
        ]);
        final Cause<num> widened = original;
        final cause = Cause<num>.combine([
          widened,
          const Cause.failure(3.5),
        ]) as CauseGroup<num>;

        expect(
          cause.causes.map((cause) => (cause as CauseFailure<num>).error),
          [1, 2, 3.5],
        );
        expect((original as CauseGroup<int>).causes, hasLength(2));
      });

      test('consumes a lazy input once', () {
        var iterations = 0;
        Iterable<Cause<int>> input() sync* {
          iterations++;
          yield const Cause.failure(1);
          yield Cause.combine([const Cause.failure(2), const Cause.failure(3)]);
        }

        final cause = Cause<int>.combine(input()) as CauseGroup<int>;

        expect(iterations, 1);
        expect(
          cause.causes.map((cause) => (cause as CauseFailure<int>).error),
          [1, 2, 3],
        );
      });
    });
  });
}

String _describe<E>(Cause<E> cause) => switch (cause) {
  CauseFailure(:final error) => 'Failed: $error',
  CauseDefect(:final error) => 'Unexpected: $error',
  CauseCancelled(:final reason) => 'Cancelled: $reason',
  CauseGroup(:final causes) => '${causes.length} failure reasons.',
};
