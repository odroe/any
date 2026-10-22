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

    group('mapError', () {
      test('maps an expected Error object without reclassifying it', () {
        final error = StateError('expected');
        final original = Cause<StateError>.failure(error);
        var calls = 0;

        final mapped = original.mapError((value) {
          expect(value, same(error));
          calls++;
          return (operation: 'load', error: value);
        });

        final failure =
            mapped as CauseFailure<({String operation, StateError error})>;
        expect(failure.error.operation, 'load');
        expect(failure.error.error, same(error));
        expect(calls, 1);
        expect((original as CauseFailure<StateError>).error, same(error));
      });

      test('supports nullable input and output errors', () {
        const original = Cause<String?>.failure(null);
        var calls = 0;

        final mapped = original.mapError<int?>((error) {
          expect(error, isNull);
          calls++;
          return null;
        });

        expect((mapped as CauseFailure<int?>).error, isNull);
        expect(calls, 1);
      });

      test('retains a defect and its trace across unrelated error types', () {
        final error = StateError('unexpected');
        final trace = StackTrace.fromString('original trace');
        final original = Cause<String>.defect(error, trace);
        final mapped = original.mapError<int>(
          (_) => fail('Unexpected mapping'),
        );

        final defect = mapped as CauseDefect<int>;
        expect(defect.error, same(error));
        expect(defect.stackTrace, same(trace));
      });

      for (final reason in [null, Object()]) {
        test('retains a cancellation reason (null: ${reason == null})', () {
          final original = Cause<int>.cancelled(reason: reason);
          final mapped = original.mapError<String>(
            (_) => fail('Unexpected mapping'),
          );

          expect((mapped as CauseCancelled<String>).reason, same(reason));
        });
      }

      test(
        'maps every failure in order while preserving flat group contents',
        () {
          const failure = Cause<int>.failure(2);
          final error = StateError('unexpected');
          final trace = StackTrace.fromString('original trace');
          final reason = Object();
          final original = Cause<int>.combine([
            failure,
            Cause.defect(error, trace),
            Cause.combine([failure, Cause.cancelled(reason: reason)]),
            const Cause.failure(1),
          ]) as CauseGroup<int>;
          final seen = <int>[];

          final mapped = original.mapError((value) {
            seen.add(value);
            return (value: value, index: seen.length);
          }) as CauseGroup<({int value, int index})>;

          final causes = mapped.causes;
          expect(seen, [2, 2, 1]);
          expect(causes, hasLength(5));
          expect((causes[0] as CauseFailure<({int value, int index})>).error, (
            value: 2,
            index: 1,
          ));
          final defect = causes[1] as CauseDefect<({int value, int index})>;
          expect(defect.error, same(error));
          expect(defect.stackTrace, same(trace));
          expect((causes[2] as CauseFailure<({int value, int index})>).error, (
            value: 2,
            index: 2,
          ));
          expect(
            (causes[3] as CauseCancelled<({int value, int index})>).reason,
            same(reason),
          );
          expect((causes[4] as CauseFailure<({int value, int index})>).error, (
            value: 1,
            index: 3,
          ));
          expect(
            causes.whereType<CauseGroup<({int value, int index})>>(),
            isEmpty,
          );
          expect(() => causes.clear(), throwsUnsupportedError);
          expect(original.causes[0], same(failure));
          expect(original.causes[2], same(failure));
        },
      );

      test('maps a group with error type Never without calling the mapper', () {
        final error = Object();
        final trace = StackTrace.fromString('original trace');
        final reason = Object();
        final original = Cause<Never>.combine([
          Cause.defect(error, trace),
          Cause.cancelled(reason: reason),
        ]);

        final mapped = original.mapError<String>(
          (_) => fail('Unexpected mapping'),
        ) as CauseGroup<String>;

        expect(mapped.causes, hasLength(2));
        final defect = mapped.causes[0] as CauseDefect<String>;
        expect(defect.error, same(error));
        expect(defect.stackTrace, same(trace));
        expect(
          (mapped.causes[1] as CauseCancelled<String>).reason,
          same(reason),
        );
      });

      test('supports widened causes and a different output error type', () {
        final Cause<num> widened = Cause<int>.combine([
          const Cause.failure(1),
          const Cause.failure(3),
        ]);

        final mapped =
            widened.mapError((num value) => value / 2) as CauseGroup<double>;

        expect(
          mapped.causes.map((cause) => (cause as CauseFailure<double>).error),
          [0.5, 1.5],
        );
      });

      test('retains a future returned by the mapper as the error value', () {
        final value = Future<int>.value(42);
        final mapped = const Cause<String>.failure('missing')
            .mapError((_) => value);

        expect((mapped as CauseFailure<Future<int>>).error, same(value));
      });

      for (final grouped in [false, true]) {
        test('propagates mapper throws immediately (grouped: $grouped)', () {
          const failure = Cause<int>.failure(1);
          final original = grouped
              ? Cause<int>.combine([failure, const Cause.failure(2)])
              : failure;
          final error = Object();
          final trace = StackTrace.fromString('mapper trace');
          final seen = <int>[];

          try {
            original.mapError<String>((value) {
              seen.add(value);
              Error.throwWithStackTrace(error, trace);
            });
            fail('Expected the mapper to throw.');
          } catch (caught, stackTrace) {
            expect(caught, same(error));
            expect(stackTrace, same(trace));
          }

          expect(seen, [1]);
          expect((failure as CauseFailure<int>).error, 1);
        });
      }
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
