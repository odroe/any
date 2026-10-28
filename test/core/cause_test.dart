import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  group('Cause', () {
    test('const factories support exhaustive matching', () {
      const failure = Cause<String>.failure('unavailable');
      const defect = Cause<String>.defect('unexpected', StackTrace.empty);
      const cancelled = Cause<String>.cancelled('requested');

      expect(_kind(failure), 'failure');
      expect(_kind(defect), 'defect');
      expect(_kind(cancelled), 'cancelled');
      expect(_kind(Cause.combine([failure, defect])), 'group');
      expect(failure, same(const Cause<String>.failure('unavailable')));
    });

    test('preserves nullable business errors and cancellation reasons', () {
      const failure = Cause<String?>.failure(null);
      const cancelled = Cause<Never>.cancelled();

      expect((failure as CauseFailure<String?>).error, isNull);
      expect((cancelled as CauseCancelled).reason, isNull);
    });

    test(
      'preserves defect objects, stack traces, and cancellation information',
      () {
        final error = StateError('unexpected');
        final stack = StackTrace.current;
        final reason = Object();
        final defect = Cause<String>.defect(error, stack) as CauseDefect;
        final cancelled = Cause<String>.cancelled(reason) as CauseCancelled;

        expect(defect.error, same(error));
        expect(defect.stackTrace, same(stack));
        expect(cancelled.reason, same(reason));
      },
    );
  });

  group('Cause.toString', () {
    test('distinguishes failure and cancellation, including null values', () {
      expect(const Cause.failure('rejected').toString(), 'Failure: rejected');
      expect(const Cause.failure(null).toString(), 'Failure: null');
      expect(const Cause.cancelled().toString(), 'Cancelled');
      expect(
        const Cause.cancelled('shutdown').toString(),
        'Cancelled: shutdown',
      );
    });

    test('keeps multiline messages and stacks under their own reason', () {
      final cause = Cause<String>.combine([
        const Cause.failure('Save rejected\nPlease retry'),
        Cause.defect(
          StateError('Close failed\nConnection unavailable'),
          StackTrace.fromString(
            'close (connection.dart:10)\nmain (app.dart:5)\n',
          ),
        ),
        const Cause.cancelled('Shutdown\nRequested by user'),
      ]);

      expect(
        cause.toString(),
        'Failure: Save rejected\n'
        '  Please retry\n'
        'Defect: Bad state: Close failed\n'
        '  Connection unavailable\n'
        '  close (connection.dart:10)\n'
        '  main (app.dart:5)\n'
        'Cancelled: Shutdown\n'
        '  Requested by user',
      );
    });

    test('preserves order, repeated errors, and each stored defect stack', () {
      final error = StateError('same error');
      final first = StackTrace.fromString('first stack');
      final second = StackTrace.fromString('second stack');
      final failure = Cause.failure(error);
      final cause = Cause.combine([
        failure,
        Cause.defect(error, first),
        Cause.cancelled('stop'),
        failure,
        Cause.defect(error, second),
      ]);

      expect(
        cause.toString(),
        'Failure: Bad state: same error\n'
        'Defect: Bad state: same error\n'
        '  first stack\n'
        'Cancelled: stop\n'
        'Failure: Bad state: same error\n'
        'Defect: Bad state: same error\n'
        '  second stack',
      );
    });

    test('does not add stack text when the stored trace is empty', () {
      expect(
        const Cause.defect('unexpected', StackTrace.empty).toString(),
        'Defect: unexpected',
      );
    });

    test('formats stored diagnostic values without revealing wrapped data', () {
      final hidden = Redacted((
        error: StateError('secret error'),
        stackTrace: StackTrace.fromString('secret stack'),
      ));
      final cause = Cause<Redacted<String>>.combine([
        const Cause.failure(Redacted('secret business error')),
        Cause.defect(hidden, StackTrace.fromString('safe diagnostic stack')),
        const Cause.cancelled(Redacted('secret cancellation')),
      ]);

      expect(
        cause.toString(),
        'Failure: Redacted(<redacted>)\n'
        'Defect: Redacted(<redacted>)\n'
        '  safe diagnostic stack\n'
        'Cancelled: Redacted(<redacted>)',
      );
    });
  });

  group('Cause.combine', () {
    test('rejects empty input', () {
      expect(() => Cause<String>.combine([]), throwsArgumentError);
    });

    test('returns a single cause unchanged, including a group', () {
      const failure = Cause<String>.failure('failed');
      final group = Cause.combine([failure, failure]);

      expect(Cause.combine([failure]), same(failure));
      expect(Cause.combine([group]), same(group));
    });

    test('flattens groups in order and retains repeated reasons', () {
      const first = Cause<String>.failure('first');
      const second = Cause<String>.failure('second');
      const cancelled = Cause<String>.cancelled('cancelled');
      final inner = Cause.combine([first, second]);
      final outer = Cause.combine([inner, cancelled, inner]);
      final combined = Cause.combine([outer, first]) as CauseGroup<String>;

      expect(combined.causes, [first, second, cancelled, first, second, first]);
      expect(combined.causes.any((cause) => cause is CauseGroup), isFalse);
      expect(combined.causes[0], same(combined.causes[3]));
      expect(combined.causes[0], same(combined.causes[5]));
    });

    test('takes an immutable snapshot of input', () {
      const first = Cause<String>.failure('first');
      const second = Cause<String>.failure('second');
      final input = <Cause<String>>[first, second];
      final combined = Cause.combine(input) as CauseGroup<String>;
      input.clear();

      expect(combined.causes, [first, second]);
      expect(() => combined.causes.add(first), throwsUnsupportedError);
      expect(() => combined.causes[0] = second, throwsUnsupportedError);
    });

    test('consumes lazy input once', () {
      var iterations = 0;
      Iterable<Cause<int>> reasons() sync* {
        iterations++;
        yield const Cause.failure(1);
        yield const Cause.failure(2);
      }

      final combined = Cause.combine(reasons()) as CauseGroup<int>;

      expect(iterations, 1);
      expect(combined.causes.length, 2);
    });

    test(
      'combines and maps ten thousand reasons without recursive traversal',
      () {
        final groups = List.generate(
          100,
          (group) => Cause<int>.combine(
            List.generate(100, (index) => Cause.failure(group * 100 + index)),
          ),
        );
        final combined = Cause.combine(groups) as CauseGroup<int>;
        final mapped =
            combined.mapError((error) => error + 1) as CauseGroup<int>;

        expect(combined.causes.length, 10000);
        expect(mapped.causes.length, 10000);
        expect((mapped.causes.first as CauseFailure<int>).error, 1);
        expect((mapped.causes.last as CauseFailure<int>).error, 10000);
        expect(mapped.causes.any((cause) => cause is CauseGroup), isFalse);
      },
    );
  });

  group('Cause.mapError', () {
    test('maps business errors in order without changing other reasons', () {
      const first = Cause<int>.failure(1);
      const second = Cause<int>.failure(2);
      final defect = Cause<int>.defect(
        StateError('unexpected'),
        StackTrace.current,
      );
      final cancelled = Cause<int>.cancelled(Object());
      final original = Cause.combine([first, defect, second, cancelled, first]);
      final visited = <int>[];

      final mapped = original.mapError((error) {
        visited.add(error);
        return 'error $error';
      }) as CauseGroup<String>;

      expect(visited, [1, 2, 1]);
      expect((mapped.causes[0] as CauseFailure<String>).error, 'error 1');
      expect(mapped.causes[1], same(defect));
      expect((mapped.causes[2] as CauseFailure<String>).error, 'error 2');
      expect(mapped.causes[3], same(cancelled));
      expect((mapped.causes[4] as CauseFailure<String>).error, 'error 1');
      expect((first as CauseFailure<int>).error, 1);
    });

    test('returns defect and cancellation leaves unchanged', () {
      final defect = Cause<int>.defect(
        StateError('unexpected'),
        StackTrace.current,
      );
      final cancelled = Cause<int>.cancelled(Object());

      String transform(int error) => fail('Unexpected business error: $error');

      expect(defect.mapError(transform), same(defect));
      expect(cancelled.mapError(transform), same(cancelled));
    });

    test('transforms nullable errors', () {
      const original = Cause<String?>.failure(null);
      final mapped = original.mapError((error) => error ?? 'missing');

      expect((mapped as CauseFailure<String>).error, 'missing');
    });

    test('uses the statically widened business error type', () {
      const Cause<num> original = Cause<int>.failure(1);
      final mapped = original.mapError((num error) => error.toDouble() + 0.5);

      expect((mapped as CauseFailure<double>).error, 1.5);
    });

    test('propagates transform errors and stops before later reasons', () {
      final exception = StateError('mapping failed');
      final original = Cause<int>.combine([
        const Cause.failure(1),
        const Cause.failure(2),
        const Cause.failure(3),
      ]);
      final visited = <int>[];

      expect(
        () => original.mapError((error) {
          visited.add(error);
          if (error == 2) throw exception;
          return error.toString();
        }),
        throwsA(same(exception)),
      );
      expect(visited, [1, 2]);
    });
  });
}

String _kind<E>(Cause<E> cause) => switch (cause) {
  CauseFailure() => 'failure',
  CauseDefect() => 'defect',
  CauseCancelled() => 'cancelled',
  CauseGroup() => 'group',
};
