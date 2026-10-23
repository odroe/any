import 'package:any/any.dart';
import 'package:any/src/core/schedule.dart';
import 'package:test/test.dart';

void main() {
  test('recurs counts only executions after the initial attempt', () {
    final schedule = Schedule.recurs(3);

    expect(_delays(schedule, 5), [
      Duration.zero,
      Duration.zero,
      Duration.zero,
      null,
      null,
    ]);
    expect(_next(Schedule.recurs(0), 0), isNull);
  });

  test('spaced uses a constant post-attempt delay without a count limit', () {
    final schedule = Schedule.spaced(const Duration(milliseconds: 7));

    expect(_next(schedule, 0), const Duration(milliseconds: 7));
    expect(_next(schedule, 1000), const Duration(milliseconds: 7));
    expect(_next(Schedule.spaced(Duration.zero), 1000), Duration.zero);
  });

  test('exponential supports growth, constant spacing, and decay', () {
    const base = Duration(microseconds: 100);

    expect(
      _delays(Schedule.exponential(base), 4),
      _micros([100, 200, 400, 800]),
    );
    expect(
      _delays(Schedule.exponential(base, factor: 1), 4),
      _micros([100, 100, 100, 100]),
    );
    expect(
      _delays(Schedule.exponential(base, factor: 0.5), 4),
      _micros([100, 50, 25, 13]),
    );
  });

  test(
    'invalid counts, durations, and factors are rejected at construction',
    () {
      const negative = Duration(microseconds: -1);
      final valid = Schedule.recurs(1);
      final invalid = <void Function()>[
        () => Schedule.recurs(-1),
        () => Schedule.spaced(negative),
        () => Schedule.exponential(negative),
        () => valid.limit(-1),
        () => valid.cap(negative),
        () => valid.whileElapsed(negative),
        for (final factor in [0.0, -1.0, double.nan, double.infinity])
          () => Schedule.exponential(Duration.zero, factor: factor),
      ];

      for (final create in invalid) {
        expect(create, throwsRangeError);
      }
    },
  );

  test('and uses the later delay and stops when either side stops', () {
    final first = Schedule.spaced(const Duration(seconds: 5)).limit(2);
    final second = Schedule.spaced(const Duration(seconds: 10)).limit(3);

    expect(_delays(first.and(second), 4), [
      const Duration(seconds: 10),
      const Duration(seconds: 10),
      null,
      null,
    ]);
    expect(_delays(second.and(first), 4), _delays(first.and(second), 4));
  });

  test(
    'or uses the earlier active delay and stops only when both sides stop',
    () {
      final first = Schedule.spaced(const Duration(seconds: 5)).limit(2);
      final second = Schedule.spaced(const Duration(seconds: 10)).limit(3);

      expect(_delays(first.or(second), 4), [
        const Duration(seconds: 5),
        const Duration(seconds: 5),
        const Duration(seconds: 10),
        null,
      ]);
      expect(_delays(second.or(first), 4), _delays(first.or(second), 4));
    },
  );

  test(
    'a limit stops before evaluating jitter and cannot extend an earlier stop',
    () {
      final random = _Random([0.5]);
      final schedule = Schedule.spaced(const Duration(seconds: 1))
          .jitter()
          .limit(1);

      expect(_next(schedule, 1, random: random), isNull);
      expect(random.draws, 0);
      expect(
        _next(schedule, 0, random: random),
        const Duration(milliseconds: 500),
      );
      expect(random.draws, 1);
      expect(_delays(Schedule.recurs(1).limit(3), 3), [
        Duration.zero,
        null,
        null,
      ]);
    },
  );

  test('cap bounds delays without extending a finite policy', () {
    final schedule = Schedule.exponential(const Duration(microseconds: 100))
        .limit(4)
        .cap(const Duration(microseconds: 250));

    expect(_delays(schedule, 5), [
      ..._micros([100, 200, 250, 250]),
      null,
    ]);
    expect(_next(schedule.cap(Duration.zero), 3), Duration.zero);
    expect(_next(schedule.cap(Duration.zero), 4), isNull);
  });

  test(
    'combinator order is observable and the underlying policy stays unchanged',
    () {
      final base = Schedule.spaced(const Duration(microseconds: 600));
      final capLast = base
          .jitter(min: 2, max: 2)
          .cap(const Duration(microseconds: 1000));
      final jitterLast = base
          .cap(const Duration(microseconds: 1000))
          .jitter(min: 2, max: 2);

      expect(_next(capLast, 0), const Duration(microseconds: 1000));
      expect(_next(jitterLast, 0), const Duration(microseconds: 1200));
      expect(_next(base, 0), const Duration(microseconds: 600));
      expect(_delays(base.limit(1).or(Schedule.recurs(3)), 4), [
        Duration.zero,
        Duration.zero,
        Duration.zero,
        null,
      ]);
      expect(_delays(base.or(Schedule.recurs(3)).limit(1), 4), [
        Duration.zero,
        null,
        null,
        null,
      ]);
    },
  );

  test(
    'whileElapsed checks the decision time without truncating its delay',
    () {
      final schedule = Schedule.spaced(const Duration(seconds: 10))
          .whileElapsed(const Duration(seconds: 1));

      expect(_next(schedule, 0), const Duration(seconds: 10));
      expect(
        _next(schedule, 1, elapsed: const Duration(milliseconds: 999)),
        const Duration(seconds: 10),
      );
      expect(_next(schedule, 1, elapsed: const Duration(seconds: 1)), isNull);
      expect(_next(schedule, 1, elapsed: const Duration(seconds: 2)), isNull);
      expect(_next(schedule.whileElapsed(Duration.zero), 0), isNull);
    },
  );

  test('an elapsed limit stops before drawing randomness', () {
    final random = _Random([]);
    final schedule = Schedule.spaced(const Duration(seconds: 1))
        .jitter()
        .whileElapsed(const Duration(seconds: 1));

    expect(
      _next(schedule, 0, elapsed: const Duration(seconds: 1), random: random),
      isNull,
    );
    expect(random.draws, 0);
  });

  test('default jitter includes zero and rounds only the final delay', () {
    final schedule = Schedule.spaced(const Duration(microseconds: 1000))
        .jitter();
    final random = _Random([0, 0.5, 0.9999]);

    expect(_next(schedule, 0, random: random), Duration.zero);
    expect(
      _next(schedule, 1, random: random),
      const Duration(microseconds: 500),
    );
    expect(
      _next(schedule, 2, random: random),
      const Duration(microseconds: 1000),
    );
    expect(random.draws, 3);
  });

  test('jitter bounds are multipliers and equal bounds consume no draw', () {
    final random = _Random([0, 0.5]);
    final base = Schedule.spaced(const Duration(microseconds: 100));
    final schedule = base.jitter(min: 2, max: 4);

    expect(
      _next(schedule, 0, random: random),
      const Duration(microseconds: 200),
    );
    expect(
      _next(schedule, 1, random: random),
      const Duration(microseconds: 300),
    );
    expect(
      _next(base.jitter(min: 2, max: 2), 0, random: random),
      const Duration(microseconds: 200),
    );
    expect(
      _next(base.jitter(min: 0, max: 0), 0, random: random),
      Duration.zero,
    );
    expect(random.draws, 2);
  });

  test('stopped and zero-delay policies do not draw randomness', () {
    final random = _Random([]);

    expect(_next(Schedule.recurs(0).jitter(), 0, random: random), isNull);
    expect(
      _next(Schedule.spaced(Duration.zero).jitter(), 0, random: random),
      Duration.zero,
    );
    expect(random.draws, 0);
  });

  test('invalid jitter bounds and random-source results are rejected', () {
    final base = Schedule.spaced(const Duration(seconds: 1));
    for (final (min, max) in [
      (-1.0, 1.0),
      (2.0, 1.0),
      (double.nan, 1.0),
      (0.0, double.nan),
      (0.0, double.infinity),
      (double.infinity, double.infinity),
    ]) {
      expect(() => base.jitter(min: min, max: max), throwsRangeError);
    }
    for (final draw in [-0.1, 1.0, double.nan, double.infinity]) {
      expect(
        () => _next(base.jitter(), 0, random: _Random([draw])),
        throwsStateError,
      );
    }
  });

  test('and and or evaluate random policies in documented order', () {
    final jitter = Schedule.spaced(const Duration(microseconds: 100)).jitter();

    expect(
      _next(jitter.and(jitter), 0, random: _Random([0.25, 0.75])),
      const Duration(microseconds: 75),
    );
    expect(
      _next(jitter.or(jitter), 0, random: _Random([0.25, 0.75])),
      const Duration(microseconds: 25),
    );
    final random = _Random([]);
    expect(_next(Schedule.recurs(0).and(jitter), 0, random: random), isNull);
    expect(random.draws, 0);
  });

  test('the same schedule can be reused without retaining iteration or elapsed state', () {
    final schedule = Schedule.exponential(const Duration(microseconds: 10))
        .limit(3)
        .whileElapsed(const Duration(seconds: 1))
        .jitter();
    final first = _Random([0.5, 0.25]);
    final second = _Random([0.5, 0.25]);

    expect(_next(schedule, 3, random: first), isNull);
    expect(
      _next(schedule, 0, elapsed: const Duration(seconds: 1), random: first),
      isNull,
    );
    expect(_next(schedule, 0, random: first), const Duration(microseconds: 5));
    expect(_next(schedule, 0, random: second), const Duration(microseconds: 5));
    expect(_next(schedule, 2, random: first), const Duration(microseconds: 10));
    expect(
      _next(schedule, 2, random: second),
      const Duration(microseconds: 10),
    );
    expect(first.draws, 2);
    expect(second.draws, 2);
  });

  test('fractional results are rounded once after all combinators', () {
    final schedule = Schedule.spaced(const Duration(microseconds: 1))
        .jitter(min: 1.4, max: 1.4)
        .jitter(min: 1.4, max: 1.4);

    expect(_next(schedule, 0), const Duration(microseconds: 2));
    expect(
      _delays(
        Schedule.exponential(const Duration(microseconds: 1), factor: 1.5),
        4,
      ),
      _micros([1, 2, 2, 3]),
    );
  });

  test('final delay conversion rejects overflow without integer wrapping', () {
    const maximum = 9007199254740991;
    final exponential = Schedule.exponential(const Duration(microseconds: 1));

    expect(
      _next(Schedule.spaced(const Duration(microseconds: maximum)), 0),
      const Duration(microseconds: maximum),
    );
    expect(
      () =>
          _next(Schedule.spaced(const Duration(microseconds: maximum + 1)), 0),
      throwsRangeError,
    );
    expect(
      _next(exponential, 52),
      const Duration(microseconds: 4503599627370496),
    );
    expect(() => _next(exponential, 53), throwsRangeError);
    expect(() => _next(exponential, 10000), throwsRangeError);
  });

  test('cap bounds huge intermediate exponential values before conversion', () {
    final exponential = Schedule.exponential(const Duration(microseconds: 1));
    final capped = exponential.cap(const Duration(seconds: 30));

    expect(_next(capped, 10000), const Duration(seconds: 30));
    expect(_next(exponential.and(Schedule.recurs(0)), 10000), isNull);
    expect(_next(exponential.jitter(min: 0, max: 0), 10000), Duration.zero);
    expect(_next(Schedule.exponential(Duration.zero), 10000), Duration.zero);
  });

  test('driver inputs reject negative iteration and elapsed time', () {
    final schedule = Schedule.recurs(1);

    expect(() => _next(schedule, -1), throwsRangeError);
    expect(
      () => _next(schedule, 0, elapsed: const Duration(microseconds: -1)),
      throwsRangeError,
    );
  });
}

Duration? _next(
  Schedule schedule,
  int iteration, {
  Duration elapsed = Duration.zero,
  RandomSource? random,
}) => scheduleDelay(
  schedule,
  iteration: iteration,
  elapsed: elapsed,
  random: random ?? _Random([]),
);

List<Duration?> _delays(Schedule schedule, int count) => [
  for (var iteration = 0; iteration < count; iteration++)
    _next(schedule, iteration),
];

List<Duration> _micros(List<int> values) => [
  for (final value in values) Duration(microseconds: value),
];

final class _Random implements RandomSource {
  _Random(this._values);

  final List<double> _values;
  var draws = 0;

  @override
  double nextDouble() => _values[draws++];

  @override
  int nextInt(int max) =>
      throw UnsupportedError('Only double draws are expected.');

  @override
  bool nextBool() => throw UnsupportedError('Only double draws are expected.');
}
