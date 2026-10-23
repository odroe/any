import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  late MetricRegistry registry;
  late Runtime runtime;

  Runtime withRegistry(MetricRegistry value) {
    final runtime = Runtime(
      context: Context.from([MetricRegistry.key.bind(value)]),
    );
    addTearDown(() async {
      expect(await runtime.close(), isA<ExitSuccess<void, Never>>());
    });
    return runtime;
  }

  setUp(() {
    registry = MetricRegistry();
    runtime = withRegistry(registry);
  });

  test('a default runtime provides a no-op registry', () async {
    final defaultRuntime = Runtime();
    addTearDown(defaultRuntime.close);
    await defaultRuntime.runOrThrow(Counter('requests').add(1));
    final metrics = await defaultRuntime.runOrThrow(
      Task.service(MetricRegistry.key),
    );
    expect(metrics.snapshot().counters, isEmpty);
    expect(metrics.snapshot().droppedMeasurements, 0);
  });

  test('measurements are lazy and repeat on each execution', () async {
    final counter = Counter(
      'requests',
      unit: 'requests',
      description: 'Total requests',
    );
    final measure = counter.add(2);
    expect(registry.snapshot().counters, isEmpty);

    await runtime.runOrThrow(measure);
    expect(registry.snapshot().counters.single.value, 2);
    await runtime.runOrThrow(measure);
    final series = registry.snapshot().counters.single;
    expect(series.value, 4);
    expect(series.metric, same(counter));
    expect(series.metric.unit, 'requests');
    expect(series.metric.description, 'Total requests');
  });

  test('each execution uses the current registry service', () async {
    final other = MetricRegistry();
    final task = Counter('requests').add(1);

    await runtime.runOrThrow(task);
    await runtime.runOrThrow(
      task.provideContext(Context.from([MetricRegistry.key.bind(other)])),
    );
    await runtime.runOrThrow(task);

    expect(registry.snapshot().counters.single.value, 2);
    expect(other.snapshot().counters.single.value, 1);
  });

  test('counter adds while gauge retains only its latest value', () async {
    final counter = Counter('bytes');
    final gauge = Gauge('temperature');
    await runtime.runOrThrow(counter.add(1));
    await runtime.runOrThrow(counter.add(0.5));
    await runtime.runOrThrow(gauge.set(4));
    await runtime.runOrThrow(gauge.set(-2.5));

    expect(registry.snapshot().counters.single.value, 1.5);
    expect(registry.snapshot().gauges.single.value, -2.5);
  });

  test(
    'histogram uses inclusive upper bounds and a final overflow bucket',
    () async {
      final histogram = Histogram('sample', boundaries: [-1, 0, 10]);
      for (final value in [-2, -1, 0, 1, 10, 11]) {
        await runtime.runOrThrow(histogram.record(value));
      }

      final series = registry.snapshot().histograms.single;
      expect(series.buckets, [2, 1, 2, 1]);
      expect(series.count, 6);
      expect(series.sum, 19);
      expect(series.metric.boundaries, [-1.0, 0.0, 10.0]);
    },
  );

  test(
    'empty histogram boundaries use one bucket for all finite values',
    () async {
      final histogram = Histogram('sample', boundaries: []);
      for (final value in [-2, 0, 3]) {
        await runtime.runOrThrow(histogram.record(value));
      }

      final series = registry.snapshot().histograms.single;
      expect(series.buckets, [3]);
      expect(series.count, 3);
      expect(series.sum, 1);
    },
  );

  test(
    'attribute order does not change identity and caller mutation is isolated',
    () async {
      final attributes = {'z': 'last', 'a': 'first'};
      final counter = Counter('requests');
      final measure = counter.add(1, attributes: attributes);
      attributes['a'] = 'changed';

      await runtime.runOrThrow(measure);
      await runtime.runOrThrow(
        counter.add(2, attributes: {'a': 'first', 'z': 'last'}),
      );

      final series = registry.snapshot().counters.single;
      expect(series.value, 3);
      expect(series.attributes, {'a': 'first', 'z': 'last'});
      expect(series.attributes.keys, ['a', 'z']);
      expect(() => series.attributes['a'] = 'wrong', throwsUnsupportedError);
    },
  );

  test(
    'attribute names and values are not concatenated into ambiguous keys',
    () async {
      final counter = Counter('requests');
      await runtime.runOrThrow(counter.add(1, attributes: {'a': 'b=c'}));
      await runtime.runOrThrow(counter.add(2, attributes: {'a=b': 'c'}));

      expect(registry.snapshot().counters.map((series) => series.value), [
        1,
        2,
      ]);
    },
  );

  test(
    'one global capacity covers all kinds and retains existing series',
    () async {
      final limited = MetricRegistry(maxSeries: 2);
      final runner = withRegistry(limited);
      final counter = Counter('requests');
      await runner.runOrThrow(counter.add(1, attributes: {'route': '/'}));
      await runner.runOrThrow(Gauge('active').set(2));
      await runner.runOrThrow(Histogram('latency', boundaries: [1]).record(1));
      await runner.runOrThrow(counter.add(2, attributes: {'route': '/new'}));
      await runner.runOrThrow(Gauge('other').set(3));
      await runner.runOrThrow(counter.add(4, attributes: {'route': '/'}));
      await runner.runOrThrow(Gauge('active').set(5));

      final snapshot = limited.snapshot();
      expect(snapshot.counters.single.value, 5);
      expect(snapshot.gauges.single.value, 5);
      expect(snapshot.histograms, isEmpty);
      expect(snapshot.droppedMeasurements, 3);
    },
  );

  test('equivalent descriptors reuse a metric name', () async {
    await runtime.runOrThrow(
      Counter('total', unit: 'item', description: 'Items').add(1),
    );
    await runtime.runOrThrow(
      Counter('total', unit: 'item', description: 'Items').add(2),
    );
    await runtime.runOrThrow(Histogram('size', boundaries: [1, 2]).record(1));
    await runtime.runOrThrow(
      Histogram('size', boundaries: [1.0, 2.0]).record(2),
    );

    expect(registry.snapshot().counters.single.value, 3);
    expect(registry.snapshot().histograms.single.buckets, [1, 1, 0]);
  });

  test(
    'conflicting kinds or metadata fail without changing existing data',
    () async {
      await runtime.runOrThrow(Counter('total').add(7));
      await runtime.runOrThrow(Histogram('size', boundaries: [1, 2]).record(1));
      final conflicts = [
        Gauge('total').set(1),
        Counter('total', unit: '').add(1),
        Counter('total', description: '').add(1),
        Histogram('size', boundaries: [1]).record(1),
        Histogram('size', boundaries: [1, 3]).record(1),
      ];

      for (final task in conflicts) {
        expect(_cause(await runtime.run(task)), _stateDefect);
      }
      expect(registry.snapshot().counters.single.value, 7);
      expect(registry.snapshot().histograms.single.count, 1);
      expect(registry.snapshot().gauges, isEmpty);
    },
  );

  test(
    'full registries still reject conflicts without retaining rejected names',
    () async {
      final limited = MetricRegistry(maxSeries: 1);
      final runner = withRegistry(limited);
      await runner.runOrThrow(Counter('accepted').add(1));

      expect(_cause(await runner.run(Gauge('accepted').set(2))), _stateDefect);
      await runner.runOrThrow(Counter('rejected').add(1));
      await runner.runOrThrow(Gauge('rejected').set(2));

      final snapshot = limited.snapshot();
      expect(snapshot.counters.single.value, 1);
      expect(snapshot.gauges, isEmpty);
      expect(snapshot.droppedMeasurements, 2);
    },
  );

  test('descriptors and registry capacity validate their fixed parameters', () {
    expect(() => Counter(''), throwsArgumentError);
    expect(() => Gauge(''), throwsArgumentError);
    expect(() => Histogram('', boundaries: []), throwsArgumentError);
    for (final limit in [0, -1]) {
      expect(() => MetricRegistry(maxSeries: limit), throwsRangeError);
    }
    for (final boundaries in [
      [1, 1],
      [2, 1],
      [double.nan],
      [double.infinity],
      [double.negativeInfinity],
    ]) {
      expect(
        () => Histogram('invalid', boundaries: boundaries),
        throwsArgumentError,
      );
    }
    final boundaries = [1, 2];
    final histogram = Histogram('valid', boundaries: boundaries);
    boundaries.clear();
    expect(histogram.boundaries, [1.0, 2.0]);
    expect(() => histogram.boundaries.clear(), throwsUnsupportedError);
  });

  test('invalid measurements fail lazily and register no series', () async {
    final counter = Counter('counter');
    final gauge = Gauge('gauge');
    final histogram = Histogram('histogram', boundaries: []);
    final tasks = [counter.add(-1)];
    for (final value in [
      double.nan,
      double.infinity,
      double.negativeInfinity,
    ]) {
      tasks.addAll([
        counter.add(value),
        gauge.set(value),
        histogram.record(value),
      ]);
    }

    for (final task in tasks) {
      expect(_cause(await runtime.run(task)), _argumentDefect);
    }
    final snapshot = registry.snapshot();
    expect(snapshot.counters, isEmpty);
    expect(snapshot.gauges, isEmpty);
    expect(snapshot.histograms, isEmpty);
    expect(snapshot.droppedMeasurements, 0);
  });

  test('counter overflow retains its previous total', () async {
    final counter = Counter('total');
    const large = 1.7976931348623157e308;
    await runtime.runOrThrow(counter.add(large));

    expect(_cause(await runtime.run(counter.add(large))), _stateDefect);
    expect(registry.snapshot().counters.single.value, large);
  });

  test('histogram sum overflow changes neither count nor buckets', () async {
    final histogram = Histogram('sum', boundaries: [0]);
    const large = -1.7976931348623157e308;
    await runtime.runOrThrow(histogram.record(large));

    expect(_cause(await runtime.run(histogram.record(large))), _stateDefect);
    final series = registry.snapshot().histograms.single;
    expect(series.sum, large);
    expect(series.count, 1);
    expect(series.buckets, [1, 0]);
  });

  test(
    'no-op registry validates values without retaining metadata or series',
    () async {
      final noop = MetricRegistry.noop();
      final runner = withRegistry(noop);
      await runner.runOrThrow(Counter('same').add(1));
      await runner.runOrThrow(Gauge('same').set(2));
      await runner.runOrThrow(Histogram('same', boundaries: [1]).record(1));
      expect(_cause(await runner.run(Counter('bad').add(-1))), _argumentDefect);
      expect(
        _cause(await runner.run(Gauge('bad').set(double.infinity))),
        _argumentDefect,
      );
      expect(
        _cause(
          await runner.run(Histogram('bad', boundaries: []).record(double.nan)),
        ),
        _argumentDefect,
      );

      final snapshot = noop.snapshot();
      expect(snapshot.counters, isEmpty);
      expect(snapshot.gauges, isEmpty);
      expect(snapshot.histograms, isEmpty);
      expect(snapshot.droppedMeasurements, 0);
    },
  );

  test(
    'snapshots are immutable and detached from later observations',
    () async {
      final counter = Counter('total');
      final gauge = Gauge('active');
      final histogram = Histogram('size', boundaries: [1]);
      await runtime.runOrThrow(counter.add(1));
      await runtime.runOrThrow(gauge.set(2));
      await runtime.runOrThrow(histogram.record(1));
      final snapshot = registry.snapshot();

      await runtime.runOrThrow(counter.add(3));
      await runtime.runOrThrow(gauge.set(4));
      await runtime.runOrThrow(histogram.record(2));

      expect(snapshot.counters.single.value, 1);
      expect(snapshot.gauges.single.value, 2);
      expect(snapshot.histograms.single.buckets, [1, 0]);
      expect(snapshot.histograms.single.count, 1);
      expect(snapshot.histograms.single.sum, 1);
      expect(() => snapshot.counters.clear(), throwsUnsupportedError);
      expect(() => snapshot.gauges.clear(), throwsUnsupportedError);
      expect(() => snapshot.histograms.clear(), throwsUnsupportedError);
      expect(
        () => snapshot.histograms.single.buckets.clear(),
        throwsUnsupportedError,
      );
    },
  );

  test('pre-cancellation prevents a measurement from being recorded', () async {
    final controller = CancellationController()..requestCancel();
    final exit = await runtime.run(
      Counter('total').add(1),
      signal: controller.signal,
    );

    expect(_cause(exit), isA<CauseCancelled>());
    expect(registry.snapshot().counters, isEmpty);
  });
}

Matcher get _argumentDefect => isA<CauseDefect>().having(
  (cause) => cause.error,
  'error',
  isA<ArgumentError>(),
);

Matcher get _stateDefect => isA<CauseDefect>().having(
  (cause) => cause.error,
  'error',
  isA<StateError>(),
);

Cause<E> _cause<T, E>(Exit<T, E> exit) => switch (exit) {
  ExitSuccess(:final value) => fail('Expected failure, got $value.'),
  ExitFailure(:final cause) => cause,
};
