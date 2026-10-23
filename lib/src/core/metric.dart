import 'dart:collection';

import 'context.dart';
import 'task.dart';

/// An immutable metric descriptor identified by its [name].
///
/// A registry permits a name to be reused only with the same metric kind,
/// [unit], [description], and histogram boundaries. Metadata is compared
/// exactly; an absent unit and an empty unit are different.
/// Names must be nonempty; invalid names throw [ArgumentError]. Measurement
/// tasks use the current [MetricRegistry] and copy their attribute maps when
/// constructed.
sealed class Metric {
  Metric._(this.name, {this.unit, this.description}) {
    if (name.isEmpty) {
      throw ArgumentError.value(name, 'name', 'Must not be empty.');
    }
  }

  /// The nonempty name identifying this metric in a registry.
  final String name;

  /// The measurement unit, if specified.
  final String? unit;

  /// A description of what the metric measures.
  final String? description;

  Task<void, Never> _measure(num value, Map<String, String> attributes) {
    final labels = Map<String, String>.unmodifiable(
      SplayTreeMap<String, String>.of(attributes),
    );
    return Task.service(MetricRegistry.key).map((registry) {
      registry._record(this, value.toDouble(), labels);
    });
  }
}

/// A monotonically increasing total, separated by attribute values.
final class Counter extends Metric {
  /// Describes a counter without registering a measurement.
  Counter(super.name, {super.unit, super.description}) : super._();

  /// Adds a finite, nonnegative [value] when the returned task executes.
  ///
  /// Invalid values become [ArgumentError] defects, even with a no-op registry.
  /// An overflowing total produces a [StateError] defect without changing the
  /// previous total.
  Task<void, Never> add(
    num value, {
    Map<String, String> attributes = const {},
  }) => _measure(value, attributes);
}

/// The most recently recorded value for each set of attributes.
final class Gauge extends Metric {
  /// Describes a gauge without registering a measurement.
  Gauge(super.name, {super.unit, super.description}) : super._();

  /// Replaces the current value with finite [value] when executed.
  ///
  /// Negative values are allowed. Non-finite values become [ArgumentError]
  /// defects, even with a no-op registry.
  Task<void, Never> set(
    num value, {
    Map<String, String> attributes = const {},
  }) => _measure(value, attributes);
}

/// A distribution with fixed upper bucket boundaries.
///
/// Buckets are non-cumulative: the first contains values `<= boundaries[0]`,
/// each subsequent bounded bucket contains values greater than its previous
/// boundary and `<=` its own, and the final bucket contains values greater than
/// the last boundary. Empty boundaries create one bucket containing all values.
final class Histogram extends Metric {
  /// Copies strictly increasing, finite [boundaries] into this descriptor.
  ///
  /// Invalid boundaries throw [ArgumentError]. Negative boundaries are allowed.
  Histogram(
    super.name, {
    required Iterable<num> boundaries,
    super.unit,
    super.description,
  }) : boundaries = List.unmodifiable(
         boundaries.map((value) => value.toDouble()),
       ),
       super._() {
    for (var index = 0; index < this.boundaries.length; index++) {
      final boundary = this.boundaries[index];
      if (!boundary.isFinite ||
          (index > 0 && boundary <= this.boundaries[index - 1])) {
        throw ArgumentError.value(
          this.boundaries,
          'boundaries',
          'Must be finite and strictly increasing.',
        );
      }
    }
  }

  /// The immutable upper bounds, excluding the final unbounded bucket.
  final List<double> boundaries;

  /// Records finite [value] when the returned task executes.
  ///
  /// Negative values are allowed. Invalid values become [ArgumentError] defects,
  /// even with a no-op registry. An overflowing sum produces a [StateError]
  /// defect without changing the sum, count, or buckets.
  Task<void, Never> record(
    num value, {
    Map<String, String> attributes = const {},
  }) => _measure(value, attributes);
}

/// A bounded, in-memory collection of metric observations.
///
/// Bind a registry through [key] to enable [Counter], [Gauge], and [Histogram]
/// measurements. Read [snapshot] to inspect or export observations; the registry
/// performs no I/O.
///
/// A series is a metric name plus its attributes, regardless of map order.
/// At capacity, new series are discarded and counted in
/// [MetricSnapshot.droppedMeasurements]; existing series continue to update and
/// are never evicted. Only accepted series register metadata. Conflicting
/// descriptors produce a [StateError] defect even when the registry is full.
///
/// ```dart
/// final metrics = MetricRegistry();
/// final requests = Counter('http.requests', unit: 'requests');
/// final runtime = Runtime(
///   context: Context.from([MetricRegistry.key.bind(metrics)]),
/// );
/// try {
///   await runtime.runOrThrow(
///     requests.add(1, attributes: {'method': 'GET'}),
///   );
///   assert(metrics.snapshot().counters.single.value == 1);
/// } finally {
///   await runtime.close();
/// }
/// ```
final class MetricRegistry {
  /// Creates an in-memory registry with a positive total [maxSeries] limit.
  ///
  /// An invalid limit throws [RangeError]. The limit covers all metric kinds
  /// together, including series with no attributes.
  MetricRegistry({int maxSeries = 1024})
    : _maxSeries = maxSeries,
      _noop = false {
    if (maxSeries <= 0) {
      throw RangeError.range(maxSeries, 1, null, 'maxSeries');
    }
  }

  /// Creates a registry that validates values but retains no observations.
  ///
  /// No descriptors or series are registered, and snapshots remain empty.
  /// Suppressed measurements are not counted as capacity drops.
  MetricRegistry.noop() : _maxSeries = 0, _noop = true;

  /// The service key used by metric measurement tasks.
  static final key = ServiceKey<MetricRegistry>('metrics');

  final int _maxSeries;
  final bool _noop;
  final _metrics = <String, Metric>{};
  final _series = <_SeriesKey, _Series>{};
  var _droppedMeasurements = 0;

  void _record(Metric metric, double value, Map<String, String> attributes) {
    if (!value.isFinite || (metric is Counter && value < 0)) {
      throw ArgumentError.value(
        value,
        'value',
        metric is Counter
            ? 'Must be finite and nonnegative.'
            : 'Must be finite.',
      );
    }
    if (_noop) return;
    final registered = _metrics[metric.name];
    if (registered != null && !_matches(registered, metric)) {
      throw StateError('Metric "${metric.name}" has conflicting metadata.');
    }
    final key = _SeriesKey(metric.name, attributes);
    var series = _series[key];
    if (series == null) {
      if (_series.length == _maxSeries) {
        _droppedMeasurements++;
        return;
      }
      series = _Series(metric, attributes);
      _series[key] = series;
      _metrics[metric.name] = registered ?? metric;
    }
    switch (metric) {
      case Counter():
        final total = series.value + value;
        if (!total.isFinite) {
          throw StateError('Counter "${metric.name}" overflowed.');
        }
        series.value = total;
      case Gauge():
        series.value = value;
      case Histogram(:final boundaries):
        final sum = series.sum + value;
        if (!sum.isFinite) {
          throw StateError('Histogram "${metric.name}" sum overflowed.');
        }
        var bucket = 0;
        while (bucket < boundaries.length && value > boundaries[bucket]) {
          bucket++;
        }
        series.sum = sum;
        series.count++;
        series.buckets[bucket]++;
    }
  }

  /// Copies the current observations without executing tasks or resetting them.
  ///
  /// Lists, attribute maps, and histogram buckets are immutable. Later
  /// measurements do not change earlier snapshots. Entries within each metric
  /// kind follow the order in which their series were first accepted.
  MetricSnapshot snapshot() {
    final counters =
        <({Counter metric, Map<String, String> attributes, double value})>[];
    final gauges =
        <({Gauge metric, Map<String, String> attributes, double value})>[];
    final histograms =
        <
          ({
            Histogram metric,
            Map<String, String> attributes,
            List<int> buckets,
            int count,
            double sum,
          })
        >[];
    for (final series in _series.values) {
      switch (series.metric) {
        case final Counter metric:
          counters.add((
            metric: metric,
            attributes: series.attributes,
            value: series.value,
          ));
        case final Gauge metric:
          gauges.add((
            metric: metric,
            attributes: series.attributes,
            value: series.value,
          ));
        case final Histogram metric:
          histograms.add((
            metric: metric,
            attributes: series.attributes,
            buckets: List.unmodifiable(series.buckets),
            count: series.count,
            sum: series.sum,
          ));
      }
    }
    return MetricSnapshot._(counters, gauges, histograms, _droppedMeasurements);
  }
}

/// An immutable snapshot of all accepted metric series and capacity drops.
final class MetricSnapshot {
  MetricSnapshot._(
    Iterable<({Counter metric, Map<String, String> attributes, double value})>
    counters,
    Iterable<({Gauge metric, Map<String, String> attributes, double value})>
    gauges,
    Iterable<
      ({
        Histogram metric,
        Map<String, String> attributes,
        List<int> buckets,
        int count,
        double sum,
      })
    >
    histograms,
    this.droppedMeasurements,
  ) : counters = List.unmodifiable(counters),
      gauges = List.unmodifiable(gauges),
      histograms = List.unmodifiable(histograms);

  /// Counter totals in series registration order.
  final List<({Counter metric, Map<String, String> attributes, double value})>
  counters;

  /// Latest gauge values in series registration order.
  final List<({Gauge metric, Map<String, String> attributes, double value})>
  gauges;

  /// Histogram observations, with non-cumulative buckets defined by the metric.
  final List<
    ({
      Histogram metric,
      Map<String, String> attributes,
      List<int> buckets,
      int count,
      double sum,
    })
  >
  histograms;

  /// Measurements discarded because their series exceeded registry capacity.
  final int droppedMeasurements;
}

bool _matches(Metric left, Metric right) {
  if (left.runtimeType != right.runtimeType ||
      left.unit != right.unit ||
      left.description != right.description) {
    return false;
  }
  if (left is Histogram && right is Histogram) {
    if (left.boundaries.length != right.boundaries.length) return false;
    for (var index = 0; index < left.boundaries.length; index++) {
      if (left.boundaries[index] != right.boundaries[index]) return false;
    }
  }
  return true;
}

final class _Series {
  _Series(this.metric, this.attributes)
    : buckets = metric is Histogram
          ? List.filled(metric.boundaries.length + 1, 0)
          : [];

  final Metric metric;
  final Map<String, String> attributes;
  double value = 0;
  double sum = 0;
  var count = 0;
  final List<int> buckets;
}

final class _SeriesKey {
  _SeriesKey(this.name, this.attributes)
    : _hash = Object.hash(
        name,
        Object.hashAll(
          attributes.entries.map(
            (entry) => Object.hash(entry.key, entry.value),
          ),
        ),
      );

  final String name;
  final Map<String, String> attributes;
  final int _hash;

  @override
  int get hashCode => _hash;

  @override
  bool operator ==(Object other) =>
      other is _SeriesKey &&
      name == other.name &&
      attributes.length == other.attributes.length &&
      attributes.entries.every(
        (entry) => other.attributes[entry.key] == entry.value,
      );
}
