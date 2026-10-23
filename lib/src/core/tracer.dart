import 'dart:async';

import 'clock.dart';
import 'context.dart';
import 'exit.dart';
import 'option.dart';
import 'random_source.dart';
import 'task.dart';
import 'task_local.dart';

/// Identifies a span and the trace to which it belongs.
///
/// Identifiers are opaque to core. Transport adapters validate their own wire
/// formats. Generated identifiers use [RandomSource] and are not credentials.
final class SpanContext {
  /// Creates a context, including one received from another process.
  const SpanContext({required this.traceId, required this.spanId});

  /// The identifier shared by spans in this trace.
  final String traceId;

  /// The identifier of this individual span.
  final String spanId;
}

/// An event recorded during a span, with immutable, shallow-copied attributes.
final class SpanEvent {
  /// Creates an event and converts [timestamp] to UTC.
  SpanEvent({
    required this.name,
    required DateTime timestamp,
    Map<String, Object?> attributes = const {},
  }) : timestamp = timestamp.toUtc(),
       attributes = Map.unmodifiable(attributes);

  /// The event's application-defined name.
  final String name;

  /// The UTC time at which the event was recorded.
  final DateTime timestamp;

  /// Structured data; nested mutable values remain shared by reference.
  final Map<String, Object?> attributes;
}

/// The immutable snapshot delivered after a span's work and cleanup finish.
///
/// Successful result values are not retained. Failure causes retain their
/// original objects and stack traces. Attribute values remain shallow copies.
final class SpanRecord {
  SpanRecord._({
    required this.name,
    required this.context,
    required this.parent,
    required this.startedAt,
    required this.endedAt,
    required this.duration,
    required this.exit,
    required Map<String, Object?> attributes,
    required List<SpanEvent> events,
    required this.droppedAttributes,
    required this.droppedEvents,
  }) : attributes = Map.unmodifiable(attributes),
       events = List.unmodifiable(events);

  /// The operation name supplied to `withSpan`.
  final String name;

  /// This span's identity.
  final SpanContext context;

  /// The local or explicitly supplied parent, absent for a root span.
  final SpanContext? parent;

  /// The UTC wall time at the beginning of this span.
  final DateTime startedAt;

  /// The UTC wall time after work and cleanup finish.
  final DateTime endedAt;

  /// Elapsed monotonic time, independent of wall-clock adjustments.
  final Duration duration;

  /// The final outcome with successful values discarded.
  final Exit<void, Object?> exit;

  /// The attributes retained within the tracer's capacity.
  final Map<String, Object?> attributes;

  /// The events retained in recording order.
  final List<SpanEvent> events;

  /// Attributes discarded across this span and its events at their capacities.
  final int droppedAttributes;

  /// Events discarded after reaching the capacity.
  final int droppedEvents;
}

/// Collects completed spans through an ordered, awaitable backend.
///
/// Backend failures are independently reported and never replace a business
/// result. This adapter does not own the backend; use a layer to acquire it and
/// flush and close it after dependent services have finished their cleanup.
final class Tracer {
  /// Creates a tracer with bounded attributes and events per span.
  ///
  /// Limits must be nonnegative. Existing attribute names can still be updated
  /// at capacity; new names and excess events are dropped and counted. Event
  /// attributes use the same attribute limit, independently for each event.
  ///
  /// Output and [flush] callbacks run serially in invocation order. They must
  /// not await another operation on this tracer. Errors are passed to [onError]
  /// in the creating zone, or to that zone's uncaught-error handler when absent.
  /// The error handler must not write back to this tracer. Exceptions from the
  /// handler are also sent to the creating zone.
  Tracer(
    FutureOr<void> Function(SpanRecord record) output, {
    this.maxAttributes = 128,
    this.maxEvents = 128,
    this._flush,
    this._onError,
  }) : _output = output,
       _zone = Zone.current {
    RangeError.checkNotNegative(maxAttributes, 'maxAttributes');
    RangeError.checkNotNegative(maxEvents, 'maxEvents');
  }

  /// Creates a tracer that does not allocate spans or read clock/random services.
  Tracer.noop()
    : maxAttributes = 0,
      maxEvents = 0,
      _output = null,
      _flush = null,
      _onError = null,
      _zone = Zone.current;

  /// The tracer used by `withSpan` in the current task context.
  static final key = ServiceKey<Tracer>('tracer');

  /// Maximum retained span attributes and maximum attributes per event.
  final int maxAttributes;

  /// Maximum retained events in each span.
  final int maxEvents;

  final FutureOr<void> Function(SpanRecord)? _output;
  final FutureOr<void> Function()? _flush;
  final void Function(Object, StackTrace)? _onError;
  final Zone _zone;
  Future<void> _pending = Future<void>.value();

  /// Waits for previously ended spans, then invokes the backend flush callback.
  ///
  /// Active spans are not ended or awaited. Later span output waits behind this
  /// barrier. The Future succeeds even if the backend independently reports an
  /// error. Call this after application work stops when a full drain is needed.
  Future<void> flush() => _enqueue(_flush ?? () {});

  Future<void> _enqueue(FutureOr<void> Function() action) =>
      _pending = _pending.then((_) async {
        try {
          await action();
        } catch (error, stackTrace) {
          _report(error, stackTrace);
        }
      });

  void _report(Object error, StackTrace stackTrace) {
    final handler = _onError;
    if (handler == null) {
      _zone.handleUncaughtError(error, stackTrace);
    } else {
      _zone.runGuarded(() => handler(error, stackTrace));
    }
  }

  Span? _start(
    String name,
    SpanContext? parent,
    Map<String, Object?> attributes,
    Clock clock,
    RandomSource random,
  ) {
    try {
      final span = Span._(
        name,
        SpanContext(
          traceId: parent?.traceId ?? _identifier(random, 16),
          spanId: _identifier(random, 8),
        ),
        parent,
        clock.now().toUtc(),
        clock.elapsed,
        clock,
        this,
      );
      span._setAttributes(attributes);
      return span;
    } catch (error, stackTrace) {
      _report(error, stackTrace);
      return null;
    }
  }
}

/// The active, bounded recording state of a task span.
///
/// Obtain it with [current]. Forks inherit the active span; their modifications
/// refer to the same recording state. Use a nested `withSpan` for a distinct
/// child. Updates after the span ends have no effect.
final class Span {
  Span._(
    this.name,
    this.context,
    this.parent,
    this._startedAt,
    this._startedElapsed,
    this._clock,
    this._tracer,
  );

  /// Reads the active span when this task runs, or [OptionNone] if absent.
  static Task<Option<Span>, Never> current() => _current.get().map(
    (span) => span == null ? const Option.none() : Option.some(span),
  );

  /// The application operation name.
  final String name;

  /// This span's identity, suitable for propagation by a transport adapter.
  final SpanContext context;

  /// This span's parent, if any.
  final SpanContext? parent;

  final DateTime _startedAt;
  final Duration _startedElapsed;
  final Clock _clock;
  final Tracer _tracer;
  final _attributes = <String, Object?>{};
  final _events = <SpanEvent>[];
  var _droppedAttributes = 0;
  var _droppedEvents = 0;
  var _ended = false;

  /// Adds or replaces attributes when the returned task executes.
  ///
  /// The input map is shallow-copied at construction. At capacity, existing
  /// keys can be replaced and new keys are dropped. Values may include `null`.
  Task<void, Never> setAttributes(Map<String, Object?> attributes) {
    final copied = Map<String, Object?>.unmodifiable(attributes);
    return Task.sync(() {
      if (!_ended) _setAttributes(copied);
    });
  }

  /// Records an event using this span's captured clock.
  ///
  /// Input attributes are shallow-copied now. Excess attributes are dropped in
  /// insertion order and included in the record's dropped-attribute count.
  /// At the event limit, no timestamp is read. Clock failures are independently
  /// reported and the event is skipped without failing the task.
  Task<void, Never> addEvent(
    String name, {
    Map<String, Object?> attributes = const {},
  }) {
    final copied = Map<String, Object?>.unmodifiable(attributes);
    return Task.sync(() {
      if (_ended) return;
      if (_events.length == _tracer.maxEvents) {
        _droppedEvents++;
        return;
      }
      try {
        final retained = <String, Object?>{};
        for (final entry in copied.entries) {
          if (retained.length < _tracer.maxAttributes) {
            retained[entry.key] = entry.value;
          } else {
            _droppedAttributes++;
          }
        }
        _events.add(
          SpanEvent(name: name, timestamp: _clock.now(), attributes: retained),
        );
      } catch (error, stackTrace) {
        _tracer._report(error, stackTrace);
      }
    });
  }

  void _setAttributes(Map<String, Object?> attributes) {
    for (final entry in attributes.entries) {
      if (_attributes.containsKey(entry.key) ||
          _attributes.length < _tracer.maxAttributes) {
        _attributes[entry.key] = entry.value;
      } else {
        _droppedAttributes++;
      }
    }
  }

  Task<void, Never> _end(Exit<Object?, Object?> exit) => Task.defer(() {
    if (_ended) return Task.succeed(null);
    _ended = true;
    var endedAt = _startedAt;
    var duration = Duration.zero;
    try {
      endedAt = _clock.now().toUtc();
      duration = _clock.elapsed - _startedElapsed;
      if (duration.isNegative) {
        duration = Duration.zero;
        throw StateError('The span clock moved backwards.');
      }
    } catch (error, stackTrace) {
      _tracer._report(error, stackTrace);
    }
    final record = SpanRecord._(
      name: name,
      context: context,
      parent: parent,
      startedAt: _startedAt,
      endedAt: endedAt,
      duration: duration,
      exit: switch (exit) {
        ExitSuccess() => const Exit.success(null),
        ExitFailure(:final cause) => Exit.failure(cause),
      },
      attributes: _attributes,
      events: _events,
      droppedAttributes: _droppedAttributes,
      droppedEvents: _droppedEvents,
    );
    return Task.fromFuture(
      () => _tracer._enqueue(() => _tracer._output!(record)),
      onError: Error.throwWithStackTrace,
    );
  });
}

/// Tracing boundaries for tasks, preserving their value and error types.
extension TaskTracingOperations<T, E> on Task<T, E> {
  /// Records this task and its resource cleanup as one span.
  ///
  /// Each execution creates a new span. [attributes] are shallow-copied now.
  /// [parent] overrides the inherited span's identity, for example when joining
  /// an incoming distributed trace. Without either, a new trace is created.
  /// Nested tasks and finalizers see this span through [Span.current].
  ///
  /// Work runs in an owned child fiber with a local resource scope. Ending the
  /// span waits for its full exit, including cancellation and cleanup defects,
  /// then awaits the backend. The backend is called at most once. Reporting
  /// errors do not change that exit. The resource boundary also applies with a
  /// no-op tracer, which does not create a span or an additional fiber.
  Task<T, E> withSpan(
    String name, {
    Map<String, Object?> attributes = const {},
    SpanContext? parent,
  }) {
    final copied = Map<String, Object?>.unmodifiable(attributes);
    return Task.service(Tracer.key).withError<E>().flatMap((tracer) {
      if (tracer._output == null) return scoped();
      return Task.uninterruptibleMask<T, E>((restore) {
        return _current.get().withError<E>().flatMap(
          (inherited) => Task.service(Clock.key).withError<E>().flatMap(
            (clock) =>
                Task.service(RandomSource.key).withError<E>().flatMap((random) {
                  final span = tracer._start(
                    name,
                    parent ?? inherited?.context,
                    copied,
                    clock,
                    random,
                  );
                  if (span == null) return restore(this);
                  return accessTaskEnvironment(
                    (scope, _) => forkTaskIn(
                      _current.locally(span, restore(this)),
                      scope,
                      inheritHost: true,
                    ),
                  ).withError<E>().flatMap(
                    (fiber) => restore(fiber.join())
                        .onExit((_) => fiber.awaitExit().flatMap(span._end)),
                  );
                }),
          ),
        );
      }).scoped();
    });
  }
}

final _current = TaskLocal<Span?>(null, name: 'span');

// Logging reads identity without exposing the mutable task-local binding.
Task<SpanContext?, Never> currentSpanContext() =>
    _current.get().map((span) => span?.context);

String _identifier(RandomSource random, int bytes) {
  final buffer = StringBuffer();
  for (var i = 0; i < bytes; i++) {
    final byte = random.nextInt(256);
    RangeError.checkValueInInterval(byte, 0, 255, 'random byte');
    buffer.write(byte.toRadixString(16).padLeft(2, '0'));
  }
  return buffer.toString();
}
