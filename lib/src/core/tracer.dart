import 'dart:async';

import 'clock.dart';
import 'context.dart';
import 'exit.dart';
import 'option.dart';
import 'random_source.dart';
import 'task.dart';
import 'task_local.dart';

/// The identity of a span within a trace.
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

/// A completed span's timing, outcome, attributes, and events.
///
/// Successful result values are not retained. Failure causes retain their
/// original objects and stack traces. Attribute maps are shallow-copied; their
/// values remain shared by reference.
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

  /// The operation name supplied to [TaskTracingOperations.withSpan].
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

/// A tracer that exports completed task spans to an application-supplied backend.
///
/// Bind a tracer through [key], then use [TaskTracingOperations.withSpan] around
/// operations to record their duration and outcome, including resource cleanup.
/// Nested spans form a trace; logs can carry the active trace and span IDs.
///
/// Backend errors go to the configured error handler without changing task
/// results. The application owns the backend and must flush and close it after
/// traced work and its cleanup have finished.
///
/// ```dart
/// final records = <SpanRecord>[];
/// final tracer = Tracer(records.add);
/// final runtime = Runtime(context: Context.from([Tracer.key.bind(tracer)]));
/// try {
///   await runtime.runOrThrow(Task.succeed(42).withSpan('calculate'));
///   assert(records.single.name == 'calculate');
///   assert(records.single.exit is ExitSuccess);
/// } finally {
///   await runtime.close();
///   await tracer.flush();
/// }
/// ```
final class Tracer {
  /// Creates a tracer with bounded attributes and events per span.
  ///
  /// Limits must be nonnegative or this throws [RangeError]. Existing names can
  /// still be updated at capacity; new names and excess events are dropped and
  /// counted. Event attributes use the same limit, independently for each event.
  ///
  /// Output and [flush] callbacks run in invocation order. They must not await
  /// this tracer. Errors are passed to [onError] in the creating zone. Without
  /// a handler, or if it throws, that zone's uncaught-error handler receives
  /// them. The error handler must not write back to this tracer.
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

  /// Creates a tracer that disables span recording.
  Tracer.noop()
    : maxAttributes = 0,
      maxEvents = 0,
      _output = null,
      _flush = null,
      _onError = null,
      _zone = Zone.current;

  /// The service key used by [TaskTracingOperations.withSpan].
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
  /// Active spans are not ended or awaited, so stop traced work before flushing
  /// to drain all output. Later output waits until flushing finishes. Backend
  /// errors are reported separately; the returned future still succeeds.
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

/// An active trace span that accepts attributes and events.
///
/// Obtain it with [current]. Forks inherit the active span; their modifications
/// refer to the same recording state. Use [TaskTracingOperations.withSpan] to
/// create a distinct child span. Updates after the span ends have no effect.
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

  /// Records a named event when the returned task executes.
  ///
  /// Input attributes are shallow-copied now. Excess attributes and events are
  /// dropped and counted in the completed [SpanRecord]. Attributes are retained
  /// in insertion order. The span's clock supplies the timestamp; a clock error
  /// is reported separately and skips the event without failing the task.
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

/// Trace spans around task executions.
extension TaskTracingOperations<T, E> on Task<T, E> {
  /// Records this task and its resource cleanup as one span.
  ///
  /// Each execution creates a new span. [attributes] are shallow-copied now.
  /// [parent] overrides the inherited span's identity, for example when joining
  /// an incoming distributed trace. Without either, a new trace is created.
  /// Nested tasks and finalizers see this span through [Span.current].
  ///
  /// Resources acquired by this task are released before the span ends. The
  /// recorded exit includes cancellation and cleanup defects. Completion waits
  /// for cleanup and the backend, which is called at most once; reporting errors
  /// do not change the task's outcome. A no-op tracer still creates the resource
  /// boundary but records no span.
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
