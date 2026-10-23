import 'dart:async';

import 'clock.dart';
import 'context.dart';
import 'task.dart';
import 'task_local.dart';
import 'tracer.dart';

/// The severity of a log record, ordered from least to most severe.
enum LogLevel {
  /// Fine-grained diagnostic events.
  trace,

  /// Information useful while diagnosing application behavior.
  debug,

  /// Normal application events.
  info,

  /// Unexpected conditions that the application can continue handling.
  warning,

  /// Operations that could not complete as intended.
  error,

  /// Failures that prevent an application or subsystem from continuing.
  fatal,
}

/// A structured log event with a UTC timestamp and immutable field map.
///
/// Field values are retained by reference. Nested collections and other mutable
/// objects are not copied. Trace identifiers are optional opaque strings.
final class LogRecord {
  /// Creates a record, converting [timestamp] to UTC and copying [fields].
  LogRecord({
    required this.level,
    required DateTime timestamp,
    required this.message,
    Map<String, Object?> fields = const {},
    this.traceId,
    this.spanId,
  }) : timestamp = timestamp.toUtc(),
       fields = Map.unmodifiable(fields);

  /// The severity of this event.
  final LogLevel level;

  /// The time this event occurred, in UTC.
  final DateTime timestamp;

  /// The human-readable description of this event.
  final String message;

  /// The immutable, shallow copy of structured fields supplied for this event.
  final Map<String, Object?> fields;

  /// An optional identifier of the trace containing this event.
  final String? traceId;

  /// An optional identifier of the span containing this event.
  final String? spanId;
}

/// A structured logger that writes records to an application-supplied backend.
///
/// Bind a logger through [key] to receive events from [Task.log]. Writes and
/// flushes run in invocation order; await them to keep pending work bounded.
/// The application owns the backend and must flush and close it after use.
///
/// Backend exceptions go to the configured error handler and never fail [write]
/// or [flush]. Backend callbacks must not await this logger, which would wait
/// for the current callback to finish.
///
/// ```dart
/// final records = <LogRecord>[];
/// final logger = Logger(records.add);
/// final runtime = Runtime(context: Context.from([Logger.key.bind(logger)]));
/// try {
///   await runtime.runOrThrow(
///     Task.log('Order accepted').withLogFields({'orderId': 'order-42'}),
///   );
///   assert(records.single.fields['orderId'] == 'order-42');
/// } finally {
///   await runtime.close();
///   await logger.flush();
/// }
/// ```
final class Logger {
  /// Creates an adapter that forwards records at or above [minLevel].
  ///
  /// [flush] is optional; without it, flushing still waits for earlier writes.
  /// Backend errors are passed to [onError] in the creating zone. Without a
  /// handler, or if the handler throws, that zone's [Zone.handleUncaughtError]
  /// receives the error. The handler must not log through this logger.
  Logger(
    FutureOr<void> Function(LogRecord record) output, {
    this.minLevel = LogLevel.info,
    this._flush,
    this._onError,
  }) : _output = output,
       _zone = Zone.current,
       _noop = false;

  /// Creates an adapter that discards every record without invoking a backend.
  Logger.noop()
    : minLevel = LogLevel.trace,
      _output = null,
      _flush = null,
      _onError = null,
      _zone = Zone.current,
      _noop = true;

  /// Creates a logger that writes to each of [outputs] in order.
  ///
  /// The outputs are copied now. Each keeps its own threshold and error handler;
  /// a backend failure does not skip later outputs. Flushing awaits every output
  /// in the same order. Empty input creates a no-op logger.
  factory Logger.combine(
    Iterable<Logger> outputs, {
    LogLevel minLevel = LogLevel.trace,
  }) {
    final sinks = List<Logger>.unmodifiable(outputs);
    if (sinks.isEmpty) return Logger.noop();
    return Logger(
      (record) async {
        for (final sink in sinks) {
          await sink.write(record);
        }
      },
      minLevel: minLevel,
      flush: () async {
        for (final sink in sinks) {
          await sink.flush();
        }
      },
    );
  }

  /// The application service used by [Task.log].
  static final key = ServiceKey<Logger>('logger');

  /// The inclusive minimum severity forwarded to this adapter's backend.
  final LogLevel minLevel;

  final FutureOr<void> Function(LogRecord)? _output;
  final FutureOr<void> Function()? _flush;
  final void Function(Object, StackTrace)? _onError;
  final Zone _zone;
  final bool _noop;
  Future<void> _pending = Future<void>.value();

  /// Enqueues [record] and waits for its backend operation to finish.
  ///
  /// Records below [minLevel] and records sent to a no-op logger are discarded.
  /// The returned future succeeds even when the backend reports an error.
  Future<void> write(LogRecord record) {
    if (!_accepts(record.level)) return Future<void>.value();
    return _enqueue(() => _output!(record));
  }

  /// Waits for earlier writes, then invokes the configured flush callback.
  ///
  /// Later writes wait for this operation. The returned future succeeds even
  /// when the backend reports an error.
  Future<void> flush() => _enqueue(_flush ?? () {});

  bool _accepts(LogLevel level) => !_noop && level.index >= minLevel.index;

  Future<void> _enqueue(FutureOr<void> Function() operation) {
    return _pending = _pending.then((_) async {
      try {
        await operation();
      } catch (error, stackTrace) {
        _report(error, stackTrace);
      }
    });
  }

  void _report(Object error, StackTrace stackTrace) {
    final onError = _onError;
    if (onError == null) {
      _zone.handleUncaughtError(error, stackTrace);
    } else {
      _zone.runGuarded(() => onError(error, stackTrace));
    }
  }
}

final _fields = TaskLocal<Map<String, Object?>>(const {}, name: 'logFields');

/// Structured logging context for tasks.
extension TaskLoggingOperations<T, E> on Task<T, E> {
  /// Runs this task with [fields] added to its inherited log fields.
  ///
  /// The map is shallow-copied when this method is called. Its values override
  /// inherited fields with the same key, including explicit `null` values.
  /// Fields supplied directly to [Task.log] take precedence over both.
  ///
  /// Fields follow [TaskLocal] inheritance: children inherit them, nested
  /// overrides stay local, and finalizers retain their registration context.
  Task<T, E> withLogFields(Map<String, Object?> fields) {
    final added = Map<String, Object?>.unmodifiable(fields);
    return _fields.get().withError<E>().flatMap(
      (inherited) => _fields.locally(
        Map<String, Object?>.unmodifiable({...inherited, ...added}),
        this,
      ),
    );
  }
}

// Shared by the public Task.log constructor; not exported from any.dart.
Task<void, Never> logTask(
  String message, {
  LogLevel level = LogLevel.info,
  Map<String, Object?> fields = const {},
}) {
  final explicit = Map<String, Object?>.unmodifiable(fields);
  return Task.service(Logger.key).flatMap((logger) {
    if (!logger._accepts(level)) return Task.succeed(null);
    return Task.service(Clock.key).flatMap(
      (clock) => _fields.get().flatMap(
        (inherited) => currentSpanContext().flatMap((span) {
          late LogRecord record;
          try {
            record = LogRecord(
              level: level,
              timestamp: clock.now(),
              message: message,
              fields: {...inherited, ...explicit},
              traceId: span?.traceId,
              spanId: span?.spanId,
            );
          } catch (error, stackTrace) {
            logger._report(error, stackTrace);
            return Task.succeed(null);
          }
          return Task.fromFuture(
            () => logger.write(record),
            onError: Error.throwWithStackTrace,
          );
        }),
      ),
    );
  });
}
