import 'dart:async';
import 'dart:collection';

import 'package:any/any.dart';

/// Executes streamed inputs with bounded task concurrency.
extension TaskStreamMapping<S> on Stream<S> {
  /// Maps inputs to tasks and emits their complete exits in completion order.
  ///
  /// The returned single-subscription stream is lazy. Each input gets a fresh
  /// [Scope] and shares [dependencies] by reference. Cleanup finishes before
  /// its exit is emitted and its slot reused. Child failures are data and do
  /// not stop subsequent inputs; throws from [task] become defects.
  ///
  /// At most [concurrency] executions or completed, undelivered exits are held
  /// at once. Pausing the listener pauses upstream dispatch. A source that
  /// buffers while paused, such as a broadcast stream, may still retain data
  /// upstream; this operation cannot bound that source's own buffer.
  ///
  /// Parent [cancellation] or a source error stops input, cancels children, and
  /// drains them before emitting a final failed exit. Source cancellation
  /// failures are included. Successful returned resources are already released.
  ///
  /// Cancelling the subscription requests child cancellation immediately and
  /// its returned future waits for source cancellation, bodies, and cleanup.
  /// Failures that can no longer be delivered are reported through that future
  /// as a [TaskStreamException]. Internal shutdown cancellations are omitted.
  ///
  /// Throws a [RangeError] immediately if [concurrency] is less than one.
  /// [yieldEvery] optionally sets each child's [Context.yieldEvery] budget and
  /// must also be positive when supplied.
  ///
  /// ```dart
  /// final results = Stream.fromIterable([1, 2, 3]).mapConcurrent(
  ///   (value) => Task<int, Never, ()>((_) => Exit.success(value * 2)),
  ///   dependencies: (),
  ///   concurrency: 2,
  /// );
  /// await for (final exit in results) {
  ///   if (exit case ExitSuccess(:final value)) print(value);
  /// }
  /// ```
  Stream<Exit<T, E>> mapConcurrent<T, E, R>(
    Task<T, E, R> Function(S value) task, {
    required R dependencies,
    required int concurrency,
    CancellationToken? cancellation,
    int? yieldEvery,
  }) {
    if (concurrency < 1) {
      throw RangeError.range(concurrency, 1, null, 'concurrency');
    }
    if (yieldEvery != null && yieldEvery < 1) {
      throw RangeError.range(yieldEvery, 1, null, 'yieldEvery');
    }
    return _ConcurrentMap<S, T, E, R>(
      this,
      task,
      dependencies,
      concurrency,
      cancellation ?? CancellationToken.none,
      yieldEvery,
    ).output.stream;
  }
}

/// Undelivered task or source-cleanup failures during subscription cancellation.
///
/// Await [StreamSubscription.cancel] and inspect [cause] to handle failures that
/// cannot be emitted after the subscription has been cancelled.
final class TaskStreamException<E> implements Exception {
  /// Creates an exception preserving the undelivered [cause].
  const TaskStreamException(this.cause);

  /// Undelivered child failures in completion order, followed by terminal
  /// source failures or parent cancellation, with their original stack traces.
  final Cause<E> cause;

  @override
  String toString() => 'TaskStreamException: $cause';
}

final class _ConcurrentMap<S, T, E, R> {
  _ConcurrentMap(
    this.source,
    this.task,
    this.dependencies,
    this.limit,
    this.parent,
    this.yieldEvery,
  ) {
    output = StreamController<Exit<T, E>>(
      sync: true,
      onListen: () => scheduleMicrotask(_start),
      onPause: _update,
      onResume: _update,
      onCancel: _cancel,
    );
  }

  final Stream<S> source;
  final Task<T, E, R> Function(S) task;
  final R dependencies;
  final int limit;
  final CancellationToken parent;
  final int? yieldEvery;
  late final StreamController<Exit<T, E>> output;
  final _children = CancellationController();
  final _stopReason = Object();
  final _ready = ListQueue<Exit<T, E>>();
  final _undelivered = <Cause<E>>[];
  final _done = Completer<void>();
  StreamSubscription<S>? _input;
  void Function()? _removeListener;
  Cause<E>? _terminal;
  var _active = 0;
  var _inputEnded = false;
  var _inputClosing = false;
  var _inputPaused = false;
  var _cancelled = false;
  var _finished = false;
  var _updating = false;

  void _start() {
    if (_finished || _cancelled) return;
    if (parent.isCancelled) {
      _terminal = Cause.cancelled(reason: parent.reason);
      _inputEnded = true;
      _update();
      return;
    }
    _removeListener = parent.onCancel((reason) {
      _appendTerminal(Cause.cancelled(reason: reason));
      _children.cancel(reason: reason);
      _stopInput();
      _update();
    });
    try {
      _input = source.listen(
        _accept,
        onError: (Object error, StackTrace trace) {
          _appendTerminal(Cause.defect(error, trace));
          _children.cancel(reason: _stopReason);
          _stopInput();
          _update();
        },
        onDone: () {
          _inputEnded = true;
          _update();
        },
      );
    } catch (error, trace) {
      _appendTerminal(Cause.defect(error, trace));
      _children.cancel(reason: _stopReason);
      _inputEnded = true;
    }
    _update();
  }

  void _accept(S value) {
    if (_cancelled || _terminal != null) return;
    _active++;
    _update();
    Task<T, E, R>((context) => task(value).runIn(context))
        .run(
          dependencies,
          cancellation: _children.token,
          yieldEvery: yieldEvery,
        )
        .then((exit) {
          _active--;
          final filtered = _filter(exit);
          if (filtered != null) _ready.addLast(filtered);
          _update();
        });
  }

  Exit<T, E>? _filter(Exit<T, E> exit) {
    if (exit is! ExitFailure<T, E>) return exit;
    final cause = exit.cause;
    final causes = cause is CauseGroup<E> ? cause.causes : [cause];
    final kept = causes.where(
      (cause) =>
          cause is! CauseCancelled<E> || !identical(cause.reason, _stopReason),
    );
    final failures = kept.toList(growable: false);
    if (failures.length == causes.length) return exit;
    return failures.isEmpty ? null : Exit.failure(Cause.combine(failures));
  }

  void _appendTerminal(Cause<E> cause) {
    _terminal = _terminal == null ? cause : Cause.combine([_terminal!, cause]);
  }

  void _stopInput() {
    if (_inputClosing || _inputEnded) return;
    final input = _input;
    if (input == null) {
      _inputEnded = true;
      return;
    }
    _inputClosing = true;
    Future<void>.sync(input.cancel).then(
      (_) {
        _inputClosing = false;
        _inputEnded = true;
        _update();
      },
      onError: (Object error, StackTrace trace) {
        _appendTerminal(Cause.defect(error, trace));
        _inputClosing = false;
        _inputEnded = true;
        _update();
      },
    );
  }

  Future<void> _cancel() {
    if (_finished) return Future<void>.value();
    _cancelled = true;
    _children.cancel(reason: _stopReason);
    _stopInput();
    _update();
    return _done.future;
  }

  void _update() {
    if (_updating || _finished) return;
    _updating = true;
    try {
      while (_ready.isNotEmpty && (_cancelled || !output.isPaused)) {
        final exit = _ready.removeFirst();
        if (_cancelled) {
          if (exit case ExitFailure(:final cause)) _undelivered.add(cause);
        } else {
          output.add(exit);
        }
      }

      if (_inputEnded &&
          !_inputClosing &&
          _active == 0 &&
          _ready.isEmpty &&
          (_cancelled || !output.isPaused)) {
        _finished = true;
        _removeListener?.call();
        _removeListener = null;
        if (_cancelled) {
          if (_terminal != null) _undelivered.add(_terminal!);
          if (_undelivered.isEmpty) {
            _done.complete();
          } else {
            _done.completeError(
              TaskStreamException<E>(Cause.combine(_undelivered)),
            );
          }
        } else {
          _done.complete();
          if (_terminal != null) output.add(Exit.failure(_terminal!));
          unawaited(output.close());
        }
        return;
      }

      final input = _input;
      if (input != null && !_inputEnded && !_inputClosing) {
        final pause =
            _cancelled ||
            _terminal != null ||
            output.isPaused ||
            _active + _ready.length >= limit;
        if (pause && !_inputPaused) {
          _inputPaused = true;
          input.pause();
        } else if (!pause && _inputPaused) {
          _inputPaused = false;
          input.resume();
        }
      }
    } finally {
      _updating = false;
    }
  }
}
