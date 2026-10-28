import 'dart:async';
import 'dart:collection';

import 'async_queue.dart';
import 'cancellation.dart';
import 'cause.dart';
import 'exit.dart';
import 'fiber.dart';
import 'internal/flow_cursor.dart';
import 'option.dart';
import 'runtime.dart';
import 'scope.dart';
import 'task.dart';

/// A lazy sequence of [T] values with business failures of type [E].
///
/// Use a flow to process values incrementally with [Task] operations and scoped
/// resources. Build a pipeline, then consume it with [FlowOperations.toList],
/// [FlowOperations.fold], or [FlowOperations.forEach]. Construction does no work;
/// each consumption starts a new iteration.
///
/// ```dart
/// import 'package:any/any.dart';
///
/// Future<void> main() async {
///   final runtime = Runtime();
///   try {
///     final values = await runtime.runOrThrow(
///       Flow.fromIterable([1, 2, 3])
///           .filter((value) => value > 1)
///           .map((value) => value * 10)
///           .toList(),
///     );
///     print(values); // [20, 30]
///   } finally {
///     await runtime.close();
///   }
/// }
/// ```
///
/// Consumption requests values as needed unless an operator buffers or processes
/// concurrently. User tasks acquire resources in the consuming task's scope;
/// ending consumption stops internal work without closing that borrowed scope.
/// Use [TaskOperations.scoped] for per-element resource use, returning data that
/// remains valid after cleanup.
///
/// Exceptions in callbacks become defects. External sources retain their own
/// sharing rules: for example, consumers of [fromQueue] compete for values.
final class Flow<T, E> {
  Flow._(this._open);

  Flow._source(Task<Task<Option<T>, E>, E> open, {bool ownsResources = false})
    : _open = ((resources) =>
          _openCursor(open, resources, ownsResources: ownsResources));

  final Task<FlowCursor<T, E>, E> Function(Scope resources) _open;

  /// Emits [values] in iteration order.
  ///
  /// Each consumption creates a new iterator without copying the iterable. Changes
  /// to the iterable remain visible according to its iterator's rules; iteration
  /// exceptions become defects. A nullable element is distinct from completion.
  static Flow<T, Never> fromIterable<T>(Iterable<T> values) => Flow._source(
    Task.sync(() {
      final iterator = values.iterator;
      return Task.sync(
        () => iterator.moveNext()
            ? Option.some(iterator.current)
            : const Option<Never>.none(),
      );
    }),
  );

  /// Emits the successful result of [task] once per consumption.
  ///
  /// A failed task emits nothing. Resources acquired by the task belong to the
  /// consuming task's scope and remain available after consumption ends.
  static Flow<T, E> fromTask<T, E>(Task<T, E> task) => Flow._source(
    Task.sync(() {
      var emitted = false;
      return Task.defer(() {
        if (emitted) return Task.succeed(const Option<Never>.none());
        emitted = true;
        return task.map(Option.some);
      });
    }),
  );

  /// Takes values from [queue] in queue order.
  ///
  /// [AsyncQueue.end] lets accepted values drain before the flow ends;
  /// [AsyncQueue.close] fails consumption with [QueueClosed]. Stopping the flow
  /// cancels its pending take without closing the queue. Multiple consumers divide
  /// the values among them rather than each receiving a copy.
  static Flow<T, QueueClosed> fromQueue<T>(AsyncQueue<T> queue) =>
      Flow._source(Task.succeed(queue.take()));

  /// Consumes a new Dart stream from [create] on each consumption.
  ///
  /// [onError] maps the first stream error and its stack trace to a business
  /// failure, after any buffered values. Exceptions thrown by [create], `listen`,
  /// or [onError] become defects. Ending or stopping consumption cancels the
  /// subscription and waits for cancellation to finish.
  ///
  /// [bufferCapacity] limits this adapter's buffer and must be positive; otherwise
  /// consumption fails with a [RangeError] defect before calling [create]. The
  /// subscription pauses between requests. A source may still buffer internally,
  /// so this is not a limit on the source's total memory use.
  ///
  /// [overflow] handles values arriving when the adapter is full. Error and done
  /// events are never dropped. With [StreamOverflowStrategy.fail], a detected
  /// overflow still fails consumption if it stops before reaching the overflow.
  static Flow<T, E> fromStream<T, E>(
    Stream<T> Function() create, {
    required E Function(Object error, StackTrace stackTrace) onError,
    int bufferCapacity = 1,
    StreamOverflowStrategy overflow = StreamOverflowStrategy.fail,
  }) => Flow._source(
    Task.acquireRelease(
      Task.sync(() {
        if (bufferCapacity < 1) {
          throw RangeError.range(bufferCapacity, 1, null, 'bufferCapacity');
        }
        return _StreamSource<T>(bufferCapacity, overflow);
      }),
      release: (source, _) => source.close(),
    ).withError<E>().flatMap(
      (source) => Task.sync(() {
        source.listen(create());
        return source.pull(onError);
      }),
    ),
    ownsResources: true,
  );

  /// Builds a sequence by applying [step] to a state, starting with [initial].
  ///
  /// Return `Option.some((value, nextState))` to emit a value, or [OptionNone] to
  /// finish. The next step waits until the previous value has been consumed. Each
  /// consumption starts with [initial]; mutable state is not copied.
  static Flow<T, E> unfold<S, T, E>(
    S initial,
    Task<Option<(T, S)>, E> Function(S state) step,
  ) => Flow._source(
    Task.sync(() {
      var state = initial;
      return Task.defer(
        () => step(state).map((next) {
          if (next case OptionSome(:final value)) {
            state = value.$2;
            return Option.some(value.$1);
          }
          return const Option<Never>.none();
        }),
      );
    }),
  );
}

/// Transformations and consumers for a [Flow].
extension FlowOperations<T, E> on Flow<T, E> {
  /// Transforms each element with [transform], preserving order.
  ///
  /// An exception from [transform] fails consumption with a defect.
  Flow<U, E> map<U>(U Function(T value) transform) => _transform(
    _open,
    (source) => source.pull().map((item) => item.map(transform)),
  );

  /// Transforms elements with tasks, optionally running several at once.
  ///
  /// [concurrency] defaults to one, with no prefetch. Higher limits include running
  /// tasks, results waiting for delivery, and the currently delivered result until
  /// downstream requests another. Input order is preserved unless [preserveOrder]
  /// is false, which emits results in completion order. A nonpositive limit causes
  /// a [RangeError] defect before source work starts.
  ///
  /// Mapping tasks and downstream callbacks use the consuming task's resource
  /// scope. Concurrency and buffering do not limit the resources accumulated in
  /// that scope. Use [TaskOperations.scoped] inside [transform] to finish each
  /// resource's use and cleanup before emitting its result.
  ///
  /// Failure stops the source and cancels unfinished mappings. Stopping early
  /// waits for their cleanup and preserves observed defects and cleanup failures.
  Flow<U, E> mapTask<U>(
    Task<U, E> Function(T value) transform, {
    int concurrency = 1,
    bool preserveOrder = true,
  }) => Flow._(
    (resources) => Task.defer(() {
      if (concurrency < 1) {
        throw RangeError.range(concurrency, 1, null, 'concurrency');
      }
      return Scope.open().withError<E>().flatMap(
        (owner) => inTaskScope(_open(resources), owner).flatMap((source) {
          final mapping = _ConcurrentMap(
            source,
            owner,
            transform,
            concurrency,
            preserveOrder,
          );
          return inTaskScope(
            Task.addFinalizer(mapping.cleanup),
            owner,
          ).withError<E>().map((_) => mapping.cursor);
        }),
      );
    }),
  );

  /// Keeps elements for which [test] returns true, preserving their order.
  Flow<T, E> filter(bool Function(T value) test) =>
      filterTask((value) => Task.sync(() => test(value)));

  /// Keeps elements whose sequential [test] task returns true.
  ///
  /// The test's resources belong to the consuming task's scope, including for
  /// rejected values. Use [TaskOperations.scoped] on the test task to close its
  /// resources after each test. A failed test stops consumption.
  Flow<T, E> filterTask(Task<bool, E> Function(T value) test) =>
      _transform(_open, (source) {
        Task<Option<T>, E> next() => Task.defer(
          () => source.pull().flatMap(
            (item) => switch (item) {
              OptionSome(:final value) => inTaskScope(
                test(value),
                source.resourceScope,
              ).flatMap((keep) => keep ? Task.succeed(item) : next()),
              OptionNone() => Task.succeed(item),
            },
          ),
        );
        return next();
      });

  /// Emits each flow returned by [transform] in sequence.
  ///
  /// Each inner flow finishes producing before the next outer value is requested.
  /// User tasks in both flows share the consuming task's resource scope. Each
  /// inner flow stops its internal production before the next opens; this does
  /// not release resources acquired by user tasks. Failure stops the sequence.
  Flow<U, E> flatMap<U>(Flow<U, E> Function(T value) transform) => Flow._(
    (resources) => _open(resources).map((source) {
      FlowCursor<U, E>? inner;
      Task<Option<U>, E> next() => Task.defer(() {
        final current = inner;
        if (current != null) {
          return current.pull().flatMap((item) {
            if (item is OptionSome<U>) return Task.succeed(item);
            inner = null;
            return next();
          });
        }
        return source.pull().flatMap(
          (item) => switch (item) {
            OptionSome(:final value) =>
              inTaskScope(
                transform(value)._open(resources),
                source.scope,
              ).flatMap((opened) {
                inner = opened;
                return next();
              }),
            OptionNone() => Task.succeed(const Option<Never>.none()),
          },
        );
      });
      return FlowCursor(
        next(),
        () => inner?.scope ?? source.scope,
        source.finish,
        () => [...source.producers(), ...?inner?.producers()],
        resourceScope: resources,
      );
    }),
  );

  /// Emits the accumulated state after each element.
  ///
  /// [initial] is the starting state, not a separate output. Each consumption uses
  /// that same initial value; mutable objects are not copied. Exceptions from
  /// [combine] become defects.
  ///
  /// ```dart
  /// final totals = Flow.fromIterable([1, 2, 3])
  ///     .scan(0, (sum, value) => sum + value)
  ///     .toList();
  /// // Running totals produces [1, 3, 6].
  /// ```
  Flow<S, E> scan<S>(S initial, S Function(S state, T value) combine) =>
      _transform(_open, (source) {
        var state = initial;
        return source.pull().map(
          (item) => item.map((value) {
            state = combine(state, value);
            return state;
          }),
        );
      });

  /// Collects consecutive elements into immutable lists of at most [size].
  ///
  /// Normal completion emits a final partial group; failure discards it. User
  /// resources remain in the consuming task's scope after a group is consumed.
  /// [size] must be positive; otherwise consumption fails with a [RangeError]
  /// defect before opening the source.
  ///
  /// ```dart
  /// final batches = Flow.fromIterable([1, 2, 3, 4, 5]).grouped(2).toList();
  /// // Running batches produces [[1, 2], [3, 4], [5]].
  /// ```
  ///
  /// At most [size] inputs are held for the next group, in addition to any groups
  /// still in use downstream.
  Flow<List<T>, E> grouped(int size) => Flow._(
    (resources) => Task.defer(() {
      if (size < 1) throw RangeError.range(size, 1, null, 'size');
      return Scope.open().withError<E>().flatMap(
        (owner) => inTaskScope(
          _open(resources),
          owner,
        ).map((source) => _Grouped(source, owner, size).cursor),
      );
    }),
  );

  /// Prefetches up to [capacity] elements while downstream processes a value.
  ///
  /// Use this between a producer and a slow consumer to let their work overlap.
  /// Values retain their input order. Source tasks and downstream callbacks
  /// acquire resources in the consuming task's scope.
  ///
  /// ```dart
  /// final work = Flow.fromIterable([1, 2, 3])
  ///     .mapTask((value) => Task.sync(() => value * 10))
  ///     .buffer(2)
  ///     .forEach((value) => Task.sync(() => print(value)));
  /// // Running work prints 10, 20, 30. Mapping can advance ahead of printing.
  /// ```
  ///
  /// The limit counts queued values and a pending read, excluding the value being
  /// consumed: up to `capacity + 1` elements may be held, plus values retained by
  /// later operators. A full buffer stops further reads. [capacity] must be
  /// positive or consumption fails with a [RangeError] defect before source work.
  ///
  /// Buffered values precede the source's completion or failure. Stopping early
  /// cancels pending production and waits for cleanup. After consumer cancellation,
  /// new business failures or cancellation from unfinished source work are ignored;
  /// defects and resource cleanup failures still propagate.
  Flow<T, E> buffer(int capacity) => Flow._(
    (resources) => Task.defer(() {
      if (capacity < 1) throw RangeError.range(capacity, 1, null, 'capacity');
      return Scope.open().withError<E>().flatMap(
        (owner) => inTaskScope(_open(resources), owner).flatMap((source) {
          final buffering = _Buffered(source, owner, capacity);
          return inTaskScope(
            Task.addFinalizer(buffering.cleanup),
            owner,
          ).withError<E>().map((_) => buffering.cursor);
        }),
      );
    }),
  );

  /// Emits at most [count] elements and then stops the source.
  ///
  /// Zero leaves the source unopened. A negative count becomes a [RangeError]
  /// defect on consumption. Stopping production leaves user resources in the
  /// consuming task's scope available to subsequent tasks.
  Flow<T, E> take(int count) => Flow._(
    (resources) => Task.defer(() {
      _checkCount(count);
      if (count == 0) return Flow.fromIterable(<T>[])._open(resources);
      return _open(resources).map((source) {
        var remaining = count;
        final pull = Task.defer(() {
          if (remaining == 0) {
            return source
                .finish(const Exit.success(null))
                .withError<E>()
                .map((_) => const Option<Never>.none());
          }
          remaining--;
          return source.pull();
        });
        return FlowCursor(
          pull,
          source.scopeForValue,
          source.finish,
          source.producers,
          resourceScope: source.resourceScope,
        );
      });
    }),
  );

  /// Skips the first [count] elements and emits the remaining elements.
  ///
  /// Negative counts become [RangeError] defects when consumed.
  Flow<T, E> drop(int count) => Flow._(
    (resources) => Task.defer(() {
      _checkCount(count);
      return _open(resources).map((source) {
        var remaining = count;
        Task<Option<T>, E> next() => Task.defer(
          () => source.pull().flatMap((item) {
            if (remaining == 0 || item is OptionNone<T>) {
              return Task.succeed(item);
            }
            remaining--;
            return next();
          }),
        );
        return FlowCursor(
          next(),
          source.scopeForValue,
          source.finish,
          source.producers,
          resourceScope: source.resourceScope,
        );
      });
    }),
  );

  /// Emits elements while [test] returns true, excluding the first false one.
  Flow<T, E> takeWhile(bool Function(T value) test) => _transform(
    _open,
    (source) => source.pull().flatMap((item) {
      if (item case OptionSome(:final value)) {
        if (!test(value)) {
          return source
              .finish(const Exit.success(null))
              .withError<E>()
              .map((_) => const Option<Never>.none());
        }
      }
      return Task.succeed(item);
    }),
  );

  /// Emits through the first element satisfying [test], including that element.
  Flow<T, E> takeUntil(bool Function(T value) test) =>
      _transform(_open, (source) {
        var stopped = false;
        return Task.defer(() {
          if (stopped) {
            return source
                .finish(const Exit.success(null))
                .withError<E>()
                .map((_) => const Option<Never>.none());
          }
          return source.pull().map((item) {
            if (item case OptionSome(:final value)) stopped = test(value);
            return item;
          });
        });
      });

  /// Emits this flow followed by [next].
  ///
  /// [next] starts only after normal completion and internal source cleanup,
  /// except for transport resources still retained downstream. User resources
  /// remain in the consuming task's scope. A failure stops the combined flow.
  Flow<T, E> concat(Flow<T, E> next) => Flow._(
    (resources) => accessTaskEnvironment(
      (owner, _) => _open(resources).map((source) {
        var current = source;
        var switched = false;
        final pull = Task.defer(
          () => current.pull().flatMap((item) {
            if (item is OptionSome<T> || switched) return Task.succeed(item);
            switched = true;
            return inTaskScope(next._open(resources), owner).flatMap((opened) {
              current = opened;
              return current.pull();
            });
          }),
        );
        return FlowCursor(
          pull,
          () => current.scope,
          (exit) => Task.defer(() => current.finish(exit)),
          () => current.producers(),
          resourceScope: resources,
        );
      }),
    ),
  );

  /// Transforms business failures without changing the emitted values.
  ///
  /// Maps errors from opening and reading the source according to
  /// [TaskOperations.mapError]. Defects, cancellation, and cleanup failures are
  /// not transformed.
  Flow<T, F> mapError<F>(F Function(E error) transform) => Flow._(
    (resources) => _open(resources)
        .mapError(transform)
        .map(
          (source) => FlowCursor(
            source.pull().mapError(transform),
            source.scopeForValue,
            source.finish,
            source.producers,
            resourceScope: source.resourceScope,
          ),
        ),
  );

  /// Continues with a replacement flow after a recoverable business failure.
  ///
  /// Uses [TaskOperations.recover] to select the first business error when the
  /// cause contains no defect. The replacement handles the entire eligible cause,
  /// including recorded cancellations. Pure cancellation and failures from the
  /// replacement propagate unchanged. Already delivered values are not repeated.
  ///
  /// Internal producer cleanup finishes before recovery, except for transport
  /// resources still retained downstream. User resources remain in the consuming
  /// task's scope. Immediate cleanup defects prevent recovery; deferred cleanup
  /// failures propagate to the consumer and cannot be recovered here.
  Flow<T, E> recover(Flow<T, E> Function(E error) recover) => Flow._(
    (resources) => accessTaskEnvironment(
      (owner, _) => _open(resources).map((opened) {
        var replaced = false;
        var current = opened;
        final pull = Task.defer(
          () => replaced
              ? current.pull()
              : current
                    .pull()
                    .onExit(
                      (exit) => exit is ExitFailure
                          ? current.finish(exit)
                          : Task.succeed(null),
                    )
                    .recover((error) {
                      replaced = true;
                      return inTaskScope(
                        recover(error)._open(resources),
                        owner,
                      ).flatMap((replacement) {
                        current = replacement;
                        return current.pull();
                      });
                    }),
        );
        return FlowCursor(
          pull,
          () => current.scope,
          (exit) => Task.defer(() => current.finish(exit)),
          () => current.producers(),
          resourceScope: resources,
        );
      }),
    ),
  );

  /// Collects all elements into an immutable list.
  ///
  /// Returns after internal source cleanup. Resources acquired by user tasks
  /// remain available to subsequent tasks in the same scope. Returning handles
  /// beyond that scope does not keep them open. Use a finite flow or [take] to
  /// bound collection size.
  Task<List<T>, E> toList() => Task.defer(() {
    final values = <T>[];
    return forEach((value) => Task.sync(() => values.add(value)))
        .map((_) => List<T>.unmodifiable(values));
  });

  /// Reduces elements in order, returning [initial] for an empty flow.
  ///
  /// Each execution reuses [initial] without copying mutable objects. Exceptions
  /// from [combine] become defects. The result is returned after source cleanup.
  Task<S, E> fold<S>(S initial, S Function(S state, T value) combine) =>
      Task.defer(() {
        var state = initial;
        return forEach(
          (value) => Task.sync(() {
            state = combine(state, value);
          }),
        ).map((_) => state);
      });

  /// Runs [action] for each element, waiting before requesting the next.
  ///
  /// Failure or cancellation stops consumption and waits for cleanup. Resources
  /// acquired by [action] belong to the consuming task's scope and may outlive
  /// consumption. Use [TaskOperations.scoped] on the returned task to close them
  /// after each call.
  Task<void, E> forEach(Task<void, E> Function(T value) action) =>
      accessTaskEnvironment(
        (resources, _) => _open(resources).flatMap((source) {
          Task<void, E> next() => Task.defer(
            () => source.pull().flatMap(
              (item) => switch (item) {
                OptionSome(:final value) => inTaskScope(
                  action(value),
                  resources,
                ).flatMap((_) => next()),
                OptionNone() => Task.succeed(null),
              },
            ),
          );
          return next();
        }).scoped(),
      );

  /// Consumes all elements for their effects and waits for producer cleanup.
  Task<void, E> drain() => forEach((_) => Task.succeed(null));

  /// Exposes this flow as a lazy, single-subscription Dart stream.
  ///
  /// Listening starts consumption in [runtime]; pausing stops further requests.
  /// Use `await for` for asynchronous processing with backpressure, since
  /// [Stream.listen] does not await an asynchronous `onData` callback.
  ///
  /// Consumption failure produces one [TaskException] containing the complete
  /// cause, followed by done. A closed runtime instead reports [RuntimeClosed]
  /// as a defect. A paused listener receives terminal events on resume without
  /// delaying resource cleanup.
  ///
  /// Cancelling the subscription waits for producer cleanup. Its future fails with
  /// [TaskException] for cleanup failures or other completed failures not yet
  /// delivered, including those waiting behind a pause. The cancellation request
  /// itself and errors already sent to the listener are not reported again.
  Stream<T> toStream(Runtime runtime) => _streamFromFlow(this, runtime);
}

/// The overflow policy for [Flow.fromStream]'s adapter buffer.
enum StreamOverflowStrategy {
  /// Fails with a [StreamBufferOverflow] defect after previously buffered data.
  fail,

  /// Discards the incoming data value while preserving buffered values.
  dropping,

  /// Discards the oldest buffered value to retain the incoming value.
  sliding,
}

/// A defect indicating that [Flow.fromStream]'s adapter buffer overflowed.
///
/// Emitted when the bridge uses [StreamOverflowStrategy.fail]. The [capacity]
/// counts buffered data values, not terminal events.
final class StreamBufferOverflow implements Exception {
  /// Creates a diagnostic for the exceeded [capacity].
  const StreamBufferOverflow(this.capacity);

  /// The configured maximum number of buffered data values.
  final int capacity;

  @override
  String toString() =>
      'StreamBufferOverflow: The data buffer exceeded capacity $capacity.';
}

/// Error-type widening for flows without business failures.
extension InfallibleFlowOperations<T> on Flow<T, Never> {
  /// Gives this flow error type [F] without changing its identity or behavior.
  Flow<T, F> withError<F>() => this;
}

void _checkCount(int count) {
  if (count < 0) throw RangeError.range(count, 0, null, 'count');
}

Flow<U, E> _transform<T, U, E>(
  Task<FlowCursor<T, E>, E> Function(Scope resources) open,
  Task<Option<U>, E> Function(FlowCursor<T, E> source) transform,
) => Flow._(
  (resources) => open(resources).map(
    (source) => FlowCursor(
      transform(source),
      source.scopeForValue,
      source.finish,
      source.producers,
      resourceScope: source.resourceScope,
    ),
  ),
);

Task<FlowCursor<T, E>, E> _openCursor<T, E>(
  Task<Task<Option<T>, E>, E> open,
  Scope resources, {
  required bool ownsResources,
}) => Scope.open().map((scope) {
  final producer = FlowProducer(scope);
  Task<Option<T>, E>? next;
  var closed = false;
  Task<void, Never> finish(Exit<Object?, Object?> exit) => Task.defer(() {
    closed = true;
    return producer.finish(exit);
  });
  final pull = Task.defer(() {
    if (closed) return Task.succeed(const Option<Never>.none());
    final read = Task.defer(() {
      final initialized = next;
      if (initialized != null) return initialized;
      return open.flatMap((initialized) {
        next = initialized;
        return initialized;
      });
    });
    return inTaskScope(read, ownsResources ? scope : resources).flatMap((item) {
      if (item is OptionSome<T>) return Task.succeed(item);
      closed = true;
      return producer
          .finish(const Exit.success(null))
          .withError<E>()
          .map((_) => item);
    });
  });
  return FlowCursor(
    pull,
    () => scope,
    finish,
    () => [producer],
    resourceScope: resources,
  );
});

final class _StreamSource<T> {
  _StreamSource(this.capacity, this.overflow);

  final int capacity;
  final StreamOverflowStrategy overflow;
  final _values = Queue<T>();
  StreamSubscription<T>? _subscription;
  void Function(Exit<void, Never>)? _waiting;
  (Object, StackTrace)? _error;
  final _faults = <FailureEvent>[];
  var _ended = false;
  var _closed = false;
  var _paused = false;

  void listen(Stream<T> stream) {
    _subscription = stream.listen(
      _data,
      onError: (Object error, StackTrace stackTrace) {
        if (_closed || _ended) return;
        _error = (error, stackTrace);
        _ended = true;
        _pause();
        _wake();
      },
      onDone: () {
        if (_closed || _ended) return;
        _ended = true;
        _wake();
      },
    );
    // A custom stream may invoke callbacks before listen returns its handle.
    if (_paused) _pauseSubscription();
  }

  void _data(T value) {
    if (_closed || _ended) return;
    if (_values.length == capacity) {
      switch (overflow) {
        case StreamOverflowStrategy.fail:
          _fault(StreamBufferOverflow(capacity), StackTrace.current);
        case StreamOverflowStrategy.dropping:
          break;
        case StreamOverflowStrategy.sliding:
          _values.removeFirst();
          _values.addLast(value);
      }
    } else {
      _values.addLast(value);
    }
    _pause();
    _wake();
  }

  void _pause() {
    if (_paused) return;
    _paused = true;
    _pauseSubscription();
  }

  void _pauseSubscription() {
    try {
      _subscription?.pause();
    } catch (error, stackTrace) {
      _fault(error, stackTrace);
    }
  }

  void _fault(Object error, StackTrace stackTrace) {
    _faults.add(FailureEvent(CauseDefect(error, stackTrace), FailureSource()));
    _ended = true;
  }

  Task<A, Never> _failure<A>() => taskFromEvaluation(
    TaskEvaluation(
      Exit<A, Never>.failure(
        Cause.combine(_faults.map((event) => event.cause as CauseDefect)),
      ),
      List.unmodifiable(_faults),
    ),
  );

  void _wake() {
    final waiting = _waiting;
    _waiting = null;
    waiting?.call(const Exit.success(null));
  }

  Task<Option<T>, E> pull<E>(
    E Function(Object error, StackTrace stackTrace) onError,
  ) =>
      Task.fromCallback<void, Never>((complete) {
        if (_values.isNotEmpty || _ended) {
          complete(const Exit.success(null));
        } else {
          _waiting = complete;
          if (_paused) {
            _paused = false;
            try {
              _subscription!.resume();
            } catch (error, stackTrace) {
              _fault(error, stackTrace);
              _wake();
            }
          }
        }
        return () {
          if (identical(_waiting, complete)) _waiting = null;
        };
      }).withError<E>().flatMap((_) {
        if (_values.isNotEmpty) {
          return Task.succeed(Option.some(_values.removeFirst()));
        }
        if (_error case final error?) {
          return Task.fail(onError(error.$1, error.$2));
        }
        if (_faults.isNotEmpty) return _failure<Option<T>>();
        return Task.succeed(const Option<Never>.none());
      });

  Task<void, Never> close() => Task.defer(() {
    if (_closed) return Task.succeed(null);
    _closed = true;
    _values.clear();
    _waiting = null;
    // Reuse occurrence identities when a fault was already delivered by pull.
    // Scope combines body and cleanup without duplicating that same failure.
    final Task<void, Never> pending = _faults.isNotEmpty
        ? _failure<void>()
        : Task.succeed(null);
    final subscription = _subscription;
    return subscription == null
        ? pending
        : pending.ensuring(
            Task.fromFuture(
              subscription.cancel,
              onError: (error, stackTrace) =>
                  Error.throwWithStackTrace(error, stackTrace),
            ),
          );
  });
}

Stream<T> _streamFromFlow<T, E>(Flow<T, E> flow, Runtime runtime) {
  final cancellation = CancellationController();
  final cancelReason = Object();
  final completion = Completer<Exit<void, E>>();
  late final StreamController<T> controller;
  void Function(Exit<void, Never>)? resumed;
  var cancelled = false;
  Exit<void, E>? terminal;
  var terminalDelivered = false;

  Task<void, Never> ready() => Task.defer(() {
    if (!controller.isPaused) return Task.succeed(null);
    return Task.fromCallback<void, Never>((complete) {
      resumed = complete;
      // Resume can happen between the preceding Task.defer and registration.
      if (!controller.isPaused) {
        resumed = null;
        complete(const Exit.success(null));
      }
      return () {
        if (identical(resumed, complete)) resumed = null;
      };
    }).flatMap((_) => ready());
  });

  Task<void, Never> deliver(T value) => Task.defer(
    () => Task.sync(() {
      // Keep the pause check and synchronous delivery in one executor step so a
      // pause at a scheduling boundary cannot move data into a hidden buffer.
      if (controller.isPaused) return false;
      controller.add(value);
      return true;
    }).flatMap((sent) => sent ? ready() : ready().flatMap((_) => deliver(value))),
  );

  void deliverTerminal() {
    final exit = terminal;
    if (exit == null || cancelled || terminalDelivered || controller.isPaused) {
      return;
    }
    // Delivery is synchronous. Mark it before addError because cancelOnError
    // invokes onCancel before dispatching that error to the listener.
    terminalDelivered = true;
    if (exit case ExitFailure(:final cause)) {
      controller.addError(TaskException(cause), _streamStack(cause));
    }
    unawaited(controller.close());
  }

  void start() {
    if (cancelled) {
      completion.complete(Exit.failure(Cause.cancelled(cancelReason)));
      return;
    }
    final running = runtime.run(
      ready().withError<E>().flatMap((_) => flow.forEach(deliver)),
      signal: cancellation.signal,
    );
    unawaited(
      running.then((exit) {
        terminal = exit;
        completion.complete(exit);
        // Retain terminal failure outside the native subscription's queue so a
        // later cancel can observe it. Runtime cleanup is already complete.
        deliverTerminal();
      }),
    );
  }

  controller = StreamController<T>(
    sync: true,
    // Native stream listeners must receive their subscription before any data.
    onListen: () => scheduleMicrotask(start),
    onResume: () {
      final waiting = resumed;
      resumed = null;
      waiting?.call(const Exit.success(null));
      // The native subscription is in its resume callback. Delivering there
      // would enqueue the error instead of observing it synchronously.
      if (terminal != null) scheduleMicrotask(deliverTerminal);
    },
    onCancel: () {
      if (terminalDelivered) return null;
      cancelled = true;
      cancellation.requestCancel(reason: cancelReason);
      return completion.future.then((exit) {
        if (exit case ExitFailure(:final cause)) {
          final reasons = switch (cause) {
            CauseGroup(:final causes) => causes,
            _ => [cause],
          };
          final retained = reasons
              .where(
                (reason) =>
                    reason is! CauseCancelled ||
                    !identical(reason.reason, cancelReason),
              )
              .toList();
          if (retained.isNotEmpty) {
            final failure = Cause.combine(retained);
            Error.throwWithStackTrace(
              TaskException(failure),
              _streamStack(failure),
            );
          }
        }
      });
    },
  );
  return controller.stream;
}

StackTrace _streamStack(Cause<Object?> cause) {
  final reasons = switch (cause) {
    CauseGroup(:final causes) => causes,
    _ => [cause],
  };
  for (final reason in reasons) {
    if (reason case CauseDefect(:final stackTrace)) return stackTrace;
  }
  return StackTrace.current;
}

final class _BufferedEntry<T> {
  _BufferedEntry(this.value, this.scope, this.producers);

  final T value;
  final Scope scope;
  final List<FlowProducer> producers;
}

final class _Buffered<T, E> {
  _Buffered(this.source, this.owner, this.capacity);

  final FlowCursor<T, E> source;
  final Scope owner;
  final int capacity;
  final _stopReason = Object();
  final _values = Queue<_BufferedEntry<T>>();
  late final _lifetime = FlowProducer(owner);
  _BufferedEntry<T>? _current;
  Fiber<void, E>? _driver;
  TaskEvaluation<void, E>? _terminal;
  List<CauseCancelled> _explicitCancellation = const [];
  void Function()? _consumerWake;
  void Function()? _driverWake;
  var _started = false;
  var _stopping = false;
  var _stoppedByUs = false;
  var _terminalDelivered = false;

  late final cursor = FlowCursor<T, E>(
    _pull(),
    () => _current?.scope ?? owner,
    _finish,
    () => [_lifetime, ...?_current?.producers],
    resourceScope: source.resourceScope,
  );

  Task<Option<T>, E> _pull() => Task.defer(() {
    final previous = _current;
    if (previous != null) {
      _current = null;
      return _release(previous.producers)
          .withError<E>()
          .flatMap((_) => _pull());
    }
    if (!_started) {
      _started = true;
      return forkTaskIn(
        _produce().mapCause((cause) {
          // This callback sees explicit cancellation, but owner-requested
          // cancellation bypasses it. Keep cleanup cancellation without replaying
          // the scope's own request during automatic shutdown.
          _explicitCancellation = _causes(cause)
              .whereType<CauseCancelled>()
              .toList();
          return cause;
        }),
        owner,
        resourceScope: source.resourceScope,
        inheritHost: true,
      ).withError<E>().flatMap((fiber) {
        _driver = fiber;
        fiber.onExit((exit) {
          _terminal = TaskEvaluation(
            exit,
            fiberFailureEvents(fiber),
            cleanupCancellations: fiberCleanupCancellations(fiber),
          );
          if (exit case ExitFailure(:final cause) when _stoppedByUs) {
            _explicitCancellation = _causes(cause)
                .whereType<CauseCancelled>()
                .where((cause) => !identical(cause.reason, _stopReason))
                .toList();
          }
          _explicitCancellation = [
            ..._explicitCancellation,
            ...fiberCleanupCancellations(fiber),
          ];
          _consumerWake?.call();
        });
        return _pull();
      });
    }
    if (_values.isNotEmpty) {
      final entry = _values.removeFirst();
      _current = entry;
      _driverWake?.call();
      return Task.succeed(Option.some(entry.value));
    }
    final terminal = _terminal;
    if (terminal != null) {
      if (terminal.exit is ExitFailure<void, E>) {
        return taskFromEvaluation(
          TaskEvaluation(
            terminal.exit.map((_) => const Option<Never>.none()),
            terminal.events,
            cleanupCancellations: terminal.cleanupCancellations,
          ),
        ).mapCause((cause) {
          _terminalDelivered = true;
          return cause;
        });
      }
      return _lifetime
          .finish(const Exit.success(null))
          .withError<E>()
          .map((_) => const Option.none());
    }
    return _wait().withError<E>().flatMap((_) => _pull());
  });

  Task<void, E> _produce() => Task.defer(() {
    if (_stopping) return Task.succeed(null);
    if (_values.length == capacity) {
      return Task.fromCallback<void, Never>((complete) {
        void notify() {
          _driverWake = null;
          complete(const Exit.success(null));
        }

        _driverWake = notify;
        if (_stopping || _values.length < capacity) notify();
        return () {
          if (identical(_driverWake, notify)) _driverWake = null;
        };
      }).withError<E>().flatMap((_) => _produce());
    }
    // The sole driver reserves the remaining slot until this pull finishes.
    return source.pull().flatMap((item) {
      if (item is OptionNone<T>) return Task.succeed(null);
      final producers = source.producers().toSet().toList();
      for (final producer in producers) {
        producer.retain();
      }
      _values.addLast(
        _BufferedEntry((item as OptionSome<T>).value, source.scope, producers),
      );
      _consumerWake?.call();
      return _produce();
    });
  });

  Task<void, Never> _wait() => Task.fromCallback((complete) {
    void notify() {
      _consumerWake = null;
      complete(const Exit.success(null));
    }

    _consumerWake = notify;
    if (_terminal != null || (!_stopping && _values.isNotEmpty)) notify();
    return () {
      if (identical(_consumerWake, notify)) _consumerWake = null;
    };
  });

  void _stop() {
    if (_stopping) return;
    _stopping = true;
    // Scope marks its descendants before stopping their fibers. If it already
    // owns shutdown, its cleanup path retains cancellation from those resources.
    _stoppedByUs = !scopeIsClosing(owner);
    _driver?.requestCancel(reason: _stopReason);
    _driverWake?.call();
  }

  Task<void, Never> _finish(Exit<Object?, Object?> exit) => Task.defer(() {
    _stop();
    if (_started && _terminal == null) {
      return _wait().flatMap((_) => _finish(exit));
    }
    final producers = [
      ...?_current?.producers,
      for (final entry in _values) ...entry.producers,
    ];
    _current = null;
    _values.clear();
    return source
        .finish(exit)
        .ensuring(_release(producers))
        .ensuring(_lifetime.finish(exit));
  });

  Task<void, Never> _release(List<FlowProducer> producers) =>
      Task.uninterruptibleMask(
        (_) => Task.fromFuture(
          () => releaseFlowProducers(producers),
          onError: Error.throwWithStackTrace,
        ).flatMap(taskFromEvaluation),
      );

  Task<void, Never> cleanup(Exit<Object?, Object?> exit) => Task.defer(() {
    // The owner already stopped its driver and closed all input scopes. Do not
    // wait on leases to resources owned by that same closing scope.
    _stop();
    _current = null;
    _values.clear();
    final events = [
      for (final event in _terminal?.events ?? const <FailureEvent>[])
        if (event.cause is CauseDefect) event,
    ];
    final cancellations = [..._explicitCancellation];
    final cleanups = [...?_terminal?.cleanupCancellations];
    final causes = <Cause<Never>>[];
    if (_terminal?.exit case ExitFailure(:final cause)) {
      for (final reason in _causes(cause)) {
        if (reason is CauseDefect ||
            (reason is CauseCancelled &&
                !_terminalDelivered &&
                cancellations.remove(reason))) {
          causes.add(reason as Cause<Never>);
        }
      }
    }
    return taskFromEvaluation(
      TaskEvaluation(
        causes.isEmpty
            ? const Exit.success(null)
            : Exit.failure(Cause.combine(causes)),
        events,
        cleanupCancellations: [
          for (final cause in causes)
            if (cause is CauseCancelled && cleanups.remove(cause)) cause,
        ],
      ),
    );
  });
}

final class _MapEntry<T, E> {
  _MapEntry(this.producer);

  final FlowProducer producer;
  TaskEvaluation<T, E>? result;
}

final class _MapFailure<E> {
  _MapFailure(this.result, this.cancellations);

  final TaskEvaluation<Object?, E> result;
  final List<CauseCancelled> cancellations;
}

final class _ConcurrentMap<T, U, E> {
  _ConcurrentMap(
    this.source,
    this.owner,
    this.transform,
    this.concurrency,
    this.preserveOrder,
  );

  final FlowCursor<T, E> source;
  final Scope owner;
  final Task<U, E> Function(T) transform;
  final int concurrency;
  final bool preserveOrder;
  final _stopReason = Object();
  final _admitted = <_MapEntry<U, E>>{};
  final _resources = <_MapEntry<U, E>>{};
  late final _lifetime = FlowProducer(owner);
  final _ordered = Queue<_MapEntry<U, E>>();
  final _ready = Queue<_MapEntry<U, E>>();
  final _failures = <_MapFailure<E>>[];
  final _active = <Fiber<U, E>>{};
  Fiber<void, E>? _driver;
  _MapEntry<U, E>? _current;
  void Function()? _consumerWake;
  void Function()? _driverWake;
  var _started = false;
  var _driverDone = false;
  var _ended = false;
  var _stopping = false;
  var _failureDelivered = false;

  late final cursor = FlowCursor<U, E>(
    _pull(),
    () => _current?.producer.scope ?? owner,
    _finish,
    () => [if (_current case final current?) current.producer],
    resourceScope: source.resourceScope,
  );

  Task<Option<U>, E> _pull() => Task.defer(() {
    final previous = _current;
    if (previous != null) {
      _current = null;
      _admitted.remove(previous);
      return previous.producer
          .finish(const Exit.success(null))
          .withError<E>()
          .flatMap((_) {
            _notify();
            return _pull();
          });
    }
    if (!_started) {
      _started = true;
      final cancellations = <CauseCancelled>[];
      return forkTaskIn(
        _produce().mapCause((cause) {
          cancellations.addAll(_causes(cause).whereType<CauseCancelled>());
          return cause;
        }),
        owner,
        resourceScope: source.resourceScope,
        inheritHost: true,
      ).withError<E>().flatMap((fiber) {
        _driver = fiber;
        fiber.onExit((exit) {
          _driverDone = true;
          if (exit is ExitFailure<void, E>) {
            _recordFailure(fiber, exit, cancellations);
          }
          _notify();
        });
        return _pull();
      });
    }
    if (_stopping) {
      if (!_driverDone || _active.isNotEmpty) {
        return _wait().withError<E>().flatMap((_) => _pull());
      }
      return taskFromEvaluation(
        _failure<Option<U>, E>(() => const Option.none()),
      ).mapCause((cause) {
        _failureDelivered = true;
        return cause;
      });
    }
    final ready = preserveOrder ? _ordered : _ready;
    if (ready.isNotEmpty && ready.first.result != null) {
      final entry = ready.removeFirst();
      if (preserveOrder) {
        _ready.remove(entry);
      } else {
        _ordered.remove(entry);
      }
      _current = entry;
      return Task.succeed(
        Option.some((entry.result!.exit as ExitSuccess<U, E>).value),
      );
    }
    if (_ended && _admitted.isEmpty && _driverDone) {
      return _lifetime
          .finish(const Exit.success(null))
          .withError<E>()
          .map((_) => const Option.none());
    }
    return _wait().withError<E>().flatMap((_) => _pull());
  });

  Task<void, E> _produce() => Task.defer(() {
    if (_stopping || _ended) return Task.succeed(null);
    if (_admitted.length >= concurrency) {
      return Task.fromCallback<void, Never>((complete) {
        void notify() {
          _driverWake = null;
          complete(const Exit.success(null));
        }

        _driverWake = notify;
        if (_stopping || _admitted.length < concurrency) notify();
        return () {
          if (identical(_driverWake, notify)) _driverWake = null;
        };
      }).withError<E>().flatMap((_) => _produce());
    }
    return source.pull().flatMap((item) {
      if (item is OptionNone<T>) {
        _ended = true;
        _notify();
        return Task.succeed(null);
      }
      final value = (item as OptionSome<T>).value;
      final cancellations = <CauseCancelled>[];
      return Task.sync(() {
        final inputs = source.producers().toSet().toList();
        final scope = createScope(parent: source.scope);
        for (final input in inputs) {
          input.retain();
        }
        _lifetime.retain();
        late final _MapEntry<U, E> entry;
        final producer = FlowProducer(
          scope,
          afterClose: (exit) async {
            final results = <TaskEvaluation<void, Never>>[];
            for (final input in inputs.reversed) {
              results.add(await input.release());
            }
            _resources.remove(entry);
            results.add(await _lifetime.release());
            return mergeFlowEvaluations(results);
          },
        );
        entry = _MapEntry(producer);
        _admitted.add(entry);
        _resources.add(entry);
        _ordered.addLast(entry);
        return entry;
      }).withError<E>().flatMap(
        (entry) =>
            forkTaskIn(
              inTaskScope(
                Task.defer(() => transform(value)),
                source.resourceScope,
              ).mapCause((cause) {
                cancellations.addAll(
                  _causes(cause).whereType<CauseCancelled>(),
                );
                return cause;
              }),
              owner,
              resourceScope: source.resourceScope,
              inheritHost: true,
            ).withError<E>().flatMap((fiber) {
              _active.add(fiber);
              if (_stopping) fiber.requestCancel(reason: _stopReason);
              fiber.onExit((exit) {
                _active.remove(fiber);
                final result = TaskEvaluation(
                  exit,
                  fiberFailureEvents(fiber),
                  cleanupCancellations: fiberCleanupCancellations(fiber),
                );
                entry.result = result;
                if (exit is ExitFailure<U, E>) {
                  _recordFailure(fiber, exit, cancellations);
                } else {
                  _ready.addLast(entry);
                }
                _notify();
              });
              return _produce();
            }),
      );
    });
  });

  bool get _canPull {
    if (_stopping) return _driverDone && _active.isEmpty;
    final ready = preserveOrder ? _ordered : _ready;
    return (ready.isNotEmpty && ready.first.result != null) ||
        (_ended && _admitted.isEmpty && _driverDone);
  }

  Task<void, Never> _wait() => Task.fromCallback((complete) {
    void notify() {
      _consumerWake = null;
      complete(const Exit.success(null));
    }

    _consumerWake = notify;
    if (_canPull) notify();
    return () {
      if (identical(_consumerWake, notify)) _consumerWake = null;
    };
  });

  void _notify() {
    _driverWake?.call();
    _consumerWake?.call();
  }

  void _stop() {
    if (_stopping) return;
    _stopping = true;
    _driver?.requestCancel(reason: _stopReason);
    for (final fiber in _active.toList()) {
      fiber.requestCancel(reason: _stopReason);
    }
    _notify();
  }

  void _recordFailure<A>(
    Fiber<A, E> fiber,
    ExitFailure<A, E> exit,
    List<CauseCancelled> cancellations,
  ) {
    // Cleanup provenance survives a pending owner cancellation even when the
    // cleanup's scope has already closed and detached from the worker.
    _failures.add(
      _MapFailure(
        TaskEvaluation(
          exit,
          fiberFailureEvents(fiber),
          cleanupCancellations: fiberCleanupCancellations(fiber),
        ),
        [...cancellations, ...fiberCleanupCancellations(fiber)],
      ),
    );
    _stop();
  }

  Task<void, Never> _finish(Exit<Object?, Object?> exit) => Task.defer(() {
    _stop();
    if (_started && (!_driverDone || _active.isNotEmpty)) {
      return _wait().flatMap((_) => _finish(exit));
    }
    final entries = _resources.toList().reversed.toList();
    _admitted.clear();
    _ordered.clear();
    _ready.clear();
    _current = null;
    Task<void, Never> finishAt(int index) => Task.defer(
      () => index == entries.length
          ? _lifetime.finish(exit)
          : entries[index].producer.finish(exit).ensuring(finishAt(index + 1)),
    );
    return finishAt(0);
  });

  Task<void, Never> cleanup(Exit<Object?, Object?> exit) => Task.defer(() {
    // Scope has already stopped every worker and closed the producer tree.
    // Waiting for leases here would wait on resources this same scope owns.
    _stop();
    _admitted.clear();
    _resources.clear();
    _ordered.clear();
    _ready.clear();
    _current = null;
    return taskFromEvaluation(_failure<void, Never>(() {}, defectsOnly: true));
  });

  TaskEvaluation<A, F> _failure<A, F>(
    A Function() success, {
    bool defectsOnly = false,
  }) {
    final causes = <Cause<F>>[];
    final events = <FailureEvent>[];
    final cleanupCancellations = <CauseCancelled>[];
    final seen = <FailureSource>{};
    for (final failure in _failures) {
      final result = failure.result;
      final cancellations = [...failure.cancellations];
      final cleanups = [...result.cleanupCancellations];
      if (result.exit case ExitFailure(:final cause)) {
        final sources = Map<Cause<Object?>, Queue<FailureEvent>>.identity();
        for (final event in result.events) {
          (sources[event.cause] ??= Queue()).addLast(event);
        }
        for (final reason in _causes(cause)) {
          switch (reason) {
            case CauseDefect():
              final event = sources[reason]!.removeFirst();
              if (seen.add(event.source)) {
                causes.add(reason);
                events.add(event);
              }
            case CauseFailure(:final error):
              final event = sources[reason]!.removeFirst();
              if (!defectsOnly && seen.add(event.source)) {
                final failure = Cause.failure(error as F);
                causes.add(failure);
                events.add(FailureEvent(failure, event.source));
              }
            case CauseCancelled():
              if ((!defectsOnly ||
                      (!_failureDelivered && cancellations.remove(reason))) &&
                  !identical(reason.reason, _stopReason)) {
                causes.add(reason);
                if (cleanups.remove(reason)) {
                  cleanupCancellations.add(reason);
                }
              }
            case CauseGroup():
              throw StateError('Cause groups must be flat.');
          }
        }
      }
    }
    return TaskEvaluation(
      causes.isEmpty
          ? Exit.success(success())
          : Exit.failure(Cause.combine(causes)),
      List.unmodifiable(events),
      cleanupCancellations: List.unmodifiable(cleanupCancellations),
    );
  }
}

List<Cause<E>> _causes<E>(Cause<E> cause) => switch (cause) {
  CauseGroup(:final causes) => causes,
  _ => [cause],
};

final class _Grouped<T, E> {
  _Grouped(this.source, this.owner, this.size);

  final FlowCursor<T, E> source;
  final Scope owner;
  final int size;
  late final _lifetime = FlowProducer(owner);
  final _values = <T>[];
  final _inputs = <FlowProducer>[];
  FlowProducer? _current;
  var _ended = false;

  late final cursor = FlowCursor<List<T>, E>(
    _pull(),
    () => _current?.scope ?? owner,
    _finish,
    () => [?_current],
    resourceScope: source.resourceScope,
  );

  Task<Option<List<T>>, E> _pull() => Task.defer(() {
    final previous = _current;
    if (previous != null) {
      _current = null;
      return previous
          .finish(const Exit.success(null))
          .withError<E>()
          .flatMap((_) => _pull());
    }
    if (_ended) {
      return _lifetime
          .finish(const Exit.success(null))
          .withError<E>()
          .map((_) => const Option.none());
    }
    return _collect().onExit(
      (exit) => exit is ExitFailure ? _finish(exit) : Task.succeed(null),
    );
  });

  Task<Option<List<T>>, E> _collect() => Task.defer(
    () => source.pull().flatMap((item) {
      if (item case OptionSome(:final value)) {
        final inputs = source.producers().toSet();
        for (final producer in inputs) {
          producer.retain();
          _inputs.add(producer);
        }
        _values.add(value);
        if (_values.length < size) return _collect();
        return Task.succeed(Option.some(_emit()));
      }
      _ended = true;
      if (_values.isNotEmpty) return Task.succeed(Option.some(_emit()));
      return _lifetime
          .finish(const Exit.success(null))
          .withError<E>()
          .map((_) => const Option.none());
    }),
  );

  List<T> _emit() {
    final values = List<T>.unmodifiable(_values);
    final inputs = _inputs.toList();
    _values.clear();
    _inputs.clear();
    final scope = createScope(parent: owner);
    _lifetime.retain();
    _current = FlowProducer(
      scope,
      afterClose: (_) async {
        final released = await releaseFlowProducers(inputs);
        return mergeFlowEvaluations([released, await _lifetime.release()]);
      },
    );
    return values;
  }

  Task<void, Never> _finish(Exit<Object?, Object?> exit) => Task.defer(() {
    _ended = true;
    final current = _current;
    _current = null;
    final inputs = _inputs.toList();
    _inputs.clear();
    _values.clear();
    final release = Task.uninterruptibleMask<void, Never>(
      (_) => Task.fromFuture(
        () => releaseFlowProducers(inputs),
        onError: Error.throwWithStackTrace,
      ).flatMap(taskFromEvaluation),
    );
    return source
        .finish(exit)
        .ensuring(current?.finish(exit) ?? Task.succeed(null))
        .ensuring(release)
        .ensuring(_lifetime.finish(exit));
  });
}

// Internal bridges for flow operators in other libraries; not exported.
Task<FlowCursor<T, E>, E> openFlowCursor<T, E>(
  Flow<T, E> flow, {
  Scope? resourceScope,
}) => resourceScope == null
    ? accessTaskEnvironment((scope, _) => flow._open(scope))
    : flow._open(resourceScope);

Flow<T, E> flowFromCursor<T, E>(
  Task<FlowCursor<T, E>, E> Function(Scope resources) open,
) => Flow._(open);
