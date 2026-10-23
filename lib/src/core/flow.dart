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

/// A lazy sequence whose production, errors, and resources use [Task].
///
/// Consumers request one element at a time. Each consumption opens independent
/// iteration state and a scope; finishing, failing, or cancelling consumption
/// waits for producer cleanup. A produced value's resources stay available
/// while its consuming callback runs. External objects, including queues and
/// iterables, retain their own sharing and ownership rules.
///
/// Consume a flow with [FlowOperations.toList], [FlowOperations.fold],
/// [FlowOperations.forEach], or [FlowOperations.drain]. No work starts merely
/// by constructing or transforming a flow. Exceptions from user callbacks
/// become defects, following Task's error rules.
final class Flow<T, E> {
  Flow._(this._open);

  Flow._source(Task<Task<Option<T>, E>, E> open) : _open = _openCursor(open);

  final Task<FlowCursor<T, E>, E> _open;

  /// Reads [values] lazily using a new iterator for each consumption.
  ///
  /// The iterable is not copied. Its iterator is created when consumption
  /// starts; iteration exceptions become defects. Nullable values remain
  /// elements, distinct from completion.
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

  /// Runs [task] once per consumption and emits its successful value.
  ///
  /// Resources acquired by [task] remain alive until the consumer requests the
  /// end of the flow or exits. Task failure produces no element.
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

  /// Takes elements from [queue] without taking ownership of the queue.
  ///
  /// Normal queue completion drains accepted elements before ending this flow.
  /// A closed queue fails with [QueueClosed]. Stopping this consumption removes
  /// its pending take but does not close the queue. Concurrent consumers compete
  /// for elements; they do not each receive a copy.
  static Flow<T, QueueClosed> fromQueue<T>(AsyncQueue<T> queue) =>
      Flow._source(Task.succeed(queue.take()));

  /// Subscribes to a new stream from [create] for each consumption.
  ///
  /// The first stream error ends production. Previously buffered values are
  /// delivered before [onError] maps that error and its original stack trace to
  /// a business failure. Exceptions from [create], `listen`, or [onError] become
  /// defects. The subscription is cancelled, and its cancellation future is
  /// awaited, when consumption ends for any reason.
  ///
  /// [bufferCapacity] must be positive and bounds data retained by this bridge.
  /// The subscription is paused while data awaits consumption and resumed when
  /// another element is requested. A source may buffer internally despite
  /// pause; this limit does not bound memory outside the bridge. [overflow]
  /// handles data delivered beyond the bridge's capacity. Error and completion
  /// signals are retained separately and are never dropped by this policy.
  /// An already detected overflow is retained during cleanup even if an early
  /// consumer limit stops before that failure would be pulled.
  ///
  /// Invalid capacity becomes a [RangeError] defect before [create] is called.
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
  );

  /// Repeatedly applies [step] to the current state, starting at [initial].
  ///
  /// A present record emits its first field and uses its second as the next
  /// state. [OptionNone] ends the sequence. Each consumption starts with
  /// [initial]; mutable objects supplied as state are not cloned. The next step
  /// does not run until the previous element has been consumed.
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

/// Transforms and consumes flows using their statically known types.
extension FlowOperations<T, E> on Flow<T, E> {
  /// Transforms each element with [transform], preserving order.
  ///
  /// Exceptions from [transform] become defects and close the producer.
  Flow<U, E> map<U>(U Function(T value) transform) => _transform(
    _open,
    (source) => source.pull().map((item) => item.map(transform)),
  );

  /// Runs [transform] with at most [concurrency] outstanding elements.
  ///
  /// By default, results retain input order. Set [preserveOrder] to false to
  /// emit them in completion order. Input is always pulled sequentially. The
  /// limit includes running tasks, completed results awaiting delivery, and the
  /// last delivered element until downstream requests another. With the default
  /// limit of one there is no prefetch. Downstream buffering retains its own
  /// resource leases without continuing to occupy this mapping's limit.
  ///
  /// Each result has a scope for resources acquired by its mapping task and
  /// downstream callbacks. They are released after that element is consumed,
  /// before releasing its input resources. Input resources remain available
  /// even if prefetch advances to another inner flow or concatenated branch.
  /// Use Task.scoped inside [transform] for resources needed only during mapping.
  ///
  /// Failure stops further input and cancels unfinished work, waiting for its
  /// cleanup. Early termination also waits and retains already observed defects.
  /// [concurrency] must be positive; invalid values become [RangeError] defects
  /// before upstream work starts.
  Flow<U, E> mapTask<U>(
    Task<U, E> Function(T value) transform, {
    int concurrency = 1,
    bool preserveOrder = true,
  }) => Flow._(
    Task.defer(() {
      if (concurrency < 1) {
        throw RangeError.range(concurrency, 1, null, 'concurrency');
      }
      return Scope.open().withError<E>().flatMap(
        (owner) => inTaskScope(_open, owner).flatMap((source) {
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

  /// Runs [test] sequentially and keeps elements whose result is true.
  ///
  /// Rejected elements are consumed without being delivered downstream.
  /// Task resources use the same lifetime rules as [mapTask].
  Flow<T, E> filterTask(Task<bool, E> Function(T value) test) =>
      _transform(_open, (source) {
        Task<Option<T>, E> next() => Task.defer(
          () => source.pull().flatMap(
            (item) => switch (item) {
              OptionSome(:final value) => inTaskScope(
                test(value),
                source.scope,
              ).flatMap((keep) => keep ? Task.succeed(item) : next()),
              OptionNone() => Task.succeed(item),
            },
          ),
        );
        return next();
      });

  /// Consumes each flow returned by [transform] fully before requesting another
  /// element from this flow.
  ///
  /// Resources from the outer element remain available to its inner flow.
  /// Downstream concurrent mapping can keep an earlier inner producer alive
  /// while its emitted values are still in use. Otherwise each inner producer
  /// closes before the next opens. Failure stops further production.
  Flow<U, E> flatMap<U>(Flow<U, E> Function(T value) transform) => Flow._(
    _open.map((source) {
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
              inTaskScope(transform(value)._open, source.scope).flatMap((
                opened,
              ) {
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
      );
    }),
  );

  /// Emits the accumulated state after each element, starting at [initial].
  ///
  /// The initial state is not emitted separately. Each consumption starts at
  /// [initial]; mutable state is not cloned. Exceptions from [combine] become
  /// defects.
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
  /// A normal end emits the last partial group. Failure discards a partial group
  /// and releases its resources. Each group's input resources remain alive until
  /// that group is consumed, including when upstream mapping or recovery moves
  /// to another producer. This operation retains at most [size] input elements
  /// per group under construction, in addition to groups retained downstream.
  ///
  /// [size] must be positive; invalid values become [RangeError] defects before
  /// opening the source.
  Flow<List<T>, E> grouped(int size) => Flow._(
    Task.defer(() {
      if (size < 1) throw RangeError.range(size, 1, null, 'size');
      return Scope.open().withError<E>().flatMap(
        (owner) => inTaskScope(
          _open,
          owner,
        ).map((source) => _Grouped(source, owner, size).cursor),
      );
    }),
  );

  /// Prefetches up to [capacity] elements while downstream consumes a value.
  ///
  /// One producer pulls upstream sequentially. Buffered values keep their input
  /// order and original resource scopes; downstream callbacks still run in the
  /// consuming task. Production starts on the first pull of each consumption.
  ///
  /// The limit includes queued values and an in-flight pull, but excludes the
  /// value currently delivered downstream. This operation therefore holds at
  /// most `capacity + 1` elements, apart from leases retained by later operators.
  /// A full buffer pauses pulling until a queued value is delivered.
  ///
  /// Buffered values are delivered before an upstream failure or normal end.
  /// Early termination cancels the producer and waits for its cleanup. Consumer
  /// cancellation discards business failures and explicit cancellation produced
  /// by unfinished source work; defects and resource cleanup failures are still
  /// retained. [capacity] must be positive; invalid values become [RangeError]
  /// defects before upstream opens.
  Flow<T, E> buffer(int capacity) => Flow._(
    Task.defer(() {
      if (capacity < 1) throw RangeError.range(capacity, 1, null, 'capacity');
      return Scope.open().withError<E>().flatMap(
        (owner) => inTaskScope(_open, owner).flatMap((source) {
          final buffering = _Buffered(source, owner, capacity);
          return inTaskScope(
            Task.addFinalizer(buffering.cleanup),
            owner,
          ).withError<E>().map((_) => buffering.cursor);
        }),
      );
    }),
  );

  /// Emits at most [count] elements, then closes the upstream producer.
  ///
  /// Zero does not open the upstream flow. Negative counts become [RangeError]
  /// defects when consumed. The last element's resources remain available to
  /// its consuming callback before cleanup starts.
  Flow<T, E> take(int count) => Flow._(
    Task.defer(() {
      _checkCount(count);
      if (count == 0) return Flow.fromIterable(<T>[])._open;
      return _open.map((source) {
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
        );
      });
    }),
  );

  /// Skips the first [count] elements and emits the remaining elements.
  ///
  /// Negative counts become [RangeError] defects when consumed.
  Flow<T, E> drop(int count) => Flow._(
    Task.defer(() {
      _checkCount(count);
      return _open.map((source) {
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

  /// Starts [next] after this producer reaches its end.
  ///
  /// Cleanup normally finishes before [next] starts. Downstream concurrent
  /// mapping can keep earlier values in use; their producer stays alive until
  /// those values finish consumption. Failures stop further production, even
  /// when a delayed cleanup fails after [next] has already started.
  Flow<T, E> concat(Flow<T, E> next) => Flow._(
    accessTaskEnvironment(
      (owner, _) => _open.map((source) {
        var current = source;
        var switched = false;
        final pull = Task.defer(
          () => current.pull().flatMap((item) {
            if (item is OptionSome<T> || switched) return Task.succeed(item);
            switched = true;
            return inTaskScope(next._open, owner).flatMap((opened) {
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
        );
      }),
    ),
  );

  /// Maps business errors from opening or pulling this flow.
  ///
  /// Defects, cancellation, and cleanup failures follow Task.mapError rules.
  Flow<T, F> mapError<F>(F Function(E error) transform) => Flow._(
    _open
        .mapError(transform)
        .map(
          (source) => FlowCursor(
            source.pull().mapError(transform),
            source.scopeForValue,
            source.finish,
            source.producers,
          ),
        ),
  );

  /// Replaces one single business failure with the flow returned by [recover].
  ///
  /// The failed producer is retired before recovery is considered. Resources
  /// still used by elements retained downstream remain alive until those
  /// elements are released. Their later cleanup failures propagate to consumption
  /// and cannot be handled by this recovery. Other cleanup finishes before the
  /// replacement starts; a cleanup defect then prevents recovery.
  ///
  /// Defects, cancellation, and grouped causes bypass recovery. Errors from the
  /// replacement flow are not recovered again. Already delivered elements remain
  /// delivered.
  Flow<T, E> recover(Flow<T, E> Function(E error) recover) => Flow._(
    accessTaskEnvironment(
      (owner, _) => _open.map((opened) {
        var replaced = false;
        var current = opened;
        final pull = Task.defer(
          () => current.pull().recover((error) {
            if (replaced) return Task.fail(error);
            final failed = Exit<Object?, E>.failure(Cause.failure(error));
            // Retire production without waiting on values held downstream.
            // Immediate cleanup failures join the body before recovery.
            return current
                .finish(failed)
                .withError<E>()
                .mapCause(
                  (cause) => Cause.combine([Cause.failure(error), cause]),
                )
                .flatMap<Option<T>>((_) => Task.fail(error))
                .recover((error) {
                  replaced = true;
                  return inTaskScope(recover(error)._open, owner).flatMap((
                    replacement,
                  ) {
                    current = replacement;
                    return current.pull();
                  });
                });
          }),
        );
        return FlowCursor(
          pull,
          () => current.scope,
          (exit) => Task.defer(() => current.finish(exit)),
          () => current.producers(),
        );
      }),
    ),
  );

  /// Collects all elements into an immutable list after producer cleanup.
  ///
  /// This retains every element; use a finite flow or limit it with [take].
  Task<List<T>, E> toList() => Task.defer(() {
    final values = <T>[];
    return forEach((value) => Task.sync(() => values.add(value)))
        .map((_) => List<T>.unmodifiable(values));
  });

  /// Reduces elements in order, returning [initial] for an empty flow.
  ///
  /// Each execution starts with [initial]; mutable objects are not cloned.
  /// Exceptions from [combine] become defects. Producer cleanup completes before
  /// the result is returned.
  Task<S, E> fold<S>(S initial, S Function(S state, T value) combine) =>
      Task.defer(() {
        var state = initial;
        return forEach(
          (value) => Task.sync(() {
            state = combine(state, value);
          }),
        ).map((_) => state);
      });

  /// Runs [action] sequentially for each element, waiting before requesting more.
  ///
  /// Failure or cancellation stops production and waits for cleanup. Resources
  /// acquired by [action] belong to the current producer, following [mapTask]'s
  /// lifetime rules. Use Task.scoped for resources needed only during one call.
  Task<void, E> forEach(Task<void, E> Function(T value) action) =>
      _open.flatMap((source) {
        Task<void, E> next() => Task.defer(
          () => source.pull().flatMap(
            (item) => switch (item) {
              OptionSome(:final value) => inTaskScope(
                action(value),
                source.scope,
              ).flatMap((_) => next()),
              OptionNone() => Task.succeed(null),
            },
          ),
        );
        return next();
      }).scoped();

  /// Consumes all elements for their effects and waits for producer cleanup.
  Task<void, E> drain() => forEach((_) => Task.succeed(null));

  /// Exposes one consumption as a lazy, single-subscription Dart stream.
  ///
  /// Listening starts a root task in [runtime]. Pausing the subscription stops
  /// further pulls; cancelling requests task cancellation and waits for producer
  /// cleanup. Its cancellation future fails with [TaskException] if cleanup or
  /// other completed work failed, excluding the subscription's own cancellation
  /// request. This includes failures awaiting delivery to a paused subscriber;
  /// errors already delivered to the subscriber are not repeated. A closed
  /// runtime rejects consumption with a [RuntimeClosed] defect.
  ///
  /// Uncancelled consumption failures, including runtime shutdown, are delivered
  /// as one [TaskException] error retaining the complete cause, followed by done.
  /// A paused subscription receives those events when it resumes. It does not
  /// delay runtime cleanup. As with other Dart streams, an asynchronous `onData`
  /// callback is not awaited; use `await for` to process elements asynchronously
  /// with backpressure.
  Stream<T> toStream(Runtime runtime) => _streamFromFlow(this, runtime);
}

/// Handles data delivered beyond a native stream bridge's buffer capacity.
enum StreamOverflowStrategy {
  /// Fails with a [StreamBufferOverflow] defect after previously buffered data.
  fail,

  /// Discards the incoming data value while preserving buffered values.
  dropping,

  /// Discards the oldest buffered value to retain the incoming value.
  sliding,
}

/// Indicates that a native source exceeded the bridge's data buffer capacity.
final class StreamBufferOverflow implements Exception {
  /// Creates a diagnostic for the exceeded [capacity].
  const StreamBufferOverflow(this.capacity);

  /// The configured maximum number of buffered data values.
  final int capacity;

  @override
  String toString() =>
      'StreamBufferOverflow: The data buffer exceeded capacity $capacity.';
}

/// Establishes a business error type for a flow with no business failures.
extension InfallibleFlowOperations<T> on Flow<T, Never> {
  /// Gives this flow error type [F] without changing its identity or behavior.
  Flow<T, F> withError<F>() => this;
}

void _checkCount(int count) {
  if (count < 0) throw RangeError.range(count, 0, null, 'count');
}

Flow<U, E> _transform<T, U, E>(
  Task<FlowCursor<T, E>, E> open,
  Task<Option<U>, E> Function(FlowCursor<T, E> source) transform,
) => Flow._(
  open.map(
    (source) => FlowCursor(
      transform(source),
      source.scopeForValue,
      source.finish,
      source.producers,
    ),
  ),
);

Task<FlowCursor<T, E>, E> _openCursor<T, E>(Task<Task<Option<T>, E>, E> open) =>
    Scope.open().map((scope) {
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
        return inTaskScope(read, scope).flatMap((item) {
          if (item is OptionSome<T>) return Task.succeed(item);
          closed = true;
          return producer
              .finish(const Exit.success(null))
              .withError<E>()
              .map((_) => item);
        });
      });
      return FlowCursor(pull, () => scope, finish, () => [producer]);
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
    _faults.add(FailureEvent(CauseDefect(error, stackTrace), Object()));
    _ended = true;
  }

  Task<A, Never> _failure<A>() => taskFromEvaluation(
    TaskEvaluation(
      Exit<A, Never>.failure(
        Cause.combine(_faults.map((event) => event.cause)),
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
        inheritHost: true,
      ).withError<E>().flatMap((fiber) {
        _driver = fiber;
        listenFiber(fiber, (exit) {
          _terminal = TaskEvaluation(exit, fiberFailureEvents(fiber));
          if (exit case ExitFailure(:final cause) when _stoppedByUs) {
            _explicitCancellation = _causes(cause)
                .whereType<CauseCancelled>()
                .where((cause) => !identical(cause.reason, _stopReason))
                .toList();
          }
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
    final events = _terminal?.events ?? const <FailureEvent>[];
    final causes = <Cause<Never>>[];
    if (_terminal?.exit case ExitFailure(:final cause)) {
      for (final reason in _causes(cause)) {
        if (reason is CauseDefect ||
            (reason is CauseCancelled &&
                !_terminalDelivered &&
                _explicitCancellation.contains(reason))) {
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
      ),
    );
  });
}

final class _MapEntry<T, E> {
  _MapEntry(this.producer);

  final FlowProducer producer;
  TaskEvaluation<T, E>? result;
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
  final _failures = <TaskEvaluation<Object?, E>>[];
  final _active = <Fiber<U, E>>{};
  Fiber<void, E>? _driver;
  _MapEntry<U, E>? _current;
  void Function()? _consumerWake;
  void Function()? _driverWake;
  var _started = false;
  var _driverDone = false;
  var _ended = false;
  var _stopping = false;

  late final cursor = FlowCursor<U, E>(
    _pull(),
    () => _current?.producer.scope ?? owner,
    _finish,
    () => [if (_current case final current?) current.producer],
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
      return forkTaskIn(
        _produce(),
        owner,
        inheritHost: true,
      ).withError<E>().flatMap((fiber) {
        _driver = fiber;
        listenFiber(fiber, (exit) {
          _driverDone = true;
          if (exit is ExitFailure<void, E>) {
            _failures.add(TaskEvaluation(exit, fiberFailureEvents(fiber)));
            _stop();
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
      );
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
                entry.producer.scope,
              ),
              owner,
              inheritHost: true,
            ).withError<E>().flatMap((fiber) {
              _active.add(fiber);
              if (_stopping) fiber.requestCancel(reason: _stopReason);
              listenFiber(fiber, (exit) {
                _active.remove(fiber);
                final result = TaskEvaluation(exit, fiberFailureEvents(fiber));
                entry.result = result;
                if (exit is ExitFailure<U, E>) {
                  _failures.add(result);
                  _stop();
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
    final seen = <Object>{};
    for (final result in _failures) {
      if (result.exit case ExitFailure(:final cause)) {
        final sources = Map<CauseDefect, Queue<FailureEvent>>.identity();
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
              if (!defectsOnly) causes.add(Cause.failure(error as F));
            case CauseCancelled(:final reason):
              if (!defectsOnly && !identical(reason, _stopReason)) {
                causes.add(Cause.cancelled(reason));
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
Task<FlowCursor<T, E>, E> openFlowCursor<T, E>(Flow<T, E> flow) => flow._open;

Flow<T, E> flowFromCursor<T, E>(Task<FlowCursor<T, E>, E> open) => Flow._(open);
