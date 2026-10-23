import 'dart:async';
import 'dart:collection';

import 'async_queue.dart';
import 'cancellation.dart';
import 'cause.dart';
import 'exit.dart';
import 'fiber.dart';
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

  final Task<_Cursor<T, E>, E> _open;

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

  /// Runs [transform] for each element, one task at a time and in input order.
  ///
  /// The next element is requested only after the current task succeeds.
  /// Acquired resources belong to the current producer and are released before
  /// its inputs. Use Task.scoped inside [transform] for resources needed by only
  /// that step. Inner flows and concatenated branches have separate lifetimes.
  Flow<U, E> mapTask<U>(Task<U, E> Function(T value) transform) => _transform(
    _open,
    (source) => source.pull().flatMap(
      (item) => switch (item) {
        OptionSome(:final value) => inTaskScope(
          transform(value),
          source.scope,
        ).map(Option.some),
        OptionNone() => Task.succeed(const Option<Never>.none()),
      },
    ),
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
  /// Each inner producer is closed before the next is opened. Failure stops
  /// both producers. Resources from the outer element remain available to its
  /// inner flow.
  Flow<U, E> flatMap<U>(Flow<U, E> Function(T value) transform) => Flow._(
    _open.map((source) {
      _Cursor<U, E>? inner;
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
      return _Cursor(next(), () => inner?.scope ?? source.scope, source.close);
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
                .close(const Exit.success(null))
                .withError<E>()
                .map((_) => const Option<Never>.none());
          }
          remaining--;
          return source.pull();
        });
        return _Cursor(pull, source._scope, source.close);
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
        return _Cursor(next(), source._scope, source.close);
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
              .close(const Exit.success(null))
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
                .close(const Exit.success(null))
                .withError<E>()
                .map((_) => const Option<Never>.none());
          }
          return source.pull().map((item) {
            if (item case OptionSome(:final value)) stopped = test(value);
            return item;
          });
        });
      });

  /// Consumes [next] only after this producer completes and cleans up normally.
  ///
  /// A failure, including a cleanup defect, prevents [next] from starting.
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
        return _Cursor(
          pull,
          () => current.scope,
          (exit) => Task.defer(() => current.close(exit)),
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
          (source) => _Cursor(
            source.pull().mapError(transform),
            source._scope,
            source.close,
          ),
        ),
  );

  /// Replaces one single business failure with the flow returned by [recover].
  ///
  /// The failed producer finishes cleanup before recovery is considered.
  /// Defects, cancellation, and grouped causes bypass recovery, including a
  /// business failure combined with a cleanup defect. Errors from the replacement
  /// flow are not recovered again. Already delivered elements remain delivered.
  Flow<T, E> recover(Flow<T, E> Function(E error) recover) => Flow._(
    accessTaskEnvironment(
      (owner, _) => _open.map((opened) {
        var replaced = false;
        var current = opened;
        final pull = Task.defer(
          () => current.pull().recover((error) {
            if (replaced) return Task.fail(error);
            final Task<Option<T>, E> failed = Task.fail(error);
            // Merge cleanup with the business failure before deciding whether
            // recovery still applies. Pending cancellation then stops recovery.
            return Task.uninterruptibleMask<Option<T>, E>(
              (_) => failed.onExit(
                (exit) => Task.defer(() => current.close(exit)),
              ),
            ).recover((error) {
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
        return _Cursor(
          pull,
          () => current.scope,
          (exit) => Task.defer(() => current.close(exit)),
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
  Task<_Cursor<T, E>, E> open,
  Task<Option<U>, E> Function(_Cursor<T, E> source) transform,
) => Flow._(
  open.map((source) => _Cursor(transform(source), source._scope, source.close)),
);

Task<_Cursor<T, E>, E> _openCursor<T, E>(Task<Task<Option<T>, E>, E> open) =>
    Scope.open().map((scope) {
      Task<Option<T>, E>? next;
      var closed = false;
      Task<void, Never> close(Exit<Object?, Object?> exit) => Task.defer(() {
        if (closed) return Task.succeed(null);
        closed = true;
        return scope.close(exit);
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
        return inTaskScope(read, scope).flatMap(
          (item) => item is OptionSome<T>
              ? Task.succeed(item)
              : close(const Exit.success(null)).withError<E>().map((_) => item),
        );
      });
      return _Cursor(pull, () => scope, close);
    });

// Transformations share their producer's lifetime. Composite flows expose the
// currently active producer so downstream acquisitions release before its inputs.
final class _Cursor<T, E> {
  _Cursor(this._pull, this._scope, this.close);

  final Task<Option<T>, E> _pull;
  final Scope Function() _scope;
  final Task<void, Never> Function(Exit<Object?, Object?> exit) close;

  Scope get scope => _scope();
  Task<Option<T>, E> pull() => _pull;
}

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
