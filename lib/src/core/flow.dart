import 'async_queue.dart';
import 'exit.dart';
import 'option.dart';
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
