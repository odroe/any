import 'dart:collection';

import 'cause.dart';
import 'exit.dart';
import 'fiber.dart';
import 'flow.dart';
import 'internal/flow_cursor.dart';
import 'option.dart';
import 'scope.dart';
import 'task.dart';

/// Concurrent combinations of two flows.
extension FlowCombinationOperations<T, E> on Flow<T, E> {
  /// Interleaves values from both flows while preserving each input's order.
  ///
  /// Values are delivered as they become available; simultaneous arrivals have
  /// no guaranteed relative order. Completion of one input leaves the other
  /// running. Both must finish for the merged flow to end.
  ///
  /// Each consumption starts both inputs independently, with at most one pending
  /// read or waiting value per input. New reads require downstream demand.
  /// User tasks in both inputs borrow the consuming task's resource scope.
  ///
  /// Failure stops both inputs. Stopping early waits for pending reads and cleanup,
  /// preserving observed defects and cleanup failures. A non-cancellable [Future]
  /// must settle before its read can stop.
  Flow<T, E> merge(Flow<T, E> other) => flowFromCursor(
    (resources) => Scope.open().withError<E>().flatMap((owner) {
      final merging = _Merged(
        [
          _PullSlot(openFlowCursor(this, resourceScope: resources), owner),
          _PullSlot(openFlowCursor(other, resourceScope: resources), owner),
        ],
        owner,
        resources,
      );
      return inTaskScope(
        Task.addFinalizer(merging.cleanup),
        owner,
      ).withError<E>().map((_) => merging.cursor);
    }),
  );

  /// Pairs elements at matching positions, ending with the shorter flow.
  ///
  /// The first record field comes from this flow; the second comes from [other].
  /// Use [combineLatest] instead when either input should update a pair without
  /// waiting for a matching next element.
  ///
  /// ```dart
  /// final pairs = Flow.fromIterable([1, 2, 3])
  ///     .zip(Flow.fromIterable(['a', 'b']))
  ///     .toList();
  /// // Running pairs produces [(1, 'a'), (2, 'b')].
  /// ```
  ///
  /// Both inputs are read concurrently, at most once each per requested pair.
  /// When one input ends, any unpaired value is discarded and the other read is
  /// cancelled. User resources remain in the consuming task's scope.
  /// Failure or stopping early waits for both inputs and cleanup, including any
  /// non-cancellable [Future].
  Flow<(T, U), E> zip<U>(Flow<U, E> other) => flowFromCursor(
    (resources) => Scope.open().withError<E>().flatMap((owner) {
      final zipped = _Zipped(
        _PullSlot(openFlowCursor(this, resourceScope: resources), owner),
        _PullSlot(openFlowCursor(other, resourceScope: resources), owner),
        owner,
        resources,
      );
      return inTaskScope(
        Task.addFinalizer(zipped.cleanup),
        owner,
      ).withError<E>().map((_) => zipped.cursor);
    }),
  );

  /// Emits each update paired with the other flow's latest value.
  ///
  /// Nothing is emitted until both inputs have a value. Then each observed update
  /// produces `(thisValue, otherValue)`, in arrival order. Earlier updates from
  /// one input are replaced while waiting for the other's first value. Use [zip]
  /// when values should instead be paired once by their positions.
  ///
  /// ```dart
  /// final pairs = Flow.fromIterable([10])
  ///     .combineLatest(Flow.fromIterable([1, 2, 3]))
  ///     .toList();
  /// // Running pairs produces [(10, 1), (10, 2), (10, 3)].
  /// ```
  ///
  /// A completed input keeps contributing its last value. An input that ends empty
  /// ends the result immediately. Both inputs run concurrently, each retaining a
  /// latest value and at most one pending read or waiting update. Replacing a
  /// latest value does not close resources acquired in the consuming task's scope.
  ///
  /// Failure or stopping early waits for both inputs and cleanup, including any
  /// non-cancellable [Future].
  Flow<(T, U), E> combineLatest<U>(Flow<U, E> other) => flowFromCursor(
    (resources) => Scope.open().withError<E>().flatMap((owner) {
      final combined = _Latest(
        _PullSlot(openFlowCursor(this, resourceScope: resources), owner),
        _PullSlot(openFlowCursor(other, resourceScope: resources), owner),
        owner,
        resources,
      );
      return inTaskScope(
        Task.addFinalizer(combined.cleanup),
        owner,
      ).withError<E>().map((_) => combined.cursor);
    }),
  );
}

final class _Entry<T> {
  _Entry(this.value, this.producers);

  final T value;
  final List<FlowProducer> producers;
}

// One read at a time per input. The retained entry is recorded before its fiber
// publishes completion, so a cancellation during handoff cannot hide a lease.
final class _PullSlot<T, E> {
  _PullSlot(this.open, this.owner);

  final Task<FlowCursor<T, E>, E> open;
  final Scope owner;
  FlowCursor<T, E>? source;
  Fiber<void, E>? fiber;
  _Entry<T>? entry;
  var ended = false;

  Task<void, E> read() => Task.uninterruptibleMask(
    (restore) =>
        Task.defer(() {
          final current = source;
          return current == null
              ? restore(inTaskScope(open, owner))
                    .map((opened) => source = opened)
              : Task.succeed(current);
        }).flatMap(
          (source) => restore(source.pull()).flatMap(
            (item) => Task.sync(() {
              switch (item) {
                case OptionNone():
                  ended = true;
                case OptionSome(:final value):
                  final producers = source.producers().toSet().toList();
                  for (final producer in producers) {
                    producer.retain();
                  }
                  entry = _Entry(value, producers);
              }
            }),
          ),
        ),
  );

  Task<void, Never> finish(Exit<Object?, Object?> exit) =>
      Task.defer(() => source?.finish(exit) ?? Task.succeed(null));
}

// The two inputs share stop, observation, and failure handling. Values stay in
// their own typed slots; this coordinator never erases their types.
final class _Readers<E> {
  _Readers(this.owner, this.resources);

  final Scope owner;
  final Scope resources;
  final _active = <Fiber<void, E>>{};
  final _stopReason = Object();
  final _failures = <_ReadFailure<E>>[];
  void Function()? _wake;
  var stopping = false;
  var _stoppedByUs = false;
  var _failureDelivered = false;

  bool get active => _active.isNotEmpty;

  Task<void, Never> start<T>(
    _PullSlot<T, E> input, {
    void Function()? onReady,
  }) => Task.defer(() {
    if (stopping || input.ended || input.fiber != null || input.entry != null) {
      return Task.succeed(null);
    }
    var explicitCancellation = <CauseCancelled>[];
    return Task.uninterruptibleMask(
      (_) =>
          forkTaskIn(
            input.read().mapCause((cause) {
              // Owner-requested cancellation bypasses this callback. Preserve
              // explicit cancellation already observed before the owner stops us.
              explicitCancellation = _causes(cause)
                  .whereType<CauseCancelled>()
                  .toList();
              return cause;
            }),
            owner,
            resourceScope: resources,
            inheritHost: true,
          ).map((fiber) {
            input.fiber = fiber;
            _active.add(fiber);
            if (stopping) fiber.requestCancel(reason: _stopReason);
            fiber.onExit((exit) {
              input.fiber = null;
              _active.remove(fiber);
              if (exit is ExitFailure<void, E>) {
                if (_stoppedByUs) {
                  explicitCancellation = _causes(exit.cause)
                      .whereType<CauseCancelled>()
                      .where((cause) => !identical(cause.reason, _stopReason))
                      .toList();
                }
                _failures.add(
                  _ReadFailure(
                    TaskEvaluation(
                      exit,
                      fiberFailureEvents(fiber),
                      cleanupCancellations: fiberCleanupCancellations(fiber),
                    ),
                    [
                      ...explicitCancellation,
                      ...fiberCleanupCancellations(fiber),
                    ],
                  ),
                );
                stop();
              } else {
                onReady?.call();
              }
              _wake?.call();
            });
          }),
    );
  });

  Task<void, Never> wait(bool Function() ready) =>
      Task.fromCallback((complete) {
        void notify() {
          _wake = null;
          complete(const Exit.success(null));
        }

        _wake = notify;
        if (ready()) notify();
        return () {
          if (identical(_wake, notify)) _wake = null;
        };
      });

  void stop() {
    if (stopping) return;
    stopping = true;
    _stoppedByUs = !scopeIsClosing(owner);
    for (final fiber in _active.toList()) {
      fiber.requestCancel(reason: _stopReason);
    }
    _wake?.call();
  }

  Task<void, Never> awaitStopped() => Task.defer(
    () => active
        ? wait(() => !active).flatMap((_) => awaitStopped())
        : Task.succeed(null),
  );

  Task<A, E> result<A>(A Function() success) =>
      taskFromEvaluation(_failure<A, E>(success)).mapCause((cause) {
        _failureDelivered = true;
        return cause;
      });

  Task<void, Never> cleanup() => Task.defer(() {
    stop();
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
      final cancellations = [...failure.explicitCancellation];
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

// An output owns a transport scope and holds its inputs plus the enclosing
// operator alive until every downstream lease releases that output.
final class _Output {
  _Output(this.owner, this.resources);

  final Scope owner;
  final Scope resources;
  late final lifetime = FlowProducer(owner);
  FlowProducer? current;

  Scope get scope => current?.scope ?? owner;
  List<FlowProducer> producers() => [?current];

  Option<T> emit<T>(T value, List<FlowProducer> inputs) {
    lifetime.retain();
    current = FlowProducer(
      createScope(parent: owner),
      afterClose: (_) async => mergeFlowEvaluations([
        await releaseFlowProducers(inputs),
        await lifetime.release(),
      ]),
    );
    return Option.some(value);
  }

  Task<void, Never> retire(Exit<Object?, Object?> exit) => Task.defer(() {
    final previous = current;
    current = null;
    return previous?.finish(exit) ?? Task.succeed(null);
  });

  Task<void, Never> finish(Exit<Object?, Object?> exit) =>
      retire(exit).ensuring(lifetime.finish(exit));
}

final class _Merged<T, E> {
  _Merged(this.inputs, Scope owner, Scope resources)
    : readers = _Readers(owner, resources),
      output = _Output(owner, resources);

  final List<_PullSlot<T, E>> inputs;
  final _Readers<E> readers;
  final _Output output;
  final _ready = Queue<_PullSlot<T, E>>();

  late final cursor = FlowCursor<T, E>(
    _pull(),
    () => output.scope,
    _finish,
    output.producers,
    resourceScope: output.resources,
  );

  Task<void, Never> _start(_PullSlot<T, E> input) => readers.start(
    input,
    onReady: () {
      if (input.entry != null) _ready.addLast(input);
    },
  );

  Task<Option<T>, E> _pull() => Task.defer(() {
    if (output.current != null) {
      return output
          .retire(const Exit.success(null))
          .withError<E>()
          .flatMap((_) => _pull());
    }
    if (readers.stopping) {
      return readers.awaitStopped().withError<E>().flatMap(
        (_) => readers.result(() => const Option<Never>.none()),
      );
    }
    return _start(
      inputs[0],
    ).flatMap((_) => _start(inputs[1])).withError<E>().flatMap((_) {
      if (readers.stopping) return _pull();
      if (_ready.isNotEmpty) {
        final input = _ready.removeFirst();
        final entry = input.entry!;
        input.entry = null;
        return Task.succeed(output.emit(entry.value, entry.producers));
      }
      if (inputs.every((input) => input.ended)) {
        return output
            .finish(const Exit.success(null))
            .withError<E>()
            .map((_) => const Option<Never>.none());
      }
      return readers
          .wait(() => readers.stopping || _ready.isNotEmpty || !readers.active)
          .withError<E>()
          .flatMap((_) => _pull());
    });
  });

  Task<void, Never> _finish(Exit<Object?, Object?> exit) => Task.defer(() {
    readers.stop();
    return readers.awaitStopped().flatMap((_) {
      final retained = <FlowProducer>[];
      for (final input in inputs) {
        retained.addAll(input.entry?.producers ?? const []);
        input.entry = null;
      }
      _ready.clear();
      return inputs[0]
          .finish(exit)
          .ensuring(inputs[1].finish(exit))
          .ensuring(_release(retained))
          .ensuring(output.finish(exit));
    });
  });

  Task<void, Never> cleanup(Exit<Object?, Object?> exit) => Task.defer(() {
    // Owner closure has already stopped readers and closed child scopes. Do
    // not wait on producer leases owned by the scope running this finalizer.
    output.current = null;
    _ready.clear();
    for (final input in inputs) {
      input.entry = null;
    }
    return readers.cleanup();
  });
}

final class _Zipped<A, B, E> {
  _Zipped(this.left, this.right, Scope owner, Scope resources)
    : readers = _Readers(owner, resources),
      output = _Output(owner, resources);

  final _PullSlot<A, E> left;
  final _PullSlot<B, E> right;
  final _Readers<E> readers;
  final _Output output;

  late final cursor = FlowCursor<(A, B), E>(
    _pull(),
    () => output.scope,
    _finish,
    output.producers,
    resourceScope: output.resources,
  );

  bool get _ended =>
      (left.ended && left.fiber == null) ||
      (right.ended && right.fiber == null);
  bool get _paired =>
      left.entry != null && right.entry != null && !readers.active;

  Task<Option<(A, B)>, E> _pull() => Task.defer(() {
    if (output.current != null) {
      return output
          .retire(const Exit.success(null))
          .withError<E>()
          .flatMap((_) => _pull());
    }
    if (readers.stopping) {
      return readers.awaitStopped().withError<E>().flatMap(
        (_) => readers.result(() => const Option<Never>.none()),
      );
    }
    return readers
        .start(left)
        .flatMap((_) => readers.start(right))
        .withError<E>()
        .flatMap((_) {
          if (readers.stopping) return _pull();
          if (_ended) {
            return _finish(const Exit.success(null))
                .withError<E>()
                .map((_) => const Option<Never>.none());
          }
          if (_paired) {
            final a = left.entry!;
            final b = right.entry!;
            left.entry = null;
            right.entry = null;
            return Task.succeed(
              output.emit((a.value, b.value), [...a.producers, ...b.producers]),
            );
          }
          return readers
              .wait(() => readers.stopping || _ended || _paired)
              .withError<E>()
              .flatMap((_) => _pull());
        });
  });

  Task<void, Never> _finish(Exit<Object?, Object?> exit) => Task.defer(() {
    readers.stop();
    return readers.awaitStopped().flatMap((_) {
      final retained = [...?left.entry?.producers, ...?right.entry?.producers];
      left.entry = null;
      right.entry = null;
      return left
          .finish(exit)
          .ensuring(right.finish(exit))
          .ensuring(_release(retained))
          .ensuring(output.finish(exit));
    });
  });

  Task<void, Never> cleanup(Exit<Object?, Object?> exit) => Task.defer(() {
    output.current = null;
    left.entry = null;
    right.entry = null;
    return readers.cleanup();
  });
}

enum _Side { left, right }

final class _Latest<A, B, E> {
  _Latest(this.left, this.right, Scope owner, Scope resources)
    : readers = _Readers(owner, resources),
      output = _Output(owner, resources);

  final _PullSlot<A, E> left;
  final _PullSlot<B, E> right;
  final _Readers<E> readers;
  final _Output output;
  final _updates = Queue<_Side>();
  _Entry<A>? _leftLatest;
  _Entry<B>? _rightLatest;

  late final cursor = FlowCursor<(A, B), E>(
    _pull(),
    () => output.scope,
    _finish,
    output.producers,
    resourceScope: output.resources,
  );

  bool get _ended =>
      (left.ended && left.fiber == null && _leftLatest == null) ||
      (right.ended && right.fiber == null && _rightLatest == null) ||
      (left.ended && right.ended && !readers.active);

  Task<Option<(A, B)>, E> _pull() => Task.defer(() {
    if (output.current != null) {
      return output
          .retire(const Exit.success(null))
          .withError<E>()
          .flatMap((_) => _pull());
    }
    if (readers.stopping) {
      return readers.awaitStopped().withError<E>().flatMap(
        (_) => readers.result(() => const Option<Never>.none()),
      );
    }
    return readers
        .start(left, onReady: () => _updates.addLast(_Side.left))
        .flatMap(
          (_) => readers.start(
            right,
            onReady: () => _updates.addLast(_Side.right),
          ),
        )
        .withError<E>()
        .flatMap((_) {
          if (readers.stopping) return _pull();
          if (_ended) {
            return _finish(const Exit.success(null))
                .withError<E>()
                .map((_) => const Option<Never>.none());
          }
          if (_updates.isNotEmpty) return _update(_updates.removeFirst());
          return readers
              .wait(() => readers.stopping || _ended || _updates.isNotEmpty)
              .withError<E>()
              .flatMap((_) => _pull());
        });
  });

  Task<Option<(A, B)>, E> _update(_Side side) {
    final List<FlowProducer> previous;
    switch (side) {
      case _Side.left:
        final next = left.entry;
        if (next == null) return _pull();
        left.entry = null;
        previous = _leftLatest?.producers ?? const [];
        _leftLatest = next;
      case _Side.right:
        final next = right.entry;
        if (next == null) return _pull();
        right.entry = null;
        previous = _rightLatest?.producers ?? const [];
        _rightLatest = next;
    }
    return _release(previous).withError<E>().flatMap((_) {
      final a = _leftLatest;
      final b = _rightLatest;
      if (readers.stopping || a == null || b == null) return _pull();
      final retained = [...a.producers, ...b.producers];
      for (final producer in retained) {
        producer.retain();
      }
      return Task.succeed(output.emit((a.value, b.value), retained));
    });
  }

  Task<void, Never> _finish(Exit<Object?, Object?> exit) => Task.defer(() {
    readers.stop();
    return readers.awaitStopped().flatMap((_) {
      final retained = [
        ...?left.entry?.producers,
        ...?right.entry?.producers,
        ...?_leftLatest?.producers,
        ...?_rightLatest?.producers,
      ];
      left.entry = null;
      right.entry = null;
      _leftLatest = null;
      _rightLatest = null;
      _updates.clear();
      return left
          .finish(exit)
          .ensuring(right.finish(exit))
          .ensuring(_release(retained))
          .ensuring(output.finish(exit));
    });
  });

  Task<void, Never> cleanup(Exit<Object?, Object?> exit) => Task.defer(() {
    output.current = null;
    left.entry = null;
    right.entry = null;
    _leftLatest = null;
    _rightLatest = null;
    _updates.clear();
    return readers.cleanup();
  });
}

final class _ReadFailure<E> {
  _ReadFailure(this.result, this.explicitCancellation);

  final TaskEvaluation<void, E> result;
  final List<CauseCancelled> explicitCancellation;
}

List<Cause<E>> _causes<E>(Cause<E> cause) => switch (cause) {
  CauseGroup(:final causes) => causes,
  _ => [cause],
};

Task<void, Never> _release(List<FlowProducer> producers) =>
    Task.uninterruptibleMask(
      (_) => Task.fromFuture(
        () => releaseFlowProducers(producers),
        onError: Error.throwWithStackTrace,
      ).flatMap(taskFromEvaluation),
    );
