import 'dart:collection';

import 'cause.dart';
import 'exit.dart';
import 'fiber.dart';
import 'flow.dart';
import 'internal/flow_cursor.dart';
import 'option.dart';
import 'scope.dart';
import 'task.dart';

/// Combines independent flow consumptions using their statically known types.
extension FlowCombinationOperations<T, E> on Flow<T, E> {
  /// Interleaves this flow with [other], preserving each input's element order.
  ///
  /// Both inputs open and read concurrently. Results are delivered in their observed
  /// completion order, which does not define an order for simultaneous values.
  /// One completed input leaves the other running; both must finish normally
  /// for the merged flow to end.
  ///
  /// Each input has at most one pending read or undelivered value. Another read
  /// starts only when downstream requests an element. Resources for delivered
  /// values remain alive through downstream consumption, including buffering
  /// and concurrent mapping. Each consumption opens both inputs independently.
  ///
  /// Failure stops both inputs and waits for cleanup. Early termination also
  /// waits for pending reads, retaining observed defects and cleanup failures.
  /// An underlying native Future must settle before its read can stop.
  Flow<T, E> merge(Flow<T, E> other) => flowFromCursor(
    Scope.open().withError<E>().flatMap((owner) {
      final merging = _Merged([
        _PullSlot(openFlowCursor(this), owner),
        _PullSlot(openFlowCursor(other), owner),
      ], owner);
      return inTaskScope(
        Task.addFinalizer(merging.cleanup),
        owner,
      ).withError<E>().map((_) => merging.cursor);
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

final class _Merged<T, E> {
  _Merged(this.inputs, this.owner);

  final List<_PullSlot<T, E>> inputs;
  final Scope owner;
  final _ready = Queue<_PullSlot<T, E>>();
  final _stopReason = Object();
  final _failures = <_ReadFailure<E>>[];
  late final _lifetime = FlowProducer(owner);
  FlowProducer? _current;
  void Function()? _wake;
  var _stopping = false;
  var _stoppedByUs = false;
  var _failureDelivered = false;

  late final cursor = FlowCursor<T, E>(
    _pull(),
    () => _current?.scope ?? owner,
    _finish,
    () => [?_current],
  );

  bool get _active => inputs.any((input) => input.fiber != null);

  Task<void, Never> _start(_PullSlot<T, E> input) => Task.defer(() {
    if (_stopping ||
        input.ended ||
        input.fiber != null ||
        input.entry != null) {
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
            inheritHost: true,
          ).map((fiber) {
            input.fiber = fiber;
            if (_stopping) fiber.requestCancel(reason: _stopReason);
            listenFiber(fiber, (exit) {
              input.fiber = null;
              if (exit is ExitFailure<void, E>) {
                if (_stoppedByUs) {
                  explicitCancellation = _causes(exit.cause)
                      .whereType<CauseCancelled>()
                      .where((cause) => !identical(cause.reason, _stopReason))
                      .toList();
                }
                _failures.add(
                  _ReadFailure(
                    TaskEvaluation(exit, fiberFailureEvents(fiber)),
                    explicitCancellation,
                  ),
                );
                _stop();
              } else if (input.entry != null) {
                _ready.addLast(input);
              }
              _wake?.call();
            });
          }),
    );
  });

  Task<Option<T>, E> _pull() => Task.defer(() {
    final previous = _current;
    if (previous != null) {
      _current = null;
      return previous
          .finish(const Exit.success(null))
          .withError<E>()
          .flatMap((_) => _pull());
    }
    if (_stopping) {
      if (_active) return _wait().withError<E>().flatMap((_) => _pull());
      return taskFromEvaluation(
        _failure<Option<T>, E>(() => const Option.none()),
      ).mapCause((cause) {
        _failureDelivered = true;
        return cause;
      });
    }
    return _start(inputs[0])
        .flatMap((_) => _start(inputs[1]))
        .withError<E>()
        .flatMap((_) {
          if (_stopping) return _pull();
          if (_ready.isNotEmpty) {
            final input = _ready.removeFirst();
            final entry = input.entry!;
            input.entry = null;
            _lifetime.retain();
            _current = FlowProducer(
              createScope(parent: owner),
              afterClose: (_) async => mergeFlowEvaluations([
                await releaseFlowProducers(entry.producers),
                await _lifetime.release(),
              ]),
            );
            return Task.succeed(Option.some(entry.value));
          }
          if (inputs.every((input) => input.ended)) {
            return _lifetime
                .finish(const Exit.success(null))
                .withError<E>()
                .map((_) => const Option<Never>.none());
          }
          return _wait().withError<E>().flatMap((_) => _pull());
        });
  });

  Task<void, Never> _wait() => Task.fromCallback((complete) {
    void notify() {
      _wake = null;
      complete(const Exit.success(null));
    }

    _wake = notify;
    if (_stopping ? !_active : _ready.isNotEmpty || !_active) notify();
    return () {
      if (identical(_wake, notify)) _wake = null;
    };
  });

  void _stop() {
    if (_stopping) return;
    _stopping = true;
    _stoppedByUs = !scopeIsClosing(owner);
    for (final input in inputs) {
      input.fiber?.requestCancel(reason: _stopReason);
    }
    _wake?.call();
  }

  Task<void, Never> _finish(Exit<Object?, Object?> exit) => Task.defer(() {
    _stop();
    if (_active) return _wait().flatMap((_) => _finish(exit));
    final current = _current;
    _current = null;
    final retained = <FlowProducer>[];
    for (final input in inputs) {
      retained.addAll(input.entry?.producers ?? const []);
      input.entry = null;
    }
    _ready.clear();
    return inputs[0]
        .finish(exit)
        .ensuring(inputs[1].finish(exit))
        .ensuring(current?.finish(exit) ?? Task.succeed(null))
        .ensuring(_release(retained))
        .ensuring(_lifetime.finish(exit));
  });

  Task<void, Never> cleanup(Exit<Object?, Object?> exit) => Task.defer(() {
    // Owner closure has already stopped readers and closed child scopes. Do
    // not wait on producer leases owned by the scope running this finalizer.
    _stop();
    _current = null;
    _ready.clear();
    for (final input in inputs) {
      input.entry = null;
    }
    return taskFromEvaluation(_failure<void, Never>(() {}, defectsOnly: true));
  });

  TaskEvaluation<A, F> _failure<A, F>(
    A Function() success, {
    bool defectsOnly = false,
  }) {
    final causes = <Cause<F>>[];
    final events = <FailureEvent>[];
    final seen = <Object>{};
    for (final failure in _failures) {
      final result = failure.result;
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
            case CauseCancelled():
              if ((!defectsOnly ||
                      (!_failureDelivered &&
                          failure.explicitCancellation.contains(reason))) &&
                  !identical(reason.reason, _stopReason)) {
                causes.add(reason);
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
