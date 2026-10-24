import 'dart:async';

import 'cause.dart';
import 'exit.dart';
import 'fiber.dart';
import 'flow.dart';
import 'internal/flow_cursor.dart';
import 'option.dart';
import 'scope.dart';
import 'task.dart';

/// Shared production with independently scoped consumers.
extension FlowSharingOperations<T, E> on Flow<T, E> {
  /// Creates a scoped flow whose consumers share one source execution.
  ///
  /// The first consumer starts the source. Each consumer receives live values in
  /// order, preceded by up to [replay] recent values. The source uses services and
  /// task-local values from this task; callbacks use their consumer's environment.
  /// Use sharing when multiple consumers should observe the same production.
  ///
  /// ```dart
  /// final work = Flow.fromIterable([1, 2, 3])
  ///     .share(capacity: 3, replay: 3)
  ///     .flatMap(
  ///       (shared) => shared.toList().flatMap(
  ///         (first) => shared.toList().map((second) => (first, second)),
  ///       ),
  ///     );
  /// // Running work produces ([1, 2, 3], [1, 2, 3]); the second list uses replay.
  /// ```
  ///
  /// Slow consumers apply backpressure. [capacity] counts shared buffered values,
  /// including a delivery until its consumer requests another; one additional
  /// read or value may be pending. Values retained by downstream operators are
  /// separate. [capacity] must be positive and [replay] between zero and [capacity],
  /// or execution fails with a [RangeError] defect.
  ///
  /// The source never restarts. Its last consumer stops production and waits for
  /// pending reads. Later consumers receive only retained replay and the terminal
  /// result. Accepted values precede source failure.
  ///
  /// Resources acquired by the source belong to the creating scope. They remain
  /// available until that scope closes, even without replay or active consumers.
  /// Collected values can therefore be used after consumption while the owner is
  /// open. Downstream callbacks acquire resources in their consumer's scope.
  ///
  /// Replay and active deliveries also retain their producer cleanup. Closing the
  /// owner rejects new consumption and waits for outstanding deliveries. A
  /// consumer in another runtime may finish its current value, then fails with a
  /// [StateError] defect. Retaining a collected value does not extend the owner's
  /// lifetime; keep all uses within the creating scope's lifetime.
  Task<Flow<T, E>, Never> share({required int capacity, int replay = 0}) =>
      Task.defer(() {
        if (capacity < 1) {
          throw RangeError.range(capacity, 1, null, 'capacity');
        }
        if (replay < 0 || replay > capacity) {
          throw RangeError.range(replay, 0, capacity, 'replay');
        }
        return accessTaskEnvironment(
          (owner, _) => captureTaskEnvironment().flatMap(
            (environment) => Task.acquireRelease(
              Task.sync(
                () => _SharedFlow(this, owner, environment, capacity, replay),
              ),
              release: (shared, exit) => shared.close(exit),
            ).map((shared) => flowFromCursor(shared.open)),
          ),
        );
      });
}

final class _SharedFlow<T, E> {
  _SharedFlow(
    this.source,
    this.owner,
    this.environment,
    this.capacity,
    this.replay,
  ) : _resources = createScope(runtimeId: scopeRuntimeId(owner)),
      _work = createScope(runtimeId: scopeRuntimeId(owner)) {
    _removeResources = addScopeCloseDependency(owner, _resources);
    _removeWork = addScopeCloseDependency(owner, _work);
    _lifetime = FlowProducer(
      _resources,
      afterClose: (_) async {
        _removeResources();
        _resourcesFinished = true;
        return const TaskEvaluation(Exit.success(null), []);
      },
    );
  }

  final Flow<T, E> source;
  final Scope owner;
  final TaskEnvironment environment;
  final int capacity;
  final int replay;
  final Scope _resources;
  final Scope _work;
  final _resourcesClosed = Completer<void>();
  final _stopped = Completer<void>();
  final _stopReason = Object();
  final _values = <int, _SharedValue<T, E>>{};
  final _subscriptions = <_SharedSubscription<T, E>>{};
  final _waiting = <void Function()>{};
  final _failures = <_SharedFailure<E>>[];
  final _defects = <Object>{};
  late final FlowProducer _lifetime;
  late final void Function() _removeResources;
  late final void Function() _removeWork;
  Fiber<void, E>? _driver;
  FlowCursor<T, E>? _source;
  Exit<Object?, Object?>? _stopExit;
  var _first = 0;
  var _next = 0;
  var _started = false;
  var _resourcesFinished = false;
  var _stopping = false;
  var _ended = false;
  var _closed = false;

  Task<FlowCursor<T, E>, E> open(Scope resources) =>
      Task.uninterruptibleMask((restore) {
        Task<FlowCursor<T, E>, E> subscribe() => Task.defer(() {
          _checkOpen();
          if (_stopping && !_ended) {
            return restore(wait(() => _closed || _ended))
                .withError<E>()
                .flatMap((_) => subscribe());
          }
          return Scope.open().withError<E>().flatMap((scope) {
            final subscription = _SharedSubscription<T, E>(
              this,
              scope,
              _replayStart,
              resources,
            );
            _subscriptions.add(subscription);
            return inTaskScope(
              Task.addFinalizer(subscription.detach),
              scope,
            ).withError<E>().map((_) {
              if (!_started) _start();
              _notify();
              return subscription.cursor;
            });
          });
        });

        return subscribe();
      });

  void _checkOpen() {
    if (_closed || scopeIsClosing(owner)) {
      throw StateError('The shared flow is closed.');
    }
  }

  int get _replayStart {
    final start = _next - replay;
    return start < _first ? _first : start;
  }

  int get _retainFrom {
    if (_closed) return _next;
    var position = _replayStart;
    for (final subscription in _subscriptions) {
      if (subscription.position < position) position = subscription.position;
    }
    return position;
  }

  bool get _canProduce =>
      _values.length < capacity ||
      _subscriptions.every((subscription) => subscription.position > _first);

  void _start() {
    _started = true;
    final task =
        inTaskScope(
          openFlowCursor(source, resourceScope: _resources).flatMap((cursor) {
            _source = cursor;
            return _produce();
          }),
          _resources,
        ).onExit((exit) {
          final cleanupExit = _stopping ? _stopExit ?? exit : exit;
          return _source?.finish(cleanupExit) ?? Task.succeed(null);
        });
    _driver = startFiber(
      task,
      owner: _work,
      resourceScope: _work,
      context: environment.context,
      locals: environment.locals,
      report: environment.report,
      inheritHost: false,
      deferStart: true,
    );
    listenFiber(_driver!, (exit) => unawaited(_driverEnded(exit)));
  }

  Future<void> _driverEnded(Exit<void, E> exit) async {
    _record(
      TaskEvaluation(
        exit,
        fiberFailureEvents(_driver!),
        cleanupCancellations: fiberCleanupCancellations(_driver!),
      ),
      stop: true,
    );
    await closeScope(_work, exit);
    _removeWork();
    _ended = true;
    _stopped.complete();
    _notify();
  }

  Task<void, E> _produce() => Task.defer(() {
    if (_stopping) return Task.succeed(null);
    if (!_canProduce) {
      return wait(() => _stopping || _canProduce)
          .withError<E>()
          .flatMap((_) => _produce());
    }
    return _source!.pull().flatMap((item) {
      if (item is OptionNone<T> || _stopping) return Task.succeed(null);
      return _admit((item as OptionSome<T>).value)
          .withError<E>()
          .flatMap((_) => _produce());
    });
  });

  Task<void, Never> _admit(T value) => Task.uninterruptibleMask((restore) {
    final inputs = _source!.producers().toSet().toList();
    for (final producer in inputs) {
      producer.retain();
    }
    _lifetime.retain();
    final entry = _SharedValue<T, E>(this, value, inputs);
    var committed = false;
    Task<void, Never> commit() => Task.defer(() {
      if (_stopping) return Task.succeed(null);
      if (!_canProduce) {
        return restore(wait(() => _stopping || _canProduce))
            .flatMap((_) => commit());
      }
      final evicted = <_SharedValue<T, E>>[];
      if (_values.length == capacity) {
        evicted.add(_values.remove(_first++)!);
      }
      committed = true;
      _values[_next++] = entry;
      _notify();
      return _releaseValues(evicted);
    });
    return commit().ensuring(
      Task.defer(
        () => committed ? Task.succeed(null) : _releaseValues([entry]),
      ),
    );
  });

  Task<void, Never> trim() => Task.defer(() {
    final until = _retainFrom;
    final removed = <_SharedValue<T, E>>[];
    while (_first < until) {
      removed.add(_values.remove(_first++)!);
    }
    _notify();
    return _releaseValues(removed);
  });

  Task<void, Never> _releaseValues(List<_SharedValue<T, E>> values) =>
      Task.uninterruptibleMask(
        (_) => Task.fromFuture(() async {
          for (final value in values) {
            await value.release();
          }
        }, onError: Error.throwWithStackTrace),
      );

  List<_SharedFailure<E>> _record<A, F extends E>(
    TaskEvaluation<A, F> result, {
    bool stop = false,
  }) {
    if (result.exit case ExitFailure(:final cause)) {
      final events = result.events.iterator;
      final added = <_SharedFailure<E>>[];
      final cancellations = [...result.cleanupCancellations];
      var removedStop = false;
      for (final cause in _causes(cause)) {
        FailureEvent? event;
        if (cause is CauseDefect) {
          events.moveNext();
          event = events.current;
          if (!_defects.add(event.source)) continue;
        }
        if (stop &&
            !removedStop &&
            cause is CauseCancelled &&
            identical(cause.reason, _stopReason)) {
          removedStop = true;
          continue;
        }
        final failure = _SharedFailure<E>(
          cause,
          event,
          cancellations.remove(cause),
        );
        _failures.add(failure);
        added.add(failure);
      }
      if (added.isNotEmpty) {
        _stop(result.exit);
        _notify();
      }
      _completeResources();
      return added;
    }
    _completeResources();
    return const [];
  }

  void _completeResources() {
    if (_resourcesFinished && !_resourcesClosed.isCompleted) {
      _resourcesClosed.complete();
    }
  }

  void _stop(Exit<Object?, Object?> exit) {
    if (_stopping || _ended) return;
    _stopping = true;
    _stopExit = exit;
    _driver?.requestCancel(reason: _stopReason);
    _notify();
  }

  Task<void, Never> wait(bool Function() ready) =>
      Task.fromCallback<void, Never>((complete) {
        void notify() {
          if (!ready()) return;
          _waiting.remove(notify);
          complete(const Exit.success(null));
        }

        _waiting.add(notify);
        notify();
        return () {
          _waiting.remove(notify);
        };
      });

  void _notify() {
    for (final wake in _waiting.toList()) {
      wake();
    }
  }

  Task<void, Never> close(Exit<Object?, Object?> exit) =>
      Task.uninterruptibleMask(
        (_) => Task.defer(() {
          _closed = true;
          final observed = {
            for (final failure in _failures)
              if (failure.claimed) failure,
          };
          _stop(exit);
          _notify();
          final Task<void, Never> stop;
          if (_started) {
            stop = _await(_stopped.future);
          } else {
            final closed = closeScope(_work, exit);
            if (closed is Future<Cause<Never>?>) {
              stop = _await(closed).map((_) => _removeWork());
            } else {
              _removeWork();
              stop = Task.succeed(null);
            }
          }
          return stop
              .ensuring(
                _lifetime.finish(exit).exit().map((_) {
                  if (_resourcesFinished) _record(scopeCleanupFor(_resources));
                }),
              )
              .ensuring(trim())
              .ensuring(_await(_resourcesClosed.future))
              .flatMap((_) => taskFromEvaluation(_cleanup(observed)));
        }),
      );

  TaskEvaluation<void, Never> _cleanup(
    Set<_SharedFailure<E>> observed, [
    Iterable<_SharedFailure<E>>? failures,
  ]) {
    final causes = <Cause<Never>>[];
    final events = <FailureEvent>[];
    final cleanupCancellations = <CauseCancelled>[];
    for (final failure in failures ?? _failures) {
      if (observed.contains(failure)) continue;
      switch (failure.cause) {
        case CauseFailure():
          continue;
        case final CauseDefect cause:
          causes.add(cause);
          events.add(failure.event!);
        case final CauseCancelled cause:
          causes.add(cause);
        case CauseGroup():
          throw StateError('Cause groups must be flat.');
      }
      if (failure.cause case final CauseCancelled cause when failure.cleanup) {
        cleanupCancellations.add(cause);
      }
      failure.claimed = true;
      observed.add(failure);
    }
    return TaskEvaluation(
      causes.isEmpty
          ? const Exit.success(null)
          : Exit.failure(Cause.combine(causes)),
      List.unmodifiable(events),
      cleanupCancellations: List.unmodifiable(cleanupCancellations),
    );
  }
}

final class _SharedSubscription<T, E> {
  _SharedSubscription(this.shared, this.scope, this.position, this.resources);

  final _SharedFlow<T, E> shared;
  final Scope scope;
  final Scope resources;
  final observed = <_SharedFailure<E>>{};
  late final _lifetime = FlowProducer(scope);
  FlowProducer? _current;
  var position = 0;
  var _detached = false;

  late final cursor = FlowCursor<T, E>(
    _pull(),
    () => _current?.scope ?? scope,
    finish,
    () => [?_current],
    resourceScope: resources,
  );

  Task<Option<T>, E> _pull() => Task.uninterruptibleMask((restore) {
    Task<Option<T>, E> read() => Task.defer(() {
      shared._checkOpen();
      if (scopeIsClosing(scope)) return Task.succeed(const Option.none());
      final current = _current;
      if (current != null) {
        _current = null;
        position++;
        return current
            .finish(const Exit.success(null))
            .ensuring(shared.trim())
            .withError<E>()
            .flatMap((_) => read());
      }
      if (_detached) return Task.succeed(const Option.none());
      final value = shared._values[position];
      if (value != null) return _deliver(value);
      if (shared._ended) return _terminal();
      return restore(
        shared.wait(
          () =>
              shared._closed ||
              shared._ended ||
              shared._values.containsKey(position),
        ),
      ).withError<E>().flatMap((_) => read());
    });

    return read();
  });

  Task<Option<T>, E> _deliver(_SharedValue<T, E> value) {
    final delivery = createScope(parent: scope);
    final removeDependency = addScopeCloseDependency(shared.owner, delivery);
    value.references++;
    _lifetime.retain();
    _current = FlowProducer(delivery, afterClose: (_) => _lifetime.release());
    return inTaskScope(
      Task.addFinalizer(
        (_) => Task.fromFuture(() async {
          final failures = await value.release();
          removeDependency();
          return shared._cleanup(observed, failures);
        }, onError: Error.throwWithStackTrace).flatMap(taskFromEvaluation),
      ),
      delivery,
    ).withError<E>().map((_) => Option.some(value.value));
  }

  Task<Option<T>, E> _terminal() => Task.defer(() {
    final causes = <Cause<E>>[];
    final events = <FailureEvent>[];
    final cleanupCancellations = <CauseCancelled>[];
    for (final failure in shared._failures) {
      if (!observed.add(failure)) continue;
      failure.claimed = true;
      causes.add(failure.cause);
      if (failure.cause case final CauseCancelled cause when failure.cleanup) {
        cleanupCancellations.add(cause);
      }
      if (failure.event case final event?) events.add(event);
    }
    final result = TaskEvaluation<Option<T>, E>(
      causes.isEmpty
          ? const Exit.success(Option.none())
          : Exit.failure(Cause.combine(causes)),
      List.unmodifiable(events),
      cleanupCancellations: List.unmodifiable(cleanupCancellations),
    );
    return finish(result.exit)
        .withError<E>()
        .flatMap((_) => taskFromEvaluation(result));
  });

  Task<void, Never> finish(Exit<Object?, Object?> exit) => Task.defer(() {
    final current = _current;
    _current = null;
    return (current?.finish(exit) ?? Task.succeed(null))
        .ensuring(detach(exit))
        .ensuring(_lifetime.finish(exit));
  });

  Task<void, Never> detach(Exit<Object?, Object?> exit) => Task.defer(() {
    if (_detached) return Task.succeed(null);
    _detached = true;
    shared._subscriptions.remove(this);
    if (shared._subscriptions.isEmpty) shared._stop(exit);
    final stop = shared._stopping && !shared._ended
        ? _await(shared._stopped.future)
        : Task.succeed(null);
    return stop
        .ensuring(shared.trim())
        .flatMap((_) => taskFromEvaluation(shared._cleanup(observed)));
  });
}

final class _SharedValue<T, E> {
  _SharedValue(this.shared, this.value, this.inputs);

  final _SharedFlow<T, E> shared;
  final T value;
  final List<FlowProducer> inputs;
  var references = 1;

  Future<List<_SharedFailure<E>>> release() async {
    if (--references != 0) return const [];
    final result = mergeFlowEvaluations([
      await releaseFlowProducers(inputs),
      await shared._lifetime.release(),
    ]);
    return shared._record(result);
  }
}

final class _SharedFailure<E> {
  _SharedFailure(this.cause, this.event, this.cleanup);

  final Cause<E> cause;
  final FailureEvent? event;
  final bool cleanup;
  var claimed = false;
}

Task<void, Never> _await(Future<void> future) =>
    Task.fromFuture(() => future, onError: Error.throwWithStackTrace);

List<Cause<E>> _causes<E>(Cause<E> cause) => switch (cause) {
  CauseGroup(:final causes) => causes,
  _ => [cause],
};
