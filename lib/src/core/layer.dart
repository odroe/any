import 'dart:async';

import 'cause.dart';
import 'context.dart';
import 'exit.dart';
import 'fiber.dart';
import 'scope.dart';
import 'task.dart';

/// A reusable description of services and the resources used to build them.
///
/// A layer builds a [Context] when supplied to Runtime.create or
/// [TaskLayerOperations.provide]. Resources acquired during construction belong
/// to that application or local scope. Constructing a layer runs no tasks.
///
/// Within one build, the same layer object is acquired once, including when
/// requested concurrently. Its first request supplies its input services and
/// task-local values. Use [LayerOperations.fresh] for independent acquisitions.
final class Layer<E> {
  const Layer._(this._create, [this._fresh = false]);

  final Task<Context, E> Function(_BuildSession) _create;
  final bool _fresh;

  /// Provides an existing binding without taking ownership of its resource.
  ///
  /// The caller remains responsible for releasing externally created objects.
  static Layer<Never> value(ServiceBinding<Object?> binding) =>
      Layer._((_) => Task.succeed(Context.from([binding])));

  /// Combines [layers], building in declaration order by default.
  ///
  /// [concurrency] limits the number of these branches being built at once and
  /// must be positive. Larger values allow independent branches to overlap;
  /// nested merges apply their own limits. Output bindings are merged in
  /// declaration order. A duplicate key produces a [DuplicateService] defect.
  ///
  /// The inputs are copied when this method is called. An invalid concurrency
  /// becomes a [RangeError] defect when building, before any branch starts.
  static Layer<E> mergeAll<E>(
    Iterable<Layer<E>> layers, {
    int concurrency = 1,
  }) {
    final inputs = List<Layer<E>>.unmodifiable(layers);
    return Layer._(
      (session) => Task.defer(() {
        if (concurrency < 1) {
          throw RangeError.range(concurrency, 1, null, 'concurrency');
        }
        return concurrency == 1 || inputs.length < 2
            ? _sequential(inputs, session)
            : _parallel(inputs, session, concurrency);
      }),
    );
  }
}

/// Describes the acquisition of a service with this key's type.
extension ServiceLayerOperations<T> on ServiceKey<T> {
  /// Builds this service using [acquire] and publishes its binding.
  ///
  /// Use Task.acquireRelease inside [acquire] to register resource cleanup.
  /// Reading dependencies uses Task.service and the current build context.
  /// A value incompatible with the key's original type fails at binding, even
  /// when this key has been accessed through a wider type or `dynamic`.
  Layer<E> layer<E>(Task<T, E> acquire) =>
      Layer._((_) => acquire.map((value) => Context.from([bind(value)])));
}

/// Combines service descriptions without running their construction tasks.
extension LayerOperations<E> on Layer<E> {
  /// Combines this output with [other]'s distinct service bindings.
  ///
  /// See [Layer.mergeAll] for ordering, concurrency, and duplicate-key rules.
  Layer<E> merge(Layer<E> other, {int concurrency = 1}) =>
      Layer.mergeAll([this, other], concurrency: concurrency);

  /// Builds [dependencies] before this layer, exposing only this layer's output.
  ///
  /// Dependency bindings override matching input services. Other input services
  /// remain available. Dependencies share the same build cache and resource
  /// scope, so shared dependencies are acquired once and released after users.
  Layer<E> provide(Layer<E> dependencies) => Layer._(
    (session) => session
        .build(dependencies)
        .flatMap((context) => session.build(this).provideContext(context)),
  );

  /// Maps business failures from construction while preserving other causes.
  ///
  /// Exceptions thrown by [transform] become defects, following Task.mapError.
  Layer<F> mapError<F>(F Function(E error) transform) =>
      Layer._((session) => session.build(this).mapError(transform));

  /// Builds this subtree with a new cache on every encounter of this wrapper.
  ///
  /// Nodes within that subtree still share acquisitions. Resources retain the
  /// enclosing application's or local computation's lifetime.
  Layer<E> fresh() => Layer._((session) => session.build(this), true);
}

/// Establishes an error type for a layer with no business failures.
extension InfallibleLayerOperations on Layer<Never> {
  /// Gives this layer error type [E] without changing its identity or behavior.
  Layer<E> withError<E>() => this;
}

/// Provides resources for one computation and its owned children.
extension TaskLayerOperations<T, E> on Task<T, E> {
  /// Builds [layer], runs this task with its services, then releases them.
  ///
  /// Each execution has an independent build cache and scope. Services override
  /// matching input bindings and inherit all others. Construction failure skips
  /// this computation and releases acquired resources. Cancellation waits for
  /// owned work and resource cleanup before this task finishes.
  Task<T, E> provide(Layer<E> layer) => accessTaskEnvironment(
    (scope, _) => buildLayer(layer, scope).flatMap(provideContext),
  ).scoped();
}

// Runtime and local provision share this construction boundary.
Task<Context, E> buildLayer<E>(Layer<E> layer, Scope scope) =>
    Task.defer(() => _BuildSession(scope).build(layer));

final class _BuildSession {
  _BuildSession(this.scope);

  final Scope scope;
  final _entries = Map<Layer<Object?>, _BuildEntry>.identity();

  Task<Context, E> build<E>(Layer<E> layer) =>
      Task.uninterruptibleMask((restore) {
        if (layer._fresh) {
          return restore(layer._create(_BuildSession(scope)));
        }
        final existing = _entries[layer];
        if (existing != null) return restore(existing.wait<E>());
        final entry = _BuildEntry();
        _entries[layer] = entry;
        // Publish the entry before starting work. Masking makes reservation and
        // publication atomic with respect to cancellation of the first waiter.
        return forkTaskIn(
          inTaskScope(Task.defer(() => layer._create(this)), scope),
          scope,
        ).exit().withError<E>().flatMap((started) {
          if (started case ExitSuccess(:final value)) {
            // The build session owns the result even if every waiter left.
            unawaited(fiberCompletion(value));
          }
          entry.ready.complete(started);
          return restore(entry.wait<E>());
        });
      });
}

final class _BuildEntry {
  final ready = Completer<Exit<Fiber<Context, Object?>, Never>>();

  Task<Context, E> wait<E>() =>
      Task.fromCallback<Fiber<Context, Object?>, Never>((complete) {
            var waiting = true;
            unawaited(
              ready.future.then((exit) {
                if (waiting) complete(exit);
              }),
            );
            return () {
              waiting = false;
            };
          })
          .withError<E>()
          .flatMap((fiber) => fiber.join().mapError((error) => error as E));
}

Task<Context, E> _sequential<E>(List<Layer<E>> layers, _BuildSession session) {
  var index = 0;
  var context = Context.from(const []);
  Task<Context, E> next() => Task.defer(() {
    if (index == layers.length) return Task.succeed(context);
    return session.build(layers[index++]).flatMap((built) {
      context = context.merge(built);
      return next();
    });
  });
  return next();
}

Task<Context, E> _parallel<E>(
  List<Layer<E>> layers,
  _BuildSession session,
  int concurrency,
) => Task.defer(() {
  final outputs = List<Context?>.filled(layers.length, null);
  final workers = <Fiber<void, E>>[];
  final failures = <FailureEvent>[];
  final recorded = <Object>{};
  final causes = <Cause<E>>[];
  final coordinationCancellation = Object();
  var next = 0;
  var stopping = false;
  var failed = false;
  var finished = 0;
  void Function(Exit<void, Never>)? notify;

  Task<void, E> work() => Task.defer(() {
    if (stopping || next == layers.length) return Task.succeed(null);
    final index = next++;
    return session
        .build(layers[index])
        .flatMap((context) {
          outputs[index] = context;
          return work();
        })
        .mapCause((cause) {
          stopping = true;
          return cause;
        });
  });

  void completed(Fiber<void, E> worker, Exit<void, E> exit) {
    if (exit case ExitFailure(:final cause)) {
      final events = fiberFailureEvents(worker).iterator;
      final reasons = switch (cause) {
        CauseGroup(:final causes) => causes,
        _ => [cause],
      };
      for (final reason in reasons) {
        if (reason is CauseDefect) {
          events.moveNext();
          final event = events.current;
          if (!recorded.add(event.source)) continue;
          failures.add(event);
        } else if (reason is CauseCancelled &&
            identical(reason.reason, coordinationCancellation)) {
          continue;
        }
        causes.add(reason);
      }
      if (!failed) {
        failed = true;
        stopping = true;
        for (final other in workers) {
          if (!identical(other, worker)) {
            other.requestCancel(reason: coordinationCancellation);
          }
        }
      }
    }
    finished++;
    if (finished == workers.length) notify?.call(const Exit.success(null));
  }

  Task<Context, E> start(int remaining) => Task.defer(() {
    if (remaining == 0) {
      for (final worker in workers) {
        listenFiber(worker, (exit) => completed(worker, exit));
      }
      return Task.fromCallback<void, Never>((complete) {
        notify = complete;
        if (finished == workers.length) complete(const Exit.success(null));
        // The enclosing scope stops and waits for workers on cancellation.
        // Their listeners remain until completion to retain cleanup defects.
        return () {
          notify = null;
        };
      }).withError<E>().flatMap((_) {
        if (causes.isNotEmpty) {
          return taskFromEvaluation(
            TaskEvaluation<Context, E>(
              Exit.failure(Cause.combine(causes)),
              failures,
            ),
          );
        }
        return Task.sync(() {
          var result = Context.from(const []);
          for (final output in outputs) {
            result = result.merge(output!);
          }
          return result;
        });
      });
    }
    return work().fork().withError<E>().flatMap((worker) {
      workers.add(worker);
      return start(remaining - 1);
    });
  });

  return Task.addFinalizer(
        (_) => Task.defer(() {
          if (failures.isEmpty) return Task.succeed(null);
          return taskFromEvaluation(
            TaskEvaluation<void, Never>(
              Exit.failure(Cause.combine(failures.map((event) => event.cause))),
              failures,
            ),
          );
        }),
      )
      .withError<E>()
      .flatMap(
        (_) => start(concurrency < layers.length ? concurrency : layers.length),
      )
      .scoped();
});
