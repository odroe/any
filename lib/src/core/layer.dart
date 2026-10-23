import 'dart:async';

import 'context.dart';
import 'exit.dart';
import 'fiber.dart';
import 'scope.dart';
import 'task.dart';

/// A reusable plan for building services and owning their resources.
///
/// A layer produces a [Context] when supplied to `Runtime.create` or
/// [TaskLayerOperations.provide]. Building a layer acquires its services; merely
/// constructing a layer runs no tasks. Resources live for the receiving runtime
/// or local computation.
///
/// Create services with [ServiceLayerOperations.layer], connect dependencies
/// with [LayerOperations.provide], and combine independent outputs with
/// [LayerOperations.merge].
///
/// ```dart
/// final greeting = ServiceKey<String>('greeting');
/// final message = ServiceKey<String>('message');
/// final greetings = Layer.value(greeting.bind('Hello'));
/// final messages = message
///     .layer(Task.service(greeting).map((value) => '$value, Any!'))
///     .provide(greetings);
/// final program = Task.service(message).provide(messages);
/// final runtime = Runtime();
/// try {
///   final value = await runtime.runOrThrow(program);
///   assert(value == 'Hello, Any!');
/// } finally {
///   await runtime.close();
/// }
/// ```
///
/// Within one build, reusing the same layer object shares one acquisition, even
/// across concurrent branches. Its first use supplies the input services and
/// task-local values. Separate builds are independent; use
/// [LayerOperations.fresh] for an independent acquisition within a build.
final class Layer<E> {
  const Layer._(this._create, [this._fresh = false]);

  final Task<Context, E> Function(_BuildSession) _create;
  final bool _fresh;

  /// Creates a layer from an existing service [binding].
  ///
  /// The caller retains ownership of the value and is responsible for its cleanup.
  static Layer<Never> value(ServiceBinding<Object?> binding) =>
      Layer._((_) => Task.succeed(Context.from([binding])));

  /// Combines the services produced by [layers].
  ///
  /// The input collection is copied immediately. Builds run in declaration order
  /// by default; [concurrency] allows up to that many branches to build at once.
  /// Nested merges have their own concurrency limits.
  ///
  /// Output bindings retain declaration order. Duplicate keys fail the build with
  /// a [DuplicateService] defect. A nonpositive [concurrency] fails with a
  /// [RangeError] defect before any branch starts.
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

/// Service construction for a typed [ServiceKey].
extension ServiceLayerOperations<T> on ServiceKey<T> {
  /// Creates a layer that acquires the service identified by this key.
  ///
  /// Read dependencies with [Task.service]. Use [Task.acquireRelease] within
  /// [acquire] when the service needs cleanup at the end of its owning lifetime.
  ///
  /// The acquired value must match the key's original type, even when the key is
  /// accessed through a wider type or `dynamic`; [ServiceKey.bind] enforces this.
  Layer<E> layer<E>(Task<T, E> acquire) =>
      Layer._((_) => acquire.map((value) => Context.from([bind(value)])));
}

/// Composition and error handling for [Layer] descriptions.
extension LayerOperations<E> on Layer<E> {
  /// Creates a layer containing both this layer's and [other]'s services.
  ///
  /// The keys must be distinct. See [Layer.mergeAll] for ordering, concurrency,
  /// and duplicate-key behavior.
  Layer<E> merge(Layer<E> other, {int concurrency = 1}) =>
      Layer.mergeAll([this, other], concurrency: concurrency);

  /// Supplies [dependencies] to this layer without adding them to its output.
  ///
  /// Dependency bindings replace matching input services; other inputs remain
  /// available. Shared dependencies are acquired once per build and kept alive
  /// until the services that use them have been released.
  Layer<E> provide(Layer<E> dependencies) => Layer._(
    (session) => session
        .build(dependencies)
        .flatMap((context) => session.build(this).provideContext(context)),
  );

  /// Transforms business errors produced while building this layer.
  ///
  /// Defects, cancellation, and cause ordering are preserved, as in
  /// [TaskOperations.mapError]. An exception from [transform] becomes a defect.
  Layer<F> mapError<F>(F Function(E error) transform) =>
      Layer._((session) => session.build(this).mapError(transform));

  /// Makes each use of this layer build its services independently.
  ///
  /// Sharing still applies within each independent build of the wrapped subtree.
  /// Acquired resources retain the enclosing runtime's or computation's lifetime.
  Layer<E> fresh() => Layer._((session) => session.build(this), true);
}

/// Error-type widening for layers without business failures.
extension InfallibleLayerOperations on Layer<Never> {
  /// Gives this layer error type [E] without changing its identity or behavior.
  Layer<E> withError<E>() => this;
}

/// Service provisioning for a task and its owned children.
extension TaskLayerOperations<T, E> on Task<T, E> {
  /// Runs this task with services acquired from [layer].
  ///
  /// Each execution builds independently and owns the services until the task and
  /// its children finish. Layer bindings replace matching input services; other
  /// services remain available.
  ///
  /// If construction fails, the task does not start and acquired resources are
  /// released. Success, failure, and cancellation all wait for owned work and
  /// resource cleanup before returning.
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
) =>
    Task.forEach<Layer<E>, Context, E>(
      layers,
      session.build,
      concurrency: concurrency,
    ).map((outputs) {
      var context = Context.from(const []);
      for (final output in outputs) {
        context = context.merge(output);
      }
      return context;
    });
