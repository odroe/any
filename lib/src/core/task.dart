import 'dart:async';

import 'cancellation.dart';
import 'cause.dart';
import 'context.dart';
import 'exit.dart';
import 'fiber.dart';
import 'internal/execution_mode.dart';
import 'internal/task_concurrency.dart';
import 'internal/task_timing.dart';
import 'internal/task_timeout.dart';
import 'logger.dart';
import 'option.dart';
import 'result.dart';
import 'schedule.dart';
import 'scope.dart';
import 'task_local.dart';

/// A reusable description of work that produces a value or a typed error.
///
/// [T] is the success value and [E] is the expected business error. Constructing
/// or combining tasks does not execute their callbacks. A runtime starts an
/// execution; each run evaluates the description independently.
///
/// ```dart
/// var calls = 0;
/// final next = Task.sync(() => ++calls).map((value) => value * 10);
/// assert(calls == 0);
/// final runtime = Runtime();
/// try {
///   final first = await runtime.runOrThrow(next);
///   final second = await runtime.runOrThrow(next);
///   assert(first == 10);
///   assert(second == 20);
/// } finally {
///   await runtime.close();
/// }
/// ```
///
/// Use [TaskOperations.map] to transform a value and [TaskOperations.flatMap] to
/// continue with another task. [sync], [fromFuture], and [fromCallback] bridge
/// Dart operations; [acquireRelease] gives resources a scoped lifetime.
///
/// An execution ends with an [Exit]. Its [Cause] distinguishes business failures,
/// unexpected exceptions (defects), and cancellation. [Never] rules out business
/// errors, not defects or cancellation. Callback exceptions become defects unless
/// an operation explicitly maps them to business errors.
///
/// Cancellation is cooperative: it stops further work when enabled and waits for
/// owned resources to be cleaned up. [uninterruptibleMask] can defer cancellation
/// across a critical section.
final class Task<T, E> {
  const Task._(this._node);

  final _Node _node;

  /// Writes a structured event through the current [Logger].
  ///
  /// [fields] are copied when this method is called and override inherited fields.
  /// Events below the configured threshold are skipped without reading the clock.
  ///
  /// An accepted write is awaited even if cancellation arrives. Backend errors are
  /// reported separately and do not fail the task.
  static Task<void, Never> log(
    String message, {
    LogLevel level = LogLevel.info,
    Map<String, Object?> fields = const {},
  }) => logTask(message, level: level, fields: fields);

  /// Waits for [duration] using the current context's clock.
  ///
  /// Nonpositive durations use a zero-delay timer. Cancellation removes the timer
  /// and ends the wait. Use [yieldNow] to let other event-loop work advance without
  /// specifying a delay.
  static Task<void, Never> sleep(Duration duration) => sleepTask(duration);

  /// Collects successful task results in input order.
  ///
  /// [concurrency] limits active executions and defaults to sequential execution.
  /// The iterable is read as capacity becomes available and must be finite. The
  /// returned list is immutable; empty input produces an empty list. Nonpositive
  /// concurrency fails with an [ArgumentError] defect.
  ///
  /// A failure stops new work, cancels active siblings, and waits for cleanup.
  /// Observed failures are combined in completion order. Cancellation used to stop
  /// siblings adds no separate cause. Owner cancellation also waits for cleanup
  /// and retains defects from interrupted work.
  ///
  /// Inputs inherit the caller's cancellation state. An enclosing
  /// [uninterruptibleMask] protects them until cancellation is restored. Stopping
  /// a masked sibling waits for its protected work to finish.
  ///
  /// Inputs share the caller's resource scope. Completing a worker leaves its
  /// resources available to later tasks; use [TaskOperations.scoped] on an input
  /// when its resources should be released before collecting its result.
  static Task<List<T>, E> all<T, E>(
    Iterable<Task<T, E>> tasks, {
    int concurrency = 1,
  }) => computeAll(tasks, concurrency: concurrency);

  /// Applies [action] to each value and collects results in input order.
  ///
  /// Each action is created only when a concurrency slot is available. Iteration
  /// and callback exceptions become defects. Input must be finite; concurrency,
  /// failure, cancellation, and immutable-result behavior follow [all].
  static Task<List<T>, E> forEach<A, T, E>(
    Iterable<A> values,
    Task<T, E> Function(A value) action, {
    int concurrency = 1,
  }) => computeForEach(values, action, concurrency: concurrency);

  /// Collects every task's [Exit], allowing individual inputs to fail.
  ///
  /// Results form an immutable list in input order. Business failures, defects,
  /// and explicit cancellation are represented as exits and do not stop other
  /// inputs. Iteration errors still fail the operation.
  ///
  /// Concurrency, cancellation masking, and input requirements follow [all].
  /// Unmasked owner cancellation stops remaining work and waits for cleanup;
  /// defects not delivered in the result remain in the cancellation cause.
  static Task<List<Exit<T, E>>, Never> allSettled<T, E>(
    Iterable<Task<T, E>> tasks, {
    int concurrency = 1,
  }) => computeAllSettled(tasks, concurrency: concurrency);

  /// Returns the outcome of the first task to complete.
  ///
  /// The finite input is started concurrently. Once a winner is observed, no more
  /// inputs start, and active losers are cancelled and cleaned up before returning.
  /// Empty input fails with an [ArgumentError] defect.
  ///
  /// Loser business failures do not change the winner. Loser defects, including
  /// cleanup defects, are retained and can turn a winning success into failure.
  /// Cancellation used to stop losers adds no separate cause.
  /// Resources acquired by participants remain in the caller's scope; apply
  /// [TaskOperations.scoped] to a participant for a separate resource lifetime.
  static Task<T, E> race<T, E>(Iterable<Task<T, E>> tasks) =>
      computeRace(tasks);

  /// Returns the first successful result, or fails if every task fails.
  ///
  /// Tasks run concurrently. The first success stops further input, cancels active
  /// losers, and waits for their cleanup. Input must be finite and nonempty; empty
  /// input fails with an [ArgumentError] defect.
  ///
  /// A winner recovers other participants' business failures. Defects are always
  /// retained, including loser cleanup failures, and can prevent success. If all
  /// participants fail, their causes are combined in completion order.
  /// Participants use the caller's resource scope, as in [race].
  static Task<T, E> firstSuccess<T, E>(Iterable<Task<T, E>> tasks) =>
      computeFirstSuccess(tasks);

  /// Combines two successful results into a Dart record.
  ///
  /// The record contains [left]'s value followed by [right]'s value. The default
  /// runs left then right; [concurrency] of two or more runs both concurrently.
  /// Validation, failure, and cleanup follow [all].
  static Task<(A, B), E> zip<A, B, E>(
    Task<A, E> left,
    Task<B, E> right, {
    int concurrency = 1,
  }) => computeZip(left, right, concurrency: concurrency);

  /// Returns a task that reads the service identified by [key].
  ///
  /// Services come from the runtime or a local [TaskOperations.provideContext].
  /// A missing binding fails with a [MissingService] defect. A nullable service
  /// bound to `null` is still present and succeeds with that value.
  static Task<T, Never> service<T>(ServiceKey<T> key) =>
      Task._(_Service((context) => context.read(key)));

  /// Creates a task that succeeds with the already available [value].
  static Task<T, Never> succeed<T>(T value) => Task._(_Success(value));

  /// Runs [task] conditionally, returning its value as an [Option].
  ///
  /// [condition] is evaluated once per execution. A false result skips the task
  /// and returns [OptionNone]; a successful task produces [OptionSome], including
  /// for a `null` value. An exception from the condition becomes a defect.
  static Task<Option<T>, E> when<T, E>(
    bool Function() condition,
    Task<T, E> task,
  ) => Task.defer(
    () => condition()
        ? task.map((value) => Option.some(value))
        : Task.succeed(const Option.none()),
  );

  /// Updates a state with [step] until [whileTrue] returns false.
  ///
  /// Each execution starts at [initial]. The condition is checked before each
  /// step; the first rejected state is the result. Failures stop the loop.
  /// Synchronous loops are stack safe and respond to cooperative cancellation.
  ///
  /// Steps share one resource scope. Wrap a step in [TaskOperations.scoped] to
  /// release its resources before the next iteration.
  static Task<S, E> loop<S, E>(
    S initial, {
    required bool Function(S state) whileTrue,
    required Task<S, E> Function(S state) step,
  }) => Task.defer(
    () => whileTrue(initial)
        ? step(initial)
              .flatMap((state) => loop(state, whileTrue: whileTrue, step: step))
        : Task.succeed(initial),
  );

  /// Reduces [values] to one state through sequential tasks.
  ///
  /// Starts with [initial] and reads the next element only after [combine]'s
  /// previous task succeeds. Empty input returns [initial]. Each execution creates
  /// a new iterator; iteration and callback exceptions become defects.
  ///
  /// Combiner tasks share the current scope. Use [TaskOperations.scoped] for
  /// resources that should be released after each element.
  static Task<S, E> fold<A, S, E>(
    Iterable<A> values,
    S initial,
    Task<S, E> Function(S state, A value) combine,
  ) => Task.defer(() {
    final iterator = values.iterator;
    Task<S, E> next(S state) => Task.defer(
      () => iterator.moveNext()
          ? combine(state, iterator.current).flatMap(next)
          : Task.succeed(state),
    );
    return next(initial);
  });

  /// Creates a task that fails with the expected business [error].
  static Task<Never, E> fail<E>(E error) =>
      Task._(_Failure(Cause.failure(error)));

  /// Creates a task that fails with a complete [cause].
  ///
  /// Use [fail] for one business error. This constructor also accepts defects,
  /// explicit cancellation, and grouped failures.
  static Task<Never, E> failCause<E>(Cause<E> cause) => Task._(_Failure(cause));

  /// Creates a task that evaluates synchronous [body] on each run.
  ///
  /// Thrown objects become defects. Use [trySync] to map expected exceptions into
  /// business errors.
  static Task<T, Never> sync<T>(T Function() body) => Task._(_Sync(body));

  /// Creates a synchronous task whose exceptions become business errors.
  ///
  /// [body] runs once per execution. [onError] receives a thrown object and its
  /// original stack trace. An exception from [onError] becomes a defect.
  static Task<T, E> trySync<T, E>(
    T Function() body, {
    required E Function(Object error, StackTrace stackTrace) onError,
  }) => Task._(_Sync(body, onError));

  /// Defers task construction until execution.
  ///
  /// [create] runs on each execution, and its returned task supplies the result.
  /// Use this for choices that depend on current state or for lazy recursion.
  /// An exception from [create] becomes a defect.
  static Task<T, E> defer<T, E>(Task<T, E> Function() create) =>
      Task._(_Suspend(() => create()._node));

  /// Protects a critical section from cancellation.
  ///
  /// [body] receives a [RestoreCancellation] function and runs with cancellation
  /// disabled. Wrap interruptible work with `restore` to use the cancellation
  /// state from before this mask. A nested mask cannot restore cancellation that
  /// was already disabled.
  ///
  /// Pending cancellation takes effect when cancellation becomes enabled again,
  /// retaining defects produced in the protected region. Work that never finishes
  /// while masked can delay cancellation indefinitely. A request observed before
  /// entry prevents the body from starting.
  ///
  /// Cancellation state is restored when a region ends, including on failure.
  /// The `restore` function captures that state, not a particular execution or
  /// signal, so tasks built with it remain reusable.
  static Task<T, E> uninterruptibleMask<T, E>(
    Task<T, E> Function(RestoreCancellation restore) body,
  ) => Task._(
    _Mask(
      (interruptible) => body(
        <A, B>(Task<A, B> task) =>
            Task._(_Interruptibility(task._node, interruptible)),
      )._node,
    ),
  );

  /// Creates a task from an already available business [result].
  ///
  /// A [ResultSuccess] becomes task success; a [ResultFailure] becomes a business
  /// failure. Use [defer] if the result itself must be calculated on each run.
  static Task<T, E> fromResult<T, E>(Result<T, E> result) =>
      Task._(switch (result) {
        ResultSuccess(:final value) => _Success(value),
        ResultFailure(:final error) => _Failure(Cause.failure(error)),
      });

  /// Creates a task from an asynchronous Dart operation.
  ///
  /// [create] starts a new [Future] on each run. [onError] maps synchronous throws
  /// and future errors into business errors; an exception from the mapper becomes
  /// a defect.
  ///
  /// A future cannot be cancelled. If enabled cancellation arrives after the
  /// operation starts, the task waits for it to settle and skips subsequent
  /// business callbacks. A late error is retained as a defect with its original
  /// stack trace, alongside cancellation, without calling [onError]. This also
  /// applies if [create] requests cancellation before throwing synchronously.
  /// While cancellation is masked, normal error mapping and callbacks continue.
  ///
  /// To cancel only a caller's wait, start the operation in a longer-lived owner
  /// with `Runtime.start` or [TaskOperations.forkIn], then wait with [Fiber.join].
  /// Acquire resources inside that operation so its owner retains them until the
  /// Future settles. Passing captured objects does not extend their lifetime.
  /// Use [fromCallback] when the native API can acknowledge that it has stopped.
  static Task<T, E> fromFuture<T, E>(
    Future<T> Function() create, {
    required E Function(Object error, StackTrace stackTrace) onError,
  }) => Task._(_FromFuture(create, onError));

  /// Creates a cancellable task from a callback-based operation.
  ///
  /// [register] receives a completion callback and must synchronously return a
  /// cancellation function. Complete with an [Exit] to deliver the result. The
  /// first completion wins; duplicate and late completions are ignored.
  ///
  /// When enabled cancellation arrives first, the task calls the cancellation
  /// function and awaits its cleanup. A request during registration waits until
  /// [register] returns. An already completed operation is not cancelled; a masked
  /// operation keeps waiting for completion.
  ///
  /// The cancellation function must finish only when the operation has stopped
  /// using its resources. Return its asynchronous stop acknowledgment when needed;
  /// merely ignoring further callbacks does not make native work safe to release.
  ///
  /// A registration exception becomes a defect even after synchronous completion.
  /// If that completion failed, its cause precedes the registration defect.
  /// Cancellation cleanup runs in the registration [Zone]; thrown objects retain
  /// their original stack traces as defects alongside cancellation.
  static Task<T, E> fromCallback<T, E>(
    FutureOr<void> Function() Function(void Function(Exit<T, E> exit) complete)
    register,
  ) => Task._(_FromCallback((complete) => register(complete)));

  /// Stops at this point if enabled cancellation has been requested.
  ///
  /// Otherwise succeeds with no value. A masked request is deferred until
  /// cancellation becomes enabled.
  static Task<void, Never> checkCancelled() => const Task._(_Success(null));

  /// Lets timers and other ready event-loop work run before continuing.
  ///
  /// An enabled cancellation request received while yielding stops execution
  /// before the next business callback.
  static Task<void, Never> yieldNow() => const Task._(_Yield());

  /// Registers a finalizer to run when the current [Scope] closes.
  ///
  /// [finalizer] receives the scope's exit. Finalizers run in reverse registration
  /// order with cancellation masked, and all are attempted even after failures.
  /// Cleanup failures are appended to the computation's cause.
  ///
  /// A finalizer can acquire resources or register further cleanup in its own
  /// temporary scope. That scope closes before the next finalizer runs.
  static Task<void, Never> addFinalizer(
    Task<void, Never> Function(Exit<Object?, Object?> exit) finalizer,
  ) => Task._(_AddFinalizer(finalizer));

  /// Acquires a resource that will be released when the current [Scope] closes.
  ///
  /// [release] receives the acquired resource and the scope's exit, and runs with
  /// cancellation masked. Complete all uses within that scope; returning a resource
  /// from `Runtime.run` does not extend its lifetime. Use [using] for a resource
  /// whose lifetime should end immediately after one computation.
  ///
  /// Acquisition and release registration are masked together, so cancellation
  /// cannot leave a successfully acquired resource without cleanup. A failed
  /// acquisition does not call [release]. An acquisition that never finishes can
  /// therefore delay cancellation indefinitely.
  ///
  /// A closing scope rejects new acquisition with a [StateError] defect. An
  /// acquisition already in progress still registers cleanup before stopping.
  static Task<T, E> acquireRelease<T, E>(
    Task<T, E> acquire, {
    required Task<void, Never> Function(T resource, Exit<Object?, Object?> exit)
    release,
  }) => Task._(
    _Mask(
      (_) => acquire
          .flatMap(
            (resource) =>
                addFinalizer((exit) => release(resource, exit))
                    .withError<E>()
                    .map((_) => resource),
          )
          ._node,
      acquisition: true,
    ),
  );

  /// Uses a resource for one computation, then releases it before returning.
  ///
  /// Returns [use]'s value after [release] completes. Release also runs after
  /// failure or cancellation and receives the use's exit. Cleanup failures follow
  /// body failures in the resulting cause.
  ///
  /// Acquisition and release use the cancellation guarantees of [acquireRelease].
  /// Thrown callback exceptions become defects.
  static Task<T, E> using<R, T, E>(
    Task<R, E> acquire, {
    required Task<T, E> Function(R resource) use,
    required Task<void, Never> Function(R resource, Exit<T, E> exit) release,
  }) => acquireRelease(
    acquire,
    release: (resource, exit) => release(resource, _typedExit<T, E>(exit)),
  ).flatMap(use).scoped();
}

/// A wrapper that restores the cancellation state preceding a mask.
///
/// Provided by [Task.uninterruptibleMask]. The wrapped task is lazy and uses its
/// own execution's signal on every run; the wrapper retains only the captured
/// cancellation state.
typedef RestoreCancellation = Task<T, E> Function<T, E>(Task<T, E> task);

/// Composition, resource lifetimes, and error handling for [Task].
extension TaskOperations<T, E> on Task<T, E> {
  /// Fails with a business error if this computation exceeds [duration].
  ///
  /// The deadline uses the current context's clock and includes completion
  /// callbacks. On expiration, the computation is stopped before [onTimeout]
  /// supplies the error. Completion cleanup defects remain alongside that error,
  /// or alongside a defect if [onTimeout] throws. Acquired resources remain in the
  /// caller's scope; use `task.scoped().timeout(...)` to include their release.
  ///
  /// This is a cooperative deadline: a non-cancellable [Future] can delay the
  /// result past the deadline. Nonpositive durations use a zero-delay timer, so
  /// synchronous work may still finish first. Completion or owner cancellation
  /// removes the timer and waits for owned work.
  Task<T, E> timeout(Duration duration, {required E Function() onTimeout}) =>
      timeoutTask(this, duration, onTimeout: onTimeout);

  /// Switches to [fallback] when this computation times out without defects.
  ///
  /// The timed computation is cancelled and cleaned up before the fallback starts.
  /// Any defect, including cleanup failure, prevents fallback. A business failure
  /// before timeout propagates directly.
  ///
  /// Timing and cancellation follow [timeout]. The fallback has no deadline of its
  /// own unless explicitly wrapped.
  Task<T, E> timeoutOrElse(Duration duration, Task<T, E> Function() fallback) =>
      timeoutOrElseTask(this, duration, fallback);

  /// Delays the start of this task by [duration].
  ///
  /// Uses [Task.sleep]; cancelling the wait prevents this task from starting.
  Task<T, E> delay(Duration duration) =>
      Task.sleep(duration).withError<E>().flatMap((_) => this);

  /// Retries recoverable business failures according to [schedule].
  ///
  /// The first attempt starts immediately; `Schedule.recurs(3)` permits three more.
  /// Only a single business failure is eligible. Defects, cancellation, and grouped
  /// failures propagate without retry. [when], if supplied, must also return true;
  /// an exception from it becomes a defect. Exhaustion returns the last failure.
  ///
  /// Attempts use the caller's resource scope. Apply [scoped] before [retry] to
  /// release each attempt's resources before retrying. Completion cleanup is
  /// awaited and its defects prevent retry. Schedule state is fresh per execution;
  /// elapsed time includes attempts and waits. Delays use the current clock.
  Task<T, E> retry(Schedule schedule, {bool Function(E error)? when}) =>
      retryTask(this, schedule, when: when);

  /// Repeats successful executions and returns the last value.
  ///
  /// The first execution starts immediately. After each success, [until] can stop
  /// by returning true; otherwise [schedule] decides whether and when to repeat.
  /// A failed attempt stops repetition. An exception from [until] becomes a defect.
  ///
  /// Attempts use the caller's resource scope. Apply [scoped] before [repeat] for
  /// a separate resource lifetime per attempt. Schedule state is fresh per
  /// execution, and delays use the current clock. Cancellation stops further
  /// attempts and awaits completion cleanup.
  Task<T, E> repeat(Schedule schedule, {bool Function(T value)? until}) =>
      repeatTask(this, schedule, until: until);

  /// Supplies local service bindings for this task and its children.
  ///
  /// [context] replaces matching keys and leaves other services available. The
  /// outer bindings are restored when the task ends. Child fibers inherit the
  /// local bindings; finalizers retain the services present at registration.
  ///
  /// The supplied objects remain externally owned. This operation neither acquires
  /// nor releases them.
  Task<T, E> provideContext(Context context) =>
      Task._(_ProvideContext(_node, context));

  /// Starts a child fiber owned by the current execution.
  ///
  /// The child borrows the current resource [Scope]. Its completion leaves those
  /// resources available to the parent. Parent completion cancels and waits for
  /// unfinished children; closing the borrowed scope also stops them first.
  ///
  /// Use [Fiber.join] to propagate the child's value or failure. The runtime
  /// reports independent failures even if the result is read or joined; starting
  /// a child does not by itself make its business failure the parent's result.
  /// Owning combinators such as [Task.all] receive their workers' results directly.
  Task<Fiber<T, E>, Never> fork() => forkTask(this, reportFailure: true);

  /// Starts a fiber whose lifetime belongs to an explicit [scope].
  ///
  /// Use this when the owner should outlive the task that starts the fiber. The
  /// scope must be open and belong to the same runtime. New resources belong to
  /// that scope; closing it cancels the fiber before releasing them.
  /// Cancellation of the creating task does not cancel the fiber. Independent
  /// failures are reported as with [fork], regardless of listeners or reads.
  /// Captured objects and service bindings retain their original lifetimes.
  ///
  /// A shorter-lived caller can wait with [Fiber.join] and cancel only its wait.
  /// The explicit owner must remain open for the whole operation. With [fork],
  /// the creating execution would instead stop and await the unfinished child.
  Task<Fiber<T, E>, Never> forkIn(Scope scope) => Task._(
    _Fork((_, _, parent, context, locals, _, runScope, _, mode) {
      if (!identical(
        scopeRuntimeId(scope),
        scopeRuntimeId(fiberScope(parent)),
      )) {
        throw StateError('Cannot fork into a scope owned by another runtime.');
      }
      return startFiber(
        this,
        owner: scope,
        resourceScope: scope,
        inheritHost: false,
        runScope: scopeOwnsScope(runScope, scope) ? runScope : null,
        context: context,
        locals: locals,
        report: fiberReport(parent),
        reportFailure: true,
        mode: mode,
      );
    }),
  );

  /// Gives this task a local resource lifetime.
  ///
  /// Each execution creates a child [Scope] and closes it before returning the
  /// result. Child scopes close before their parent's resources. Cleanup failures
  /// follow the body's cause without wrapping or repeating it.
  Task<T, E> scoped() => Task._(_Scoped(_node));

  /// Runs [cleanup] after this task on success, failure, or cancellation.
  ///
  /// Cleanup is registered before this task starts and runs with cancellation
  /// masked. Its failures are retained alongside any body failure. Use [onExit]
  /// when cleanup needs to inspect the outcome.
  Task<T, E> ensuring(Task<void, Never> cleanup) => onExit((_) => cleanup);

  /// Runs [cleanup] with this task's outcome before returning its result.
  ///
  /// The callback and its task run with cancellation masked in the registration
  /// environment. Cleanup failures follow the body's failure. Resources acquired
  /// by cleanup have a temporary scope; body resources keep their original owner.
  ///
  /// This observes the wrapped expression, without waiting for children it forked.
  /// Use `task.scoped().onExit(cleanup)` to observe a local scope's release too.
  Task<T, E> onExit(Task<void, Never> Function(Exit<T, E> exit) cleanup) =>
      Task._(_OnExit(_node, (exit) => cleanup(_typedExit<T, E>(exit))));

  /// Transforms this task's successful value.
  ///
  /// A failure skips [transform]. Use [flatMap] when the transformation itself
  /// returns a task.
  Task<U, E> map<U>(U Function(T value) transform) => Task._(
    _Continue(
      _node,
      _Frame(
        consumesValue: true,
        success: (value) => _Success(transform(value as T)),
      ),
    ),
  );

  /// Continues with the task returned by [next] after success.
  ///
  /// Both tasks share the execution and resource scope. A failure skips [next].
  /// Use [mapError] to align different business error types before composing.
  Task<U, E> flatMap<U>(Task<U, E> Function(T value) next) => Task._(
    _Continue(
      _node,
      _Frame(consumesValue: true, success: (value) => next(value as T)._node),
    ),
  );

  /// Runs [action] on success and preserves the original value.
  ///
  /// A failure from the action fails the combined task.
  Task<T, E> tap(Task<void, E> Function(T value) action) =>
      flatMap((value) => action(value).map((_) => value));

  /// Changes the business error type by transforming each business failure.
  ///
  /// This includes failures inside a [CauseGroup]. Defects, stack traces,
  /// cancellation reasons, and cause ordering are preserved. Exceptions from
  /// [transform] become defects. Owner cancellation stops transformations that
  /// have not started, including later members of a group.
  Task<T, F> mapError<F>(F Function(E error) transform) => Task._(
    _Continue(
      _node,
      _Frame(
        failure: (cause, events, cleanups, signal) => _preserveFailure(
          _mapErrors(cause, (error) => transform(error as E), signal),
          events,
          cleanups,
          preserveOrder: true,
        ),
      ),
    ),
  );

  /// Handles a single business failure with a replacement task.
  ///
  /// Defects, cancellation, and groups bypass [recover]. Use [recoverCause] to
  /// handle a complete cause. Owner cancellation stops recovery.
  Task<T, E> recover(Task<T, E> Function(E error) recover) => Task._(
    _Continue(
      _node,
      _Frame(
        failure: (cause, events, cleanups, _) => switch (cause) {
          CauseFailure(:final error) => recover(error as E)._node,
          _ => _Failure(cause, events, cleanups),
        },
      ),
    ),
  );

  /// Handles a complete failure cause with a replacement task.
  ///
  /// [recover] can inspect business failures, defects, and explicit cancellation.
  /// It cannot override cancellation requested by the execution's owner. An
  /// exception from the callback becomes a defect.
  Task<T, F> recoverCause<F>(Task<T, F> Function(Cause<E> cause) recover) =>
      Task._(
        _Continue(
          _node,
          _Frame(
            failure: (cause, _, _, _) => recover(_typedCause<E>(cause))._node,
          ),
        ),
      );

  /// Replaces a failure's complete cause with [transform]'s result.
  ///
  /// Use [mapError] to change business errors while preserving other reasons.
  /// Owner cancellation skips [transform]; an exception from it becomes a defect.
  Task<T, F> mapCause<F>(Cause<F> Function(Cause<E> cause) transform) => Task._(
    _Continue(
      _node,
      _Frame(
        failure: (cause, events, cleanups, _) {
          final typed = _typedCause<E>(cause);
          return _preserveFailure(
            transform(typed),
            identical(typed, cause) ? events : _retagEvents(typed, events),
            cleanups,
          );
        },
      ),
    ),
  );

  /// Turns this task's success or failure into an [Exit] value.
  ///
  /// This allows inspection of business failures, defects, and explicit
  /// cancellation without propagating them. Cancellation requested by the owner
  /// still terminates the enclosing task.
  Task<Exit<T, E>, Never> exit() => Task._(
    _Continue(
      _node,
      _Frame(
        success: (value) => _Success(Exit<T, E>.success(value as T)),
        failure: (cause, _, _, _) =>
            _Success(Exit<T, E>.failure(_typedCause<E>(cause))),
      ),
    ),
  );

  /// Treats business failures as defects instead of recoverable errors.
  ///
  /// Converted errors receive a stack trace at this boundary. Non-null errors are
  /// retained as the defect object; a `null` error retains its original
  /// [CauseFailure] as the non-null object. Existing defects, their stack traces,
  /// and cancellation remain unchanged.
  Task<T, Never> orDie() => Task._(
    _Continue(
      _node,
      _Frame(
        failure: (cause, events, cleanups, _) => _preserveFailure(
          _defects(cause, StackTrace.current),
          events,
          cleanups,
          preserveOrder: true,
        ),
      ),
    ),
  );
}

/// Error-type widening for tasks without business failures.
extension InfallibleTaskOperations<T> on Task<T, Never> {
  /// Allows this task to participate in a computation with business error [F].
  ///
  /// Returns the same task with a wider static error type. It neither runs the task
  /// nor changes its behavior.
  Task<T, F> withError<F>() => this;
}

// Internal execution results retain provenance only for the current failure.
// Capturing or recovering a failure returns success without these events.
final class TaskEvaluation<T, E> {
  const TaskEvaluation(
    this.exit,
    this.events, {
    this.cleanupCancellations = const [],
    this.pendingFailures = const [],
  });

  factory TaskEvaluation.fresh(Exit<T, E> exit) =>
      TaskEvaluation(exit, switch (exit) {
        ExitFailure(:final cause) => _failureEvents(cause),
        _ => const [],
      });

  final Exit<T, E> exit;
  final List<FailureEvent> events;
  // Only occurrences still present on this failure path, not earlier cleanup
  // failures that have already been captured or recovered.
  final List<CauseCancelled> cleanupCancellations;
  // Captured failures awaiting delivery as a successful value, such as settled
  // results. Cancellation before consumption must retain their defects.
  final List<FailureEvent> pendingFailures;
}

// Runtime's execution boundary. This declaration is not exported by any.dart.
FutureOr<TaskEvaluation<T, E>> evaluateTask<T, E>(
  Task<T, E> task, {
  bool interruptible = true,
  CancellationSignal? signal,
  Scope? scope,
  Scope? resourceScope,
  Scope? runScope,
  Fiber<Object?, Object?>? fiber,
  Context? context,
  Map<TaskLocal<Object?>, Object?> locals = const {},
  List<Scope> closingScopes = const [],
  ExecutionMode? mode,
}) {
  final executionMode = mode ?? ExecutionMode();
  final ownedScope =
      scope ??
      createScope(
        runtimeId: fiber == null ? null : scopeRuntimeId(fiberScope(fiber)),
      );

  FutureOr<TaskEvaluation<T, E>> finish(_EvaluationResult<T, E> evaluated) {
    var (result, cancellationObserved, handledSources, pendingExit) = evaluated;
    if (interruptible &&
        !cancellationObserved &&
        (signal?.isCancelled ?? false)) {
      result = TaskEvaluation(
        _appendCleanup(result.exit, Cause.cancelled(signal!.reason)),
        result.events,
        cleanupCancellations: result.cleanupCancellations,
      );
      cancellationObserved = true;
    }

    TaskEvaluation<T, E> complete() {
      final excluded = {
        ...handledSources,
        for (final event in result.events) event.source,
      };
      var cleanup = scopeCleanupFor(ownedScope, {
        ...excluded,
        ...?pendingExit?.sources,
      });
      // Pending exits can be returned only if cleanup and cancellation preserve
      // the successful value. Otherwise retain their defects in owner cleanup.
      if (pendingExit != null &&
          (result.exit is ExitFailure<T, E> ||
              cleanup.exit is ExitFailure<void, Never> ||
              (interruptible && (signal?.isCancelled ?? false)))) {
        cleanup = scopeCleanupFor(ownedScope, excluded);
      }
      result = _appendEvaluation(result, cleanup);
      if (interruptible &&
          !cancellationObserved &&
          (signal?.isCancelled ?? false)) {
        result = TaskEvaluation(
          _appendCleanup(result.exit, Cause.cancelled(signal!.reason)),
          result.events,
          cleanupCancellations: result.cleanupCancellations,
        );
      }
      if (result.exit is ExitFailure<T, E>) {
        result = _prependPendingCleanup(
          result,
          pendingExit?.cleanup ?? const [],
        );
      }
      return result;
    }

    final closing = closeScope(ownedScope, result.exit, mode: executionMode);
    return closing is Future<Cause<Never>?>
        ? closing.then((_) => complete())
        : complete();
  }

  final evaluated = _evaluate(
    task,
    interruptible: interruptible,
    signal: signal,
    scope: ownedScope,
    resourceScope: resourceScope ?? ownedScope,
    runScope: runScope ?? ownedScope,
    closingScopes: closingScopes,
    fiber: fiber,
    context: context ?? Context.from(const []),
    locals: locals,
    mode: executionMode,
  );
  return evaluated is Future<_EvaluationResult<T, E>>
      ? evaluated.then(finish)
      : finish(evaluated);
}

FutureOr<TaskEvaluation<void, Never>> _evaluateCleanup(
  Task<void, Never> task, {
  required List<Scope> closingScopes,
  required Fiber<Object?, Object?>? fiber,
  required Context context,
  required Map<TaskLocal<Object?>, Object?> locals,
  required ExecutionMode mode,
}) {
  final scope = createScope(
    runtimeId: fiber == null ? null : scopeRuntimeId(fiberScope(fiber)),
  );
  // Cleanup can borrow resources owned elsewhere. Its temporary scope must
  // participate in self-wait checks just like the main fiber's owned scope.
  return inScopeFinalizer(
    scope,
    () => evaluateTask(
      task,
      interruptible: false,
      scope: scope,
      closingScopes: [...closingScopes, scope],
      fiber: fiber,
      context: context,
      locals: locals,
      mode: mode,
    ),
  );
}

// Scope's public operations create instructions without exposing runtime state.
Task<Scope, Never> openScopeTask() => const Task._(_OpenScope());

Task<void, Never> closeScopeTask(Scope scope, Exit<Object?, Object?> exit) =>
    Task._(_CloseScope(scope, exit));

Task<Exit<T, E>, Never> awaitFiberTask<T, E>(
  Fiber<T, E> target, {
  bool cancel = false,
  Object? reason,
}) => Task._(_AwaitFiber(target, cancel: cancel, reason: reason));

Task<T, E> joinFiberTask<T, E>(Fiber<T, E> target) =>
    Task._(_AwaitFiber(target, join: true));

Task<T, Never> readTaskLocal<T>(TaskLocal<T> local, T defaultValue) => Task._(
  _ReadLocal(
    (values) => values.containsKey(local) ? values[local] as T : defaultValue,
  ),
);

Task<R, E> provideTaskLocal<T, R, E>(
  TaskLocal<T> local,
  T value,
  Task<R, E> task,
) => Task._(_ProvideLocal(task._node, local, value));

// Layer construction borrows resource ownership without changing the root
// scope that this execution must close before it finishes.
Task<T, E> inTaskScope<T, E>(Task<T, E> task, Scope scope) =>
    Task._(_InScope(task._node, scope));

// Structured combinators borrow their caller's cancellation region. A pending
// request remains on the child signal so an explicit restore can still see it.
Task<Fiber<T, E>, Never> forkTask<T, E>(
  Task<T, E> task, {
  bool inheritCancellation = false,
  bool reportFailure = false,
}) => Task._(
  _Fork(
    (
      scope,
      owner,
      parent,
      context,
      locals,
      signal,
      runScope,
      interruptible,
      mode,
    ) => startFiber(
      task,
      owner: owner,
      resourceScope: scope,
      signal: signal,
      interruptible: !inheritCancellation || interruptible,
      runScope: runScope,
      context: context,
      locals: locals,
      report: fiberReport(parent),
      reportFailure: reportFailure,
      mode: mode,
    ),
  ),
);

Task<Fiber<T, E>, Never> forkTaskIn<T, E>(
  Task<T, E> task,
  Scope scope, {
  bool inheritHost = false,
  Scope? resourceScope,
  bool deferStart = true,
}) => Task._(
  _Fork(
    (_, _, parent, context, locals, _, runScope, _, mode) => startFiber(
      task,
      owner: scope,
      resourceScope: resourceScope ?? scope,
      inheritHost: inheritHost,
      deferStart: deferStart,
      runScope: scopeOwnsScope(runScope, scope) ? runScope : null,
      context: context,
      locals: locals,
      report: fiberReport(parent),
      reportFailure: false,
      mode: mode,
    ),
  ),
);

Task<T, E> accessTaskEnvironment<T, E>(
  Task<T, E> Function(Scope scope, Context context) use,
) => Task._(_Environment((scope, context, _, _) => use(scope, context)._node));

Task<Scope, Never> currentTaskRunScope() => const Task._(_RunScope());

// A shared service retains its creation environment without retaining the
// creating fiber. Context and task-local maps are immutable snapshots.
typedef TaskEnvironment = ({
  Context context,
  Map<TaskLocal<Object?>, Object?> locals,
  void Function(Cause<Object?>) report,
});

Task<TaskEnvironment, Never> captureTaskEnvironment() => Task._(
  _Environment((_, context, locals, fiber) {
    if (fiber == null) {
      throw StateError(
        'Capturing an environment requires a runtime execution.',
      );
    }
    return _Success((
      context: context,
      locals: locals,
      report: fiberReport(fiber),
    ));
  }),
);

Task<T, E> taskFromEvaluation<T, E>(TaskEvaluation<T, E> result) =>
    Task._(switch (result.exit) {
      ExitSuccess(:final value) => _Success(value, result.pendingFailures),
      ExitFailure(:final cause) => _Failure(
        cause,
        result.events,
        result.cleanupCancellations,
      ),
    });

typedef _PendingExit = ({Set<Object> sources, List<FailureEvent> cleanup});

typedef _EvaluationResult<T, E> = (
  TaskEvaluation<T, E>,
  bool,
  Set<Object>,
  _PendingExit?,
);

FutureOr<_EvaluationResult<T, E>> _evaluate<T, E>(
  Task<T, E> task, {
  required Scope scope,
  required Scope resourceScope,
  required Scope runScope,
  required List<Scope> closingScopes,
  bool interruptible = true,
  CancellationSignal? signal,
  Fiber<Object?, Object?>? fiber,
  required Context context,
  required Map<TaskLocal<Object?>, Object?> locals,
  required ExecutionMode mode,
}) {
  var current = task._node;
  final frames = <_Frame>[];
  // Accepted results affect cleanup merging within this execution only.
  // Finalizers have independent executions, even when they belong to this fiber.
  final handledSources = <Object>{};
  // A captured fiber exit is still only a pending value until a continuation
  // consumes it, or the execution returns it after cleanup.
  _PendingExit? pendingExit;
  var evaluating = true;
  var currentScope = resourceScope;
  var currentOwnedScope = scope;
  var currentContext = context;
  var currentLocals = locals;
  Object? value;
  Cause<Object?>? failure;
  var events = const <FailureEvent>[];
  var cleanupCancellations = const <CauseCancelled>[];
  var remaining = 2048;
  var cancellationObserved = false;

  void finishPendingExit(bool consumed) {
    final pending = pendingExit;
    if (pending == null) return;
    if (consumed) {
      handledSources.addAll(pending.sources);
    } else {
      final completed = _prependPendingCleanup(
        TaskEvaluation<Object?, Object?>(
          failure == null ? Exit.success(value) : Exit.failure(failure!),
          events,
          cleanupCancellations: cleanupCancellations,
        ),
        pending.cleanup,
      );
      failure = switch (completed.exit) {
        ExitFailure(:final cause) => cause,
        _ => null,
      };
      events = completed.events;
      cleanupCancellations = completed.cleanupCancellations;
    }
    pendingExit = null;
  }

  late FutureOr<_EvaluationResult<T, E>> Function() run;

  void defect(Object error, StackTrace stackTrace) {
    failure = Cause.defect(error, stackTrace);
    events = _failureEvents(failure!);
    cleanupCancellations = const [];
    evaluating = false;
  }

  Future<_EvaluationResult<T, E>>? waitFor<A>(
    FutureOr<A> pending,
    void Function(A) resume, {
    void Function(Object, StackTrace)? onError,
  }) {
    if (pending is! Future<A>) {
      resume(pending);
      return null;
    }
    return pending.then<_EvaluationResult<T, E>>(
      (value) {
        try {
          resume(value);
        } catch (error, stackTrace) {
          defect(error, stackTrace);
        }
        return run();
      },
      onError: (Object error, StackTrace stackTrace) {
        try {
          (onError ?? defect)(error, stackTrace);
        } catch (error, stackTrace) {
          defect(error, stackTrace);
        }
        return run();
      },
    );
  }

  run = () {
    while (true) {
      final effectiveSignal = interruptible ? signal : null;
      if (!cancellationObserved && (effectiveSignal?.isCancelled ?? false)) {
        finishPendingExit(false);
        failure = _cancelled(failure, effectiveSignal!.reason);
        events = _failureEvents(failure!, events);
        cancellationObserved = true;
        // Completion frames already removed from the stack still have to run.
        current = switch (current) {
          _FinishScope(:final scope) when evaluating => _FinishScope(
            scope,
            Exit.failure(failure!),
            events,
            cleanupCancellations,
          ),
          _FinishCleanup(:final run) when evaluating => _FinishCleanup(
            run,
            Exit.failure(failure!),
            events,
            cleanupCancellations,
          ),
          _ => current,
        };
        if (current is! _FinishScope && current is! _FinishCleanup) {
          evaluating = false;
        }
      }
      if (mode.automaticYield && --remaining == 0) {
        remaining = 2048;
        return waitFor(Future<void>.delayed(Duration.zero), (_) {})!;
      }

      try {
        if (evaluating) {
          switch (current) {
            case _RunScope():
              value = runScope;
              failure = null;
              events = const [];
              cleanupCancellations = const [];
              evaluating = false;
            case _Environment(:final use):
              current = use(currentScope, currentContext, currentLocals, fiber);
            case _InScope(:final source, scope: final borrowed):
              final release = fiber == null || identical(scope, borrowed)
                  ? null
                  : borrowScopeFiber(borrowed, fiber, untilReleased: true);
              frames.add(_Frame(scope: currentScope, releaseBorrow: release));
              currentScope = borrowed;
              current = source;
            case _ReadLocal(:final read):
              value = read(currentLocals);
              failure = null;
              events = const [];
              cleanupCancellations = const [];
              evaluating = false;
            case _ProvideLocal(
              :final source,
              :final local,
              value: final provided,
            ):
              frames.add(_Frame(locals: currentLocals));
              currentLocals = Map.unmodifiable({
                ...currentLocals,
                local: provided,
              });
              current = source;
            case _Service(:final read):
              value = read(currentContext);
              failure = null;
              events = const [];
              cleanupCancellations = const [];
              evaluating = false;
            case _ProvideContext(:final source, :final context):
              frames.add(_Frame(context: currentContext));
              currentContext = overlayContexts(currentContext, context);
              current = source;
            case _Fork(:final start):
              if (fiber == null) {
                throw StateError('Fork requires an active runtime execution.');
              }
              value = start(
                currentScope,
                currentOwnedScope,
                fiber,
                currentContext,
                currentLocals,
                signal,
                runScope,
                interruptible,
                mode,
              );
              failure = null;
              events = const [];
              cleanupCancellations = const [];
              evaluating = false;
            case _AwaitFiber(
              :final target,
              :final cancel,
              :final reason,
              :final join,
            ):
              if (fiberIsActive(target) &&
                  fiber != null &&
                  (identical(target, fiber) ||
                      scopeWaitsForFiber(fiberScope(target), fiber) ||
                      currentFibers().contains(target))) {
                throw StateError(
                  'A fiber cannot wait for itself or an active ancestor.',
                );
              }
              if (cancel) target.requestCancel(reason: reason);
              var delivered = const <FailureEvent>[];
              var deliveredCleanups = const <CauseCancelled>[];
              if (!join) {
                pendingExit = (sources: <Object>{}, cleanup: <FailureEvent>[]);
              }
              final pending = _runCallback(
                (complete) => target.onExit((exit) {
                  delivered = fiberFailureEvents(target);
                  deliveredCleanups = fiberCleanupCancellations(target);
                  final sources = delivered.map((event) => event.source);
                  if (join) {
                    handledSources.addAll(sources);
                  } else {
                    pendingExit!.sources.addAll(sources);
                  }
                  complete(join ? exit : Exit.success(exit));
                }),
                effectiveSignal,
              );
              final suspended = waitFor(pending, (exit) {
                switch (exit) {
                  case ExitSuccess(value: final result):
                    value = result;
                    failure = null;
                    events = const [];
                    cleanupCancellations = const [];
                  case ExitFailure(:final cause):
                    failure = cause;
                    events = _failureEvents(cause, delivered);
                    cleanupCancellations = join ? deliveredCleanups : const [];
                }
                evaluating = false;
              });
              if (suspended != null) return suspended;
            case _Success(value: final result, :final pendingFailures):
              if (pendingFailures.isNotEmpty) {
                pendingExit = (
                  sources: {for (final event in pendingFailures) event.source},
                  cleanup: [...pendingFailures],
                );
              }
              value = result;
              failure = null;
              events = const [];
              cleanupCancellations = const [];
              evaluating = false;
            case _Failure(
              :final cause,
              events: final sources,
              cleanupCancellations: final cleanups,
            ):
              failure = cause;
              events = sources ?? _failureEvents(cause);
              cleanupCancellations = cleanups;
              evaluating = false;
            case _Sync(:final body, :final onError):
              try {
                current = _Success(body());
              } catch (error, stackTrace) {
                failure =
                    onError == null || (effectiveSignal?.isCancelled ?? false)
                    ? Cause.defect(error, stackTrace)
                    : Cause.failure(onError(error, stackTrace));
                events = _failureEvents(failure!);
                cleanupCancellations = const [];
                evaluating = false;
              }
            case _Suspend(:final create):
              current = create();
            case _Mask(:final create, :final acquisition):
              if (acquisition && scopeIsClosing(currentScope)) {
                throw StateError(
                  'Cannot acquire resources in a closing or closed scope.',
                );
              }
              final previous = interruptible;
              frames.add(_Frame(interruptible: previous));
              interruptible = false;
              current = create(previous);
            case _Interruptibility(
              :final source,
              interruptible: final restored,
            ):
              frames.add(_Frame(interruptible: interruptible));
              interruptible = restored;
              current = source;
            case _FromFuture(:final create, :final onError):
              void fail(Object error, StackTrace stackTrace) {
                failure = effectiveSignal?.isCancelled ?? false
                    ? Cause.defect(error, stackTrace)
                    : Cause.failure(onError(error, stackTrace));
                events = _failureEvents(failure!);
                cleanupCancellations = const [];
                evaluating = false;
              }
              try {
                // A user Future may invoke then synchronously. Normalize it so
                // this instruction always suspends before continuing.
                return waitFor<Object?>(Future<Object?>.value(create()), (
                  result,
                ) {
                  value = result;
                  failure = null;
                  events = const [];
                  cleanupCancellations = const [];
                  evaluating = false;
                }, onError: fail)!;
              } catch (error, stackTrace) {
                fail(error, stackTrace);
              }
            case _FromCallback(:final register):
              final suspended = waitFor(
                _runCallback(register, effectiveSignal),
                (exit) {
                  switch (exit) {
                    case ExitSuccess(value: final result):
                      value = result;
                      failure = null;
                      events = const [];
                      cleanupCancellations = const [];
                    case ExitFailure(:final cause):
                      failure = cause;
                      events = _failureEvents(cause);
                      cleanupCancellations = const [];
                  }
                  evaluating = false;
                },
              );
              if (suspended != null) return suspended;
            case _Yield():
              return waitFor(Future<void>.delayed(Duration.zero), (_) {
                value = null;
                failure = null;
                events = const [];
                cleanupCancellations = const [];
                evaluating = false;
              })!;
            case _Continue(:final source, :final frame):
              frames.add(frame);
              current = source;
            case _OpenScope():
              value = createScope(parent: currentScope);
              failure = null;
              events = const [];
              cleanupCancellations = const [];
              evaluating = false;
            case _AddFinalizer(:final finalizer):
              final zone = Zone.current;
              final services = currentContext;
              final localValues = currentLocals;
              final registrationScope = currentScope;
              final pending = addScopeFinalizer(
                currentScope,
                (exit) => zone.run(
                  () => inScopeFinalizer(
                    registrationScope,
                    () => finalizer(exit),
                  ),
                ),
                mode: mode,
                runFinalizer: (task, scope, closingMode) => zone.run(
                  () => inScopeFinalizer(
                    scope,
                    () => _evaluateCleanup(
                      task,
                      closingScopes: [...closingScopes, scope],
                      fiber: fiber,
                      context: services,
                      locals: localValues,
                      mode: closingMode,
                    ),
                  ),
                ),
              );
              final suspended = waitFor(pending, (result) {
                failure = switch (result?.exit) {
                  ExitFailure(:final cause) => cause,
                  _ => null,
                };
                events = result?.events ?? const [];
                cleanupCancellations = switch (result?.exit) {
                  ExitFailure(:final cause) => _cancellations(cause),
                  _ => const [],
                };
                value = null;
                evaluating = false;
              });
              if (suspended != null) return suspended;
            case _OnExit(:final source, :final cleanup):
              final zone = Zone.current;
              final services = currentContext;
              final localValues = currentLocals;
              FutureOr<TaskEvaluation<void, Never>> run(
                Exit<Object?, Object?> exit,
              ) => zone.run(
                () => _evaluateCleanup(
                  Task.defer(() => cleanup(exit)),
                  fiber: fiber,
                  context: services,
                  locals: localValues,
                  closingScopes: closingScopes,
                  mode: mode,
                ),
              );
              frames.add(
                _Frame(
                  cleanup: true,
                  success: (value) =>
                      _FinishCleanup(run, Exit.success(value), const []),
                  failure: (cause, events, cleanups, _) => _FinishCleanup(
                    run,
                    Exit.failure(cause),
                    events,
                    cleanups,
                  ),
                ),
              );
              current = source;
            case _FinishCleanup(
              :final run,
              :final exit,
              events: final bodyEvents,
              cleanupCancellations: final bodyCleanups,
            ):
              final suspended = waitFor(run(exit), (cleanup) {
                final completed = _appendEvaluation(
                  TaskEvaluation(
                    exit,
                    bodyEvents,
                    cleanupCancellations: bodyCleanups,
                  ),
                  cleanup,
                );
                switch (completed.exit) {
                  case ExitSuccess(value: final result):
                    value = result;
                    failure = null;
                  case ExitFailure(:final cause):
                    failure = cause;
                }
                events = completed.events;
                cleanupCancellations = completed.cleanupCancellations;
                if (failure != null) finishPendingExit(false);
                evaluating = false;
              });
              if (suspended != null) return suspended;
            case _Scoped(:final source):
              final parent = currentScope;
              final parentOwned = currentOwnedScope;
              // Local boundaries belong to this execution's lexical cleanup tree.
              // A borrowed resource scope may close while this body unwinds.
              final child = createScope(parent: parentOwned);
              currentScope = child;
              currentOwnedScope = child;
              frames.add(
                _Frame(
                  cleanup: true,
                  success: (value) {
                    currentScope = parent;
                    currentOwnedScope = parentOwned;
                    return _FinishScope(child, Exit.success(value), const []);
                  },
                  failure: (cause, events, cleanups, _) {
                    currentScope = parent;
                    currentOwnedScope = parentOwned;
                    return _FinishScope(
                      child,
                      Exit.failure(cause),
                      events,
                      cleanups,
                    );
                  },
                ),
              );
              current = source;
            case _FinishScope(
              :final scope,
              :final exit,
              events: final bodyEvents,
              cleanupCancellations: final bodyCleanups,
            ):
              final suspended = waitFor(closeScope(scope, exit, mode: mode), (
                _,
              ) {
                final excluded = {
                  ...handledSources,
                  for (final event in bodyEvents) event.source,
                };
                final fullCleanup = scopeCleanupFor(scope, excluded);
                final filteredCleanup = scopeCleanupFor(scope, {
                  ...excluded,
                  ...?pendingExit?.sources,
                });
                final canDeliver =
                    exit is ExitSuccess &&
                    filteredCleanup.exit is ExitSuccess<void, Never> &&
                    !(effectiveSignal?.isCancelled ?? false);
                if (pendingExit case final pending? when canDeliver) {
                  // This scope detaches after closing. Keep the excluded occurrences
                  // until delivery so a later cancellation or failure can retain them.
                  pending.cleanup.addAll(
                    fullCleanup.events.where(
                      (event) => pending.sources.contains(event.source),
                    ),
                  );
                }
                final completed = _appendEvaluation(
                  TaskEvaluation(
                    exit,
                    bodyEvents,
                    cleanupCancellations: bodyCleanups,
                  ),
                  canDeliver ? filteredCleanup : fullCleanup,
                );
                switch (completed.exit) {
                  case ExitSuccess(value: final result):
                    value = result;
                    failure = null;
                  case ExitFailure(:final cause):
                    failure = cause;
                }
                events = completed.events;
                cleanupCancellations = completed.cleanupCancellations;
                if (!canDeliver) finishPendingExit(false);
                evaluating = false;
              });
              if (suspended != null) return suspended;
            case _CloseScope(:final scope, :final exit):
              if (scopeCloseWouldDeadlock(scope, currentScope) ||
                  (fiber != null &&
                      [fiber, ...currentFibers()].any(
                        (member) =>
                            fiberIsActive(member) &&
                            scopeWaitsForFiber(scope, member),
                      )) ||
                  closingScopes.any(
                    (owner) => scopeCloseWouldDeadlock(scope, owner),
                  )) {
                throw StateError(
                  'Closing this scope would wait on the caller.',
                );
              }
              final suspended = waitFor(closeScope(scope, exit, mode: mode), (
                _,
              ) {
                final completed = scopeCleanupFor(scope);
                failure = switch (completed.exit) {
                  ExitFailure(:final cause) => cause,
                  _ => null,
                };
                events = completed.events;
                cleanupCancellations = completed.cleanupCancellations;
                handledSources.addAll(events.map((event) => event.source));
                value = null;
                evaluating = false;
              });
              if (suspended != null) return suspended;
          }
          continue;
        }

        if (frames.isEmpty) {
          final Exit<T, E> exit = failure == null
              ? Exit.success(value as T)
              : Exit.failure(_typedCause<E>(failure!));
          if (exit case ExitFailure(:final cause)
              when !identical(cause, failure)) {
            events = _retagEvents(cause, events);
          }
          return (
            TaskEvaluation(
              exit,
              events,
              cleanupCancellations: cleanupCancellations,
            ),
            cancellationObserved,
            handledSources,
            pendingExit,
          );
        }

        final frame = frames.removeLast();
        if (frame.scope case final restored?) {
          frame.releaseBorrow?.call();
          currentScope = restored;
          continue;
        }
        if (frame.locals case final restored?) {
          currentLocals = restored;
          continue;
        }
        if (frame.context case final restored?) {
          currentContext = restored;
          continue;
        }
        if (frame.interruptible case final restored?) {
          interruptible = restored;
          continue;
        }
        if (cancellationObserved && !frame.cleanup) continue;
        if (failure case final cause?) {
          final resume = frame.failure;
          if (resume == null) continue;
          finishPendingExit(false);
          current = resume(
            failure ?? cause,
            events,
            cleanupCancellations,
            effectiveSignal,
          );
        } else {
          final resume = frame.success;
          if (resume == null) continue;
          if (frame.consumesValue) finishPendingExit(true);
          current = resume(value);
        }
        // A continuation has already produced this failure. Record it before the
        // next cancellation check so defects cannot disappear between frames.
        if (current case _Failure(
          :final cause,
          events: final sources,
          cleanupCancellations: final cleanups,
        )) {
          failure = cause;
          events = sources ?? _failureEvents(cause);
          cleanupCancellations = cleanups;
          evaluating = false;
        } else if (current case _FinishScope(
          exit: ExitFailure(:final cause),
          events: final sources,
          cleanupCancellations: final cleanups,
        )) {
          // Closing still has to execute, but cancellation before it starts must
          // retain the body failure that is already being handed to the scope.
          failure = cause;
          events = sources;
          cleanupCancellations = cleanups;
          evaluating = true;
        } else if (current case _FinishCleanup(
          exit: ExitFailure(:final cause),
          events: final sources,
          cleanupCancellations: final cleanups,
        )) {
          failure = cause;
          events = sources;
          cleanupCancellations = cleanups;
          evaluating = true;
        } else {
          failure = null;
          events = const [];
          cleanupCancellations = const [];
          evaluating = true;
        }
      } catch (error, stackTrace) {
        defect(error, stackTrace);
      }
    }
  };
  return run();
}

// Cause transformations can retain an occurrence from their current input.
// A new task failure has no prior events, even when it reuses the same Cause.
List<FailureEvent> _failureEvents(
  Cause<Object?> cause, [
  Iterable<FailureEvent> previous = const [],
]) {
  final available = Map<Cause<Object?>, List<FailureEvent>>.identity();
  for (final event in previous.toList().reversed) {
    (available[event.cause] ??= []).add(event);
  }
  final reasons = switch (cause) {
    CauseGroup(:final causes) => causes,
    _ => [cause],
  };
  return List.unmodifiable([
    for (final reason in reasons)
      if (reason is! CauseCancelled)
        if (available[reason] case final candidates? when candidates.isNotEmpty)
          candidates.removeLast()
        else
          FailureEvent(reason, FailureSource()),
  ]);
}

// Type casts, mapError, and orDie keep the same ordered failure occurrences.
List<FailureEvent> _retagEvents(
  Cause<Object?> cause,
  List<FailureEvent> previous,
) {
  final sources = previous.iterator;
  final events = <FailureEvent>[];
  for (final reason in switch (cause) {
    CauseGroup(:final causes) => causes,
    _ => [cause],
  }) {
    if (reason is CauseCancelled) continue;
    sources.moveNext();
    final event = sources.current;
    events.add(
      identical(reason, event.cause)
          ? event
          : FailureEvent(reason, event.source),
    );
  }
  return List.unmodifiable(events);
}

List<CauseCancelled> _cancellations(Cause<Object?> cause) => switch (cause) {
  CauseCancelled() => [cause],
  CauseGroup(:final causes) => causes.whereType<CauseCancelled>().toList(),
  _ => const [],
};

_Failure _preserveFailure(
  Cause<Object?> cause,
  List<FailureEvent> events,
  List<CauseCancelled> cleanupCancellations, {
  bool preserveOrder = false,
}) {
  final retained = preserveOrder
      ? _retagEvents(cause, events)
      : _failureEvents(cause, events);
  if (cleanupCancellations.isEmpty) {
    return _Failure(cause, retained);
  }
  final available = [...cleanupCancellations];
  return _Failure(cause, retained, [
    for (final cancellation in _cancellations(cause))
      if (available.remove(cancellation)) cancellation,
  ]);
}

TaskEvaluation<T, E> _prependPendingCleanup<T, E>(
  TaskEvaluation<T, E> result,
  List<FailureEvent> pending,
) {
  if (pending.isEmpty) return result;
  final sources = {for (final event in result.events) event.source};
  final events = [
    for (final event in pending)
      if (event.cause is CauseDefect && sources.add(event.source)) event,
  ];
  if (events.isEmpty) return result;
  return TaskEvaluation(
    Exit.failure(
      Cause<E>.combine([
        for (final event in events) event.cause as CauseDefect,
        if (result.exit case ExitFailure(:final cause)) cause,
      ]),
    ),
    [...events, ...result.events],
    cleanupCancellations: result.cleanupCancellations,
  );
}

TaskEvaluation<T, E> _appendEvaluation<T, E>(
  TaskEvaluation<T, E> result,
  TaskEvaluation<void, Never> cleanup,
) {
  final cleanupCause = switch (cleanup.exit) {
    ExitFailure(:final cause) => cause,
    _ => null,
  };
  if (cleanupCause == null) return result;
  final sources = {for (final event in result.events) event.source};
  final pending = cleanup.events.iterator;
  final causes = <Cause<Never>>[];
  final events = <FailureEvent>[];
  for (final cause in switch (cleanupCause) {
    CauseGroup(:final causes) => causes,
    _ => [cleanupCause],
  }) {
    if (cause is CauseDefect) {
      pending.moveNext();
      final event = pending.current;
      if (!sources.add(event.source)) continue;
      events.add(event);
    }
    causes.add(cause);
  }
  if (causes.isEmpty) return result;
  final cause = Cause<Never>.combine(causes);
  return TaskEvaluation(
    _appendCleanup(result.exit, cause),
    [...result.events, ...events],
    cleanupCancellations: [
      ...result.cleanupCancellations,
      ..._cancellations(cause),
    ],
  );
}

FutureOr<Exit<Object?, Object?>> _runCallback(
  _RegisterCallback register,
  CancellationSignal? signal,
) {
  final zone = Zone.current;
  final completion = Completer<Exit<Object?, Object?>>();
  Exit<Object?, Object?>? completed;
  Exit<Object?, Object?>? settled;
  _CancelCallback? cancel;
  var registered = false;
  var cancelled = false;
  var cleaning = false;
  void Function()? unsubscribe;

  void settle(Exit<Object?, Object?> exit) {
    unsubscribe?.call();
    settled = exit;
    completion.complete(exit);
  }

  Future<void> cleanup() async {
    if (cleaning) return;
    cleaning = true;
    try {
      final pending = cancel!();
      if (pending is Future<void>) await pending;
      // The execution loop reports cancellation after cleanup has finished.
      settle(const Exit.success(null));
    } catch (error, stackTrace) {
      settle(Exit.failure(Cause.defect(error, stackTrace)));
    }
  }

  unsubscribe = signal?.onCancel((_) {
    if (completed != null || cancelled || completion.isCompleted) return;
    cancelled = true;
    if (registered) unawaited(zone.run(cleanup));
  });

  if (cancelled) {
    settle(const Exit.success(null));
    return settled ?? completion.future;
  }

  try {
    cancel = register((exit) {
      if (completed != null || cancelled || completion.isCompleted) return;
      completed = exit;
      if (registered) settle(exit);
    });
  } catch (error, stackTrace) {
    final defect = Cause<Object?>.defect(error, stackTrace);
    settle(
      Exit.failure(switch (completed) {
        ExitFailure(:final cause) => Cause.combine([cause, defect]),
        _ => defect,
      }),
    );
    return settled ?? completion.future;
  }

  registered = true;
  if (cancelled) {
    unawaited(zone.run(cleanup));
  } else if (completed case final exit?) {
    settle(exit);
  }
  return settled ?? completion.future;
}

typedef _CancelCallback = FutureOr<void> Function();
typedef _RegisterCallback = _CancelCallback Function(
  void Function(Exit<Object?, Object?>) complete,
);

Cause<E> _typedCause<E>(Cause<Object?> cause) =>
    cause is Cause<E> ? cause : cause.mapError((error) => error as E);

Exit<T, E> _typedExit<T, E>(Exit<Object?, Object?> exit) => switch (exit) {
  ExitSuccess(:final value) => Exit.success(value as T),
  ExitFailure(:final cause) => Exit.failure(_typedCause<E>(cause)),
};

Exit<T, E> _appendCleanup<T, E>(Exit<T, E> exit, Cause<Never>? cleanup) {
  if (cleanup == null) return exit;
  return switch (exit) {
    ExitSuccess() => Exit.failure(cleanup),
    ExitFailure(:final cause) => Exit.failure(Cause.combine([cause, cleanup])),
  };
}

Cause<Object?> _mapErrors(
  Cause<Object?> cause,
  Object? Function(Object?) transform,
  CancellationSignal? signal,
) {
  if (cause case CauseGroup(:final causes)) {
    final mapped = <Cause<Object?>>[];
    for (final reason in causes) {
      if (signal?.isCancelled ?? false) return cause;
      mapped.add(reason.mapError(transform));
    }
    return Cause.combine(mapped);
  }
  return cause.mapError(transform);
}

Cause<Never> _defects(Cause<Object?> cause, StackTrace stackTrace) =>
    switch (cause) {
      CauseFailure(:final error) => Cause.defect(error ?? cause, stackTrace),
      CauseDefect() => cause,
      CauseCancelled() => cause,
      CauseGroup(:final causes) => Cause.combine(
        causes.map((reason) => _defects(reason, stackTrace)),
      ),
    };

Cause<Never> _cancelled(Cause<Object?>? failure, Object? reason) {
  final retained = <Cause<Never>>[];
  final causes = switch (failure) {
    null => const <Cause<Object?>>[],
    CauseGroup(:final causes) => causes,
    _ => [failure],
  };
  for (final cause in causes) {
    switch (cause) {
      case CauseDefect():
        retained.add(cause);
      case CauseCancelled():
        retained.add(cause);
      default:
        break;
    }
  }
  retained.add(Cause.cancelled(reason));
  return Cause.combine(retained);
}

sealed class _Node {
  const _Node();
}

final class _Fork extends _Node {
  const _Fork(this.start);
  final Object Function(
    Scope,
    Scope,
    Fiber<Object?, Object?>,
    Context,
    Map<TaskLocal<Object?>, Object?>,
    CancellationSignal?,
    Scope,
    bool,
    ExecutionMode,
  )
  start;
}

final class _Service extends _Node {
  const _Service(this.read);
  final Object? Function(Context) read;
}

final class _Environment extends _Node {
  const _Environment(this.use);
  final _Node Function(
    Scope,
    Context,
    Map<TaskLocal<Object?>, Object?>,
    Fiber<Object?, Object?>?,
  )
  use;
}

final class _RunScope extends _Node {
  const _RunScope();
}

final class _InScope extends _Node {
  const _InScope(this.source, this.scope);
  final _Node source;
  final Scope scope;
}

final class _ReadLocal extends _Node {
  const _ReadLocal(this.read);
  final Object? Function(Map<TaskLocal<Object?>, Object?>) read;
}

final class _ProvideLocal extends _Node {
  const _ProvideLocal(this.source, this.local, this.value);
  final _Node source;
  final TaskLocal<Object?> local;
  final Object? value;
}

final class _ProvideContext extends _Node {
  const _ProvideContext(this.source, this.context);
  final _Node source;
  final Context context;
}

final class _AwaitFiber extends _Node {
  const _AwaitFiber(
    this.target, {
    this.cancel = false,
    this.reason,
    this.join = false,
  });
  final Fiber<Object?, Object?> target;
  final bool cancel;
  final Object? reason;
  final bool join;
}

final class _Success extends _Node {
  const _Success(this.value, [this.pendingFailures = const []]);
  final Object? value;
  final List<FailureEvent> pendingFailures;
}

final class _Failure extends _Node {
  const _Failure(
    this.cause, [
    this.events,
    this.cleanupCancellations = const [],
  ]);
  final Cause<Object?> cause;
  final List<FailureEvent>? events;
  final List<CauseCancelled> cleanupCancellations;
}

final class _Sync extends _Node {
  const _Sync(this.body, [this.onError]);
  final Object? Function() body;
  final Object? Function(Object, StackTrace)? onError;
}

final class _Suspend extends _Node {
  const _Suspend(this.create);
  final _Node Function() create;
}

final class _Mask extends _Node {
  const _Mask(this.create, {this.acquisition = false});
  final _Node Function(bool interruptible) create;
  final bool acquisition;
}

final class _OnExit extends _Node {
  const _OnExit(this.source, this.cleanup);
  final _Node source;
  final Task<void, Never> Function(Exit<Object?, Object?>) cleanup;
}

final class _FinishCleanup extends _Node {
  const _FinishCleanup(
    this.run,
    this.exit,
    this.events, [
    this.cleanupCancellations = const [],
  ]);
  final FutureOr<TaskEvaluation<void, Never>> Function(Exit<Object?, Object?>)
  run;
  final Exit<Object?, Object?> exit;
  final List<FailureEvent> events;
  final List<CauseCancelled> cleanupCancellations;
}

final class _Interruptibility extends _Node {
  const _Interruptibility(this.source, this.interruptible);
  final _Node source;
  final bool interruptible;
}

final class _FromFuture extends _Node {
  const _FromFuture(this.create, this.onError);
  final Future<Object?> Function() create;
  final Object? Function(Object, StackTrace) onError;
}

final class _FromCallback extends _Node {
  const _FromCallback(this.register);
  final _RegisterCallback register;
}

final class _Yield extends _Node {
  const _Yield();
}

final class _Continue extends _Node {
  const _Continue(this.source, this.frame);
  final _Node source;
  final _Frame frame;
}

final class _OpenScope extends _Node {
  const _OpenScope();
}

final class _AddFinalizer extends _Node {
  const _AddFinalizer(this.finalizer);
  final Task<void, Never> Function(Exit<Object?, Object?>) finalizer;
}

final class _Scoped extends _Node {
  const _Scoped(this.source);
  final _Node source;
}

final class _FinishScope extends _Node {
  const _FinishScope(
    this.scope,
    this.exit,
    this.events, [
    this.cleanupCancellations = const [],
  ]);
  final Scope scope;
  final Exit<Object?, Object?> exit;
  final List<FailureEvent> events;
  final List<CauseCancelled> cleanupCancellations;
}

final class _CloseScope extends _Node {
  const _CloseScope(this.scope, this.exit);
  final Scope scope;
  final Exit<Object?, Object?> exit;
}

final class _Frame {
  const _Frame({
    this.success,
    this.failure,
    this.consumesValue = false,
    this.interruptible,
    this.context,
    this.locals,
    this.scope,
    this.releaseBorrow,
    this.cleanup = false,
  });
  final _Node Function(Object?)? success;
  final bool consumesValue;
  final _Node Function(
    Cause<Object?>,
    List<FailureEvent>,
    List<CauseCancelled>,
    CancellationSignal?,
  )?
  failure;
  final bool? interruptible;
  final Context? context;
  final Map<TaskLocal<Object?>, Object?>? locals;
  final Scope? scope;
  final void Function()? releaseBorrow;
  final bool cleanup;
}
