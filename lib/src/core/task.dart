import 'dart:async';

import 'cancellation.dart';
import 'cause.dart';
import 'context.dart';
import 'exit.dart';
import 'fiber.dart';
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
  static Task<T, E> acquireRelease<T, E>(
    Task<T, E> acquire, {
    required Task<void, Never> Function(T resource, Exit<Object?, Object?> exit)
    release,
  }) => uninterruptibleMask(
    (_) => acquire.flatMap(
      (resource) =>
          addFinalizer((exit) => release(resource, exit))
              .withError<E>()
              .map((_) => resource),
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
  /// The deadline uses the current context's clock and includes resource cleanup.
  /// On expiration, the computation is cancelled and fully cleaned up before
  /// [onTimeout] supplies the error. Cleanup defects remain alongside that error,
  /// or alongside a defect if [onTimeout] throws.
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
  /// Each attempt has its own resource scope and finishes cleanup before the next
  /// attempt or delay. Cleanup failures prevent retry. Schedule state is fresh per
  /// execution, and elapsed time includes attempts, cleanup, and waits. Delays use
  /// the current clock and are cancellable.
  Task<T, E> retry(Schedule schedule, {bool Function(E error)? when}) =>
      retryTask(this, schedule, when: when);

  /// Repeats successful executions and returns the last value.
  ///
  /// The first execution starts immediately. After each success, [until] can stop
  /// by returning true; otherwise [schedule] decides whether and when to repeat.
  /// A failed attempt stops repetition. An exception from [until] becomes a defect.
  ///
  /// Each attempt finishes its own resource scope before another begins, including
  /// cleanup of the last attempt before returning. Schedule state is fresh per
  /// execution, elapsed time includes attempts and cleanup, and delays use the
  /// current clock. Cancellation stops further attempts and awaits current cleanup.
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

  /// Starts a child fiber owned by the current [Scope].
  ///
  /// Each execution starts a new fiber. Parent cancellation reaches the child, and
  /// the scope stops and waits for unfinished children before releasing resources.
  ///
  /// Use [Fiber.join] to propagate the child's value or business failure. An
  /// unobserved failure is reported by the runtime; starting a child does not by
  /// itself make its business failure the parent's result.
  Task<Fiber<T, E>, Never> fork() => forkTask(this);

  /// Starts a fiber whose lifetime belongs to an explicit [scope].
  ///
  /// Use this when the owner should outlive the task that starts the fiber. The
  /// scope must be open; closing it cancels the fiber and waits for cleanup.
  /// Cancellation of the creating task does not cancel the fiber.
  Task<Fiber<T, E>, Never> forkIn(Scope scope) => Task._(
    _Fork(
      (_, parent, context, locals, _, runScope, _) => startFiber(
        this,
        owner: scope,
        inheritHost: false,
        runScope: scopeOwnsScope(runScope, scope) ? runScope : null,
        context: context,
        locals: locals,
        report: fiberReport(parent),
      ),
    ),
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
  /// Cleanup is registered before the task starts. The callback and its returned
  /// task run with cancellation masked in the registration [Zone]. Their exceptions
  /// become cleanup defects, retained alongside any body failure.
  Task<T, E> onExit(Task<void, Never> Function(Exit<T, E> exit) cleanup) =>
      Task.addFinalizer((exit) => cleanup(_typedExit<T, E>(exit)))
          .withError<E>()
          .flatMap((_) => this)
          .scoped();

  /// Transforms this task's successful value.
  ///
  /// A failure skips [transform]. Use [flatMap] when the transformation itself
  /// returns a task.
  Task<U, E> map<U>(U Function(T value) transform) => Task._(
    _Continue(
      _node,
      _Frame(success: (value) => _Success(transform(value as T))),
    ),
  );

  /// Continues with the task returned by [next] after success.
  ///
  /// Both tasks share the execution and resource scope. A failure skips [next].
  /// Use [mapError] to align different business error types before composing.
  Task<U, E> flatMap<U>(Task<U, E> Function(T value) next) => Task._(
    _Continue(_node, _Frame(success: (value) => next(value as T)._node)),
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
        failure: (cause, events, cleanups, _) => _preserveFailure(
          transform(_typedCause<E>(cause)),
          events,
          cleanups,
        ),
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
}

// Runtime's execution boundary. This declaration is not exported by any.dart.
Future<TaskEvaluation<T, E>> evaluateTask<T, E>(
  Task<T, E> task, {
  bool interruptible = true,
  CancellationSignal? signal,
  Scope? scope,
  Scope? runScope,
  Fiber<Object?, Object?>? fiber,
  Context? context,
  Map<TaskLocal<Object?>, Object?> locals = const {},
  List<Scope> closingScopes = const [],
}) async {
  final ownedScope = scope ?? createScope();
  final (evaluated, cancelled, observed) = await _evaluate(
    task,
    interruptible: interruptible,
    signal: signal,
    scope: ownedScope,
    runScope: runScope ?? ownedScope,
    closingScopes: closingScopes,
    fiber: fiber,
    context: context ?? Context.from(const []),
    locals: locals,
  );
  var result = evaluated;
  var cancellationObserved = cancelled;
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
  await closeScope(ownedScope, result.exit);
  result = _appendEvaluation(
    result,
    scopeCleanupFor(ownedScope, {
      ...observed,
      for (final event in result.events) event.source,
    }),
  );
  if (interruptible &&
      !cancellationObserved &&
      (signal?.isCancelled ?? false)) {
    result = TaskEvaluation(
      _appendCleanup(result.exit, Cause.cancelled(signal!.reason)),
      result.events,
      cleanupCancellations: result.cleanupCancellations,
    );
  }
  return result;
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
}) => Task._(
  _Fork(
    (scope, parent, context, locals, signal, runScope, interruptible) =>
        startFiber(
          task,
          owner: scope,
          signal: signal,
          interruptible: !inheritCancellation || interruptible,
          runScope: scopeOwnsScope(runScope, scope) ? runScope : null,
          context: context,
          locals: locals,
          report: fiberReport(parent),
        ),
  ),
);

Task<Fiber<T, E>, Never> forkTaskIn<T, E>(
  Task<T, E> task,
  Scope scope, {
  bool inheritHost = false,
}) => Task._(
  _Fork(
    (_, parent, context, locals, _, runScope, _) => startFiber(
      task,
      owner: scope,
      inheritHost: inheritHost,
      deferStart: true,
      runScope: scopeOwnsScope(runScope, scope) ? runScope : null,
      context: context,
      locals: locals,
      report: fiberReport(parent),
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
      ExitSuccess(:final value) => _Success(value),
      ExitFailure(:final cause) => _Failure(
        cause,
        result.events,
        result.cleanupCancellations,
      ),
    });

Future<(TaskEvaluation<T, E>, bool, Set<Object>)> _evaluate<T, E>(
  Task<T, E> task, {
  required Scope scope,
  required Scope runScope,
  required List<Scope> closingScopes,
  bool interruptible = true,
  CancellationSignal? signal,
  Fiber<Object?, Object?>? fiber,
  required Context context,
  required Map<TaskLocal<Object?>, Object?> locals,
}) async {
  var current = task._node;
  final frames = <_Frame>[];
  // Observation affects automatic cleanup merging within this execution only.
  // Finalizers have independent executions, even when they belong to this fiber.
  final observed = <Object>{};
  var evaluating = true;
  var currentScope = scope;
  var currentContext = context;
  var currentLocals = locals;
  Object? value;
  Cause<Object?>? failure;
  var events = const <FailureEvent>[];
  var cleanupCancellations = const <CauseCancelled>[];
  var remaining = 2048;

  while (true) {
    final effectiveSignal = interruptible ? signal : null;
    if (effectiveSignal?.isCancelled ?? false) {
      return (
        TaskEvaluation(
          Exit<T, E>.failure(_cancelled(failure, effectiveSignal!.reason)),
          events,
          cleanupCancellations: cleanupCancellations,
        ),
        true,
        observed,
      );
    }
    if (--remaining == 0) {
      remaining = 2048;
      await Future<void>.delayed(Duration.zero);
      continue;
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
            frames.add(_Frame(scope: currentScope));
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
              fiber,
              currentContext,
              currentLocals,
              signal,
              runScope,
              interruptible,
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
            final pending = _runCallback(
              (complete) => listenFiber(target, (exit) {
                delivered = fiberFailureEvents(target);
                deliveredCleanups = fiberCleanupCancellations(target);
                observed.addAll(delivered.map((event) => event.source));
                complete(join ? exit : Exit.success(exit));
              }),
              effectiveSignal,
            );
            final exit = pending is Future<Exit<Object?, Object?>>
                ? await pending
                : pending;
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
          case _Success(value: final result):
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
              events = _failureEvents(failure);
              cleanupCancellations = const [];
              evaluating = false;
            }
          case _Suspend(:final create):
            current = create();
          case _Mask(:final create):
            final previous = interruptible;
            frames.add(_Frame(interruptible: previous));
            interruptible = false;
            current = create(previous);
          case _Interruptibility(:final source, interruptible: final restored):
            frames.add(_Frame(interruptible: interruptible));
            interruptible = restored;
            current = source;
          case _FromFuture(:final create, :final onError):
            try {
              value = await create();
              failure = null;
              events = const [];
              cleanupCancellations = const [];
            } catch (error, stackTrace) {
              failure = effectiveSignal?.isCancelled ?? false
                  ? Cause.defect(error, stackTrace)
                  : Cause.failure(onError(error, stackTrace));
              events = _failureEvents(failure);
              cleanupCancellations = const [];
            }
            evaluating = false;
          case _FromCallback(:final register):
            final pending = _runCallback(register, effectiveSignal);
            final exit = pending is Future<Exit<Object?, Object?>>
                ? await pending
                : pending;
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
          case _Yield():
            await Future<void>.delayed(Duration.zero);
            value = null;
            failure = null;
            events = const [];
            cleanupCancellations = const [];
            evaluating = false;
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
                () =>
                    inScopeFinalizer(registrationScope, () => finalizer(exit)),
              ),
              runFinalizer: (task, scope) => zone.run(
                () => inScopeFinalizer(
                  scope,
                  () => evaluateTask(
                    task,
                    closingScopes: [...closingScopes, scope],
                    fiber: fiber,
                    context: services,
                    locals: localValues,
                  ),
                ),
              ),
            );
            final result = pending is Future<TaskEvaluation<void, Never>?>
                ? await pending
                : pending;
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
          case _Scoped(:final source):
            final parent = currentScope;
            // Borrowed application scopes can outlive this execution. A local
            // scope still belongs to the execution, so its owner cannot claim
            // its cleanup tree and then wait for this same fiber to close it.
            final child = createScope(
              parent: scopeContains(scope, parent) ? parent : scope,
            );
            currentScope = child;
            frames.add(
              _Frame(
                success: (value) {
                  currentScope = parent;
                  return _FinishScope(child, Exit.success(value), const []);
                },
                failure: (cause, events, cleanups, _) {
                  currentScope = parent;
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
            await closeScope(scope, exit);
            final completed = _appendEvaluation(
              TaskEvaluation(
                exit,
                bodyEvents,
                cleanupCancellations: bodyCleanups,
              ),
              scopeCleanupFor(scope, {
                ...observed,
                for (final event in bodyEvents) event.source,
              }),
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
            evaluating = false;
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
              throw StateError('Closing this scope would wait on the caller.');
            }
            await closeScope(scope, exit);
            final completed = scopeCleanupFor(scope);
            failure = switch (completed.exit) {
              ExitFailure(:final cause) => cause,
              _ => null,
            };
            events = completed.events;
            cleanupCancellations = completed.cleanupCancellations;
            observed.addAll(events.map((event) => event.source));
            value = null;
            evaluating = false;
        }
        continue;
      }

      if (frames.isEmpty) {
        final Exit<T, E> exit = failure == null
            ? Exit.success(value as T)
            : Exit.failure(_typedCause<E>(failure));
        return (
          TaskEvaluation(
            exit,
            events,
            cleanupCancellations: cleanupCancellations,
          ),
          false,
          observed,
        );
      }

      final frame = frames.removeLast();
      if (frame.scope case final restored?) {
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
      if (failure case final cause?) {
        final resume = frame.failure;
        if (resume == null) continue;
        current = resume(cause, events, cleanupCancellations, effectiveSignal);
      } else {
        final resume = frame.success;
        if (resume == null) continue;
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
      } else {
        failure = null;
        events = const [];
        cleanupCancellations = const [];
        evaluating = true;
      }
    } catch (error, stackTrace) {
      failure = Cause.defect(error, stackTrace);
      events = _failureEvents(failure);
      cleanupCancellations = const [];
      evaluating = false;
    }
  }
}

// Cause transformations can retain an occurrence from their current input.
// A new task failure has no prior events, even when it reuses the same Cause.
List<FailureEvent> _failureEvents(
  Cause<Object?> cause, [
  Iterable<FailureEvent> previous = const [],
]) {
  final available = Map<CauseDefect, List<FailureEvent>>.identity();
  for (final event in previous.toList().reversed) {
    (available[event.cause] ??= []).add(event);
  }
  final reasons = switch (cause) {
    CauseGroup(:final causes) => causes,
    _ => [cause],
  };
  return List.unmodifiable([
    for (final reason in reasons)
      if (reason is CauseDefect)
        if (available[reason] case final candidates? when candidates.isNotEmpty)
          candidates.removeLast()
        else
          FailureEvent(reason, Object()),
  ]);
}

List<CauseCancelled> _cancellations(Cause<Object?> cause) => switch (cause) {
  CauseCancelled() => [cause],
  CauseGroup(:final causes) => causes.whereType<CauseCancelled>().toList(),
  _ => const [],
};

_Failure _preserveFailure(
  Cause<Object?> cause,
  List<FailureEvent> events,
  List<CauseCancelled> cleanupCancellations,
) {
  if (cleanupCancellations.isEmpty) {
    return _Failure(cause, _failureEvents(cause, events));
  }
  final available = [...cleanupCancellations];
  return _Failure(cause, _failureEvents(cause, events), [
    for (final cancellation in _cancellations(cause))
      if (available.remove(cancellation)) cancellation,
  ]);
}

TaskEvaluation<T, E> _appendEvaluation<T, E>(
  TaskEvaluation<T, E> result,
  TaskEvaluation<void, Never> cleanup,
) {
  final cause = switch (cleanup.exit) {
    ExitFailure(:final cause) => cause,
    _ => null,
  };
  if (cause == null) return result;
  return TaskEvaluation(
    _appendCleanup(result.exit, cause),
    [...result.events, ...cleanup.events],
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
    cause.mapError((error) => error as E);

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
    Fiber<Object?, Object?>,
    Context,
    Map<TaskLocal<Object?>, Object?>,
    CancellationSignal?,
    Scope,
    bool,
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
  const _Success(this.value);
  final Object? value;
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
  const _Mask(this.create);
  final _Node Function(bool interruptible) create;
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
    this.interruptible,
    this.context,
    this.locals,
    this.scope,
  });
  final _Node Function(Object?)? success;
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
}
