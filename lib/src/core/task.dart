import 'dart:async';

import 'cancellation.dart';
import 'cause.dart';
import 'context.dart';
import 'exit.dart';
import 'fiber.dart';
import 'internal/task_concurrency.dart';
import 'internal/task_timing.dart';
import 'internal/task_timeout.dart';
import 'option.dart';
import 'result.dart';
import 'schedule.dart';
import 'scope.dart';
import 'task_local.dart';

/// A lazy computation producing a value of type [T] or a business error [E].
///
/// Constructing and combining tasks does not run their callbacks. Each execution
/// starts with independent state. Unexpected exceptions become defects, and
/// cancellation remains separate from business errors, including when [E] is
/// [Never]. Cancellation is observed only while enabled; [uninterruptibleMask]
/// can defer it across a critical section.
final class Task<T, E> {
  const Task._(this._node);

  final _Node _node;

  /// Waits for [duration] using the clock in the current context.
  ///
  /// The timer is registered only when this task runs. Nonpositive durations
  /// schedule a zero-delay timer. Cancellation removes the timer and stops the
  /// wait. Use [yieldNow] to yield directly to the host event loop instead.
  static Task<void, Never> sleep(Duration duration) => sleepTask(duration);

  /// Runs [tasks] with at most [concurrency] active executions.
  ///
  /// The default runs sequentially. Results are immutable and retain input
  /// order. Input is read lazily as capacity becomes available; use a finite
  /// iterable because all results are retained. An empty input succeeds with
  /// an empty list. Nonpositive concurrency becomes an [ArgumentError] defect.
  ///
  /// A failure stops new work, cancels active siblings, and waits for their
  /// cleanup. Already observed failures are combined in completion order;
  /// cancellation requested by this operation does not add a separate cause.
  /// Owner cancellation follows the usual Task rules and preserves defects.
  static Task<List<T>, E> all<T, E>(
    Iterable<Task<T, E>> tasks, {
    int concurrency = 1,
  }) => computeAll(tasks, concurrency: concurrency);

  /// Applies [action] lazily to [values], preserving input order in the results.
  ///
  /// At most [concurrency] actions run at once. Each action is created only
  /// when its execution starts. Iteration and action exceptions become defects.
  /// Failure, cancellation, finite-input, and cleanup rules are those of [all].
  static Task<List<T>, E> forEach<A, T, E>(
    Iterable<A> values,
    Task<T, E> Function(A value) action, {
    int concurrency = 1,
  }) => computeForEach(values, action, concurrency: concurrency);

  /// Runs every input and collects its complete exit in input order.
  ///
  /// Individual failures, including defects and cancellation, are values in
  /// the returned immutable list and do not stop other inputs. Input iteration
  /// errors still fail the operation. Owner cancellation stops remaining work
  /// and waits for cleanup, preserving defects that have not been delivered.
  /// Concurrency, lazy input, and empty-input rules are those of [all].
  static Task<List<Exit<T, E>>, Never> allSettled<T, E>(
    Iterable<Task<T, E>> tasks, {
    int concurrency = 1,
  }) => computeAllSettled(tasks, concurrency: concurrency);

  /// Uses the first completed execution from a finite iterable of [tasks].
  ///
  /// Participants run concurrently. Once an exit is observed, no new inputs
  /// start; active losers are cancelled and their cleanup is awaited. The
  /// winner's value or cause determines the result, but any loser defect is
  /// retained and can turn a winning success into failure. Loser business errors
  /// and this operation's cancellation requests do not replace the winner.
  /// Empty input becomes an [ArgumentError] defect when executed.
  static Task<T, E> race<T, E>(Iterable<Task<T, E>> tasks) =>
      computeRace(tasks);

  /// Uses the first successful execution, or combines failures if all fail.
  ///
  /// A success recovers other participants' business failures. Defects are
  /// always retained, including failures during loser cleanup. Active losers
  /// are cancelled and fully cleaned up before returning. All-failure causes
  /// follow completion order. Input must be finite; empty input becomes an
  /// [ArgumentError] defect. As with [race], a decision stops further input.
  static Task<T, E> firstSuccess<T, E>(Iterable<Task<T, E>> tasks) =>
      computeFirstSuccess(tasks);

  /// Runs two tasks and returns their values as a record in argument order.
  ///
  /// The default executes [left] before [right]. A [concurrency] of two or more
  /// runs both concurrently; other validation and failure rules follow [all].
  static Task<(A, B), E> zip<A, B, E>(
    Task<A, E> left,
    Task<B, E> right, {
    int concurrency = 1,
  }) => computeZip(left, right, concurrency: concurrency);

  /// Reads [key] from the services available when this task executes.
  ///
  /// Runtime installs the application's services. A local [provideContext]
  /// can replace bindings for a computation and its children. A missing key
  /// produces a [MissingService] defect; nullable service values are preserved.
  static Task<T, Never> service<T>(ServiceKey<T> key) =>
      Task._(_Service((context) => context.read(key)));

  /// Describes a computation that succeeds with [value].
  static Task<T, Never> succeed<T>(T value) => Task._(_Success(value));

  /// Runs [task] only when [condition] returns true during this execution.
  ///
  /// A skipped computation succeeds with [OptionNone]; a computed value,
  /// including `null`, is wrapped in [OptionSome]. The condition is checked once
  /// per execution, and exceptions from it become defects.
  static Task<Option<T>, E> when<T, E>(
    bool Function() condition,
    Task<T, E> task,
  ) => Task.defer(
    () => condition()
        ? task.map((value) => Option.some(value))
        : Task.succeed(const Option.none()),
  );

  /// Repeatedly applies [step] while [whileTrue] accepts the current state.
  ///
  /// Each execution begins with [initial]. The condition is checked before
  /// every step; the first state it rejects is returned. Failures stop the loop,
  /// and exceptions in either callback become defects. Synchronous loops are
  /// stack safe and use the executor's cooperative cancellation checks.
  ///
  /// Steps share the current resource scope. Apply [TaskOperations.scoped] to a
  /// step when its resources must be released before the next iteration.
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

  /// Reduces [values] sequentially with a task-producing [combine] callback.
  ///
  /// Each execution creates a new iterator and starts with [initial]. The next
  /// element is read only after the preceding task succeeds. Empty input
  /// returns [initial]; iteration or callback exceptions become defects.
  /// Combiner tasks share the current resource scope, like [flatMap].
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

  /// Describes a computation that fails with the business [error].
  static Task<Never, E> fail<E>(E error) =>
      Task._(_Failure(Cause.failure(error)));

  /// Describes a computation that fails with the complete [cause].
  static Task<Never, E> failCause<E>(Cause<E> cause) => Task._(_Failure(cause));

  /// Calls [body] on each execution, treating thrown objects as defects.
  static Task<T, Never> sync<T>(T Function() body) => Task._(_Sync(body));

  /// Calls [body] on each execution and maps its exceptions with [onError].
  ///
  /// If [onError] itself throws, its exception and stack trace become a defect.
  static Task<T, E> trySync<T, E>(
    T Function() body, {
    required E Function(Object error, StackTrace stackTrace) onError,
  }) => Task._(_Sync(body, onError));

  /// Calls [create] on each execution and continues with the returned task.
  ///
  /// Use this to defer a choice or describe recursion without building the
  /// entire computation up front. Exceptions from [create] become defects.
  static Task<T, E> defer<T, E>(Task<T, E> Function() create) =>
      Task._(_Suspend(() => create()._node));

  /// Defers cancellation while executing the task returned by [body].
  ///
  /// [body] runs lazily on each execution with cancellation disabled. Its
  /// `restore` function wraps a task with the cancellation state that was active
  /// before this mask. Nested masks therefore cannot enable cancellation that
  /// was already disabled. Leaving either region restores its enclosing state,
  /// including when a callback throws.
  ///
  /// Pending cancellation is observed when cancellation becomes enabled again.
  /// Defects already produced by the body are retained alongside cancellation.
  /// A task that never completes while masked can indefinitely delay
  /// cancellation; use `restore` around work that should remain cancellable.
  /// Cancellation observed before this mask starts prevents [body] from running.
  ///
  /// `restore` only captures a cancellation state, not an execution or signal.
  /// Tasks it returns are lazy and may be reused, including in another execution,
  /// where they use that execution's signal with the captured state.
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

  /// Describes the already available business [result].
  static Task<T, E> fromResult<T, E>(Result<T, E> result) =>
      Task._(switch (result) {
        ResultSuccess(:final value) => _Success(value),
        ResultFailure(:final error) => _Failure(Cause.failure(error)),
      });

  /// Starts a new Future on each execution and maps exceptions with [onError].
  ///
  /// Both synchronous exceptions from [create] and errors completing its Future
  /// are mapped. An exception thrown by [onError] becomes a defect.
  ///
  /// A Future cannot be cancelled. If enabled cancellation arrives after it
  /// starts, execution waits for it to settle before returning. No subsequent
  /// business callback runs. A Future error observed after cancellation is
  /// retained as a defect with its original stack trace alongside cancellation;
  /// [onError] is skipped. This also applies to a synchronous exception from
  /// [create] if it requests cancellation before throwing. While cancellation is
  /// masked, [onError] and subsequent callbacks run normally.
  static Task<T, E> fromFuture<T, E>(
    Future<T> Function() create, {
    required E Function(Object error, StackTrace stackTrace) onError,
  }) => Task._(_FromFuture(create, onError));

  /// Registers a callback operation each time this task executes.
  ///
  /// [register] receives a completion callback and synchronously returns a
  /// cancellation function. The first completion wins; duplicate and late
  /// completions are ignored. A registration exception becomes a defect, even
  /// if completion was already called synchronously. If that completion failed,
  /// its cause is retained before the registration defect.
  ///
  /// Enabled cancellation waits for the returned function to stop using resources
  /// and finish cleanup. Cancellation during registration waits for [register] to
  /// return before calling that function. An operation that completed before
  /// cancellation does not receive a cancellation call. Cleanup exceptions keep
  /// their original object and stack trace alongside cancellation. Cleanup runs
  /// in the Zone where registration executed, including after asynchronous gaps.
  /// A masked operation waits for completion without calling its cancellation
  /// function.
  static Task<T, E> fromCallback<T, E>(
    FutureOr<void> Function() Function(void Function(Exit<T, E> exit) complete)
    register,
  ) => Task._(_FromCallback((complete) => register(complete)));

  /// Checks for owner cancellation when this task executes.
  ///
  /// Succeeds with no value if cancellation has not been requested or is masked.
  /// An enabled request terminates execution before the next business callback.
  static Task<void, Never> checkCancelled() => const Task._(_Success(null));

  /// Yields to the event loop before continuing with the next task.
  ///
  /// Timers and other ready work can advance. Enabled cancellation received
  /// while yielding terminates execution before the next business callback.
  static Task<void, Never> yieldNow() => const Task._(_Yield());

  /// Registers cleanup in the current scope when this task executes.
  ///
  /// Finalizers receive the scope's body exit and run in reverse registration
  /// order with cancellation masked. Every finalizer is attempted. Cleanup
  /// failures are appended to the body's cause before the scope returns.
  /// Cleanup has its own temporary scope, so it can register further cleanup
  /// and use scoped resources before the enclosing scope continues closing.
  static Task<void, Never> addFinalizer(
    Task<void, Never> Function(Exit<Object?, Object?> exit) finalizer,
  ) => Task._(_AddFinalizer(finalizer));

  /// Acquires a resource and registers its release in the current scope.
  ///
  /// Acquisition and registration are masked from cancellation. A pending
  /// request is handled only after a successful resource has its release
  /// registered. An acquisition that never finishes therefore cannot be
  /// cancelled promptly. A failed acquisition does not call [release].
  ///
  /// [release] runs with cancellation masked and observes the scope's exit.
  /// Use [using] for a local lifetime, or complete all uses within this scope.
  /// Returning a resource from Runtime.run does not extend its lifetime.
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

  /// Acquires, uses, and releases a resource within one local scope.
  ///
  /// Returns [use]'s result after [release] finishes, including on failure or
  /// cancellation. Release observes the use exit; release failures follow body
  /// failures in the final cause. Thrown callback exceptions become defects.
  static Task<T, E> using<R, T, E>(
    Task<R, E> acquire, {
    required Task<T, E> Function(R resource) use,
    required Task<void, Never> Function(R resource, Exit<T, E> exit) release,
  }) => acquireRelease(
    acquire,
    release: (resource, exit) => release(resource, _typedExit<T, E>(exit)),
  ).flatMap(use).scoped();
}

/// Wraps a task with the cancellation state captured by an enclosing mask.
///
/// The wrapper is lazy. Repeated executions use their own cancellation signals
/// and the same captured state; no execution or signal is retained.
typedef RestoreCancellation = Task<T, E> Function<T, E>(Task<T, E> task);

/// Transforms and combines tasks using their static value and error types.
extension TaskOperations<T, E> on Task<T, E> {
  /// Limits this computation's time and maps expiration into its error type.
  ///
  /// Timing uses the current clock and includes resource cleanup. If the timer
  /// wins, this cancels the computation and waits for it to finish before
  /// calling [onTimeout]. Cleanup defects remain alongside the timeout error;
  /// an exception in [onTimeout] becomes a defect without hiding those failures.
  ///
  /// This is a cooperative deadline. Non-cancellable Futures must settle before
  /// this task returns. Nonpositive durations schedule a zero-delay timer;
  /// synchronous work may finish before that timer is observed. Completion or
  /// owner cancellation removes the timer and waits for all owned work.
  Task<T, E> timeout(Duration duration, {required E Function() onTimeout}) =>
      timeoutTask(this, duration, onTimeout: onTimeout);

  /// Runs [fallback] if this computation exceeds [duration] without defects.
  ///
  /// The timed computation is cancelled and fully cleaned up before [fallback]
  /// is called. Any defect, including cleanup failure, prevents the fallback
  /// from starting. An ordinary business failure is propagated directly.
  /// The fallback has no deadline of its own unless explicitly wrapped.
  /// Timing and cancellation boundaries are those of [timeout].
  Task<T, E> timeoutOrElse(Duration duration, Task<T, E> Function() fallback) =>
      timeoutOrElseTask(this, duration, fallback);

  /// Waits for [duration] before starting this computation.
  ///
  /// Uses [Task.sleep], including its clock and cancellation rules. Cancelling
  /// the delay prevents the computation from starting.
  Task<T, E> delay(Duration duration) =>
      Task.sleep(duration).withError<E>().flatMap((_) => this);

  /// Retries single business failures according to [schedule].
  ///
  /// The first attempt starts immediately. `Schedule.recurs(3)` allows three
  /// further attempts. [when], if supplied, must return true to retry; a thrown
  /// exception becomes a defect. Defects, cancellation, and grouped failures
  /// are never retried. Exhaustion returns the last business failure.
  ///
  /// Each attempt has its own scope and completes cleanup before another
  /// attempt or delay begins. Cleanup failures prevent retry. Counting and
  /// elapsed time start fresh on each execution; elapsed time includes attempts,
  /// cleanup, and waits. Waiting uses the current clock and is cancellable.
  Task<T, E> retry(Schedule schedule, {bool Function(E error)? when}) =>
      retryTask(this, schedule, when: when);

  /// Repeats successful attempts and returns the last successful value.
  ///
  /// The first attempt starts immediately. After each success, [until] can stop
  /// repetition by returning true; otherwise [schedule] decides whether and
  /// when to continue. A failed attempt stops immediately, and an exception in
  /// [until] becomes a defect. Each attempt finishes its scoped resources before
  /// the next begins, including the last attempt before returning its value.
  ///
  /// Policy state is independent per execution. Time includes attempts and
  /// cleanup, and waits use the current clock. Cancellation stops future
  /// attempts and waits for current work and cleanup under the usual Task rules.
  Task<T, E> repeat(Schedule schedule, {bool Function(T value)? until}) =>
      repeatTask(this, schedule, until: until);

  /// Runs this computation with [context] overlaid on its current services.
  ///
  /// Provided bindings replace matching keys and leave all other services
  /// available. The enclosing context is restored on success or failure.
  /// Child fibers inherit the local bindings, and finalizers retain the
  /// services present when they were registered. This does not acquire or
  /// release the supplied service objects.
  Task<T, E> provideContext(Context context) =>
      Task._(_ProvideContext(_node, context));

  /// Starts this task as a child owned by the current task scope.
  ///
  /// Parent cancellation reaches the child. The scope cancels and waits for
  /// unfinished children before releasing resources. A child's business error
  /// is propagated by joining it; an unobserved failure is reported by Runtime.
  /// Each execution starts a fresh fiber.
  Task<Fiber<T, E>, Never> fork() => Task._(
    _Fork(
      (scope, parent, context, locals, signal) => startFiber(
        this,
        owner: scope,
        signal: signal,
        context: context,
        locals: locals,
        report: fiberReport(parent),
      ),
    ),
  );

  /// Starts this task with its lifetime owned by [scope].
  ///
  /// The scope must still be open. Its closure cancels and waits for this fiber.
  /// Cancellation of the task creating the fiber does not cancel it; this is
  /// useful for work whose explicit owner outlives the creating task.
  Task<Fiber<T, E>, Never> forkIn(Scope scope) => Task._(
    _Fork(
      (_, parent, context, locals, _) => startFiber(
        this,
        owner: scope,
        inheritHost: false,
        context: context,
        locals: locals,
        report: fiberReport(parent),
      ),
    ),
  );

  /// Runs this task in a child scope, waiting for its cleanup before continuing.
  ///
  /// Every execution creates a fresh scope. Child scopes close before parent
  /// resources, and cleanup failures follow the body's cause without wrapping
  /// or repeating that cause.
  Task<T, E> scoped() => Task._(_Scoped(_node));

  /// Runs [cleanup] after this task, including failure and cancellation.
  ///
  /// Cleanup is registered before starting this task and runs with cancellation
  /// masked. Its failures are retained alongside the body's failure.
  Task<T, E> ensuring(Task<void, Never> cleanup) => onExit((_) => cleanup);

  /// Runs [cleanup] with this task's exit before continuing to the caller.
  ///
  /// The callback is registered in a local scope before starting this task.
  /// Both the callback and returned cleanup task run in the registration Zone
  /// with cancellation masked. Their exceptions become cleanup defects.
  Task<T, E> onExit(Task<void, Never> Function(Exit<T, E> exit) cleanup) =>
      Task.addFinalizer((exit) => cleanup(_typedExit<T, E>(exit)))
          .withError<E>()
          .flatMap((_) => this)
          .scoped();

  /// Transforms the successful value when this task runs.
  ///
  /// Failures skip [transform]. Exceptions from [transform] become defects.
  Task<U, E> map<U>(U Function(T value) transform) => Task._(
    _Continue(
      _node,
      _Frame(success: (value) => _Success(transform(value as T))),
    ),
  );

  /// Continues with [next] after success, using the same business error type.
  ///
  /// Both tasks run in the same execution. A failure skips [next]; use
  /// [mapError] to adapt a different business error type before composing.
  Task<U, E> flatMap<U>(Task<U, E> Function(T value) next) => Task._(
    _Continue(_node, _Frame(success: (value) => next(value as T)._node)),
  );

  /// Runs [action] after success and retains the original successful value.
  ///
  /// A failure from [action] becomes the result of the combined task.
  Task<T, E> tap(Task<void, E> Function(T value) action) =>
      flatMap((value) => action(value).map((_) => value));

  /// Transforms every business failure, retaining other reasons and their order.
  ///
  /// Exceptions from [transform] become defects. External cancellation stops
  /// transformations that have not started, including later group members.
  Task<T, F> mapError<F>(F Function(E error) transform) => Task._(
    _Continue(
      _node,
      _Frame(
        failure: (cause, events, signal) => _preserveFailure(
          _mapErrors(cause, (error) => transform(error as E), signal),
          events,
        ),
      ),
    ),
  );

  /// Recovers a single business failure using the same business error type.
  ///
  /// Defects, cancellation, and groups bypass [recover]. Use [recoverCause] to
  /// explicitly handle a complete cause. External cancellation stops recovery.
  Task<T, E> recover(Task<T, E> Function(E error) recover) => Task._(
    _Continue(
      _node,
      _Frame(
        failure: (cause, events, _) => switch (cause) {
          CauseFailure(:final error) => recover(error as E)._node,
          _ => _Failure(cause, events),
        },
      ),
    ),
  );

  /// Recovers a complete failure with [recover].
  ///
  /// [recover] receives all business failures, defects, and explicit
  /// cancellation reasons. It cannot override cancellation requested by the
  /// running task's owner. Exceptions from [recover] become defects.
  Task<T, F> recoverCause<F>(Task<T, F> Function(Cause<E> cause) recover) =>
      Task._(
        _Continue(
          _node,
          _Frame(
            failure: (cause, _, _) => recover(_typedCause<E>(cause))._node,
          ),
        ),
      );

  /// Transforms the complete failure cause when this task fails.
  ///
  /// External cancellation skips [transform]. Exceptions from [transform]
  /// become defects.
  Task<T, F> mapCause<F>(Cause<F> Function(Cause<E> cause) transform) => Task._(
    _Continue(
      _node,
      _Frame(
        failure: (cause, events, _) =>
            _preserveFailure(transform(_typedCause<E>(cause)), events),
      ),
    ),
  );

  /// Captures this computation's complete [Exit] as a successful value.
  ///
  /// Explicit failure and cancellation causes can be inspected this way.
  /// Cancellation requested by the owner still terminates the enclosing task.
  Task<Exit<T, E>, Never> exit() => Task._(
    _Continue(
      _node,
      _Frame(
        success: (value) => _Success(Exit<T, E>.success(value as T)),
        failure: (cause, _, _) =>
            _Success(Exit<T, E>.failure(_typedCause<E>(cause))),
      ),
    ),
  );

  /// Converts every business failure into a defect at this conversion boundary.
  ///
  /// Non-null errors are retained unchanged. For a null business error, the
  /// original [CauseFailure] is retained as the defect's non-null error object.
  /// Existing defects, their stack traces, and cancellation remain unchanged.
  Task<T, Never> orDie() => Task._(
    _Continue(
      _node,
      _Frame(
        failure: (cause, events, _) =>
            _preserveFailure(_defects(cause, StackTrace.current), events),
      ),
    ),
  );
}

/// Establishes a business error type for a task that has no business failures.
extension InfallibleTaskOperations<T> on Task<T, Never> {
  /// Gives this task error type [F] so it can begin a flow using that type.
  ///
  /// This does not run the task or change its behavior.
  Task<T, F> withError<F>() => this;
}

// Internal execution results retain provenance only for the current failure.
// Capturing or recovering a failure returns success without these events.
final class TaskEvaluation<T, E> {
  const TaskEvaluation(this.exit, this.events);

  factory TaskEvaluation.fresh(Exit<T, E> exit) =>
      TaskEvaluation(exit, switch (exit) {
        ExitFailure(:final cause) => _failureEvents(cause),
        _ => const [],
      });

  final Exit<T, E> exit;
  final List<FailureEvent> events;
}

// Runtime's execution boundary. This declaration is not exported by any.dart.
Future<TaskEvaluation<T, E>> evaluateTask<T, E>(
  Task<T, E> task, {
  CancellationSignal? signal,
  Scope? scope,
  Fiber<Object?, Object?>? fiber,
  Context? context,
  Map<TaskLocal<Object?>, Object?> locals = const {},
  List<Scope> closingScopes = const [],
}) async {
  final ownedScope = scope ?? createScope();
  final (evaluated, cancelled, observed) = await _evaluate(
    task,
    signal: signal,
    scope: ownedScope,
    closingScopes: closingScopes,
    fiber: fiber,
    context: context ?? Context.from(const []),
    locals: locals,
  );
  var result = evaluated;
  var cancellationObserved = cancelled;
  if (!cancellationObserved && (signal?.isCancelled ?? false)) {
    result = TaskEvaluation(
      _appendCleanup(result.exit, Cause.cancelled(signal!.reason)),
      result.events,
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
  if (!cancellationObserved && (signal?.isCancelled ?? false)) {
    result = TaskEvaluation(
      _appendCleanup(result.exit, Cause.cancelled(signal!.reason)),
      result.events,
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

Task<Fiber<T, E>, Never> forkTaskIn<T, E>(
  Task<T, E> task,
  Scope scope, {
  bool inheritHost = false,
}) =>
    Task._(
      _Fork(
        (_, parent, context, locals, _) => startFiber(
          task,
          owner: scope,
          inheritHost: inheritHost,
          deferStart: true,
          context: context,
          locals: locals,
          report: fiberReport(parent),
        ),
      ),
    );

Task<T, E> accessTaskEnvironment<T, E>(
  Task<T, E> Function(Scope scope, Context context) use,
) => Task._(_Environment((scope, context) => use(scope, context)._node));

Task<T, E> taskFromEvaluation<T, E>(TaskEvaluation<T, E> result) =>
    Task._(switch (result.exit) {
      ExitSuccess(:final value) => _Success(value),
      ExitFailure(:final cause) => _Failure(cause, result.events),
    });

Future<(TaskEvaluation<T, E>, bool, Set<Object>)> _evaluate<T, E>(
  Task<T, E> task, {
  required Scope scope,
  required List<Scope> closingScopes,
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
  var interruptible = true;
  var currentScope = scope;
  var currentContext = context;
  var currentLocals = locals;
  Object? value;
  Cause<Object?>? failure;
  var events = const <FailureEvent>[];
  var remaining = 2048;

  while (true) {
    final effectiveSignal = interruptible ? signal : null;
    if (effectiveSignal?.isCancelled ?? false) {
      return (
        TaskEvaluation(
          Exit<T, E>.failure(_cancelled(failure, effectiveSignal!.reason)),
          events,
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
          case _Environment(:final use):
            current = use(currentScope, currentContext);
          case _InScope(:final source, scope: final borrowed):
            frames.add(_Frame(scope: currentScope));
            currentScope = borrowed;
            current = source;
          case _ReadLocal(:final read):
            value = read(currentLocals);
            failure = null;
            events = const [];
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
            );
            failure = null;
            events = const [];
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
                    scopeOwnsFiber(fiberScope(target), fiber) ||
                    currentFibers().contains(target))) {
              throw StateError(
                'A fiber cannot wait for itself or an active ancestor.',
              );
            }
            if (cancel) target.requestCancel(reason: reason);
            var delivered = const <FailureEvent>[];
            final pending = _runCallback(
              (complete) => listenFiber(target, (exit) {
                delivered = fiberFailureEvents(target);
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
              case ExitFailure(:final cause):
                failure = cause;
                events = _failureEvents(cause, delivered);
            }
            evaluating = false;
          case _Success(value: final result):
            value = result;
            failure = null;
            events = const [];
            evaluating = false;
          case _Failure(:final cause, events: final sources):
            failure = cause;
            events = sources ?? _failureEvents(cause);
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
            } catch (error, stackTrace) {
              failure = effectiveSignal?.isCancelled ?? false
                  ? Cause.defect(error, stackTrace)
                  : Cause.failure(onError(error, stackTrace));
              events = _failureEvents(failure);
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
              case ExitFailure(:final cause):
                failure = cause;
                events = _failureEvents(cause);
            }
            evaluating = false;
          case _Yield():
            await Future<void>.delayed(Duration.zero);
            value = null;
            failure = null;
            events = const [];
            evaluating = false;
          case _Continue(:final source, :final frame):
            frames.add(frame);
            current = source;
          case _OpenScope():
            value = createScope(parent: currentScope);
            failure = null;
            events = const [];
            evaluating = false;
          case _AddFinalizer(:final finalizer):
            final zone = Zone.current;
            final services = currentContext;
            final localValues = currentLocals;
            final pending = addScopeFinalizer(
              currentScope,
              (exit) => zone.run(() => finalizer(exit)),
              runFinalizer: (task, scope) => zone.run(
                () => evaluateTask(
                  task,
                  closingScopes: [...closingScopes, scope],
                  fiber: fiber,
                  context: services,
                  locals: localValues,
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
                failure: (cause, events, _) {
                  currentScope = parent;
                  return _FinishScope(child, Exit.failure(cause), events);
                },
              ),
            );
            current = source;
          case _FinishScope(
            :final scope,
            :final exit,
            events: final bodyEvents,
          ):
            await closeScope(scope, exit);
            final completed = _appendEvaluation(
              TaskEvaluation(exit, bodyEvents),
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
            evaluating = false;
          case _CloseScope(:final scope, :final exit):
            if (scopeCloseWouldDeadlock(scope, currentScope) ||
                (fiber != null &&
                    [fiber, ...currentFibers()].any(
                      (member) =>
                          fiberIsActive(member) &&
                          scopeOwnsFiber(scope, member),
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
        return (TaskEvaluation(exit, events), false, observed);
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
        current = resume(cause, events, effectiveSignal);
      } else {
        final resume = frame.success;
        if (resume == null) continue;
        current = resume(value);
      }
      // A continuation has already produced this failure. Record it before the
      // next cancellation check so defects cannot disappear between frames.
      if (current case _Failure(:final cause, events: final sources)) {
        failure = cause;
        events = sources ?? _failureEvents(cause);
        evaluating = false;
      } else if (current case _FinishScope(
        exit: ExitFailure(:final cause),
        events: final sources,
      )) {
        // Closing still has to execute, but cancellation before it starts must
        // retain the body failure that is already being handed to the scope.
        failure = cause;
        events = sources;
        evaluating = true;
      } else {
        failure = null;
        events = const [];
        evaluating = true;
      }
    } catch (error, stackTrace) {
      failure = Cause.defect(error, stackTrace);
      events = _failureEvents(failure);
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

_Failure _preserveFailure(Cause<Object?> cause, List<FailureEvent> events) =>
    _Failure(cause, _failureEvents(cause, events));

TaskEvaluation<T, E> _appendEvaluation<T, E>(
  TaskEvaluation<T, E> result,
  TaskEvaluation<void, Never> cleanup,
) {
  final cause = switch (cleanup.exit) {
    ExitFailure(:final cause) => cause,
    _ => null,
  };
  if (cause == null) return result;
  return TaskEvaluation(_appendCleanup(result.exit, cause), [
    ...result.events,
    ...cleanup.events,
  ]);
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
  )
  start;
}

final class _Service extends _Node {
  const _Service(this.read);
  final Object? Function(Context) read;
}

final class _Environment extends _Node {
  const _Environment(this.use);
  final _Node Function(Scope, Context) use;
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
  const _Failure(this.cause, [this.events]);
  final Cause<Object?> cause;
  final List<FailureEvent>? events;
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
  const _FinishScope(this.scope, this.exit, this.events);
  final Scope scope;
  final Exit<Object?, Object?> exit;
  final List<FailureEvent> events;
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
  final _Node Function(Cause<Object?>, List<FailureEvent>, CancellationSignal?)?
  failure;
  final bool? interruptible;
  final Context? context;
  final Map<TaskLocal<Object?>, Object?>? locals;
  final Scope? scope;
}
