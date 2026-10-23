import 'dart:async';
import 'dart:collection';

import 'cause.dart';
import 'context.dart';
import 'exit.dart';
import 'fiber.dart';
import 'result.dart';
import 'scope.dart';
import 'task.dart';
import 'task_local.dart';

/// A reusable loader that combines keyed requests into bounded batches.
///
/// Use a resolver to fetch many records in one database or service call. Calls
/// to [RequestResolverOperations.request] batch automatically and share duplicate
/// keys. Use [RequestResolverOperations.batch] when the complete key list is
/// already known and individual failures should remain values.
///
/// ```dart
/// import 'package:any/any.dart';
///
/// Future<List<String>> userNames() async {
///   final users = {1: 'Ada', 2: 'Linus'};
///   final names = RequestResolver<int, String, String>(
///     (ids) => Task.succeed([
///       for (final id in ids)
///         switch (users[id]) {
///           final name? => Result.success(name),
///           null => Result.failure('Unknown user $id'),
///         },
///     ]),
///     maxBatchSize: 50,
///   );
///   final runtime = Runtime();
///   try {
///     return await runtime.runOrThrow(
///       names
///           .batch([1, 2, 1, 99])
///           .map(
///             (results) => [
///               for (final result in results)
///                 switch (result) {
///                   ResultSuccess(:final value) => value,
///                   ResultFailure(:final error) => error,
///                 },
///             ],
///           ),
///     );
///   } finally {
///     await runtime.close();
///   }
/// }
/// ```
///
/// Sharing is local to a resolver and a root task execution. Different service
/// or task-local bindings have separate batches and caches; binding values must
/// be identical, regardless of their custom equality. Separate runtime runs and
/// tasks forked into an external scope also have separate request domains. Keys
/// use Dart equality and must have stable hash codes.
///
/// Each domain releases its pending work and cached values when it closes.
/// Loader cleanup finishes before results are delivered. Returned values do not
/// hold resource leases; use shared resources or a pool for open connections.
final class RequestResolver<K, V, E> {
  /// Creates a resolver that loads distinct keys with [resolve].
  ///
  /// [resolve] receives an immutable key list and must return one [Result] per
  /// key in the same order. A [ResultFailure] affects only its key. A failed task
  /// or failed cleanup affects the whole batch; a wrong result count is a defect.
  /// Recursively waiting on this resolver fails with a [StateError] defect.
  ///
  /// [maxBatchSize] limits keys per batch. [concurrency] limits active batches
  /// across all environments in one request domain, not across runtime runs.
  /// Neither setting bounds the number of waiting callers.
  ///
  /// With [cache] enabled, successful values are kept in least recently used
  /// order up to [cacheCapacity] across the domain's environments. Failures are
  /// never cached. Disabling caching still shares unfinished requests.
  ///
  /// Throws [RangeError] if [maxBatchSize], [concurrency], or [cacheCapacity] is
  /// not positive.
  RequestResolver(
    Task<List<Result<V, E>>, E> Function(List<K> keys) resolve, {
    int maxBatchSize = 100,
    int concurrency = 1,
    this._cache = true,
    int cacheCapacity = 1024,
  }) : _resolve = resolve,
       _maxBatchSize = maxBatchSize,
       _concurrency = concurrency,
       _cacheCapacity = cacheCapacity {
    if (maxBatchSize < 1) {
      throw RangeError.range(maxBatchSize, 1, null, 'maxBatchSize');
    }
    if (concurrency < 1) {
      throw RangeError.range(concurrency, 1, null, 'concurrency');
    }
    if (cacheCapacity < 1) {
      throw RangeError.range(cacheCapacity, 1, null, 'cacheCapacity');
    }
  }

  final Task<List<Result<V, E>>, E> Function(List<K>) _resolve;
  final int _maxBatchSize;
  final int _concurrency;
  final bool _cache;
  final int _cacheCapacity;
  final _runs = Expando<_Run<K, V, E>>();

  Task<_Run<K, V, E>, Never> _run() => currentTaskRunScope().flatMap(
    (scope) => Task.defer(() {
      final existing = _runs[scope];
      if (existing != null) return Task.succeed(existing);
      final run = _Run(this, scope);
      _runs[scope] = run;
      return inTaskScope(
        Task.addFinalizer(
          (_) => Task.sync(() {
            run.close();
            _runs[scope] = null;
          }),
        ),
        scope,
      ).map((_) => run);
    }),
  );

  Task<List<Result<V, E>>, E> _submit(Iterable<K> keys, bool explicit) =>
      Task.defer(() {
        final copied = List<K>.of(keys);
        if (copied.isEmpty) return Task.succeed(const []);
        final call = _Call<K, V, E>();
        return Task.uninterruptibleMask<List<Result<V, E>>, E>((restore) {
          return Task.addFinalizer(call.release)
              .flatMap((_) => _executing.get())
              .flatMap((executing) {
                if (executing.contains(this)) {
                  throw StateError('A request resolver cannot await itself.');
                }
                return _run();
              })
              .flatMap(
                (run) => captureTaskEnvironment().map((environment) {
                  call.run = run;
                  run.admit(call, copied, environment, explicit);
                }),
              )
              .withError<E>()
              .flatMap((_) => restore(call.awaitResults()))
              .flatMap(taskFromEvaluation);
        }).scoped();
      });
}

/// Requests values and invalidates results in the current request domain.
extension RequestResolverOperations<K, V, E> on RequestResolver<K, V, E> {
  /// Returns the value for [key], sharing compatible requests and cached results.
  ///
  /// New requests collect until the next batching microtask. Use [batch] to
  /// submit a known set of keys together.
  ///
  /// Cancelling one caller leaves other callers waiting. If a sent batch loses
  /// all its callers, its last caller waits for cancellation and loader cleanup.
  Task<V, E> request(K key) => _submit([
    key,
  ], false).flatMap((results) => Task.fromResult(results.single));

  /// Resolves [keys] together and returns their results in input order.
  ///
  /// [keys] must be finite and is copied before submission. Duplicate keys share
  /// work while keeping their positions. Newly queued keys form a batch group,
  /// split by the size limit; later requests cannot add keys to that group.
  /// Already pending or cached results may still be reused.
  ///
  /// Waits for every key and returns an immutable list. Individual business
  /// errors remain [ResultFailure] values. Failed loader batches instead fail
  /// this task, combining their causes in completion order. Cancellation follows
  /// [request].
  Task<List<Result<V, E>>, E> batch(Iterable<K> keys) => _submit(keys, true);

  /// Makes the next compatible request for [key] load a fresh result.
  ///
  /// Existing callers keep their original result, which cannot replace the new
  /// one in the cache. Other environments and root runs are unaffected. A missing
  /// key has no effect.
  Task<void, Never> invalidate(K key) => Task.uninterruptibleMask((_) {
    return _run().flatMap(
      (run) => captureTaskEnvironment().map((environment) {
        run.invalidate(key, environment);
      }),
    );
  });
}

final _executing = TaskLocal<List<Object>>(const [], name: 'requestResolvers');

final class _Run<K, V, E> {
  _Run(this.resolver, this.scope);

  final RequestResolver<K, V, E> resolver;
  final Scope scope;
  final partitions = <_Partition<K, V, E>>[];
  final pending = Queue<_Group<K, V, E>>();
  final active = <_Batch<K, V, E>>{};
  final cached = <_Entry<K, V, E>>{};
  var closed = false;
  var scheduled = false;
  var completionOrder = 0;

  _Partition<K, V, E>? findPartition(TaskEnvironment environment) {
    for (final partition in partitions) {
      if (contextBindingsIdentical(
            partition.environment.context,
            environment.context,
          ) &&
          _localsIdentical(partition.environment.locals, environment.locals)) {
        return partition;
      }
    }
    return null;
  }

  void admit(
    _Call<K, V, E> call,
    List<K> keys,
    TaskEnvironment environment,
    bool explicit,
  ) {
    if (closed || scopeIsClosing(scope)) {
      throw StateError('The request domain is closed.');
    }
    var partition = findPartition(environment);
    if (partition == null) {
      partition = _Partition(environment);
      partitions.add(partition);
    }
    if (explicit) partition.automatic = null;
    _Group<K, V, E>? group;
    for (final key in keys) {
      var entry = partition.entries[key];
      if (entry == null) {
        entry = _Entry(key, partition);
        partition.entries[key] = entry;
        group ??= explicit ? _Group(partition) : partition.automatic;
        if (group == null) {
          group = _Group(partition);
          partition.automatic = group;
        }
        if (!group.queued) {
          group.queued = true;
          pending.add(group);
        }
        group.entries.add(entry);
        entry.group = group;
      } else if (cached.remove(entry)) {
        cached.add(entry);
      }
      entry.callers.add(call);
      call.entries.add(entry);
    }
    schedule();
  }

  void invalidate(K key, TaskEnvironment environment) {
    if (closed) throw StateError('The request domain is closed.');
    final entry = findPartition(environment)?.entries[key];
    if (entry != null) retire(entry);
  }

  void retire(_Entry<K, V, E> entry) {
    entry.retired = true;
    final partition = entry.partition;
    if (identical(partition.entries[entry.key], entry)) {
      partition.entries.remove(entry.key);
    }
    cached.remove(entry);
    if (partition.entries.isEmpty) partitions.remove(partition);
  }

  void removeQueued(_Entry<K, V, E> entry) {
    final group = entry.group;
    if (group == null) return;
    entry.group = null;
    group.entries.remove(entry);
    if (group.entries.isEmpty) {
      pending.remove(group);
      if (identical(group.partition.automatic, group)) {
        group.partition.automatic = null;
      }
    }
  }

  void schedule() {
    if (scheduled || closed) return;
    scheduled = true;
    scheduleMicrotask(() {
      scheduled = false;
      pump();
    });
  }

  void pump() {
    if (closed || scopeIsClosing(scope)) return;
    while (active.length < resolver._concurrency && pending.isNotEmpty) {
      final group = pending.first;
      if (identical(group.partition.automatic, group)) {
        group.partition.automatic = null;
      }
      final entries = <_Entry<K, V, E>>[];
      while (group.entries.isNotEmpty &&
          entries.length < resolver._maxBatchSize) {
        final entry = group.entries.removeFirst();
        entry.group = null;
        if (entry.result == null && entry.callers.isNotEmpty) {
          entries.add(entry);
        }
      }
      if (group.entries.isEmpty) pending.removeFirst();
      if (entries.isEmpty) continue;
      final batch = _Batch(entries);
      active.add(batch);
      for (final entry in entries) {
        entry.batch = batch;
      }
      final environment = group.partition.environment;
      try {
        final keys = List<K>.unmodifiable(entries.map((entry) => entry.key));
        final task = _executing.get().withError<E>().flatMap(
          (executing) => _executing.locally(
            List<Object>.unmodifiable([...executing, resolver]),
            Task.defer(() => resolver._resolve(keys)).map((results) {
              if (results.length != keys.length) {
                throw StateError(
                  'The request resolver returned ${results.length} results '
                  'for ${keys.length} keys.',
                );
              }
              return List<Result<V, E>>.unmodifiable(results);
            }),
          ),
        );
        final fiber = startFiber(
          task,
          owner: scope,
          runScope: scope,
          context: environment.context,
          locals: environment.locals,
          report: environment.report,
          deferStart: true,
        );
        batch.fiber = fiber;
        listenFiber(fiber, (exit) {
          finish(batch, TaskEvaluation(exit, fiberFailureEvents(fiber)));
        });
      } catch (error, stackTrace) {
        finish(
          batch,
          TaskEvaluation.fresh(Exit.failure(Cause.defect(error, stackTrace))),
        );
      }
    }
  }

  void finish(
    _Batch<K, V, E> batch,
    TaskEvaluation<List<Result<V, E>>, E> result,
  ) {
    if (batch.result != null) return;
    batch.result = result;
    active.remove(batch);
    final order = completionOrder++;
    for (var i = 0; i < batch.entries.length; i++) {
      final entry = batch.entries[i];
      entry.order = order;
      if (result.exit is ExitSuccess<List<Result<V, E>>, E>) {
        // A cached value must not retain the completed batch and all its peers.
        entry.batch = null;
      }
      entry.result = switch (result.exit) {
        ExitSuccess(:final value) => TaskEvaluation(
          Exit.success(value[i]),
          const [],
        ),
        ExitFailure(:final cause) => TaskEvaluation(
          Exit.failure(cause),
          result.events,
        ),
      };
      final successful = switch (entry.result!.exit) {
        ExitSuccess(value: ResultSuccess()) => true,
        _ => false,
      };
      if (resolver._cache && !entry.retired && !closed && successful) {
        cached.add(entry);
        while (cached.length > resolver._cacheCapacity) {
          retire(cached.first);
        }
      } else {
        retire(entry);
      }
    }
    for (final entry in batch.entries) {
      for (final call in entry.callers.toList()) {
        call.wake?.call();
      }
    }
    for (final listener in batch.listeners.toList()) {
      listener();
    }
    batch.listeners.clear();
    schedule();
  }

  void close() {
    closed = true;
    pending.clear();
    cached.clear();
    partitions.clear();
  }
}

final class _Partition<K, V, E> {
  _Partition(this.environment);
  final TaskEnvironment environment;
  final entries = <K, _Entry<K, V, E>>{};
  _Group<K, V, E>? automatic;
}

final class _Group<K, V, E> {
  _Group(this.partition);
  final _Partition<K, V, E> partition;
  final entries = Queue<_Entry<K, V, E>>();
  var queued = false;
}

final class _Entry<K, V, E> {
  _Entry(this.key, this.partition);
  final K key;
  final _Partition<K, V, E> partition;
  final callers = <_Call<K, V, E>>{};
  _Batch<K, V, E>? batch;
  _Group<K, V, E>? group;
  TaskEvaluation<Result<V, E>, E>? result;
  var retired = false;
  var order = 0;
}

final class _Batch<K, V, E> {
  _Batch(this.entries);
  final List<_Entry<K, V, E>> entries;
  final stopReason = Object();
  final listeners = <void Function()>{};
  Fiber<List<Result<V, E>>, E>? fiber;
  TaskEvaluation<List<Result<V, E>>, E>? result;

  Task<void, Never> awaitEnd() => Task.fromCallback((complete) {
    void done() => complete(const Exit.success(null));
    if (result != null) {
      done();
    } else {
      listeners.add(done);
    }
    return () {
      listeners.remove(done);
    };
  });
}

final class _Call<K, V, E> {
  _Run<K, V, E>? run;
  final entries = <_Entry<K, V, E>>[];
  void Function()? wake;
  var released = false;
  var detached = false;
  final stopped = <_Batch<K, V, E>>{};

  Task<TaskEvaluation<List<Result<V, E>>, E>, Never>
  awaitResults() => Task.fromCallback((complete) {
    void check() {
      if (entries.any((entry) => entry.result == null)) return;
      wake = null;
      final failures = <Object, _Entry<K, V, E>>{};
      for (final entry in entries) {
        if (entry.result!.exit is ExitFailure) {
          failures[entry.batch ?? entry] = entry;
        }
      }
      if (failures.isEmpty) {
        complete(
          Exit.success(
            TaskEvaluation(
              Exit.success(
                List<Result<V, E>>.unmodifiable([
                  for (final entry in entries)
                    (entry.result!.exit as ExitSuccess<Result<V, E>, E>).value,
                ]),
              ),
              const [],
            ),
          ),
        );
      } else {
        final ordered = failures.values.toList()
          ..sort((a, b) => a.order.compareTo(b.order));
        final events = _uniqueEvents(
          ordered.expand((entry) => entry.result!.events),
        );
        complete(
          Exit.success(
            TaskEvaluation(
              Exit.failure(
                Cause.combine([
                  for (final entry in ordered)
                    (entry.result!.exit as ExitFailure<Result<V, E>, E>).cause,
                ]),
              ),
              events,
            ),
          ),
        );
      }
    }

    wake = check;
    check();
    return detach;
  });

  void detach() {
    if (detached) return;
    detached = true;
    wake = null;
    for (final entry in entries.toSet()) {
      entry.callers.remove(this);
      if (entry.result != null || entry.callers.isNotEmpty) continue;
      run!.retire(entry);
      run!.removeQueued(entry);
      final batch = entry.batch;
      if (batch != null &&
          batch.entries.every((entry) => entry.callers.isEmpty)) {
        stopped.add(batch);
      }
    }
    for (final batch in stopped) {
      batch.fiber?.requestCancel(reason: batch.stopReason);
    }
    run?.schedule();
  }

  Task<void, Never> release(Exit<Object?, Object?> exit) => Task.defer(() {
    if (released) return Task.succeed(null);
    released = true;
    detach();
    return Task.forEach(stopped, (batch) => batch.awaitEnd()).flatMap((_) {
      final completed = <Object, _Entry<K, V, E>>{};
      for (final entry in entries) {
        if (entry.result?.exit is ExitFailure) {
          completed[entry.batch ?? entry] = entry;
        }
      }
      final ordered = completed.values.toList()
        ..sort((a, b) => a.order.compareTo(b.order));
      final reasons = <Cause<Never>>[];
      final events = <FailureEvent>[];
      final seen = <Object>{};
      for (final entry in ordered) {
        final delivered = switch (exit) {
          ExitFailure(:final cause) => _reasons(
            cause,
          ).whereType<CauseCancelled>().toList(),
          _ => <CauseCancelled>[],
        };
        var stoppedCancellationRemoved = false;
        final evaluation = entry.result!;
        final sources = evaluation.events.iterator;
        final cause = (evaluation.exit as ExitFailure<Result<V, E>, E>).cause;
        for (final reason in _reasons(cause)) {
          switch (reason) {
            case CauseDefect():
              sources.moveNext();
              if (seen.add(sources.current.source)) {
                reasons.add(reason);
                events.add(sources.current);
              }
            case CauseCancelled():
              final index = delivered.indexWhere(
                (value) => identical(value, reason),
              );
              if (index >= 0) {
                delivered.removeAt(index);
              } else if (!stoppedCancellationRemoved &&
                  entry.batch != null &&
                  identical(reason.reason, entry.batch!.stopReason)) {
                stoppedCancellationRemoved = true;
              } else {
                reasons.add(reason);
              }
            default:
              // Cancelled callers do not turn a loader's business error into
              // a cleanup defect; cancellation and real defects remain distinct.
              break;
          }
        }
      }
      return taskFromEvaluation(
        TaskEvaluation<void, Never>(
          reasons.isEmpty
              ? const Exit.success(null)
              : Exit.failure(Cause.combine(reasons)),
          events,
        ),
      );
    });
  });
}

bool _localsIdentical(
  Map<TaskLocal<Object?>, Object?> left,
  Map<TaskLocal<Object?>, Object?> right,
) {
  if (identical(left, right)) return true;
  if (left.length != right.length) return false;
  for (final entry in left.entries) {
    if (!right.containsKey(entry.key) ||
        !identical(entry.value, right[entry.key])) {
      return false;
    }
  }
  return true;
}

List<FailureEvent> _uniqueEvents(Iterable<FailureEvent> events) {
  final seen = <Object>{};
  return [
    for (final event in events)
      if (seen.add(event.source)) event,
  ];
}

List<Cause<T>> _reasons<T>(Cause<T> cause) => switch (cause) {
  CauseGroup(:final causes) => causes,
  _ => [cause],
};
