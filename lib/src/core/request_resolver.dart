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

/// Resolves keyed requests in bounded batches within one root execution.
///
/// Requests share work only when their resolver, root request scope, services,
/// and task-local bindings match. Service and local values are compared by
/// identity, not by their custom equality. Independent runtime runs and work
/// forked into an external scope have separate request domains. Keys follow
/// Dart's equality and hash-code contract and must remain stable while in use.
///
/// The resolver itself is reusable. Queues, loaders, and cached results belong
/// to each request domain and are released when that domain closes. Values have
/// no resource lease; use shared resources or a pool for owned connections.
final class RequestResolver<K, V, E> {
  /// Creates a lazy resolver using [resolve] for each batch of distinct keys.
  ///
  /// The returned list must have one [Result] per input key, in the same order.
  /// A failed task, including its cleanup failure, fails every key in that
  /// batch. A result-count mismatch is a defect for the entire batch. The
  /// callback must not recursively wait for this same resolver.
  ///
  /// [maxBatchSize], [concurrency], and [cacheCapacity] must be positive.
  /// Concurrency limits active batches across all environments in one request
  /// domain, not across independent runtime runs. Pending callers are bounded
  /// by application concurrency, not by these limits.
  ///
  /// With [cache] enabled, successful values are retained in least recently
  /// used order up to [cacheCapacity] across the domain's environments. Business
  /// failures, defects, and cancellation are never cached. Disabling caching
  /// still shares unfinished requests for the same key and environment.
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

/// Executes requests with their statically known key, value, and error types.
extension RequestResolverOperations<K, V, E> on RequestResolver<K, V, E> {
  /// Gets [key], sharing a cached value or an unfinished compatible request.
  ///
  /// Newly queued requests are collected until the next batching microtask.
  /// Use [batch] when a known set of keys must be submitted together. Cancelling
  /// one caller leaves other callers running. If an entire sent batch loses
  /// its callers, the last caller waits for cancellation and loader cleanup.
  Task<V, E> request(K key) => _submit([
    key,
  ], false).flatMap((results) => Task.fromResult(results.single));

  /// Submits a finite set of [keys] together and preserves input order.
  ///
  /// Inputs are copied during execution before any requests are submitted.
  /// Duplicate keys share work but retain their positions in the result. Newly
  /// queued distinct keys form a submission group, split by the batch-size
  /// limit; later keys are not appended to that group. Already queued, loading,
  /// or cached keys can still be reused.
  ///
  /// Waits for all submitted keys. Individual business errors remain [Result]
  /// values. If any loader batch fails, this task fails with the combined batch
  /// causes in completion order. The returned list is immutable.
  Task<List<Result<V, E>>, E> batch(Iterable<K> keys) => _submit(keys, true);

  /// Removes [key] from the current environment's reusable entries.
  ///
  /// Existing callers keep their original result; its completion cannot replace
  /// the new generation. Other environments and root runs are unaffected.
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
