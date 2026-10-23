import 'dart:collection';

import 'exit.dart';
import 'task.dart';

/// A fixed set of permits shared by tasks in the same isolate.
///
/// [SemaphoreOperations.withPermits] waits for all requested permits, runs a
/// task, and returns them after its scoped resources finish closing. Requests
/// are served in arrival order; smaller requests cannot bypass the queue head.
final class Semaphore {
  Semaphore._(this._capacity) : _available = _capacity;

  final int _capacity;
  int _available;
  final _waiting = Queue<_PermitRequest>();
  var _draining = false;

  /// Creates a semaphore with [permits] available on each execution.
  ///
  /// [permits] must be positive. An invalid value becomes a [RangeError] defect
  /// when the returned task executes. Repeated executions create independent
  /// semaphores.
  static Task<Semaphore, Never> create(int permits) => Task.sync(() {
    if (permits <= 0) throw RangeError.range(permits, 1, null, 'permits');
    return Semaphore._(permits);
  });

  void _release(_PermitRequest request) {
    if (request.finished) return;
    request.finished = true;
    if (request.granted) {
      _available += request.count;
    } else {
      _waiting.remove(request);
    }
    _drain();
  }

  void _drain() {
    if (_draining) return;
    _draining = true;
    try {
      while (_waiting.isNotEmpty && _waiting.first.count <= _available) {
        final request = _waiting.removeFirst();
        _available -= request.count;
        request.granted = true;
        request.complete(const Exit.success(null));
      }
    } finally {
      _draining = false;
    }
  }
}

/// Runs tasks while holding permits from a [Semaphore].
extension SemaphoreOperations on Semaphore {
  /// Waits for [count] permits and holds them until [task] and its cleanup finish.
  ///
  /// Waiting and task execution respect the caller's cancellation state. A
  /// cancelled waiter leaves the queue without affecting other requests. Once
  /// granted, permits are returned on success, failure, or cancellation,
  /// including cancellation during the handoff to [task].
  ///
  /// [count] must be positive and no greater than this semaphore's capacity.
  /// An invalid value becomes a [RangeError] defect when executed, before
  /// [task] starts. Each execution makes a separate request. Requests do not
  /// inherit permits already held by the caller: a nested request waits in
  /// the same queue and can deadlock if the caller holds permits it needs.
  Task<T, E> withPermits<T, E>(int count, Task<T, E> task) =>
      Task.uninterruptibleMask<T, E>((restore) {
        RangeError.checkValueInInterval(count, 1, _capacity, 'count');
        final request = _PermitRequest(count);

        // Register before waiting so cancellation immediately after a grant
        // still returns the permits, even before the next continuation runs.
        return Task.addFinalizer((_) => Task.sync(() => _release(request)))
            .flatMap(
              (_) => restore(
                Task.fromCallback<void, Never>((complete) {
                  request.complete = complete;
                  _waiting.addLast(request);
                  _drain();
                  return () => _release(request);
                }),
              ),
            )
            .withError<E>()
            .flatMap((_) => restore(task));
      }).scoped();
}

final class _PermitRequest {
  _PermitRequest(this.count);

  final int count;
  late final void Function(Exit<void, Never>) complete;
  var granted = false;
  var finished = false;
}
