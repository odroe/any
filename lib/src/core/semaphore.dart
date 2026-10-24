import 'dart:collection';

import 'exit.dart';
import 'task.dart';

/// A fixed number of permits for limiting concurrent work within an isolate.
///
/// Use [SemaphoreOperations.withPermits] to run tasks under a shared limit.
/// Requesting one permit limits the number of concurrent operations; requesting
/// several lets more expensive operations consume a larger share of capacity.
/// Permits remain held until the wrapped task completes. To include resource
/// cleanup in that limit, pass a task wrapped in [TaskOperations.scoped].
///
/// Requests are served in arrival order. A smaller request waits behind an
/// earlier request even when enough permits are available for the smaller one.
final class Semaphore {
  Semaphore._(this._capacity) : _available = _capacity;

  final int _capacity;
  int _available;
  final _waiting = Queue<_PermitRequest>();
  var _draining = false;

  /// Creates a semaphore with [permits] available.
  ///
  /// [permits] must be positive; otherwise execution fails with a [RangeError]
  /// defect. Each execution creates an independent semaphore.
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

/// Runs tasks within a semaphore's capacity.
extension SemaphoreOperations on Semaphore {
  /// Runs [task] with [count] permits and releases them when it completes.
  ///
  /// Waits for enough permits, then returns the task's result or failure.
  /// [count] must be positive and no greater than capacity; an invalid value
  /// fails with a [RangeError] defect before [task] starts.
  ///
  /// Waiting and execution respect the caller's cancellation state. A cancelled
  /// waiter leaves other requests unaffected. Granted permits are returned on
  /// success, failure, or cancellation, including cancellation before [task]
  /// starts.
  ///
  /// Resources acquired by [task] belong to the caller's scope. Use
  /// `withPermits(count, task.scoped())` to keep the permits through cleanup.
  ///
  /// Nested requests do not reuse permits held by the caller. They can wait
  /// forever when the caller holds permits they need.
  Task<T, E> withPermits<T, E>(int count, Task<T, E> task) =>
      Task.uninterruptibleMask<T, E>((restore) {
        RangeError.checkValueInInterval(count, 1, _capacity, 'count');
        final request = _PermitRequest(count);

        // Install completion cleanup before waiting so a grant cannot be lost
        // when cancellation prevents the body from starting.
        return restore(
              Task.fromCallback<void, Never>((complete) {
                request.complete = complete;
                _waiting.addLast(request);
                _drain();
                return () => _release(request);
              }),
            )
            .withError<E>()
            .flatMap((_) => restore(task))
            .onExit((_) => Task.sync(() => _release(request)));
      });
}

final class _PermitRequest {
  _PermitRequest(this.count);

  final int count;
  late final void Function(Exit<void, Never>) complete;
  var granted = false;
  var finished = false;
}
