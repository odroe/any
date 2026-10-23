import 'dart:async';
import 'dart:collection';

import 'package:any/any.dart';

/// A shared limit on concurrently running tasks, including their cleanup.
///
/// Share one instance across workflows to enforce a common limit. Waiting
/// tasks acquire permits in registration order. A capacity of one serializes
/// access; acquisition is not reentrant, so a task must not wait for another
/// permit from an exhausted semaphore that it already holds.
///
/// ```dart
/// final requests = Semaphore(4);
/// final limited = requests.withPermit(
///   Task<int, Never, ()>((_) => const Exit.success(42)),
/// );
/// final exit = await limited.run(());
/// ```
final class Semaphore {
  /// Creates a semaphore with [capacity] permits.
  ///
  /// Throws a [RangeError] if [capacity] is less than one.
  Semaphore(this.capacity) : _available = capacity {
    if (capacity < 1) {
      throw RangeError.range(capacity, 1, null, 'capacity');
    }
  }

  /// The maximum number of tasks allowed to hold permits simultaneously.
  final int capacity;
  int _available;
  final _waiting = LinkedList<_Waiter>();

  /// A task that acquires one permit, runs [task], and releases the permit.
  ///
  /// Acquisition is lazy and repeated on every run. Cancellation while waiting
  /// removes that waiter without starting [task]. A granted permit is always
  /// released, including when cancellation races with acquisition.
  ///
  /// The wrapped task owns a fresh [Scope] and shares the caller's dependencies
  /// and cancellation token. Its cleanup finishes before the permit is reused.
  /// Returned resources have therefore already been released; acquire and use
  /// them inside [task]. The caller's own scope stays open.
  Task<T, E, R> withPermit<T, E, R>(Task<T, E, R> task) =>
      Task((context) async {
        if (_available > 0) {
          _available--;
        } else {
          final waiter = _Waiter();
          _waiting.add(waiter);
          final removeListener = context.cancellation.onCancel((_) {
            if (waiter.list != null) {
              waiter.unlink();
              waiter.ready.complete(false);
            }
          });
          try {
            if (!await waiter.ready.future) {
              return Exit.failure(
                Cause.cancelled(reason: context.cancellation.reason),
              );
            }
          } finally {
            removeListener();
          }
        }

        try {
          return await task.run(
            context.dependencies,
            cancellation: context.cancellation,
            yieldEvery: context.yieldEvery,
          );
        } finally {
          if (_waiting.isEmpty) {
            _available++;
          } else {
            final next = _waiting.first..unlink();
            next.ready.complete(true);
          }
        }
      });
}

final class _Waiter extends LinkedListEntry<_Waiter> {
  final ready = Completer<bool>();
}
