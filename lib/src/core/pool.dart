import 'dart:collection';

import 'cause.dart';
import 'exit.dart';
import 'internal/shared_resource_state.dart';
import 'latch.dart';
import 'option.dart';
import 'task.dart';

/// Reuses a bounded number of resources through exclusive, scope-owned leases.
///
/// Each [acquire] reserves one resource until the caller's scope closes. Waiting
/// callers are served in arrival order. Acquisition, active leases, idle values,
/// and unfinished cleanup all count toward capacity. Cancelling a waiting
/// caller removes only its request.
///
/// The creating scope owns this pool. Acquisition and cleanup use the services
/// and task-local values captured there. Closing rejects new requests, stops
/// unfinished acquisitions, and waits for active leases before cleanup. Borrowers
/// in another runtime must release their own leases. Returning the pool from a
/// completed Runtime.run does not extend its lifetime.
final class Pool<T, E> {
  Pool._(this._state, this._maxSize);

  final SharedResourceState<_Slot, T, E> _state;
  final int _maxSize;
  final _slots = <_Slot>{};
  final _idle = Queue<_Slot>();
  final _waiting = Queue<_Request>();
  var _closed = false;

  /// Creates a pool, optionally acquiring [initialSize] resources before return.
  ///
  /// [maxSize] must be positive. [initialSize] must be between zero and
  /// [maxSize], and a supplied [idleTimeout] must not be negative. Invalid
  /// arguments become defects during execution. Each execution creates an
  /// independent pool.
  ///
  /// Null [idleTimeout] retains returned resources until invalidated or closed.
  /// Zero releases them upon return. A positive timeout starts when a resource
  /// returns and is cancelled when it is borrowed again.
  /// Prewarming follows the same idle policy and does not establish a minimum
  /// size. A failed or cancelled prewarm releases everything before returning.
  /// Background idle cleanup failures are reported to the creating runtime.
  ///
  /// [acquire] should produce independently usable resources. If it returns the
  /// same object more than once, different slots can expose that object at the
  /// same time; [invalidate] retires all slots containing that object.
  static Task<Pool<T, E>, E> create<T, E>(
    Task<T, E> acquire, {
    required int maxSize,
    int initialSize = 0,
    Duration? idleTimeout,
  }) => Task.defer(() {
    if (maxSize <= 0) {
      throw RangeError.range(maxSize, 1, null, 'maxSize');
    }
    if (initialSize < 0 || initialSize > maxSize) {
      throw RangeError.range(initialSize, 0, maxSize, 'initialSize');
    }
    if (idleTimeout?.isNegative ?? false) {
      throw ArgumentError.value(
        idleTimeout,
        'idleTimeout',
        'Must not be negative.',
      );
    }
    late Pool<T, E> pool;
    return SharedResourceState.create<_Slot, T, E>(
          (_) => acquire,
          idleTimeout: idleTimeout,
          onClosing: (slot, completion) => pool._closing(slot, completion),
        )
        .flatMap(
          (state) => Task.acquireRelease(
            Task.sync(() => pool = Pool._(state, maxSize)),
            release: (pool, exit) => pool._close(exit),
          ),
        )
        .withError<E>()
        .flatMap(
          (pool) => pool
              ._warm(initialSize)
              .onExit(
                (exit) => exit is ExitFailure
                    ? pool._close(exit)
                    : Task.succeed(null),
              )
              .map((_) => pool),
        );
  });

  void _checkOpen() {
    if (_closed) throw StateError('The pool is closed.');
  }

  void _drain() {
    while (!_closed && _waiting.isNotEmpty) {
      final request = _waiting.first;
      final complete = request.complete;
      if (complete == null) return;
      final _Slot slot;
      if (_idle.isNotEmpty) {
        slot = _idle.removeFirst();
      } else if (_slots.length < _maxSize) {
        slot = _Slot();
        _slots.add(slot);
      } else {
        return;
      }
      _waiting.removeFirst();
      request.complete = null;
      request.slot = slot;
      slot.reserved = true;
      complete(Exit.success(slot));
    }
  }

  void _closing(_Slot slot, Future<TaskEvaluation<void, Never>> completion) {
    slot.closing = true;
    _idle.remove(slot);
    completion.then((_) {
      slot.closed = true;
      if (!slot.reserved) _slots.remove(slot);
      _drain();
    });
  }

  Task<_Slot, Never> _awaitSlot(_Request request) =>
      Task.fromCallback((complete) {
        _checkOpen();
        request.complete = complete;
        if (!_waiting.contains(request)) _waiting.addLast(request);
        _drain();
        return () {
          request.complete = null;
          _waiting.remove(request);
        };
      });

  Task<T, E> _take(
    _Request request,
    RestoreCancellation restore,
  ) => restore(_awaitSlot(request)).withError<E>().flatMap((slot) {
    _checkOpen();
    final acquire = slot.hasValue
        ? _state.acquireExisting(slot)
        : _state.acquire(slot).map(Option.some);
    return restore(acquire).flatMap(
      (result) => switch (result) {
        OptionSome(:final value) => Task.sync(() {
          slot.value = value;
          slot.hasValue = true;
          return value;
        }),
        OptionNone() => Task.defer(() {
          slot.reserved = false;
          if (slot.closed) _slots.remove(slot);
          request.slot = null;
          // Keep this request ahead of later waiters while its next callback
          // registration crosses any executor scheduling boundary.
          _waiting.addFirst(request);
          return _take(request, restore);
        }),
      },
    );
  });

  Task<T, E> _acquire() => Task.uninterruptibleMask((restore) {
    final request = _Request();
    return Task.addFinalizer((_) => _release(request))
        .withError<E>()
        .flatMap((_) => _take(request, restore));
  });

  Task<void, Never> _release(_Request request) => Task.defer(() {
    if (request.released) return Task.succeed(null);
    request.released = true;
    _waiting.remove(request);
    final slot = request.slot;
    if (slot == null) {
      _drain();
      return Task.succeed(null);
    }
    if (!_closed && !slot.closing && (!slot.hasValue || slot.retired)) {
      slot.retired = true;
      // A cancelled handoff may have acquired a value without publishing it to
      // this request. Retire that hidden value before giving up its slot.
      return _state
          .invalidate(slot)
          .onExit(
            (_) => Task.sync(() {
              slot.reserved = false;
              _slots.remove(slot);
              _drain();
            }),
          );
    }
    slot.reserved = false;
    if (slot.closed || _closed) {
      _slots.remove(slot);
    } else if (!slot.closing) {
      _idle.addLast(slot);
    }
    _drain();
    return Task.succeed(null);
  });

  Task<void, Never> _invalidate(Object? value) => Task.uninterruptibleMask(
    (_) => Task.defer(() {
      _checkOpen();
      final selected = [
        for (final slot in _slots)
          if (slot.hasValue &&
              !slot.retired &&
              !slot.closing &&
              identical(slot.value, value))
            slot,
      ];
      for (final slot in selected) {
        slot.retired = true;
        _idle.remove(slot);
      }
      if (selected.isEmpty) return Task.succeed(null);
      return _state.invalidateAll(selected);
    }),
  );

  Task<void, Never> _close(Exit<Object?, Object?> exit) => Task.defer(() {
    _closed = true;
    _idle.clear();
    for (final request in _waiting.toList()) {
      final complete = request.complete;
      request.complete = null;
      complete?.call(
        Exit.failure(
          Cause.defect(StateError('The pool is closed.'), StackTrace.current),
        ),
      );
    }
    _waiting.clear();
    return _state.close(exit);
  });

  Task<void, E> _warm(int count) {
    if (count == 0) return Task.succeed(null);
    return Latch.create().withError<E>().flatMap((ready) {
      var acquired = 0;
      return Task.forEach<int, void, E>(
        List.generate(count, (index) => index),
        (_) => _acquire().flatMap(
          (_) => Task.defer(
            () => ++acquired == count ? ready.open() : ready.awaitOpen(),
          ),
        ),
        concurrency: count,
      ).map((_) {});
    });
  }
}

/// Borrows and invalidates pool resources using their known value/error types.
extension PoolOperations<T, E> on Pool<T, E> {
  /// Acquires an exclusive resource lease until the calling scope closes.
  ///
  /// Waits in FIFO order when all slots are occupied. Acquisition and cleanup
  /// must finish before a slot can serve another request. Cancellation before
  /// delivery cannot leak a resource or reservation. A closed pool fails with a
  /// [StateError] defect. Acquiring while holding every slot can wait forever;
  /// compose nested work within an existing lease instead.
  Task<T, E> acquire() => Task.defer(_acquire);

  /// Retires every current slot whose acquired value is identical to [value].
  ///
  /// Active borrowers keep their resource until their scopes close; subsequent
  /// callers cannot reuse it. Unused resources are cleaned up before this task
  /// finishes, and those cleanup failures propagate. Cleanup of an active lease
  /// belongs to its returning borrower. Unknown or already retiring values do
  /// nothing. Cancellation waits for cleanup this call has already started.
  Task<void, Never> invalidate(T value) => Task.defer(() => _invalidate(value));

  /// Borrows a resource for [use], returning it before continuing to the caller.
  ///
  /// The callback and its resources run in a child scope. Its failure is retained
  /// alongside any resource cleanup failure. Exceptions from [use] are defects.
  Task<U, E> use<U>(Task<U, E> Function(T value) use) =>
      acquire().flatMap(use).scoped();
}

final class _Slot {
  Object? value;
  bool hasValue = false;
  bool reserved = false;
  bool retired = false;
  bool closing = false;
  bool closed = false;
}

final class _Request {
  void Function(Exit<_Slot, Never>)? complete;
  _Slot? slot;
  bool released = false;
}
