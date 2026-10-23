import 'dart:collection';

import 'cause.dart';
import 'exit.dart';
import 'internal/shared_resource_state.dart';
import 'latch.dart';
import 'option.dart';
import 'task.dart';

/// A bounded set of reusable resources leased to one caller at a time.
///
/// Use a pool for connections or handles that cannot safely serve concurrent
/// operations. [PoolOperations.use] borrows a resource for one operation;
/// [PoolOperations.acquire] keeps it until the caller's scope closes. Waiting
/// borrowers are served in arrival order.
///
/// The creating scope owns the pool and supplies the services and task-local
/// values for acquisition and cleanup. Closing it rejects new requests, stops
/// pending acquisitions, and waits for active leases. Borrowers in another
/// runtime must release their own leases. A pool returned from a finished
/// `Runtime.run` is already closed.
///
/// Here each file handle has its own position, and `use` prevents overlapping
/// seeks on that handle.
///
/// ```dart
/// import 'dart:io';
///
/// import 'package:any/any.dart';
///
/// Future<List<int>> readBytes(String path, List<int> offsets) async {
///   final runtime = Runtime();
///   final handle = Task.acquireRelease(
///     Task.fromFuture(
///       () => File(path).open(),
///       onError: (error, _) => error.toString(),
///     ),
///     release: (file, _) => Task.fromFuture(() async {
///       await file.close();
///     }, onError: Error.throwWithStackTrace),
///   );
///   final program = Pool.create(handle, maxSize: 4).flatMap(
///     (pool) => Task.forEach(
///       offsets,
///       (offset) => pool.use(
///         (file) => Task.fromFuture(() async {
///           await file.setPosition(offset);
///           return file.readByte();
///         }, onError: (error, _) => error.toString()),
///       ),
///       concurrency: 8,
///     ),
///   );
///   try {
///     return await runtime.runOrThrow(program);
///   } finally {
///     await runtime.close();
///   }
/// }
/// ```
final class Pool<T, E> {
  Pool._(this._state, this._maxSize);

  final SharedResourceState<_Slot, T, E> _state;
  final int _maxSize;
  final _slots = <_Slot>{};
  final _idle = Queue<_Slot>();
  final _waiting = Queue<_Request>();
  var _closed = false;

  /// Creates a pool, acquiring [initialSize] resources before it is ready.
  ///
  /// [maxSize] must be positive; [initialSize] must be between zero and [maxSize].
  /// Invalid values, including a negative [idleTimeout], become defects when
  /// the task runs. Capacity includes acquisitions, borrowed and idle resources,
  /// and unfinished cleanup. Each execution creates a separate pool.
  ///
  /// A null [idleTimeout] keeps returned resources until invalidation or closure.
  /// Zero releases them on return. A positive duration releases resources that
  /// remain idle that long; borrowing one resets its idle period. Idle cleanup
  /// failures are reported to the creating runtime.
  ///
  /// [initialSize] warms the pool without setting a minimum size. Warmed resources
  /// follow the same idle policy. A failed or cancelled warmup finishes all
  /// cleanup before returning.
  ///
  /// [acquire] should create independently usable resources. Returning the same
  /// object for multiple slots allows concurrent borrowers of that object;
  /// [PoolOperations.invalidate] retires all slots containing it.
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

/// Borrows, uses, and retires pooled resources.
extension PoolOperations<T, E> on Pool<T, E> {
  /// Borrows an exclusive resource until the calling scope closes.
  ///
  /// Waits in arrival order when the pool is full. Resources being acquired or
  /// cleaned up continue to occupy their slots. Cancellation removes a waiting
  /// request without affecting other borrowers; a delivered lease is released
  /// on scope closure.
  ///
  /// A closed pool fails with a [StateError] defect. Nested acquisition can wait
  /// forever if the caller already holds every slot. Reuse the current lease for
  /// nested work, or use [use] to bound its lifetime.
  Task<T, E> acquire() => Task.defer(_acquire);

  /// Removes [value] from reuse, matching by object identity.
  ///
  /// All slots containing that object are retired. Active borrowers keep their
  /// resource until their scopes close; their cleanup failures belong to those
  /// borrowers. For unused resources, this task waits for cleanup and propagates
  /// its failures.
  ///
  /// Unknown or already retiring values have no effect. Cancellation waits for
  /// cleanup already started. A closed pool fails with a [StateError] defect.
  Task<void, Never> invalidate(T value) => Task.defer(() => _invalidate(value));

  /// Borrows a resource for [use] and returns it before completing.
  ///
  /// Returns the callback's result after its child scope closes and releases
  /// the lease. If release also closes the resource, its cleanup finishes first.
  /// Callback failures and cleanup failures are both preserved; thrown callback
  /// exceptions become defects.
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
