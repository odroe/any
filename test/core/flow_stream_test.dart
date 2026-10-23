import 'dart:async';

import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  late Runtime runtime;
  final reports = <Cause<Object?>>[];
  setUp(() {
    reports.clear();
    runtime = Runtime(onUnhandledError: reports.add);
  });
  tearDown(() async {
    await runtime.close();
    await _turn();
    expect(reports, isEmpty);
  });

  test(
    'native factories are lazy and independent for each consumption',
    () async {
      var starts = 0;
      final flow = Flow.fromStream(() {
        starts++;
        return Stream<int?>.fromIterable([null, 2]);
      }, onError: (error, _) => error.toString());
      expect(starts, 0);
      expect(_value(await runtime.run(flow.toList())), [null, 2]);
      expect(_value(await runtime.run(flow.toList())), [null, 2]);
      expect(starts, 2);
    },
  );

  test(
    'the first native error follows buffered data and maps its original stack',
    () async {
      final native = StateError('native');
      final stack = StackTrace.fromString('native stack');
      var mapped = 0;
      final source = _PushStream<int>((subscription) {
        subscription.data(1);
        subscription.error(native, stack);
        subscription.data(2);
        subscription.error(StateError('ignored'), StackTrace.current);
      });
      final values = <int>[];
      final flow = Flow.fromStream(
        () => source,
        onError: (error, trace) {
          mapped++;
          expect(error, same(native));
          expect(trace, same(stack));
          return 'read failed';
        },
      );
      final result = await runtime.run(
        flow.forEach((value) => Task.sync(() => values.add(value))),
      );
      expect(values, [1]);
      expect((_cause(result) as CauseFailure).error, 'read failed');
      expect(mapped, 1);
      expect(source.subscription.cancelled, 1);
    },
  );

  test(
    'factory, listen, and error mapping exceptions become defects',
    () async {
      final error = StateError('callback');
      final stack = StackTrace.fromString('callback stack');
      final sources = [
        Flow.fromStream<int, String>(
          () => Error.throwWithStackTrace(error, stack),
          onError: (_, _) => 'unused',
        ),
        Flow.fromStream<int, String>(
          () => _PushStream((_) => Error.throwWithStackTrace(error, stack)),
          onError: (_, _) => 'unused',
        ),
        Flow.fromStream<int, String>(
          () => Stream.error('native'),
          onError: (_, _) => Error.throwWithStackTrace(error, stack),
        ),
      ];
      for (final flow in sources) {
        final defect = _defects(_cause(await runtime.run(flow.drain()))).single;
        expect(defect.error, same(error));
        expect(defect.stackTrace.toString(), stack.toString());
      }
    },
  );

  test('invalid capacity fails lazily without calling the factory', () async {
    var starts = 0;
    for (final capacity in [0, -1]) {
      final flow = Flow.fromStream(
        () {
          starts++;
          return const Stream<int>.empty();
        },
        onError: (error, _) => error,
        bufferCapacity: capacity,
      );
      expect(
        _defects(_cause(await runtime.run(flow.drain()))).single.error,
        isA<RangeError>(),
      );
    }
    expect(starts, 0);
  });

  test(
    'cooperative subscriptions pause until another element is requested',
    () async {
      var pauses = 0;
      var resumes = 0;
      final controller = StreamController<int>(
        sync: true,
        onPause: () => pauses++,
        onResume: () => resumes++,
      );
      final gate = Completer<void>();
      final values = <int>[];
      final flow = Flow.fromStream(() => controller.stream, onError: _defect);
      final running = runtime.run(
        flow.forEach((value) {
          values.add(value);
          return value == 1
              ? Task.fromFuture(() => gate.future, onError: _defect)
              : Task.succeed(null);
        }),
      );
      await _until(() => controller.hasListener);
      controller.add(1);
      await _until(() => values.isNotEmpty);
      expect(controller.isPaused, isTrue);
      controller.add(2);
      await _turn();
      expect(values, [1]);
      gate.complete();
      await _until(() => values.length == 2);
      unawaited(controller.close());
      expect(await running, isA<ExitSuccess>());
      expect(values, [1, 2]);
      expect(pauses, greaterThanOrEqualTo(1));
      expect(resumes, greaterThanOrEqualTo(1));
    },
  );

  test(
    'overflow fails after buffered data without invoking native error mapping',
    () async {
      var mapped = false;
      final source = _PushStream<int>((subscription) {
        subscription.data(1);
        subscription.data(2);
        subscription.data(3);
      });
      final values = <int>[];
      final flow = Flow.fromStream(
        () => source,
        bufferCapacity: 2,
        onError: (_, _) {
          mapped = true;
          return 'unused';
        },
      );
      final result = await runtime.run(
        flow.forEach((value) => Task.sync(() => values.add(value))),
      );
      expect(values, [1, 2]);
      expect(
        _defects(_cause(result)).single.error,
        isA<StreamBufferOverflow>().having(
          (error) => error.capacity,
          'capacity',
          2,
        ),
      );
      expect(mapped, isFalse);
      expect(source.subscription.cancelled, 1);
    },
  );

  for (final strategy in [
    StreamOverflowStrategy.dropping,
    StreamOverflowStrategy.sliding,
  ]) {
    test('$strategy bounds data from a source that ignores pause', () async {
      final source = _PushStream<int>((subscription) {
        for (var i = 1; i <= 4; i++) {
          subscription.data(i);
        }
        subscription.done();
      });
      final flow = Flow.fromStream(
        () => source,
        onError: _defect,
        bufferCapacity: 2,
        overflow: strategy,
      );
      expect(
        _value(await runtime.run(flow.toList())),
        strategy == StreamOverflowStrategy.dropping ? [1, 2] : [3, 4],
      );
      expect(source.subscription.cancelled, 1);
    });
  }

  test('dropping data does not discard a terminal error', () async {
    final source = _PushStream<int>((subscription) {
      subscription.data(1);
      subscription.data(2);
      subscription.error('terminal', StackTrace.current);
    });
    final values = <int>[];
    final result = await runtime.run(
      Flow.fromStream(
        () => source,
        overflow: StreamOverflowStrategy.dropping,
        onError: (error, _) => error.toString(),
      ).forEach((value) => Task.sync(() => values.add(value))),
    );
    expect(values, [1]);
    expect((_cause(result) as CauseFailure).error, 'terminal');
  });

  test('early termination awaits native cancellation', () async {
    final cancelling = Completer<void>();
    final gate = Completer<void>();
    final source = _PushStream<int>(
      (subscription) => subscription.data(1),
      onCancel: () {
        cancelling.complete();
        return gate.future;
      },
    );
    var complete = false;
    final running = runtime
        .run(Flow.fromStream(() => source, onError: _defect).take(1).toList())
        .then((exit) {
          complete = true;
          return exit;
        });
    await cancelling.future;
    expect(complete, isFalse);
    gate.complete();
    expect(_value(await running), [1]);
    expect(source.subscription.cancelled, 1);
  });

  test(
    'native business failure and cancellation cleanup defect are retained',
    () async {
      final error = StateError('cancel');
      final stack = StackTrace.fromString('cancel stack');
      final source = _PushStream<int>(
        (subscription) => subscription.error('native', StackTrace.current),
        onCancel: () => Future.error(error, stack),
      );
      final result = await runtime.run(
        Flow.fromStream(
          () => source,
          onError: (error, _) => error.toString(),
        ).drain(),
      );
      final reasons = _reasons(_cause(result));
      expect(reasons, hasLength(2));
      expect((reasons.first as CauseFailure).error, 'native');
      expect((reasons.last as CauseDefect).error, same(error));
      expect(
        (reasons.last as CauseDefect).stackTrace.toString(),
        stack.toString(),
      );
    },
  );

  test('owner cancellation prevents pending native error conversion', () async {
    final source = _PushStream<int>((_) {});
    final controller = CancellationController();
    var mapped = false;
    final running = runtime.run(
      Flow.fromStream(
        () => source,
        onError: (_, _) {
          mapped = true;
          return 'unused';
        },
      ).drain(),
      signal: controller.signal,
    );
    await _until(() => source.listening);
    source.subscription.error('native', StackTrace.current);
    controller.requestCancel(reason: 'owner');
    expect(_cause(await running), isA<CauseCancelled>());
    expect(mapped, isFalse);
    expect(source.subscription.cancelled, 1);
  });

  test(
    'detected overflow is retained when early take stops before reading it',
    () async {
      final source = _PushStream<int>((subscription) {
        subscription.data(1);
        subscription.data(2);
      });
      final result = await runtime.run(
        Flow.fromStream(() => source, onError: _defect).take(1).drain(),
      );
      expect(
        _defects(_cause(result)).single.error,
        isA<StreamBufferOverflow>(),
      );
      expect(source.subscription.cancelled, 1);
    },
  );

  test(
    'native error remains when pausing its subscription also fails',
    () async {
      final pauseError = StateError('pause');
      final source = _PushStream<int>(
        (subscription) => subscription.error('native', StackTrace.current),
        onPause: (_) => throw pauseError,
      );
      final result = await runtime.run(
        Flow.fromStream(
          () => source,
          onError: (error, _) => error.toString(),
        ).drain(),
      );
      final causes = _reasons(_cause(result));
      expect((causes.first as CauseFailure).error, 'native');
      expect((causes.last as CauseDefect).error, same(pauseError));
      expect(causes, hasLength(2));
    },
  );

  test(
    'overflow and reentrant pause failure each survive exactly once',
    () async {
      final pauseError = StateError('pause');
      final source = _PushStream<int>(
        (subscription) => subscription.data(1),
        onPause: (subscription) {
          subscription.data(2);
          throw pauseError;
        },
      );
      final result = await runtime.run(
        Flow.fromStream(() => source, onError: _defect).drain(),
      );
      final errors = _defects(_cause(result))
          .map((cause) => cause.error)
          .toList();
      expect(errors, [isA<StreamBufferOverflow>(), same(pauseError)]);
      expect(source.subscription.cancelled, 1);
    },
  );

  test(
    'native error remains when resuming its subscription also fails',
    () async {
      final resumeError = StateError('resume');
      final source = _PushStream<int>(
        (subscription) => subscription.data(1),
        onResume: (subscription) {
          subscription.error('native', StackTrace.current);
          throw resumeError;
        },
      );
      final result = await runtime.run(
        Flow.fromStream(
          () => source,
          onError: (error, _) => error.toString(),
        ).drain(),
      );
      final causes = _reasons(_cause(result));
      expect((causes.first as CauseFailure).error, 'native');
      expect((causes.last as CauseDefect).error, same(resumeError));
      expect(causes, hasLength(2));
      expect(source.subscription.cancelled, 1);
    },
  );

  test('toStream is lazy and does not deliver before listen returns', () async {
    var starts = 0;
    var listeningReturned = false;
    final stream = Flow.fromTask(Task.sync(() => ++starts)).toStream(runtime);
    expect(starts, 0);
    final done = Completer<void>();
    final values = <int>[];
    stream.listen((value) {
      expect(listeningReturned, isTrue);
      values.add(value);
    }, onDone: done.complete);
    listeningReturned = true;
    await done.future;
    expect(values, [1]);
    expect(starts, 1);
    expect(() => stream.listen((_) {}), throwsStateError);
  });

  test('toStream pause prevents further upstream pulls', () async {
    final pulled = <int>[];
    final values = <int>[];
    Iterable<int> source() sync* {
      for (var i = 1; i <= 3; i++) {
        pulled.add(i);
        yield i;
      }
    }

    final done = Completer<void>();
    late StreamSubscription<int> subscription;
    subscription = Flow.fromIterable(source())
        .toStream(runtime)
        .listen((value) {
          values.add(value);
          if (value == 1) subscription.pause();
        }, onDone: done.complete);
    await _until(() => values.isNotEmpty);
    await _turn();
    expect(pulled, [1]);
    subscription.resume();
    await done.future;
    expect(values, [1, 2, 3]);
  });

  test('repeated asynchronous pause and resume cannot lose a wakeup', () async {
    final values = <int>[];
    final done = Completer<void>();
    late StreamSubscription<int> subscription;
    subscription = Flow.fromIterable(List.generate(3000, (i) => i))
        .toStream(runtime)
        .listen((value) {
          values.add(value);
          final resume = Completer<void>();
          subscription.pause(resume.future);
          scheduleMicrotask(resume.complete);
        }, onDone: done.complete);
    try {
      await done.future.timeout(const Duration(seconds: 3));
      expect(values, List.generate(3000, (i) => i));
    } finally {
      await subscription.cancel();
    }
  });

  test('pausing before the startup microtask prevents source work', () async {
    var starts = 0;
    final done = Completer<void>();
    final subscription = Flow.fromTask(
      Task.sync(() => ++starts),
    ).toStream(runtime).listen((_) {}, onDone: done.complete)..pause();
    await _turn();
    expect(starts, 0);
    subscription.resume();
    await done.future;
    expect(starts, 1);
  });

  test('cancelling before startup does not run the producer', () async {
    var starts = 0;
    final subscription = Flow.fromTask(Task.sync(() => ++starts))
        .toStream(runtime)
        .listen((_) {});
    await subscription.cancel();
    expect(starts, 0);
  });

  test('synchronous onData cancellation waits for producer cleanup', () async {
    final releasing = Completer<void>();
    final gate = Completer<void>();
    var complete = false;
    final values = <int>[];
    late Future<void> cancelled;
    late StreamSubscription<int> subscription;
    final flow = Flow.fromTask(
      Task.acquireRelease(
        Task.succeed(1),
        release: (_, _) => Task.fromFuture(() {
          releasing.complete();
          return gate.future;
        }, onError: _defect),
      ),
    ).concat(Flow.fromIterable([2]));
    subscription = flow.toStream(runtime).listen((value) {
      values.add(value);
      cancelled = subscription.cancel().then((_) {
        complete = true;
      });
    });
    await releasing.future;
    expect(complete, isFalse);
    expect(values, [1]);
    gate.complete();
    await cancelled;
    expect(complete, isTrue);
    expect(values, [1]);
  });

  test('subscription cancellation exposes cleanup defects without its own cancellation', () async {
    final error = StateError('release');
    final stack = StackTrace.fromString('release stack');
    final cancelled = Completer<void>();
    late StreamSubscription<int> subscription;
    final flow = Flow.fromTask(
      Task.acquireRelease(
        Task.succeed(1),
        release: (_, _) =>
            Task.sync(() => Error.throwWithStackTrace(error, stack)),
      ),
    );
    subscription = flow.toStream(runtime).listen((_) {
      unawaited(
        subscription.cancel().then(
          (_) => cancelled.complete(),
          onError: (Object error, StackTrace stack) =>
              cancelled.completeError(error, stack),
        ),
      );
    });
    await expectLater(
      cancelled.future,
      throwsA(
        isA<TaskException>().having(
          (exception) => _reasons(exception.cause),
          'causes',
          [
            isA<CauseDefect>().having(
              (cause) => cause.error,
              'error',
              same(error),
            ),
          ],
        ),
      ),
    );
  });

  test(
    'toStream delivers the complete failure once followed by done',
    () async {
      final defect = Cause<Never>.defect(
        StateError('defect'),
        StackTrace.fromString('defect stack'),
      );
      final cause = Cause<String>.combine([
        const Cause.failure('business'),
        defect,
      ]);
      final errors = <TaskException>[];
      final stacks = <StackTrace>[];
      final done = Completer<void>();
      Flow.fromTask<int, String>(Task.failCause(cause))
          .toStream(runtime)
          .listen(
            (_) {},
            onError: (Object error, StackTrace stack) {
              errors.add(error as TaskException);
              stacks.add(stack);
            },
            onDone: done.complete,
          );
      await done.future;
      expect(errors, hasLength(1));
      expect(_reasons(errors.single.cause), [
        isA<CauseFailure<String>>(),
        same(defect),
      ]);
      expect(stacks.single.toString(), 'defect stack');
    },
  );

  test(
    'cancelOnError does not report the delivered failure a second time',
    () async {
      final error = Completer<Object>();
      Flow.fromTask<int, String>(Task.fail('business'))
          .toStream(runtime)
          .listen((_) {}, onError: error.complete, cancelOnError: true);
      expect(await error.future, isA<TaskException>());
      await _turn();
    },
  );

  test('cancelling a paused stream retains an undelivered failure', () async {
    final started = Completer<void>();
    final result = Completer<int>();
    final defect = StateError('producer');
    final stack = StackTrace.fromString('producer stack');
    final errors = <Object>[];
    final subscription = Flow.fromTask(
      Task.fromFuture(() {
        started.complete();
        return result.future;
      }, onError: _defect),
    ).toStream(runtime).listen((_) {}, onError: errors.add);
    await started.future;
    subscription.pause();
    result.completeError(defect, stack);
    await _turn();
    expect(errors, isEmpty);
    await expectLater(
      subscription.cancel(),
      throwsA(
        isA<TaskException>()
            .having(
              (error) => _defects(error.cause).single.error,
              'defect',
              same(defect),
            )
            .having(
              (error) => _defects(error.cause).single.stackTrace,
              'stack',
              same(stack),
            ),
      ),
    );
    expect(errors, isEmpty);
  });

  test(
    'cancelling immediately after resume retains a pending failure',
    () async {
      final started = Completer<void>();
      final result = Completer<int>();
      final errors = <Object>[];
      final subscription = Flow.fromTask(
        Task.fromFuture(() {
          started.complete();
          return result.future;
        }, onError: (_, _) => 'business'),
      ).toStream(runtime).listen((_) {}, onError: errors.add);
      await started.future;
      subscription.pause();
      result.completeError(StateError('producer'));
      await _turn();
      subscription.resume();
      await expectLater(
        subscription.cancel(),
        throwsA(
          isA<TaskException>().having(
            (error) => (error.cause as CauseFailure<String>).error,
            'business error',
            'business',
          ),
        ),
      );
      expect(errors, isEmpty);
    },
  );

  for (final cancelOnError in [false, true]) {
    test(
      'a resumed failure is delivered once with cancelOnError=$cancelOnError',
      () async {
        final started = Completer<void>();
        final result = Completer<int>();
        final delivered = Completer<void>();
        final errors = <Object>[];
        late StreamSubscription<int> subscription;
        subscription =
            Flow.fromTask(
                  Task.fromFuture(() {
                    started.complete();
                    return result.future;
                  }, onError: (_, _) => 'business'),
                )
                .toStream(runtime)
                .listen(
                  (_) {},
                  onError: (Object error) {
                    errors.add(error);
                    unawaited(
                      subscription.cancel().then(
                        delivered.complete,
                        onError: delivered.completeError,
                      ),
                    );
                  },
                  cancelOnError: cancelOnError,
                );
        await started.future;
        subscription.pause();
        result.completeError(StateError('producer'));
        await _turn();
        expect(errors, isEmpty);
        subscription.resume();
        await delivered.future;
        expect(errors.single, isA<TaskException>());
        await _turn();
      },
    );
  }

  test('runtime shutdown terminates an active stream after cleanup', () async {
    final ready = Completer<void>();
    var cleaned = false;
    final done = Completer<void>();
    final errors = <TaskException>[];
    Flow.fromTask(
          Task.fromCallback<int, Never>((_) {
            ready.complete();
            return () {
              cleaned = true;
            };
          }),
        )
        .toStream(runtime)
        .listen(
          (_) {},
          onError: (Object error) => errors.add(error as TaskException),
          onDone: done.complete,
        );
    await ready.future;
    await runtime.close();
    await done.future;
    expect(cleaned, isTrue);
    expect(errors, hasLength(1));
    expect(errors.single.cause, isA<CauseCancelled>());
  });

  test(
    'runtime shutdown does not wait for a paused listener to receive done',
    () async {
      var cleaned = false;
      final delivered = Completer<void>();
      final done = Completer<void>();
      final errors = <TaskException>[];
      late StreamSubscription<int> subscription;
      subscription =
          Flow.fromTask(
                Task.acquireRelease(
                  Task.succeed(1),
                  release: (_, _) => Task.sync(() {
                    cleaned = true;
                  }),
                ),
              )
              .toStream(runtime)
              .listen(
                (_) {
                  subscription.pause();
                  delivered.complete();
                },
                onError: (Object error) => errors.add(error as TaskException),
                onDone: done.complete,
              );
      await delivered.future;
      await runtime.close().timeout(const Duration(seconds: 2));
      expect(cleaned, isTrue);
      expect(done.isCompleted, isFalse);
      subscription.resume();
      await done.future;
      expect(errors.single.cause, isA<CauseCancelled>());
    },
  );

  test('a closed runtime rejects stream work without starting it', () async {
    await runtime.close();
    var started = false;
    final stream = Flow.fromTask(
      Task.sync(() {
        started = true;
        return 1;
      }),
    ).toStream(runtime);
    await expectLater(
      stream.toList(),
      throwsA(
        isA<TaskException>().having(
          (error) => _defects(error.cause).single.error,
          'error',
          isA<RuntimeClosed>(),
        ),
      ),
    );
    expect(started, isFalse);
  });

  test('await for break waits for resource cleanup', () async {
    var alive = false;
    final flow = Flow.fromTask(
      Task.acquireRelease(
        Task.sync(() {
          alive = true;
          return 1;
        }),
        release: (_, _) => Task.sync(() {
          alive = false;
        }),
      ),
    );
    await for (final value in flow.toStream(runtime)) {
      expect(value, 1);
      await _turn();
      expect(alive, isTrue);
      break;
    }
    expect(alive, isFalse);
  });
}

// A deliberately non-cooperative source exercises the bridge's own buffer;
// normal Dart subscriptions may instead buffer data internally while paused.
final class _PushStream<T> extends Stream<T> {
  _PushStream(this.onListen, {this.onCancel, this.onPause, this.onResume});

  final void Function(_PushSubscription<T>) onListen;
  final Future<void> Function()? onCancel;
  final void Function(_PushSubscription<T>)? onPause;
  final void Function(_PushSubscription<T>)? onResume;
  late _PushSubscription<T> subscription;
  var listening = false;

  @override
  StreamSubscription<T> listen(
    void Function(T)? onData, {
    Function? onError,
    void Function()? onDone,
    bool? cancelOnError,
  }) {
    listening = true;
    subscription = _PushSubscription(
      onData,
      onError,
      onDone,
      onCancel,
      onPause,
      onResume,
    );
    onListen(subscription);
    return subscription;
  }
}

final class _PushSubscription<T> implements StreamSubscription<T> {
  _PushSubscription(
    this._data,
    this._error,
    this._done,
    this._cancel,
    this._pause,
    this._resume,
  );

  void Function(T)? _data;
  Function? _error;
  void Function()? _done;
  final Future<void> Function()? _cancel;
  final void Function(_PushSubscription<T>)? _pause;
  final void Function(_PushSubscription<T>)? _resume;
  var cancelled = 0;
  @override
  bool isPaused = false;

  void data(T value) {
    if (cancelled == 0) _data?.call(value);
  }

  void error(Object error, StackTrace stack) {
    if (cancelled == 0) Function.apply(_error!, [error, stack]);
  }

  void done() {
    if (cancelled == 0) _done?.call();
  }

  @override
  Future<void> cancel() async {
    cancelled++;
    await _cancel?.call();
  }

  @override
  void pause([Future<void>? resumeSignal]) {
    isPaused = true;
    _pause?.call(this);
  }

  @override
  void resume() {
    isPaused = false;
    _resume?.call(this);
  }

  @override
  void onData(void Function(T)? handleData) {
    _data = handleData;
  }

  @override
  void onError(Function? handleError) {
    _error = handleError;
  }

  @override
  void onDone(void Function()? handleDone) {
    _done = handleDone;
  }

  @override
  Future<E> asFuture<E>([E? futureValue]) =>
      throw UnsupportedError('Unused by the stream bridge.');
}

Never _defect(Object error, StackTrace stack) =>
    Error.throwWithStackTrace(error, stack);
T _value<T, E>(Exit<T, E> exit) => switch (exit) {
  ExitSuccess(:final value) => value,
  ExitFailure(:final cause) => fail('Expected success, got ${_reasons(cause)}'),
};
Cause<E> _cause<T, E>(Exit<T, E> exit) => switch (exit) {
  ExitSuccess(:final value) => fail('Expected failure, got $value'),
  ExitFailure(:final cause) => cause,
};
List<Cause<E>> _reasons<E>(Cause<E> cause) => switch (cause) {
  CauseGroup(:final causes) => causes,
  _ => [cause],
};
Iterable<CauseDefect> _defects(Cause<Object?> cause) =>
    _reasons(cause).whereType<CauseDefect>();
Future<void> _turn() => Future<void>.delayed(Duration.zero);
Future<void> _until(bool Function() condition) async {
  for (var attempt = 0; attempt < 100 && !condition(); attempt++) {
    await _turn();
  }
  expect(condition(), isTrue);
}
