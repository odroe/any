import 'dart:async';

import 'package:any/any.dart';
import 'package:any/src/core/fiber.dart' show fiberCleanupCancellations;
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
    expect(reports, isEmpty);
  });

  for (final identityMapping in [false, true]) {
    test(
      'identity mapping preserves callback resources: $identityMapping',
      () async {
        final events = <String>[];
        final handles = <_Handle>[];
        var flow = Flow.fromIterable([1, 2]);
        if (identityMapping) flow = flow.mapTask(Task.succeed);

        final exit = await runtime.run(
          flow
              .forEach((id) => _open(id, events).map(handles.add))
              .flatMap(
                (_) => Task.sync(() {
                  expect(handles.every((handle) => handle.open), isTrue);
                  events.add('consumed');
                }),
              ),
        );

        expect(exit, isA<ExitSuccess>());
        expect(events, ['open 1', 'open 2', 'consumed', 'close 2', 'close 1']);
      },
    );
  }

  for (final concurrency in [1, 3]) {
    test(
      'collected resources survive buffering: concurrency $concurrency',
      () async {
        final events = <String>[];
        final exit = await runtime.run(
          Flow.fromIterable([1, 2, 3])
              .mapTask((id) => _open(id, events), concurrency: concurrency)
              .buffer(2)
              .toList()
              .flatMap(
                (handles) => Task.sync(() {
                  expect(handles.map((handle) => handle.id), [1, 2, 3]);
                  expect(handles.every((handle) => handle.open), isTrue);
                  expect(
                    events.where((event) => event.startsWith('close')),
                    isEmpty,
                  );
                }),
              ),
        );

        expect(exit, isA<ExitSuccess>());
        expect(events.where((event) => event.startsWith('close')), [
          'close 3',
          'close 2',
          'close 1',
        ]);
      },
    );
  }

  test('explicit per-element scopes release before the next mapping', () async {
    final events = <String>[];
    final exit = await runtime.run(
      Flow.fromIterable([1, 2])
          .mapTask(
            (id) => _open(id, events).map((handle) => handle.id).scoped(),
          )
          .toList(),
    );

    expect(exit, isA<ExitSuccess<List<int>, Never>>());
    expect(events, ['open 1', 'close 1', 'open 2', 'close 2']);
  });

  test('native subscription closes before consuming task resources', () async {
    final events = <String>[];
    late StreamController<int> controller;
    controller = StreamController<int>(
      sync: true,
      onListen: () => controller.add(1),
      onCancel: () async {
        events.add('cancel subscription');
      },
    );
    final exit = await runtime.run(
      Flow.fromStream<int, Never>(
            () => controller.stream,
            onError: Error.throwWithStackTrace,
          )
          .mapTask((id) => _open(id, events))
          .take(1)
          .toList()
          .flatMap(
            (handles) => Task.sync(() {
              expect(handles.single.open, isTrue);
              expect(events, ['open 1', 'cancel subscription']);
              events.add('use result');
            }),
          ),
    );

    expect(exit, isA<ExitSuccess>());
    expect(events, ['open 1', 'cancel subscription', 'use result', 'close 1']);
    await controller.close();
  });

  test(
    'cancellation stops pending production before releasing resources',
    () async {
      final events = <String>[];
      final started = Completer<void>();
      final fiber = runtime.start(
        Flow.unfold<int, int, Never>(
          0,
          (state) => _open(state, events).flatMap(
            (_) => Task.fromCallback<Option<(int, int)>, Never>((complete) {
              started.complete();
              return () => events.add('stop production');
            }),
          ),
        ).buffer(1).toList(),
      );

      await started.future;
      fiber.requestCancel();
      final exit = await runtime.run(fiber.awaitExit());

      expect(exit, isA<ExitSuccess>());
      expect(events, ['open 0', 'stop production', 'close 0']);
    },
  );

  test(
    'shared production and consumer callbacks have different owners',
    () async {
      final events = <String>[];
      late _Handle sourceHandle;
      late _Handle consumerHandle;
      final exit = await runtime.run(
        Flow.fromTask(
              _open(1, events).map((handle) {
                sourceHandle = handle;
                return handle;
              }),
            )
            .share(capacity: 1)
            .flatMap(
              (shared) => shared
                  .mapTask(
                    (_) => _open(2, events).map((handle) {
                      consumerHandle = handle;
                      return handle;
                    }),
                  )
                  .toList()
                  .flatMap(
                    (handles) => Task.sync(() {
                      expect(handles.single.open, isTrue);
                      expect(sourceHandle.open, isTrue);
                    }),
                  )
                  .scoped()
                  .flatMap(
                    (_) => Task.sync(() {
                      expect(consumerHandle.open, isFalse);
                      expect(sourceHandle.open, isTrue);
                    }),
                  ),
            ),
      );

      expect(exit, isA<ExitSuccess>());
      expect(events, ['open 1', 'open 2', 'close 2', 'close 1']);
    },
  );

  test(
    'shared collected resources outlive production without replay',
    () async {
      final events = <String>[];
      late StreamController<int> controller;
      controller = StreamController<int>(
        sync: true,
        onListen: () => controller.add(1),
        onCancel: () => events.add('cancel subscription'),
      );
      final exit = await runtime.run(
        Flow.fromStream<int, Never>(
              () => controller.stream,
              onError: Error.throwWithStackTrace,
            )
            .mapTask((id) => _open(id, events))
            .share(capacity: 1)
            .flatMap(
              (shared) => shared
                  .take(1)
                  .toList()
                  .flatMap(
                    (handles) => Task.sync(() {
                      expect(handles.single.open, isTrue);
                      expect(events, ['open 1', 'cancel subscription']);
                      events.add('use collected value');
                    }),
                  ),
            ),
      );

      expect(exit, isA<ExitSuccess>());
      expect(events, [
        'open 1',
        'cancel subscription',
        'use collected value',
        'close 1',
      ]);
      await controller.close();
    },
  );

  final consumers = <String, Task<void, Never> Function(Flow<int, Never>)>{
    'buffer': (flow) => flow.buffer(2).drain(),
    'merge': (flow) => flow.merge(Flow.fromIterable(<int>[])).drain(),
    'zip': (flow) => flow.zip(Flow.fromIterable([0, 1])).drain(),
    'combineLatest': (flow) =>
        flow.combineLatest(Flow.fromIterable([0])).drain(),
    'groupedWithin': (flow) =>
        flow.groupedWithin(3, const Duration(minutes: 1)).drain(),
    'debounce': (flow) => flow.debounce(const Duration(minutes: 1)).drain(),
    'share': (flow) =>
        flow.share(capacity: 2).flatMap((shared) => shared.drain()),
  };
  for (final consumer in consumers.entries) {
    test(
      '${consumer.key} counts a reused body and cleanup cancellation separately',
      () async {
        const cancellation = Cause<Never>.cancelled('same object');
        final source = Flow.fromTask(
          Task.acquireRelease(
            Task.succeed(1),
            release: (_, _) => Task.failCause(cancellation),
          ).flatMap<int>((_) => Task.failCause(cancellation)).scoped(),
        );
        final fiber = runtime.start(consumer.value(source));
        final observed = await runtime.run(fiber.awaitExit());
        expect(observed, isA<ExitSuccess>());
        final result =
            (observed as ExitSuccess<Exit<void, Never>, Never>).value;
        expect(result, isA<ExitFailure>());
        final cause = (result as ExitFailure<void, Never>).cause;
        expect((cause as CauseGroup<Never>).causes, [
          same(cancellation),
          same(cancellation),
        ]);
        expect(fiberCleanupCancellations(fiber), [same(cancellation)]);
      },
    );
    test(
      '${consumer.key} preserves local cleanup cancellation during shutdown',
      () async {
        final started = Completer<void>();
        final release = Completer<void>();
        final controller = CancellationController();
        final source = Flow.fromIterable([1]).concat(
          Flow.fromTask(
            Task.acquireRelease(
              Task.succeed(2),
              release: (_, _) =>
                  Task.fromFuture<void, Never>(() {
                    started.complete();
                    return release.future;
                  }, onError: Error.throwWithStackTrace).flatMap(
                    (_) => Task.failCause(
                      Cause.combine([
                        const Cause.cancelled('cleanup'),
                        const Cause.cancelled('cleanup'),
                      ]),
                    ),
                  ),
            ).scoped(),
          ),
        );
        final running = runtime.run(
          consumer.value(source),
          signal: controller.signal,
        );

        await started.future;
        controller.requestCancel(reason: 'owner');
        release.complete();
        final result = await running;

        final causes = switch (result) {
          ExitFailure(cause: CauseGroup(:final causes)) => causes,
          ExitFailure(:final cause) => [cause],
          _ => const <Cause<Never>>[],
        };
        expect(
          causes.whereType<CauseCancelled>().map((cause) => cause.reason),
          unorderedEquals(['owner', 'cleanup', 'cleanup']),
        );
      },
    );
  }
}

final class _Handle {
  _Handle(this.id);

  final int id;
  var open = true;
}

Task<_Handle, Never> _open(int id, List<String> events) => Task.acquireRelease(
  Task.sync(() {
    events.add('open $id');
    return _Handle(id);
  }),
  release: (handle, _) => Task.sync(() {
    expect(handle.open, isTrue);
    handle.open = false;
    events.add('close $id');
  }),
);
