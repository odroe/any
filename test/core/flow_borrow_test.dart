import 'dart:async';

import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  late Runtime runtime;
  setUp(() {
    runtime = Runtime();
    addTearDown(runtime.close);
  });

  test(
    'consumption stops callback forks while their resources stay open',
    () async {
      var stopped = false;
      var released = false;
      final pending = Task.fromCallback<void, Never>((complete) {
        return () {
          stopped = true;
        };
      });
      final child = Task.addFinalizer((_) => Task.sync(() => released = true))
          .flatMap((_) => pending);
      await runtime.runOrThrow(
        Flow.fromTask(child.fork()).toList().map((fibers) {
          expect(stopped, isTrue);
          expect(released, isFalse);
          expect(fibers.single.poll(), isA<OptionSome>());
        }),
      );
      expect(released, isTrue);
    },
  );

  test(
    'pending native reads stop before subscription cleanup finishes',
    () async {
      final subscribed = Completer<void>();
      final cancelling = Completer<void>();
      final finishCancel = Completer<void>();
      final events = <String>[];
      final controller = StreamController<int>(
        onListen: subscribed.complete,
        onCancel: () async {
          events.add('cancel subscription');
          cancelling.complete();
          await finishCancel.future;
          events.add('subscription closed');
        },
      );
      final worker = runtime.start(
        Task.addFinalizer(
          (_) => Task.sync(() => events.add('resources released')),
        ).flatMap(
          (_) => Flow.fromStream<int, Never>(
            () => controller.stream,
            onError: Error.throwWithStackTrace,
          ).toList(),
        ),
      );
      await subscribed.future;
      worker.requestCancel();
      await cancelling.future;
      expect(worker.poll(), isA<OptionNone>());
      expect(events, ['cancel subscription']);
      finishCancel.complete();
      final exit = await runtime.runOrThrow(worker.awaitExit());
      expect((exit as ExitFailure).cause, isA<CauseCancelled>());
      expect(events, [
        'cancel subscription',
        'subscription closed',
        'resources released',
      ]);
      await controller.close();
    },
  );
}
