import 'package:any/any.dart';
import 'package:test/test.dart';

void main() {
  late Runtime runtime;

  setUp(() => runtime = Runtime());
  tearDown(() async {
    expect(await runtime.close(), isA<ExitSuccess<void, Never>>());
  });

  test('create produces independent deferreds on repeated execution', () async {
    final create = Deferred.create<int, String>();
    final first = await runtime.runOrThrow(create);
    final second = await runtime.runOrThrow(create);

    expect(first, isNot(same(second)));
    expect(first.poll(), isA<OptionNone<Exit<int, String>>>());
    expect(second.poll(), isA<OptionNone<Exit<int, String>>>());

    expect(await runtime.runOrThrow(first.succeed(7)), isTrue);
    expect(second.poll(), isA<OptionNone<Exit<int, String>>>());
  });

  test('completion is lazy and only its first execution wins', () async {
    final deferred = await runtime.runOrThrow(Deferred.create<int, String>());
    const exit = Exit<int, String>.success(7);
    final complete = deferred.complete(exit);

    expect(deferred.poll(), isA<OptionNone<Exit<int, String>>>());
    expect(await runtime.runOrThrow(complete), isTrue);
    expect(await runtime.runOrThrow(complete), isFalse);
    expect(await runtime.runOrThrow(deferred.succeed(9)), isFalse);
    expect(await runtime.runOrThrow(deferred.fail('late')), isFalse);
    expect(
      deferred.poll(),
      isA<OptionSome<Exit<int, String>>>().having(
        (option) => option.value,
        'stored exit',
        same(exit),
      ),
    );
  });

  test('the same waiting task supports multiple observers', () async {
    final deferred = await runtime.runOrThrow(Deferred.create<int, String>());
    final waiting = deferred.awaitExit();
    final first = runtime.run(waiting);
    final second = runtime.run(waiting);
    const exit = Exit<int, String>.success(7);

    expect(await runtime.runOrThrow(deferred.complete(exit)), isTrue);

    expect(_value(await first), same(exit));
    expect(_value(await second), same(exit));
    expect(await runtime.runOrThrow(deferred.awaitExit()), same(exit));
    expect(await runtime.runOrThrow(deferred.join()), 7);
  });

  test('a completed failure is available to late observers', () async {
    final deferred = await runtime.runOrThrow(Deferred.create<int, String>());
    expect(await runtime.runOrThrow(deferred.fail('missing')), isTrue);
    expect(await runtime.runOrThrow(deferred.succeed(7)), isFalse);

    final observed = await runtime.runOrThrow(deferred.awaitExit());
    final joined = await runtime.run(deferred.join());

    expect(
      _cause(observed),
      isA<CauseFailure<String>>().having(
        (cause) => cause.error,
        'error',
        'missing',
      ),
    );
    expect(
      _cause(joined),
      isA<CauseFailure<String>>().having(
        (cause) => cause.error,
        'error',
        'missing',
      ),
    );
  });

  test(
    'complete retains mixed failures and original defect information',
    () async {
      final deferred = await runtime.runOrThrow(Deferred.create<int, String>());
      final defect = StateError('cleanup');
      final stackTrace = StackTrace.fromString('original deferred defect');
      final reason = Object();
      final exit = Exit<int, String>.failure(
        Cause.combine([
          const Cause.failure('business'),
          Cause.defect(defect, stackTrace),
          Cause.cancelled(reason),
        ]),
      );
      final waiting = runtime.run(deferred.awaitExit());

      expect(await runtime.runOrThrow(deferred.complete(exit)), isTrue);
      expect(_value(await waiting), same(exit));

      final cause = _cause(await runtime.run(deferred.join()));
      expect(cause, isA<CauseGroup<String>>());
      final reasons = (cause as CauseGroup<String>).causes;
      expect(reasons, hasLength(3));
      expect(
        reasons[0],
        isA<CauseFailure<String>>().having(
          (cause) => cause.error,
          'error',
          'business',
        ),
      );
      expect(
        reasons[1],
        isA<CauseDefect>()
            .having((cause) => cause.error, 'error', same(defect))
            .having(
              (cause) => cause.stackTrace,
              'stack trace',
              same(stackTrace),
            ),
      );
      expect(
        reasons[2],
        isA<CauseCancelled>().having(
          (cause) => cause.reason,
          'reason',
          same(reason),
        ),
      );
    },
  );

  test(
    'nullable success and nullable business failure remain distinct',
    () async {
      final success = await runtime.runOrThrow(
        Deferred.create<String?, String?>(),
      );
      final failure = await runtime.runOrThrow(
        Deferred.create<String?, String?>(),
      );

      expect(await runtime.runOrThrow(success.succeed(null)), isTrue);
      expect(await runtime.runOrThrow(failure.fail(null)), isTrue);

      expect(await runtime.runOrThrow(success.join()), isNull);
      expect(
        _cause(await runtime.run(failure.join())),
        isA<CauseFailure<String?>>().having(
          (cause) => cause.error,
          'error',
          isNull,
        ),
      );
    },
  );

  test(
    'cancelling one join does not affect other observers or completion',
    () async {
      final deferred = await runtime.runOrThrow(Deferred.create<int, String>());
      final controller = CancellationController();
      final waiting = deferred.join();
      final first = runtime.run(waiting, signal: controller.signal);
      final second = runtime.run(waiting);

      controller.requestCancel(reason: 'stop first');
      expect(
        _cause(await first),
        isA<CauseCancelled>().having(
          (cause) => cause.reason,
          'reason',
          'stop first',
        ),
      );
      expect(deferred.poll(), isA<OptionNone<Exit<int, String>>>());

      expect(await runtime.runOrThrow(deferred.succeed(7)), isTrue);
      expect(_value(await second), 7);
      expect(await runtime.runOrThrow(deferred.join()), 7);
    },
  );

  test('awaitExit does not capture cancellation of its own wait', () async {
    final deferred = await runtime.runOrThrow(Deferred.create<int, String>());
    final controller = CancellationController();
    final waiting = runtime.run(
      deferred.awaitExit(),
      signal: controller.signal,
    );

    controller.requestCancel(reason: 'stop observer');

    expect(
      _cause(await waiting),
      isA<CauseCancelled>().having(
        (cause) => cause.reason,
        'reason',
        'stop observer',
      ),
    );
    expect(deferred.poll(), isA<OptionNone<Exit<int, String>>>());
    expect(await runtime.runOrThrow(deferred.succeed(7)), isTrue);
    expect(await runtime.runOrThrow(deferred.join()), 7);
  });

  test('completion is visible before resumed continuations run', () async {
    final deferred = await runtime.runOrThrow(Deferred.create<int, String>());
    final observer = runtime.run(
      deferred.join().flatMap((_) => deferred.succeed(9)),
    );

    expect(await runtime.runOrThrow(deferred.succeed(7)), isTrue);

    expect(_value(await observer), isFalse);
    expect(await runtime.runOrThrow(deferred.join()), 7);
  });

  test(
    'a cancelled completion does not consume the first completion',
    () async {
      final deferred = await runtime.runOrThrow(Deferred.create<int, String>());
      final controller = CancellationController()..requestCancel();

      expect(
        _cause(
          await runtime.run(deferred.succeed(7), signal: controller.signal),
        ),
        isA<CauseCancelled>(),
      );
      expect(deferred.poll(), isA<OptionNone<Exit<int, String>>>());
      expect(await runtime.runOrThrow(deferred.succeed(9)), isTrue);
      expect(await runtime.runOrThrow(deferred.join()), 9);
    },
  );
}

T _value<T, E>(Exit<T, E> exit) => switch (exit) {
  ExitSuccess(:final value) => value,
  ExitFailure(:final cause) => fail('Expected success, got $cause.'),
};

Cause<E> _cause<T, E>(Exit<T, E> exit) => switch (exit) {
  ExitSuccess(:final value) => fail('Expected failure, got $value.'),
  ExitFailure(:final cause) => cause,
};
