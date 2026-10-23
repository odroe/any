import '../clock.dart';
import '../exit.dart';
import '../random_source.dart';
import '../schedule.dart';
import '../task.dart';

Task<void, Never> sleepTask(Duration duration) =>
    Task.service(Clock.key).flatMap((clock) => _sleep(clock, duration));

Task<T, E> retryTask<T, E>(
  Task<T, E> task,
  Schedule schedule, {
  bool Function(E error)? when,
}) => _withPolicy<T, E>((nextDelay, wait) {
  Task<T, E> attempt() => task.scoped().recover(
    (error) => Task.defer(() {
      if (when != null && !when(error)) return Task.fail(error);
      final delay = nextDelay();
      if (delay == null) return Task.fail(error);
      return delay == Duration.zero
          ? attempt()
          : wait(delay).withError<E>().flatMap((_) => attempt());
    }),
  );
  return attempt();
}, schedule);

Task<T, E> repeatTask<T, E>(
  Task<T, E> task,
  Schedule schedule, {
  bool Function(T value)? until,
}) => _withPolicy<T, E>((nextDelay, wait) {
  Task<T, E> attempt() => task.scoped().flatMap(
    (value) => Task.defer(() {
      if (until != null && until(value)) return Task.succeed(value);
      final delay = nextDelay();
      if (delay == null) return Task.succeed(value);
      return delay == Duration.zero
          ? attempt()
          : wait(delay).withError<E>().flatMap((_) => attempt());
    }),
  );
  return attempt();
}, schedule);

Task<T, E> _withPolicy<T, E>(
  Task<T, E> Function(
    Duration? Function() nextDelay,
    Task<void, Never> Function(Duration) wait,
  )
  create,
  Schedule schedule,
) => Task.service(Clock.key).withError<E>().flatMap(
  (clock) => Task.service(RandomSource.key).withError<E>().flatMap(
    (random) => Task.defer(() {
      final started = clock.elapsed;
      var iteration = 0;
      return create(
        () => scheduleDelay(
          schedule,
          iteration: iteration++,
          elapsed: clock.elapsed - started,
          random: random,
        ),
        (duration) => _sleep(clock, duration),
      );
    }),
  ),
);

Task<void, Never> _sleep(Clock clock, Duration duration) => Task.fromCallback(
  (complete) =>
      clock.schedule(duration, () => complete(const Exit.success(null))),
);
