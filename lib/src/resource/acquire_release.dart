import 'dart:async';

import '../../any.dart';

/// A task that acquires a resource and releases it when its scope closes.
///
/// Each run calls [acquire] with its execution context. An [ExitSuccess]
/// registers its value for [release] before cancellation is checked again.
/// An [ExitFailure] is preserved without calling [release]; thrown objects
/// become [CauseDefect] values. If acquisition fails or throws, [acquire] is
/// responsible for cleaning up any partially acquired resource.
///
/// [release] receives the resource and the scope's closing exit. It runs once
/// per successful acquisition, in reverse registration order alongside other
/// finalizers. Its returned future is awaited. Thrown objects become cleanup
/// defects and are combined with any existing failure by [Task.run].
///
/// Use the resource within [TaskComposition.map] or [TaskComposition.flatMap].
/// [Task.run] releases it before returning, even if the task returns the resource
/// itself. [Task.runIn] leaves cleanup to the owner of the supplied scope.
///
/// Cancellation before acquisition starts skips both callbacks. A running
/// [acquire] can observe [Context.cancellation] and is still awaited. If it
/// succeeds after a cancellation request, its resource is registered for cleanup.
/// Cancellation does not interrupt [release].
///
/// The scope's owner must await running tasks before calling [Scope.close].
/// If the scope closes during acquisition, the resource is released immediately
/// with an [ExitFailure] containing a [CauseDefect] for the registration
/// [StateError]. The task fails with that defect, combined with a cleanup defect
/// if [release] also throws.
///
/// ```dart
/// import 'dart:io';
///
/// import 'package:any/any.dart';
/// import 'package:any/resource.dart';
///
/// final length = acquireRelease<RandomAccessFile, Never, File>(
///   acquire: (context) async => Exit.success(await context.dependencies.open()),
///   release: (file, _) async {
///     await file.close();
///   },
/// ).flatMap(
///   (file) => Task<int, Never, File>(
///     (_) async => Exit.success(await file.length()),
///   ),
/// );
///
/// // The file stays open while reading its length and closes before completion.
/// final exit = await length.run(File('example.txt'));
/// ```
Task<T, E, R> acquireRelease<T, E, R>({
  required FutureOr<Exit<T, E>> Function(Context<R> context) acquire,
  required FutureOr<void> Function(T value, Exit<void, Object?> exit) release,
}) => Task((context) async {
  final acquired = acquire(context);
  final exit = acquired is Future<Exit<T, E>> ? await acquired : acquired;
  if (exit case ExitSuccess(:final value)) {
    try {
      context.scope.addFinalizer((exit) => release(value, exit));
    } catch (error, stackTrace) {
      final cause = Cause<E>.defect(error, stackTrace);
      final failure = Exit<T, E>.failure(cause);
      // Ownership cannot pass to a closed scope, so release before returning.
      try {
        await release(value, failure);
      } catch (error, stackTrace) {
        return Exit.failure(
          Cause.combine([cause, Cause.defect(error, stackTrace)]),
        );
      }
      return failure;
    }
  }
  return exit;
});
