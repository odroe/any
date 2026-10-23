import 'dart:async';
import 'dart:isolate';

import 'package:any/any.dart';

// Keep the spawning closure outside the task callback so it cannot capture
// that callback's Context, including its unsendable cancellation listeners.
Future<Exit<T, E>> executeIsolate<T, E, R>(
  FutureOr<Exit<T, E>> Function(R) computation,
  R dependencies, {
  String? debugName,
}) => Isolate.run(() => computation(dependencies), debugName: debugName);
