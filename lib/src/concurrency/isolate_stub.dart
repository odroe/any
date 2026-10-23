import 'dart:async';

import 'package:any/any.dart';

Future<Exit<T, E>> executeIsolate<T, E, R>(
  FutureOr<Exit<T, E>> Function(R) computation,
  R dependencies, {
  String? debugName,
}) => throw UnsupportedError('isolateTask requires native Dart isolates.');
