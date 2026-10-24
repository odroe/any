// A root's initial synchronous drive shares this mode with its child work.
// Restoring automatic yielding also applies to continuations already suspended.
final class ExecutionMode {
  ExecutionMode({this.automaticYield = true});

  bool automaticYield;
}
