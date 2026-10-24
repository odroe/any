# AGENTS.md

## 1. Do not add defensive complexity without approval

Before adding complexity to address speculative or highly unlikely races or security-boundary concerns, explain the concrete failure mode, its likelihood in actual use, and the implementation and maintenance costs. Obtain the user's explicit approval before proceeding.

Do not silently expand the task with defensive checks, fallback paths, compatibility layers, configuration options, or extra state merely for theoretical robustness.

"Safer," "more robust," and "we might need it later" are not sufficient reasons. Identify the current path that would fail and explain why the existing contract cannot handle it.

Explicitly required input validation, access control, cancellation guarantees, and resource-release guarantees are not speculative hardening and must be implemented as requested.

## 2. Design from caller code first

Before changing a public API, show concrete Dart usage examples, including necessary initialization, composition, and cleanup.

Compare complete caller code, not just library internals. Saving a few lines inside the library while making callers maintain another wrapper is not simplification.

Prioritize fewer concepts to understand, fewer handwritten declarations, and less repetition. Do not burden ordinary application code with environment wrappers, manual dependency lists, or type parameters with no practical value merely to simplify implementation.

Validate the calling pattern before choosing abstractions. Do not build a framework first and look for use cases afterward.

## 3. Prefer existing mechanisms

First try to solve the problem with the existing Task, Fiber, Scope, Cause, Context, and Layer mechanisms.

Do not create a parallel cancellation protocol, resource-ownership model, failure-tracking system, or dependency graph for an adjacent feature. If a new mechanism is necessary, explain what the existing mechanisms lack, not just what the new one can do.

Reuse does not mean forcing different semantics together. Do not extract a generic framework from similar-looking code that has no shared contract.

## 4. Validate once at the responsible boundary

Validate unknown input at the boundary responsible for receiving it, then pass validated values internally.

Do not recheck conditions already guaranteed by types, constructors, or upstream contracts. Revalidate only when crossing a new trust boundary or when those guarantees no longer hold.

Do not hide errors behind default values, swallowed exceptions, empty results, or silent fallbacks. A fallback must be explicit product behavior, not a way to make the program appear not to have failed.

## 5. Keep implementation direct and readable

Make the normal path clear in the main flow. Put complex supporting logic in small nearby functions when useful, but do not add layers of forwarding merely for the sake of layering.

Extract a helper only when it provides actual reuse, isolates a complex boundary, or gives the logic a name that materially improves understanding. Single use is neither a requirement nor a prohibition on extraction.

Comments should explain non-obvious reasons, constraints, and tradeoffs rather than restate the code.

When replacing a design, migrate callers and remove obsolete state and paths. Do not retain two implementations by default unless compatibility is an explicit requirement.

## 6. Do not trade runtime semantics for shorter code

Cancellation, waiting, execution ownership, and resource borrowing are distinct concepts. Handle each according to its own contract.

Cancelling a waiter must not cancel its producer without authorization from the contract. Do not release a resource while its owning task or borrowers may still access it.

Completion must include the cleanup required by the contract. Follow the specific operation's contract for propagating, recovering from, ignoring, or reporting business failures, defects, cancellation, and cleanup failures.

Do not silently change results in the name of safety. Do not lose failures that must be preserved or report the same failure more than once.

Simplify internal bookkeeping, not correctness guarantees.

## 7. Verify behavior without piling up proof

Tests should cover caller-observable behavior and the runtime guarantees affected by the change.

Control ordering explicitly in concurrency tests. Do not rely on incidental scheduling or arbitrary sleeps.

Passing tests do not justify an abstraction or replace evaluation of its calling pattern. Do not add repetitive tests around implementation details just to increase the test count.

When changing an API, update affected Dartdoc, examples, exports, and callers.

Run the existing checks before delivery:

- `dart pub get`
- `dart format --output=none --set-exit-if-changed .`
- `dart analyze --fatal-infos`
- `dart test`

Clearly distinguish passed, failed, and unrun checks. Do not present planned verification as a result.

## 8. Do not expand the task without approval

Read the relevant code and issues first. Distinguish agreed design from implemented behavior. Point out contradictions rather than guessing and rewriting the contract.

Solve the current request. Report adjacent issues when useful, but do not casually refactor, expand features, or introduce compatibility measures for hypothetical future needs.

Keep delivery notes brief: what changed, why, what was verified, and what limitations remain. Avoid self-praise, lengthy implementation logs, and unrelated proof.
