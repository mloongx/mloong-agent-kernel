# Native and hosted executable conformance plan

B1 should turn public, deterministic rows of `CONTRACT_MATRIX.md` into a shared
semantic oracle. It must not expose registry cells or create an eighty-method
`ContractRuntime` trait.

## Capability groups

1. `LifecycleDriver`: install acceptance, activation visibility, caller-drop and
   failure rollback.
2. `ScopeDriver`: tree creation, inheritance, disposal ownership, GC/reclaim and
   stale-ID behavior.
3. `ServiceDriver`: key identity, nearest-provider resolution, exact-generation
   ServiceHandle pin and drain behavior.
4. `InvocationDriver`: resolution, middleware order, snapshot isolation,
   cancellation, timeout, panic and queued admission.
5. `EventDriver`: ephemeral modes, ordering and short-circuit semantics.
6. `OwnedResourceDriver`: tasks, timers, Effect order, panic and cancellation.
7. `ReloadDriver`: hidden staging, pre-commit rollback, cutover, accepted-old-work
   drain and post-commit cleanup truth.
8. `DisposalDriver`: first-poll registration, shared immutable completion,
   incomplete versus committed truth and GC survival.
9. `ShutdownDriver`: admission close, shared attempts, blockers, retry and terminal
   completion.
10. `HostDriver`: the Host-relevant subset defined by B2.0, implemented in
    B2.1/B2.2 after the required typed Host-error API delta.

The Native Runtime is the first target. A Process Host-backed plugin should later
run the Host-relevant subset against the same expected outcomes. Internal-only
selector, gate, registry and coherent-snapshot tests remain in the Kernel invariant
suite; B1 must not publish private implementation merely to observe them.

## Initial readiness

Most public lifecycle, Service, Invocation, Event, task, Effect, HMR, disposal and
shutdown contracts already have deterministic tests and are ready to extract.
Highest-priority missing tests/design work are:

1. ServiceSymbol Runtime-locality remains a caller obligation: the unbranded token
   can numerically alias a slot in another Runtime, so no cross-Runtime rejection
   guarantee or negative conformance test exists.
2. A public conformance fixture that proves Context weak ownership after GC without
   private registry inspection.
3. Stable domain-error class assertions that do not freeze incidental diagnostic
   strings.
4. Host authority negative tests once a Host protocol exists.
5. External invocation format/bytes round trip, unknown format, size limit,
   cancellation and transport-error tests after wire rules are specified.

B1 Native conformance is complete. B2.0 now contracts protocol negotiation,
correlation, domain/Host errors, deadline, cancellation, crash/disconnect,
authority, limits, HMR, and shutdown. B2.1 may implement the reference
ProcessHost after adding the typed Host-error public model identified in
`HOST_ERROR_MODEL.md`; remote Service and Event remain explicitly deferred.

The complete hosted matrix and its contract mapping are maintained in
[B2 ProcessHost conformance plan](B2_PROCESS_HOST_CONFORMANCE_PLAN.md).

## Kernel to AgentLayer stable surface

AgentLayer may rely on Scope/Fiber lifecycle outcomes, Runtime-owned convergence,
ServiceKey identity and nearest-provider resolution, exact-generation
ServiceHandle lifetime, InvocationKey and invocation ordering/error semantics,
ephemeral Event modes, owned task/Effect lifecycle, Context cancellation/staleness,
HMR pre/post-commit generation behavior, structured disposal/shutdown outcomes and
public health/snapshot observations.

AgentLayer must not depend on lock types, registry structures, FiberMutable,
GenerationExecution representation, selectors, cache behavior/capacity,
ServiceSymbol persistence, local ID bit patterns, GC algorithm, worker scheduling,
Rust Arc layout or benchmark numbers.

## Contract conflict audit

| Concern | Source says | Existing docs say | Tests say | Classification | Resolution |
| --- | --- | --- | --- | --- | --- |
| `Context::scope()` | performs generation/lifecycle/gate admission and returns current committed Scope | `HARDENING.md` called `scope()` generation-neutral; `LOCKING.md` says it performs admission | `context_scope_tracks_same_generation_hmr_commit`, `stale_old_generation_context_cannot_follow_new_generation_scope` | DOC STALE | Correct HARDENING wording; contract is CTX-003/004. |
| PluginHost remote support | only artifact-to-lifecycle-proxy trait exists | ARCHITECTURE describes the authority rule for an implementation even in another process | no remote test/transport exists | AMBIGUOUS OVERSTATEMENT | Keep authority rule as HOST contract; explicitly classify current remote execution support as absent. |
| “durable” completion | immutable in-process Runtime-owned result | several docs say durable completion | GC/caller-drop tests prove in-process survival only | AMBIGUOUS TERM | Define durability boundary explicitly; no process-crash persistence. |
| ServiceValue::External | opaque string, rejected by native typed lookup, no transport producer | core doc calls it host-neutral/future bridge | no remote test | PLACEHOLDER | Do not treat as wire service reference. |
| InvocationMetadata | contains local ScopeId/FiberId | described only as non-payload metadata | local invocation tests only | TEST GAP | Prohibit wholesale remote authority serialization; design separate wire envelope later. |

No production-code violation was found. Other audited truth-source, shutdown,
reload commit, disposal-observer, Effect and admission-order wording agrees with
source and deterministic tests.
