# Runtime hardening issue ledger

## Batch 5F

- [x] Move accepted install and initial activation work under Runtime worker ownership.
- [x] Preserve dependency-restart activation ownership in the deduplicated reconciliation worker.
- [x] Add ordered, phase-attributed cleanup diagnostics and committed disposal outcomes.
- [x] Preserve activation primary errors alongside multiple rollback issues.
- [x] Add deterministic caller-drop and structured cleanup fault-injection coverage.
- [x] Pass final format, strict clippy, debug, all-features, and release validation.

## Batch 5F.1

- [x] Route ordinary activation commit errors through Runtime-owned rollback.
- [x] Prove shutdown-after-validation and dropped-observer commit failure convergence.
- [x] Preserve an incomplete lifecycle's terminal primary blocker alongside ordered cleanup issues.
- [x] Prevent Scope disposal commit while any descendant remains non-terminal.
- [x] Pass final format, strict clippy, debug, all-features, and release validation.

## Batch 5F.2

- [x] Publish legacy result and detailed terminal truth in one durable completion observation.
- [x] Remove post-completion Registry reads from Fiber, Scope, and activation cleanup observers.
- [x] Preserve registered detailed outcomes across deterministic GC-before-notify races.
- [x] Keep post-reclaim brand-new requests as NotFound without a global history registry.
- [x] Pass final format, strict clippy, debug, all-features, and release validation.

This ledger tracks the current Batch 4A.1 correctness closure and Batch 4B
registry extraction. A checked item is implemented and verified by the batch
quality gates; an unchecked item is still in progress.

## Batch 4A.1

- [x] Treat capability publication as a checked activation commit step.
- [x] Allocate a fresh capability gate and cancellation generation on dependency restart.
- [x] Keep retired generation gates permanently closed.
- [x] Release ordinary and HMR task barriers only after capability publication.
- [x] Use `AdmissionGate` as the operational shutdown admission authority.
- [x] Reject all context mutations after activation sealing.
- [x] Complete deterministic race and HMR publication-failure coverage.
- [x] Pass format, clippy, debug tests, and release tests.
- [x] Commit Batch 4A.1 independently.

## Batch 4B

- [x] Extract `ServiceRegistry`, including interning, epoch, and resolution cache.
- [x] Extract the symbol-only `DependencyGraph`.
- [x] Remove service and dependency maps from lifecycle `State`.
- [x] Add registry isolation and lock-boundary tests.
- [x] Update locking documentation and pass all quality gates.
- [x] Commit Batch 4B independently.

## Batch 4B.1

- [x] Prepare and validate all staged service replacements before mutation.
- [x] Commit a multi-service revision under one ServiceRegistry write lock.
- [x] Preserve old services and capability visibility on publication failure.
- [x] Keep staged tasks blocked on every failed pre-commit path.
- [x] Pass format, clippy, debug tests, and release tests.
- [x] Commit Batch 4B.1 independently.

## Batch 4C-S

- [x] Extract ScopeRegistry without extracting FiberRegistry.
- [x] Move ancestry and service hot paths off legacy Runtime State.
- [x] Move scope disposal, root handling, GC, and quota ownership.
- [x] Add deterministic topology, isolation, disposal, and churn tests.
- [x] Update lock documentation and pass all quality gates.
- [x] Commit Batch 4C-S independently.

## Batch 4C-GEN

- [x] Introduce stable logical PluginId, GenerationId, GenerationSelector, and PluginRegistry.
- [x] Bind every Fiber activation generation to a unique selector generation.
- [x] Unify Event and Invocation visibility on selector snapshots; remove Invocation active bool.
- [x] Move multi-Service HMR cutover to the shared selector CAS.
- [x] Make dependency restart allocate a fresh generation and old disposal CAS-safe.
- [x] Preserve live PluginSlot while any generation Fiber remains.
- [x] Complete deterministic generation/cutover/GC coverage and documentation.
- [x] Pass final format, clippy, debug, all-features, and release gates.
- [x] Commit final 4C-GEN closure independently.

## Batch 4C-F

- [x] Make GenerationSelector equality the sole reader visibility truth.
- [x] Detect PluginRegistry Fiber reference underflow with a typed invariant error.
- [x] Extract generation-safe `FiberRegistry` with short arena operations.
- [x] Move mutable lifecycle/resource ownership into per-Fiber `FiberCell` synchronization.
- [x] Move Context invoke ownership to `Weak<FiberCell>` and remove legacy State/arena hot-path lookup.
- [x] Linearize Fiber creation and Scope membership against scope disposal.
- [x] Make Fiber GC/Plugin detach exactly once and detach permanent Scope membership at disposal commit.
- [x] Add deterministic isolation, race, stale-context, accounting, and churn coverage.
- [x] Pass final format, clippy, debug, all-features, and release gates.
- [x] Commit final 4C-F closure independently.

## Batch 4C-F.1

- [x] Create restart GenerationId, CapabilityGate, and CancellationToken in one begin-activation critical section.
- [x] Keep the old cancelled token through dependency-disposal finalization.
- [x] Remove Scope-to-Fiber lock nesting from health, snapshot, GC, and dependency validation.
- [x] Snapshot unstarted-Fiber metadata before cross-registry rollback.
- [x] Add deterministic restart-cancellation and Scope/Fiber lock-order tests.
- [x] Pass format, strict clippy, debug, and release gates.
- [x] Commit 4C-F.1 independently.

## Batch 4D

- [x] Extract shutdown execution ownership and shared completion into `ShutdownCoordinator`.
- [x] Keep `AdmissionGate` as the only shutdown admission authority.
- [x] Preserve detached, caller-drop-safe, exactly-once shutdown cleanup.
- [x] Publish terminal lifecycle and cleanup result consistently without holding the coordinator across await.
- [x] Remove legacy Runtime `State` and `RuntimeInner.state`.
- [x] Verify get/invoke/emit do not consult the coordinator or a Runtime-wide data lock.
- [x] Add concurrent waiter, cleanup-once, caller-drop, admission, and coordinator lock-boundary coverage.

## Batch 5A

- [x] Replace lifetime task accounting with supervised live Fiber task ownership.
- [x] Reap normal completion and panic without waiting for Fiber disposal.
- [x] Linearize task tracking/membership before starting user futures.
- [x] Use one shared Fiber task shutdown deadline and abort remaining joins.
- [x] Add Runtime worker ownership, completion, panic/error diagnostics, and shutdown convergence.
- [x] Remove naked activation/rollback scheduling and coalesce keyed activation reconciliation with a dirty bit.
- [x] Expose bounded task/worker metrics and deterministic race/churn coverage.

## Batch 5A.1

- [x] Store stable Fiber/Scope attribution in each live task entry.
- [x] Make the arena remove winner own terminal metrics, diagnostics, and membership reconciliation.
- [x] Unify normal and shutdown Runtime worker terminal recording.
- [x] Preserve completed/panicked outcomes observed after the deadline instead of misclassifying them as aborted.

## Batch 5B

- [x] Add per-generation linearizable execution admission, inflight accounting, and irreversible draining.
- [x] Cover the complete Invocation middleware/handler chain with RAII generation leases.
- [x] Require a generation lease before every Event handler executes; stale snapshots skip execution.
- [x] Drain provider execution before handler/service/effect cleanup and preserve resources on timeout.
- [x] Support old-draining/new-active HMR and fresh execution state on dependency restart.
- [x] Expose active/draining generation and provider inflight metrics.

## Batch 5B.1

- [x] Acquire global invocation capacity before taking Scope/Invocation dispatch snapshots.
- [x] Revalidate caller lifecycle and cancellation after permit wait.
- [x] Ensure queued invocations own no provider generation lease.
- [x] Resolve the current generation after an HMR cutover while queued.
- [x] Document provider inflight as execution-lease count rather than logical invocation count.

## Batch 5C

- [x] Move reload execution into RuntimeWorkerSupervisor ownership.
- [x] Preserve rollback/finalize after the reload observer is dropped.
- [x] Add explicit prepared dependency and scope revisions around the selector commit point.
- [x] Keep pre-commit control-plane changes reversible and staged tasks blocked.
- [x] Make selector CAS irreversible and post-commit cleanup forward-only.
- [x] Distinguish pre-commit `ReloadFailed` from committed cleanup failure/pending outcomes.
- [x] Continue old-generation draining in the Runtime-owned reload worker after the observer deadline.
- [x] Expose reload and staging convergence counters.
- [x] Add deterministic caller-drop, cleanup-pending, cleanup-error, rollback, shutdown/dispose/double-reload races, and 200-round churn coverage.

## Batch 5C.1

- [x] Revalidate and atomically apply/rollback prepared Scope revisions under one writer.
- [x] Revalidate and atomically apply/rollback multi-symbol Dependency revisions under one writer.
- [x] Enforce reload transaction ownership for staged Fiber public lifecycle admission.
- [x] Prove pre-selector conflicts, atomic deltas, and privileged rollback with deterministic tests.

## Batch 5D

- [x] Capture immutable generation identity in Context and preserve it across clones.
- [x] Reject stale and draining Context Runtime interactions with a typed error.
- [x] Keep staged Context valid after its same-generation HMR commit.
- [x] Prove same-Fiber dependency restart invalidation and no recursive expansion while draining.
- [x] H-019 is closed by generation-tracked `ServiceHandle` lookup results; service-defined independent clones remain the service type's contract.

## Batch 5C.2

- [x] Fence target Scope lifecycle from final revision apply through selector CAS.
- [x] Roll Scope topology back before releasing the cutover fence on CAS failure.
- [x] Audit and document the one-way `ScopeRegistry -> ServiceRegistry` commit lock edge.
- [x] Prove disposal/cutover single-winner and snapshot ordering with deterministic barriers.

## Batch 5D.1

- [x] Co-locate current generation identity, exact gate, and cancellation in one FiberMutable snapshot.
- [x] Acquire a short non-cloneable ContextAdmission lease for every generation-sensitive Runtime interaction.
- [x] Linearize Context admission against generation draining on GenerationExecution.
- [x] Preserve invocation permit-before-caller/provider generation admission ordering.
- [x] Prove exact-gate restart races, resource handoff, recursive rejection, staged rollback, and churn convergence.

## Batch 5C.3

- [x] Acquire staged Fiber metadata ownership before the Scope cutover writer.
- [x] Finalize committed Fiber scope/staging/reload ownership inside the selector fence.
- [x] Extract activation barrier under the fence and signal it only after committed metadata is visible.
- [x] Preserve staging Fiber truth and fenced Scope rollback on selector failure.
- [x] Audit Fiber-to-Scope-to-Invocation/Service reverse edges and await boundaries.
- [x] Prove post-CAS disposal exclusion, relocated Service cleanup, and activation ordering.

## Batch 5D.2

- [x] Define Context immutable identity as FiberId plus GenerationId.
- [x] Capture current ScopeId in the coherent ContextAdmission snapshot.
- [x] Make `scope()` generation-validated and retain `initial_scope()` for diagnostics only.
- [x] Route Scope-sensitive service, invocation, task, quota, diagnostics, registration, and child-Scope operations through admission Scope.
- [x] Prove same-generation Context and clones follow the target Scope after commit.
- [x] Prove a retained Context works after full reload and actual staging-Scope reclamation.
- [x] Prove stale old-generation Context cannot follow a replacement generation Scope.

## Batch 5E

- [x] Replace bare-Arc get/try_get/get_symbol results with non-Clone generation-tracked ServiceHandle.
- [x] Bind service object and lease to one exact provider entry/generation snapshot.
- [x] Retry lookup once when exact provider admission loses to draining.
- [x] Keep old handles valid while blocking HMR/disposal/dependency-loss cleanup.
- [x] Prove last-handle drop wakes drain and new lookup selects the new generation.
- [x] Separate caller Context admission from provider ServiceHandle lifetime.
- [x] Support official trait-object registration/lookup without exposing raw Arc extraction.
- [x] Expose service-handle inflight diagnostics without changing total drain truth.
- [x] Prove 10,000 handle uses and 100 reloads converge without bookkeeping leaks.

## Final freeze closure

- [x] H-014: Context publication is declaration-first and lookup-only; full-capacity undeclared keys return typed validation errors without symbol growth.
- [x] H-020: GC registration and shutdown close share the AdmissionGate fence; deterministic tests cover both winners and forbid post-Complete GC workers.
- [x] H-022: Rust 1.85 check/tests, cargo audit, cargo deny, strict lint, release, docs, panic, unsafe, spawn, and await-under-lock audits passed.
- [x] H-001 through H-024 satisfy the Kernel v1 freeze gate.
