# Cordis Runtime locking rules

## Lifecycle operation ownership

Install acceptance is linearized by creation of the supervised `Install`
worker after admission. Initial activation is owned by that worker; dependency
recovery is owned by the deduplicated `DependencyReconcile` worker. Reload and
shutdown retain their transaction/coordinator supervisors. Fiber and Scope
disposal bodies publish shared completion records, so callers are observers.

The per-Fiber async lifecycle mutex remains the unique
activation/reload/dispose serializer. No `FiberCell::inner` or Registry guard
crosses an await boundary. Dedicated Fiber/Scope body-supervisor pairs are
operation-owned and publish durable completion; they are not untracked tasks.

Ordinary activation performs every fallible check while holding the lifecycle
operation and before capability publication. Publication is its commit point.
Any earlier error releases the operation guard and enters the shared
Runtime-owned failure convergence path; no `?` crosses that boundary. After
publication, state finalization and task-barrier release are infallible and
forward-only.

Disposal finalization updates terminal Fiber/Scope state and constructs the
immutable completion observation while holding only the object's short
synchronous guard. The guard is released before notification. Registered
waiters own completion Arcs, so Registry reclamation cannot rewrite a published
outcome; absence only affects brand-new requests that did not join that
operation.

## Lock domains

| Domain | Protected data | Primitive | Across `.await` | Callback rule |
| --- | --- | --- | --- | --- |
| `AdmissionGate` | shutdown admission state | atomic + `parking_lot::RwLock` | never | no callbacks |
| `ShutdownCoordinator` | shutdown execution ownership, shared completion, terminal cleanup result | `parking_lot::Mutex` | never | its start callback may only close admission and spawn Runtime-owned tasks |
| `FiberRegistry` | `FiberId -> Arc<FiberCell>` arena membership | `parking_lot::RwLock` | never | lookup/insert/remove only; no lifecycle work |
| `FiberCell::inner` | one Fiber's coherent lifecycle state and resource ownership ledger | `parking_lot::RwLock` | never | Context admission and diagnostics take short read guards; lifecycle/ownership mutation takes a write guard |
| `FiberCell::lifecycle` | one Fiber's async lifecycle body serialization | Tokio mutex | yes | no synchronous guard may accompany it across await |
| `PluginRegistry` | logical Plugin slots, monotonic generation allocation, selectors, Fiber reference counts | `parking_lot::RwLock` | never | allocate/lookup then release; no registry callbacks |
| `GenerationSelector` | active runtime generation (`0` means none) | `AtomicU64` | not applicable | CAS is the logical data-plane cutover point |
| `GenerationExecution` | one generation's execution admission, inflight count, draining state | packed `AtomicUsize` + diagnostic `AtomicUsize` + Tokio `Notify` | drain wait may | no Runtime registry callbacks during admission or drain |
| `ScopeRegistry` | scope arena, parent/children, membership, lifecycle/disposal metadata | `parking_lot::RwLock` | never | never calls another Runtime registry while guarded |
| `ServiceRegistry` | service entries, interner, cache, epoch, HMR service transaction | `parking_lot::RwLock` | never | never calls `ScopeRegistry`; ancestry is passed as an owned snapshot |
| `DependencyGraph` | symbol provider/dependent indexes and cycle validation | `parking_lot::RwLock` | never | no interning or registry callbacks while guarded |
| `EventBus` | handler slots and immutable snapshots | registry-local locks + `ArcSwap` | guards never cross; snapshots may | user futures run from snapshots only |
| `InvocationRegistry` | handler/middleware registrations | `parking_lot::Mutex` | never | invocation futures run from snapshots only |
| `TaskSupervisor::arena` | live Fiber task ownership, cancellation tokens, join handles | `parking_lot::Mutex` | never | task starts only after supervisor tracking and Fiber membership attach |
| `RuntimeWorkerSupervisor` | live Runtime worker ownership and join handles | `parking_lot::Mutex` | never | normal and cleanup workers are tracked before execution |
| `CapabilityGate` | immutable selector/generation binding and one-way retirement | atomic | not applicable | no callbacks; visibility is selector equality |

## Ordering and cross-domain operations

Admission is acquired first. FiberRegistry arena lookup clones an `Arc<FiberCell>`
and releases the arena lock before Fiber-local synchronization. Registry guards
and `FiberCell::inner` are released before user code or `.await`.

Cross-domain work follows `lock -> snapshot/prepare -> unlock -> next domain`.
Fiber creation is the intentional transaction boundary: it holds the ScopeRegistry
write lock while validating an Active scope, inserting the Fiber, and attaching
membership, so scope disposal and creation have one winner. Long chains such as
Fiber -> Scope -> Plugin -> Service are forbidden.

There is no `ScopeRegistry -> FiberCell::inner` path. The only retained
Scope/Fiber nesting is the short `FiberCell::inner -> ScopeRegistry` transaction
in `Context::create_scope`; it calls no other registry and never crosses await.
Health, snapshot, GC, dependency validation, and unstarted-Fiber rollback build
independent domain snapshots before entering the next lock domain.

Hot-path service resolution follows a stricter snapshot boundary:

1. `ScopeRegistry::ancestry` creates an owned `Vec<ScopeId>` and releases its lock.
2. `ServiceRegistry` resolves against that snapshot.

`ServiceRegistry` never calls `ScopeRegistry`. `DependencyGraph` receives
`ServiceSymbol` values and never interns while holding its lock. Scope disposal
snapshots child Scope IDs and Fiber IDs, releases the Scope lock, then the
Runtime orchestrator awaits descendant cleanup.

PluginRegistry locks are not held while preparing Service, Event, Invocation,
or Scope metadata and never cross `.await`. GenerationSelector CAS uses
Acquire/Release ordering: successful release publishes prepared metadata, and
acquire loads observe the winning generation before selecting capabilities.

Shutdown execution ownership is selected under `ShutdownCoordinator`, then all
Fiber/Scope cleanup runs after releasing that mutex. Final result publication and
the terminal coordinator state are committed together. `AdmissionGate` remains
the sole authority for accepting new work; the coordinator has no admission API.

Task completion uses the TaskSupervisor arena remove as its exactly-once winner.
The supervisor guard is released before detaching `FiberCell::inner`. Spawn is the
only short `FiberCell::inner -> TaskSupervisor` transaction: quota check, supervisor
reservation, Fiber membership attach, then start-barrier release. The reverse
`TaskSupervisor -> FiberCell::inner` nesting is forbidden. Fiber disposal takes its
task membership, releases the Fiber lock, removes supervisor winners, broadcasts
cancellation, and joins concurrently against one absolute deadline. Runtime worker
completion and shutdown use the same remove/snapshot-before-await rule.

Context hot paths do not use `ShutdownCoordinator`. `get` snapshots Scope ancestry then
resolves through ServiceRegistry, clones the exact entry value/gate/provider metadata,
releases the Registry lock, and acquires a provider service-use lease; `emit` enters AdmissionGate then uses EventBus;
`invoke` upgrades its `Weak<FiberCell>`, takes a Fiber-local cancellation/state
snapshot, and releases the Fiber lock before waiting for global invocation capacity.
After acquiring that permit it revalidates the caller, then takes fresh Scope ancestry
and Invocation snapshots. A queued invocation therefore owns no provider generation
lease and cannot pin an old generation during HMR.

Context is an immutable FiberId plus GenerationId identity, not execution ownership.
The current ScopeId, exact CapabilityGate, lifecycle state, and, only for a
cancellable operation, CancellationToken are captured with GenerationId from one
short `FiberCell::inner` read snapshot. Scope is
a generation-bound Runtime binding rather than frozen Context identity. The Fiber guard is released before
`CapabilityGate::try_acquire`; that method and `begin_draining` compete through the same
packed GenerationExecution state/inflight CAS. A successful non-cloneable `ContextAdmission` holds the
short GenerationLease for one Runtime interaction only.

The admission path is `Weak<FiberCell>::upgrade -> FiberCell::inner read snapshot ->
clone exact gate -> release Fiber guard -> exact gate try_acquire -> Registry
interaction`. It never
re-reads the current gate, and there is no `GenerationExecution -> FiberCell::inner`
edge because admission calls no Fiber operation before returning the
lease. Registry locks and Fiber guards never cross user await; only GenerationLease
may do so. Invocation waits for global capacity before caller Context admission, so
a queued call pins neither caller nor provider generation. `fiber()` and
`generation()` are immutable identity accessors; `scope()` performs generation
admission and returns the current matching Fiber binding. `initial_scope()` is
diagnostic only and is never used for Runtime operations.

## HMR publication

All fallible Fiber, scope, invocation, service ownership, and capability-state
validation occurs before cutover. The ServiceRegistry then holds one write lock
while it performs `selector.compare_exchange(old_generation, new_generation)`,
replaces the entire prepared service set, increments the epoch, and clears the cache.
Service readers therefore observe either the complete old set or the complete
new set, never a partial replacement or temporary miss.

Event and Invocation snapshots use the same selector generation. They cache one
selector value per dispatch snapshot, preventing a concurrent CAS from selecting
both old and new entries within one operation. Invocation metadata and scope topology are prepared before this service
cutover. The Starting task barrier is released only after cutover succeeds.
Pre-commit failure leaves the old capability and old service entries untouched;
staged disposal removes only staging-scope entries.

## Generation execution draining

`GenerationSelector` remains the only visibility truth. After selection,
`GenerationExecution::try_acquire` linearizes execution admission and inflight
increment in one packed-word CAS. `begin_draining` closes admission with a CAS on
that same state/inflight word and is irreversible. Every successful acquire returns
a non-cloneable RAII `GenerationLease`; dropping the last lease atomically transitions
Draining/1 to Drained/0 and wakes waiters. The packed word is truth; `Notify` is only
a wake mechanism. Drain registers its notification future before rechecking the
atomic word, preventing lost wakeups.

Normal accepting-state acquire/drop takes no mutex. `service_handle_inflight` is
an exactly-once diagnostic atomic subset of packed inflight. Service acquisition
increments packed inflight before the subset counter; drop decrements the subset
before publishing packed inflight reduction. Snapshot decodes state and inflight
from one Acquire load and is coherent. GenerationExecution is cache-line aligned
so independently allocated generations do not regress through false sharing.

Invocation snapshots acquire leases before releasing execution to middleware and
handlers. The leases cover the complete chain and drop on success, error, panic,
timeout, or caller cancellation. Event dispatch retains `VisibilityCache`, then
acquires one lease per handler immediately before user code; a stale handler that
loses admission is skipped. No GenerationLease holds ServiceRegistry, EventBus,
InvocationRegistry, FiberCell, or ScopeRegistry guards across await.

`provider_inflight` retains its compatibility name but counts all active generation
execution leases, including short ContextAdmission leases and provider execution
leases and live `ServiceHandle` values, not logical invocation requests. A single invocation may therefore
contribute a caller admission plus several provider/middleware leases.
`service_handle_inflight` is a diagnostic subset of this same total; draining still
depends only on total inflight.

Service lookup never holds ServiceRegistry across `GenerationExecution::try_acquire`,
await, or user code. A resolved entry snapshots one service `Arc`, exact provider
CapabilityGate, FiberId, generation, symbol, and resolved Scope. If admission loses
to draining, lookup performs one fresh resolution attempt. A successful non-Clone
`ServiceHandle<T>` owns the exact provider lease and dereferences without locks. It
holds neither Registry nor Fiber guards and exposes no raw-Arc extraction API.

## Reload transaction ownership

`ReloadTxn` is represented by the Runtime-owned reload operation and its prepared
domain revisions; it is ownership/state, not a mutex held across the whole reload.
The caller observes a oneshot outcome and may disappear without cancelling work.
The worker alone owns rollback before commit and forward finalization after commit.

Prepared control-plane revisions are not authority. Scope and Dependency commit
must revalidate the complete expected world state under the same registry writer
that applies the complete revision. Their inverse rollback follows the same rule;
it reports a conflict instead of overwriting an unexpected concurrent mutation.

A reload-staged Fiber is lifecycle-owned by its ReloadTxn until rollback begins or
the generation selector commits. Public dispose and reload admission reject that
Fiber; Runtime-owned rollback and shutdown cleanup use the narrow privileged path.

The reload cutover has one explicitly allowed short chain:
`staged FiberCell.inner -> ScopeRegistry write -> InvocationRegistry/ServiceRegistry write`.
The staged Fiber guard is acquired first, preserving the existing Fiber-to-Scope
direction. The Scope writer covers final Scope revalidation, the complete topology
delta, selector CAS, committed Fiber metadata (`scope`, `staged`, `reload_owned`,
activation state), and (on CAS failure) the complete inverse Scope delta. Scope
disposal needs the same Scope writer to perform
`Active -> Disposing`, so disposal and publication have one winner. ServiceRegistry
and InvocationRegistry never call ScopeRegistry or FiberCell; ScopeRegistry never
locks FiberCell. A workspace audit found no reverse edge for the chain.
This synchronous fence crosses no await; Dependency rollback runs after the Scope
inverse delta and fence release.

Preparation visits one domain at a time and releases each registry guard before the
next domain or any await. Dependency and Scope revisions carry immutable expected
ownership and reversible deltas. Invocation metadata is staged behind the generation
gate. `PreparedServiceRevision` revalidates and performs the selector CAS under its
short ServiceRegistry writer.

Before selector CAS, rollback restores prepared Dependency, Scope, and Invocation
metadata and disposes the hidden Fiber/Scope. After CAS, selector rollback is forbidden.
The new task barrier is released only after forward control-plane finalization. The
same supervised worker then observes persistent old-Fiber disposal; if the public
cleanup deadline expires it publishes cleanup-pending and continues without holding
Service, Invocation, Scope, Fiber, or Dependency guards across the drain wait.
