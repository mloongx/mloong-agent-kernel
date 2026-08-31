# V2 Phase 1.3: FiberCell admission design characterization

## Decision

**Phase 1.3 did not admit Optimization 3.** `FiberCell::inner` remains a
very-high-confidence shared-caller hotspot. The full admission-like
`parking_lot::RwLock` shadow did not scale sufficiently, but it also cloned a
shared gate Arc and CancellationToken on every operation. Phase 1.4 therefore
narrows the attribution: the experiment is valid for the complete snapshot but
does not independently attribute its remaining collapse to RwLock reader
synchronization. No raw Phase 1.3 measurement changes.

## Current layout and closure sanity

At HEAD `6cf2b0c`, `GenerationExecution` is 64 bytes with 64-byte alignment, so
10,000 objects occupy 640,000 bytes (625 KiB), excluding allocator/container
overhead. `FiberCell` is 432 bytes/alignment 8 and `FiberMutable` is 408
bytes/alignment 8 on the characterized x86-64 Windows target.

The source-defined `FiberCell` contains `plugin_id`, the async lifecycle mutex,
and `inner: parking_lot::Mutex<FiberMutable>`. `FiberMutable` contains `scope`,
`descriptor`, `state`, `plugin`, `effects`, `tasks`, `handlers`,
`invocation_handlers`, `invocation_middleware`, `provided`, `child_scopes`,
`cancellation`, `activation`, `activation_sealed`, `capabilities`, `generation`,
`staged`, `reload_owned`, and `disposal`.

The Context admission hot read set is only `generation`, `state`, `capabilities`,
`scope`, and `cancellation`. Admission copies the scalar IDs/state, clones one
`Arc<CapabilityGate>`, clones one `CancellationToken`, drops the Fiber guard,
and only then attempts exact-generation gate admission. The remaining ownership
vectors and lifecycle metadata are cold for this path but share its lock domain.

## Production access audit

The audit found 57 production `FiberCell::inner` acquisitions. Test sites and
the unrelated `ShutdownCoordinator::inner` mutex are excluded.

| Area | Sites | Read-only | Write | Mixed/conditional | Fields or purpose | Temperature |
|---|---:|---:|---:|---:|---|---|
| `context.rs` | 10 | 3 | 6 | 1 | admission snapshot; lifecycle validation; resources/tasks/children | admission reads hot; mutations data/control plane |
| `fiber_registry.rs` | 2 | 1 | 1 | 0 | generic `with`/`with_mut` | control plane |
| `task.rs` | 1 | 0 | 1 | 0 | detach owned task | data-plane completion write |
| `runtime/dependency.rs` | 2 | 2 | 0 | 0 | scope/descriptor/state graph snapshots | control plane read |
| `runtime/shutdown.rs` | 3 | 3 | 0 | 0 | state/staged/blocker inspection | diagnostic read |
| `runtime/fiber.rs` | 17 | 9 | 7 | 1 | activation, reload, validation, relocation, transitions | lifecycle/control plane |
| `runtime/snapshot.rs` | 16 | 16 | 0 | 0 | health and diagnostic snapshots | diagnostic read |
| `runtime/disposal.rs` | 6 | 0 | 5 | 1 | disposal state, resource extraction, completion | lifecycle write |
| **Total** | **57** | **34** | **20** | **3** | | |

The single high-frequency pure-read operation is Context admission. Most other
reads are control-plane or diagnostic. Writes are predominantly rare lifecycle
mutation: FiberState transitions; activation/reactivation metadata; generation,
gate and cancellation replacement; reload Scope relocation; ownership-ledger
mutation; and disposal bookkeeping. Task detach and Context resource
registration are the notable data-plane writes.

The critical field mutations are source-local and auditable: reactivation
replaces cancellation, generation and gate and sets Starting under one guard;
reload commit assigns `staged_record.scope = target_scope` while holding the
Fiber guard across its existing transaction fence; activation/disposal change
state under that same domain. Capability publication/retirement remains tied to
the exact gate selected from this record.

## Admission coherence contract

The five hot fields require one coherent Fiber snapshot.

- Generation and gate must identify the same generation; `N+1` with gate `N`
  could admit obsolete execution after reactivation.
- State and gate must describe the same lifecycle publication; Active cannot be
  combined with an incompatible or still-staged gate.
- Scope and generation must be coherent so future Context admission observes a
  committed same-generation reload relocation without fabricating a
  cross-generation binding.
- Cancellation and generation must be coherent so a new generation never
  inherits the old generation's cancellation token.

The frozen order is Weak owner upgrade, coherent Fiber snapshot, exact
generation comparison, lifecycle-state check, exact gate clone, current Scope
capture, current cancellation clone, Fiber guard release, exact gate lease
acquisition, then the existing stale-generation result if gate admission loses.
No synchronous guard crosses an await.

## Current production baseline

Release-mode points are ten-run medians in M snapshots/s. Ranges are min-max.

| Workers | Shared FiberCell | Independent FiberCell | Shared Context | Independent Context |
|---:|---:|---:|---:|---:|
| 1 | 18.795 (16.597-20.312) | 18.712 (16.989-19.959) | 10.839 (10.615-11.181) | 10.750 (10.269-11.483) |
| 2 | 10.534 (9.594-12.628) | 37.170 (36.183-37.794) | 4.948 (4.244-6.157) | 20.859 (19.784-21.944) |
| 4 | 5.614 (4.584-6.167) | 71.400 (63.586-73.984) | 3.398 (3.157-4.248) | 39.719 (28.282-41.922) |
| 8 | 2.498 (2.338-2.746) | 100.718 (74.480-118.382) | 1.974 (1.855-2.060) | 49.552 (41.617-60.925) |
| 16 | 1.552 (1.419-1.726) | 103.345 (97.202-116.687) | 1.189 (1.145-1.274) | 59.993 (55.289-68.198) |

Shared FiberCell 8/4 is 0.445 and 16/4 is 0.276. The independent control is
17.9x/66.6x faster at 8/16 workers, confirming that the hotspot remains.

The five-Symbol-access realistic workload is 0.740/0.652/0.439/0.390 M logical
ops/s for a shared caller and 0.724/1.074/1.129/1.096 M logical ops/s for
independent callers at 1/4/8/16 workers. These are the locked future before
points.

## Mutex and RwLock shadow model

The test-only payload contains scalar Scope/state/generation fields, an `Arc`
gate surrogate, and a real `CancellationToken`. Both variants perform identical
comparison/copy/clone work; only the parking_lot lock changes. Values are
ten-run median M snapshots/s.

| Workers | Shared Mutex | Shared RwLock | Independent Mutex | Independent RwLock |
|---:|---:|---:|---:|---:|
| 1 | 17.549 | 16.890 | 17.835 | 16.897 |
| 2 | 10.599 | 6.355 | 32.436 | 32.058 |
| 4 | 3.263 | 3.225 | 24.778 | 24.317 |
| 8 | 2.345 | 2.623 | 43.062 | 39.206 |
| 16 | 1.540 | 2.676 | 59.489 | 54.656 |

Shared RwLock single-reader throughput is 3.8% below Mutex. Its 8/4 and 16/4
ratios are 0.813 and 0.830 versus Mutex 0.719 and 0.472. Although the 16-worker
tail is better, absolute throughput remains only 2.68 M/s and the full snapshot
does not recover reader scaling. This proves that RwLock alone is insufficient,
not that reader bookkeeping is the residual cause. Run-to-run ranges were noisy
(14.6-62.4% for shared RwLock), but the complete-snapshot result is unambiguous.

## RwLock reader/writer characterization

Writer latency includes acquisition, coherent replacement of all five shadow
fields, and release. The paced cases ran for three seconds; the continuous
starvation cases ran for 30 seconds.

| Readers | Writer | Reader M/s | Writes | P50 | P95 | P99 | Max |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 8 | 1 Hz | 2.712 | 3 | 27.4 us | 64.7 us | 64.7 us | 64.7 us |
| 8 | 10 Hz | 3.173 | 30 | 33.5 us | 113.5 us | 854.5 us | 854.5 us |
| 8 | 100 Hz | 2.555 | 288 | 25.2 us | 71.4 us | 211.2 us | 971.8 us |
| 16 | 1 Hz | 2.783 | 3 | 82.1 us | 119.9 us | 119.9 us | 119.9 us |
| 16 | 10 Hz | 2.700 | 30 | 71.7 us | 225.4 us | 315.1 us | 315.1 us |
| 16 | 100 Hz | 2.993 | 282 | 68.4 us | 211.1 us | 1.019 ms | 1.478 ms |
| 8 | continuous | 1.839 | 44,249,621 | 0.2 us | 0.6 us | 2.7 us | 6.76 ms |
| 16 | continuous | 2.779 | 8,871,272 | 0.2 us | 0.6 us | 1.1 us | 15.99 ms |

Writers make sustained progress, so no starvation was observed. Paced lifecycle
P99 stays around 0.06-1.02 ms in this shadow. Writer starvation was not the
reason Phase 1.3 withheld admission.

## Design options

### A: `RwLock<FiberMutable>`

This preserves one coherent state domain, needs no dependency, and has low
correctness complexity. It would require classifying 57 sites into reads and
writes and updating lock documentation, but does not change the abstract lock
DAG. Guards must still never cross await. Phase 1.3 suitability remained
**UNRESOLVED** because the full shadow failed the performance threshold despite
acceptable writer progress and only a 3.8% single-reader cost; the lock-only
component was not isolated until Phase 1.4.

### B: split hot admission state

The proposed hot fields would be scope, state, generation, capability gate, and
cancellation. Publication would be required during initial activation,
WaitingDependencies reactivation, reload relocation/cutover, activation commit,
failure, and disposal entry/finalization. Generation, gate and cancellation
replacement must be one publication; Scope relocation must become visible to
same-generation future admission; state/gate publication must not expose Active
with a staged or incompatible gate.

A second lock would require a defined hot/cold lock order and coordinated commit
with ownership/disposal metadata. Independent publication would risk mixed
generation snapshots and partial lifecycle visibility. An immutable combined
hot snapshot could avoid field tearing, but it still introduces an explicit
publication protocol at every lifecycle writer. Complexity and lifecycle risk
are **HIGH**. Since no shadow has proved enough end-to-end gain to justify this
transaction, the option is not admitted.

### C: immutable or atomic snapshot

`ArcSwap<AdmissionSnapshot>`, a versioned pointer, or equivalent could publish
the five fields coherently and make reads non-locking. It adds per-admission
reference-count traffic, writer allocation, memory reclamation/dependency and
MSRV review, plus the same lifecycle publication audit as Option B. Packing only
the scalar fields cannot coherently include the Arc gate and cancellation token.
This remains possible but is **DEFERRED** until a smaller targeted shadow model
demonstrates a material realistic-workload gain.

| Property | Mutex current | RwLock | Split snapshot | Atomic/immutable |
|---|---|---|---|---|
| Reader scaling | poor | poor in shadow | unknown | potentially strong |
| Coherent snapshot | native | native | explicit transaction | one published object |
| Writer simplicity | simple | simple | complex | allocation/publication |
| Callsite breadth | none | 57 classifications | lifecycle writers plus reads | lifecycle writers plus reads |
| New dependency | no | no | not necessarily | likely |
| Lifecycle risk | frozen | low | high | medium-high |
| Expected performance | measured collapse | full snapshot insufficient; lock component unresolved in this phase | unproven | unproven |

The existing lock direction remains FiberCell to ScopeRegistry only for the
short reload/create-scope transactions; there is no ScopeRegistry-to-FiberCell
edge. ServiceRegistry, PluginRegistry and CapabilityGate operations do not add a
reverse nested Fiber lock edge. A future design must preserve this DAG and keep
both read and write guards out of await points.

## Optimization 2 regression smoke

Five-run medians at 4/8/16 workers remain healthy: GE-A is
11.353/10.012/8.355 M/s, GE-C is 8.937/7.477/5.808 M/s, PB1 is
3.841/4.328/4.107 M/s, and PB2 is 4.149/4.539/4.285 M/s. These agree with the
Optimization 2 direction and show no return of the former mutex collapse.

## Future evidence and correctness gates

The next characterization should prototype one coherently published admission
snapshot without changing production, then compare its realistic five-access
workload against the locked before points. Any future implementation must add
deterministic races for generation/gate/cancellation replacement, Active versus
staged gate publication, same-generation Scope relocation, disposal versus
admission, stale Context versus reactivation, and writer publication versus
gate cutover. Only a material gain with clean publication proofs can admit
Optimization 3 implementation.
