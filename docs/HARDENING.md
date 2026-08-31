# Cordis Kernel Hardening 清单

状态说明：`[ ]` 待处理，`[~]` 处理中，`[x]` 已完成。每项只有在实现、确定性测试和质量门禁都通过后才能标记完成。

## P0：生命周期正确性

- [x] H-001：install/activate/reload 均由 Runtime-owned worker 持有；已接受 operation 的 observer drop 不取消收敛。
- [x] H-002：建立统一 AdmissionGate；shutdown 线性化后拒绝 mutation、invoke、emit 和新生命周期工作。
- [x] H-003：为每个 Fiber 序列化 activate/reconcile/reload/dispose 操作，保证唯一 winner。
- [x] H-004：实现 ActivationTxn；commit 前资源不可见，commit 不得部分失败。
- [x] H-005：Starting 阶段创建的 owned task 必须等待 activation commit，rollback 时直接取消。
- [x] H-006：建立稳定 PluginId 与 active Fiber generation，以单一逻辑点完成 HMR cutover。
- [x] H-007：实现取消安全的 ReloadTxn，区分 pre-commit failure 与 committed cleanup failure。
- [x] H-008：引入 generation-local Draining 与 Runtime-tracked provider in-flight lease；Effect cleanup 前等待 Invocation/Event execution 归零。
- [x] H-009：activation 保留 primary 与有序 cleanup issues；Fiber/Scope disposal 区分 committed truth、cleanup issues 与 incomplete convergence。

### Batch 5F closure evidence

- Install admission 后创建 `RuntimeWorkerKind::Install` 与 oneshot observer；worker creation 是接受线性化点，observer drop 不影响 start、commit 或 rollback。
- Initial activation 随 install worker 执行，dependency restoration activation 随 coalesced `DependencyReconcile` worker 执行；外部 trigger future 不持有 activation body。
- `CleanupIssue` 按真实执行顺序记录 `CleanupPhase`；独立 Effect/child cleanup 继续并聚合，provider drain timeout 仍在 destructive cleanup 前终止。
- `dispose_fiber_detailed` 与 `dispose_scope_detailed` 将 terminal lifecycle truth 和 cleanup diagnostics 分离；兼容的旧 `Result` API 保留。
- Fault injection 验证 start primary + 两个 LIFO cleanup errors、Fiber committed cleanup issue、Scope committed descendant issue。

Dropping an observer is not Runtime cancellation. Future explicit lifecycle cancellation must use a dedicated token/API. Shutdown is a separate higher authority: it closes admission and can force uncommitted work to roll back.

### Batch 5F.1 closure evidence

- `converge_failed_activation` 统一处理 start、seal、revision validation、ordinary commit、shutdown-before-publication 与 panic failure。
- `commit_activation` 的所有 fallible checks 位于 capability publication 前；`Err` 保证未 publish，publish 后 metadata 与 task barrier release 为 infallible forward-only steps。
- `DisposeOutcome::Incomplete` 和 `ScopeDisposeOutcome::Incomplete` 分离 `primary` terminal blocker 与 ordered `issues`；已有 issue 不再隐藏后续 generation drain blocker。
- Scope 只有在所有 child/Fiber 真正到达 terminal committed state 后才提交 `Disposed`；仍在 `Disposing` 的 descendant 会令 Scope 返回 `Incomplete`。

### Batch 5F.2 durable completion evidence

- Fiber/Scope finalizers now publish one immutable `DisposalObservation` containing the legacy result, terminal truth, primary blocker, and ordered cleanup issues before notifying waiters.
- Legacy and detailed APIs join the same completion Arc. Detailed observers no longer re-read FiberRegistry or ScopeRegistry after completion.
- Registered observers retain committed, committed-with-issues, or incomplete truth across GC. A brand-new request against an already reclaimed ID still reports NotFound; no tombstone/history registry was added.
- Failed activation rollback consumes the same durable Fiber observation, so cleanup aggregation does not depend on post-completion Registry membership.

### Batch 5F.3 completion linearization evidence

- Fiber disposal uses an internal `Finalizing` phase while remaining in `FiberState::Disposing`; GC therefore cannot reclaim the Fiber during cross-registry convergence.
- Permanent disposal removes dependency consumer edges, provider declarations, and owning Scope membership before committing terminal Fiber bookkeeping and publishing the immutable `DisposalObservation`.
- `DisposalCompletion::publish` is the final synchronous visibility point. After publication only lock release, deterministic test instrumentation, and waiter notification remain; notification is not treated as a commit fence.
- Dependency-deactivation keeps its consumer/provider declarations and Scope membership, commits `WaitingDependencies`, and only then publishes. Incomplete convergence keeps the non-terminal safe state and publishes the primary blocker without destructive index detachment.
- Deterministic pre-publication tests prove that direct observation and detailed waiters remain pending, the Finalizing Fiber is not GC-reclaimable, and both clean and cleanup-issue committed outcomes imply fully converged Scope and dependency indexes.

### Batch 5F.3.1 Scope publication ordering evidence

- Committed Scope finalization now updates terminal bookkeeping and unlinks the Scope from `parent.children` before publishing its immutable completion, all under one short ScopeRegistry writer.
- The ScopeRegistry writer protects topology atomicity but is not treated as a completion visibility fence: independent `observation()` readers only see committed truth after parent topology has converged.
- Incomplete or terminated Scope disposal retains its existing diagnostic topology semantics and does not unlink from the parent.
- A deterministic pre-commit hook proves legacy and multiple detailed waiters cannot complete while the parent still contains the Scope. Clean and cleanup-issue outcomes both imply immediate final parent topology.

### Batch 3 closure evidence

- 每个 Fiber 的 async lifecycle mutex 串行化 activation、reload 与 disposal；disposal admission 取消正在执行的 activation，worker 随后等待同一 operation guard。
- activation 在 `Starting` 下收集资源，`plugin.start` 返回后 seal topology；commit 在 Runtime state writer 临界区中重新检查 shutdown、operation state 和全部资源，再发布 registry、切换 `Active` 并释放 task barrier。
- 此处原有的 `ActivationGuard` cancellation rollback 已被 Batch 5F supersede：accepted activation 由 Runtime worker 持有，所有 pre-publication failure 通过显式 convergence helper rollback。
- 确定性测试覆盖 single activation winner、dispose/shutdown vs activation、调用者取消、topology seal、commit visibility，以及 staged task commit/rollback。

### Batch 4C-GEN closure evidence

- `PluginRegistry` owns stable logical `PluginId` slots, monotonic runtime `GenerationId` allocation, the shared `GenerationSelector`, and Fiber reference counts.
- Service, Event, and Invocation entries carry one generation-bound `CapabilityGate`; selector CAS is their only normal visibility cutover truth.
- Event and Invocation snapshots cache one active generation per selector, so a cutover during snapshot construction cannot select both generations or neither generation.
- `PreparedServiceRevision` remains an all-or-nothing physical replacement under the ServiceRegistry write lock; selector CAS occurs in that same critical section.
- This closes data-plane generation cutover only. H-007 remains open because Scope/Dependency/Fiber control-plane rollback is not yet a complete `ReloadTxn`.

### Batch 4C-F closure evidence

- `FiberRegistry` owns only generation-safe arena membership; operations clone an `Arc<FiberCell>` before releasing its arena lock.
- Each `FiberCell` owns its async lifecycle mutex and one coherent synchronous `RwLock<FiberMutable>` resource ledger. Context admission uses a short read guard, lifecycle/ownership mutation uses a write guard, and no Fiber synchronous guard crosses `.await`.
- Context retains a `Weak<FiberCell>`. Its get/emit/invoke hot paths do not consult legacy State, and invoke does not perform an arena lookup.
- Scope disposal and Fiber creation linearize on the ScopeRegistry write lock, while permanent Fiber disposal detaches membership before completion publication.
- Plugin reference accounting is checked and FiberRegistry removal is the exactly-once GC detach point.

### Batch 4C-F.1 closure evidence

- Dependency restart creates its fresh generation, gate, and cancellation token together after acquiring the Fiber lifecycle guard; the activation body clones that exact new token.
- Dependency-loss finalization leaves the old token cancelled. Disposal racing restart therefore cancels the token actually selected by the running activation body.
- Health, snapshot, GC, dependency validation, and unstarted-Fiber rollback no longer hold Scope and Fiber locks together. `Context::create_scope` is the sole short Fiber-to-Scope nesting; Scope-to-Fiber is forbidden.
- H-018 remains open: a retained Context can still upgrade the reused FiberCell across dependency generations.

### Batch 4D closure evidence

- `ShutdownCoordinator` owns only shutdown execution selection, the Runtime-owned supervisor, shared completion, and terminal cleanup outcome; it is never held across async cleanup.
- `AdmissionGate::begin_shutdown` remains the single admission linearization point. Cleanup failure leaves admission permanently closed and publishes one stable result to every waiter.
- Dropping any shutdown caller does not abort the detached supervisor, and concurrent callers observe the same completion while cleanup runs once.
- The legacy Runtime `State` and `RuntimeInner.state` are removed. Context get/invoke/emit do not consult the coordinator or any Runtime-wide data mutex.
- H-007, H-008, H-018, and H-019 remain open and are not changed by this extraction.

### Batch 5A closure evidence

- `TaskSupervisor` tracks each Fiber task before its start barrier opens. Normal completion, cancellation, and panic race through one arena remove winner and detach live Fiber membership without waiting for disposal.
- Fiber task quota is concurrent-live ownership. Disposal takes the group without holding `FiberCell::inner`, cancels once, joins concurrently to one absolute deadline, and aborts remaining joins.
- `RuntimeWorkerSupervisor` tracks activation reconciliation and cancellation-triggered rollback cleanup, observes completion/panic/error, and is drained by shutdown. Normal scheduling closes with `AdmissionGate`; cleanup-authorized work remains admissible.
- Per-Fiber activation reconciliation uses one pending/running entry plus a dirty bit, so bursts coalesce and a change observed during execution triggers one convergence rerun.
- Dedicated Fiber/scope/shutdown body-supervisor pairs remain explicit operation-owned Tokio tasks: their completion records own the handles, observe panic/cancellation, and preserve caller-drop convergence. They are documented exceptions, not fire-and-forget work.

### Batch 5A.1 closure evidence

- Live task entries retain immutable Fiber/Scope attribution. Whichever path removes the entry owns terminal classification, metrics, diagnostics, and membership reconciliation exactly once.
- Runtime worker normal completion and shutdown drain share one terminal recorder. Deadline abort first requests cancellation, then classifies each actual JoinHandle result, so already-completed and panicked work is not mislabeled aborted.

### Batch 5B closure evidence

- Every CapabilityGate owns a fresh per-generation `GenerationExecution`. Lease acquisition and begin-draining serialize on one local mutex, making check-and-increment atomic and draining irreversible.
- Invocation holds leases across the full middleware/handler chain. Event handlers acquire immediately before user code and stale snapshots that lose admission are skipped.
- Generation cutover remains the visibility linearization point. Old execution may drain concurrently while new generation Invocation/Event work starts immediately.
- Fiber disposal closes execution admission, converges Fiber tasks, waits provider inflight to zero, and only then removes handlers/services and disposes Effects. Drain timeout leaves the Fiber non-disposed and resources intact.
- H-019 remains open because escaped `Arc<Service>` values have no Kernel-observable release boundary.

### Batch 5B.1 closure evidence

- Invocation capacity is acquired before Scope ancestry, Invocation snapshot, or generation leases. Queue time therefore is not provider inflight.
- After permit acquisition the caller Fiber/cancellation state is revalidated, then dispatch resolves the current selector generation.
- Deterministic max-concurrency/HMR coverage proves a queued invocation does not pin old draining state and executes the new handler after cutover.

### Batch 5C closure evidence

- Reload is a RuntimeWorkerSupervisor-owned operation. Dropping the API observer cannot cancel prepare rollback or committed finalization.
- `PreparedServiceRevision`, `PreparedDependencyRevision`, and `PreparedScopeRevision` validate ownership before commit. Reversible control-plane preparation is rolled back if selector publication fails.
- `GenerationSelector` CAS remains the sole commit point. Before CAS rollback is allowed; after CAS rollback is forbidden and only forward convergence executes.
- `ReloadFailed { primary, cleanup }` distinguishes pre-commit failure. `ReloadOutcome::CommittedWithCleanupPending` and `ReloadCommitted` preserve successful commit truth across drain timeout or cleanup failure.
- A persistent, supervised reload worker continues old-generation disposal after the observer deadline. Active reload, cleanup-pending, staged Fiber, and staged Scope counts are exposed without cross-registry lock nesting.
- Deterministic caller-drop, publication rollback, post-commit timeout, cleanup-failure, reload/shutdown, reload/dispose, double-reload winner, and 200-generation churn tests prove convergence. H-001 and H-009 remain partial only because their non-reload lifecycle surfaces are outside this batch.

### Batch 5C.1 closure evidence

- Scope revision commit and inverse rollback revalidate lifecycle, old/staged membership, and child parentage while holding the single ScopeRegistry writer that applies the full topology delta.
- Dependency provider relocation and inverse rollback validate every symbol and move the full batch under one DependencyGraph writer; a conflict leaves every prepared symbol untouched.
- Reload-staged Fibers carry explicit transaction lifecycle ownership. Public dispose/reload reject them, internal rollback remains privileged, and ownership is released only by rollback disposal or successful selector publication.
- Deterministic conflict and atomicity tests cover target disposal, staged membership, child topology, multi-symbol dependency migration, and public lifecycle races. These close H-007's remaining control-plane gap.

### Batch 5C.2 closure evidence

- The ScopeRegistry writer now remains held from final Scope revalidation through topology apply and ServiceRegistry selector CAS. Target disposal cannot enter `Disposing` or snapshot membership inside that window.
- Selector failure performs the complete inverse Scope revision before releasing the fence; Dependency rollback follows after the Scope topology is restored.
- The only new lock edge is the documented short `ScopeRegistry -> ServiceRegistry` commit edge. A workspace audit found no reverse ServiceRegistry-to-ScopeRegistry path and the fence crosses no await.
- Deterministic barriers pause immediately before selector CAS and prove successful cutover, disposal snapshot exclusion, and failed-CAS rollback ordering. H-007 is closed by the resulting single-winner lifecycle/cutover rule.

### Batch 5C.3 closure evidence

- Reload commit owns staged Fiber metadata before acquiring the Scope cutover fence and retains both through selector CAS and committed Fiber metadata finalization.
- Target disposal cannot enter after publication until the new Fiber names the target Scope, staging/reload ownership flags are cleared, and the activation barrier has been extracted.
- CAS failure never writes committed Fiber metadata and restores Invocation and Scope control-plane state before fence release.
- Post-CAS deterministic race, relocated Service cleanup, and staged-task barrier tests close the remaining H-007 metadata window.

## P1：资源收敛与隔离

- [x] H-010：实现 TaskSupervisor；自动回收完成任务、即时记录 panic，并使用一个共享绝对 deadline join。
- [x] H-011：移除 activation/reconciliation/rollback 的无 ownership spawn；Runtime worker 被监督并对同 Fiber 请求合并。
- [x] H-012：Event dispatch 统一 panic boundary，并返回结构化 `EventHandlerPanicked` 错误。
- [x] H-013：Event 查询不创建空 slot，移除最后一个 handler 后回收 slot。
- [x] H-014：Service 查询使用 lookup-only interner；稳定 symbol 不复用，并受可配置容量限制。
- [x] H-015：Service resolution cache 只保存 location metadata，使用可配置 FIFO 上限，零容量可安全禁用。
- [x] H-016：dispose commit 立即撤销 declared/dependents 索引，不依赖手工 GC。
- [x] H-017：Runtime-owned coalesced GC 自动回收 terminal Fiber/Scope，并 exactly-once 收敛 plugin refcount。
- [x] H-018：Context 冻结创建 generation，并在每次 Runtime interaction 前统一校验 generation、lifecycle 与 accepting gate。
- [x] H-019：Service lookup 返回绑定 exact provider GenerationLease 的非 Clone ServiceHandle，不再暴露裸 Service Arc。
- [x] H-020：shutdown 使用绝对 deadline、结构化 blocker、immutable per-attempt completion，并在 admission 持续关闭时支持重试收敛。

### Final convergence and bounding evidence

- `shutdown_detailed` distinguishes `Complete`, `CompleteWithIssues`, and `Incomplete`; incomplete outcomes identify live Fiber, Scope, worker, task, reload, staging, and exact generation inflight ownership.
- Shutdown-triggered Scope/Fiber disposal uses persistent generation drain. Reaching the global deadline ends only the current observer attempt; dropping the last ServiceHandle allows a later immutable attempt to converge without reopening admission.
- Final shutdown convergence drains Runtime workers, revisits lifecycle residue created by already-accepted transactions, runs GC, and audits global blockers before publication.
- Stable service symbols have a configurable non-reusing bound with typed `ResourceLimitExceeded`. The reconstructible service-resolution cache has a configurable FIFO bound and stores no service Arc.
- Install/reload preparation is the only lifecycle path that admits declared Service symbols. `Context::provide*` validates the declaration first and performs lookup-only reuse, so undeclared and repeated publication cannot consume capacity. Configuration rejects limits outside the `u32` ServiceSymbol ID space.
- Terminal disposal requests one coalesced Runtime-owned GC worker. Collection is safe alongside lifecycle workers, preserves durable registered waiters, and performs PluginRegistry Fiber detach exactly once.
- GC worker registration holds an `AdmissionGate` read admission through synchronous `RuntimeWorkerSupervisor` registration. Shutdown close takes the matching writer fence: GC-first registration is drained, while shutdown-first rejection leaves `gc_state` untouched and relies on final synchronous GC.

### Batch 5D closure evidence

- Context captures its immutable `GenerationId`; cloning preserves that identity and dependency restart cannot upgrade an old Context to the reused FiberCell generation.
- One validation path distinguishes physical reclamation (`FiberNotFound`) from a retained but stale or draining capability (`StaleContextGeneration { fiber, expected, actual }`). Context stores no `GenerationLease`.
- Runtime-interacting reads, mutations, registration, task/timer, Scope, Event, and Invocation methods all require matching generation, an admitted lifecycle, and an accepting generation gate. `fiber()` and `generation()` are immutable identity accessors; `scope()` performs admission because the current Scope binding may relocate within the same generation.
- HMR staged Context remains valid after selector commit because its captured generation is the committed generation. Old HMR and dependency-restart Contexts cannot create recursive work or enter the new provider world.
- H-019 remains open: an `Arc<T>` obtained while Context was valid can still escape past generation invalidation.

### Batch 5D.1 closure evidence

- GenerationId moved into FiberMutable beside CapabilityGate and CancellationToken, so activation, restart, Context creation, and admission obtain a coherent generation bundle under one Fiber-local guard.
- `ContextAdmission` snapshots the exact gate matching the Context generation, releases the Fiber guard, and acquires a non-cloneable short GenerationLease from that gate. It never re-reads a replacement gate.
- Context admission and generation draining share GenerationExecution's try-acquire/begin-draining linearization. Admission-first interactions finish against the old generation; draining-first interactions never start.
- Base admission retains only owner, Scope, and lease. Registration admission also retains the exact gate used for admission; cancellable admission captures the exact CancellationToken only when required. Generation, gate, Scope, cancellation, and lifecycle always originate in the same coherent Fiber read snapshot.
- Every admission transiently clones its exact gate, releases the Fiber read guard, then calls `try_acquire`; registration publishes that same gate and never re-reads a replacement.
- Runtime-interacting Context APIs retain admission through their ownership handoff or operation. Invocation waits for the global permit before caller admission, preserving the no-queued-generation-pin rule.
- Deterministic restart barriers cover exact-gate, spawn, effect, provide, child Scope, queued invoke, old-handler recursion, staged rollback, and churn behavior. Context itself remains cloneable without storing a lease; H-019 remains open.

### Batch 5D.2 closure evidence

- Context immutable identity is FiberId plus GenerationId. The creation-time Scope is exposed only as diagnostic `initial_scope()` and is not used for Runtime operations.
- ContextAdmission captures current ScopeId with generation, exact gate, cancellation, and lifecycle under one Fiber-local guard. All Scope-sensitive APIs use this admitted binding.
- Same-generation HMR moves the Fiber binding from hidden staging Scope to target Scope without updating Context clones; later admission naturally observes target Scope.
- A full reload test reclaims the staging Scope before exercising retained Context `scope`, `parent`, service lookup, invocation, and child-Scope creation against target ancestry.
- Old-generation Context scope lookup remains typed-stale after dependency restart. Context still stores no lease, and escaped Service Arc lifetime remains H-019.

### Batch 5E closure evidence

- `get`, `try_get`, and `get_symbol` return non-Clone `ServiceHandle<T>` values containing the service Arc and a real lease from the exact ServiceRegistry entry gate.
- Service entry object, provider FiberId/generation, resolved Scope, symbol, and gate are one Registry snapshot. Lookup releases Registry state before gate admission and retries fresh resolution once after losing to draining.
- Existing old handles remain usable through HMR, ordinary disposal, and dependency loss while blocking destructive Effect/resource cleanup. Last-handle drop wakes the shared GenerationExecution drainer.
- New lookup after cutover resolves the new provider; handles never follow selectors or rebind. Caller ContextAdmission drops after lookup, leaving only the provider generation pinned.
- Trait-object services use `provide_arc` plus `get::<dyn Trait>`. Handles are naturally Send/Sync when the service is, deliberately non-Clone, and expose no `into_arc`, `arc`, or `clone_arc` API.
- Health and Runtime snapshots report `service_handle_inflight` as a diagnostic subset of total generation inflight. Deterministic lookup/HMR, cleanup, dependency restart, 10,000-handle, and 100-reload churn tests converge to zero.
- The guarantee covers use through ServiceHandle. If a service type itself clones or exposes independent ownership, that type defines the escaped value's lifetime semantics.

## 工程化验收

- [x] H-021：Linux、Windows、MSRV 1.85 与 stable CI matrix 已配置。
- [x] H-022：Rust 1.85 check/test/all-features、cargo-audit、cargo-deny、docs/release 与 unsafe/panic/spawn/lock/API audit 均已实际通过并记录。
- [x] H-023：取消、shutdown admission/retry、HMR 混代、provider drain、publication 与 task/key race matrix 已确定性覆盖。
- [x] H-024：install/dispose、reload、ServiceHandle、Scope、symbol、cache、worker 与 arena churn 均有资源收敛断言。

## 每批必跑门禁

```powershell
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
```
