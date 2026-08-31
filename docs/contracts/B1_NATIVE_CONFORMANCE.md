# B1 Native Runtime conformance manifest

The standalone Native oracle is `cargo test -p cordis-runtime --test
conformance`. It is a Cargo integration target and can access only public APIs
from `cordis-runtime` and `cordis-core`. It uses no production `cfg(test)` hook,
private registry, or generic runtime-driver abstraction.

The B0 inventory is 55 total (35 PUBLIC, 12 INTERNAL, 4 HOST, 2
WIRE-CANDIDATE, 2 PLACEHOLDER). The earlier 53 count came from inventory logic
that required a three-letter prefix and therefore omitted `GC-001` and `GC-002`.

## Public contracts

| Contract | B0 Class | B1 Mode | Executable Test(s) | Result | Notes |
| --- | --- | --- | --- | --- | --- |
| OWN-001 | PUBLIC | EXECUTABLE | `own_001_install_observer_drop_does_not_cancel_convergence`; `own_001_reload_observer_drop_does_not_cancel_convergence` | PASS | Install and reload observers are not owners. |
| OWN-002 | PUBLIC | EXECUTABLE | `own_002_fiber_disposal_observer_drop_does_not_cancel_convergence`; `dsp_001_registered_observers_share_one_immutable_completion`; `shd_002_concurrent_callers_share_attempt_and_observer_drop_does_not_cancel` | PASS | Fiber, Scope, and shutdown convergence remain Runtime-owned. |
| OWN-003 | PUBLIC | EXECUTABLE | `own_003_retained_context_does_not_own_fiber_lifetime` | PASS | Only typed stale/not-found classes are asserted. |
| OWN-004 | PUBLIC | EXECUTABLE | `own_004_svc_004_existing_service_handle_never_retargets` | PASS | Old handle pins value and delays cleanup until drop. |
| SCP-001 | PUBLIC | EXECUTABLE | `scp_001_scopes_form_a_rooted_strict_tree` | PASS | Root/parent/current Scope are public observations. |
| SCP-002 | PUBLIC | EXECUTABLE | `scp_002_parent_disposal_waits_for_child_cleanup_convergence` | PASS | Plugin-owned cleanup barrier proves order. |
| SCP-003 | PUBLIC | EXECUTABLE | `scp_003_disposed_scope_rejects_children_and_has_no_active_fibers` | PASS | Allows `ScopeDisposed` or reclaimed `ScopeNotFound`. |
| GEN-001 | PUBLIC | EXECUTABLE | `gen_001_reload_preserves_plugin_identity_and_replaces_fiber_identity` | PASS | Generation is inferred by replacement/staleness; private GenerationId is not read. |
| GEN-003 | PUBLIC | EXECUTABLE | `gen_003_ctx_004_old_context_never_acquires_replacement_authority` | PASS | No generation reuse can revalidate the retained Context. |
| CTX-003 | PUBLIC | EXECUTABLE | `ctx_003_retained_context_tracks_same_generation_scope_relocation` | PASS | Public reload supplies a staged Context whose current Scope relocates at commit while the same retained Context remains valid. |
| CTX-004 | PUBLIC | EXECUTABLE | `gen_003_ctx_004_old_context_never_acquires_replacement_authority` | PASS | Stale or physically reclaimed not-found is accepted. |
| SVC-001 | PUBLIC | EXECUTABLE | `svc_001_service_key_is_logical_identity` | PASS | No layout/Display assertion. |
| SVC-002 | PUBLIC | EXECUTABLE | `svc_002_nearest_visible_provider_wins` | PASS | Public Scope hierarchy only. |
| SVC-003 | PUBLIC | CALLER-OBLIGATION | `svc_003_same_runtime_interning_is_stable_but_cross_runtime_is_not_claimed` | PASS | Unbranded, non-portable token; no cross-Runtime rejection guarantee. |
| SVC-004 | PUBLIC | EXECUTABLE | `own_004_svc_004_existing_service_handle_never_retargets` | PASS | Existing handle remains exact-generation. |
| INV-001 | PUBLIC | EXECUTABLE | `inv_001_invocation_key_is_logical_identity` | PASS | Logical fields only. |
| INV-004 | PUBLIC | EXECUTABLE | `inv_004_middleware_is_root_to_leaf_then_registration_order`; `inv_004_in_flight_invocation_uses_one_immutable_dispatch_snapshot` | PASS | Covers public chain order and immutable in-flight routing. |
| INV-005 | PUBLIC | EXECUTABLE | `inv_005_type_mismatch_not_found_panic_and_timeout_are_distinguishable` | PASS | Variant classes, never strings. Caller-cancellation remains existing public integration evidence. |
| EVT-001 | PUBLIC | DOCUMENTED-BOUNDARY | `evt_001_later_handler_does_not_receive_an_old_event` | PASS/PARTIAL | No implicit replay is executable; process-crash durability/state-store absence is documentation. |
| EVT-002 | PUBLIC | EXECUTABLE | `evt_002_emit_bail_serial_parallel_and_waterfall_semantics_are_public` | PASS | Public ordering, bail, parallel completion, and waterfall propagation. |
| TSK-001 | PUBLIC | EXECUTABLE | `tsk_001_owned_task_is_cancelled_and_joined_on_disposal` | PASS | Scheduler placement is not observed. |
| TSK-002 | PUBLIC | EXECUTABLE | `tsk_002_task_panic_is_observed_and_effect_cleanup_continues` | PASS | Panic prose is not asserted. |
| EFF-001 | PUBLIC | EXECUTABLE | `eff_001_effects_run_at_most_once_in_lifo_order` | PASS | LIFO and once-only public effect behavior. |
| EFF-002 | PUBLIC | EXECUTABLE | `eff_002_cleanup_issue_does_not_rollback_committed_disposal_truth` | PASS | Structured committed-with-issues outcome. |
| HMR-002 | PUBLIC | EXECUTABLE | `hmr_002_precommit_failure_keeps_old_generation_authoritative` | PASS | Staged service is never selected. |
| HMR-003 | PUBLIC | EXECUTABLE | `hmr_003_cutover_routes_new_work_to_new_generation_while_old_work_finishes` | PASS | Accepted old work finishes without retargeting; post-cutover work selects the replacement. |
| HMR-004 | PUBLIC | EXECUTABLE | `hmr_004_postcommit_cleanup_issue_cannot_rollback_new_generation` | PASS | `ReloadCommitted` plus new authoritative service. |
| DSP-001 | PUBLIC | EXECUTABLE | `dsp_001_registered_observers_share_one_immutable_completion` | PASS | Semantic equality, not Arc identity. |
| DSP-002 | PUBLIC | EXECUTABLE | `dsp_002_registration_begins_on_first_poll_and_fresh_request_after_gc_is_not_found` | PASS | Unpolled construction and fresh post-GC request are distinguished. |
| SHD-001 | PUBLIC | EXECUTABLE | `shd_001_shutdown_closes_admission_permanently`; `shd_003_incomplete_blockers_allow_retry_without_reopening_admission` | PASS | Complete and Incomplete both keep admission closed. |
| SHD-002 | PUBLIC | EXECUTABLE | `shd_002_concurrent_callers_share_attempt_and_observer_drop_does_not_cancel` | PASS | Cleanup runs once. |
| SHD-003 | PUBLIC | EXECUTABLE | `shd_003_incomplete_blockers_allow_retry_without_reopening_admission` | PASS | Concrete blockers then terminal retry. |
| SHD-004 | PUBLIC | EXECUTABLE | `shd_004_one_absolute_deadline_bounds_multiple_blockers` | PASS | Bounds attempt, not internal timer type. |
| ERR-001 | PUBLIC | EXECUTABLE | tests carrying `err_001`, `inv_005`, `hmr_002`, `hmr_004`, `eff_002`, `shd_003` | PASS/PARTIAL | All publicly inducible representative classes are executable without strings. Disposal-incomplete remains structurally public and internally tested but has no deterministic public fixture. |
| OBS-001 | PUBLIC | EXECUTABLE | `obs_001_snapshot_and_health_are_detached_observations_not_runtime_authority` | PASS | Old owned observations remain detached while fresh observations reflect later Runtime truth. |

Public B1 modes: 33 EXECUTABLE, 1 CALLER-OBLIGATION, and 1
DOCUMENTED-BOUNDARY; total 35. `UNRESOLVED` is zero. The Native public semantic
oracle is complete; ERR-001 explicitly distinguishes executable representative
classes from a structurally exposed, internally tested disposal-incomplete branch
that cannot be induced deterministically without a private hook.

## B1.1 closure

The standalone target contains 36 public-API tests. CTX-003, INV-004, HMR-003,
and OBS-001 now have dedicated executable coverage. ERR-001 covers not-found,
stale, cancellation, timeout, native request/response mismatch, handler panic,
reload pre/post-commit truth, committed disposal issues, and shutdown blockers
without diagnostic-string equality. The structurally exposed disposal-incomplete
branch remains deterministic only in the Kernel invariant suite and does not
represent an implementation or public API gap.

Final B1 decision: **COMPLETE**.

## Internal, Host, and wire contracts

| Contracts | B0 Class | B1 Mode | Evidence/result |
| --- | --- | --- | --- |
| SCP-004, FIB-001, GEN-002, CTX-001, CTX-002, INV-006, HMR-001, DSP-003, DSP-004, GC-001, GC-002 | INTERNAL | INTERNAL-ONLY | Mapped to the existing Kernel tests named in `CONTRACT_MATRIX.md`; not moved. |
| FIB-002 | INTERNAL | EXECUTABLE supporting invariant | Existing public integration evidence remains; not treated as a PUBLIC contract. |
| HST-001, HST-002, INV-003, WIR-002 | HOST | DEFERRED-HOST | No B1 Host tests or implementation. |
| INV-002, WIR-001 | WIRE-CANDIDATE | DEFERRED-HOST | Native portion exists; no protocol invented. |
| SVC-005, WIR-003 | PLACEHOLDER | PLACEHOLDER | No authority/schema semantics invented. |

## CTX-003 reassessment

The prior API-gap conclusion was incorrect. Public `Runtime::reload_detailed`
invokes replacement `NativePlugin::start` with its staged `Context`. A fixture can
retain that Context, observe its staging Scope during start, and observe its target
Scope after commit. No private registry or hook is required.
