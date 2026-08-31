# Cordis Kernel v2 performance candidates

| Rank | Module | Evidence | Root-cause status | Candidate decision |
| ---: | --- | --- | --- | --- |
| 1 | Service concurrency | Optimization 1 changed isolated C 8/4 from 0.626 to 1.043 and 16/4 from 0.354 to 0.987 | Cache-hit global writer confirmed and removed | **ACCEPTED**; characterize shared admission and interner next, no sharding |
| 2 | Shared generation admission | Optimization 2 raised shared normal/service at 16 workers from 2.538/2.530 to 8.553/6.092 M/s; PB1/PB2 improved 119-131% | `GenerationExecution.state` mutex contention confirmed and removed from accepting fast path | **Optimization 2 ACCEPTED** |
| 3 | Shared Fiber admission | Real shared Context improved from 1.958/1.331 to 5.745/5.691 M/s at 8/16; the five-Service shared workload improved from 0.436/0.268 to 0.787/0.778 M logical/s | Exclusive Fiber lock and unconditional cancellation capture were confirmed and removed from the base path without changing the release-before-gate boundary | **Optimization 3 ACCEPTED**: shared-reader synchronization plus operation-specific minimal coherent payload; no state split/ArcSwap |
| 4 | Service interner | Symbol is about 35-47% faster at 4-16; same/many keys scale alike and 8-128 character keys are similar | Fixed reader/HashMap/hash cost, not writer-style collapse | Recommend Symbol reuse on repeated hot paths; redesign DEFER |
| 5 | Dependency provider loss | affected=1 grows with total graph; at total=10k cost also grows from about 2.8 ms to 94 ms as affected reaches 5k | Mixed total-scan and affected-work components | P1 profile provider-loss path before considering index changes |
| 6 | GC full scan | 1k=2.8 us, 10k=42.4 us, 50k=291 us with fixed actual reclaim=0/1 | Total-size dominated | P2; queue remains DEFER because absolute cost is low |
| 7 | Scope ancestry | Cache saves about 25% at depth 32; cached absolute cost about 1.0 us | Existing cache works | Keep current algorithm; new cache DEFER |

The following architectural optimizations remain **DEFER**: FiberCell snapshot redesign, registry sharding, interner redesign, incremental reclaim queue, DependencyGraph reverse-index redesign, TaskSupervisor structure changes, and general memory-layout optimization. Generation drain and TaskSupervisor hypotheses are now substantially weakened by data.

Event dispatch, basic invocation, scope lifecycle, task completion, idle reload and empty shutdown remain unchanged. Optimizations 1, 2, and 3 are accepted. Any Optimization 4 remains unadmitted pending fresh post-Optimization-3 characterization.

Phase 1.5 completed that characterization and found no Optimization 4 candidate.
Track A Performance is **COMPLETE/FROZEN**; the canonical numbers and deferred
costs are recorded in `V2_PERFORMANCE_BASELINE.md`.
