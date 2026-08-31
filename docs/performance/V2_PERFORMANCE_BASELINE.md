# Cordis Kernel v2 canonical performance baseline

This is the canonical Track A baseline after accepted Optimizations 1-3 and the
Phase 1.5 closure. Results are single-machine characterization, not portable
latency guarantees.

## Environment and methodology

- Windows 11 x64; Intel Core i7-12650H, 10 physical / 16 logical cores
- rustc 1.96.0 MSVC, LLVM 22.1.2; Rust 1.85 is the verified MSRV
- Cargo release/bench profile, default features unless stated
- warm caches; Service tests use depth one and `ServiceSymbol` unless stated
- medians are used for throughput; before/after comparisons use identical source
- canonical post-Optimization-3 commit: `afda259`

## Canonical results

| Area | Canonical result |
| --- | --- |
| Context shared 1/2/4/8/16 | 15.886/6.801/6.276/5.913/5.653 M/s |
| Context independent 1/2/4/8/16 | 15.275/31.053/48.892/62.732/84.347 M/s |
| Base admission 1/2/4/8/16 | 15.597/6.949/6.239/5.705/5.448 M/s |
| Registration admission | 14.429/6.231/6.293/6.285/5.296 M/s |
| Cancellable admission | 10.230/4.050/3.054/2.098/2.149 M/s |
| Service A 1/2/4/8/16 | 3.695/2.171/3.025/3.615/3.473 M/s |
| Service B 1/2/4/8/16 | 3.685/4.656/4.082/4.456/4.149 M/s |
| Service C 1/2/4/8/16 | 3.870/4.501/5.372/5.795/5.564 M/s |
| No-op invocation 1/2/4/8/16/32 | 0.854/0.999/0.864/0.553/0.497/0.488 M/s |
| Event fanout 1/8/32/128 | 0.390/2.137/6.890/27.234 us |
| Task batch 1/8/32/128/512/1000 | 155/196/443/546/512/658 K tasks/s |
| HMR 0/1/10 Hz, 8 workers | 3.304/3.241/3.249 M foreground ops/s; 6.1/6.2/5.8 us P99 |
| GC 10k/50k | 0.041/0.305 ms median |
| Dependency 10k, affected 1/1000 | 2.34/12.12 ms |
| 300-second mixed soak | 1.056 M component ops/s; 2.714 ms P99; full logical convergence |

`ServiceKey` is a secondary fixed cost: current C at 1/4/8/16 is
3.253/3.900/4.172/3.894 M/s versus Symbol
3.736/5.287/5.849/5.490 M/s. Scope ancestry remains proportional: Symbol C at
depth 1/8/32 is 3.776/2.455/1.212 M/s with one worker and
5.850/4.883/3.419 M/s with eight.

## Historical chain

1. [Phase 0.x characterization](V1_CHARACTERIZATION.md) isolated the initial
   serialization and control-plane candidates.
2. [Optimization 1](V2_OPT_01_SERVICE_READ_FAST_PATH.md) removed the
   ServiceRegistry cache-hit writer.
3. [Phases 1.1 and 1.2](V2_PHASE_1_2_GENERATION_ADMISSION.md) isolated shared
   generation admission; [Optimization 2](V2_OPT_02_GENERATION_ATOMIC_FAST_PATH.md)
   replaced its accepting mutex path with atomic admission.
4. [Phases 1.3 and 1.4](V2_PHASE_1_4_CONTEXT_ADMISSION_DECOMPOSITION.md)
   decomposed Context admission.
5. [Optimization 3](V2_OPT_03_MINIMAL_CONTEXT_ADMISSION.md) introduced coherent
   shared reads and operation-specific minimal admission.
6. [Phase 1.5](V2_PHASE_1_5_PERFORMANCE_CLOSURE.md) reproduced the final
   distribution, attributed anomalies, completed the soak, and froze Track A.

## Deferred costs

- CancellationToken cloning remains only on genuinely cancellable operations.
- Exact-gate Arc cloning preserves release-before-acquire race semantics.
- Weak owner upgrade preserves Context/Fiber ownership semantics.
- GenerationExecution retains unavoidable atomic lease traffic.
- Prefer `ServiceSymbol`; no interner redesign is justified.
- Scope ancestry Vec, GC full scan, and DependencyGraph traversal remain bounded
  control-plane or topology-dependent costs.
- Registry sharding, Fiber state splitting, and ArcSwap publication have no
  supporting evidence and are not admitted.

Track A Performance is complete. Further v2 work should move to Contract
Extraction, Conformance, and Process Host unless new controlled regression
evidence appears.
