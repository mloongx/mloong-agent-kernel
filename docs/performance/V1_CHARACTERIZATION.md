# Cordis Kernel v1 performance characterization

## Methodology closure

The synthetic workload is separated into data-plane, lifecycle, mixed and GC-stress modes. Warmup uses the same concurrency as measurement. Mixed mode no longer invokes manual GC unless requested. Latency includes P99.9 and max, RSS is periodically sampled, and topology is explicitly torn down before final resource/RSS reporting.

Every tested workload converged to zero Fibers, Tasks, Runtime workers and generations, with only the root Scope retained. RSS remained above startup after teardown, consistent with allocator retention rather than a logical Runtime leak.

The ten-minute mixed/32 run completed 20,334,445 runs at 33,891 runs/s (1.017M logical ops/s). P50/P95/P99/P99.9 were 0.720/2.402/3.346/5.222 ms. Across 507 periodic samples, steady RSS was 16.94 MB minimum, 17.28 MB average and 17.58 MB peak. After workload and full topology teardown it fell to 10.75 MB and 9.87 MB respectively, versus 5.28 MB initial. Logical resources returned to `fibers=0`, `scopes=1`, `tasks=0`, `workers=0`, `generations=0`.

## Main conclusions

1. Per-run manual GC polluted old mixed throughput by about 23%, but removing it does not remove the high-concurrency plateau.
2. Data-plane scaling improves through four workers and regresses at eight. ETW shows substantial idle CPU, so physical CPU exhaustion is not the sole cause.
3. Steady Service resolution reproduces the regression with both OS threads and Tokio tasks after excluding creation/join setup. Phase 0.3 A/B/C isolation shows the collapse survives independent callers, providers, keys and cache entries. Together with the source-visible cache-hit global writer, `ServiceRegistry::resolve` is attributed with VERY HIGH confidence.
4. The existing resolution cache is useful. At depth 32 it reduces latency by roughly 25%, and a second ancestry cache is not justified.
5. Generation drain does not scale materially with retained lease count through 1,000. Atomic GenerationExecution work is unsupported.
6. Full-scan GC cost scales with total registry size, but 50k live scopes still cost only about 0.29 ms per pass. Complexity is proven; P0 urgency is not.
7. Dependency loss follows both total graph and affected set. A reverse-index redesign is not admitted without function-level attribution and another topology.
8. TaskSupervisor scales cleanly through 1,000 no-op tasks.
9. HMR foreground impact is small at the tested rates, generations converge, but reload latency has rare cleanup-pending outliers at 10 Hz.

The critical 4/8/16-worker data-plane points were repeated three times after the
histogram conversion. Median throughput was 59.7k/38.5k/36.3k runs/s. The
within-point ranges were 2.6%, 3.9% and 1.9%, respectively, so the 4-to-8 drop
is repeatable rather than a greater-than-10% noisy result. These short reruns
used the same 1 s concurrent warmup and 3 s measurement window; the earlier
full matrix remains the source for the 1-to-256 table.

## Correctness gate note

Formatting, clippy, the default workspace test suite, release workspace test
suite, benchmark compilation and characterization smoke suite passed. The
all-features suite exposed an intermittent pre-existing shutdown/scope race in
`one_hundred_fast_scope_and_shutdown_operations_have_one_result`: one full-suite
run failed, and a focused repetition failed on run 9 of 10. No production source
was changed in this characterization, so this is recorded as a separate frozen
baseline correctness gap rather than repaired or hidden in a performance commit.

## Phase 0.3 closure

Controlled isolation plus source synchronization audit closes the last
architecture-admission question without depending indefinitely on Windows PDB
symbol resolution. The prior fast-dispose failure was a test contract mismatch:
future construction was mistaken for waiter registration, and late first polls
correctly observed `ScopeNotFound` after GC. Deterministic registered-waiter and
fresh-post-GC tests now express the two contracts separately.

Performance characterization is **COMPLETE**. The narrow Service resolution
cache-hit read fast path is admitted as the first V2 proposal, but is not
implemented in this phase. Allocation callsites, extra DependencyGraph topology
and isolated GenerationExecution measurements remain deferred research rather
than blockers for this admission decision.
