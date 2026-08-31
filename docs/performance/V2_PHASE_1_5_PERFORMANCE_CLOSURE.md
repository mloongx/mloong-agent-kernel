# V2 Phase 1.5: Post-Optimization-3 Performance Closure

Track A is complete. This phase changed characterization harnesses only; no
production code or public API changed. The frozen baseline is `12de983`, the
before checkout is `f3362e5`, and the accepted Optimization 3 baseline is
`afda259`.

## Five-Service single-worker investigation

The historical 0.691 to 0.477 M logical operations/s comparison was not an
apples-to-apples regression. The identical `perf_service` source, compiler,
release profile, machine, depth-one topology, warm cache, and environment were
run in isolated `f3362e5` and `afda259` worktrees. Twenty-run one-worker medians
were 0.708 M/s before and 0.779 M/s after, a 10.0% improvement. Ten-run medians
at 4/8/16 changed from 0.589/0.399/0.247 to 0.712/0.846/0.779 M/s.

The N-lookup after curve is compositional. For N=1/2/5/10, one-worker throughput
was 3.809/1.900/0.780/0.383 M logical operations/s, or
3.809/3.799/3.899/3.831 M lookups/s and 262.5/263.2/256.4/261.0 ns per lookup.
Retaining all five handles until the end of the logical operation was 0.765 M/s
versus 0.751 M/s for immediate drop, within run dispersion. The historical
single-worker anomaly is classified as measurement/harness noise with high
confidence, not an RwLock or ServiceHandle regression.

## Writer-tail attribution

Five 30-second production reload rounds at 0/8/16 busy reader threads completed
about 278/277/252 reloads per round. Median round P50 was approximately
0.137/0.155/0.258 ms. Eight-reader P99 stayed between 0.807 and 1.154 ms. Every
sixteen-reader round reproduced a 37.6-53.6 ms P99 and a 54.2-79.6 ms maximum.
Across 1,260 sixteen-reader samples, 484 exceeded 1 ms, 314 exceeded 5 ms, 229
exceeded 10 ms, 124 exceeded 25 ms, and 14 exceeded 50 ms.

Those Service readers use the caller Fiber while reload mutates the distinct
provider Fiber, so their tail cannot be acquisition of the reloaded Fiber's
`inner` lock. It is primarily CPU scheduling/control-plane latency under sixteen
non-yielding OS threads. A separate real-Fiber same-lock test shows that extreme
continuous same-Fiber reads can independently create RwLock writer tails:
8/16/32-reader write-wait P50 was 1.2/2.3/25.3 us, P95
3.9 us/1.06 ms/0.814 ms, P99 64.6 us/18.2 ms/38.0 ms, and maximum
0.373/36.3/70.3 ms. Writers continued to progress in every run.

This is an extreme saturation tradeoff, not starvation or representative HMR.
At 0/1/10 reloads/s with eight Service workers, foreground throughput was
3.304/3.241/3.249 M/s, foreground P99 6.1/6.2/5.8 us, and reload P99 at 1/10 Hz
0.332/0.496 ms, with no draining-generation accumulation.

## Invocation and control-plane closure

Identical-harness no-op invocation before/after throughput at 1/2/4/8/16/32
workers was 0.714/0.881/0.732/0.498/0.404/0.406 M/s before and
0.854/0.999/0.864/0.553/0.497/0.488 M/s after. Allocation remained exactly
8.0001 allocations and 580.01 bytes per invocation. Optimization 3 improves the
end-to-end path; shared CancellationToken ownership is not a new v2 blocker.

Event fanout 1/8/32/128 took 0.390/2.137/6.890/27.234 us and remains
approximately fanout-linear. Task spawn-and-complete throughput at
1/8/32/128/512/1000 per batch was 155/196/443/546/512/658 K tasks/s.
Generation drain with 0/1/32/128/1000 handles converged with last-drop-to-reload
latencies of 0.176/0.115/0.106/0.112/0.063 ms.

GC scans at 10k and 50k scopes had 0.041 and 0.305 ms medians. Dependency loss
at 10k total nodes completed in 2.34 ms for one affected node and 12.12 ms for
1,000 affected nodes. Both remain bounded control-plane costs and are deferred.

## Long run

A 300-second mixed soak with 32 workers completed 10,557,958 runs at 35,193
runs/s and 1.056 M component operations/s. Latency P50/P95/P99/P99.9/max was
0.815/1.926/2.714/5.333/43.221 ms. RSS was 6.85 MiB initially, 16.19 MiB peak,
8.90 MiB after workload, and 8.90 MiB after teardown. Final state was zero
Fibers, tasks, workers, generations, and draining generations, with only the
root Scope remaining. There is no logical leak.

## Decision

Optimizations 1, 2, and 3 are stable. No repeatable material data-plane problem
supports Optimization 4. CancellationToken clone, exact-gate Arc clone, Weak
upgrade, residual GenerationExecution CAS, ServiceKey hashing, ancestry Vec,
GC scan, and DependencyGraph traversal remain explicit deferred costs. Registry
sharding, Fiber state splitting, and ArcSwap publication are rejected for lack
of evidence. Track A Performance is frozen and complete.
