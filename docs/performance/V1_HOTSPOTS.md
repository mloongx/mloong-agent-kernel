# Cordis Kernel v1 hotspot assessment

## RESOLVED BY V2 OPTIMIZATION 1

- Service resolution global-writer contention: the cache-hit read fast path
  restored isolated Scenario C scaling. The 8/4 ratio moved from 0.626 to 1.043
  and 16/4 from 0.354 to 0.987, with allocation unchanged. Registry sharding is
  not justified.

## YELLOW

- Shared caller/provider admission and key-path interner cost are visible in the
  post-optimization controls, but require fresh isolation before any change.
- Service ancestry: depth 32 is about 1.0 us with cache and 1.35 us without it. The cache is effective, while the absolute maximum-depth cost remains small.
- Dependency provider loss: cost follows both total graph and affected set. Flat total=10k/affected=5k reached 94 ms.
- GC: full-scan cost is total-size dominated, reaching 0.29 ms at 50k live scopes. The complexity is proven but its absolute cost is not yet RED.
- Memory: 1,001 fibers/scopes increased sampled RSS by roughly 8 MB in the 30 s run and did not return to the pre-topology value. This is expected while topology remains live; long post-disposal convergence was not measured.

## GREEN

- Event fan-out is close to proportional: 1 handler 0.49 us, 128 handlers 16.8 us.
- No-op invocation intrinsic latency is about 1.2 us and sustains 0.77–0.92M ops/s through 32 in-flight. The drop at 64 matches the configured admission limit.
- Short tasks sustain roughly 0.65–0.70M tasks/s through batches of 1,000, with live task count returning to zero.
- Generation drain finalization is flat through 1,000 leases and should remain unchanged.
- HMR foreground P99 impact is small in the measured 8-worker runs, and old generations do not accumulate.
- Idle reload (22 us), empty shutdown (18 us), and scope lifecycle (9 us) are small in isolation.
- At 1,000+ fibers, workload throughput remains stable for the 30 s measured interval and runtime-owned counters converge.

## Profiling status

ETW/samply recording works after installing WPT, but Rust PDB function names remain unresolved. The profile disproves CPU saturation and quantifies module shares; named-function and named-lock rankings remain open.
