# Cordis Kernel v1 performance baseline

Freeze commit `12de9831b77680bc218f23ccfd412fd420bb44ee` is tagged `cordis-kernel-v1.0-baseline`. Measurements used Criterion 0.7 quick mode unless stated otherwise. Quick mode is suitable for characterization but the checked-in values are not a CI regression threshold.

## Microbenchmarks

| Path | Result |
| --- | ---: |
| Context clone | 19.5 ns |
| Service root lookup | 319 ns |
| Service parent lookup | 338 ns |
| Service symbol lookup in this run | 497 ns |
| Service depth 1 / 4 / 8 / 16 / 32 | 377 / 416 / 713 / 653 / 913 ns |
| Event 1 / 8 / 32 / 128 handlers | 491 ns / 1.56 / 4.29 / 16.81 us |
| Scope create + dispose + GC | 9.06 us |
| Fiber install + dispose + GC | 27.08 us |
| Invocation no-op | 1.20 us |
| Invocation throughput, 1 / 8 / 32 / 128 in-flight | 766k / 892k / 703k / 635k ops/s |
| 100 task spawn + completion | 149.7 us (about 1.50 us/task) |
| Idle reload | 22.1 us |
| Empty shutdown | 17.8 us |

Scope depth above 32 is N/A: frozen v1 enforces a hard depth limit of 32. The service symbol result was not faster than the key path in this run, so a cache-hit benefit is not established.

The threaded service batch benchmark measured 2.52M ops/s at one worker, 2.32M at 2, 2.02M at 4, 1.77M at 8, 1.00M at 16, 1.11M at 32, 1.04M at 64 and 0.90M at 128. Thread start/barrier cost is amortized over 2,000 operations per worker but remains part of this end-to-end batch; it proves scaling degradation, not its lock-level cause.

## Synthetic workload

The workload contains 100 parent scopes, 1,000 child scopes/fibers, 50 services, 20 invocation handlers and 20 event handlers. Each run performs 5 service lookups, 10 invocations, 10 event emissions, 3 owned task spawns and one scope create/dispose (29 logical operations).

The primary steady-state run used 5 s warmup, 30 s measure and concurrency 32: 22,902 runs/s, 664,168 ops/s, P50 1.121 ms, P95 3.341 ms and P99 5.073 ms. RSS was 8.22 MB before and 16.29 MB at the end sample. Live tasks, Runtime workers, draining generations and provider inflight all converged to zero.

| Concurrency | Runs/s | Scaling vs 1 | P50 | P95 | P99 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 10,895 | 1.00x | 0.070 ms | 0.183 ms | 0.285 ms |
| 8 | 24,532 | 2.25x | 0.279 ms | 0.629 ms | 1.132 ms |
| 32 | 22,902 | 2.10x | 1.121 ms | 3.341 ms | 5.073 ms |
| 64 | 26,873 | 2.47x | 1.662 ms | 7.518 ms | 10.170 ms |
| 128 | 26,923 | 2.47x | 5.049 ms | 9.198 ms | 12.021 ms |
| 256 | 26,396 | 2.42x | 9.087 ms | 17.082 ms | 21.373 ms |

Only concurrency 32 used the full 30 s window; the other scaling points used 1 s warmup and 3 s measurement. Throughput plateaus around 64 workers while oversubscription sharply worsens tail latency.

## Phase 0.2 controlled results

The old workload's per-run manual GC reduced mixed throughput by 22.6%, 23.7% and 23.2% at 32, 64 and 128 workers. Auto-GC-only mixed throughput was stable near 33.6k–34.5k runs/s; manual GC every 100 runs cost only 1.2%–1.9%.

Data-plane throughput peaked at 75.2k runs/s at four workers in the exploratory matrix, then fell to 35.0k at eight and plateaued near 29.6k–32.6k through 256. ETW reported 49.6% system Idle during an 8-worker profile, so whole-machine CPU saturation is disproven as the sole cause.

An independent three-run confirmation after switching latency storage to HDR
histograms measured median 59.7k/38.5k/36.3k runs/s at 4/8/16 workers, with
2.6%/3.9%/1.9% ranges. Absolute values differ from the exploratory run, but the
four-worker peak and eight-worker cliff are reproducible.

Steady Service lookup, with worker creation outside timing, peaked near 2.4M ops/s at 2–4 workers and fell to 0.85–0.90M at 16. OS-thread and Tokio-task curves closely match. At depth 32, disabling the resolution cache cost about 1.35 us/lookup versus 1.01 us with the cache enabled.

Generation drain finalization remained roughly 0.06–0.20 ms across 0–1,000 retained handles. Dropping handles itself rose from sub-microsecond at 1–8 to 22.8 us for 1,000.

GC live-scan medians were 2.8 us at 1k scopes, 14.5 us at 5k, 42.4 us at 10k, 126.6 us at 25k and 291.4 us at 50k. The observed model is total-size dominated.

Flat DependencyGraph provider loss has both components: with affected=1 it rose from roughly 0.15–0.22 ms at total=100 to 2.4–2.8 ms at total=10k; with total=10k it rose to 11.0/23.8/94.1 ms for affected=500/1k/5k.

Allocation counts and HMR results are recorded in `PROFILING.md` and `V1_CHARACTERIZATION.md`.

The 600-second mixed/32 stability run sustained 33,891 runs/s with steady RSS between 16.94 and 17.58 MB. Final Runtime resources returned to the root-only baseline; final RSS was 9.87 MB versus 5.28 MB initial, classified as allocator retention rather than a logical leak.
