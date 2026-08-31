# Performance characterization methodology

The v1 characterization separates four workloads:

- `data-plane`: fixed topology; only five Service resolutions, ten no-op Invocations and ten no-op Events per run.
- `lifecycle`: Scope create/dispose plus three owned no-op Tasks; automatic GC only.
- `mixed`: data-plane plus lifecycle; automatic GC by default.
- `gc-stress`: Scope churn with an explicit `collect_garbage` call.

Warmup and measurement use the same worker count, topology and operation mix. Each worker records latency into a bounded HDR Histogram, and compatible histograms are merged after measurement. Results include P50, P95, P99, P99.9 and maximum latency without retaining every request sample.

RSS is sampled periodically by a separate thread. On Windows the sampler invokes `Get-Process`; its default one-second interval is intentionally conservative because spawning PowerShell more frequently would perturb the workload. RSS samples are paired with Runtime logical-resource snapshots. A higher final RSS is not classified as a leak when Fibers, Scopes, Tasks, workers and generations return to baseline.

Service concurrency has separate OS-thread and Tokio-task runners. Workers are created before a start barrier, each performs at least 100,000 lookups, and completion joins are outside the measured interval. Cache effect is measured by the same public path with `max_resolution_cache_entries=4096` versus zero; symbol interning is not treated as the resolution cache.

Key microbenchmarks use Criterion quick mode for exploration. Architecture decisions require repeated runs or a controlled matrix. Variance above ten percent is marked noisy.

Windows CPU sampling uses samply backed by xperf/ETW from the Windows Performance Toolkit. Raw ETL and processed profiles remain under `target/perf` and are not committed. A dedicated Cargo `profiling` profile is release-optimized with full debug information.
