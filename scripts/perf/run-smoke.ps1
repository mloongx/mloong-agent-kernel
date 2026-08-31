$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Push-Location $root
try {
    cargo bench -p cordis-runtime --bench runtime --no-run
    foreach ($mode in "data-plane", "lifecycle", "mixed", "gc-stress") {
        cargo run --release -p cordis-runtime --example perf_workload -- --mode=$mode --parents=2 --children=2 --concurrency=2 --warmup=1 --measure=1
    }
    cargo run --release -p cordis-runtime --example perf_hmr -- --mode=drain --leases=8
    cargo run --release -p cordis-runtime --example perf_gc -- --total=100 --reclaimable=1
    cargo run --release -p cordis-runtime --example perf_dependency -- --total=100 --affected=1
    cargo run --release -p cordis-runtime --example perf_service -- --mode=threaded --workers=2 --operations=1000
    cargo run --release -p cordis-runtime --example perf_alloc
} finally {
    Pop-Location
}
