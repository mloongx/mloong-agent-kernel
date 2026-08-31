$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$profile = Join-Path $root "target\perf\data-plane-c64.json.gz"
New-Item -ItemType Directory -Force (Split-Path $profile) | Out-Null
$wpt = "C:\Program Files (x86)\Windows Kits\10\Windows Performance Toolkit"
$env:PATH = "$wpt;$env:PATH"
Push-Location $root
try {
    cargo build --profile profiling -p cordis-runtime --example perf_workload
    samply record --save-only --unstable-presymbolicate -o $profile -- target\profiling\examples\perf_workload.exe --mode=data-plane --parents=100 --children=10 --concurrency=64 --warmup=5 --measure=30
} finally {
    Pop-Location
}
Write-Output $profile
