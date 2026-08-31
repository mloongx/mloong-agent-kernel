$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$output = Join-Path $root "target\perf\ENVIRONMENT.md"
New-Item -ItemType Directory -Force (Split-Path $output) | Out-Null
$os = Get-CimInstance Win32_OperatingSystem
$cpu = Get-CimInstance Win32_Processor | Select-Object -First 1
$rust = rustc -Vv | Out-String
$cargo = cargo -V
$head = git -C $root rev-parse HEAD
$tag = git -C $root tag --points-at HEAD
@"
# Cordis performance environment

- OS: $($os.Caption) $($os.Version) $($os.OSArchitecture)
- CPU: $($cpu.Name)
- Physical cores: $($cpu.NumberOfCores)
- Logical cores: $($cpu.NumberOfLogicalProcessors)
- Maximum reported frequency: $($cpu.MaxClockSpeed) MHz
- RAM: $([math]::Round($os.TotalVisibleMemorySize / 1MB, 2)) GiB
- Rust: $($rust.Trim() -replace "`r?`n", "; ")
- Cargo: $cargo
- Build profile: Cargo bench/release (optimized)
- Features: default unless a result states otherwise
- Git commit: $head
- Tags at commit: $($tag -join ", ")
"@ | Set-Content -Encoding utf8 $output
Write-Output $output
