# Profiling record

## Environment and tools

- Windows 11 10.0.26200 on Intel i7-12650H, 10 physical and 16 logical cores.
- samply 0.13.1 with Windows Performance Toolkit/xperf.
- Sampling target: release-equivalent `profiling` build with full debug information.

The data-plane/8 trace completed successfully. xperf's module report attributed approximately 23.4% of whole-system samples to the target process: 9.76% kernel, 6.34% `ntdll`, 6.64% user executable, plus small runtime-library shares. The system Idle share was 49.6%. This is strong evidence that the 8-worker throughput fall was not caused by whole-machine CPU saturation.

Rust function names were not decoded by the installed xperf/samply PDB path even with full debug information and a generated symcache. Consequently there is no defensible top-function list or named-lock wait table. Module-level samples and controlled workload comparisons are usable; address-only `fun_<rva>` entries are not presented as functions.

WPAExporter also failed in this host's headless session while initializing WPF fonts. Raw traces are intentionally excluded from Git.

Additional 64-worker and mixed captures could not be completed in the current
non-elevated session: xperf/samply required administrator privileges after the
successful 8-worker capture. The profiling script and debug-symbol profile are
checked in, but this environmental limitation is not treated as function-level
evidence.

## Contention evidence

- Steady Service throughput peaks near 2.4M lookups/s at 2–4 workers and falls to roughly 0.85–0.90M at 16 workers.
- OS-thread and Tokio-task curves closely agree, and worker creation is outside timing.
- ETW shows substantial idle CPU during the 8-worker data-plane profile.

Together these support a shared synchronization or shared-memory bottleneck in the Service resolution path. They do not identify GenerationExecution, ServiceRegistry or ScopeRegistry individually. Named-lock attribution remains an evidence gap.

## Allocation profiling

`stats_alloc` wraps the allocator only in the benchmark example. For 10,000 public-path operations it measured:

| Path | Allocations/op | Bytes/op |
| --- | ---: | ---: |
| Service lookup | 1.00 | 32 |
| Invocation | 8.00 | 580 |
| Event | 3.00 | 172 |
| Task spawn/completion | 4.00 | 720 |
| Scope lifecycle | 12.00 | 1,936 |

These are path-level counts, not callsite stacks. Scope lifecycle is the largest measured allocator consumer; Invocation is the largest data-plane allocator consumer.
