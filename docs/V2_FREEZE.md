# Cordis Kernel v2.0 freeze

- Freeze date: 2026-09-01
- MSRV: Rust 1.85.0
- Freeze commit and annotated tag are recorded after final gates; the tag name is
  `cordis-kernel-v2.0`.
- Contract inventory: 70 total; 35 PUBLIC, 12 INTERNAL, 11 HOST, 10
  WIRE-CANDIDATE, 2 PLACEHOLDER; zero PARTIAL and zero UNRESOLVED.

Native semantics are frozen by the public conformance oracle. Reference
ProcessHost supports artifact load, plugin start, `InvocationValue::External`,
typed remote-domain and Host failures, protocol failure, Cancel, relative
deadline propagation, bounded backpressure, process-crash convergence, hosted
HMR, hosted shutdown, force kill and reap.

It does not support remote Service, remote EventBus, transparent reconnect,
automatic replay, multi-node/distributed Runtime, or sandboxing. ProcessHost is
a process/failure boundary, not a security sandbox; it promises no filesystem,
network, syscall, memory, CPU or secret isolation.

Runtime-owned lifecycle convergence survives observer drop within the running
process. Durable Action C0-C13 belongs above the Kernel and must prevent or
reconcile duplicate external side effects. V2 does not persist whole Runtime
state across restart, checkpoint workflows, guarantee remote execution or side
effects exactly once, or transparently replay invocation.

Resource bounds and ProcessHost stress evidence are authoritative in
`contracts/B2_PROCESS_HOST_CONFORMANCE.md`; performance baselines are regression
references under `performance/`, never SLA. The stable AgentLayer dependency
surface is defined by `AGENT_LAYER_BOUNDARY.md`.

## Resource bounds

| Resource | Bound and enforcement |
| --- | --- |
| pre-handshake frame | 64 KiB absolute header check before body allocation |
| negotiated/control frame | peer/local minimum plus configured control cap |
| artifact bytes | `ProcessHostConfig::max_artifact_bytes` before spawn/load |
| descriptor strings/arrays | 4 KiB / 256 during decode |
| invocation declarations | 256 during descriptor decode |
| request/response payload | independent negotiated maxima |
| outbound queue | configured bounded channel capacity |
| pending/active remote invocation maps | negotiated inflight semaphore |
| Runtime Fibers/Scopes/Tasks | `RuntimeConfig` quotas |
| Host child processes | caller lifecycle admission; every accepted session is owned and reaped |

No peer-controlled collection allocates before its length/count bound is
validated.

## Cancellation ownership

| Operation | Pre-acceptance caller drop | Post-acceptance observer drop | Owner |
| --- | --- | --- | --- |
| install/reload | cancels Host preparation; external resources reaped | commit/rollback continues | Runtime transaction |
| dispose/shutdown | unpolled future registers nothing | registered convergence continues | Runtime supervisor |
| `PluginHost::load` | caller-owned preparation is cancelled and reaped | returned proxy is Fiber-owned | caller, then Runtime Fiber |
| remote invocation | cancels observation and best-effort sends Cancel | local winner remains terminal; no replay | caller invocation state |
| remote Cancel | one-way best effort | no CancelAck ownership | session transport |
| Effect cleanup | not started before lifecycle acceptance | continues under disposal/shutdown | Runtime lifecycle |

Runtime lifecycle operation ownership is deliberately different from
caller-owned invocation observation.

## Panic, TODO and unsafe audit

Production contains nine panic-like sites: one impossible reserved atomic state,
one checked diagnostic-counter overflow, six accesses immediately following
validated staging/target membership, and construction from the statically valid
default configuration. They are proved internal invariants, not peer-input
paths. Host/Wire peer-controlled panic sites are zero. Unknown core TODO/FIXME
markers are zero, and production `unsafe` occurrences are zero.

Known post-v2 work: remote Service, remote EventBus, distributed scheduler,
checkpoint/replay, whole-execution durability, reconnect/replay, SandboxHost,
ContainerHost, Node Host, WASM Host, advanced CPU/memory quotas, full OTel
propagation, Service interner redesign, Scope ancestry optimization, dependency
reverse index and specialized GC reclaim queue. None is a v2 blocker.

After freeze, v2 enters contractual maintenance mode: correctness, security,
compatibility and measured-regression fixes only. New capability belongs to
post-v2/v3; AgentLayer is the next development target.

## Final freeze soak

The final freeze reran 300 seconds rather than 600 because B2.2 had already
independently completed a prior clean 300-second ProcessHost soak. The final run
kept the ProcessHost workload at 32 workers while concurrently running eight
Native mixed workers covering service lookup, native invocation, Event, Task and
GC; shutdown ran as an independent final phase.

- Native: 9,052,062 logical runs / about 271.6 million primitive operations,
  905,204 ops/s; P50 0.246 ms, P95 0.451 ms, P99 0.641 ms; RSS 5,365,760 start,
  9,662,464 peak and 7,946,240 bytes final.
- ProcessHost: 563,947 operations, 1,879 ops/s; P50 1.865 ms, P95 3.819 ms,
  P99 13.479 ms; RSS 5,672,960 start, 9,834,496 peak and 9,715,712 bytes end.
- Final state: zero Fiber, non-root Scope, Task, RuntimeWorker, Generation,
  draining generation, Host child, Host reader/writer/supervisor/watcher and
  pending request. No logical or resource leak was observed.
