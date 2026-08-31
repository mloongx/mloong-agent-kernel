# Reference ProcessHost Full Conformance

Canonical command:

```text
cargo test -p cordis-runtime --test process_host_full_conformance
```

The oracle composes the public transport, invocation, and lifecycle suites and
adds static authority-negative checks over the private reference codec and Host
implementation. The codec is an implementation detail, not a Cordis ABI.

| Contract | Class | Executable evidence | Result | Notes |
| --- | --- | --- | --- | --- |
| HST-001 | authority | publication, staged activation, authority-negative audit | PASS | Runtime retains Scope, Fiber, generation and registry authority. |
| HST-002 | failure | typed load/start/dispose/crash tests | PASS | Host facts cannot self-declare lifecycle success. |
| HST-003 | session | immutable terminal races, fresh-session HMR | PASS | No reconnect, resurrection, or inherited route authority. |
| HST-004 | convergence | active/postcommit/old-drain crash tests | PASS | Exact-generation Runtime worker owns convergence; no replay. |
| HST-005 | winner | cancellation/deadline/late-result tests | PASS | Runtime remains result-winner authority. |
| HST-006 | capability | bounded declarations, feature rejection, wire audit | PASS | Declarations publish only through the exact staged Context. |
| HST-007 | HMR | old/new isolation and crash matrix | PASS | Cutover is irreversible; old authority is never resurrected. |
| HST-008 | shutdown | clean, ignore, force/reap, blocker/retry tests | PASS | One Runtime deadline; `HostedExecution` only while live/unreaped. |
| HST-009 | preparation | handshake/load cancellation and rollback tests | PASS | Pre-acceptance failure leaves no Runtime operation or child. |
| WIR-002 | authority | schema field audit | PASS | No Runtime identity/authority type crosses the wire. |
| WIR-004 | negotiation | version/features/min-limits/state tests | PASS | Negotiated features and limits are immutable before Ready. |
| WIR-005 | request ID | monotonic/nonwrap/future/late/out-of-order tests | PASS | IDs are opaque and session-scoped. |
| WIR-006 | errors | typed domain/Host/protocol tests | PASS | Error classes remain structurally distinct. |
| WIR-007 | deadline | actual-write budget and timeout races | PASS | Relative budget only; Host cannot extend Runtime deadline. |
| WIR-008 | cancel | one-way/drop/late-result/10k cancellation tests | PASS | No CancelAck requirement or resurrection. |
| WIR-009 | bounds | frame/payload/queue/pending/declaration/inflight tests | PASS | Every peer-controlled collection is bounded before allocation. |
| WIR-010 | payload | echo/Native rejection/format/size tests | PASS | External format plus immutable opaque bytes only. |
| WIR-011 | route | real-process HMR and authority audit | PASS | Routes are opaque, session-local, and non-authoritative. |

Reference ProcessHost admitted scope has zero PARTIAL and zero UNRESOLVED
contracts. Remote Service (`SVC-005`) remains a placeholder and remote Event
remains deferred; neither belongs to the admitted scope.

## Hostile-peer coverage

The private codec tests reject oversized pre-handshake and negotiated headers,
truncated header/body, unknown tags, trailing bytes, invalid nested lengths,
array/declaration/descriptor overflow, malformed structured errors, wrong state
and response kind, future IDs, and terminal-state resurrection. Length headers
are validated before payload allocation. Results: zero parent panic, zero hang,
and zero unbounded allocation.

## Enforced bounds

| Resource | Bound |
| --- | --- |
| Pre-handshake frame | 64 KiB absolute cap |
| Negotiated frame | minimum accepted frame limit |
| Control frame | configured control cap and negotiated frame cap |
| Artifact | negotiated artifact limit |
| Descriptor string | 4 KiB |
| Descriptor arrays | 256 items |
| Invocation declarations | 256 items |
| Request/response payload | independent negotiated limits |
| Outbound queue | `ProcessHostConfig::outbound_queue_capacity` |
| Pending/inflight | negotiated `min(local, peer)` semaphore |
| Fixture active invocations | negotiated inflight limit |

There is no peer-controlled unbounded collection. Process count remains caller
admission policy rather than a hidden Host-global limit.

## Locking and authority audit

Session/pending locks are released before submitting a Runtime operation. Host
reader, writer, supervisor, client, and owner code contains no `FiberCell`,
`RuntimeInner`, `ScopeRegistry`, or generation-selector authority. Runtime
registry/Fiber guards are not held across Host or user awaits. No new lock-graph
edge was introduced by the conformance closure.

## Stress evidence

- 50,000 mixed success/domain/cancel/timeout operations at concurrency 32.
- 500 real child sessions with zero reader/writer/supervisor/pending leak.
- 300-second mixed invocation and lifecycle soak: 559,858 operations, 1,866
  ops/s, P50 1.853 ms, P95 4.775 ms, P99 15.687 ms; RSS 5,660,672 bytes
  start, 9,883,648 bytes peak, and 9,728,000 bytes end. The same window
  exercised install, reload, old-call drain, crash disposal, and shutdown.
- Critical process race matrix: 50/50 for preparation drop, invocation crash,
  deadline/late response, immediate postcommit crash, old-drain crash,
  dispose, shutdown, force/reap, and retry.

ProcessHost is not a sandbox. It provides no filesystem, network, syscall,
memory, CPU, or secret isolation.
