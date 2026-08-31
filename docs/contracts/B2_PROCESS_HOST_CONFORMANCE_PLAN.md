# B2 reference ProcessHost conformance plan

This is the executable plan for B2.1/B2.2, not an implementation or wire-codec
specification. Tests must exercise the public ProcessHost/Runtime boundary and a
controllable peer without granting test-only Runtime authority.

## Support matrix

| Capability | Native | Reference ProcessHost v2 | Deferred |
| --- | --- | --- | --- |
| artifact load and descriptor | yes | yes | no |
| start/dispose lifecycle | yes | yes | no |
| invocation handler | native value | external format/bytes | no |
| caller cancellation/deadline | yes | yes | no |
| HMR and old-generation drain | yes | yes | no |
| bounded shutdown/crash convergence | local | yes | no |
| Service publication/consumption | yes | no | post-v2 Host capability |
| EventBus transport | local ephemeral | no | future explicit contract |
| automatic replay/reconnect | no | no | separate durable/reconnect contract |
| sandbox/container isolation | no | no | future SandboxHost/ContainerHost |

## Handshake cases

- compatible major/minor and complete required feature intersection;
- incompatible major rejects before non-handshake work;
- missing or unknown required feature rejects;
- unknown optional feature is ignored and not enabled;
- local/peer limits negotiate to the minimum and remain immutable;
- non-handshake messages before Ready are rejected; and
- a fresh connection after failure receives a fresh session and cannot reuse
  routes or requests.

## Authority-negative cases

A malicious or broken peer sends fake `ScopeId`, `FiberId`, generation, handler
ID, or `ServiceSymbol`; attempts to self-publish a capability; uses a stale route;
or reuses a remote object from another session. No case gains registry access,
changes the generation selector, or creates lifecycle authority. Capability
declarations must pass through the exact staged local `Context`, and declarations
after topology sealing are rejected.

## Invocation cases

- external request/response success and format preservation;
- stable remote domain failure distinct from Host failure;
- transport close and process exit publish at most one terminal local completion
  for each tracked call, without claiming exactly-once remote execution;
- request too large rejected before unbounded buffering;
- response too large and control message too large;
- unsupported format classified as protocol/value compatibility failure;
- local overload before remote acceptance versus failure after acceptance;
- cancellation before response and response before cancellation;
- duplicate Cancel idempotence and caller completion without CancelAck;
- late response after cancellation is drained;
- local deadline, propagated remaining budget, and late response after deadline;
- host crash during invocation with no transparent replay; and
- maximum inflight and bounded pending-map behavior under concurrency.

The race oracle is always the Runtime-local request state, monotonic deadline,
generation gate, or shutdown admission boundary—not remote packet timestamps.

## Lifecycle cases

B2.1A already provides public fixtures for caller drop and Host failure during
pre-acceptance `install_hosted`/`reload_hosted_detailed` load, plus hosted reload
cutover and typed wrapper preservation. B2.1B must repeat preparation cancellation
with a real child and prove the child/session is reclaimed.

- crash or protocol failure during load;
- crash, domain failure, or protocol failure during start;
- unsupported non-empty Service dependency/provision set is rejected;
- capability declarations remain hidden until local activation commit;
- active-session crash rejects new work and Runtime converges the proxy;
- dispose success and domain cleanup issue;
- dispose transport failure after committed lifecycle truth; and
- objectively live/unreaped process produces an incomplete blocker.

## HMR cases

- replacement Host failure before local commit leaves old generation authoritative;
- successful replacement commits only at the Runtime selector;
- old admitted invocation continues while post-cutover work uses replacement;
- old Host cleanup failure after commit cannot roll back replacement;
- replacement crashes immediately after cutover: committed cutover remains truth,
  new proxy converges failed, and old generation is not silently restored; and
- no new request enters an old drained session.

## Shutdown cases

- graceful hosted shutdown under the one Runtime absolute deadline;
- Host ignores stop, force termination succeeds, and a cleanup issue is reported;
- process already crashed is observed and reaped without hanging;
- force termination fails or child remains unreaped, producing an incomplete
  Host blocker;
- peer request for more grace cannot extend the attempt;
- shutdown-first rejects new Host work while admitted-first work converges; and
- no Host worker, transport task, request, or child remains after terminal
  `Complete`/`CompleteWithIssues`.

## Contract-to-suite map

| Contracts | Planned executable suite |
| --- | --- |
| `HST-001`, `HST-003`, `WIR-002`, `WIR-005`, `WIR-011` | authority-negative/session isolation |
| `HST-004`, `WIR-006` | domain/Host/protocol failure and crash |
| `HST-005`, `WIR-007`, `WIR-008` | invocation cancel/deadline/race |
| `HST-006` | hosted capability staging/publication |
| `HST-007` | remote HMR prepare/cutover/drain/cleanup |
| `HST-008` | hosted shutdown and forced termination |
| `HST-009` | pre-acceptance preparation cancellation and child/session cleanup |
| `WIR-004` | handshake negotiation |
| `WIR-009`, `WIR-010` | backpressure, message limits, external payload |

Native conformance remains the semantic reference for shared lifecycle,
invocation, HMR, and shutdown outcomes. Host-only tests add failures and negative
authority cases that Native execution cannot express.

## Explicit non-goals

Reference ProcessHost v2 does not provide remote Services, remote EventBus,
distributed scheduling, multi-node Runtime, transparent reconnect, automatic
invocation replay, durable workflow checkpoint/replay, SandboxHost, container
isolation, network security/TLS/PKI, MCP, A2A, Node Host, WASM Host, or a
multi-tenant scheduler.

## B2.1B implemented foundation

`tests/process_host_conformance.rs` and the private fixture now execute
handshake compatibility, required features, absolute header rejection before
allocation, bounded artifact and descriptor handling, real lifecycle over child
stdio, typed activation/cleanup failures, preparation cancellation kill/reap,
and dropped graceful-cleanup force kill/reap. Codec unit tests cover unknown
tags, trailing bytes, truncation, and oversized nested lengths.

Invocation, Cancel, Deadline, active-crash-to-Fiber convergence, hosted HMR
crash hardening, and full Runtime shutdown blocker integration remain B2.1C or
B2.1D work.

## B2.1B.1 hardening evidence

Private deterministic tests cover immutable terminal winners, close/fail races,
failure between HelloAck and Ready, terminal pending drain, 10,000 dropped
request futures under a four-request limit, spawn-guard cleanup, out-of-order
dispatch, and reader/writer/supervisor convergence. A 200-session real-child soak
returns actor counters and pending state to baseline. Process conformance also
requires an exit after Shutdown but before ShutdownAck to remain a typed cleanup
failure rather than clean close.

## B2.1B.2 hardening evidence

Deterministic tests cover nonwrapping request-ID exhaustion, concurrent unique ID
allocation, monotonic high-watermark publication, exact state admission, Failure
as the universal typed response, and terminal wrong-kind correlation. Real peers
advertising smaller and larger inflight limits prove the effective minimum is
actively enforced in both directions. Batched reverse-order peers exercise 10,240
responses in one session; cancellation restores negotiated capacity across
10,000 dropped requests; and a 50-session/16,000-request soak returns actor,
pending, permit, and child ownership to baseline.

Invocation, wire Cancel and Deadline, remote Service/Event, and active-crash to
Fiber convergence remain outside B2.1B.2.

## B2.1C executable invocation evidence

The private minor-1 peer negotiates separate supported/required Lifecycle,
Invocation, Cancel, and Deadline features. Bounded Started declarations register
through the exact staged Context. Public process tests cover External echo,
Native rejection, structured domain details, UnsupportedFormat, independent
request/response limits, immediate inflight overload, cancellation capacity
recovery, local deadline winner and late-result discard, terminal protocol
failure, crash without replay, 10,240 out-of-order calls, and real old/new
process HMR isolation. Private tests add terminal fanout, actual-write budget
derivation, and 10,000 drop/remove/Cancel operations.

Remote Service/Event remain deferred. B2.1D closes active Host failure through
an exact-generation Runtime worker and closes hosted shutdown force/reap/retry.
