# Host session and hosted lifecycle contract

This document fixes the transport-independent semantics for a Cordis execution
host. It does not select a process topology, transport, codec, task model, or
security sandbox. The reference ProcessHost implementation must preserve these
rules while presenting a local `NativePlugin` lifecycle proxy to Runtime.

## Identities

Three identities are deliberately separate:

- **Host kind** is an adapter-family diagnostic such as `process`, `wasm`, or
  `remote-rpc`. A peer-declared kind never grants authority.
- **Host session** is one Runtime-local transport relationship with immutable
  negotiated version, features, and limits. Its identity is diagnostic and
  lifetime-scoped, not a Runtime capability.
- **Remote object route** is an opaque token that addresses one remote plugin
  object inside exactly one session. It cannot address Runtime registries,
  survive session replacement, or grant local authority.

Local `ScopeId`, `FiberId`, `ServiceSymbol`, generation values, handler IDs, and
`InvocationId` are never accepted from a peer as permission, ownership proof, or
routing authority.

## HostSession state machine

| State | Permitted work | Legal next states |
| --- | --- | --- |
| `Created` | establish the transport relationship only | `Handshaking`, `Failed` |
| `Handshaking` | version, feature, and hard-limit negotiation only | `Ready`, `Failed` |
| `Ready` | admitted load/start/invoke/control work within negotiated limits | `Draining`, `Closed`, `Failed` |
| `Draining` | finish accepted work; cancel, dispose, or shutdown control; no new ordinary work | `Closed`, `Failed` |
| `Closed` | observation and resource reaping only | none |
| `Failed` | observation, failure delivery, termination, and reaping only | none |

`Closed` and `Failed` are terminal. Protocol violation, terminal transport loss,
or process exit transitions every non-terminal state to `Failed`. Every locally
tracked unresolved request then publishes at most one terminal local completion. A known
session failure must not leave requests waiting for an unrelated invocation
timeout.

This is not an exactly-once remote-execution guarantee. At failure, remote work
may be not started, partially executed, or complete with its response lost.

There is no transparent session resurrection. A replacement connection is a new
session with new negotiated parameters and new remote routes. Recovery requires
a Runtime-owned restart or reload and a fresh generation; old routes and
generation authority are never revived.

## Authority table

| Concept | Runtime | Host | Wire | Authority? |
| --- | --- | --- | --- | --- |
| Scope | owns tree, state, and admission | may observe host-local context only | must not carry `ScopeId` as permission | Runtime only |
| Fiber | owns lifecycle and cleanup | executes one proxy instance | must not carry `FiberId` as permission | Runtime only |
| Generation | owns identity, selector, gate, and drain | executes accepted old/new work | may carry opaque route association only | Runtime only |
| Plugin route | binds a route to one proxy/session | allocates or recognizes token | carries opaque session-scoped token | correlation/routing only |
| Invocation correlation | creates local invocation and wire mapping | correlates response/cancel | carries `WireRequestId` | no lifecycle authority |
| Capability registration | validates, stages, seals, publishes | declares requested invocation capability | typed declaration only | Runtime through local `Context` |
| HMR commit | owns the selector transaction | reports replacement preparation | readiness/result messages only | Runtime only |
| Shutdown completion | owns attempt deadline and outcome | cooperates with stop/dispose | bounded control and observations | Runtime only |

Remote input containing fake local IDs, an old route, or a route from another
session is rejected and can never mutate Runtime truth.

## Hosted plugin lifecycle

### Preparation versus Runtime acceptance

Artifact load, transport/session establishment, handshake, and descriptor
discovery are Host preparation and may occur before Runtime lifecycle acceptance.
They have zero Scope, Fiber, generation, Context, registry, or publication
authority. A spawned and handshaken child is still only a prepared external
resource.

In the current API, successful Runtime worker creation inside `install_owned` or
`reload_owned` is the lifecycle-acceptance boundary. Dropping a caller during
`PluginHost::load` therefore cancels the pre-acceptance observer future and no
Runtime-owned lifecycle operation continues. The Host implementation must make
that preparation cancellation-safe and reclaim partially created external
resources. After Runtime acceptance, existing `OWN-001` observer-drop semantics
apply and Runtime-owned convergence continues.

The reference host supports artifact load, descriptor retrieval, start, remote
invocation using `InvocationValue::External`, cancellation, deadline propagation,
disposal, HMR, shutdown, and crash reporting. A remote descriptor may describe
dependencies or provisions, but B2.1 must reject a non-empty remote Service
capability set with a structured unsupported-capability failure. It must never
silently ignore those fields.

During remote start, capability declarations flow through the local proxy's
exact staged `Context`. The Context itself never crosses the wire. For an
invocation declaration, the proxy calls `Context::handle_invocation` with a
local remote-handler proxy. Runtime therefore retains exact Scope, generation,
gate, topology-seal, and publication semantics. A declaration received after
start/topology sealing is rejected; the peer cannot write a registry directly.

An active session failure makes the lifecycle proxy terminal: new proxy work is
rejected, accepted requests fail exactly once unless a result already won, and
Runtime owns legal Fiber convergence and disposal. The Host cannot declare a
Fiber disposed or a shutdown complete. B2.1 must map this convergence onto the
existing legal Fiber transitions rather than add a second remote state machine.

## HMR across the boundary

Replacement Host preparation may finish before the reload transaction is
accepted. Only proxy start and local capability registration occur inside the
hidden staging generation. A remote `Started` or `Ready` result means
only that remote preparation completed; it is not the cutover.

- Failure before local commit rolls back staging and leaves the old generation
  authoritative.
- Runtime alone commits the generation selector.
- After commit, new work selects the replacement while already admitted old work
  may finish under the exact old generation lease.
- Old-session disposal or cleanup failure after commit is a committed cleanup
  issue and cannot roll back the new generation.
- If the new host fails immediately after cutover, the committed generation
  remains the historical cutover truth and Runtime converges it as a failed
  active proxy. It does not silently restore the old generation.

## Hosted shutdown

Runtime first closes admission permanently. It then sends graceful stop/dispose
with the remaining budget while retaining one local absolute monotonic deadline
for the entire attempt. A peer cannot extend that deadline.

At expiry the reference ProcessHost force-terminates and reaps its child. If the
process is confirmed dead and reaped, shutdown may converge with a cleanup issue.
If it remains objectively live or unreaped, shutdown is `Incomplete` with a
Host-process/session blocker. A successful `Complete` or `CompleteWithIssues`
outcome permits no post-completion Host worker, transport task, request, or child
owned by that Runtime.

## Reference topology and security

B2.1 should use one HostSession and child process per hosted plugin generation.
This simplifies failure attribution, HMR, and shutdown, but it is an implementation
choice rather than a Host contract; future hosts may multiplex safely.

Process isolation is not a sandbox. The reference host promises no filesystem,
network, syscall, secret, CPU, or memory isolation. For a local child, peer
provenance may come from a parent-created transport and child handle. A
self-declared name or Host kind remains diagnostic. Sandbox/container policy and
network authentication are separate future contracts.

## Joint-state constraints

| Runtime generation | HostSession | Remote plugin | Meaning |
| --- | --- | --- | --- |
| staging | `Ready` | started | legal; capabilities remain locally hidden |
| active | `Ready` | started | legal; new work may be admitted locally |
| draining | `Ready` or `Draining` | disposing/started | legal only for accepted old work and cleanup |
| active | `Failed` | unknown/terminal | convergence must be in progress; new work rejected |
| disposed | non-terminal with admitted work | any | illegal; Runtime has not converged owned Host work |

It is also illegal to publish local capabilities before local commit, route new
work to a drained old generation, or treat remote `Started` as Runtime activation
truth.

## B2.1B.1 terminal publication and actor convergence

`Closed` and `Failed` are immutable competing terminal outcomes selected by one
CAS-based transition primitive. Neither can transition to the other or return to
Created, Handshaking, Ready, or Draining. Non-terminal transitions validate their
exact source state, so a failure between HelloAck and Ready cannot resurrect the
session.

Terminal publication resolves every locally pending request and synchronously
signals reader/writer actor stop. Clean `Closed` requires a matching ShutdownAck,
a successful child exit, and reap; merely sending Shutdown is not clean-exit
authority. The unique SessionOwner joins the supervisor, which first joins reader
and writer, before graceful cleanup returns.

Request-observer cancellation is independent of remote Cancel semantics. A
private RAII pending registration removes its entry whenever the local request
future is dropped, and its inflight permit is released by ordinary future stack
unwinding. This preserves `pending <= max_inflight_requests` under arbitrary
observer cancellation.

## B2.1B.2 request admission and correlation

The reference request allocator is session-local and nonwrapping. Concurrent
allocation publishes a monotonic issued-ID high-watermark, so an older allocator
cannot regress the future-response boundary. ID exhaustion rejects locally with
a typed Host failure and does not reuse an identity.

Handshake, ordinary lifecycle work, and shutdown have exact admission states:
Handshaking, Ready, and Draining respectively. After HelloAck, the implementation
removes permits until the semaphore equals the immutable minimum of local and
peer inflight policy. Cancellation removes the pending entry and restores one
permit. A live response must be either the request's matching success kind or a
typed Failure; another success kind fails the entire session as a protocol
violation.

## B2.1B executable session ownership

The private reference session implements `Created -> Handshaking -> Ready ->
Draining -> Closed`, with failure from each non-terminal state to `Failed`.
`Closed` and `Failed` are terminal; there is no respawn, reconnect, or replay.

Only the supervisor owns `tokio::process::Child` and performs wait, forced kill,
and reap. Reader and writer own stdout and stdin respectively. Cloneable clients
issue requests but do not own process lifetime. A non-clone session owner is the
unique lifetime token; its synchronous Drop signals force termination and the
supervisor then kills and reaps.

`ProcessHost::load` owns this token through spawn, handshake, load, and descriptor
validation. `RemotePluginProxy::start` first moves it into a Fiber-owned Effect,
then sends remote Start. Cleanup sends Dispose, Shutdown, and waits for reap;
dropping cleanup signals forced cleanup. An active crash makes the session
terminal and a staged Fiber-owned watcher submits the exact generation-bound
Host fact to a Runtime worker, so HST-004 is
still partial.

## B2.1C invocation ownership

Remote declarations remain preparation until registered through the staged
local `Context`; the Runtime generation gate is their only publication and
admission authority. Normal HMR sends new calls to the replacement session while
an admitted old call retains its old generation lease and session until drain.

Local cancellation or deadline drops the bounded pending observation, sends
best-effort Cancel, and cannot be reversed by a late result. Remote domain errors
are request-scoped. ProtocolViolation, HandshakeIncompatible, TransportClosed,
ProcessExited, and ProcessKilled reported by a peer are session-terminal and fan
out to all pending requests. Other typed invocation Host outcomes remain
request-scoped.

An active child crash terminates pending calls and rejects later remote calls
without replay. Runtime-owned Fiber convergence follows the existing
`Active/Reloading -> Failed -> Disposing -> Disposed` path;
transport actors receive no Fiber, Scope, registry, or lifecycle authority.
