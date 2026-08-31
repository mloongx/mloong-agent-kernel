# Host logical protocol contract

This document defines transport-independent message semantics for a hosted
plugin. It deliberately does not freeze a codec, framing, opcode numbering,
transport, UUID representation, process topology, thread count, or task layout.
B2.1 must choose bounded representations that implement this model.

## Negotiation

A protocol version is logically `(major, minor)`. Different major versions are
incompatible. For the same major, Runtime selects the highest mutually supported
minor for which every required semantic feature is present. Minor equality alone
does not imply feature compatibility.

Handshake exchanges:

- supported protocol ranges;
- supported and required features;
- hard limits; and
- enough peer/provenance diagnostics to bind the transport to a fresh session.

The reference feature set is lifecycle, external invocation, cancellation,
deadline, HMR, and shutdown. Remote Service and Event are not B2.1 features.
Runtime is negotiation authority: it computes the intersection and publishes the
accepted immutable session parameters. An unknown optional feature is not
negotiated; an unknown required feature rejects the handshake. No behavior is
silently enabled.

Effective numeric limits are the minimum of local policy and the peer's accepted
limit. At minimum the session fixes maximum inflight requests, request bytes,
response bytes, and control-message bytes. An implementation may also bound
plugin instances per session. Limits cannot increase during a session.

## Correlation and envelope

Each logical request/response has a negotiated session context, typed message
kind, `WireRequestId` where correlation is needed, and a typed bounded payload.
The transport may implicitly identify the session; a duplicated session ID field
is not required and never grants authority.

`WireRequestId` is opaque, unique only among live requests in one HostSession,
and valid only until that session terminates. It correlates request, response,
and cancel; it is not globally unique and grants no Runtime authority. Runtime
maintains a local `InvocationId <-> WireRequestId` mapping. The peer never chooses
the local `InvocationId`; if a local ID appears in trace metadata it is diagnostic
only. Cross-session request or route reuse is rejected.

The logical message families are:

| Family | Messages | Meaning |
| --- | --- | --- |
| handshake | `Hello`, `HelloAccepted`, `HelloRejected` | negotiate immutable session parameters |
| artifact | `Load`, `LoadResult` | validate/load one opaque artifact |
| activation | `Start`, `RegisterInvocation*`, `StartResult` | execute start and declare bounded invocation capabilities |
| invocation | `Invoke`, `InvokeResult`, `Cancel` | run/correlate/cancel external invocation |
| lifecycle | `Dispose`, `DisposeResult`, `Shutdown`, `ShutdownResult` | bounded cleanup and stop |
| failure | `ProtocolError` or terminal transport failure | reject invalid messages or fail session |

Messages are typed, versioned, and bounded. A generic string message carrying an
unbounded dynamic value is not an acceptable final schema. `StartResult` means
the remote start body finished; only local validation, topology sealing, and
Runtime commit can publish capabilities.

## Invocation payload

`InvocationValue::External { format, bytes }` is the payload primitive:

- `format` is a stable payload/schema identifier;
- `bytes` is an opaque immutable encoded payload; and
- protocol metadata, route, deadline, and correlation live in the wire message,
  not in `InvocationValue`.

Request and response sizes are independently bounded by the negotiated effective
limits. Framing must enforce the applicable bound before unbounded buffering; an
implementation cannot read an arbitrary frame into memory and reject it later.
An unsupported declared format is a protocol/value compatibility failure, not a
plugin business-domain failure.

## Deadline

Runtime owns an absolute local monotonic deadline and continues timing locally.
Immediately before send it derives a non-negative remaining budget for the wire.
The Host may enforce that budget or a stricter one but cannot extend it. No wall
clock synchronization is required.

If Runtime's deadline linearizes first, the caller receives the existing timeout
semantic result. A later remote response is drained or discarded, cannot revive
the request, and cannot mutate lifecycle truth. A remote deadline report received
first is a Host failure observation; Runtime still applies its local public
mapping and deadline authority.

## Cancellation and race winners

`Cancel(WireRequestId)` is idempotent. Runtime may complete the caller with
cancellation after its local cancellation boundary wins and sends Cancel on a
best-effort basis. A `CancelAck`, if a codec includes one, is observational and
is not required for caller completion.

| Race | Winner authority | Late side behavior |
| --- | --- | --- |
| response vs cancel | Runtime-local response/cancellation linearization | losing response is drained; losing cancel is harmless/idempotent |
| response vs deadline | Runtime local monotonic deadline boundary | late response is drained and cannot replace timeout |
| crash vs response | first event accepted by local session/request state | later response or failure signal cannot complete twice |
| HMR cutover vs invocation admission | Runtime generation selector/gate | admitted old work keeps old lease; later work selects replacement |
| shutdown vs new Host request | Runtime admission-close boundary | shutdown-first rejects; admitted-first work is bounded by convergence |

Packet creation time on the remote machine is not a winner authority.

## Boundedness and overload

All transport frame buffers, outbound and inbound queues, pending-request maps,
capability-declaration streams, inflight invocation sets, control messages, and
request/response payloads are bounded. A full local queue or exhausted inflight
budget rejects work before remote acceptance with a structured Host overload
failure. This is distinct from a request accepted remotely and later returning a
domain or execution failure. No unbounded queue or pending map is conforming.

## Failure, disconnect, and replay

Clean EOF, broken transport, framing/protocol failure, peer shutdown, and process
exit are Host/transport outcomes, never domain success. Once terminal failure is
known, the session fails and each locally tracked unresolved request publishes
at most one terminal local completion. This makes no exactly-once claim about
remote execution or side effects.

There is no automatic reconnect, invocation replay, or transparent retry.
Remote work may already have committed side effects, so replay would silently
change at-most-once observation into possible duplicate execution. An AgentLayer
may apply an explicit retry or durable-action policy above this protocol.

## Trace metadata

An implementation may carry bounded opaque trace context in addition to
`WireRequestId`. This does not grant authority. B2.0 does not freeze OpenTelemetry
or any other tracing representation.

## Codec and implementation freedom

JSON, CBOR, protobuf, bincode, postcard, MessagePack, named pipes, Unix sockets,
TCP, stdio, PID/UUID shape, and one-process-per-plugin are not protocol contracts.
The chosen B2.1 codec and framing must merely preserve typed message distinctions,
versioning, limits, and the semantic rules above.

## B2.1B reference transport implementation choices

The repository reference `ProcessHost` uses child stdin for parent-to-child
messages, child stdout for child-to-parent messages, and child stderr for
diagnostics. Its private frame is a four-byte big-endian payload length followed
by a tagged binary payload. Stdio, tags, field encodings, and one child per
hosted generation are implementation choices, not the Cordis Host ABI.

Before negotiation, payloads are capped at 64 KiB and the reader validates the
header before allocation. Nested strings, bytes, and descriptor arrays are
bounded against the remaining frame and semantic limits; trailing bytes and
unknown tags are violations. Negotiated limits are the immutable minimum of
local and peer limits after the 1.0 Lifecycle handshake.

The private request layer uses session-local, non-zero, monotonically allocated
IDs that never wrap or reuse within a session. Exhausting the ID space is a typed
local Host failure. Its published issued-ID high-watermark is monotonic even when
multiple requests allocate concurrently. A bounded outbound channel, an actively
enforced negotiated semaphore, a bounded pending map, and typed out-of-order
dispatch complete the request layer. The effective permit count is the immutable
minimum of local and peer inflight limits; negotiation never expands local policy.
Future IDs fail the session; already-issued non-pending IDs are discarded as late
or duplicate responses without an unbounded tombstone set.

Each live request records its expected success response kind. `Failure` is valid
for every request kind, but a different success response kind for a live request
is a session-terminal `ProtocolViolation`; it is never delivered to an unrelated
caller. `Hello` is admitted only while Handshaking, ordinary lifecycle requests
only while Ready, and `Shutdown` only while Draining. Invocation messages remain
intentionally absent in B2.1B.

B2.1B.1 makes local request observation cancellation-safe without adding a wire
Cancel message: dropping a request future removes its pending registration and
releases its permit. A late response therefore finds an already-issued but
non-pending ID and is discarded. No completed-ID or tombstone collection is
needed. Every terminal session transition drains pending observations. One local
terminal completion still makes no exactly-once claim about remote execution.

`ProcessHostConfig::max_control_bytes` is enforced for encoded control requests
and decoded control responses, in addition to the absolute/negotiated frame
bound. Load/Loaded remain governed by artifact, descriptor, and frame bounds.

## B2.1C executable invocation transport

Private protocol minor 1 separates supported and required feature bitmaps and
negotiates Lifecycle, Invocation, Cancel, and Deadline by intersection. Unknown
optional features are ignored; either peer requiring an unsupported feature
rejects the handshake. The accepted features and minimum limits are immutable
before Ready. This exact-minor private codec policy is not the Host ABI.

`Started` carries at most 256 bounded `(InvocationKey, session-local route)`
declarations. They publish only through the staged local `Context` and existing
`InvocationRegistry`. Invocation, Cancel, and Deadline must all be negotiated.
No `InvocationId`, Scope, Fiber, generation, metadata envelope, or `Context`
crosses the wire.

`Invoke` carries a `WireRequestId`, route, opaque External format/bytes, and a
relative remaining nanosecond budget. Request size, inflight capacity, and queue
capacity reject before remote acceptance. `InvokeResult` has an independent
response bound and preserves Success, RemoteDomain details, and typed Host
failure outcomes.

The absolute monotonic deadline remains private. Queue/admission wait consumes
it, and the writer recomputes a downward relative budget immediately before
encoding. Dropping the local observation removes pending correlation before a
best-effort one-way idempotent Cancel; no CancelAck is required. Late results
cannot replace cancellation or timeout. Remote execution is not exactly-once
and is never transparently replayed.
