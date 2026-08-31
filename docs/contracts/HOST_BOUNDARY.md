# Host and wire boundary

The current `PluginHost` is an artifact-to-`NativePlugin` lifecycle-proxy
factory. No process transport, remote runtime, handshake, reconnect loop or wire
authority implementation exists in this repository. Runtime remains the sole
authority for Scope, Fiber, generation, admission and capability publication.

| Concern | Current support | Contract status | Process Host requirement |
| --- | --- | --- | --- |
| Host identity | `kind()` diagnostic string | Placeholder | authenticated instance identity and trust policy |
| Protocol version | none | Gap | negotiated protocol and compatibility rules |
| Handshake | none | Gap | capabilities, limits, schema and lifecycle negotiation |
| Artifact load | opaque format/revision/payload passed to adapter | Placeholder | integrity, origin, size limits, validation and stable error model |
| Plugin descriptor | returned by lifecycle proxy | Local support | encode stable descriptor fields without local authority IDs |
| Start | proxy `NativePlugin::start(Context)` | Host rule | explicit start request/result and crash/timeout semantics |
| Capability registration | only through local Context | HOST contract | remote requests registration; Runtime validates and publishes |
| Invocation request | local registry plus `InvocationValue` | Partial | request envelope, correlation, deadline, cancellation and routing |
| Invocation response | local `InvocationValue`/CordisError | Partial | response envelope and domain-vs-transport error separation |
| Domain error | `CordisError` locally | Gap for wire | stable domain error codes and forward-compatible details |
| Transport error | no first-class type | Gap | disconnect/protocol/IO/crash categories distinct from domain failure |
| Deadline | local Duration/timeout | Gap | absolute/relative semantics, clock model and propagation |
| Cancellation | local token | Gap | idempotent cancel message, acknowledgement and late-result rule |
| Remote crash | none | Gap | classify failure, revoke proxy capability and converge lifecycle |
| Disconnect | none | Gap | transient versus terminal policy and inflight request outcome |
| Restart/reconnect | none | Gap | identity continuity, generation replacement and replay prohibition |
| Service reference | `ServiceValue::External(Arc<str>)` only | Placeholder | host, route, generation, revocation, lifetime and authority proof |
| Event transport | none; Events are local/ephemeral | Not currently admitted | decide explicitly whether any Host event subset exists |
| Shutdown | local Runtime convergence | Host rule incomplete | stop request, grace, crash fallback and completion evidence |
| HMR | local transactional replacement | Host rule incomplete | staged remote instance, cutover acknowledgement and old-instance drain |
| Backpressure | local invocation semaphore | Gap | remote credits/queue limits/overload semantics |
| Message size limits | none | Gap | negotiated hard limits and structured rejection |
| Schema negotiation | format label only | Gap | registry/version compatibility and unknown-format behavior |
| Trace/correlation | local InvocationId | Wire candidate | globally safe correlation without granting Runtime authority |

## Authority rules

- A Host cannot create, reuse or interpret local IDs as capability authority.
- `ScopeId`, `FiberId`, `ServiceSymbol`, internal generation and handler IDs must
  not be accepted from remote input as proof of permission.
- `InvocationMetadata` is local authority metadata and must not be serialized
  wholesale. A future wire envelope needs separate correlation and routing data.
- Remote crash is failure, never successful plugin completion.
- Capability publication remains a Runtime transaction through Context.

`InvocationValue::External { format, bytes }` is a useful payload primitive but
not a complete protocol. `ServiceValue::External` and `PluginArtifact` remain
placeholders until the missing identity, authority, lifetime and version rules
are fixed by contract and executable conformance.

Track B2.0 fixes the design rules in [Host state machine](HOST_STATE_MACHINE.md),
[Host protocol](HOST_PROTOCOL.md), [Host error model](HOST_ERROR_MODEL.md), and
[Process Host conformance plan](B2_PROCESS_HOST_CONFORMANCE_PLAN.md). These are
logical semantic contracts, not evidence that a process transport exists.

The reference ProcessHost v2 surface is deliberately limited to lifecycle and
external invocation: artifact load, descriptor retrieval, start, invocation,
cancel, deadline, disposal, HMR, shutdown, and crash convergence. Remote Service
publication/consumption and remote EventBus transport remain deferred. A remote
descriptor with non-empty Service requirements or provisions must be rejected
as unsupported rather than ignored.

`PluginHost::load` is Host preparation, not a Runtime-accepted lifecycle
operation. It may establish a session and load an artifact before Runtime creates
its install/reload worker, but receives no Context or Runtime authority. If its
future is dropped or fails, the Host owns cancellation-safe cleanup of every
prepared external resource. `OWN-001` begins only after the Runtime worker is
successfully created.

Hosted replacement is publicly symmetric with Native replacement:
`reload_hosted` and `reload_hosted_detailed` perform Host preparation first and
then enter the same private `Arc<dyn NativePlugin>` reload transaction used by
`reload`/`reload_detailed`. A load failure occurs before transaction acceptance,
leaves the old Fiber Active, and is not wrapped as `ReloadFailed`.
