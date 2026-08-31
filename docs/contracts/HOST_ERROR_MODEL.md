# Hosted execution error model

Hosted execution preserves a strict distinction between plugin domain failure,
Host/transport failure, and protocol/value compatibility failure. Cordis must not
serialize `CordisError` wholesale: it contains Runtime-local identifiers and
lifecycle facts that are neither portable nor remote authority.

## Logical error classes

A **remote domain error** means plugin code ran and intentionally returned a
failure. Its logical payload contains a stable semantic code, a diagnostic
message, and optional format-tagged opaque bounded details. Code is semantic;
message is not a stable oracle.

A **Host failure** means the execution backend or transport could not preserve
the requested operation. Required semantic categories include handshake
incompatibility, protocol violation, transport closed, remote process exited or
killed, message too large, unsupported format, overload, remote deadline, and
remote unavailable. Exact Rust variant names are a B2.1 API decision.

A **protocol failure** is malformed/out-of-state messaging or incompatibility in
version, required feature, route, payload format, or negotiated limit. A severe
violation fails the session. A bounded operation-level compatibility rejection
may fail only that request when the session remains trustworthy.

## Failure table

| Failure | Class | User-visible result | Lifecycle effect | Automatic retry? |
| --- | --- | --- | --- | --- |
| plugin returns stable error code | domain | remote domain failure retaining code/details | none unless operation contract says so | no |
| incompatible major/missing required feature | protocol/Host | handshake incompatible | session never becomes Ready | no |
| malformed or out-of-state message | protocol | protocol violation | normally session `Failed` | no |
| request/response exceeds limit | protocol/Host | message too large | request rejection or session failure if framing trust lost | no |
| unsupported payload format | protocol/value | unsupported format | request/start rejected | no |
| local/remote capacity exhausted | Host | overloaded | no admission or one accepted operation fails | no transparent retry |
| clean/broken transport close | Host/transport | transport closed | session `Failed`; unresolved work fails once | no |
| child exits unexpectedly | Host | remote process exited | session `Failed`; Runtime converges proxy | no |
| child force-killed during cleanup | Host cleanup | cleanup issue after confirmed reap | may still reach committed lifecycle truth | no |
| child remains live/unreaped | Host lifecycle | incomplete outcome with Host blocker | shutdown/disposal remains incomplete | retry convergence only |
| local cancellation wins | Runtime invocation | `InvocationCancelled` | request locally terminal; best-effort Cancel | no |
| local deadline wins | Runtime invocation | `InvocationTimedOut` | request locally terminal; best-effort Cancel/drain | no |
| replacement fails before HMR commit | domain/Host/protocol | `ReloadFailed` with typed cause | staging rollback; old remains authoritative | no |
| old cleanup fails after HMR commit | Host cleanup | `ReloadCommitted` with cleanup issue | new remains authoritative | no |

## B2.1A public API foundation

`PluginStartFailed(String)` and `InvocationFailed(String)` cannot preserve the
Host-versus-domain distinction required by `HST-002`, `INV-005`, and `WIR-006`.
B2.1A implements the minimal typed public foundation:

1. `HostFailureKind` and `HostError` are forward-compatible public types;
2. `CordisError::Host(HostError)` remains distinguishable through load, start,
   invocation, disposal, reload, and shutdown nesting;
3. retain operation/lifecycle wrappers such as `ReloadFailed` and
   `ReloadCommitted`, placing the typed Host error in their existing primary or
   cleanup cause rather than flattening commit truth; and
4. `RemoteDomainError` plus `CordisError::RemoteDomain` preserve a stable opaque
   domain code, diagnostic message, and optional bounded format-tagged details.

The single Host-error integration remains preferred over operation-specific Host
variants. Existing `InvocationFailed(String)` remains for Native compatibility;
remote paths use the typed remote-domain variant.

Further ProcessHost implementation deltas are limited:

- represent a live or unreaped hosted session as non-exhaustive `ShutdownBlocker::HostedExecution { fiber }` and
  corresponding diagnostic/health/resource taxonomies only where implementation
  evidence requires them;
- retain `PluginHost`, `PluginArtifact`, and `InvocationValue` as-is;
- do not serialize `InvocationMetadata` or add protocol metadata to
  `InvocationValue`;
- retain `ServiceValue::External` as a placeholder and provide no remote Service
  behavior in the reference host.

The existing `PluginHost::load() -> Arc<dyn NativePlugin>` remains sufficient:
ProcessHost returns a `RemotePluginProxy` implementing `NativePlugin`. The trait
does not need redesign for B2.1.

## B2.1B transport preservation

The reference codec maps peer business failures to
`CordisError::RemoteDomain` and process, transport, handshake, framing, limit,
and unsupported-capability failures to `CordisError::Host`. Activation and
cleanup wrappers retain these causes. EOF is never domain success. Service
dependencies/provisions remain rejected as `UnsupportedCapability`.

## B2.1C invocation failures

Invocation outcomes preserve External success, RemoteDomain code/message/details,
and typed Host failures without string flattening. UnsupportedFormat,
MessageTooLarge, Overloaded, UnsupportedCapability, and Unavailable are
request-scoped. Peer-reported ProtocolViolation, HandshakeIncompatible,
TransportClosed, ProcessExited, and ProcessKilled fail the session and every
pending observation. Local cancellation and timeout remain Runtime winners;
late results are discarded and no CancelAck or replay is required.
