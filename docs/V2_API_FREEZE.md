# Cordis Kernel v2 public API freeze

This inventory was regenerated from the final `cordis-core` and
`cordis-runtime` sources at the v2 freeze candidate. It contains 19 public
enums, 29 public structs, 6 public traits, 3 public type aliases, 2 free
functions, and 104 public inherent methods.

## Semantic classification

| Classification | Public surface |
| --- | --- |
| FROZEN SEMANTIC | `Runtime`, `Context`, `NativePlugin`, `PluginHost`, `ProcessHost`, `ServiceKey`, `ServiceSymbol`, `ServiceHandle`, `InvocationKey`, `InvocationValue`, `EventKey`, `FiberState`, all lifecycle outcome types |
| EXTENSION-READY | `CordisError`, `HostFailureKind`, `HostError`, `RemoteDomainError`, `ResourceKind`, `CleanupPhase`, `CleanupIssue`, `ShutdownBlocker`, `HealthIssueKind`, `RuntimeConfig`, `PluginArtifact`, `InvocationMetadata`, snapshots and reports |
| INTENTIONALLY CLOSED ALGEBRA | `FiberState`, `DispatchMode`, `DependencyPolicy`, `ReloadOutcome`, `DisposeOutcome`, `ScopeDisposeOutcome`, `ShutdownOutcome`, `RuntimeShutdownState`, `RuntimeHealth`, `DisposalPhase`, `ScopeState`, `InvocationValue` |
| PLACEHOLDER | `ServiceValue::External`; `PluginArtifact` format as a public wire ABI |
| DEFERRED CAPABILITY | remote Service, remote EventBus, reconnect/replay and distributed execution |

Extension-prone enums/structs use `#[non_exhaustive]`. Closed lifecycle and
dispatch algebras deliberately remain exhaustive. This is not a mechanical
policy: adding a closed variant changes semantics and therefore requires an
explicit compatibility event.

## Construction and plugin ABI

- `RuntimeConfig`: `Default`, followed by mutation of public fields.
- `ProcessHostConfig`: `Default`, followed by mutation of public fields.
- `PluginArtifact`: `PluginArtifact::new(format, revision, payload)`.
- `HostError` and `RemoteDomainError`: public constructors and semantic accessors.
- `InvocationMetadata`: public constructor; the reference wire never serializes
  its local authority fields.
- `PluginDescriptor`: intentionally exhaustive and externally constructible. Its
  five fields are the frozen v2 `NativePlugin` descriptor ABI. Host metadata does
  not reopen this structure.

Examples, benches, integration tests, and external trait implementations compile
against these construction paths. Freeze-critical API issues: **none**.

## Stable versus private

Agent-facing identifiers, keys, lifecycle outcomes and typed errors are stable.
`RuntimeInner`, `FiberCell`, generation encoding, selector and registry layout,
`WireRequestId`, remote routes, `HostSession`, private codec, stdio transport,
actor topology, scheduler details, GC implementation and dependency graph layout
are not public contracts.
