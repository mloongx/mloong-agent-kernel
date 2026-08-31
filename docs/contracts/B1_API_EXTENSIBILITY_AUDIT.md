# B1.1 public API extensibility audit

This audit records intentional Rust source-evolution policy before the Cordis v2
public freeze. It changes no Runtime behavior and introduces no Host or wire
semantics.

## Inventory

The exported data surface contains 18 enums and 33 structs (including tuple,
opaque-ID, Context/Runtime capability, and generic handle structs).

| Type(s) | Kind | External construction/matching | Host pressure | Contract nature | Decision |
| --- | --- | --- | --- | --- | --- |
| `CordisError` | enum | matched externally | high | extensible error taxonomy | already `#[non_exhaustive]` |
| `ResourceKind` | enum | matched externally | high | extensible quota taxonomy | `#[non_exhaustive]` |
| `HealthIssueKind` | enum | matched externally | high | extensible diagnostic taxonomy | `#[non_exhaustive]` |
| `ShutdownBlocker` | enum | matched externally | high | extensible convergence diagnostics | `#[non_exhaustive]` |
| `CleanupPhase` | enum | matched externally | high | extensible cleanup diagnostics | `#[non_exhaustive]` |
| `FiberState`, `ScopeState`, `DisposalPhase`, `RuntimeShutdownState` | enum | matched externally | low | lifecycle state machines | keep exhaustive |
| `DispatchMode` | enum | constructed/matched | low | closed dispatch algebra | keep exhaustive |
| `DependencyPolicy` | enum | constructed/matched | low | closed dependency policy | keep exhaustive |
| `RuntimeHealth` | enum | matched externally | low | closed three-level classification | keep exhaustive |
| `ShutdownOutcome` | enum | matched externally | medium | closed terminal truth; blockers/issues carry extension | keep exhaustive |
| `ReloadOutcome` | enum | matched externally | low | closed commit truth | keep exhaustive |
| `DisposeOutcome`, `ScopeDisposeOutcome` | enum | matched externally | low | closed committed/incomplete truth | keep exhaustive |
| `InvocationValue`, `ServiceValue` | enum | constructed/matched | medium | Native/opaque-external value algebra | keep exhaustive pending B2 wire design |
| `RuntimeConfig` | struct | constructed externally | high | extensible configuration envelope | `#[non_exhaustive]`; construct with `Default`, then mutate public fields |
| `PluginArtifact` | struct | constructed externally | high | extensible Host input envelope | `#[non_exhaustive]`; `PluginArtifact::new` preserves construction |
| `InvocationMetadata` | struct | Runtime-produced | high | extensible local metadata | `#[non_exhaustive]`; Runtime uses `InvocationMetadata::new` |
| `RuntimeSnapshot`, `FiberSnapshot`, `ScopeSnapshot`, `ServiceSnapshot`, `GarbageReport` | struct | Runtime-produced | high | extensible observations | `#[non_exhaustive]`; fields remain readable |
| `HealthIssue`, `HealthReport`, `CleanupIssue` | struct | Runtime-produced | high | extensible observations | `#[non_exhaustive]`; fields remain readable |
| `PluginDescriptor` | struct | constructed by every plugin | medium | v2 NativePlugin descriptor ABI | keep exhaustive intentionally |
| `ServiceKey`, `InvocationKey` | opaque-field struct | constructors | medium | stable logical identities | current constructors permit private field evolution |
| `ServiceSymbol` | opaque-field struct | factory/index methods | low | non-portable Runtime token | keep opaque |
| `EventKey`, `InvocationId`, `PluginRevision` | tuple struct | constructed externally | low | stable primitive wrappers | keep current construction |
| `ScopeId`, `FiberId`, `PluginId`, `HandlerId`, `InvocationHandlerId`, `InvocationMiddlewareId`, `EffectId`, `TaskId` | opaque ID struct | Runtime-produced | low | stable opaque identities | keep opaque |
| `EventOutcome` | tuple struct | handler-produced | low | stable event result wrapper | keep constructible |
| `Runtime`, `Context`, `InvocationContext`, `Next`, `NextInvocation`, `ServiceHandle<T>` | capability/handle struct | Runtime-produced | low | behavior-bearing opaque capability | already non-constructible by field |

## Enum decisions

`ShutdownBlocker` must be non-exhaustive because Process Host sessions, transport
workers, remote queues, or host cleanup can add blocker categories. The outer
`ShutdownOutcome` remains exhaustive: `Complete`, `CompleteWithIssues`, and
`Incomplete { blockers, issues }` are a closed convergence truth algebra, and
future blocker categories fit inside it.

`HealthIssueKind`, `ResourceKind`, and `CleanupPhase` are diagnostic/quota
taxonomies rather than closed semantics, so they are non-exhaustive. In contrast,
`FiberState`, `DispatchMode`, `DependencyPolicy`, lifecycle states, and outcome
truth enums remain exhaustive because adding a variant changes the promised
semantic algebra and should require an intentional compatibility event.

## Struct decisions

`RuntimeConfig` remains easy to construct through `Default` followed by public
field mutation. `PluginArtifact::new` accepts the existing format, revision, and
payload while leaving future fields defaultable. `InvocationMetadata` and all
snapshot/report types are Runtime-produced and non-exhaustive; their current
fields stay public and readable, but external construction and exhaustive
destructuring no longer freeze their layout.

`PluginDescriptor` intentionally remains exhaustive. Its five fields are the v2
NativePlugin descriptor contract, and every external plugin must construct it.
Host/security capability negotiation belongs to separate Host/Wire contracts,
not silent fields in the Native descriptor. A future change to this descriptor is
therefore explicit plugin-ABI evolution rather than an unresolved freeze risk.

## Compatibility verification

Integration tests, examples, and benches compile as external crates. Every
external `RuntimeConfig` literal was migrated to `RuntimeConfig::default()` plus
field assignment. Non-exhaustive diagnostic matching uses wildcard-compatible
patterns. No Host IDs, transport errors, protocol versions, RPC envelopes, or
remote authority types were introduced.

Freeze-critical extensibility issues remaining: **none**.
