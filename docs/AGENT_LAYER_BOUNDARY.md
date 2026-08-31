# AgentLayer stable Kernel boundary

AgentLayer may rely on `Runtime`, Scope/Fiber lifecycle, `ServiceKey` resolution,
exact-generation `ServiceHandle` lifetime, `InvocationKey`, `InvocationValue`,
structured invocation errors, Event dispatch modes, owned tasks, Effects,
Context cancellation/staleness, HMR/disposal/shutdown outcomes, `PluginHost`,
`ProcessHost`, `HostError`, and `RemoteDomainError`.

AgentLayer must not rely on `RuntimeInner`, `FiberCell`, `GenerationExecution`
encoding, selector internals, registry layout, `ServiceSymbol` persistence,
`WireRequestId`, `HostSession`, remote routes, the private frame codec, stdio
transport, reader/writer/supervisor topology, Tokio scheduling details, GC
implementation, or DependencyGraph internals.

Agent concepts such as model providers, tools, sessions, journals, checkpoints,
durable actions and orchestration runs remain above the Kernel. They do not
justify new v2 Kernel capability.
