# Runtime ownership contract

Cordis Kernel design version 1.0 separates an operation observer from the Runtime owner that converges accepted work.

- `Runtime` owns admission, registries, supervisors, shutdown attempts, and automatic GC.
- `PluginId` identifies a logical plugin; `FiberId` identifies one execution instance; the internal generation ID identifies one activation generation.
- `ScopeRegistry` owns Scope topology. A Scope disposal completion is published only after committed parent topology converges.
- `FiberCell` owns mutable generation resources. Its async lifecycle mutex serializes lifecycle operations; its synchronous inner guard never crosses an await.
- `TaskSupervisor` owns live Fiber tasks. `RuntimeWorkerSupervisor` owns accepted install, reload, dependency reconciliation, and GC work.
- `ServiceHandle<T>` owns the exact provider-generation lease for an escaped service use. Dropping the last handle allows generation drain to continue.
- `DisposalCompletion` owns immutable Fiber/Scope operation truth. Registered observers retain it even if GC reclaims the registry object.
- `ShutdownCoordinator` owns the current immutable convergence attempt. An incomplete attempt may be followed by a new attempt without reopening admission.
- Automatic GC reclaims only committed `Disposed` Fibers and Scopes. Fiber reclamation performs the exactly-once PluginRegistry detach.
- Service symbols are owned by lifecycle preparation. Context publication validates its declared provision and only consumes the already-admitted stable symbol.
- GC worker registration owns an `AdmissionGate` registration lease until the worker is synchronously present in `RuntimeWorkerSupervisor`. Shutdown owns the opposing close fence and the final synchronous collection.
