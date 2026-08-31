//! Production-oriented native Rust Cordis runtime.
//!
//! Public methods consistently return [`cordis_core::CordisError`]. Individual
//! variants are documented on that enum, avoiding repetitive per-method lists.

#![allow(clippy::missing_errors_doc)]

mod config;
mod dependency_graph;
mod disposal;
mod event_bus;
mod fiber_registry;
mod gate;
mod health;
mod host;
mod invocation;
mod plugin_registry;
mod runtime;
mod scope_registry;
mod service_handle;
mod service_registry;
mod shutdown_coordinator;
mod task;

pub use config::RuntimeConfig;
pub use cordis_core::{HostError, HostFailureKind, RemoteDomainError};
pub use disposal::{
    CleanupIssue, CleanupPhase, DisposalPhase, DisposeOutcome, ScopeDisposeOutcome, ScopeState,
};
pub use event_bus::{EventHandler, EventOutcome, Next};
pub use health::{HealthIssue, HealthIssueKind, HealthReport, RuntimeHealth};
pub use host::{PluginArtifact, PluginHost, ProcessHost, ProcessHostConfig};
pub use invocation::{
    InvocationContext, InvocationHandler, InvocationMiddleware, InvocationOutcome, NextInvocation,
    invocation_handler_fn,
};
pub use runtime::{
    Context, FiberSnapshot, GarbageReport, NativePlugin, ReloadOutcome, Runtime,
    RuntimeShutdownState, RuntimeSnapshot, ScopeSnapshot, ServiceSnapshot, ShutdownBlocker,
    ShutdownOutcome,
};
pub use service_handle::ServiceHandle;
