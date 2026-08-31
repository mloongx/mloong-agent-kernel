//! Stable, runtime-independent contracts for Cordis.

mod effect;
mod error;
mod event;
mod id;
mod invocation;
mod lifecycle;
mod plugin;
mod service;

pub use effect::{Effect, EffectFuture, effect_fn};
pub use error::{CordisError, HostError, HostFailureKind, RemoteDomainError, ResourceKind};
pub use event::{DispatchMode, EventKey, EventValue};
pub use id::{
    EffectId, FiberId, HandlerId, InvocationHandlerId, InvocationMiddlewareId, PluginId, ScopeId,
    TaskId,
};
pub use invocation::{InvocationId, InvocationKey, InvocationMetadata, InvocationValue};
pub use lifecycle::FiberState;
pub use plugin::{DependencyPolicy, PluginDescriptor, PluginRevision};
pub use service::{ServiceKey, ServiceSymbol, ServiceValue};
