use crate::{
    EventHandler, EventOutcome, InvocationHandler, InvocationMiddleware, PluginArtifact,
    PluginHost, RuntimeConfig,
    dependency_graph::DependencyGraph,
    disposal::{
        CleanupIssue, CleanupPhase, DisposalCompletion, DisposalObservation, DisposalPhase,
        DisposalTerminal, DisposeOutcome, FiberDisposal, ScopeDisposal, ScopeDisposeOutcome,
        ScopeState,
    },
    event_bus::EventBus,
    fiber_registry::{FiberCell, FiberMutable, FiberRegistry},
    gate::{AdmissionGate, CapabilityGate, DrainOutcome, GenerationExecutionState},
    health::{Diagnostics, HealthIssue, HealthIssueKind, HealthReport, RuntimeHealth},
    invocation::{InvocationRegistry, invocation_context},
    plugin_registry::{GenerationId, PluginRegistry},
    scope_registry::{ScopeRecord, ScopeRegistry},
    service_handle::ServiceHandle,
    service_registry::{ServiceEntry, ServiceRegistry},
    shutdown_coordinator::{ShutdownCompletion, ShutdownCoordinator},
    task::{RuntimeWorkerKind, RuntimeWorkerSupervisor, TaskSupervisor},
};
use async_trait::async_trait;
use cordis_core::{
    CordisError, DependencyPolicy, Effect, EventKey, EventValue, FiberId, FiberState, HandlerId,
    HostError, InvocationHandlerId, InvocationId, InvocationKey, InvocationMetadata,
    InvocationMiddlewareId, InvocationValue, PluginDescriptor, PluginId, ResourceKind, ScopeId,
    ServiceKey, ServiceSymbol, ServiceValue, TaskId,
};
use futures::FutureExt;
use parking_lot::{Mutex, RwLock};
use smallvec::SmallVec;
use std::{
    any::Any,
    collections::{HashMap, HashSet},
    future::Future,
    panic::AssertUnwindSafe,
    sync::{
        Arc, Weak,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Mutex as AsyncMutex, Semaphore, watch};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info_span};

/// A native Rust plugin. Bridge hosts can implement the same activation contract.
#[async_trait]
pub trait NativePlugin: Send + Sync + 'static {
    /// Host-neutral descriptor and dependencies.
    fn descriptor(&self) -> PluginDescriptor;
    /// Activates the plugin. Context mutations are automatically owned by its fiber.
    async fn start(&self, context: Context) -> Result<(), CordisError>;
}

pub use crate::shutdown_coordinator::{RuntimeShutdownState, ShutdownBlocker, ShutdownOutcome};

struct RuntimeInner {
    shutdown: ShutdownCoordinator,
    fibers: FiberRegistry,
    events: EventBus,
    invocations: InvocationRegistry,
    tasks: Arc<TaskSupervisor>,
    workers: Arc<RuntimeWorkerSupervisor>,
    root: ScopeId,
    scopes: ScopeRegistry,
    plugins: PluginRegistry,
    services: ServiceRegistry,
    dependencies: DependencyGraph,
    config: RuntimeConfig,
    invocation_permits: Semaphore,
    diagnostics: Arc<Diagnostics>,
    next_invocation: AtomicU64,
    active_reloads: AtomicUsize,
    reload_cleanup_pending: AtomicUsize,
    gc_state: AtomicUsize,
    pending_activations: Mutex<HashMap<FiberId, bool>>,
    admission: AdmissionGate,
    shutdown_deadline: Mutex<Option<tokio::time::Instant>>,
    #[cfg(test)]
    fail_next_reload_publication: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    reload_before_selector_hook: Mutex<Option<Arc<std::sync::Barrier>>>,
    #[cfg(test)]
    reload_after_selector_hook: Mutex<Option<Arc<std::sync::Barrier>>>,
    #[cfg(test)]
    fail_selector_after_scope: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    context_before_gate_hook: Mutex<Option<Arc<std::sync::Barrier>>>,
    #[cfg(test)]
    service_before_gate_hook: Mutex<Option<Arc<std::sync::Barrier>>>,
    #[cfg(test)]
    activation_before_commit_hook: Mutex<Option<Arc<TestActivationCommitHook>>>,
    #[cfg(test)]
    gc_registration_hook: Mutex<Option<Arc<std::sync::Barrier>>>,
}

#[cfg(test)]
#[derive(Default)]
struct TestActivationCommitHook {
    entered: std::sync::atomic::AtomicBool,
    entered_notify: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

impl RuntimeInner {
    fn quota_error(
        &self,
        resource: ResourceKind,
        limit: usize,
        scope: Option<ScopeId>,
        fiber: Option<FiberId>,
    ) -> CordisError {
        self.diagnostics
            .quota_rejections
            .fetch_add(1, Ordering::Relaxed);
        self.diagnostics.push(HealthIssue {
            at: std::time::SystemTime::now(),
            kind: HealthIssueKind::QuotaRejected,
            scope,
            fiber,
            invocation: None,
        });
        CordisError::ResourceLimitExceeded { resource, limit }
    }
}

/// Global Cordis runtime.
#[derive(Clone)]
pub struct Runtime(Arc<RuntimeInner>);

/// Structured result of a logically committed reload operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReloadOutcome {
    /// The new generation committed and old-generation cleanup completed.
    Completed {
        /// Newly active Fiber.
        new_fiber: FiberId,
    },
    /// The new generation committed and is usable while Runtime-owned cleanup
    /// continues for the old generation.
    CommittedWithCleanupPending {
        /// Newly active Fiber.
        new_fiber: FiberId,
        /// Old Fiber still converging in the background.
        old_fiber: FiberId,
    },
}

/// Cheap capability handle scoped to a plugin activation.
#[derive(Clone)]
pub struct Context {
    runtime: Weak<RuntimeInner>,
    scope: ScopeId,
    fiber: FiberId,
    owner: Weak<FiberCell>,
    generation: GenerationId,
}

pub(crate) fn panic_message(payload: &(dyn Any + Send)) -> String {
    payload.downcast_ref::<&str>().map_or_else(
        || {
            payload
                .downcast_ref::<String>()
                .cloned()
                .unwrap_or_else(|| "non-string panic payload".into())
        },
        |message| (*message).to_owned(),
    )
}

fn is_terminal_cleanup_error(error: &CordisError) -> bool {
    matches!(
        error,
        CordisError::DisposalWorkerPanicked(_)
            | CordisError::DisposalWorkerCancelled
            | CordisError::DisposalWorkerTerminated(_)
            | CordisError::ScopeDisposalPanicked(_)
            | CordisError::ScopeDisposalCancelled
            | CordisError::ScopeDisposalTerminated(_)
    )
}

include!("lifecycle.rs");
include!("scope.rs");
include!("fiber.rs");
include!("dependency.rs");
include!("disposal.rs");
include!("service.rs");
include!("snapshot.rs");
include!("shutdown.rs");
include!("../context.rs");

#[cfg(test)]
include!("tests.rs");
