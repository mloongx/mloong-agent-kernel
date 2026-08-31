use cordis_core::CordisError;
use parking_lot::Mutex;
use std::sync::Arc;
#[cfg(test)]
use tokio::task::AbortHandle;
use tokio::{sync::Notify, task::JoinHandle};

/// Durable progress of a fiber disposal transaction.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum DisposalPhase {
    /// Disposal has not started.
    #[default]
    Idle,
    /// Owned tasks are being cancelled and joined.
    Tasks,
    /// Provider execution admission is closed and existing leases are draining.
    Draining,
    /// Owned child scopes are being disposed.
    ChildScopes,
    /// Event handlers are being removed.
    Handlers,
    /// Provided services are being withdrawn.
    Services,
    /// Explicit effects are being disposed in LIFO order.
    Effects,
    /// Dependency changes are being reconciled.
    Reconciling,
    /// The disposal body is complete while synchronous control-plane indexes converge.
    Finalizing,
    /// Cleanup reached its normal terminal path, possibly with aggregated errors.
    Complete,
    /// The worker ended abnormally and cleanup may be unfinished.
    Terminated,
}

/// Lifecycle of a scope cleanup operation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ScopeState {
    /// The scope accepts new owned resources.
    #[default]
    Active,
    /// A Runtime-owned supervisor is traversing descendants.
    Disposing,
    /// Traversal completed, possibly with aggregated cleanup errors.
    Disposed,
    /// Traversal ended abnormally and cleanup may be unfinished.
    Terminated,
}

/// Stage that reported a best-effort cleanup problem.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CleanupPhase {
    /// Owned task cancellation and joining.
    TaskDrain,
    /// In-flight provider generation admission drain.
    ProviderDrain,
    /// Descendant Scope cleanup.
    ChildScopeCleanup,
    /// Event and invocation registration removal.
    EventCleanup,
    /// Service publication withdrawal.
    ServiceCleanup,
    /// Explicit plugin Effect cleanup.
    EffectCleanup,
    /// Dependency graph reconciliation.
    DependencyCleanup,
    /// Scope membership detachment.
    ScopeDetach,
    /// Fiber membership detachment.
    FiberDetach,
    /// Cleanup of an uncommitted activation bundle.
    StagingRollback,
    /// Runtime worker termination or panic.
    Worker,
}

/// One ordered cleanup diagnostic. Lifecycle truth is reported separately.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct CleanupIssue {
    /// Cleanup stage that observed the issue.
    pub phase: CleanupPhase,
    /// Stable human-readable diagnostic.
    pub message: String,
    /// Typed cause when cleanup exposed one without flattening.
    pub cause: Option<CordisError>,
}

/// Structured result of a Fiber disposal observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DisposeOutcome {
    /// Disposal committed without an observed cleanup problem.
    Disposed,
    /// Disposal committed, while best-effort cleanup reported diagnostics.
    CommittedWithCleanupIssues {
        /// Ordered diagnostics in cleanup execution order.
        issues: Vec<CleanupIssue>,
    },
    /// Safe convergence stopped before the terminal lifecycle commit.
    Incomplete {
        /// Error that prevented terminal lifecycle convergence.
        primary: CordisError,
        /// Ordered diagnostics explaining incomplete convergence.
        issues: Vec<CleanupIssue>,
    },
}

/// Structured result of a Scope disposal observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ScopeDisposeOutcome {
    /// Scope disposal committed without an observed cleanup problem.
    Disposed,
    /// Scope disposal committed, while descendant cleanup reported diagnostics.
    CommittedWithCleanupIssues {
        /// Ordered diagnostics in descendant traversal order.
        issues: Vec<CleanupIssue>,
    },
    /// Scope convergence stopped before its terminal lifecycle commit.
    Incomplete {
        /// Error that prevented terminal Scope convergence.
        primary: CordisError,
        /// Ordered diagnostics explaining incomplete convergence.
        issues: Vec<CleanupIssue>,
    },
}

#[derive(Clone, Debug)]
pub(crate) enum DisposalTerminal {
    Committed,
    Incomplete(CordisError),
}

#[derive(Clone, Debug)]
pub(crate) struct DisposalObservation {
    pub(crate) legacy_result: Result<(), CordisError>,
    pub(crate) terminal: DisposalTerminal,
    pub(crate) issues: Vec<CleanupIssue>,
}

impl DisposalObservation {
    pub(crate) fn fiber_outcome(&self) -> DisposeOutcome {
        match &self.terminal {
            DisposalTerminal::Committed if self.issues.is_empty() => DisposeOutcome::Disposed,
            DisposalTerminal::Committed => DisposeOutcome::CommittedWithCleanupIssues {
                issues: self.issues.clone(),
            },
            DisposalTerminal::Incomplete(primary) => DisposeOutcome::Incomplete {
                primary: primary.clone(),
                issues: self.issues.clone(),
            },
        }
    }

    pub(crate) fn scope_outcome(&self) -> ScopeDisposeOutcome {
        match &self.terminal {
            DisposalTerminal::Committed if self.issues.is_empty() => ScopeDisposeOutcome::Disposed,
            DisposalTerminal::Committed => ScopeDisposeOutcome::CommittedWithCleanupIssues {
                issues: self.issues.clone(),
            },
            DisposalTerminal::Incomplete(primary) => ScopeDisposeOutcome::Incomplete {
                primary: primary.clone(),
                issues: self.issues.clone(),
            },
        }
    }
}

pub(crate) struct ScopeDisposal {
    pub(crate) completion: Arc<DisposalCompletion>,
    pub(crate) result: Option<Result<(), CordisError>>,
    pub(crate) supervisor: Option<JoinHandle<()>>,
    pub(crate) issues: Vec<CleanupIssue>,
    pub(crate) persistent: bool,
    #[cfg(test)]
    pub(crate) body_abort: Option<AbortHandle>,
    #[cfg(test)]
    pub(crate) test_hook: Option<Arc<TestDisposalHook>>,
}

impl Default for ScopeDisposal {
    fn default() -> Self {
        Self {
            completion: Arc::new(DisposalCompletion::default()),
            result: None,
            supervisor: None,
            issues: Vec::new(),
            persistent: false,
            #[cfg(test)]
            body_abort: None,
            #[cfg(test)]
            test_hook: None,
        }
    }
}

pub(crate) struct DisposalCompletion {
    observation: Mutex<Option<Arc<DisposalObservation>>>,
    pub(crate) notify: Notify,
    #[cfg(test)]
    pub(crate) waiter_registrations: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    pub(crate) waiter_notify: Notify,
}

impl DisposalCompletion {
    #[cfg(test)]
    pub(crate) fn result(&self) -> Option<Result<(), CordisError>> {
        self.observation
            .lock()
            .as_ref()
            .map(|item| item.legacy_result.clone())
    }

    pub(crate) fn observation(&self) -> Option<Arc<DisposalObservation>> {
        self.observation.lock().clone()
    }

    pub(crate) fn publish(&self, observation: DisposalObservation) -> Result<(), CordisError> {
        let mut slot = self.observation.lock();
        if slot.is_some() {
            return Err(CordisError::Invariant(
                "disposal completion published more than once".into(),
            ));
        }
        *slot = Some(Arc::new(observation));
        Ok(())
    }
}

impl Default for DisposalCompletion {
    fn default() -> Self {
        Self {
            observation: Mutex::new(None),
            notify: Notify::new(),
            #[cfg(test)]
            waiter_registrations: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            waiter_notify: Notify::new(),
        }
    }
}

pub(crate) struct FiberDisposal {
    pub(crate) phase: DisposalPhase,
    pub(crate) wait_for_dependencies: bool,
    pub(crate) errors: Vec<CleanupIssue>,
    pub(crate) result: Option<Result<(), CordisError>>,
    pub(crate) completion: Arc<DisposalCompletion>,
    /// Reload finalization keeps waiting after the public cleanup deadline.
    pub(crate) persistent_drain: bool,
    /// Runtime-owned supervisor task; callers only observe its durable result.
    pub(crate) supervisor: Option<JoinHandle<()>>,
    #[cfg(test)]
    pub(crate) worker_abort: Option<AbortHandle>,
    #[cfg(test)]
    pub(crate) test_hook: Option<std::sync::Arc<TestDisposalHook>>,
}

impl Default for FiberDisposal {
    fn default() -> Self {
        Self {
            phase: DisposalPhase::Idle,
            wait_for_dependencies: true,
            errors: Vec::new(),
            result: None,
            completion: Arc::new(DisposalCompletion::default()),
            persistent_drain: false,
            supervisor: None,
            #[cfg(test)]
            worker_abort: None,
            #[cfg(test)]
            test_hook: None,
        }
    }
}

#[cfg(test)]
#[derive(Default)]
#[allow(clippy::struct_excessive_bools)]
pub(crate) struct TestDisposalHook {
    pub(crate) panic_before_finish: bool,
    pub(crate) pause_before_finish: bool,
    pub(crate) entered: std::sync::atomic::AtomicBool,
    pub(crate) entered_notify: tokio::sync::Notify,
    pub(crate) release: tokio::sync::Notify,
    pub(crate) pause_before_index_cleanup: bool,
    pub(crate) finalizing: std::sync::atomic::AtomicBool,
    pub(crate) finalizing_notify: tokio::sync::Notify,
    pub(crate) release_index_cleanup: tokio::sync::Notify,
    pub(crate) pause_before_scope_topology_commit: bool,
    pub(crate) scope_commit_pending: std::sync::atomic::AtomicBool,
    pub(crate) scope_commit_pending_notify: tokio::sync::Notify,
    pub(crate) release_scope_topology_commit: tokio::sync::Notify,
    pub(crate) pause_after_publish: bool,
    pub(crate) published: std::sync::atomic::AtomicBool,
    pub(crate) published_notify: tokio::sync::Notify,
    pub(crate) release_after_publish: tokio::sync::Notify,
}
