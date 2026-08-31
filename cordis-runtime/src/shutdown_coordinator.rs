use crate::disposal::CleanupIssue;
use cordis_core::{FiberId, ScopeId};
use parking_lot::Mutex;
use std::sync::Arc;
use tokio::{sync::Notify, task::JoinHandle};

/// Runtime shutdown lifecycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeShutdownState {
    /// Runtime accepts new work.
    Running,
    /// A shutdown convergence attempt is running.
    ShuttingDown,
    /// The last attempt ended with durable blockers and admission remains closed.
    Incomplete,
    /// Global convergence completed.
    Complete,
}

/// A live ownership or control-plane fact preventing shutdown completion.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ShutdownBlocker {
    /// A Fiber has not converged to permanent disposal.
    Fiber(FiberId),
    /// A hosted execution still owns a live or unreaped child process/session.
    HostedExecution {
        /// Owning Fiber; process identifiers are intentionally not public authority.
        fiber: FiberId,
    },
    /// A non-root Scope remains live.
    Scope(ScopeId),
    /// Runtime-owned control-plane workers remain live.
    RuntimeWorkers(usize),
    /// Fiber-owned tasks remain live.
    Tasks(usize),
    /// A provider generation still has admitted execution or handles.
    GenerationInflight {
        /// Provider Fiber.
        fiber: FiberId,
        /// Provider generation identity.
        generation: u64,
        /// Total admitted generation uses.
        inflight: usize,
        /// Uses specifically retained by `ServiceHandle` values.
        service_handles: usize,
    },
    /// Reload transactions remain active.
    ReloadTransactions(usize),
    /// Hidden staging resources remain.
    Staging {
        /// Staged Fibers.
        fibers: usize,
        /// Staging Scopes.
        scopes: usize,
    },
}

/// Durable result of one shutdown convergence attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ShutdownOutcome {
    /// The Runtime reached complete global convergence without diagnostics.
    Complete,
    /// Global convergence completed with non-blocking cleanup diagnostics.
    CompleteWithIssues {
        /// Ordered cleanup diagnostics.
        issues: Vec<CleanupIssue>,
    },
    /// The attempt ended while live ownership or control-plane blockers remained.
    Incomplete {
        /// Concrete facts preventing completion.
        blockers: Vec<ShutdownBlocker>,
        /// Ordered non-blocking diagnostics observed by cleanup.
        issues: Vec<CleanupIssue>,
    },
}

#[derive(Default)]
pub(crate) struct ShutdownCompletion {
    outcome: Mutex<Option<Arc<ShutdownOutcome>>>,
    pub(crate) notify: Notify,
}

impl ShutdownCompletion {
    pub(crate) fn outcome(&self) -> Option<Arc<ShutdownOutcome>> {
        self.outcome.lock().clone()
    }

    fn publish(&self, outcome: ShutdownOutcome) -> bool {
        let mut slot = self.outcome.lock();
        if slot.is_some() {
            return false;
        }
        *slot = Some(Arc::new(outcome));
        true
    }
}

struct ShutdownOperation {
    completion: Arc<ShutdownCompletion>,
    supervisor_active: bool,
}

struct CoordinatorState {
    lifecycle: RuntimeShutdownState,
    operation: Option<ShutdownOperation>,
}

pub(crate) struct ShutdownCoordinator {
    inner: Mutex<CoordinatorState>,
}

impl Default for ShutdownCoordinator {
    fn default() -> Self {
        Self {
            inner: Mutex::new(CoordinatorState {
                lifecycle: RuntimeShutdownState::Running,
                operation: None,
            }),
        }
    }
}

impl ShutdownCoordinator {
    pub(crate) fn start_or_observe(
        &self,
        start: impl FnOnce(Arc<ShutdownCompletion>) -> JoinHandle<()>,
    ) -> Arc<ShutdownCompletion> {
        let mut state = self.inner.lock();
        if let Some(operation) = &state.operation {
            if operation.completion.outcome().is_none()
                || state.lifecycle == RuntimeShutdownState::Complete
            {
                return operation.completion.clone();
            }
        }
        state.lifecycle = RuntimeShutdownState::ShuttingDown;
        let completion = Arc::new(ShutdownCompletion::default());
        let supervisor = start(completion.clone());
        drop(supervisor);
        state.operation = Some(ShutdownOperation {
            completion: completion.clone(),
            supervisor_active: true,
        });
        completion
    }

    pub(crate) fn finish(&self, outcome: ShutdownOutcome, completion: &Arc<ShutdownCompletion>) {
        let mut state = self.inner.lock();
        state.lifecycle = if matches!(outcome, ShutdownOutcome::Incomplete { .. }) {
            RuntimeShutdownState::Incomplete
        } else {
            RuntimeShutdownState::Complete
        };
        if let Some(operation) = &mut state.operation {
            operation.supervisor_active = false;
        }
        let published = completion.publish(outcome);
        debug_assert!(published);
    }

    pub(crate) fn state(&self) -> RuntimeShutdownState {
        self.inner.lock().lifecycle
    }

    pub(crate) fn snapshot(&self) -> (RuntimeShutdownState, Option<Arc<ShutdownOutcome>>, bool) {
        let state = self.inner.lock();
        let outcome = state
            .operation
            .as_ref()
            .and_then(|operation| operation.completion.outcome());
        (
            state.lifecycle,
            outcome,
            state.lifecycle == RuntimeShutdownState::ShuttingDown,
        )
    }

    #[cfg(test)]
    pub(crate) fn completion(&self) -> Option<Arc<ShutdownCompletion>> {
        self.inner
            .lock()
            .operation
            .as_ref()
            .map(|operation| operation.completion.clone())
    }

    #[cfg(test)]
    pub(crate) fn supervisor_finished(&self) -> bool {
        self.inner
            .lock()
            .operation
            .as_ref()
            .is_some_and(|operation| !operation.supervisor_active)
    }
}
