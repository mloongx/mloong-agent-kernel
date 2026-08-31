//! Bounded, payload-free runtime health diagnostics.

use crate::RuntimeShutdownState;
use cordis_core::{FiberId, InvocationId, ScopeId};
use parking_lot::Mutex;
use std::{collections::VecDeque, sync::atomic::AtomicU64, time::SystemTime};

/// Overall runtime health classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeHealth {
    /// Running without known lifecycle failures.
    Healthy,
    /// Running with recoverable waiting or operation errors.
    Degraded,
    /// Runtime-owned shutdown is active.
    ShuttingDown,
    /// A terminal lifecycle failure or failed shutdown exists.
    Failed,
}

/// Payload-free diagnostic category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum HealthIssueKind {
    /// Invocation returned an error.
    InvocationError,
    /// Invocation timed out.
    InvocationTimeout,
    /// Invocation was cancelled.
    InvocationCancelled,
    /// Handler or middleware panicked.
    InvocationPanic,
    /// A Fiber-owned task panicked.
    TaskPanic,
    /// A Runtime-owned worker panicked.
    RuntimeWorkerPanic,
    /// A Runtime-owned worker returned an error.
    RuntimeWorkerError,
    /// A quota rejected work.
    QuotaRejected,
}

/// One bounded diagnostic entry. Business values are never retained.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct HealthIssue {
    /// Observation time.
    pub at: SystemTime,
    /// Stable category.
    pub kind: HealthIssueKind,
    /// Related Scope, when available.
    pub scope: Option<ScopeId>,
    /// Related Fiber, when available.
    pub fiber: Option<FiberId>,
    /// Related Invocation, when available.
    pub invocation: Option<InvocationId>,
}

/// Synchronous health and counter snapshot.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct HealthReport {
    /// Overall classification.
    pub status: RuntimeHealth,
    /// Active Scope count.
    pub active_scopes: usize,
    /// Active Fiber count.
    pub active_fibers: usize,
    /// Fibers waiting for dependencies.
    pub waiting_fibers: usize,
    /// Fibers currently disposing.
    pub disposing_fibers: usize,
    /// Fibers with terminated cleanup.
    pub terminated_fibers: usize,
    /// Scopes with terminated cleanup.
    pub terminated_scopes: usize,
    /// Number of owned tasks.
    pub active_tasks: usize,
    /// Fiber tasks reclaimed after reaching a terminal outcome.
    pub reaped_tasks: u64,
    /// Fiber tasks that completed normally.
    pub completed_tasks: u64,
    /// Fiber tasks that completed through cancellation.
    pub cancelled_tasks: u64,
    /// Fiber task panics observed by the supervisor.
    pub task_panics: u64,
    /// Fiber tasks forcibly aborted after their shared deadline.
    pub aborted_tasks: u64,
    /// Runtime-owned workers currently supervised.
    pub live_runtime_workers: usize,
    /// Runtime-owned workers reclaimed after completion.
    pub reaped_runtime_workers: u64,
    /// Runtime-owned worker panics observed by the supervisor.
    pub runtime_worker_panics: u64,
    /// Runtime-owned worker errors observed by the supervisor.
    pub runtime_worker_errors: u64,
    /// Runtime workers that completed through cancellation.
    pub cancelled_runtime_workers: u64,
    /// Runtime workers forcibly aborted at deadline.
    pub aborted_runtime_workers: u64,
    /// Invocations currently holding a concurrency permit.
    pub active_invocations: usize,
    /// Generations currently accepting provider execution.
    pub active_generation_executions: usize,
    /// Generations that have closed execution admission and are draining.
    pub draining_generations: usize,
    /// Runtime-tracked Invocation/Event executions holding generation leases.
    pub provider_inflight: usize,
    /// Live generation leases owned by returned `ServiceHandle` values.
    pub service_handle_inflight: usize,
    /// Runtime-owned reload transactions still running.
    pub active_reloads: usize,
    /// Committed reloads with old-generation cleanup pending.
    pub reload_cleanup_pending: usize,
    /// Hidden staged Fibers awaiting commit or rollback.
    pub staging_fibers: usize,
    /// Hidden staging Scopes awaiting commit cleanup or rollback.
    pub staging_scopes: usize,
    /// Shutdown lifecycle.
    pub shutdown_state: RuntimeShutdownState,
    /// Completed successful invocations.
    pub invocation_successes: u64,
    /// Completed invocation errors.
    pub invocation_errors: u64,
    /// Invocation timeouts.
    pub invocation_timeouts: u64,
    /// Invocation cancellations.
    pub invocation_cancellations: u64,
    /// Isolated invocation panics.
    pub invocation_panics: u64,
    /// Quota rejections observed by instrumented paths.
    pub quota_rejections: u64,
    /// Most recent payload-free issues, oldest first.
    pub recent_errors: Vec<HealthIssue>,
}

pub(crate) struct Diagnostics {
    pub(crate) successes: AtomicU64,
    pub(crate) errors: AtomicU64,
    pub(crate) timeouts: AtomicU64,
    pub(crate) cancellations: AtomicU64,
    pub(crate) panics: AtomicU64,
    pub(crate) quota_rejections: AtomicU64,
    recent: Mutex<VecDeque<HealthIssue>>,
}

impl Default for Diagnostics {
    fn default() -> Self {
        Self {
            successes: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            timeouts: AtomicU64::new(0),
            cancellations: AtomicU64::new(0),
            panics: AtomicU64::new(0),
            quota_rejections: AtomicU64::new(0),
            recent: Mutex::new(VecDeque::with_capacity(64)),
        }
    }
}

impl Diagnostics {
    pub(crate) fn push(&self, issue: HealthIssue) {
        let mut recent = self.recent.lock();
        if recent.len() == 64 {
            recent.pop_front();
        }
        recent.push_back(issue);
    }

    pub(crate) fn recent(&self) -> Vec<HealthIssue> {
        self.recent.lock().iter().cloned().collect()
    }
}
