impl Runtime {
    pub(crate) fn request_gc(&self) {
        let Ok(_admission) = self.0.admission.enter() else {
            return;
        };
        #[cfg(test)]
        if let Some(hook) = self.0.gc_registration_hook.lock().clone() {
            hook.wait();
            hook.wait();
        }
        match self
            .0
            .gc_state
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => {
                let runtime = self.clone();
                self.0.workers.spawn(RuntimeWorkerKind::GcReconcile, true, async move {
                    tokio::task::yield_now().await;
                    loop {
                        let _ = runtime.collect_garbage();
                        match runtime.0.gc_state.compare_exchange(
                            1,
                            0,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        ) {
                            Err(2) => {
                                let _ = runtime.0.gc_state.compare_exchange(
                                    2,
                                    1,
                                    Ordering::AcqRel,
                                    Ordering::Acquire,
                                );
                            }
                            Ok(_) | Err(_) => return Ok(()),
                        }
                    }
                });
            }
            Err(1) => {
                let _ = self
                    .0
                    .gc_state
                    .compare_exchange(1, 2, Ordering::AcqRel, Ordering::Acquire);
            }
            Err(_) => {}
        }
    }

    /// Returns a synchronous, payload-free health report.
    #[must_use]
    pub fn health(&self) -> HealthReport {
        let shutdown_state = self.0.shutdown.state();
        let (terminated_scopes, active_scopes, staging_scopes) = {
            let scopes = self.0.scopes.read();
            (
                scopes.values().filter(|scope| scope.state == ScopeState::Terminated).count(),
                scopes.values().filter(|scope| scope.state == ScopeState::Active).count(),
                scopes
                    .values()
                    .filter(|scope| scope.hidden && scope.state != ScopeState::Disposed)
                    .count(),
            )
        };
        let fibers = self.0.fibers.snapshot();
        let terminated_fibers = fibers
            .iter()
            .filter(|(_, cell)| cell.inner.read().disposal.phase == DisposalPhase::Terminated)
            .count();
        let waiting_fibers = fibers
            .iter()
            .filter(|(_, cell)| cell.inner.read().state == FiberState::WaitingDependencies)
            .count();
        let execution_snapshots: Vec<_> = fibers.iter().map(|(_, cell)| cell.inner.read().capabilities.execution_snapshot()).collect();
        let status = match shutdown_state {
            RuntimeShutdownState::ShuttingDown => RuntimeHealth::ShuttingDown,
            RuntimeShutdownState::Incomplete => RuntimeHealth::Failed,
            _ if terminated_scopes != 0 || terminated_fibers != 0 => RuntimeHealth::Failed,
            RuntimeShutdownState::Running if waiting_fibers != 0 => RuntimeHealth::Degraded,
            RuntimeShutdownState::Running | RuntimeShutdownState::Complete => RuntimeHealth::Healthy,
        };
        HealthReport {
            status,
            active_scopes,
            active_fibers: fibers
                .iter()
                .filter(|(_, cell)| cell.inner.read().state == FiberState::Active)
                .count(),
            waiting_fibers,
            disposing_fibers: fibers
                .iter()
                .filter(|(_, cell)| cell.inner.read().state == FiberState::Disposing)
                .count(),
            terminated_fibers,
            terminated_scopes,
            active_tasks: self.0.tasks.live_fiber_tasks(),
            reaped_tasks: self.0.tasks.reaped(),
            completed_tasks: self.0.tasks.completed(),
            cancelled_tasks: self.0.tasks.cancelled(),
            task_panics: self.0.tasks.panicked(),
            aborted_tasks: self.0.tasks.aborted(),
            live_runtime_workers: self.0.workers.live(),
            reaped_runtime_workers: self.0.workers.reaped(),
            runtime_worker_panics: self.0.workers.panicked(),
            runtime_worker_errors: self.0.workers.errors(),
            cancelled_runtime_workers: self.0.workers.cancelled(),
            aborted_runtime_workers: self.0.workers.aborted(),
            active_invocations: self.0.config.max_concurrent_invocations
                - self.0.invocation_permits.available_permits(),
            active_generation_executions: execution_snapshots.iter().filter(|(state, _)| *state == GenerationExecutionState::Accepting).count(),
            draining_generations: execution_snapshots.iter().filter(|(state, _)| *state == GenerationExecutionState::Draining).count(),
            provider_inflight: execution_snapshots.iter().map(|(_, inflight)| inflight).sum(),
            service_handle_inflight: fibers.iter().map(|(_, cell)| {
                cell.inner.read().capabilities.service_handle_inflight()
            }).sum(),
            active_reloads: self.0.active_reloads.load(Ordering::Relaxed),
            reload_cleanup_pending: self.0.reload_cleanup_pending.load(Ordering::Relaxed),
            staging_fibers: fibers
                .iter()
                .filter(|(_, cell)| {
                    let fiber = cell.inner.read();
                    fiber.staged && fiber.state != FiberState::Disposed
                })
                .count(),
            staging_scopes,
            shutdown_state,
            invocation_successes: self.0.diagnostics.successes.load(Ordering::Relaxed),
            invocation_errors: self.0.diagnostics.errors.load(Ordering::Relaxed),
            invocation_timeouts: self.0.diagnostics.timeouts.load(Ordering::Relaxed),
            invocation_cancellations: self
                .0
                .diagnostics
                .cancellations
                .load(Ordering::Relaxed),
            invocation_panics: self.0.diagnostics.panics.load(Ordering::Relaxed),
            quota_rejections: self
                .0
                .diagnostics
                .quota_rejections
                .load(Ordering::Relaxed),
            recent_errors: self.0.diagnostics.recent(),
        }
    }

    /// Returns an introspection snapshot without exposing internal locks.
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn snapshot(&self) -> RuntimeSnapshot {
        let (shutdown_state, shutdown_result, shutdown_in_progress) = self.0.shutdown.snapshot();
        let (scope_snapshots, terminated_scope_count, staging_scope_count) = {
            let scopes = self.0.scopes.read();
            let terminated = scopes.values().filter(|scope| scope.state == ScopeState::Terminated).count();
            let snapshots = scopes.iter().map(|(id, s)| ScopeSnapshot {
                id,
                name: s.name.clone(),
                parent: s.parent,
                disposed: s.state != ScopeState::Active,
                state: s.state,
                disposal_error: s.disposal.result.as_ref().and_then(|result| result.as_ref().err()).map(ToString::to_string),
                unfinished_cleanup: matches!(s.state, ScopeState::Disposing | ScopeState::Terminated),
                child_count: s.children.len(),
                fiber_count: s.fibers.len(),
                hidden: s.hidden,
            }).collect();
            let staging = scopes
                .values()
                .filter(|scope| scope.hidden && scope.state != ScopeState::Disposed)
                .count();
            (snapshots, terminated, staging)
        };
        let fibers = self.0.fibers.snapshot();
        RuntimeSnapshot {
            scopes: scope_snapshots,
            fibers: fibers
                .iter()
                .map(|(id, cell)| {
                    let f = cell.inner.read();
                    FiberSnapshot {
                    id: *id,
                    plugin_id: cell.plugin_id,
                    plugin: f.descriptor.name.clone(),
                    scope: f.scope,
                    state: f.state,
                    dependencies: f.descriptor.dependencies.clone(),
                    declared_services: f.descriptor.provisions.clone(),
                    provided_services: f.provided.clone(),
                    task_ids: f.tasks.clone(),
                    handler_ids: f.handlers.clone(),
                    task_count: f.tasks.len(),
                    effect_count: f.effects.len(),
                    disposal_phase: f.disposal.phase,
                    disposal_error: f
                        .disposal
                        .result
                        .as_ref()
                        .and_then(|result| result.as_ref().err())
                        .map(ToString::to_string),
                    unfinished_cleanup: !matches!(
                        f.disposal.phase,
                        DisposalPhase::Idle | DisposalPhase::Complete
                    ),
                }})
                .collect(),
            service_count: self.0.services.count(),
            services: self.0.services.snapshots()
                .into_iter()
                .map(|(scope, symbol, owner)| ServiceSnapshot {
                    scope,
                    symbol,
                    owner,
                })
                .collect(),
            handler_count: fibers.iter().map(|(_, cell)| cell.inner.read().handlers.len()).sum(),
            live_fiber_tasks: self.0.tasks.live_fiber_tasks(),
            reaped_tasks: self.0.tasks.reaped(),
            completed_tasks: self.0.tasks.completed(),
            cancelled_tasks: self.0.tasks.cancelled(),
            task_panics: self.0.tasks.panicked(),
            aborted_tasks: self.0.tasks.aborted(),
            live_runtime_workers: self.0.workers.live(),
            reaped_runtime_workers: self.0.workers.reaped(),
            runtime_worker_panics: self.0.workers.panicked(),
            runtime_worker_errors: self.0.workers.errors(),
            cancelled_runtime_workers: self.0.workers.cancelled(),
            aborted_runtime_workers: self.0.workers.aborted(),
            active_generation_executions: fibers.iter().filter(|(_, cell)| cell.inner.read().capabilities.execution_snapshot().0 == GenerationExecutionState::Accepting).count(),
            draining_generations: fibers.iter().filter(|(_, cell)| cell.inner.read().capabilities.execution_snapshot().0 == GenerationExecutionState::Draining).count(),
            provider_inflight: fibers.iter().map(|(_, cell)| cell.inner.read().capabilities.execution_snapshot().1).sum(),
            service_handle_inflight: fibers.iter().map(|(_, cell)| {
                cell.inner.read().capabilities.service_handle_inflight()
            }).sum(),
            active_reloads: self.0.active_reloads.load(Ordering::Relaxed),
            reload_cleanup_pending: self.0.reload_cleanup_pending.load(Ordering::Relaxed),
            staging_fibers: fibers
                .iter()
                .filter(|(_, cell)| {
                    let fiber = cell.inner.read();
                    fiber.staged && fiber.state != FiberState::Disposed
                })
                .count(),
            staging_scopes: staging_scope_count,
            shutting_down: shutdown_state != RuntimeShutdownState::Running,
            shutdown_state,
            shutdown_error: shutdown_result.and_then(|outcome| match outcome.as_ref() {
                ShutdownOutcome::Complete => None,
                ShutdownOutcome::CompleteWithIssues { issues } => Some(format!(
                    "shutdown completed with {} cleanup issue(s)",
                    issues.len()
                )),
                ShutdownOutcome::Incomplete { blockers, .. } => {
                    Some(format!("shutdown incomplete with {} blocker(s)", blockers.len()))
                }
            }),
            shutdown_in_progress,
            terminated_scope_count,
            terminated_fiber_count: fibers
                .iter()
                .filter(|(_, cell)| cell.inner.read().disposal.phase == DisposalPhase::Terminated)
                .count(),
        }
    }

    /// Reclaims disposed arena slots while preserving generation safety.
    #[must_use]
    pub fn collect_garbage(&self) -> GarbageReport {
        let disposed_fibers: Vec<_> = self.0.fibers
            .snapshot()
            .into_iter()
            .filter_map(|(id, cell)| {
                (cell.inner.read().state == FiberState::Disposed).then_some((id, cell.plugin_id))
            })
            .collect();
        let mut removed = Vec::new();
        for (fiber, plugin) in &disposed_fibers {
            if self.0.fibers.remove(*fiber).is_none() {
                continue;
            }
            removed.push((*fiber, *plugin));
        }
        let disposed_ids: HashSet<_> = removed.iter().map(|(id, _)| *id).collect();
        self.0.dependencies.retain_fibers(|id| !disposed_ids.contains(&id));
        for (_, plugin) in &removed {
            let detached = self.0.plugins.detach_fiber(*plugin);
            debug_assert!(detached.is_ok(), "fiber/plugin accounting diverged: {detached:?}");
            self.0.plugins.reclaim_if_dead(*plugin);
        }
        let mut scopes = self.0.scopes.write();
        for scope in scopes.values_mut() {
            scope.fibers.retain(|id| !disposed_ids.contains(id));
        }
        let disposed_scopes: Vec<_> = scopes
            .iter()
            .filter_map(|(id, scope)| {
                (id != self.0.root
                    && scope.state == ScopeState::Disposed
                    && scope.children.is_empty()
                    && scope.fibers.is_empty())
                .then_some(id)
            })
            .collect();
        for scope in &disposed_scopes {
            scopes.remove(*scope);
        }
        GarbageReport {
            fibers: removed.len(),
            scopes: disposed_scopes.len(),
        }
    }
}

use tracing::Instrument;

/// Read-only runtime state.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct RuntimeSnapshot {
    /// All allocated scopes.
    pub scopes: Vec<ScopeSnapshot>,
    /// All allocated fibers.
    pub fibers: Vec<FiberSnapshot>,
    /// Number of active service registrations.
    pub service_count: usize,
    /// All currently visible service registrations and owners.
    pub services: Vec<ServiceSnapshot>,
    /// Number of active handlers.
    pub handler_count: usize,
    /// Fiber tasks currently tracked by the supervisor.
    pub live_fiber_tasks: usize,
    /// Fiber tasks reclaimed after terminal completion.
    pub reaped_tasks: u64,
    /// Fiber tasks that completed normally.
    pub completed_tasks: u64,
    /// Fiber tasks that completed through cancellation.
    pub cancelled_tasks: u64,
    /// Fiber task panics observed by the supervisor.
    pub task_panics: u64,
    /// Fiber tasks aborted after the shared shutdown deadline.
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
    /// Generations currently accepting provider execution.
    pub active_generation_executions: usize,
    /// Generations that are waiting for provider execution leases to leave.
    pub draining_generations: usize,
    /// Runtime-tracked provider executions currently in flight.
    pub provider_inflight: usize,
    /// Live `ServiceHandle` leases across all generations.
    pub service_handle_inflight: usize,
    /// Runtime-owned reload transactions still running.
    pub active_reloads: usize,
    /// Committed reloads whose old-generation cleanup is still converging.
    pub reload_cleanup_pending: usize,
    /// Fibers currently hidden in reload preparation.
    pub staging_fibers: usize,
    /// Internal hidden scopes currently owned by reload preparation/finalization.
    pub staging_scopes: usize,
    /// Whether shutdown began.
    pub shutting_down: bool,
    /// Detailed shutdown lifecycle.
    pub shutdown_state: RuntimeShutdownState,
    /// Stable terminal shutdown error for diagnostics.
    pub shutdown_error: Option<String>,
    /// Whether the Runtime-owned shutdown operation is still active.
    pub shutdown_in_progress: bool,
    /// Number of scopes with unfinished terminated cleanup.
    pub terminated_scope_count: usize,
    /// Number of fibers with unfinished terminated cleanup.
    pub terminated_fiber_count: usize,
}
/// Number of generation-arena slots reclaimed by a garbage collection pass.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct GarbageReport {
    /// Reclaimed fibers.
    pub fibers: usize,
    /// Reclaimed scopes.
    pub scopes: usize,
}
/// Read-only service ownership state.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ServiceSnapshot {
    /// Providing scope.
    pub scope: ScopeId,
    /// Interned service identity.
    pub symbol: ServiceSymbol,
    /// Owning fiber.
    pub owner: FiberId,
}
/// Read-only scope state.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ScopeSnapshot {
    /// Scope handle.
    pub id: ScopeId,
    /// Diagnostic name.
    pub name: Arc<str>,
    /// Parent, absent only for root.
    pub parent: Option<ScopeId>,
    /// Disposal marker.
    pub disposed: bool,
    /// Detailed lifecycle state.
    pub state: ScopeState,
    /// Terminal scope cleanup error for diagnostics.
    pub disposal_error: Option<String>,
    /// Whether traversal is active or terminated before completion.
    pub unfinished_cleanup: bool,
    /// Direct child scope count.
    pub child_count: usize,
    /// Direct owned fiber count.
    pub fiber_count: usize,
    /// Whether this is an internal transaction staging scope.
    pub hidden: bool,
}
/// Read-only fiber state.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct FiberSnapshot {
    /// Fiber handle.
    pub id: FiberId,
    /// Logical plugin handle.
    pub plugin_id: PluginId,
    /// Plugin name.
    pub plugin: Arc<str>,
    /// Owning scope.
    pub scope: ScopeId,
    /// Lifecycle state.
    pub state: FiberState,
    /// Required services.
    pub dependencies: Arc<[ServiceKey]>,
    /// Services declared before activation for graph validation.
    pub declared_services: Arc<[ServiceKey]>,
    /// Services currently provided.
    pub provided_services: Vec<ServiceKey>,
    /// Owned task handles.
    pub task_ids: Vec<TaskId>,
    /// Owned event handler handles.
    pub handler_ids: Vec<HandlerId>,
    /// Owned task count.
    pub task_count: usize,
    /// Owned effect count.
    pub effect_count: usize,
    /// Durable disposal progress for diagnostics.
    pub disposal_phase: DisposalPhase,
    /// Terminal disposal error, when the worker did not finish cleanly.
    pub disposal_error: Option<String>,
    /// Whether cleanup is active or terminated with resources potentially unfinished.
    pub unfinished_cleanup: bool,
}

