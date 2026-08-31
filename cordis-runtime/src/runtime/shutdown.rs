impl Runtime {
    /// Shuts down all owned work through one Runtime-owned convergence attempt.
    pub async fn shutdown(&self) -> Result<(), CordisError> {
        match self.shutdown_detailed().await {
            ShutdownOutcome::Complete => Ok(()),
            ShutdownOutcome::CompleteWithIssues { issues } => Err(CordisError::CleanupFailed(
                issues
                    .iter()
                    .map(|issue| format!("{:?}: {}", issue.phase, issue.message))
                    .collect::<Vec<_>>()
                    .join("; "),
            )),
            ShutdownOutcome::Incomplete { blockers, .. } => Err(CordisError::CleanupFailed(
                format!("shutdown incomplete: {blockers:?}"),
            )),
        }
    }

    /// Runs or joins one shutdown convergence attempt and returns durable global truth.
    pub async fn shutdown_detailed(&self) -> ShutdownOutcome {
        let runtime = self.clone();
        let completion = self.0.shutdown.start_or_observe(move |completion| {
            runtime.0.admission.begin_shutdown();
            let shutdown_grace = runtime.0.config.shutdown_grace;
            let supervisor_runtime = runtime.clone();
            let supervisor_completion = completion.clone();
            tokio::spawn(async move {
                use futures::FutureExt;
                use std::panic::AssertUnwindSafe;

                let outcome = match AssertUnwindSafe(
                    supervisor_runtime.run_shutdown_attempt(shutdown_grace),
                )
                .catch_unwind()
                .await
                {
                    Ok(outcome) => outcome,
                    Err(payload) => ShutdownOutcome::Incomplete {
                        blockers: supervisor_runtime.shutdown_blockers(),
                        issues: vec![CleanupIssue {
                            phase: CleanupPhase::Worker,
                            message: CordisError::ShutdownWorkerPanicked(panic_message(payload.as_ref())).to_string(),
                            cause: Some(CordisError::ShutdownWorkerPanicked(panic_message(payload.as_ref()))),
                        }],
                    },
                };
                supervisor_runtime.finalize_shutdown(outcome, &supervisor_completion);
            })
        });
        self.wait_for_shutdown_completion(completion).await
    }

    async fn run_shutdown_attempt(&self, shutdown_grace: Duration) -> ShutdownOutcome {
        let deadline = tokio::time::Instant::now() + shutdown_grace;
        *self.0.shutdown_deadline.lock() = Some(deadline);
        let mut issues = Vec::new();
        let root_result = tokio::time::timeout_at(
            deadline,
            self.dispose_scope_for_shutdown(self.root()),
        )
        .await;
        match root_result {
            Ok(Ok(observation)) => {
                issues.extend(observation.issues.clone());
                self.0.workers.shutdown_until(deadline).await;
                self.converge_shutdown_residue(deadline, &mut issues).await;
            }
            Ok(Err(error)) => issues.push(CleanupIssue {
                phase: CleanupPhase::ChildScopeCleanup,
                message: error.to_string(),
                cause: Some(error),
            }),
            Err(_) => {}
        }
        let _ = self.collect_garbage();
        let blockers = self.shutdown_blockers();
        if blockers.is_empty() {
            if issues.is_empty() {
                ShutdownOutcome::Complete
            } else {
                ShutdownOutcome::CompleteWithIssues { issues }
            }
        } else {
            ShutdownOutcome::Incomplete { blockers, issues }
        }
    }

    fn shutdown_blockers(&self) -> Vec<ShutdownBlocker> {
        let mut blockers = Vec::new();
        let fibers = self.0.fibers.snapshot();
        for (fiber, cell) in &fibers {
            let record = cell.inner.read();
            if record.state != FiberState::Disposed {
                blockers.push(ShutdownBlocker::Fiber(*fiber));
            }
            if record
                .host_processes
                .iter()
                .any(|live| live.load(Ordering::Acquire))
            {
                blockers.push(ShutdownBlocker::HostedExecution { fiber: *fiber });
            }
            let (_, inflight) = record.capabilities.execution_snapshot();
            let service_handles = record.capabilities.service_handle_inflight();
            if inflight != 0 || service_handles != 0 {
                blockers.push(ShutdownBlocker::GenerationInflight {
                    fiber: *fiber,
                    generation: record.generation.get(),
                    inflight,
                    service_handles,
                });
            }
        }
        let scopes = self.0.scopes.read();
        for (scope, record) in scopes.iter() {
            if scope != self.0.root && record.state != ScopeState::Disposed {
                blockers.push(ShutdownBlocker::Scope(scope));
            }
        }
        let staging_scopes = scopes
            .values()
            .filter(|scope| scope.hidden && scope.state != ScopeState::Disposed)
            .count();
        drop(scopes);
        let workers = self.0.workers.live();
        if workers != 0 {
            blockers.push(ShutdownBlocker::RuntimeWorkers(workers));
        }
        let tasks = self.0.tasks.live_fiber_tasks();
        if tasks != 0 {
            blockers.push(ShutdownBlocker::Tasks(tasks));
        }
        let reloads = self.0.active_reloads.load(Ordering::Relaxed)
            + self.0.reload_cleanup_pending.load(Ordering::Relaxed);
        if reloads != 0 {
            blockers.push(ShutdownBlocker::ReloadTransactions(reloads));
        }
        let staging_fibers = fibers
            .iter()
            .filter(|(_, cell)| {
                let record = cell.inner.read();
                record.staged && record.state != FiberState::Disposed
            })
            .count();
        if staging_fibers != 0 || staging_scopes != 0 {
            blockers.push(ShutdownBlocker::Staging {
                fibers: staging_fibers,
                scopes: staging_scopes,
            });
        }
        blockers
    }

    async fn converge_shutdown_residue(
        &self,
        deadline: tokio::time::Instant,
        issues: &mut Vec<CleanupIssue>,
    ) {
        for _ in 0..4 {
            let scopes: Vec<_> = self
                .0
                .scopes
                .read()
                .iter()
                .filter_map(|(scope, record)| {
                    (scope != self.0.root && record.state != ScopeState::Disposed).then_some(scope)
                })
                .collect();
            for scope in &scopes {
                match tokio::time::timeout_at(deadline, self.dispose_scope_for_shutdown(*scope))
                    .await
                {
                    Ok(Ok(observation)) => issues.extend(observation.issues.clone()),
                    Ok(Err(error)) => issues.push(CleanupIssue {
                        phase: CleanupPhase::ChildScopeCleanup,
                        message: error.to_string(),
                        cause: Some(error),
                    }),
                    Err(_) => return,
                }
            }
            let fibers: Vec<_> = self
                .0
                .fibers
                .snapshot()
                .into_iter()
                .filter_map(|(fiber, cell)| {
                    (cell.inner.read().state != FiberState::Disposed).then_some(fiber)
                })
                .collect();
            for fiber in &fibers {
                if tokio::time::timeout_at(
                    deadline,
                    self.dispose_fiber_persistently(*fiber, false),
                )
                .await
                .is_err()
                {
                    return;
                }
            }
            if scopes.is_empty() && fibers.is_empty() {
                return;
            }
            tokio::task::yield_now().await;
        }
    }

    async fn wait_for_shutdown_completion(
        &self,
        completion: Arc<ShutdownCompletion>,
    ) -> ShutdownOutcome {
        loop {
            if let Some(outcome) = completion.outcome() {
                return (*outcome).clone();
            }
            let notified = completion.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(outcome) = completion.outcome() {
                return (*outcome).clone();
            }
            notified.await;
        }
    }

    fn finalize_shutdown(
        &self,
        outcome: ShutdownOutcome,
        completion: &Arc<ShutdownCompletion>,
    ) {
        let complete = !matches!(outcome, ShutdownOutcome::Incomplete { .. });
        self.0.shutdown.finish(outcome, completion);
        if complete {
            self.0.admission.complete_shutdown();
        }
        completion.notify.notify_waiters();
    }

    /// Returns the detailed shutdown lifecycle without exposing internal locks.
    #[must_use]
    pub fn shutdown_state(&self) -> RuntimeShutdownState {
        self.0.shutdown.state()
    }
}

impl Default for Runtime {
    fn default() -> Self {
        Self::new()
    }
}
