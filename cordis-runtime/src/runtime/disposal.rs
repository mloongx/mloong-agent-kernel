impl Runtime {
    /// Disposes a plugin fiber and all resources it owns.
    pub async fn dispose_fiber(
        &self,
        fiber: FiberId,
        wait_for_dependencies: bool,
    ) -> Result<(), CordisError> {
        self.dispose_fiber_observation(fiber, wait_for_dependencies, false, false)
            .await
            .and_then(|observation| observation.legacy_result.clone())
    }

    /// Disposes a Fiber while preserving committed lifecycle truth separately
    /// from ordered best-effort cleanup diagnostics.
    pub async fn dispose_fiber_detailed(
        &self,
        fiber: FiberId,
        wait_for_dependencies: bool,
    ) -> DisposeOutcome {
        match self
            .dispose_fiber_observation(fiber, wait_for_dependencies, false, false)
            .await
        {
            Ok(observation) => observation.fiber_outcome(),
            Err(primary) => DisposeOutcome::Incomplete {
                primary,
                issues: Vec::new(),
            },
        }
    }

    pub(crate) async fn dispose_reload_owned_fiber(
        &self,
        fiber: FiberId,
    ) -> Result<(), CordisError> {
        self.dispose_fiber_observation(fiber, false, false, true)
            .await
            .and_then(|observation| observation.legacy_result.clone())
    }

    pub(crate) async fn dispose_reload_owned_fiber_persistently(
        &self,
        fiber: FiberId,
    ) -> Result<(), CordisError> {
        self.dispose_fiber_observation(fiber, false, true, true)
            .await
            .and_then(|observation| observation.legacy_result.clone())
    }

    pub(crate) async fn dispose_reload_owned_fiber_observation(
        &self,
        fiber: FiberId,
    ) -> Result<Arc<DisposalObservation>, CordisError> {
        self.dispose_fiber_observation(fiber, false, false, true).await
    }

    async fn dispose_fiber_persistently(
        &self,
        fiber: FiberId,
        wait_for_dependencies: bool,
    ) -> Result<(), CordisError> {
        self.dispose_fiber_observation(fiber, wait_for_dependencies, true, false)
            .await
            .and_then(|observation| observation.legacy_result.clone())
    }

    async fn dispose_fiber_observation(
        &self,
        fiber: FiberId,
        wait_for_dependencies: bool,
        persistent_drain: bool,
        allow_reload_owned: bool,
    ) -> Result<Arc<DisposalObservation>, CordisError> {
        let cell = self.0.fibers.get(fiber).ok_or(CordisError::FiberNotFound)?;
        let completion = {
            let mut record = cell.inner.write();
            if record.reload_owned && !allow_reload_owned {
                return Err(CordisError::FiberLifecycleOwned(fiber));
            }
            if allow_reload_owned {
                record.reload_owned = false;
            }
            // A permanent disposal dominates a dependency-restart disposal.
            record.disposal.wait_for_dependencies &= wait_for_dependencies;
            record.disposal.persistent_drain |= persistent_drain;
            if record.disposal.phase == DisposalPhase::Idle {
                if record.state != FiberState::Disposing {
                    if !record.state.can_transition_to(FiberState::Disposing) {
                        return Err(CordisError::InvalidFiberState {
                            fiber,
                            from: record.state,
                            to: FiberState::Disposing,
                        });
                    }
                    record.state = FiberState::Disposing;
                }
                record.cancellation.cancel();
                record.capabilities.close();
                record.activation.take();
                record.activation_sealed = false;
                record.disposal.phase = DisposalPhase::Tasks;
                let body_runtime = self.clone();
                let body =
                    tokio::spawn(async move { body_runtime.run_fiber_disposal_body(fiber).await });
                #[cfg(test)]
                {
                    record.disposal.worker_abort = Some(body.abort_handle());
                }
                let supervisor_runtime = self.clone();
                let supervisor = tokio::spawn(async move {
                    let (result, target, terminated) = match body.await {
                        Ok(outcome) => outcome,
                        Err(error) if error.is_cancelled() => {
                            (Err(CordisError::DisposalWorkerCancelled), None, true)
                        }
                        Err(error) => {
                            let payload = error.into_panic();
                            let message = panic_message(payload.as_ref());
                            (
                                Err(CordisError::DisposalWorkerPanicked(message)),
                                None,
                                true,
                            )
                        }
                    };
                    if let Err(error) = supervisor_runtime
                        .finalize_fiber_disposal(fiber, result, target, terminated)
                        .await
                    {
                        debug!(?fiber, %error, "disposal supervisor could not publish result");
                    }
                });
                record.disposal.supervisor = Some(supervisor);
            }
            record.disposal.completion.clone()
        };
        self.wait_for_fiber_disposal(completion).await
    }

    async fn wait_for_fiber_disposal(
        &self,
        completion: Arc<DisposalCompletion>,
    ) -> Result<Arc<DisposalObservation>, CordisError> {
        #[cfg(test)]
        {
            completion
                .waiter_registrations
                .fetch_add(1, Ordering::SeqCst);
            completion.waiter_notify.notify_waiters();
        }
        loop {
            if let Some(observation) = completion.observation() {
                return Ok(observation);
            }
            let notified = completion.notify.notified();
            if let Some(observation) = completion.observation() {
                return Ok(observation);
            }
            notified.await;
        }
    }

    fn set_disposal_phase(&self, fiber: FiberId, phase: DisposalPhase) {
        if let Some(cell) = self.0.fibers.get(fiber) {
            let mut record = cell.inner.write();
            record.disposal.phase = phase;
        }
    }

    fn push_disposal_error(&self, fiber: FiberId, phase: CleanupPhase, error: CordisError) {
        if let Some(cell) = self.0.fibers.get(fiber) {
            let mut record = cell.inner.write();
            record.disposal.errors.push(CleanupIssue {
                phase,
                message: error.to_string(),
                cause: Some(error),
            });
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn run_fiber_disposal_body(
        &self,
        fiber: FiberId,
    ) -> (Result<(), CordisError>, Option<FiberState>, bool) {
        let Some(cell) = self.0.fibers.get(fiber) else {
            return (
                Err(CordisError::DisposalWorkerTerminated(
                    "fiber disappeared before lifecycle serialization".into(),
                )),
                None,
                true,
            );
        };
        let lifecycle = cell.lifecycle.clone();
        let _operation = lifecycle.lock_owned().await;
        // The job, rather than the caller's future, owns resources while an
        // async cleanup step is in flight. Completed steps are removed from the
        // FiberRecord one at a time, leaving durable progress for inspection.
        let (tasks, cancellation, capabilities) = {
            let mut record = cell.inner.write();
            (
                std::mem::take(&mut record.tasks),
                record.cancellation.clone(),
                record.capabilities.clone(),
            )
        };
        cancellation.cancel();
        if let Err(error) = self
            .0
            .tasks
            .cancel_all(tasks, self.0.config.task_grace)
            .await
        {
            self.push_disposal_error(fiber, CleanupPhase::TaskDrain, error);
        }

        self.set_disposal_phase(fiber, DisposalPhase::Draining);
        loop {
            let drain_deadline = tokio::time::Instant::now() + self.0.config.task_grace;
            if let DrainOutcome::TimedOut { remaining } =
                capabilities.drain_until(drain_deadline).await
            {
                let persistent = self
                    .0
                    .fibers
                    .with(fiber, |record| record.disposal.persistent_drain)
                    .unwrap_or(false);
                if persistent {
                    continue;
                }
                return (
                    Err(CordisError::CleanupFailed(format!(
                        "generation drain timed out with {remaining} provider executions"
                    ))),
                    None,
                    true,
                );
            }
            break;
        }

        self.set_disposal_phase(fiber, DisposalPhase::ChildScopes);
        loop {
            let child = self.0.fibers.with(fiber, |record| record.child_scopes.last().copied()).flatten();
            let Some(child) = child else { break };
            if let Err(error) = self.dispose_scope(child).await {
                self.push_disposal_error(fiber, CleanupPhase::ChildScopeCleanup, error);
            }
            self.0.fibers.with_mut(fiber, |record| record.child_scopes.retain(|id| *id != child));
        }

        self.set_disposal_phase(fiber, DisposalPhase::Handlers);
        loop {
            let handler = self.0.fibers.with_mut(fiber, |record| record.handlers.pop()).flatten();
            let Some(handler) = handler else { break };
            self.0.events.remove(handler);
        }
        loop {
            let handler = self.0.fibers.with_mut(fiber, |record| record.invocation_handlers.pop()).flatten();
            let Some(handler) = handler else { break };
            self.0.invocations.remove_handler(handler);
        }
        loop {
            let middleware = self.0.fibers.with_mut(fiber, |record| record.invocation_middleware.pop()).flatten();
            let Some(middleware) = middleware else { break };
            self.0.invocations.remove_middleware(middleware);
        }

        self.set_disposal_phase(fiber, DisposalPhase::Services);
        let owner_scope = match self.fiber_scope(fiber) {
            Ok(scope) => scope,
            Err(error) => {
                return (
                    Err(CordisError::DisposalWorkerTerminated(error.to_string())),
                    None,
                    true,
                );
            }
        };
        let mut removed_symbols = Vec::new();
        loop {
            let service = self.0.fibers.with_mut(fiber, |record| record.provided.pop()).flatten();
            let Some(service) = service else { break };
            let symbol = self.intern(&service);
            if self.0.services.get(owner_scope, symbol).is_some_and(|entry| entry.owner == fiber) {
                self.0.services.remove(owner_scope, symbol);
                removed_symbols.push(symbol);
            }
        }
        self.bump_service_epoch();

        self.set_disposal_phase(fiber, DisposalPhase::Effects);
        loop {
            let effect = self.0.fibers.with_mut(fiber, |record| record.effects.pop()).flatten();
            let Some(effect) = effect else { break };
            let result = AssertUnwindSafe(effect.dispose()).catch_unwind().await;
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => self.push_disposal_error(fiber, CleanupPhase::EffectCleanup, error),
                Err(payload) => self.push_disposal_error(
                    fiber, CleanupPhase::EffectCleanup,
                    CordisError::PluginDisposeFailed(payload.downcast_ref::<&str>().map_or_else(
                        || {
                            payload
                                .downcast_ref::<String>()
                                .cloned()
                                .unwrap_or_else(|| "non-string dispose panic".into())
                        },
                        |message| (*message).to_owned(),
                    )),
                ),
            }
        }

        #[cfg(test)]
        self.run_disposal_test_hook(fiber).await;

        self.set_disposal_phase(fiber, DisposalPhase::Reconciling);
        let wait_for_dependencies = self.0.fibers
            .with(fiber, |record| record.disposal.wait_for_dependencies)
            .unwrap_or(false);
        let target = if wait_for_dependencies {
            FiberState::WaitingDependencies
        } else {
            FiberState::Disposed
        };
        if !removed_symbols.is_empty() && self.0.admission.enter().is_ok() {
            if let Err(error) = Box::pin(self.reconcile_symbols(removed_symbols)).await {
                self.push_disposal_error(fiber, CleanupPhase::DependencyCleanup, error);
            }
        }
        let result = self.0.fibers.with(fiber, |record| {
                if record.disposal.errors.is_empty() {
                    Ok(())
                } else if record.disposal.errors.len() == 1 {
                    Err(record.disposal.errors[0].cause.clone().unwrap_or_else(|| {
                        CordisError::PluginDisposeFailed(record.disposal.errors[0].message.clone())
                    }))
                } else {
                    Err(CordisError::PluginDisposeFailed(record.disposal.errors.iter().map(|issue| format!("{:?}: {}", issue.phase, issue.message)).collect::<Vec<_>>().join("; ")))
                }
            }).unwrap_or_else(
            || {
                Err(CordisError::DisposalWorkerTerminated(
                    "fiber disappeared after cleanup".into(),
                ))
            },
        );
        (result, Some(target), false)
    }

    // Publication is the final synchronous commit point. Test builds can pause
    // either while the Fiber is non-reclaimable and indexes are still stale, or
    // after the durable observation is committed but before waiter notification.
    #[allow(clippy::too_many_lines, clippy::unused_async)]
    async fn finalize_fiber_disposal(
        &self,
        fiber: FiberId,
        mut result: Result<(), CordisError>,
        target: Option<FiberState>,
        terminated: bool,
    ) -> Result<(), CordisError> {
        let (completion, test_hook, index_cleanup) = {
            let Some(cell) = self.0.fibers.get(fiber) else {
                return Err(CordisError::DisposalWorkerTerminated(
                    "fiber disappeared before finalization".into(),
                ));
            };
            let mut record = cell.inner.write();
            if let Some(target) = target {
                if !record.state.can_transition_to(target) {
                    result = Err(CordisError::DisposalWorkerTerminated(format!(
                        "invalid final state transition: {:?} -> {target:?}",
                        record.state
                    )));
                }
            }
            let terminated = terminated
                || matches!(
                    result,
                    Err(CordisError::DisposalWorkerPanicked(_)
                        | CordisError::DisposalWorkerCancelled
                        | CordisError::DisposalWorkerTerminated(_))
                );
            // Keep the Fiber non-terminal and therefore non-GC-reclaimable
            // while cross-registry control-plane indexes converge.
            record.disposal.phase = DisposalPhase::Finalizing;
            let completion = record.disposal.completion.clone();
            #[cfg(test)]
            let test_hook = record.disposal.test_hook.clone();
            #[cfg(not(test))]
            let test_hook: Option<Arc<()>> = None;
            let index_cleanup = (target == Some(FiberState::Disposed) && !terminated).then(|| {
                (
                    record.scope,
                    record.descriptor.dependencies.clone(),
                    record.descriptor.provisions.clone(),
                )
            });
            (completion, test_hook, index_cleanup)
        };
        #[cfg(test)]
        if let Some(hook) = test_hook
            .as_ref()
            .filter(|hook| hook.pause_before_index_cleanup)
        {
            hook.finalizing.store(true, Ordering::SeqCst);
            hook.finalizing_notify.notify_waiters();
            hook.release_index_cleanup.notified().await;
        }
        if let Some((scope, dependencies, provisions)) = index_cleanup {
            for dependency in dependencies.iter() {
                let symbol = self.intern(dependency);
                self.0.dependencies.remove_dependency(symbol, fiber);
            }
            for provision in provisions.iter() {
                let symbol = self.intern(provision);
                self.0.dependencies.remove_provider(scope, symbol, fiber);
            }
            if let Some(scope) = self.0.scopes.write().get_mut(scope) {
                scope.fibers.retain(|id| *id != fiber);
            }
        }
        {
            let Some(cell) = self.0.fibers.get(fiber) else {
                return Err(CordisError::DisposalWorkerTerminated(
                    "fiber disappeared during finalization".into(),
                ));
            };
            let mut record = cell.inner.write();
            if let Some(target) = target.filter(|_| !terminated) {
                record.state = target;
            }
            record.disposal.result = Some(result.clone());
            record.disposal.phase = if terminated {
                DisposalPhase::Terminated
            } else {
                DisposalPhase::Complete
            };
            record.disposal.supervisor = None;
            #[cfg(test)]
            {
                record.disposal.worker_abort = None;
            }
            let terminal = if terminated {
                DisposalTerminal::Incomplete(result.clone().err().unwrap_or_else(|| {
                    CordisError::DisposalWorkerTerminated("disposal terminated".into())
                }))
            } else {
                DisposalTerminal::Committed
            };
            // This slot write is the operation's visibility linearization: all
            // required synchronous index work and terminal bookkeeping is done.
            completion.publish(DisposalObservation {
                legacy_result: result.clone(),
                terminal,
                issues: record.disposal.errors.clone(),
            })?;
        }
        #[cfg(test)]
        if let Some(hook) = test_hook.filter(|hook| hook.pause_after_publish) {
            hook.published.store(true, Ordering::SeqCst);
            hook.published_notify.notify_waiters();
            hook.release_after_publish.notified().await;
        }
        #[cfg(not(test))]
        let _ = test_hook;
        self.request_gc();
        completion.notify.notify_waiters();
        result
    }

    #[cfg(test)]
    async fn run_disposal_test_hook(&self, fiber: FiberId) {
        let hook = self
            .0
            .fibers
            .with(fiber, |record| record.disposal.test_hook.clone()).flatten();
        let Some(hook) = hook else { return };
        if hook.pause_before_finish {
            hook.entered.store(true, Ordering::SeqCst);
            hook.entered_notify.notify_waiters();
            hook.release.notified().await;
        }
        assert!(!hook.panic_before_finish, "disposal worker test panic");
    }

    #[cfg(test)]
    fn abort_disposal_worker(&self, fiber: FiberId) {
        if let Some(abort) = self
            .0
            .fibers
            .with(fiber, |record| record.disposal.worker_abort.clone()).flatten()
        {
            abort.abort();
        }
    }

    fn fiber_scope(&self, fiber: FiberId) -> Result<ScopeId, CordisError> {
        self.0
            .fibers
            .with(fiber, |f| f.scope)
            .ok_or(CordisError::FiberNotFound)
    }

}
