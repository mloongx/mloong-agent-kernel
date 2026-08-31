impl Runtime {
    /// Creates a child scope.
    pub fn create_scope(
        &self,
        parent: ScopeId,
        name: impl Into<Arc<str>>,
    ) -> Result<ScopeId, CordisError> {
        self.create_scope_internal(parent, name.into(), false)
    }

    fn create_scope_internal(
        &self,
        parent: ScopeId,
        name: Arc<str>,
        hidden: bool,
    ) -> Result<ScopeId, CordisError> {
        let _admission = self.0.admission.enter()?;
        if self.0.scopes.len() >= self.0.config.max_scopes {
            let _ = self.collect_garbage();
        }
        let mut scopes = self.0.scopes.write();
        if scopes.len() >= self.0.config.max_scopes {
            return Err(self.0.quota_error(ResourceKind::Scopes, self.0.config.max_scopes, Some(parent), None));
        }
        let mut depth = 1_usize;
        let mut cursor = Some(parent);
        while let Some(scope) = cursor {
            cursor = scopes.get(scope).and_then(|record| record.parent);
            if cursor.is_some() {
                depth += 1;
            }
        }
        if depth > self.0.config.max_scope_depth {
            return Err(self.0.quota_error(ResourceKind::ScopeDepth, self.0.config.max_scope_depth, Some(parent), None));
        }
        if scopes
            .get(parent)
            .ok_or(CordisError::ScopeNotFound)?
            .state
            != ScopeState::Active
        {
            return Err(CordisError::ScopeDisposed(parent));
        }
        let id = scopes.insert(ScopeRecord {
            name,
            parent: Some(parent),
            children: SmallVec::new(),
            fibers: SmallVec::new(),
            state: ScopeState::Active,
            hidden,
            disposal: ScopeDisposal::default(),
        });
        scopes
            .get_mut(parent)
            .ok_or(CordisError::ScopeNotFound)?
            .children
            .push(id);
        Ok(id)
    }

    /// Recursively destroys a scope, its child scopes, and fibers.
    ///
    /// Every child is attempted even when an earlier cleanup reports an error.
    #[must_use]
    pub fn dispose_scope(
        &self,
        scope: ScopeId,
    ) -> futures::future::BoxFuture<'_, Result<(), CordisError>> {
        Box::pin(async move {
            self.dispose_scope_observation(scope, false)
                .await
                .and_then(|observation| observation.legacy_result.clone())
        })
    }

    fn dispose_scope_observation(
        &self,
        scope: ScopeId,
        persistent: bool,
    ) -> futures::future::BoxFuture<'_, Result<Arc<DisposalObservation>, CordisError>> {
        Box::pin(async move {
            let completion = {
                let mut scopes = self.0.scopes.write();
                let record = scopes
                    .get_mut(scope)
                    .ok_or(CordisError::ScopeNotFound)?;
                record.disposal.persistent |= persistent;
                if record.state == ScopeState::Active {
                    record.state = ScopeState::Disposing;
                    let body_runtime = self.clone();
                    let body =
                        tokio::spawn(async move { body_runtime.dispose_scope_body(scope).await });
                    #[cfg(test)]
                    {
                        record.disposal.body_abort = Some(body.abort_handle());
                    }
                    let supervisor_runtime = self.clone();
                    let supervisor = tokio::spawn(async move {
                        let (result, terminated) = match body.await {
                            Ok(outcome) => outcome,
                            Err(error) if error.is_cancelled() => {
                                (Err(CordisError::ScopeDisposalCancelled), true)
                            }
                            Err(error) => (
                                Err(CordisError::ScopeDisposalPanicked(panic_message(
                                    error.into_panic().as_ref(),
                                ))),
                                true,
                            ),
                        };
                        if let Err(error) = supervisor_runtime
                            .finalize_scope_disposal(scope, result, terminated)
                            .await
                        {
                            debug!(?scope, %error, "scope supervisor could not publish result");
                        }
                    });
                    record.disposal.supervisor = Some(supervisor);
                }
                record.disposal.completion.clone()
            };
            self.wait_for_completion_observation(completion).await
        })
    }

    /// Disposes a Scope and reports its terminal truth independently from
    /// ordered descendant cleanup diagnostics.
    pub async fn dispose_scope_detailed(&self, scope: ScopeId) -> ScopeDisposeOutcome {
        match self.dispose_scope_observation(scope, false).await {
            Ok(observation) => observation.scope_outcome(),
            Err(primary) => ScopeDisposeOutcome::Incomplete {
                primary,
                issues: Vec::new(),
            },
        }
    }

    pub(crate) async fn dispose_scope_for_shutdown(
        &self,
        scope: ScopeId,
    ) -> Result<Arc<DisposalObservation>, CordisError> {
        self.dispose_scope_observation(scope, true).await
    }

    async fn wait_for_completion_observation(
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

    async fn dispose_scope_body(&self, scope: ScopeId) -> (Result<(), CordisError>, bool) {
        let (children, fibers, persistent) = {
            let scopes = self.0.scopes.read();
            let Some(record) = scopes.get(scope) else {
                return (
                    Err(CordisError::ScopeDisposalTerminated(
                        "scope disappeared during traversal".into(),
                    )),
                    true,
                );
            };
            (
                record.children.clone(),
                record.fibers.clone(),
                record.disposal.persistent,
            )
        };
        let mut errors = Vec::new();
        let mut terminated = false;
        for child in children.into_iter().rev() {
            let child_result = if persistent {
                self.dispose_scope_observation(child, true)
                    .await
                    .and_then(|observation| observation.legacy_result.clone())
            } else {
                self.dispose_scope(child).await
            };
            if let Err(error) = child_result {
                let child_committed = self
                    .0
                    .scopes
                    .read()
                    .get(child)
                    .is_some_and(|record| record.state == ScopeState::Disposed);
                terminated |= !child_committed || is_terminal_cleanup_error(&error);
                errors.push(CleanupIssue { phase: CleanupPhase::ChildScopeCleanup, message: format!("child scope {child:?}: {error}"), cause: Some(error) });
            }
        }
        for fiber in fibers.into_iter().rev() {
            let fiber_state = self.0.fibers.with(fiber, |f| f.state);
            if matches!(fiber_state, None | Some(FiberState::Disposed)) {
                continue;
            }
            let fiber_result = if persistent {
                self.dispose_reload_owned_fiber_persistently(fiber).await
            } else {
                self.dispose_reload_owned_fiber(fiber).await
            };
            if let Err(error) = fiber_result {
                let fiber_committed = self
                    .0
                    .fibers
                    .with(fiber, |record| {
                        matches!(record.state, FiberState::Disposed | FiberState::WaitingDependencies)
                    })
                    .unwrap_or(false);
                terminated |= !fiber_committed || is_terminal_cleanup_error(&error);
                errors.push(CleanupIssue { phase: CleanupPhase::FiberDetach, message: format!("fiber {fiber:?}: {error}"), cause: Some(error) });
            }
        }
        #[cfg(test)]
        self.run_scope_test_hook(scope).await;
        let result = if errors.is_empty() {
            Ok(())
        } else {
            Err(CordisError::CleanupFailed(errors.iter().map(|issue| format!("{:?}: {}", issue.phase, issue.message)).collect::<Vec<_>>().join("; ")))
        };
        if let Some(record) = self.0.scopes.write().get_mut(scope) {
            record.disposal.issues = errors;
        }
        (result, terminated)
    }

    #[allow(clippy::unused_async)]
    async fn finalize_scope_disposal(
        &self,
        scope: ScopeId,
        result: Result<(), CordisError>,
        terminated: bool,
    ) -> Result<(), CordisError> {
        let result = if terminated
            && !matches!(
                &result,
                Err(CordisError::ScopeDisposalPanicked(_)
                    | CordisError::ScopeDisposalCancelled
                    | CordisError::ScopeDisposalTerminated(_))
            ) {
            Err(CordisError::ScopeDisposalTerminated(
                result.as_ref().err().map_or_else(
                    || "descendant cleanup terminated".into(),
                    ToString::to_string,
                ),
            ))
        } else {
            result
        };
        #[cfg(test)]
        let test_hook = self
            .0
            .scopes
            .read()
            .get(scope)
            .and_then(|record| record.disposal.test_hook.clone());
        #[cfg(not(test))]
        let test_hook: Option<Arc<()>> = None;
        #[cfg(test)]
        if let Some(hook) = test_hook
            .as_ref()
            .filter(|hook| hook.pause_before_scope_topology_commit)
        {
            hook.scope_commit_pending.store(true, Ordering::SeqCst);
            hook.scope_commit_pending_notify.notify_waiters();
            hook.release_scope_topology_commit.notified().await;
        }
        let (completion, test_hook) = {
            let mut scopes = self.0.scopes.write();
            let (parent, completion, observation) = {
                let record = scopes
                    .get_mut(scope)
                    .ok_or(CordisError::ScopeNotFound)?;
                record.state = if terminated {
                    ScopeState::Terminated
                } else {
                    ScopeState::Disposed
                };
                record.disposal.result = Some(result.clone());
                record.disposal.supervisor = None;
                #[cfg(test)]
                {
                    record.disposal.body_abort = None;
                }
                let observation = DisposalObservation {
                    legacy_result: result.clone(),
                    terminal: if terminated {
                        DisposalTerminal::Incomplete(result.clone().err().unwrap_or_else(|| {
                            CordisError::ScopeDisposalTerminated("scope disposal terminated".into())
                        }))
                    } else {
                        DisposalTerminal::Committed
                    },
                    issues: record.disposal.issues.clone(),
                };
                (
                    record.parent,
                    record.disposal.completion.clone(),
                    observation,
                )
            };
            if !terminated {
                if let Some(parent) = parent.and_then(|id| scopes.get_mut(id)) {
                    parent.children.retain(|id| *id != scope);
                }
            }
            // Completion is independently observable without the ScopeRegistry
            // lock, so topology convergence must precede this final publication.
            completion.publish(observation)?;
            (completion, test_hook)
        };
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
    async fn run_scope_test_hook(&self, scope: ScopeId) {
        let hook = self
            .0
            .scopes
            .read()
            .get(scope)
            .and_then(|record| record.disposal.test_hook.clone());
        let Some(hook) = hook else { return };
        if hook.pause_before_finish {
            hook.entered.store(true, Ordering::SeqCst);
            hook.entered_notify.notify_waiters();
            hook.release.notified().await;
        }
        assert!(!hook.panic_before_finish, "scope disposal test panic");
    }

}
