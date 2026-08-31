struct PreparedDependencyRevision {
    symbols: Vec<ServiceSymbol>,
    from: ScopeId,
    to: ScopeId,
    staged: FiberId,
}

impl PreparedDependencyRevision {
    fn commit(&self, runtime: &Runtime) -> Result<(), CordisError> {
        runtime.0.dependencies.move_providers(
            self.from,
            self.to,
            &self.symbols,
            self.staged,
        )
    }

    fn rollback(&self, runtime: &Runtime) -> Result<(), CordisError> {
        runtime.0.dependencies.move_providers(
            self.to,
            self.from,
            &self.symbols,
            self.staged,
        )
    }
}

struct PreparedScopeRevision {
    staging: ScopeId,
    target: ScopeId,
    staged: FiberId,
    old: FiberId,
    children: Vec<ScopeId>,
}

impl PreparedScopeRevision {
    fn prepare(
        runtime: &Runtime,
        staging: ScopeId,
        target: ScopeId,
        staged: FiberId,
        old: FiberId,
        children: Vec<ScopeId>,
    ) -> Result<Self, CordisError> {
        let scopes = runtime.0.scopes.read();
        let staging_record = scopes.get(staging).ok_or(CordisError::ScopeNotFound)?;
        let target_record = scopes.get(target).ok_or(CordisError::ScopeNotFound)?;
        if staging_record.state != ScopeState::Active
            || target_record.state != ScopeState::Active
            || !staging_record.fibers.contains(&staged)
            || !target_record.fibers.contains(&old)
            || children
                .iter()
                .any(|child| scopes.get(*child).is_none_or(|scope| scope.parent != Some(staging)))
        {
            return Err(CordisError::RevisionValidationFailed(
                "prepared scope topology changed before reload commit".into(),
            ));
        }
        Ok(Self {
            staging,
            target,
            staged,
            old,
            children,
        })
    }

    fn commit_locked(
        &self,
        scopes: &mut slotmap::SlotMap<ScopeId, ScopeRecord>,
    ) -> Result<(), CordisError> {
        let valid = scopes.get(self.staging).is_some_and(|record| {
            record.state == ScopeState::Active
                && record.fibers.iter().filter(|id| **id == self.staged).count() == 1
                && self.children.iter().all(|child| record.children.contains(child))
        }) && scopes.get(self.target).is_some_and(|record| {
            record.state == ScopeState::Active
                && record.fibers.iter().filter(|id| **id == self.old).count() == 1
                && !record.fibers.contains(&self.staged)
                && self.children.iter().all(|child| !record.children.contains(child))
        }) && scopes.iter().all(|(id, record)| {
            id == self.staging || !record.fibers.contains(&self.staged)
        }) && self.children.iter().all(|child| {
            scopes.get(*child).is_some_and(|record| record.parent == Some(self.staging))
        });
        if !valid {
            return Err(CordisError::RevisionValidationFailed(
                "scope topology changed before reload commit".into(),
            ));
        }
        let staging = scopes.get_mut(self.staging).expect("validated staging scope");
        staging.fibers.retain(|id| *id != self.staged);
        staging.children.retain(|id| !self.children.contains(id));
        let target = scopes.get_mut(self.target).expect("validated target scope");
        target.fibers.push(self.staged);
        target.children.extend(self.children.iter().copied());
        for child in &self.children {
            scopes.get_mut(*child).expect("validated child scope").parent = Some(self.target);
        }
        Ok(())
    }

    fn rollback_locked(
        &self,
        scopes: &mut slotmap::SlotMap<ScopeId, ScopeRecord>,
    ) -> Result<(), CordisError> {
        let valid = scopes.get(self.target).is_some_and(|record| {
            record.state == ScopeState::Active
                && record.fibers.iter().filter(|id| **id == self.old).count() == 1
                && record.fibers.iter().filter(|id| **id == self.staged).count() == 1
                && self.children.iter().all(|child| record.children.contains(child))
        }) && scopes.get(self.staging).is_some_and(|record| {
            record.state == ScopeState::Active
                && !record.fibers.contains(&self.staged)
                && self.children.iter().all(|child| !record.children.contains(child))
        }) && scopes.iter().all(|(id, record)| {
            id == self.target || !record.fibers.contains(&self.staged)
        }) && self.children.iter().all(|child| {
            scopes.get(*child).is_some_and(|record| record.parent == Some(self.target))
        });
        if !valid {
            return Err(CordisError::RevisionValidationFailed(
                "scope topology changed before reload rollback".into(),
            ));
        }
        let target = scopes.get_mut(self.target).expect("validated target scope");
        target.fibers.retain(|id| *id != self.staged);
        target.children.retain(|id| !self.children.contains(id));
        let staging = scopes.get_mut(self.staging).expect("validated staging scope");
        staging.fibers.push(self.staged);
        staging.children.extend(self.children.iter().copied());
        for child in &self.children {
            scopes.get_mut(*child).expect("validated child scope").parent = Some(self.staging);
        }
        Ok(())
    }

    #[cfg(test)]
    fn commit(&self, runtime: &Runtime) -> Result<(), CordisError> {
        self.commit_locked(&mut runtime.0.scopes.write())
    }

    #[cfg(test)]
    fn rollback(&self, runtime: &Runtime) -> Result<(), CordisError> {
        self.rollback_locked(&mut runtime.0.scopes.write())
    }
}

struct ReloadActivity(Arc<RuntimeInner>);

impl Drop for ReloadActivity {
    fn drop(&mut self) {
        self.0.active_reloads.fetch_sub(1, Ordering::Relaxed);
    }
}

struct ReloadCleanupPending(Arc<RuntimeInner>);

impl Drop for ReloadCleanupPending {
    fn drop(&mut self) {
        self.0
            .reload_cleanup_pending
            .fetch_sub(1, Ordering::Relaxed);
    }
}

impl Runtime {
    /// Installs and activates a native plugin, or leaves it waiting for dependencies.
    pub async fn install<P: NativePlugin>(
        &self,
        scope: ScopeId,
        plugin: P,
    ) -> Result<FiberId, CordisError> {
        self.install_owned(scope, Arc::new(plugin)).await
    }

    /// Loads an external artifact through a host adapter and installs its proxy.
    pub async fn install_hosted<H: PluginHost>(
        &self,
        scope: ScopeId,
        host: &H,
        artifact: PluginArtifact,
    ) -> Result<FiberId, CordisError> {
        let plugin = host.load(artifact).await?;
        self.install_owned(scope, plugin).await
    }

    async fn install_owned(
        &self,
        scope: ScopeId,
        plugin: Arc<dyn NativePlugin>,
    ) -> Result<FiberId, CordisError> {
        // Successful worker creation is the operation-acceptance linearization
        // point. The caller owns only this observer; the Runtime owns all later
        // activation commit or rollback work.
        let admission = self.0.admission.enter()?;
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let runtime = self.clone();
        self.0.workers.spawn(RuntimeWorkerKind::Install, true, async move {
            let outcome = runtime.install_arc(scope, plugin, false, None).await;
            let worker_result = outcome.as_ref().map(|_| ()).map_err(Clone::clone);
            let _ = result_tx.send(outcome);
            worker_result
        });
        drop(admission);
        result_rx.await.map_err(|_| {
            CordisError::Invariant("install worker ended without a structured outcome".into())
        })?
    }

    /// Transactionally replaces an active plugin revision while preserving the
    /// legacy `FiberId` success contract.
    pub async fn reload<P: NativePlugin>(
        &self,
        old: FiberId,
        plugin: P,
    ) -> Result<FiberId, CordisError> {
        Self::legacy_reload_result(self.reload_detailed(old, plugin).await?)
    }

    /// Loads a hosted lifecycle proxy and transactionally replaces an active
    /// plugin while preserving the legacy `FiberId` success contract.
    pub async fn reload_hosted<H: PluginHost>(
        &self,
        old: FiberId,
        host: &H,
        artifact: PluginArtifact,
    ) -> Result<FiberId, CordisError> {
        Self::legacy_reload_result(self.reload_hosted_detailed(old, host, artifact).await?)
    }

    fn legacy_reload_result(outcome: ReloadOutcome) -> Result<FiberId, CordisError> {
        match outcome {
            ReloadOutcome::Completed { new_fiber } => Ok(new_fiber),
            ReloadOutcome::CommittedWithCleanupPending { new_fiber, .. } => {
                Err(CordisError::ReloadCommitted {
                    new_fiber,
                    cleanup: Box::new(CordisError::CleanupFailed(
                        "old-generation cleanup continues in a Runtime-owned worker".into(),
                    )),
                })
            }
        }
    }

    /// Starts a Runtime-owned reload transaction and observes its structured
    /// commit outcome. Dropping the returned future does not cancel the transaction.
    pub async fn reload_detailed<P: NativePlugin>(
        &self,
        old: FiberId,
        plugin: P,
    ) -> Result<ReloadOutcome, CordisError> {
        self.reload_owned(old, Arc::new(plugin)).await
    }

    /// Loads a hosted lifecycle proxy before Runtime acceptance, then starts a
    /// Runtime-owned reload transaction and observes its structured outcome.
    /// Dropping during Host load cancels only that pre-acceptance preparation;
    /// dropping after transaction acceptance does not cancel convergence.
    pub async fn reload_hosted_detailed<H: PluginHost>(
        &self,
        old: FiberId,
        host: &H,
        artifact: PluginArtifact,
    ) -> Result<ReloadOutcome, CordisError> {
        let plugin = host.load(artifact).await?;
        self.reload_owned(old, plugin).await
    }

    async fn reload_owned(
        &self,
        old: FiberId,
        plugin: Arc<dyn NativePlugin>,
    ) -> Result<ReloadOutcome, CordisError> {
        {
            let _admission = self.0.admission.enter()?;
            let cell = self.0.fibers.get(old).ok_or(CordisError::FiberNotFound)?;
            if cell.inner.read().reload_owned {
                return Err(CordisError::FiberLifecycleOwned(old));
            }
        }
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let runtime = self.clone();
        self.0.active_reloads.fetch_add(1, Ordering::Relaxed);
        self.0.workers.spawn(
            RuntimeWorkerKind::ReloadTransaction,
            true,
            async move {
                let _activity = ReloadActivity(runtime.0.clone());
                runtime
                    .run_reload_transaction(old, plugin, result_tx)
                    .await
            },
        );
        result_rx
            .await
            .map_err(|_| CordisError::Invariant("reload worker ended without an outcome".into()))?
    }

    #[allow(clippy::too_many_lines)]
    async fn run_reload_transaction(
        &self,
        old: FiberId,
        plugin: Arc<dyn NativePlugin>,
        result: tokio::sync::oneshot::Sender<Result<ReloadOutcome, CordisError>>,
    ) -> Result<(), CordisError> {
        let lifecycle = match (|| {
            let _admission = self.0.admission.enter()?;
            self.0
                .fibers
                .get(old)
                .ok_or(CordisError::FiberNotFound)
                .map(|cell| cell.lifecycle.clone())
        })() {
            Ok(lifecycle) => lifecycle,
            Err(error) => {
                let _ = result.send(Err(error.clone()));
                return Err(error);
            }
        };
        let operation = lifecycle.lock_owned().await;
        let (target_scope, plugin_id) = {
            let Some(cell) = self.0.fibers.get(old) else {
                let error = CordisError::FiberNotFound;
                let _ = result.send(Err(error.clone()));
                return Err(error);
            };
            let record = cell.inner.read();
            if record.state != FiberState::Active {
                let error = CordisError::InvalidFiberState {
                    fiber: old,
                    from: record.state,
                    to: FiberState::Reloading,
                };
                let _ = result.send(Err(error.clone()));
                return Err(error);
            }
            (record.scope, cell.plugin_id)
        };
        if let Err(error) = self.transition(old, FiberState::Reloading) {
            let _ = result.send(Err(error.clone()));
            return Err(error);
        }
        let staging = match self.create_scope_internal(target_scope, "hmr-staging".into(), true) {
            Ok(staging) => staging,
            Err(error) => {
                let failure = self.rollback_reload(old, None, None, error).await;
                let _ = result.send(Err(failure.clone()));
                return Err(failure);
            }
        };
        let staged = match self.install_arc(staging, plugin, true, Some(plugin_id)).await {
            Ok(fiber) => fiber,
            Err(error) => {
                let failure = self.rollback_reload(old, None, Some(staging), error).await;
                let _ = result.send(Err(failure.clone()));
                return Err(failure);
            }
        };

        if let Err(error) = self.validate_staged_revision(staged) {
            let failure = self
                .rollback_reload(old, Some(staged), Some(staging), error)
                .await;
            let _ = result.send(Err(failure.clone()));
            return Err(failure);
        }

        match self.commit_staged_revision(old, staged, staging, target_scope) {
            Ok(_) => {}
            Err(error) => {
                let failure = self
                    .rollback_reload(old, Some(staged), Some(staging), error)
                    .await;
                let _ = result.send(Err(failure.clone()));
                return Err(failure);
            }
        }
        drop(operation);
        let mut cleanup = Box::pin(self.dispose_fiber_persistently(old, false));
        if let Ok(disposal) =
            tokio::time::timeout(self.0.config.task_grace, &mut cleanup).await
        {
            let scope_cleanup = self.dispose_scope(staging).await;
            let cleanup_result = Self::combine_reload_cleanup(disposal, scope_cleanup);
            match cleanup_result {
                Ok(()) => {
                    let _ = result.send(Ok(ReloadOutcome::Completed { new_fiber: staged }));
                    Ok(())
                }
                Err(error) => {
                    let committed = CordisError::ReloadCommitted {
                        new_fiber: staged,
                        cleanup: Box::new(error),
                    };
                    let _ = result.send(Err(committed.clone()));
                    Err(committed)
                }
            }
        } else {
            self.0.reload_cleanup_pending.fetch_add(1, Ordering::Relaxed);
            let _pending = ReloadCleanupPending(self.0.clone());
            let _ = result.send(Ok(ReloadOutcome::CommittedWithCleanupPending {
                    new_fiber: staged,
                    old_fiber: old,
            }));
            let disposal = cleanup.await;
            let scope_cleanup = self.dispose_scope(staging).await;
            Self::combine_reload_cleanup(disposal, scope_cleanup)
        }
    }

    fn combine_reload_cleanup(
        disposal: Result<(), CordisError>,
        scope: Result<(), CordisError>,
    ) -> Result<(), CordisError> {
        let mut issues: Vec<_> = [disposal.err(), scope.err()].into_iter().flatten().collect();
        match issues.len() {
            0 => Ok(()),
            1 if matches!(issues[0], CordisError::Host(_) | CordisError::RemoteDomain(_)) => {
                Err(issues.pop().expect("one typed hosted cleanup issue"))
            }
            _ => Err(CordisError::CleanupFailed(
                issues
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join("; "),
            )),
        }
    }

    async fn rollback_reload(
        &self,
        old: FiberId,
        staged: Option<FiberId>,
        staging: Option<ScopeId>,
        primary: CordisError,
    ) -> CordisError {
        let mut cleanup = Vec::new();
        if let Some(staged) = staged {
            if let Err(error) = self.dispose_reload_owned_fiber(staged).await {
                cleanup.push(error);
            }
        }
        if let Some(staging) = staging {
            if let Err(error) = self.dispose_scope(staging).await {
                cleanup.push(error);
            }
        }
        if let Err(error) = self.transition(old, FiberState::Active) {
            cleanup.push(error);
        }
        CordisError::ReloadFailed {
            primary: Box::new(primary),
            cleanup,
        }
    }

    fn validate_staged_revision(&self, fiber: FiberId) -> Result<(), CordisError> {
        let cell = self.0.fibers.get(fiber).ok_or(CordisError::FiberNotFound)?;
        let record = cell.inner.read();
        let declared: HashSet<_> = record.descriptor.provisions.iter().cloned().collect();
        let actual: HashSet<_> = record.provided.iter().cloned().collect();
        if declared != actual {
            return Err(CordisError::RevisionValidationFailed(format!(
                "declared provisions {declared:?} do not match actual services {actual:?}"
            )));
        }
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    fn commit_staged_revision(
        &self,
        old: FiberId,
        staged: FiberId,
        staging_scope: ScopeId,
        target_scope: ScopeId,
    ) -> Result<Vec<HandlerId>, CordisError> {
        let _admission = self.0.admission.enter()?;
        let old_cell = self.0.fibers.get(old).ok_or(CordisError::FiberNotFound)?;
        let staged_cell = self.0.fibers.get(staged).ok_or(CordisError::FiberNotFound)?;
        let old_state = old_cell.inner.read().state;
        if old_state != FiberState::Reloading {
            return Err(CordisError::InvalidFiberState {
                fiber: old,
                from: old_state,
                to: FiberState::Active,
            });
        }

        #[cfg(test)]
        if self
            .0
            .fail_next_reload_publication
            .swap(false, Ordering::SeqCst)
        {
            staged_cell.inner.read().capabilities.close();
        }
        let (provided, handlers, invocation_handlers, invocation_middleware, children, descriptor) = {
            let record = staged_cell.inner.read();
            if record.state != FiberState::Active || !record.capabilities.is_staged() {
                return Err(CordisError::CapabilityPublicationFailed(staged));
            }
            (
                record.provided.clone(),
                record.handlers.clone(),
                record.invocation_handlers.clone(),
                record.invocation_middleware.clone(),
                record.child_scopes.clone(),
                record.descriptor.clone(),
            )
        };
        let (old_invocation_handlers, old_invocation_middleware) = {
            let record = old_cell.inner.read();
            (
                record.invocation_handlers.clone(),
                record.invocation_middleware.clone(),
            )
        };
        self.0.invocations.validate_revision(
            &old_invocation_handlers,
            &invocation_handlers,
            target_scope,
        )?;
        let service_symbols: Vec<_> = provided.iter().map(|key| self.intern(key)).collect();
        for (key, symbol) in provided.iter().zip(&service_symbols) {
            if let Some(entry) = self.0.services.get(target_scope, *symbol) {
                if entry.owner != old {
                    return Err(CordisError::DuplicateService(key.clone()));
                }
            }
        }
        let service_revision = self.0.services.prepare_revision(
            staging_scope,
            target_scope,
            service_symbols,
            old,
            staged,
        )?;
        let dependency_revision = PreparedDependencyRevision {
            symbols: descriptor
                .provisions
                .iter()
                .map(|key| self.intern(key))
                .collect(),
            from: staging_scope,
            to: target_scope,
            staged,
        };
        let scope_revision = PreparedScopeRevision::prepare(
            self,
            staging_scope,
            target_scope,
            staged,
            old,
            children.clone(),
        )?;
        let old_gate = old_cell.inner.read().capabilities.clone();

        // Own the staged Fiber metadata before entering the Scope cutover
        // fence. This preserves the existing Fiber -> Scope lock direction and
        // lets selector publication and the committed Fiber binding become one
        // externally indivisible transition.
        let mut staged_record = staged_cell.inner.write();
        if staged_record.state != FiberState::Active || !staged_record.capabilities.is_staged() {
            return Err(CordisError::CapabilityPublicationFailed(staged));
        }
        let staged_gate = staged_record.capabilities.clone();

        // Prepared control-plane revisions are reversible while the selector
        // still names the old generation. Publication is the sole commit point.
        dependency_revision.commit(self)?;
        let mut scopes = self.0.scopes.write();
        if let Err(error) = scope_revision.commit_locked(&mut scopes) {
            drop(scopes);
            let cleanup = dependency_revision.rollback(self).err().into_iter().collect();
            return Err(CordisError::ReloadFailed {
                primary: Box::new(error),
                cleanup,
            });
        }
        self.0.invocations.commit_revision(
            &old_invocation_handlers,
            &old_invocation_middleware,
            &invocation_handlers,
            &invocation_middleware,
            target_scope,
        );
        #[cfg(test)]
        let reload_before_selector_hook = { self.0.reload_before_selector_hook.lock().clone() };
        #[cfg(test)]
        if let Some(hook) = reload_before_selector_hook {
            hook.wait();
            hook.wait();
        }
        #[cfg(test)]
        if self.0.fail_selector_after_scope.swap(false, Ordering::SeqCst) {
            old_gate.close();
        }
        if let Err(error) = self.0.services.commit_revision_and_publish(
            &service_revision,
            &staged_gate,
            &old_gate,
        ) {
            self.0.invocations.commit_revision(
                &old_invocation_handlers,
                &old_invocation_middleware,
                &invocation_handlers,
                &invocation_middleware,
                staging_scope,
            );
            let mut cleanup = scope_revision.rollback_locked(&mut scopes).err().into_iter().collect::<Vec<_>>();
            drop(scopes);
            cleanup.extend(dependency_revision.rollback(self).err());
            return Err(CordisError::ReloadFailed {
                primary: Box::new(error),
                cleanup,
            });
        }

        #[cfg(test)]
        let reload_after_selector_hook = { self.0.reload_after_selector_hook.lock().clone() };
        #[cfg(test)]
        if let Some(hook) = reload_after_selector_hook {
            hook.wait();
            hook.wait();
        }

        // Selector publication and committed Fiber metadata are covered by the
        // same Fiber -> Scope fence. Target disposal cannot observe selector
        // new while this Fiber still names its hidden staging Scope.
        staged_record.scope = target_scope;
        staged_record.staged = false;
        staged_record.reload_owned = false;
        staged_record.activation_sealed = false;
        let barrier = staged_record.activation.take();
        drop(scopes);
        drop(staged_record);
        if let Some(barrier) = barrier {
            let _ = barrier.send(true);
        }
        Ok(handlers)
    }

    async fn install_arc(
        &self,
        scope: ScopeId,
        plugin: Arc<dyn NativePlugin>,
        staged: bool,
        logical_plugin: Option<PluginId>,
    ) -> Result<FiberId, CordisError> {
        if self.0.fibers.len() >= self.0.config.max_fibers {
            let _ = self.collect_garbage();
        }
        let descriptor = plugin.descriptor();
        let fiber = {
            let _admission = self.0.admission.enter()?;
            for key in descriptor.dependencies.iter().chain(descriptor.provisions.iter()) {
                self.0.services.try_intern(key)?;
            }
            if self.0.fibers.len() >= self.0.config.max_fibers {
                return Err(self.0.quota_error(ResourceKind::Fibers, self.0.config.max_fibers, Some(scope), None));
            }
            let mut scopes = self.0.scopes.write();
            if scopes
                .get(scope)
                .ok_or(CordisError::ScopeNotFound)?
                .state
                != ScopeState::Active
            {
                return Err(CordisError::ScopeDisposed(scope));
            }
            let plugin_id = logical_plugin.unwrap_or_else(|| self.0.plugins.create());
            let (generation, selector) = self.0.plugins.allocate_generation(plugin_id)?;
            self.0.plugins.attach_fiber(plugin_id)?;
            let id = self.0.fibers.insert(FiberCell {
                plugin_id,
                lifecycle: Arc::new(AsyncMutex::new(())),
                inner: RwLock::new(FiberMutable {
                    scope,
                    descriptor: descriptor.clone(),
                    state: FiberState::Created,
                    plugin,
                    effects: Vec::new(),
                    tasks: Vec::new(),
                    handlers: Vec::new(),
                    invocation_handlers: Vec::new(),
                    invocation_middleware: Vec::new(),
                    provided: Vec::new(),
                    child_scopes: Vec::new(),
                    host_processes: Vec::new(),
                    cancellation: CancellationToken::new(),
                    activation: None,
                    activation_sealed: false,
                    capabilities: Arc::new(CapabilityGate::staged(selector, generation)),
                    generation,
                    staged,
                    reload_owned: staged,
                    disposal: FiberDisposal::default(),
                }),
            });
            scopes
                .get_mut(scope)
                .ok_or(CordisError::ScopeNotFound)?
                .fibers
                .push(id);
            drop(scopes);
            for dep in descriptor.dependencies.iter() {
                let symbol = self.intern(dep);
                self.0.dependencies.add_dependency(symbol, id);
            }
            for provision in descriptor.provisions.iter() {
                let symbol = self.intern(provision);
                self.0.dependencies.add_provider(scope, symbol, id);
            }
            id
        };
        if let Err(error) = self.validate_dependency_graph() {
            self.remove_unstarted_fiber(fiber);
            return Err(error);
        }
        if self.dependencies_met(scope, &descriptor.dependencies) {
            self.activate(fiber).await?;
        } else {
            self.transition(fiber, FiberState::WaitingDependencies)?;
        }
        Ok(fiber)
    }

    #[allow(clippy::too_many_lines)]
    async fn activate(&self, fiber: FiberId) -> Result<(), CordisError> {
        match AssertUnwindSafe(self.activate_body(fiber)).catch_unwind().await {
            Ok(result) => result,
            Err(payload) => {
                Err(self.converge_failed_activation(
                    fiber,
                    CordisError::PluginPanicked(format!(
                        "activation worker: {}",
                        panic_message(payload.as_ref())
                    )),
                ).await)
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn activate_body(&self, fiber: FiberId) -> Result<(), CordisError> {
        let cell = self.0.fibers.get(fiber).ok_or(CordisError::FiberNotFound)?;
        let lifecycle = cell.lifecycle.clone();
        let operation = lifecycle.lock_owned().await;
        let (plugin, scope, name, cancellation, generation) = {
            let _admission = self.0.admission.enter()?;
            let mut record = cell.inner.write();
            if !matches!(record.state, FiberState::Created | FiberState::WaitingDependencies) {
                return Err(CordisError::InvalidFiberState {
                    fiber,
                    from: record.state,
                    to: FiberState::Starting,
                });
            }
            if record.state == FiberState::WaitingDependencies
                && record.disposal.phase == DisposalPhase::Complete
            {
                record.disposal = FiberDisposal::default();
                record.cancellation = CancellationToken::new();
                let (generation, selector) = self.0.plugins.allocate_generation(cell.plugin_id)?;
                record.generation = generation;
                record.capabilities = Arc::new(CapabilityGate::staged(selector, generation));
            }
            record.state = FiberState::Starting;
            record.activation_sealed = false;
            record.activation = Some(watch::channel(false).0);
            (
                record.plugin.clone(),
                record.scope,
                record.descriptor.name.clone(),
                record.cancellation.clone(),
                record.generation,
            )
        };
        let span = info_span!("fiber.start", ?fiber, plugin = %name);
        let start = AssertUnwindSafe(
                plugin
                .start(Context {
                    runtime: Arc::downgrade(&self.0),
                    scope,
                    fiber,
                    owner: Arc::downgrade(&cell),
                    generation,
                })
                .instrument(span),
        )
        .catch_unwind();
        let result = tokio::select! {
            biased;
            () = cancellation.cancelled() => Ok(Err(CordisError::TaskCancelled)),
            result = start => result,
        };
        let result = match result {
            Ok(result) => result,
            Err(payload) => {
                let message = payload.downcast_ref::<&str>().map_or_else(
                    || {
                        payload
                            .downcast_ref::<String>()
                            .cloned()
                            .unwrap_or_else(|| "non-string panic payload".into())
                    },
                    |message| (*message).to_owned(),
                );
                Err(CordisError::PluginPanicked(message))
            }
        };
        match result {
            Ok(()) => {
                let sealed = if let Ok(_admission) = self.0.admission.enter() {
                    let mut record = cell.inner.write();
                    if record.state == FiberState::Starting {
                        record.activation_sealed = true;
                        true
                    } else {
                        false
                    }
                } else {
                    false
                };
                if !sealed {
                    drop(operation);
                    return Err(self.converge_failed_activation(
                        fiber,
                        CordisError::RuntimeShuttingDown,
                    ).await);
                }
                match self.validate_staged_revision(fiber) {
                Ok(()) => {
                    #[cfg(test)]
                    self.run_activation_before_commit_hook().await;
                    let deferred = cell.inner.read().staged;
                    let symbols = if deferred {
                        cell.inner.write().state = FiberState::Active;
                        Vec::new()
                    } else {
                        match self.commit_activation(fiber) {
                            Ok(symbols) => symbols,
                            Err(primary) => {
                                drop(operation);
                                return Err(self.converge_failed_activation(fiber, primary).await);
                            }
                        }
                    };
                    if !symbols.is_empty() {
                        if let Err(error) = Box::pin(self.reconcile_symbols(symbols)).await {
                            tracing::error!(?fiber, %error, "post-commit dependency reconciliation failed");
                        }
                    }
                    Ok(())
                }
                Err(error) => {
                    drop(operation);
                    Err(self.converge_failed_activation(fiber, error).await)
                }
                }
            }
            Err(error) => {
                drop(operation);
                let primary = match error {
                    error @ (CordisError::Host(_) | CordisError::RemoteDomain(_)) => error,
                    error => CordisError::PluginStartFailed(error.to_string()),
                };
                Err(self.converge_failed_activation(fiber, primary).await)
            }
        }
    }

    #[cfg(test)]
    async fn run_activation_before_commit_hook(&self) {
        let hook = self.0.activation_before_commit_hook.lock().clone();
        if let Some(hook) = hook {
            hook.entered.store(true, Ordering::SeqCst);
            hook.entered_notify.notify_waiters();
            hook.release.notified().await;
        }
    }

    async fn converge_failed_activation(
        &self,
        fiber: FiberId,
        primary: CordisError,
    ) -> CordisError {
        self.fail_activation(fiber);
        let cleanup_result = self.dispose_reload_owned_fiber_observation(fiber).await;
        let cleanup = Self::activation_cleanup(cleanup_result);
        Self::activation_failure(primary, cleanup)
    }

    fn activation_failure(primary: CordisError, cleanup: Vec<CordisError>) -> CordisError {
        if cleanup.is_empty() {
            primary
        } else {
            CordisError::ActivationFailed {
                primary: Box::new(primary),
                cleanup,
            }
        }
    }

    fn activation_cleanup(
        result: Result<Arc<DisposalObservation>, CordisError>,
    ) -> Vec<CordisError> {
        let (issues, terminal, legacy_error) = match result {
            Ok(observation) => (
                observation.issues.clone(),
                observation.terminal.clone(),
                observation.legacy_result.clone().err(),
            ),
            Err(error) => (Vec::new(), DisposalTerminal::Incomplete(error.clone()), Some(error)),
        };
        let mut cleanup: Vec<_> = issues
            .into_iter()
            .map(|issue| {
                issue.cause.unwrap_or_else(|| {
                    CordisError::PluginDisposeFailed(format!(
                        "{:?}: {}",
                        issue.phase, issue.message
                    ))
                })
            })
            .collect();
        // Complete disposal errors aggregate the issues already recorded above.
        // A terminated disposal result is instead the independent convergence blocker.
        if cleanup.is_empty() || matches!(terminal, DisposalTerminal::Incomplete(_)) {
            cleanup.extend(legacy_error);
        }
        cleanup
    }

    fn fail_activation(&self, fiber: FiberId) {
        if let Some(cell) = self.0.fibers.get(fiber) {
            let mut record = cell.inner.write();
            if record.state == FiberState::Starting {
                record.state = FiberState::Failed;
            }
            record.activation.take();
            record.activation_sealed = true;
            record.cancellation.cancel();
        }
    }

    /// All fallible checks precede capability publication, the sole commit
    /// point. `Err` proves publication did not occur; post-publication metadata
    /// finalization and task-barrier release are infallible and forward-only.
    fn commit_activation(&self, fiber: FiberId) -> Result<Vec<ServiceSymbol>, CordisError> {
        let (scope, provided, handlers, invocation_handlers, invocation_middleware) = {
            let cell = self.0.fibers.get(fiber).ok_or(CordisError::FiberNotFound)?;
            let record = cell.inner.read();
            (
                record.scope,
                record.provided.clone(),
                record.handlers.clone(),
                record.invocation_handlers.clone(),
                record.invocation_middleware.clone(),
            )
        };
        let symbols: Vec<_> = provided
            .iter()
            .map(|key| {
                self.lookup_symbol(key).ok_or_else(|| {
                    CordisError::Invariant("provided service was not interned".into())
                })
            })
            .collect::<Result<_, _>>()?;
        let _admission = self.0.admission.enter()?;
        for symbol in &symbols {
            let entry = self.0.services.get(scope, *symbol).ok_or_else(|| {
                CordisError::Invariant("activation service disappeared".into())
            })?;
            if entry.owner != fiber {
                return Err(CordisError::Invariant(
                    "activation service owner changed".into(),
                ));
            }
        }
        self.0
            .invocations
            .validate_activation(&invocation_handlers, &invocation_middleware)?;
        let cell = self.0.fibers.get(fiber).ok_or(CordisError::FiberNotFound)?;
        let mut record = cell.inner.write();
        if record.state != FiberState::Starting || !record.activation_sealed {
            return Err(CordisError::InvalidFiberState {
                fiber,
                from: record.state,
                to: FiberState::Active,
            });
        }
        if !record.capabilities.is_staged() {
            return Err(CordisError::CapabilityPublicationFailed(fiber));
        }
        let capabilities = record.capabilities.clone();
        if !capabilities.publish() {
            return Err(CordisError::CapabilityPublicationFailed(fiber));
        }
        record.staged = false;
        record.state = FiberState::Active;
        record.activation_sealed = false;
        let barrier = record.activation.take();
        self.bump_service_epoch();
        let _ = handlers;
        drop(record);
        if let Some(barrier) = barrier {
            let _ = barrier.send(true);
        }
        Ok(symbols)
    }

    fn transition(&self, fiber: FiberId, next: FiberState) -> Result<(), CordisError> {
        let cell = self.0.fibers.get(fiber).ok_or(CordisError::FiberNotFound)?;
        let mut record = cell.inner.write();
        if !record.state.can_transition_to(next) {
            return Err(CordisError::InvalidFiberState {
                fiber,
                from: record.state,
                to: next,
            });
        }
        debug!(?fiber, from=?record.state, to=?next, "fiber transition");
        record.state = next;
        Ok(())
    }

}
