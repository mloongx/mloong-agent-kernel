impl Runtime {
    pub(crate) fn submit_host_failure(
        &self,
        fiber: FiberId,
        generation: GenerationId,
        error: HostError,
    ) {
        let runtime = self.clone();
        self.0.workers.spawn(
            RuntimeWorkerKind::HostFailureConvergence,
            true,
            async move { runtime.converge_host_failure(fiber, generation, error).await },
        );
    }

    async fn converge_host_failure(
        &self,
        fiber: FiberId,
        generation: GenerationId,
        error: HostError,
    ) -> Result<(), CordisError> {
        let Some(cell) = self.0.fibers.get(fiber) else {
            return Ok(());
        };
        {
            let _lifecycle = cell.lifecycle.lock().await;
            let mut record = cell.inner.write();
            if record.generation != generation
                || record.staged
                || !matches!(record.state, FiberState::Active | FiberState::Reloading)
            {
                return Ok(());
            }
            record.state = FiberState::Failed;
        }
        tracing::warn!(?fiber, generation = generation.get(), %error, "hosted execution failed; Runtime is converging its owning Fiber");
        match self.dispose_fiber(fiber, false).await {
            Ok(()) | Err(CordisError::FiberNotFound | CordisError::FiberLifecycleOwned(_)) => {
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    fn intern(&self, key: &ServiceKey) -> ServiceSymbol {
        self.0.services.intern(key)
    }

    fn lookup_symbol(&self, key: &ServiceKey) -> Option<ServiceSymbol> {
        self.0.services.lookup(key)
    }

    /// Interns a stable service key for allocation-free hot-path lookup.
    pub fn intern_service(&self, key: &ServiceKey) -> Result<ServiceSymbol, CordisError> {
        self.0.services.try_intern(key)
    }

    fn bump_service_epoch(&self) {
        self.0.services.bump_epoch();
    }
    /// Creates an empty runtime with one root scope.
    ///
    /// # Panics
    ///
    /// Panics only if the built-in [`RuntimeConfig::default`] becomes invalid.
    #[must_use]
    pub fn new() -> Self {
        Self::with_config(RuntimeConfig::default()).expect("default runtime config is valid")
    }

    /// Creates a runtime with validated personal-resource limits.
    pub fn with_config(config: RuntimeConfig) -> Result<Self, CordisError> {
        config.validate()?;
        let scopes = ScopeRegistry::new();
        let root = scopes.root();
        let diagnostics = Arc::new(Diagnostics::default());
        Ok(Self(Arc::new(RuntimeInner {
            shutdown: ShutdownCoordinator::default(),
            fibers: FiberRegistry::default(),
            events: EventBus::default(),
            invocations: InvocationRegistry::default(),
            tasks: TaskSupervisor::new(diagnostics.clone()),
            workers: RuntimeWorkerSupervisor::new(diagnostics.clone()),
            root,
            scopes,
            plugins: PluginRegistry::default(),
            services: ServiceRegistry::new(
                config.max_interned_symbols,
                config.max_resolution_cache_entries,
            ),
            dependencies: DependencyGraph::default(),
            invocation_permits: Semaphore::new(config.max_concurrent_invocations),
            diagnostics,
            config,
            next_invocation: AtomicU64::new(1),
            active_reloads: AtomicUsize::new(0),
            reload_cleanup_pending: AtomicUsize::new(0),
            gc_state: AtomicUsize::new(0),
            pending_activations: Mutex::new(HashMap::new()),
            admission: AdmissionGate::default(),
            shutdown_deadline: Mutex::new(None),
            #[cfg(test)]
            fail_next_reload_publication: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            reload_before_selector_hook: Mutex::new(None),
            #[cfg(test)]
            reload_after_selector_hook: Mutex::new(None),
            #[cfg(test)]
            fail_selector_after_scope: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            context_before_gate_hook: Mutex::new(None),
            #[cfg(test)]
            service_before_gate_hook: Mutex::new(None),
            #[cfg(test)]
            activation_before_commit_hook: Mutex::new(None),
            #[cfg(test)]
            gc_registration_hook: Mutex::new(None),
        })))
    }

    fn schedule_activation(&self, fiber: FiberId) {
        let Ok(_admission) = self.0.admission.enter() else { return };
        {
            let mut pending = self.0.pending_activations.lock();
            if let Some(dirty) = pending.get_mut(&fiber) {
                *dirty = true;
                return;
            }
            pending.insert(fiber, false);
        }
        let runtime = self.clone();
        self.0.workers.spawn(RuntimeWorkerKind::DependencyReconcile, false, async move {
            loop {
                let result = runtime.activate(fiber).await;
                let rerun = {
                    let mut pending = runtime.0.pending_activations.lock();
                    if pending.get(&fiber).copied().unwrap_or(false) {
                        pending.insert(fiber, false);
                        true
                    } else {
                        pending.remove(&fiber);
                        false
                    }
                };
                if !rerun { return result; }
            }
        });
    }

    /// Root scope ID.
    #[must_use]
    pub fn root(&self) -> ScopeId {
        self.0.root
    }

}
