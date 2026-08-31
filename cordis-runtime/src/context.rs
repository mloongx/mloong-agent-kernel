struct ContextAdmission {
    owner: Arc<FiberCell>,
    scope: ScopeId,
    _lease: crate::gate::GenerationLease,
}

struct RegistrationAdmission {
    base: ContextAdmission,
    gate: Arc<CapabilityGate>,
}

struct CancellableAdmission {
    base: ContextAdmission,
    cancellation: CancellationToken,
}

struct ContextAdmissionCore {
    owner: Arc<FiberCell>,
    scope: ScopeId,
    gate: Arc<CapabilityGate>,
    cancellation: Option<CancellationToken>,
    lease: crate::gate::GenerationLease,
}

#[cfg(test)]
pub(crate) static SKIP_CONTEXT_BEFORE_GATE_HOOK: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

impl Context {
    pub(crate) fn runtime_shutdown_deadline(&self) -> Option<tokio::time::Instant> {
        self.runtime
            .upgrade()
            .and_then(|runtime| *runtime.shutdown_deadline.lock())
    }

    pub(crate) fn register_host_process(
        &self,
        live: Arc<std::sync::atomic::AtomicBool>,
    ) -> Result<(), CordisError> {
        let runtime = self.runtime()?;
        let _admission = runtime.admission.enter()?;
        let admission = self.admit()?;
        let mut fiber = admission.owner.inner.write();
        Self::ensure_mutable(&fiber, self.fiber)?;
        fiber.host_processes.push(live);
        Ok(())
    }

    pub(crate) fn report_host_failure(&self, error: HostError) {
        let Some(inner) = self.runtime.upgrade() else {
            return;
        };
        let runtime = Runtime(inner);
        runtime.submit_host_failure(self.fiber, self.generation, error);
    }

    fn runtime(&self) -> Result<Arc<RuntimeInner>, CordisError> {
        self.runtime
            .upgrade()
            .ok_or(CordisError::RuntimeShuttingDown)
    }
    fn owner(&self) -> Result<Arc<FiberCell>, CordisError> {
        self.owner.upgrade().ok_or(CordisError::FiberNotFound)
    }
    fn admit_core(&self, capture_cancellation: bool) -> Result<ContextAdmissionCore, CordisError> {
        let owner = self.owner()?;
        let fiber = owner.inner.read();
        let actual = fiber.generation;
        if actual != self.generation {
            return Err(CordisError::StaleContextGeneration {
                fiber: self.fiber,
                expected: self.generation.get(),
                actual: actual.get(),
            });
        }
        let lifecycle_admitted = matches!(
            fiber.state,
            FiberState::Starting | FiberState::Active | FiberState::Reloading
        );
        let gate = fiber.capabilities.clone();
        let scope = fiber.scope;
        let cancellation = capture_cancellation.then(|| fiber.cancellation.clone());
        drop(fiber);
        #[cfg(test)]
        let context_before_gate_hook = if SKIP_CONTEXT_BEFORE_GATE_HOOK
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            None
        } else {
            self.runtime
                .upgrade()
                .and_then(|runtime| runtime.context_before_gate_hook.lock().clone())
        };
        #[cfg(test)]
        if let Some(hook) = context_before_gate_hook {
            hook.wait();
            hook.wait();
        }
        let Some(lease) = gate.try_acquire() else {
            return Err(CordisError::StaleContextGeneration {
                fiber: self.fiber,
                expected: self.generation.get(),
                actual: actual.get(),
            });
        };
        if !lifecycle_admitted {
            return Err(CordisError::FiberInactive(self.fiber));
        }
        Ok(ContextAdmissionCore {
            owner,
            scope,
            gate,
            cancellation,
            lease,
        })
    }
    fn admit(&self) -> Result<ContextAdmission, CordisError> {
        let ContextAdmissionCore {
            owner,
            scope,
            lease,
            ..
        } = self.admit_core(false)?;
        Ok(ContextAdmission {
            owner,
            scope,
            _lease: lease,
        })
    }
    fn admit_registration(&self) -> Result<RegistrationAdmission, CordisError> {
        let ContextAdmissionCore {
            owner,
            scope,
            gate,
            lease,
            ..
        } = self.admit_core(false)?;
        Ok(RegistrationAdmission {
            base: ContextAdmission {
                owner,
                scope,
                _lease: lease,
            },
            gate,
        })
    }
    fn admit_cancellable(&self) -> Result<CancellableAdmission, CordisError> {
        let ContextAdmissionCore {
            owner,
            scope,
            cancellation,
            lease,
            ..
        } = self.admit_core(true)?;
        Ok(CancellableAdmission {
            base: ContextAdmission {
                owner,
                scope,
                _lease: lease,
            },
            cancellation: cancellation.expect("cancellable admission captures cancellation"),
        })
    }
    #[cfg(test)]
    pub(crate) fn characterize_base_admission(&self) -> Result<(), CordisError> {
        std::hint::black_box(self.admit()?);
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn characterize_registration_admission(&self) -> Result<(), CordisError> {
        std::hint::black_box(self.admit_registration()?);
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn characterize_cancellable_admission(&self) -> Result<(), CordisError> {
        std::hint::black_box(self.admit_cancellable()?);
        Ok(())
    }
    fn ensure_mutable(fiber: &FiberMutable, fiber_id: FiberId) -> Result<(), CordisError> {
        if fiber.activation_sealed {
            return Err(CordisError::ActivationSealed(fiber_id));
        }
        if matches!(
            fiber.state,
            FiberState::Starting | FiberState::Active | FiberState::Reloading
        ) {
            Ok(())
        } else {
            Err(CordisError::FiberInactive(fiber_id))
        }
    }
    /// Returns the current Scope binding for this Context generation.
    pub fn scope(&self) -> Result<ScopeId, CordisError> {
        Ok(self.admit()?.scope)
    }

    /// Scope captured when the Context was created. This value is diagnostic
    /// only; Runtime operations use the generation-bound current Scope.
    #[must_use]
    pub const fn initial_scope(&self) -> ScopeId {
        self.scope
    }
    /// Current fiber owner.
    #[must_use]
    pub const fn fiber(&self) -> FiberId {
        self.fiber
    }

    /// Immutable generation identity captured with this Context.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation.get()
    }

    /// Returns the parent scope, if this is not root.
    pub fn parent(&self) -> Result<Option<ScopeId>, CordisError> {
        let context_admission = self.admit()?;
        self.runtime()?.scopes.parent(context_admission.scope)
    }

    /// Returns the root scope.
    pub fn root(&self) -> Result<ScopeId, CordisError> {
        let _context_admission = self.admit()?;
        Ok(self.runtime()?.root)
    }

    /// Registers a typed service in the current scope.
    pub fn provide<T: Any + Send + Sync>(
        &self,
        key: ServiceKey,
        value: T,
    ) -> Result<(), CordisError> {
        self.provide_arc(key, Arc::new(value))
    }

    /// Registers an already-shared sized or trait-object service.
    pub fn provide_arc<T: ?Sized + Send + Sync + 'static>(
        &self,
        key: ServiceKey,
        value: Arc<T>,
    ) -> Result<(), CordisError> {
        let runtime = self.runtime()?;
        let _admission = runtime.admission.enter()?;
        let registration = self.admit_registration()?;
        let owner = registration.base.owner.clone();
        let waiting = {
            let mut fiber = owner.inner.write();
            Self::ensure_mutable(&fiber, self.fiber)?;
            let (declared, active) = {
                (
                    fiber.descriptor.provisions.contains(&key),
                    fiber.state == FiberState::Active && !fiber.staged,
                )
            };
            if !declared {
                return Err(CordisError::RevisionValidationFailed(format!(
                    "undeclared service provision: {key}"
                )));
            }
            let symbol = runtime.services.lookup(&key).ok_or_else(|| {
                CordisError::Invariant(format!(
                    "declared service key is missing from the admitted interner: {key}"
                ))
            })?;
            if runtime.services.contains(registration.base.scope, symbol) {
                return Err(CordisError::DuplicateService(key));
            }
            runtime.services.insert(
                registration.base.scope,
                symbol,
                ServiceEntry {
                    owner: self.fiber,
                    scope: registration.base.scope,
                    value: ServiceValue::Native(Arc::new(value)),
                    gate: registration.gate.clone(),
                },
            );
            fiber.provided.push(key.clone());
            if active {
                runtime.services.bump_epoch();
                runtime.dependencies.dependents(symbol)
            } else {
                SmallVec::new()
            }
        };
        for dependent in waiting {
            let cordis = Runtime(runtime.clone());
            let candidate = runtime.fibers.with(dependent, |fiber| {
                    (fiber.state == FiberState::WaitingDependencies)
                        .then(|| (fiber.scope, fiber.descriptor.dependencies.clone()))
                }).flatten();
            if candidate
                .is_some_and(|(scope, dependencies)| cordis.dependencies_met(scope, &dependencies))
            {
                cordis.schedule_activation(dependent);
            }
        }
        Ok(())
    }

    /// Resolves a typed service through current-to-root scope inheritance and
    /// returns a handle that pins the exact provider generation while retained.
    pub fn get<T: ?Sized + Send + Sync + 'static>(
        &self,
        key: &ServiceKey,
    ) -> Result<ServiceHandle<T>, CordisError> {
        self.try_get(key)?
            .ok_or_else(|| CordisError::ServiceNotFound(key.clone()))
    }

    /// Attempts to resolve a typed service synchronously. A visible entry that
    /// loses provider admission to a cutover is resolved once more.
    pub fn try_get<T: ?Sized + Send + Sync + 'static>(
        &self,
        key: &ServiceKey,
    ) -> Result<Option<ServiceHandle<T>>, CordisError> {
        let context_admission = self.admit()?;
        let runtime = Runtime(self.runtime()?);
        let Some(symbol) = runtime.lookup_symbol(key) else {
            return Ok(None);
        };
        for attempt in 0..2 {
            let Some(entry) = runtime.resolve_owned(context_admission.scope, symbol, self.fiber)
            else {
                return Ok(None);
            };
            #[cfg(test)]
            if attempt == 0 {
                let hook = runtime.0.service_before_gate_hook.lock().take();
                if let Some(hook) = hook {
                    hook.wait();
                    hook.wait();
                }
            }
            let Some(lease) = entry.gate.try_acquire_service() else {
                if attempt == 0 {
                    continue;
                }
                return Err(CordisError::ServiceGenerationDraining { provider: entry.owner });
            };
            return match entry.value {
                ServiceValue::Native(value) => value
                    .downcast::<Arc<T>>()
                    .map(|service| Some(ServiceHandle::new(
                        Arc::clone(service.as_ref()),
                        lease,
                        entry.owner,
                        entry.gate.generation_id().get(),
                        entry.scope,
                        symbol,
                    )))
                    .map_err(|_| CordisError::TypeMismatch(key.clone())),
                ServiceValue::External(_) => Err(CordisError::TypeMismatch(key.clone())),
            };
        }
        unreachable!("bounded service lookup retry")
    }

    /// Resolves a generation-tracked service using a pre-interned hot-path symbol.
    pub fn get_symbol<T: ?Sized + Send + Sync + 'static>(
        &self,
        symbol: ServiceSymbol,
    ) -> Result<ServiceHandle<T>, CordisError> {
        let context_admission = self.admit()?;
        let runtime = Runtime(self.runtime()?);
        for attempt in 0..2 {
            let entry = runtime
                .resolve_owned(context_admission.scope, symbol, self.fiber)
                .ok_or_else(|| CordisError::Invariant(format!(
                    "service symbol {} was not found", symbol.index()
                )))?;
            let Some(lease) = entry.gate.try_acquire_service() else {
                if attempt == 0 { continue; }
                return Err(CordisError::ServiceGenerationDraining { provider: entry.owner });
            };
            return match entry.value {
                ServiceValue::Native(value) => value.downcast::<Arc<T>>().map(|service| {
                    ServiceHandle::new(
                        Arc::clone(service.as_ref()),
                        lease,
                        entry.owner,
                        entry.gate.generation_id().get(),
                        entry.scope,
                        symbol,
                    )
                }).map_err(|_| CordisError::Invariant(format!(
                    "service symbol {} type mismatch", symbol.index()
                ))),
                ServiceValue::External(_) => Err(CordisError::Invariant(format!(
                    "service symbol {} is external", symbol.index()
                ))),
            };
        }
        unreachable!("bounded service symbol retry")
    }

    /// Returns whether a service is visible.
    pub fn contains(&self, key: &ServiceKey) -> Result<bool, CordisError> {
        let context_admission = self.admit()?;
        let runtime = Runtime(self.runtime()?);
        let Some(symbol) = runtime.lookup_symbol(key) else {
            return Ok(false);
        };
        Ok(runtime.resolve_owned(context_admission.scope, symbol, self.fiber).is_some())
    }

    /// Adds an arbitrary async cleanup effect to the current activation.
    pub fn effect(&self, effect: Box<dyn Effect>) -> Result<(), CordisError> {
        let runtime = self.runtime()?;
        let _admission = runtime.admission.enter()?;
        let context_admission = self.admit()?;
        let owner = context_admission.owner.clone();
        let mut fiber = owner.inner.write();
        Self::ensure_mutable(&fiber, self.fiber)?;
        if fiber.effects.len() >= runtime.config.max_effects_per_fiber {
            return Err(runtime.quota_error(ResourceKind::EffectsPerFiber, runtime.config.max_effects_per_fiber, Some(context_admission.scope), Some(self.fiber)));
        }
        fiber.effects.push(effect);
        Ok(())
    }

    /// Registers an owned event handler.
    pub fn on(
        &self,
        event: EventKey,
        handler: Arc<dyn EventHandler>,
    ) -> Result<HandlerId, CordisError> {
        let runtime = self.runtime()?;
        let _admission = runtime.admission.enter()?;
        let registration = self.admit_registration()?;
        let owner = registration.base.owner.clone();
        let mut fiber = owner.inner.write();
        Self::ensure_mutable(&fiber, self.fiber)?;
        Self::ensure_handler_quota(&runtime, &fiber, self.fiber)?;
        let id = runtime
            .events
            .register_gated(event, handler, registration.gate.clone());
        fiber.handlers.push(id);
        Ok(id)
    }

    fn ensure_handler_quota(
        runtime: &RuntimeInner,
        fiber: &FiberMutable,
        fiber_id: FiberId,
    ) -> Result<(), CordisError> {
        let count = fiber.handlers.len()
            + fiber.invocation_handlers.len()
            + fiber.invocation_middleware.len();
        if count >= runtime.config.max_handlers_per_fiber {
            Err(runtime.quota_error(ResourceKind::HandlersPerFiber, runtime.config.max_handlers_per_fiber, None, Some(fiber_id)))
        } else {
            Ok(())
        }
    }

    /// Registers the single scoped handler for an invocation key.
    pub fn handle_invocation(
        &self,
        key: InvocationKey,
        handler: Arc<dyn InvocationHandler>,
    ) -> Result<InvocationHandlerId, CordisError> {
        let runtime = self.runtime()?;
        let _admission = runtime.admission.enter()?;
        let registration = self.admit_registration()?;
        let owner = registration.base.owner.clone();
        let mut fiber = owner.inner.write();
        Self::ensure_mutable(&fiber, self.fiber)?;
        Self::ensure_handler_quota(&runtime, &fiber, self.fiber)?;
        let id = runtime
            .invocations
            .register_handler(
                registration.base.scope,
                key,
                handler,
                registration.gate.clone(),
            )?;
        fiber.invocation_handlers.push(id);
        Ok(id)
    }

    /// Registers scoped middleware for one invocation key. Middleware runs
    /// root-to-leaf and then in registration order within each scope.
    pub fn invocation_middleware(
        &self,
        key: InvocationKey,
        middleware: Arc<dyn InvocationMiddleware>,
    ) -> Result<InvocationMiddlewareId, CordisError> {
        let runtime = self.runtime()?;
        let _admission = runtime.admission.enter()?;
        let registration = self.admit_registration()?;
        let owner = registration.base.owner.clone();
        let mut fiber = owner.inner.write();
        Self::ensure_mutable(&fiber, self.fiber)?;
        Self::ensure_handler_quota(&runtime, &fiber, self.fiber)?;
        let id = runtime
            .invocations
            .register_middleware(
                registration.base.scope,
                key,
                middleware,
                registration.gate.clone(),
            );
        fiber.invocation_middleware.push(id);
        Ok(id)
    }

    #[allow(clippy::too_many_lines)]
    async fn invoke_inner(
        &self,
        key: &InvocationKey,
        input: InvocationValue,
        deadline: tokio::time::Instant,
    ) -> Result<InvocationValue, CordisError> {
        let runtime = self.runtime()?;
        let cancellation = {
            let _admission = runtime.admission.enter()?;
            let owner = self.owner()?;
            let fiber = owner.inner.read();
            if !matches!(fiber.state, FiberState::Starting | FiberState::Active) {
                return Err(CordisError::FiberInactive(self.fiber));
            }
            fiber.cancellation.clone()
        };
        let permit = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(CordisError::InvocationCancelled),
            permit = runtime.invocation_permits.acquire() => {
                permit.map_err(|_| CordisError::RuntimeShuttingDown)?
            }
        };
        // Waiting for global execution capacity is not provider execution. Take
        // the dispatch snapshot and generation leases only after capacity is
        // available, then immediately enter the selected provider chain.
        let context_admission = {
            let _admission = runtime.admission.enter()?;
            self.admit_cancellable()?
        };
        {
            let owner = context_admission.base.owner.clone();
            let fiber = owner.inner.read();
            if !matches!(fiber.state, FiberState::Starting | FiberState::Active)
                || fiber.cancellation.is_cancelled()
            {
                return Err(CordisError::InvocationCancelled);
            }
        }
        let scope = context_admission.base.scope;
        let scopes = runtime.scopes.ancestry_root_to_leaf(scope, true)?;
        let snapshot = runtime.invocations.snapshot(&scopes, key)?;
        let id = InvocationId(runtime.next_invocation.fetch_add(1, Ordering::Relaxed));
        let context = invocation_context(
            InvocationMetadata::new(id, key.clone(), scope, self.fiber),
            deadline,
        );
        let span = info_span!(
            "cordis.invocation",
            invocation_id = id.0,
            invocation_key = %key,
            scope = ?scope,
            fiber = ?self.fiber
        );
        let result = tokio::select! {
            biased;
            () = context_admission.cancellation.cancelled() => Err(CordisError::InvocationCancelled),
            result = snapshot.invoke(context, input).instrument(span) => result,
        };
        drop(permit);
        match &result {
            Ok(_) => {
                runtime
                    .diagnostics
                    .successes
                    .fetch_add(1, Ordering::Relaxed);
            }
            Err(CordisError::InvocationCancelled) => {
                runtime
                    .diagnostics
                    .cancellations
                    .fetch_add(1, Ordering::Relaxed);
                runtime.diagnostics.push(HealthIssue {
                    at: std::time::SystemTime::now(),
                    kind: HealthIssueKind::InvocationCancelled,
                    scope: Some(scope),
                    fiber: Some(self.fiber),
                    invocation: Some(id),
                });
            }
            Err(
                CordisError::InvocationHandlerPanicked(_)
                | CordisError::InvocationMiddlewarePanicked(_),
            ) => {
                runtime.diagnostics.panics.fetch_add(1, Ordering::Relaxed);
                runtime.diagnostics.push(HealthIssue {
                    at: std::time::SystemTime::now(),
                    kind: HealthIssueKind::InvocationPanic,
                    scope: Some(scope),
                    fiber: Some(self.fiber),
                    invocation: Some(id),
                });
            }
            Err(_) => {
                runtime.diagnostics.errors.fetch_add(1, Ordering::Relaxed);
                runtime.diagnostics.push(HealthIssue {
                    at: std::time::SystemTime::now(),
                    kind: HealthIssueKind::InvocationError,
                    scope: Some(scope),
                    fiber: Some(self.fiber),
                    invocation: Some(id),
                });
            }
        }
        result
    }

    /// Invokes a scoped handler using an immutable snapshot and the default deadline.
    #[allow(clippy::single_match_else)]
    pub async fn invoke(
        &self,
        key: &InvocationKey,
        input: InvocationValue,
    ) -> Result<InvocationValue, CordisError> {
        let timeout = self.runtime()?.config.default_invocation_timeout;
        let deadline = tokio::time::Instant::now() + timeout;
        match tokio::time::timeout_at(deadline, self.invoke_inner(key, input, deadline)).await {
            Ok(result) => result,
            Err(_) => {
                let runtime = self.runtime()?;
                runtime
                    .diagnostics
                    .timeouts
                    .fetch_add(1, Ordering::Relaxed);
                runtime.diagnostics.push(HealthIssue {
                    at: std::time::SystemTime::now(),
                    kind: HealthIssueKind::InvocationTimeout,
                    scope: self.scope().ok(),
                    fiber: Some(self.fiber),
                    invocation: None,
                });
                Err(CordisError::InvocationTimedOut)
            }
        }
    }

    /// Invokes with a caller-owned deadline. Timing out drops the handler chain;
    /// no detached handler task remains running.
    #[allow(clippy::single_match_else)]
    pub async fn invoke_with_timeout(
        &self,
        key: &InvocationKey,
        input: InvocationValue,
        timeout: Duration,
    ) -> Result<InvocationValue, CordisError> {
        let deadline = tokio::time::Instant::now() + timeout;
        match tokio::time::timeout_at(deadline, self.invoke_inner(key, input, deadline)).await {
            Ok(result) => result,
            Err(_) => {
                let runtime = self.runtime()?;
                runtime
                    .diagnostics
                    .timeouts
                    .fetch_add(1, Ordering::Relaxed);
                runtime.diagnostics.push(HealthIssue {
                    at: std::time::SystemTime::now(),
                    kind: HealthIssueKind::InvocationTimeout,
                    scope: self.scope().ok(),
                    fiber: Some(self.fiber),
                    invocation: None,
                });
                Err(CordisError::InvocationTimedOut)
            }
        }
    }

    /// Invokes a native handler and downcasts its response without panicking.
    pub async fn invoke_typed<Request, Response>(
        &self,
        key: &InvocationKey,
        input: Arc<Request>,
    ) -> Result<Arc<Response>, CordisError>
    where
        Request: Any + Send + Sync,
        Response: Any + Send + Sync,
    {
        self.invoke(key, InvocationValue::native(input))
            .await?
            .downcast_native::<Response>()
            .map_err(|_| CordisError::InvocationTypeMismatch(key.clone()))
    }

    /// Typed variant of [`Self::invoke_with_timeout`].
    pub async fn invoke_typed_with_timeout<Request, Response>(
        &self,
        key: &InvocationKey,
        input: Arc<Request>,
        timeout: Duration,
    ) -> Result<Arc<Response>, CordisError>
    where
        Request: Any + Send + Sync,
        Response: Any + Send + Sync,
    {
        self.invoke_with_timeout(key, InvocationValue::native(input), timeout)
            .await?
            .downcast_native::<Response>()
            .map_err(|_| CordisError::InvocationTypeMismatch(key.clone()))
    }

    /// Spawns an owned task cancelled when the fiber is disposed.
    pub fn spawn<F>(&self, future: F) -> Result<TaskId, CordisError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let runtime = self.runtime()?;
        let _admission = runtime.admission.enter()?;
        let context_admission = self.admit()?;
        let owner = context_admission.owner.clone();
        let mut fiber = owner.inner.write();
        if !matches!(fiber.state, FiberState::Starting | FiberState::Active)
            || (fiber.state == FiberState::Starting && fiber.activation_sealed)
        {
            return Err(CordisError::FiberInactive(self.fiber));
        }
        if fiber.tasks.len() >= runtime.config.max_tasks_per_fiber {
            return Err(runtime.quota_error(ResourceKind::TasksPerFiber, runtime.config.max_tasks_per_fiber, Some(context_admission.scope), Some(self.fiber)));
        }
        let cancellation = fiber.cancellation.clone();
        let activation = fiber.activation.as_ref().map(watch::Sender::subscribe);
        let (id, start) = runtime.tasks.spawn(
            self.fiber,
            context_admission.scope,
            Arc::downgrade(&owner),
            &cancellation,
            activation,
            future,
        );
        fiber.tasks.push(id);
        let _ = start.send(());
        Ok(id)
    }

    /// Sleeps until the duration elapses or this fiber is disposed.
    pub async fn sleep(&self, duration: Duration) -> Result<(), CordisError> {
        let context_admission = self.admit_cancellable()?;
        tokio::select! {
            () = context_admission.cancellation.cancelled() => Err(CordisError::TaskCancelled),
            () = tokio::time::sleep(duration) => Ok(()),
        }
    }

    /// Runs a future with both lifecycle cancellation and a deadline.
    pub async fn timeout<F, T>(&self, duration: Duration, future: F) -> Result<T, CordisError>
    where
        F: Future<Output = T> + Send,
    {
        let context_admission = self.admit_cancellable()?;
        tokio::select! {
            () = context_admission.cancellation.cancelled() => Err(CordisError::TaskCancelled),
            result = tokio::time::timeout(duration, future) => result.map_err(|_| CordisError::Timeout),
        }
    }

    /// Starts a lifecycle-owned interval task.
    pub fn interval<F, Fut>(&self, period: Duration, callback: F) -> Result<TaskId, CordisError>
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let _context_admission = self.admit()?;
        if period.is_zero() {
            return Err(CordisError::InvalidRuntimeConfig(
                "interval period must be non-zero".into(),
            ));
        }
        self.spawn(async move {
            let mut timer = tokio::time::interval(period);
            timer.tick().await;
            loop {
                timer.tick().await;
                callback().await;
            }
        })
    }

    /// Creates and owns a child scope.
    pub fn create_scope(&self, name: impl Into<Arc<str>>) -> Result<ScopeId, CordisError> {
        let runtime = self.runtime()?;
        let _admission = runtime.admission.enter()?;
        if runtime.scopes.len() >= runtime.config.max_scopes {
            let _ = Runtime(runtime.clone()).collect_garbage();
        }
        let context_admission = self.admit()?;
        let scope = context_admission.scope;
        let owner = context_admission.owner.clone();
        let mut fiber = owner.inner.write();
        Self::ensure_mutable(&fiber, self.fiber)?;
        if fiber.child_scopes.len() >= runtime.config.max_child_scopes_per_fiber {
            return Err(runtime.quota_error(ResourceKind::ChildScopesPerFiber, runtime.config.max_child_scopes_per_fiber, Some(scope), Some(self.fiber)));
        }
        let mut scopes = runtime.scopes.write();
        if scopes
            .get(scope)
            .ok_or(CordisError::ScopeNotFound)?
            .state
            != ScopeState::Active
        {
            return Err(CordisError::ScopeDisposed(scope));
        }
        if scopes.len() >= runtime.config.max_scopes {
            return Err(runtime.quota_error(ResourceKind::Scopes, runtime.config.max_scopes, Some(scope), Some(self.fiber)));
        }
        let mut depth = 1_usize;
        let mut cursor = Some(scope);
        while let Some(scope) = cursor {
            cursor = scopes.get(scope).and_then(|record| record.parent);
            if cursor.is_some() {
                depth += 1;
            }
        }
        if depth > runtime.config.max_scope_depth {
            return Err(runtime.quota_error(ResourceKind::ScopeDepth, runtime.config.max_scope_depth, Some(scope), Some(self.fiber)));
        }
        let id = scopes.insert(ScopeRecord {
            name: name.into(),
            parent: Some(scope),
            children: SmallVec::new(),
            fibers: SmallVec::new(),
            state: ScopeState::Active,
            hidden: false,
            disposal: ScopeDisposal::default(),
        });
        scopes
            .get_mut(scope)
            .ok_or(CordisError::ScopeNotFound)?
            .children
            .push(id);
        drop(scopes);
        fiber.child_scopes.push(id);
        Ok(id)
    }

    /// Emits an event serially using a lock-free handler snapshot.
    pub async fn emit(&self, event: &EventKey, value: EventValue) -> Result<(), CordisError> {
        let runtime = self.runtime()?;
        Self::ensure_event_admitted(&runtime)?;
        let _context_admission = self.admit()?;
        runtime.events.emit(event, value).await
    }
    /// Dispatches until a handler returns a value.
    pub async fn bail(
        &self,
        event: &EventKey,
        value: EventValue,
    ) -> Result<EventOutcome, CordisError> {
        let runtime = self.runtime()?;
        Self::ensure_event_admitted(&runtime)?;
        let _context_admission = self.admit()?;
        runtime.events.bail(event, value).await
    }
    /// Dispatches handlers serially and collects their outcomes.
    pub async fn serial(
        &self,
        event: &EventKey,
        value: EventValue,
    ) -> Result<Vec<EventOutcome>, CordisError> {
        let runtime = self.runtime()?;
        Self::ensure_event_admitted(&runtime)?;
        let _context_admission = self.admit()?;
        runtime.events.serial(event, value).await
    }
    /// Dispatches all handlers concurrently.
    pub async fn parallel(
        &self,
        event: &EventKey,
        value: EventValue,
    ) -> Result<Vec<EventOutcome>, CordisError> {
        let runtime = self.runtime()?;
        Self::ensure_event_admitted(&runtime)?;
        let _context_admission = self.admit()?;
        runtime.events.parallel(event, value).await
    }
    /// Dispatches a consumed-continuation middleware chain.
    pub async fn waterfall(
        &self,
        event: &EventKey,
        value: EventValue,
    ) -> Result<EventOutcome, CordisError> {
        let runtime = self.runtime()?;
        Self::ensure_event_admitted(&runtime)?;
        let _context_admission = self.admit()?;
        runtime.events.waterfall(event, value).await
    }

    fn ensure_event_admitted(runtime: &RuntimeInner) -> Result<(), CordisError> {
        let _admission = runtime.admission.enter()?;
        Ok(())
    }
}
