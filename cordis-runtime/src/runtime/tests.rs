#[cfg(test)]
mod disposal_worker_tests {
    use super::*;
    use crate::disposal::TestDisposalHook;
    use crate::gate::GenerationExecution;
    use futures::future::join_all;
    use hdrhistogram::Histogram;
    use parking_lot::RwLock as ParkingRwLock;
    use std::sync::{
        Barrier,
        atomic::{AtomicBool, AtomicU64 as StdAtomicU64, Ordering as StdOrdering},
    };
    use std::time::Instant as StdInstant;

    struct EmptyPlugin;
    struct SelfCyclePlugin;
    struct RestartBlockingPlugin {
        starts: Arc<AtomicU64>,
        entered: Arc<tokio::sync::Notify>,
    }

    #[derive(Default)]
    struct EffectGate {
        entered: std::sync::atomic::AtomicBool,
        entered_notify: tokio::sync::Notify,
        release: tokio::sync::Notify,
        executions: AtomicU64,
    }

    struct BlockingEffectPlugin(Arc<EffectGate>);
    struct FailingEffectPlugin;
    struct IndexedDisposalPlugin {
        dependency: ServiceKey,
        key: ServiceKey,
        fail_cleanup: bool,
    }
    struct MultiCleanupFailingStartPlugin;
    struct BlockingInvocationPlugin {
        key: InvocationKey,
        entered: Arc<tokio::sync::Semaphore>,
        release: Arc<tokio::sync::Semaphore>,
    }
    struct CountingCleanupPlugin(Arc<AtomicU64>);
    struct CountingStartPlugin(Arc<AtomicU64>);
    struct CountingTaskPlugin(Arc<AtomicU64>);
    struct RetainContextPlugin(Arc<Mutex<Option<Context>>>);
    struct ProvideProbePlugin {
        declared: ServiceKey,
        undeclared: Option<ServiceKey>,
        repeat_declared: bool,
        results: Arc<Mutex<Vec<Result<(), CordisError>>>>,
    }
    struct RetainInvocationContextPlugin {
        slot: Arc<Mutex<Option<Context>>>,
        key: InvocationKey,
    }
    struct ServiceRevisionPlugin {
        values: Vec<(ServiceKey, &'static str)>,
        task_runs: Option<Arc<AtomicU64>>,
    }
    struct LeasedServicePlugin {
        key: ServiceKey,
        value: &'static str,
        cleaned: Arc<AtomicU64>,
    }
    trait DynTestService: Send + Sync { fn value(&self) -> u64; }
    struct DynTestServiceImpl;
    impl DynTestService for DynTestServiceImpl { fn value(&self) -> u64 { 42 } }
    struct TraitServicePlugin(ServiceKey);
    struct ReloadGatePlugin {
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Semaphore>,
        fail: bool,
    }
    struct NoopEventHandler;
    struct NoopInvocationHandler;

    #[async_trait]
    impl EventHandler for NoopEventHandler {
        async fn call(
            &self,
            _value: EventValue,
            _next: Option<crate::Next>,
        ) -> Result<EventOutcome, CordisError> {
            Ok(EventOutcome::default())
        }
    }

    #[async_trait]
    impl InvocationHandler for NoopInvocationHandler {
        async fn call(
            &self,
            _context: crate::InvocationContext,
            input: InvocationValue,
        ) -> Result<crate::InvocationOutcome, CordisError> {
            Ok(input)
        }
    }

    #[async_trait]
    impl NativePlugin for RetainInvocationContextPlugin {
        fn descriptor(&self) -> PluginDescriptor {
            PluginDescriptor {
                name: "retained-invocation-context".into(),
                dependencies: Arc::new([]),
                provisions: Arc::new([]),
                dependency_policy: DependencyPolicy::default(),
                revision: cordis_core::PluginRevision::default(),
            }
        }

        async fn start(&self, context: Context) -> Result<(), CordisError> {
            context.handle_invocation(self.key.clone(), Arc::new(NoopInvocationHandler))?;
            *self.slot.lock() = Some(context);
            Ok(())
        }
    }

    #[async_trait]
    impl NativePlugin for ProvideProbePlugin {
        fn descriptor(&self) -> PluginDescriptor {
            PluginDescriptor {
                name: "provide-probe".into(),
                dependencies: Arc::new([]),
                provisions: Arc::new([self.declared.clone()]),
                dependency_policy: DependencyPolicy::default(),
                revision: cordis_core::PluginRevision::default(),
            }
        }

        async fn start(&self, context: Context) -> Result<(), CordisError> {
            if let Some(key) = &self.undeclared {
                self.results.lock().push(context.provide(key.clone(), "undeclared"));
            }
            context.provide(self.declared.clone(), "declared")?;
            if self.repeat_declared {
                self.results
                    .lock()
                    .push(context.provide(self.declared.clone(), "duplicate"));
            }
            Ok(())
        }
    }

    #[async_trait]
    impl NativePlugin for EmptyPlugin {
        fn descriptor(&self) -> PluginDescriptor {
            PluginDescriptor {
                name: "worker-test".into(),
                dependencies: Arc::new([]),
                provisions: Arc::new([]),
                dependency_policy: DependencyPolicy::default(),
                revision: cordis_core::PluginRevision::default(),
            }
        }

        async fn start(&self, _context: Context) -> Result<(), CordisError> {
            Ok(())
        }
    }

    #[async_trait]
    impl NativePlugin for BlockingEffectPlugin {
        fn descriptor(&self) -> PluginDescriptor {
            EmptyPlugin.descriptor()
        }

        async fn start(&self, context: Context) -> Result<(), CordisError> {
            let gate = self.0.clone();
            context.effect(cordis_core::effect_fn(move || async move {
                gate.executions.fetch_add(1, Ordering::SeqCst);
                gate.entered.store(true, Ordering::SeqCst);
                gate.entered_notify.notify_waiters();
                gate.release.notified().await;
                Ok(())
            }))
        }
    }

    #[async_trait]
    impl NativePlugin for FailingEffectPlugin {
        fn descriptor(&self) -> PluginDescriptor {
            EmptyPlugin.descriptor()
        }

        async fn start(&self, context: Context) -> Result<(), CordisError> {
            context.effect(cordis_core::effect_fn(|| async {
                Err(CordisError::PluginDisposeFailed("expected".into()))
            }))
        }
    }

    #[async_trait]
    impl NativePlugin for CountingCleanupPlugin {
        fn descriptor(&self) -> PluginDescriptor {
            EmptyPlugin.descriptor()
        }

        async fn start(&self, context: Context) -> Result<(), CordisError> {
            let count = self.0.clone();
            context.effect(cordis_core::effect_fn(move || async move {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }))
        }
    }

    #[async_trait]
    impl NativePlugin for IndexedDisposalPlugin {
        fn descriptor(&self) -> PluginDescriptor {
            PluginDescriptor {
                name: "indexed-disposal".into(),
                dependencies: Arc::new([self.dependency.clone()]),
                provisions: Arc::new([self.key.clone()]),
                dependency_policy: DependencyPolicy::default(),
                revision: cordis_core::PluginRevision::default(),
            }
        }

        async fn start(&self, context: Context) -> Result<(), CordisError> {
            context.provide(self.key.clone(), 1_u64)?;
            if self.fail_cleanup {
                context.effect(cordis_core::effect_fn(|| async {
                    Err(CordisError::PluginDisposeFailed("expected index test issue".into()))
                }))?;
            }
            Ok(())
        }
    }

    #[async_trait]
    impl NativePlugin for MultiCleanupFailingStartPlugin {
        fn descriptor(&self) -> PluginDescriptor {
            EmptyPlugin.descriptor()
        }

        async fn start(&self, context: Context) -> Result<(), CordisError> {
            context.effect(cordis_core::effect_fn(|| async {
                Err(CordisError::PluginDisposeFailed("cleanup-a".into()))
            }))?;
            context.effect(cordis_core::effect_fn(|| async {
                Err(CordisError::PluginDisposeFailed("cleanup-b".into()))
            }))?;
            Err(CordisError::PluginStartFailed("primary".into()))
        }
    }

    #[async_trait]
    impl NativePlugin for BlockingInvocationPlugin {
        fn descriptor(&self) -> PluginDescriptor {
            EmptyPlugin.descriptor()
        }

        async fn start(&self, context: Context) -> Result<(), CordisError> {
            context.handle_invocation(self.key.clone(), Arc::new(NoopInvocationHandler))?;
            self.entered.add_permits(1);
            let _permit = self.release.acquire().await.expect("release semaphore");
            Ok(())
        }
    }

    #[async_trait]
    impl NativePlugin for ReloadGatePlugin {
        fn descriptor(&self) -> PluginDescriptor {
            EmptyPlugin.descriptor()
        }

        async fn start(&self, _context: Context) -> Result<(), CordisError> {
            self.entered.notify_waiters();
            let _permit = self
                .release
                .acquire()
                .await
                .map_err(|_| CordisError::TaskCancelled)?;
            if self.fail {
                Err(CordisError::PluginStartFailed("injected prepare failure".into()))
            } else {
                Ok(())
            }
        }
    }

    #[async_trait]
    impl NativePlugin for SelfCyclePlugin {
        fn descriptor(&self) -> PluginDescriptor {
            let key = ServiceKey::new("fiber", "self-cycle", 1);
            PluginDescriptor {
                name: "self-cycle".into(),
                dependencies: Arc::new([key.clone()]),
                provisions: Arc::new([key]),
                dependency_policy: DependencyPolicy::default(),
                revision: cordis_core::PluginRevision::default(),
            }
        }

        async fn start(&self, _context: Context) -> Result<(), CordisError> {
            Ok(())
        }
    }

    #[async_trait]
    impl NativePlugin for RestartBlockingPlugin {
        fn descriptor(&self) -> PluginDescriptor {
            EmptyPlugin.descriptor()
        }

        async fn start(&self, _context: Context) -> Result<(), CordisError> {
            if self.starts.fetch_add(1, Ordering::SeqCst) != 0 {
                self.entered.notify_waiters();
                std::future::pending::<()>().await;
            }
            Ok(())
        }
    }

    #[async_trait]
    impl NativePlugin for CountingStartPlugin {
        fn descriptor(&self) -> PluginDescriptor {
            PluginDescriptor {
                name: "serialized-activation".into(),
                dependencies: Arc::new([ServiceKey::new("test", "missing", 1)]),
                provisions: Arc::new([]),
                dependency_policy: DependencyPolicy::default(),
                revision: cordis_core::PluginRevision::default(),
            }
        }

        async fn start(&self, _context: Context) -> Result<(), CordisError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[async_trait]
    impl NativePlugin for CountingTaskPlugin {
        fn descriptor(&self) -> PluginDescriptor {
            EmptyPlugin.descriptor()
        }

        async fn start(&self, context: Context) -> Result<(), CordisError> {
            let count = self.0.clone();
            context.spawn(async move {
                count.fetch_add(1, Ordering::SeqCst);
            })?;
            Ok(())
        }
    }

    #[async_trait]
    impl NativePlugin for RetainContextPlugin {
        fn descriptor(&self) -> PluginDescriptor { EmptyPlugin.descriptor() }

        async fn start(&self, context: Context) -> Result<(), CordisError> {
            *self.0.lock() = Some(context);
            Ok(())
        }
    }

    async fn retained_context(runtime: &Runtime) -> (FiberId, Context) {
        let slot = Arc::new(Mutex::new(None));
        let fiber = runtime
            .install(runtime.root(), RetainContextPlugin(slot.clone()))
            .await
            .expect("install");
        let context = slot.lock().clone().expect("context");
        (fiber, context)
    }

    async fn replaced_generation_contexts() -> (Runtime, Context, Context) {
        let runtime = Runtime::new();
        let slot = Arc::new(Mutex::new(None));
        let fiber = runtime
            .install(runtime.root(), RetainContextPlugin(slot.clone()))
            .await
            .expect("initial activation");
        let old = slot.lock().clone().expect("old context");
        runtime.dispose_fiber(fiber, true).await.expect("dependency loss");
        runtime.activate(fiber).await.expect("dependency restart");
        let fresh = slot.lock().clone().expect("fresh context");
        (runtime, old, fresh)
    }

    #[allow(clippy::needless_pass_by_value)]
    fn assert_stale(result: Result<impl Sized, CordisError>, context: &Context) {
        assert!(matches!(
            result,
            Err(CordisError::StaleContextGeneration { fiber, expected, actual })
                if fiber == context.fiber && expected == context.generation.get() && actual != expected
        ));
    }

    enum ContextRaceOperation {
        Spawn,
        Effect,
        Provide,
        EventRegistration,
        InvocationRegistration,
        CreateScope,
    }

    async fn context_restart_race(operation: ContextRaceOperation) {
        let runtime = Runtime::new();
        let (fiber, context) = retained_context(&runtime).await;
        let old_gate = runtime.0.fibers.with(fiber, |record| record.capabilities.clone()).unwrap();
        let hook = Arc::new(std::sync::Barrier::new(2));
        *runtime.0.context_before_gate_hook.lock() = Some(hook.clone());
        let interaction = tokio::task::spawn_blocking(move || match operation {
            ContextRaceOperation::Spawn => context.spawn(async {}).map(|_| ()),
            ContextRaceOperation::Effect => context
                .effect(cordis_core::effect_fn(|| async { Ok(()) })),
            ContextRaceOperation::Provide => context
                .provide(ServiceKey::new("race", "service", 1), 1_u64),
            ContextRaceOperation::EventRegistration => context
                .on(EventKey("race.event".into()), Arc::new(NoopEventHandler))
                .map(|_| ()),
            ContextRaceOperation::InvocationRegistration => context
                .handle_invocation(
                    InvocationKey::new("race", "invocation", 1),
                    Arc::new(NoopInvocationHandler),
                )
                .map(|_| ()),
            ContextRaceOperation::CreateScope => context.create_scope("race-child").map(|_| ()),
        });
        let entered = hook.clone();
        tokio::task::spawn_blocking(move || entered.wait()).await.expect("snapshot pause");
        runtime.dispose_fiber(fiber, true).await.expect("dependency loss");
        runtime.activate(fiber).await.expect("restart");
        runtime.0.context_before_gate_hook.lock().take();
        tokio::task::spawn_blocking(move || hook.wait()).await.expect("resume admission");
        assert!(matches!(
            interaction.await.expect("interaction"),
            Err(CordisError::StaleContextGeneration { .. })
        ));
        assert!(old_gate.try_acquire().is_none());
    }

    #[tokio::test]
    async fn context_creation_and_generation_restart_produce_coherent_generation_bundle() {
        let runtime = Runtime::new();
        let (fiber, context) = retained_context(&runtime).await;
        let cell = runtime.0.fibers.get(fiber).unwrap();
        let record = cell.inner.read();
        assert_eq!(context.generation, record.generation);
        assert_eq!(context.generation, record.capabilities.generation_id());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn stale_context_never_acquires_new_generation_gate() {
        context_restart_race(ContextRaceOperation::Spawn).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn context_spawn_racing_generation_restart_has_single_admission_winner() {
        context_restart_race(ContextRaceOperation::Spawn).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn context_effect_racing_generation_restart_never_attaches_to_new_generation() {
        context_restart_race(ContextRaceOperation::Effect).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn context_provide_racing_generation_restart_never_publishes_into_new_generation() {
        context_restart_race(ContextRaceOperation::Provide).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn context_event_registration_racing_restart_keeps_exact_gate() {
        context_restart_race(ContextRaceOperation::EventRegistration).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn context_invocation_registration_racing_restart_keeps_exact_gate() {
        context_restart_race(ContextRaceOperation::InvocationRegistration).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cancellable_admission_releases_fiber_before_restart_and_keeps_exact_token() {
        let runtime = Runtime::new();
        let (fiber, context) = retained_context(&runtime).await;
        let old_token = runtime
            .0
            .fibers
            .get(fiber)
            .expect("fiber")
            .inner
            .read()
            .cancellation
            .clone();
        let hook = Arc::new(std::sync::Barrier::new(2));
        *runtime.0.context_before_gate_hook.lock() = Some(hook.clone());
        let operation = tokio::spawn(async move { context.sleep(Duration::from_secs(10)).await });
        let entered = hook.clone();
        tokio::task::spawn_blocking(move || entered.wait())
            .await
            .expect("snapshot pause");
        runtime
            .dispose_fiber(fiber, true)
            .await
            .expect("dependency loss while admission hook is paused");
        runtime.activate(fiber).await.expect("restart");
        let new_token = runtime
            .0
            .fibers
            .get(fiber)
            .expect("fiber")
            .inner
            .read()
            .cancellation
            .clone();
        assert!(old_token.is_cancelled());
        assert!(!new_token.is_cancelled());
        runtime.0.context_before_gate_hook.lock().take();
        tokio::task::spawn_blocking(move || hook.wait())
            .await
            .expect("resume admission");
        assert!(matches!(
            operation.await.expect("operation"),
            Err(CordisError::StaleContextGeneration { .. })
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn context_create_scope_racing_generation_restart_has_single_owner_generation() {
        context_restart_race(ContextRaceOperation::CreateScope).await;
    }

    #[tokio::test]
    async fn context_captures_generation_at_creation() {
        let runtime = Runtime::new();
        let (fiber, context) = retained_context(&runtime).await;
        assert_eq!(context.generation, runtime.0.fibers.get(fiber).unwrap().inner.read().generation);
    }

    #[tokio::test]
    async fn context_clone_preserves_generation() {
        let (_runtime, old, _) = replaced_generation_contexts().await;
        assert_eq!(old.clone().generation, old.generation);
    }

    #[tokio::test]
    async fn dependency_restart_invalidates_old_context() {
        let (_runtime, old, fresh) = replaced_generation_contexts().await;
        assert_stale(old.parent(), &old);
        assert!(fresh.parent().is_ok());
        assert_eq!(old.fiber, fresh.fiber);
        assert_ne!(old.generation, fresh.generation);
    }

    #[tokio::test]
    async fn stale_context_cannot_spawn_into_new_generation() {
        let (_runtime, old, _) = replaced_generation_contexts().await;
        assert_stale(old.spawn(async {}), &old);
    }

    #[tokio::test]
    async fn stale_context_cannot_register_effect_in_new_generation() {
        let (_runtime, old, _) = replaced_generation_contexts().await;
        assert_stale(old.effect(cordis_core::effect_fn(|| async { Ok(()) })), &old);
    }

    #[tokio::test]
    async fn stale_context_cannot_provide_service_to_new_generation() {
        let (_runtime, old, _) = replaced_generation_contexts().await;
        assert_stale(old.provide(ServiceKey::new("stale", "service", 1), 1_u64), &old);
    }

    #[tokio::test]
    async fn stale_context_cannot_register_event_in_new_generation() {
        let (_runtime, old, _) = replaced_generation_contexts().await;
        assert_stale(old.on(EventKey("stale.event".into()), Arc::new(NoopEventHandler)), &old);
    }

    #[tokio::test]
    async fn stale_context_cannot_register_invocation_in_new_generation() {
        let (_runtime, old, _) = replaced_generation_contexts().await;
        assert_stale(old.handle_invocation(
            InvocationKey::new("stale", "invoke", 1), Arc::new(NoopInvocationHandler),
        ), &old);
    }

    #[tokio::test]
    async fn stale_context_cannot_create_child_scope_in_new_generation() {
        let (_runtime, old, _) = replaced_generation_contexts().await;
        assert_stale(old.create_scope("stale-child"), &old);
    }

    #[tokio::test]
    async fn stale_context_cannot_invoke_new_generation() {
        let (_runtime, old, _) = replaced_generation_contexts().await;
        assert_stale(old.invoke(
            &InvocationKey::new("stale", "invoke", 1), InvocationValue::native(Arc::new(())),
        ).await, &old);
    }

    #[tokio::test]
    async fn stale_context_cannot_emit_after_generation_replacement() {
        let (_runtime, old, _) = replaced_generation_contexts().await;
        assert_stale(old.emit(&EventKey("stale.emit".into()), Arc::new(()) as EventValue).await, &old);
    }

    #[tokio::test]
    async fn stale_context_cannot_get_service_from_new_generation() {
        let (_runtime, old, _) = replaced_generation_contexts().await;
        assert_stale(old.try_get::<()>(&ServiceKey::new("stale", "get", 1)), &old);
    }

    #[tokio::test]
    async fn staged_context_remains_valid_after_hmr_commit_for_same_generation() {
        let runtime = Runtime::new();
        let old = runtime.install(runtime.root(), EmptyPlugin).await.expect("old");
        let plugin_id = runtime.0.fibers.get(old).expect("old").plugin_id;
        let staging = runtime.create_scope_internal(runtime.root(), "hmr-staging".into(), true)
            .expect("staging");
        runtime.transition(old, FiberState::Reloading).expect("reloading");
        let slot = Arc::new(Mutex::new(None));
        let staged = runtime.install_arc(
            staging, Arc::new(RetainContextPlugin(slot.clone())), true, Some(plugin_id),
        ).await.expect("staged");
        let context = slot.lock().clone().expect("context");
        runtime.commit_staged_revision(old, staged, staging, runtime.root()).expect("commit");
        assert!(context.parent().is_ok());
        assert_eq!(context.generation, runtime.0.fibers.get(staged).unwrap().inner.read().generation);
    }

    #[tokio::test]
    async fn context_scope_tracks_same_generation_hmr_commit() {
        let runtime = Runtime::new();
        let target = runtime.create_scope(runtime.root(), "scope-target").expect("target");
        let old = runtime.install(target, EmptyPlugin).await.expect("old");
        let plugin_id = runtime.0.fibers.get(old).expect("old").plugin_id;
        let staging = runtime.create_scope_internal(target, "scope-staging".into(), true)
            .expect("staging");
        runtime.transition(old, FiberState::Reloading).expect("reloading");
        let slot = Arc::new(Mutex::new(None));
        let staged = runtime.install_arc(
            staging, Arc::new(RetainContextPlugin(slot.clone())), true, Some(plugin_id),
        ).await.expect("staged");
        let context = slot.lock().clone().expect("context");
        let clone = context.clone();
        assert_eq!(context.scope().expect("staging binding"), staging);
        runtime.commit_staged_revision(old, staged, staging, target).expect("commit");
        assert_eq!(context.scope().expect("target binding"), target);
        assert_eq!(clone.scope().expect("clone target binding"), target);
        assert_eq!(context.initial_scope(), staging);
    }

    #[tokio::test]
    async fn retained_staged_context_uses_target_scope_after_full_reload() {
        let runtime = Runtime::new();
        let service = ServiceKey::new("retained", "ancestor", 1);
        runtime.install(runtime.root(), ServiceRevisionPlugin {
            values: vec![(service.clone(), "ancestor")], task_runs: None,
        }).await.expect("ancestor provider");
        let target = runtime.create_scope(runtime.root(), "retained-target").expect("target");
        let old = runtime.install(target, EmptyPlugin).await.expect("old");
        let invocation = InvocationKey::new("retained", "target", 1);
        let slot = Arc::new(Mutex::new(None));
        runtime.reload_detailed(old, RetainInvocationContextPlugin {
            slot: slot.clone(), key: invocation.clone(),
        }).await.expect("full reload");
        let context = slot.lock().clone().expect("retained context");
        let staging = context.initial_scope();
        let _ = runtime.collect_garbage();
        assert!(matches!(runtime.0.scopes.parent(staging), Err(CordisError::ScopeNotFound)));
        assert_eq!(context.scope().expect("current scope"), target);
        assert_eq!(context.parent().expect("parent"), Some(runtime.root()));
        assert_eq!(*context.get::<&'static str>(&service).expect("ancestor service"), "ancestor");
        context.invoke(&invocation, InvocationValue::native(Arc::new(7_u64)))
            .await.expect("target invocation");
        let child = context.create_scope("retained-child").expect("child");
        assert_eq!(runtime.0.scopes.parent(child).expect("child parent"), Some(target));
    }

    #[tokio::test]
    async fn stale_old_generation_context_cannot_follow_new_generation_scope() {
        let (runtime, old, fresh) = replaced_generation_contexts().await;
        assert!(matches!(old.scope(), Err(CordisError::StaleContextGeneration { .. })));
        assert_eq!(fresh.scope().expect("fresh scope"), runtime.root());
    }

    async fn draining_context() -> (Runtime, Context, crate::gate::GenerationLease) {
        let runtime = Runtime::new();
        let (fiber, context) = retained_context(&runtime).await;
        let gate = runtime.0.fibers.with(fiber, |record| record.capabilities.clone()).unwrap();
        let lease = gate.try_acquire().expect("existing execution lease");
        gate.close();
        (runtime, context, lease)
    }

    #[tokio::test]
    async fn old_invocation_context_cannot_spawn_after_hmr_cutover() {
        let (_runtime, context, _lease) = draining_context().await;
        assert!(matches!(context.spawn(async {}), Err(CordisError::StaleContextGeneration { .. })));
    }

    #[tokio::test]
    async fn old_invocation_context_cannot_invoke_after_hmr_cutover() {
        let (_runtime, context, _lease) = draining_context().await;
        assert!(matches!(context.invoke(
            &InvocationKey::new("drain", "invoke", 1), InvocationValue::native(Arc::new(())),
        ).await, Err(CordisError::StaleContextGeneration { .. })));
    }

    #[tokio::test]
    async fn old_invocation_context_cannot_emit_after_hmr_cutover() {
        let (_runtime, context, _lease) = draining_context().await;
        assert!(matches!(context.emit(
            &EventKey("drain.emit".into()), Arc::new(()) as EventValue,
        ).await, Err(CordisError::StaleContextGeneration { .. })));
    }

    #[tokio::test]
    async fn old_handler_may_finish_current_execution_while_context_is_no_longer_admitted() {
        let (_runtime, context, lease) = draining_context().await;
        assert!(matches!(
            context.parent(),
            Err(CordisError::StaleContextGeneration { .. } | CordisError::FiberNotFound)
        ));
        drop(lease);
    }

    #[tokio::test]
    async fn generation_lease_does_not_make_stale_context_valid() {
        let (_runtime, context, _lease) = draining_context().await;
        assert!(matches!(context.root(), Err(CordisError::StaleContextGeneration { .. })));
    }

    #[tokio::test]
    async fn same_fiber_id_new_generation_rejects_old_context() {
        let (_runtime, old, fresh) = replaced_generation_contexts().await;
        assert_eq!(old.fiber, fresh.fiber);
        assert_stale(old.parent(), &old);
    }

    #[tokio::test]
    async fn fresh_context_after_dependency_restart_is_valid() {
        let (_runtime, _, fresh) = replaced_generation_contexts().await;
        assert!(fresh.parent().is_ok());
    }

    #[tokio::test]
    async fn old_and_new_contexts_are_distinguished_only_by_generation_identity() {
        let (_runtime, old, fresh) = replaced_generation_contexts().await;
        assert_eq!(old.fiber, fresh.fiber);
        assert_eq!(old.initial_scope(), fresh.initial_scope());
        assert_ne!(old.generation, fresh.generation);
    }

    #[tokio::test]
    async fn old_hmr_context_becomes_stale_after_selector_cutover() {
        let runtime = Runtime::new();
        let (_, context) = retained_context(&runtime).await;
        runtime.reload(context.fiber, EmptyPlugin).await.expect("reload");
        assert!(matches!(
            context.parent(),
            Err(CordisError::StaleContextGeneration { .. } | CordisError::FiberNotFound)
        ));
    }

    #[tokio::test]
    async fn new_staged_context_becomes_normal_valid_context_after_commit() {
        let (_runtime, _, fresh) = replaced_generation_contexts().await;
        assert!(fresh.root().is_ok());
    }

    #[tokio::test]
    async fn old_context_never_becomes_valid_again() {
        let (_runtime, old, fresh) = replaced_generation_contexts().await;
        assert_stale(old.parent(), &old);
        assert!(fresh.parent().is_ok());
        assert_stale(old.parent(), &old);
    }

    #[tokio::test]
    async fn generation_id_is_never_reused_to_revalidate_old_context() {
        let (_runtime, old, fresh) = replaced_generation_contexts().await;
        assert!(fresh.generation.get() > old.generation.get());
        assert_stale(old.parent(), &old);
    }

    #[tokio::test]
    async fn old_invocation_with_existing_generation_lease_cannot_spawn_after_cutover() {
        let (_runtime, context, _lease) = draining_context().await;
        assert!(matches!(context.spawn(async {}), Err(CordisError::StaleContextGeneration { .. })));
    }

    #[tokio::test]
    async fn old_invocation_with_existing_generation_lease_cannot_invoke_after_cutover() {
        let (_runtime, context, _lease) = draining_context().await;
        assert!(matches!(context.invoke(
            &InvocationKey::new("recursive", "invoke", 1), InvocationValue::native(Arc::new(())),
        ).await, Err(CordisError::StaleContextGeneration { .. })));
    }

    #[tokio::test]
    async fn old_invocation_with_existing_generation_lease_cannot_emit_after_cutover() {
        let (_runtime, context, _lease) = draining_context().await;
        assert!(matches!(context.emit(
            &EventKey("recursive.emit".into()), Arc::new(()) as EventValue,
        ).await, Err(CordisError::StaleContextGeneration { .. })));
    }

    #[tokio::test]
    async fn stale_context_waiting_for_invocation_permit_cannot_invoke_after_restart() {
        let config = RuntimeConfig {
            max_concurrent_invocations: 1,
            ..RuntimeConfig::default()
        };
        let runtime = Runtime::with_config(config).expect("runtime");
        let slot = Arc::new(Mutex::new(None));
        let fiber = runtime.install(
            runtime.root(), RetainContextPlugin(slot.clone()),
        ).await.expect("initial");
        let old = slot.lock().clone().expect("old");
        let permit = runtime.0.invocation_permits.acquire().await.expect("permit");
        let queued = tokio::spawn({
            let old = old.clone();
            async move { old.invoke(
                &InvocationKey::new("queued", "restart", 1),
                InvocationValue::native(Arc::new(())),
            ).await }
        });
        tokio::task::yield_now().await;
        runtime.dispose_fiber(fiber, true).await.expect("loss");
        runtime.activate(fiber).await.expect("restart");
        drop(permit);
        assert!(matches!(
            queued.await.expect("queued"),
            Err(CordisError::InvocationCancelled | CordisError::StaleContextGeneration { .. })
        ));
    }

    #[tokio::test]
    async fn rolled_back_staged_context_never_becomes_valid() {
        let runtime = Runtime::new();
        let old = runtime.install(runtime.root(), EmptyPlugin).await.expect("old");
        runtime.transition(old, FiberState::Reloading).expect("reloading");
        let plugin_id = runtime.0.fibers.get(old).unwrap().plugin_id;
        let staging = runtime.create_scope_internal(runtime.root(), "rollback-stage".into(), true)
            .expect("staging");
        let slot = Arc::new(Mutex::new(None));
        let staged = runtime.install_arc(
            staging, Arc::new(RetainContextPlugin(slot.clone())), true, Some(plugin_id),
        ).await.expect("staged");
        let context = slot.lock().clone().expect("context");
        let _ = runtime.rollback_reload(
            old, Some(staged), Some(staging), CordisError::RevisionValidationFailed("test".into()),
        ).await;
        assert!(matches!(
            context.parent(),
            Err(CordisError::StaleContextGeneration { .. } | CordisError::FiberNotFound)
        ));
    }

    #[tokio::test]
    async fn reload_restart_context_churn_converges() {
        let runtime = Runtime::new();
        let slot = Arc::new(Mutex::new(None));
        let fiber = runtime.install(runtime.root(), RetainContextPlugin(slot.clone()))
            .await.expect("initial");
        for _ in 0..25 {
            let old = slot.lock().clone().expect("old");
            old.spawn(async {}).expect("fresh work");
            runtime.dispose_fiber(fiber, true).await.expect("loss");
            runtime.activate(fiber).await.expect("restart");
            assert!(matches!(old.parent(), Err(CordisError::StaleContextGeneration { .. })));
            slot.lock().clone().expect("new").parent().expect("new valid");
        }
        wait_for_no_live_tasks(&runtime).await;
        assert_eq!(runtime.snapshot().provider_inflight, 0);
    }

    async fn wait_for_no_live_tasks(runtime: &Runtime) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while runtime.0.tasks.live_fiber_tasks() != 0 { tokio::task::yield_now().await; }
        }).await.expect("task reap");
    }

    async fn wait_for_no_runtime_workers(runtime: &Runtime) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while runtime.0.workers.live() != 0 { tokio::task::yield_now().await; }
        }).await.expect("runtime worker reap");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dropping_install_caller_during_start_does_not_orphan_operation() {
        let runtime = Runtime::new();
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let observer = tokio::spawn({
            let runtime = runtime.clone();
            let entered = entered.clone();
            let release = release.clone();
            async move { runtime.install(runtime.root(), ReloadGatePlugin {
                entered, release, fail: false,
            }).await }
        });
        entered.notified().await;
        observer.abort();
        release.add_permits(1);
        wait_for_no_runtime_workers(&runtime).await;
        let snapshot = runtime.snapshot();
        assert_eq!(snapshot.fibers.iter().filter(|fiber| fiber.state == FiberState::Active).count(), 1);
        assert_eq!(snapshot.staging_fibers, 0);
        assert_eq!(snapshot.provider_inflight, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dropping_install_caller_during_failure_fully_rolls_back() {
        let runtime = Runtime::new();
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let observer = tokio::spawn({
            let runtime = runtime.clone();
            let entered = entered.clone();
            let release = release.clone();
            async move { runtime.install(runtime.root(), ReloadGatePlugin {
                entered, release, fail: true,
            }).await }
        });
        entered.notified().await;
        observer.abort();
        release.add_permits(1);
        wait_for_no_runtime_workers(&runtime).await;
        let _ = runtime.collect_garbage();
        let snapshot = runtime.snapshot();
        assert_eq!(snapshot.fibers.len(), 0);
        assert_eq!(snapshot.service_count, 0);
        assert_eq!(snapshot.handler_count, 0);
        assert_eq!(snapshot.provider_inflight, 0);
        assert_eq!(runtime.0.plugins.total_fiber_count(), 0);
    }

    #[tokio::test]
    async fn completed_task_is_reaped_without_disposal() {
        let runtime = Runtime::new();
        let (fiber, context) = retained_context(&runtime).await;
        let completed = Arc::new(AtomicU64::new(0));
        let marker = completed.clone();
        context.spawn(async move { marker.fetch_add(1, Ordering::SeqCst); }).expect("spawn");
        wait_for_no_live_tasks(&runtime).await;
        assert_eq!(completed.load(Ordering::SeqCst), 1);
        assert!(runtime
            .0
            .fibers
            .get(fiber)
            .is_none_or(|cell| cell.inner.read().tasks.is_empty()));
    }

    #[tokio::test]
    async fn many_sequential_tasks_do_not_exhaust_live_quota() {
        let config = RuntimeConfig { max_tasks_per_fiber: 4, ..RuntimeConfig::default() };
        let runtime = Runtime::with_config(config).expect("runtime");
        let (_, context) = retained_context(&runtime).await;
        for _ in 0..100 {
            context.spawn(async {}).expect("live quota, not lifetime quota");
            wait_for_no_live_tasks(&runtime).await;
        }
        assert_eq!(runtime.0.tasks.reaped(), 100);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_spawn_respects_task_quota() {
        let config = RuntimeConfig { max_tasks_per_fiber: 4, ..RuntimeConfig::default() };
        let runtime = Runtime::with_config(config).expect("runtime");
        let (_, context) = retained_context(&runtime).await;
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let mut accepted = 0;
        for _ in 0..16 {
            let release = release.clone();
            if context.spawn(async move { let _ = release.acquire().await; }).is_ok() { accepted += 1; }
        }
        assert_eq!(accepted, 4);
        release.add_permits(accepted);
        wait_for_no_live_tasks(&runtime).await;
    }

    #[tokio::test]
    async fn task_panic_is_observed_and_reaped() {
        let runtime = Runtime::new();
        let (_, context) = retained_context(&runtime).await;
        context.spawn(async { panic!("expected task panic"); }).expect("spawn");
        wait_for_no_live_tasks(&runtime).await;
        assert_eq!(runtime.0.tasks.panicked(), 1);
        assert!(runtime.health().recent_errors.iter().any(|issue| issue.kind == HealthIssueKind::TaskPanic));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn task_completion_racing_disposal_is_reaped_exactly_once() {
        let runtime = Runtime::new();
        let (fiber, context) = retained_context(&runtime).await;
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let task_release = release.clone();
        context.spawn(async move { let _ = task_release.acquire().await; }).expect("spawn");
        let before = runtime.0.tasks.reaped();
        let dispose_runtime = runtime.clone();
        let disposal = tokio::spawn(async move { dispose_runtime.dispose_fiber(fiber, false).await });
        release.add_permits(1);
        disposal.await.expect("join").expect("dispose");
        assert_eq!(runtime.0.tasks.reaped() - before, 1);
        assert_eq!(runtime.0.tasks.live_fiber_tasks(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn disposal_owned_task_panic_keeps_fiber_attribution() {
        let runtime = Runtime::new();
        let (fiber, context) = retained_context(&runtime).await;
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let panicked = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let task_release = release.clone();
        let task_panicked = panicked.clone();
        let task = context.spawn(async move {
            let _ = task_release.acquire().await;
            task_panicked.store(true, Ordering::SeqCst);
            panic!("disposal-owned panic");
        }).expect("spawn");
        runtime.0.tasks.enable_take_hook();
        let tasks = runtime.0.tasks.clone();
        let disposal = tokio::spawn(async move { tasks.cancel_all(vec![task], Duration::from_secs(1)).await });
        runtime.0.tasks.wait_for_take_hook().await;
        release.add_permits(1);
        tokio::time::timeout(Duration::from_secs(1), async {
            while !panicked.load(Ordering::SeqCst) { tokio::task::yield_now().await; }
        }).await.expect("task panic");
        runtime.0.tasks.release_take_hook();
        let _ = disposal.await.expect("join");
        assert!(runtime.health().recent_errors.iter().any(|issue| {
            issue.kind == HealthIssueKind::TaskPanic && issue.fiber == Some(fiber)
        }));
        assert_eq!(runtime.0.tasks.panicked(), 1);
        assert_eq!(runtime.0.tasks.aborted(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn deadline_boundary_completed_task_is_not_misclassified_as_aborted() {
        let runtime = Runtime::new();
        let (_, context) = retained_context(&runtime).await;
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let finished = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let task_release = release.clone();
        let task_finished = finished.clone();
        let task = context.spawn(async move {
            let _ = task_release.acquire().await;
            task_finished.store(true, Ordering::SeqCst);
        }).expect("spawn");
        runtime.0.tasks.enable_take_hook();
        let tasks = runtime.0.tasks.clone();
        let disposal = tokio::spawn(async move { tasks.cancel_all(vec![task], Duration::from_secs(1)).await });
        runtime.0.tasks.wait_for_take_hook().await;
        release.add_permits(1);
        tokio::time::timeout(Duration::from_secs(1), async {
            while !finished.load(Ordering::SeqCst) { tokio::task::yield_now().await; }
        }).await.expect("task completion");
        runtime.0.tasks.release_take_hook();
        disposal.await.expect("join").expect("dispose");
        assert_eq!(runtime.0.tasks.completed(), 1);
        assert_eq!(runtime.0.tasks.aborted(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn spawn_racing_disposal_has_single_winner() {
        let runtime = Runtime::new();
        let (fiber, context) = retained_context(&runtime).await;
        let barrier = Arc::new(tokio::sync::Barrier::new(3));
        let spawn_barrier = barrier.clone();
        let spawning = tokio::spawn(async move { spawn_barrier.wait().await; context.spawn(async {}) });
        let dispose_runtime = runtime.clone();
        let dispose_barrier = barrier.clone();
        let disposing = tokio::spawn(async move {
            dispose_barrier.wait().await;
            dispose_runtime.dispose_fiber(fiber, false).await
        });
        barrier.wait().await;
        let _ = spawning.await.expect("spawn join");
        disposing.await.expect("dispose join").expect("dispose");
        assert_eq!(runtime.0.tasks.live_fiber_tasks(), 0);
        assert!(runtime
            .0
            .fibers
            .get(fiber)
            .is_none_or(|cell| cell.inner.read().tasks.is_empty()));
    }

    #[tokio::test]
    async fn long_running_task_churn_converges() {
        let config = RuntimeConfig { max_tasks_per_fiber: 4, ..RuntimeConfig::default() };
        let runtime = Runtime::with_config(config).expect("runtime");
        let (_, context) = retained_context(&runtime).await;
        for _ in 0..10_000 {
            context.spawn(async {}).expect("spawn");
            wait_for_no_live_tasks(&runtime).await;
        }
        assert_eq!(runtime.0.tasks.live_fiber_tasks(), 0);
        assert_eq!(runtime.0.tasks.reaped(), 10_000);
    }

    #[tokio::test]
    async fn runtime_worker_is_tracked_before_execution_and_reaped() {
        let runtime = Runtime::new();
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let worker_release = release.clone();
        runtime.0.workers.spawn(RuntimeWorkerKind::DependencyReconcile, false, async move {
            let _ = worker_release.acquire().await;
            Ok(())
        });
        assert_eq!(runtime.0.workers.live(), 1);
        release.add_permits(1);
        wait_for_no_runtime_workers(&runtime).await;
        assert_eq!(runtime.0.workers.reaped(), 1);
    }

    #[tokio::test]
    async fn runtime_worker_panic_and_error_are_observed() {
        let runtime = Runtime::new();
        runtime.0.workers.spawn(RuntimeWorkerKind::DependencyReconcile, false, async {
            panic!("expected worker panic");
        });
        runtime.0.workers.spawn(RuntimeWorkerKind::DependencyReconcile, false, async {
            Err(CordisError::Invariant("expected worker error".into()))
        });
        wait_for_no_runtime_workers(&runtime).await;
        assert_eq!(runtime.0.workers.panicked(), 1);
        assert_eq!(runtime.0.workers.errors(), 1);
        let issues = runtime.health().recent_errors;
        assert!(issues.iter().any(|issue| issue.kind == HealthIssueKind::RuntimeWorkerPanic));
        assert!(issues.iter().any(|issue| issue.kind == HealthIssueKind::RuntimeWorkerError));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn duplicate_reconcile_requests_are_coalesced() {
        let runtime = Runtime::new();
        let fiber = runtime.install(runtime.root(), CountingStartPlugin(Arc::new(AtomicU64::new(0))))
            .await.expect("waiting fiber");
        let cell = runtime.0.fibers.get(fiber).expect("fiber");
        let guard = cell.lifecycle.clone().lock_owned().await;
        for _ in 0..100 { runtime.schedule_activation(fiber); }
        assert_eq!(runtime.0.workers.live(), 1);
        assert_eq!(runtime.0.pending_activations.lock().len(), 1);
        drop(guard);
        wait_for_no_runtime_workers(&runtime).await;
        assert!(runtime.0.pending_activations.lock().is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shutdown_waits_for_cleanup_runtime_workers() {
        let runtime = Runtime::new();
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let entered = Arc::new(tokio::sync::Notify::new());
        let worker_release = release.clone();
        let worker_entered = entered.clone();
        runtime.0.workers.spawn(RuntimeWorkerKind::ActivationRollback, true, async move {
            worker_entered.notify_one();
            let _ = worker_release.acquire().await;
            Ok(())
        });
        entered.notified().await;
        let shutdown_runtime = runtime.clone();
        let shutdown = tokio::spawn(async move { shutdown_runtime.shutdown().await });
        tokio::task::yield_now().await;
        assert!(!shutdown.is_finished());
        release.add_permits(1);
        shutdown.await.expect("join").expect("shutdown");
        assert_eq!(runtime.0.workers.live(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn runtime_worker_panic_and_error_during_shutdown_are_diagnosed_once() {
        let runtime = Runtime::new();
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let panic_release = release.clone();
        runtime.0.workers.spawn(RuntimeWorkerKind::ActivationRollback, true, async move {
            let _ = panic_release.acquire().await;
            panic!("shutdown worker panic");
        });
        let error_release = release.clone();
        runtime.0.workers.spawn(RuntimeWorkerKind::ActivationRollback, true, async move {
            let _ = error_release.acquire().await;
            Err(CordisError::Invariant("shutdown worker error".into()))
        });
        let shutdown_runtime = runtime.clone();
        let shutdown = tokio::spawn(async move { shutdown_runtime.shutdown().await });
        tokio::task::yield_now().await;
        release.add_permits(2);
        shutdown.await.expect("join").expect("shutdown");
        assert_eq!(runtime.0.workers.panicked(), 1);
        assert_eq!(runtime.0.workers.errors(), 1);
        assert!(runtime.0.workers.reaped() >= 2);
        let issues = runtime.health().recent_errors;
        assert_eq!(issues.iter().filter(|issue| issue.kind == HealthIssueKind::RuntimeWorkerPanic).count(), 1);
        assert_eq!(issues.iter().filter(|issue| issue.kind == HealthIssueKind::RuntimeWorkerError).count(), 1);
    }

    #[async_trait]
    impl NativePlugin for ServiceRevisionPlugin {
        fn descriptor(&self) -> PluginDescriptor {
            PluginDescriptor {
                name: "service-revision".into(),
                dependencies: Arc::new([]),
                provisions: self.values.iter().map(|(key, _)| key.clone()).collect(),
                dependency_policy: DependencyPolicy::default(),
                revision: cordis_core::PluginRevision::default(),
            }
        }

        async fn start(&self, context: Context) -> Result<(), CordisError> {
            for (key, value) in &self.values {
                context.provide(key.clone(), *value)?;
            }
            if let Some(task_runs) = &self.task_runs {
                let task_runs = task_runs.clone();
                context.spawn(async move {
                    task_runs.fetch_add(1, Ordering::SeqCst);
                })?;
            }
            Ok(())
        }
    }

    #[async_trait]
    impl NativePlugin for LeasedServicePlugin {
        fn descriptor(&self) -> PluginDescriptor {
            PluginDescriptor {
                name: "leased-service".into(),
                dependencies: Arc::new([]),
                provisions: Arc::new([self.key.clone()]),
                dependency_policy: DependencyPolicy::default(),
                revision: cordis_core::PluginRevision::default(),
            }
        }

        async fn start(&self, context: Context) -> Result<(), CordisError> {
            context.provide(self.key.clone(), self.value)?;
            let cleaned = self.cleaned.clone();
            context.effect(cordis_core::effect_fn(move || async move {
                cleaned.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }))
        }
    }

    #[async_trait]
    impl NativePlugin for TraitServicePlugin {
        fn descriptor(&self) -> PluginDescriptor {
            PluginDescriptor {
                name: "trait-service".into(),
                dependencies: Arc::new([]),
                provisions: Arc::new([self.0.clone()]),
                dependency_policy: DependencyPolicy::default(),
                revision: cordis_core::PluginRevision::default(),
            }
        }

        async fn start(&self, context: Context) -> Result<(), CordisError> {
            let service: Arc<dyn DynTestService> = Arc::new(DynTestServiceImpl);
            context.provide_arc(self.0.clone(), service)
        }
    }

    #[tokio::test]
    async fn concurrent_activate_has_single_winner() {
        let runtime = Runtime::new();
        let starts = Arc::new(AtomicU64::new(0));
        let fiber = runtime
            .install(runtime.root(), CountingStartPlugin(starts.clone()))
            .await
            .expect("waiting fiber");
        let (first, second) = tokio::join!(runtime.activate(fiber), runtime.activate(fiber));
        assert!(first.is_ok() ^ second.is_ok());
        assert_eq!(starts.load(Ordering::SeqCst), 1);
        assert_eq!(
            runtime.snapshot().fibers.into_iter().find(|item| item.id == fiber).expect("fiber").state,
            FiberState::Active
        );
    }

    #[tokio::test]
    async fn topology_is_sealed_before_commit() {
        let runtime = Runtime::new();
        let fiber = runtime
            .install(runtime.root(), CountingStartPlugin(Arc::new(AtomicU64::new(0))))
            .await
            .expect("waiting fiber");
        {
            let cell = runtime.0.fibers.get(fiber).expect("fiber");
            let mut record = cell.inner.write();
            record.state = FiberState::Starting;
            record.activation_sealed = true;
        }
        let context = Context {
            runtime: Arc::downgrade(&runtime.0),
            scope: runtime.root(),
            fiber,
            owner: Arc::downgrade(&runtime.0.fibers.get(fiber).expect("fiber")),
            generation: runtime.0.fibers.get(fiber).expect("fiber").inner.read().generation,
        };
        assert!(matches!(
            context.effect(cordis_core::effect_fn(|| async { Ok(()) })),
            Err(CordisError::ActivationSealed(id)) if id == fiber
        ));
    }

    #[tokio::test]
    async fn activation_ok_implies_capability_published() {
        let runtime = Runtime::new();
        let fiber = runtime
            .install(runtime.root(), EmptyPlugin)
            .await
            .expect("activation");
        let cell = runtime.0.fibers.get(fiber).expect("fiber");
        let record = cell.inner.read();
        assert_eq!(record.state, FiberState::Active);
        assert!(record.capabilities.is_visible());
    }

    #[test]
    fn service_interner_is_stable_and_bounded_without_id_reuse() {
        let runtime = Runtime::with_config(RuntimeConfig {
            max_interned_symbols: 2,
            ..RuntimeConfig::default()
        })
        .expect("runtime");
        let first = ServiceKey::new("bound", "first", 1);
        let second = ServiceKey::new("bound", "second", 1);
        let rejected = ServiceKey::new("bound", "rejected", 1);
        let first_symbol = runtime.intern_service(&first).expect("first");
        assert_eq!(runtime.intern_service(&first).expect("stable"), first_symbol);
        runtime.intern_service(&second).expect("second");
        assert_eq!(runtime.0.services.symbol_count(), 2);
        assert!(matches!(
            runtime.intern_service(&rejected),
            Err(CordisError::ResourceLimitExceeded {
                resource: ResourceKind::ServiceSymbols,
                limit: 2
            })
        ));
        assert_eq!(runtime.intern_service(&first).expect("existing remains"), first_symbol);
    }

    fn characterization_number(name: &str, default: usize) -> usize {
        std::env::var(name)
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(default)
    }

    fn characterize_primitive<T: Sync>(
        resources: &[T],
        shared: bool,
        workers: usize,
        operations: usize,
        operation: impl Fn(&T) + Sync,
    ) -> u128 {
        let ready = Arc::new(Barrier::new(workers + 1));
        let start = Arc::new(Barrier::new(workers + 1));
        let finish = Arc::new(Barrier::new(workers + 1));
        let mut elapsed = 0;
        std::thread::scope(|threads| {
            for worker in 0..workers {
                let resource = &resources[if shared { 0 } else { worker }];
                let ready = ready.clone();
                let start = start.clone();
                let finish = finish.clone();
                let operation = &operation;
                threads.spawn(move || {
                    ready.wait();
                    start.wait();
                    for _ in 0..operations {
                        operation(resource);
                    }
                    finish.wait();
                });
            }
            ready.wait();
            let timer = StdInstant::now();
            start.wait();
            finish.wait();
            elapsed = timer.elapsed().as_nanos();
        });
        elapsed
    }

    #[derive(Clone)]
    struct AdmissionShadowData {
        scope: usize,
        state: u8,
        generation: usize,
        gate: Arc<()>,
        cancellation: tokio_util::sync::CancellationToken,
    }

    #[derive(Clone, Copy)]
    struct ScalarAdmissionData {
        scope: usize,
        state: u8,
        generation: usize,
        gate_id: usize,
        cancellation_id: usize,
    }

    struct BorrowGateData {
        scope: usize,
        gate: Arc<CapabilityGate>,
    }

    impl Default for ScalarAdmissionData {
        fn default() -> Self {
            Self {
                scope: 1,
                state: 1,
                generation: 1,
                gate_id: 1,
                cancellation_id: 1,
            }
        }
    }

    impl Default for AdmissionShadowData {
        fn default() -> Self {
            Self {
                scope: 1,
                state: 1,
                generation: 1,
                gate: Arc::new(()),
                cancellation: tokio_util::sync::CancellationToken::new(),
            }
        }
    }

    fn read_admission_shadow(data: &AdmissionShadowData) {
        std::hint::black_box(data.generation == 1);
        std::hint::black_box(data.state);
        std::hint::black_box(data.scope);
        std::hint::black_box(data.gate.clone());
        std::hint::black_box(data.cancellation.clone());
    }

    fn read_scalar_admission(data: ScalarAdmissionData) {
        std::hint::black_box(data.scope);
        std::hint::black_box(data.state);
        std::hint::black_box(data.generation);
        std::hint::black_box(data.gate_id);
        std::hint::black_box(data.cancellation_id);
    }

    fn read_admission_layer(data: &AdmissionShadowData, gate: bool, cancellation: bool) {
        std::hint::black_box(data.generation == 1);
        std::hint::black_box(data.state);
        std::hint::black_box(data.scope);
        if gate {
            std::hint::black_box(data.gate.clone());
        }
        if cancellation {
            std::hint::black_box(data.cancellation.clone());
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "manual performance characterization"]
    #[allow(clippy::cast_precision_loss, clippy::too_many_lines)]
    async fn generation_admission_primitive_characterization() {
        let case = std::env::var("CORDIS_PERF_CASE").expect("CORDIS_PERF_CASE");
        let workers = characterization_number("CORDIS_PERF_WORKERS", 1);
        let operations = characterization_number("CORDIS_PERF_OPERATIONS", 1_000_000);
        let elapsed = match case.as_str() {
            "ge_shared_normal" | "ge_independent_normal" => {
                let shared = case == "ge_shared_normal";
                let count = if shared { 1 } else { workers };
                let resources: Vec<_> = (0..count)
                    .map(|_| Arc::new(GenerationExecution::default()))
                    .collect();
                characterize_primitive(&resources, shared, workers, operations, |execution| {
                    let lease = execution.try_acquire().expect("lease");
                    std::hint::black_box(&lease);
                    drop(lease);
                })
            }
            "ge_shared_service" | "ge_independent_service" => {
                let shared = case == "ge_shared_service";
                let count = if shared { 1 } else { workers };
                let resources: Vec<_> = (0..count)
                    .map(|_| Arc::new(CapabilityGate::published()))
                    .collect();
                let elapsed = characterize_primitive(
                    &resources,
                    shared,
                    workers,
                    operations,
                    |gate| {
                        let lease = gate.try_acquire_service().expect("service lease");
                        std::hint::black_box(&lease);
                        drop(lease);
                    },
                );
                for gate in resources {
                    assert_eq!(gate.execution_snapshot().1, 0);
                    assert_eq!(gate.service_handle_inflight(), 0);
                }
                elapsed
            }
            "fc_shared" | "fc_independent" => {
                let shared = case == "fc_shared";
                let count = if shared { 1 } else { workers };
                let runtime = Runtime::new();
                let mut resources = Vec::with_capacity(count);
                for _ in 0..count {
                    let (fiber, _) = retained_context(&runtime).await;
                    resources.push(runtime.0.fibers.get(fiber).expect("fiber"));
                }
                characterize_primitive(&resources, shared, workers, operations, |cell| {
                    let fiber = cell.inner.read();
                    std::hint::black_box(fiber.generation);
                    std::hint::black_box(fiber.state);
                    std::hint::black_box(fiber.scope);
                    let gate = fiber.capabilities.clone();
                    let cancellation = fiber.cancellation.clone();
                    drop(fiber);
                    std::hint::black_box(gate);
                    std::hint::black_box(cancellation);
                })
            }
            "ctx_shared" | "ctx_independent" => {
                SKIP_CONTEXT_BEFORE_GATE_HOOK.store(true, Ordering::Relaxed);
                let shared = case == "ctx_shared";
                let count = if shared { 1 } else { workers };
                let runtime = Runtime::new();
                let mut resources = Vec::with_capacity(count);
                for _ in 0..count {
                    resources.push(retained_context(&runtime).await.1);
                }
                let elapsed = characterize_primitive(&resources, shared, workers, operations, |context| {
                    std::hint::black_box(context.scope().expect("scope"));
                });
                SKIP_CONTEXT_BEFORE_GATE_HOOK.store(false, Ordering::Relaxed);
                elapsed
            }
            case if case.starts_with("admission_") => {
                SKIP_CONTEXT_BEFORE_GATE_HOOK.store(true, Ordering::Relaxed);
                let independent = case.ends_with("independent");
                let count = if independent { workers } else { 1 };
                let runtime = Runtime::new();
                let mut resources = Vec::with_capacity(count);
                for _ in 0..count {
                    resources.push(retained_context(&runtime).await.1);
                }
                let elapsed = characterize_primitive(
                    &resources,
                    !independent,
                    workers,
                    operations,
                    |context| match case {
                        "admission_base_shared" | "admission_base_independent" => {
                            context.characterize_base_admission().expect("base admission");
                        }
                        "admission_registration_shared" | "admission_registration_independent" => {
                            context
                                .characterize_registration_admission()
                                .expect("registration admission");
                        }
                        "admission_cancellable_shared" | "admission_cancellable_independent" => {
                            context
                                .characterize_cancellable_admission()
                                .expect("cancellable admission");
                        }
                        _ => panic!("unknown admission case: {case}"),
                    },
                );
                SKIP_CONTEXT_BEFORE_GATE_HOOK.store(false, Ordering::Relaxed);
                elapsed
            }
            "shadow_mutex_shared" | "shadow_mutex_independent" => {
                let shared = case == "shadow_mutex_shared";
                let count = if shared { 1 } else { workers };
                let resources: Vec<_> = (0..count)
                    .map(|_| parking_lot::Mutex::new(AdmissionShadowData::default()))
                    .collect();
                characterize_primitive(&resources, shared, workers, operations, |shadow| {
                    read_admission_shadow(&shadow.lock());
                })
            }
            "shadow_rwlock_shared" | "shadow_rwlock_independent" => {
                let shared = case == "shadow_rwlock_shared";
                let count = if shared { 1 } else { workers };
                let resources: Vec<_> = (0..count)
                    .map(|_| ParkingRwLock::new(AdmissionShadowData::default()))
                    .collect();
                characterize_primitive(&resources, shared, workers, operations, |shadow| {
                    read_admission_shadow(&shadow.read());
                })
            }
            case if case.starts_with("scalar_mutex_") => {
                let shared = case.ends_with("shared") && !case.ends_with("independent");
                let count = if shared { 1 } else { workers };
                let resources: Vec<_> = (0..count)
                    .map(|_| parking_lot::Mutex::new(ScalarAdmissionData::default()))
                    .collect();
                characterize_primitive(&resources, shared, workers, operations, |shadow| {
                    read_scalar_admission(*shadow.lock());
                })
            }
            case if case.starts_with("scalar_rwlock_") => {
                let shared = case.ends_with("shared") && !case.ends_with("independent");
                let count = if shared { 1 } else { workers };
                let resources: Vec<_> = (0..count)
                    .map(|_| ParkingRwLock::new(ScalarAdmissionData::default()))
                    .collect();
                characterize_primitive(&resources, shared, workers, operations, |shadow| {
                    read_scalar_admission(*shadow.read());
                })
            }
            "arc_unit_shared" | "arc_unit_independent" => {
                let shared = case == "arc_unit_shared";
                let count = if shared { 1 } else { workers };
                let resources: Vec<_> = (0..count).map(|_| Arc::new(())).collect();
                characterize_primitive(&resources, shared, workers, operations, |value| {
                    std::hint::black_box(value.clone());
                })
            }
            "arc_gate_shared" | "arc_gate_independent" => {
                let shared = case == "arc_gate_shared";
                let count = if shared { 1 } else { workers };
                let resources: Vec<_> = (0..count)
                    .map(|_| Arc::new(CapabilityGate::published()))
                    .collect();
                characterize_primitive(&resources, shared, workers, operations, |gate| {
                    std::hint::black_box(gate.clone());
                })
            }
            "cancel_shared" | "cancel_independent" => {
                let shared = case == "cancel_shared";
                let count = if shared { 1 } else { workers };
                let resources: Vec<_> = (0..count)
                    .map(|_| tokio_util::sync::CancellationToken::new())
                    .collect();
                characterize_primitive(&resources, shared, workers, operations, |token| {
                    std::hint::black_box(token.clone());
                })
            }
            "both_shared" | "both_independent" => {
                let shared = case == "both_shared";
                let count = if shared { 1 } else { workers };
                let resources: Vec<_> = (0..count).map(|_| AdmissionShadowData::default()).collect();
                characterize_primitive(&resources, shared, workers, operations, |data| {
                    std::hint::black_box(data.gate.clone());
                    std::hint::black_box(data.cancellation.clone());
                })
            }
            case if case.starts_with("layer_mutex_") => {
                let (gate, cancellation) = match case {
                    "layer_mutex_scalar" => (false, false),
                    "layer_mutex_gate" => (true, false),
                    "layer_mutex_cancel" => (false, true),
                    "layer_mutex_full" => (true, true),
                    _ => unreachable!(),
                };
                let resources = [parking_lot::Mutex::new(AdmissionShadowData::default())];
                characterize_primitive(&resources, true, workers, operations, |shadow| {
                    read_admission_layer(&shadow.lock(), gate, cancellation);
                })
            }
            case if case.starts_with("layer_rwlock_") => {
                let (gate, cancellation) = match case {
                    "layer_rwlock_scalar" => (false, false),
                    "layer_rwlock_gate" => (true, false),
                    "layer_rwlock_cancel" => (false, true),
                    "layer_rwlock_full" => (true, true),
                    _ => unreachable!(),
                };
                let resources = [ParkingRwLock::new(AdmissionShadowData::default())];
                characterize_primitive(&resources, true, workers, operations, |shadow| {
                    read_admission_layer(&shadow.read(), gate, cancellation);
                })
            }
            "rwlock_borrow_gate" | "rwlock_clone_gate_lease" => {
                let resources = [ParkingRwLock::new(BorrowGateData {
                    scope: 1,
                    gate: Arc::new(CapabilityGate::published()),
                })];
                characterize_primitive(&resources, true, workers, operations, |shadow| {
                    let (scope, gate) = {
                        let data = shadow.read();
                        let gate = (case == "rwlock_clone_gate_lease").then(|| data.gate.clone());
                        if gate.is_none() {
                            let lease = data.gate.try_acquire().expect("lease");
                            std::hint::black_box(&lease);
                            drop(lease);
                        }
                        (data.scope, gate)
                    };
                    let lease = gate.as_ref().map(|gate| gate.try_acquire().expect("lease"));
                    std::hint::black_box(scope);
                    drop(lease);
                })
            }
            case if matches!(case, "c0_owner" | "c1_snapshot" | "c2_gate" | "c3_cancel" | "c4_lease") => {
                let runtime = Runtime::new();
                let (_, context) = retained_context(&runtime).await;
                let resources = [context];
                characterize_primitive(&resources, true, workers, operations, |context| {
                    let owner = context.owner.upgrade().expect("owner");
                    if case == "c0_owner" {
                        std::hint::black_box(&owner);
                        return;
                    }
                    let fiber = owner.inner.read();
                    std::hint::black_box(fiber.generation == context.generation);
                    std::hint::black_box(fiber.state);
                    std::hint::black_box(fiber.scope);
                    let gate = matches!(case, "c2_gate" | "c3_cancel" | "c4_lease")
                        .then(|| fiber.capabilities.clone());
                    let cancellation = matches!(case, "c3_cancel" | "c4_lease")
                        .then(|| fiber.cancellation.clone());
                    drop(fiber);
                    if case == "c4_lease" {
                        let lease = gate.as_ref().expect("gate").try_acquire().expect("lease");
                        std::hint::black_box(&lease);
                        drop(lease);
                    }
                    std::hint::black_box(gate);
                    std::hint::black_box(cancellation);
                })
            }
            "weak_fiber_shared" | "weak_fiber_independent" => {
                let shared = case == "weak_fiber_shared";
                let count = if shared { 1 } else { workers };
                let runtime = Runtime::new();
                let mut strong = Vec::with_capacity(count);
                for _ in 0..count {
                    let (fiber, _) = retained_context(&runtime).await;
                    strong.push(runtime.0.fibers.get(fiber).expect("fiber"));
                }
                let resources: Vec<_> = strong.iter().map(Arc::downgrade).collect();
                let elapsed = characterize_primitive(&resources, shared, workers, operations, |owner| {
                    std::hint::black_box(owner.upgrade().expect("owner"));
                });
                std::hint::black_box(strong);
                elapsed
            }
            _ => panic!("unknown CORDIS_PERF_CASE: {case}"),
        };
        let total = workers * operations;
        println!(
            "CORDIS_PRIMITIVE_RESULT case={case} workers={workers} operations_per_worker={operations} elapsed_ns={elapsed} aggregate_ops_sec={:.2} ns_per_op={:.2}",
            total as f64 * 1e9 / elapsed as f64,
            elapsed as f64 / total as f64,
        );
    }

    #[test]
    #[ignore = "manual performance characterization"]
    fn fibercell_layout_characterization() {
        println!(
            "CORDIS_LAYOUT_RESULT generation_execution_size={} generation_execution_align={} fiber_cell_size={} fiber_cell_align={} fiber_mutable_size={} fiber_mutable_align={}",
            std::mem::size_of::<GenerationExecution>(),
            std::mem::align_of::<GenerationExecution>(),
            std::mem::size_of::<FiberCell>(),
            std::mem::align_of::<FiberCell>(),
            std::mem::size_of::<FiberMutable>(),
            std::mem::align_of::<FiberMutable>(),
        );
    }

    #[tokio::test]
    #[ignore = "manual performance characterization"]
    #[allow(clippy::cast_precision_loss)]
    async fn fibercell_rwlock_writer_characterization() {
        let readers = characterization_number("CORDIS_PERF_READERS", 8);
        let seconds = characterization_number("CORDIS_PERF_SECONDS", 5);
        let writer_hz = std::env::var("CORDIS_PERF_WRITER_HZ")
            .expect("CORDIS_PERF_WRITER_HZ");
        let writer_hz = (writer_hz != "continuous")
            .then(|| writer_hz.parse::<u64>().expect("writer Hz"));
        let runtime = Runtime::new();
        let (fiber, _) = retained_context(&runtime).await;
        let cell = runtime.0.fibers.get(fiber).expect("fiber");
        let stop = Arc::new(AtomicBool::new(false));
        let reader_operations = Arc::new(StdAtomicU64::new(0));
        let started = StdInstant::now();
        let (writer_count, histogram) = std::thread::scope(|threads| {
            for _ in 0..readers {
                let cell = cell.clone();
                let stop = stop.clone();
                let reader_operations = reader_operations.clone();
                threads.spawn(move || {
                    let mut local = 0_u64;
                    while !stop.load(StdOrdering::Acquire) {
                        let fiber = cell.inner.read();
                        std::hint::black_box(fiber.generation);
                        std::hint::black_box(fiber.state);
                        std::hint::black_box(fiber.scope);
                        std::hint::black_box(fiber.capabilities.clone());
                        drop(fiber);
                        local += 1;
                    }
                    reader_operations.fetch_add(local, StdOrdering::Relaxed);
                });
            }
            let mut histogram = Histogram::<u64>::new(3).expect("histogram");
            let mut count = 0_u64;
            let deadline = started + Duration::from_secs(seconds as u64);
            while StdInstant::now() < deadline {
                let before = StdInstant::now();
                {
                    let mut data = cell.inner.write();
                    data.activation_sealed = !data.activation_sealed;
                }
                histogram
                    .record(before.elapsed().as_nanos().try_into().unwrap_or(u64::MAX))
                    .expect("writer latency");
                count += 1;
                if let Some(hz) = writer_hz {
                    std::thread::sleep(Duration::from_nanos(1_000_000_000 / hz));
                }
            }
            stop.store(true, StdOrdering::Release);
            (count, histogram)
        });
        let elapsed = started.elapsed();
        let reads = reader_operations.load(StdOrdering::Relaxed);
        println!(
            "CORDIS_RWLOCK_WRITER_RESULT readers={readers} writer_hz={} seconds={seconds} reader_ops_sec={:.2} writer_count={writer_count} writer_p50_ns={} writer_p95_ns={} writer_p99_ns={} writer_max_ns={}",
            writer_hz.map_or_else(|| "continuous".to_owned(), |hz| hz.to_string()),
            reads as f64 / elapsed.as_secs_f64(),
            histogram.value_at_quantile(0.50),
            histogram.value_at_quantile(0.95),
            histogram.value_at_quantile(0.99),
            histogram.max(),
        );
    }

    #[tokio::test]
    async fn undeclared_provide_does_not_consume_interner_capacity() {
        let runtime = Runtime::new();
        let slot = Arc::new(Mutex::new(None));
        runtime
            .install(runtime.root(), RetainContextPlugin(slot.clone()))
            .await
            .expect("provider");
        let context = slot.lock().clone().expect("retained context");
        let before = runtime.0.services.symbol_count();
        let error = context
            .provide(ServiceKey::new("interner", "undeclared", 1), "value")
            .expect_err("undeclared provision");
        assert!(matches!(error, CordisError::RevisionValidationFailed(_)));
        assert_eq!(runtime.0.services.symbol_count(), before);
    }

    #[tokio::test]
    async fn undeclared_provide_at_full_interner_returns_typed_error() {
        let runtime = Runtime::with_config(RuntimeConfig {
            max_interned_symbols: 2,
            ..RuntimeConfig::default()
        })
        .expect("runtime");
        runtime
            .intern_service(&ServiceKey::new("interner", "filler", 1))
            .expect("first symbol");
        let declared = ServiceKey::new("interner", "declared", 1);
        let results = Arc::new(Mutex::new(Vec::new()));
        runtime
            .install(
                runtime.root(),
                ProvideProbePlugin {
                    declared,
                    undeclared: Some(ServiceKey::new("interner", "undeclared-full", 1)),
                    repeat_declared: false,
                    results: results.clone(),
                },
            )
            .await
            .expect("declared provider");
        assert_eq!(runtime.0.services.symbol_count(), 2);
        assert!(matches!(
            results.lock().as_slice(),
            [Err(CordisError::RevisionValidationFailed(_))]
        ));
        assert_eq!(runtime.0.services.symbol_count(), 2);
    }

    #[tokio::test]
    async fn declared_provide_at_full_interner_uses_existing_symbol() {
        let runtime = Runtime::with_config(RuntimeConfig {
            max_interned_symbols: 2,
            ..RuntimeConfig::default()
        })
        .expect("runtime");
        runtime
            .intern_service(&ServiceKey::new("interner", "filler-full", 1))
            .expect("first symbol");
        let declared = ServiceKey::new("interner", "declared-full", 1);
        runtime
            .install(
                runtime.root(),
                ServiceRevisionPlugin {
                    values: vec![(declared, "value")],
                    task_runs: None,
                },
            )
            .await
            .expect("pre-admitted provision");
        assert_eq!(runtime.0.services.symbol_count(), 2);
    }

    #[tokio::test]
    async fn repeated_declared_provide_does_not_grow_interner() {
        let runtime = Runtime::new();
        let declared = ServiceKey::new("interner", "repeated", 1);
        let results = Arc::new(Mutex::new(Vec::new()));
        runtime
            .install(
                runtime.root(),
                ProvideProbePlugin {
                    declared,
                    undeclared: None,
                    repeat_declared: true,
                    results: results.clone(),
                },
            )
            .await
            .expect("provider");
        assert!(matches!(
            results.lock().as_slice(),
            [Err(CordisError::DuplicateService(_))]
        ));
        assert_eq!(runtime.0.services.symbol_count(), 1);
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn interner_config_rejects_symbol_limit_above_id_space() {
        let result = Runtime::with_config(RuntimeConfig {
            max_interned_symbols: u32::MAX as usize + 1,
            ..RuntimeConfig::default()
        });
        let Err(error) = result else {
            panic!("symbol limit must fit ServiceSymbol");
        };
        assert!(matches!(error, CordisError::InvalidRuntimeConfig(_)));
    }

    #[tokio::test]
    async fn service_resolution_cache_is_bounded_and_eviction_is_correct() {
        for capacity in [0, 2] {
            let runtime = Runtime::with_config(RuntimeConfig {
                max_resolution_cache_entries: capacity,
                ..RuntimeConfig::default()
            })
            .expect("runtime");
            let keys: Vec<_> = (0..4)
                .map(|index| ServiceKey::new("cache-bound", format!("key-{index}"), 1))
                .collect();
            runtime
                .install(
                    runtime.root(),
                    ServiceRevisionPlugin {
                        values: keys.iter().cloned().map(|key| (key, "value")).collect(),
                        task_runs: None,
                    },
                )
                .await
                .expect("provider");
            let (_, context) = retained_context(&runtime).await;
            for key in keys.iter().chain(keys.iter().rev()) {
                assert_eq!(*context.get::<&'static str>(key).expect("resolved"), "value");
                assert!(runtime.0.services.cache_len() <= capacity);
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn service_cache_miss_write_upgrade_double_checks_current_truth() {
        let runtime = Runtime::new();
        let key = ServiceKey::new("cache-race", "late-provider", 1);
        runtime.intern_service(&key).expect("symbol");
        let (_, context) = retained_context(&runtime).await;
        let fence = Arc::new(std::sync::Barrier::new(2));
        runtime.0.services.set_miss_before_write_hook(fence.clone());
        let lookup = tokio::task::spawn_blocking({
            let context = context.clone();
            let key = key.clone();
            move || context.try_get::<&'static str>(&key)
        });
        let entered = fence.clone();
        tokio::task::spawn_blocking(move || entered.wait())
            .await
            .expect("reader released read guard");
        runtime
            .install(
                runtime.root(),
                ServiceRevisionPlugin {
                    values: vec![(key.clone(), "current")],
                    task_runs: None,
                },
            )
            .await
            .expect("provider");
        assert_eq!(
            *context.get::<&'static str>(&key).expect("cache fill"),
            "current"
        );
        tokio::task::spawn_blocking(move || fence.wait())
            .await
            .expect("resume reader");
        let handle = lookup
            .await
            .expect("lookup join")
            .expect("lookup")
            .expect("current provider");
        assert_eq!(*handle, "current");
    }

    #[tokio::test]
    async fn negative_service_cache_is_invalidated_when_provider_appears() {
        let runtime = Runtime::new();
        let key = ServiceKey::new("cache-negative", "provider", 1);
        runtime.intern_service(&key).expect("symbol");
        let (_, context) = retained_context(&runtime).await;
        assert!(context
            .try_get::<&'static str>(&key)
            .expect("negative lookup")
            .is_none());
        runtime
            .install(
                runtime.root(),
                ServiceRevisionPlugin {
                    values: vec![(key.clone(), "visible")],
                    task_runs: None,
                },
            )
            .await
            .expect("provider");
        assert_eq!(*context.get::<&'static str>(&key).expect("provider"), "visible");
    }

    #[tokio::test]
    async fn cached_service_location_is_invalidated_when_provider_is_removed() {
        let runtime = Runtime::new();
        let key = ServiceKey::new("cache-remove", "provider", 1);
        let provider = runtime
            .install(
                runtime.root(),
                ServiceRevisionPlugin {
                    values: vec![(key.clone(), "removed")],
                    task_runs: None,
                },
            )
            .await
            .expect("provider");
        let (_, context) = retained_context(&runtime).await;
        assert_eq!(*context.get::<&'static str>(&key).expect("cached"), "removed");
        runtime
            .dispose_fiber(provider, false)
            .await
            .expect("remove provider");
        assert!(context
            .try_get::<&'static str>(&key)
            .expect("post-removal lookup")
            .is_none());
    }

    #[test]
    fn service_registry_lock_is_independent_from_lifecycle_state() {
        let runtime = Runtime::new();
        let key = ServiceKey::new("isolation", "service", 1);
        let symbol = runtime.0.services.try_intern(&key).expect("symbol admission");
        assert_eq!(runtime.0.services.intern(&key), symbol);
    }

    #[tokio::test]
    async fn dependency_restart_uses_fresh_capability_gate() {
        let runtime = Runtime::new();
        let fiber = runtime
            .install(runtime.root(), EmptyPlugin)
            .await
            .expect("activation");
        let old_gate = runtime.0.fibers.with(fiber, |f| f.capabilities.clone()).expect("fiber");
        runtime
            .dispose_fiber(fiber, true)
            .await
            .expect("dependency disposal");
        assert!(!old_gate.is_visible());
        runtime.activate(fiber).await.expect("restart");
        let new_gate = runtime.0.fibers.with(fiber, |f| f.capabilities.clone()).expect("fiber");
        assert!(!Arc::ptr_eq(&old_gate, &new_gate));
        assert!(new_gate.is_visible());
        assert!(!old_gate.publish());
        assert!(!old_gate.is_visible());
    }

    #[tokio::test]
    async fn reload_preserves_plugin_id_and_allocates_new_generation() {
        let runtime = Runtime::new();
        let old = runtime.install(runtime.root(), EmptyPlugin).await.expect("old");
        let old_cell = runtime.0.fibers.get(old).expect("old");
        let plugin = old_cell.plugin_id;
        let old_generation = old_cell.inner.read().generation;
        let new = runtime.reload(old, EmptyPlugin).await.expect("reload");
        let new_cell = runtime.0.fibers.get(new).expect("new");
        assert_eq!(new_cell.plugin_id, plugin);
        assert_ne!(new_cell.inner.read().generation, old_generation);
    }

    #[tokio::test]
    async fn dependency_restart_preserves_plugin_id_and_allocates_new_generation() {
        let runtime = Runtime::new();
        let fiber = runtime.install(runtime.root(), EmptyPlugin).await.expect("install");
        let cell = runtime.0.fibers.get(fiber).expect("fiber");
        let plugin = cell.plugin_id;
        let old_generation = cell.inner.read().generation;
        runtime.dispose_fiber(fiber, true).await.expect("loss");
        runtime.activate(fiber).await.expect("restore");
        assert_eq!(cell.plugin_id, plugin);
        assert_ne!(cell.inner.read().generation, old_generation);
    }

    async fn restart_cancellation_scenario() {
        let runtime = Runtime::new();
        let starts = Arc::new(AtomicU64::new(0));
        let entered = Arc::new(tokio::sync::Notify::new());
        let fiber = runtime.install(runtime.root(), RestartBlockingPlugin {
            starts: starts.clone(), entered: entered.clone(),
        }).await.expect("initial activation");
        let cell = runtime.0.fibers.get(fiber).expect("fiber");
        let old_token = cell.inner.read().cancellation.clone();
        let old_generation = cell.inner.read().generation;
        runtime.dispose_fiber(fiber, true).await.expect("dependency loss");
        assert!(old_token.is_cancelled());
        assert!(cell.inner.read().cancellation.is_cancelled(), "finalize must retain old token");

        let activation = {
            let runtime = runtime.clone();
            tokio::spawn(async move { runtime.activate(fiber).await })
        };
        loop {
            let notified = entered.notified();
            if starts.load(Ordering::SeqCst) >= 2 { break; }
            notified.await;
        }
        let current_token = cell.inner.read().cancellation.clone();
        assert!(!current_token.is_cancelled());
        assert_ne!(cell.inner.read().generation, old_generation);

        let disposal = {
            let runtime = runtime.clone();
            tokio::spawn(async move { runtime.dispose_fiber(fiber, false).await })
        };
        let (activation_result, disposal_result) = tokio::time::timeout(
            Duration::from_secs(1),
            async { (activation.await.expect("activation join"), disposal.await.expect("disposal join")) },
        ).await.expect("restart cancellation must converge");
        assert!(activation_result.is_err());
        disposal_result.expect("disposal");
        assert!(current_token.is_cancelled());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dependency_restart_activation_uses_current_generation_token() {
        restart_cancellation_scenario().await;
    }

    #[tokio::test]
    async fn old_generation_token_is_not_reused_for_restart() {
        let runtime = Runtime::new();
        let fiber = runtime.install(runtime.root(), EmptyPlugin).await.expect("install");
        let cell = runtime.0.fibers.get(fiber).expect("fiber");
        let old = cell.inner.read().cancellation.clone();
        runtime.dispose_fiber(fiber, true).await.expect("loss");
        runtime.activate(fiber).await.expect("restart");
        let current = cell.inner.read().cancellation.clone();
        assert!(old.is_cancelled());
        assert!(!current.is_cancelled());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dispose_racing_restart_activation_cancels_exact_activation_token() {
        restart_cancellation_scenario().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn restart_activation_cancellation_allows_disposal_to_converge() {
        restart_cancellation_scenario().await;
    }

    #[tokio::test]
    async fn reclaiming_old_fiber_does_not_remove_live_plugin_slot() {
        let runtime = Runtime::new();
        let old = runtime.install(runtime.root(), EmptyPlugin).await.expect("old");
        let plugin = runtime.0.fibers.get(old).expect("old").plugin_id;
        let new = runtime.reload(old, EmptyPlugin).await.expect("reload");
        let _ = runtime.collect_garbage();
        assert!(runtime.0.plugins.contains(plugin));
        assert_eq!(runtime.0.fibers.get(new).expect("new").plugin_id, plugin);
        runtime.dispose_fiber(new, false).await.expect("dispose");
        let _ = runtime.collect_garbage();
        assert!(!runtime.0.plugins.contains(plugin));
    }

    #[tokio::test]
    async fn old_generation_disposal_after_cutover_cannot_hide_new_generation() {
        let runtime = Runtime::new();
        let old = runtime.install(runtime.root(), EmptyPlugin).await.expect("old");
        let old_gate = runtime.0.fibers.with(old, |f| f.capabilities.clone()).expect("old");
        let new = runtime.reload(old, EmptyPlugin).await.expect("reload");
        let new_gate = runtime.0.fibers.with(new, |f| f.capabilities.clone()).expect("new");
        assert!(new_gate.is_visible());assert!(!old_gate.is_visible());
        old_gate.close();
        assert!(new_gate.is_visible());assert!(!old_gate.publish());
    }

    #[test]
    fn scope_ancestry_is_correct() {
        let runtime = Runtime::new();
        let child = runtime.create_scope(runtime.root(), "child").expect("child");
        let leaf = runtime.create_scope(child, "leaf").expect("leaf");
        assert_eq!(runtime.0.scopes.ancestry(leaf).expect("ancestry"), vec![leaf, child, runtime.root()]);
        assert_eq!(runtime.0.scopes.ancestry_root_to_leaf(leaf, true).expect("root to leaf"), vec![runtime.root(), child, leaf]);
    }

    #[tokio::test]
    async fn service_lookup_does_not_require_runtime_state_lock() {
        let runtime = Runtime::new();
        let key = ServiceKey::new("scope", "hot-path", 1);
        let fiber = runtime.install(
            runtime.root(),
            ServiceRevisionPlugin { values: vec![(key.clone(), "value")], task_runs: None },
        ).await.expect("provider");
        let context = Context {
            runtime: Arc::downgrade(&runtime.0),
            scope: runtime.root(),
            fiber,
            owner: Arc::downgrade(&runtime.0.fibers.get(fiber).expect("fiber")),
            generation: runtime.0.fibers.get(fiber).expect("fiber").inner.read().generation,
        };
        assert_eq!(*context.get::<&'static str>(&key).expect("service"), "value");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fiber_a_lifecycle_lock_does_not_block_fiber_b_invoke() {
        let runtime = Runtime::new();
        let a = runtime.install(runtime.root(), EmptyPlugin).await.expect("a");
        let b = runtime.install(runtime.root(), EmptyPlugin).await.expect("b");
        let a_cell = runtime.0.fibers.get(a).expect("a");
        let _a_lifecycle = a_cell.lifecycle.clone().lock_owned().await;
        let b_cell = runtime.0.fibers.get(b).expect("b");
        let context = Context {
            runtime: Arc::downgrade(&runtime.0),
            scope: runtime.root(),
            fiber: b,
            owner: Arc::downgrade(&b_cell),
            generation: b_cell.inner.read().generation,
        };
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            context.invoke(&InvocationKey::new("isolation", "missing", 1), InvocationValue::native(Arc::new(()))),
        )
        .await;
        assert!(result.is_ok(), "Fiber B waited for Fiber A lifecycle lock");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fiber_a_mutable_lock_does_not_block_fiber_b_context_operations() {
        let runtime = Runtime::new();
        let a = runtime.install(runtime.root(), EmptyPlugin).await.expect("a");
        let b = runtime.install(runtime.root(), EmptyPlugin).await.expect("b");
        let a_cell = runtime.0.fibers.get(a).expect("a");
        let (locked_tx, locked_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let holder = std::thread::spawn(move || {
            let _guard = a_cell.inner.read();
            locked_tx.send(()).expect("locked");
            release_rx.recv().expect("release");
        });
        locked_rx.recv().expect("Fiber A locked");
        let b_cell = runtime.0.fibers.get(b).expect("b");
        let context = Context {
            runtime: Arc::downgrade(&runtime.0),
            scope: runtime.root(),
            fiber: b,
            owner: Arc::downgrade(&b_cell),
            generation: b_cell.inner.read().generation,
        };
        assert!(context.try_get::<()>(&ServiceKey::new("isolation", "missing", 1)).expect("get").is_none());
        assert!(tokio::time::timeout(
            Duration::from_secs(1),
            context.emit(&EventKey("isolation.empty".into()), Arc::new(()) as EventValue),
        ).await.expect("emit did not wait for Fiber A").is_ok());
        release_tx.send(()).expect("release Fiber A");
        holder.join().expect("holder");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn fiber_creation_racing_scope_disposal_has_single_winner() {
        let runtime = Runtime::new();
        let scope = runtime.create_scope(runtime.root(), "race").expect("scope");
        let barrier = Arc::new(tokio::sync::Barrier::new(3));
        let installer = {
            let runtime = runtime.clone();
            let barrier = barrier.clone();
            tokio::spawn(async move { barrier.wait().await; runtime.install(scope, EmptyPlugin).await })
        };
        let disposer = {
            let runtime = runtime.clone();
            let barrier = barrier.clone();
            tokio::spawn(async move { barrier.wait().await; runtime.dispose_scope(scope).await })
        };
        barrier.wait().await;
        let install_result = installer.await.expect("installer");
        disposer.await.expect("disposer").expect("dispose");
        match install_result {
            Ok(fiber) => assert!(matches!(
                runtime.0.fibers.with(fiber, |f| f.state),
                None | Some(FiberState::Disposed)
            )),
            Err(CordisError::ScopeDisposed(id)) => assert_eq!(id, scope),
            Err(CordisError::ScopeNotFound) => {}
            Err(error) => panic!("unexpected install result: {error}"),
        }
        assert!(runtime.0.scopes.read().get(scope).is_none_or(|s| s.fibers.is_empty()));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn parallel_fiber_lifecycle_churn_converges() {
        let runtime = Runtime::new();
        let installs = (0..32).map(|_| {
            let runtime = runtime.clone();
            tokio::spawn(async move {
                let fiber = runtime.install(runtime.root(), EmptyPlugin).await.expect("install");
                runtime.dispose_fiber(fiber, false).await.expect("dispose");
            })
        });
        for task in installs {
            task.await.expect("churn task");
        }
        wait_for_no_runtime_workers(&runtime).await;
        assert_eq!(runtime.0.fibers.len(), 0);
        assert_eq!(runtime.0.plugins.total_fiber_count(), 0);
        assert!(runtime.0.scopes.read().get(runtime.root()).expect("root").fibers.is_empty());
    }

    async fn create_scope_introspection_race(operation: fn(&Runtime)) {
        let runtime = Runtime::new();
        let fiber = runtime.install(runtime.root(), EmptyPlugin).await.expect("fiber");
        let cell = runtime.0.fibers.get(fiber).expect("fiber");
        let context = Context {
            runtime: Arc::downgrade(&runtime.0),
            scope: runtime.root(),
            fiber,
            owner: Arc::downgrade(&cell),
            generation: cell.inner.read().generation,
        };
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let creator = {
            let barrier = barrier.clone();
            tokio::task::spawn_blocking(move || {
                barrier.wait();
                for index in 0..16 {
                    context.create_scope(format!("lock-race-{index}")).expect("scope");
                }
            })
        };
        let observer = {
            let runtime = runtime.clone();
            let barrier = barrier.clone();
            tokio::task::spawn_blocking(move || {
                barrier.wait();
                for _ in 0..32 { operation(&runtime); }
            })
        };
        barrier.wait();
        tokio::time::timeout(Duration::from_secs(2), async {
            creator.await.expect("creator");
            observer.await.expect("observer");
        }).await.expect("Scope/Fiber lock order deadlock");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn create_scope_vs_gc_does_not_deadlock() {
        create_scope_introspection_race(|runtime| { let _ = runtime.collect_garbage(); }).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn create_scope_vs_snapshot_does_not_deadlock() {
        create_scope_introspection_race(|runtime| { let _ = runtime.snapshot(); }).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn create_scope_vs_health_does_not_deadlock() {
        create_scope_introspection_race(|runtime| { let _ = runtime.health(); }).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn scope_disposal_vs_fiber_snapshot_does_not_deadlock() {
        let runtime = Runtime::new();
        let scope = runtime.create_scope(runtime.root(), "dispose-race").expect("scope");
        runtime.install(scope, EmptyPlugin).await.expect("fiber");
        let disposer = { let runtime = runtime.clone(); tokio::spawn(async move { runtime.dispose_scope(scope).await }) };
        for _ in 0..32 { let _ = runtime.snapshot(); }
        tokio::time::timeout(Duration::from_secs(2), disposer).await.expect("deadlock").expect("join").expect("dispose");
    }

    #[tokio::test]
    async fn dependency_validation_does_not_hold_fiber_and_scope_locks_together() {
        let runtime = Runtime::new();
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            runtime.install(runtime.root(), SelfCyclePlugin),
        ).await.expect("validation deadlock");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn existing_context_invoke_does_not_require_fiber_registry_arena_lock() {
        let runtime = Runtime::new();
        let fiber = runtime.install(runtime.root(), EmptyPlugin).await.expect("fiber");
        let cell = runtime.0.fibers.get(fiber).expect("cell");
        let context = Context {
            runtime: Arc::downgrade(&runtime.0),
            scope: runtime.root(),
            fiber,
            owner: Arc::downgrade(&cell),
            generation: cell.inner.read().generation,
        };
        assert!(runtime.0.fibers.remove(fiber).is_some());
        let result = context
            .invoke(
                &InvocationKey::new("fiber", "missing-after-arena-remove", 1),
                InvocationValue::native(Arc::new(())),
            )
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn fiber_disposal_detaches_scope_membership_exactly_once() {
        let runtime = Runtime::new();
        let fiber = runtime.install(runtime.root(), EmptyPlugin).await.expect("fiber");
        runtime.dispose_fiber(fiber, false).await.expect("first dispose");
        runtime.dispose_fiber(fiber, false).await.expect("idempotent observe");
        let root = runtime.0.scopes.read();
        assert!(!root.get(runtime.root()).expect("root").fibers.contains(&fiber));
    }

    #[tokio::test]
    async fn fiber_creation_failure_rolls_back_plugin_ref() {
        let runtime = Runtime::new();
        assert!(runtime.install(runtime.root(), SelfCyclePlugin).await.is_err());
        assert_eq!(runtime.0.fibers.len(), 0);
        assert_eq!(runtime.0.plugins.total_fiber_count(), 0);
        assert!(runtime.0.scopes.read().get(runtime.root()).expect("root").fibers.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fiber_gc_detaches_plugin_ref_exactly_once() {
        let runtime = Runtime::new();
        let fiber = runtime.install(runtime.root(), EmptyPlugin).await.expect("fiber");
        runtime.dispose_fiber(fiber, false).await.expect("dispose");
        let first = {
            let runtime = runtime.clone();
            tokio::task::spawn_blocking(move || runtime.collect_garbage())
        };
        let second = {
            let runtime = runtime.clone();
            tokio::task::spawn_blocking(move || runtime.collect_garbage())
        };
        let (first, second) = tokio::join!(first, second);
        assert!(first.expect("first").fibers + second.expect("second").fibers <= 1);
        assert_eq!(runtime.0.plugins.total_fiber_count(), 0);
    }

    #[tokio::test]
    async fn create_child_rejects_disposing_parent_and_scope_lock_is_not_held_across_fiber_await() {
        let runtime = Runtime::new();
        let parent = runtime.create_scope(runtime.root(), "parent").expect("parent");
        let gate = Arc::new(EffectGate::default());
        runtime.install(parent, BlockingEffectPlugin(gate.clone())).await.expect("plugin");
        let disposing = { let runtime=runtime.clone(); tokio::spawn(async move { runtime.dispose_scope(parent).await }) };
        wait_for_effect(&gate).await;
        assert!(matches!(runtime.create_scope(parent, "late"), Err(CordisError::ScopeDisposed(id)) if id == parent));
        assert_eq!(runtime.0.scopes.parent(parent).expect("scope lock available"), Some(runtime.root()));
        gate.release.notify_one();
        disposing.await.expect("join").expect("dispose");
    }

    #[tokio::test]
    async fn root_scope_cannot_be_accidentally_reclaimed() {
        let runtime = Runtime::new();
        let root = runtime.root();
        runtime.shutdown().await.expect("shutdown");
        let _ = runtime.collect_garbage();
        assert!(runtime.0.scopes.read().get(root).is_some());
        assert_eq!(runtime.0.scopes.ancestry(root).expect("root ancestry"), vec![root]);
    }

    #[tokio::test]
    async fn disposed_scope_is_reclaimed_under_quota_pressure_and_stale_id_does_not_alias() {
        let config = RuntimeConfig { max_scopes: 2, ..RuntimeConfig::default() };
        let runtime = Runtime::with_config(config).expect("runtime");
        let stale = runtime.create_scope(runtime.root(), "old").expect("old");
        runtime.dispose_scope(stale).await.expect("dispose");
        let fresh = runtime.create_scope(runtime.root(), "fresh").expect("pressure reclaim");
        assert_ne!(stale, fresh);
        assert!(matches!(runtime.0.scopes.parent(stale), Err(CordisError::ScopeNotFound)));
    }

    async fn prepare_staged_task_revision(
        runtime: &Runtime,
        count: Arc<AtomicU64>,
    ) -> (FiberId, FiberId, ScopeId) {
        let old = runtime
            .install(runtime.root(), EmptyPlugin)
            .await
            .expect("old revision");
        runtime
            .transition(old, FiberState::Reloading)
            .expect("reload transition");
        let staging = runtime
            .create_scope_internal(runtime.root(), "test-staging".into(), true)
            .expect("staging scope");
        let plugin_id = runtime.0.fibers.get(old).expect("old").plugin_id;
        let staged = runtime
            .install_arc(
                staging,
                Arc::new(CountingTaskPlugin(count)),
                true,
                Some(plugin_id),
            )
            .await
            .expect("staged revision");
        (old, staged, staging)
    }

    #[tokio::test]
    async fn staged_task_does_not_run_before_staged_capability_publish() {
        let runtime = Runtime::new();
        let count = Arc::new(AtomicU64::new(0));
        let (old, staged, staging) =
            prepare_staged_task_revision(&runtime, count.clone()).await;
        tokio::task::yield_now().await;
        assert_eq!(count.load(Ordering::SeqCst), 0);
        let gate = runtime.0.fibers.with(staged, |f| f.capabilities.clone()).expect("staged");
        assert!(gate.is_staged());
        runtime
            .commit_staged_revision(old, staged, staging, runtime.root())
            .expect("commit");
        assert!(gate.is_visible());
        tokio::task::yield_now().await;
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn staged_task_never_runs_when_staged_publish_fails() {
        let runtime = Runtime::new();
        let count = Arc::new(AtomicU64::new(0));
        let (old, staged, staging) =
            prepare_staged_task_revision(&runtime, count.clone()).await;
        runtime.0.fibers.with(staged, |f| f.capabilities.close()).expect("staged");
        assert_eq!(
            runtime
                .commit_staged_revision(old, staged, staging, runtime.root())
                .expect_err("closed gate"),
            CordisError::CapabilityPublicationFailed(staged)
        );
        runtime
            .dispose_reload_owned_fiber(staged)
            .await
            .expect("rollback staged");
        tokio::task::yield_now().await;
        assert_eq!(count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn reload_scope_commit_revalidates_target_lifecycle() {
        let runtime = Runtime::new();
        let count = Arc::new(AtomicU64::new(0));
        let (old, staged, staging) = prepare_staged_task_revision(&runtime, count).await;
        let revision = PreparedScopeRevision::prepare(
            &runtime,
            staging,
            runtime.root(),
            staged,
            old,
            Vec::new(),
        )
        .expect("prepare");
        runtime.0.scopes.write().get_mut(runtime.root()).expect("root").state =
            ScopeState::Disposing;
        assert!(matches!(
            revision.commit(&runtime),
            Err(CordisError::RevisionValidationFailed(_))
        ));
        assert!(runtime.0.scopes.read().get(staging).expect("staging").fibers.contains(&staged));
    }

    #[tokio::test]
    async fn reload_scope_prepare_racing_target_disposal_fails_before_selector_commit() {
        let runtime = Runtime::new();
        let (old, staged, staging) =
            prepare_staged_task_revision(&runtime, Arc::new(AtomicU64::new(0))).await;
        let revision = PreparedScopeRevision::prepare(
            &runtime, staging, runtime.root(), staged, old, Vec::new(),
        ).expect("prepare");
        runtime.0.scopes.write().get_mut(runtime.root()).expect("root").state =
            ScopeState::Disposing;
        assert!(revision.commit(&runtime).is_err());
        assert!(runtime.0.fibers.with(staged, |fiber| fiber.capabilities.is_staged()).unwrap());
    }

    #[tokio::test]
    async fn reload_scope_commit_revalidates_staged_membership() {
        let runtime = Runtime::new();
        let (old, staged, staging) =
            prepare_staged_task_revision(&runtime, Arc::new(AtomicU64::new(0))).await;
        let revision = PreparedScopeRevision::prepare(
            &runtime, staging, runtime.root(), staged, old, Vec::new(),
        )
        .expect("prepare");
        runtime.0.scopes.write().get_mut(staging).expect("staging").fibers.clear();
        assert!(revision.commit(&runtime).is_err());
        assert!(!runtime.0.scopes.read().get(runtime.root()).expect("root").fibers.contains(&staged));
    }

    #[tokio::test]
    async fn reload_scope_commit_revalidates_child_parent() {
        let runtime = Runtime::new();
        let (old, staged, staging) =
            prepare_staged_task_revision(&runtime, Arc::new(AtomicU64::new(0))).await;
        let child = runtime.create_scope_internal(staging, "child".into(), true).expect("child");
        let revision = PreparedScopeRevision::prepare(
            &runtime, staging, runtime.root(), staged, old, vec![child],
        )
        .expect("prepare");
        runtime.0.scopes.write().get_mut(child).expect("child").parent = Some(runtime.root());
        assert!(revision.commit(&runtime).is_err());
        assert_eq!(runtime.0.scopes.read().get(child).expect("child").parent, Some(runtime.root()));
    }

    #[tokio::test]
    async fn scope_revision_commit_is_atomic() {
        let runtime = Runtime::new();
        let (old, staged, staging) =
            prepare_staged_task_revision(&runtime, Arc::new(AtomicU64::new(0))).await;
        let child = runtime.create_scope_internal(staging, "child".into(), true).expect("child");
        let revision = PreparedScopeRevision::prepare(
            &runtime, staging, runtime.root(), staged, old, vec![child],
        )
        .expect("prepare");
        revision.commit(&runtime).expect("commit");
        let scopes = runtime.0.scopes.read();
        assert!(!scopes.get(staging).expect("staging").fibers.contains(&staged));
        assert!(scopes.get(runtime.root()).expect("root").fibers.contains(&staged));
        assert_eq!(scopes.get(child).expect("child").parent, Some(runtime.root()));
    }

    #[tokio::test]
    async fn scope_revision_rollback_is_atomic() {
        let runtime = Runtime::new();
        let (old, staged, staging) =
            prepare_staged_task_revision(&runtime, Arc::new(AtomicU64::new(0))).await;
        let revision = PreparedScopeRevision::prepare(
            &runtime, staging, runtime.root(), staged, old, Vec::new(),
        )
        .expect("prepare");
        revision.commit(&runtime).expect("commit");
        revision.rollback(&runtime).expect("rollback");
        let scopes = runtime.0.scopes.read();
        assert!(scopes.get(staging).expect("staging").fibers.contains(&staged));
        assert!(!scopes.get(runtime.root()).expect("root").fibers.contains(&staged));
    }

    #[tokio::test]
    async fn dependency_revision_commit_is_atomic() {
        let runtime = Runtime::new();
        let (old, staged, staging) =
            prepare_staged_task_revision(&runtime, Arc::new(AtomicU64::new(0))).await;
        let symbols: Vec<_> = ["a", "b", "c", "d"]
            .into_iter()
            .map(|name| {
                runtime
                    .intern_service(&ServiceKey::new("revision", name, 1))
                    .expect("test symbol")
            })
            .collect();
        for symbol in &symbols {
            runtime.0.dependencies.add_provider(staging, *symbol, staged);
        }
        let revision = PreparedDependencyRevision {
            symbols: symbols.clone(), from: staging, to: runtime.root(), staged,
        };
        revision.commit(&runtime).expect("commit");
        assert!(runtime.0.dependencies.provider_revision_snapshot(
            staging, runtime.root(), &symbols, staged,
        ).iter().all(|state| *state == (false, true)));
        assert!(runtime.0.fibers.get(old).is_some());
    }

    #[tokio::test]
    async fn dependency_revision_rollback_is_atomic() {
        let runtime = Runtime::new();
        let (_, staged, staging) =
            prepare_staged_task_revision(&runtime, Arc::new(AtomicU64::new(0))).await;
        let symbols: Vec<_> = ["a", "b", "c", "d"]
            .into_iter()
            .map(|name| {
                runtime
                    .intern_service(&ServiceKey::new("rollback", name, 1))
                    .expect("test symbol")
            })
            .collect();
        for symbol in &symbols { runtime.0.dependencies.add_provider(staging, *symbol, staged); }
        let revision = PreparedDependencyRevision {
            symbols: symbols.clone(), from: staging, to: runtime.root(), staged,
        };
        revision.commit(&runtime).expect("commit");
        revision.rollback(&runtime).expect("rollback");
        assert!(runtime.0.dependencies.provider_revision_snapshot(
            staging, runtime.root(), &symbols, staged,
        ).iter().all(|state| *state == (true, false)));
    }

    #[tokio::test]
    async fn dependency_revision_is_never_partially_visible() {
        let runtime = Runtime::new();
        let (_, staged, staging) =
            prepare_staged_task_revision(&runtime, Arc::new(AtomicU64::new(0))).await;
        let symbols: Vec<_> = ["a", "b", "c", "d"]
            .into_iter()
            .map(|name| {
                runtime
                    .intern_service(&ServiceKey::new("visibility", name, 1))
                    .expect("test symbol")
            })
            .collect();
        for symbol in &symbols { runtime.0.dependencies.add_provider(staging, *symbol, staged); }
        let revision = PreparedDependencyRevision {
            symbols: symbols.clone(), from: staging, to: runtime.root(), staged,
        };
        let before = runtime.0.dependencies.provider_revision_snapshot(
            staging, runtime.root(), &symbols, staged,
        );
        revision.commit(&runtime).expect("commit");
        let after = runtime.0.dependencies.provider_revision_snapshot(
            staging, runtime.root(), &symbols, staged,
        );
        assert!(before.iter().all(|state| *state == (true, false)));
        assert!(after.iter().all(|state| *state == (false, true)));
    }

    #[tokio::test]
    async fn reload_dependency_commit_revalidates_expected_provider() {
        let runtime = Runtime::new();
        let (_, staged, staging) =
            prepare_staged_task_revision(&runtime, Arc::new(AtomicU64::new(0))).await;
        let symbols: Vec<_> = ["a", "b", "c", "d"]
            .into_iter()
            .map(|name| {
                runtime
                    .intern_service(&ServiceKey::new("conflict", name, 1))
                    .expect("test symbol")
            })
            .collect();
        for symbol in &symbols { runtime.0.dependencies.add_provider(staging, *symbol, staged); }
        runtime.0.dependencies.remove_provider(staging, symbols[2], staged);
        let revision = PreparedDependencyRevision {
            symbols: symbols.clone(), from: staging, to: runtime.root(), staged,
        };
        assert!(revision.commit(&runtime).is_err());
        assert!(runtime.0.dependencies.provider_revision_snapshot(
            staging, runtime.root(), &symbols, staged,
        ).iter().all(|state| !state.1));
    }

    #[tokio::test]
    async fn public_dispose_cannot_steal_reload_staged_fiber() {
        let runtime = Runtime::new();
        let (old, staged, staging) =
            prepare_staged_task_revision(&runtime, Arc::new(AtomicU64::new(0))).await;
        assert_eq!(runtime.dispose_fiber(staged, false).await,
            Err(CordisError::FiberLifecycleOwned(staged)));
        runtime.commit_staged_revision(old, staged, staging, runtime.root()).expect("commit");
        assert!(!runtime.0.fibers.with(staged, |fiber| fiber.reload_owned).expect("staged"));
    }

    #[tokio::test]
    async fn public_reload_cannot_steal_reload_staged_fiber() {
        let runtime = Runtime::new();
        let (_, staged, _) =
            prepare_staged_task_revision(&runtime, Arc::new(AtomicU64::new(0))).await;
        assert_eq!(runtime.reload_detailed(staged, EmptyPlugin).await,
            Err(CordisError::FiberLifecycleOwned(staged)));
    }

    #[tokio::test]
    async fn reload_precommit_failure_releases_staged_fiber_ownership() {
        let runtime = Runtime::new();
        let (old, staged, staging) =
            prepare_staged_task_revision(&runtime, Arc::new(AtomicU64::new(0))).await;
        let failure = runtime.rollback_reload(
            old, Some(staged), Some(staging), CordisError::RevisionValidationFailed("test".into()),
        ).await;
        assert!(matches!(failure, CordisError::ReloadFailed { .. }));
        assert!(!runtime.0.fibers.with(staged, |fiber| fiber.reload_owned).unwrap_or(false));
    }

    async fn scope_cutover_fence_scenario(force_cas_failure: bool) {
        let runtime = Runtime::new();
        let target = runtime.create_scope(runtime.root(), "cutover-target").expect("target");
        let old = runtime.install(target, EmptyPlugin).await.expect("old");
        let fence = Arc::new(std::sync::Barrier::new(2));
        *runtime.0.reload_before_selector_hook.lock() = Some(fence.clone());
        if force_cas_failure {
            runtime.0.fail_selector_after_scope.store(true, Ordering::SeqCst);
        }
        let reload = {
            let runtime = runtime.clone();
            tokio::spawn(async move { runtime.reload_detailed(old, EmptyPlugin).await })
        };
        let entered = fence.clone();
        tokio::task::spawn_blocking(move || entered.wait()).await.expect("fence entered");
        let disposal = {
            let runtime = runtime.clone();
            tokio::spawn(async move { runtime.dispose_scope(target).await })
        };
        tokio::task::yield_now().await;
        assert!(!disposal.is_finished(), "scope disposal crossed the cutover fence");
        runtime.0.reload_before_selector_hook.lock().take();
        tokio::task::spawn_blocking(move || fence.wait()).await.expect("fence release");
        let reload_result = reload.await.expect("reload worker");
        if force_cas_failure {
            assert!(reload_result.is_err());
        }
        disposal.await.expect("disposal worker").expect("target disposal");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn reload_scope_commit_racing_target_disposal_has_single_cutover_winner() {
        scope_cutover_fence_scenario(false).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn target_scope_disposal_cannot_snapshot_reload_staged_fiber_before_commit_truth_is_fixed() {
        scope_cutover_fence_scenario(false).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn selector_cas_failure_rolls_back_scope_before_disposal_can_enter() {
        scope_cutover_fence_scenario(true).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn reload_postcas_target_disposal_sees_committed_fiber_metadata() {
        let runtime = Runtime::new();
        let target = runtime.create_scope(runtime.root(), "postcas-target").expect("target");
        let old = runtime.install(target, EmptyPlugin).await.expect("old");
        let fence = Arc::new(std::sync::Barrier::new(2));
        *runtime.0.reload_after_selector_hook.lock() = Some(fence.clone());
        let reload = {
            let runtime = runtime.clone();
            tokio::spawn(async move { runtime.reload_detailed(old, EmptyPlugin).await })
        };
        let entered = fence.clone();
        tokio::task::spawn_blocking(move || entered.wait()).await.expect("post-CAS fence");
        let disposal = {
            let runtime = runtime.clone();
            tokio::spawn(async move { runtime.dispose_scope(target).await })
        };
        tokio::task::yield_now().await;
        assert!(!disposal.is_finished(), "target disposal crossed metadata fence");
        runtime.0.reload_after_selector_hook.lock().take();
        tokio::task::spawn_blocking(move || fence.wait()).await.expect("release fence");
        let staged = match reload.await.expect("reload") {
            Ok(
                ReloadOutcome::Completed { new_fiber }
                | ReloadOutcome::CommittedWithCleanupPending { new_fiber, .. },
            )
            | Err(CordisError::ReloadCommitted { new_fiber, .. }) => new_fiber,
            Err(error) => panic!("reload did not commit: {error:?}"),
        };
        let committed = runtime.0.fibers.with(staged, |fiber| {
            (fiber.scope, fiber.staged, fiber.reload_owned)
        });
        if let Some(committed) = committed {
            assert_eq!(committed, (target, false, false));
        }
        disposal.await.expect("disposal").expect("target disposal");
    }

    #[tokio::test]
    async fn reload_postcas_scope_disposal_removes_relocated_service_from_target() {
        let runtime = Runtime::new();
        let target = runtime.create_scope(runtime.root(), "service-target").expect("target");
        let key = ServiceKey::new("reload", "postcas-cleanup", 1);
        let old = runtime.install(target, ServiceRevisionPlugin {
            values: vec![(key.clone(), "old")], task_runs: None,
        }).await.expect("old");
        runtime.reload_detailed(old, ServiceRevisionPlugin {
            values: vec![(key.clone(), "new")], task_runs: None,
        }).await.expect("reload");
        let symbol = runtime.lookup_symbol(&key).expect("symbol");
        assert!(runtime.0.services.get(target, symbol).map(|entry| entry.owner).is_some());
        runtime.dispose_scope(target).await.expect("dispose target");
        assert!(runtime.0.services.get(target, symbol).is_none());
    }

    #[tokio::test]
    async fn reload_staged_task_starts_only_after_committed_fiber_metadata() {
        let runtime = Runtime::new();
        let count = Arc::new(AtomicU64::new(0));
        let (old, staged, staging) = prepare_staged_task_revision(&runtime, count.clone()).await;
        runtime.commit_staged_revision(old, staged, staging, runtime.root()).expect("commit");
        for _ in 0..32 {
            if count.load(Ordering::SeqCst) != 0 { break; }
            tokio::task::yield_now().await;
        }
        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert_eq!(runtime.0.fibers.with(staged, |fiber| {
            (fiber.scope, fiber.staged, fiber.reload_owned)
        }), Some((runtime.root(), false, false)));
    }

    fn resolved_str(runtime: &Runtime, key: &ServiceKey) -> &'static str {
        let symbol = runtime.lookup_symbol(key).expect("symbol");
        let entry = runtime
            .resolve_symbol(runtime.root(), symbol)
            .expect("visible service");
        let ServiceValue::Native(value) = entry.value else {
            panic!("native service")
        };
        **value.downcast::<Arc<&'static str>>().expect("string service")
    }

    #[tokio::test]
    async fn reload_publish_failure_preserves_old_services() {
        let runtime = Runtime::new();
        let key = ServiceKey::new("reload", "single", 1);
        let old = runtime
            .install(
                runtime.root(),
                ServiceRevisionPlugin {
                    values: vec![(key.clone(), "old")],
                    task_runs: None,
                },
            )
            .await
            .expect("old revision");
        let task_runs = Arc::new(AtomicU64::new(0));
        runtime
            .0
            .fail_next_reload_publication
            .store(true, Ordering::SeqCst);
        assert!(matches!(
            runtime
                .reload(
                    old,
                    ServiceRevisionPlugin {
                        values: vec![(key.clone(), "new")],
                        task_runs: Some(task_runs.clone()),
                    },
                )
                .await,
            Err(CordisError::ReloadFailed { primary, .. })
                if matches!(*primary, CordisError::CapabilityPublicationFailed(_))
        ));
        assert_eq!(resolved_str(&runtime, &key), "old");
        assert_eq!(task_runs.load(Ordering::SeqCst), 0);
        assert_eq!(
            runtime
                .snapshot()
                .fibers
                .into_iter()
                .find(|fiber| fiber.id == old)
                .expect("old fiber")
                .state,
            FiberState::Active
        );
        assert_eq!(runtime.0.services.count(), 1);
    }

    #[tokio::test]
    async fn service_handle_holds_generation_lease() {
        let runtime = Runtime::new();
        let key = ServiceKey::new("handle", "lease", 1);
        let cleaned = Arc::new(AtomicU64::new(0));
        let provider = runtime.install(runtime.root(), LeasedServicePlugin {
            key: key.clone(), value: "value", cleaned,
        }).await.expect("provider");
        let (_, context) = retained_context(&runtime).await;
        let handle = context.get::<&'static str>(&key).expect("handle");
        assert_eq!(handle.provider_fiber(), provider);
        assert_eq!(runtime.health().service_handle_inflight, 1);
        assert_eq!(*handle, "value");
        drop(handle);
        assert_eq!(runtime.health().service_handle_inflight, 0);
        assert_eq!(runtime.health().provider_inflight, 0);
    }

    #[tokio::test]
    async fn service_handle_trait_object_is_usable() {
        fn assert_send_sync<T: Send + Sync>(_value: &T) {}

        let runtime = Runtime::new();
        let key = ServiceKey::new("handle", "trait", 1);
        runtime.install(runtime.root(), TraitServicePlugin(key.clone())).await.expect("provider");
        let (_, context) = retained_context(&runtime).await;
        let handle: ServiceHandle<dyn DynTestService> = context.get(&key).expect("trait handle");
        assert_send_sync(&handle);
        assert_eq!(handle.value(), 42);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn old_service_handle_blocks_old_generation_cleanup() {
        let config = RuntimeConfig { task_grace: Duration::from_millis(50), ..RuntimeConfig::default() };
        let runtime = Runtime::with_config(config).expect("runtime");
        let key = ServiceKey::new("handle", "hmr", 1);
        let old_cleaned = Arc::new(AtomicU64::new(0));
        let old = runtime.install(runtime.root(), LeasedServicePlugin {
            key: key.clone(), value: "old", cleaned: old_cleaned.clone(),
        }).await.expect("old provider");
        let (_, context) = retained_context(&runtime).await;
        let old_handle = context.get::<&'static str>(&key).expect("old handle");
        let old_generation = old_handle.generation();
        let new_cleaned = Arc::new(AtomicU64::new(0));
        let reload = {
            let runtime = runtime.clone();
            let key = key.clone();
            tokio::spawn(async move { runtime.reload_detailed(old, LeasedServicePlugin {
                key, value: "new", cleaned: new_cleaned,
            }).await })
        };
        let new_handle = loop {
            if let Ok(handle) = context.get::<&'static str>(&key) {
                if handle.generation() != old_generation { break handle; }
            }
            tokio::task::yield_now().await;
        };
        assert_eq!(*old_handle, "old");
        assert_eq!(*new_handle, "new");
        assert_eq!(old_cleaned.load(Ordering::SeqCst), 0);
        let outcome = reload.await.expect("reload worker").expect("committed reload");
        assert!(matches!(outcome, ReloadOutcome::CommittedWithCleanupPending { .. }));
        assert_eq!(old_cleaned.load(Ordering::SeqCst), 0);
        drop(old_handle);
        for _ in 0..100 {
            if old_cleaned.load(Ordering::SeqCst) == 1 { break; }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(old_cleaned.load(Ordering::SeqCst), 1);
        drop(new_handle);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn service_lookup_racing_hmr_retries_to_new_generation() {
        let runtime = Runtime::new();
        let key = ServiceKey::new("handle", "lookup-race", 1);
        let old = runtime.install(runtime.root(), ServiceRevisionPlugin {
            values: vec![(key.clone(), "old")], task_runs: None,
        }).await.expect("old");
        let (_, context) = retained_context(&runtime).await;
        let fence = Arc::new(std::sync::Barrier::new(2));
        *runtime.0.service_before_gate_hook.lock() = Some(fence.clone());
        let lookup = tokio::task::spawn_blocking({
            let context = context.clone();
            let key = key.clone();
            move || context.get::<&'static str>(&key)
        });
        let entered = fence.clone();
        tokio::task::spawn_blocking(move || entered.wait()).await.expect("lookup snapshot");
        runtime.reload_detailed(old, ServiceRevisionPlugin {
            values: vec![(key.clone(), "new")], task_runs: None,
        }).await.expect("reload");
        tokio::task::spawn_blocking(move || fence.wait()).await.expect("resume lookup");
        let handle = lookup.await.expect("lookup worker").expect("retried handle");
        assert_eq!(*handle, "new");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn provider_effect_cleanup_waits_for_service_handle_drop() {
        let runtime = Runtime::new();
        let key = ServiceKey::new("handle", "dispose", 1);
        let cleaned = Arc::new(AtomicU64::new(0));
        let provider = runtime.install(runtime.root(), LeasedServicePlugin {
            key: key.clone(), value: "live", cleaned: cleaned.clone(),
        }).await.expect("provider");
        let (_, context) = retained_context(&runtime).await;
        let handle = context.get::<&'static str>(&key).expect("handle");
        let disposal = {
            let runtime = runtime.clone();
            tokio::spawn(async move { runtime.dispose_fiber(provider, false).await })
        };
        tokio::task::yield_now().await;
        assert!(!disposal.is_finished());
        assert_eq!(cleaned.load(Ordering::SeqCst), 0);
        assert_eq!(*handle, "live");
        drop(handle);
        disposal.await.expect("disposal worker").expect("dispose");
        assert_eq!(cleaned.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shutdown_retry_converges_after_live_service_handle_drop() {
        let runtime = Runtime::with_config(RuntimeConfig {
            shutdown_grace: Duration::from_millis(20),
            ..RuntimeConfig::default()
        })
        .expect("runtime");
        let key = ServiceKey::new("shutdown", "live-handle", 1);
        let provider = runtime
            .install(
                runtime.root(),
                LeasedServicePlugin {
                    key: key.clone(),
                    value: "live",
                    cleaned: Arc::new(AtomicU64::new(0)),
                },
            )
            .await
            .expect("provider");
        let (_, context) = retained_context(&runtime).await;
        let handle = context.get::<&'static str>(&key).expect("handle");

        let first = runtime.shutdown_detailed().await;
        assert!(matches!(
            first,
            ShutdownOutcome::Incomplete { ref blockers, .. }
                if blockers.iter().any(|blocker| matches!(
                    blocker,
                    ShutdownBlocker::GenerationInflight {
                        fiber,
                        service_handles: 1,
                        ..
                    } if *fiber == provider
                ))
        ));
        assert_eq!(runtime.shutdown_state(), RuntimeShutdownState::Incomplete);
        assert!(matches!(
            runtime.install(runtime.root(), EmptyPlugin).await,
            Err(CordisError::RuntimeShuttingDown)
        ));

        drop(handle);
        assert_eq!(runtime.shutdown_detailed().await, ShutdownOutcome::Complete);
        assert_eq!(runtime.shutdown_state(), RuntimeShutdownState::Complete);
        let snapshot = runtime.snapshot();
        assert_eq!(snapshot.live_fiber_tasks, 0);
        assert_eq!(snapshot.live_runtime_workers, 0);
        assert_eq!(snapshot.provider_inflight, 0);
        assert_eq!(snapshot.service_handle_inflight, 0);
        assert_eq!(snapshot.staging_fibers, 0);
        assert_eq!(snapshot.staging_scopes, 0);
    }

    #[tokio::test]
    async fn service_handle_churn_converges() {
        let runtime = Runtime::new();
        let key = ServiceKey::new("handle", "churn", 1);
        runtime.install(runtime.root(), ServiceRevisionPlugin {
            values: vec![(key.clone(), "value")], task_runs: None,
        }).await.expect("provider");
        let (_, context) = retained_context(&runtime).await;
        for _ in 0..10_000 {
            let handle = context.get::<&'static str>(&key).expect("handle");
            assert_eq!(*handle, "value");
        }
        assert_eq!(runtime.health().service_handle_inflight, 0);
        assert_eq!(runtime.health().provider_inflight, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn old_service_handle_survives_dependency_loss_until_drop() {
        let runtime = Runtime::new();
        let key = ServiceKey::new("handle", "restart", 1);
        let cleaned = Arc::new(AtomicU64::new(0));
        let provider = runtime.install(runtime.root(), LeasedServicePlugin {
            key: key.clone(), value: "generation-n", cleaned,
        }).await.expect("provider");
        let (_, context) = retained_context(&runtime).await;
        let old = context.get::<&'static str>(&key).expect("old handle");
        let old_generation = old.generation();
        let provider_gate = runtime.0.fibers.with(provider, |fiber| {
            fiber.capabilities.clone()
        }).expect("provider gate");
        let disposal = {
            let runtime = runtime.clone();
            tokio::spawn(async move { runtime.dispose_fiber(provider, true).await })
        };
        while provider_gate.execution_snapshot().0 == GenerationExecutionState::Accepting {
            tokio::task::yield_now().await;
        }
        assert_eq!(*old, "generation-n");
        assert!(context.try_get::<&'static str>(&key).expect("loss lookup").is_none());
        assert!(!disposal.is_finished());
        drop(old);
        disposal.await.expect("loss worker").expect("dependency loss");
        runtime.activate(provider).await.expect("fresh generation");
        let fresh = context.get::<&'static str>(&key).expect("fresh handle");
        assert_eq!(*fresh, "generation-n");
        assert_ne!(fresh.generation(), old_generation);
    }

    #[tokio::test]
    async fn caller_context_admission_does_not_pin_provider_after_get_returns() {
        let runtime = Runtime::new();
        let key = ServiceKey::new("handle", "domains", 1);
        let provider = runtime.install(runtime.root(), ServiceRevisionPlugin {
            values: vec![(key.clone(), "provider")], task_runs: None,
        }).await.expect("provider");
        let (caller, context) = retained_context(&runtime).await;
        let handle = context.get::<&'static str>(&key).expect("handle");
        let caller_inflight = runtime.0.fibers.with(caller, |fiber| {
            fiber.capabilities.execution_snapshot().1
        }).expect("caller");
        let provider_inflight = runtime.0.fibers.with(provider, |fiber| {
            fiber.capabilities.execution_snapshot().1
        }).expect("provider");
        assert_eq!(caller_inflight, 0);
        assert_eq!(provider_inflight, 1);
        drop(handle);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn service_handle_reload_churn_converges() {
        let config = RuntimeConfig {
            task_grace: Duration::from_millis(200),
            ..RuntimeConfig::default()
        };
        let runtime = Runtime::with_config(config).expect("runtime");
        let key = ServiceKey::new("handle", "reload-churn", 1);
        let cleaned = Arc::new(AtomicU64::new(0));
        let mut provider = runtime.install(runtime.root(), LeasedServicePlugin {
            key: key.clone(), value: "generation", cleaned: cleaned.clone(),
        }).await.expect("provider");
        let (_, context) = retained_context(&runtime).await;
        let mut old_handle = context.get::<&'static str>(&key).expect("initial handle");
        for _ in 0..100 {
            let previous_generation = old_handle.generation();
            let reload = {
                let runtime = runtime.clone();
                let key = key.clone();
                let cleaned = cleaned.clone();
                tokio::spawn(async move { runtime.reload_detailed(provider, LeasedServicePlugin {
                    key, value: "generation", cleaned,
                }).await })
            };
            let new_handle = loop {
                if let Ok(handle) = context.get::<&'static str>(&key) {
                    if handle.generation() != previous_generation { break handle; }
                }
                tokio::task::yield_now().await;
            };
            drop(old_handle);
            provider = match reload.await.expect("reload worker").expect("reload") {
                ReloadOutcome::Completed { new_fiber }
                | ReloadOutcome::CommittedWithCleanupPending { new_fiber, .. } => new_fiber,
            };
            old_handle = new_handle;
        }
        drop(old_handle);
        wait_for_no_runtime_workers(&runtime).await;
        let snapshot = runtime.snapshot();
        assert_eq!(snapshot.service_handle_inflight, 0);
        assert_eq!(snapshot.provider_inflight, 0);
        assert_eq!(snapshot.draining_generations, 0);
        assert_eq!(snapshot.staging_scopes, 0);
    }

    #[tokio::test]
    async fn reload_precommit_failure_preserves_multiple_old_services() {
        let runtime = Runtime::new();
        let first = ServiceKey::new("reload", "first", 1);
        let second = ServiceKey::new("reload", "second", 1);
        let old = runtime
            .install(
                runtime.root(),
                ServiceRevisionPlugin {
                    values: vec![(first.clone(), "old-first"), (second.clone(), "old-second")],
                    task_runs: None,
                },
            )
            .await
            .expect("old revision");
        runtime
            .0
            .fail_next_reload_publication
            .store(true, Ordering::SeqCst);
        assert!(runtime
            .reload(
                old,
                ServiceRevisionPlugin {
                    values: vec![(first.clone(), "new-first"), (second.clone(), "new-second")],
                    task_runs: None,
                },
            )
            .await
            .is_err());
        assert_eq!(resolved_str(&runtime, &first), "old-first");
        assert_eq!(resolved_str(&runtime, &second), "old-second");
        assert_eq!(runtime.0.services.count(), 2);
    }

    #[tokio::test]
    async fn dropping_reload_caller_during_prepare_does_not_orphan_transaction() {
        for fail in [false, true] {
            let runtime = Runtime::new();
            let old = runtime
                .install(runtime.root(), EmptyPlugin)
                .await
                .expect("old revision");
            let entered = Arc::new(tokio::sync::Notify::new());
            let release = Arc::new(tokio::sync::Semaphore::new(0));
            let caller = tokio::spawn({
                let runtime = runtime.clone();
                let entered = entered.clone();
                let release = release.clone();
                async move {
                    runtime
                        .reload_detailed(
                            old,
                            ReloadGatePlugin {
                                entered,
                                release,
                                fail,
                            },
                        )
                        .await
                }
            });
            entered.notified().await;
            caller.abort();
            assert!(caller.await.expect_err("caller aborted").is_cancelled());
            release.add_permits(1);
            wait_for_no_runtime_workers(&runtime).await;
            let snapshot = runtime.snapshot();
            assert_eq!(snapshot.active_reloads, 0);
            assert_eq!(snapshot.reload_cleanup_pending, 0);
            assert_eq!(snapshot.staging_fibers, 0);
            assert_eq!(snapshot.staging_scopes, 0);
            if fail {
                assert_eq!(
                    snapshot
                        .fibers
                        .iter()
                        .find(|fiber| fiber.id == old)
                        .expect("old fiber")
                        .state,
                    FiberState::Active
                );
            } else {
                assert!(snapshot
                    .fibers
                    .iter()
                    .any(|fiber| fiber.id != old && fiber.state == FiberState::Active));
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn reload_postcommit_drain_timeout_keeps_new_active_and_converges() {
        let runtime = Runtime::with_config(RuntimeConfig {
            task_grace: Duration::from_millis(1),
            ..RuntimeConfig::default()
        })
        .expect("runtime");
        let old = runtime
            .install(runtime.root(), EmptyPlugin)
            .await
            .expect("old revision");
        let old_gate = runtime
            .0
            .fibers
            .get(old)
            .expect("old fiber")
            .inner
            .read()
            .capabilities
            .clone();
        let lease = old_gate.try_acquire().expect("old execution");
        let reload = tokio::spawn({
            let runtime = runtime.clone();
            async move { runtime.reload_detailed(old, EmptyPlugin).await }
        });
        while runtime
            .snapshot()
            .fibers
            .iter()
            .any(|fiber| fiber.id == old && fiber.disposal_phase != DisposalPhase::Draining)
        {
            tokio::task::yield_now().await;
        }
        tokio::time::advance(Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        let outcome = reload.await.expect("reload caller").expect("committed outcome");
        let new = match outcome {
            ReloadOutcome::CommittedWithCleanupPending {
                new_fiber,
                old_fiber,
            } => {
                assert_eq!(old_fiber, old);
                new_fiber
            }
            ReloadOutcome::Completed { .. } => panic!("old lease must force cleanup pending"),
        };
        let snapshot = runtime.snapshot();
        assert_eq!(
            snapshot
                .fibers
                .iter()
                .find(|fiber| fiber.id == new)
                .expect("new fiber")
                .state,
            FiberState::Active
        );
        assert_eq!(snapshot.reload_cleanup_pending, 1);
        assert_eq!(snapshot.active_reloads, 1);
        drop(lease);
        wait_for_no_runtime_workers(&runtime).await;
        let _ = runtime.collect_garbage();
        let snapshot = runtime.snapshot();
        assert_eq!(snapshot.active_reloads, 0);
        assert_eq!(snapshot.reload_cleanup_pending, 0);
        assert_eq!(snapshot.staging_scopes, 0);
        assert_eq!(snapshot.fibers.len(), 1);
        assert_eq!(snapshot.fibers[0].id, new);
    }

    #[tokio::test]
    async fn reload_postcommit_cleanup_failure_reports_committed_state() {
        let runtime = Runtime::new();
        let old = runtime
            .install(runtime.root(), FailingEffectPlugin)
            .await
            .expect("old revision");
        let error = runtime
            .reload_detailed(old, EmptyPlugin)
            .await
            .expect_err("effect cleanup must be reported");
        let new = match error {
            CordisError::ReloadCommitted {
                new_fiber,
                cleanup,
            } => {
                assert!(matches!(*cleanup, CordisError::CleanupFailed(_)));
                new_fiber
            }
            other => panic!("expected committed cleanup failure, got {other:?}"),
        };
        let snapshot = runtime.snapshot();
        assert_eq!(
            snapshot
                .fibers
                .iter()
                .find(|fiber| fiber.id == new)
                .expect("new fiber")
                .state,
            FiberState::Active
        );
        assert!(!runtime
            .0
            .fibers
            .get(old)
            .expect("old retained until gc")
            .inner
            .read()
            .capabilities
            .is_visible());
    }

    #[tokio::test]
    async fn repeated_reload_churn_converges() {
        let runtime = Runtime::new();
        let mut active = runtime
            .install(runtime.root(), EmptyPlugin)
            .await
            .expect("initial revision");
        for _ in 0..200 {
            active = match runtime
                .reload_detailed(active, EmptyPlugin)
                .await
                .expect("reload")
            {
                ReloadOutcome::Completed { new_fiber } => new_fiber,
                ReloadOutcome::CommittedWithCleanupPending { .. } => {
                    panic!("empty revisions should finalize synchronously")
                }
            };
            let _ = runtime.collect_garbage();
        }
        wait_for_no_runtime_workers(&runtime).await;
        let _ = runtime.collect_garbage();
        let snapshot = runtime.snapshot();
        assert_eq!(snapshot.fibers.len(), 1);
        assert_eq!(snapshot.fibers[0].id, active);
        assert_eq!(snapshot.staging_scopes, 0);
        assert_eq!(snapshot.staging_fibers, 0);
        assert_eq!(snapshot.live_runtime_workers, 0);
        assert_eq!(runtime.0.plugins.total_fiber_count(), 1);
    }

    #[tokio::test]
    async fn reload_racing_shutdown_has_single_commit_winner() {
        let runtime = Runtime::new();
        let old = runtime
            .install(runtime.root(), EmptyPlugin)
            .await
            .expect("old revision");
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let reload = tokio::spawn({
            let runtime = runtime.clone();
            let entered = entered.clone();
            let release = release.clone();
            async move {
                runtime
                    .reload_detailed(
                        old,
                        ReloadGatePlugin {
                            entered,
                            release,
                            fail: false,
                        },
                    )
                    .await
            }
        });
        entered.notified().await;
        let shutdown = tokio::spawn({
            let runtime = runtime.clone();
            async move { runtime.shutdown().await }
        });
        while runtime.shutdown_state() == RuntimeShutdownState::Running {
            tokio::task::yield_now().await;
        }
        release.add_permits(1);
        assert!(reload.await.expect("reload join").is_err());
        shutdown.await.expect("shutdown join").expect("shutdown");
        assert_eq!(runtime.shutdown_state(), RuntimeShutdownState::Complete);
        assert_eq!(runtime.snapshot().active_reloads, 0);
    }

    #[tokio::test]
    async fn reload_racing_dispose_has_deterministic_lifecycle_outcome() {
        let runtime = Runtime::new();
        let old = runtime
            .install(runtime.root(), EmptyPlugin)
            .await
            .expect("old revision");
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let reload = tokio::spawn({
            let runtime = runtime.clone();
            let entered = entered.clone();
            let release = release.clone();
            async move {
                runtime
                    .reload_detailed(
                        old,
                        ReloadGatePlugin {
                            entered,
                            release,
                            fail: false,
                        },
                    )
                    .await
            }
        });
        entered.notified().await;
        let disposal = tokio::spawn({
            let runtime = runtime.clone();
            async move { runtime.dispose_fiber(old, false).await }
        });
        while runtime
            .snapshot()
            .fibers
            .iter()
            .any(|fiber| fiber.id == old && fiber.state != FiberState::Disposing)
        {
            tokio::task::yield_now().await;
        }
        release.add_permits(1);
        assert!(reload.await.expect("reload join").is_err());
        disposal.await.expect("dispose join").expect("dispose");
        assert_eq!(
            runtime
                .snapshot()
                .fibers
                .iter()
                .find(|fiber| fiber.id == old)
                .expect("old fiber")
                .state,
            FiberState::Disposed
        );
    }

    #[tokio::test]
    async fn two_reloads_of_same_plugin_do_not_commit_concurrently() {
        let runtime = Runtime::new();
        let old = runtime
            .install(runtime.root(), EmptyPlugin)
            .await
            .expect("old revision");
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let first = tokio::spawn({
            let runtime = runtime.clone();
            let entered = entered.clone();
            let release = release.clone();
            async move {
                runtime
                    .reload_detailed(
                        old,
                        ReloadGatePlugin {
                            entered,
                            release,
                            fail: false,
                        },
                    )
                    .await
            }
        });
        entered.notified().await;
        let second = tokio::spawn({
            let runtime = runtime.clone();
            async move { runtime.reload_detailed(old, EmptyPlugin).await }
        });
        tokio::task::yield_now().await;
        release.add_permits(1);
        let first = first.await.expect("first join");
        let second = second.await.expect("second join");
        assert_ne!(first.is_ok(), second.is_ok());
        wait_for_no_runtime_workers(&runtime).await;
        assert_eq!(
            runtime
                .snapshot()
                .fibers
                .iter()
                .filter(|fiber| fiber.state == FiberState::Active)
                .count(),
            1
        );
    }

    async fn wait_for_hook(hook: &TestDisposalHook) {
        loop {
            let notified = hook.entered_notify.notified();
            if hook.entered.load(Ordering::SeqCst) {
                return;
            }
            notified.await;
        }
    }

    async fn wait_for_effect(gate: &EffectGate) {
        loop {
            let notified = gate.entered_notify.notified();
            if gate.entered.load(Ordering::SeqCst) {
                return;
            }
            notified.await;
        }
    }

    async fn observe_terminal(runtime: &Runtime, fiber: FiberId) -> Result<(), CordisError> {
        loop {
            let notify = {
                let cell = runtime.0.fibers.get(fiber).expect("fiber");
                let record = cell.inner.read();
                if let Some(result) = &record.disposal.result {
                    return result.clone();
                }
                record.disposal.completion.clone()
            };
            let notified = notify.notify.notified();
            if let Some(result) = notify.result() {
                return result;
            }
            notified.await;
        }
    }

    async fn wait_for_registrations(completion: &DisposalCompletion, expected: usize) {
        loop {
            let notified = completion.waiter_notify.notified();
            if completion.waiter_registrations.load(Ordering::SeqCst) >= expected {
                return;
            }
            notified.await;
        }
    }

    async fn wait_for_publish(hook: &TestDisposalHook) {
        loop {
            let notified = hook.published_notify.notified();
            if hook.published.load(Ordering::SeqCst) {
                return;
            }
            notified.await;
        }
    }

    fn set_hook(runtime: &Runtime, fiber: FiberId, hook: Arc<TestDisposalHook>) {
        runtime.0.fibers.with_mut(fiber, |record| record.disposal.test_hook = Some(hook)).expect("fiber");
    }

    fn set_scope_hook(runtime: &Runtime, scope: ScopeId, hook: Arc<TestDisposalHook>) {
        runtime
            .0
            .scopes
            .write()
            .get_mut(scope)
            .expect("scope")
            .disposal
            .test_hook = Some(hook);
    }

    fn abort_scope_body(runtime: &Runtime, scope: ScopeId) {
        runtime
            .0
            .scopes
            .read()
            .get(scope)
            .and_then(|record| record.disposal.body_abort.clone())
            .expect("scope body")
            .abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn panic_before_finalize_wakes_every_waiter_and_shutdown_stays_failed() {
        let runtime = Runtime::new();
        let fiber = runtime
            .install(runtime.root(), EmptyPlugin)
            .await
            .expect("install");
        let hook = Arc::new(TestDisposalHook {
            panic_before_finish: true,
            pause_before_finish: true,
            ..TestDisposalHook::default()
        });
        set_hook(&runtime, fiber, hook.clone());

        let first_runtime = runtime.clone();
        let first = tokio::spawn(async move { first_runtime.dispose_fiber(fiber, false).await });
        wait_for_hook(&hook).await;
        let waiters: Vec<_> = (0..4)
            .map(|_| {
                let runtime = runtime.clone();
                tokio::spawn(async move { runtime.dispose_fiber(fiber, false).await })
            })
            .collect();
        hook.release.notify_one();

        let mut results = tokio::time::timeout(Duration::from_secs(1), join_all(waiters))
            .await
            .expect("waiters must not hang")
            .into_iter()
            .map(|result| result.expect("waiter join"))
            .collect::<Vec<_>>();
        results.push(first.await.expect("first waiter join"));
        assert!(results.iter().all(|result| matches!(
            result,
            Err(CordisError::DisposalWorkerPanicked(message))
                if message.contains("disposal worker test panic")
        )));

        for _ in 0..2 {
            let error = tokio::time::timeout(Duration::from_secs(1), runtime.shutdown())
                .await
                .expect("shutdown must not hang")
                .expect_err("terminated worker keeps shutdown failed");
            assert!(matches!(error, CordisError::CleanupFailed(_)));
            assert_eq!(runtime.shutdown_state(), RuntimeShutdownState::Incomplete);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn aborted_worker_has_terminal_error_and_cannot_be_collected_midflight() {
        let runtime = Runtime::with_config(RuntimeConfig {
            shutdown_grace: Duration::from_millis(20),
            ..RuntimeConfig::default()
        })
        .expect("runtime");
        let fiber = runtime
            .install(runtime.root(), EmptyPlugin)
            .await
            .expect("install");
        let hook = Arc::new(TestDisposalHook {
            pause_before_finish: true,
            ..TestDisposalHook::default()
        });
        set_hook(&runtime, fiber, hook.clone());
        let first_runtime = runtime.clone();
        let first = tokio::spawn(async move { first_runtime.dispose_fiber(fiber, false).await });
        wait_for_hook(&hook).await;
        assert_eq!(runtime.collect_garbage().fibers, 0);

        let second_runtime = runtime.clone();
        let second = tokio::spawn(async move { second_runtime.dispose_fiber(fiber, false).await });
        runtime.abort_disposal_worker(fiber);
        let (first_result, second_result) = tokio::time::timeout(Duration::from_secs(1), async {
            (
                first.await.expect("first join"),
                second.await.expect("second join"),
            )
        })
        .await
        .expect("cancelled worker waiters must not hang");
        assert!(matches!(
            first_result,
            Err(CordisError::DisposalWorkerCancelled)
        ));
        assert!(matches!(
            second_result,
            Err(CordisError::DisposalWorkerCancelled)
        ));
        assert_eq!(runtime.collect_garbage().fibers, 0);
        let shutdown = tokio::time::timeout(Duration::from_secs(1), runtime.shutdown())
            .await
            .expect("shutdown must not hang")
            .expect_err("cancelled worker keeps shutdown failed");
        assert!(matches!(shutdown, CordisError::CleanupFailed(_)));
        assert_eq!(runtime.shutdown_state(), RuntimeShutdownState::Incomplete);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn fast_worker_publication_has_no_stale_handle_race() {
        for _ in 0..32 {
            let runtime = Runtime::new();
            let fiber = runtime
                .install(runtime.root(), EmptyPlugin)
                .await
                .expect("install");
            let waiters = (0..8).map(|_| {
                let runtime = runtime.clone();
                async move { runtime.dispose_fiber(fiber, false).await }
            });
            assert!(
                join_all(waiters)
                    .await
                    .into_iter()
                    .all(|result| result.is_ok())
            );
            if let Some(cell) = runtime.0.fibers.get(fiber) {
                let record = cell.inner.read();
                assert_eq!(record.disposal.phase, DisposalPhase::Complete);
                assert!(record.disposal.result.as_ref().is_some_and(Result::is_ok));
                assert!(record.disposal.supervisor.is_none());
                assert!(record.disposal.worker_abort.is_none());
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn supervisor_finalizes_after_only_shutdown_waiter_is_aborted() {
        let runtime = Runtime::new();
        let gate = Arc::new(EffectGate::default());
        let fiber = runtime
            .install(runtime.root(), BlockingEffectPlugin(gate.clone()))
            .await
            .expect("install");
        let shutdown_runtime = runtime.clone();
        let shutdown = tokio::spawn(async move { shutdown_runtime.shutdown().await });
        wait_for_effect(&gate).await;
        let shutdown_completion = runtime.0.shutdown.completion().expect("shutdown operation");
        shutdown.abort();
        assert!(shutdown.await.expect_err("shutdown aborted").is_cancelled());
        gate.release.notify_one();

        tokio::time::timeout(Duration::from_secs(1), observe_terminal(&runtime, fiber))
            .await
            .expect("supervisor must finalize without a disposal waiter")
            .expect("cleanup result");
        assert_eq!(
            runtime.wait_for_shutdown_completion(shutdown_completion).await,
            ShutdownOutcome::Complete
        );
        assert_eq!(gate.executions.load(Ordering::SeqCst), 1);
        if let Some(cell) = runtime.0.fibers.get(fiber) {
            let record = cell.inner.read();
            assert_eq!(record.state, FiberState::Disposed);
            assert_eq!(record.disposal.phase, DisposalPhase::Complete);
            assert!(record.disposal.result.as_ref().is_some_and(Result::is_ok));
            assert!(record.disposal.supervisor.is_none());
            assert!(record.disposal.worker_abort.is_none());
        }
        assert_eq!(runtime.shutdown_state(), RuntimeShutdownState::Complete);
        tokio::time::timeout(Duration::from_secs(1), runtime.shutdown())
            .await
            .expect("observing shutdown must be immediate")
            .expect("shutdown");
        assert_eq!(runtime.shutdown_state(), RuntimeShutdownState::Complete);
    }

    async fn wait_for_finalizing(hook: &TestDisposalHook) {
        loop {
            let notified = hook.finalizing_notify.notified();
            if hook.finalizing.load(Ordering::SeqCst) {
                return;
            }
            notified.await;
        }
    }

    async fn wait_for_scope_commit_pending(hook: &TestDisposalHook) {
        loop {
            let notified = hook.scope_commit_pending_notify.notified();
            if hook.scope_commit_pending.load(Ordering::SeqCst) {
                return;
            }
            notified.await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shutdown_does_not_hold_coordinator_lock_across_fiber_cleanup() {
        let runtime = Runtime::new();
        let gate = Arc::new(EffectGate::default());
        runtime
            .install(runtime.root(), BlockingEffectPlugin(gate.clone()))
            .await
            .expect("install");

        let shutdown_runtime = runtime.clone();
        let shutdown = tokio::spawn(async move { shutdown_runtime.shutdown().await });
        wait_for_effect(&gate).await;

        tokio::time::timeout(Duration::from_secs(1), async {
            assert_eq!(runtime.shutdown_state(), RuntimeShutdownState::ShuttingDown);
            assert!(runtime.snapshot().shutdown_in_progress);
        })
        .await
        .expect("coordinator lock leaked across fiber cleanup");

        gate.release.notify_one();
        shutdown.await.expect("join").expect("shutdown");
    }

    #[tokio::test(start_paused = true)]
    async fn generation_drain_timeout_does_not_cleanup_live_provider_resources() {
        let config = RuntimeConfig { task_grace: Duration::from_millis(1), ..RuntimeConfig::default() };
        let runtime = Runtime::with_config(config).expect("runtime");
        let cleaned = Arc::new(AtomicU64::new(0));
        let fiber = runtime.install(runtime.root(), CountingCleanupPlugin(cleaned.clone())).await.expect("install");
        let gate = runtime.0.fibers.get(fiber).expect("fiber").inner.read().capabilities.clone();
        let lease = gate.try_acquire().expect("provider lease");
        let dispose_runtime = runtime.clone();
        let disposal = tokio::spawn(async move { dispose_runtime.dispose_fiber(fiber, false).await });
        while runtime.snapshot().fibers.iter().any(|item| item.id == fiber && item.disposal_phase != DisposalPhase::Draining) {
            tokio::task::yield_now().await;
        }
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(matches!(disposal.await.expect("join"), Err(CordisError::CleanupFailed(_))));
        assert_eq!(cleaned.load(Ordering::SeqCst), 0);
        let snapshot = runtime.snapshot().fibers.into_iter().find(|item| item.id == fiber).expect("fiber");
        assert_eq!(snapshot.state, FiberState::Disposing);
        assert_eq!(snapshot.disposal_phase, DisposalPhase::Terminated);
        drop(lease);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn panic_is_finalized_after_all_waiters_are_aborted() {
        let runtime = Runtime::new();
        let fiber = runtime
            .install(runtime.root(), EmptyPlugin)
            .await
            .expect("install");
        let hook = Arc::new(TestDisposalHook {
            panic_before_finish: true,
            pause_before_finish: true,
            ..TestDisposalHook::default()
        });
        set_hook(&runtime, fiber, hook.clone());
        let waiters: Vec<_> = (0..4)
            .map(|_| {
                let runtime = runtime.clone();
                tokio::spawn(async move { runtime.dispose_fiber(fiber, false).await })
            })
            .collect();
        wait_for_hook(&hook).await;
        for waiter in &waiters {
            waiter.abort();
        }
        hook.release.notify_one();
        let result =
            tokio::time::timeout(Duration::from_secs(1), observe_terminal(&runtime, fiber))
                .await
                .expect("panic must be finalized without a waiter");
        assert!(matches!(
            result,
            Err(CordisError::DisposalWorkerPanicked(message))
                if message.contains("disposal worker test panic")
        ));
        let cell = runtime.0.fibers.get(fiber).expect("fiber");
        let record = cell.inner.read();
        assert_eq!(record.disposal.phase, DisposalPhase::Terminated);
        assert!(record.disposal.supervisor.is_none());
        assert!(record.disposal.worker_abort.is_none());
        drop(record);
        let snapshot = runtime
            .snapshot()
            .fibers
            .into_iter()
            .find(|item| item.id == fiber)
            .expect("snapshot");
        assert_eq!(snapshot.disposal_phase, DisposalPhase::Terminated);
        assert!(snapshot.unfinished_cleanup);
        assert!(
            snapshot
                .disposal_error
                .as_deref()
                .is_some_and(|error| error.contains("panicked"))
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn body_abort_is_finalized_without_a_waiter() {
        let runtime = Runtime::new();
        let fiber = runtime
            .install(runtime.root(), EmptyPlugin)
            .await
            .expect("install");
        let hook = Arc::new(TestDisposalHook {
            pause_before_finish: true,
            ..TestDisposalHook::default()
        });
        set_hook(&runtime, fiber, hook.clone());
        let waiter_runtime = runtime.clone();
        let waiter = tokio::spawn(async move { waiter_runtime.dispose_fiber(fiber, false).await });
        wait_for_hook(&hook).await;
        waiter.abort();
        assert!(waiter.await.expect_err("waiter aborted").is_cancelled());
        runtime.abort_disposal_worker(fiber);

        let result =
            tokio::time::timeout(Duration::from_secs(1), observe_terminal(&runtime, fiber))
                .await
                .expect("abort must be finalized without a waiter");
        assert!(matches!(result, Err(CordisError::DisposalWorkerCancelled)));
        let cell = runtime.0.fibers.get(fiber).expect("fiber");
        let record = cell.inner.read();
        assert_eq!(record.state, FiberState::Disposing);
        assert_eq!(record.disposal.phase, DisposalPhase::Terminated);
        assert!(record.disposal.supervisor.is_none());
        assert!(record.disposal.worker_abort.is_none());
    }

    async fn gc_between_publish_and_notify(plugin: impl NativePlugin, expect_error: bool) {
        let runtime = Runtime::new();
        let fiber = runtime
            .install(runtime.root(), plugin)
            .await
            .expect("install");
        let hook = Arc::new(TestDisposalHook {
            pause_before_finish: true,
            pause_after_publish: true,
            ..TestDisposalHook::default()
        });
        set_hook(&runtime, fiber, hook.clone());
        let completion = runtime.0.fibers.with(fiber, |record| record.disposal.completion.clone()).expect("fiber");
        let waiters: Vec<_> = (0..8)
            .map(|_| {
                let runtime = runtime.clone();
                tokio::spawn(async move { runtime.dispose_fiber(fiber, false).await })
            })
            .collect();
        let detailed_waiters: Vec<_> = (0..4)
            .map(|_| {
                let runtime = runtime.clone();
                tokio::spawn(async move { runtime.dispose_fiber_detailed(fiber, false).await })
            })
            .collect();
        wait_for_hook(&hook).await;
        wait_for_registrations(&completion, waiters.len() + detailed_waiters.len()).await;
        hook.release.notify_one();
        wait_for_publish(&hook).await;
        assert_eq!(runtime.collect_garbage().fibers, 1);
        hook.release_after_publish.notify_one();

        let results = tokio::time::timeout(Duration::from_secs(1), join_all(waiters))
            .await
            .expect("registered waiters must survive GC");
        for result in results {
            let result = result.expect("waiter join");
            if expect_error {
                assert!(matches!(result, Err(CordisError::PluginDisposeFailed(_))));
            } else {
                result.expect("successful disposal result");
            }
        }
        for result in join_all(detailed_waiters).await {
            let outcome = result.expect("detailed waiter join");
            if expect_error {
                assert!(matches!(outcome, DisposeOutcome::CommittedWithCleanupIssues { ref issues } if issues.len() == 1));
            } else {
                assert_eq!(outcome, DisposeOutcome::Disposed);
            }
        }
        assert!(matches!(runtime.dispose_fiber_detailed(fiber, false).await,
            DisposeOutcome::Incomplete { primary: CordisError::FiberNotFound, .. }));
        assert!(matches!(
            runtime.dispose_fiber(fiber, false).await,
            Err(CordisError::FiberNotFound)
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn registered_waiters_keep_success_result_across_gc() {
        gc_between_publish_and_notify(EmptyPlugin, false).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn registered_waiters_keep_cleanup_error_across_gc() {
        gc_between_publish_and_notify(FailingEffectPlugin, true).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn terminal_fiber_and_scope_are_eventually_auto_collected() {
        let runtime = Runtime::new();
        let scope = runtime.create_scope(runtime.root(), "auto-gc").expect("scope");
        let fiber = runtime.install(scope, EmptyPlugin).await.expect("fiber");
        runtime.dispose_fiber(fiber, false).await.expect("dispose fiber");
        wait_for_no_runtime_workers(&runtime).await;
        assert!(runtime.0.fibers.get(fiber).is_none());
        assert_eq!(runtime.0.plugins.total_fiber_count(), 0);

        runtime.dispose_scope(scope).await.expect("dispose scope");
        wait_for_no_runtime_workers(&runtime).await;
        assert!(runtime.0.scopes.read().get(scope).is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn coalesced_auto_gc_converges_disposal_churn() {
        let runtime = Runtime::new();
        let mut fibers = Vec::new();
        for _ in 0..100 {
            fibers.push(runtime.install(runtime.root(), EmptyPlugin).await.expect("fiber"));
        }
        let disposals = fibers.iter().copied().map(|fiber| {
            let runtime = runtime.clone();
            async move { runtime.dispose_fiber(fiber, false).await }
        });
        for result in join_all(disposals).await {
            result.expect("dispose");
        }
        wait_for_no_runtime_workers(&runtime).await;
        assert!(runtime.snapshot().fibers.is_empty());
        assert_eq!(runtime.0.plugins.total_fiber_count(), 0);
        assert_eq!(runtime.0.gc_state.load(Ordering::Acquire), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shutdown_waiter_keeps_completion_across_concurrent_gc() {
        let runtime = Runtime::new();
        let fiber = runtime
            .install(runtime.root(), EmptyPlugin)
            .await
            .expect("install");
        let hook = Arc::new(TestDisposalHook {
            pause_before_finish: true,
            pause_after_publish: true,
            ..TestDisposalHook::default()
        });
        set_hook(&runtime, fiber, hook.clone());
        let completion = runtime.0.fibers.with(fiber, |record| record.disposal.completion.clone()).expect("fiber");
        let shutdown_runtime = runtime.clone();
        let shutdown = tokio::spawn(async move { shutdown_runtime.shutdown().await });
        wait_for_hook(&hook).await;
        wait_for_registrations(&completion, 1).await;
        hook.release.notify_one();
        wait_for_publish(&hook).await;
        assert_eq!(runtime.collect_garbage().fibers, 1);
        hook.release_after_publish.notify_one();

        tokio::time::timeout(Duration::from_secs(1), shutdown)
            .await
            .expect("shutdown waiter must survive GC")
            .expect("shutdown join")
            .expect("shutdown result");
        assert_eq!(runtime.shutdown_state(), RuntimeShutdownState::Complete);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shutdown_complete_prevents_post_complete_gc_worker_registration() {
        let runtime = Runtime::new();
        let fiber = runtime.install(runtime.root(), EmptyPlugin).await.expect("fiber");
        let hook = Arc::new(TestDisposalHook {
            pause_after_publish: true,
            ..TestDisposalHook::default()
        });
        set_hook(&runtime, fiber, hook.clone());
        let completion = runtime
            .0
            .fibers
            .with(fiber, |record| record.disposal.completion.clone())
            .expect("fiber");
        let shutdown_runtime = runtime.clone();
        let shutdown = tokio::spawn(async move { shutdown_runtime.shutdown_detailed().await });
        wait_for_publish(&hook).await;
        completion.notify.notify_waiters();
        let outcome = tokio::time::timeout(Duration::from_secs(1), shutdown)
            .await
            .expect("shutdown must complete while finalizer is paused")
            .expect("shutdown join");
        assert_eq!(outcome, ShutdownOutcome::Complete);
        assert_eq!(runtime.shutdown_state(), RuntimeShutdownState::Complete);
        let before = runtime.snapshot();
        assert_eq!(before.live_runtime_workers, 0);
        assert_eq!(runtime.0.gc_state.load(Ordering::Acquire), 0);

        hook.release_after_publish.notify_one();
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        let after = runtime.snapshot();
        assert_eq!(after.live_runtime_workers, 0);
        assert_eq!(runtime.0.gc_state.load(Ordering::Acquire), 0);
        assert_eq!(after.fibers.len(), before.fibers.len());
        assert_eq!(after.scopes.len(), before.scopes.len());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn gc_registration_winning_before_shutdown_is_drained_by_shutdown() {
        let runtime = Runtime::new();
        let fence = Arc::new(std::sync::Barrier::new(2));
        *runtime.0.gc_registration_hook.lock() = Some(fence.clone());
        let gc_runtime = runtime.clone();
        let request = tokio::task::spawn_blocking(move || gc_runtime.request_gc());
        let entered = fence.clone();
        tokio::task::spawn_blocking(move || entered.wait())
            .await
            .expect("GC admission fence");

        let shutdown_runtime = runtime.clone();
        let shutdown = tokio::spawn(async move { shutdown_runtime.shutdown_detailed().await });
        tokio::task::yield_now().await;
        tokio::task::spawn_blocking(move || fence.wait())
            .await
            .expect("release GC registration");
        request.await.expect("GC request");
        runtime.0.gc_registration_hook.lock().take();
        let outcome = tokio::time::timeout(Duration::from_secs(1), shutdown)
            .await
            .expect("shutdown must drain admitted GC")
            .expect("shutdown join");
        assert_eq!(outcome, ShutdownOutcome::Complete);
        assert_eq!(runtime.0.workers.live(), 0);
        assert_eq!(runtime.0.gc_state.load(Ordering::Acquire), 0);
        assert!(runtime.0.workers.reaped() >= 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn terminated_fiber_propagates_through_four_scope_levels() {
        let runtime = Runtime::new();
        let one = runtime.create_scope(runtime.root(), "one").expect("one");
        let two = runtime.create_scope(one, "two").expect("two");
        let sibling = runtime.create_scope(two, "sibling").expect("sibling");
        let three = runtime.create_scope(two, "three").expect("three");
        let sibling_cleaned = Arc::new(AtomicU64::new(0));
        runtime
            .install(sibling, CountingCleanupPlugin(sibling_cleaned.clone()))
            .await
            .expect("sibling plugin");
        let fiber = runtime
            .install(three, EmptyPlugin)
            .await
            .expect("install terminated fiber");
        let hook = Arc::new(TestDisposalHook {
            panic_before_finish: true,
            ..TestDisposalHook::default()
        });
        set_hook(&runtime, fiber, hook);

        let error = runtime.shutdown().await.expect_err("terminal shutdown");
        assert!(matches!(error, CordisError::CleanupFailed(_)));
        assert_eq!(runtime.shutdown_state(), RuntimeShutdownState::Incomplete);
        let snapshot = runtime.snapshot();
        for scope in [runtime.root(), one, two, three] {
            let item = snapshot
                .scopes
                .iter()
                .find(|item| item.id == scope)
                .expect("scope snapshot");
            assert_eq!(item.state, ScopeState::Terminated);
            assert!(item.unfinished_cleanup);
        }
        assert_eq!(snapshot.terminated_scope_count, 4);
        assert_eq!(sibling_cleaned.load(Ordering::SeqCst), 1);
        assert!(snapshot
            .scopes
            .iter()
            .find(|item| item.id == sibling)
            .is_none_or(|item| item.state == ScopeState::Disposed));
        assert_eq!(
            runtime.collect_garbage().scopes,
            0,
            "disposed sibling may already be auto-collected"
        );
        assert_eq!(
            runtime.snapshot().terminated_scope_count,
            4,
            "terminated ancestors remain allocated"
        );
        assert_eq!(
            runtime.shutdown().await.expect_err("stable terminal"),
            error
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn completed_cleanup_error_does_not_mark_scope_tree_terminated() {
        let runtime = Runtime::new();
        let one = runtime.create_scope(runtime.root(), "one").expect("one");
        let two = runtime.create_scope(one, "two").expect("two");
        runtime
            .install(two, FailingEffectPlugin)
            .await
            .expect("failing cleanup plugin");

        let error = runtime.shutdown().await.expect_err("cleanup error");
        assert!(matches!(error, CordisError::CleanupFailed(_)));
        let snapshot = runtime.snapshot();
        assert!(
            snapshot
                .scopes
                .iter()
                .all(|scope| scope.state == ScopeState::Disposed)
        );
        assert_eq!(snapshot.terminated_scope_count, 0);
        let report = runtime.collect_garbage();
        assert_eq!(report.scopes, 0, "shutdown performs its final GC pass");
        assert_eq!(
            runtime.shutdown().await.expect_err("stable cleanup error"),
            error
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn scope_body_panic_and_abort_publish_terminal_results() {
        for panic in [true, false] {
            let runtime = Runtime::new();
            let scope = runtime
                .create_scope(runtime.root(), "terminal")
                .expect("scope");
            let hook = Arc::new(TestDisposalHook {
                panic_before_finish: panic,
                pause_before_finish: !panic,
                ..TestDisposalHook::default()
            });
            set_scope_hook(&runtime, scope, hook.clone());
            let waiters: Vec<_> = (0..8)
                .map(|_| {
                    let runtime = runtime.clone();
                    tokio::spawn(async move { runtime.dispose_scope(scope).await })
                })
                .collect();
            if !panic {
                wait_for_hook(&hook).await;
                abort_scope_body(&runtime, scope);
            }
            let results = tokio::time::timeout(Duration::from_secs(1), join_all(waiters))
                .await
                .expect("scope waiters");
            for result in results {
                let error = result.expect("join").expect_err("terminal scope");
                if panic {
                    assert!(matches!(error, CordisError::ScopeDisposalPanicked(_)));
                } else {
                    assert_eq!(error, CordisError::ScopeDisposalCancelled);
                }
            }
            let snapshot = runtime.snapshot();
            assert_eq!(
                snapshot
                    .scopes
                    .iter()
                    .find(|item| item.id == scope)
                    .expect("scope")
                    .state,
                ScopeState::Terminated
            );
            assert_eq!(runtime.collect_garbage().scopes, 0);
        }
    }

    async fn scope_gc_between_publish_and_notify(plugin: Option<FailingEffectPlugin>) {
        let runtime = Runtime::new();
        let scope = runtime.create_scope(runtime.root(), "gc").expect("scope");
        let expect_error = plugin.is_some();
        if let Some(plugin) = plugin {
            runtime.install(scope, plugin).await.expect("plugin");
        }
        let hook = Arc::new(TestDisposalHook {
            pause_before_finish: true,
            pause_after_publish: true,
            ..TestDisposalHook::default()
        });
        set_scope_hook(&runtime, scope, hook.clone());
        let completion = runtime
            .0
            .scopes
            .read()
            .get(scope)
            .expect("scope")
            .disposal
            .completion
            .clone();
        let waiters: Vec<_> = (0..8)
            .map(|_| {
                let runtime = runtime.clone();
                tokio::spawn(async move { runtime.dispose_scope(scope).await })
            })
            .collect();
        let detailed_waiters: Vec<_> = (0..4)
            .map(|_| {
                let runtime = runtime.clone();
                tokio::spawn(async move { runtime.dispose_scope_detailed(scope).await })
            })
            .collect();
        wait_for_hook(&hook).await;
        wait_for_registrations(&completion, waiters.len() + detailed_waiters.len()).await;
        hook.release.notify_one();
        wait_for_publish(&hook).await;
        assert!(runtime.collect_garbage().scopes <= 1);
        hook.release_after_publish.notify_one();
        let results = join_all(waiters).await;
        for result in results {
            let result = result.expect("join");
            if expect_error {
                assert!(matches!(result, Err(CordisError::CleanupFailed(_))));
            } else {
                result.expect("scope result");
            }
        }
        for result in join_all(detailed_waiters).await {
            let outcome = result.expect("detailed scope waiter join");
            if expect_error {
                assert!(matches!(outcome, ScopeDisposeOutcome::CommittedWithCleanupIssues { ref issues } if issues.len() == 1));
            } else {
                assert_eq!(outcome, ScopeDisposeOutcome::Disposed);
            }
        }
        assert!(matches!(runtime.dispose_scope_detailed(scope).await,
            ScopeDisposeOutcome::Incomplete { primary: CordisError::ScopeNotFound, .. }));
        assert!(matches!(
            runtime.dispose_scope(scope).await,
            Err(CordisError::ScopeNotFound)
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn scope_registered_completion_survives_gc_and_fresh_request_is_not_found() {
        scope_gc_between_publish_and_notify(None).await;
        scope_gc_between_publish_and_notify(Some(FailingEffectPlugin)).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn one_hundred_registered_scope_waiters_and_shutdown_operations_have_one_result() {
        for _ in 0..100 {
            let runtime = Runtime::new();
            let scope = runtime.create_scope(runtime.root(), "fast").expect("scope");
            let hook = Arc::new(TestDisposalHook {
                pause_before_finish: true,
                ..TestDisposalHook::default()
            });
            set_scope_hook(&runtime, scope, hook.clone());
            let completion = runtime
                .0
                .scopes
                .read()
                .get(scope)
                .expect("scope")
                .disposal
                .completion
                .clone();
            let scopes: Vec<_> = (0..4)
                .map(|_| {
                    let runtime = runtime.clone();
                    tokio::spawn(async move { runtime.dispose_scope(scope).await })
                })
                .collect();
            wait_for_hook(&hook).await;
            wait_for_registrations(&completion, scopes.len()).await;
            hook.release.notify_one();
            assert!(join_all(scopes)
                .await
                .into_iter()
                .all(|result| result.expect("scope waiter join").is_ok()));
            let shutdowns = (0..4).map(|_| {
                let runtime = runtime.clone();
                async move { runtime.shutdown().await }
            });
            assert!(
                join_all(shutdowns)
                    .await
                    .into_iter()
                    .all(|result| result.is_ok())
            );
            assert!(runtime.0.shutdown.supervisor_finished());
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_parent_and_child_share_child_completion() {
        let runtime = Runtime::new();
        let parent = runtime
            .create_scope(runtime.root(), "parent")
            .expect("parent");
        let child = runtime.create_scope(parent, "child").expect("child");
        let cleaned = Arc::new(AtomicU64::new(0));
        runtime
            .install(child, CountingCleanupPlugin(cleaned.clone()))
            .await
            .expect("plugin");
        let hook = Arc::new(TestDisposalHook {
            pause_before_finish: true,
            ..TestDisposalHook::default()
        });
        set_scope_hook(&runtime, child, hook.clone());
        let child_runtime = runtime.clone();
        let child_waiter = tokio::spawn(async move { child_runtime.dispose_scope(child).await });
        wait_for_hook(&hook).await;
        let parent_runtime = runtime.clone();
        let parent_waiter = tokio::spawn(async move { parent_runtime.dispose_scope(parent).await });
        hook.release.notify_one();
        child_waiter
            .await
            .expect("child join")
            .expect("child result");
        parent_waiter
            .await
            .expect("parent join")
            .expect("parent result");
        assert_eq!(cleaned.load(Ordering::SeqCst), 1);
        let snapshot = runtime.snapshot();
        assert!(snapshot
            .scopes
            .iter()
            .find(|item| item.id == parent)
            .is_none_or(|item| item.state == ScopeState::Disposed));
    }

    async fn assert_scope_publication_follows_parent_unlink(with_issue: bool) {
        let runtime = Runtime::new();
        let parent = runtime
            .create_scope(runtime.root(), "publication-parent")
            .expect("parent");
        let scope = runtime
            .create_scope(parent, "publication-child")
            .expect("scope");
        if with_issue {
            runtime
                .install(scope, FailingEffectPlugin)
                .await
                .expect("cleanup issue plugin");
        }
        let hook = Arc::new(TestDisposalHook {
            pause_before_scope_topology_commit: true,
            ..TestDisposalHook::default()
        });
        set_scope_hook(&runtime, scope, hook.clone());
        let completion = runtime
            .0
            .scopes
            .read()
            .get(scope)
            .expect("scope")
            .disposal
            .completion
            .clone();
        let legacy_runtime = runtime.clone();
        let legacy = tokio::spawn(async move { legacy_runtime.dispose_scope(scope).await });
        let detailed: Vec<_> = (0..2)
            .map(|_| {
                let runtime = runtime.clone();
                tokio::spawn(async move { runtime.dispose_scope_detailed(scope).await })
            })
            .collect();

        wait_for_scope_commit_pending(&hook).await;
        assert!(completion.observation().is_none());
        assert!(!legacy.is_finished());
        assert!(detailed.iter().all(|waiter| !waiter.is_finished()));
        {
            let scopes = runtime.0.scopes.read();
            assert_eq!(scopes.get(scope).expect("scope").state, ScopeState::Disposing);
            assert!(scopes.get(parent).expect("parent").children.contains(&scope));
        }

        hook.release_scope_topology_commit.notify_one();
        let legacy_result = legacy.await.expect("legacy join");
        if with_issue {
            assert!(matches!(legacy_result, Err(CordisError::CleanupFailed(_))));
        } else {
            legacy_result.expect("legacy disposal");
        }
        for outcome in join_all(detailed).await {
            let outcome = outcome.expect("detailed join");
            if with_issue {
                assert!(matches!(outcome, ScopeDisposeOutcome::CommittedWithCleanupIssues { ref issues } if issues.len() == 1));
            } else {
                assert_eq!(outcome, ScopeDisposeOutcome::Disposed);
            }
        }
        assert!(completion.observation().is_some());
        let scopes = runtime.0.scopes.read();
        assert!(scopes
            .get(scope)
            .is_none_or(|record| record.state == ScopeState::Disposed));
        assert!(!scopes.get(parent).expect("parent").children.contains(&scope));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn scope_completion_is_not_visible_before_parent_unlink() {
        assert_scope_publication_follows_parent_unlink(false).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn scope_committed_cleanup_issue_outcome_implies_parent_unlinked() {
        assert_scope_publication_follows_parent_unlink(true).await;
    }

    async fn assert_fiber_publication_linearizes_indexes(fail_cleanup: bool) {
        let runtime = Runtime::new();
        let scope = runtime
            .create_scope(runtime.root(), "publication")
            .expect("scope");
        let dependency = ServiceKey::new("disposal", "dependency", 1);
        runtime
            .install(
                scope,
                ServiceRevisionPlugin {
                    values: vec![(dependency.clone(), "dependency")],
                    task_runs: None,
                },
            )
            .await
            .expect("dependency provider");
        let key = ServiceKey::new("disposal", "linearization", 1);
        let dependency_symbol = runtime.intern_service(&dependency).expect("dependency symbol");
        let symbol = runtime.intern_service(&key).expect("service symbol");
        let fiber = runtime
            .install(
                scope,
                IndexedDisposalPlugin {
                    dependency,
                    key,
                    fail_cleanup,
                },
            )
            .await
            .expect("install");
        assert!(runtime.0.dependencies.providers(scope, symbol).contains(&fiber));
        assert!(runtime
            .0
            .dependencies
            .dependents(dependency_symbol)
            .contains(&fiber));
        let hook = Arc::new(TestDisposalHook {
            pause_before_index_cleanup: true,
            ..TestDisposalHook::default()
        });
        set_hook(&runtime, fiber, hook.clone());
        let completion = runtime
            .0
            .fibers
            .with(fiber, |record| record.disposal.completion.clone())
            .expect("fiber");
        let waiter_runtime = runtime.clone();
        let waiter = tokio::spawn(async move {
            waiter_runtime.dispose_fiber_detailed(fiber, false).await
        });

        wait_for_finalizing(&hook).await;
        assert!(completion.observation().is_none());
        assert!(!waiter.is_finished());
        {
            let cell = runtime.0.fibers.get(fiber).expect("finalizing fiber");
            let record = cell.inner.read();
            assert_eq!(record.state, FiberState::Disposing);
            assert_eq!(record.disposal.phase, DisposalPhase::Finalizing);
        }
        assert!(runtime.0.scopes.read().get(scope).expect("scope").fibers.contains(&fiber));
        assert!(runtime.0.dependencies.providers(scope, symbol).contains(&fiber));
        assert!(runtime
            .0
            .dependencies
            .dependents(dependency_symbol)
            .contains(&fiber));
        assert_eq!(runtime.collect_garbage().fibers, 0);

        hook.release_index_cleanup.notify_one();
        let outcome = waiter.await.expect("waiter join");
        if fail_cleanup {
            assert!(matches!(outcome, DisposeOutcome::CommittedWithCleanupIssues { ref issues } if issues.len() == 1));
        } else {
            assert_eq!(outcome, DisposeOutcome::Disposed);
        }
        assert!(completion.observation().is_some());
        assert!(!runtime.0.scopes.read().get(scope).expect("scope").fibers.contains(&fiber));
        assert!(!runtime.0.dependencies.providers(scope, symbol).contains(&fiber));
        assert!(!runtime
            .0
            .dependencies
            .dependents(dependency_symbol)
            .contains(&fiber));
        assert!(runtime
            .0
            .fibers
            .get(fiber)
            .is_none_or(|cell| cell.inner.read().state == FiberState::Disposed));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn detailed_fiber_waiter_cannot_complete_before_index_cleanup() {
        assert_fiber_publication_linearizes_indexes(false).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn committed_cleanup_issue_outcome_implies_indexes_already_detached() {
        assert_fiber_publication_linearizes_indexes(true).await;
    }

    #[tokio::test]
    async fn activation_collects_multiple_cleanup_issues_without_losing_primary() {
        let runtime = Runtime::new();
        let error = runtime.install(runtime.root(), MultiCleanupFailingStartPlugin).await.expect_err("start must fail");
        match error {
            CordisError::ActivationFailed { primary, cleanup } => {
                assert!(matches!(*primary, CordisError::PluginStartFailed(ref message) if message.contains("primary")));
                assert_eq!(cleanup.len(), 2);
                assert!(cleanup[0].to_string().contains("cleanup-b"));
                assert!(cleanup[1].to_string().contains("cleanup-a"));
            }
            other => panic!("expected structured activation failure, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn disposal_reports_committed_truth_with_cleanup_issue() {
        let runtime = Runtime::new();
        let fiber = runtime.install(runtime.root(), FailingEffectPlugin).await.expect("install");
        match runtime.dispose_fiber_detailed(fiber, false).await {
            DisposeOutcome::CommittedWithCleanupIssues { issues } => {
                assert_eq!(issues.len(), 1);
                assert_eq!(issues[0].phase, CleanupPhase::EffectCleanup);
            }
            other => panic!("expected committed cleanup issue, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn scope_disposal_preserves_scope_truth_when_child_cleanup_fails() {
        let runtime = Runtime::new();
        let scope = runtime.create_scope(runtime.root(), "faulty").expect("scope");
        runtime.install(scope, FailingEffectPlugin).await.expect("install");
        let outcome = runtime.dispose_scope_detailed(scope).await;
        assert!(matches!(outcome, ScopeDisposeOutcome::CommittedWithCleanupIssues { ref issues }
            if issues.len() == 1 && issues[0].phase == CleanupPhase::FiberDetach));
    }

    #[tokio::test]
    async fn dropped_install_observer_does_not_orphan_commit_failure_rollback() {
        let runtime = Runtime::new();
        let key = InvocationKey::new("activation", "commit-conflict", 1);
        let entered = Arc::new(tokio::sync::Semaphore::new(0));
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let install_runtime = runtime.clone();
        let observer = tokio::spawn({
            let key = key.clone();
            let entered = entered.clone();
            let release = release.clone();
            async move {
                install_runtime
                    .install(install_runtime.root(), BlockingInvocationPlugin { key, entered, release })
                    .await
            }
        });
        entered.acquire().await.expect("start entered").forget();
        observer.abort();

        let winner_context = Arc::new(Mutex::new(None));
        runtime
            .install(
                runtime.root(),
                RetainInvocationContextPlugin {
                    slot: winner_context.clone(),
                    key: key.clone(),
                },
            )
            .await
            .expect("conflict winner");
        release.add_permits(1);
        wait_for_no_runtime_workers(&runtime).await;

        assert!(runtime.snapshot().fibers.iter().all(|fiber| fiber.state != FiberState::Starting));
        let context = winner_context.lock().clone().expect("winner context");
        context
            .invoke(&key, InvocationValue::native(Arc::new(())))
            .await
            .expect("winning invocation remains usable");
        assert_eq!(runtime.snapshot().live_runtime_workers, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn activation_commit_failure_rolls_back_staged_generation() {
        let runtime = Runtime::new();
        let key = InvocationKey::new("activation", "observed-conflict", 1);
        let entered = Arc::new(tokio::sync::Semaphore::new(0));
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let install_runtime = runtime.clone();
        let observer = tokio::spawn({
            let key = key.clone();
            let entered = entered.clone();
            let release = release.clone();
            async move {
                install_runtime
                    .install(install_runtime.root(), BlockingInvocationPlugin { key, entered, release })
                    .await
            }
        });
        entered.acquire().await.expect("start entered").forget();
        runtime
            .install(
                runtime.root(),
                RetainInvocationContextPlugin { slot: Arc::new(Mutex::new(None)), key: key.clone() },
            )
            .await
            .expect("winner");
        release.add_permits(1);
        let error = observer.await.expect("observer join").expect_err("commit conflict");
        assert!(matches!(error, CordisError::DuplicateInvocationHandler(ref actual) if actual == &key));
        assert!(runtime.snapshot().fibers.iter().all(|fiber| fiber.state != FiberState::Starting));
        assert_eq!(runtime.snapshot().live_fiber_tasks, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shutdown_winning_after_activation_validation_rolls_back_before_publish() {
        let runtime = Runtime::new();
        let hook = Arc::new(TestActivationCommitHook::default());
        *runtime.0.activation_before_commit_hook.lock() = Some(hook.clone());
        let install_runtime = runtime.clone();
        let install = tokio::spawn(async move { install_runtime.install(install_runtime.root(), EmptyPlugin).await });
        loop {
            let notified = hook.entered_notify.notified();
            if hook.entered.load(Ordering::SeqCst) { break; }
            notified.await;
        }
        let shutdown_runtime = runtime.clone();
        let shutdown = tokio::spawn(async move { shutdown_runtime.shutdown().await });
        while runtime.shutdown_state() == RuntimeShutdownState::Running {
            tokio::task::yield_now().await;
        }
        hook.release.notify_waiters();
        let error = install.await.expect("install join").expect_err("shutdown wins");
        assert!(matches!(error, CordisError::RuntimeShuttingDown | CordisError::ActivationFailed { .. }));
        shutdown.await.expect("shutdown join").expect("shutdown");
        assert!(runtime.snapshot().fibers.iter().all(|fiber| fiber.state != FiberState::Starting));
        assert_eq!(runtime.snapshot().live_fiber_tasks, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn fiber_incomplete_outcome_preserves_terminal_blocker_after_prior_issue() {
        let config = RuntimeConfig { task_grace: Duration::from_millis(1), ..RuntimeConfig::default() };
        let runtime = Runtime::with_config(config).expect("runtime");
        let cleaned = Arc::new(AtomicU64::new(0));
        let fiber = runtime.install(runtime.root(), CountingCleanupPlugin(cleaned.clone())).await.expect("install");
        let gate = runtime.0.fibers.get(fiber).expect("fiber").inner.read().capabilities.clone();
        let lease = gate.try_acquire().expect("provider lease");
        runtime.push_disposal_error(
            fiber,
            CleanupPhase::TaskDrain,
            CordisError::CleanupFailed("prior task issue".into()),
        );
        let dispose_runtime = runtime.clone();
        let disposal = tokio::spawn(async move { dispose_runtime.dispose_fiber_detailed(fiber, false).await });
        while runtime.snapshot().fibers.iter().any(|item| item.id == fiber && item.disposal_phase != DisposalPhase::Draining) {
            tokio::task::yield_now().await;
        }
        tokio::time::advance(Duration::from_millis(1)).await;
        match disposal.await.expect("join") {
            DisposeOutcome::Incomplete { primary, issues } => {
                assert!(primary.to_string().contains("generation drain timed out"));
                assert_eq!(issues.len(), 1);
                assert_eq!(issues[0].phase, CleanupPhase::TaskDrain);
            }
            other => panic!("expected incomplete outcome, got {other:?}"),
        }
        assert_eq!(cleaned.load(Ordering::SeqCst), 0);
        assert_eq!(runtime.snapshot().fibers.iter().find(|item| item.id == fiber).expect("fiber").state, FiberState::Disposing);
        drop(lease);
    }

    #[test]
    fn activation_primary_issue_and_terminal_blocker_are_all_preserved() {
        let cleanup = Runtime::activation_cleanup(Ok(Arc::new(DisposalObservation {
            legacy_result: Err(CordisError::CleanupFailed("generation drain timed out".into())),
            terminal: DisposalTerminal::Incomplete(CordisError::CleanupFailed(
                "generation drain timed out".into(),
            )),
            issues: vec![CleanupIssue {
                phase: CleanupPhase::TaskDrain,
                message: "prior task issue".into(),
                cause: None,
            }],
        })));
        let error = Runtime::activation_failure(
            CordisError::DuplicateInvocationHandler(InvocationKey::new("primary", "conflict", 1)),
            cleanup,
        );
        match error {
            CordisError::ActivationFailed { primary, cleanup } => {
                assert!(matches!(*primary, CordisError::DuplicateInvocationHandler(_)));
                assert_eq!(cleanup.len(), 2);
                assert!(cleanup[0].to_string().contains("prior task issue"));
                assert!(cleanup[1].to_string().contains("generation drain timed out"));
            }
            other => panic!("expected activation failure, got {other:?}"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn scope_incomplete_outcome_preserves_terminal_blocker_and_child_issue() {
        let config = RuntimeConfig { task_grace: Duration::from_millis(1), ..RuntimeConfig::default() };
        let runtime = Runtime::with_config(config).expect("runtime");
        let scope = runtime.create_scope(runtime.root(), "incomplete").expect("scope");
        let fiber = runtime.install(scope, EmptyPlugin).await.expect("install");
        let gate = runtime.0.fibers.get(fiber).expect("fiber").inner.read().capabilities.clone();
        let lease = gate.try_acquire().expect("provider lease");
        let dispose_runtime = runtime.clone();
        let disposal = tokio::spawn(async move { dispose_runtime.dispose_scope_detailed(scope).await });
        while runtime.snapshot().fibers.iter().any(|item| item.id == fiber && item.disposal_phase != DisposalPhase::Draining) {
            tokio::task::yield_now().await;
        }
        tokio::time::advance(Duration::from_millis(1)).await;
        match disposal.await.expect("join") {
            ScopeDisposeOutcome::Incomplete { primary, issues } => {
                assert!(primary.to_string().contains("scope disposal terminated"));
                assert_eq!(issues.len(), 1);
                assert_eq!(issues[0].phase, CleanupPhase::FiberDetach);
                assert!(issues[0].message.contains("generation drain timed out"));
            }
            other => panic!("expected incomplete scope outcome, got {other:?}"),
        }
        assert_eq!(runtime.snapshot().scopes.iter().find(|item| item.id == scope).expect("scope").state, ScopeState::Terminated);
        drop(lease);
    }
}
