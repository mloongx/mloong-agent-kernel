//! Public hosted-lifecycle foundation conformance.

use async_trait::async_trait;
use cordis_core::{
    CordisError, DependencyPolicy, FiberState, HostError, HostFailureKind, InvocationKey,
    InvocationValue, PluginDescriptor, PluginRevision, RemoteDomainError, effect_fn,
};
use cordis_runtime::{
    Context, DisposeOutcome, InvocationContext, InvocationHandler, InvocationOutcome, NativePlugin,
    PluginArtifact, PluginHost, ReloadOutcome, Runtime, invocation_handler_fn,
};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Notify, oneshot};

type StartFuture = Pin<Box<dyn Future<Output = Result<(), CordisError>> + Send>>;

struct Plugin {
    name: &'static str,
    revision: u64,
    start: Arc<dyn Fn(Context) -> StartFuture + Send + Sync>,
}

impl Plugin {
    fn new(
        name: &'static str,
        revision: u64,
        start: impl Fn(Context) -> StartFuture + Send + Sync + 'static,
    ) -> Self {
        Self {
            name,
            revision,
            start: Arc::new(start),
        }
    }
}

#[async_trait]
impl NativePlugin for Plugin {
    fn descriptor(&self) -> PluginDescriptor {
        PluginDescriptor {
            name: self.name.into(),
            dependencies: Arc::new([]),
            provisions: Arc::new([]),
            dependency_policy: DependencyPolicy::Restart,
            revision: PluginRevision(self.revision),
        }
    }

    async fn start(&self, context: Context) -> Result<(), CordisError> {
        (self.start)(context).await
    }
}

fn artifact() -> PluginArtifact {
    PluginArtifact::new("test", PluginRevision(1), Arc::<[u8]>::from([]))
}

struct StaticHost(Result<Arc<dyn NativePlugin>, CordisError>);

#[async_trait]
impl PluginHost for StaticHost {
    fn kind(&self) -> &'static str {
        "fixture"
    }

    async fn load(&self, _: PluginArtifact) -> Result<Arc<dyn NativePlugin>, CordisError> {
        self.0.clone()
    }
}

struct PreparationGuard(Arc<AtomicBool>);

impl Drop for PreparationGuard {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

struct BlockingHost {
    entered: Mutex<Option<oneshot::Sender<()>>>,
    release: Arc<Notify>,
    dropped: Arc<AtomicBool>,
    plugin: Arc<dyn NativePlugin>,
}

#[async_trait]
impl PluginHost for BlockingHost {
    fn kind(&self) -> &'static str {
        "blocking-fixture"
    }

    async fn load(&self, _: PluginArtifact) -> Result<Arc<dyn NativePlugin>, CordisError> {
        let _guard = PreparationGuard(self.dropped.clone());
        if let Some(entered) = self.entered.lock().expect("entered mutex").take() {
            let _ = entered.send(());
        }
        self.release.notified().await;
        Ok(self.plugin.clone())
    }
}

fn blocking_host(
    plugin: Arc<dyn NativePlugin>,
) -> (Arc<BlockingHost>, oneshot::Receiver<()>, Arc<AtomicBool>) {
    let (entered_tx, entered_rx) = oneshot::channel();
    let dropped = Arc::new(AtomicBool::new(false));
    (
        Arc::new(BlockingHost {
            entered: Mutex::new(Some(entered_tx)),
            release: Arc::new(Notify::new()),
            dropped: dropped.clone(),
            plugin,
        }),
        entered_rx,
        dropped,
    )
}

async fn capture_context(runtime: &Runtime) -> Context {
    let captured = Arc::new(Mutex::new(None));
    runtime
        .install(
            runtime.root(),
            Plugin::new("caller", 0, {
                let captured = captured.clone();
                move |context| {
                    let captured = captured.clone();
                    Box::pin(async move {
                        *captured.lock().expect("context mutex") = Some(context);
                        Ok(())
                    })
                }
            }),
        )
        .await
        .expect("caller install");
    captured
        .lock()
        .expect("context mutex")
        .clone()
        .expect("context")
}

#[tokio::test]
async fn hosted_install_and_reload_load_are_pre_acceptance_and_cancellation_safe() {
    let runtime = Runtime::new();
    let plugin: Arc<dyn NativePlugin> =
        Arc::new(Plugin::new("prepared", 1, |_| Box::pin(async { Ok(()) })));
    let (host, entered, dropped) = blocking_host(plugin.clone());
    let install = tokio::spawn({
        let runtime = runtime.clone();
        let host = host.clone();
        async move {
            runtime
                .install_hosted(runtime.root(), host.as_ref(), artifact())
                .await
        }
    });
    entered.await.expect("install load entered");
    install.abort();
    assert!(
        install
            .await
            .expect_err("install observer aborted")
            .is_cancelled()
    );
    assert!(
        dropped.load(Ordering::SeqCst),
        "Host preparation future was dropped"
    );
    assert!(
        runtime.snapshot().fibers.is_empty(),
        "Runtime accepted no install"
    );

    let old = runtime
        .install(
            runtime.root(),
            Plugin::new("old", 0, |_| Box::pin(async { Ok(()) })),
        )
        .await
        .expect("old");
    let (host, entered, dropped) = blocking_host(plugin);
    let reload = tokio::spawn({
        let runtime = runtime.clone();
        let host = host.clone();
        async move {
            runtime
                .reload_hosted_detailed(old, host.as_ref(), artifact())
                .await
        }
    });
    entered.await.expect("reload load entered");
    reload.abort();
    assert!(
        reload
            .await
            .expect_err("reload observer aborted")
            .is_cancelled()
    );
    assert!(
        dropped.load(Ordering::SeqCst),
        "Host reload preparation was dropped"
    );
    assert_eq!(
        runtime
            .snapshot()
            .fibers
            .iter()
            .find(|fiber| fiber.id == old)
            .expect("old fiber")
            .state,
        FiberState::Active
    );
}

#[tokio::test]
async fn hosted_load_failure_never_starts_a_runtime_operation() {
    let runtime = Runtime::new();
    let failure = CordisError::Host(HostError::new(HostFailureKind::TransportClosed, "fixture"));
    let host = StaticHost(Err(failure.clone()));
    assert!(
        matches!(runtime.install_hosted(runtime.root(), &host, artifact()).await, Err(CordisError::Host(error)) if error.kind() == HostFailureKind::TransportClosed)
    );
    assert!(runtime.snapshot().fibers.is_empty());

    let old = runtime
        .install(
            runtime.root(),
            Plugin::new("old", 0, |_| Box::pin(async { Ok(()) })),
        )
        .await
        .expect("old");
    assert!(
        matches!(runtime.reload_hosted_detailed(old, &host, artifact()).await, Err(CordisError::Host(error)) if error.kind() == HostFailureKind::TransportClosed)
    );
    assert_eq!(
        runtime
            .snapshot()
            .fibers
            .iter()
            .find(|fiber| fiber.id == old)
            .expect("old")
            .state,
        FiberState::Active
    );
}

struct BlockingInvocation {
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

#[async_trait]
impl InvocationHandler for BlockingInvocation {
    async fn call(
        &self,
        _: InvocationContext,
        _: InvocationValue,
    ) -> Result<InvocationOutcome, CordisError> {
        self.entered.notify_one();
        self.release.notified().await;
        Ok(InvocationValue::native(Arc::new(1_u32)))
    }
}

#[tokio::test]
async fn hosted_reload_uses_native_hmr_cutover_and_drain_semantics() {
    let runtime = Runtime::new();
    let key = InvocationKey::new("host", "reload", 1);
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let old = runtime
        .install(
            runtime.root(),
            Plugin::new("old", 0, {
                let key = key.clone();
                let entered = entered.clone();
                let release = release.clone();
                move |context| {
                    let key = key.clone();
                    let handler = BlockingInvocation {
                        entered: entered.clone(),
                        release: release.clone(),
                    };
                    Box::pin(async move {
                        context
                            .handle_invocation(key, Arc::new(handler))
                            .map(|_| ())
                    })
                }
            }),
        )
        .await
        .expect("old");
    let caller = capture_context(&runtime).await;
    let old_call = tokio::spawn({
        let caller = caller.clone();
        let key = key.clone();
        async move { caller.invoke_typed::<u32, u32>(&key, Arc::new(0)).await }
    });
    entered.notified().await;

    let replacement: Arc<dyn NativePlugin> = Arc::new(Plugin::new("new", 1, {
        let key = key.clone();
        move |context| {
            let key = key.clone();
            Box::pin(async move {
                context.handle_invocation(
                    key,
                    invocation_handler_fn(|_, _: Arc<u32>| async { Ok(Arc::new(2_u32)) }),
                )?;
                Ok(())
            })
        }
    }));
    let host = Arc::new(StaticHost(Ok(replacement)));
    let reload = tokio::spawn({
        let runtime = runtime.clone();
        let host = host.clone();
        async move {
            runtime
                .reload_hosted_detailed(old, host.as_ref(), artifact())
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while runtime
            .snapshot()
            .fibers
            .iter()
            .any(|fiber| fiber.id == old && fiber.state != FiberState::Disposing)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cutover");
    assert_eq!(
        *caller
            .invoke_typed::<u32, u32>(&key, Arc::new(0))
            .await
            .expect("new call"),
        2
    );
    release.notify_waiters();
    assert_eq!(*old_call.await.expect("old join").expect("old result"), 1);
    assert!(matches!(
        reload.await.expect("reload join").expect("reload"),
        ReloadOutcome::Completed { .. }
    ));
}

#[tokio::test]
async fn hosted_reload_preserves_typed_precommit_and_postcommit_causes() {
    let runtime = Runtime::new();
    let old = runtime
        .install(
            runtime.root(),
            Plugin::new("old", 0, |_| Box::pin(async { Ok(()) })),
        )
        .await
        .expect("old");
    let failed: Arc<dyn NativePlugin> = Arc::new(Plugin::new("failed", 1, |_| {
        Box::pin(async {
            Err(CordisError::Host(HostError::new(
                HostFailureKind::Unavailable,
                "start",
            )))
        })
    }));
    let error = runtime
        .reload_hosted_detailed(old, &StaticHost(Ok(failed)), artifact())
        .await
        .expect_err("precommit");
    assert!(
        matches!(error, CordisError::ReloadFailed { primary, .. } if matches!(*primary, CordisError::Host(ref error) if error.kind() == HostFailureKind::Unavailable))
    );

    let old = runtime
        .install(
            runtime.root(),
            Plugin::new("cleanup-old", 0, |context| {
                Box::pin(async move {
                    context.effect(effect_fn(|| async {
                        Err(CordisError::Host(HostError::new(
                            HostFailureKind::TransportClosed,
                            "cleanup",
                        )))
                    }))?;
                    Ok(())
                })
            }),
        )
        .await
        .expect("cleanup old");
    let replacement: Arc<dyn NativePlugin> = Arc::new(Plugin::new("replacement", 1, |_| {
        Box::pin(async { Ok(()) })
    }));
    let error = runtime
        .reload_hosted_detailed(old, &StaticHost(Ok(replacement)), artifact())
        .await
        .expect_err("postcommit");
    assert!(
        matches!(error, CordisError::ReloadCommitted { cleanup, .. } if matches!(*cleanup, CordisError::Host(ref error) if error.kind() == HostFailureKind::TransportClosed))
    );
}

#[tokio::test]
async fn activation_and_disposal_keep_typed_host_and_remote_domain_causes() {
    let runtime = Runtime::new();
    let error = runtime
        .install(
            runtime.root(),
            Plugin::new("activation", 0, |context| {
                Box::pin(async move {
                    context.effect(effect_fn(|| async {
                        Err(CordisError::Host(HostError::new(
                            HostFailureKind::ProcessExited,
                            "rollback",
                        )))
                    }))?;
                    Err(CordisError::RemoteDomain(RemoteDomainError::new(
                        "start.denied",
                        "diagnostic",
                    )))
                })
            }),
        )
        .await
        .expect_err("activation");
    assert!(
        matches!(error, CordisError::ActivationFailed { primary, cleanup }
        if matches!(*primary, CordisError::RemoteDomain(ref error) if error.code() == "start.denied")
        && matches!(cleanup.as_slice(), [CordisError::Host(error)] if error.kind() == HostFailureKind::ProcessExited))
    );

    let fiber = runtime
        .install(
            runtime.root(),
            Plugin::new("disposal", 0, |context| {
                Box::pin(async move {
                    context.effect(effect_fn(|| async {
                        Err(CordisError::Host(HostError::new(
                            HostFailureKind::ProcessKilled,
                            "dispose",
                        )))
                    }))?;
                    Ok(())
                })
            }),
        )
        .await
        .expect("disposal plugin");
    let outcome = runtime.dispose_fiber_detailed(fiber, false).await;
    assert!(
        matches!(outcome, DisposeOutcome::CommittedWithCleanupIssues { issues }
        if matches!(issues.as_slice(), [issue]
            if matches!(issue.cause.as_ref(), Some(CordisError::Host(error)) if error.kind() == HostFailureKind::ProcessKilled)))
    );
}
