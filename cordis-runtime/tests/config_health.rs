//! Runtime limits, concurrency, timeout, and health regression tests.

use async_trait::async_trait;
use cordis_core::{
    CordisError, DependencyPolicy, InvocationKey, PluginDescriptor, PluginRevision, ResourceKind,
};
use cordis_runtime::{
    Context, HealthIssueKind, InvocationContext, InvocationHandler, InvocationOutcome,
    NativePlugin, Runtime, RuntimeConfig, RuntimeHealth, invocation_handler_fn,
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::Notify;

struct Plugin {
    start: Arc<dyn Fn(Context) -> Result<(), CordisError> + Send + Sync>,
}
#[async_trait]
impl NativePlugin for Plugin {
    fn descriptor(&self) -> PluginDescriptor {
        PluginDescriptor {
            name: "test".into(),
            dependencies: Arc::new([]),
            provisions: Arc::new([]),
            dependency_policy: DependencyPolicy::Restart,
            revision: PluginRevision(0),
        }
    }
    async fn start(&self, context: Context) -> Result<(), CordisError> {
        (self.start)(context)
    }
}
fn empty() -> Plugin {
    Plugin {
        start: Arc::new(|_| Ok(())),
    }
}

#[test]
fn invalid_config_is_structured() {
    let mut config = RuntimeConfig::default();
    config.max_concurrent_invocations = 0;
    assert!(matches!(
        Runtime::with_config(config),
        Err(CordisError::InvalidRuntimeConfig(_))
    ));
}

#[tokio::test]
async fn scope_and_fiber_limits_recover_after_gc() {
    let mut config = RuntimeConfig::default();
    config.max_scopes = 2;
    config.max_fibers = 1;
    let runtime = Runtime::with_config(config).expect("runtime");
    let child = runtime.create_scope(runtime.root(), "one").expect("scope");
    assert!(matches!(
        runtime.create_scope(runtime.root(), "two"),
        Err(CordisError::ResourceLimitExceeded {
            resource: ResourceKind::Scopes,
            limit: 2
        })
    ));
    runtime.dispose_scope(child).await.expect("dispose scope");
    let _ = runtime.collect_garbage();
    runtime
        .create_scope(runtime.root(), "replacement")
        .expect("scope quota restored");
    let fiber = runtime
        .install(runtime.root(), empty())
        .await
        .expect("fiber");
    assert!(matches!(
        runtime.install(runtime.root(), empty()).await,
        Err(CordisError::ResourceLimitExceeded {
            resource: ResourceKind::Fibers,
            limit: 1
        })
    ));
    assert_eq!(runtime.health().quota_rejections, 2);
    runtime.dispose_fiber(fiber, false).await.expect("dispose");
    let _ = runtime.collect_garbage();
    runtime
        .install(runtime.root(), empty())
        .await
        .expect("fiber quota restored");
}

struct Blocking {
    entered: Arc<Notify>,
    release: Arc<Notify>,
}
#[async_trait]
impl InvocationHandler for Blocking {
    async fn call(
        &self,
        _: InvocationContext,
        _: cordis_core::InvocationValue,
    ) -> Result<InvocationOutcome, CordisError> {
        self.entered.notify_one();
        self.release.notified().await;
        Ok(cordis_core::InvocationValue::native(Arc::new(1_u32)))
    }
}

#[tokio::test]
async fn concurrency_permit_is_released_by_caller_cancellation() {
    let mut config = RuntimeConfig::default();
    config.max_concurrent_invocations = 1;
    let runtime = Runtime::with_config(config).expect("runtime");
    let key = InvocationKey::new("test", "limited", 1);
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    runtime
        .install(
            runtime.root(),
            Plugin {
                start: Arc::new({
                    let key = key.clone();
                    let entered = entered.clone();
                    let release = release.clone();
                    move |context| {
                        context.handle_invocation(
                            key.clone(),
                            Arc::new(Blocking {
                                entered: entered.clone(),
                                release: release.clone(),
                            }),
                        )?;
                        Ok(())
                    }
                }),
            },
        )
        .await
        .expect("provider");
    let captured = Arc::new(Mutex::new(None));
    runtime
        .install(
            runtime.root(),
            Plugin {
                start: Arc::new({
                    let captured = captured.clone();
                    move |context| {
                        *captured.lock().expect("mutex") = Some(context);
                        Ok(())
                    }
                }),
            },
        )
        .await
        .expect("caller");
    let caller = captured.lock().expect("mutex").clone().expect("context");
    let first = tokio::spawn({
        let caller = caller.clone();
        let key = key.clone();
        async move { caller.invoke_typed::<u32, u32>(&key, Arc::new(0)).await }
    });
    entered.notified().await;
    let second = tokio::spawn({
        let caller = caller.clone();
        let key = key.clone();
        async move { caller.invoke_typed::<u32, u32>(&key, Arc::new(0)).await }
    });
    tokio::task::yield_now().await;
    assert_eq!(runtime.health().active_invocations, 1);
    first.abort();
    first.await.expect_err("aborted");
    entered.notified().await;
    release.notify_waiters();
    assert_eq!(*second.await.expect("join").expect("second"), 1);
}

#[tokio::test]
async fn health_counts_success_panic_and_timeout_without_payloads() {
    let runtime = Runtime::new();
    let ok = InvocationKey::new("test", "ok", 1);
    let panic_key = InvocationKey::new("test", "panic", 1);
    let block = InvocationKey::new("test", "block", 1);
    runtime
        .install(
            runtime.root(),
            Plugin {
                start: Arc::new({
                    let ok = ok.clone();
                    let panic_key = panic_key.clone();
                    let block = block.clone();
                    move |context| {
                        context.handle_invocation(
                            ok.clone(),
                            invocation_handler_fn(|_, value: Arc<u32>| async move { Ok(value) }),
                        )?;
                        context.handle_invocation(panic_key.clone(), Arc::new(PanicHandler))?;
                        context.handle_invocation(
                            block.clone(),
                            Arc::new(Blocking {
                                entered: Arc::new(Notify::new()),
                                release: Arc::new(Notify::new()),
                            }),
                        )?;
                        Ok(())
                    }
                }),
            },
        )
        .await
        .expect("provider");
    let captured = Arc::new(Mutex::new(None));
    runtime
        .install(
            runtime.root(),
            Plugin {
                start: Arc::new({
                    let captured = captured.clone();
                    move |context| {
                        *captured.lock().expect("mutex") = Some(context);
                        Ok(())
                    }
                }),
            },
        )
        .await
        .expect("caller");
    let caller = captured.lock().expect("mutex").clone().expect("context");
    caller
        .invoke_typed::<u32, u32>(&ok, Arc::new(1))
        .await
        .expect("ok");
    for _ in 0..70 {
        assert!(
            caller
                .invoke(
                    &panic_key,
                    cordis_core::InvocationValue::native(Arc::new(()))
                )
                .await
                .is_err()
        );
    }
    assert!(matches!(
        caller
            .invoke_with_timeout(
                &block,
                cordis_core::InvocationValue::native(Arc::new(())),
                Duration::ZERO
            )
            .await,
        Err(CordisError::InvocationTimedOut)
    ));
    let health = runtime.health();
    assert_eq!(health.status, RuntimeHealth::Healthy);
    assert_eq!(health.invocation_successes, 1);
    assert_eq!(health.invocation_panics, 70);
    assert_eq!(health.invocation_timeouts, 1);
    assert!(
        health
            .recent_errors
            .iter()
            .any(|issue| issue.kind == HealthIssueKind::InvocationPanic)
    );
    assert_eq!(health.recent_errors.len(), 64);
}

struct PanicHandler;
#[async_trait]
impl InvocationHandler for PanicHandler {
    async fn call(
        &self,
        _: InvocationContext,
        _: cordis_core::InvocationValue,
    ) -> Result<InvocationOutcome, CordisError> {
        panic!("test panic")
    }
}

struct EventNoop;
#[async_trait]
impl cordis_runtime::EventHandler for EventNoop {
    async fn call(
        &self,
        _: cordis_core::EventValue,
        _: Option<cordis_runtime::Next>,
    ) -> Result<cordis_runtime::EventOutcome, CordisError> {
        Ok(cordis_runtime::EventOutcome::default())
    }
}

#[tokio::test]
async fn every_per_fiber_limit_and_zero_interval_is_enforced() {
    let mut config = RuntimeConfig::default();
    config.max_tasks_per_fiber = 1;
    config.max_handlers_per_fiber = 1;
    config.max_effects_per_fiber = 1;
    config.max_child_scopes_per_fiber = 1;
    let runtime = Runtime::with_config(config).expect("runtime");
    let captured = Arc::new(Mutex::new(None));
    runtime
        .install(
            runtime.root(),
            Plugin {
                start: Arc::new({
                    let captured = captured.clone();
                    move |context| {
                        *captured.lock().expect("mutex") = Some(context);
                        Ok(())
                    }
                }),
            },
        )
        .await
        .expect("fiber");
    let context = captured.lock().expect("mutex").clone().expect("context");
    context
        .effect(cordis_core::effect_fn(|| async { Ok(()) }))
        .expect("effect");
    assert!(matches!(
        context.effect(cordis_core::effect_fn(|| async { Ok(()) })),
        Err(CordisError::ResourceLimitExceeded {
            resource: ResourceKind::EffectsPerFiber,
            ..
        })
    ));
    context.spawn(std::future::pending()).expect("task");
    assert!(matches!(
        context.spawn(async {}),
        Err(CordisError::ResourceLimitExceeded {
            resource: ResourceKind::TasksPerFiber,
            ..
        })
    ));
    context
        .on(cordis_core::EventKey("quota".into()), Arc::new(EventNoop))
        .expect("handler");
    assert!(matches!(
        context.on(cordis_core::EventKey("quota-2".into()), Arc::new(EventNoop)),
        Err(CordisError::ResourceLimitExceeded {
            resource: ResourceKind::HandlersPerFiber,
            ..
        })
    ));
    context.create_scope("child").expect("child");
    assert!(matches!(
        context.create_scope("child-2"),
        Err(CordisError::ResourceLimitExceeded {
            resource: ResourceKind::ChildScopesPerFiber,
            ..
        })
    ));
    assert!(matches!(
        context.interval(Duration::ZERO, || async {}),
        Err(CordisError::InvalidRuntimeConfig(_))
    ));

    let mut depth_config = RuntimeConfig::default();
    depth_config.max_scope_depth = 1;
    let depth_runtime = Runtime::with_config(depth_config).expect("depth runtime");
    let child = depth_runtime
        .create_scope(depth_runtime.root(), "child")
        .expect("child");
    assert!(matches!(
        depth_runtime.create_scope(child, "too-deep"),
        Err(CordisError::ResourceLimitExceeded {
            resource: ResourceKind::ScopeDepth,
            limit: 1
        })
    ));
}
