//! Scoped invocation integration tests.

use async_trait::async_trait;
use cordis_core::{
    CordisError, DependencyPolicy, FiberState, InvocationKey, InvocationValue, PluginDescriptor,
    PluginRevision,
};
use cordis_runtime::{
    Context, InvocationContext, InvocationHandler, InvocationMiddleware, InvocationOutcome,
    NativePlugin, NextInvocation, Runtime, RuntimeConfig, invocation_handler_fn,
};
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
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
        start: impl Fn(Context) -> StartFuture + Send + Sync + 'static,
    ) -> Self {
        Self {
            name,
            revision: 0,
            start: Arc::new(start),
        }
    }

    fn revision(mut self, revision: u64) -> Self {
        self.revision = revision;
        self
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

fn key() -> InvocationKey {
    InvocationKey::new("agent", "model.generate", 1)
}

async fn capture_context(
    runtime: &Runtime,
    scope: cordis_core::ScopeId,
) -> (cordis_core::FiberId, Context) {
    let captured = Arc::new(Mutex::new(None));
    let fiber = runtime
        .install(
            scope,
            Plugin::new("caller", {
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
        .expect("install caller");
    let context = captured
        .lock()
        .expect("context mutex")
        .clone()
        .expect("captured context");
    (fiber, context)
}

fn value_handler(value: u32) -> Arc<dyn InvocationHandler> {
    invocation_handler_fn(
        move |_context, input: Arc<u32>| async move { Ok(Arc::new(*input + value)) },
    )
}

#[tokio::test]
async fn typed_invocation_reports_missing_request_and_response_mismatches() {
    let runtime = Runtime::new();
    let operation = key();
    runtime
        .install(
            runtime.root(),
            Plugin::new("provider", {
                let operation = operation.clone();
                move |context| {
                    let operation = operation.clone();
                    Box::pin(async move {
                        context.handle_invocation(operation, value_handler(1))?;
                        Ok(())
                    })
                }
            }),
        )
        .await
        .expect("install provider");
    let (_, caller) = capture_context(&runtime, runtime.root()).await;
    assert_eq!(
        *caller
            .invoke_typed::<u32, u32>(&operation, Arc::new(41))
            .await
            .expect("typed invoke"),
        42
    );
    assert!(matches!(
        caller
            .invoke_typed::<String, u32>(&operation, Arc::new("bad".to_owned()))
            .await,
        Err(CordisError::InvocationTypeMismatch(_))
    ));
    assert!(matches!(
        caller
            .invoke_typed::<u32, String>(&operation, Arc::new(1))
            .await,
        Err(CordisError::InvocationTypeMismatch(_))
    ));
    assert!(matches!(
        caller
            .invoke(
                &InvocationKey::new("missing", "handler", 1),
                InvocationValue::native(Arc::new(1_u32))
            )
            .await,
        Err(CordisError::InvocationHandlerNotFound(_))
    ));
}

struct RecordingMiddleware {
    id: u32,
    order: Arc<Mutex<Vec<u32>>>,
}

#[async_trait]
impl InvocationMiddleware for RecordingMiddleware {
    async fn call(
        &self,
        context: InvocationContext,
        input: InvocationValue,
        next: NextInvocation,
    ) -> Result<InvocationOutcome, CordisError> {
        self.order.lock().expect("order mutex").push(self.id);
        let result = next.run(context, input).await;
        self.order.lock().expect("order mutex").push(self.id * 10);
        result
    }
}

#[tokio::test]
async fn middleware_is_root_to_leaf_then_registration_order() {
    let runtime = Runtime::new();
    let operation = key();
    let order = Arc::new(Mutex::new(Vec::new()));
    runtime
        .install(
            runtime.root(),
            Plugin::new("root-provider", {
                let operation = operation.clone();
                let order = order.clone();
                move |context| {
                    let operation = operation.clone();
                    let order = order.clone();
                    Box::pin(async move {
                        context.handle_invocation(operation.clone(), value_handler(1))?;
                        for id in [1, 2] {
                            context.invocation_middleware(
                                operation.clone(),
                                Arc::new(RecordingMiddleware {
                                    id,
                                    order: order.clone(),
                                }),
                            )?;
                        }
                        Ok(())
                    })
                }
            }),
        )
        .await
        .expect("root provider");
    let child = runtime
        .create_scope(runtime.root(), "child")
        .expect("child");
    runtime
        .install(
            child,
            Plugin::new("child-middleware", {
                let operation = operation.clone();
                let order = order.clone();
                move |context| {
                    let operation = operation.clone();
                    let order = order.clone();
                    Box::pin(async move {
                        context.invocation_middleware(
                            operation,
                            Arc::new(RecordingMiddleware { id: 3, order }),
                        )?;
                        Ok(())
                    })
                }
            }),
        )
        .await
        .expect("child middleware");
    let (_, caller) = capture_context(&runtime, child).await;
    caller
        .invoke_typed::<u32, u32>(&operation, Arc::new(1))
        .await
        .expect("invoke");
    assert_eq!(
        *order.lock().expect("order mutex"),
        vec![1, 2, 3, 30, 20, 10]
    );
}

#[tokio::test]
async fn scope_inheritance_shadow_and_sibling_isolation_are_deterministic() {
    let runtime = Runtime::new();
    let operation = key();
    let root_provider = runtime
        .install(
            runtime.root(),
            Plugin::new("root", {
                let operation = operation.clone();
                move |context| {
                    let operation = operation.clone();
                    Box::pin(async move {
                        context.handle_invocation(operation, value_handler(10))?;
                        Ok(())
                    })
                }
            }),
        )
        .await
        .expect("root provider");
    let child = runtime
        .create_scope(runtime.root(), "child")
        .expect("child");
    let sibling = runtime
        .create_scope(runtime.root(), "sibling")
        .expect("sibling");
    let child_provider = runtime
        .install(
            child,
            Plugin::new("shadow", {
                let operation = operation.clone();
                move |context| {
                    let operation = operation.clone();
                    Box::pin(async move {
                        context.handle_invocation(operation, value_handler(20))?;
                        Ok(())
                    })
                }
            }),
        )
        .await
        .expect("shadow");
    let (_, child_caller) = capture_context(&runtime, child).await;
    let (_, sibling_caller) = capture_context(&runtime, sibling).await;
    assert_eq!(
        *child_caller
            .invoke_typed::<u32, u32>(&operation, Arc::new(1))
            .await
            .expect("child"),
        21
    );
    assert_eq!(
        *sibling_caller
            .invoke_typed::<u32, u32>(&operation, Arc::new(1))
            .await
            .expect("sibling"),
        11
    );
    runtime
        .dispose_fiber(child_provider, false)
        .await
        .expect("dispose shadow");
    assert_eq!(
        *child_caller
            .invoke_typed::<u32, u32>(&operation, Arc::new(1))
            .await
            .expect("inherited"),
        11
    );
    runtime
        .dispose_fiber(root_provider, false)
        .await
        .expect("dispose root");
    assert!(matches!(
        child_caller
            .invoke_typed::<u32, u32>(&operation, Arc::new(1))
            .await,
        Err(CordisError::InvocationHandlerNotFound(_))
    ));
}

struct BlockingHandler {
    entered: Arc<Notify>,
    release: Arc<Notify>,
    dropped: Arc<Notify>,
    value: u32,
}

struct DropSignal(Arc<Notify>);

impl Drop for DropSignal {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

#[async_trait]
impl InvocationHandler for BlockingHandler {
    async fn call(
        &self,
        _context: InvocationContext,
        _input: InvocationValue,
    ) -> Result<InvocationOutcome, CordisError> {
        let _drop = DropSignal(self.dropped.clone());
        self.entered.notify_one();
        self.release.notified().await;
        Ok(InvocationValue::native(Arc::new(self.value)))
    }
}

#[tokio::test]
async fn in_flight_snapshot_survives_handler_disposal() {
    let runtime = Runtime::new();
    let operation = key();
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let cleaned = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let provider = runtime
        .install(
            runtime.root(),
            Plugin::new("blocking", {
                let operation = operation.clone();
                let entered = entered.clone();
                let release = release.clone();
                let cleaned = cleaned.clone();
                move |context| {
                    let operation = operation.clone();
                    let handler = BlockingHandler {
                        entered: entered.clone(),
                        release: release.clone(),
                        dropped: Arc::new(Notify::new()),
                        value: 7,
                    };
                    let cleaned = cleaned.clone();
                    Box::pin(async move {
                        context.handle_invocation(operation, Arc::new(handler))?;
                        context.effect(cordis_core::effect_fn(move || async move {
                            cleaned.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            Ok(())
                        }))?;
                        Ok(())
                    })
                }
            }),
        )
        .await
        .expect("provider");
    let (_, caller) = capture_context(&runtime, runtime.root()).await;
    let task = tokio::spawn({
        let operation = operation.clone();
        let caller = caller.clone();
        async move {
            caller
                .invoke_typed::<u32, u32>(&operation, Arc::new(0))
                .await
        }
    });
    entered.notified().await;
    let dispose_runtime = runtime.clone();
    let disposal =
        tokio::spawn(async move { dispose_runtime.dispose_fiber(provider, false).await });
    while runtime
        .snapshot()
        .fibers
        .iter()
        .any(|item| item.id == provider && item.state != FiberState::Disposing)
    {
        tokio::task::yield_now().await;
    }
    assert!(matches!(
        caller
            .invoke_typed::<u32, u32>(&operation, Arc::new(0))
            .await,
        Err(CordisError::InvocationHandlerNotFound(_))
    ));
    assert_eq!(cleaned.load(std::sync::atomic::Ordering::SeqCst), 0);
    release.notify_waiters();
    assert_eq!(*task.await.expect("join").expect("old snapshot"), 7);
    disposal.await.expect("join").expect("dispose provider");
    assert_eq!(cleaned.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[tokio::test]
async fn disposing_caller_cancels_and_drops_handler_future() {
    let runtime = Runtime::new();
    let operation = key();
    let entered = Arc::new(Notify::new());
    let dropped = Arc::new(Notify::new());
    runtime
        .install(
            runtime.root(),
            Plugin::new("blocking", {
                let operation = operation.clone();
                let entered = entered.clone();
                let dropped = dropped.clone();
                move |context| {
                    let operation = operation.clone();
                    let handler = BlockingHandler {
                        entered: entered.clone(),
                        release: Arc::new(Notify::new()),
                        dropped: dropped.clone(),
                        value: 0,
                    };
                    Box::pin(async move {
                        context
                            .handle_invocation(operation, Arc::new(handler))
                            .map(|_| ())
                    })
                }
            }),
        )
        .await
        .expect("provider");
    let (caller_fiber, caller) = capture_context(&runtime, runtime.root()).await;
    let invoke = tokio::spawn({
        let operation = operation.clone();
        async move {
            caller
                .invoke_typed::<u32, u32>(&operation, Arc::new(0))
                .await
        }
    });
    entered.notified().await;
    let dropped_wait = dropped.notified();
    runtime
        .dispose_fiber(caller_fiber, false)
        .await
        .expect("dispose caller");
    assert!(matches!(
        invoke.await.expect("join"),
        Err(CordisError::InvocationCancelled)
    ));
    dropped_wait.await;
}

#[tokio::test]
async fn handler_and_middleware_panics_are_structured() {
    struct PanicHandler;
    #[async_trait]
    impl InvocationHandler for PanicHandler {
        async fn call(
            &self,
            _: InvocationContext,
            _: InvocationValue,
        ) -> Result<InvocationOutcome, CordisError> {
            panic!("handler panic")
        }
    }
    struct PanicMiddleware;
    #[async_trait]
    impl InvocationMiddleware for PanicMiddleware {
        async fn call(
            &self,
            _: InvocationContext,
            _: InvocationValue,
            _: NextInvocation,
        ) -> Result<InvocationOutcome, CordisError> {
            panic!("middleware panic")
        }
    }
    for middleware in [false, true] {
        let runtime = Runtime::new();
        let operation = key();
        runtime
            .install(
                runtime.root(),
                Plugin::new("panic", {
                    let operation = operation.clone();
                    move |context| {
                        let operation = operation.clone();
                        Box::pin(async move {
                            context.handle_invocation(operation.clone(), Arc::new(PanicHandler))?;
                            if middleware {
                                context
                                    .invocation_middleware(operation, Arc::new(PanicMiddleware))?;
                            }
                            Ok(())
                        })
                    }
                }),
            )
            .await
            .expect("provider");
        let (_, caller) = capture_context(&runtime, runtime.root()).await;
        let error = caller
            .invoke(&operation, InvocationValue::native(Arc::new(())))
            .await
            .expect_err("panic isolated");
        assert!(if middleware {
            matches!(error, CordisError::InvocationMiddlewarePanicked(_))
        } else {
            matches!(error, CordisError::InvocationHandlerPanicked(_))
        });
        assert_eq!(runtime.snapshot().provider_inflight, 0);
    }
}

#[tokio::test]
async fn hmr_staging_is_hidden_and_commit_replaces_future_calls() {
    let runtime = Runtime::new();
    let operation = key();
    let provider = |operation: InvocationKey,
                    name,
                    value,
                    entered: Option<Arc<Notify>>,
                    release: Option<Arc<Notify>>| {
        Plugin::new(name, {
            move |context| {
                let operation = operation.clone();
                let entered = entered.clone();
                let release = release.clone();
                Box::pin(async move {
                    context.handle_invocation(operation, value_handler(value))?;
                    if let (Some(entered), Some(release)) = (entered, release) {
                        entered.notify_one();
                        release.notified().await;
                    }
                    Ok(())
                })
            }
        })
    };
    let old = runtime
        .install(
            runtime.root(),
            provider(operation.clone(), "old", 1, None, None),
        )
        .await
        .expect("old");
    let (_, caller) = capture_context(&runtime, runtime.root()).await;
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let (result_tx, result_rx) = oneshot::channel();
    let reload_operation = operation.clone();
    tokio::spawn({
        let runtime = runtime.clone();
        let entered = entered.clone();
        let release = release.clone();
        async move {
            let result = runtime
                .reload(
                    old,
                    provider(reload_operation, "new", 2, Some(entered), Some(release)).revision(1),
                )
                .await;
            let _ = result_tx.send(result);
        }
    });
    entered.notified().await;
    assert_eq!(
        *caller
            .invoke_typed::<u32, u32>(&operation, Arc::new(0))
            .await
            .expect("old visible"),
        1
    );
    release.notify_waiters();
    result_rx.await.expect("reload channel").expect("reload");
    assert_eq!(
        *caller
            .invoke_typed::<u32, u32>(&operation, Arc::new(0))
            .await
            .expect("new visible"),
        2
    );
}

#[tokio::test]
async fn duplicate_active_handler_is_structured_and_failed_hmr_keeps_old() {
    let runtime = Runtime::new();
    let operation = key();
    let captured = Arc::new(Mutex::new(None));
    let old = runtime
        .install(
            runtime.root(),
            Plugin::new("old", {
                let operation = operation.clone();
                let captured = captured.clone();
                move |context| {
                    let operation = operation.clone();
                    let captured = captured.clone();
                    Box::pin(async move {
                        context.handle_invocation(operation, value_handler(1))?;
                        *captured.lock().expect("context mutex") = Some(context);
                        Ok(())
                    })
                }
            }),
        )
        .await
        .expect("old provider");
    let provider_context = captured
        .lock()
        .expect("context mutex")
        .clone()
        .expect("provider context");
    assert!(matches!(
        provider_context.handle_invocation(operation.clone(), value_handler(9)),
        Err(CordisError::DuplicateInvocationHandler(_))
    ));

    let replacement = Plugin::new("bad replacement", {
        let operation = operation.clone();
        move |context| {
            let operation = operation.clone();
            Box::pin(async move {
                context.handle_invocation(operation, value_handler(2))?;
                Err(CordisError::PluginStartFailed("validation failed".into()))
            })
        }
    })
    .revision(1);
    assert!(runtime.reload(old, replacement).await.is_err());
    let (_, caller) = capture_context(&runtime, runtime.root()).await;
    assert_eq!(
        *caller
            .invoke_typed::<u32, u32>(&operation, Arc::new(0))
            .await
            .expect("old remains"),
        1
    );
}

struct CountingMiddleware(Arc<std::sync::atomic::AtomicUsize>);

#[async_trait]
impl InvocationMiddleware for CountingMiddleware {
    async fn call(
        &self,
        context: InvocationContext,
        input: InvocationValue,
        next: NextInvocation,
    ) -> Result<InvocationOutcome, CordisError> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        next.run(context, input).await
    }
}

#[tokio::test]
async fn duplicate_middleware_arc_has_independent_fiber_lifetimes() {
    let runtime = Runtime::new();
    let operation = key();
    runtime
        .install(
            runtime.root(),
            Plugin::new("handler", {
                let operation = operation.clone();
                move |context| {
                    let operation = operation.clone();
                    Box::pin(async move {
                        context.handle_invocation(operation, value_handler(0))?;
                        Ok(())
                    })
                }
            }),
        )
        .await
        .expect("handler");
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let middleware: Arc<dyn InvocationMiddleware> = Arc::new(CountingMiddleware(calls.clone()));
    let install_middleware = |name, middleware: Arc<dyn InvocationMiddleware>| {
        Plugin::new(name, {
            let operation = operation.clone();
            move |context| {
                let operation = operation.clone();
                let middleware = middleware.clone();
                Box::pin(async move {
                    context.invocation_middleware(operation, middleware)?;
                    Ok(())
                })
            }
        })
    };
    let first = runtime
        .install(
            runtime.root(),
            install_middleware("first", middleware.clone()),
        )
        .await
        .expect("first");
    let second = runtime
        .install(runtime.root(), install_middleware("second", middleware))
        .await
        .expect("second");
    let (_, caller) = capture_context(&runtime, runtime.root()).await;
    caller
        .invoke_typed::<u32, u32>(&operation, Arc::new(0))
        .await
        .expect("two");
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    runtime
        .dispose_fiber(first, false)
        .await
        .expect("first dispose");
    caller
        .invoke_typed::<u32, u32>(&operation, Arc::new(0))
        .await
        .expect("one");
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 3);
    runtime
        .dispose_fiber(second, false)
        .await
        .expect("second dispose");
    caller
        .invoke_typed::<u32, u32>(&operation, Arc::new(0))
        .await
        .expect("zero");
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 3);
}

#[tokio::test]
async fn same_handler_arc_in_sibling_scopes_is_removed_by_registration_id() {
    let runtime = Runtime::new();
    let operation = key();
    let left = runtime.create_scope(runtime.root(), "left").expect("left");
    let right = runtime
        .create_scope(runtime.root(), "right")
        .expect("right");
    let handler = value_handler(5);
    let install_handler = |operation: InvocationKey, name, handler: Arc<dyn InvocationHandler>| {
        Plugin::new(name, move |context| {
            let operation = operation.clone();
            let handler = handler.clone();
            Box::pin(async move { context.handle_invocation(operation, handler).map(|_| ()) })
        })
    };
    let left_provider = runtime
        .install(
            left,
            install_handler(operation.clone(), "left provider", handler.clone()),
        )
        .await
        .expect("left provider");
    runtime
        .install(
            right,
            install_handler(operation.clone(), "right provider", handler),
        )
        .await
        .expect("right provider");
    let (_, left_caller) = capture_context(&runtime, left).await;
    let (_, right_caller) = capture_context(&runtime, right).await;
    assert_eq!(
        *left_caller
            .invoke_typed::<u32, u32>(&operation, Arc::new(1))
            .await
            .expect("left"),
        6
    );
    assert_eq!(
        *right_caller
            .invoke_typed::<u32, u32>(&operation, Arc::new(1))
            .await
            .expect("right"),
        6
    );
    runtime
        .dispose_fiber(left_provider, false)
        .await
        .expect("dispose left");
    assert!(matches!(
        left_caller
            .invoke_typed::<u32, u32>(&operation, Arc::new(1))
            .await,
        Err(CordisError::InvocationHandlerNotFound(_))
    ));
    assert_eq!(
        *right_caller
            .invoke_typed::<u32, u32>(&operation, Arc::new(1))
            .await
            .expect("right remains"),
        6
    );
}

#[tokio::test]
async fn concurrent_invocations_each_use_one_complete_snapshot() {
    let runtime = Runtime::new();
    let operation = key();
    runtime
        .install(
            runtime.root(),
            Plugin::new("provider", {
                let operation = operation.clone();
                move |context| {
                    let operation = operation.clone();
                    Box::pin(async move {
                        context
                            .handle_invocation(operation, value_handler(1))
                            .map(|_| ())
                    })
                }
            }),
        )
        .await
        .expect("provider");
    let (_, caller) = capture_context(&runtime, runtime.root()).await;
    let mut tasks = Vec::new();
    for value in 0..128_u32 {
        let caller = caller.clone();
        let operation = operation.clone();
        tasks.push(tokio::spawn(async move {
            *caller
                .invoke_typed::<u32, u32>(&operation, Arc::new(value))
                .await
                .expect("invoke")
        }));
    }
    let mut values = Vec::new();
    for task in tasks {
        values.push(task.await.expect("join"));
    }
    values.sort_unstable();
    assert_eq!(values, (1..=128).collect::<Vec<_>>());
    runtime.shutdown().await.expect("shutdown");
    assert!(matches!(
        caller
            .invoke_typed::<u32, u32>(&operation, Arc::new(0))
            .await,
        Err(CordisError::RuntimeShuttingDown)
    ));
}

#[tokio::test(start_paused = true)]
async fn timeout_and_caller_drop_cancel_the_owned_handler_future() {
    async fn setup() -> (Runtime, InvocationKey, Context, Arc<Notify>, Arc<Notify>) {
        let runtime = Runtime::new();
        let operation = key();
        let entered = Arc::new(Notify::new());
        let dropped = Arc::new(Notify::new());
        runtime
            .install(
                runtime.root(),
                Plugin::new("blocking", {
                    let operation = operation.clone();
                    let entered = entered.clone();
                    let dropped = dropped.clone();
                    move |context| {
                        let operation = operation.clone();
                        let handler = BlockingHandler {
                            entered: entered.clone(),
                            release: Arc::new(Notify::new()),
                            dropped: dropped.clone(),
                            value: 0,
                        };
                        Box::pin(async move {
                            context
                                .handle_invocation(operation, Arc::new(handler))
                                .map(|_| ())
                        })
                    }
                }),
            )
            .await
            .expect("provider");
        let (_, caller) = capture_context(&runtime, runtime.root()).await;
        (runtime, operation, caller, entered, dropped)
    }

    let (runtime, operation, caller, entered, dropped) = setup().await;
    let timed = tokio::spawn(async move {
        caller
            .invoke_typed_with_timeout::<u32, u32>(
                &operation,
                Arc::new(0),
                std::time::Duration::from_secs(5),
            )
            .await
    });
    entered.notified().await;
    tokio::time::advance(std::time::Duration::from_secs(5)).await;
    assert!(matches!(
        timed.await.expect("timeout task"),
        Err(CordisError::InvocationTimedOut)
    ));
    dropped.notified().await;
    assert_eq!(runtime.snapshot().provider_inflight, 0);

    let (runtime, operation, caller, entered, dropped) = setup().await;
    let invoke = tokio::spawn(async move {
        caller
            .invoke_typed::<u32, u32>(&operation, Arc::new(0))
            .await
    });
    entered.notified().await;
    let dropped_wait = dropped.notified();
    invoke.abort();
    assert!(invoke.await.expect_err("aborted caller").is_cancelled());
    dropped_wait.await;
    assert_eq!(runtime.snapshot().provider_inflight, 0);
}

#[tokio::test]
async fn hmr_keeps_old_in_flight_snapshot_while_new_calls_use_new_handler() {
    let runtime = Runtime::new();
    let operation = key();
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let old = runtime
        .install(
            runtime.root(),
            Plugin::new("old blocking", {
                let operation = operation.clone();
                let entered = entered.clone();
                let release = release.clone();
                move |context| {
                    let operation = operation.clone();
                    let handler = BlockingHandler {
                        entered: entered.clone(),
                        release: release.clone(),
                        dropped: Arc::new(Notify::new()),
                        value: 1,
                    };
                    Box::pin(async move {
                        context
                            .handle_invocation(operation, Arc::new(handler))
                            .map(|_| ())
                    })
                }
            }),
        )
        .await
        .expect("old");
    let (_, caller) = capture_context(&runtime, runtime.root()).await;
    let old_call = tokio::spawn({
        let caller = caller.clone();
        let operation = operation.clone();
        async move {
            caller
                .invoke_typed::<u32, u32>(&operation, Arc::new(0))
                .await
        }
    });
    entered.notified().await;
    let reload_runtime = runtime.clone();
    let reload_operation = operation.clone();
    let reload = tokio::spawn(async move {
        reload_runtime
            .reload(
                old,
                Plugin::new("new", {
                    let operation = reload_operation.clone();
                    move |context| {
                        let operation = operation.clone();
                        Box::pin(async move {
                            context.handle_invocation(operation, value_handler(2))?;
                            Ok(())
                        })
                    }
                })
                .revision(1),
            )
            .await
    });
    while runtime
        .snapshot()
        .fibers
        .iter()
        .any(|item| item.id == old && item.state != FiberState::Disposing)
    {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        *caller
            .invoke_typed::<u32, u32>(&operation, Arc::new(0))
            .await
            .expect("new call"),
        2
    );
    release.notify_waiters();
    assert_eq!(*old_call.await.expect("join").expect("old call"), 1);
    reload.await.expect("join").expect("reload");
}

#[tokio::test]
async fn queued_invocation_resolves_after_permit_without_pinning_old_generation() {
    let mut config = RuntimeConfig::default();
    config.max_concurrent_invocations = 1;
    let runtime = Runtime::with_config(config).expect("runtime");
    let operation = key();
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let old = runtime
        .install(
            runtime.root(),
            Plugin::new("old blocking", {
                let operation = operation.clone();
                let entered = entered.clone();
                let release = release.clone();
                move |context| {
                    let operation = operation.clone();
                    let handler = BlockingHandler {
                        entered: entered.clone(),
                        release: release.clone(),
                        dropped: Arc::new(Notify::new()),
                        value: 1,
                    };
                    Box::pin(async move {
                        context
                            .handle_invocation(operation, Arc::new(handler))
                            .map(|_| ())
                    })
                }
            }),
        )
        .await
        .expect("old");
    let (_, caller) = capture_context(&runtime, runtime.root()).await;
    let first = tokio::spawn({
        let caller = caller.clone();
        let operation = operation.clone();
        async move {
            caller
                .invoke_typed::<u32, u32>(&operation, Arc::new(0))
                .await
        }
    });
    entered.notified().await;
    let queued = tokio::spawn({
        let caller = caller.clone();
        let operation = operation.clone();
        async move {
            caller
                .invoke_typed::<u32, u32>(&operation, Arc::new(0))
                .await
        }
    });
    tokio::task::yield_now().await;
    // One caller Context admission plus one provider execution lease. The
    // queued invocation has acquired neither yet.
    assert_eq!(runtime.snapshot().provider_inflight, 2);

    let reload = tokio::spawn({
        let runtime = runtime.clone();
        let operation = operation.clone();
        async move {
            runtime
                .reload(
                    old,
                    Plugin::new("new", move |context| {
                        let operation = operation.clone();
                        Box::pin(async move {
                            context.handle_invocation(operation, value_handler(2))?;
                            Ok(())
                        })
                    })
                    .revision(1),
                )
                .await
        }
    });
    while runtime
        .snapshot()
        .fibers
        .iter()
        .any(|item| item.id == old && item.state != FiberState::Disposing)
    {
        tokio::task::yield_now().await;
    }
    assert_eq!(runtime.snapshot().provider_inflight, 2);
    release.notify_waiters();
    assert_eq!(*first.await.expect("first join").expect("old result"), 1);
    assert_eq!(*queued.await.expect("queued join").expect("new result"), 2);
    reload.await.expect("reload join").expect("reload");
}

#[tokio::test]
async fn shutdown_cancels_active_invocation_and_rejects_later_calls() {
    let runtime = Runtime::new();
    let operation = key();
    let entered = Arc::new(Notify::new());
    runtime
        .install(
            runtime.root(),
            Plugin::new("blocking", {
                let operation = operation.clone();
                let entered = entered.clone();
                move |context| {
                    let operation = operation.clone();
                    let handler = BlockingHandler {
                        entered: entered.clone(),
                        release: Arc::new(Notify::new()),
                        dropped: Arc::new(Notify::new()),
                        value: 0,
                    };
                    Box::pin(async move {
                        context
                            .handle_invocation(operation, Arc::new(handler))
                            .map(|_| ())
                    })
                }
            }),
        )
        .await
        .expect("provider");
    let (_, caller) = capture_context(&runtime, runtime.root()).await;
    let invoke = tokio::spawn({
        let caller = caller.clone();
        let operation = operation.clone();
        async move {
            caller
                .invoke_typed::<u32, u32>(&operation, Arc::new(0))
                .await
        }
    });
    entered.notified().await;
    runtime.shutdown().await.expect("shutdown");
    assert!(matches!(
        invoke.await.expect("invoke join"),
        Err(CordisError::InvocationCancelled)
    ));
    assert!(matches!(
        caller
            .invoke_typed::<u32, u32>(&operation, Arc::new(0))
            .await,
        Err(CordisError::RuntimeShuttingDown)
    ));
}
