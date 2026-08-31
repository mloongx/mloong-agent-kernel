//! Runtime lifecycle integration tests.

use async_trait::async_trait;
use cordis_core::{
    CordisError, DependencyPolicy, EventKey, EventValue, FiberState, PluginDescriptor,
    PluginRevision, ServiceKey, effect_fn,
};
use cordis_runtime::RuntimeShutdownState;
use cordis_runtime::{Context, EventHandler, EventOutcome, NativePlugin, Next, Runtime};
use std::{
    future::Future,
    pin::Pin,
    sync::atomic::{AtomicUsize, Ordering},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{Barrier, Semaphore, oneshot};

type StartFuture = Pin<Box<dyn Future<Output = Result<(), CordisError>> + Send>>;

struct TestPlugin {
    descriptor: PluginDescriptor,
    start: Arc<dyn Fn(Context) -> StartFuture + Send + Sync>,
}

struct DropGuard(Arc<Mutex<bool>>);

impl Drop for DropGuard {
    fn drop(&mut self) {
        *self.0.lock().expect("test mutex") = true;
    }
}

struct RecordingHandler {
    order: Arc<Mutex<Vec<usize>>>,
    id: usize,
    outcome: bool,
}

#[async_trait]
impl EventHandler for RecordingHandler {
    async fn call(
        &self,
        _value: EventValue,
        _next: Option<Next>,
    ) -> Result<EventOutcome, CordisError> {
        self.order.lock().expect("mutex").push(self.id);
        Ok(EventOutcome(
            self.outcome.then(|| Arc::new(self.id) as EventValue),
        ))
    }
}

struct WaterfallHandler {
    order: Arc<Mutex<Vec<usize>>>,
    id: usize,
    continue_chain: bool,
}

#[async_trait]
impl EventHandler for WaterfallHandler {
    async fn call(
        &self,
        _value: EventValue,
        next: Option<Next>,
    ) -> Result<EventOutcome, CordisError> {
        self.order.lock().expect("mutex").push(self.id);
        let outcome = if self.continue_chain {
            next.expect("waterfall next").run().await?
        } else {
            EventOutcome(Some(Arc::new(self.id)))
        };
        self.order.lock().expect("mutex").push(self.id * 10);
        Ok(outcome)
    }
}

struct ParallelHandler {
    barrier: Arc<Barrier>,
    completed: Arc<AtomicUsize>,
}

#[async_trait]
impl EventHandler for ParallelHandler {
    async fn call(
        &self,
        _value: EventValue,
        _next: Option<Next>,
    ) -> Result<EventOutcome, CordisError> {
        self.barrier.wait().await;
        self.completed.fetch_add(1, Ordering::SeqCst);
        Ok(EventOutcome::default())
    }
}

impl TestPlugin {
    fn new(
        name: &'static str,
        dependencies: Vec<ServiceKey>,
        start: impl Fn(Context) -> StartFuture + Send + Sync + 'static,
    ) -> Self {
        Self::contract(name, dependencies, Vec::new(), start)
    }

    fn contract(
        name: &'static str,
        dependencies: Vec<ServiceKey>,
        provisions: Vec<ServiceKey>,
        start: impl Fn(Context) -> StartFuture + Send + Sync + 'static,
    ) -> Self {
        Self {
            descriptor: PluginDescriptor {
                name: name.into(),
                dependencies: dependencies.into(),
                provisions: provisions.into(),
                dependency_policy: DependencyPolicy::default(),
                revision: PluginRevision::default(),
            },
            start: Arc::new(start),
        }
    }
}

#[async_trait]
impl NativePlugin for TestPlugin {
    fn descriptor(&self) -> PluginDescriptor {
        self.descriptor.clone()
    }
    async fn start(&self, context: Context) -> Result<(), CordisError> {
        (self.start)(context).await
    }
}

fn key(name: &str) -> ServiceKey {
    ServiceKey::new("test", name, 1)
}

#[tokio::test]
async fn starting_task_does_not_run_before_commit_and_runs_after_commit() {
    let runtime = Runtime::new();
    let (task_tx, mut task_rx) = oneshot::channel();
    let task_tx = Arc::new(Mutex::new(Some(task_tx)));
    let (started_tx, started_rx) = oneshot::channel();
    let started_tx = Arc::new(Mutex::new(Some(started_tx)));
    let (release_tx, release_rx) = oneshot::channel();
    let release_rx = Arc::new(Mutex::new(Some(release_rx)));
    let install_runtime = runtime.clone();
    let install = tokio::spawn(async move {
        install_runtime
            .install(
                install_runtime.root(),
                TestPlugin::new("barrier", vec![], move |ctx| {
                    let task_tx = task_tx.clone();
                    let started_tx = started_tx.clone();
                    let release_rx = release_rx.clone();
                    Box::pin(async move {
                        ctx.spawn(async move {
                            if let Some(tx) = task_tx.lock().expect("mutex").take() {
                                let _ = tx.send(());
                            }
                        })?;
                        started_tx
                            .lock()
                            .expect("mutex")
                            .take()
                            .expect("once")
                            .send(())
                            .ok();
                        let rx = release_rx.lock().expect("mutex").take().expect("once");
                        let _ = rx.await;
                        Ok(())
                    })
                }),
            )
            .await
    });
    started_rx.await.expect("plugin started");
    assert!(matches!(
        task_rx.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    release_tx.send(()).expect("release activation");
    install.await.expect("join").expect("activation");
    task_rx.await.expect("task runs after commit");
}

#[tokio::test]
async fn activation_commit_and_active_state_are_linearized() {
    let runtime = Runtime::new();
    let service = key("linearized-activation");
    let (proof_tx, proof_rx) = oneshot::channel();
    let proof_tx = Arc::new(Mutex::new(Some(proof_tx)));
    let task_runtime = runtime.clone();
    let task_service = service.clone();
    let fiber = runtime
        .install(
            runtime.root(),
            TestPlugin::contract("linearized", vec![], vec![service], move |ctx| {
                let proof_tx = proof_tx.clone();
                let runtime = task_runtime.clone();
                let service = task_service.clone();
                Box::pin(async move {
                    ctx.provide(service.clone(), 7_u64)?;
                    let task_context = ctx.clone();
                    ctx.spawn(async move {
                        let service_visible = task_context.get::<u64>(&service).is_ok();
                        let active = runtime
                            .snapshot()
                            .fibers
                            .into_iter()
                            .find(|item| item.id == task_context.fiber())
                            .is_some_and(|item| item.state == FiberState::Active);
                        proof_tx
                            .lock()
                            .expect("mutex")
                            .take()
                            .expect("once")
                            .send((active, service_visible))
                            .ok();
                    })?;
                    Ok(())
                })
            }),
        )
        .await
        .expect("activation");
    assert_eq!(proof_rx.await.expect("task proof"), (true, true));
    assert_eq!(
        runtime
            .snapshot()
            .fibers
            .into_iter()
            .find(|item| item.id == fiber)
            .expect("fiber")
            .state,
        FiberState::Active
    );
}

#[tokio::test]
async fn activation_failure_cancels_staged_task_without_running_it() {
    let runtime = Runtime::new();
    let runs = Arc::new(AtomicUsize::new(0));
    let task_runs = runs.clone();
    let error = runtime
        .install(
            runtime.root(),
            TestPlugin::new("rollback-barrier", vec![], move |ctx| {
                let task_runs = task_runs.clone();
                Box::pin(async move {
                    ctx.spawn(async move {
                        task_runs.fetch_add(1, Ordering::SeqCst);
                    })?;
                    Err(CordisError::Invariant("start failure".into()))
                })
            }),
        )
        .await
        .expect_err("activation fails");
    assert!(matches!(error, CordisError::PluginStartFailed(_)));
    assert_eq!(runs.load(Ordering::SeqCst), 0);
    assert_eq!(runtime.snapshot().service_count, 0);
}

#[tokio::test]
async fn dispose_racing_activation_cannot_publish_disposing_fiber() {
    let runtime = Runtime::new();
    let service = key("activation-race");
    let (provided_tx, provided_rx) = oneshot::channel();
    let provided_tx = Arc::new(Mutex::new(Some(provided_tx)));
    let (_release_tx, release_rx) = oneshot::channel::<()>();
    let release_rx = Arc::new(Mutex::new(Some(release_rx)));
    let install_runtime = runtime.clone();
    let install_service = service.clone();
    let install = tokio::spawn(async move {
        install_runtime
            .install(
                install_runtime.root(),
                TestPlugin::contract(
                    "dispose-race",
                    vec![],
                    vec![install_service.clone()],
                    move |ctx| {
                        let provided_tx = provided_tx.clone();
                        let release_rx = release_rx.clone();
                        let service = install_service.clone();
                        Box::pin(async move {
                            ctx.provide(service, 1_u64)?;
                            provided_tx
                                .lock()
                                .expect("mutex")
                                .take()
                                .expect("once")
                                .send(())
                                .ok();
                            let rx = release_rx.lock().expect("mutex").take().expect("once");
                            let _ = rx.await;
                            Ok(())
                        })
                    },
                ),
            )
            .await
    });
    provided_rx.await.expect("staged service");
    let fiber = runtime
        .snapshot()
        .fibers
        .into_iter()
        .find(|item| item.state == FiberState::Starting)
        .expect("starting fiber")
        .id;
    runtime
        .dispose_fiber(fiber, false)
        .await
        .expect("dispose wins");
    assert!(install.await.expect("join").is_err());
    assert_eq!(runtime.snapshot().service_count, 0);
}

#[tokio::test]
async fn shutdown_racing_activation_cannot_publish_after_barrier() {
    let runtime = Runtime::new();
    let service = key("shutdown-activation-race");
    let (provided_tx, provided_rx) = oneshot::channel();
    let provided_tx = Arc::new(Mutex::new(Some(provided_tx)));
    let (_release_tx, release_rx) = oneshot::channel::<()>();
    let release_rx = Arc::new(Mutex::new(Some(release_rx)));
    let install_runtime = runtime.clone();
    let install_service = service.clone();
    let install = tokio::spawn(async move {
        install_runtime
            .install(
                install_runtime.root(),
                TestPlugin::contract(
                    "shutdown-race",
                    vec![],
                    vec![install_service.clone()],
                    move |ctx| {
                        let provided_tx = provided_tx.clone();
                        let release_rx = release_rx.clone();
                        let service = install_service.clone();
                        Box::pin(async move {
                            ctx.provide(service, 1_u64)?;
                            provided_tx
                                .lock()
                                .expect("mutex")
                                .take()
                                .expect("once")
                                .send(())
                                .ok();
                            let rx = release_rx.lock().expect("mutex").take().expect("once");
                            let _ = rx.await;
                            Ok(())
                        })
                    },
                ),
            )
            .await
    });
    provided_rx.await.expect("staged service");
    runtime.shutdown().await.expect("shutdown wins");
    assert!(install.await.expect("join").is_err());
    assert_eq!(runtime.snapshot().service_count, 0);
    assert_eq!(runtime.shutdown_state(), RuntimeShutdownState::Complete);
}

#[tokio::test]
async fn dropped_install_observer_does_not_cancel_activation() {
    let runtime = Runtime::new();
    let service = key("cancelled-activation");
    let (provided_tx, provided_rx) = oneshot::channel();
    let provided_tx = Arc::new(Mutex::new(Some(provided_tx)));
    let (release_tx, release_rx) = oneshot::channel::<()>();
    let release_rx = Arc::new(Mutex::new(Some(release_rx)));
    let install_runtime = runtime.clone();
    let install_service = service.clone();
    let install = tokio::spawn(async move {
        install_runtime
            .install(
                install_runtime.root(),
                TestPlugin::contract(
                    "cancelled",
                    vec![],
                    vec![install_service.clone()],
                    move |ctx| {
                        let provided_tx = provided_tx.clone();
                        let release_rx = release_rx.clone();
                        let service = install_service.clone();
                        Box::pin(async move {
                            ctx.provide(service, 1_u64)?;
                            provided_tx
                                .lock()
                                .expect("mutex")
                                .take()
                                .expect("once")
                                .send(())
                                .ok();
                            let rx = release_rx.lock().expect("mutex").take().expect("once");
                            let _ = rx.await;
                            Ok(())
                        })
                    },
                ),
            )
            .await
    });
    provided_rx.await.expect("staged service");
    install.abort();
    assert!(install.await.expect_err("cancelled").is_cancelled());
    release_tx.send(()).expect("release activation");
    let convergence = tokio::time::timeout(Duration::from_secs(1), async {
        while !runtime
            .snapshot()
            .fibers
            .iter()
            .any(|item| item.state == FiberState::Active)
        {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(
        convergence.is_ok(),
        "snapshot: {:?}",
        runtime.snapshot().fibers
    );
    assert_eq!(runtime.snapshot().service_count, 1);
}

#[tokio::test]
async fn effects_are_lifo_and_async() {
    let runtime = Runtime::new();
    let order = Arc::new(Mutex::new(Vec::new()));
    let captured = order.clone();
    let plugin = TestPlugin::new("effects", vec![], move |ctx| {
        let captured = captured.clone();
        Box::pin(async move {
            for value in [1, 2, 3] {
                let captured = captured.clone();
                ctx.effect(effect_fn(move || async move {
                    tokio::task::yield_now().await;
                    captured.lock().expect("test mutex").push(value);
                    Ok(())
                }))?;
            }
            Ok(())
        })
    });
    let fiber = runtime
        .install(runtime.root(), plugin)
        .await
        .expect("install");
    runtime.dispose_fiber(fiber, false).await.expect("dispose");
    assert_eq!(*order.lock().expect("test mutex"), vec![3, 2, 1]);
}

#[tokio::test]
async fn effect_panic_remains_a_plugin_disposal_error_and_cleanup_continues() {
    let runtime = Runtime::new();
    let cleaned = Arc::new(AtomicUsize::new(0));
    let fiber = runtime
        .install(
            runtime.root(),
            TestPlugin::new("effect-panic", vec![], {
                let cleaned = cleaned.clone();
                move |ctx| {
                    let cleaned = cleaned.clone();
                    Box::pin(async move {
                        ctx.effect(effect_fn(move || async move {
                            cleaned.fetch_add(1, Ordering::SeqCst);
                            Ok(())
                        }))?;
                        ctx.effect(effect_fn(|| async move {
                            panic!("expected effect panic");
                            #[allow(unreachable_code)]
                            Ok(())
                        }))?;
                        Ok(())
                    })
                }
            }),
        )
        .await
        .expect("install");

    let error = runtime
        .dispose_fiber(fiber, false)
        .await
        .expect_err("effect panic must be reported");
    assert!(matches!(error, CordisError::PluginDisposeFailed(_)));
    assert_eq!(cleaned.load(Ordering::SeqCst), 1);
    assert_eq!(fiber_state(&runtime, fiber), FiberState::Disposed);
}

#[tokio::test]
async fn startup_failure_rolls_back() {
    let runtime = Runtime::new();
    let cleaned = Arc::new(Mutex::new(false));
    let observed = cleaned.clone();
    let plugin = TestPlugin::new("failure", vec![], move |ctx| {
        let observed = observed.clone();
        Box::pin(async move {
            ctx.effect(effect_fn(move || async move {
                *observed.lock().expect("test mutex") = true;
                Ok(())
            }))?;
            Err(CordisError::PluginStartFailed("expected".into()))
        })
    });
    assert!(runtime.install(runtime.root(), plugin).await.is_err());
    assert!(*cleaned.lock().expect("test mutex"));
    assert_eq!(runtime.snapshot().fibers[0].state, FiberState::Disposed);
}

#[tokio::test]
async fn scope_inheritance_shadowing_and_isolation() {
    let runtime = Runtime::new();
    let service = key("value");
    let root_plugin = TestPlugin::contract("root-provider", vec![], vec![service.clone()], {
        let service = service.clone();
        move |ctx| {
            let service = service.clone();
            Box::pin(async move { ctx.provide(service, 10_u32) })
        }
    });
    runtime
        .install(runtime.root(), root_plugin)
        .await
        .expect("root provider");
    let left = runtime.create_scope(runtime.root(), "left").expect("left");
    let right = runtime
        .create_scope(runtime.root(), "right")
        .expect("right");
    let seen = Arc::new(Mutex::new(Vec::new()));
    let left_reader = TestPlugin::new("left-reader", vec![service.clone()], {
        let service = service.clone();
        let seen = seen.clone();
        move |ctx| {
            let service = service.clone();
            let seen = seen.clone();
            Box::pin(async move {
                seen.lock()
                    .expect("test mutex")
                    .push(*ctx.get::<u32>(&service)?);
                Ok(())
            })
        }
    });
    runtime
        .install(left, left_reader)
        .await
        .expect("left reader");
    let shadow = TestPlugin::contract("shadow", vec![], vec![service.clone()], {
        let service = service.clone();
        move |ctx| {
            let service = service.clone();
            Box::pin(async move { ctx.provide(service, 20_u32) })
        }
    });
    runtime.install(left, shadow).await.expect("shadow");
    let right_reader = TestPlugin::new("right-reader", vec![service.clone()], {
        let service = service.clone();
        let seen = seen.clone();
        move |ctx| {
            let service = service.clone();
            let seen = seen.clone();
            Box::pin(async move {
                seen.lock()
                    .expect("test mutex")
                    .push(*ctx.get::<u32>(&service)?);
                Ok(())
            })
        }
    });
    runtime
        .install(right, right_reader)
        .await
        .expect("right reader");
    assert_eq!(*seen.lock().expect("test mutex"), vec![10, 10]);
}

#[tokio::test]
async fn waiting_dependency_activates_when_service_appears() {
    let runtime = Runtime::new();
    let service = key("late");
    let started = Arc::new(Mutex::new(false));
    let consumer = TestPlugin::new("consumer", vec![service.clone()], {
        let started = started.clone();
        move |_ctx| {
            let started = started.clone();
            Box::pin(async move {
                *started.lock().expect("test mutex") = true;
                Ok(())
            })
        }
    });
    let consumer_id = runtime
        .install(runtime.root(), consumer)
        .await
        .expect("waiting install");
    assert_eq!(
        runtime
            .snapshot()
            .fibers
            .iter()
            .find(|f| f.id == consumer_id)
            .expect("fiber")
            .state,
        FiberState::WaitingDependencies
    );
    let provider = TestPlugin::contract("provider", vec![], vec![service.clone()], {
        let service = service.clone();
        move |ctx| {
            let service = service.clone();
            Box::pin(async move { ctx.provide(service, ()) })
        }
    });
    runtime
        .install(runtime.root(), provider)
        .await
        .expect("provider");
    tokio::time::timeout(Duration::from_secs(1), async {
        while !*started.lock().expect("test mutex") {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("activation timeout");
}

#[tokio::test]
async fn owned_infinite_task_is_cancelled() {
    let runtime = Runtime::new();
    let dropped = Arc::new(Mutex::new(false));
    let plugin = TestPlugin::new("task", vec![], {
        let dropped = dropped.clone();
        move |ctx| {
            let dropped = dropped.clone();
            Box::pin(async move {
                ctx.spawn(async move {
                    let _guard = DropGuard(dropped);
                    std::future::pending::<()>().await;
                })?;
                Ok(())
            })
        }
    });
    let fiber = runtime
        .install(runtime.root(), plugin)
        .await
        .expect("install");
    tokio::task::yield_now().await;
    runtime.dispose_fiber(fiber, false).await.expect("dispose");
    assert!(*dropped.lock().expect("test mutex"));
}

#[tokio::test]
async fn shutdown_rejects_new_work() {
    let runtime = Runtime::new();
    runtime.shutdown().await.expect("shutdown");
    assert!(matches!(
        runtime.create_scope(runtime.root(), "late"),
        Err(CordisError::RuntimeShuttingDown)
    ));
}

#[tokio::test]
async fn shutdown_aggregates_errors_without_skipping_fibers_or_child_scopes() {
    let runtime = Runtime::new();
    let cleaned = Arc::new(Mutex::new(Vec::new()));
    let cleanup_plugin = |name: &'static str, marker: usize, fail: bool| {
        TestPlugin::new(name, vec![], {
            let cleaned = cleaned.clone();
            move |ctx| {
                let cleaned = cleaned.clone();
                Box::pin(async move {
                    ctx.effect(effect_fn(move || async move {
                        cleaned.lock().expect("mutex").push(marker);
                        if fail {
                            Err(CordisError::PluginDisposeFailed(format!("effect {marker}")))
                        } else {
                            Ok(())
                        }
                    }))
                })
            }
        })
    };

    // Reverse cleanup visits root-fail before root-ok.
    runtime
        .install(runtime.root(), cleanup_plugin("root-ok", 1, false))
        .await
        .expect("root ok");
    runtime
        .install(runtime.root(), cleanup_plugin("root-fail", 2, true))
        .await
        .expect("root fail");

    let child_ok = runtime
        .create_scope(runtime.root(), "child-ok")
        .expect("child ok");
    runtime
        .install(child_ok, cleanup_plugin("child-ok-plugin", 3, false))
        .await
        .expect("child ok plugin");
    let child_fail = runtime
        .create_scope(runtime.root(), "child-fail")
        .expect("child fail");
    runtime
        .install(child_fail, cleanup_plugin("child-fail-plugin", 4, true))
        .await
        .expect("child fail plugin");

    let error = runtime
        .shutdown()
        .await
        .expect_err("cleanup errors must be reported");
    assert!(matches!(error, CordisError::CleanupFailed(_)));
    let mut markers = cleaned.lock().expect("mutex").clone();
    markers.sort_unstable();
    assert_eq!(
        markers,
        vec![1, 2, 3, 4],
        "all fibers and child scopes must clean"
    );
    assert!(
        runtime
            .snapshot()
            .fibers
            .iter()
            .all(|fiber| fiber.state == FiberState::Disposed)
    );
    assert_eq!(runtime.shutdown_state(), RuntimeShutdownState::Complete);

    // Published shutdown failures are stable and never repeat effects.
    let repeated = runtime.shutdown().await.expect_err("stable failure");
    assert_eq!(repeated, error);
    assert_eq!(runtime.shutdown_state(), RuntimeShutdownState::Complete);
    assert_eq!(
        cleaned.lock().expect("mutex").len(),
        4,
        "effects run exactly once"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_shutdown_runs_cleanup_once_and_waiters_observe_same_completion() {
    let runtime = Runtime::new();
    let cleaned = Arc::new(AtomicUsize::new(0));
    runtime
        .install(
            runtime.root(),
            TestPlugin::new("failing-cleanup", vec![], {
                let cleaned = cleaned.clone();
                move |ctx| {
                    let cleaned = cleaned.clone();
                    Box::pin(async move {
                        ctx.effect(effect_fn(move || async move {
                            cleaned.fetch_add(1, Ordering::SeqCst);
                            tokio::task::yield_now().await;
                            Err(CordisError::PluginDisposeFailed("expected".into()))
                        }))
                    })
                }
            }),
        )
        .await
        .expect("plugin");

    let barrier = Arc::new(Barrier::new(3));
    let first_runtime = runtime.clone();
    let first_barrier = barrier.clone();
    let first = tokio::spawn(async move {
        first_barrier.wait().await;
        first_runtime.shutdown().await
    });
    let second_runtime = runtime.clone();
    let second_barrier = barrier.clone();
    let second = tokio::spawn(async move {
        second_barrier.wait().await;
        second_runtime.shutdown().await
    });
    barrier.wait().await;
    let results = [
        first.await.expect("first join"),
        second.await.expect("second join"),
    ];
    assert!(results.iter().all(Result::is_err));
    assert_eq!(results[0], results[1]);
    assert_eq!(cleaned.load(Ordering::SeqCst), 1);
    assert_eq!(runtime.shutdown_state(), RuntimeShutdownState::Complete);
    assert!(
        runtime
            .snapshot()
            .fibers
            .iter()
            .all(|fiber| fiber.state == FiberState::Disposed)
    );
}

#[tokio::test]
async fn dependency_loss_rolls_back_and_restore_restarts() {
    let runtime = Runtime::new();
    let service = key("restartable");
    let starts = Arc::new(AtomicUsize::new(0));
    let cleanups = Arc::new(AtomicUsize::new(0));
    let consumer = TestPlugin::new("consumer", vec![service.clone()], {
        let starts = starts.clone();
        let cleanups = cleanups.clone();
        move |ctx| {
            let starts = starts.clone();
            let cleanups = cleanups.clone();
            Box::pin(async move {
                starts.fetch_add(1, Ordering::SeqCst);
                ctx.effect(effect_fn(move || async move {
                    cleanups.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }))
            })
        }
    });
    let consumer_id = runtime
        .install(runtime.root(), consumer)
        .await
        .expect("consumer");
    let provider = |name: &'static str| {
        TestPlugin::contract(name, vec![], vec![service.clone()], {
            let service = service.clone();
            move |ctx| {
                let service = service.clone();
                Box::pin(async move { ctx.provide(service, 1_u8) })
            }
        })
    };
    let first = runtime
        .install(runtime.root(), provider("provider-1"))
        .await
        .expect("provider");
    wait_until(|| starts.load(Ordering::SeqCst) == 1).await;
    runtime
        .dispose_fiber(first, false)
        .await
        .expect("provider dispose");
    assert_eq!(cleanups.load(Ordering::SeqCst), 1);
    assert_eq!(
        fiber_state(&runtime, consumer_id),
        FiberState::WaitingDependencies
    );
    runtime
        .install(runtime.root(), provider("provider-2"))
        .await
        .expect("replacement");
    wait_until(|| starts.load(Ordering::SeqCst) == 2).await;
    assert_eq!(fiber_state(&runtime, consumer_id), FiberState::Active);
}

#[tokio::test]
async fn dependency_cycle_is_rejected_before_activation() {
    let runtime = Runtime::new();
    let a = key("a");
    let b = key("b");
    let first = TestPlugin::contract("a", vec![b.clone()], vec![a.clone()], |_ctx| {
        Box::pin(async { Ok(()) })
    });
    runtime
        .install(runtime.root(), first)
        .await
        .expect("first waits");
    let second = TestPlugin::contract("b", vec![a], vec![b], |_ctx| Box::pin(async { Ok(()) }));
    assert!(matches!(
        runtime.install(runtime.root(), second).await,
        Err(CordisError::DependencyCycle(_))
    ));
}

#[tokio::test]
async fn plugin_panic_is_isolated_and_rolled_back() {
    let runtime = Runtime::new();
    let cleaned = Arc::new(AtomicUsize::new(0));
    let plugin = TestPlugin::new("panic", vec![], {
        let cleaned = cleaned.clone();
        move |ctx| {
            let cleaned = cleaned.clone();
            Box::pin(async move {
                ctx.effect(effect_fn(move || async move {
                    cleaned.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }))?;
                panic!("plugin boundary panic");
            })
        }
    });
    let error = runtime
        .install(runtime.root(), plugin)
        .await
        .expect_err("panic must fail");
    assert!(error.to_string().contains("plugin panicked"));
    assert_eq!(cleaned.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn interned_lookup_cache_invalidates_on_shadowing() {
    let runtime = Runtime::new();
    let service = key("cached");
    let root_provider = TestPlugin::contract("root", vec![], vec![service.clone()], {
        let service = service.clone();
        move |ctx| {
            let service = service.clone();
            Box::pin(async move { ctx.provide(service, 1_u32) })
        }
    });
    runtime
        .install(runtime.root(), root_provider)
        .await
        .expect("root");
    let child = runtime
        .create_scope(runtime.root(), "child")
        .expect("child");
    let slot = Arc::new(Mutex::new(None));
    runtime
        .install(
            child,
            TestPlugin::new("capture", vec![], {
                let slot = slot.clone();
                move |ctx| {
                    let slot = slot.clone();
                    Box::pin(async move {
                        *slot.lock().expect("mutex") = Some(ctx);
                        Ok(())
                    })
                }
            }),
        )
        .await
        .expect("capture");
    let context = slot.lock().expect("mutex").clone().expect("context");
    let symbol = runtime.intern_service(&service).expect("intern");
    assert_eq!(*context.get_symbol::<u32>(symbol).expect("root cached"), 1);
    runtime
        .install(
            child,
            TestPlugin::contract("shadow", vec![], vec![service.clone()], {
                let service = service.clone();
                move |ctx| {
                    let service = service.clone();
                    Box::pin(async move { ctx.provide(service, 2_u32) })
                }
            }),
        )
        .await
        .expect("shadow");
    assert_eq!(
        *context.get_symbol::<u32>(symbol).expect("shadow cached"),
        2
    );
}

#[tokio::test]
async fn hmr_success_swaps_atomically_and_failure_keeps_old() {
    let runtime = Runtime::new();
    let service = key("hmr");
    let provider = |name: &'static str, value: u32| {
        TestPlugin::contract(name, vec![], vec![service.clone()], {
            let service = service.clone();
            move |ctx| {
                let service = service.clone();
                Box::pin(async move { ctx.provide(service, value) })
            }
        })
    };
    let old = runtime
        .install(runtime.root(), provider("v1", 1))
        .await
        .expect("v1");
    let slot = Arc::new(Mutex::new(None));
    runtime
        .install(
            runtime.root(),
            TestPlugin::new("reader", vec![service.clone()], {
                let slot = slot.clone();
                move |ctx| {
                    let slot = slot.clone();
                    Box::pin(async move {
                        *slot.lock().expect("mutex") = Some(ctx);
                        Ok(())
                    })
                }
            }),
        )
        .await
        .expect("reader");
    let reader = slot.lock().expect("mutex").clone().expect("reader context");
    let new = runtime
        .reload(old, provider("v2", 2))
        .await
        .expect("reload");
    assert_eq!(*reader.get::<u32>(&service).expect("v2 service"), 2);
    assert_eq!(fiber_state(&runtime, old), FiberState::Disposed);
    let invalid = TestPlugin::contract("v3-invalid", vec![], vec![service.clone()], |_ctx| {
        Box::pin(async { Ok(()) })
    });
    assert!(matches!(
        runtime.reload(new, invalid).await,
        Err(CordisError::ReloadFailed { primary, .. })
            if matches!(*primary, CordisError::RevisionValidationFailed(_))
    ));
    assert_eq!(*reader.get::<u32>(&service).expect("v2 retained"), 2);
    assert_eq!(fiber_state(&runtime, new), FiberState::Active);
}

#[tokio::test]
async fn timers_are_lifecycle_owned() {
    let runtime = Runtime::new();
    let ticks = Arc::new(AtomicUsize::new(0));
    let plugin = TestPlugin::new("timers", vec![], {
        let ticks = ticks.clone();
        move |ctx| {
            let ticks = ticks.clone();
            Box::pin(async move {
                ctx.interval(Duration::from_millis(5), move || {
                    let ticks = ticks.clone();
                    async move {
                        ticks.fetch_add(1, Ordering::SeqCst);
                    }
                })?;
                assert!(matches!(
                    ctx.timeout(Duration::from_millis(1), std::future::pending::<()>())
                        .await,
                    Err(CordisError::Timeout)
                ));
                Ok(())
            })
        }
    });
    let fiber = runtime
        .install(runtime.root(), plugin)
        .await
        .expect("timer plugin");
    wait_until(|| ticks.load(Ordering::SeqCst) > 0).await;
    runtime.dispose_fiber(fiber, false).await.expect("dispose");
    let after = ticks.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(ticks.load(Ordering::SeqCst), after);
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn event_modes_order_short_circuit_parallel_and_remove() {
    let runtime = Runtime::new();
    let serial_event = EventKey("test.serial".into());
    let bail_event = EventKey("test.bail".into());
    let waterfall_event = EventKey("test.waterfall".into());
    let parallel_event = EventKey("test.parallel".into());
    let order = Arc::new(Mutex::new(Vec::new()));
    let barrier = Arc::new(Barrier::new(3));
    let completed = Arc::new(AtomicUsize::new(0));
    let slot = Arc::new(Mutex::new(None));
    let listener = TestPlugin::new("events", vec![], {
        let serial_event = serial_event.clone();
        let bail_event = bail_event.clone();
        let waterfall_event = waterfall_event.clone();
        let parallel_event = parallel_event.clone();
        let order = order.clone();
        let barrier = barrier.clone();
        let completed = completed.clone();
        let slot = slot.clone();
        move |ctx| {
            let serial_event = serial_event.clone();
            let bail_event = bail_event.clone();
            let waterfall_event = waterfall_event.clone();
            let parallel_event = parallel_event.clone();
            let order = order.clone();
            let barrier = barrier.clone();
            let completed = completed.clone();
            let slot = slot.clone();
            Box::pin(async move {
                for event in [serial_event, bail_event.clone()] {
                    for id in 1..=3 {
                        ctx.on(
                            event.clone(),
                            Arc::new(RecordingHandler {
                                order: order.clone(),
                                id,
                                outcome: event == bail_event && id == 2,
                            }),
                        )?;
                    }
                }
                for (id, continue_chain) in [(1, true), (2, false), (3, true)] {
                    ctx.on(
                        waterfall_event.clone(),
                        Arc::new(WaterfallHandler {
                            order: order.clone(),
                            id,
                            continue_chain,
                        }),
                    )?;
                }
                for _ in 0..3 {
                    ctx.on(
                        parallel_event.clone(),
                        Arc::new(ParallelHandler {
                            barrier: barrier.clone(),
                            completed: completed.clone(),
                        }),
                    )?;
                }
                *slot.lock().expect("mutex") = Some(ctx);
                Ok(())
            })
        }
    });
    let listener_id = runtime
        .install(runtime.root(), listener)
        .await
        .expect("listeners");
    let context = slot.lock().expect("mutex").clone().expect("context");

    context
        .serial(&serial_event, Arc::new(()))
        .await
        .expect("serial");
    assert_eq!(*order.lock().expect("mutex"), vec![1, 2, 3]);
    order.lock().expect("mutex").clear();
    let bail = context.bail(&bail_event, Arc::new(())).await.expect("bail");
    assert_eq!(
        *bail
            .0
            .expect("bail value")
            .downcast::<usize>()
            .expect("usize"),
        2
    );
    assert_eq!(*order.lock().expect("mutex"), vec![1, 2]);
    order.lock().expect("mutex").clear();
    context
        .waterfall(&waterfall_event, Arc::new(()))
        .await
        .expect("waterfall");
    assert_eq!(*order.lock().expect("mutex"), vec![1, 2, 20, 10]);
    tokio::time::timeout(
        Duration::from_secs(1),
        context.parallel(&parallel_event, Arc::new(())),
    )
    .await
    .expect("parallel timeout")
    .expect("parallel");
    assert_eq!(completed.load(Ordering::SeqCst), 3);

    runtime
        .dispose_fiber(listener_id, false)
        .await
        .expect("listener dispose");
    order.lock().expect("mutex").clear();
    assert!(matches!(
        context.emit(&serial_event, Arc::new(())).await,
        Err(CordisError::StaleContextGeneration { .. })
    ));
    assert!(order.lock().expect("mutex").is_empty());
}

#[tokio::test]
async fn disposed_context_cannot_create_unowned_resources() {
    let runtime = Runtime::new();
    let slot = Arc::new(Mutex::new(None));
    let fiber = runtime
        .install(
            runtime.root(),
            TestPlugin::new("capture", vec![], {
                let slot = slot.clone();
                move |ctx| {
                    let slot = slot.clone();
                    Box::pin(async move {
                        *slot.lock().expect("mutex") = Some(ctx);
                        Ok(())
                    })
                }
            }),
        )
        .await
        .expect("capture");
    let context = slot.lock().expect("mutex").clone().expect("context");
    runtime.dispose_fiber(fiber, false).await.expect("dispose");
    assert!(matches!(
        context.spawn(async {}),
        Err(CordisError::StaleContextGeneration { .. })
    ));
    assert!(matches!(
        context.effect(effect_fn(|| async { Ok(()) })),
        Err(CordisError::StaleContextGeneration { .. })
    ));
}

#[tokio::test]
async fn service_type_mismatch_is_explicit() {
    let runtime = Runtime::new();
    let service = key("typed");
    let slot = Arc::new(Mutex::new(None));
    runtime
        .install(
            runtime.root(),
            TestPlugin::contract("typed", vec![], vec![service.clone()], {
                let service = service.clone();
                let slot = slot.clone();
                move |ctx| {
                    let service = service.clone();
                    let slot = slot.clone();
                    Box::pin(async move {
                        ctx.provide(service, 42_u32)?;
                        *slot.lock().expect("mutex") = Some(ctx);
                        Ok(())
                    })
                }
            }),
        )
        .await
        .expect("typed provider");
    let context = slot.lock().expect("mutex").clone().expect("context");
    assert!(matches!(
        context.get::<String>(&service),
        Err(CordisError::TypeMismatch(_))
    ));
}

#[tokio::test]
async fn task_panic_is_reported_without_skipping_effect_cleanup() {
    let runtime = Runtime::new();
    let cleaned = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(AtomicUsize::new(0));
    let plugin_cleaned = cleaned.clone();
    let plugin_started = started.clone();
    let fiber = runtime
        .install(
            runtime.root(),
            TestPlugin::new("task-panic", vec![], {
                move |ctx| {
                    let cleaned = plugin_cleaned.clone();
                    let started = plugin_started.clone();
                    Box::pin(async move {
                        ctx.effect(effect_fn(move || async move {
                            cleaned.fetch_add(1, Ordering::SeqCst);
                            Ok(())
                        }))?;
                        ctx.spawn(async move {
                            started.fetch_add(1, Ordering::SeqCst);
                            panic!("owned task panic");
                        })?;
                        Ok(())
                    })
                }
            }),
        )
        .await
        .expect("install");
    wait_until(|| started.load(Ordering::SeqCst) == 1).await;
    wait_until(|| runtime.health().task_panics == 1).await;
    runtime.dispose_fiber(fiber, false).await.expect("dispose");
    assert_eq!(cleaned.load(Ordering::SeqCst), 1);
    assert_eq!(fiber_state(&runtime, fiber), FiberState::Disposed);
}

#[tokio::test]
async fn hmr_staged_handlers_are_invisible_until_commit() {
    let runtime = Runtime::new();
    let event = EventKey("test.hmr-event".into());
    let order = Arc::new(Mutex::new(Vec::new()));
    let context_slot = Arc::new(Mutex::new(None));
    let old = runtime
        .install(
            runtime.root(),
            TestPlugin::new("event-v1", vec![], {
                let event = event.clone();
                let order = order.clone();
                let context_slot = context_slot.clone();
                move |ctx| {
                    let event = event.clone();
                    let order = order.clone();
                    let context_slot = context_slot.clone();
                    Box::pin(async move {
                        ctx.on(
                            event,
                            Arc::new(RecordingHandler {
                                order,
                                id: 1,
                                outcome: false,
                            }),
                        )?;
                        *context_slot.lock().expect("mutex") = Some(ctx);
                        Ok(())
                    })
                }
            }),
        )
        .await
        .expect("old event plugin");
    let context = context_slot
        .lock()
        .expect("mutex")
        .clone()
        .expect("context");
    let (registered_tx, registered_rx) = oneshot::channel();
    let registered_tx = Arc::new(Mutex::new(Some(registered_tx)));
    let (release_tx, release_rx) = oneshot::channel();
    let release_rx = Arc::new(Mutex::new(Some(release_rx)));
    let new_context_slot = Arc::new(Mutex::new(None));
    let new_plugin = TestPlugin::new("event-v2", vec![], {
        let event = event.clone();
        let order = order.clone();
        let registered_tx = registered_tx.clone();
        let release_rx = release_rx.clone();
        let new_context_slot = new_context_slot.clone();
        move |ctx| {
            let event = event.clone();
            let order = order.clone();
            let registered = registered_tx
                .lock()
                .expect("mutex")
                .take()
                .expect("one start");
            let release = release_rx.lock().expect("mutex").take().expect("one start");
            let new_context_slot = new_context_slot.clone();
            Box::pin(async move {
                ctx.on(
                    event,
                    Arc::new(RecordingHandler {
                        order,
                        id: 2,
                        outcome: false,
                    }),
                )?;
                *new_context_slot.lock().expect("mutex") = Some(ctx.clone());
                let _ = registered.send(());
                let _ = release.await;
                Ok(())
            })
        }
    });
    let reload_runtime = runtime.clone();
    let reload = tokio::spawn(async move { reload_runtime.reload(old, new_plugin).await });
    registered_rx.await.expect("new handler registered");
    context
        .emit(&event, Arc::new(()))
        .await
        .expect("pre-commit emit");
    assert_eq!(*order.lock().expect("mutex"), vec![1]);
    order.lock().expect("mutex").clear();
    release_tx.send(()).expect("release reload");
    reload.await.expect("reload join").expect("reload result");
    assert!(matches!(
        context.emit(&event, Arc::new(())).await,
        Err(CordisError::StaleContextGeneration { .. })
    ));
    let new_context = new_context_slot
        .lock()
        .expect("mutex")
        .clone()
        .expect("new context");
    new_context
        .emit(&event, Arc::new(()))
        .await
        .expect("post-commit emit");
    assert_eq!(*order.lock().expect("mutex"), vec![2]);
}

#[tokio::test]
async fn garbage_collection_reuses_slots_without_aba() {
    let runtime = Runtime::new();
    let old = runtime
        .install(
            runtime.root(),
            TestPlugin::new("old", vec![], |_ctx| Box::pin(async { Ok(()) })),
        )
        .await
        .expect("old");
    runtime
        .dispose_fiber(old, false)
        .await
        .expect("dispose old");
    let report = runtime.collect_garbage();
    assert_eq!(report.fibers, 1);
    let new = runtime
        .install(
            runtime.root(),
            TestPlugin::new("new", vec![], |_ctx| Box::pin(async { Ok(()) })),
        )
        .await
        .expect("new");
    assert_ne!(old, new, "slot reuse must advance the generation");
    assert!(matches!(
        runtime.dispose_fiber(old, false).await,
        Err(CordisError::FiberNotFound)
    ));
}

#[tokio::test]
async fn failed_activation_never_publishes_staged_services_or_handlers() {
    let runtime = Runtime::new();
    let service = key("transactional");
    let event = EventKey("test.transactional".into());
    let starts = Arc::new(AtomicUsize::new(0));
    let consumer = TestPlugin::new("consumer", vec![service.clone()], {
        let starts = starts.clone();
        move |_ctx| {
            let starts = starts.clone();
            Box::pin(async move {
                starts.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        }
    });
    runtime
        .install(runtime.root(), consumer)
        .await
        .expect("waiting consumer");
    let calls = Arc::new(Mutex::new(Vec::new()));
    let failed = TestPlugin::contract("failed-provider", vec![], vec![service.clone()], {
        let service = service.clone();
        let event = event.clone();
        let calls = calls.clone();
        move |ctx| {
            let service = service.clone();
            let event = event.clone();
            let calls = calls.clone();
            Box::pin(async move {
                ctx.provide(service, 1_u8)?;
                ctx.on(
                    event,
                    Arc::new(RecordingHandler {
                        order: calls,
                        id: 1,
                        outcome: false,
                    }),
                )?;
                Err(CordisError::PluginStartFailed("rollback".into()))
            })
        }
    });
    assert!(runtime.install(runtime.root(), failed).await.is_err());
    tokio::task::yield_now().await;
    assert_eq!(starts.load(Ordering::SeqCst), 0);
    let emitter_slot = Arc::new(Mutex::new(None));
    runtime
        .install(
            runtime.root(),
            TestPlugin::new("emitter", vec![], {
                let emitter_slot = emitter_slot.clone();
                move |ctx| {
                    let emitter_slot = emitter_slot.clone();
                    Box::pin(async move {
                        *emitter_slot.lock().expect("mutex") = Some(ctx);
                        Ok(())
                    })
                }
            }),
        )
        .await
        .expect("emitter");
    let emitter = emitter_slot
        .lock()
        .expect("mutex")
        .clone()
        .expect("context");
    emitter.emit(&event, Arc::new(())).await.expect("emit");
    assert!(calls.lock().expect("mutex").is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn aborted_shutdown_waiter_does_not_cancel_effect_cleanup_job() {
    let runtime = Runtime::new();
    let service = key("cancel-safe");
    let event = EventKey("test.cancel-safe".into());
    let context_slot = Arc::new(Mutex::new(None));
    let child_slot = Arc::new(Mutex::new(None));
    let handler_calls = Arc::new(Mutex::new(Vec::new()));
    let task_dropped = Arc::new(Mutex::new(false));
    let effect_order = Arc::new(Mutex::new(Vec::new()));
    let effect_counts = Arc::new([AtomicUsize::new(0), AtomicUsize::new(0)]);
    let child_cleaned = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(Semaphore::new(0));
    let (task_started_tx, task_started_rx) = oneshot::channel();
    let task_started_tx = Arc::new(Mutex::new(Some(task_started_tx)));
    let (effect_entered_tx, effect_entered_rx) = oneshot::channel();
    let effect_entered_tx = Arc::new(Mutex::new(Some(effect_entered_tx)));

    let parent = TestPlugin::contract("cancel-safe-parent", vec![], vec![service.clone()], {
        let service = service.clone();
        let event = event.clone();
        let context_slot = context_slot.clone();
        let child_slot = child_slot.clone();
        let handler_calls = handler_calls.clone();
        let task_dropped = task_dropped.clone();
        let task_started_tx = task_started_tx.clone();
        let effect_order = effect_order.clone();
        let effect_counts = effect_counts.clone();
        let effect_entered_tx = effect_entered_tx.clone();
        let release = release.clone();
        move |ctx| {
            let service = service.clone();
            let event = event.clone();
            let context_slot = context_slot.clone();
            let child_slot = child_slot.clone();
            let handler_calls = handler_calls.clone();
            let task_dropped = task_dropped.clone();
            let task_started = task_started_tx.lock().expect("mutex").take();
            let effect_order = effect_order.clone();
            let effect_counts = effect_counts.clone();
            let effect_entered = effect_entered_tx.lock().expect("mutex").take();
            let release = release.clone();
            Box::pin(async move {
                ctx.provide(service, 7_u32)?;
                ctx.on(
                    event,
                    Arc::new(RecordingHandler {
                        order: handler_calls,
                        id: 9,
                        outcome: false,
                    }),
                )?;
                ctx.spawn(async move {
                    let _guard = DropGuard(task_dropped);
                    if let Some(started) = task_started {
                        let _ = started.send(());
                    }
                    std::future::pending::<()>().await;
                })?;
                let child = ctx.create_scope("owned-child")?;
                *child_slot.lock().expect("mutex") = Some(child);
                ctx.effect(effect_fn({
                    let effect_order = effect_order.clone();
                    let effect_counts = effect_counts.clone();
                    move || async move {
                        effect_counts[0].fetch_add(1, Ordering::SeqCst);
                        effect_order.lock().expect("mutex").push(1);
                        Ok(())
                    }
                }))?;
                ctx.effect(effect_fn(move || {
                    let effect_order = effect_order.clone();
                    let effect_counts = effect_counts.clone();
                    let release = release.clone();
                    let effect_entered = effect_entered;
                    async move {
                        effect_counts[1].fetch_add(1, Ordering::SeqCst);
                        if let Some(entered) = effect_entered {
                            let _ = entered.send(());
                        }
                        release.acquire().await.expect("semaphore open").forget();
                        effect_order.lock().expect("mutex").push(2);
                        Ok(())
                    }
                }))?;
                *context_slot.lock().expect("mutex") = Some(ctx);
                Ok(())
            })
        }
    });
    runtime
        .install(runtime.root(), parent)
        .await
        .expect("install parent");
    task_started_rx.await.expect("task started");
    let child = child_slot.lock().expect("mutex").expect("child scope");
    runtime
        .install(
            child,
            TestPlugin::new("child", vec![], {
                let child_cleaned = child_cleaned.clone();
                move |ctx| {
                    let child_cleaned = child_cleaned.clone();
                    Box::pin(async move {
                        ctx.effect(effect_fn(move || async move {
                            child_cleaned.fetch_add(1, Ordering::SeqCst);
                            Ok(())
                        }))?;
                        Ok(())
                    })
                }
            }),
        )
        .await
        .expect("install child");

    let context = context_slot
        .lock()
        .expect("mutex")
        .clone()
        .expect("context");
    context
        .emit(&event, Arc::new(()))
        .await
        .expect("pre-shutdown emit");
    assert_eq!(*handler_calls.lock().expect("mutex"), vec![9]);
    handler_calls.lock().expect("mutex").clear();
    assert_eq!(runtime.snapshot().service_count, 1);

    let first_runtime = runtime.clone();
    let first = tokio::spawn(async move { first_runtime.shutdown().await });
    effect_entered_rx.await.expect("blocking effect entered");
    first.abort();
    assert!(
        first
            .await
            .expect_err("shutdown waiter aborted")
            .is_cancelled()
    );
    release.add_permits(1);
    wait_until(|| runtime.shutdown_state() == RuntimeShutdownState::Complete).await;

    assert_eq!(*effect_order.lock().expect("mutex"), vec![2, 1]);
    assert_eq!(effect_counts[0].load(Ordering::SeqCst), 1);
    assert_eq!(effect_counts[1].load(Ordering::SeqCst), 1);
    assert!(*task_dropped.lock().expect("mutex"));
    assert_eq!(child_cleaned.load(Ordering::SeqCst), 1);
    assert_eq!(runtime.snapshot().service_count, 0);
    assert!(matches!(
        context.emit(&event, Arc::new(())).await,
        Err(CordisError::RuntimeShuttingDown)
    ));
    assert!(handler_calls.lock().expect("mutex").is_empty());
    assert!(
        runtime
            .snapshot()
            .fibers
            .iter()
            .all(|fiber| fiber.state == FiberState::Disposed)
    );
    assert_eq!(runtime.shutdown_state(), RuntimeShutdownState::Complete);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn aborted_shutdown_during_child_cleanup_completes_without_retry() {
    let runtime = Runtime::new();
    let child = runtime
        .create_scope(runtime.root(), "blocking-child")
        .expect("child");
    let release = Arc::new(Semaphore::new(0));
    let count = Arc::new(AtomicUsize::new(0));
    let (entered_tx, entered_rx) = oneshot::channel();
    let entered_tx = Arc::new(Mutex::new(Some(entered_tx)));
    runtime
        .install(
            child,
            TestPlugin::new("blocking-child-plugin", vec![], {
                let release = release.clone();
                let count = count.clone();
                let entered_tx = entered_tx.clone();
                move |ctx| {
                    let release = release.clone();
                    let count = count.clone();
                    let entered = entered_tx.lock().expect("mutex").take();
                    Box::pin(async move {
                        ctx.effect(effect_fn(move || async move {
                            count.fetch_add(1, Ordering::SeqCst);
                            if let Some(entered) = entered {
                                let _ = entered.send(());
                            }
                            release.acquire().await.expect("semaphore open").forget();
                            Ok(())
                        }))?;
                        Ok(())
                    })
                }
            }),
        )
        .await
        .expect("install child plugin");
    let sibling_cleaned = Arc::new(AtomicUsize::new(0));
    runtime
        .install(
            runtime.root(),
            TestPlugin::new("root-sibling", vec![], {
                let sibling_cleaned = sibling_cleaned.clone();
                move |ctx| {
                    let sibling_cleaned = sibling_cleaned.clone();
                    Box::pin(async move {
                        ctx.effect(effect_fn(move || async move {
                            sibling_cleaned.fetch_add(1, Ordering::SeqCst);
                            Ok(())
                        }))?;
                        Ok(())
                    })
                }
            }),
        )
        .await
        .expect("install sibling");

    let first_runtime = runtime.clone();
    let first = tokio::spawn(async move { first_runtime.shutdown().await });
    entered_rx.await.expect("child cleanup entered");
    first.abort();
    assert!(
        first
            .await
            .expect_err("shutdown waiter aborted")
            .is_cancelled()
    );
    release.add_permits(1);
    wait_until(|| runtime.shutdown_state() == RuntimeShutdownState::Complete).await;

    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert_eq!(sibling_cleaned.load(Ordering::SeqCst), 1);
    assert!(
        runtime
            .snapshot()
            .fibers
            .iter()
            .all(|fiber| fiber.state == FiberState::Disposed)
    );
    assert_eq!(runtime.shutdown_state(), RuntimeShutdownState::Complete);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_scope_disposal_shares_one_operation() {
    let runtime = Runtime::new();
    let scope = runtime
        .create_scope(runtime.root(), "shared")
        .expect("scope");
    let cleaned = Arc::new(AtomicUsize::new(0));
    runtime
        .install(
            scope,
            TestPlugin::new("shared-cleanup", vec![], {
                let cleaned = cleaned.clone();
                move |ctx| {
                    let cleaned = cleaned.clone();
                    Box::pin(async move {
                        ctx.effect(effect_fn(move || async move {
                            cleaned.fetch_add(1, Ordering::SeqCst);
                            Ok(())
                        }))
                    })
                }
            }),
        )
        .await
        .expect("plugin");
    let barrier = Arc::new(Barrier::new(33));
    let waiters: Vec<_> = (0..32)
        .map(|_| {
            let runtime = runtime.clone();
            let barrier = barrier.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                runtime.dispose_scope(scope).await
            })
        })
        .collect();
    barrier.wait().await;
    let results = futures::future::join_all(waiters).await;
    assert!(results.into_iter().all(|result| matches!(
        result.expect("join"),
        Ok(()) | Err(CordisError::ScopeNotFound)
    )));
    assert_eq!(cleaned.load(Ordering::SeqCst), 1);
    let snapshot = runtime
        .snapshot()
        .scopes
        .into_iter()
        .find(|item| item.id == scope);
    assert!(snapshot.is_none_or(|item| item.state == cordis_runtime::ScopeState::Disposed));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_scope_waiter_does_not_cancel_scope_operation() {
    let runtime = Runtime::new();
    let scope = runtime
        .create_scope(runtime.root(), "cancelled")
        .expect("scope");
    let release = Arc::new(Semaphore::new(0));
    let count = Arc::new(AtomicUsize::new(0));
    let (entered_tx, entered_rx) = oneshot::channel();
    let entered_tx = Arc::new(Mutex::new(Some(entered_tx)));
    runtime
        .install(
            scope,
            TestPlugin::new("blocking", vec![], {
                let release = release.clone();
                let count = count.clone();
                let entered_tx = entered_tx.clone();
                move |ctx| {
                    let release = release.clone();
                    let count = count.clone();
                    let entered = entered_tx.lock().expect("mutex").take();
                    Box::pin(async move {
                        ctx.effect(effect_fn(move || async move {
                            count.fetch_add(1, Ordering::SeqCst);
                            if let Some(entered) = entered {
                                let _ = entered.send(());
                            }
                            release.acquire().await.expect("release").forget();
                            Ok(())
                        }))
                    })
                }
            }),
        )
        .await
        .expect("plugin");
    let dispose_runtime = runtime.clone();
    let waiter = tokio::spawn(async move { dispose_runtime.dispose_scope(scope).await });
    entered_rx.await.expect("entered cleanup");
    waiter.abort();
    assert!(waiter.await.expect_err("aborted waiter").is_cancelled());
    release.add_permits(1);
    wait_until(|| {
        runtime
            .snapshot()
            .scopes
            .iter()
            .find(|item| item.id == scope)
            .is_none_or(|item| item.state == cordis_runtime::ScopeState::Disposed)
    })
    .await;
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn ten_thousand_fiber_lifecycles_leave_no_active_fiber() {
    let runtime = Runtime::new();
    for _ in 0..10_000 {
        let fiber = runtime
            .install(
                runtime.root(),
                TestPlugin::new("churn", vec![], |_context| Box::pin(async { Ok(()) })),
            )
            .await
            .expect("install");
        runtime.dispose_fiber(fiber, false).await.expect("dispose");
        let _ = runtime.collect_garbage();
    }
    assert!(
        runtime
            .snapshot()
            .fibers
            .iter()
            .all(|fiber| fiber.state != FiberState::Active)
    );
}

fn fiber_state(runtime: &Runtime, fiber: cordis_core::FiberId) -> FiberState {
    runtime
        .snapshot()
        .fibers
        .into_iter()
        .find(|item| item.id == fiber)
        .expect("fiber snapshot")
        .state
}

async fn wait_until(predicate: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while !predicate() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("condition timeout");
}
