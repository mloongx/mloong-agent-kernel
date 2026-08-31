use super::invocation_event::BlockingInvocation;
use super::support::{Plugin, capture_context, service_key};
use cordis_core::{CordisError, FiberState, InvocationKey, effect_fn};
use cordis_runtime::{ReloadOutcome, Runtime, invocation_handler_fn};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Notify, oneshot};

fn provider(key: cordis_core::ServiceKey, value: u32, revision: u64) -> Plugin {
    Plugin::contract(
        "reload-provider",
        revision,
        vec![],
        vec![key.clone()],
        move |context| {
            let key = key.clone();
            Box::pin(async move {
                context.provide(key, value)?;
                Ok(())
            })
        },
    )
}

#[tokio::test]
async fn ctx_003_retained_context_tracks_same_generation_scope_relocation() {
    let runtime = Runtime::new();
    let target = runtime
        .create_scope(runtime.root(), "ctx-relocation-target")
        .expect("target");
    let old = runtime
        .install(
            target,
            Plugin::new("ctx-old", |_| Box::pin(async { Ok(()) })),
        )
        .await
        .expect("old");
    let captured = Arc::new(Mutex::new(None));
    let during_start = Arc::new(Mutex::new(None));
    runtime
        .reload_detailed(
            old,
            Plugin::contract("ctx-replacement", 1, vec![], vec![], {
                let captured = captured.clone();
                let during_start = during_start.clone();
                move |context| {
                    let captured = captured.clone();
                    let during_start = during_start.clone();
                    Box::pin(async move {
                        *during_start.lock().expect("scope mutex") =
                            Some(context.scope().expect("staged current scope"));
                        *captured.lock().expect("context mutex") = Some(context);
                        Ok(())
                    })
                }
            }),
        )
        .await
        .expect("reload");
    let retained = captured
        .lock()
        .expect("context mutex")
        .clone()
        .expect("retained context");
    let staging_scope = during_start
        .lock()
        .expect("scope mutex")
        .expect("scope during start");
    assert_ne!(staging_scope, target);
    assert_eq!(retained.initial_scope(), staging_scope);
    assert_eq!(retained.scope().expect("committed current scope"), target);
    assert_eq!(
        retained.parent().expect("target parent"),
        Some(runtime.root())
    );
}

#[tokio::test]
async fn hmr_003_cutover_routes_new_work_to_new_generation_while_old_work_finishes() {
    let runtime = Runtime::new();
    let operation = InvocationKey::new("conformance", "hmr-cutover", 1);
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let old = runtime
        .install(
            runtime.root(),
            Plugin::new("hmr-old", {
                let operation = operation.clone();
                let entered = entered.clone();
                let release = release.clone();
                move |context| {
                    let operation = operation.clone();
                    let handler = BlockingInvocation {
                        entered: entered.clone(),
                        release: release.clone(),
                        value: 1,
                    };
                    Box::pin(async move {
                        context.handle_invocation(operation, Arc::new(handler))?;
                        Ok(())
                    })
                }
            }),
        )
        .await
        .expect("old provider");
    let (_, caller) = capture_context(&runtime, runtime.root(), "hmr-caller").await;
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
    let reload = tokio::spawn({
        let runtime = runtime.clone();
        let operation = operation.clone();
        async move {
            runtime
                .reload_detailed(
                    old,
                    Plugin::contract("hmr-new", 1, vec![], vec![], move |context| {
                        let operation = operation.clone();
                        Box::pin(async move {
                            context.handle_invocation(
                                operation,
                                invocation_handler_fn(|_, _: Arc<u32>| async {
                                    Ok(Arc::new(2_u32))
                                }),
                            )?;
                            Ok(())
                        })
                    }),
                )
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while runtime
            .snapshot()
            .fibers
            .iter()
            .any(|item| item.id == old && item.state != FiberState::Disposing)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cutover published");
    assert_eq!(
        *caller
            .invoke_typed::<u32, u32>(&operation, Arc::new(0))
            .await
            .expect("new routing after cutover"),
        2
    );
    release.notify_waiters();
    assert_eq!(*old_call.await.expect("old join").expect("old result"), 1);
    reload.await.expect("reload join").expect("reload outcome");
}

#[tokio::test]
async fn own_001_reload_observer_drop_does_not_cancel_convergence() {
    let runtime = Runtime::new();
    let key = service_key("reload-observer");
    let old = runtime
        .install(runtime.root(), provider(key.clone(), 1, 0))
        .await
        .expect("old");
    let (_, caller) = capture_context(&runtime, runtime.root(), "reload-reader").await;
    let (entered_tx, entered_rx) = oneshot::channel();
    let entered_tx = Arc::new(Mutex::new(Some(entered_tx)));
    let (release_tx, release_rx) = oneshot::channel();
    let release_rx = Arc::new(Mutex::new(Some(release_rx)));
    let worker_runtime = runtime.clone();
    let reload_key = key.clone();
    let observer = tokio::spawn(async move {
        worker_runtime
            .reload_detailed(
                old,
                Plugin::contract(
                    "reload-provider",
                    1,
                    vec![],
                    vec![reload_key.clone()],
                    move |context| {
                        let key = reload_key.clone();
                        let entered_tx = entered_tx.clone();
                        let release_rx = release_rx.clone();
                        Box::pin(async move {
                            context.provide(key, 2_u32)?;
                            entered_tx
                                .lock()
                                .expect("entered mutex")
                                .take()
                                .expect("once")
                                .send(())
                                .ok();
                            let receiver = release_rx
                                .lock()
                                .expect("release mutex")
                                .take()
                                .expect("once");
                            let _ = receiver.await;
                            Ok(())
                        })
                    },
                ),
            )
            .await
    });
    entered_rx.await.expect("replacement entered");
    observer.abort();
    release_tx.send(()).expect("release replacement");
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if caller.get::<u32>(&key).is_ok_and(|value| *value == 2) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("runtime-owned reload converges");
}

#[tokio::test]
async fn gen_001_reload_preserves_plugin_identity_and_replaces_fiber_identity() {
    let runtime = Runtime::new();
    let key = service_key("generation-identity");
    let old = runtime
        .install(runtime.root(), provider(key.clone(), 1, 0))
        .await
        .expect("old");
    let old_snapshot = runtime
        .snapshot()
        .fibers
        .into_iter()
        .find(|fiber| fiber.id == old)
        .expect("old snapshot");
    let new = match runtime
        .reload_detailed(old, provider(key, 2, 1))
        .await
        .expect("reload")
    {
        ReloadOutcome::Completed { new_fiber }
        | ReloadOutcome::CommittedWithCleanupPending { new_fiber, .. } => new_fiber,
    };
    let new_snapshot = runtime
        .snapshot()
        .fibers
        .into_iter()
        .find(|fiber| fiber.id == new)
        .expect("new snapshot");
    assert_ne!(old, new);
    assert_eq!(old_snapshot.plugin_id, new_snapshot.plugin_id);
}

#[tokio::test]
async fn gen_003_ctx_004_old_context_never_acquires_replacement_authority() {
    let runtime = Runtime::new();
    let key = service_key("stale-context");
    let captured = Arc::new(Mutex::new(None));
    let old = runtime
        .install(
            runtime.root(),
            Plugin::contract("reload-provider", 0, vec![], vec![key.clone()], {
                let key = key.clone();
                let captured = captured.clone();
                move |context| {
                    let key = key.clone();
                    let captured = captured.clone();
                    Box::pin(async move {
                        context.provide(key, 1_u32)?;
                        *captured.lock().expect("capture mutex") = Some(context);
                        Ok(())
                    })
                }
            }),
        )
        .await
        .expect("old");
    let old_context = captured
        .lock()
        .expect("capture mutex")
        .clone()
        .expect("old context");
    runtime
        .reload_detailed(old, provider(key.clone(), 2, 1))
        .await
        .expect("reload");
    assert!(matches!(
        old_context.get::<u32>(&key),
        Err(CordisError::StaleContextGeneration { .. } | CordisError::FiberNotFound)
    ));
}

#[tokio::test]
async fn own_004_svc_004_existing_service_handle_never_retargets() {
    let runtime = Runtime::new();
    let key = service_key("exact-generation-handle");
    let cleaned = Arc::new(AtomicUsize::new(0));
    let old = runtime
        .install(
            runtime.root(),
            Plugin::contract("reload-provider", 0, vec![], vec![key.clone()], {
                let key = key.clone();
                let cleaned = cleaned.clone();
                move |context| {
                    let key = key.clone();
                    let cleaned = cleaned.clone();
                    Box::pin(async move {
                        context.provide(key, 1_u32)?;
                        context.effect(effect_fn(move || async move {
                            cleaned.fetch_add(1, Ordering::SeqCst);
                            Ok(())
                        }))?;
                        Ok(())
                    })
                }
            }),
        )
        .await
        .expect("old");
    let (_, caller) = capture_context(&runtime, runtime.root(), "handle-reader").await;
    let old_handle = caller.get::<u32>(&key).expect("old handle");
    let outcome = runtime
        .reload_detailed(old, provider(key.clone(), 2, 1))
        .await
        .expect("reload commit");
    assert!(matches!(
        outcome,
        ReloadOutcome::CommittedWithCleanupPending { .. }
    ));
    assert_eq!(*old_handle, 1);
    assert_eq!(*caller.get::<u32>(&key).expect("new handle"), 2);
    assert_eq!(cleaned.load(Ordering::SeqCst), 0);
    drop(old_handle);
    tokio::time::timeout(Duration::from_secs(2), async {
        while cleaned.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("old generation cleanup");
}

#[tokio::test]
async fn hmr_002_precommit_failure_keeps_old_generation_authoritative() {
    let runtime = Runtime::new();
    let key = service_key("precommit");
    let old = runtime
        .install(runtime.root(), provider(key.clone(), 1, 0))
        .await
        .expect("old");
    let (_, caller) = capture_context(&runtime, runtime.root(), "precommit-reader").await;
    let replacement = Plugin::contract("reload-provider", 1, vec![], vec![key.clone()], {
        let key = key.clone();
        move |context| {
            let key = key.clone();
            Box::pin(async move {
                context.provide(key, 2_u32)?;
                Err(CordisError::PluginStartFailed("expected".into()))
            })
        }
    });
    assert!(matches!(
        runtime.reload_detailed(old, replacement).await,
        Err(CordisError::ReloadFailed { .. })
    ));
    assert_eq!(*caller.get::<u32>(&key).expect("old retained"), 1);
}

#[tokio::test]
async fn hmr_004_postcommit_cleanup_issue_cannot_rollback_new_generation() {
    let runtime = Runtime::new();
    let key = service_key("postcommit");
    let old = runtime
        .install(
            runtime.root(),
            Plugin::contract("reload-provider", 0, vec![], vec![key.clone()], {
                let key = key.clone();
                move |context| {
                    let key = key.clone();
                    Box::pin(async move {
                        context.provide(key, 1_u32)?;
                        context.effect(effect_fn(|| async {
                            Err(CordisError::PluginDisposeFailed("expected".into()))
                        }))?;
                        Ok(())
                    })
                }
            }),
        )
        .await
        .expect("old");
    let (_, caller) = capture_context(&runtime, runtime.root(), "postcommit-reader").await;
    let error = runtime
        .reload_detailed(old, provider(key.clone(), 2, 1))
        .await
        .expect_err("cleanup issue");
    assert!(matches!(error, CordisError::ReloadCommitted { .. }));
    assert_eq!(*caller.get::<u32>(&key).expect("new authoritative"), 2);
    assert!(
        runtime.snapshot().fibers.iter().any(
            |fiber| fiber.state == FiberState::Active && fiber.provided_services.contains(&key)
        )
    );
}
