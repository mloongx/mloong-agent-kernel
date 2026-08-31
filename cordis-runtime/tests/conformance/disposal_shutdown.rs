use super::support::{Plugin, capture_context, service_key};
use cordis_core::{CordisError, effect_fn};
use cordis_runtime::{
    Runtime, RuntimeConfig, RuntimeShutdownState, ScopeDisposeOutcome, ShutdownOutcome,
};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::oneshot;

#[tokio::test]
async fn scp_002_parent_disposal_waits_for_child_cleanup_convergence() {
    let runtime = Runtime::new();
    let parent = runtime
        .create_scope(runtime.root(), "parent")
        .expect("parent");
    let child = runtime.create_scope(parent, "child").expect("child");
    let (entered_tx, entered_rx) = oneshot::channel();
    let entered_tx = Arc::new(Mutex::new(Some(entered_tx)));
    let (release_tx, release_rx) = oneshot::channel();
    let release_rx = Arc::new(Mutex::new(Some(release_rx)));
    runtime
        .install(
            child,
            Plugin::new("child-cleanup", move |context| {
                let entered_tx = entered_tx.clone();
                let release_rx = release_rx.clone();
                Box::pin(async move {
                    context.effect(effect_fn(move || async move {
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
                    }))?;
                    Ok(())
                })
            }),
        )
        .await
        .expect("child plugin");
    let worker = runtime.clone();
    let disposal = tokio::spawn(async move { worker.dispose_scope_detailed(parent).await });
    entered_rx.await.expect("child cleanup entered");
    assert!(!disposal.is_finished());
    release_tx.send(()).expect("release child");
    assert!(matches!(
        disposal.await.expect("join"),
        ScopeDisposeOutcome::Disposed
    ));
}

#[tokio::test]
async fn scp_003_disposed_scope_rejects_children_and_has_no_active_fibers() {
    let runtime = Runtime::new();
    let scope = runtime
        .create_scope(runtime.root(), "terminal")
        .expect("scope");
    let _ = capture_context(&runtime, scope, "owned-fiber").await;
    assert!(matches!(
        runtime.dispose_scope_detailed(scope).await,
        ScopeDisposeOutcome::Disposed
    ));
    assert!(matches!(
        runtime.create_scope(scope, "late"),
        Err(CordisError::ScopeDisposed(_) | CordisError::ScopeNotFound)
    ));
    assert!(
        !runtime
            .snapshot()
            .fibers
            .iter()
            .any(|fiber| fiber.scope == scope && fiber.state == cordis_core::FiberState::Active)
    );
}

#[tokio::test]
async fn dsp_001_registered_observers_share_one_immutable_completion() {
    let runtime = Runtime::new();
    let scope = runtime
        .create_scope(runtime.root(), "shared-disposal")
        .expect("scope");
    let left_runtime = runtime.clone();
    let right_runtime = runtime.clone();
    let left = tokio::spawn(async move { left_runtime.dispose_scope_detailed(scope).await });
    let right = tokio::spawn(async move { right_runtime.dispose_scope_detailed(scope).await });
    assert_eq!(left.await.expect("left"), right.await.expect("right"));
}

#[tokio::test]
async fn dsp_002_registration_begins_on_first_poll_and_fresh_request_after_gc_is_not_found() {
    let runtime = Runtime::new();
    let untouched = runtime
        .create_scope(runtime.root(), "unpolled")
        .expect("scope");
    let unpolled = runtime.dispose_scope(untouched);
    drop(unpolled);
    runtime
        .create_scope(untouched, "still-active")
        .expect("unpolled future did not register");

    let observed = runtime
        .create_scope(runtime.root(), "observed")
        .expect("scope");
    let observer_runtime = runtime.clone();
    let waiter =
        tokio::spawn(async move { observer_runtime.dispose_scope_detailed(observed).await });
    assert!(matches!(
        waiter.await.expect("registered observer"),
        ScopeDisposeOutcome::Disposed
    ));
    let _ = runtime.collect_garbage();
    assert!(matches!(
        runtime.dispose_scope(observed).await,
        Err(CordisError::ScopeNotFound)
    ));
}

#[tokio::test]
async fn shd_001_shutdown_closes_admission_permanently() {
    let runtime = Runtime::new();
    assert!(matches!(
        runtime.shutdown_detailed().await,
        ShutdownOutcome::Complete
    ));
    assert_eq!(runtime.shutdown_state(), RuntimeShutdownState::Complete);
    assert!(matches!(
        runtime.create_scope(runtime.root(), "late"),
        Err(CordisError::RuntimeShuttingDown)
    ));
    assert!(matches!(
        runtime
            .install(
                runtime.root(),
                Plugin::new("late", |_| Box::pin(async { Ok(()) }))
            )
            .await,
        Err(CordisError::RuntimeShuttingDown)
    ));
}

#[tokio::test]
async fn shd_002_concurrent_callers_share_attempt_and_observer_drop_does_not_cancel() {
    let runtime = Runtime::new();
    let cleaned = Arc::new(AtomicUsize::new(0));
    runtime
        .install(
            runtime.root(),
            Plugin::new("shutdown-cleanup", {
                let cleaned = cleaned.clone();
                move |context| {
                    let cleaned = cleaned.clone();
                    Box::pin(async move {
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
        .expect("install");
    let first_runtime = runtime.clone();
    let second_runtime = runtime.clone();
    let first = tokio::spawn(async move { first_runtime.shutdown_detailed().await });
    let second = tokio::spawn(async move { second_runtime.shutdown_detailed().await });
    first.abort();
    let outcome = second.await.expect("remaining observer");
    assert!(matches!(
        outcome,
        ShutdownOutcome::Complete | ShutdownOutcome::CompleteWithIssues { .. }
    ));
    assert_eq!(cleaned.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn shd_003_incomplete_blockers_allow_retry_without_reopening_admission() {
    let mut config = RuntimeConfig::default();
    config.shutdown_grace = Duration::from_millis(30);
    let runtime = Runtime::with_config(config).expect("runtime");
    let key = service_key("shutdown-blocker");
    runtime
        .install(
            runtime.root(),
            Plugin::contract("provider", 0, vec![], vec![key.clone()], {
                let key = key.clone();
                move |context| {
                    let key = key.clone();
                    Box::pin(async move {
                        context.provide(key, 1_u32)?;
                        Ok(())
                    })
                }
            }),
        )
        .await
        .expect("provider");
    let (_, caller) = capture_context(&runtime, runtime.root(), "handle-owner").await;
    let handle = caller.get::<u32>(&key).expect("handle");
    assert!(
        matches!(runtime.shutdown_detailed().await, ShutdownOutcome::Incomplete { blockers, .. } if !blockers.is_empty())
    );
    assert!(matches!(
        runtime.create_scope(runtime.root(), "late"),
        Err(CordisError::RuntimeShuttingDown)
    ));
    drop(handle);
    assert!(matches!(
        runtime.shutdown_detailed().await,
        ShutdownOutcome::Complete | ShutdownOutcome::CompleteWithIssues { .. }
    ));
}

#[tokio::test]
async fn shd_004_one_absolute_deadline_bounds_multiple_blockers() {
    let mut config = RuntimeConfig::default();
    config.shutdown_grace = Duration::from_millis(40);
    config.task_grace = Duration::from_secs(1);
    let runtime = Runtime::with_config(config).expect("runtime");
    for name in ["blocker-a", "blocker-b"] {
        runtime
            .install(
                runtime.root(),
                Plugin::new(name, |context| {
                    Box::pin(async move {
                        context.spawn(std::future::pending::<()>())?;
                        Ok(())
                    })
                }),
            )
            .await
            .expect("install blocker");
    }
    let started = Instant::now();
    let _ = runtime.shutdown_detailed().await;
    assert!(
        started.elapsed() < Duration::from_millis(160),
        "one global deadline, not one per blocker"
    );
}
