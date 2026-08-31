use super::support::{Plugin, assert_not_found_or_stale, capture_context, service_key};
use cordis_core::{CordisError, FiberState, effect_fn};
use cordis_runtime::{DisposeOutcome, Runtime};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::oneshot;

struct DropGuard(Arc<AtomicUsize>);
impl Drop for DropGuard {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn own_001_install_observer_drop_does_not_cancel_convergence() {
    let runtime = Runtime::new();
    let key = service_key("observer-drop");
    let (entered_tx, entered_rx) = oneshot::channel();
    let entered_tx = Arc::new(Mutex::new(Some(entered_tx)));
    let (release_tx, release_rx) = oneshot::channel();
    let release_rx = Arc::new(Mutex::new(Some(release_rx)));
    let worker_runtime = runtime.clone();
    let task = tokio::spawn(async move {
        worker_runtime
            .install(
                worker_runtime.root(),
                Plugin::contract(
                    "blocking-install",
                    0,
                    vec![],
                    vec![key.clone()],
                    move |context| {
                        let entered_tx = entered_tx.clone();
                        let release_rx = release_rx.clone();
                        let key = key.clone();
                        Box::pin(async move {
                            context.provide(key, 1_u32)?;
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
    entered_rx.await.expect("start entered");
    task.abort();
    assert!(task.await.expect_err("observer aborted").is_cancelled());
    release_tx.send(()).expect("release start");
    tokio::time::timeout(Duration::from_secs(2), async {
        while !runtime
            .snapshot()
            .fibers
            .iter()
            .any(|fiber| fiber.state == FiberState::Active)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("runtime-owned install converges");
}

#[tokio::test]
async fn own_002_fiber_disposal_observer_drop_does_not_cancel_convergence() {
    let runtime = Runtime::new();
    let cleaned = Arc::new(AtomicUsize::new(0));
    let (release_tx, release_rx) = oneshot::channel();
    let release_rx = Arc::new(Mutex::new(Some(release_rx)));
    let fiber = runtime
        .install(
            runtime.root(),
            Plugin::new("blocking-cleanup", {
                let cleaned = cleaned.clone();
                move |context| {
                    let cleaned = cleaned.clone();
                    let release_rx = release_rx.clone();
                    Box::pin(async move {
                        context.effect(effect_fn(move || async move {
                            let receiver = release_rx
                                .lock()
                                .expect("release mutex")
                                .take()
                                .expect("once");
                            let _ = receiver.await;
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
    let disposal_runtime = runtime.clone();
    let observer = tokio::spawn(async move { disposal_runtime.dispose_fiber(fiber, false).await });
    tokio::task::yield_now().await;
    observer.abort();
    release_tx.send(()).expect("release cleanup");
    tokio::time::timeout(Duration::from_secs(2), async {
        while cleaned.load(Ordering::SeqCst) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("runtime-owned disposal converges");
}

#[tokio::test]
async fn own_003_retained_context_does_not_own_fiber_lifetime() {
    let runtime = Runtime::new();
    let (fiber, context) = capture_context(&runtime, runtime.root(), "weak-context").await;
    runtime.dispose_fiber(fiber, false).await.expect("dispose");
    let _ = runtime.collect_garbage();
    let error = context
        .spawn(async {})
        .expect_err("retained context must fail");
    assert_not_found_or_stale(&error);
}

#[tokio::test]
async fn tsk_001_owned_task_is_cancelled_and_joined_on_disposal() {
    let runtime = Runtime::new();
    let dropped = Arc::new(AtomicUsize::new(0));
    let fiber = runtime
        .install(
            runtime.root(),
            Plugin::new("owned-task", {
                let dropped = dropped.clone();
                move |context| {
                    let dropped = dropped.clone();
                    Box::pin(async move {
                        context.spawn(async move {
                            let _guard = DropGuard(dropped);
                            std::future::pending::<()>().await;
                        })?;
                        Ok(())
                    })
                }
            }),
        )
        .await
        .expect("install");
    tokio::task::yield_now().await;
    runtime.dispose_fiber(fiber, false).await.expect("dispose");
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn tsk_002_task_panic_is_observed_and_effect_cleanup_continues() {
    let runtime = Runtime::new();
    let cleaned = Arc::new(AtomicUsize::new(0));
    let fiber = runtime
        .install(
            runtime.root(),
            Plugin::new("task-panic", {
                let cleaned = cleaned.clone();
                move |context| {
                    let cleaned = cleaned.clone();
                    Box::pin(async move {
                        context.effect(effect_fn(move || async move {
                            cleaned.fetch_add(1, Ordering::SeqCst);
                            Ok(())
                        }))?;
                        context.spawn(async move { panic!("conformance task panic") })?;
                        Ok(())
                    })
                }
            }),
        )
        .await
        .expect("install");
    tokio::time::timeout(Duration::from_secs(2), async {
        while runtime.health().task_panics == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("panic observed");
    runtime.dispose_fiber(fiber, false).await.expect("dispose");
    assert_eq!(cleaned.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn eff_001_effects_run_at_most_once_in_lifo_order() {
    let runtime = Runtime::new();
    let order = Arc::new(Mutex::new(Vec::new()));
    let fiber = runtime
        .install(
            runtime.root(),
            Plugin::new("effects", {
                let order = order.clone();
                move |context| {
                    let order = order.clone();
                    Box::pin(async move {
                        for marker in [1, 2, 3] {
                            let order = order.clone();
                            context.effect(effect_fn(move || async move {
                                order.lock().expect("order mutex").push(marker);
                                Ok(())
                            }))?;
                        }
                        Ok(())
                    })
                }
            }),
        )
        .await
        .expect("install");
    assert!(matches!(
        runtime.dispose_fiber_detailed(fiber, false).await,
        DisposeOutcome::Disposed
    ));
    assert_eq!(*order.lock().expect("order mutex"), vec![3, 2, 1]);
}

#[tokio::test]
async fn eff_002_cleanup_issue_does_not_rollback_committed_disposal_truth() {
    let runtime = Runtime::new();
    let fiber = runtime
        .install(
            runtime.root(),
            Plugin::new("effect-error", |context| {
                Box::pin(async move {
                    context.effect(effect_fn(|| async {
                        Err(CordisError::PluginDisposeFailed("expected".into()))
                    }))?;
                    Ok(())
                })
            }),
        )
        .await
        .expect("install");
    assert!(matches!(
        runtime.dispose_fiber_detailed(fiber, false).await,
        DisposeOutcome::CommittedWithCleanupIssues { .. }
    ));
}

#[tokio::test]
async fn obs_001_snapshot_and_health_are_detached_observations_not_runtime_authority() {
    let runtime = Runtime::new();
    let (fiber, _) = capture_context(&runtime, runtime.root(), "observation-owner").await;
    let before = runtime.snapshot();
    let health_before = runtime.health();
    let before_fiber = before
        .fibers
        .iter()
        .find(|item| item.id == fiber)
        .expect("fiber in old observation");
    assert_eq!(before_fiber.state, FiberState::Active);

    runtime
        .dispose_fiber(fiber, false)
        .await
        .expect("real runtime mutation");
    let after = runtime.snapshot();
    let health_after = runtime.health();
    assert_eq!(before_fiber.state, FiberState::Active);
    assert!(
        after
            .fibers
            .iter()
            .find(|item| item.id == fiber)
            .is_none_or(|item| item.state == FiberState::Disposed)
    );
    assert_eq!(health_before.status, health_after.status);
}
