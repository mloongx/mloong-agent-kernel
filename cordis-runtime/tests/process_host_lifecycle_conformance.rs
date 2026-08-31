//! Runtime-owned hosted lifecycle convergence across a real process boundary.

use async_trait::async_trait;
use cordis_core::{
    CordisError, DependencyPolicy, FiberId, FiberState, HostFailureKind, InvocationKey,
    InvocationValue, PluginDescriptor, PluginRevision,
};
use cordis_runtime::{
    Context, NativePlugin, PluginArtifact, ProcessHost, ReloadOutcome, Runtime, RuntimeConfig,
    ShutdownBlocker, ShutdownOutcome,
};
use parking_lot::Mutex;
use std::sync::Arc;
use std::time::Duration;

fn fixture(mode: &str) -> ProcessHost {
    ProcessHost::new(env!("CARGO_BIN_EXE_cordis-process-host-fixture")).arg(mode)
}

fn artifact() -> PluginArtifact {
    PluginArtifact::new("fixture", PluginRevision(1), [])
}

struct Caller(Arc<Mutex<Option<Context>>>);

#[async_trait]
impl NativePlugin for Caller {
    fn descriptor(&self) -> PluginDescriptor {
        PluginDescriptor {
            name: "lifecycle-caller".into(),
            dependencies: Arc::new([]),
            provisions: Arc::new([]),
            dependency_policy: DependencyPolicy::Restart,
            revision: PluginRevision(1),
        }
    }

    async fn start(&self, context: Context) -> Result<(), CordisError> {
        *self.0.lock() = Some(context);
        Ok(())
    }
}

async fn wait_reclaimed(runtime: &Runtime, fiber: FiberId) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if runtime
                .snapshot()
                .fibers
                .iter()
                .find(|item| item.id == fiber)
                .is_none_or(|item| item.state == FiberState::Disposed)
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("host failure must converge the exact hosted Fiber");
}

#[tokio::test]
async fn active_failure_after_commit_converges_through_runtime_ownership() {
    let runtime = Runtime::new();
    let hosted = runtime
        .install_hosted(runtime.root(), &fixture("crash_after_start"), artifact())
        .await
        .unwrap();
    wait_reclaimed(&runtime, hosted).await;
    assert!(
        runtime
            .snapshot()
            .fibers
            .iter()
            .find(|fiber| fiber.id == hosted)
            .is_none_or(|fiber| fiber.state == FiberState::Disposed)
    );
}

#[tokio::test]
async fn precommit_failure_rolls_back_without_a_second_convergence() {
    let runtime = Runtime::new();
    let result = runtime
        .install_hosted(runtime.root(), &fixture("crash_during_start"), artifact())
        .await;
    assert!(match result {
        Err(CordisError::Host(error)) => matches!(
            error.kind(),
            HostFailureKind::TransportClosed
                | HostFailureKind::ProcessExited
                | HostFailureKind::ProtocolViolation
        ),
        Err(CordisError::ActivationFailed { .. }) => true,
        _ => false,
    });
    assert!(
        runtime
            .snapshot()
            .fibers
            .iter()
            .all(|fiber| fiber.plugin.as_ref() != "process-fixture"
                || matches!(fiber.state, FiberState::Failed | FiberState::Disposed))
    );
}

#[tokio::test]
async fn postcommit_replacement_failure_never_resurrects_the_old_generation() {
    let runtime = Runtime::new();
    let old = runtime
        .install_hosted(runtime.root(), &fixture("remote_old"), artifact())
        .await
        .unwrap();
    let outcome = runtime
        .reload_hosted_detailed(old, &fixture("crash_after_start"), artifact())
        .await
        .unwrap();
    let new_fiber = match outcome {
        ReloadOutcome::Completed { new_fiber }
        | ReloadOutcome::CommittedWithCleanupPending { new_fiber, .. } => new_fiber,
    };
    wait_reclaimed(&runtime, new_fiber).await;
    assert!(
        runtime
            .snapshot()
            .fibers
            .iter()
            .find(|fiber| fiber.id == old)
            .is_none_or(|fiber| fiber.state == FiberState::Disposed)
    );
}

#[tokio::test]
async fn shutdown_uses_one_deadline_and_reaps_forced_host_before_completion() {
    let mut config = RuntimeConfig::default();
    config.shutdown_grace = Duration::from_millis(50);
    let runtime = Runtime::with_config(config).unwrap();
    let hosted = runtime
        .install_hosted(runtime.root(), &fixture("ignore_shutdown"), artifact())
        .await
        .unwrap();
    let first = runtime.shutdown_detailed().await;
    if let ShutdownOutcome::Incomplete { blockers, .. } = &first {
        assert!(blockers.iter().any(|blocker| matches!(
            blocker,
            ShutdownBlocker::HostedExecution { fiber } if *fiber == hosted
        )));
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let second = runtime.shutdown_detailed().await;
    assert!(
        matches!(
            &second,
            ShutdownOutcome::Complete | ShutdownOutcome::CompleteWithIssues { .. }
        ),
        "second shutdown outcome: {second:?}"
    );
}

#[tokio::test]
async fn host_failure_racing_explicit_dispose_has_one_disposal_convergence() {
    let runtime = Runtime::new();
    let hosted = runtime
        .install_hosted(runtime.root(), &fixture("crash_after_start"), artifact())
        .await
        .unwrap();
    let _ = runtime.dispose_fiber_detailed(hosted, false).await;
    wait_reclaimed(&runtime, hosted).await;
}

#[tokio::test]
async fn old_drain_crash_fails_old_work_without_affecting_replacement() {
    let runtime = Runtime::new();
    let old = runtime
        .install_hosted(runtime.root(), &fixture("delayed_crash_invoke"), artifact())
        .await
        .unwrap();
    let key = InvocationKey::new("fixture", "echo", 1);
    let captured = Arc::new(Mutex::new(None));
    runtime
        .install(runtime.root(), Caller(Arc::clone(&captured)))
        .await
        .unwrap();
    let old_context = captured.lock().clone().unwrap();
    let old_call = tokio::spawn({
        let key = key.clone();
        async move {
            old_context
                .invoke(
                    &key,
                    InvocationValue::External {
                        format: Arc::from("application/test"),
                        bytes: Arc::from([]),
                    },
                )
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    let outcome = runtime
        .reload_hosted_detailed(old, &fixture("remote_new"), artifact())
        .await;
    let new_fiber = match outcome {
        Ok(
            ReloadOutcome::Completed { new_fiber }
            | ReloadOutcome::CommittedWithCleanupPending { new_fiber, .. },
        )
        | Err(CordisError::ReloadCommitted { new_fiber, .. }) => new_fiber,
        other => panic!("unexpected old-drain reload result: {other:?}"),
    };
    assert!(matches!(old_call.await.unwrap(), Err(CordisError::Host(_))));
    assert_eq!(
        runtime
            .snapshot()
            .fibers
            .iter()
            .find(|fiber| fiber.id == new_fiber)
            .map(|fiber| fiber.state),
        Some(FiberState::Active)
    );
    let _ = runtime.dispose_fiber_detailed(new_fiber, false).await;
}
