//! Executable conformance tests for the reference child-process host.

use cordis_core::{CordisError, HostFailureKind, PluginRevision};
use cordis_runtime::{DisposeOutcome, PluginArtifact, ProcessHost, ProcessHostConfig, Runtime};
use std::{path::PathBuf, sync::Arc, time::Duration};

fn fixture(mode: &str) -> ProcessHost {
    ProcessHost::new(env!("CARGO_BIN_EXE_cordis-process-host-fixture")).arg(mode)
}

fn artifact(payload: impl Into<Arc<[u8]>>) -> PluginArtifact {
    PluginArtifact::new("fixture", PluginRevision(11), payload)
}

#[tokio::test]
async fn normal_process_plugin_loads_starts_disposes_and_reaps() {
    let runtime = Runtime::new();
    let fiber = runtime
        .install_hosted(runtime.root(), &fixture("normal"), artifact([]))
        .await
        .unwrap();
    assert_eq!(runtime.health().active_fibers, 1);
    assert_eq!(
        runtime.dispose_fiber_detailed(fiber, false).await,
        DisposeOutcome::Disposed
    );
}

#[tokio::test]
async fn handshake_and_load_failures_are_typed() {
    let runtime = Runtime::new();
    for (mode, kind) in [
        ("reject_version", HostFailureKind::HandshakeIncompatible),
        ("missing_feature", HostFailureKind::HandshakeIncompatible),
        ("crash_during_handshake", HostFailureKind::TransportClosed),
        ("crash_during_load", HostFailureKind::TransportClosed),
        ("oversized_frame_header", HostFailureKind::MessageTooLarge),
        ("pre_ready_message", HostFailureKind::ProtocolViolation),
        ("future_response_id", HostFailureKind::ProtocolViolation),
    ] {
        let result = runtime
            .install_hosted(runtime.root(), &fixture(mode), artifact([]))
            .await;
        let expected = matches!(&result, Err(CordisError::Host(error))
            if error.kind() == kind
                || (mode.starts_with("crash_during_")
                    && error.kind() == HostFailureKind::ProcessExited));
        assert!(expected, "mode {mode}: {result:?}");
    }
}

#[tokio::test]
async fn descriptor_services_are_rejected() {
    let runtime = Runtime::new();
    for mode in [
        "descriptor_with_service_dependency",
        "descriptor_with_service_provision",
    ] {
        let result = runtime
            .install_hosted(runtime.root(), &fixture(mode), artifact([]))
            .await;
        let unsupported = match &result {
            Err(CordisError::Host(error)) => error.kind() == HostFailureKind::UnsupportedCapability,
            Err(CordisError::ActivationFailed { primary, .. }) => {
                matches!(primary.as_ref(), CordisError::Host(error) if error.kind() == HostFailureKind::UnsupportedCapability)
            }
            _ => false,
        };
        assert!(unsupported, "mode {mode}: {result:?}");
    }
}

#[tokio::test]
async fn peer_min_limits_late_duplicate_response_and_descriptor_bounds_are_enforced() {
    let runtime = Runtime::new();
    for mode in ["small_limits", "duplicate_hello_response"] {
        let fiber = runtime
            .install_hosted(runtime.root(), &fixture(mode), artifact([]))
            .await
            .unwrap();
        assert_eq!(
            runtime.dispose_fiber_detailed(fiber, false).await,
            DisposeOutcome::Disposed
        );
    }
    let bad = runtime
        .install_hosted(
            runtime.root(),
            &fixture("oversized_descriptor_name"),
            artifact([]),
        )
        .await;
    assert!(
        matches!(bad, Err(CordisError::Host(error)) if error.kind() == HostFailureKind::MessageTooLarge)
    );
}

#[tokio::test]
async fn oversized_artifact_is_rejected_locally() {
    let mut config = ProcessHostConfig::default();
    config.max_frame_bytes = 32;
    config.max_artifact_bytes = 16;
    config.max_request_bytes = 16;
    config.max_response_bytes = 16;
    config.max_control_bytes = 16;
    let host = ProcessHost::with_config(env!("CARGO_BIN_EXE_cordis-process-host-fixture"), config)
        .arg("normal");
    let runtime = Runtime::new();
    let result = runtime
        .install_hosted(runtime.root(), &host, artifact(vec![0; 17]))
        .await;
    assert!(
        matches!(result, Err(CordisError::Host(error)) if error.kind() == HostFailureKind::MessageTooLarge)
    );
}

#[tokio::test]
async fn max_control_bytes_is_enforced() {
    let mut config = ProcessHostConfig::default();
    config.max_control_bytes = 8;
    let host = ProcessHost::with_config(env!("CARGO_BIN_EXE_cordis-process-host-fixture"), config)
        .arg("normal");
    let runtime = Runtime::new();
    let result = runtime
        .install_hosted(runtime.root(), &host, artifact([]))
        .await;
    assert!(matches!(result, Err(CordisError::Host(error))
        if error.kind() == HostFailureKind::MessageTooLarge));
}

#[tokio::test]
async fn start_domain_and_host_failures_remain_structured() {
    let runtime = Runtime::new();
    let domain = runtime
        .install_hosted(runtime.root(), &fixture("start_domain_error"), artifact([]))
        .await;
    let domain_typed = match &domain {
        Err(CordisError::RemoteDomain(error)) => error.code() == "fixture.start",
        Err(CordisError::ActivationFailed { primary, .. }) => matches!(
            primary.as_ref(),
            CordisError::RemoteDomain(error) if error.code() == "fixture.start"
        ),
        _ => false,
    };
    assert!(domain_typed, "{domain:?}");
    let crash = runtime
        .install_hosted(runtime.root(), &fixture("crash_during_start"), artifact([]))
        .await;
    assert!(
        matches!(crash, Err(CordisError::ActivationFailed { primary, .. })
        if matches!(primary.as_ref(), CordisError::Host(_)))
    );
}

#[tokio::test]
async fn disposal_preserves_remote_domain_cleanup_cause() {
    let runtime = Runtime::new();
    let fiber = runtime
        .install_hosted(
            runtime.root(),
            &fixture("dispose_domain_error"),
            artifact([]),
        )
        .await
        .unwrap();
    let outcome = runtime.dispose_fiber_detailed(fiber, false).await;
    assert!(
        matches!(outcome, DisposeOutcome::CommittedWithCleanupIssues { issues }
        if matches!(issues.as_slice(), [issue] if matches!(issue.cause, Some(CordisError::RemoteDomain(ref error)) if error.code() == "fixture.dispose")))
    );
}

#[tokio::test]
async fn disposal_preserves_host_cleanup_cause() {
    let runtime = Runtime::new();
    let fiber = runtime
        .install_hosted(
            runtime.root(),
            &fixture("crash_during_dispose"),
            artifact([]),
        )
        .await
        .unwrap();
    let outcome = runtime.dispose_fiber_detailed(fiber, false).await;
    assert!(
        matches!(outcome, DisposeOutcome::CommittedWithCleanupIssues { issues }
        if matches!(issues.as_slice(), [issue] if matches!(issue.cause, Some(CordisError::Host(_)))))
    );
}

#[tokio::test]
async fn exit_after_shutdown_without_ack_is_typed_failure_not_clean_close() {
    let runtime = Runtime::new();
    let fiber = runtime
        .install_hosted(
            runtime.root(),
            &fixture("exit_after_shutdown_without_ack"),
            artifact([]),
        )
        .await
        .unwrap();
    let outcome = runtime.dispose_fiber_detailed(fiber, false).await;
    assert!(
        matches!(outcome, DisposeOutcome::CommittedWithCleanupIssues { issues }
        if matches!(issues.as_slice(), [issue] if matches!(issue.cause, Some(CordisError::Host(_)))))
    );
}

#[tokio::test]
async fn cancelled_handshake_and_load_kill_and_reap_real_children() {
    for mode in ["stall_handshake", "stall_load"] {
        let path = observation_path(mode);
        let host = fixture(mode).arg(path.as_os_str());
        let runtime = Runtime::new();
        let task = tokio::spawn({
            let runtime = runtime.clone();
            async move {
                runtime
                    .install_hosted(runtime.root(), &host, artifact([]))
                    .await
            }
        });
        let pid = wait_observation(
            &path,
            if mode == "stall_load" {
                "load"
            } else {
                "handshake"
            },
        )
        .await;
        task.abort();
        let _ = task.await;
        wait_process_exit(pid).await;
        let _ = std::fs::remove_file(path);
    }
}

fn observation_path(mode: &str) -> PathBuf {
    let test_name: String = std::thread::current()
        .name()
        .unwrap_or("test")
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character
            } else {
                '_'
            }
        })
        .collect();
    std::env::temp_dir().join(format!(
        "cordis-{mode}-{}-{}.txt",
        std::process::id(),
        test_name
    ))
}

async fn wait_observation(path: &PathBuf, expected: &str) -> u32 {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(value) = std::fs::read_to_string(path) {
                if let Some((pid, event)) = value.split_once(':') {
                    if event == expected {
                        return pid.parse().unwrap();
                    }
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("fixture observation timeout")
}

async fn wait_process_exit(pid: u32) {
    tokio::time::timeout(Duration::from_secs(5), async move {
        while process_exists(pid) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("child was not killed and reaped");
}

#[cfg(windows)]
fn process_exists(pid: u32) -> bool {
    let output = std::process::Command::new("tasklist")
        .args(["/FI", &format!("PID eq {pid}"), "/NH"])
        .output()
        .expect("tasklist");
    String::from_utf8_lossy(&output.stdout).contains(&pid.to_string())
}

#[cfg(unix)]
fn process_exists(pid: u32) -> bool {
    std::process::Command::new("kill")
        .args(["-0", &pid.to_string()])
        .status()
        .is_ok_and(|status| status.success())
}
