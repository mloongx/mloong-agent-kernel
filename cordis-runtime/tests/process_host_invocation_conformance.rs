//! Executable remote-invocation conformance for the private `ProcessHost` codec.

use async_trait::async_trait;
use cordis_core::{
    CordisError, DependencyPolicy, HostFailureKind, InvocationKey, InvocationValue,
    PluginDescriptor, PluginRevision,
};
use cordis_runtime::{
    Context, DisposeOutcome, NativePlugin, PluginArtifact, ProcessHost, ProcessHostConfig, Runtime,
};
use parking_lot::Mutex;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

const FORMAT: &str = "application/test";

fn key() -> InvocationKey {
    InvocationKey::new("fixture", "echo", 1)
}

fn fixture(mode: &str) -> ProcessHost {
    ProcessHost::new(env!("CARGO_BIN_EXE_cordis-process-host-fixture")).arg(mode)
}

fn artifact() -> PluginArtifact {
    PluginArtifact::new("fixture", PluginRevision(1), [])
}

fn external(bytes: impl Into<Arc<[u8]>>) -> InvocationValue {
    InvocationValue::External {
        format: Arc::from(FORMAT),
        bytes: bytes.into(),
    }
}

fn external_bytes(value: InvocationValue) -> Arc<[u8]> {
    match value {
        InvocationValue::External { format, bytes } => {
            assert_eq!(&*format, FORMAT);
            bytes
        }
        InvocationValue::Native(_) => panic!("remote invocation returned Native"),
    }
}

struct CallerPlugin(Arc<Mutex<Option<Context>>>);

#[async_trait]
impl NativePlugin for CallerPlugin {
    fn descriptor(&self) -> PluginDescriptor {
        PluginDescriptor {
            name: "caller".into(),
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

async fn setup(mode: &str) -> (Runtime, Context, cordis_core::FiberId) {
    setup_host(fixture(mode)).await
}

async fn setup_host(host: ProcessHost) -> (Runtime, Context, cordis_core::FiberId) {
    let runtime = Runtime::new();
    let hosted = runtime
        .install_hosted(runtime.root(), &host, artifact())
        .await
        .unwrap();
    let captured = Arc::new(Mutex::new(None));
    runtime
        .install(runtime.root(), CallerPlugin(Arc::clone(&captured)))
        .await
        .unwrap();
    let context = captured.lock().clone().unwrap();
    (runtime, context, hosted)
}

#[tokio::test]
async fn external_echo_and_native_rejection_use_the_existing_registry() {
    let (runtime, context, hosted) = setup("remote_echo").await;
    let result = context
        .invoke(&key(), external(&b"hello"[..]))
        .await
        .unwrap();
    assert_eq!(&*external_bytes(result), b"hello");
    let native = context
        .invoke(&key(), InvocationValue::native(Arc::new(7_u32)))
        .await;
    assert!(matches!(native, Err(CordisError::InvocationTypeMismatch(value)) if value == key()));
    assert_eq!(
        runtime.dispose_fiber_detailed(hosted, false).await,
        DisposeOutcome::Disposed
    );
}

#[tokio::test]
async fn domain_and_request_scoped_host_failures_remain_structured_and_healthy() {
    let (runtime, context, hosted) = setup("remote_domain").await;
    let domain = context.invoke(&key(), external([])).await.unwrap_err();
    assert!(matches!(domain, CordisError::RemoteDomain(error)
        if error.code() == "fixture.domain"
        && error.message() == "remote domain failure"
        && matches!(error.details(), Some(("application/test", bytes)) if bytes == b"details")));
    assert!(matches!(
        context.invoke(&key(), external([])).await,
        Err(CordisError::RemoteDomain(_))
    ));
    assert_eq!(
        runtime.dispose_fiber_detailed(hosted, false).await,
        DisposeOutcome::Disposed
    );

    let (_runtime, context, _hosted) = setup("unsupported_format").await;
    assert!(matches!(context.invoke(&key(), external([])).await,
        Err(CordisError::Host(error)) if error.kind() == HostFailureKind::UnsupportedFormat));
}

#[tokio::test]
async fn request_and_response_limits_are_independent_and_bounded() {
    let mut config = ProcessHostConfig::default();
    config.max_request_bytes = 4;
    config.max_response_bytes = 4;
    let host = ProcessHost::with_config(env!("CARGO_BIN_EXE_cordis-process-host-fixture"), config)
        .arg("oversized_invoke_response");
    let runtime = Runtime::new();
    let hosted = runtime
        .install_hosted(runtime.root(), &host, artifact())
        .await
        .unwrap();
    let captured = Arc::new(Mutex::new(None));
    runtime
        .install(runtime.root(), CallerPlugin(Arc::clone(&captured)))
        .await
        .unwrap();
    let context = captured.lock().clone().unwrap();
    assert!(matches!(context.invoke(&key(), external(vec![0; 5])).await,
        Err(CordisError::Host(error)) if error.kind() == HostFailureKind::MessageTooLarge));
    assert!(matches!(context.invoke(&key(), external(vec![0; 4])).await,
        Err(CordisError::Host(error)) if error.kind() == HostFailureKind::MessageTooLarge));
    let _ = runtime.dispose_fiber_detailed(hosted, false).await;
}

#[tokio::test]
async fn missing_required_invocation_features_reject_publication() {
    for mode in ["missing_invocation", "missing_cancel", "missing_deadline"] {
        let runtime = Runtime::new();
        let result = runtime
            .install_hosted(runtime.root(), &fixture(mode), artifact())
            .await;
        assert!(
            matches!(&result,
            Err(CordisError::Host(error))
                if error.kind() == HostFailureKind::UnsupportedCapability)
                || matches!(&result,
                Err(CordisError::ActivationFailed { primary, .. })
                if matches!(primary.as_ref(), CordisError::Host(error)
                    if error.kind() == HostFailureKind::UnsupportedCapability)),
            "{mode}: {result:?}"
        );
    }
    let runtime = Runtime::new();
    assert!(
        matches!(runtime.install_hosted(runtime.root(), &fixture("unknown_required_feature"), artifact()).await,
        Err(CordisError::Host(error)) if error.kind() == HostFailureKind::HandshakeIncompatible)
    );
    let fiber = runtime
        .install_hosted(
            runtime.root(),
            &fixture("unknown_optional_feature"),
            artifact(),
        )
        .await
        .unwrap();
    assert_eq!(
        runtime.dispose_fiber_detailed(fiber, false).await,
        DisposeOutcome::Disposed
    );
}

#[tokio::test]
async fn inflight_exhaustion_rejects_immediately_and_cancellation_restores_capacity() {
    let mut config = ProcessHostConfig::default();
    config.max_inflight_requests = 4;
    let host = ProcessHost::with_config(env!("CARGO_BIN_EXE_cordis-process-host-fixture"), config)
        .arg("delayed_invoke");
    let runtime = Runtime::new();
    let hosted = runtime
        .install_hosted(runtime.root(), &host, artifact())
        .await
        .unwrap();
    let captured = Arc::new(Mutex::new(None));
    runtime
        .install(runtime.root(), CallerPlugin(Arc::clone(&captured)))
        .await
        .unwrap();
    let context = captured.lock().clone().unwrap();
    let mut active = Vec::new();
    for _ in 0..4 {
        active.push(tokio::spawn({
            let context = context.clone();
            async move { context.invoke(&key(), external([])).await }
        }));
    }
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(matches!(context.invoke(&key(), external([])).await,
        Err(CordisError::Host(error)) if error.kind() == HostFailureKind::Overloaded));
    for task in active {
        task.abort();
        let _ = task.await;
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        context
            .invoke_with_timeout(&key(), external([]), Duration::from_secs(1))
            .await
            .is_ok()
    );
    assert_eq!(
        runtime.dispose_fiber_detailed(hosted, false).await,
        DisposeOutcome::Disposed
    );
}

#[tokio::test]
async fn local_deadline_wins_and_late_success_does_not_revive_it() {
    let (runtime, context, hosted) = setup("delayed_invoke").await;
    assert!(matches!(
        context
            .invoke_with_timeout(&key(), external([]), Duration::from_millis(20))
            .await,
        Err(CordisError::InvocationTimedOut)
    ));
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(
        context
            .invoke_with_timeout(&key(), external([]), Duration::from_secs(1))
            .await
            .is_ok()
    );
    assert_eq!(
        runtime.dispose_fiber_detailed(hosted, false).await,
        DisposeOutcome::Disposed
    );
}

#[tokio::test]
async fn protocol_failure_and_crash_fail_pending_invocations_without_replay() {
    let (_runtime, context, _hosted) = setup("protocol_failure_invoke").await;
    assert!(matches!(context.invoke(&key(), external([])).await,
        Err(CordisError::Host(error)) if error.kind() == HostFailureKind::ProtocolViolation));
    let rejected = context.invoke(&key(), external([])).await;
    assert!(
        matches!(
            &rejected,
            Err(CordisError::Host(error)) if error.kind() == HostFailureKind::ProtocolViolation
        ) || matches!(&rejected, Err(CordisError::InvocationHandlerNotFound(_)))
    );

    let (_runtime, context, _hosted) = setup("crash_during_invoke").await;
    let rejected = context.invoke(&key(), external([])).await;
    assert!(matches!(
        rejected,
        Err(CordisError::Host(_) | CordisError::InvocationHandlerNotFound(_))
    ));
    assert!(matches!(
        context.invoke(&key(), external([])).await,
        Err(CordisError::Host(_) | CordisError::InvocationHandlerNotFound(_))
    ));
}

#[tokio::test]
async fn ten_thousand_out_of_order_remote_invocations_preserve_correlation() {
    let (runtime, context, hosted) = setup("out_of_order_invoke").await;
    let mut workers = tokio::task::JoinSet::new();
    for worker in 0..32_u32 {
        let context = context.clone();
        workers.spawn(async move {
            for sequence in 0..320_u32 {
                let expected = format!("{worker}:{sequence}").into_bytes();
                let actual = context
                    .invoke(&key(), external(expected.clone()))
                    .await
                    .unwrap();
                assert_eq!(&*external_bytes(actual), &expected);
            }
        });
    }
    while let Some(result) = workers.join_next().await {
        result.unwrap();
    }
    assert_eq!(
        runtime.dispose_fiber_detailed(hosted, false).await,
        DisposeOutcome::Disposed
    );
}

#[tokio::test]
async fn real_process_hmr_keeps_old_inflight_on_old_session() {
    let (runtime, context, old) = setup("remote_old_delayed").await;
    let old_call = tokio::spawn({
        let context = context.clone();
        async move { context.invoke(&key(), external([])).await }
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    let reload = runtime
        .reload_hosted_detailed(old, &fixture("remote_new"), artifact())
        .await
        .unwrap();
    assert!(matches!(
        reload,
        cordis_runtime::ReloadOutcome::Completed { .. }
            | cordis_runtime::ReloadOutcome::CommittedWithCleanupPending { .. }
    ));
    assert_eq!(&*external_bytes(old_call.await.unwrap().unwrap()), b"old");
    assert_eq!(
        &*external_bytes(context.invoke(&key(), external([])).await.unwrap()),
        b"new"
    );
}

#[tokio::test]
#[ignore = "manual B2.1C performance characterization"]
async fn characterize_remote_echo_concurrency() {
    for workers in [1_usize, 4, 8, 16] {
        let (runtime, context, hosted) = setup("remote_echo").await;
        let operations = 2_000_usize;
        let latencies = Arc::new(Mutex::new(Vec::with_capacity(operations)));
        let started = std::time::Instant::now();
        let mut tasks = tokio::task::JoinSet::new();
        for worker in 0..workers {
            let context = context.clone();
            let latencies = Arc::clone(&latencies);
            tasks.spawn(async move {
                let mut sequence = worker;
                while sequence < operations {
                    let before = std::time::Instant::now();
                    context.invoke(&key(), external([])).await.unwrap();
                    latencies.lock().push(before.elapsed().as_nanos());
                    sequence += workers;
                }
            });
        }
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
        let elapsed = started.elapsed();
        let mut samples = latencies.lock().clone();
        samples.sort_unstable();
        let percentile = |percent: usize| samples[(samples.len() - 1) * percent / 100];
        let ops_per_second =
            u128::try_from(operations).unwrap() * 1_000_000_000 / elapsed.as_nanos().max(1);
        println!(
            "remote_echo workers={workers} ops_s={ops_per_second} p50_us={} p95_us={} p99_us={}",
            percentile(50) / 1_000,
            percentile(95) / 1_000,
            percentile(99) / 1_000,
        );
        assert_eq!(
            runtime.dispose_fiber_detailed(hosted, false).await,
            DisposeOutcome::Disposed
        );
    }
}

#[tokio::test]
#[ignore = "manual B2.2 mixed 50,000-operation concurrency soak"]
async fn mixed_success_domain_cancel_timeout_soak() {
    let (success_runtime, success, success_hosted) = setup("remote_echo").await;
    let (domain_runtime, domain, domain_hosted) = setup("remote_domain").await;
    let soak_host = |mode: &str| {
        let mut config = ProcessHostConfig::default();
        config.outbound_queue_capacity = 50_000;
        ProcessHost::with_config(env!("CARGO_BIN_EXE_cordis-process-host-fixture"), config)
            .arg(mode)
    };
    let (cancel_runtime, cancel, cancel_hosted) = setup_host(soak_host("delayed_invoke")).await;
    let (timeout_runtime, timeout, timeout_hosted) = setup_host(soak_host("delayed_invoke")).await;
    let counts = Arc::new([
        AtomicUsize::new(0),
        AtomicUsize::new(0),
        AtomicUsize::new(0),
        AtomicUsize::new(0),
    ]);
    let mut workers = tokio::task::JoinSet::new();
    for worker in 0..32_usize {
        let success = success.clone();
        let domain = domain.clone();
        let cancel = cancel.clone();
        let timeout = timeout.clone();
        let counts = Arc::clone(&counts);
        workers.spawn(async move {
            let mut operation = worker;
            while operation < 50_000 {
                match operation % 4 {
                    0 => {
                        success.invoke(&key(), external([])).await.unwrap();
                        counts[0].fetch_add(1, Ordering::Relaxed);
                    }
                    1 => {
                        assert!(matches!(
                            domain.invoke(&key(), external([])).await,
                            Err(CordisError::RemoteDomain(_))
                        ));
                        counts[1].fetch_add(1, Ordering::Relaxed);
                    }
                    2 => {
                        let invocation_key = key();
                        let invoke = cancel.invoke(&invocation_key, external([]));
                        tokio::pin!(invoke);
                        tokio::select! {
                            biased;
                            () = tokio::time::sleep(Duration::from_millis(1)) => {}
                            result = &mut invoke => panic!("cancel soak completed early: {result:?}"),
                        }
                        counts[2].fetch_add(1, Ordering::Relaxed);
                    }
                    _ => {
                        assert!(matches!(
                            timeout
                                .invoke_with_timeout(
                                    &key(),
                                    external([]),
                                    Duration::from_millis(1),
                                )
                                .await,
                            Err(CordisError::InvocationTimedOut)
                        ));
                        counts[3].fetch_add(1, Ordering::Relaxed);
                    }
                }
                operation += 32;
            }
        });
    }
    while let Some(result) = workers.join_next().await {
        result.unwrap();
    }
    assert_eq!(
        counts
            .iter()
            .map(|count| count.load(Ordering::Relaxed))
            .collect::<Vec<_>>(),
        vec![12_500; 4]
    );
    for (runtime, hosted) in [
        (success_runtime, success_hosted),
        (domain_runtime, domain_hosted),
        (cancel_runtime, cancel_hosted),
        (timeout_runtime, timeout_hosted),
    ] {
        assert_eq!(
            runtime.dispose_fiber_detailed(hosted, false).await,
            DisposeOutcome::Disposed
        );
    }
}

#[tokio::test]
#[ignore = "manual B2.2 300-second mixed ProcessHost soak"]
#[allow(clippy::too_many_lines)]
async fn long_mixed_process_host_soak_300_seconds() {
    use hdrhistogram::Histogram;
    use std::sync::atomic::AtomicU64;

    let duration = Duration::from_secs(300);
    let deadline = tokio::time::Instant::now() + duration;
    let rss_start = process_rss_bytes();
    let rss_peak = Arc::new(AtomicU64::new(rss_start));
    let operations = Arc::new(AtomicUsize::new(0));
    let latencies = Arc::new(Mutex::new(Histogram::<u64>::new(3).unwrap()));

    let (success_runtime, success, success_hosted) = setup("remote_echo").await;
    let (domain_runtime, domain, domain_hosted) = setup("remote_domain").await;
    let long_host = || {
        let mut config = ProcessHostConfig::default();
        config.outbound_queue_capacity = 100_000;
        ProcessHost::with_config(env!("CARGO_BIN_EXE_cordis-process-host-fixture"), config)
            .arg("delayed_invoke")
    };
    let (cancel_runtime, cancel, cancel_hosted) = setup_host(long_host()).await;
    let (timeout_runtime, timeout, timeout_hosted) = setup_host(long_host()).await;

    let sampler_peak = Arc::clone(&rss_peak);
    let sampler = tokio::spawn(async move {
        while tokio::time::Instant::now() < deadline {
            sampler_peak.fetch_max(process_rss_bytes(), Ordering::Relaxed);
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    });

    let mut workers = tokio::task::JoinSet::new();
    for worker in 0..32_usize {
        if worker == 0 {
            let operations = Arc::clone(&operations);
            let latencies = Arc::clone(&latencies);
            workers.spawn(async move {
                while tokio::time::Instant::now() < deadline {
                    let started = std::time::Instant::now();
                    let (runtime, context, old) = setup("remote_old_delayed").await;
                    let old_call = tokio::spawn({
                        let context = context.clone();
                        async move { context.invoke(&key(), external([])).await }
                    });
                    tokio::time::sleep(Duration::from_millis(30)).await;
                    let reload = runtime
                        .reload_hosted_detailed(old, &fixture("remote_new"), artifact())
                        .await;
                    assert!(
                        matches!(
                            &reload,
                            Ok(cordis_runtime::ReloadOutcome::Completed { .. }
                                | cordis_runtime::ReloadOutcome::CommittedWithCleanupPending { .. })
                        ),
                        "lifecycle soak reload failed: {reload:?}"
                    );
                    assert!(old_call.await.unwrap().is_ok());
                    assert!(context.invoke(&key(), external([])).await.is_ok());
                    assert!(matches!(
                        runtime.shutdown_detailed().await,
                        cordis_runtime::ShutdownOutcome::Complete
                            | cordis_runtime::ShutdownOutcome::CompleteWithIssues { .. }
                    ));

                    let crash_runtime = Runtime::new();
                    let crashed = crash_runtime
                        .install_hosted(
                            crash_runtime.root(),
                            &fixture("crash_after_start"),
                            artifact(),
                        )
                        .await
                        .unwrap();
                    tokio::time::timeout(Duration::from_secs(5), async {
                        loop {
                            if crash_runtime
                                .snapshot()
                                .fibers
                                .iter()
                                .find(|fiber| fiber.id == crashed)
                                .is_none_or(|fiber| {
                                    fiber.state == cordis_core::FiberState::Disposed
                                })
                            {
                                break;
                            }
                            tokio::task::yield_now().await;
                        }
                    })
                    .await
                    .unwrap();
                    let _ = crash_runtime.shutdown_detailed().await;
                    latencies
                        .lock()
                        .record(u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX))
                        .unwrap();
                    operations.fetch_add(6, Ordering::Relaxed);
                }
            });
            continue;
        }
        let success = success.clone();
        let domain = domain.clone();
        let cancel = cancel.clone();
        let timeout = timeout.clone();
        let operations = Arc::clone(&operations);
        let latencies = Arc::clone(&latencies);
        workers.spawn(async move {
            while tokio::time::Instant::now() < deadline {
                let started = std::time::Instant::now();
                match worker % 4 {
                    0 => {
                        success.invoke(&key(), external([])).await.unwrap();
                    }
                    1 => assert!(matches!(
                        domain.invoke(&key(), external([])).await,
                        Err(CordisError::RemoteDomain(_))
                    )),
                    2 => {
                        let invocation_key = key();
                        let invoke = cancel.invoke(&invocation_key, external([]));
                        tokio::pin!(invoke);
                        tokio::select! {
                            () = tokio::time::sleep(Duration::from_millis(1)) => {}
                            result = &mut invoke => assert!(result.is_ok(), "unexpected cancel-race result: {result:?}"),
                        }
                    }
                    _ => {
                        let result = timeout
                            .invoke_with_timeout(&key(), external([]), Duration::from_millis(1))
                            .await;
                        assert!(
                            result.is_ok()
                                || matches!(result, Err(CordisError::InvocationTimedOut)),
                            "unexpected deadline-race result: {result:?}"
                        );
                    }
                }
                latencies
                    .lock()
                    .record(u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX))
                    .unwrap();
                operations.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });
    }
    while let Some(result) = workers.join_next().await {
        result.unwrap();
    }
    sampler.await.unwrap();

    for (runtime, hosted) in [
        (success_runtime, success_hosted),
        (domain_runtime, domain_hosted),
        (cancel_runtime, cancel_hosted),
        (timeout_runtime, timeout_hosted),
    ] {
        let _ = runtime.dispose_fiber_detailed(hosted, false).await;
        assert!(matches!(
            runtime.shutdown_detailed().await,
            cordis_runtime::ShutdownOutcome::Complete
                | cordis_runtime::ShutdownOutcome::CompleteWithIssues { .. }
        ));
    }
    let completed = operations.load(Ordering::Relaxed);
    let histogram = latencies.lock();
    let rss_end = process_rss_bytes();
    rss_peak.fetch_max(rss_end, Ordering::Relaxed);
    println!(
        "long_soak duration_s=300 operations={completed} ops_s={} p50_us={} p95_us={} p99_us={} rss_start={} rss_peak={} rss_end={}",
        completed / 300,
        histogram.value_at_quantile(0.50),
        histogram.value_at_quantile(0.95),
        histogram.value_at_quantile(0.99),
        rss_start,
        rss_peak.load(Ordering::Relaxed),
        rss_end,
    );
}

#[cfg(windows)]
fn process_rss_bytes() -> u64 {
    let pid = std::process::id().to_string();
    let output = std::process::Command::new("tasklist")
        .args(["/FI", &format!("PID eq {pid}"), "/FO", "CSV", "/NH"])
        .output()
        .expect("tasklist RSS query");
    let line = String::from_utf8_lossy(&output.stdout);
    let kib = line
        .rsplit("\",\"")
        .next()
        .map(|field| {
            field
                .chars()
                .filter(char::is_ascii_digit)
                .collect::<String>()
        })
        .and_then(|digits| digits.parse::<u64>().ok())
        .unwrap_or(0);
    kib * 1024
}

#[cfg(not(windows))]
fn process_rss_bytes() -> u64 {
    0
}
