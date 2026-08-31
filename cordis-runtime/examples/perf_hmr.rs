//! Generation drain and HMR-under-load characterization harness.

#![allow(
    missing_docs,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::too_many_lines
)]

use async_trait::async_trait;
use cordis_core::{
    CordisError, DependencyPolicy, InvocationKey, PluginDescriptor, PluginRevision, ServiceKey,
};
use cordis_runtime::{Context, NativePlugin, ReloadOutcome, Runtime, invocation_handler_fn};
use std::{
    env,
    sync::{
        Arc, Barrier, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

struct Provider {
    revision: u64,
    service: ServiceKey,
    invocation: InvocationKey,
}
#[async_trait]
impl NativePlugin for Provider {
    fn descriptor(&self) -> PluginDescriptor {
        PluginDescriptor {
            name: "perf-hmr-provider".into(),
            dependencies: Arc::new([]),
            provisions: Arc::new([self.service.clone()]),
            dependency_policy: DependencyPolicy::Restart,
            revision: PluginRevision(self.revision),
        }
    }
    async fn start(&self, context: Context) -> Result<(), CordisError> {
        context.provide(self.service.clone(), self.revision)?;
        context.handle_invocation(
            self.invocation.clone(),
            invocation_handler_fn(|_, input: Arc<u64>| async move { Ok(input) }),
        )?;
        Ok(())
    }
}

struct Capture(Arc<Mutex<Option<Context>>>);
#[async_trait]
impl NativePlugin for Capture {
    fn descriptor(&self) -> PluginDescriptor {
        PluginDescriptor {
            name: "perf-hmr-caller".into(),
            dependencies: Arc::new([]),
            provisions: Arc::new([]),
            dependency_policy: DependencyPolicy::Restart,
            revision: PluginRevision(1),
        }
    }
    async fn start(&self, context: Context) -> Result<(), CordisError> {
        *self.0.lock().expect("context mutex") = Some(context);
        Ok(())
    }
}

fn argument(name: &str, default: &str) -> String {
    env::args()
        .skip(1)
        .find_map(|arg| arg.strip_prefix(&format!("--{name}=")).map(str::to_owned))
        .unwrap_or_else(|| default.to_owned())
}
fn number(name: &str, default: u64) -> u64 {
    argument(name, &default.to_string())
        .parse()
        .expect("numeric argument")
}
fn decimal(name: &str, default: f64) -> f64 {
    argument(name, &default.to_string())
        .parse()
        .expect("decimal argument")
}
fn percentile(values: &mut [u64], percentile: f64) -> u64 {
    values.sort_unstable();
    let index = ((values.len().saturating_sub(1) as f64) * percentile).round() as usize;
    values.get(index).copied().unwrap_or(0)
}

async fn setup() -> Result<
    (
        Runtime,
        cordis_core::FiberId,
        Context,
        ServiceKey,
        InvocationKey,
    ),
    CordisError,
> {
    let runtime = Runtime::new();
    let service = ServiceKey::new("perf", "hmr", 1);
    let invocation = InvocationKey::new("perf", "hmr", 1);
    let provider = runtime
        .install(
            runtime.root(),
            Provider {
                revision: 0,
                service: service.clone(),
                invocation: invocation.clone(),
            },
        )
        .await?;
    let slot = Arc::new(Mutex::new(None));
    runtime
        .install(runtime.root(), Capture(slot.clone()))
        .await?;
    let context = slot
        .lock()
        .expect("context mutex")
        .clone()
        .expect("context");
    Ok((runtime, provider, context, service, invocation))
}

async fn reload_provider(
    runtime: &Runtime,
    old: cordis_core::FiberId,
    provider: Provider,
) -> Result<cordis_core::FiberId, CordisError> {
    Ok(match runtime.reload_detailed(old, provider).await? {
        ReloadOutcome::Completed { new_fiber }
        | ReloadOutcome::CommittedWithCleanupPending { new_fiber, .. } => new_fiber,
    })
}

async fn drain(leases: usize) -> Result<(), CordisError> {
    let (runtime, provider, context, service, invocation) = setup().await?;
    let handles: Vec<_> = (0..leases)
        .map(|_| context.get::<u64>(&service).expect("service handle"))
        .collect();
    let reload_runtime = runtime.clone();
    let reload_service = service.clone();
    let reload_invocation = invocation.clone();
    let reload = tokio::spawn(async move {
        reload_provider(
            &reload_runtime,
            provider,
            Provider {
                revision: 1,
                service: reload_service,
                invocation: reload_invocation,
            },
        )
        .await
    });
    if leases != 0 {
        for _ in 0..10_000 {
            if runtime.snapshot().draining_generations != 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    }
    let release_start = Instant::now();
    drop(handles);
    let drop_ns = release_start.elapsed().as_nanos();
    let replacement = reload.await.expect("reload task")?;
    let finalized_ns = release_start.elapsed().as_nanos();
    let snapshot = runtime.snapshot();
    println!(
        "CORDIS_DRAIN_RESULT leases={leases} drop_ns={drop_ns} last_drop_to_reload_ns={} draining={} generations={}",
        finalized_ns.saturating_sub(drop_ns),
        snapshot.draining_generations,
        snapshot.active_generation_executions
    );
    runtime.dispose_fiber(replacement, false).await?;
    runtime.shutdown().await?;
    Ok(())
}

async fn hmr(path: &str, rate: f64, workers: usize, seconds: u64) -> Result<(), CordisError> {
    let (runtime, initial, context, service, invocation) = setup().await?;
    let deadline = Instant::now() + Duration::from_secs(seconds);
    let mut foreground = Vec::with_capacity(workers);
    for worker in 0..workers {
        let context = context.clone();
        let service = service.clone();
        let invocation = invocation.clone();
        let path = path.to_owned();
        foreground.push(tokio::spawn(async move {
            let mut values =
                Vec::with_capacity(seconds as usize * 100_000 / workers.max(1) + 1_024);
            let mut sequence = worker as u64;
            while Instant::now() < deadline {
                let start = Instant::now();
                if path == "service" {
                    std::hint::black_box(context.get::<u64>(&service)?);
                } else {
                    std::hint::black_box(
                        context
                            .invoke_typed::<u64, u64>(&invocation, Arc::new(sequence))
                            .await?,
                    );
                }
                values.push(start.elapsed().as_nanos() as u64);
                sequence += workers as u64;
            }
            Ok::<_, CordisError>(values)
        }));
    }
    let reload_runtime = runtime.clone();
    let reload_service = service.clone();
    let reload_invocation = invocation.clone();
    let reloads = tokio::spawn(async move {
        let mut current = initial;
        let mut revision = 1;
        let mut latencies = Vec::new();
        if rate > 0.0 {
            let interval = Duration::from_secs_f64(1.0 / rate);
            while Instant::now() < deadline {
                tokio::time::sleep(interval).await;
                let start = Instant::now();
                current = reload_provider(
                    &reload_runtime,
                    current,
                    Provider {
                        revision,
                        service: reload_service.clone(),
                        invocation: reload_invocation.clone(),
                    },
                )
                .await?;
                latencies.push(start.elapsed().as_nanos() as u64);
                revision += 1;
            }
        }
        Ok::<_, CordisError>((current, latencies))
    });
    let mut values = Vec::new();
    for task in foreground {
        values.extend(task.await.expect("foreground")?);
    }
    let (current, mut reload_values) = reloads.await.expect("reload worker")?;
    let operations = values.len();
    let p50 = percentile(&mut values.clone(), 0.50);
    let p95 = percentile(&mut values.clone(), 0.95);
    let p99 = percentile(&mut values.clone(), 0.99);
    let p999 = percentile(&mut values, 0.999);
    let reload_p50 = percentile(&mut reload_values.clone(), 0.50);
    let reload_p99 = percentile(&mut reload_values, 0.99);
    let snapshot = runtime.snapshot();
    println!(
        "CORDIS_HMR_RESULT path={path} rate_hz={rate} workers={workers} seconds={seconds} ops_per_sec={:.2} p50_ns={p50} p95_ns={p95} p99_ns={p99} p999_ns={p999} reloads={} reload_p50_ns={reload_p50} reload_p99_ns={reload_p99} fibers={} generations={} draining={}",
        operations as f64 / seconds as f64,
        reload_values.len(),
        snapshot.fibers.len(),
        snapshot.active_generation_executions,
        snapshot.draining_generations
    );
    runtime.dispose_fiber(current, false).await?;
    runtime.shutdown().await?;
    Ok(())
}

async fn writer_stress(workers: usize, seconds: u64) -> Result<(), CordisError> {
    let (runtime, mut current, context, service, invocation) = setup().await?;
    std::hint::black_box(context.get::<u64>(&service)?);
    let stop = Arc::new(AtomicBool::new(false));
    let start = Arc::new(Barrier::new(workers + 1));
    let mut readers = Vec::with_capacity(workers);
    for _ in 0..workers {
        let context = context.clone();
        let service = service.clone();
        let stop = stop.clone();
        let start = start.clone();
        readers.push(std::thread::spawn(move || {
            start.wait();
            let mut operations = 0_u64;
            while !stop.load(Ordering::Relaxed) {
                std::hint::black_box(context.get::<u64>(&service).expect("service"));
                operations += 1;
            }
            operations
        }));
    }
    start.wait();

    let auxiliary_service = ServiceKey::new("perf", "writer-aux", 1);
    let auxiliary_invocation = InvocationKey::new("perf", "writer-aux", 1);
    let install_start = Instant::now();
    let auxiliary = runtime
        .install(
            runtime.root(),
            Provider {
                revision: 1,
                service: auxiliary_service,
                invocation: auxiliary_invocation,
            },
        )
        .await?;
    let install_ns = install_start.elapsed().as_nanos() as u64;
    let dispose_start = Instant::now();
    runtime.dispose_fiber(auxiliary, false).await?;
    let dispose_ns = dispose_start.elapsed().as_nanos() as u64;

    let deadline = Instant::now() + Duration::from_secs(seconds);
    let mut revision = 1_u64;
    let mut reload_values = Vec::new();
    while Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let reload_start = Instant::now();
        current = reload_provider(
            &runtime,
            current,
            Provider {
                revision,
                service: service.clone(),
                invocation: invocation.clone(),
            },
        )
        .await?;
        reload_values.push(reload_start.elapsed().as_nanos() as u64);
        revision += 1;
    }
    stop.store(true, Ordering::Relaxed);
    let operations: u64 = readers
        .into_iter()
        .map(|reader| reader.join().expect("reader"))
        .sum();
    let reload_p50 = percentile(&mut reload_values.clone(), 0.50);
    let reload_p90 = percentile(&mut reload_values.clone(), 0.90);
    let reload_p95 = percentile(&mut reload_values.clone(), 0.95);
    let reload_p99 = percentile(&mut reload_values.clone(), 0.99);
    let reload_p999 = percentile(&mut reload_values.clone(), 0.999);
    let reload_max = reload_values.iter().copied().max().unwrap_or(0);
    let above = |threshold: u64| {
        reload_values
            .iter()
            .filter(|&&value| value > threshold)
            .count()
    };
    let snapshot = runtime.snapshot();
    println!(
        "CORDIS_WRITER_STRESS_RESULT workers={workers} seconds={seconds} reader_ops_sec={:.2} reloads={} reload_p50_ns={reload_p50} reload_p90_ns={reload_p90} reload_p95_ns={reload_p95} reload_p99_ns={reload_p99} reload_p999_ns={reload_p999} reload_max_ns={reload_max} above_1ms={} above_5ms={} above_10ms={} above_25ms={} above_50ms={} install_ns={install_ns} dispose_ns={dispose_ns} fibers={} generations={} draining={} workers_live={}",
        operations as f64 / seconds as f64,
        reload_values.len(),
        above(1_000_000),
        above(5_000_000),
        above(10_000_000),
        above(25_000_000),
        above(50_000_000),
        snapshot.fibers.len(),
        snapshot.active_generation_executions,
        snapshot.draining_generations,
        snapshot.live_runtime_workers
    );
    runtime.dispose_fiber(current, false).await?;
    runtime.shutdown().await?;
    Ok(())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), CordisError> {
    match argument("mode", "drain").as_str() {
        "drain" => drain(number("leases", 0) as usize).await,
        "hmr" => {
            hmr(
                &argument("path", "invocation"),
                decimal("rate", 1.0),
                number("workers", 8) as usize,
                number("seconds", 10),
            )
            .await
        }
        "writer-stress" => {
            writer_stress(number("workers", 8) as usize, number("seconds", 30)).await
        }
        _ => panic!("mode must be drain, hmr or writer-stress"),
    }
}
