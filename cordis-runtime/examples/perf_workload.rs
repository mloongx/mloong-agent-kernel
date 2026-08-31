//! Manual synthetic Runtime load and resource-characterization harness.

#![allow(
    missing_docs,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::too_many_lines
)]

use async_trait::async_trait;
use cordis_core::{
    CordisError, DependencyPolicy, EventKey, EventValue, InvocationKey, PluginDescriptor,
    PluginRevision, ScopeId, ServiceKey,
};
use cordis_runtime::{
    Context, EventHandler, EventOutcome, NativePlugin, Runtime, invocation_handler_fn,
};
use hdrhistogram::Histogram;
use std::{
    env,
    process::Command,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

struct NoopEvent;
#[async_trait]
impl EventHandler for NoopEvent {
    async fn call(
        &self,
        _value: EventValue,
        _next: Option<cordis_runtime::Next>,
    ) -> Result<EventOutcome, CordisError> {
        Ok(EventOutcome::default())
    }
}

struct Infrastructure {
    services: Arc<[ServiceKey]>,
    invocations: Arc<[InvocationKey]>,
    events: Arc<[EventKey]>,
}
#[async_trait]
impl NativePlugin for Infrastructure {
    fn descriptor(&self) -> PluginDescriptor {
        PluginDescriptor {
            name: "perf-infrastructure".into(),
            dependencies: Arc::new([]),
            provisions: self.services.clone(),
            dependency_policy: DependencyPolicy::Restart,
            revision: PluginRevision(1),
        }
    }
    async fn start(&self, context: Context) -> Result<(), CordisError> {
        for (index, key) in self.services.iter().enumerate() {
            context.provide(key.clone(), index as u64)?;
        }
        for key in self.invocations.iter() {
            context.handle_invocation(
                key.clone(),
                invocation_handler_fn(|_, input: Arc<u64>| async move { Ok(input) }),
            )?;
        }
        for key in self.events.iter() {
            context.on(key.clone(), Arc::new(NoopEvent))?;
        }
        Ok(())
    }
}

struct Capture(Arc<Mutex<Option<Context>>>);
#[async_trait]
impl NativePlugin for Capture {
    fn descriptor(&self) -> PluginDescriptor {
        PluginDescriptor {
            name: "perf-worker".into(),
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

#[derive(Clone, Copy, Debug)]
enum Mode {
    DataPlane,
    Lifecycle,
    Mixed,
    GcStress,
}
impl Mode {
    fn parse(value: &str) -> Self {
        match value {
            "data-plane" => Self::DataPlane,
            "lifecycle" => Self::Lifecycle,
            "mixed" => Self::Mixed,
            "gc-stress" => Self::GcStress,
            _ => panic!("mode must be data-plane, lifecycle, mixed, or gc-stress"),
        }
    }
    const fn operations(self) -> u64 {
        match self {
            Self::DataPlane => 25,
            Self::Lifecycle => 5,
            Self::Mixed => 30,
            Self::GcStress => 3,
        }
    }
}

#[derive(Clone, Copy)]
struct Config {
    parents: usize,
    children_per_parent: usize,
    concurrency: usize,
    warmup: Duration,
    measure: Duration,
    rss_interval: Duration,
    mode: Mode,
    manual_gc_every: usize,
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

fn rss() -> u64 {
    #[cfg(windows)]
    let output = Command::new("powershell")
        .args([
            "-NoProfile",
            "-Command",
            &format!("(Get-Process -Id {}).WorkingSet64", std::process::id()),
        ])
        .output();
    #[cfg(not(windows))]
    let output = Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output();
    output
        .ok()
        .and_then(|value| String::from_utf8(value.stdout).ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map_or(0, |value| if cfg!(windows) { value } else { value * 1024 })
}

struct RssSampler {
    stop: Arc<AtomicBool>,
    samples: Arc<Mutex<Vec<u64>>>,
    handle: Option<thread::JoinHandle<()>>,
}
impl RssSampler {
    fn start(interval: Duration) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let samples = Arc::new(Mutex::new(Vec::new()));
        let thread_stop = stop.clone();
        let thread_samples = samples.clone();
        let handle = thread::spawn(move || {
            while !thread_stop.load(Ordering::Relaxed) {
                thread_samples.lock().expect("rss samples").push(rss());
                thread::sleep(interval);
            }
        });
        Self {
            stop,
            samples,
            handle: Some(handle),
        }
    }
    fn finish(mut self) -> Vec<u64> {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            handle.join().expect("rss sampler");
        }
        Arc::try_unwrap(self.samples)
            .expect("rss sampler references")
            .into_inner()
            .expect("rss samples")
    }
}

async fn data_plane_run(
    context: &Context,
    services: &[ServiceKey],
    invocations: &[InvocationKey],
    events: &[EventKey],
    sequence: usize,
) -> Result<(), CordisError> {
    for offset in 0..5 {
        std::hint::black_box(context.get::<u64>(&services[(sequence + offset) % services.len()])?);
    }
    for offset in 0..10 {
        std::hint::black_box(
            context
                .invoke_typed::<u64, u64>(
                    &invocations[(sequence + offset) % invocations.len()],
                    Arc::new(sequence as u64),
                )
                .await?,
        );
    }
    for offset in 0..10 {
        context
            .emit(&events[(sequence + offset) % events.len()], Arc::new(()))
            .await?;
    }
    Ok(())
}

async fn lifecycle_run(
    runtime: &Runtime,
    context: &Context,
    manual_gc_every: usize,
    sequence: usize,
) -> Result<(), CordisError> {
    let child = runtime.create_scope(context.scope()?, "ephemeral")?;
    for _ in 0..3 {
        context.spawn(async {})?;
    }
    runtime.dispose_scope(child).await?;
    if manual_gc_every != 0 && sequence % manual_gc_every == 0 {
        std::hint::black_box(runtime.collect_garbage());
    }
    Ok(())
}

async fn logical_run(
    config: Config,
    runtime: &Runtime,
    context: &Context,
    services: &[ServiceKey],
    invocations: &[InvocationKey],
    events: &[EventKey],
    sequence: usize,
) -> Result<(), CordisError> {
    match config.mode {
        Mode::DataPlane => data_plane_run(context, services, invocations, events, sequence).await,
        Mode::Lifecycle => lifecycle_run(runtime, context, config.manual_gc_every, sequence).await,
        Mode::Mixed => {
            data_plane_run(context, services, invocations, events, sequence).await?;
            lifecycle_run(runtime, context, config.manual_gc_every, sequence).await
        }
        Mode::GcStress => {
            let child = runtime.create_scope(context.scope()?, "gc-ephemeral")?;
            runtime.dispose_scope(child).await?;
            std::hint::black_box(runtime.collect_garbage());
            Ok(())
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn phase(
    config: Config,
    duration: Duration,
    record_latency: bool,
    runtime: &Runtime,
    contexts: Arc<[Context]>,
    services: Arc<[ServiceKey]>,
    invocations: Arc<[InvocationKey]>,
    events: Arc<[EventKey]>,
) -> Result<Histogram<u64>, CordisError> {
    let deadline = Instant::now() + duration;
    let mut workers = Vec::with_capacity(config.concurrency);
    for worker in 0..config.concurrency {
        let runtime = runtime.clone();
        let contexts = contexts.clone();
        let services = services.clone();
        let invocations = invocations.clone();
        let events = events.clone();
        workers.push(tokio::spawn(async move {
            let mut latencies =
                Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3).expect("latency histogram");
            let mut sequence = worker;
            while Instant::now() < deadline {
                let start = Instant::now();
                logical_run(
                    config,
                    &runtime,
                    &contexts[sequence % contexts.len()],
                    &services,
                    &invocations,
                    &events,
                    sequence,
                )
                .await?;
                if record_latency {
                    latencies
                        .record(start.elapsed().as_nanos() as u64)
                        .expect("latency in histogram range");
                }
                sequence += config.concurrency;
            }
            Ok::<_, CordisError>(latencies)
        }));
    }
    let mut latencies =
        Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3).expect("latency histogram");
    for worker in workers {
        latencies
            .add(worker.await.expect("workload worker")?)
            .expect("compatible histograms");
    }
    Ok(latencies)
}

async fn wait_for_convergence(runtime: &Runtime) {
    for _ in 0..1_000 {
        let snapshot = runtime.snapshot();
        if snapshot.live_fiber_tasks == 0 && snapshot.live_runtime_workers == 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), CordisError> {
    let config = Config {
        parents: number("parents", 100) as usize,
        children_per_parent: number("children", 10) as usize,
        concurrency: number("concurrency", 32) as usize,
        warmup: Duration::from_secs(number("warmup", 5)),
        measure: Duration::from_secs(number("measure", 30)),
        rss_interval: Duration::from_millis(number("rss-ms", 1_000)),
        mode: Mode::parse(&argument("mode", "mixed")),
        manual_gc_every: number("manual-gc-every", 0) as usize,
    };
    assert!(config.concurrency > 0, "concurrency must be non-zero");
    let rss_initial = rss();
    let runtime = Runtime::new();
    let services: Arc<[ServiceKey]> = (0..50)
        .map(|index| ServiceKey::new("perf", format!("service-{index}"), 1))
        .collect();
    let invocations: Arc<[InvocationKey]> = (0..20)
        .map(|index| InvocationKey::new("perf", format!("invoke-{index}"), 1))
        .collect();
    let events: Arc<[EventKey]> = (0..20)
        .map(|index| EventKey(format!("perf.event.{index}").into()))
        .collect();
    let infrastructure = runtime
        .install(
            runtime.root(),
            Infrastructure {
                services: services.clone(),
                invocations: invocations.clone(),
                events: events.clone(),
            },
        )
        .await?;
    let mut parent_scopes: Vec<ScopeId> = Vec::with_capacity(config.parents);
    let mut contexts = Vec::with_capacity(config.parents * config.children_per_parent);
    for parent_index in 0..config.parents {
        let parent = runtime.create_scope(runtime.root(), format!("parent-{parent_index}"))?;
        parent_scopes.push(parent);
        for child_index in 0..config.children_per_parent {
            let child =
                runtime.create_scope(parent, format!("child-{parent_index}-{child_index}"))?;
            let slot = Arc::new(Mutex::new(None));
            runtime.install(child, Capture(slot.clone())).await?;
            contexts.push(
                slot.lock()
                    .expect("context mutex")
                    .clone()
                    .expect("context"),
            );
        }
    }
    let contexts: Arc<[Context]> = contexts.into();
    let rss_topology = rss();
    phase(
        config,
        config.warmup,
        false,
        &runtime,
        contexts.clone(),
        services.clone(),
        invocations.clone(),
        events.clone(),
    )
    .await?;
    wait_for_convergence(&runtime).await;
    let sampler = RssSampler::start(config.rss_interval);
    let start = Instant::now();
    let latencies = phase(
        config,
        config.measure,
        true,
        &runtime,
        contexts,
        services,
        invocations,
        events,
    )
    .await?;
    let elapsed = start.elapsed().as_secs_f64();
    let rss_samples = sampler.finish();
    wait_for_convergence(&runtime).await;
    let rss_after_workload = rss();
    let before_teardown = runtime.snapshot();
    for parent in parent_scopes.into_iter().rev() {
        runtime.dispose_scope(parent).await?;
    }
    runtime.dispose_fiber(infrastructure, false).await?;
    wait_for_convergence(&runtime).await;
    for _ in 0..100 {
        let report = runtime.collect_garbage();
        if report.fibers == 0 && report.scopes == 0 {
            break;
        }
        tokio::task::yield_now().await;
    }
    let final_snapshot = runtime.snapshot();
    let rss_final = rss();
    let runs = latencies.len();
    let p50 = latencies.value_at_quantile(0.50);
    let p95 = latencies.value_at_quantile(0.95);
    let p99 = latencies.value_at_quantile(0.99);
    let p999 = latencies.value_at_quantile(0.999);
    let max = latencies.max();
    let rss_min = rss_samples
        .iter()
        .copied()
        .min()
        .unwrap_or(rss_after_workload);
    let rss_peak = rss_samples
        .iter()
        .copied()
        .max()
        .unwrap_or(rss_after_workload);
    let rss_average = if rss_samples.is_empty() {
        rss_after_workload
    } else {
        rss_samples.iter().sum::<u64>() / rss_samples.len() as u64
    };
    println!("CORDIS_PERF_RESULT_V2");
    println!(
        "mode={:?} parents={} children={} workers={} warmup_s={} measure_s={} manual_gc_every={}",
        config.mode,
        config.parents,
        config.parents * config.children_per_parent,
        config.concurrency,
        config.warmup.as_secs(),
        config.measure.as_secs(),
        config.manual_gc_every
    );
    println!(
        "runs={runs} runs_per_sec={:.2} operations_per_sec={:.2}",
        runs as f64 / elapsed,
        runs as f64 * config.mode.operations() as f64 / elapsed
    );
    println!("p50_ns={p50} p95_ns={p95} p99_ns={p99} p999_ns={p999} max_ns={max}");
    println!(
        "rss_initial_bytes={rss_initial} rss_topology_bytes={rss_topology} rss_steady_min_bytes={rss_min} rss_steady_average_bytes={rss_average} rss_peak_bytes={rss_peak} rss_after_workload_bytes={rss_after_workload} rss_final_bytes={rss_final} rss_samples={}",
        rss_samples.len()
    );
    println!(
        "before_teardown_fibers={} before_teardown_scopes={} tasks={} workers={} generations={} draining={}",
        before_teardown.fibers.len(),
        before_teardown.scopes.len(),
        before_teardown.live_fiber_tasks,
        before_teardown.live_runtime_workers,
        before_teardown.active_generation_executions,
        before_teardown.draining_generations
    );
    println!(
        "final_fibers={} final_scopes={} final_tasks={} final_workers={} final_generations={} final_draining={}",
        final_snapshot.fibers.len(),
        final_snapshot.scopes.len(),
        final_snapshot.live_fiber_tasks,
        final_snapshot.live_runtime_workers,
        final_snapshot.active_generation_executions,
        final_snapshot.draining_generations
    );
    runtime.shutdown().await?;
    Ok(())
}
