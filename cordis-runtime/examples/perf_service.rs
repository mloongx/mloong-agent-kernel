//! Steady-state Service lookup scaling and cache-effect harness.

#![allow(
    missing_docs,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss
)]

use async_trait::async_trait;
use cordis_core::{
    CordisError, DependencyPolicy, PluginDescriptor, PluginRevision, ServiceKey, ServiceSymbol,
};
use cordis_runtime::{Context, NativePlugin, Runtime, RuntimeConfig};
use std::{
    env,
    sync::{Arc, Barrier, Mutex},
    time::Instant,
};

struct Plugin {
    services: Vec<ServiceKey>,
    captured: Arc<Mutex<Option<Context>>>,
}
#[async_trait]
impl NativePlugin for Plugin {
    fn descriptor(&self) -> PluginDescriptor {
        PluginDescriptor {
            name: "perf-service".into(),
            dependencies: Arc::new([]),
            provisions: self.services.clone().into(),
            dependency_policy: DependencyPolicy::Restart,
            revision: PluginRevision(1),
        }
    }
    async fn start(&self, context: Context) -> Result<(), CordisError> {
        for key in &self.services {
            context.provide(key.clone(), 1_u64)?;
        }
        *self.captured.lock().expect("context mutex") = Some(context);
        Ok(())
    }
}

fn argument(name: &str, default: &str) -> String {
    env::args()
        .skip(1)
        .find_map(|arg| arg.strip_prefix(&format!("--{name}=")).map(str::to_owned))
        .unwrap_or_else(|| default.to_owned())
}
fn number(name: &str, default: usize) -> usize {
    argument(name, &default.to_string())
        .parse()
        .expect("numeric argument")
}

struct Lane {
    context: Context,
    key: ServiceKey,
    symbol: ServiceSymbol,
}

async fn install_context(
    runtime: &Runtime,
    scope: cordis_core::ScopeId,
) -> Result<Context, CordisError> {
    let slot = Arc::new(Mutex::new(None));
    runtime
        .install(
            scope,
            Plugin {
                services: Vec::new(),
                captured: slot.clone(),
            },
        )
        .await?;
    let context = slot
        .lock()
        .expect("context mutex")
        .clone()
        .expect("context");
    Ok(context)
}

fn caller_scope(
    runtime: &Runtime,
    depth: usize,
    lane: usize,
) -> Result<cordis_core::ScopeId, CordisError> {
    let mut scope = runtime.root();
    for level in 0..depth {
        scope = runtime.create_scope(scope, format!("lane-{lane}-depth-{level}"))?;
    }
    Ok(scope)
}

async fn setup(
    cache_entries: usize,
    depth: usize,
    workers: usize,
    scenario: &str,
    key_length: usize,
) -> Result<(Runtime, Vec<Lane>), CordisError> {
    let mut config = RuntimeConfig::default();
    config.max_resolution_cache_entries = cache_entries;
    let runtime = Runtime::with_config(config)?;
    let lane_count = if scenario == "a" { 1 } else { workers };
    let provider_count = if ["c", "ca", "cb", "ik1", "ik2"].contains(&scenario) {
        workers
    } else {
        1
    };
    let mut keys = Vec::with_capacity(provider_count);
    for provider in 0..provider_count {
        let suffix = format!("-{provider}");
        let body = "k".repeat(key_length.saturating_sub(suffix.len()).max(1));
        let key = if scenario == "ik1" {
            ServiceKey::new("perf", "same-key", 1)
        } else {
            ServiceKey::new("perf", format!("{body}{suffix}"), 1)
        };
        keys.push(key);
    }
    if scenario == "pb2" {
        keys.clear();
        for provider in 0..workers {
            let suffix = format!("-{provider}");
            let body = "k".repeat(key_length.saturating_sub(suffix.len()).max(1));
            keys.push(ServiceKey::new("perf", format!("{body}{suffix}"), 1));
        }
    }
    if scenario == "ik1" {
        let mut lanes = Vec::with_capacity(workers);
        for (lane, key) in keys.iter().enumerate() {
            let provider_scope =
                runtime.create_scope(runtime.root(), format!("provider-{lane}"))?;
            runtime
                .install(
                    provider_scope,
                    Plugin {
                        services: vec![key.clone()],
                        captured: Arc::new(Mutex::new(None)),
                    },
                )
                .await?;
            let caller_scope = runtime.create_scope(provider_scope, format!("caller-{lane}"))?;
            let context = install_context(&runtime, caller_scope).await?;
            let symbol = runtime.intern_service(key)?;
            lanes.push(Lane {
                context,
                key: key.clone(),
                symbol,
            });
        }
        return Ok((runtime, lanes));
    }
    let provider_groups = if scenario == "pb2" {
        vec![keys.clone()]
    } else {
        keys.iter().cloned().map(|key| vec![key]).collect()
    };
    for services in provider_groups {
        let slot = Arc::new(Mutex::new(None));
        runtime
            .install(
                runtime.root(),
                Plugin {
                    services,
                    captured: slot,
                },
            )
            .await?;
    }
    let shared_context = if scenario == "cb" {
        Some(install_context(&runtime, caller_scope(&runtime, depth, 0)?).await?)
    } else {
        None
    };
    let mut lanes = Vec::with_capacity(lane_count);
    for lane in 0..lane_count {
        let context = if let Some(context) = &shared_context {
            context.clone()
        } else {
            let scope = caller_scope(&runtime, depth, lane)?;
            install_context(&runtime, scope).await?
        };
        let key = keys[if ["c", "ca", "cb", "pb2", "ik2"].contains(&scenario) {
            lane
        } else {
            0
        }]
        .clone();
        let symbol = runtime.intern_service(&key)?;
        lanes.push(Lane {
            context,
            key,
            symbol,
        });
    }
    Ok((runtime, lanes))
}

fn lookup(lane: &Lane, path: &str, repeats: usize, retention: &str) {
    let mut retained = Vec::with_capacity(if retention == "retain" { repeats } else { 0 });
    for _ in 0..repeats {
        let handle = if path == "symbol" {
            lane.context
                .get_symbol::<u64>(lane.symbol)
                .expect("service")
        } else {
            lane.context.get::<u64>(&lane.key).expect("service")
        };
        if retention == "retain" {
            retained.push(handle);
        } else {
            std::hint::black_box(handle);
        }
    }
    std::hint::black_box(retained);
}

fn threaded(
    lanes: &[Lane],
    path: &str,
    workers: usize,
    operations: usize,
    repeats: usize,
    retention: &str,
) -> u128 {
    let start_barrier = Arc::new(Barrier::new(workers + 1));
    let finish_barrier = Arc::new(Barrier::new(workers + 1));
    let mut elapsed = 0;
    std::thread::scope(|threads| {
        for worker in 0..workers {
            let lane = &lanes[if lanes.len() == 1 { 0 } else { worker }];
            let start = start_barrier.clone();
            let finish = finish_barrier.clone();
            let retention = retention.to_owned();
            threads.spawn(move || {
                start.wait();
                for _ in 0..operations {
                    lookup(lane, path, repeats, &retention);
                }
                finish.wait();
            });
        }
        let start = Instant::now();
        start_barrier.wait();
        finish_barrier.wait();
        elapsed = start.elapsed().as_nanos();
    });
    elapsed
}

async fn tokio_tasks(
    lanes: &[Lane],
    path: &str,
    workers: usize,
    operations: usize,
    repeats: usize,
    retention: &str,
) -> u128 {
    let start_barrier = Arc::new(tokio::sync::Barrier::new(workers + 1));
    let finish_barrier = Arc::new(tokio::sync::Barrier::new(workers + 1));
    let mut tasks = Vec::with_capacity(workers);
    for worker in 0..workers {
        let lane = &lanes[if lanes.len() == 1 { 0 } else { worker }];
        let context = lane.context.clone();
        let key = lane.key.clone();
        let symbol = lane.symbol;
        let path = path.to_owned();
        let retention = retention.to_owned();
        let start = start_barrier.clone();
        let finish = finish_barrier.clone();
        tasks.push(tokio::spawn(async move {
            start.wait().await;
            for _ in 0..operations {
                let mut retained =
                    Vec::with_capacity(if retention == "retain" { repeats } else { 0 });
                for _ in 0..repeats {
                    let handle = if path == "symbol" {
                        context.get_symbol::<u64>(symbol).expect("service")
                    } else {
                        context.get::<u64>(&key).expect("service")
                    };
                    if retention == "retain" {
                        retained.push(handle);
                    } else {
                        std::hint::black_box(handle);
                    }
                }
                std::hint::black_box(retained);
                tokio::task::consume_budget().await;
            }
            finish.wait().await;
        }));
    }
    let start = Instant::now();
    start_barrier.wait().await;
    finish_barrier.wait().await;
    let elapsed = start.elapsed().as_nanos();
    for task in tasks {
        task.await.expect("service task");
    }
    elapsed
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), CordisError> {
    let mode = argument("mode", "threaded");
    let workers = number("workers", 1);
    let operations = number("operations", 100_000);
    let depth = number("depth", 8);
    let cache_entries = number("cache", 4_096);
    let scenario = argument("scenario", "a");
    let path = argument("path", "key");
    let key_length = number("key-length", 8);
    let repeats = number("repeats", 1);
    let retention = argument("retention", "drop");
    assert!(
        ["a", "b", "c", "ca", "cb", "pb1", "pb2", "ik1", "ik2"].contains(&scenario.as_str()),
        "unsupported scenario"
    );
    assert!(
        ["key", "symbol"].contains(&path.as_str()),
        "path must be key or symbol"
    );
    assert!(
        ["drop", "retain"].contains(&retention.as_str()),
        "retention must be drop or retain"
    );
    let (runtime, lanes) = setup(cache_entries, depth, workers, &scenario, key_length).await?;
    for lane in &lanes {
        lookup(lane, &path, repeats, &retention);
    }
    let elapsed = if mode == "threaded" {
        threaded(&lanes, &path, workers, operations, repeats, &retention)
    } else {
        tokio_tasks(&lanes, &path, workers, operations, repeats, &retention).await
    };
    let total = workers * operations;
    println!(
        "CORDIS_SERVICE_RESULT mode={mode} scenario={scenario} path={path} retention={retention} workers={workers} operations_per_worker={operations} repeats={repeats} key_length={key_length} depth={depth} cache={cache_entries} elapsed_ns={elapsed} aggregate_ops_sec={:.2} aggregate_lookups_sec={:.2} ns_per_lookup={:.2} per_worker_ops_sec={:.2}",
        total as f64 * 1e9 / elapsed as f64,
        (total * repeats) as f64 * 1e9 / elapsed as f64,
        elapsed as f64 / (total * repeats) as f64,
        operations as f64 * 1e9 / elapsed as f64
    );
    runtime.shutdown().await?;
    Ok(())
}
