//! Criterion hot- and cold-path runtime benchmarks.

#![allow(missing_docs, clippy::too_many_lines)]

use async_trait::async_trait;
use cordis_core::{
    CordisError, DependencyPolicy, EventKey, EventValue, InvocationKey, PluginDescriptor,
    PluginRevision, ServiceKey,
};
use cordis_runtime::{
    Context, EventHandler, EventOutcome, NativePlugin, Runtime, RuntimeConfig,
    invocation_handler_fn,
};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use futures::future::join_all;
use std::{
    sync::{Arc, Barrier, Mutex},
    time::Duration,
};

const OPS_PER_WORKER: usize = 2_000;

struct Capture {
    context: Arc<Mutex<Option<Context>>>,
    service: Option<(ServiceKey, u64)>,
}

#[async_trait]
impl NativePlugin for Capture {
    fn descriptor(&self) -> PluginDescriptor {
        PluginDescriptor {
            name: "benchmark".into(),
            dependencies: Vec::new().into(),
            provisions: self
                .service
                .iter()
                .map(|(key, _)| key.clone())
                .collect::<Vec<_>>()
                .into(),
            dependency_policy: DependencyPolicy::Restart,
            revision: PluginRevision(1),
        }
    }
    async fn start(&self, context: Context) -> Result<(), CordisError> {
        if let Some((key, value)) = &self.service {
            context.provide(key.clone(), *value)?;
        }
        *self.context.lock().expect("benchmark mutex") = Some(context);
        Ok(())
    }
}

struct Noop;
#[async_trait]
impl EventHandler for Noop {
    async fn call(
        &self,
        _value: EventValue,
        _next: Option<cordis_runtime::Next>,
    ) -> Result<EventOutcome, CordisError> {
        Ok(EventOutcome::default())
    }
}

fn context(
    runtime: &tokio::runtime::Runtime,
    cordis: &Runtime,
    scope: cordis_core::ScopeId,
    service: Option<(ServiceKey, u64)>,
) -> Context {
    let slot = Arc::new(Mutex::new(None));
    runtime
        .block_on(cordis.install(
            scope,
            Capture {
                context: slot.clone(),
                service,
            },
        ))
        .expect("install benchmark plugin");
    slot.lock()
        .expect("benchmark mutex")
        .clone()
        .expect("captured context")
}

fn benchmarks(c: &mut Criterion) {
    let tokio = tokio::runtime::Runtime::new().expect("tokio runtime");
    let mut config = RuntimeConfig::default();
    config.max_tasks_per_fiber = 2_048;
    config.max_scopes = 8_192;
    let cordis = Runtime::with_config(config).expect("benchmark config");
    let local_key = ServiceKey::new("bench", "local", 1);
    let root_context = context(&tokio, &cordis, cordis.root(), Some((local_key.clone(), 1)));
    let child = cordis
        .create_scope(cordis.root(), "child")
        .expect("child scope");
    let child_context = context(&tokio, &cordis, child, None);
    let local_symbol = cordis.intern_service(&local_key).expect("benchmark symbol");

    c.bench_function("context/clone", |b| {
        b.iter(|| std::hint::black_box(root_context.clone()));
    });
    c.bench_function("service/root_lookup", |b| {
        b.iter(|| std::hint::black_box(root_context.get::<u64>(&local_key).expect("service")));
    });
    c.bench_function("service/parent_lookup", |b| {
        b.iter(|| std::hint::black_box(child_context.get::<u64>(&local_key).expect("service")));
    });
    c.bench_function("service/cache_hit_symbol", |b| {
        b.iter(|| {
            std::hint::black_box(
                child_context
                    .get_symbol::<u64>(local_symbol)
                    .expect("service"),
            )
        });
    });

    let mut depth_group = c.benchmark_group("service/depth");
    // RuntimeConfig intentionally caps ScopeDepth at 32 in the frozen v1 API.
    for depth in [1_usize, 2, 4, 8, 16, 24, 32] {
        // Each input gets its own exact-depth chain so setup is excluded.
        let mut scope = cordis.root();
        for level in 0..depth {
            scope = cordis
                .create_scope(scope, format!("depth-{depth}-{level}"))
                .expect("depth scope");
        }
        let lookup = context(&tokio, &cordis, scope, None);
        depth_group.bench_with_input(BenchmarkId::from_parameter(depth), &depth, |b, _| {
            b.iter(|| std::hint::black_box(lookup.get::<u64>(&local_key).expect("service")));
        });
    }
    depth_group.finish();

    let mut concurrent = c.benchmark_group("service/concurrent");
    for workers in [1_usize, 2, 4, 8, 16, 32, 64, 128] {
        concurrent.throughput(Throughput::Elements((workers * OPS_PER_WORKER) as u64));
        concurrent.bench_with_input(
            BenchmarkId::from_parameter(workers),
            &workers,
            |b, &workers| {
                b.iter(|| {
                    let barrier = Arc::new(Barrier::new(workers));
                    std::thread::scope(|threads| {
                        for _ in 0..workers {
                            let lookup = child_context.clone();
                            let key = local_key.clone();
                            let barrier = barrier.clone();
                            threads.spawn(move || {
                                barrier.wait();
                                for _ in 0..OPS_PER_WORKER {
                                    std::hint::black_box(lookup.get::<u64>(&key).expect("service"));
                                }
                            });
                        }
                    });
                });
            },
        );
    }
    concurrent.finish();

    let event = EventKey("bench.event".into());
    for count in [1_usize, 8, 32, 128] {
        let event_context = context(&tokio, &cordis, cordis.root(), None);
        for _ in 0..count {
            event_context
                .on(event.clone(), Arc::new(Noop))
                .expect("handler");
        }
        c.bench_with_input(BenchmarkId::new("event/emit", count), &count, |b, _| {
            b.iter(|| {
                tokio
                    .block_on(event_context.emit(&event, Arc::new(())))
                    .expect("emit");
            });
        });
    }

    c.bench_function("scope/create_dispose", |b| {
        b.iter(|| {
            let scope = cordis
                .create_scope(cordis.root(), "temporary")
                .expect("scope");
            tokio
                .block_on(cordis.dispose_scope(scope))
                .expect("dispose scope");
            std::hint::black_box(cordis.collect_garbage());
        });
    });
    c.bench_function("fiber/install_dispose_gc", |b| {
        b.iter(|| {
            let slot = Arc::new(Mutex::new(None));
            let fiber = tokio
                .block_on(cordis.install(
                    cordis.root(),
                    Capture {
                        context: slot,
                        service: None,
                    },
                ))
                .expect("install");
            tokio
                .block_on(cordis.dispose_fiber(fiber, false))
                .expect("dispose");
            std::hint::black_box(cordis.collect_garbage());
        });
    });

    let invocation = InvocationKey::new("bench", "noop", 1);
    let invocation_context = context(&tokio, &cordis, cordis.root(), None);
    invocation_context
        .handle_invocation(
            invocation.clone(),
            invocation_handler_fn(|_, input: Arc<u64>| async move { Ok(input) }),
        )
        .expect("invocation handler");
    c.bench_function("invocation/noop", |b| {
        b.iter(|| {
            tokio
                .block_on(invocation_context.invoke_typed::<u64, u64>(&invocation, Arc::new(1)))
                .expect("invoke")
        });
    });
    let mut invoke_group = c.benchmark_group("invocation/concurrent");
    for workers in [1_usize, 2, 4, 8, 16, 32, 64, 128] {
        invoke_group.throughput(Throughput::Elements(workers as u64));
        invoke_group.bench_with_input(
            BenchmarkId::from_parameter(workers),
            &workers,
            |b, &workers| {
                b.iter(|| {
                    tokio.block_on(async {
                        join_all((0..workers).map(|_| {
                            invocation_context.invoke_typed::<u64, u64>(&invocation, Arc::new(1))
                        }))
                        .await
                        .into_iter()
                        .for_each(|result| {
                            result.expect("invoke");
                        });
                    });
                });
            },
        );
    }
    invoke_group.finish();

    let mut task_group = c.benchmark_group("task/spawn_complete");
    for count in [1_usize, 8, 32, 128, 512, 1_000] {
        task_group.throughput(Throughput::Elements(count as u64));
        task_group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &count| {
            b.iter(|| {
                tokio.block_on(async {
                    for _ in 0..count {
                        invocation_context.spawn(async {}).expect("spawn");
                    }
                    while cordis.snapshot().live_fiber_tasks != 0 {
                        tokio::task::yield_now().await;
                    }
                });
            });
        });
    }
    task_group.finish();

    c.bench_function("reload/idle", |b| {
        let mut old = tokio
            .block_on(cordis.install(
                cordis.root(),
                Capture {
                    context: Arc::new(Mutex::new(None)),
                    service: None,
                },
            ))
            .expect("reload seed");
        b.iter(|| {
            old = tokio
                .block_on(cordis.reload(
                    old,
                    Capture {
                        context: Arc::new(Mutex::new(None)),
                        service: None,
                    },
                ))
                .expect("reload");
        });
    });

    c.bench_function("shutdown/empty", |b| {
        b.iter_batched(
            Runtime::new,
            |runtime| tokio.block_on(runtime.shutdown()).expect("shutdown"),
            criterion::BatchSize::SmallInput,
        );
    });

    // Allow Runtime-owned completion workers to settle before process exit.
    tokio.block_on(async { tokio::time::sleep(Duration::from_millis(10)).await });
}

criterion_group!(benches, benchmarks);
criterion_main!(benches);
