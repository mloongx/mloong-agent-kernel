//! Allocation-count characterization for public Runtime paths.

#![allow(missing_docs, clippy::cast_precision_loss)]

use async_trait::async_trait;
use cordis_core::{
    CordisError, DependencyPolicy, EventKey, EventValue, InvocationKey, PluginDescriptor,
    PluginRevision, ServiceKey,
};
use cordis_runtime::{
    Context, EventHandler, EventOutcome, NativePlugin, Runtime, invocation_handler_fn,
};
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};
use std::{
    alloc::System,
    env,
    sync::{Arc, Mutex},
};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;
const OPERATIONS: usize = 10_000;

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

struct Plugin {
    captured: Arc<Mutex<Option<Context>>>,
    service: ServiceKey,
    invocation: InvocationKey,
    event: EventKey,
}
#[async_trait]
impl NativePlugin for Plugin {
    fn descriptor(&self) -> PluginDescriptor {
        PluginDescriptor {
            name: "perf-alloc".into(),
            dependencies: Arc::new([]),
            provisions: Arc::new([self.service.clone()]),
            dependency_policy: DependencyPolicy::Restart,
            revision: PluginRevision(1),
        }
    }
    async fn start(&self, context: Context) -> Result<(), CordisError> {
        context.provide(self.service.clone(), 1_u64)?;
        context.handle_invocation(
            self.invocation.clone(),
            invocation_handler_fn(|_, input: Arc<u64>| async move { Ok(input) }),
        )?;
        context.on(self.event.clone(), Arc::new(NoopEvent))?;
        *self.captured.lock().expect("context mutex") = Some(context);
        Ok(())
    }
}

fn report(name: &str, operations: usize, region: &Region<'_, System>) {
    let stats = region.change();
    println!(
        "CORDIS_ALLOC_RESULT path={name} operations={operations} allocations={} allocations_per_op={:.4} bytes={} bytes_per_op={:.2} reallocations={}",
        stats.allocations,
        stats.allocations as f64 / operations as f64,
        stats.bytes_allocated,
        stats.bytes_allocated as f64 / operations as f64,
        stats.reallocations
    );
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), CordisError> {
    let runtime = Runtime::new();
    let depth = number("depth", 1);
    let path = argument("path", "key");
    let operations = number("operations", OPERATIONS);
    let mut scope = runtime.root();
    for level in 1..depth {
        scope = runtime.create_scope(scope, format!("alloc-depth-{level}"))?;
    }
    let service = ServiceKey::new("perf", "alloc", 1);
    let invocation = InvocationKey::new("perf", "alloc", 1);
    let event = EventKey("perf.alloc".into());
    let slot = Arc::new(Mutex::new(None));
    runtime
        .install(
            scope,
            Plugin {
                captured: slot.clone(),
                service: service.clone(),
                invocation: invocation.clone(),
                event: event.clone(),
            },
        )
        .await?;
    let context = slot
        .lock()
        .expect("context mutex")
        .clone()
        .expect("context");
    let symbol = runtime.intern_service(&service)?;
    if path == "symbol" {
        std::hint::black_box(context.get_symbol::<u64>(symbol)?);
    } else {
        std::hint::black_box(context.get::<u64>(&service)?);
    }
    let region = Region::new(GLOBAL);
    for _ in 0..operations {
        if path == "symbol" {
            std::hint::black_box(context.get_symbol::<u64>(symbol)?);
        } else {
            std::hint::black_box(context.get::<u64>(&service)?);
        }
    }
    report(
        &format!("service-{path}-depth-{depth}"),
        operations,
        &region,
    );
    let region = Region::new(GLOBAL);
    for _ in 0..OPERATIONS {
        std::hint::black_box(
            context
                .invoke_typed::<u64, u64>(&invocation, Arc::new(1))
                .await?,
        );
    }
    report("invocation", OPERATIONS, &region);
    let region = Region::new(GLOBAL);
    for _ in 0..OPERATIONS {
        context.emit(&event, Arc::new(())).await?;
    }
    report("event", OPERATIONS, &region);
    let region = Region::new(GLOBAL);
    for _ in 0..OPERATIONS {
        context.spawn(async {})?;
        tokio::task::yield_now().await;
    }
    while runtime.snapshot().live_fiber_tasks != 0 {
        tokio::task::yield_now().await;
    }
    report("task", OPERATIONS, &region);
    let region = Region::new(GLOBAL);
    for _ in 0..OPERATIONS {
        let scope = runtime.create_scope(runtime.root(), "alloc")?;
        runtime.dispose_scope(scope).await?;
    }
    report("scope_lifecycle", OPERATIONS, &region);
    runtime.shutdown().await?;
    Ok(())
}
