//! Public-path dependency-loss scaling harness.

#![allow(missing_docs)]

use async_trait::async_trait;
use cordis_core::{CordisError, DependencyPolicy, PluginDescriptor, PluginRevision, ServiceKey};
use cordis_runtime::{Context, NativePlugin, Runtime, RuntimeConfig};
use std::{
    env,
    sync::Arc,
    time::{Duration, Instant},
};

struct Provider(ServiceKey);
#[async_trait]
impl NativePlugin for Provider {
    fn descriptor(&self) -> PluginDescriptor {
        PluginDescriptor {
            name: "dependency-provider".into(),
            dependencies: Arc::new([]),
            provisions: Arc::new([self.0.clone()]),
            dependency_policy: DependencyPolicy::Restart,
            revision: PluginRevision(1),
        }
    }
    async fn start(&self, context: Context) -> Result<(), CordisError> {
        context.provide(self.0.clone(), ())
    }
}

struct Node {
    name: Arc<str>,
    dependencies: Arc<[ServiceKey]>,
}
#[async_trait]
impl NativePlugin for Node {
    fn descriptor(&self) -> PluginDescriptor {
        PluginDescriptor {
            name: self.name.clone(),
            dependencies: self.dependencies.clone(),
            provisions: Arc::new([]),
            dependency_policy: DependencyPolicy::Restart,
            revision: PluginRevision(1),
        }
    }
    async fn start(&self, _context: Context) -> Result<(), CordisError> {
        Ok(())
    }
}

fn number(name: &str, default: usize) -> usize {
    env::args()
        .skip(1)
        .find_map(|arg| arg.strip_prefix(&format!("--{name}=")).map(str::to_owned))
        .map_or(default, |value| value.parse().expect("numeric argument"))
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), CordisError> {
    let total = number("total", 1_000).max(2);
    let affected = number("affected", 1).min(total - 1);
    let dependency = ServiceKey::new("perf", "dependency", 1);
    let mut config = RuntimeConfig::default();
    config.max_fibers = total + 1_024;
    config.max_scopes = total + 1_024;
    let runtime = Runtime::with_config(config)?;
    let provider = runtime
        .install(runtime.root(), Provider(dependency.clone()))
        .await?;
    for index in 0..(total - 1) {
        let dependencies: Arc<[ServiceKey]> = if index < affected {
            Arc::new([dependency.clone()])
        } else {
            Arc::new([])
        };
        runtime
            .install(
                runtime.root(),
                Node {
                    name: format!("dependent-{index}").into(),
                    dependencies,
                },
            )
            .await?;
    }
    let start = Instant::now();
    runtime.dispose_fiber(provider, false).await?;
    for _ in 0..10_000 {
        if runtime.snapshot().live_runtime_workers == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let elapsed = start.elapsed();
    let snapshot = runtime.snapshot();
    println!(
        "CORDIS_DEPENDENCY_RESULT topology=flat total={total} affected={affected} elapsed_ns={} fibers={} workers={} generations={} draining={}",
        elapsed.as_nanos(),
        snapshot.fibers.len(),
        snapshot.live_runtime_workers,
        snapshot.active_generation_executions,
        snapshot.draining_generations
    );
    for fiber in snapshot.fibers {
        let _ = runtime.dispose_fiber(fiber.id, false).await;
    }
    std::hint::black_box(runtime.collect_garbage());
    Ok(())
}
