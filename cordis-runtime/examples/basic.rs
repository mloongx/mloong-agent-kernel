//! Logger, tools, and dependent agent lifecycle example.

use async_trait::async_trait;
use cordis_core::{CordisError, DependencyPolicy, PluginDescriptor, PluginRevision, ServiceKey};
use cordis_runtime::{Context, NativePlugin, Runtime};

fn logger_key() -> ServiceKey {
    ServiceKey::new("cordis", "logger", 1)
}
fn tools_key() -> ServiceKey {
    ServiceKey::new("dsh", "tools", 1)
}

struct Provider {
    name: &'static str,
    key: ServiceKey,
    value: &'static str,
}

#[async_trait]
impl NativePlugin for Provider {
    fn descriptor(&self) -> PluginDescriptor {
        PluginDescriptor {
            name: self.name.into(),
            dependencies: Vec::new().into(),
            provisions: vec![self.key.clone()].into(),
            dependency_policy: DependencyPolicy::Restart,
            revision: PluginRevision(1),
        }
    }
    async fn start(&self, ctx: Context) -> Result<(), CordisError> {
        ctx.provide(self.key.clone(), self.value.to_owned())
    }
}

struct Agent;

#[async_trait]
impl NativePlugin for Agent {
    fn descriptor(&self) -> PluginDescriptor {
        PluginDescriptor {
            name: "agent".into(),
            dependencies: vec![logger_key(), tools_key()].into(),
            provisions: Vec::new().into(),
            dependency_policy: DependencyPolicy::Restart,
            revision: PluginRevision(1),
        }
    }
    async fn start(&self, ctx: Context) -> Result<(), CordisError> {
        let logger = ctx.get::<String>(&logger_key())?;
        let tools = ctx.get::<String>(&tools_key())?;
        println!("agent activated with {logger} and {tools}");
        ctx.spawn(async { std::future::pending::<()>().await })?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), CordisError> {
    let runtime = Runtime::new();
    runtime
        .install(
            runtime.root(),
            Provider {
                name: "logger",
                key: logger_key(),
                value: "tracing logger",
            },
        )
        .await?;
    runtime
        .install(
            runtime.root(),
            Provider {
                name: "tools",
                key: tools_key(),
                value: "tool registry",
            },
        )
        .await?;
    let agent = runtime.install(runtime.root(), Agent).await?;
    println!("active fibers: {}", runtime.snapshot().fibers.len());
    runtime.dispose_fiber(agent, false).await?;
    runtime.shutdown().await
}
