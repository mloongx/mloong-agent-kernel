use async_trait::async_trait;
use cordis_core::{CordisError, DependencyPolicy, PluginDescriptor, PluginRevision, ServiceKey};
use cordis_runtime::{Context, NativePlugin, Runtime};
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};

pub type StartFuture = Pin<Box<dyn Future<Output = Result<(), CordisError>> + Send>>;

pub struct Plugin {
    descriptor: PluginDescriptor,
    start: Arc<dyn Fn(Context) -> StartFuture + Send + Sync>,
}

impl Plugin {
    pub fn new(
        name: &'static str,
        start: impl Fn(Context) -> StartFuture + Send + Sync + 'static,
    ) -> Self {
        Self::contract(name, 0, Vec::new(), Vec::new(), start)
    }

    pub fn contract(
        name: &'static str,
        revision: u64,
        dependencies: Vec<ServiceKey>,
        provisions: Vec<ServiceKey>,
        start: impl Fn(Context) -> StartFuture + Send + Sync + 'static,
    ) -> Self {
        Self {
            descriptor: PluginDescriptor {
                name: name.into(),
                dependencies: dependencies.into(),
                provisions: provisions.into(),
                dependency_policy: DependencyPolicy::Restart,
                revision: PluginRevision(revision),
            },
            start: Arc::new(start),
        }
    }
}

#[async_trait]
impl NativePlugin for Plugin {
    fn descriptor(&self) -> PluginDescriptor {
        self.descriptor.clone()
    }

    async fn start(&self, context: Context) -> Result<(), CordisError> {
        (self.start)(context).await
    }
}

pub fn service_key(name: &str) -> ServiceKey {
    ServiceKey::new("conformance", name, 1)
}

pub async fn capture_context(
    runtime: &Runtime,
    scope: cordis_core::ScopeId,
    name: &'static str,
) -> (cordis_core::FiberId, Context) {
    let captured = Arc::new(Mutex::new(None));
    let fiber = runtime
        .install(
            scope,
            Plugin::new(name, {
                let captured = captured.clone();
                move |context| {
                    let captured = captured.clone();
                    Box::pin(async move {
                        *captured.lock().expect("context mutex") = Some(context);
                        Ok(())
                    })
                }
            }),
        )
        .await
        .expect("install context fixture");
    let context = captured
        .lock()
        .expect("context mutex")
        .clone()
        .expect("captured context");
    (fiber, context)
}

pub fn assert_not_found_or_stale(error: &CordisError) {
    assert!(matches!(
        error,
        CordisError::FiberNotFound | CordisError::StaleContextGeneration { .. }
    ));
}
