use crate::{
    NativePlugin, disposal::FiberDisposal, gate::CapabilityGate, plugin_registry::GenerationId,
};
use cordis_core::{
    Effect, FiberId, FiberState, HandlerId, InvocationHandlerId, InvocationMiddlewareId,
    PluginDescriptor, PluginId, ScopeId, ServiceKey, TaskId,
};
use parking_lot::RwLock;
use slotmap::SlotMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use tokio::sync::{Mutex as AsyncMutex, watch};
use tokio_util::sync::CancellationToken;

pub(crate) struct FiberMutable {
    pub(crate) scope: ScopeId,
    pub(crate) descriptor: PluginDescriptor,
    pub(crate) state: FiberState,
    pub(crate) plugin: Arc<dyn NativePlugin>,
    pub(crate) effects: Vec<Box<dyn Effect>>,
    pub(crate) tasks: Vec<TaskId>,
    pub(crate) handlers: Vec<HandlerId>,
    pub(crate) invocation_handlers: Vec<InvocationHandlerId>,
    pub(crate) invocation_middleware: Vec<InvocationMiddlewareId>,
    pub(crate) provided: Vec<ServiceKey>,
    pub(crate) child_scopes: Vec<ScopeId>,
    pub(crate) host_processes: Vec<Arc<AtomicBool>>,
    pub(crate) cancellation: CancellationToken,
    pub(crate) activation: Option<watch::Sender<bool>>,
    pub(crate) activation_sealed: bool,
    pub(crate) capabilities: Arc<CapabilityGate>,
    pub(crate) generation: GenerationId,
    pub(crate) staged: bool,
    pub(crate) reload_owned: bool,
    pub(crate) disposal: FiberDisposal,
}

pub(crate) struct FiberCell {
    pub(crate) plugin_id: PluginId,
    pub(crate) lifecycle: Arc<AsyncMutex<()>>,
    pub(crate) inner: RwLock<FiberMutable>,
}

#[derive(Default)]
pub(crate) struct FiberRegistry {
    arena: RwLock<SlotMap<FiberId, Arc<FiberCell>>>,
}

impl FiberRegistry {
    pub(crate) fn insert(&self, cell: FiberCell) -> FiberId {
        self.arena.write().insert(Arc::new(cell))
    }

    pub(crate) fn get(&self, id: FiberId) -> Option<Arc<FiberCell>> {
        self.arena.read().get(id).cloned()
    }

    pub(crate) fn remove(&self, id: FiberId) -> Option<Arc<FiberCell>> {
        self.arena.write().remove(id)
    }

    pub(crate) fn len(&self) -> usize {
        self.arena.read().len()
    }

    pub(crate) fn snapshot(&self) -> Vec<(FiberId, Arc<FiberCell>)> {
        self.arena
            .read()
            .iter()
            .map(|(id, cell)| (id, cell.clone()))
            .collect()
    }

    pub(crate) fn with<R>(&self, id: FiberId, f: impl FnOnce(&FiberMutable) -> R) -> Option<R> {
        let cell = self.get(id)?;
        let inner = cell.inner.read();
        Some(f(&inner))
    }

    pub(crate) fn with_mut<R>(
        &self,
        id: FiberId,
        f: impl FnOnce(&mut FiberMutable) -> R,
    ) -> Option<R> {
        let cell = self.get(id)?;
        let mut inner = cell.inner.write();
        Some(f(&mut inner))
    }
}
