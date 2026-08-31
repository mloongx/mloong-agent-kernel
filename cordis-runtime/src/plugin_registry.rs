use crate::gate::GenerationSelector;
use cordis_core::{CordisError, PluginId};
use parking_lot::RwLock;
use slotmap::SlotMap;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct GenerationId(pub(crate) u64);

impl GenerationId {
    pub(crate) const NONE: Self = Self(0);
    pub(crate) const fn get(self) -> u64 {
        self.0
    }
}

struct PluginSlot {
    selector: Arc<GenerationSelector>,
    next_generation: u64,
    fiber_count: usize,
}

#[derive(Default)]
pub(crate) struct PluginRegistry {
    slots: RwLock<SlotMap<PluginId, PluginSlot>>,
}

impl PluginRegistry {
    pub(crate) fn create(&self) -> PluginId {
        self.slots.write().insert(PluginSlot {
            selector: Arc::new(GenerationSelector::default()),
            next_generation: 1,
            fiber_count: 0,
        })
    }
    pub(crate) fn allocate_generation(
        &self,
        plugin: PluginId,
    ) -> Result<(GenerationId, Arc<GenerationSelector>), CordisError> {
        let mut slots = self.slots.write();
        let slot = slots.get_mut(plugin).ok_or(CordisError::Invariant(
            "logical plugin slot disappeared".into(),
        ))?;
        let generation = GenerationId(slot.next_generation);
        slot.next_generation = slot
            .next_generation
            .checked_add(1)
            .ok_or_else(|| CordisError::Invariant("plugin generation space exhausted".into()))?;
        Ok((generation, slot.selector.clone()))
    }
    pub(crate) fn attach_fiber(&self, plugin: PluginId) -> Result<(), CordisError> {
        let mut slots = self.slots.write();
        let slot = slots.get_mut(plugin).ok_or(CordisError::Invariant(
            "logical plugin slot disappeared".into(),
        ))?;
        slot.fiber_count = slot
            .fiber_count
            .checked_add(1)
            .ok_or_else(|| CordisError::Invariant("plugin fiber count overflow".into()))?;
        Ok(())
    }
    pub(crate) fn detach_fiber(&self, plugin: PluginId) -> Result<(), CordisError> {
        let mut slots = self.slots.write();
        let slot = slots.get_mut(plugin).ok_or(CordisError::Invariant(
            "logical plugin slot disappeared during fiber detach".into(),
        ))?;
        slot.fiber_count = slot.fiber_count.checked_sub(1).ok_or_else(|| {
            tracing::error!(?plugin, "plugin fiber count underflow");
            CordisError::Invariant("plugin fiber count underflow".into())
        })?;
        Ok(())
    }
    pub(crate) fn reclaim_if_dead(&self, plugin: PluginId) -> bool {
        let mut slots = self.slots.write();
        if slots.get(plugin).is_some_and(|slot| {
            slot.fiber_count == 0 && slot.selector.active() == GenerationId::NONE
        }) {
            slots.remove(plugin);
            true
        } else {
            false
        }
    }
    #[cfg(test)]
    pub(crate) fn contains(&self, plugin: PluginId) -> bool {
        self.slots.read().contains_key(plugin)
    }
    #[cfg(test)]
    pub(crate) fn total_fiber_count(&self) -> usize {
        self.slots
            .read()
            .values()
            .map(|slot| slot.fiber_count)
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn double_plugin_fiber_detach_is_detected() {
        let registry = PluginRegistry::default();
        let plugin = registry.create();
        registry.attach_fiber(plugin).expect("attach");
        registry.detach_fiber(plugin).expect("first detach");
        assert!(matches!(
            registry.detach_fiber(plugin),
            Err(CordisError::Invariant(_))
        ));
    }
}
