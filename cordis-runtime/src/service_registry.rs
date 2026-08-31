use crate::gate::CapabilityGate;
use cordis_core::{FiberId, ScopeId, ServiceKey, ServiceSymbol, ServiceValue};
use parking_lot::RwLock;
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

#[derive(Clone)]
pub(crate) struct ServiceEntry {
    pub(crate) owner: FiberId,
    pub(crate) scope: ScopeId,
    pub(crate) value: ServiceValue,
    pub(crate) gate: Arc<CapabilityGate>,
}
#[derive(Clone)]
struct CacheEntry {
    epoch: u64,
    location: Option<(ScopeId, FiberId)>,
}

enum CacheLookup {
    Miss,
    Hit(Option<ServiceEntry>),
}

#[derive(Default)]
struct ServiceState {
    by_key: HashMap<ServiceKey, ServiceSymbol>,
    keys: Vec<ServiceKey>,
    entries: HashMap<(ScopeId, ServiceSymbol), ServiceEntry>,
    epoch: u64,
    cache: HashMap<(ScopeId, ServiceSymbol), CacheEntry>,
    cache_order: VecDeque<(ScopeId, ServiceSymbol)>,
}
pub(crate) struct ServiceRegistry {
    state: RwLock<ServiceState>,
    max_symbols: usize,
    max_cache_entries: usize,
    #[cfg(test)]
    miss_before_write_hook: parking_lot::Mutex<Option<Arc<std::sync::Barrier>>>,
}

pub(crate) struct PreparedServiceRevision {
    from: ScopeId,
    to: ScopeId,
    symbols: Vec<ServiceSymbol>,
    old_owner: FiberId,
    staged_owner: FiberId,
}

impl ServiceRegistry {
    pub(crate) fn new(max_symbols: usize, max_cache_entries: usize) -> Self {
        Self {
            state: RwLock::new(ServiceState::default()),
            max_symbols,
            max_cache_entries,
            #[cfg(test)]
            miss_before_write_hook: parking_lot::Mutex::new(None),
        }
    }

    pub(crate) fn try_intern(
        &self,
        key: &ServiceKey,
    ) -> Result<ServiceSymbol, cordis_core::CordisError> {
        let mut s = self.state.write();
        if let Some(v) = s.by_key.get(key) {
            return Ok(*v);
        }
        if s.keys.len() >= self.max_symbols {
            return Err(cordis_core::CordisError::ResourceLimitExceeded {
                resource: cordis_core::ResourceKind::ServiceSymbols,
                limit: self.max_symbols,
            });
        }
        let index = u32::try_from(s.keys.len()).map_err(|_| {
            cordis_core::CordisError::InvalidRuntimeConfig(
                "max_interned_symbols exceeds the ServiceSymbol ID space".into(),
            )
        })?;
        let v = ServiceSymbol::from_index(index);
        s.keys.push(key.clone());
        s.by_key.insert(key.clone(), v);
        Ok(v)
    }

    pub(crate) fn intern(&self, key: &ServiceKey) -> ServiceSymbol {
        self.lookup(key)
            .expect("service key was admitted before use")
    }
    pub(crate) fn lookup(&self, key: &ServiceKey) -> Option<ServiceSymbol> {
        self.state.read().by_key.get(key).copied()
    }
    pub(crate) fn contains(&self, scope: ScopeId, symbol: ServiceSymbol) -> bool {
        self.state.read().entries.contains_key(&(scope, symbol))
    }
    pub(crate) fn get(&self, scope: ScopeId, symbol: ServiceSymbol) -> Option<ServiceEntry> {
        self.state.read().entries.get(&(scope, symbol)).cloned()
    }
    pub(crate) fn insert(&self, scope: ScopeId, symbol: ServiceSymbol, entry: ServiceEntry) {
        let mut s = self.state.write();
        let mut entry = entry;
        entry.scope = scope;
        s.entries.insert((scope, symbol), entry);
        Self::bump(&mut s);
    }
    pub(crate) fn remove(&self, scope: ScopeId, symbol: ServiceSymbol) -> Option<ServiceEntry> {
        let mut s = self.state.write();
        let v = s.entries.remove(&(scope, symbol));
        if v.is_some() {
            Self::bump(&mut s);
        }
        v
    }
    pub(crate) fn prepare_revision(
        &self,
        from: ScopeId,
        to: ScopeId,
        symbols: Vec<ServiceSymbol>,
        old_owner: FiberId,
        staged_owner: FiberId,
    ) -> Result<PreparedServiceRevision, cordis_core::CordisError> {
        let s = self.state.read();
        for symbol in &symbols {
            if s.entries
                .get(&(from, *symbol))
                .is_none_or(|entry| entry.owner != staged_owner)
            {
                return Err(cordis_core::CordisError::RevisionValidationFailed(
                    "staged service disappeared or changed owner".into(),
                ));
            }
            if s.entries
                .get(&(to, *symbol))
                .is_some_and(|entry| entry.owner != old_owner)
            {
                return Err(cordis_core::CordisError::RevisionValidationFailed(
                    "service replacement target changed owner".into(),
                ));
            }
        }
        Ok(PreparedServiceRevision {
            from,
            to,
            symbols,
            old_owner,
            staged_owner,
        })
    }

    pub(crate) fn commit_revision_and_publish(
        &self,
        prepared: &PreparedServiceRevision,
        staged_gate: &CapabilityGate,
        old_gate: &CapabilityGate,
    ) -> Result<(), cordis_core::CordisError> {
        let mut s = self.state.write();
        for symbol in &prepared.symbols {
            if s.entries
                .get(&(prepared.from, *symbol))
                .is_none_or(|entry| entry.owner != prepared.staged_owner)
                || s.entries
                    .get(&(prepared.to, *symbol))
                    .is_some_and(|entry| entry.owner != prepared.old_owner)
            {
                return Err(cordis_core::CordisError::RevisionValidationFailed(
                    "prepared service revision changed before commit".into(),
                ));
            }
        }
        if !staged_gate.cutover_from(old_gate) {
            return Err(cordis_core::CordisError::CapabilityPublicationFailed(
                prepared.staged_owner,
            ));
        }
        let staged: Vec<_> = prepared
            .symbols
            .iter()
            .map(|symbol| {
                (
                    *symbol,
                    s.entries
                        .remove(&(prepared.from, *symbol))
                        .expect("validated staged service"),
                )
            })
            .collect();
        for (symbol, mut entry) in staged {
            entry.scope = prepared.to;
            s.entries.insert((prepared.to, symbol), entry);
        }
        Self::bump(&mut s);
        Ok(())
    }
    pub(crate) fn resolve(
        &self,
        requested: ScopeId,
        ancestry: &[ScopeId],
        symbol: ServiceSymbol,
    ) -> Option<ServiceEntry> {
        if self.max_cache_entries != 0 {
            {
                let s = self.state.read();
                if let CacheLookup::Hit(entry) = Self::cached_resolution(&s, requested, symbol) {
                    return entry;
                }
            }
            #[cfg(test)]
            let miss_before_write_hook = { self.miss_before_write_hook.lock().take() };
            #[cfg(test)]
            if let Some(hook) = miss_before_write_hook {
                hook.wait();
                hook.wait();
            }
        }
        let mut s = self.state.write();
        if self.max_cache_entries != 0 {
            if let CacheLookup::Hit(entry) = Self::cached_resolution(&s, requested, symbol) {
                return entry;
            }
        }
        let loc = ancestry.iter().copied().find_map(|scope| {
            s.entries
                .get(&(scope, symbol))
                .filter(|e| e.gate.is_visible())
                .map(|e| (scope, e.owner))
        });
        let epoch = s.epoch;
        if self.max_cache_entries != 0 {
            let cache_key = (requested, symbol);
            if !s.cache.contains_key(&cache_key) {
                while s.cache.len() >= self.max_cache_entries {
                    if let Some(oldest) = s.cache_order.pop_front() {
                        s.cache.remove(&oldest);
                    }
                }
                s.cache_order.push_back(cache_key);
            }
            s.cache.insert(
                cache_key,
                CacheEntry {
                    epoch,
                    location: loc,
                },
            );
        }
        loc.and_then(|(scope, owner)| {
            s.entries
                .get(&(scope, symbol))
                .filter(|e| e.owner == owner && e.gate.is_visible())
                .cloned()
        })
    }

    fn cached_resolution(
        state: &ServiceState,
        requested: ScopeId,
        symbol: ServiceSymbol,
    ) -> CacheLookup {
        let Some(cached) = state.cache.get(&(requested, symbol)) else {
            return CacheLookup::Miss;
        };
        if cached.epoch != state.epoch {
            return CacheLookup::Miss;
        }
        let Some((scope, owner)) = cached.location else {
            return CacheLookup::Hit(None);
        };
        match state
            .entries
            .get(&(scope, symbol))
            .filter(|entry| entry.owner == owner && entry.gate.is_visible())
            .cloned()
        {
            Some(entry) => CacheLookup::Hit(Some(entry)),
            None => CacheLookup::Miss,
        }
    }
    pub(crate) fn resolve_owned(
        &self,
        ancestry: &[ScopeId],
        symbol: ServiceSymbol,
        owner: FiberId,
    ) -> Option<ServiceEntry> {
        let s = self.state.read();
        ancestry.iter().find_map(|scope| {
            s.entries
                .get(&(*scope, symbol))
                .filter(|e| e.owner == owner && !e.gate.is_visible())
                .cloned()
        })
    }
    pub(crate) fn count(&self) -> usize {
        self.state.read().entries.len()
    }
    pub(crate) fn snapshots(&self) -> Vec<(ScopeId, ServiceSymbol, FiberId)> {
        self.state
            .read()
            .entries
            .iter()
            .map(|((scope, symbol), entry)| (*scope, *symbol, entry.owner))
            .collect()
    }
    pub(crate) fn bump_epoch(&self) {
        Self::bump(&mut self.state.write());
    }
    fn bump(s: &mut ServiceState) {
        s.epoch = s.epoch.wrapping_add(1);
        s.cache.clear();
        s.cache_order.clear();
    }

    #[cfg(test)]
    pub(crate) fn symbol_count(&self) -> usize {
        self.state.read().keys.len()
    }

    #[cfg(test)]
    pub(crate) fn cache_len(&self) -> usize {
        self.state.read().cache.len()
    }

    #[cfg(test)]
    pub(crate) fn set_miss_before_write_hook(&self, hook: Arc<std::sync::Barrier>) {
        *self.miss_before_write_hook.lock() = Some(hook);
    }
}
