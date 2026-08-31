use crate::disposal::{ScopeDisposal, ScopeState};
use cordis_core::{CordisError, FiberId, ScopeId};
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use slotmap::SlotMap;
use smallvec::SmallVec;
use std::sync::Arc;

pub(crate) struct ScopeRecord {
    pub(crate) name: Arc<str>,
    pub(crate) parent: Option<ScopeId>,
    pub(crate) children: SmallVec<[ScopeId; 4]>,
    pub(crate) fibers: SmallVec<[FiberId; 4]>,
    pub(crate) state: ScopeState,
    pub(crate) hidden: bool,
    pub(crate) disposal: ScopeDisposal,
}

pub(crate) struct ScopeRegistry {
    scopes: RwLock<SlotMap<ScopeId, ScopeRecord>>,
    root: ScopeId,
}

impl ScopeRegistry {
    pub(crate) fn new() -> Self {
        let mut scopes = SlotMap::with_key();
        let root = scopes.insert(ScopeRecord {
            name: "root".into(),
            parent: None,
            children: SmallVec::new(),
            fibers: SmallVec::new(),
            state: ScopeState::Active,
            hidden: false,
            disposal: ScopeDisposal::default(),
        });
        Self {
            scopes: RwLock::new(scopes),
            root,
        }
    }
    pub(crate) const fn root(&self) -> ScopeId {
        self.root
    }
    pub(crate) fn read(&self) -> RwLockReadGuard<'_, SlotMap<ScopeId, ScopeRecord>> {
        self.scopes.read()
    }
    pub(crate) fn write(&self) -> RwLockWriteGuard<'_, SlotMap<ScopeId, ScopeRecord>> {
        self.scopes.write()
    }
    pub(crate) fn parent(&self, scope: ScopeId) -> Result<Option<ScopeId>, CordisError> {
        Ok(self
            .scopes
            .read()
            .get(scope)
            .ok_or(CordisError::ScopeNotFound)?
            .parent)
    }
    pub(crate) fn ancestry(&self, scope: ScopeId) -> Result<Vec<ScopeId>, CordisError> {
        let scopes = self.scopes.read();
        let mut result = Vec::new();
        let mut cursor = Some(scope);
        while let Some(id) = cursor {
            let record = scopes.get(id).ok_or(CordisError::ScopeNotFound)?;
            result.push(id);
            cursor = record.parent;
        }
        Ok(result)
    }
    pub(crate) fn ancestry_root_to_leaf(
        &self,
        scope: ScopeId,
        require_active: bool,
    ) -> Result<Vec<ScopeId>, CordisError> {
        let scopes = self.scopes.read();
        let mut result = Vec::new();
        let mut cursor = Some(scope);
        while let Some(id) = cursor {
            let record = scopes.get(id).ok_or(CordisError::ScopeNotFound)?;
            if require_active && record.state != ScopeState::Active {
                return Err(CordisError::ScopeDisposed(id));
            }
            result.push(id);
            cursor = record.parent;
        }
        result.reverse();
        Ok(result)
    }
    pub(crate) fn len(&self) -> usize {
        self.scopes.read().len()
    }
}
