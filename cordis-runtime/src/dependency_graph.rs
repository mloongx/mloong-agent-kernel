use cordis_core::{CordisError, FiberId, ScopeId, ServiceSymbol};
use parking_lot::RwLock;
use smallvec::SmallVec;
use std::collections::{HashMap, HashSet};
#[derive(Default)]
struct GraphState {
    dependents: HashMap<ServiceSymbol, SmallVec<[FiberId; 4]>>,
    declared: HashMap<(ScopeId, ServiceSymbol), SmallVec<[FiberId; 2]>>,
}
#[derive(Default)]
pub(crate) struct DependencyGraph {
    state: RwLock<GraphState>,
}
impl DependencyGraph {
    pub(crate) fn add_dependency(&self, s: ServiceSymbol, f: FiberId) {
        self.state.write().dependents.entry(s).or_default().push(f);
    }
    pub(crate) fn add_provider(&self, sc: ScopeId, s: ServiceSymbol, f: FiberId) {
        self.state
            .write()
            .declared
            .entry((sc, s))
            .or_default()
            .push(f);
    }
    pub(crate) fn remove_dependency(&self, s: ServiceSymbol, f: FiberId) {
        if let Some(v) = self.state.write().dependents.get_mut(&s) {
            v.retain(|id| *id != f);
        }
    }
    pub(crate) fn remove_provider(&self, sc: ScopeId, s: ServiceSymbol, f: FiberId) {
        if let Some(v) = self.state.write().declared.get_mut(&(sc, s)) {
            v.retain(|id| *id != f);
        }
    }
    pub(crate) fn move_providers(
        &self,
        from: ScopeId,
        to: ScopeId,
        symbols: &[ServiceSymbol],
        fiber: FiberId,
    ) -> Result<(), CordisError> {
        let mut g = self.state.write();
        if symbols.iter().copied().collect::<HashSet<_>>().len() != symbols.len() {
            return Err(CordisError::RevisionValidationFailed(
                "dependency provider revision contains duplicate symbols".into(),
            ));
        }
        for symbol in symbols {
            let source_count = g.declared.get(&(from, *symbol)).map_or(0, |providers| {
                providers.iter().filter(|id| **id == fiber).count()
            });
            let target_contains = g
                .declared
                .get(&(to, *symbol))
                .is_some_and(|providers| providers.contains(&fiber));
            let attached_elsewhere = g.declared.iter().any(|((scope, candidate), providers)| {
                *candidate == *symbol && *scope != from && providers.contains(&fiber)
            });
            if source_count != 1 || target_contains || attached_elsewhere {
                return Err(CordisError::RevisionValidationFailed(
                    "dependency provider topology changed before reload revision".into(),
                ));
            }
        }
        for symbol in symbols {
            if let Some(providers) = g.declared.get_mut(&(from, *symbol)) {
                providers.retain(|id| *id != fiber);
            }
            g.declared.entry((to, *symbol)).or_default().push(fiber);
        }
        Ok(())
    }
    pub(crate) fn dependents(&self, s: ServiceSymbol) -> SmallVec<[FiberId; 4]> {
        self.state
            .read()
            .dependents
            .get(&s)
            .cloned()
            .unwrap_or_default()
    }
    pub(crate) fn providers(&self, sc: ScopeId, s: ServiceSymbol) -> SmallVec<[FiberId; 4]> {
        self.state
            .read()
            .declared
            .get(&(sc, s))
            .map_or_else(SmallVec::new, |v| v.iter().copied().collect())
    }
    #[cfg(test)]
    pub(crate) fn provider_revision_snapshot(
        &self,
        from: ScopeId,
        to: ScopeId,
        symbols: &[ServiceSymbol],
        fiber: FiberId,
    ) -> Vec<(bool, bool)> {
        let graph = self.state.read();
        symbols
            .iter()
            .map(|symbol| {
                (
                    graph
                        .declared
                        .get(&(from, *symbol))
                        .is_some_and(|providers| providers.contains(&fiber)),
                    graph
                        .declared
                        .get(&(to, *symbol))
                        .is_some_and(|providers| providers.contains(&fiber)),
                )
            })
            .collect()
    }
    pub(crate) fn retain_fibers(&self, mut keep: impl FnMut(FiberId) -> bool) {
        let mut g = self.state.write();
        for v in g.dependents.values_mut() {
            v.retain(|id| keep(*id));
        }
        for v in g.declared.values_mut() {
            v.retain(|id| keep(*id));
        }
    }

    pub(crate) fn validate_cycles(
        edges: &HashMap<FiberId, SmallVec<[FiberId; 4]>>,
    ) -> Result<(), cordis_core::CordisError> {
        fn visit(
            node: FiberId,
            edges: &HashMap<FiberId, SmallVec<[FiberId; 4]>>,
            visiting: &mut HashSet<FiberId>,
            visited: &mut HashSet<FiberId>,
            path: &mut Vec<FiberId>,
        ) -> Result<(), cordis_core::CordisError> {
            if visiting.contains(&node) {
                path.push(node);
                return Err(cordis_core::CordisError::DependencyCycle(format!(
                    "{path:?}"
                )));
            }
            if !visited.insert(node) {
                return Ok(());
            }
            visiting.insert(node);
            path.push(node);
            if let Some(next) = edges.get(&node) {
                for target in next {
                    visit(*target, edges, visiting, visited, path)?;
                }
            }
            path.pop();
            visiting.remove(&node);
            Ok(())
        }
        let mut visiting = HashSet::new();
        let mut visited = HashSet::new();
        for node in edges.keys().copied() {
            visit(node, edges, &mut visiting, &mut visited, &mut Vec::new())?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn symbol_graph_detects_cycle_without_service_registry() {
        let a = FiberId::default();
        let b = FiberId::default();
        let mut edges = HashMap::new();
        edges.insert(a, smallvec::smallvec![b]);
        edges.insert(b, smallvec::smallvec![a]);
        assert!(matches!(
            DependencyGraph::validate_cycles(&edges),
            Err(cordis_core::CordisError::DependencyCycle(_))
        ));
    }
}
