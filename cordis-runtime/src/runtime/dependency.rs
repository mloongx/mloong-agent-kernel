impl Runtime {
    fn dependencies_met(&self, scope: ScopeId, deps: &[ServiceKey]) -> bool {
        deps.iter()
            .all(|key| self.resolve_symbol(scope, self.intern(key)).is_some())
    }

    fn remove_unstarted_fiber(&self, fiber: FiberId) {
        let Some(cell) = self.0.fibers.remove(fiber) else {
            return;
        };
        let (scope_id, dependencies, provisions) = {
            let record = cell.inner.read();
            (
                record.scope,
                record.descriptor.dependencies.clone(),
                record.descriptor.provisions.clone(),
            )
        };
        if let Some(scope) = self.0.scopes.write().get_mut(scope_id) {
            scope.fibers.retain(|id| *id != fiber);
        }
        for dependency in dependencies.iter() {
            let symbol = self.intern(dependency);
            self.0.dependencies.remove_dependency(symbol, fiber);
        }
        for provision in provisions.iter() {
            let symbol = self.intern(provision);
            self.0.dependencies.remove_provider(scope_id, symbol, fiber);
        }
        let detached = self.0.plugins.detach_fiber(cell.plugin_id);
        debug_assert!(detached.is_ok(), "fiber/plugin accounting diverged: {detached:?}");
        self.0.plugins.reclaim_if_dead(cell.plugin_id);
    }

    #[allow(clippy::items_after_statements)]
    fn validate_dependency_graph(&self) -> Result<(), CordisError> {
        let mut edges: HashMap<FiberId, SmallVec<[FiberId; 4]>> = HashMap::new();
        let fibers: Vec<_> = self.0.fibers.snapshot().into_iter().filter_map(|(fiber_id, cell)| {
            let fiber = cell.inner.read();
            (fiber.state != FiberState::Disposed).then(|| {
                (fiber_id, fiber.scope, fiber.descriptor.dependencies.clone())
            })
        }).collect();
        for (fiber_id, fiber_scope, dependencies) in fibers {
            for dependency in dependencies.iter() {
                let symbol = self.intern(dependency);
                let mut cursor = Some(fiber_scope);
                let mut providers = SmallVec::<[FiberId; 4]>::new();
                while let Some(scope) = cursor {
                    if let Some(service) = self.0.services.get(scope, symbol).filter(|entry| entry.gate.is_visible()) {
                        providers.push(service.owner);
                        break;
                    }
                    let declared = self.0.dependencies.providers(scope, symbol);
                    if !declared.is_empty() {
                        providers.extend(declared);
                        break;
                    }
                    cursor = self
                        .0
                        .scopes
                        .read()
                        .get(scope)
                        .and_then(|record| record.parent);
                }
                edges.entry(fiber_id).or_default().extend(providers);
            }
        }
        DependencyGraph::validate_cycles(&edges)
    }

    fn reconcile_symbols(
        &self,
        symbols: Vec<ServiceSymbol>,
    ) -> futures::future::BoxFuture<'_, Result<(), CordisError>> {
        Box::pin(async move {
            let mut candidates = HashSet::new();
            {
                for symbol in symbols {
                    candidates.extend(self.0.dependencies.dependents(symbol));
                }
            }
            for fiber_id in candidates {
                let candidate = self.0.fibers.with(fiber_id, |fiber| {
                        (
                            fiber.state,
                            fiber.scope,
                            fiber.descriptor.dependencies.clone(),
                            fiber.descriptor.dependency_policy,
                        )
                    });
                let Some((state, scope, dependencies, policy)) = candidate else {
                    continue;
                };
                let satisfied = self.dependencies_met(scope, &dependencies);
                match (state, satisfied) {
                    (FiberState::Active, false) => {
                        Box::pin(self.dispose_fiber(fiber_id, policy == DependencyPolicy::Restart))
                            .await?;
                    }
                    (FiberState::WaitingDependencies, true) => self.activate(fiber_id).await?,
                    _ => {}
                }
            }
            Ok(())
        })
    }

}
