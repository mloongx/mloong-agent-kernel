impl Runtime {
    fn scope_ancestry(&self, scope: ScopeId) -> Vec<ScopeId> {
        self.0.scopes.ancestry(scope).unwrap_or_default()
    }

    fn resolve_symbol(&self, scope: ScopeId, symbol: ServiceSymbol) -> Option<ServiceEntry> {
        let ancestry = self.scope_ancestry(scope);
        self.0.services.resolve(scope, &ancestry, symbol)
    }

    fn resolve_owned(
        &self,
        scope: ScopeId,
        symbol: ServiceSymbol,
        owner: FiberId,
    ) -> Option<ServiceEntry> {
        let ancestry = self.scope_ancestry(scope);
        self.0
            .services
            .resolve_owned(&ancestry, symbol, owner)
            .or_else(|| self.0.services.resolve(scope, &ancestry, symbol))
    }
}
