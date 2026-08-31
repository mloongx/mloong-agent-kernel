use crate::gate::GenerationLease;
use cordis_core::{FiberId, ScopeId, ServiceSymbol};
use std::{fmt, ops::Deref, sync::Arc};

/// A generation-tracked reference to a Runtime service.
///
/// The handle intentionally does not implement `Clone` and exposes no API that
/// extracts its internal `Arc<T>`. It represents one admitted use of the exact
/// provider generation and keeps that generation alive until the handle drops.
///
/// The lifetime guarantee covers access through this handle. A service type
/// that implements `Clone` or otherwise exports independent resource ownership
/// defines the lifetime semantics of those independently escaped values.
pub struct ServiceHandle<T: ?Sized> {
    service: Arc<T>,
    _lease: GenerationLease,
    provider: FiberId,
    generation: u64,
    scope: ScopeId,
    symbol: ServiceSymbol,
}

impl<T: ?Sized> ServiceHandle<T> {
    pub(crate) fn new(
        service: Arc<T>,
        lease: GenerationLease,
        provider: FiberId,
        generation: u64,
        scope: ScopeId,
        symbol: ServiceSymbol,
    ) -> Self {
        Self {
            service,
            _lease: lease,
            provider,
            generation,
            scope,
            symbol,
        }
    }

    /// Provider Fiber owning this immutable service binding.
    #[must_use]
    pub const fn provider_fiber(&self) -> FiberId {
        self.provider
    }

    /// Provider generation captured by this handle.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Scope where this service entry was resolved.
    #[must_use]
    pub const fn scope(&self) -> ScopeId {
        self.scope
    }

    /// Interned service symbol used for this lookup.
    #[must_use]
    pub const fn symbol(&self) -> ServiceSymbol {
        self.symbol
    }
}

impl<T: ?Sized> Deref for ServiceHandle<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.service.as_ref()
    }
}

impl<T: ?Sized> fmt::Debug for ServiceHandle<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServiceHandle")
            .field("provider", &self.provider)
            .field("generation", &self.generation)
            .field("scope", &self.scope)
            .field("symbol", &self.symbol)
            .finish_non_exhaustive()
    }
}

impl<T: ?Sized + fmt::Display> fmt::Display for ServiceHandle<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.deref().fmt(formatter)
    }
}
