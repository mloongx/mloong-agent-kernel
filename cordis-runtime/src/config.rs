//! Personal-runtime resource and deadline configuration.

use cordis_core::CordisError;
use std::time::Duration;

/// Bounded resource defaults for a single-user Agent runtime.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct RuntimeConfig {
    /// Grace period for lifecycle-owned tasks.
    pub task_grace: Duration,
    /// Grace period reserved for shutdown orchestration.
    pub shutdown_grace: Duration,
    /// Default invocation deadline.
    pub default_invocation_timeout: Duration,
    /// Global active invocation limit.
    pub max_concurrent_invocations: usize,
    /// Maximum allocated scopes, including staging scopes.
    pub max_scopes: usize,
    /// Maximum allocated fibers, including staging fibers.
    pub max_fibers: usize,
    /// Maximum owned tasks per fiber.
    pub max_tasks_per_fiber: usize,
    /// Maximum event, invocation-handler, and middleware registrations per fiber.
    pub max_handlers_per_fiber: usize,
    /// Maximum effects per fiber.
    pub max_effects_per_fiber: usize,
    /// Maximum child scopes created by one fiber.
    pub max_child_scopes_per_fiber: usize,
    /// Maximum scope nesting below the root.
    pub max_scope_depth: usize,
    /// Maximum stable service symbols retained for the Runtime lifetime.
    pub max_interned_symbols: usize,
    /// Maximum reconstructible service-resolution cache entries. Zero disables caching.
    pub max_resolution_cache_entries: usize,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            task_grace: Duration::from_secs(2),
            shutdown_grace: Duration::from_secs(10),
            default_invocation_timeout: Duration::from_secs(60),
            max_concurrent_invocations: 64,
            max_scopes: 4_096,
            max_fibers: 8_192,
            max_tasks_per_fiber: 128,
            max_handlers_per_fiber: 256,
            max_effects_per_fiber: 256,
            max_child_scopes_per_fiber: 128,
            max_scope_depth: 32,
            max_interned_symbols: 16_384,
            max_resolution_cache_entries: 4_096,
        }
    }
}

impl RuntimeConfig {
    pub(crate) fn validate(&self) -> Result<(), CordisError> {
        if self.task_grace.is_zero()
            || self.shutdown_grace.is_zero()
            || self.default_invocation_timeout.is_zero()
        {
            return Err(CordisError::InvalidRuntimeConfig(
                "grace periods and invocation timeout must be non-zero".into(),
            ));
        }
        if [
            self.max_concurrent_invocations,
            self.max_scopes,
            self.max_fibers,
            self.max_tasks_per_fiber,
            self.max_handlers_per_fiber,
            self.max_effects_per_fiber,
            self.max_child_scopes_per_fiber,
            self.max_scope_depth,
            self.max_interned_symbols,
        ]
        .contains(&0)
        {
            return Err(CordisError::InvalidRuntimeConfig(
                "resource limits must be non-zero".into(),
            ));
        }
        if self.max_interned_symbols > u32::MAX as usize {
            return Err(CordisError::InvalidRuntimeConfig(
                "max_interned_symbols exceeds the ServiceSymbol ID space".into(),
            ));
        }
        Ok(())
    }
}
