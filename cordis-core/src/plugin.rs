//! Host-neutral plugin metadata.

use crate::ServiceKey;
use std::sync::Arc;

/// Behavior when a required service disappears.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum DependencyPolicy {
    /// Roll back activation and wait for dependencies to return.
    #[default]
    Restart,
    /// Permanently dispose the plugin instance.
    Dispose,
}

/// Monotonic plugin source revision used by future HMR hosts.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct PluginRevision(pub u64);

/// Runtime-independent plugin metadata.
#[derive(Clone, Debug)]
pub struct PluginDescriptor {
    /// Stable plugin name.
    pub name: Arc<str>,
    /// Required services.
    pub dependencies: Arc<[ServiceKey]>,
    /// Services this plugin promises to provide after successful activation.
    pub provisions: Arc<[ServiceKey]>,
    /// Missing dependency policy.
    pub dependency_policy: DependencyPolicy,
    /// Source revision.
    pub revision: PluginRevision,
}
