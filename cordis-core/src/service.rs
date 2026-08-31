//! Stable service identities and cross-host values.

use std::{any::Any, fmt, sync::Arc};

/// Runtime-local interned service symbol used on hot paths.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct ServiceSymbol(u32);

impl ServiceSymbol {
    /// Creates a symbol from an interner slot.
    #[must_use]
    pub const fn from_index(index: u32) -> Self {
        Self(index)
    }
    /// Returns the interner slot.
    #[must_use]
    pub const fn index(self) -> u32 {
        self.0
    }
}

/// Stable service identity suitable for Rust, JS, WASM, or RPC providers.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ServiceKey {
    namespace: Arc<str>,
    name: Arc<str>,
    version: u32,
}

impl ServiceKey {
    /// Creates a stable service key.
    #[must_use]
    pub fn new(namespace: impl Into<Arc<str>>, name: impl Into<Arc<str>>, version: u32) -> Self {
        Self {
            namespace: namespace.into(),
            name: name.into(),
            version,
        }
    }

    /// Namespace portion.
    #[must_use]
    pub fn namespace(&self) -> &str {
        &self.namespace
    }
    /// Name portion.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    /// ABI version.
    #[must_use]
    pub const fn version(&self) -> u32 {
        self.version
    }
}

impl fmt::Display for ServiceKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}@{}", self.namespace, self.name, self.version)
    }
}

/// Host-neutral service envelope. Native Rust services use `Native`.
#[derive(Clone)]
pub enum ServiceValue {
    /// An in-process, typed Rust value.
    Native(Arc<dyn Any + Send + Sync>),
    /// An opaque future bridge handle.
    External(Arc<str>),
}

impl fmt::Debug for ServiceValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Native(_) => f.write_str("Native(..)"),
            Self::External(id) => f.debug_tuple("External").field(id).finish(),
        }
    }
}
