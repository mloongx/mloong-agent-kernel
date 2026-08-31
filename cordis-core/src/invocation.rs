//! Stable request/response invocation contracts.

use crate::{FiberId, ScopeId};
use std::{any::Any, fmt, sync::Arc};

/// Stable invocation identity shared by native and external hosts.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct InvocationKey {
    namespace: Arc<str>,
    name: Arc<str>,
    version: u32,
}

impl InvocationKey {
    /// Creates a versioned invocation key.
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

    /// Operation name portion.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Protocol version.
    #[must_use]
    pub const fn version(&self) -> u32 {
        self.version
    }
}

impl fmt::Display for InvocationKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}.{}@{}",
            self.namespace, self.name, self.version
        )
    }
}

/// Runtime-generated invocation correlation ID.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct InvocationId(pub u64);

/// Immutable request or response envelope.
#[derive(Clone)]
pub enum InvocationValue {
    /// In-process typed Rust value.
    Native(Arc<dyn Any + Send + Sync>),
    /// Host-neutral opaque bytes with a stable format label.
    External {
        /// Payload encoding or media type.
        format: Arc<str>,
        /// Immutable encoded payload.
        bytes: Arc<[u8]>,
    },
}

impl InvocationValue {
    /// Wraps a native typed value.
    #[must_use]
    pub fn native<T: Any + Send + Sync>(value: Arc<T>) -> Self {
        Self::Native(value)
    }

    /// Attempts to recover a native typed value without panicking.
    ///
    /// # Errors
    ///
    /// Returns the original envelope when it is external or has another
    /// native Rust type.
    pub fn downcast_native<T: Any + Send + Sync>(self) -> Result<Arc<T>, Self> {
        match self {
            Self::Native(value) => Arc::downcast(value).map_err(Self::Native),
            external @ Self::External { .. } => Err(external),
        }
    }
}

impl fmt::Debug for InvocationValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Native(_) => formatter.write_str("Native(..)"),
            Self::External { format, bytes } => formatter
                .debug_struct("External")
                .field("format", format)
                .field("length", &bytes.len())
                .finish(),
        }
    }
}

/// Non-payload metadata passed through an invocation chain.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct InvocationMetadata {
    /// Correlation ID.
    pub id: InvocationId,
    /// Stable operation identity.
    pub key: InvocationKey,
    /// Scope from which handler resolution began.
    pub scope: ScopeId,
    /// Fiber that initiated the call.
    pub caller: FiberId,
}

impl InvocationMetadata {
    /// Creates local invocation metadata while allowing future runtime-produced
    /// fields to gain defaults without external struct construction.
    #[must_use]
    pub const fn new(
        id: InvocationId,
        key: InvocationKey,
        scope: ScopeId,
        caller: FiberId,
    ) -> Self {
        Self {
            id,
            key,
            scope,
            caller,
        }
    }
}
