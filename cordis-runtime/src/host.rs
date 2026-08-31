//! Stable extension boundary for Node.js, WASM, and remote plugin hosts.

use crate::NativePlugin;
use async_trait::async_trait;
use cordis_core::{CordisError, PluginRevision};
use std::sync::Arc;

mod process;
pub use process::{ProcessHost, ProcessHostConfig};

/// Opaque plugin artifact passed to a host adapter.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct PluginArtifact {
    /// Host-specific format, for example `node-esm`, `wasm-component`, or `rpc`.
    pub format: Arc<str>,
    /// Logical source revision.
    pub revision: PluginRevision,
    /// Immutable artifact bytes or a serialized host descriptor.
    pub payload: Arc<[u8]>,
}

impl PluginArtifact {
    /// Creates an opaque artifact without freezing future Host metadata fields.
    #[must_use]
    pub fn new(
        format: impl Into<Arc<str>>,
        revision: PluginRevision,
        payload: impl Into<Arc<[u8]>>,
    ) -> Self {
        Self {
            format: format.into(),
            revision,
            payload: payload.into(),
        }
    }
}

/// Adapter implemented by an external execution host.
///
/// The returned plugin is a lifecycle proxy: all capabilities it exposes must
/// still be registered through [`crate::Context`], keeping ownership in Rust.
#[async_trait]
pub trait PluginHost: Send + Sync + 'static {
    /// Diagnostic host kind.
    fn kind(&self) -> &'static str;

    /// Loads and validates an artifact, returning a lifecycle proxy.
    async fn load(&self, artifact: PluginArtifact) -> Result<Arc<dyn NativePlugin>, CordisError>;
}
