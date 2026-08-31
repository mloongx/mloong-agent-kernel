//! Native Runtime semantic conformance oracle.
//!
//! This target intentionally imports only the public surfaces of `cordis-core`
//! and `cordis-runtime`. Contract IDs in test names map to the B1 manifest.

#[path = "conformance/disposal_shutdown.rs"]
mod disposal_shutdown;
#[path = "conformance/identity_scope_service.rs"]
mod identity_scope_service;
#[path = "conformance/invocation_event.rs"]
mod invocation_event;
#[path = "conformance/lifecycle_resources.rs"]
mod lifecycle_resources;
#[path = "conformance/reload.rs"]
mod reload;
#[path = "conformance/support.rs"]
mod support;
