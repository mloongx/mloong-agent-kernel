//! Event contracts shared by plugin hosts.

use std::{any::Any, sync::Arc};

/// Stable event identity.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EventKey(pub Arc<str>);

/// Dynamically typed event payload/result for host interoperability.
pub type EventValue = Arc<dyn Any + Send + Sync>;

/// Dispatch contract for an event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DispatchMode {
    /// Notify every handler serially and ignore values.
    Emit,
    /// Stop at the first handler producing a value.
    Bail,
    /// Run every handler serially and collect outcomes.
    Serial,
    /// Run all handlers concurrently.
    Parallel,
    /// Run consumed-continuation middleware.
    Waterfall,
}
