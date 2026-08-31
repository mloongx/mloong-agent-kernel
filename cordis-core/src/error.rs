//! Explicit runtime errors.

use crate::{FiberId, FiberState, InvocationKey, ScopeId, ServiceKey};
use std::{fmt, sync::Arc};
use thiserror::Error;

/// Stable semantic category for execution-host failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum HostFailureKind {
    /// Runtime and Host could not negotiate a compatible protocol.
    HandshakeIncompatible,
    /// The peer violated the negotiated protocol or message state machine.
    ProtocolViolation,
    /// The underlying transport closed before the operation completed.
    TransportClosed,
    /// The hosted process exited unexpectedly.
    ProcessExited,
    /// The hosted process was forcefully terminated.
    ProcessKilled,
    /// A message exceeded a negotiated hard limit.
    MessageTooLarge,
    /// The Host cannot process the declared payload format.
    UnsupportedFormat,
    /// The Host does not implement a requested capability.
    UnsupportedCapability,
    /// The Host rejected work because bounded capacity was exhausted.
    Overloaded,
    /// The Host or remote object is unavailable.
    Unavailable,
}

/// Typed execution-host failure with diagnostic, non-semantic text.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct HostError {
    kind: HostFailureKind,
    message: Arc<str>,
}

impl HostError {
    /// Creates a typed Host failure.
    #[must_use]
    pub fn new(kind: HostFailureKind, message: impl Into<Arc<str>>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    /// Stable semantic failure category.
    #[must_use]
    pub const fn kind(&self) -> HostFailureKind {
        self.kind
    }

    /// Diagnostic text; callers must not use it as a semantic oracle.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for HostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}: {}", self.kind, self.message)
    }
}

impl std::error::Error for HostError {}

/// Typed failure intentionally returned by remote plugin code.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct RemoteDomainError {
    code: Arc<str>,
    message: Arc<str>,
    details: Option<(Arc<str>, Arc<[u8]>)>,
}

impl RemoteDomainError {
    /// Creates a remote domain failure without opaque details.
    #[must_use]
    pub fn new(code: impl Into<Arc<str>>, message: impl Into<Arc<str>>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            details: None,
        }
    }

    /// Attaches format-tagged details already validated against Host limits.
    #[must_use]
    pub fn with_details(
        mut self,
        format: impl Into<Arc<str>>,
        bytes: impl Into<Arc<[u8]>>,
    ) -> Self {
        self.details = Some((format.into(), bytes.into()));
        self
    }

    /// Stable opaque domain code.
    #[must_use]
    pub fn code(&self) -> &str {
        &self.code
    }

    /// Diagnostic text; callers must not use it as a semantic oracle.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Optional bounded opaque details as `(format, bytes)`.
    #[must_use]
    pub fn details(&self) -> Option<(&str, &[u8])> {
        self.details
            .as_ref()
            .map(|(format, bytes)| (format.as_ref(), bytes.as_ref()))
    }
}

impl fmt::Display for RemoteDomainError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for RemoteDomainError {}

/// Runtime resource governed by a configured quota.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ResourceKind {
    /// Scope arena entries.
    Scopes,
    /// Fiber arena entries.
    Fibers,
    /// Tasks owned by one Fiber.
    TasksPerFiber,
    /// Event and invocation registrations owned by one Fiber.
    HandlersPerFiber,
    /// Effects owned by one Fiber.
    EffectsPerFiber,
    /// Child scopes owned by one Fiber.
    ChildScopesPerFiber,
    /// Scope tree depth.
    ScopeDepth,
    /// Stable service symbols retained by the Runtime interner.
    ServiceSymbols,
}

/// Errors surfaced by the Cordis core contract.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum CordisError {
    /// A runtime configuration value is invalid.
    #[error("invalid runtime configuration: {0}")]
    InvalidRuntimeConfig(String),
    /// An execution Host, transport, or Host protocol operation failed.
    #[error("host failure: {0}")]
    Host(HostError),
    /// Remote plugin code intentionally returned a typed domain failure.
    #[error("remote domain failure: {0}")]
    RemoteDomain(RemoteDomainError),
    /// A configured resource quota was reached.
    #[error("resource limit exceeded for {resource:?}: {limit}")]
    ResourceLimitExceeded {
        /// Resource whose quota was reached.
        resource: ResourceKind,
        /// Configured maximum.
        limit: usize,
    },
    /// A lifecycle transition was not legal.
    #[error("invalid fiber state transition for {fiber:?}: {from:?} -> {to:?}")]
    InvalidFiberState {
        /// Target fiber.
        fiber: FiberId,
        /// Current state.
        from: FiberState,
        /// Requested state.
        to: FiberState,
    },
    /// A referenced fiber no longer exists.
    #[error("fiber not found")]
    FiberNotFound,
    /// A context attempted mutation outside an active activation lifecycle.
    #[error("fiber {0:?} is not accepting context mutations")]
    FiberInactive(FiberId),
    /// A public lifecycle operation targeted a Fiber owned by a reload transaction.
    #[error("fiber {0:?} lifecycle is owned by a reload transaction")]
    FiberLifecycleOwned(FiberId),
    /// A retained Context belongs to a different or no-longer-accepting generation.
    #[error("stale context generation for {fiber:?}: captured {expected}, current {actual}")]
    StaleContextGeneration {
        /// Context owner.
        fiber: FiberId,
        /// Generation captured when the Context was created.
        expected: u64,
        /// `FiberCell` generation observed by the rejected operation.
        actual: u64,
    },
    /// Activation returned and its staged topology can no longer be changed.
    #[error("activation topology is sealed for fiber {0:?}")]
    ActivationSealed(FiberId),
    /// A committed activation could not publish its capability generation.
    #[error("capability publication failed for fiber {0:?}")]
    CapabilityPublicationFailed(FiberId),
    /// A referenced scope no longer exists.
    #[error("scope not found")]
    ScopeNotFound,
    /// An operation targeted a disposed scope.
    #[error("scope {0:?} is disposed")]
    ScopeDisposed(ScopeId),
    /// A service could not be resolved.
    #[error("service not found: {0}")]
    ServiceNotFound(ServiceKey),
    /// A resolved provider generation stopped accepting use before a tracked
    /// service handle could be acquired.
    #[error("service provider generation is draining: {provider:?}")]
    ServiceGenerationDraining {
        /// Provider whose exact generation lost the lookup/drain race.
        provider: FiberId,
    },
    /// A resolved service had an unexpected native Rust type.
    #[error("service type mismatch: {0}")]
    TypeMismatch(ServiceKey),
    /// A scope already has a provider for this key.
    #[error("duplicate service: {0}")]
    DuplicateService(ServiceKey),
    /// Required services are absent.
    #[error("dependency missing: {0}")]
    DependencyMissing(ServiceKey),
    /// The dependency graph contains a cycle.
    #[error("dependency cycle: {0}")]
    DependencyCycle(String),
    /// Plugin activation failed.
    #[error("plugin start failed: {0}")]
    PluginStartFailed(String),
    /// Activation failed before commit and rollback also reported failures.
    #[error("activation failed: {primary}; rollback failures: {cleanup:?}")]
    ActivationFailed {
        /// Original pre-commit failure.
        primary: Box<CordisError>,
        /// Failures observed while rolling staged resources back.
        cleanup: Vec<CordisError>,
    },
    /// Reload failed before its generation selector commit and rollback also
    /// reported zero or more cleanup issues.
    #[error("reload preparation failed: {primary}; rollback failures: {cleanup:?}")]
    ReloadFailed {
        /// Original pre-commit failure.
        primary: Box<CordisError>,
        /// Failures observed while reclaiming staged resources.
        cleanup: Vec<CordisError>,
    },
    /// Reload committed its new generation, but old-generation cleanup did not
    /// complete cleanly. The new Fiber remains the active data-plane truth.
    #[error("reload committed fiber {new_fiber:?}, but cleanup failed: {cleanup}")]
    ReloadCommitted {
        /// Newly committed Fiber.
        new_fiber: FiberId,
        /// Post-commit cleanup failure.
        cleanup: Box<CordisError>,
    },
    /// Plugin cleanup failed.
    #[error("plugin dispose failed: {0}")]
    PluginDisposeFailed(String),
    /// The runtime-owned disposal worker panicked outside a plugin boundary.
    #[error("disposal worker panicked: {0}")]
    DisposalWorkerPanicked(String),
    /// The runtime-owned disposal worker was cancelled before completion.
    #[error("disposal worker was cancelled")]
    DisposalWorkerCancelled,
    /// The disposal worker terminated without being able to finalize its fiber.
    #[error("disposal worker terminated: {0}")]
    DisposalWorkerTerminated(String),
    /// A runtime-owned scope disposal worker panicked.
    #[error("scope disposal worker panicked: {0}")]
    ScopeDisposalPanicked(String),
    /// A runtime-owned scope disposal worker was cancelled.
    #[error("scope disposal worker was cancelled")]
    ScopeDisposalCancelled,
    /// A scope disposal could not complete its traversal.
    #[error("scope disposal terminated: {0}")]
    ScopeDisposalTerminated(String),
    /// A runtime-owned shutdown worker panicked.
    #[error("shutdown worker panicked: {0}")]
    ShutdownWorkerPanicked(String),
    /// A runtime-owned shutdown worker was cancelled.
    #[error("shutdown worker was cancelled")]
    ShutdownWorkerCancelled,
    /// Runtime shutdown exceeded its configured grace period.
    #[error("shutdown timed out")]
    ShutdownTimedOut,
    /// A scope or runtime cleanup completed best-effort with one or more errors.
    #[error("cleanup failed: {0}")]
    CleanupFailed(String),
    /// The runtime no longer accepts new work.
    #[error("runtime is shutting down")]
    RuntimeShuttingDown,
    /// An event value did not match the expected type.
    #[error("event value type mismatch")]
    EventTypeMismatch,
    /// An event handler panicked at its host boundary.
    #[error("event handler panicked: {0}")]
    EventHandlerPanicked(String),
    /// No visible handler exists for an invocation key.
    #[error("invocation handler not found: {0}")]
    InvocationHandlerNotFound(InvocationKey),
    /// A selected provider generation stopped accepting execution before admission.
    #[error("invocation generation changed while acquiring execution lease")]
    InvocationGenerationChanged,
    /// A native invocation request or response had an unexpected Rust type.
    #[error("invocation type mismatch: {0}")]
    InvocationTypeMismatch(InvocationKey),
    /// The same scope already contains an active handler for the key.
    #[error("duplicate invocation handler: {0}")]
    DuplicateInvocationHandler(InvocationKey),
    /// The caller fiber ended while the invocation was running.
    #[error("invocation was cancelled")]
    InvocationCancelled,
    /// An invocation handler panicked at its host boundary.
    #[error("invocation handler panicked: {0}")]
    InvocationHandlerPanicked(String),
    /// Invocation middleware panicked at its host boundary.
    #[error("invocation middleware panicked: {0}")]
    InvocationMiddlewarePanicked(String),
    /// A handler or middleware returned a domain failure.
    #[error("invocation failed: {0}")]
    InvocationFailed(String),
    /// An invocation exceeded its configured deadline.
    #[error("invocation timed out")]
    InvocationTimedOut,
    /// An owned task or timer was cancelled by lifecycle disposal.
    #[error("owned task was cancelled")]
    TaskCancelled,
    /// An owned task panicked.
    #[error("owned task panicked: {0}")]
    TaskPanicked(String),
    /// A timeout elapsed.
    #[error("operation timed out")]
    Timeout,
    /// A plugin panicked across its host boundary.
    #[error("plugin panicked: {0}")]
    PluginPanicked(String),
    /// A staged plugin revision failed validation.
    #[error("plugin revision validation failed: {0}")]
    RevisionValidationFailed(String),
    /// A runtime invariant was violated.
    #[error("runtime invariant violated: {0}")]
    Invariant(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_host_and_remote_domain_failures_preserve_semantic_fields() {
        let host = HostError::new(HostFailureKind::TransportClosed, "diagnostic only");
        assert_eq!(host.kind(), HostFailureKind::TransportClosed);
        assert_eq!(host.message(), "diagnostic only");
        assert!(
            matches!(CordisError::Host(host), CordisError::Host(error) if error.kind() == HostFailureKind::TransportClosed)
        );

        let domain = RemoteDomainError::new("quota.denied", "diagnostic")
            .with_details("application/example", Arc::<[u8]>::from([1, 2, 3]));
        assert_eq!(domain.code(), "quota.denied");
        assert_eq!(domain.message(), "diagnostic");
        assert_eq!(
            domain.details(),
            Some(("application/example", &[1, 2, 3][..]))
        );
    }
}
