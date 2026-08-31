//! Fiber lifecycle state machine.

/// State of a lifecycle-owned runtime unit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FiberState {
    /// Allocated but not dependency-checked.
    Created,
    /// At least one required service is absent.
    WaitingDependencies,
    /// Plugin activation is executing.
    Starting,
    /// Plugin is active.
    Active,
    /// A replacement activation is being prepared.
    Reloading,
    /// Cleanup is executing.
    Disposing,
    /// Cleanup completed.
    Disposed,
    /// Activation or execution failed.
    Failed,
}

impl FiberState {
    /// Returns whether `self -> next` is a legal runtime transition.
    #[must_use]
    pub const fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (
                Self::Created,
                Self::WaitingDependencies | Self::Starting | Self::Disposing
            ) | (Self::WaitingDependencies, Self::Starting | Self::Disposing)
                | (
                    Self::Starting | Self::Reloading,
                    Self::Active | Self::Failed | Self::Disposing
                )
                | (
                    Self::Active,
                    Self::Reloading | Self::Disposing | Self::Failed
                )
                | (Self::Failed, Self::Disposing)
                | (Self::Disposing, Self::Disposed | Self::WaitingDependencies)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::FiberState;

    #[test]
    fn rejects_illegal_transition() {
        assert!(!FiberState::Created.can_transition_to(FiberState::Active));
        assert!(!FiberState::Disposed.can_transition_to(FiberState::Starting));
    }
}
