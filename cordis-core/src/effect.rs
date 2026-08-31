//! Async, fallible cleanup abstraction.

use crate::CordisError;
use std::{future::Future, pin::Pin};

/// Boxed cleanup future.
pub type EffectFuture = Pin<Box<dyn Future<Output = Result<(), CordisError>> + Send + 'static>>;

/// A lifecycle-owned reversible side effect.
pub trait Effect: Send + Sync + 'static {
    /// Reverts the effect.
    fn dispose(self: Box<Self>) -> EffectFuture;
}

struct FnEffect<F>(Option<F>);

impl<F, Fut> Effect for FnEffect<F>
where
    F: FnOnce() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), CordisError>> + Send + 'static,
{
    fn dispose(mut self: Box<Self>) -> EffectFuture {
        match self.0.take() {
            Some(dispose) => Box::pin(dispose()),
            None => Box::pin(async { Err(CordisError::Invariant("effect disposed twice".into())) }),
        }
    }
}

/// Creates an effect from an async or immediately-ready cleanup closure.
pub fn effect_fn<F, Fut>(dispose: F) -> Box<dyn Effect>
where
    F: FnOnce() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), CordisError>> + Send + 'static,
{
    Box::new(FnEffect(Some(dispose)))
}
