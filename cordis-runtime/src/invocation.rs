//! Scoped request/response invocation with immutable dispatch snapshots.

use crate::gate::{CapabilityGate, GenerationLease, VisibilityCache};
use async_trait::async_trait;
use cordis_core::{
    CordisError, InvocationHandlerId, InvocationKey, InvocationMetadata, InvocationMiddlewareId,
    InvocationValue, ScopeId,
};
use futures::FutureExt;
use parking_lot::Mutex;
use slotmap::SlotMap;
use std::{future::Future, panic::AssertUnwindSafe, pin::Pin, sync::Arc};
use tokio::time::Instant;

/// Handler result envelope.
pub type InvocationOutcome = InvocationValue;

/// Context visible to handlers and middleware. It contains correlation data,
/// never the business payload itself.
#[derive(Clone, Debug)]
pub struct InvocationContext {
    metadata: InvocationMetadata,
    deadline: Instant,
}

impl InvocationContext {
    /// Returns immutable correlation and ownership metadata.
    #[must_use]
    pub const fn metadata(&self) -> &InvocationMetadata {
        &self.metadata
    }

    pub(crate) const fn deadline(&self) -> Instant {
        self.deadline
    }
}

/// One request/response endpoint.
#[async_trait]
pub trait InvocationHandler: Send + Sync + 'static {
    /// Handles one immutable request value.
    async fn call(
        &self,
        context: InvocationContext,
        input: InvocationValue,
    ) -> Result<InvocationOutcome, CordisError>;
}

/// Middleware around an invocation handler.
#[async_trait]
pub trait InvocationMiddleware: Send + Sync + 'static {
    /// Handles a request and optionally consumes `next` to continue.
    async fn call(
        &self,
        context: InvocationContext,
        input: InvocationValue,
        next: NextInvocation,
    ) -> Result<InvocationOutcome, CordisError>;
}

/// Consumed-on-call continuation. A middleware cannot invoke downstream twice.
pub struct NextInvocation {
    chain: Arc<InvocationChain>,
    index: usize,
}

impl NextInvocation {
    /// Continues the immutable invocation snapshot.
    pub async fn run(
        self,
        context: InvocationContext,
        input: InvocationValue,
    ) -> Result<InvocationOutcome, CordisError> {
        run_chain(self.chain, self.index, context, input).await
    }
}

/// Builds a type-safe native handler from an async function.
pub fn invocation_handler_fn<Request, Response, Function, HandlerFuture>(
    function: Function,
) -> Arc<dyn InvocationHandler>
where
    Request: Send + Sync + 'static,
    Response: Send + Sync + 'static,
    Function: Fn(InvocationContext, Arc<Request>) -> HandlerFuture + Send + Sync + 'static,
    HandlerFuture: Future<Output = Result<Arc<Response>, CordisError>> + Send + 'static,
{
    Arc::new(TypedHandler::<Request, Response, Function> {
        function,
        marker: std::marker::PhantomData,
    })
}

struct TypedHandler<Request, Response, Function> {
    function: Function,
    marker: std::marker::PhantomData<fn(Request) -> Response>,
}

#[async_trait]
impl<Request, Response, Function, HandlerFuture> InvocationHandler
    for TypedHandler<Request, Response, Function>
where
    Request: Send + Sync + 'static,
    Response: Send + Sync + 'static,
    Function: Fn(InvocationContext, Arc<Request>) -> HandlerFuture + Send + Sync + 'static,
    HandlerFuture: Future<Output = Result<Arc<Response>, CordisError>> + Send + 'static,
{
    async fn call(
        &self,
        context: InvocationContext,
        input: InvocationValue,
    ) -> Result<InvocationOutcome, CordisError> {
        let key = context.metadata.key.clone();
        let input = input
            .downcast_native::<Request>()
            .map_err(|_| CordisError::InvocationTypeMismatch(key))?;
        (self.function)(context, input)
            .await
            .map(InvocationValue::native)
    }
}

#[derive(Clone)]
struct HandlerEntry {
    scope: ScopeId,
    key: InvocationKey,
    handler: Arc<dyn InvocationHandler>,
    gate: Arc<CapabilityGate>,
}

#[derive(Clone)]
struct MiddlewareEntry {
    scope: ScopeId,
    key: InvocationKey,
    middleware: Arc<dyn InvocationMiddleware>,
    gate: Arc<CapabilityGate>,
    order: u64,
}

#[derive(Default)]
struct RegistryState {
    handlers: SlotMap<InvocationHandlerId, HandlerEntry>,
    middleware: SlotMap<InvocationMiddlewareId, MiddlewareEntry>,
    next_order: u64,
}

/// Invocation registrations are updated under one short registry lock. Invoke
/// clones an immutable chain and releases the lock before polling user code.
#[derive(Default)]
pub(crate) struct InvocationRegistry {
    state: Mutex<RegistryState>,
}

pub(crate) struct InvocationSnapshot {
    chain: Arc<InvocationChain>,
    leases: Vec<GenerationLease>,
}

struct InvocationChain {
    handler: Arc<dyn InvocationHandler>,
    middleware: Arc<[Arc<dyn InvocationMiddleware>]>,
}

impl InvocationRegistry {
    pub(crate) fn register_handler(
        &self,
        scope: ScopeId,
        key: InvocationKey,
        handler: Arc<dyn InvocationHandler>,
        gate: Arc<CapabilityGate>,
    ) -> Result<InvocationHandlerId, CordisError> {
        let mut state = self.state.lock();
        if state.handlers.values().any(|entry| {
            (entry.gate.is_visible() || Arc::ptr_eq(&entry.gate, &gate))
                && entry.scope == scope
                && entry.key == key
        }) {
            return Err(CordisError::DuplicateInvocationHandler(key));
        }
        Ok(state.handlers.insert(HandlerEntry {
            scope,
            key,
            handler,
            gate,
        }))
    }

    pub(crate) fn register_middleware(
        &self,
        scope: ScopeId,
        key: InvocationKey,
        middleware: Arc<dyn InvocationMiddleware>,
        gate: Arc<CapabilityGate>,
    ) -> InvocationMiddlewareId {
        let mut state = self.state.lock();
        let order = state.next_order;
        state.next_order = state.next_order.wrapping_add(1);
        state.middleware.insert(MiddlewareEntry {
            scope,
            key,
            middleware,
            gate,
            order,
        })
    }

    pub(crate) fn validate_activation(
        &self,
        handler_ids: &[InvocationHandlerId],
        middleware_ids: &[InvocationMiddlewareId],
    ) -> Result<(), CordisError> {
        let state = self.state.lock();
        for id in handler_ids {
            let entry = state.handlers.get(*id).ok_or(CordisError::FiberNotFound)?;
            if state.handlers.iter().any(|(candidate_id, candidate)| {
                candidate_id != *id
                    && (candidate.gate.is_visible() || handler_ids.contains(&candidate_id))
                    && candidate.scope == entry.scope
                    && candidate.key == entry.key
            }) {
                return Err(CordisError::DuplicateInvocationHandler(entry.key.clone()));
            }
        }
        if middleware_ids
            .iter()
            .any(|id| state.middleware.get(*id).is_none())
        {
            return Err(CordisError::FiberNotFound);
        }
        Ok(())
    }

    pub(crate) fn remove_handler(&self, id: InvocationHandlerId) -> bool {
        self.state.lock().handlers.remove(id).is_some()
    }

    pub(crate) fn remove_middleware(&self, id: InvocationMiddlewareId) -> bool {
        self.state.lock().middleware.remove(id).is_some()
    }

    pub(crate) fn validate_revision(
        &self,
        old_handlers: &[InvocationHandlerId],
        staged_handlers: &[InvocationHandlerId],
        target_scope: ScopeId,
    ) -> Result<(), CordisError> {
        let state = self.state.lock();
        for id in staged_handlers {
            let entry = state.handlers.get(*id).ok_or(CordisError::FiberNotFound)?;
            if state.handlers.iter().any(|(candidate_id, candidate)| {
                !old_handlers.contains(&candidate_id)
                    && candidate_id != *id
                    && (candidate.gate.is_visible() || staged_handlers.contains(&candidate_id))
                    && candidate.scope == target_scope
                    && candidate.key == entry.key
            }) {
                return Err(CordisError::DuplicateInvocationHandler(entry.key.clone()));
            }
        }
        Ok(())
    }

    pub(crate) fn commit_revision(
        &self,
        old_handlers: &[InvocationHandlerId],
        old_middleware: &[InvocationMiddlewareId],
        staged_handlers: &[InvocationHandlerId],
        staged_middleware: &[InvocationMiddlewareId],
        target_scope: ScopeId,
    ) {
        let mut state = self.state.lock();
        let _ = (old_handlers, old_middleware);
        for id in staged_handlers {
            if let Some(entry) = state.handlers.get_mut(*id) {
                entry.scope = target_scope;
            }
        }
        for id in staged_middleware {
            if let Some(entry) = state.middleware.get_mut(*id) {
                entry.scope = target_scope;
            }
        }
    }

    pub(crate) fn snapshot(
        &self,
        scopes_root_to_leaf: &[ScopeId],
        key: &InvocationKey,
    ) -> Result<InvocationSnapshot, CordisError> {
        let state = self.state.lock();
        for attempt in 0..2 {
            let mut visibility = VisibilityCache::default();
            let handler_entry = scopes_root_to_leaf
                .iter()
                .rev()
                .find_map(|scope| {
                    state
                        .handlers
                        .values()
                        .find(|entry| {
                            visibility.visible(&entry.gate)
                                && entry.scope == *scope
                                && entry.key == *key
                        })
                        .cloned()
                })
                .ok_or_else(|| CordisError::InvocationHandlerNotFound(key.clone()))?;
            let mut middleware: Vec<_> = state
                .middleware
                .values()
                .filter_map(|entry| {
                    let depth = scopes_root_to_leaf
                        .iter()
                        .position(|scope| *scope == entry.scope)?;
                    (visibility.visible(&entry.gate) && entry.key == *key)
                        .then(|| (depth, entry.order, entry.clone()))
                })
                .collect();
            middleware.sort_by_key(|(depth, order, _)| (*depth, *order));
            let mut leases = Vec::with_capacity(middleware.len() + 1);
            let admitted = handler_entry
                .gate
                .try_acquire()
                .map(|lease| leases.push(lease))
                .is_some()
                && middleware.iter().all(|(_, _, entry)| {
                    entry
                        .gate
                        .try_acquire()
                        .map(|lease| leases.push(lease))
                        .is_some()
                });
            if !admitted {
                drop(leases);
                if attempt == 0 {
                    continue;
                }
                return Err(CordisError::InvocationGenerationChanged);
            }
            return Ok(InvocationSnapshot {
                chain: Arc::new(InvocationChain {
                    handler: handler_entry.handler,
                    middleware: middleware
                        .into_iter()
                        .map(|(_, _, entry)| entry.middleware)
                        .collect::<Vec<_>>()
                        .into(),
                }),
                leases,
            });
        }
        Err(CordisError::InvocationGenerationChanged)
    }
}

impl InvocationSnapshot {
    pub(crate) async fn invoke(
        self,
        context: InvocationContext,
        input: InvocationValue,
    ) -> Result<InvocationOutcome, CordisError> {
        let InvocationSnapshot { chain, leases } = self;
        let _leases = leases;
        run_chain(chain, 0, context, input).await
    }
}

fn run_chain(
    chain: Arc<InvocationChain>,
    index: usize,
    context: InvocationContext,
    input: InvocationValue,
) -> Pin<Box<dyn Future<Output = Result<InvocationOutcome, CordisError>> + Send>> {
    Box::pin(async move {
        if let Some(middleware) = chain.middleware.get(index).cloned() {
            let next = NextInvocation {
                chain,
                index: index + 1,
            };
            return AssertUnwindSafe(middleware.call(context, input, next))
                .catch_unwind()
                .await
                .map_err(|panic| {
                    CordisError::InvocationMiddlewarePanicked(panic_message(panic.as_ref()))
                })?;
        }
        AssertUnwindSafe(chain.handler.call(context, input))
            .catch_unwind()
            .await
            .map_err(|panic| {
                CordisError::InvocationHandlerPanicked(panic_message(panic.as_ref()))
            })?
    })
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    payload.downcast_ref::<&str>().map_or_else(
        || {
            payload
                .downcast_ref::<String>()
                .cloned()
                .unwrap_or_else(|| "non-string panic payload".into())
        },
        |message| (*message).to_owned(),
    )
}

pub(crate) fn invocation_context(
    metadata: InvocationMetadata,
    deadline: Instant,
) -> InvocationContext {
    InvocationContext { metadata, deadline }
}
