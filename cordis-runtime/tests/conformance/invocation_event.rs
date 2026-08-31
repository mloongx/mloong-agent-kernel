use super::support::{Plugin, capture_context};
use async_trait::async_trait;
use cordis_core::{CordisError, EventKey, EventValue, InvocationKey, InvocationValue};
use cordis_runtime::{
    EventHandler, EventOutcome, InvocationContext, InvocationHandler, InvocationMiddleware,
    InvocationOutcome, Next, NextInvocation, Runtime, invocation_handler_fn,
};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Barrier, Notify};

fn invocation_key() -> InvocationKey {
    InvocationKey::new("conformance", "invoke", 1)
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn inv_005_type_mismatch_not_found_panic_and_timeout_are_distinguishable() {
    struct PanicHandler;
    #[async_trait]
    impl InvocationHandler for PanicHandler {
        async fn call(
            &self,
            _: InvocationContext,
            _: InvocationValue,
        ) -> Result<InvocationOutcome, CordisError> {
            panic!("conformance handler panic")
        }
    }

    let runtime = Runtime::new();
    let typed = invocation_key();
    let panic_key = InvocationKey::new("conformance", "panic", 1);
    runtime
        .install(
            runtime.root(),
            Plugin::new("invocation-provider", {
                let typed = typed.clone();
                let panic_key = panic_key.clone();
                move |context| {
                    let typed = typed.clone();
                    let panic_key = panic_key.clone();
                    Box::pin(async move {
                        context.handle_invocation(
                            typed,
                            invocation_handler_fn(|_, value: Arc<u32>| async move {
                                Ok(Arc::new(*value + 1))
                            }),
                        )?;
                        context.handle_invocation(panic_key, Arc::new(PanicHandler))?;
                        Ok(())
                    })
                }
            }),
        )
        .await
        .expect("provider");
    let (_, caller) = capture_context(&runtime, runtime.root(), "invocation-caller").await;
    assert_eq!(
        *caller
            .invoke_typed::<u32, u32>(&typed, Arc::new(1))
            .await
            .expect("typed"),
        2
    );
    assert!(matches!(
        caller
            .invoke_typed::<String, u32>(&typed, Arc::new("bad".into()))
            .await,
        Err(CordisError::InvocationTypeMismatch(_))
    ));
    assert!(matches!(
        caller
            .invoke_typed::<u32, String>(&typed, Arc::new(1))
            .await,
        Err(CordisError::InvocationTypeMismatch(_))
    ));
    assert!(matches!(
        caller
            .invoke(
                &InvocationKey::new("missing", "handler", 1),
                InvocationValue::native(Arc::new(()))
            )
            .await,
        Err(CordisError::InvocationHandlerNotFound(_))
    ));
    assert!(matches!(
        caller
            .invoke(&panic_key, InvocationValue::native(Arc::new(())))
            .await,
        Err(CordisError::InvocationHandlerPanicked(_))
    ));

    let pending = InvocationKey::new("conformance", "pending", 1);
    runtime
        .install(
            runtime.root(),
            Plugin::new("pending-provider", {
                let pending = pending.clone();
                move |context| {
                    let pending = pending.clone();
                    Box::pin(async move {
                        context.handle_invocation(
                            pending,
                            invocation_handler_fn(|_, _: Arc<()>| async move {
                                std::future::pending::<Result<Arc<()>, CordisError>>().await
                            }),
                        )?;
                        Ok(())
                    })
                }
            }),
        )
        .await
        .expect("pending provider");
    assert!(matches!(
        caller
            .invoke_typed_with_timeout::<(), ()>(&pending, Arc::new(()), Duration::from_millis(20))
            .await,
        Err(CordisError::InvocationTimedOut)
    ));
}

struct RecordingMiddleware {
    id: usize,
    order: Arc<Mutex<Vec<usize>>>,
}

#[async_trait]
impl InvocationMiddleware for RecordingMiddleware {
    async fn call(
        &self,
        context: InvocationContext,
        input: InvocationValue,
        next: NextInvocation,
    ) -> Result<InvocationOutcome, CordisError> {
        self.order.lock().expect("order mutex").push(self.id);
        let result = next.run(context, input).await;
        self.order.lock().expect("order mutex").push(self.id * 10);
        result
    }
}

#[tokio::test]
async fn inv_004_middleware_is_root_to_leaf_then_registration_order() {
    let runtime = Runtime::new();
    let operation = InvocationKey::new("conformance", "middleware-order", 1);
    let order = Arc::new(Mutex::new(Vec::new()));
    runtime
        .install(
            runtime.root(),
            Plugin::new("root-invocation", {
                let operation = operation.clone();
                let order = order.clone();
                move |context| {
                    let operation = operation.clone();
                    let order = order.clone();
                    Box::pin(async move {
                        context.handle_invocation(
                            operation.clone(),
                            invocation_handler_fn({
                                let order = order.clone();
                                move |_, value: Arc<u32>| {
                                    let order = order.clone();
                                    async move {
                                        order.lock().expect("order mutex").push(4);
                                        Ok(value)
                                    }
                                }
                            }),
                        )?;
                        for id in [1, 2] {
                            context.invocation_middleware(
                                operation.clone(),
                                Arc::new(RecordingMiddleware {
                                    id,
                                    order: order.clone(),
                                }),
                            )?;
                        }
                        Ok(())
                    })
                }
            }),
        )
        .await
        .expect("root chain");
    let child = runtime
        .create_scope(runtime.root(), "middleware-child")
        .expect("child");
    runtime
        .install(
            child,
            Plugin::new("child-middleware", {
                let operation = operation.clone();
                let order = order.clone();
                move |context| {
                    let operation = operation.clone();
                    let order = order.clone();
                    Box::pin(async move {
                        context.invocation_middleware(
                            operation,
                            Arc::new(RecordingMiddleware { id: 3, order }),
                        )?;
                        Ok(())
                    })
                }
            }),
        )
        .await
        .expect("child middleware");
    let (_, caller) = capture_context(&runtime, child, "middleware-caller").await;
    caller
        .invoke_typed::<u32, u32>(&operation, Arc::new(1))
        .await
        .expect("invoke");
    assert_eq!(
        *order.lock().expect("order mutex"),
        vec![1, 2, 3, 4, 30, 20, 10]
    );
}

pub(super) struct BlockingInvocation {
    pub(super) entered: Arc<Notify>,
    pub(super) release: Arc<Notify>,
    pub(super) value: u32,
}

#[async_trait]
impl InvocationHandler for BlockingInvocation {
    async fn call(
        &self,
        _: InvocationContext,
        _: InvocationValue,
    ) -> Result<InvocationOutcome, CordisError> {
        self.entered.notify_one();
        self.release.notified().await;
        Ok(InvocationValue::native(Arc::new(self.value)))
    }
}

#[tokio::test]
async fn inv_004_in_flight_invocation_uses_one_immutable_dispatch_snapshot() {
    let runtime = Runtime::new();
    let operation = InvocationKey::new("conformance", "snapshot", 1);
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let provider = runtime
        .install(
            runtime.root(),
            Plugin::new("snapshot-provider", {
                let operation = operation.clone();
                let entered = entered.clone();
                let release = release.clone();
                move |context| {
                    let operation = operation.clone();
                    let handler = BlockingInvocation {
                        entered: entered.clone(),
                        release: release.clone(),
                        value: 7,
                    };
                    Box::pin(async move {
                        context.handle_invocation(operation, Arc::new(handler))?;
                        Ok(())
                    })
                }
            }),
        )
        .await
        .expect("provider");
    let (_, caller) = capture_context(&runtime, runtime.root(), "snapshot-caller").await;
    let admitted = tokio::spawn({
        let caller = caller.clone();
        let operation = operation.clone();
        async move {
            caller
                .invoke_typed::<u32, u32>(&operation, Arc::new(0))
                .await
        }
    });
    entered.notified().await;
    let disposal = tokio::spawn({
        let runtime = runtime.clone();
        async move { runtime.dispose_fiber(provider, false).await }
    });
    tokio::task::yield_now().await;
    assert!(!admitted.is_finished());
    release.notify_waiters();
    assert_eq!(*admitted.await.expect("join").expect("old snapshot"), 7);
    disposal.await.expect("dispose join").expect("dispose");
    assert!(matches!(
        caller
            .invoke_typed::<u32, u32>(&operation, Arc::new(0))
            .await,
        Err(CordisError::InvocationHandlerNotFound(_))
    ));
}

#[tokio::test]
async fn err_001_caller_disposal_cancels_active_invocation() {
    let runtime = Runtime::new();
    let operation = InvocationKey::new("conformance", "cancellation", 1);
    let entered = Arc::new(Notify::new());
    runtime
        .install(
            runtime.root(),
            Plugin::new("cancellation-provider", {
                let operation = operation.clone();
                let entered = entered.clone();
                move |context| {
                    let operation = operation.clone();
                    let handler = BlockingInvocation {
                        entered: entered.clone(),
                        release: Arc::new(Notify::new()),
                        value: 0,
                    };
                    Box::pin(async move {
                        context.handle_invocation(operation, Arc::new(handler))?;
                        Ok(())
                    })
                }
            }),
        )
        .await
        .expect("provider");
    let (caller_fiber, caller) =
        capture_context(&runtime, runtime.root(), "cancellation-caller").await;
    let invocation = tokio::spawn(async move {
        caller
            .invoke_typed::<u32, u32>(&operation, Arc::new(0))
            .await
    });
    entered.notified().await;
    runtime
        .dispose_fiber(caller_fiber, false)
        .await
        .expect("dispose caller");
    assert!(matches!(
        invocation.await.expect("invocation join"),
        Err(CordisError::InvocationCancelled)
    ));
}

struct RecordingHandler {
    id: usize,
    outcome: bool,
    order: Arc<Mutex<Vec<usize>>>,
}
#[async_trait]
impl EventHandler for RecordingHandler {
    async fn call(&self, _: EventValue, _: Option<Next>) -> Result<EventOutcome, CordisError> {
        self.order.lock().expect("order mutex").push(self.id);
        Ok(EventOutcome(
            self.outcome.then(|| Arc::new(self.id) as EventValue),
        ))
    }
}

struct ParallelHandler {
    barrier: Arc<Barrier>,
    completed: Arc<AtomicUsize>,
}
#[async_trait]
impl EventHandler for ParallelHandler {
    async fn call(&self, _: EventValue, _: Option<Next>) -> Result<EventOutcome, CordisError> {
        self.barrier.wait().await;
        self.completed.fetch_add(1, Ordering::SeqCst);
        Ok(EventOutcome::default())
    }
}

#[tokio::test]
async fn evt_001_later_handler_does_not_receive_an_old_event() {
    let runtime = Runtime::new();
    let event = EventKey("conformance.no-replay".into());
    let (_, emitter) = capture_context(&runtime, runtime.root(), "early-emitter").await;
    emitter
        .emit(&event, Arc::new(()))
        .await
        .expect("emit without handler");
    let calls = Arc::new(AtomicUsize::new(0));
    runtime
        .install(
            runtime.root(),
            Plugin::new("late-listener", {
                let event = event.clone();
                let calls = calls.clone();
                move |context| {
                    let event = event.clone();
                    let calls = calls.clone();
                    Box::pin(async move {
                        struct Counter(Arc<AtomicUsize>);
                        #[async_trait]
                        impl EventHandler for Counter {
                            async fn call(
                                &self,
                                _: EventValue,
                                _: Option<Next>,
                            ) -> Result<EventOutcome, CordisError> {
                                self.0.fetch_add(1, Ordering::SeqCst);
                                Ok(EventOutcome::default())
                            }
                        }
                        context.on(event, Arc::new(Counter(calls)))?;
                        Ok(())
                    })
                }
            }),
        )
        .await
        .expect("late listener");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    emitter.emit(&event, Arc::new(())).await.expect("new emit");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn evt_002_emit_bail_serial_parallel_and_waterfall_semantics_are_public() {
    let runtime = Runtime::new();
    let serial = EventKey("conformance.serial".into());
    let bail = EventKey("conformance.bail".into());
    let parallel = EventKey("conformance.parallel".into());
    let order = Arc::new(Mutex::new(Vec::new()));
    let barrier = Arc::new(Barrier::new(2));
    let completed = Arc::new(AtomicUsize::new(0));
    let captured = Arc::new(Mutex::new(None));
    runtime
        .install(
            runtime.root(),
            Plugin::new("event-modes", {
                let serial = serial.clone();
                let bail = bail.clone();
                let parallel = parallel.clone();
                let order = order.clone();
                let barrier = barrier.clone();
                let completed = completed.clone();
                let captured = captured.clone();
                move |context| {
                    let serial = serial.clone();
                    let bail = bail.clone();
                    let parallel = parallel.clone();
                    let order = order.clone();
                    let barrier = barrier.clone();
                    let completed = completed.clone();
                    let captured = captured.clone();
                    Box::pin(async move {
                        for id in 1..=3 {
                            context.on(
                                serial.clone(),
                                Arc::new(RecordingHandler {
                                    id,
                                    outcome: false,
                                    order: order.clone(),
                                }),
                            )?;
                        }
                        for id in 1..=3 {
                            context.on(
                                bail.clone(),
                                Arc::new(RecordingHandler {
                                    id,
                                    outcome: id == 2,
                                    order: order.clone(),
                                }),
                            )?;
                        }
                        for _ in 0..2 {
                            context.on(
                                parallel.clone(),
                                Arc::new(ParallelHandler {
                                    barrier: barrier.clone(),
                                    completed: completed.clone(),
                                }),
                            )?;
                        }
                        *captured.lock().expect("capture mutex") = Some(context);
                        Ok(())
                    })
                }
            }),
        )
        .await
        .expect("listeners");
    let context = captured
        .lock()
        .expect("capture mutex")
        .clone()
        .expect("context");
    context.emit(&serial, Arc::new(())).await.expect("emit");
    order.lock().expect("order mutex").clear();
    context.serial(&serial, Arc::new(())).await.expect("serial");
    assert_eq!(*order.lock().expect("order mutex"), vec![1, 2, 3]);
    order.lock().expect("order mutex").clear();
    let outcome = context.bail(&bail, Arc::new(())).await.expect("bail");
    assert_eq!(
        *outcome
            .0
            .expect("bail value")
            .downcast::<usize>()
            .expect("usize"),
        2
    );
    assert_eq!(*order.lock().expect("order mutex"), vec![1, 2]);
    context
        .parallel(&parallel, Arc::new(()))
        .await
        .expect("parallel");
    assert_eq!(completed.load(Ordering::SeqCst), 2);
    let waterfall = context
        .waterfall(
            &EventKey("conformance.unhandled-waterfall".into()),
            Arc::new(7_u32),
        )
        .await
        .expect("waterfall");
    assert_eq!(
        *waterfall
            .0
            .expect("original value")
            .downcast::<u32>()
            .expect("u32"),
        7
    );
}
