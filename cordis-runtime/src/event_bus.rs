use crate::gate::{CapabilityGate, VisibilityCache};
use arc_swap::ArcSwap;
use async_trait::async_trait;
use cordis_core::{CordisError, EventKey, EventValue, HandlerId};
use futures::{FutureExt, future::join_all};
use parking_lot::Mutex;
use slotmap::SlotMap;
use std::{collections::HashMap, sync::Arc};

/// Result returned by an event handler.
#[derive(Clone, Default)]
pub struct EventOutcome(pub Option<EventValue>);

/// Consumed-on-call continuation. Rust prevents calling the same `Next` twice.
pub struct Next {
    handlers: Arc<[SnapshotHandler]>,
    index: usize,
    value: EventValue,
}

impl Next {
    /// Runs the next waterfall handler, consuming this continuation.
    pub async fn run(self) -> Result<EventOutcome, CordisError> {
        run_waterfall(self.handlers, self.index, self.value).await
    }
}

/// Async event handler usable by native or bridge adapters.
#[async_trait]
pub trait EventHandler: Send + Sync + 'static {
    /// Handles one event. Waterfall handlers receive a consumed continuation.
    async fn call(
        &self,
        value: EventValue,
        next: Option<Next>,
    ) -> Result<EventOutcome, CordisError>;
}

struct EventSlot {
    snapshot: ArcSwap<Vec<SnapshotHandler>>,
    /// Serializes copy-on-write publication for this event only. Dispatch never
    /// takes this lock and therefore never holds it across `.await`.
    writer: Mutex<()>,
}
#[derive(Clone)]
struct SnapshotHandler {
    id: HandlerId,
    handler: Arc<dyn EventHandler>,
    gate: Arc<CapabilityGate>,
}
#[allow(dead_code)]
struct HandlerEntry {
    event: EventKey,
    handler: Arc<dyn EventHandler>,
    gate: Arc<CapabilityGate>,
}

#[derive(Default)]
pub(crate) struct EventBus {
    slots: Mutex<HashMap<EventKey, Arc<EventSlot>>>,
    handlers: Mutex<SlotMap<HandlerId, HandlerEntry>>,
}

impl EventBus {
    fn slot(&self, key: &EventKey) -> Arc<EventSlot> {
        self.slots
            .lock()
            .entry(key.clone())
            .or_insert_with(|| {
                Arc::new(EventSlot {
                    snapshot: ArcSwap::from_pointee(Vec::new()),
                    writer: Mutex::new(()),
                })
            })
            .clone()
    }

    fn existing_slot(&self, key: &EventKey) -> Option<Arc<EventSlot>> {
        self.slots.lock().get(key).cloned()
    }

    #[allow(clippy::needless_pass_by_value)]
    #[allow(dead_code)]
    pub fn register(
        &self,
        event: EventKey,
        handler: Arc<dyn EventHandler>,
        active: bool,
    ) -> HandlerId {
        let gate = Arc::new(if active {
            CapabilityGate::published()
        } else {
            CapabilityGate::standalone_staged()
        });
        self.register_gated(event, handler, gate)
    }

    #[allow(clippy::needless_pass_by_value)]
    pub fn register_gated(
        &self,
        event: EventKey,
        handler: Arc<dyn EventHandler>,
        gate: Arc<CapabilityGate>,
    ) -> HandlerId {
        loop {
            let slot = self.slot(&event);
            let _writer = slot.writer.lock();
            let current = self
                .slots
                .lock()
                .get(&event)
                .is_some_and(|candidate| Arc::ptr_eq(candidate, &slot));
            if !current {
                continue;
            }
            // Arena mutation and snapshot publication share the slot writer, so a
            // completed update can never be overwritten by another RMW sequence.
            let id = self.handlers.lock().insert(HandlerEntry {
                event: event.clone(),
                handler: handler.clone(),
                gate: gate.clone(),
            });
            let mut next = (**slot.snapshot.load()).clone();
            next.push(SnapshotHandler {
                id,
                handler: handler.clone(),
                gate: gate.clone(),
            });
            slot.snapshot.store(Arc::new(next));
            return id;
        }
    }

    #[allow(dead_code)]
    pub fn activate(&self, id: HandlerId) -> bool {
        let event = {
            let handlers = self.handlers.lock();
            let Some(entry) = handlers.get(id) else {
                return false;
            };
            entry.event.clone()
        };
        loop {
            let slot = self.slot(&event);
            let _writer = slot.writer.lock();
            if !self
                .slots
                .lock()
                .get(&event)
                .is_some_and(|candidate| Arc::ptr_eq(candidate, &slot))
            {
                continue;
            }
            let (handler, gate) = {
                let mut handlers = self.handlers.lock();
                let Some(entry) = handlers.get_mut(id) else {
                    return false;
                };
                if entry.gate.is_visible() {
                    return true;
                }
                entry.gate.publish();
                (entry.handler.clone(), entry.gate.clone())
            };
            let mut next = (**slot.snapshot.load()).clone();
            if !next.iter().any(|entry| entry.id == id) {
                next.push(SnapshotHandler { id, handler, gate });
                slot.snapshot.store(Arc::new(next));
            }
            return true;
        }
    }

    pub fn remove(&self, id: HandlerId) -> bool {
        let event = {
            let handlers = self.handlers.lock();
            let Some(entry) = handlers.get(id) else {
                return false;
            };
            entry.event.clone()
        };
        let slot = self.slot(&event);
        let _writer = slot.writer.lock();
        let (_entry, event_is_unregistered) = {
            let mut handlers = self.handlers.lock();
            let Some(entry) = handlers.remove(id) else {
                return false;
            };
            let event_is_unregistered = !handlers.values().any(|item| item.event == event);
            (entry, event_is_unregistered)
        };
        let mut next = (**slot.snapshot.load()).clone();
        next.retain(|handler| handler.id != id);
        slot.snapshot.store(Arc::new(next));
        if event_is_unregistered && slot.snapshot.load().is_empty() {
            self.slots.lock().remove(&event);
        }
        true
    }

    pub async fn emit(&self, event: &EventKey, value: EventValue) -> Result<(), CordisError> {
        let Some(slot) = self.existing_slot(event) else {
            return Ok(());
        };
        let handlers = slot.snapshot.load_full();
        let mut visibility = VisibilityCache::default();
        let mut first_error = None;
        for handler in handlers.iter() {
            if !visibility.visible(&handler.gate) {
                continue;
            }
            let Some(_lease) = handler.gate.try_acquire() else {
                continue;
            };
            if let Err(error) = call_handler(&handler.handler, value.clone(), None).await {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    pub async fn serial(
        &self,
        event: &EventKey,
        value: EventValue,
    ) -> Result<Vec<EventOutcome>, CordisError> {
        let Some(slot) = self.existing_slot(event) else {
            return Ok(Vec::new());
        };
        let snapshot = slot.snapshot.load_full();
        let mut visibility = VisibilityCache::default();
        let mut outcomes = Vec::with_capacity(snapshot.len());
        for handler in snapshot.iter() {
            if !visibility.visible(&handler.gate) {
                continue;
            }
            let Some(_lease) = handler.gate.try_acquire() else {
                continue;
            };
            outcomes.push(call_handler(&handler.handler, value.clone(), None).await?);
        }
        Ok(outcomes)
    }

    pub async fn bail(
        &self,
        event: &EventKey,
        value: EventValue,
    ) -> Result<EventOutcome, CordisError> {
        let Some(slot) = self.existing_slot(event) else {
            return Ok(EventOutcome::default());
        };
        let snapshot = slot.snapshot.load_full();
        let mut visibility = VisibilityCache::default();
        for handler in snapshot.iter() {
            if !visibility.visible(&handler.gate) {
                continue;
            }
            let Some(_lease) = handler.gate.try_acquire() else {
                continue;
            };
            let result = call_handler(&handler.handler, value.clone(), None).await?;
            if result.0.is_some() {
                return Ok(result);
            }
        }
        Ok(EventOutcome::default())
    }

    pub async fn parallel(
        &self,
        event: &EventKey,
        value: EventValue,
    ) -> Result<Vec<EventOutcome>, CordisError> {
        let Some(slot) = self.existing_slot(event) else {
            return Ok(Vec::new());
        };
        let snapshot = slot.snapshot.load_full();
        let mut visibility = VisibilityCache::default();
        let admitted: Vec<_> = snapshot
            .iter()
            .filter(|handler| visibility.visible(&handler.gate))
            .filter_map(|handler| {
                handler
                    .gate
                    .try_acquire()
                    .map(|lease| (handler.handler.clone(), lease))
            })
            .collect();
        join_all(admitted.into_iter().map(|(handler, lease)| {
            let value = value.clone();
            async move {
                let _lease = lease;
                call_handler(&handler, value, None).await
            }
        }))
        .await
        .into_iter()
        .collect()
    }

    pub async fn waterfall(
        &self,
        event: &EventKey,
        value: EventValue,
    ) -> Result<EventOutcome, CordisError> {
        let Some(slot) = self.existing_slot(event) else {
            return Ok(EventOutcome(Some(value)));
        };
        let snapshot = slot.snapshot.load_full();
        let mut visibility = VisibilityCache::default();
        let handlers: Arc<[SnapshotHandler]> = snapshot
            .iter()
            .filter(|entry| visibility.visible(&entry.gate))
            .cloned()
            .collect::<Vec<_>>()
            .into();
        run_waterfall(handlers, 0, value).await
    }
}

async fn call_handler(
    handler: &Arc<dyn EventHandler>,
    value: EventValue,
    next: Option<Next>,
) -> Result<EventOutcome, CordisError> {
    std::panic::AssertUnwindSafe(handler.call(value, next))
        .catch_unwind()
        .await
        .map_err(|payload| {
            CordisError::EventHandlerPanicked(crate::runtime::panic_message(payload.as_ref()))
        })?
}

fn run_waterfall(
    handlers: Arc<[SnapshotHandler]>,
    index: usize,
    value: EventValue,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<EventOutcome, CordisError>> + Send>>
{
    Box::pin(async move {
        let Some(handler) = handlers.get(index).cloned() else {
            return Ok(EventOutcome(Some(value)));
        };
        let Some(_lease) = handler.gate.try_acquire() else {
            return run_waterfall(handlers, index + 1, value).await;
        };
        let next = Next {
            handlers,
            index: index + 1,
            value: value.clone(),
        };
        call_handler(&handler.handler, value, Some(next)).await
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gate::{DrainOutcome, GenerationExecutionState};
    use std::{
        collections::HashSet,
        sync::{
            Barrier,
            atomic::{AtomicUsize, Ordering},
        },
    };

    struct CountingHandler(Arc<AtomicUsize>);
    struct CutoverHandler {
        count: Arc<AtomicUsize>,
        new: Arc<CapabilityGate>,
        old: Arc<CapabilityGate>,
    }
    struct BlockingHandler {
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Semaphore>,
    }
    struct PanicHandler;
    #[async_trait]
    impl EventHandler for PanicHandler {
        async fn call(
            &self,
            _value: EventValue,
            _next: Option<Next>,
        ) -> Result<EventOutcome, CordisError> {
            panic!("expected event panic");
        }
    }
    #[async_trait]
    impl EventHandler for BlockingHandler {
        async fn call(
            &self,
            _value: EventValue,
            _next: Option<Next>,
        ) -> Result<EventOutcome, CordisError> {
            self.entered.notify_one();
            let _ = self.release.acquire().await;
            Ok(EventOutcome::default())
        }
    }

    #[async_trait]
    impl EventHandler for CountingHandler {
        async fn call(
            &self,
            _value: EventValue,
            _next: Option<Next>,
        ) -> Result<EventOutcome, CordisError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(EventOutcome::default())
        }
    }

    #[async_trait]
    impl EventHandler for CutoverHandler {
        async fn call(
            &self,
            _value: EventValue,
            _next: Option<Next>,
        ) -> Result<EventOutcome, CordisError> {
            self.count.fetch_add(1, Ordering::SeqCst);
            assert!(self.new.cutover_from(&self.old));
            Ok(EventOutcome::default())
        }
    }

    #[tokio::test]
    async fn event_cutover_never_observes_old_and_new_together() {
        use crate::{gate::GenerationSelector, plugin_registry::GenerationId};
        let selector = Arc::new(GenerationSelector::default());
        let old = Arc::new(CapabilityGate::staged(selector.clone(), GenerationId(1)));
        let new = Arc::new(CapabilityGate::staged(selector, GenerationId(2)));
        assert!(old.publish());
        let bus = EventBus::default();
        let event = EventKey("generation-cutover".into());
        let old_count = Arc::new(AtomicUsize::new(0));
        let new_count = Arc::new(AtomicUsize::new(0));
        bus.register_gated(
            event.clone(),
            Arc::new(CutoverHandler {
                count: old_count.clone(),
                new: new.clone(),
                old: old.clone(),
            }),
            old,
        );
        bus.register_gated(
            event.clone(),
            Arc::new(CountingHandler(new_count.clone())),
            new,
        );
        bus.emit(&event, Arc::new(()))
            .await
            .expect("cutover dispatch");
        assert_eq!(
            (
                old_count.load(Ordering::SeqCst),
                new_count.load(Ordering::SeqCst)
            ),
            (1, 0)
        );
        bus.emit(&event, Arc::new(())).await.expect("new dispatch");
        assert_eq!(
            (
                old_count.load(Ordering::SeqCst),
                new_count.load(Ordering::SeqCst)
            ),
            (1, 1)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn event_handler_holds_generation_lease_during_execution() {
        let bus = Arc::new(EventBus::default());
        let event = EventKey("leased-event".into());
        let gate = Arc::new(CapabilityGate::published());
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        bus.register_gated(
            event.clone(),
            Arc::new(BlockingHandler {
                entered: entered.clone(),
                release: release.clone(),
            }),
            gate.clone(),
        );
        let emitting = {
            let bus = bus.clone();
            let event = event.clone();
            tokio::spawn(async move { bus.emit(&event, Arc::new(())).await })
        };
        entered.notified().await;
        gate.close();
        assert_eq!(
            gate.execution_snapshot(),
            (GenerationExecutionState::Draining, 1)
        );
        release.add_permits(1);
        emitting.await.expect("join").expect("emit");
        assert_eq!(
            gate.drain_until(tokio::time::Instant::now() + std::time::Duration::from_secs(1))
                .await,
            DrainOutcome::Drained
        );
    }

    #[tokio::test]
    async fn event_snapshot_that_loses_execution_admission_skips_old_handler() {
        let count = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(CapabilityGate::published());
        let selected = SnapshotHandler {
            id: HandlerId::default(),
            handler: Arc::new(CountingHandler(count.clone())),
            gate: gate.clone(),
        };
        let mut visibility = VisibilityCache::default();
        assert!(visibility.visible(&selected.gate));
        gate.close();
        if let Some(_lease) = selected.gate.try_acquire() {
            let _ = call_handler(&selected.handler, Arc::new(()), None).await;
        }
        assert_eq!(count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn event_handler_panic_releases_generation_lease() {
        let bus = EventBus::default();
        let event = EventKey("panic-lease".into());
        let gate = Arc::new(CapabilityGate::published());
        bus.register_gated(event.clone(), Arc::new(PanicHandler), gate.clone());
        assert!(matches!(
            bus.emit(&event, Arc::new(())).await,
            Err(CordisError::EventHandlerPanicked(_))
        ));
        assert_eq!(
            gate.execution_snapshot(),
            (GenerationExecutionState::Accepting, 0)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_registration_preserves_every_handler_once() {
        for round in 0..8 {
            let bus = Arc::new(EventBus::default());
            let event = EventKey(format!("register-{round}").into());
            let counters: Vec<_> = (0..32).map(|_| Arc::new(AtomicUsize::new(0))).collect();
            std::thread::scope(|scope| {
                for counter in &counters {
                    let bus = bus.clone();
                    let event = event.clone();
                    let counter = counter.clone();
                    scope.spawn(move || {
                        bus.register(event, Arc::new(CountingHandler(counter)), true);
                    });
                }
            });
            bus.emit(&event, Arc::new(())).await.expect("emit");
            assert!(
                counters
                    .iter()
                    .all(|counter| counter.load(Ordering::SeqCst) == 1)
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_activate_remove_and_register_stays_consistent() {
        let bus = Arc::new(EventBus::default());
        let event = EventKey("activate-remove".into());
        let removed_counters: Vec<_> = (0..32).map(|_| Arc::new(AtomicUsize::new(0))).collect();
        let ids: Vec<_> = removed_counters
            .iter()
            .map(|counter| {
                bus.register(
                    event.clone(),
                    Arc::new(CountingHandler(counter.clone())),
                    false,
                )
            })
            .collect();
        let survivor_counters: Vec<_> = (0..32).map(|_| Arc::new(AtomicUsize::new(0))).collect();
        let barrier = Arc::new(Barrier::new(ids.len() * 2 + survivor_counters.len()));

        std::thread::scope(|scope| {
            for id in &ids {
                let activate_bus = bus.clone();
                let activate_barrier = barrier.clone();
                let id = *id;
                scope.spawn(move || {
                    activate_barrier.wait();
                    activate_bus.activate(id);
                });
                let remove_bus = bus.clone();
                let remove_barrier = barrier.clone();
                scope.spawn(move || {
                    remove_barrier.wait();
                    remove_bus.remove(id);
                });
            }
            for counter in &survivor_counters {
                let bus = bus.clone();
                let barrier = barrier.clone();
                let event = event.clone();
                let counter = counter.clone();
                scope.spawn(move || {
                    barrier.wait();
                    bus.register(event, Arc::new(CountingHandler(counter)), true);
                });
            }
        });

        assert!(ids.iter().all(|id| bus.handlers.lock().get(*id).is_none()));
        bus.emit(&event, Arc::new(())).await.expect("emit");
        assert!(
            removed_counters
                .iter()
                .all(|counter| counter.load(Ordering::SeqCst) == 0)
        );
        assert!(
            survivor_counters
                .iter()
                .all(|counter| counter.load(Ordering::SeqCst) == 1)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn removed_handler_is_not_resurrected_by_concurrent_publication() {
        for round in 0..8 {
            let bus = Arc::new(EventBus::default());
            let event = EventKey(format!("remove-{round}").into());
            let victim = Arc::new(AtomicUsize::new(0));
            let victim_id = bus.register(
                event.clone(),
                Arc::new(CountingHandler(victim.clone())),
                true,
            );
            let additions: Vec<_> = (0..32).map(|_| Arc::new(AtomicUsize::new(0))).collect();
            let barrier = Arc::new(Barrier::new(additions.len() + 1));
            std::thread::scope(|scope| {
                let remove_bus = bus.clone();
                let remove_barrier = barrier.clone();
                scope.spawn(move || {
                    remove_barrier.wait();
                    assert!(remove_bus.remove(victim_id));
                });
                for counter in &additions {
                    let bus = bus.clone();
                    let barrier = barrier.clone();
                    let event = event.clone();
                    let counter = counter.clone();
                    scope.spawn(move || {
                        barrier.wait();
                        bus.register(event, Arc::new(CountingHandler(counter)), true);
                    });
                }
            });
            bus.emit(&event, Arc::new(())).await.expect("emit");
            assert_eq!(victim.load(Ordering::SeqCst), 0);
            assert!(
                additions
                    .iter()
                    .all(|counter| counter.load(Ordering::SeqCst) == 1)
            );
        }
    }

    #[tokio::test]
    async fn duplicate_arc_registrations_are_removed_by_handler_id() {
        let bus = EventBus::default();
        let event = EventKey("duplicate-arc".into());
        let counter = Arc::new(AtomicUsize::new(0));
        let handler: Arc<dyn EventHandler> = Arc::new(CountingHandler(counter.clone()));
        let first = bus.register(event.clone(), handler.clone(), true);
        let second = bus.register(event.clone(), handler, true);

        bus.emit(&event, Arc::new(())).await.expect("first emit");
        assert_eq!(counter.load(Ordering::SeqCst), 2);
        assert!(bus.remove(first));
        bus.emit(&event, Arc::new(())).await.expect("second emit");
        assert_eq!(counter.load(Ordering::SeqCst), 3);
        assert!(bus.handlers.lock().get(second).is_some());
        assert_eq!(bus.slot(&event).snapshot.load().len(), 1);

        assert!(bus.remove(second));
        bus.emit(&event, Arc::new(())).await.expect("third emit");
        assert_eq!(counter.load(Ordering::SeqCst), 3);
        assert!(bus.handlers.lock().is_empty());
        assert!(bus.existing_slot(&event).is_none());
    }

    struct PanickingHandler;

    #[async_trait]
    impl EventHandler for PanickingHandler {
        async fn call(
            &self,
            _value: EventValue,
            _next: Option<Next>,
        ) -> Result<EventOutcome, CordisError> {
            panic!("event panic")
        }
    }

    #[tokio::test]
    async fn handler_panic_is_converted_and_missing_event_does_not_allocate_slot() {
        let bus = EventBus::default();
        let missing = EventKey("missing".into());
        bus.emit(&missing, Arc::new(())).await.expect("empty emit");
        assert!(bus.existing_slot(&missing).is_none());

        let event = EventKey("panic".into());
        bus.register(event.clone(), Arc::new(PanickingHandler), true);
        assert!(matches!(
            bus.emit(&event, Arc::new(())).await,
            Err(CordisError::EventHandlerPanicked(message)) if message == "event panic"
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_duplicate_arc_updates_keep_arena_and_snapshot_consistent() {
        for round in 0..16 {
            let bus = Arc::new(EventBus::default());
            let event = EventKey(format!("duplicate-race-{round}").into());
            let counter = Arc::new(AtomicUsize::new(0));
            let handler: Arc<dyn EventHandler> = Arc::new(CountingHandler(counter.clone()));
            let ids: Vec<_> = (0..32)
                .map(|_| bus.register(event.clone(), handler.clone(), false))
                .collect();
            let barrier = Arc::new(Barrier::new(ids.len() * 2));
            std::thread::scope(|scope| {
                for id in &ids {
                    let activate_bus = bus.clone();
                    let activate_barrier = barrier.clone();
                    let id = *id;
                    scope.spawn(move || {
                        activate_barrier.wait();
                        activate_bus.activate(id);
                    });
                    let remove_bus = bus.clone();
                    let remove_barrier = barrier.clone();
                    scope.spawn(move || {
                        remove_barrier.wait();
                        remove_bus.remove(id);
                    });
                }
            });

            let arena_ids: HashSet<_> = bus
                .handlers
                .lock()
                .iter()
                .filter_map(|(id, entry)| entry.gate.is_visible().then_some(id))
                .collect();
            let snapshot_ids: HashSet<_> = bus
                .slot(&event)
                .snapshot
                .load()
                .iter()
                .map(|entry| entry.id)
                .collect();
            assert_eq!(arena_ids, snapshot_ids);
            assert!(arena_ids.is_empty());
            bus.emit(&event, Arc::new(())).await.expect("emit");
            assert_eq!(counter.load(Ordering::SeqCst), 0);
        }
    }
}
