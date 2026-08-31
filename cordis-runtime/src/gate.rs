use crate::plugin_registry::GenerationId;
use cordis_core::CordisError;
use parking_lot::{RwLock, RwLockReadGuard};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU8, Ordering},
};
use tokio::{sync::Notify, time::Instant};

const RUNNING: u8 = 0;
const SHUTTING_DOWN: u8 = 1;
const SHUTDOWN: u8 = 2;

pub(crate) struct GenerationSelector(AtomicU64);

impl Default for GenerationSelector {
    fn default() -> Self {
        Self(AtomicU64::new(GenerationId::NONE.get()))
    }
}
impl GenerationSelector {
    pub(crate) fn active(&self) -> GenerationId {
        GenerationId(self.0.load(Ordering::Acquire))
    }
    pub(crate) fn switch(&self, expected: GenerationId, next: GenerationId) -> bool {
        // AcqRel publishes prepared registry metadata; Acquire observes a competing winner.
        self.0
            .compare_exchange(
                expected.get(),
                next.get(),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }
}

pub(crate) struct AdmissionGate {
    state: AtomicU8,
    linearization: RwLock<()>,
}

impl Default for AdmissionGate {
    fn default() -> Self {
        Self {
            state: AtomicU8::new(RUNNING),
            linearization: RwLock::new(()),
        }
    }
}

impl AdmissionGate {
    pub fn enter(&self) -> Result<RwLockReadGuard<'_, ()>, CordisError> {
        let guard = self.linearization.read();
        if self.state.load(Ordering::Acquire) == RUNNING {
            Ok(guard)
        } else {
            Err(CordisError::RuntimeShuttingDown)
        }
    }

    pub fn begin_shutdown(&self) -> bool {
        let _guard = self.linearization.write();
        self.state
            .compare_exchange(RUNNING, SHUTTING_DOWN, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    pub fn complete_shutdown(&self) {
        self.state.store(SHUTDOWN, Ordering::Release);
    }
}

pub(crate) struct CapabilityGate {
    selector: Arc<GenerationSelector>,
    generation: GenerationId,
    retired: AtomicBool,
    execution: Arc<GenerationExecution>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GenerationExecutionState {
    Accepting,
    Draining,
    Drained,
}

// Independent generations are frequently allocated together during topology setup. Keep their
// admission counters from sharing a cache line after replacing the larger mutex-backed state.
#[repr(align(64))]
pub(crate) struct GenerationExecution {
    word: AtomicUsize,
    service_handles: AtomicUsize,
    zero: Notify,
}

const EXECUTION_STATE_BITS: usize = 2;
const EXECUTION_STATE_MASK: usize = (1 << EXECUTION_STATE_BITS) - 1;
const EXECUTION_INFLIGHT_ONE: usize = 1 << EXECUTION_STATE_BITS;
const EXECUTION_MAX_INFLIGHT: usize = usize::MAX >> EXECUTION_STATE_BITS;
const EXECUTION_ACCEPTING: usize = 0;
const EXECUTION_DRAINING: usize = 1;
const EXECUTION_DRAINED: usize = 2;

const fn execution_state(word: usize) -> GenerationExecutionState {
    match word & EXECUTION_STATE_MASK {
        EXECUTION_ACCEPTING => GenerationExecutionState::Accepting,
        EXECUTION_DRAINING => GenerationExecutionState::Draining,
        EXECUTION_DRAINED => GenerationExecutionState::Drained,
        _ => panic!("reserved generation execution state"),
    }
}

const fn execution_inflight(word: usize) -> usize {
    word >> EXECUTION_STATE_BITS
}

const fn execution_word(state: usize, inflight: usize) -> usize {
    (inflight << EXECUTION_STATE_BITS) | state
}

impl Default for GenerationExecution {
    fn default() -> Self {
        Self {
            word: AtomicUsize::new(EXECUTION_ACCEPTING),
            service_handles: AtomicUsize::new(0),
            zero: Notify::new(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DrainOutcome {
    Drained,
    TimedOut { remaining: usize },
}

pub(crate) struct GenerationLease {
    execution: Arc<GenerationExecution>,
    service_handle: bool,
}

impl Drop for GenerationLease {
    fn drop(&mut self) {
        if self.service_handle {
            let previous = self
                .execution
                .service_handles
                .fetch_sub(1, Ordering::Release);
            assert!(previous != 0, "service handle lease underflow");
        }
        self.execution.drop_inflight();
    }
}

impl GenerationExecution {
    pub(crate) fn try_acquire(self: &Arc<Self>) -> Option<GenerationLease> {
        self.try_acquire_kind(false)
    }

    fn try_acquire_kind(self: &Arc<Self>, service_handle: bool) -> Option<GenerationLease> {
        let mut current = self.word.load(Ordering::Acquire);
        loop {
            if execution_state(current) != GenerationExecutionState::Accepting {
                return None;
            }
            let inflight = execution_inflight(current);
            assert!(
                inflight < EXECUTION_MAX_INFLIGHT,
                "generation inflight counter overflow"
            );
            let next = current + EXECUTION_INFLIGHT_ONE;
            match self.word.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
        if service_handle {
            self.service_handles
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                    value.checked_add(1)
                })
                .expect("service handle counter overflow");
        }
        Some(GenerationLease {
            execution: self.clone(),
            service_handle,
        })
    }

    pub(crate) fn begin_draining(&self) {
        let mut current = self.word.load(Ordering::Acquire);
        loop {
            if execution_state(current) != GenerationExecutionState::Accepting {
                return;
            }
            let inflight = execution_inflight(current);
            let next = if inflight == 0 {
                execution_word(EXECUTION_DRAINED, 0)
            } else {
                execution_word(EXECUTION_DRAINING, inflight)
            };
            match self.word.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    if inflight == 0 {
                        self.zero.notify_waiters();
                    }
                    return;
                }
                Err(actual) => current = actual,
            }
        }
    }

    pub(crate) async fn drain_until(&self, deadline: Instant) -> DrainOutcome {
        self.begin_draining();
        loop {
            let notified = self.zero.notified();
            let word = self.word.load(Ordering::Acquire);
            let remaining = execution_inflight(word);
            if remaining == 0 {
                debug_assert_eq!(execution_state(word), GenerationExecutionState::Drained);
                return DrainOutcome::Drained;
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return DrainOutcome::TimedOut { remaining };
            }
        }
    }

    pub(crate) fn snapshot(&self) -> (GenerationExecutionState, usize) {
        let word = self.word.load(Ordering::Acquire);
        (execution_state(word), execution_inflight(word))
    }

    pub(crate) fn service_handle_inflight(&self) -> usize {
        self.service_handles.load(Ordering::Acquire)
    }

    fn drop_inflight(&self) {
        let mut current = self.word.load(Ordering::Acquire);
        loop {
            let state = execution_state(current);
            let inflight = execution_inflight(current);
            assert!(inflight != 0, "generation lease underflow");
            debug_assert!(state != GenerationExecutionState::Drained);
            let remaining = inflight - 1;
            let next_state = if state == GenerationExecutionState::Draining && remaining == 0 {
                EXECUTION_DRAINED
            } else {
                current & EXECUTION_STATE_MASK
            };
            let next = execution_word(next_state, remaining);
            match self.word.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    if next_state == EXECUTION_DRAINED {
                        self.zero.notify_waiters();
                    }
                    return;
                }
                Err(actual) => current = actual,
            }
        }
    }
}

#[derive(Default)]
pub(crate) struct VisibilityCache(HashMap<usize, GenerationId>);

impl VisibilityCache {
    pub(crate) fn visible(&mut self, gate: &CapabilityGate) -> bool {
        let key = Arc::as_ptr(&gate.selector) as usize;
        let active = *self.0.entry(key).or_insert_with(|| gate.selector.active());
        active == gate.generation
    }
}

impl CapabilityGate {
    pub fn staged(selector: Arc<GenerationSelector>, generation: GenerationId) -> Self {
        Self {
            selector,
            generation,
            retired: AtomicBool::new(false),
            execution: Arc::new(GenerationExecution::default()),
        }
    }

    #[allow(dead_code)]
    pub fn published() -> Self {
        let selector = Arc::new(GenerationSelector::default());
        let generation = GenerationId(1);
        assert!(selector.switch(GenerationId::NONE, generation));
        Self::staged(selector, generation)
    }
    pub fn standalone_staged() -> Self {
        Self::staged(Arc::new(GenerationSelector::default()), GenerationId(1))
    }

    pub fn publish(&self) -> bool {
        !self.retired.load(Ordering::Acquire)
            && self.selector.switch(GenerationId::NONE, self.generation)
    }

    pub fn is_staged(&self) -> bool {
        !self.retired.load(Ordering::Acquire) && self.selector.active() != self.generation
    }

    pub fn close(&self) {
        self.retired.store(true, Ordering::Release);
        let _ = self.selector.switch(self.generation, GenerationId::NONE);
        self.execution.begin_draining();
    }

    pub fn is_visible(&self) -> bool {
        self.selector.active() == self.generation
    }

    pub(crate) fn cutover_from(&self, old: &Self) -> bool {
        if self.retired.load(Ordering::Acquire) || !Arc::ptr_eq(&self.selector, &old.selector) {
            return false;
        }
        if !self.selector.switch(old.generation, self.generation) {
            return false;
        }
        old.retired.store(true, Ordering::Release);
        old.execution.begin_draining();
        true
    }

    pub(crate) fn try_acquire(&self) -> Option<GenerationLease> {
        self.execution.try_acquire()
    }
    pub(crate) fn try_acquire_service(&self) -> Option<GenerationLease> {
        self.execution.try_acquire_kind(true)
    }
    pub(crate) async fn drain_until(&self, deadline: Instant) -> DrainOutcome {
        self.execution.drain_until(deadline).await
    }
    pub(crate) fn execution_snapshot(&self) -> (GenerationExecutionState, usize) {
        self.execution.snapshot()
    }
    pub(crate) fn service_handle_inflight(&self) -> usize {
        self.execution.service_handle_inflight()
    }

    pub(crate) const fn generation_id(&self) -> GenerationId {
        self.generation
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn closed_capability_gate_cannot_be_republished() {
        let gate = CapabilityGate::standalone_staged();
        gate.close();
        assert!(!gate.publish());
        assert!(!gate.is_visible());
    }

    #[test]
    fn generation_selector_switches_old_to_new_atomically() {
        let selector = Arc::new(GenerationSelector::default());
        let old = CapabilityGate::staged(selector.clone(), GenerationId(1));
        let new = CapabilityGate::staged(selector, GenerationId(2));
        assert!(old.publish());
        let mut before = VisibilityCache::default();
        assert!(before.visible(&old));
        assert!(new.cutover_from(&old));
        assert!(!before.visible(&new));
        let mut after = VisibilityCache::default();
        assert!(!after.visible(&old));
        assert!(after.visible(&new));
    }

    #[test]
    fn failed_generation_cas_preserves_old_generation() {
        let selector = Arc::new(GenerationSelector::default());
        let old = CapabilityGate::staged(selector.clone(), GenerationId(1));
        let loser = CapabilityGate::staged(selector, GenerationId(2));
        assert!(old.publish());
        assert!(!loser.publish());
        assert!(old.is_visible());
        assert!(!loser.is_visible());
    }

    #[test]
    fn capability_visibility_depends_only_on_selector_generation() {
        let selector = Arc::new(GenerationSelector::default());
        let gate = CapabilityGate::staged(selector, GenerationId(1));
        assert!(gate.publish());
        gate.retired.store(true, Ordering::Release);
        assert!(gate.is_visible());
        let mut cache = VisibilityCache::default();
        assert!(cache.visible(&gate));
    }

    #[test]
    fn retired_bit_does_not_create_cross_registry_visibility_split() {
        let selector = Arc::new(GenerationSelector::default());
        let old = CapabilityGate::staged(selector.clone(), GenerationId(1));
        let new = CapabilityGate::staged(selector, GenerationId(2));
        assert!(old.publish());
        old.retired.store(true, Ordering::Release);
        assert!(old.is_visible());
        assert!(new.cutover_from(&old));
        assert!(!old.is_visible());
        assert!(new.is_visible());
    }

    #[tokio::test]
    async fn generation_execution_lease_and_drain_are_linearized() {
        let execution = Arc::new(GenerationExecution::default());
        let lease = execution.try_acquire().expect("lease");
        assert_eq!(
            execution.snapshot(),
            (GenerationExecutionState::Accepting, 1)
        );
        execution.begin_draining();
        assert!(execution.try_acquire().is_none());
        assert_eq!(
            execution.snapshot(),
            (GenerationExecutionState::Draining, 1)
        );
        drop(lease);
        assert_eq!(
            execution
                .drain_until(Instant::now() + std::time::Duration::from_secs(1))
                .await,
            DrainOutcome::Drained
        );
        assert_eq!(execution.snapshot(), (GenerationExecutionState::Drained, 0));
        assert!(execution.try_acquire().is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn drain_waits_for_existing_lease_and_last_drop_wakes_without_lost_wakeup() {
        let execution = Arc::new(GenerationExecution::default());
        let lease = execution.try_acquire().expect("lease");
        let draining = execution.clone();
        let waiter = tokio::spawn(async move {
            draining
                .drain_until(Instant::now() + std::time::Duration::from_secs(1))
                .await
        });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());
        drop(lease);
        assert_eq!(waiter.await.expect("join"), DrainOutcome::Drained);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn last_drop_vs_waiter_registration_has_no_lost_wakeup_stress() {
        for _ in 0..1_000 {
            let execution = Arc::new(GenerationExecution::default());
            let lease = execution.try_acquire().expect("lease");
            execution.begin_draining();
            let draining = execution.clone();
            let waiter = tokio::spawn(async move {
                draining
                    .drain_until(Instant::now() + std::time::Duration::from_secs(1))
                    .await
            });
            drop(lease);
            assert_eq!(waiter.await.expect("join"), DrainOutcome::Drained);
            assert_eq!(execution.snapshot(), (GenerationExecutionState::Drained, 0));
        }
    }

    fn concurrent_drain_rejects_late_acquires(service: bool) {
        const WORKERS: usize = 32;
        let execution = Arc::new(GenerationExecution::default());
        let gate = Arc::new(CapabilityGate::published());
        let acquired = Arc::new(std::sync::Barrier::new(WORKERS + 1));
        let release = Arc::new(std::sync::Barrier::new(WORKERS + 1));
        std::thread::scope(|threads| {
            for _ in 0..WORKERS {
                let execution = execution.clone();
                let gate = gate.clone();
                let acquired = acquired.clone();
                let release = release.clone();
                threads.spawn(move || {
                    let lease = if service {
                        gate.try_acquire_service().expect("service lease")
                    } else {
                        execution.try_acquire().expect("lease")
                    };
                    acquired.wait();
                    release.wait();
                    drop(lease);
                });
            }
            acquired.wait();
            if service {
                gate.execution.begin_draining();
                assert!(gate.try_acquire_service().is_none());
                assert_eq!(gate.execution_snapshot().1, WORKERS);
                assert_eq!(gate.service_handle_inflight(), WORKERS);
            } else {
                execution.begin_draining();
                assert!(execution.try_acquire().is_none());
                assert_eq!(execution.snapshot().1, WORKERS);
            }
            release.wait();
        });
        if service {
            assert_eq!(
                gate.execution_snapshot(),
                (GenerationExecutionState::Drained, 0)
            );
            assert_eq!(gate.service_handle_inflight(), 0);
            assert!(gate.try_acquire_service().is_none());
        } else {
            assert_eq!(execution.snapshot(), (GenerationExecutionState::Drained, 0));
            assert!(execution.try_acquire().is_none());
        }
    }

    #[test]
    fn high_concurrency_normal_drain_has_one_winner_and_converges() {
        concurrent_drain_rejects_late_acquires(false);
    }

    #[test]
    fn high_concurrency_service_drain_accounts_exactly_once_and_converges() {
        concurrent_drain_rejects_late_acquires(true);
    }

    #[test]
    fn packed_execution_encoding_preserves_state_and_counter() {
        for state in [EXECUTION_ACCEPTING, EXECUTION_DRAINING, EXECUTION_DRAINED] {
            for inflight in [0, 1, 2, 31, 1_024, EXECUTION_MAX_INFLIGHT] {
                let word = execution_word(state, inflight);
                assert_eq!(execution_inflight(word), inflight);
                assert_eq!(word & EXECUTION_STATE_MASK, state);
            }
        }
    }

    #[tokio::test]
    async fn drain_timeout_reports_remaining_and_never_reopens() {
        let execution = Arc::new(GenerationExecution::default());
        let _lease = execution.try_acquire().expect("lease");
        let outcome = execution.drain_until(Instant::now()).await;
        assert_eq!(outcome, DrainOutcome::TimedOut { remaining: 1 });
        assert!(execution.try_acquire().is_none());
    }

    #[test]
    fn new_generation_accepts_while_old_generation_is_draining() {
        let selector = Arc::new(GenerationSelector::default());
        let old = CapabilityGate::staged(selector.clone(), GenerationId(1));
        let new = CapabilityGate::staged(selector, GenerationId(2));
        assert!(old.publish());
        let old_lease = old.try_acquire().expect("old lease");
        assert!(new.cutover_from(&old));
        assert_eq!(
            old.execution_snapshot(),
            (GenerationExecutionState::Draining, 1)
        );
        assert!(old.try_acquire().is_none());
        assert!(new.try_acquire().is_some());
        drop(old_lease);
        assert_eq!(
            old.execution_snapshot(),
            (GenerationExecutionState::Drained, 0)
        );
    }
}
