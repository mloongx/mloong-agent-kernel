use crate::{
    fiber_registry::FiberCell,
    health::{Diagnostics, HealthIssue, HealthIssueKind},
};
use cordis_core::{CordisError, FiberId, TaskId};
use futures::{FutureExt, StreamExt, stream::FuturesUnordered};
use parking_lot::Mutex;
use slotmap::SlotMap;
use std::{
    collections::HashMap,
    future::Future,
    panic::AssertUnwindSafe,
    sync::{
        Arc, Weak,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{oneshot, watch},
    task::JoinHandle,
    time::Instant,
};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TaskOutcome {
    Completed,
    Cancelled,
    Panicked,
    Aborted,
    Errored,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeWorkerKind {
    Install,
    DependencyReconcile,
    #[cfg(test)]
    ActivationRollback,
    ReloadTransaction,
    GcReconcile,
    HostFailureConvergence,
}

struct RuntimeWorker {
    kind: RuntimeWorkerKind,
    token: CancellationToken,
    cleanup: bool,
    handle: JoinHandle<TaskOutcome>,
}

pub(crate) struct RuntimeWorkerSupervisor {
    workers: Mutex<HashMap<u64, RuntimeWorker>>,
    next: AtomicU64,
    diagnostics: Arc<Diagnostics>,
    reaped: AtomicU64,
    panicked: AtomicU64,
    errors: AtomicU64,
    cancelled: AtomicU64,
    aborted: AtomicU64,
}

impl RuntimeWorkerSupervisor {
    pub(crate) fn new(diagnostics: Arc<Diagnostics>) -> Arc<Self> {
        Arc::new(Self {
            workers: Mutex::new(HashMap::new()),
            next: AtomicU64::new(1),
            diagnostics,
            reaped: AtomicU64::new(0),
            panicked: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            cancelled: AtomicU64::new(0),
            aborted: AtomicU64::new(0),
        })
    }

    pub(crate) fn spawn<F>(self: &Arc<Self>, kind: RuntimeWorkerKind, cleanup: bool, future: F)
    where
        F: Future<Output = Result<(), CordisError>> + Send + 'static,
    {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        let token = CancellationToken::new();
        let child = token.clone();
        let supervisor = self.clone();
        let (start_tx, start_rx) = oneshot::channel();
        let handle = tokio::spawn(async move {
            if start_rx.await.is_err() {
                return TaskOutcome::Cancelled;
            }
            let result = AssertUnwindSafe(async {
                if cleanup { future.await.map(Some) } else {
                    tokio::select! { () = child.cancelled() => Ok(None), result = future => result.map(Some) }
                }
            }).catch_unwind().await;
            let outcome = match result {
                Ok(Ok(Some(()))) => TaskOutcome::Completed,
                Ok(Ok(None)) => TaskOutcome::Cancelled,
                Ok(Err(_)) => TaskOutcome::Errored,
                Err(_) => TaskOutcome::Panicked,
            };
            supervisor.complete_worker(id, kind, outcome);
            outcome
        });
        self.workers.lock().insert(
            id,
            RuntimeWorker {
                kind,
                token,
                cleanup,
                handle,
            },
        );
        let _ = start_tx.send(());
    }

    fn complete_worker(&self, id: u64, kind: RuntimeWorkerKind, outcome: TaskOutcome) {
        if self.workers.lock().remove(&id).is_none() {
            return;
        }
        self.record_worker_outcome(kind, outcome);
    }

    fn record_worker_outcome(&self, _kind: RuntimeWorkerKind, outcome: TaskOutcome) {
        self.reaped.fetch_add(1, Ordering::Relaxed);
        let kind = match outcome {
            TaskOutcome::Panicked => {
                self.panicked.fetch_add(1, Ordering::Relaxed);
                Some(HealthIssueKind::RuntimeWorkerPanic)
            }
            TaskOutcome::Errored => {
                self.errors.fetch_add(1, Ordering::Relaxed);
                Some(HealthIssueKind::RuntimeWorkerError)
            }
            TaskOutcome::Cancelled => {
                self.cancelled.fetch_add(1, Ordering::Relaxed);
                None
            }
            TaskOutcome::Aborted => {
                self.aborted.fetch_add(1, Ordering::Relaxed);
                None
            }
            TaskOutcome::Completed => None,
        };
        if let Some(kind) = kind {
            self.diagnostics.push(HealthIssue {
                at: std::time::SystemTime::now(),
                kind,
                scope: None,
                fiber: None,
                invocation: None,
            });
        }
    }

    pub(crate) async fn shutdown_until(&self, deadline: Instant) {
        let workers: Vec<_> = self
            .workers
            .lock()
            .drain()
            .map(|(_, worker)| worker)
            .collect();
        for worker in &workers {
            if !worker.cleanup {
                worker.token.cancel();
            }
        }
        let aborts: Vec<_> = workers
            .iter()
            .map(|worker| worker.handle.abort_handle())
            .collect();
        let mut joins = FuturesUnordered::new();
        for worker in workers {
            joins.push(async move { (worker.kind, worker.handle.await) });
        }
        loop {
            match tokio::time::timeout_at(deadline, joins.next()).await {
                Ok(Some((kind, Ok(outcome)))) => self.record_worker_outcome(kind, outcome),
                Ok(Some((kind, Err(error)))) => self.record_worker_outcome(
                    kind,
                    if error.is_panic() {
                        TaskOutcome::Panicked
                    } else {
                        TaskOutcome::Aborted
                    },
                ),
                Ok(None) => break,
                Err(_) => {
                    for abort in &aborts {
                        abort.abort();
                    }
                    while let Some((kind, result)) = joins.next().await {
                        let outcome = match result {
                            Ok(outcome) => outcome,
                            Err(error) if error.is_panic() => TaskOutcome::Panicked,
                            Err(_) => TaskOutcome::Aborted,
                        };
                        self.record_worker_outcome(kind, outcome);
                    }
                    break;
                }
            }
        }
    }

    pub(crate) fn live(&self) -> usize {
        self.workers.lock().len()
    }
    pub(crate) fn reaped(&self) -> u64 {
        self.reaped.load(Ordering::Relaxed)
    }
    pub(crate) fn panicked(&self) -> u64 {
        self.panicked.load(Ordering::Relaxed)
    }
    pub(crate) fn errors(&self) -> u64 {
        self.errors.load(Ordering::Relaxed)
    }
    pub(crate) fn cancelled(&self) -> u64 {
        self.cancelled.load(Ordering::Relaxed)
    }
    pub(crate) fn aborted(&self) -> u64 {
        self.aborted.load(Ordering::Relaxed)
    }
}

struct OwnedTask {
    id: TaskId,
    owner_id: FiberId,
    scope_id: cordis_core::ScopeId,
    owner: Weak<FiberCell>,
    token: CancellationToken,
    handle: Option<JoinHandle<TaskOutcome>>,
}

#[derive(Default)]
struct TaskCounters {
    reaped: AtomicU64,
    completed: AtomicU64,
    cancelled: AtomicU64,
    panicked: AtomicU64,
    aborted: AtomicU64,
}

pub(crate) struct TaskSupervisor {
    arena: Mutex<SlotMap<TaskId, OwnedTask>>,
    diagnostics: Arc<Diagnostics>,
    counters: TaskCounters,
    #[cfg(test)]
    test_take_hook: TaskTakeHook,
}

#[cfg(test)]
struct TaskTakeHook {
    enabled: std::sync::atomic::AtomicBool,
    entered: std::sync::atomic::AtomicBool,
    notify: tokio::sync::Notify,
    release: tokio::sync::Semaphore,
}
#[cfg(test)]
impl Default for TaskTakeHook {
    fn default() -> Self {
        Self {
            enabled: std::sync::atomic::AtomicBool::new(false),
            entered: std::sync::atomic::AtomicBool::new(false),
            notify: tokio::sync::Notify::new(),
            release: tokio::sync::Semaphore::new(0),
        }
    }
}

impl TaskSupervisor {
    pub(crate) fn new(diagnostics: Arc<Diagnostics>) -> Arc<Self> {
        Arc::new(Self {
            arena: Mutex::new(SlotMap::with_key()),
            diagnostics,
            counters: TaskCounters::default(),
            #[cfg(test)]
            test_take_hook: TaskTakeHook::default(),
        })
    }

    pub(crate) fn spawn<F>(
        self: &Arc<Self>,
        owner_id: FiberId,
        scope_id: cordis_core::ScopeId,
        owner: Weak<FiberCell>,
        parent: &CancellationToken,
        activation: Option<watch::Receiver<bool>>,
        future: F,
    ) -> (TaskId, oneshot::Sender<()>)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let token = parent.child_token();
        let child = token.clone();
        let supervisor = self.clone();
        let wrapper_owner = owner.clone();
        let (start_tx, start_rx) = oneshot::channel();
        let id = self.arena.lock().insert_with_key(move |id| {
            let handle = tokio::spawn(async move {
                if start_rx.await.is_err() {
                    return TaskOutcome::Cancelled;
                }
                if let Some(mut activation) = activation {
                    let admitted = tokio::select! {
                        () = child.cancelled() => false,
                        result = activation.wait_for(|committed| *committed) => result.is_ok(),
                    };
                    if !admitted {
                        let outcome = TaskOutcome::Cancelled;
                        supervisor.complete(id, owner_id, &wrapper_owner, outcome);
                        return outcome;
                    }
                }
                let outcome = match AssertUnwindSafe(async {
                    tokio::select! { () = child.cancelled() => false, () = future => true }
                })
                .catch_unwind()
                .await
                {
                    Ok(true) => TaskOutcome::Completed,
                    Ok(false) => TaskOutcome::Cancelled,
                    Err(_) => TaskOutcome::Panicked,
                };
                supervisor.complete(id, owner_id, &wrapper_owner, outcome);
                outcome
            });
            OwnedTask {
                id,
                owner_id,
                scope_id,
                owner: owner.clone(),
                token,
                handle: Some(handle),
            }
        });
        (id, start_tx)
    }

    fn complete(
        &self,
        id: TaskId,
        owner_id: FiberId,
        owner: &Weak<FiberCell>,
        outcome: TaskOutcome,
    ) {
        let Some(task) = self.arena.lock().remove(id) else {
            return;
        };
        let _ = (owner_id, owner);
        self.record_task_outcome(&task, outcome);
        Self::detach_task(&task, id);
    }

    fn record_task_outcome(&self, task: &OwnedTask, outcome: TaskOutcome) {
        self.counters.reaped.fetch_add(1, Ordering::Relaxed);
        match outcome {
            TaskOutcome::Completed => {
                self.counters.completed.fetch_add(1, Ordering::Relaxed);
            }
            TaskOutcome::Cancelled => {
                self.counters.cancelled.fetch_add(1, Ordering::Relaxed);
            }
            TaskOutcome::Panicked => {
                self.counters.panicked.fetch_add(1, Ordering::Relaxed);
                self.diagnostics.push(HealthIssue {
                    at: std::time::SystemTime::now(),
                    kind: HealthIssueKind::TaskPanic,
                    scope: Some(task.scope_id),
                    fiber: Some(task.owner_id),
                    invocation: None,
                });
            }
            TaskOutcome::Aborted => {
                self.counters.aborted.fetch_add(1, Ordering::Relaxed);
            }
            TaskOutcome::Errored => {}
        }
    }

    fn detach_task(task: &OwnedTask, id: TaskId) {
        if let Some(owner) = task.owner.upgrade() {
            owner.inner.write().tasks.retain(|task| *task != id);
        }
    }

    pub(crate) async fn cancel_all(
        &self,
        ids: Vec<TaskId>,
        grace: Duration,
    ) -> Result<(), CordisError> {
        let tasks: Vec<_> = {
            let mut arena = self.arena.lock();
            ids.into_iter().filter_map(|id| arena.remove(id)).collect()
        };
        #[cfg(test)]
        if self.test_take_hook.enabled.load(Ordering::SeqCst) {
            self.test_take_hook.entered.store(true, Ordering::SeqCst);
            self.test_take_hook.notify.notify_waiters();
            let _ = self.test_take_hook.release.acquire().await;
        }
        for task in &tasks {
            task.token.cancel();
        }
        let deadline = Instant::now() + grace;
        let mut joins = FuturesUnordered::new();
        let aborts: Vec<_> = tasks
            .iter()
            .map(|task| {
                task.handle
                    .as_ref()
                    .expect("live task handle")
                    .abort_handle()
            })
            .collect();
        for mut task in tasks {
            let handle = task.handle.take().expect("live task handle");
            joins.push(async move {
                let result = handle.await;
                (task, result)
            });
        }
        let mut panic = false;
        loop {
            match tokio::time::timeout_at(deadline, joins.next()).await {
                Ok(Some((task, Ok(outcome)))) => {
                    self.record_task_outcome(&task, outcome);
                    Self::detach_task(&task, task.id);
                    panic |= outcome == TaskOutcome::Panicked;
                }
                Ok(Some((task, Err(error)))) => {
                    let outcome = if error.is_panic() {
                        TaskOutcome::Panicked
                    } else {
                        TaskOutcome::Aborted
                    };
                    self.record_task_outcome(&task, outcome);
                    Self::detach_task(&task, task.id);
                    panic |= error.is_panic();
                }
                Ok(None) => break,
                Err(_) => {
                    for abort in &aborts {
                        abort.abort();
                    }
                    while let Some((task, result)) = joins.next().await {
                        let outcome = match result {
                            Ok(outcome) => outcome,
                            Err(error) if error.is_panic() => TaskOutcome::Panicked,
                            Err(_) => TaskOutcome::Aborted,
                        };
                        panic |= outcome == TaskOutcome::Panicked;
                        self.record_task_outcome(&task, outcome);
                        Self::detach_task(&task, task.id);
                    }
                    break;
                }
            }
        }
        if panic {
            Err(CordisError::TaskPanicked("owned task panicked".into()))
        } else {
            Ok(())
        }
    }

    pub(crate) fn live_fiber_tasks(&self) -> usize {
        self.arena.lock().len()
    }
    pub(crate) fn reaped(&self) -> u64 {
        self.counters.reaped.load(Ordering::Relaxed)
    }
    pub(crate) fn panicked(&self) -> u64 {
        self.counters.panicked.load(Ordering::Relaxed)
    }
    pub(crate) fn completed(&self) -> u64 {
        self.counters.completed.load(Ordering::Relaxed)
    }
    pub(crate) fn cancelled(&self) -> u64 {
        self.counters.cancelled.load(Ordering::Relaxed)
    }
    #[cfg(test)]
    pub(crate) fn enable_take_hook(&self) {
        self.test_take_hook.enabled.store(true, Ordering::SeqCst);
    }
    #[cfg(test)]
    pub(crate) async fn wait_for_take_hook(&self) {
        loop {
            let notified = self.test_take_hook.notify.notified();
            if self.test_take_hook.entered.load(Ordering::SeqCst) {
                return;
            }
            notified.await;
        }
    }
    #[cfg(test)]
    pub(crate) fn release_take_hook(&self) {
        self.test_take_hook.release.add_permits(1);
    }
    pub(crate) fn aborted(&self) -> u64 {
        self.counters.aborted.load(Ordering::Relaxed)
    }
}
