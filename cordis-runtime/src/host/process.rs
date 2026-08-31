use super::{PluginArtifact, PluginHost};
use crate::{Context, InvocationContext, InvocationHandler, NativePlugin};
use async_trait::async_trait;
use cordis_core::{
    CordisError, HostError, HostFailureKind, InvocationKey, InvocationValue, PluginDescriptor,
    effect_fn,
};
use parking_lot::{Mutex, RwLock};
use std::{
    collections::HashMap,
    ffi::{OsStr, OsString},
    path::PathBuf,
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    process::{Child, Command},
    sync::{Semaphore, mpsc, oneshot},
    task::JoinHandle,
    time::Instant,
};
use tokio_util::sync::CancellationToken;

#[cfg(test)]
use std::sync::atomic::AtomicUsize;

#[path = "protocol.rs"]
mod protocol;
use protocol::{
    ABSOLUTE_HANDSHAKE_FRAME_LIMIT, FEATURE_CANCEL, FEATURE_DEADLINE, FEATURE_INVOCATION,
    InvocationDeclaration, InvokeOutcome, Limits, Message, PROTOCOL_MAJOR, PROTOCOL_MINOR,
    REQUIRED_FEATURES, SUPPORTED_FEATURES, WireFailure,
};

/// Resource limits for the reference child-process host.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ProcessHostConfig {
    /// Maximum time allowed for the initial handshake.
    pub handshake_timeout: Duration,
    /// Maximum negotiated frame payload size.
    pub max_frame_bytes: usize,
    /// Maximum artifact payload size.
    pub max_artifact_bytes: usize,
    /// Reserved request limit for B2.1C invocation traffic.
    pub max_request_bytes: usize,
    /// Reserved response limit for B2.1C invocation traffic.
    pub max_response_bytes: usize,
    /// Maximum control-message payload size.
    pub max_control_bytes: usize,
    /// Maximum number of pending protocol requests.
    pub max_inflight_requests: usize,
    /// Capacity of the session writer queue.
    pub outbound_queue_capacity: usize,
}

impl Default for ProcessHostConfig {
    fn default() -> Self {
        Self {
            handshake_timeout: Duration::from_secs(5),
            max_frame_bytes: 1024 * 1024,
            max_artifact_bytes: 512 * 1024,
            max_request_bytes: 256 * 1024,
            max_response_bytes: 256 * 1024,
            max_control_bytes: 64 * 1024,
            max_inflight_requests: 256,
            outbound_queue_capacity: 256,
        }
    }
}

impl ProcessHostConfig {
    fn validate(&self) -> Result<(), CordisError> {
        let invalid = self.handshake_timeout.is_zero()
            || self.max_frame_bytes == 0
            || self.max_frame_bytes > u32::MAX as usize
            || self.max_artifact_bytes == 0
            || self.max_artifact_bytes > self.max_frame_bytes
            || self.max_request_bytes == 0
            || self.max_request_bytes > self.max_frame_bytes
            || self.max_response_bytes == 0
            || self.max_response_bytes > self.max_frame_bytes
            || self.max_control_bytes == 0
            || self.max_control_bytes > self.max_frame_bytes
            || self.max_inflight_requests == 0
            || self.max_inflight_requests > u32::MAX as usize
            || self.outbound_queue_capacity == 0;
        if invalid {
            return Err(CordisError::InvalidRuntimeConfig(
                "invalid ProcessHost limits".into(),
            ));
        }
        Ok(())
    }

    fn limits(&self) -> Limits {
        Limits {
            frame: self.max_frame_bytes,
            artifact: self.max_artifact_bytes,
            request: self.max_request_bytes,
            response: self.max_response_bytes,
            inflight: self.max_inflight_requests,
        }
    }
}

/// Reference [`PluginHost`] backed by one supervised child process per load.
#[derive(Clone, Debug)]
pub struct ProcessHost {
    program: PathBuf,
    args: Vec<OsString>,
    config: ProcessHostConfig,
}

impl ProcessHost {
    /// Creates a process host using default bounded transport limits.
    #[must_use]
    pub fn new(program: impl AsRef<OsStr>) -> Self {
        Self::with_config(program, ProcessHostConfig::default())
    }

    /// Creates a process host using explicit limits.
    #[must_use]
    pub fn with_config(program: impl AsRef<OsStr>, config: ProcessHostConfig) -> Self {
        Self {
            program: PathBuf::from(program.as_ref()),
            args: Vec::new(),
            config,
        }
    }

    /// Appends one child-process argument.
    #[must_use]
    pub fn arg(mut self, arg: impl Into<OsString>) -> Self {
        self.args.push(arg.into());
        self
    }

    /// Appends child-process arguments.
    #[must_use]
    pub fn args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<OsString>,
    {
        self.args.extend(args.into_iter().map(Into::into));
        self
    }
}

#[async_trait]
impl PluginHost for ProcessHost {
    fn kind(&self) -> &'static str {
        "process"
    }

    async fn load(&self, artifact: PluginArtifact) -> Result<Arc<dyn NativePlugin>, CordisError> {
        self.config.validate()?;
        if artifact.payload.len() > self.config.max_artifact_bytes {
            return Err(CordisError::Host(HostError::new(
                HostFailureKind::MessageTooLarge,
                "artifact exceeds local ProcessHost limit",
            )));
        }

        let (client, owner) = SessionOwner::spawn(self)?;
        let handshake = tokio::time::timeout(self.config.handshake_timeout, client.handshake())
            .await
            .map_err(|_| {
                CordisError::Host(client.0.terminal_error.lock().clone().unwrap_or_else(|| {
                    HostError::new(
                        HostFailureKind::Unavailable,
                        "process host handshake timed out",
                    )
                }))
            })??;
        client.apply_negotiated(handshake)?;
        let (route, descriptor) = client.load(artifact).await?;
        if !descriptor.dependencies.is_empty() || !descriptor.provisions.is_empty() {
            return Err(CordisError::Host(HostError::new(
                HostFailureKind::UnsupportedCapability,
                "remote Service dependencies and provisions are not supported",
            )));
        }
        Ok(Arc::new(RemotePluginProxy {
            descriptor,
            route,
            client,
            owner: Mutex::new(Some(owner)),
        }))
    }
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SessionState {
    Created,
    Handshaking,
    Ready,
    Draining,
    Closed,
    Failed,
}

#[cfg(test)]
static READER_ACTORS: AtomicUsize = AtomicUsize::new(0);
#[cfg(test)]
static WRITER_ACTORS: AtomicUsize = AtomicUsize::new(0);
#[cfg(test)]
static SUPERVISOR_ACTORS: AtomicUsize = AtomicUsize::new(0);
#[cfg(test)]
static PROCESS_ACTOR_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[cfg(test)]
struct ActorCounterGuard(&'static AtomicUsize);

#[cfg(test)]
impl ActorCounterGuard {
    fn new(counter: &'static AtomicUsize) -> Self {
        counter.fetch_add(1, Ordering::AcqRel);
        Self(counter)
    }
}

#[cfg(test)]
impl Drop for ActorCounterGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

struct Outbound {
    message: Message,
    invocation_deadline: Option<Instant>,
}

struct PendingEntry {
    sender: oneshot::Sender<Result<Message, CordisError>>,
    expected: ExpectedResponseKind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExpectedResponseKind {
    HelloAck,
    Loaded,
    Started,
    Disposed,
    ShutdownAck,
    InvokeResult,
}

impl ExpectedResponseKind {
    fn accepts(self, message: &Message) -> bool {
        (self != Self::InvokeResult && matches!(message, Message::Failure { .. }))
            || matches!(
                (self, message),
                (Self::HelloAck, Message::HelloAck { .. })
                    | (Self::Loaded, Message::Loaded { .. })
                    | (Self::Started, Message::Started { .. })
                    | (Self::Disposed, Message::Disposed { .. })
                    | (Self::ShutdownAck, Message::ShutdownAck { .. })
                    | (Self::InvokeResult, Message::InvokeResult { .. })
            )
    }
}

struct SessionInner {
    state: AtomicU8,
    limits: RwLock<Limits>,
    negotiated_features: AtomicU64,
    outbound: mpsc::Sender<Outbound>,
    pending: Mutex<HashMap<u64, PendingEntry>>,
    permits: Arc<Semaphore>,
    next_id: AtomicU64,
    highest_issued: AtomicU64,
    force: CancellationToken,
    actor_stop: CancellationToken,
    shutdown_acked: AtomicBool,
    max_control_bytes: usize,
    terminal_error: Mutex<Option<HostError>>,
    actors_done: AtomicBool,
    terminal_notify: tokio::sync::Notify,
    live: Arc<AtomicBool>,
}

impl SessionInner {
    fn state(&self) -> SessionState {
        match self.state.load(Ordering::Acquire) {
            0 => SessionState::Created,
            1 => SessionState::Handshaking,
            2 => SessionState::Ready,
            3 => SessionState::Draining,
            4 => SessionState::Closed,
            _ => SessionState::Failed,
        }
    }

    fn transition(&self, expected: SessionState, next: SessionState) -> Result<(), CordisError> {
        match self.state.compare_exchange(
            expected as u8,
            next as u8,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => Ok(()),
            Err(_) => Err(self.terminal_cordis_error("invalid process host session transition")),
        }
    }

    fn transition_to_failed(&self, error: HostError) -> bool {
        let mut terminal_error = self.terminal_error.lock();
        loop {
            let current = self.state();
            if matches!(current, SessionState::Closed | SessionState::Failed) {
                return false;
            }
            if self
                .state
                .compare_exchange(
                    current as u8,
                    SessionState::Failed as u8,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                let pending_error = error.clone();
                *terminal_error = Some(error);
                drop(terminal_error);
                self.resolve_pending(&pending_error);
                self.actor_stop.cancel();
                self.force.cancel();
                self.terminal_notify.notify_waiters();
                return true;
            }
        }
    }

    fn transition_to_closed(&self) -> bool {
        let terminal_error = self.terminal_error.lock();
        loop {
            let current = self.state();
            if matches!(current, SessionState::Closed | SessionState::Failed) {
                return false;
            }
            if self
                .state
                .compare_exchange(
                    current as u8,
                    SessionState::Closed as u8,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                drop(terminal_error);
                self.resolve_pending(&HostError::new(
                    HostFailureKind::Unavailable,
                    "process host closed with a pending request",
                ));
                self.actor_stop.cancel();
                self.terminal_notify.notify_waiters();
                return true;
            }
        }
    }

    fn resolve_pending(&self, error: &HostError) {
        for (_, entry) in self.pending.lock().drain() {
            let _ = entry.sender.send(Err(CordisError::Host(error.clone())));
        }
    }

    fn terminal_cordis_error(&self, fallback: &'static str) -> CordisError {
        CordisError::Host(
            self.terminal_error
                .lock()
                .clone()
                .unwrap_or_else(|| HostError::new(HostFailureKind::Unavailable, fallback)),
        )
    }

    fn publish_actors_done(&self) {
        self.actors_done.store(true, Ordering::Release);
    }

    fn publish_issued_id(&self, id: u64) {
        self.highest_issued.fetch_max(id, Ordering::AcqRel);
    }

    fn allocate_request_id(&self) -> Result<u64, CordisError> {
        loop {
            let id = self.next_id.load(Ordering::Acquire);
            if id == 0 || id == u64::MAX {
                return Err(CordisError::Host(HostError::new(
                    HostFailureKind::Unavailable,
                    "process host request ID space exhausted",
                )));
            }
            if self
                .next_id
                .compare_exchange_weak(id, id + 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.publish_issued_id(id);
                return Ok(id);
            }
        }
    }
}

#[derive(Clone)]
struct SessionClient(Arc<SessionInner>);

#[derive(Clone, Copy)]
struct NegotiatedSession {
    limits: Limits,
    features: u64,
}

struct PendingRegistration {
    id: u64,
    session: Arc<SessionInner>,
    armed: bool,
}

impl PendingRegistration {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for PendingRegistration {
    fn drop(&mut self) {
        if self.armed {
            self.session.pending.lock().remove(&self.id);
        }
    }
}

impl SessionClient {
    async fn wait_for_failure(&self) -> Option<HostError> {
        loop {
            match self.0.state() {
                SessionState::Failed => return self.0.terminal_error.lock().clone(),
                SessionState::Closed => return None,
                _ => {}
            }
            let notified = self.0.terminal_notify.notified();
            match self.0.state() {
                SessionState::Failed => return self.0.terminal_error.lock().clone(),
                SessionState::Closed => return None,
                _ => notified.await,
            }
        }
    }

    async fn request(
        &self,
        required_state: SessionState,
        expected: ExpectedResponseKind,
        build: impl FnOnce(u64) -> Message,
    ) -> Result<Message, CordisError> {
        if self.0.state() != required_state {
            return Err(self
                .0
                .terminal_cordis_error("request is not allowed in the current session state"));
        }
        let _permit = self.0.permits.clone().acquire_owned().await.map_err(|_| {
            CordisError::Host(HostError::new(
                HostFailureKind::Unavailable,
                "session request permits closed",
            ))
        })?;
        if self.0.state() != required_state {
            return Err(self
                .0
                .terminal_cordis_error("session state changed before request admission"));
        }
        let id = self.0.allocate_request_id()?;
        let message = build(id);
        let (sender, receiver) = oneshot::channel();
        self.0
            .pending
            .lock()
            .insert(id, PendingEntry { sender, expected });
        let mut registration = PendingRegistration {
            id,
            session: Arc::clone(&self.0),
            armed: true,
        };
        if self.0.state() != required_state {
            return Err(self
                .0
                .terminal_cordis_error("session state changed after request registration"));
        }
        if self
            .0
            .outbound
            .send(Outbound {
                message,
                invocation_deadline: None,
            })
            .await
            .is_err()
        {
            return Err(CordisError::Host(HostError::new(
                HostFailureKind::TransportClosed,
                "process host writer stopped",
            )));
        }
        let result = receiver.await.map_err(|_| {
            CordisError::Host(HostError::new(
                HostFailureKind::TransportClosed,
                "process host response channel closed",
            ))
        })?;
        registration.disarm();
        result
    }

    async fn handshake(&self) -> Result<NegotiatedSession, CordisError> {
        self.0
            .transition(SessionState::Created, SessionState::Handshaking)?;
        let local = *self.0.limits.read();
        match self
            .request(
                SessionState::Handshaking,
                ExpectedResponseKind::HelloAck,
                |id| Message::Hello {
                    id,
                    major: PROTOCOL_MAJOR,
                    minor: PROTOCOL_MINOR,
                    supported_features: SUPPORTED_FEATURES,
                    required_features: REQUIRED_FEATURES,
                    limits: local,
                },
            )
            .await?
        {
            Message::HelloAck {
                major,
                minor,
                supported_features,
                required_features,
                limits,
                ..
            } if major == PROTOCOL_MAJOR
                && minor == PROTOCOL_MINOR
                && REQUIRED_FEATURES & !supported_features == 0
                && required_features & !SUPPORTED_FEATURES == 0 =>
            {
                Ok(NegotiatedSession {
                    limits: Limits {
                        frame: local.frame.min(limits.frame),
                        artifact: local.artifact.min(limits.artifact),
                        request: local.request.min(limits.request),
                        response: local.response.min(limits.response),
                        inflight: local.inflight.min(limits.inflight),
                    },
                    features: SUPPORTED_FEATURES & supported_features,
                })
            }
            Message::HelloAck { .. } => Err(CordisError::Host(HostError::new(
                HostFailureKind::HandshakeIncompatible,
                "incompatible process host protocol or missing Lifecycle feature",
            ))),
            Message::Failure { failure, .. } => Err(wire_failure(failure)),
            _ => Err(protocol_error("expected HelloAck during handshake")),
        }
    }

    fn apply_negotiated(&self, negotiated: NegotiatedSession) -> Result<(), CordisError> {
        let limits = negotiated.limits;
        if limits.frame == 0 || limits.artifact == 0 || limits.inflight == 0 {
            return Err(CordisError::Host(HostError::new(
                HostFailureKind::HandshakeIncompatible,
                "peer negotiated a zero session limit",
            )));
        }
        let local_inflight = self.0.limits.read().inflight;
        let shrink = local_inflight.checked_sub(limits.inflight).ok_or_else(|| {
            CordisError::Host(HostError::new(
                HostFailureKind::HandshakeIncompatible,
                "peer attempted to expand local inflight policy",
            ))
        })?;
        if self.0.permits.forget_permits(shrink) != shrink {
            return Err(protocol_error(
                "inflight permits were active during negotiation",
            ));
        }
        *self.0.limits.write() = limits;
        self.0
            .negotiated_features
            .store(negotiated.features, Ordering::Release);
        self.0
            .transition(SessionState::Handshaking, SessionState::Ready)
    }

    async fn load(&self, artifact: PluginArtifact) -> Result<(u64, PluginDescriptor), CordisError> {
        let limits = *self.0.limits.read();
        if artifact.payload.len() > limits.artifact {
            return Err(CordisError::Host(HostError::new(
                HostFailureKind::MessageTooLarge,
                "artifact exceeds negotiated process host limit",
            )));
        }
        match self
            .request(SessionState::Ready, ExpectedResponseKind::Loaded, |id| {
                Message::Load {
                    id,
                    format: artifact.format,
                    revision: artifact.revision.0,
                    payload: artifact.payload,
                }
            })
            .await?
        {
            Message::Loaded {
                route, descriptor, ..
            } => Ok((route, descriptor)),
            Message::Failure { failure, .. } => Err(wire_failure(failure)),
            _ => Err(protocol_error("expected Loaded response")),
        }
    }

    async fn start(&self, route: u64) -> Result<Vec<InvocationDeclaration>, CordisError> {
        match self
            .request(SessionState::Ready, ExpectedResponseKind::Started, |id| {
                Message::Start { id, route }
            })
            .await?
        {
            Message::Started { invocations, .. } => {
                let required = FEATURE_INVOCATION | FEATURE_CANCEL | FEATURE_DEADLINE;
                if !invocations.is_empty()
                    && self.0.negotiated_features.load(Ordering::Acquire) & required != required
                {
                    return Err(CordisError::Host(HostError::new(
                        HostFailureKind::UnsupportedCapability,
                        "remote invocation declarations require Invocation, Cancel, and Deadline",
                    )));
                }
                Ok(invocations)
            }
            Message::Failure { failure, .. } => Err(wire_failure(failure)),
            _ => Err(protocol_error("expected Started response")),
        }
    }

    async fn dispose(&self, route: u64) -> Result<(), CordisError> {
        match self
            .request(SessionState::Ready, ExpectedResponseKind::Disposed, |id| {
                Message::Dispose { id, route }
            })
            .await?
        {
            Message::Disposed { .. } => Ok(()),
            Message::Failure { failure, .. } => Err(wire_failure(failure)),
            _ => Err(protocol_error("expected Disposed response")),
        }
    }

    async fn shutdown(&self) -> Result<(), CordisError> {
        self.0
            .transition(SessionState::Ready, SessionState::Draining)?;
        match self
            .request(
                SessionState::Draining,
                ExpectedResponseKind::ShutdownAck,
                |id| Message::Shutdown { id },
            )
            .await?
        {
            Message::ShutdownAck { .. } => {
                self.0.shutdown_acked.store(true, Ordering::Release);
                Ok(())
            }
            Message::Failure { failure, .. } => Err(wire_failure(failure)),
            _ => Err(protocol_error("expected ShutdownAck response")),
        }
    }

    async fn invoke(
        &self,
        route: u64,
        format: Arc<str>,
        bytes: Arc<[u8]>,
        deadline: Instant,
    ) -> Result<InvocationValue, CordisError> {
        if bytes.len() > self.0.limits.read().request {
            return Err(CordisError::Host(HostError::new(
                HostFailureKind::MessageTooLarge,
                "invocation request exceeds negotiated process host limit",
            )));
        }
        if self.0.state() != SessionState::Ready {
            return Err(self
                .0
                .terminal_cordis_error("invocation is not allowed outside Ready"));
        }
        let _permit = self.0.permits.clone().try_acquire_owned().map_err(|_| {
            CordisError::Host(HostError::new(
                HostFailureKind::Overloaded,
                "process host inflight capacity exhausted",
            ))
        })?;
        let id = self.0.allocate_request_id()?;
        let (sender, receiver) = oneshot::channel();
        self.0.pending.lock().insert(
            id,
            PendingEntry {
                sender,
                expected: ExpectedResponseKind::InvokeResult,
            },
        );
        let registration = PendingRegistration {
            id,
            session: Arc::clone(&self.0),
            armed: true,
        };
        if self.0.state() != SessionState::Ready {
            return Err(self
                .0
                .terminal_cordis_error("session changed before invocation enqueue"));
        }
        let outbound = Outbound {
            message: Message::Invoke {
                id,
                route,
                format,
                bytes,
                remaining_nanos: 0,
            },
            invocation_deadline: Some(deadline),
        };
        self.0
            .outbound
            .try_send(outbound)
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => CordisError::Host(HostError::new(
                    HostFailureKind::Overloaded,
                    "process host outbound queue is full",
                )),
                mpsc::error::TrySendError::Closed(_) => self
                    .0
                    .terminal_cordis_error("process host writer stopped before invocation"),
            })?;
        let mut guard = RemoteInvokeGuard {
            registration: Some(registration),
            session: Arc::clone(&self.0),
            id,
            armed: true,
        };
        let result = receiver.await.map_err(|_| {
            self.0
                .terminal_cordis_error("process host invocation response channel closed")
        })?;
        guard.disarm();
        let message = result?;
        match message {
            Message::InvokeResult {
                outcome: InvokeOutcome::Success { format, bytes },
                ..
            } => Ok(InvocationValue::External { format, bytes }),
            Message::InvokeResult {
                outcome: InvokeOutcome::RemoteDomain(error),
                ..
            } => Err(CordisError::RemoteDomain(error)),
            Message::InvokeResult {
                outcome: InvokeOutcome::HostFailure(error),
                ..
            } => Err(CordisError::Host(error)),
            _ => Err(protocol_error("expected InvokeResult response")),
        }
    }
}

struct RemoteInvokeGuard {
    registration: Option<PendingRegistration>,
    session: Arc<SessionInner>,
    id: u64,
    armed: bool,
}

impl RemoteInvokeGuard {
    fn disarm(&mut self) {
        if let Some(mut registration) = self.registration.take() {
            registration.disarm();
        }
        self.armed = false;
    }
}

impl Drop for RemoteInvokeGuard {
    fn drop(&mut self) {
        if self.armed {
            self.registration.take();
            if matches!(
                self.session.state(),
                SessionState::Ready | SessionState::Draining
            ) {
                let _ = self.session.outbound.try_send(Outbound {
                    message: Message::Cancel { id: self.id },
                    invocation_deadline: None,
                });
            }
        }
    }
}

fn protocol_error(message: &'static str) -> CordisError {
    CordisError::Host(HostError::new(HostFailureKind::ProtocolViolation, message))
}

fn wire_failure(failure: WireFailure) -> CordisError {
    match failure {
        WireFailure::Host(error) => CordisError::Host(error),
        WireFailure::Domain(error) => CordisError::RemoteDomain(error),
    }
}

struct SessionOwner {
    client: SessionClient,
    supervisor: Option<JoinHandle<()>>,
    armed: bool,
}

struct SpawnGuard(Option<Child>);

impl SpawnGuard {
    fn child_mut(&mut self) -> &mut Child {
        self.0.as_mut().expect("spawn guard contains child")
    }

    fn handoff(mut self) -> Child {
        self.0.take().expect("spawn guard contains child")
    }
}

impl Drop for SpawnGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.start_kill();
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                runtime.spawn(async move {
                    let _ = child.wait().await;
                });
            }
        }
    }
}

impl SessionOwner {
    fn spawn(host: &ProcessHost) -> Result<(SessionClient, Self), CordisError> {
        Self::spawn_with_setup_failure(host, false)
    }

    fn spawn_with_setup_failure(
        host: &ProcessHost,
        fail_after_spawn: bool,
    ) -> Result<(SessionClient, Self), CordisError> {
        let mut command = Command::new(&host.program);
        command
            .args(&host.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(false);
        let child = command.spawn().map_err(|error| {
            CordisError::Host(HostError::new(
                HostFailureKind::Unavailable,
                format!("failed to spawn process host: {error}"),
            ))
        })?;
        let mut guard = SpawnGuard(Some(child));
        if fail_after_spawn {
            return Err(protocol_error("injected process host setup failure"));
        }
        let stdin = guard
            .child_mut()
            .stdin
            .take()
            .ok_or_else(|| protocol_error("child stdin unavailable"))?;
        let stdout = guard
            .child_mut()
            .stdout
            .take()
            .ok_or_else(|| protocol_error("child stdout unavailable"))?;
        let (outbound, receiver) = mpsc::channel(host.config.outbound_queue_capacity);
        let inner = Arc::new(SessionInner {
            state: AtomicU8::new(SessionState::Created as u8),
            limits: RwLock::new(host.config.limits()),
            negotiated_features: AtomicU64::new(0),
            outbound,
            pending: Mutex::new(HashMap::new()),
            permits: Arc::new(Semaphore::new(host.config.max_inflight_requests)),
            next_id: AtomicU64::new(1),
            highest_issued: AtomicU64::new(0),
            force: CancellationToken::new(),
            actor_stop: CancellationToken::new(),
            shutdown_acked: AtomicBool::new(false),
            max_control_bytes: host.config.max_control_bytes,
            terminal_error: Mutex::new(None),
            actors_done: AtomicBool::new(false),
            terminal_notify: tokio::sync::Notify::new(),
            live: Arc::new(AtomicBool::new(true)),
        });
        let writer = tokio::spawn(writer_task(stdin, receiver, Arc::clone(&inner)));
        let reader = tokio::spawn(reader_task(stdout, Arc::clone(&inner)));
        let child = guard.handoff();
        let supervisor = tokio::spawn(supervisor_task(child, reader, writer, Arc::clone(&inner)));
        let client = SessionClient(inner);
        Ok((
            client.clone(),
            Self {
                client,
                supervisor: Some(supervisor),
                armed: true,
            },
        ))
    }

    async fn cleanup(mut self, route: u64) -> Result<(), CordisError> {
        let dispose = self.client.dispose(route).await;
        let shutdown = self.client.shutdown().await;
        let completion = self.wait_completion().await;
        self.armed = false;
        dispose.and(shutdown).and(completion)
    }

    async fn cleanup_until(self, route: u64, deadline: Option<Instant>) -> Result<(), CordisError> {
        if let Some(deadline) = deadline {
            tokio::time::timeout_at(deadline, self.cleanup(route))
                .await
                .map_err(|_| {
                    CordisError::Host(HostError::new(
                        HostFailureKind::ProcessKilled,
                        "process host exceeded the Runtime shutdown deadline",
                    ))
                })?
        } else {
            self.cleanup(route).await
        }
    }

    async fn wait_completion(&mut self) -> Result<(), CordisError> {
        let supervisor = self
            .supervisor
            .take()
            .ok_or_else(|| protocol_error("session supervisor already joined"))?;
        supervisor.await.map_err(|error| {
            protocol_error(if error.is_panic() {
                "session supervisor panicked"
            } else {
                "session supervisor was cancelled"
            })
        })?;
        match self.client.0.state() {
            SessionState::Closed => Ok(()),
            SessionState::Failed => Err(self.client.0.terminal_cordis_error("process host failed")),
            _ => Err(protocol_error("session actors ended before terminal state")),
        }
    }
}

impl Drop for SessionOwner {
    fn drop(&mut self) {
        if self.armed {
            self.client.0.force.cancel();
        }
    }
}

struct RemotePluginProxy {
    descriptor: PluginDescriptor,
    route: u64,
    client: SessionClient,
    owner: Mutex<Option<SessionOwner>>,
}

#[async_trait]
impl NativePlugin for RemotePluginProxy {
    fn descriptor(&self) -> PluginDescriptor {
        self.descriptor.clone()
    }

    async fn start(&self, context: Context) -> Result<(), CordisError> {
        let owner = self.owner.lock().take().ok_or_else(|| {
            CordisError::Host(HostError::new(
                HostFailureKind::Unavailable,
                "remote process ownership already transferred",
            ))
        })?;
        let route = self.route;
        let cleanup_context = context.clone();
        context.effect(effect_fn(move || {
            let deadline = cleanup_context.runtime_shutdown_deadline();
            owner.cleanup_until(route, deadline)
        }))?;
        let declarations = self.client.start(route).await?;
        context.register_host_process(Arc::clone(&self.client.0.live))?;
        let reporter = context.clone();
        let liveness = self.client.clone();
        context.spawn(async move {
            if let Some(error) = liveness.wait_for_failure().await {
                reporter.report_host_failure(error);
            }
        })?;
        for declaration in declarations {
            let handler = RemoteInvocationHandler {
                key: declaration.key.clone(),
                route: declaration.route,
                client: self.client.clone(),
            };
            context.handle_invocation(declaration.key, Arc::new(handler))?;
        }
        Ok(())
    }
}

struct RemoteInvocationHandler {
    key: InvocationKey,
    route: u64,
    client: SessionClient,
}

#[async_trait]
impl InvocationHandler for RemoteInvocationHandler {
    async fn call(
        &self,
        context: InvocationContext,
        input: InvocationValue,
    ) -> Result<InvocationValue, CordisError> {
        let InvocationValue::External { format, bytes } = input else {
            return Err(CordisError::InvocationTypeMismatch(self.key.clone()));
        };
        self.client
            .invoke(self.route, format, bytes, context.deadline())
            .await
    }
}

async fn writer_task(
    mut stdin: tokio::process::ChildStdin,
    mut receiver: mpsc::Receiver<Outbound>,
    inner: Arc<SessionInner>,
) {
    #[cfg(test)]
    let _actor = ActorCounterGuard::new(&WRITER_ACTORS);
    loop {
        let mut outbound = tokio::select! {
            value = receiver.recv() => match value { Some(value) => value, None => return },
            () = inner.actor_stop.cancelled() => return,
        };
        if let (
            Message::Invoke {
                remaining_nanos, ..
            },
            Some(deadline),
        ) = (&mut outbound.message, outbound.invocation_deadline)
        {
            *remaining_nanos = wire_budget_nanos(deadline, Instant::now());
        }
        let limit = if matches!(
            inner.state(),
            SessionState::Created | SessionState::Handshaking
        ) {
            ABSOLUTE_HANDSHAKE_FRAME_LIMIT.min(inner.max_control_bytes)
        } else if outbound.message.is_control() {
            inner.limits.read().frame.min(inner.max_control_bytes)
        } else {
            inner.limits.read().frame
        };
        let payload = match protocol::encode(&outbound.message, limit) {
            Ok(payload) => payload,
            Err(error) => {
                inner.transition_to_failed(error);
                return;
            }
        };
        let length = if let Ok(length) = u32::try_from(payload.len()) {
            length.to_be_bytes()
        } else {
            inner.transition_to_failed(HostError::new(
                HostFailureKind::MessageTooLarge,
                "frame exceeds u32",
            ));
            return;
        };
        let write_result = async {
            stdin.write_all(&length).await?;
            stdin.write_all(&payload).await
        }
        .await;
        if let Err(error) = write_result {
            inner.transition_to_failed(HostError::new(
                HostFailureKind::TransportClosed,
                format!("process host write failed: {error}"),
            ));
            return;
        }
        if let Err(error) = stdin.flush().await {
            inner.transition_to_failed(HostError::new(
                HostFailureKind::TransportClosed,
                format!("process host flush failed: {error}"),
            ));
            return;
        }
    }
}

fn wire_budget_nanos(deadline: Instant, now: Instant) -> u64 {
    u64::try_from(deadline.saturating_duration_since(now).as_nanos()).unwrap_or(u64::MAX)
}

async fn reader_task(mut stdout: tokio::process::ChildStdout, inner: Arc<SessionInner>) {
    #[cfg(test)]
    let _actor = ActorCounterGuard::new(&READER_ACTORS);
    loop {
        let limit = if matches!(
            inner.state(),
            SessionState::Created | SessionState::Handshaking
        ) {
            ABSOLUTE_HANDSHAKE_FRAME_LIMIT.min(inner.max_control_bytes)
        } else {
            inner.limits.read().frame
        };
        let frame = tokio::select! {
            value = read_frame(&mut stdout, limit) => value,
            () = inner.actor_stop.cancelled() => return,
        };
        let payload = match frame {
            Ok(payload) => payload,
            Err(error) => {
                if inner.state() != SessionState::Closed
                    && !inner.shutdown_acked.load(Ordering::Acquire)
                {
                    inner.transition_to_failed(error);
                }
                return;
            }
        };
        if protocol::payload_is_control(&payload) && payload.len() > inner.max_control_bytes {
            inner.transition_to_failed(HostError::new(
                HostFailureKind::MessageTooLarge,
                "control frame exceeds max_control_bytes",
            ));
            return;
        }
        let message = match protocol::decode(&payload, *inner.limits.read()) {
            Ok(message) => message,
            Err(error) => {
                inner.transition_to_failed(error);
                return;
            }
        };
        if !dispatch_response(&inner, message) {
            return;
        }
    }
}

fn dispatch_response(inner: &Arc<SessionInner>, message: Message) -> bool {
    let id = message.id();
    if id > inner.highest_issued.load(Ordering::Acquire) {
        inner.transition_to_failed(HostError::new(
            HostFailureKind::ProtocolViolation,
            "response used a future request id",
        ));
        return false;
    }
    let Some(entry) = inner.pending.lock().remove(&id) else {
        return true;
    };
    if !entry.expected.accepts(&message) {
        let error = HostError::new(
            HostFailureKind::ProtocolViolation,
            "response kind did not match the live request",
        );
        inner.transition_to_failed(error.clone());
        let _ = entry.sender.send(Err(CordisError::Host(error)));
        return false;
    }
    let terminal_remote = match &message {
        Message::Failure {
            failure: WireFailure::Host(error),
            ..
        }
        | Message::InvokeResult {
            outcome: InvokeOutcome::HostFailure(error),
            ..
        } if remote_failure_is_session_terminal(error.kind()) => Some(error.clone()),
        _ => None,
    };
    if let Some(error) = terminal_remote {
        inner.transition_to_failed(error.clone());
        let _ = entry.sender.send(Err(CordisError::Host(error)));
        return false;
    }
    if entry.expected == ExpectedResponseKind::ShutdownAck {
        inner.shutdown_acked.store(true, Ordering::Release);
    }
    let _ = entry.sender.send(Ok(message));
    true
}

fn remote_failure_is_session_terminal(kind: HostFailureKind) -> bool {
    matches!(
        kind,
        HostFailureKind::ProtocolViolation
            | HostFailureKind::HandshakeIncompatible
            | HostFailureKind::TransportClosed
            | HostFailureKind::ProcessExited
            | HostFailureKind::ProcessKilled
    )
}

async fn read_frame(
    reader: &mut (impl AsyncRead + Unpin),
    limit: usize,
) -> Result<Vec<u8>, HostError> {
    let mut header = [0_u8; 4];
    reader.read_exact(&mut header).await.map_err(|error| {
        HostError::new(
            HostFailureKind::TransportClosed,
            format!("process host frame header failed: {error}"),
        )
    })?;
    let length = u32::from_be_bytes(header) as usize;
    if length > limit {
        return Err(HostError::new(
            HostFailureKind::MessageTooLarge,
            "frame length exceeds current hard limit",
        ));
    }
    let mut payload = vec![0; length];
    reader.read_exact(&mut payload).await.map_err(|error| {
        HostError::new(
            HostFailureKind::ProtocolViolation,
            format!("truncated process host frame: {error}"),
        )
    })?;
    Ok(payload)
}

async fn supervisor_task(
    mut child: Child,
    reader: JoinHandle<()>,
    writer: JoinHandle<()>,
    inner: Arc<SessionInner>,
) {
    #[cfg(test)]
    let _actor = ActorCounterGuard::new(&SUPERVISOR_ACTORS);
    tokio::select! {
        status = child.wait() => {
            match status {
                Ok(status) if status.success() && inner.shutdown_acked.load(Ordering::Acquire) => { inner.transition_to_closed(); }
                Ok(status) => { inner.transition_to_failed(HostError::new(HostFailureKind::ProcessExited, format!("process host exited without a complete graceful shutdown: {status}"))); }
                Err(error) => { inner.transition_to_failed(HostError::new(HostFailureKind::ProcessExited, format!("process host wait failed: {error}"))); }
            }
        }
        () = inner.force.cancelled() => {
            let _ = child.kill().await;
            match child.wait().await {
                Ok(_) => {
                    if inner.state() == SessionState::Failed {
                        inner.actor_stop.cancel();
                    } else {
                        inner.transition_to_failed(HostError::new(HostFailureKind::ProcessKilled, "process host was force terminated"));
                    }
                }
                Err(error) => { inner.transition_to_failed(HostError::new(HostFailureKind::ProcessKilled, format!("process host reap failed: {error}"))); }
            }
        }
    }
    inner.actor_stop.cancel();
    let _ = reader.await;
    let _ = writer.await;
    inner.publish_actors_done();
    inner.live.store(false, Ordering::Release);
}

#[cfg(test)]
mod tests {
    use super::*;
    use cordis_core::PluginRevision;
    use std::path::PathBuf;

    #[test]
    fn invalid_configs_are_rejected_without_panicking() {
        let config = ProcessHostConfig {
            max_frame_bytes: 0,
            ..ProcessHostConfig::default()
        };
        assert!(matches!(
            config.validate(),
            Err(CordisError::InvalidRuntimeConfig(_))
        ));
    }

    #[test]
    fn terminal_fail_cannot_leave_failed_and_close_cannot_leave_closed() {
        let (failed, _) = test_session(SessionState::Ready, 4);
        let first = HostError::new(HostFailureKind::TransportClosed, "first terminal failure");
        assert!(failed.0.transition_to_failed(first.clone()));
        assert!(!failed.0.transition_to_closed());
        assert!(!failed.0.transition_to_failed(HostError::new(
            HostFailureKind::ProcessExited,
            "later failure",
        )));
        assert_eq!(failed.0.state(), SessionState::Failed);
        assert_eq!(failed.0.terminal_error.lock().as_ref(), Some(&first));

        let (closed, _) = test_session(SessionState::Draining, 4);
        assert!(closed.0.transition_to_closed());
        assert!(!closed.0.transition_to_failed(HostError::new(
            HostFailureKind::TransportClosed,
            "late teardown",
        )));
        assert_eq!(closed.0.state(), SessionState::Closed);
        assert!(closed.0.terminal_error.lock().is_none());
    }

    #[test]
    fn close_vs_fail_has_one_immutable_terminal_winner() {
        for _ in 0..1_000 {
            let (client, _) = test_session(SessionState::Ready, 4);
            let barrier = Arc::new(std::sync::Barrier::new(3));
            let close_inner = Arc::clone(&client.0);
            let close_barrier = Arc::clone(&barrier);
            let close = std::thread::spawn(move || {
                close_barrier.wait();
                close_inner.transition_to_closed()
            });
            let fail_inner = Arc::clone(&client.0);
            let fail_barrier = Arc::clone(&barrier);
            let fail = std::thread::spawn(move || {
                fail_barrier.wait();
                fail_inner.transition_to_failed(HostError::new(
                    HostFailureKind::TransportClosed,
                    "race failure",
                ))
            });
            barrier.wait();
            let close_won = close.join().unwrap();
            let fail_won = fail.join().unwrap();
            assert_ne!(close_won, fail_won);
            match client.0.state() {
                SessionState::Closed => assert!(client.0.terminal_error.lock().is_none()),
                SessionState::Failed => assert!(client.0.terminal_error.lock().is_some()),
                state => panic!("non-terminal race result: {state:?}"),
            }
        }
    }

    #[test]
    fn issued_high_watermark_never_regresses() {
        let (client, _) = test_session(SessionState::Ready, 4);
        client.0.publish_issued_id(9);
        client.0.publish_issued_id(3);
        assert_eq!(client.0.highest_issued.load(Ordering::Acquire), 9);
    }

    #[test]
    fn request_id_allocation_refuses_wraparound() {
        let (client, _) = test_session(SessionState::Ready, 4);
        client.0.next_id.store(u64::MAX - 1, Ordering::Release);
        assert_eq!(client.0.allocate_request_id().unwrap(), u64::MAX - 1);
        let error = client.0.allocate_request_id().unwrap_err();
        assert!(matches!(
            error,
            CordisError::Host(error) if error.kind() == HostFailureKind::Unavailable
        ));
        assert_eq!(client.0.next_id.load(Ordering::Acquire), u64::MAX);
        assert_eq!(
            client.0.highest_issued.load(Ordering::Acquire),
            u64::MAX - 1
        );
    }

    #[test]
    fn concurrent_request_id_publication_is_unique_and_monotonic() {
        let (client, _) = test_session(SessionState::Ready, 4);
        let mut workers = Vec::new();
        for _ in 0..32 {
            let inner = Arc::clone(&client.0);
            workers.push(std::thread::spawn(move || {
                (0..320)
                    .map(|_| inner.allocate_request_id().unwrap())
                    .collect::<Vec<_>>()
            }));
        }
        let mut ids = workers
            .into_iter()
            .flat_map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>();
        ids.sort_unstable();
        assert_eq!(ids, (1..=10_240).collect::<Vec<_>>());
        assert_eq!(client.0.highest_issued.load(Ordering::Acquire), 10_240);
    }

    #[tokio::test]
    async fn ordinary_requests_are_rejected_before_ready() {
        let (client, mut outbound) = test_session(SessionState::Handshaking, 4);
        assert!(client.start(7).await.is_err());
        assert!(client.0.pending.lock().is_empty());
        assert!(outbound.try_recv().is_err());
    }

    #[test]
    fn failure_is_valid_for_every_live_request_kind() {
        let failure = Message::Failure {
            id: 1,
            failure: WireFailure::Host(HostError::new(
                HostFailureKind::Unavailable,
                "peer rejected request",
            )),
        };
        for expected in [
            ExpectedResponseKind::HelloAck,
            ExpectedResponseKind::Loaded,
            ExpectedResponseKind::Started,
            ExpectedResponseKind::Disposed,
            ExpectedResponseKind::ShutdownAck,
        ] {
            assert!(expected.accepts(&failure));
        }
    }

    #[tokio::test]
    async fn wrong_live_response_kind_fails_the_entire_session() {
        let (client, mut outbound) = test_session(SessionState::Ready, 4);
        let request = tokio::spawn({
            let client = client.clone();
            async move {
                client
                    .load(PluginArtifact::new("fixture", PluginRevision(1), []))
                    .await
            }
        });
        let sent = outbound.recv().await.unwrap();
        assert!(!dispatch_response(
            &client.0,
            Message::ShutdownAck {
                id: sent.message.id()
            }
        ));
        let error = request.await.unwrap().unwrap_err();
        assert!(matches!(
            error,
            CordisError::Host(error) if error.kind() == HostFailureKind::ProtocolViolation
        ));
        assert_eq!(client.0.state(), SessionState::Failed);
        assert!(client.0.pending.lock().is_empty());
    }

    #[test]
    fn wire_budget_is_derived_from_actual_write_time_without_extension() {
        let now = Instant::now();
        let deadline = now + Duration::from_millis(50);
        assert_eq!(wire_budget_nanos(deadline, now), 50_000_000);
        assert_eq!(
            wire_budget_nanos(deadline, now + Duration::from_millis(20)),
            30_000_000
        );
        assert_eq!(wire_budget_nanos(deadline, deadline), 0);
        assert_eq!(
            wire_budget_nanos(deadline, deadline + Duration::from_millis(1)),
            0
        );
    }

    #[tokio::test]
    async fn terminal_remote_failure_fans_out_every_pending_invocation() {
        let (client, mut outbound) = test_session(SessionState::Ready, 4);
        let mut calls = Vec::new();
        for _ in 0..2 {
            calls.push(tokio::spawn({
                let client = client.clone();
                async move {
                    client
                        .invoke(
                            70,
                            Arc::from("application/test"),
                            Arc::from(&b"x"[..]),
                            Instant::now() + Duration::from_secs(1),
                        )
                        .await
                }
            }));
        }
        let first = outbound.recv().await.unwrap();
        let _second = outbound.recv().await.unwrap();
        assert!(!dispatch_response(
            &client.0,
            Message::InvokeResult {
                id: first.message.id(),
                outcome: InvokeOutcome::HostFailure(HostError::new(
                    HostFailureKind::ProtocolViolation,
                    "terminal remote failure",
                )),
            }
        ));
        for call in calls {
            assert!(matches!(call.await.unwrap(),
                Err(CordisError::Host(error)) if error.kind() == HostFailureKind::ProtocolViolation));
        }
        assert_eq!(client.0.state(), SessionState::Failed);
        assert!(client.0.pending.lock().is_empty());
    }

    #[tokio::test]
    async fn ten_thousand_remote_invoke_drops_remove_pending_before_best_effort_cancel() {
        let (client, mut outbound) = test_session(SessionState::Ready, 4);
        for _ in 0..2_500 {
            let mut calls = Vec::new();
            for _ in 0..4 {
                calls.push(tokio::spawn({
                    let client = client.clone();
                    async move {
                        client
                            .invoke(
                                70,
                                Arc::from("application/test"),
                                Arc::from(&b"x"[..]),
                                Instant::now() + Duration::from_secs(1),
                            )
                            .await
                    }
                }));
            }
            for _ in 0..4 {
                assert!(matches!(
                    outbound.recv().await.unwrap().message,
                    Message::Invoke { .. }
                ));
            }
            wait_pending(&client, 4).await;
            for call in calls {
                call.abort();
                let _ = call.await;
            }
            wait_pending(&client, 0).await;
            for _ in 0..4 {
                assert!(matches!(
                    outbound.recv().await.unwrap().message,
                    Message::Cancel { .. }
                ));
            }
        }
        assert_eq!(client.0.permits.available_permits(), 4);
    }

    #[tokio::test]
    async fn full_outbound_queue_rejects_invoke_without_pending_or_permit_leak() {
        let (client, _outbound) = test_session(SessionState::Ready, 4);
        for id in 1..=4 {
            assert!(
                client
                    .0
                    .outbound
                    .try_send(Outbound {
                        message: Message::Cancel { id },
                        invocation_deadline: None,
                    })
                    .is_ok()
            );
        }
        let error = client
            .invoke(
                70,
                Arc::from("application/test"),
                Arc::from(&b"x"[..]),
                Instant::now() + Duration::from_secs(1),
            )
            .await
            .unwrap_err();
        assert!(matches!(error,
            CordisError::Host(error) if error.kind() == HostFailureKind::Overloaded));
        assert!(client.0.pending.lock().is_empty());
        assert_eq!(client.0.permits.available_permits(), 4);
    }

    #[test]
    fn failure_between_hello_ack_and_ready_cannot_resurrect_session() {
        let (client, _) = test_session(SessionState::Handshaking, 4);
        client.0.transition_to_failed(HostError::new(
            HostFailureKind::TransportClosed,
            "failed after HelloAck",
        ));
        assert!(
            client
                .apply_negotiated(NegotiatedSession {
                    limits: test_limits(4),
                    features: SUPPORTED_FEATURES,
                })
                .is_err()
        );
        assert_eq!(client.0.state(), SessionState::Failed);
    }

    #[tokio::test]
    async fn terminal_transition_drains_pending() {
        let (client, mut receiver) = test_session(SessionState::Ready, 4);
        let request = tokio::spawn({
            let client = client.clone();
            async move { client.start(1).await }
        });
        wait_pending(&client, 1).await;
        let _outbound = receiver.recv().await.unwrap();
        assert!(client.0.transition_to_closed());
        assert!(request.await.unwrap().is_err());
        assert!(client.0.pending.lock().is_empty());
    }

    #[tokio::test]
    async fn ten_thousand_dropped_requests_remain_bounded() {
        let (client, mut receiver) = test_session(SessionState::Ready, 4);
        let drain = tokio::spawn(async move { while receiver.recv().await.is_some() {} });
        let mut max_pending = 0;
        for _ in 0..10_000 {
            let request = tokio::spawn({
                let client = client.clone();
                async move { client.start(1).await }
            });
            wait_pending(&client, 1).await;
            max_pending = max_pending.max(client.0.pending.lock().len());
            request.abort();
            let _ = request.await;
            assert!(client.0.pending.lock().is_empty());
        }
        assert!(max_pending <= 4);
        assert_eq!(client.0.permits.available_permits(), 4);
        drain.abort();
    }

    #[tokio::test]
    async fn oversized_header_is_rejected_before_payload_read() {
        let bytes = u32::try_from(ABSOLUTE_HANDSHAKE_FRAME_LIMIT + 1)
            .expect("test limit fits u32")
            .to_be_bytes();
        let error = read_frame(&mut &bytes[..], ABSOLUTE_HANDSHAKE_FRAME_LIMIT)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), HostFailureKind::MessageTooLarge);
        let negotiated = 32 * 1024;
        let bytes = u32::try_from(negotiated + 1)
            .expect("test limit fits u32")
            .to_be_bytes();
        let error = read_frame(&mut &bytes[..], negotiated).await.unwrap_err();
        assert_eq!(error.kind(), HostFailureKind::MessageTooLarge);
    }

    #[tokio::test]
    async fn dropped_graceful_cleanup_force_kills_and_reaps_child() {
        let _test_lock = PROCESS_ACTOR_TEST_LOCK.lock().await;
        let baseline = actor_counts();
        let observation =
            std::env::temp_dir().join(format!("cordis-force-reap-{}.txt", std::process::id()));
        let host = ProcessHost::new(fixture_path())
            .arg("ignore_shutdown")
            .arg(observation.as_os_str());
        let (client, owner) = SessionOwner::spawn(&host).unwrap();
        let negotiated = client.handshake().await.unwrap();
        client.apply_negotiated(negotiated).unwrap();
        let (route, _) = client
            .load(PluginArtifact::new("fixture", PluginRevision(1), []))
            .await
            .unwrap();
        let cleanup = tokio::spawn(owner.cleanup(route));
        let pid = wait_observation(&observation, "shutdown").await;
        cleanup.abort();
        let _ = cleanup.await;
        wait_process_exit(pid).await;
        wait_actor_counts(baseline).await;
        let _ = std::fs::remove_file(observation);
    }

    #[tokio::test]
    async fn pending_map_dispatches_out_of_order_responses() {
        let _test_lock = PROCESS_ACTOR_TEST_LOCK.lock().await;
        let host = ProcessHost::new(fixture_path()).arg("out_of_order_start");
        let (client, owner) = SessionOwner::spawn(&host).unwrap();
        let negotiated = client.handshake().await.unwrap();
        client.apply_negotiated(negotiated).unwrap();
        let (route, _) = client
            .load(PluginArtifact::new("fixture", PluginRevision(1), []))
            .await
            .unwrap();
        let (first, second) = tokio::join!(client.start(route), client.start(route));
        first.unwrap();
        second.unwrap();
        owner.cleanup(route).await.unwrap();
    }

    #[tokio::test]
    async fn peer_smaller_inflight_limit_is_actively_enforced() {
        let _test_lock = PROCESS_ACTOR_TEST_LOCK.lock().await;
        exercise_negotiated_concurrency("peer_inflight_4", 64, 4, 4, 8).await;
    }

    #[tokio::test]
    async fn local_smaller_inflight_limit_remains_authoritative() {
        let _test_lock = PROCESS_ACTOR_TEST_LOCK.lock().await;
        exercise_negotiated_concurrency("peer_inflight_64", 4, 4, 4, 8).await;
    }

    #[tokio::test]
    async fn ten_thousand_out_of_order_responses_preserve_correlation() {
        let _test_lock = PROCESS_ACTOR_TEST_LOCK.lock().await;
        exercise_negotiated_concurrency("concurrent_out_of_order", 32, 32, 32, 320).await;
    }

    #[tokio::test]
    async fn fifty_sessions_and_sixteen_thousand_requests_leave_no_leaks() {
        let _test_lock = PROCESS_ACTOR_TEST_LOCK.lock().await;
        let baseline = actor_counts();
        for _ in 0..50 {
            exercise_negotiated_concurrency("concurrent_out_of_order", 32, 32, 32, 10).await;
        }
        wait_actor_counts(baseline).await;
    }

    #[tokio::test]
    async fn cancelled_requests_restore_negotiated_capacity() {
        let _test_lock = PROCESS_ACTOR_TEST_LOCK.lock().await;
        let observation = std::env::temp_dir().join(format!(
            "cordis-negotiated-cancel-{}.txt",
            std::process::id()
        ));
        let config = ProcessHostConfig {
            max_inflight_requests: 64,
            ..ProcessHostConfig::default()
        };
        let host = ProcessHost::with_config(fixture_path(), config)
            .arg("stall_start_inflight_4")
            .arg(observation.as_os_str());
        let (client, owner) = SessionOwner::spawn(&host).unwrap();
        let negotiated = client.handshake().await.unwrap();
        client.apply_negotiated(negotiated).unwrap();
        let (route, _) = client
            .load(PluginArtifact::new("fixture", PluginRevision(1), []))
            .await
            .unwrap();
        for _ in 0..2_500 {
            let mut requests = Vec::new();
            for _ in 0..4 {
                requests.push(tokio::spawn({
                    let client = client.clone();
                    async move { client.start(route).await }
                }));
            }
            wait_pending(&client, 4).await;
            for request in requests {
                request.abort();
                let _ = request.await;
            }
            wait_pending(&client, 0).await;
        }
        assert_eq!(client.0.permits.available_permits(), 4);
        owner.cleanup(route).await.unwrap();
        let _ = std::fs::remove_file(observation);
    }

    #[tokio::test]
    async fn wrong_response_kind_from_real_peer_is_terminal() {
        let _test_lock = PROCESS_ACTOR_TEST_LOCK.lock().await;
        let host = ProcessHost::new(fixture_path()).arg("wrong_load_response_kind");
        let Err(error) = host
            .load(PluginArtifact::new("fixture", PluginRevision(1), []))
            .await
        else {
            panic!("wrong response kind unexpectedly loaded a plugin");
        };
        assert!(matches!(
            error,
            CordisError::Host(error) if error.kind() == HostFailureKind::ProtocolViolation
        ));
    }

    #[tokio::test]
    async fn spawn_handoff_error_cannot_leave_unowned_child() {
        let observation =
            std::env::temp_dir().join(format!("cordis-spawn-guard-{}.txt", std::process::id()));
        let mut command = Command::new(fixture_path());
        command
            .arg("stall_handshake")
            .arg(observation.as_os_str())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        let child = command.spawn().unwrap();
        let guard = SpawnGuard(Some(child));
        let pid = wait_observation(&observation, "handshake").await;
        drop(guard);
        wait_process_exit(pid).await;
        let _ = std::fs::remove_file(observation);
    }

    #[tokio::test]
    async fn five_hundred_sessions_leave_no_actor_or_pending_leak() {
        let _test_lock = PROCESS_ACTOR_TEST_LOCK.lock().await;
        let baseline = (
            READER_ACTORS.load(Ordering::Acquire),
            WRITER_ACTORS.load(Ordering::Acquire),
            SUPERVISOR_ACTORS.load(Ordering::Acquire),
        );
        for _ in 0..500 {
            let host = ProcessHost::new(fixture_path()).arg("normal");
            let (client, owner) = SessionOwner::spawn(&host).unwrap();
            let negotiated = client.handshake().await.unwrap();
            client.apply_negotiated(negotiated).unwrap();
            let (route, _) = client
                .load(PluginArtifact::new("fixture", PluginRevision(1), []))
                .await
                .unwrap();
            client.start(route).await.unwrap();
            owner.cleanup(route).await.unwrap();
            assert!(client.0.pending.lock().is_empty());
        }
        assert_eq!(READER_ACTORS.load(Ordering::Acquire), baseline.0);
        assert_eq!(WRITER_ACTORS.load(Ordering::Acquire), baseline.1);
        assert_eq!(SUPERVISOR_ACTORS.load(Ordering::Acquire), baseline.2);
    }

    #[tokio::test]
    async fn reader_writer_and_supervisor_exit_after_failure() {
        let _test_lock = PROCESS_ACTOR_TEST_LOCK.lock().await;
        let baseline = actor_counts();
        let host = ProcessHost::new(fixture_path()).arg("crash_during_handshake");
        assert!(
            host.load(PluginArtifact::new("fixture", PluginRevision(1), []))
                .await
                .is_err()
        );
        wait_actor_counts(baseline).await;
    }

    fn actor_counts() -> (usize, usize, usize) {
        (
            READER_ACTORS.load(Ordering::Acquire),
            WRITER_ACTORS.load(Ordering::Acquire),
            SUPERVISOR_ACTORS.load(Ordering::Acquire),
        )
    }

    async fn exercise_negotiated_concurrency(
        mode: &str,
        local_inflight: usize,
        effective_inflight: usize,
        workers: usize,
        iterations: usize,
    ) {
        let observation =
            std::env::temp_dir().join(format!("cordis-{mode}-{}.txt", std::process::id()));
        let config = ProcessHostConfig {
            max_inflight_requests: local_inflight,
            outbound_queue_capacity: local_inflight.max(32),
            ..ProcessHostConfig::default()
        };
        let host = ProcessHost::with_config(fixture_path(), config)
            .arg(mode)
            .arg(observation.as_os_str());
        let (client, owner) = SessionOwner::spawn(&host).unwrap();
        let negotiated = client.handshake().await.unwrap();
        client.apply_negotiated(negotiated).unwrap();
        assert_eq!(client.0.limits.read().inflight, effective_inflight);
        assert_eq!(client.0.permits.available_permits(), effective_inflight);
        let (route, _) = client
            .load(PluginArtifact::new("fixture", PluginRevision(1), []))
            .await
            .unwrap();
        let mut starts = tokio::task::JoinSet::new();
        for _ in 0..workers {
            let client = client.clone();
            starts.spawn(async move {
                for _ in 0..iterations {
                    client.start(route).await.unwrap();
                }
            });
        }
        while let Some(result) = starts.join_next().await {
            result.unwrap();
        }
        wait_observation(&observation, &format!("max-{effective_inflight}")).await;
        assert_eq!(client.0.state(), SessionState::Ready);
        assert!(client.0.pending.lock().is_empty());
        assert_eq!(client.0.permits.available_permits(), effective_inflight);
        owner.cleanup(route).await.unwrap();
        let _ = std::fs::remove_file(observation);
    }

    async fn wait_actor_counts(expected: (usize, usize, usize)) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while actor_counts() != expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("session actors did not converge");
    }

    fn test_limits(inflight: usize) -> Limits {
        Limits {
            frame: 1024,
            artifact: 512,
            request: 256,
            response: 256,
            inflight,
        }
    }

    fn test_session(
        state: SessionState,
        max_inflight: usize,
    ) -> (SessionClient, mpsc::Receiver<Outbound>) {
        let (outbound, receiver) = mpsc::channel(max_inflight);
        let inner = Arc::new(SessionInner {
            state: AtomicU8::new(state as u8),
            limits: RwLock::new(test_limits(max_inflight)),
            negotiated_features: AtomicU64::new(SUPPORTED_FEATURES),
            outbound,
            pending: Mutex::new(HashMap::new()),
            permits: Arc::new(Semaphore::new(max_inflight)),
            next_id: AtomicU64::new(1),
            highest_issued: AtomicU64::new(0),
            force: CancellationToken::new(),
            actor_stop: CancellationToken::new(),
            shutdown_acked: AtomicBool::new(false),
            max_control_bytes: 1024,
            terminal_error: Mutex::new(None),
            actors_done: AtomicBool::new(false),
            terminal_notify: tokio::sync::Notify::new(),
            live: Arc::new(AtomicBool::new(true)),
        });
        (SessionClient(inner), receiver)
    }

    async fn wait_pending(client: &SessionClient, count: usize) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while client.0.pending.lock().len() != count {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("pending registration timeout");
    }

    fn fixture_path() -> PathBuf {
        let mut path = std::env::current_exe().expect("current test executable");
        path.pop();
        if path.ends_with("deps") {
            path.pop();
        }
        path.push("cordis-process-host-fixture");
        path.set_extension(std::env::consts::EXE_EXTENSION);
        path
    }

    async fn wait_observation(path: &PathBuf, expected: &str) -> u32 {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Ok(value) = std::fs::read_to_string(path) {
                    if let Some((pid, event)) = value.split_once(':') {
                        if event == expected {
                            return pid.parse().unwrap();
                        }
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("fixture observation timeout")
    }

    async fn wait_process_exit(pid: u32) {
        tokio::time::timeout(Duration::from_secs(5), async move {
            while process_exists(pid) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("force-terminated child was not reaped");
    }

    #[cfg(windows)]
    fn process_exists(pid: u32) -> bool {
        let output = std::process::Command::new("tasklist")
            .args(["/FI", &format!("PID eq {pid}"), "/NH"])
            .output()
            .expect("tasklist");
        String::from_utf8_lossy(&output.stdout).contains(&pid.to_string())
    }

    #[cfg(unix)]
    fn process_exists(pid: u32) -> bool {
        std::process::Command::new("kill")
            .args(["-0", &pid.to_string()])
            .status()
            .is_ok_and(|status| status.success())
    }
}
