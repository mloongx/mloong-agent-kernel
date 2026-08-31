//! Private test peer for the reference `ProcessHost` protocol.

#[path = "../host/protocol.rs"]
mod protocol;

use cordis_core::{
    DependencyPolicy, HostError, HostFailureKind, InvocationKey, PluginDescriptor, PluginRevision,
    RemoteDomainError, ServiceKey,
};
use protocol::{
    FEATURE_CANCEL, FEATURE_DEADLINE, FEATURE_INVOCATION, InvocationDeclaration, InvokeOutcome,
    Limits, Message, PROTOCOL_MAJOR, PROTOCOL_MINOR, REQUIRED_FEATURES, SUPPORTED_FEATURES,
    WireFailure,
};
use std::{collections::HashMap, env, sync::Arc, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() {
    let mode = env::args().nth(1).unwrap_or_else(|| "normal".into());
    let observation = env::args().nth(2);
    observe(observation.as_ref(), "spawned");
    if mode == "stall_handshake" {
        observe(observation.as_ref(), "handshake");
        std::future::pending::<()>().await;
    }
    let mut limits = Limits {
        frame: 1024 * 1024,
        artifact: 512 * 1024,
        request: 256 * 1024,
        response: 256 * 1024,
        inflight: 64,
    };
    if mode == "peer_inflight_4" || mode == "stall_start_inflight_4" {
        limits.inflight = 4;
    }
    if mode == "oversized_frame_header" {
        use std::io::Write as _;
        let length = u32::try_from(protocol::ABSOLUTE_HANDSHAKE_FRAME_LIMIT + 1)
            .expect("handshake limit fits u32")
            .to_be_bytes();
        std::io::stdout()
            .write_all(&length)
            .expect("write oversized header");
        std::io::stdout().flush().expect("flush oversized header");
        std::future::pending::<()>().await;
    }
    let mut input = tokio::io::stdin();
    let output = Arc::new(Mutex::new(tokio::io::stdout()));
    let active_invocations = Arc::new(Mutex::new(HashMap::<u64, CancellationToken>::new()));
    let mut held_starts = Vec::new();
    let mut max_held_starts = 0;
    loop {
        let Some(payload) = read_frame(&mut input, 1024 * 1024).await else {
            return;
        };
        let Ok(message) = protocol::decode(&payload, limits) else {
            return;
        };
        let response = match message {
            Message::Hello { id, .. } => {
                if mode == "crash_during_handshake" {
                    return;
                }
                if mode == "duplicate_hello_response" {
                    let response = Message::HelloAck {
                        id,
                        major: PROTOCOL_MAJOR,
                        minor: PROTOCOL_MINOR,
                        supported_features: SUPPORTED_FEATURES,
                        required_features: REQUIRED_FEATURES,
                        limits,
                    };
                    write_frame(&output, &response, limits.frame).await;
                    write_frame(&output, &response, limits.frame).await;
                    continue;
                } else if mode == "pre_ready_message" {
                    Message::Disposed { id }
                } else {
                    Message::HelloAck {
                        id: if mode == "future_response_id" {
                            id + 1
                        } else {
                            id
                        },
                        major: if mode == "reject_version" {
                            2
                        } else {
                            PROTOCOL_MAJOR
                        },
                        minor: PROTOCOL_MINOR,
                        supported_features: match mode.as_str() {
                            "missing_feature" => 0,
                            "missing_invocation" => SUPPORTED_FEATURES & !FEATURE_INVOCATION,
                            "missing_cancel" => SUPPORTED_FEATURES & !FEATURE_CANCEL,
                            "missing_deadline" => SUPPORTED_FEATURES & !FEATURE_DEADLINE,
                            "unknown_optional_feature" => SUPPORTED_FEATURES | (1 << 63),
                            _ => SUPPORTED_FEATURES,
                        },
                        required_features: if mode == "unknown_required_feature" {
                            1 << 63
                        } else {
                            REQUIRED_FEATURES
                        },
                        limits: if matches!(
                            mode.as_str(),
                            "small_limits" | "oversized_negotiated_frame"
                        ) {
                            Limits {
                                frame: 32 * 1024,
                                artifact: 16 * 1024,
                                request: 8 * 1024,
                                response: 8 * 1024,
                                inflight: 4,
                            }
                        } else {
                            limits
                        },
                    }
                }
            }
            Message::Load { id, revision, .. } => {
                if mode == "crash_during_load" {
                    return;
                }
                if mode == "stall_load" {
                    observe(observation.as_ref(), "load");
                    std::future::pending::<()>().await;
                    unreachable!();
                }
                if mode == "oversized_negotiated_frame" {
                    let length = (32_u32 * 1024 + 1).to_be_bytes();
                    output
                        .lock()
                        .await
                        .write_all(&length)
                        .await
                        .expect("write negotiated oversized header");
                    output
                        .lock()
                        .await
                        .flush()
                        .await
                        .expect("flush negotiated oversized header");
                    std::future::pending::<()>().await;
                }
                if mode == "wrong_load_response_kind" {
                    Message::ShutdownAck { id }
                } else {
                    let dependency = (mode == "descriptor_with_service_dependency")
                        .then(|| ServiceKey::new("fixture", "required", 1));
                    let provision = (mode == "descriptor_with_service_provision")
                        .then(|| ServiceKey::new("fixture", "provided", 1));
                    Message::Loaded {
                        id,
                        route: 7,
                        descriptor: PluginDescriptor {
                            name: if mode == "oversized_descriptor_name" {
                                Arc::from("x".repeat(5000))
                            } else {
                                Arc::from("process-fixture")
                            },
                            dependencies: dependency.into_iter().collect::<Vec<_>>().into(),
                            provisions: provision.into_iter().collect::<Vec<_>>().into(),
                            dependency_policy: DependencyPolicy::Restart,
                            revision: PluginRevision(revision),
                        },
                    }
                }
            }
            Message::Start { id, .. } => {
                if mode == "crash_during_start" {
                    return;
                }
                if mode == "stall_start_inflight_4" {
                    observe(observation.as_ref(), "start");
                    continue;
                }
                let batch = match mode.as_str() {
                    "out_of_order_start" => Some(2),
                    "peer_inflight_4" | "peer_inflight_64" => Some(4),
                    "concurrent_out_of_order" => Some(32),
                    _ => None,
                };
                if let Some(batch) = batch {
                    held_starts.push(id);
                    max_held_starts = max_held_starts.max(held_starts.len());
                    observe(observation.as_ref(), &format!("max-{max_held_starts}"));
                    if held_starts.len() == batch {
                        for id in held_starts.drain(..).rev() {
                            write_frame(
                                &output,
                                &Message::Started {
                                    id,
                                    invocations: Vec::new(),
                                },
                                limits.frame,
                            )
                            .await;
                        }
                    }
                    continue;
                } else if mode == "start_domain_error" {
                    Message::Failure {
                        id,
                        failure: WireFailure::Domain(RemoteDomainError::new(
                            "fixture.start",
                            "remote start rejected",
                        )),
                    }
                } else {
                    Message::Started {
                        id,
                        invocations: invocation_declarations(&mode),
                    }
                }
            }
            Message::Invoke {
                id,
                format,
                bytes,
                remaining_nanos,
                ..
            } => {
                observe(observation.as_ref(), "invoke");
                if mode == "crash_during_invoke" {
                    return;
                }
                if mode == "delayed_crash_invoke" {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    return;
                }
                if matches!(
                    mode.as_str(),
                    "delayed_invoke"
                        | "out_of_order_invoke"
                        | "deadline_enforced"
                        | "remote_old_delayed"
                ) {
                    let token = CancellationToken::new();
                    let mut active = active_invocations.lock().await;
                    if active.len() >= limits.inflight {
                        drop(active);
                        write_frame(
                            &output,
                            &Message::InvokeResult {
                                id,
                                outcome: InvokeOutcome::HostFailure(HostError::new(
                                    HostFailureKind::Overloaded,
                                    "fixture invocation capacity exhausted",
                                )),
                            },
                            limits.frame,
                        )
                        .await;
                        continue;
                    }
                    active.insert(id, token.clone());
                    drop(active);
                    let active = Arc::clone(&active_invocations);
                    let writer = Arc::clone(&output);
                    let mode = mode.clone();
                    tokio::spawn(async move {
                        let delay = if mode == "out_of_order_invoke" {
                            Duration::from_millis(5 - id % 5)
                        } else if mode == "deadline_enforced" {
                            Duration::from_nanos(remaining_nanos.saturating_add(1))
                        } else {
                            Duration::from_millis(200)
                        };
                        tokio::select! {
                            () = token.cancelled() => {}
                            () = tokio::time::sleep(delay) => {
                                let response = Message::InvokeResult {
                                    id,
                                    outcome: InvokeOutcome::Success {
                                        format,
                                        bytes: if mode == "remote_old_delayed" {
                                            Arc::from(&b"old"[..])
                                        } else {
                                            bytes
                                        },
                                    },
                                };
                                write_frame(&writer, &response, limits.frame).await;
                            }
                        }
                        active.lock().await.remove(&id);
                    });
                    continue;
                }
                let outcome = match mode.as_str() {
                    "remote_domain" => InvokeOutcome::RemoteDomain(
                        RemoteDomainError::new("fixture.domain", "remote domain failure")
                            .with_details("application/test", b"details".to_vec()),
                    ),
                    "unsupported_format" => InvokeOutcome::HostFailure(HostError::new(
                        HostFailureKind::UnsupportedFormat,
                        "fixture does not support request format",
                    )),
                    "protocol_failure_invoke" => InvokeOutcome::HostFailure(HostError::new(
                        HostFailureKind::ProtocolViolation,
                        "fixture reported a protocol violation",
                    )),
                    "oversized_invoke_response" => InvokeOutcome::Success {
                        format,
                        bytes: Arc::from(vec![0_u8; limits.response + 1]),
                    },
                    "remote_old" => InvokeOutcome::Success {
                        format,
                        bytes: Arc::from(&b"old"[..]),
                    },
                    "remote_new" => InvokeOutcome::Success {
                        format,
                        bytes: Arc::from(&b"new"[..]),
                    },
                    _ => InvokeOutcome::Success { format, bytes },
                };
                Message::InvokeResult { id, outcome }
            }
            Message::Cancel { id } => {
                observe(observation.as_ref(), "cancel");
                if let Some(token) = active_invocations.lock().await.remove(&id) {
                    token.cancel();
                }
                continue;
            }
            Message::Dispose { id, .. } => {
                if mode == "crash_during_dispose" {
                    return;
                }
                if mode == "dispose_domain_error" {
                    Message::Failure {
                        id,
                        failure: WireFailure::Domain(RemoteDomainError::new(
                            "fixture.dispose",
                            "remote dispose rejected",
                        )),
                    }
                } else {
                    Message::Disposed { id }
                }
            }
            Message::Shutdown { id } => {
                observe(observation.as_ref(), "shutdown");
                if mode == "exit_after_shutdown_without_ack" {
                    std::process::exit(17);
                }
                if mode == "ignore_shutdown" {
                    std::future::pending::<()>().await;
                    unreachable!();
                }
                write_frame(&output, &Message::ShutdownAck { id }, limits.frame).await;
                return;
            }
            other => Message::Failure {
                id: other.id(),
                failure: WireFailure::Host(HostError::new(
                    HostFailureKind::ProtocolViolation,
                    "unexpected fixture request",
                )),
            },
        };
        write_frame(&output, &response, limits.frame).await;
        if mode == "crash_after_start" && matches!(response, Message::Started { .. }) {
            return;
        }
    }
}

fn observe(path: Option<&String>, event: &str) {
    if let Some(path) = path {
        std::fs::write(path, format!("{}:{event}", std::process::id()))
            .expect("write fixture observation");
    }
}

async fn read_frame(reader: &mut (impl AsyncReadExt + Unpin), limit: usize) -> Option<Vec<u8>> {
    let mut header = [0_u8; 4];
    reader.read_exact(&mut header).await.ok()?;
    let length = u32::from_be_bytes(header) as usize;
    if length > limit {
        return None;
    }
    let mut payload = vec![0; length];
    reader.read_exact(&mut payload).await.ok()?;
    Some(payload)
}

fn invocation_declarations(mode: &str) -> Vec<InvocationDeclaration> {
    let declares = matches!(
        mode,
        "nonempty_invocation"
            | "remote_echo"
            | "remote_domain"
            | "unsupported_format"
            | "oversized_invoke_response"
            | "crash_during_invoke"
            | "delayed_crash_invoke"
            | "crash_after_start"
            | "protocol_failure_invoke"
            | "delayed_invoke"
            | "out_of_order_invoke"
            | "deadline_enforced"
            | "remote_old"
            | "remote_old_delayed"
            | "remote_new"
            | "missing_invocation"
            | "missing_cancel"
            | "missing_deadline"
            | "unknown_optional_feature"
    );
    declares
        .then(|| InvocationDeclaration {
            route: 70,
            key: InvocationKey::new("fixture", "echo", 1),
        })
        .into_iter()
        .collect()
}

async fn write_frame(writer: &Arc<Mutex<tokio::io::Stdout>>, message: &Message, limit: usize) {
    let payload = protocol::encode(message, limit).expect("encode fixture response");
    let mut writer = writer.lock().await;
    writer
        .write_all(
            &u32::try_from(payload.len())
                .expect("fixture payload fits u32")
                .to_be_bytes(),
        )
        .await
        .expect("write frame header");
    writer
        .write_all(&payload)
        .await
        .expect("write frame payload");
    writer.flush().await.expect("flush frame");
}
