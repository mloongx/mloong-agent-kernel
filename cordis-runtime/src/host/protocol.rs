//! Private reference-process wire format. This is not a Cordis Host ABI.

use cordis_core::{
    DependencyPolicy, HostError, HostFailureKind, InvocationKey, PluginDescriptor, PluginRevision,
    RemoteDomainError, ServiceKey,
};
use std::sync::Arc;

pub(crate) const ABSOLUTE_HANDSHAKE_FRAME_LIMIT: usize = 64 * 1024;
pub(crate) const PROTOCOL_MAJOR: u16 = 1;
pub(crate) const PROTOCOL_MINOR: u16 = 1;
pub(crate) const FEATURE_LIFECYCLE: u64 = 1;
pub(crate) const FEATURE_INVOCATION: u64 = 1 << 1;
pub(crate) const FEATURE_CANCEL: u64 = 1 << 2;
pub(crate) const FEATURE_DEADLINE: u64 = 1 << 3;
pub(crate) const SUPPORTED_FEATURES: u64 =
    FEATURE_LIFECYCLE | FEATURE_INVOCATION | FEATURE_CANCEL | FEATURE_DEADLINE;
pub(crate) const REQUIRED_FEATURES: u64 = FEATURE_LIFECYCLE;
pub(crate) const MAX_DESCRIPTOR_STRING: usize = 4 * 1024;
pub(crate) const MAX_DESCRIPTOR_ITEMS: usize = 256;
pub(crate) const MAX_INVOCATION_DECLARATIONS: usize = 256;

#[derive(Clone, Debug)]
pub(crate) struct InvocationDeclaration {
    pub(crate) route: u64,
    pub(crate) key: InvocationKey,
}

#[derive(Debug)]
pub(crate) enum InvokeOutcome {
    Success { format: Arc<str>, bytes: Arc<[u8]> },
    RemoteDomain(RemoteDomainError),
    HostFailure(HostError),
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct Limits {
    pub(crate) frame: usize,
    pub(crate) artifact: usize,
    pub(crate) request: usize,
    pub(crate) response: usize,
    pub(crate) inflight: usize,
}

#[derive(Debug)]
pub(crate) enum Message {
    Hello {
        id: u64,
        major: u16,
        minor: u16,
        supported_features: u64,
        required_features: u64,
        limits: Limits,
    },
    HelloAck {
        id: u64,
        major: u16,
        minor: u16,
        supported_features: u64,
        required_features: u64,
        limits: Limits,
    },
    Load {
        id: u64,
        format: Arc<str>,
        revision: u64,
        payload: Arc<[u8]>,
    },
    Loaded {
        id: u64,
        route: u64,
        descriptor: PluginDescriptor,
    },
    Start {
        id: u64,
        route: u64,
    },
    Started {
        id: u64,
        invocations: Vec<InvocationDeclaration>,
    },
    Invoke {
        id: u64,
        route: u64,
        format: Arc<str>,
        bytes: Arc<[u8]>,
        remaining_nanos: u64,
    },
    InvokeResult {
        id: u64,
        outcome: InvokeOutcome,
    },
    Cancel {
        id: u64,
    },
    Dispose {
        id: u64,
        route: u64,
    },
    Disposed {
        id: u64,
    },
    Shutdown {
        id: u64,
    },
    ShutdownAck {
        id: u64,
    },
    Failure {
        id: u64,
        failure: WireFailure,
    },
}

#[derive(Debug)]
pub(crate) enum WireFailure {
    Host(HostError),
    Domain(RemoteDomainError),
}

impl Message {
    pub(crate) fn id(&self) -> u64 {
        match self {
            Self::Hello { id, .. }
            | Self::HelloAck { id, .. }
            | Self::Load { id, .. }
            | Self::Loaded { id, .. }
            | Self::Start { id, .. }
            | Self::Started { id, .. }
            | Self::Invoke { id, .. }
            | Self::InvokeResult { id, .. }
            | Self::Cancel { id }
            | Self::Dispose { id, .. }
            | Self::Disposed { id }
            | Self::Shutdown { id }
            | Self::ShutdownAck { id }
            | Self::Failure { id, .. } => *id,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn is_control(&self) -> bool {
        !matches!(
            self,
            Self::Load { .. }
                | Self::Loaded { .. }
                | Self::Invoke { .. }
                | Self::InvokeResult { .. }
        )
    }
}

#[allow(dead_code)]
pub(crate) fn payload_is_control(payload: &[u8]) -> bool {
    !matches!(payload.first(), Some(3 | 4 | 12 | 13))
}

fn violation(message: impl Into<Arc<str>>) -> HostError {
    HostError::new(HostFailureKind::ProtocolViolation, message)
}

struct Encoder(Vec<u8>);
impl Encoder {
    fn u8(&mut self, value: u8) {
        self.0.push(value);
    }
    fn u16(&mut self, value: u16) {
        self.0.extend_from_slice(&value.to_be_bytes());
    }
    fn u32(&mut self, value: usize) -> Result<(), HostError> {
        let value = u32::try_from(value).map_err(|_| violation("wire length exceeds u32"))?;
        self.0.extend_from_slice(&value.to_be_bytes());
        Ok(())
    }
    fn u64(&mut self, value: u64) {
        self.0.extend_from_slice(&value.to_be_bytes());
    }
    fn bytes(&mut self, value: &[u8]) -> Result<(), HostError> {
        self.u32(value.len())?;
        self.0.extend_from_slice(value);
        Ok(())
    }
    fn string(&mut self, value: &str) -> Result<(), HostError> {
        self.bytes(value.as_bytes())
    }
    fn limits(&mut self, value: Limits) -> Result<(), HostError> {
        self.u32(value.frame)?;
        self.u32(value.artifact)?;
        self.u32(value.request)?;
        self.u32(value.response)?;
        self.u32(value.inflight)
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) fn encode(message: &Message, limit: usize) -> Result<Vec<u8>, HostError> {
    let mut e = Encoder(Vec::new());
    match message {
        Message::Hello {
            id,
            major,
            minor,
            supported_features,
            required_features,
            limits,
        } => {
            e.u8(1);
            e.u64(*id);
            e.u16(*major);
            e.u16(*minor);
            e.u64(*supported_features);
            e.u64(*required_features);
            e.limits(*limits)?;
        }
        Message::HelloAck {
            id,
            major,
            minor,
            supported_features,
            required_features,
            limits,
        } => {
            e.u8(2);
            e.u64(*id);
            e.u16(*major);
            e.u16(*minor);
            e.u64(*supported_features);
            e.u64(*required_features);
            e.limits(*limits)?;
        }
        Message::Load {
            id,
            format,
            revision,
            payload,
        } => {
            e.u8(3);
            e.u64(*id);
            e.string(format)?;
            e.u64(*revision);
            e.bytes(payload)?;
        }
        Message::Loaded {
            id,
            route,
            descriptor,
        } => {
            e.u8(4);
            e.u64(*id);
            e.u64(*route);
            encode_descriptor(&mut e, descriptor)?;
        }
        Message::Start { id, route } => {
            e.u8(5);
            e.u64(*id);
            e.u64(*route);
        }
        Message::Started { id, invocations } => {
            e.u8(6);
            e.u64(*id);
            e.u32(invocations.len())?;
            for declaration in invocations {
                e.u64(declaration.route);
                encode_invocation_key(&mut e, &declaration.key)?;
            }
        }
        Message::Dispose { id, route } => {
            e.u8(7);
            e.u64(*id);
            e.u64(*route);
        }
        Message::Disposed { id } => {
            e.u8(8);
            e.u64(*id);
        }
        Message::Shutdown { id } => {
            e.u8(9);
            e.u64(*id);
        }
        Message::ShutdownAck { id } => {
            e.u8(10);
            e.u64(*id);
        }
        Message::Failure { id, failure } => {
            e.u8(11);
            e.u64(*id);
            match failure {
                WireFailure::Host(error) => {
                    e.u8(0);
                    e.u8(host_kind_code(error.kind()));
                    e.string(error.message())?;
                }
                WireFailure::Domain(error) => {
                    e.u8(1);
                    e.string(error.code())?;
                    e.string(error.message())?;
                    if let Some((format, payload)) = error.details() {
                        e.u8(1);
                        e.string(format)?;
                        e.bytes(payload)?;
                    } else {
                        e.u8(0);
                    }
                }
            }
        }
        Message::Invoke {
            id,
            route,
            format,
            bytes,
            remaining_nanos,
        } => {
            e.u8(12);
            e.u64(*id);
            e.u64(*route);
            e.string(format)?;
            e.bytes(bytes)?;
            e.u64(*remaining_nanos);
        }
        Message::InvokeResult { id, outcome } => {
            e.u8(13);
            e.u64(*id);
            match outcome {
                InvokeOutcome::Success { format, bytes } => {
                    e.u8(0);
                    e.string(format)?;
                    e.bytes(bytes)?;
                }
                InvokeOutcome::RemoteDomain(error) => {
                    e.u8(1);
                    encode_domain_error(&mut e, error)?;
                }
                InvokeOutcome::HostFailure(error) => {
                    e.u8(2);
                    e.u8(host_kind_code(error.kind()));
                    e.string(error.message())?;
                }
            }
        }
        Message::Cancel { id } => {
            e.u8(14);
            e.u64(*id);
        }
    }
    if e.0.len() > limit {
        return Err(HostError::new(
            HostFailureKind::MessageTooLarge,
            "encoded frame exceeds limit",
        ));
    }
    Ok(e.0)
}

fn encode_descriptor(e: &mut Encoder, d: &PluginDescriptor) -> Result<(), HostError> {
    e.string(&d.name)?;
    e.u32(d.dependencies.len())?;
    for key in d.dependencies.iter() {
        encode_key(e, key)?;
    }
    e.u32(d.provisions.len())?;
    for key in d.provisions.iter() {
        encode_key(e, key)?;
    }
    e.u8(match d.dependency_policy {
        DependencyPolicy::Restart => 0,
        DependencyPolicy::Dispose => 1,
    });
    e.u64(d.revision.0);
    Ok(())
}
fn encode_key(e: &mut Encoder, key: &ServiceKey) -> Result<(), HostError> {
    e.string(key.namespace())?;
    e.string(key.name())?;
    e.u32(key.version() as usize)
}

fn encode_invocation_key(e: &mut Encoder, key: &InvocationKey) -> Result<(), HostError> {
    e.string(key.namespace())?;
    e.string(key.name())?;
    e.u32(key.version() as usize)
}

fn encode_domain_error(e: &mut Encoder, error: &RemoteDomainError) -> Result<(), HostError> {
    e.string(error.code())?;
    e.string(error.message())?;
    if let Some((format, payload)) = error.details() {
        e.u8(1);
        e.string(format)?;
        e.bytes(payload)
    } else {
        e.u8(0);
        Ok(())
    }
}

struct Decoder<'a> {
    bytes: &'a [u8],
    at: usize,
}
impl<'a> Decoder<'a> {
    fn take(&mut self, count: usize) -> Result<&'a [u8], HostError> {
        let end = self
            .at
            .checked_add(count)
            .ok_or_else(|| violation("wire offset overflow"))?;
        let value = self
            .bytes
            .get(self.at..end)
            .ok_or_else(|| violation("truncated field"))?;
        self.at = end;
        Ok(value)
    }
    fn u8(&mut self) -> Result<u8, HostError> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, HostError> {
        Ok(u16::from_be_bytes(
            self.take(2)?
                .try_into()
                .map_err(|_| violation("truncated u16"))?,
        ))
    }
    fn u32(&mut self) -> Result<usize, HostError> {
        Ok(u32::from_be_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| violation("truncated u32"))?,
        ) as usize)
    }
    fn u64(&mut self) -> Result<u64, HostError> {
        Ok(u64::from_be_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| violation("truncated u64"))?,
        ))
    }
    fn bytes(&mut self, semantic_limit: usize) -> Result<Arc<[u8]>, HostError> {
        let len = self.u32()?;
        if len > semantic_limit {
            return Err(HostError::new(
                HostFailureKind::MessageTooLarge,
                "nested byte field exceeds limit",
            ));
        }
        Ok(Arc::from(self.take(len)?))
    }
    fn string(&mut self, semantic_limit: usize) -> Result<Arc<str>, HostError> {
        let bytes = self.bytes(semantic_limit)?;
        let value = std::str::from_utf8(&bytes).map_err(|_| violation("invalid UTF-8 string"))?;
        Ok(Arc::from(value))
    }
    fn limits(&mut self) -> Result<Limits, HostError> {
        Ok(Limits {
            frame: self.u32()?,
            artifact: self.u32()?,
            request: self.u32()?,
            response: self.u32()?,
            inflight: self.u32()?,
        })
    }
    fn finish(self) -> Result<(), HostError> {
        if self.at == self.bytes.len() {
            Ok(())
        } else {
            Err(violation("trailing frame data"))
        }
    }
}

pub(crate) fn decode(payload: &[u8], semantic: Limits) -> Result<Message, HostError> {
    let mut d = Decoder {
        bytes: payload,
        at: 0,
    };
    let tag = d.u8()?;
    let id = d.u64()?;
    let message = match tag {
        1 => Message::Hello {
            id,
            major: d.u16()?,
            minor: d.u16()?,
            supported_features: d.u64()?,
            required_features: d.u64()?,
            limits: d.limits()?,
        },
        2 => Message::HelloAck {
            id,
            major: d.u16()?,
            minor: d.u16()?,
            supported_features: d.u64()?,
            required_features: d.u64()?,
            limits: d.limits()?,
        },
        3 => Message::Load {
            id,
            format: d.string(MAX_DESCRIPTOR_STRING)?,
            revision: d.u64()?,
            payload: d.bytes(semantic.artifact)?,
        },
        4 => Message::Loaded {
            id,
            route: d.u64()?,
            descriptor: decode_descriptor(&mut d)?,
        },
        5 => Message::Start {
            id,
            route: d.u64()?,
        },
        6 => Message::Started {
            id,
            invocations: decode_invocation_declarations(&mut d)?,
        },
        7 => Message::Dispose {
            id,
            route: d.u64()?,
        },
        8 => Message::Disposed { id },
        9 => Message::Shutdown { id },
        10 => Message::ShutdownAck { id },
        11 => {
            let kind = d.u8()?;
            let failure = if kind == 0 {
                let hk = decode_host_kind(d.u8()?)?;
                WireFailure::Host(HostError::new(hk, d.string(MAX_DESCRIPTOR_STRING)?))
            } else if kind == 1 {
                let code = d.string(MAX_DESCRIPTOR_STRING)?;
                let message = d.string(MAX_DESCRIPTOR_STRING)?;
                let domain = if d.u8()? == 1 {
                    let format = d.string(MAX_DESCRIPTOR_STRING)?;
                    let payload = d.bytes(semantic.response)?;
                    RemoteDomainError::new(code, message).with_details(format, payload)
                } else {
                    RemoteDomainError::new(code, message)
                };
                WireFailure::Domain(domain)
            } else {
                return Err(violation("unknown failure category"));
            };
            Message::Failure { id, failure }
        }
        12 => Message::Invoke {
            id,
            route: d.u64()?,
            format: d.string(MAX_DESCRIPTOR_STRING)?,
            bytes: d.bytes(semantic.request)?,
            remaining_nanos: d.u64()?,
        },
        13 => {
            let outcome = match d.u8()? {
                0 => InvokeOutcome::Success {
                    format: d.string(MAX_DESCRIPTOR_STRING)?,
                    bytes: d.bytes(semantic.response)?,
                },
                1 => InvokeOutcome::RemoteDomain(decode_domain_error(&mut d, semantic.response)?),
                2 => InvokeOutcome::HostFailure(HostError::new(
                    decode_host_kind(d.u8()?)?,
                    d.string(MAX_DESCRIPTOR_STRING)?,
                )),
                _ => return Err(violation("unknown invocation outcome")),
            };
            Message::InvokeResult { id, outcome }
        }
        14 => Message::Cancel { id },
        _ => return Err(violation("unknown message tag")),
    };
    d.finish()?;
    Ok(message)
}

fn decode_descriptor(d: &mut Decoder<'_>) -> Result<PluginDescriptor, HostError> {
    let name = d.string(MAX_DESCRIPTOR_STRING)?;
    let dependencies = decode_keys(d)?;
    let provisions = decode_keys(d)?;
    let dependency_policy = match d.u8()? {
        0 => DependencyPolicy::Restart,
        1 => DependencyPolicy::Dispose,
        _ => return Err(violation("unknown dependency policy")),
    };
    Ok(PluginDescriptor {
        name,
        dependencies: Arc::from(dependencies),
        provisions: Arc::from(provisions),
        dependency_policy,
        revision: PluginRevision(d.u64()?),
    })
}
fn decode_keys(d: &mut Decoder<'_>) -> Result<Vec<ServiceKey>, HostError> {
    let count = d.u32()?;
    if count > MAX_DESCRIPTOR_ITEMS {
        return Err(HostError::new(
            HostFailureKind::MessageTooLarge,
            "descriptor array exceeds limit",
        ));
    }
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        let namespace = d.string(MAX_DESCRIPTOR_STRING)?;
        let name = d.string(MAX_DESCRIPTOR_STRING)?;
        let version = u32::try_from(d.u32()?).map_err(|_| violation("service version overflow"))?;
        values.push(ServiceKey::new(namespace, name, version));
    }
    Ok(values)
}

fn decode_invocation_declarations(
    d: &mut Decoder<'_>,
) -> Result<Vec<InvocationDeclaration>, HostError> {
    let count = d.u32()?;
    if count > MAX_INVOCATION_DECLARATIONS {
        return Err(HostError::new(
            HostFailureKind::MessageTooLarge,
            "invocation declaration array exceeds limit",
        ));
    }
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        let route = d.u64()?;
        let namespace = d.string(MAX_DESCRIPTOR_STRING)?;
        let name = d.string(MAX_DESCRIPTOR_STRING)?;
        let version =
            u32::try_from(d.u32()?).map_err(|_| violation("invocation version overflow"))?;
        values.push(InvocationDeclaration {
            route,
            key: InvocationKey::new(namespace, name, version),
        });
    }
    Ok(values)
}

fn decode_domain_error(
    d: &mut Decoder<'_>,
    payload_limit: usize,
) -> Result<RemoteDomainError, HostError> {
    let code = d.string(MAX_DESCRIPTOR_STRING)?;
    let message = d.string(MAX_DESCRIPTOR_STRING)?;
    Ok(if d.u8()? == 1 {
        let format = d.string(MAX_DESCRIPTOR_STRING)?;
        let payload = d.bytes(payload_limit)?;
        RemoteDomainError::new(code, message).with_details(format, payload)
    } else {
        RemoteDomainError::new(code, message)
    })
}

fn host_kind_code(kind: HostFailureKind) -> u8 {
    match kind {
        HostFailureKind::HandshakeIncompatible => 0,
        HostFailureKind::TransportClosed => 2,
        HostFailureKind::ProcessExited => 3,
        HostFailureKind::ProcessKilled => 4,
        HostFailureKind::MessageTooLarge => 5,
        HostFailureKind::UnsupportedFormat => 6,
        HostFailureKind::UnsupportedCapability => 7,
        HostFailureKind::Overloaded => 8,
        HostFailureKind::Unavailable => 9,
        _ => 1,
    }
}
fn decode_host_kind(code: u8) -> Result<HostFailureKind, HostError> {
    Ok(match code {
        0 => HostFailureKind::HandshakeIncompatible,
        1 => HostFailureKind::ProtocolViolation,
        2 => HostFailureKind::TransportClosed,
        3 => HostFailureKind::ProcessExited,
        4 => HostFailureKind::ProcessKilled,
        5 => HostFailureKind::MessageTooLarge,
        6 => HostFailureKind::UnsupportedFormat,
        7 => HostFailureKind::UnsupportedCapability,
        8 => HostFailureKind::Overloaded,
        9 => HostFailureKind::Unavailable,
        _ => return Err(violation("unknown host failure kind")),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> Limits {
        Limits {
            frame: 1024,
            artifact: 128,
            request: 128,
            response: 128,
            inflight: 4,
        }
    }

    #[test]
    fn rejects_unknown_tag_and_trailing_data() {
        let mut unknown = vec![99];
        unknown.extend_from_slice(&1_u64.to_be_bytes());
        assert_eq!(
            decode(&unknown, limits()).unwrap_err().kind(),
            HostFailureKind::ProtocolViolation
        );

        let mut trailing = encode(&Message::Disposed { id: 1 }, 1024).unwrap();
        trailing.push(0);
        assert_eq!(
            decode(&trailing, limits()).unwrap_err().kind(),
            HostFailureKind::ProtocolViolation
        );
    }

    #[test]
    fn rejects_truncated_and_oversized_nested_fields() {
        let mut truncated = vec![3];
        truncated.extend_from_slice(&1_u64.to_be_bytes());
        truncated.extend_from_slice(&5_u32.to_be_bytes());
        truncated.push(b'x');
        assert_eq!(
            decode(&truncated, limits()).unwrap_err().kind(),
            HostFailureKind::ProtocolViolation
        );

        let mut oversized = vec![3];
        oversized.extend_from_slice(&1_u64.to_be_bytes());
        oversized.extend_from_slice(&u32::MAX.to_be_bytes());
        assert_eq!(
            decode(&oversized, limits()).unwrap_err().kind(),
            HostFailureKind::MessageTooLarge
        );
    }
}
