//! Typed invalidation commands over the compatible notification envelope.
//!
//! Public `BackplaneMessage` literals and legacy custom backplanes remain valid.
//! Control commands carry a versioned, escaped source frame; ordinary key names
//! never select control behavior. Receivers explicitly decode that boundary.

use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, watch};

use crate::error::Result;
use crate::options::KeyModifierMode;
use crate::tags::{CacheScope, MarkerError, MarkerKind, MarkerVersion, StoredMarker, Tag};
use crate::time::Timestamp;

const CONTROL_PREFIX: &str = "\u{1f}amalgam-control-v2:";

/// The existing data notification discriminants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackplaneAction {
    /// Re-pull a newly written value from L2.
    Set,
    /// Hard-remove the corresponding local entry.
    Remove,
    /// Logically expire a corresponding local entry.
    Expire,
}

/// Compatible data notification DTO. Control decoding is a separate boundary.
#[derive(Debug, Clone)]
pub struct BackplaneMessage {
    /// The publishing instance's identity.
    pub source_id: Arc<str>,
    /// The original operation's revision.
    pub timestamp: Timestamp,
    /// The data mutation.
    pub action: BackplaneAction,
    /// The prefixed data key.
    pub key: Arc<str>,
}

/// A validated control notification with an explicit namespace and marker kind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarkerCommand {
    source_id: Arc<str>,
    scope: CacheScope,
    marker: StoredMarker,
}

impl MarkerCommand {
    /// Validates a source identity before creating a typed control command.
    pub fn new(
        source_id: impl AsRef<str>,
        scope: CacheScope,
        marker: StoredMarker,
    ) -> std::result::Result<Self, MarkerError> {
        if source_id.as_ref().trim().is_empty() {
            return Err(MarkerError::Protocol {
                detail: "control source must not be blank".into(),
            });
        }
        Ok(Self {
            source_id: Arc::from(source_id.as_ref()),
            scope,
            marker,
        })
    }

    /// The original source, preserved even when it contains protocol delimiters.
    #[must_use]
    pub fn source_id(&self) -> &str {
        &self.source_id
    }

    /// The affected value namespace.
    #[must_use]
    pub fn scope(&self) -> &CacheScope {
        &self.scope
    }

    /// The durable maximum being announced.
    #[must_use]
    pub fn marker(&self) -> &StoredMarker {
        &self.marker
    }
}

/// Closed notification commands, independent from legacy data action variants.
#[derive(Debug, Clone)]
pub enum BackplaneCommand {
    /// An ordinary data-key operation.
    Data(BackplaneMessage),
    /// An explicit namespaced invalidation marker.
    Marker(MarkerCommand),
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind")]
enum SourceFrame {
    Data {
        source: String,
    },
    Marker {
        source: String,
        prefix: String,
        version: String,
        modifier: u8,
        marker: WireMarker,
        ticks: i64,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", content = "tag")]
enum WireMarker {
    Tag(String),
    ClearExpire,
    ClearRemove,
}

impl BackplaneCommand {
    /// Returns the logical source identity after boundary decoding.
    #[must_use]
    pub fn source_id(&self) -> &str {
        match self {
            Self::Data(message) => &message.source_id,
            Self::Marker(command) => command.source_id(),
        }
    }

    /// Encodes a command into the existing public envelope.
    pub fn into_message(self) -> std::result::Result<BackplaneMessage, MarkerError> {
        match self {
            Self::Data(mut message) => {
                if message.source_id.starts_with(CONTROL_PREFIX) {
                    message.source_id = encode_source(&SourceFrame::Data {
                        source: message.source_id.to_string(),
                    })?
                    .into();
                }
                Ok(message)
            }
            Self::Marker(command) => {
                let scope = command.scope();
                let modifier = modifier_byte(scope.modifier());
                let marker = match command.marker().kind() {
                    MarkerKind::Tag(tag) => WireMarker::Tag(tag.as_str().into()),
                    MarkerKind::ClearExpire => WireMarker::ClearExpire,
                    MarkerKind::ClearRemove => WireMarker::ClearRemove,
                };
                let timestamp = command.marker().version().timestamp();
                let source_id = encode_source(&SourceFrame::Marker {
                    source: command.source_id().into(),
                    prefix: scope.prefix().into(),
                    version: scope.wire_version().into(),
                    modifier,
                    marker,
                    ticks: timestamp.ticks(),
                })?
                .into();
                Ok(BackplaneMessage {
                    source_id,
                    timestamp,
                    action: BackplaneAction::Set,
                    key: scope.storage_id().into(),
                })
            }
        }
    }

    /// Decodes a typed control or a legacy data notification exactly once.
    pub fn from_message(message: BackplaneMessage) -> std::result::Result<Self, MarkerError> {
        let Some(encoded) = message.source_id.strip_prefix(CONTROL_PREFIX) else {
            return Ok(Self::Data(message));
        };
        let bytes = decode_hex(encoded)?;
        let frame: SourceFrame = serde_json::from_slice(&bytes).map_err(MarkerError::protocol)?;
        match frame {
            SourceFrame::Data { source } => Ok(Self::Data(BackplaneMessage {
                source_id: source.into(),
                ..message
            })),
            SourceFrame::Marker {
                source,
                prefix,
                version,
                modifier,
                marker,
                ticks,
            } => {
                let modifier = modifier_from_byte(modifier)?;
                let scope = CacheScope::new(prefix, version, modifier)?;
                let kind = match marker {
                    WireMarker::Tag(tag) => {
                        MarkerKind::Tag(Tag::new(tag).map_err(MarkerError::protocol)?)
                    }
                    WireMarker::ClearExpire => MarkerKind::ClearExpire,
                    WireMarker::ClearRemove => MarkerKind::ClearRemove,
                };
                if message.timestamp.ticks() != ticks
                    || message.key.as_ref() != scope.storage_id()
                    || message.action != BackplaneAction::Set
                {
                    return Err(MarkerError::Protocol {
                        detail: "control envelope disagrees with its typed frame".into(),
                    });
                }
                let marker =
                    StoredMarker::new(kind, MarkerVersion::new(Timestamp::from_ticks(ticks)));
                MarkerCommand::new(source, scope, marker).map(Self::Marker)
            }
        }
    }
}

fn encode_source(frame: &SourceFrame) -> std::result::Result<String, MarkerError> {
    let bytes = serde_json::to_vec(frame).map_err(MarkerError::protocol)?;
    Ok(format!("{CONTROL_PREFIX}{}", encode_hex(&bytes)))
}

pub(crate) fn protocol_error(error: impl std::fmt::Display) -> MarkerError {
    MarkerError::Protocol {
        detail: error.to_string(),
    }
}

pub(crate) fn modifier_byte(modifier: KeyModifierMode) -> u8 {
    match modifier {
        KeyModifierMode::Prefix => 1,
        KeyModifierMode::Suffix => 2,
        KeyModifierMode::None => 3,
    }
}

fn modifier_from_byte(byte: u8) -> std::result::Result<KeyModifierMode, MarkerError> {
    match byte {
        1 => Ok(KeyModifierMode::Prefix),
        2 => Ok(KeyModifierMode::Suffix),
        3 => Ok(KeyModifierMode::None),
        value => Err(protocol_error(format!(
            "unknown namespace modifier {value}"
        ))),
    }
}

pub(crate) fn encode_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        encoded.push(DIGITS[usize::from(byte >> 4)] as char);
        encoded.push(DIGITS[usize::from(byte & 15)] as char);
    }
    encoded
}

pub(crate) fn decode_hex(encoded: &str) -> std::result::Result<Vec<u8>, MarkerError> {
    if !encoded.len().is_multiple_of(2) {
        return Err(protocol_error("odd hex frame length"));
    }
    encoded
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| {
            let high = (pair[0] as char)
                .to_digit(16)
                .ok_or_else(|| protocol_error("invalid hex frame"))?;
            let low = (pair[1] as char)
                .to_digit(16)
                .ok_or_else(|| protocol_error("invalid hex frame"))?;
            Ok(((high << 4) | low) as u8)
        })
        .collect()
}

/// A monotonic connection-continuity identity which never silently wraps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ContinuityEpoch(u64);

impl ContinuityEpoch {
    /// The initial acknowledged subscription epoch.
    pub const INITIAL: Self = Self(1);

    /// Advances continuity, rejecting exhaustion explicitly.
    pub fn next(self) -> std::result::Result<Self, MarkerError> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or_else(|| protocol_error("backplane continuity epoch exhausted"))
    }

    /// Diagnostic monotonic sequence number.
    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }
}

/// Latest connection state, retained even when a listener subscribes late.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackplaneState {
    /// The configured channel has an acknowledged subscription.
    Connected {
        /// Current continuity epoch.
        epoch: ContinuityEpoch,
    },
    /// Messages may have been missed; reconciliation is required.
    Disconnected {
        /// Last acknowledged continuity epoch.
        epoch: ContinuityEpoch,
    },
    /// This provider has permanently stopped.
    Stopped,
}

/// An extensible notification provider. Legacy methods remain required unchanged.
#[allow(
    clippy::double_must_use,
    reason = "async-trait 0.1.89 emits must_use on boxed futures"
)]
#[async_trait]
pub trait Backplane: Send + Sync {
    /// Publishes an existing data envelope.
    async fn publish(&self, message: BackplaneMessage) -> Result<()>;

    /// Receives data envelopes; core decodes typed commands at the boundary.
    fn subscribe(&self) -> broadcast::Receiver<BackplaneMessage>;

    /// Publishes a typed command through a compatibility-preserving envelope.
    async fn publish_command(&self, command: BackplaneCommand) -> Result<()> {
        self.publish(command.into_message()?).await
    }

    /// Supplies retained continuity state; legacy providers explicitly lack it.
    fn connection_state(&self) -> Option<watch::Receiver<BackplaneState>> {
        None
    }

    /// Explicitly closes a provider's owned resources. Legacy providers own none.
    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }
}

/// Bounded in-process reference notifications with retained health state.
#[derive(Clone)]
pub struct InProcessBackplane {
    sender: broadcast::Sender<BackplaneMessage>,
    state: watch::Sender<BackplaneState>,
}

impl InProcessBackplane {
    /// Creates a bounded reference stream (zero is the legacy minimum-one adapter).
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity.max(1));
        let (state, _) = watch::channel(BackplaneState::Connected {
            epoch: ContinuityEpoch::INITIAL,
        });
        Self { sender, state }
    }
}

impl Default for InProcessBackplane {
    fn default() -> Self {
        Self::with_capacity(256)
    }
}

#[async_trait]
impl Backplane for InProcessBackplane {
    async fn publish(&self, message: BackplaneMessage) -> Result<()> {
        let _ = self.sender.send(message);
        Ok(())
    }
    fn subscribe(&self) -> broadcast::Receiver<BackplaneMessage> {
        self.sender.subscribe()
    }
    fn connection_state(&self) -> Option<watch::Receiver<BackplaneState>> {
        Some(self.state.subscribe())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error as _;

    #[test]
    fn malformed_typed_control_keeps_the_original_json_error() {
        let message = BackplaneMessage {
            source_id: format!("{CONTROL_PREFIX}{}", encode_hex(b"{")).into(),
            timestamp: Timestamp::MIN,
            action: BackplaneAction::Set,
            key: "control".into(),
        };
        let error = BackplaneCommand::from_message(message).unwrap_err();
        assert!(matches!(error, MarkerError::ProtocolWithSource { .. }));
        assert!(
            error
                .source()
                .unwrap()
                .downcast_ref::<serde_json::Error>()
                .is_some()
        );
    }

    #[test]
    fn reserved_source_and_magic_data_key_stay_data() {
        let message = BackplaneMessage {
            source_id: format!("{CONTROL_PREFIX}garbage|source").into(),
            timestamp: Timestamp::MIN,
            action: BackplaneAction::Set,
            key: "__amalgam:clear:remove".into(),
        };
        let decoded = BackplaneCommand::from_message(
            BackplaneCommand::Data(message.clone())
                .into_message()
                .unwrap(),
        )
        .unwrap();
        let BackplaneCommand::Data(decoded) = decoded else {
            panic!("ordinary key became control")
        };
        assert_eq!(decoded.source_id, message.source_id);
        assert_eq!(decoded.key, message.key);
    }
}
