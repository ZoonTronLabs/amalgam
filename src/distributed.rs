//! L2 distributed cache: the wire envelope, a serializer abstraction, the
//! [`DistributedCache`] backend trait, and an in-memory reference backend.
//!
//! FusionCache's L2 is any `IDistributedCache`; here it is any implementor of
//! [`DistributedCache`] (a byte-oriented key/value store with TTL). Values cross
//! the wire as a [`DistributedEntry`] — value plus the metadata needed to
//! reconstruct freshness on another node — encoded by a [`DistributedSerializer`].
//!
//! A Redis-backed backend is intentionally left to a feature-gated adapter; the
//! [`InMemoryDistributedCache`] here is a faithful reference used by tests and
//! single-process multi-instance scenarios.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use dashmap::DashMap;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::distributed_lock::{LeaseError, LeaseProof};
use crate::entry::Entry;
use crate::error::{Error, FactoryCancellationReason, Result};
use crate::execution::FactoryCancellation;
use crate::marker_snapshots::{
    MarkerSnapshot, MarkerSnapshotCache, MarkerSnapshotCacheError, MarkerSnapshotLimits,
    MarkerSnapshotRead, MarkerSnapshotRenewal,
};
use crate::options::{EntryOptions, EntryWeight, Priority};
use crate::tags::{
    CacheScope, MarkerAdvanceOutcome, MarkerError, MarkerKind, MarkerState, MarkerStoreLimits,
    MarkerVersion, StoredMarker, try_collect_tags,
};
use crate::time::{Clock, Timestamp};

/// The serializable L2 envelope: a value together with the metadata required to
/// rebuild its freshness/fail-safe state on any node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributedEntry<V> {
    /// The cached value.
    pub value: V,
    /// Creation tick (for tag-marker comparison).
    pub created_ticks: i64,
    /// Logical-expiration tick.
    pub logical_expiration_ticks: i64,
    /// Physical-expiration tick (fail-safe boundary).
    pub physical_expiration_ticks: i64,
    /// Whether the value came from a fail-safe activation.
    pub is_from_fail_safe: bool,
    /// Optional `ETag` for conditional refresh.
    pub etag: Option<String>,
    /// Optional `LastModified` tick for conditional refresh.
    pub last_modified_ticks: Option<i64>,
    /// The tags attached to the entry.
    pub tags: Vec<String>,
}

impl<V: Clone> DistributedEntry<V> {
    /// Captures an in-memory [`Entry`] as a wire envelope.
    #[must_use]
    pub fn from_entry(entry: &Entry<V>) -> Self {
        let meta = entry.meta();
        Self {
            value: entry.value_cloned(),
            created_ticks: meta.created().ticks(),
            logical_expiration_ticks: meta.logical_expiration().ticks(),
            physical_expiration_ticks: meta.physical_expiration().ticks(),
            is_from_fail_safe: meta.is_from_fail_safe(),
            etag: meta.etag().map(str::to_owned),
            last_modified_ticks: meta.last_modified().map(Timestamp::ticks),
            tags: meta.tags().iter().map(|t| t.as_str().to_owned()).collect(),
        }
    }

    /// Creates independent L2 deadlines anchored to actual fresh insertion.
    pub fn from_entry_with_options(
        entry: &Entry<V>,
        options: &EntryOptions,
        inserted_at: Timestamp,
    ) -> Result<Self> {
        options.validate()?;
        let mut snapshot = Self::from_entry(entry);
        let logical = inserted_at.saturating_add(options.resolved_distributed_duration());
        let physical = inserted_at.saturating_add(options.distributed_physical_ttl());
        if entry.meta().is_from_fail_safe() {
            snapshot.logical_expiration_ticks =
                logical.min(entry.meta().logical_expiration()).ticks();
            snapshot.physical_expiration_ticks =
                physical.min(entry.meta().physical_expiration()).ticks();
        } else {
            snapshot.logical_expiration_ticks = logical.ticks();
            snapshot.physical_expiration_ticks = physical.ticks();
        }
        snapshot.validate()?;
        Ok(snapshot)
    }

    /// Canonical wire hydration: validates tags and deadline invariants.
    pub fn try_into_entry(self, now: Timestamp) -> Result<Entry<V>> {
        let tags = try_collect_tags(self.tags)?;
        Entry::try_rehydrate(
            self.value,
            Timestamp::from_ticks(self.created_ticks),
            Timestamp::from_ticks(self.logical_expiration_ticks),
            Timestamp::from_ticks(self.physical_expiration_ticks),
            self.is_from_fail_safe,
            self.etag,
            self.last_modified_ticks.map(Timestamp::from_ticks),
            tags,
            now,
        )
    }

    /// Legacy infallible DTO adapter. Invalid manually constructed metadata is a
    /// contract violation; distributed input must use `try_into_entry`.
    #[must_use]
    pub fn into_entry(self, now: Timestamp) -> Entry<V> {
        match self.try_into_entry(now) {
            Ok(entry) => entry,
            Err(error) => panic!("invalid legacy distributed DTO: {error}"),
        }
    }
}

impl<V> DistributedEntry<V> {
    /// Validates protocol fields without requiring value cloning.
    pub fn validate(&self) -> Result<()> {
        if self.logical_expiration_ticks > self.physical_expiration_ticks {
            return Err(crate::ConfigError::InvalidEntryDeadlines.into());
        }
        for tag in &self.tags {
            crate::Tag::new(tag)?;
        }
        Ok(())
    }
}

/// Persisted retention information, absent in legacy codec payloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotRetention {
    /// A legacy snapshot lets per-read options supply retention metadata.
    Unspecified,
    /// Stored priority takes precedence; absent size may use caller fallback.
    Specified {
        /// Optional persisted entry weight.
        size: Option<EntryWeight>,
        /// Persisted eviction priority.
        priority: Priority,
    },
}

/// A V2 snapshot around the unchanged public positional codec DTO.
#[derive(Debug, Clone)]
pub struct DistributedSnapshot<V> {
    entry: DistributedEntry<V>,
    inserted_at: Timestamp,
    retention: SnapshotRetention,
}

impl<V> DistributedSnapshot<V> {
    /// Validates the legacy payload before attaching outer metadata.
    pub fn new(
        entry: DistributedEntry<V>,
        inserted_at: Timestamp,
        retention: SnapshotRetention,
    ) -> Result<Self> {
        entry.validate()?;
        Ok(Self {
            entry,
            inserted_at,
            retention,
        })
    }

    /// The opaque codec's unchanged DTO.
    #[must_use]
    pub fn entry(&self) -> &DistributedEntry<V> {
        &self.entry
    }

    /// The original insertion instant which anchored absolute L2 deadlines.
    #[must_use]
    pub const fn inserted_at(&self) -> Timestamp {
        self.inserted_at
    }

    /// Persisted capacity metadata.
    #[must_use]
    pub const fn retention(&self) -> SnapshotRetention {
        self.retention
    }

    /// Remaining source physical lifetime at a delayed write or retry.
    #[must_use]
    pub fn backend_ttl_at(&self, now: Timestamp) -> Duration {
        Timestamp::from_ticks(self.entry.physical_expiration_ticks).saturating_duration_since(now)
    }
}

impl<V: Clone> DistributedSnapshot<V> {
    /// Captures separate L2 deadlines and retention, without changing codec fields.
    pub fn from_entry_with_options(
        entry: &Entry<V>,
        options: &EntryOptions,
        inserted_at: Timestamp,
    ) -> Result<Self> {
        let retention = match entry.meta().stored_priority() {
            Some(priority) => SnapshotRetention::Specified {
                size: entry.meta().size(),
                priority,
            },
            None => SnapshotRetention::Unspecified,
        };
        Self::new(
            DistributedEntry::from_entry_with_options(entry, options, inserted_at)?,
            inserted_at,
            retention,
        )
    }

    /// Hydrates source deadlines and persisted retention without extending them.
    pub fn try_into_entry(self, now: Timestamp) -> Result<Entry<V>> {
        let entry = self.entry.try_into_entry(now)?;
        Ok(match self.retention {
            SnapshotRetention::Unspecified => entry,
            SnapshotRetention::Specified { size, priority } => entry.with_retention(size, priority),
        })
    }

    /// Derives independent local deadlines, capped by source freshness/lifetime.
    pub fn for_memory_hydration(
        self,
        options: &EntryOptions,
        now: Timestamp,
    ) -> Result<Option<Entry<V>>> {
        self.try_into_entry(now)?.for_memory_hydration(options, now)
    }
}

const SNAPSHOT_MAGIC: &[u8; 8] = b"AMALGAM\0";
const SNAPSHOT_VERSION: u8 = 2;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SnapshotHeader {
    inserted_ticks: i64,
    retention: WireRetention,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind")]
enum WireRetention {
    Unspecified,
    Specified { size: Option<u64>, priority: u8 },
}

fn frame_snapshot<V>(snapshot: &DistributedSnapshot<V>, payload: Vec<u8>) -> Result<Vec<u8>> {
    let retention = match snapshot.retention {
        SnapshotRetention::Unspecified => WireRetention::Unspecified,
        SnapshotRetention::Specified { size, priority } => WireRetention::Specified {
            size: size.map(EntryWeight::units),
            priority: priority_byte(priority),
        },
    };
    let header = serde_json::to_vec(&SnapshotHeader {
        inserted_ticks: snapshot.inserted_at.ticks(),
        retention,
    })
    .map_err(Error::serialization)?;
    let length = u32::try_from(header.len())
        .map_err(|_| Error::Serialization("snapshot header is too large".into()))?;
    let mut bytes = Vec::with_capacity(
        13_usize
            .saturating_add(header.len())
            .saturating_add(payload.len()),
    );
    bytes.extend_from_slice(SNAPSHOT_MAGIC);
    bytes.push(SNAPSHOT_VERSION);
    bytes.extend_from_slice(&length.to_be_bytes());
    bytes.extend_from_slice(&header);
    bytes.extend(payload);
    Ok(bytes)
}

fn unframe_snapshot(bytes: &[u8]) -> Result<Option<(SnapshotHeader, &[u8])>> {
    if !bytes.starts_with(SNAPSHOT_MAGIC) {
        return Ok(None);
    }
    if bytes.len() < 13 || bytes[8] != SNAPSHOT_VERSION {
        return Err(Error::Deserialization(
            "unknown or truncated snapshot frame".into(),
        ));
    }
    let length = u32::from_be_bytes([bytes[9], bytes[10], bytes[11], bytes[12]]) as usize;
    let end = 13_usize
        .checked_add(length)
        .filter(|end| *end <= bytes.len())
        .ok_or_else(|| Error::Deserialization("truncated snapshot header".into()))?;
    let header = serde_json::from_slice(&bytes[13..end]).map_err(Error::deserialization)?;
    Ok(Some((header, &bytes[end..])))
}

fn priority_byte(priority: Priority) -> u8 {
    match priority {
        Priority::Low => 1,
        Priority::Normal => 2,
        Priority::High => 3,
        Priority::NeverRemove => 4,
    }
}

fn priority_from_byte(byte: u8) -> Result<Priority> {
    match byte {
        1 => Ok(Priority::Low),
        2 => Ok(Priority::Normal),
        3 => Ok(Priority::High),
        4 => Ok(Priority::NeverRemove),
        _ => Err(Error::Deserialization("unknown persisted priority".into())),
    }
}

/// Encodes and decodes [`DistributedEntry`] values for the wire.
///
/// This is object-safe (no generic methods), so a cache holds it as
/// `Arc<dyn DistributedSerializer<V>>` and the serialization format is fully
/// pluggable.
pub trait DistributedSerializer<V>: Send + Sync {
    /// Optional isolated value-copy strategy supplied by this codec.
    ///
    /// Custom codecs keep their existing contract by default. A codec which
    /// can round-trip an isolated value may expose a cloner without coupling
    /// ordinary non-Serde values to distributed storage.
    fn value_cloner(&self) -> Option<Arc<dyn crate::serializers::ValueCloner<V>>> {
        None
    }

    /// Serializes an envelope to bytes.
    ///
    /// # Errors
    /// Returns [`Error::Serialization`] if encoding fails.
    fn serialize(&self, entry: &DistributedEntry<V>) -> Result<Vec<u8>>;

    /// Deserializes an envelope from bytes.
    ///
    /// # Errors
    /// Returns [`Error::Deserialization`] if decoding fails.
    fn deserialize(&self, bytes: &[u8]) -> Result<DistributedEntry<V>>;
    /// Serializes a versioned outer snapshot around this codec's unchanged payload.
    fn serialize_snapshot(&self, snapshot: &DistributedSnapshot<V>) -> Result<Vec<u8>> {
        frame_snapshot(snapshot, self.serialize(snapshot.entry())?)
    }

    /// Accepts V2 frames and legacy raw payloads; malformed frames never fall back.
    fn deserialize_snapshot(&self, bytes: &[u8]) -> Result<DistributedSnapshot<V>> {
        match unframe_snapshot(bytes)? {
            Some((header, payload)) => {
                let retention = match header.retention {
                    WireRetention::Unspecified => SnapshotRetention::Unspecified,
                    WireRetention::Specified { size, priority } => SnapshotRetention::Specified {
                        size: size.map(EntryWeight::new),
                        priority: priority_from_byte(priority)?,
                    },
                };
                DistributedSnapshot::new(
                    self.deserialize(payload)?,
                    Timestamp::from_ticks(header.inserted_ticks),
                    retention,
                )
            }
            None => {
                let entry = self.deserialize(bytes)?;
                let inserted_at = Timestamp::from_ticks(entry.created_ticks);
                DistributedSnapshot::new(entry, inserted_at, SnapshotRetention::Unspecified)
            }
        }
    }
}

/// Selects the preferred codec model without removing the other capability.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SerializationMode {
    /// Use an available synchronous codec directly. Existing caches keep this default.
    #[default]
    SyncPreferred,
    /// Use the asynchronous snapshot codec when configured.
    AsyncPreferred,
}

/// An asynchronous codec for the complete validated L2 snapshot.
///
/// Implementations may await compression, encryption or other codec work. The
/// cache owns and cancels the future with the calling operation. Synchronous
/// codecs remain source compatible through [`DistributedSerializer`]. Snapshot
/// deadlines and tags are validated before hydration regardless of the codec.
#[allow(
    clippy::double_must_use,
    reason = "async-trait adds must_use to futures"
)]
#[async_trait]
pub trait AsyncDistributedSerializer<V>: Send + Sync {
    /// Encodes a complete snapshot, preserving its deadlines and retention.
    async fn serialize_snapshot(&self, snapshot: &DistributedSnapshot<V>) -> Result<Vec<u8>>
    where
        V: Send + Sync;
    /// Decodes a complete snapshot. Invalid data must produce a typed failure.
    async fn deserialize_snapshot(&self, bytes: &[u8]) -> Result<DistributedSnapshot<V>>
    where
        V: Send + Sync;
    /// Encodes with the cancellation signal of the work owning this snapshot.
    ///
    /// Override to pass the signal into cooperative codec I/O. Existing codecs
    /// retain their methods through this adapter; their futures are still owned
    /// and dropped on cancellation. Returning cancellation never degrades to a
    /// successful local-only write, regardless of serialization error policy.
    async fn serialize_snapshot_with_cancellation(
        &self,
        snapshot: &DistributedSnapshot<V>,
        cancellation: FactoryCancellation,
    ) -> Result<Vec<u8>>
    where
        V: Send + Sync,
    {
        cancellation.check()?;
        let result = self.serialize_snapshot(snapshot).await;
        cancellation.check()?;
        result
    }
    /// Decodes with the owning read, eager, passive or recovery work's signal.
    ///
    /// Foreground caller cancellation and cache shutdown end the owning scope.
    /// Permitted background work has an independent token; a completed caller
    /// does not cancel it. Successful completion ends that token as well.
    async fn deserialize_snapshot_with_cancellation(
        &self,
        bytes: &[u8],
        cancellation: FactoryCancellation,
    ) -> Result<DistributedSnapshot<V>>
    where
        V: Send + Sync,
    {
        cancellation.check()?;
        let result = self.deserialize_snapshot(bytes).await;
        cancellation.check()?;
        result
    }
    /// Optional synchronous counterpart used by [`SerializationMode::SyncPreferred`].
    fn sync_serializer(&self) -> Option<&dyn DistributedSerializer<V>> {
        None
    }
    /// Optional isolated value-copy strategy; auto-clone stays synchronous.
    fn value_cloner(&self) -> Option<Arc<dyn crate::serializers::ValueCloner<V>>> {
        self.sync_serializer()
            .and_then(DistributedSerializer::value_cloner)
    }
}

pub(crate) enum Serializer<V> {
    Sync(Arc<dyn DistributedSerializer<V>>),
    Async(Arc<dyn AsyncDistributedSerializer<V>>),
}

enum CodecModel<'a, V> {
    Sync(&'a dyn DistributedSerializer<V>),
    Async(&'a dyn AsyncDistributedSerializer<V>),
}

impl<V> Serializer<V> {
    pub(crate) fn value_cloner(&self) -> Option<Arc<dyn crate::serializers::ValueCloner<V>>> {
        match self {
            Self::Sync(codec) => codec.value_cloner(),
            Self::Async(codec) => codec.value_cloner(),
        }
    }
    pub(crate) async fn encode(
        &self,
        snapshot: &DistributedSnapshot<V>,
        mode: SerializationMode,
        cancellation: &FactoryCancellation,
    ) -> Result<Vec<u8>>
    where
        V: Send + Sync,
    {
        cancellation.check()?;
        let result = match self.model(mode) {
            CodecModel::Sync(codec) => codec.serialize_snapshot(snapshot),
            CodecModel::Async(codec) => {
                codec
                    .serialize_snapshot_with_cancellation(snapshot, cancellation.clone())
                    .await
            }
        };
        cancellation.check()?;
        result
    }
    pub(crate) async fn decode(
        &self,
        bytes: &[u8],
        mode: SerializationMode,
        cancellation: &FactoryCancellation,
    ) -> Result<DistributedSnapshot<V>>
    where
        V: Send + Sync,
    {
        cancellation.check()?;
        let result = match self.model(mode) {
            CodecModel::Sync(codec) => codec.deserialize_snapshot(bytes),
            CodecModel::Async(codec) => {
                codec
                    .deserialize_snapshot_with_cancellation(bytes, cancellation.clone())
                    .await
            }
        };
        cancellation.check()?;
        result
    }

    fn model(&self, mode: SerializationMode) -> CodecModel<'_, V> {
        match (self, mode) {
            (Self::Sync(codec), _) => CodecModel::Sync(codec.as_ref()),
            (Self::Async(codec), SerializationMode::AsyncPreferred) => {
                CodecModel::Async(codec.as_ref())
            }
            (Self::Async(codec), SerializationMode::SyncPreferred) => {
                match codec.sync_serializer() {
                    Some(sync) => CodecModel::Sync(sync),
                    None => CodecModel::Async(codec.as_ref()),
                }
            }
        }
    }
}

/// A JSON serializer built on `serde_json`.
#[derive(Debug, Clone, Copy, Default)]
pub struct JsonSerializer;

impl<V> DistributedSerializer<V> for JsonSerializer
where
    V: Serialize + DeserializeOwned,
{
    fn value_cloner(&self) -> Option<Arc<dyn crate::serializers::ValueCloner<V>>> {
        Some(Arc::new(*self))
    }

    fn serialize(&self, entry: &DistributedEntry<V>) -> Result<Vec<u8>> {
        serde_json::to_vec(entry).map_err(Error::serialization)
    }

    fn deserialize(&self, bytes: &[u8]) -> Result<DistributedEntry<V>> {
        serde_json::from_slice(bytes).map_err(Error::deserialization)
    }
}

/// Atomic ownership validation available for value writes.
///
/// A declaration is a provider contract: `Atomic` requires `write_with_lease`
/// to validate ownership and commit in one indivisible backend operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FencedWriteSupport {
    /// Ordinary value I/O cannot authorize a lease-fenced commit.
    Unavailable,
    /// The provider implements atomic ownership-checked value writes.
    Atomic,
}

/// A byte-oriented L2 distributed cache backend.
///
/// Implement this over Redis, Memcached, a database, etc. The cache layer adds
/// serialization, fail-safe and stampede protection on top.
#[allow(
    clippy::double_must_use,
    reason = "async-trait 0.1.89 emits must_use on boxed futures"
)]
#[async_trait]
pub trait DistributedCache: Send + Sync {
    /// Reads the bytes stored at `key`, if present and unexpired.
    ///
    /// # Errors
    /// Returns [`Error::Distributed`] on backend failure.
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>>;

    /// Writes `value` at `key` with an optional TTL.
    ///
    /// # Errors
    /// Returns [`Error::Distributed`] on backend failure.
    async fn set(&self, key: &str, value: Vec<u8>, ttl: Option<Duration>) -> Result<()>;

    /// Removes `key`.
    ///
    /// # Errors
    /// Returns [`Error::Distributed`] on backend failure.
    async fn remove(&self, key: &str) -> Result<()>;
    /// An optional real atomic marker provider; ordinary legacy I/O stays usable.
    fn invalidation_store(&self) -> Option<Arc<dyn InvalidationStore>> {
        None
    }

    /// Declares the atomic capability checked by strict cache construction.
    fn fenced_write_support(&self) -> FencedWriteSupport {
        FencedWriteSupport::Unavailable
    }

    /// Atomically checks ownership and commits a value mutation. Renewal alone
    /// cannot provide this guarantee across a partition.
    async fn write_with_lease(
        &self,
        _key: &str,
        _mutation: LeasedMutation,
        _proof: &LeaseProof,
    ) -> std::result::Result<LeasedWriteOutcome, LeaseError> {
        Err(LeaseError::UnsupportedFencing)
    }
}

/// A value mutation performed in the same atomic operation as ownership checking.
#[derive(Debug, Clone)]
pub enum LeasedMutation {
    /// Write the exact serialized snapshot with its remaining physical lifetime.
    Set {
        /// Opaque bytes.
        bytes: Vec<u8>,
        /// Remaining lifetime, or unbounded storage.
        ttl: Option<Duration>,
    },
    /// Remove the value only while the expected ownership token is still current.
    Remove,
}

/// Complete expected result of an atomic fenced value mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeasedWriteOutcome {
    /// Mutation committed under verified ownership.
    Committed,
    /// The ownership token was absent, expired, or replaced.
    LeaseLost,
}

/// Complete expected failure family of a cooperative marker read.
///
/// Provider transport and control decoding failures belong in [`MarkerError`],
/// preserving their original source. Cancellation cannot be suppressed as a
/// provider fault or mistaken for a successful absent marker.
#[derive(Debug, thiserror::Error)]
pub enum MarkerReadError {
    /// Storage or control protocol failure.
    #[error(transparent)]
    Provider(#[from] MarkerError),
    /// The read's owning scope ended with its precise terminal reason.
    #[error("marker read cancelled: {}", reason.as_str())]
    Cancelled {
        /// Cancellation of this owned phase.
        reason: FactoryCancellationReason,
    },
}

impl MarkerReadError {
    /// Checks the owned token using this closed marker-read failure family.
    pub fn check_cancellation(cancellation: &FactoryCancellation) -> std::result::Result<(), Self> {
        match cancellation.reason() {
            Some(reason) => Err(Self::Cancelled { reason }),
            None => Ok(()),
        }
    }
}

impl From<MarkerReadError> for Error {
    fn from(error: MarkerReadError) -> Self {
        match error {
            MarkerReadError::Provider(error) => Self::Marker(error),
            MarkerReadError::Cancelled { reason } => Self::OperationCancelled { reason },
        }
    }
}

/// Durable invalidation storage. `advance` must implement a real atomic max.
#[allow(
    clippy::double_must_use,
    reason = "async-trait 0.1.89 emits must_use on boxed futures"
)]
#[async_trait]
pub trait InvalidationStore: Send + Sync {
    /// Optional expiring snapshot area; default providers retain durable-only I/O.
    fn snapshot_cache(&self) -> Option<Arc<dyn MarkerSnapshotCache>> {
        None
    }

    /// Reads one durable maximum from a genuinely separate control area.
    async fn read(
        &self,
        scope: &CacheScope,
        kind: &MarkerKind,
    ) -> std::result::Result<Option<MarkerVersion>, MarkerError>;

    /// Additive cooperative read hook. The default preserves old providers and
    /// checks cancellation both before and after the legacy await. The signal
    /// belongs to the actual owned marker phase, including its exact deadline.
    async fn read_with_cancellation(
        &self,
        scope: &CacheScope,
        kind: &MarkerKind,
        cancellation: FactoryCancellation,
    ) -> std::result::Result<Option<MarkerVersion>, MarkerReadError> {
        MarkerReadError::check_cancellation(&cancellation)?;
        let result = self.read(scope, kind).await;
        MarkerReadError::check_cancellation(&cancellation)?;
        Ok(result?)
    }

    /// Advances an atomic maximum; any compaction first promotes ClearRemove.
    async fn advance(
        &self,
        scope: &CacheScope,
        kind: MarkerKind,
        candidate: MarkerVersion,
    ) -> std::result::Result<MarkerAdvanceOutcome, MarkerError>;

    /// Batch compatibility adapter; native providers may supply one atomic read.
    async fn read_many(
        &self,
        scope: &CacheScope,
        kinds: &[MarkerKind],
    ) -> std::result::Result<Box<[StoredMarker]>, MarkerError> {
        let mut observations = Vec::with_capacity(kinds.len());
        for kind in kinds {
            if let Some(version) = self.read(scope, kind).await? {
                observations.push(StoredMarker::new(kind.clone(), version));
            }
        }
        Ok(observations.into_boxed_slice())
    }
}

/// Reference durable maxima, independent from the ordinary value-key map.
#[derive(Clone)]
pub struct InMemoryInvalidationStore {
    scopes: Arc<Mutex<HashMap<CacheScope, MarkerState>>>,
    snapshots: Arc<Mutex<HashMap<(CacheScope, MarkerKind), MarkerSnapshot>>>,
    limits: MarkerStoreLimits,
    snapshot_limits: MarkerSnapshotLimits,
}

impl InMemoryInvalidationStore {
    /// Creates a bounded provider without expiring live tombstones.
    #[must_use]
    pub fn new(limits: MarkerStoreLimits) -> Self {
        Self::with_snapshot_limits(limits, MarkerSnapshotLimits::default())
    }

    /// Creates independently bounded permanent facts and expendable snapshots.
    #[must_use]
    pub fn with_snapshot_limits(
        limits: MarkerStoreLimits,
        snapshot_limits: MarkerSnapshotLimits,
    ) -> Self {
        Self {
            scopes: Arc::new(Mutex::new(HashMap::new())),
            snapshots: Arc::new(Mutex::new(HashMap::new())),
            limits,
            snapshot_limits,
        }
    }

    /// Independent durable scopes currently retained.
    #[must_use]
    pub fn scope_count(&self) -> usize {
        self.scopes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    /// Snapshot records retained; expired records are removed by reads/renewals.
    #[must_use]
    pub fn snapshot_count(&self) -> usize {
        crate::execution::lock(&self.snapshots).len()
    }
}

impl Default for InMemoryInvalidationStore {
    fn default() -> Self {
        Self::new(MarkerStoreLimits::default())
    }
}

#[async_trait]
impl InvalidationStore for InMemoryInvalidationStore {
    fn snapshot_cache(&self) -> Option<Arc<dyn MarkerSnapshotCache>> {
        Some(Arc::new(self.clone()))
    }
    async fn read(
        &self,
        scope: &CacheScope,
        kind: &MarkerKind,
    ) -> std::result::Result<Option<MarkerVersion>, MarkerError> {
        Ok(self
            .scopes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(scope)
            .and_then(|state| state.read(kind)))
    }

    async fn advance(
        &self,
        scope: &CacheScope,
        kind: MarkerKind,
        candidate: MarkerVersion,
    ) -> std::result::Result<MarkerAdvanceOutcome, MarkerError> {
        let mut scopes = self
            .scopes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !scopes.contains_key(scope) && scopes.len() >= self.limits.max_scopes() {
            return Err(MarkerError::ScopeCapacity {
                limit: self.limits.max_scopes(),
            });
        }
        Ok(scopes.entry(scope.clone()).or_default().advance(
            kind,
            candidate,
            self.limits.max_tags(),
        ))
    }

    async fn read_many(
        &self,
        scope: &CacheScope,
        kinds: &[MarkerKind],
    ) -> std::result::Result<Box<[StoredMarker]>, MarkerError> {
        let scopes = self
            .scopes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(state) = scopes.get(scope) else {
            return Ok(Box::new([]));
        };
        Ok(kinds
            .iter()
            .filter_map(|kind| {
                state
                    .read(kind)
                    .map(|at| StoredMarker::new(kind.clone(), at))
            })
            .collect())
    }
}

#[async_trait]
impl MarkerSnapshotCache for InMemoryInvalidationStore {
    async fn read_snapshot(
        &self,
        scope: &CacheScope,
        kind: &MarkerKind,
        now: Timestamp,
        cancellation: FactoryCancellation,
    ) -> std::result::Result<MarkerSnapshotRead, MarkerSnapshotCacheError> {
        MarkerSnapshotCacheError::check_cancellation(&cancellation)?;
        let scopes = crate::execution::lock(&self.scopes);
        let maximum = scopes.get(scope).and_then(|state| state.read(kind));
        let mut snapshots = crate::execution::lock(&self.snapshots);
        let key = (scope.clone(), kind.clone());
        let result = match snapshots.get(&key).copied() {
            Some(snapshot) if snapshot.is_physically_expired(now) => {
                snapshots.remove(&key);
                MarkerSnapshotRead::Missing { maximum }
            }
            Some(snapshot) => MarkerSnapshotRead::Snapshot(snapshot.with_maximum(maximum)),
            None => MarkerSnapshotRead::Missing { maximum },
        };
        MarkerSnapshotCacheError::check_cancellation(&cancellation)?;
        Ok(result)
    }

    async fn renew_snapshot(
        &self,
        scope: &CacheScope,
        kind: &MarkerKind,
        snapshot: MarkerSnapshot,
        now: Timestamp,
        cancellation: FactoryCancellation,
    ) -> std::result::Result<MarkerSnapshotRenewal, MarkerSnapshotCacheError> {
        MarkerSnapshotCacheError::check_cancellation(&cancellation)?;
        if snapshot.is_physically_expired(now) {
            return Ok(MarkerSnapshotRenewal::Expired);
        }
        let result = self.renew_snapshot_atomic(scope, kind, snapshot, now);
        MarkerSnapshotCacheError::check_cancellation(&cancellation)?;
        Ok(result)
    }

    async fn renew_snapshot_with_lease(
        &self,
        scope: &CacheScope,
        kind: &MarkerKind,
        snapshot: MarkerSnapshot,
        now: Timestamp,
        proof: &LeaseProof,
        cancellation: FactoryCancellation,
    ) -> std::result::Result<MarkerSnapshotRenewal, MarkerSnapshotCacheError> {
        MarkerSnapshotCacheError::check_cancellation(&cancellation)?;
        let result = proof
            .with_memory_ownership(|| self.renew_snapshot_atomic(scope, kind, snapshot, now))?
            .ok_or(LeaseError::Lost)?;
        MarkerSnapshotCacheError::check_cancellation(&cancellation)?;
        Ok(result)
    }
}

impl InMemoryInvalidationStore {
    fn renew_snapshot_atomic(
        &self,
        scope: &CacheScope,
        kind: &MarkerKind,
        snapshot: MarkerSnapshot,
        now: Timestamp,
    ) -> MarkerSnapshotRenewal {
        if snapshot.is_physically_expired(now) {
            return MarkerSnapshotRenewal::Expired;
        }
        // All operations take journal before snapshot: renewal and durable
        // advance cannot race across the atomic maximum decision.
        let scopes = crate::execution::lock(&self.scopes);
        let maximum = scopes.get(scope).and_then(|state| state.read(kind));
        let mut snapshots = crate::execution::lock(&self.snapshots);
        snapshots.retain(|_, entry| !entry.is_physically_expired(now));
        let key = (scope.clone(), kind.clone());
        if let Some(current) = snapshots.get(&key).copied()
            && current.supersedes(snapshot)
        {
            return MarkerSnapshotRenewal::KeptNewer(current.with_maximum(maximum));
        }
        if !snapshots.contains_key(&key)
            && snapshots.len() >= self.snapshot_limits.max_entries()
            && let Some(oldest) = snapshots
                .iter()
                .min_by_key(|(_, entry)| entry.created())
                .map(|(key, _)| key.clone())
        {
            snapshots.remove(&oldest);
        }
        let snapshot = snapshot.with_maximum(maximum);
        snapshots.insert(key, snapshot);
        MarkerSnapshotRenewal::Stored(snapshot)
    }
}

/// An in-memory reference [`DistributedCache`] — a concurrent map with TTL.
///
/// Share one instance between multiple [`Cache`](crate::Cache) instances (via
/// `Arc`) to simulate several nodes pointing at the same L2 within one process.
#[derive(Clone)]
pub struct InMemoryDistributedCache {
    map: Arc<DashMap<String, StoredBytes>>,
    clock: Arc<dyn Clock>,
    invalidation: Arc<InMemoryInvalidationStore>,
}

#[derive(Clone)]
struct StoredBytes {
    bytes: Vec<u8>,
    expires_at: Option<Timestamp>,
}

impl InMemoryDistributedCache {
    /// Creates an empty backend using the given clock for TTL accounting.
    #[must_use]
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        Self {
            map: Arc::new(DashMap::new()),
            clock,
            invalidation: Arc::new(InMemoryInvalidationStore::default()),
        }
    }
}

#[async_trait]
impl DistributedCache for InMemoryDistributedCache {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let now = self.clock.now();
        // Resolve to an owned value so no DashMap guard is held across `remove`.
        let hit = self.map.get(key).and_then(|stored| {
            if stored.expires_at.is_none_or(|exp| now.is_before(exp)) {
                Some(stored.bytes.clone())
            } else {
                None
            }
        });
        if hit.is_none() {
            // Absent or expired: drop any expired entry lazily.
            self.map.remove_if(key, |_, stored| {
                stored.expires_at.is_some_and(|expires| now >= expires)
            });
        }
        Ok(hit)
    }

    async fn set(&self, key: &str, value: Vec<u8>, ttl: Option<Duration>) -> Result<()> {
        let expires_at = ttl.map(|d| self.clock.now().saturating_add(d));
        self.map.insert(
            key.to_owned(),
            StoredBytes {
                bytes: value,
                expires_at,
            },
        );
        Ok(())
    }

    async fn remove(&self, key: &str) -> Result<()> {
        self.map.remove(key);
        Ok(())
    }
    fn invalidation_store(&self) -> Option<Arc<dyn InvalidationStore>> {
        Some(self.invalidation.clone())
    }

    fn fenced_write_support(&self) -> FencedWriteSupport {
        FencedWriteSupport::Atomic
    }

    async fn write_with_lease(
        &self,
        key: &str,
        mutation: LeasedMutation,
        proof: &LeaseProof,
    ) -> std::result::Result<LeasedWriteOutcome, LeaseError> {
        let committed = proof.with_memory_ownership(|| match mutation {
            LeasedMutation::Set { bytes, ttl } => {
                let expires_at = ttl.map(|duration| self.clock.now().saturating_add(duration));
                self.map
                    .insert(key.to_owned(), StoredBytes { bytes, expires_at });
            }
            LeasedMutation::Remove => {
                self.map.remove(key);
            }
        })?;
        Ok(if committed.is_some() {
            LeasedWriteOutcome::Committed
        } else {
            LeasedWriteOutcome::LeaseLost
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::time::ManualClock;

    #[tokio::test]
    async fn in_memory_l2_round_trips_and_expires() {
        let clock = Arc::new(ManualClock::default());
        let dyn_clock: Arc<dyn Clock> = clock.clone();
        let l2 = InMemoryDistributedCache::new(dyn_clock);

        l2.set("k", b"hello".to_vec(), Some(Duration::from_secs(10)))
            .await
            .unwrap();
        assert_eq!(l2.get("k").await.unwrap(), Some(b"hello".to_vec()));

        clock.advance(Duration::from_secs(11));
        assert_eq!(l2.get("k").await.unwrap(), None);
    }

    #[test]
    fn json_serializer_round_trips_envelope() {
        let entry = DistributedEntry {
            value: "v".to_owned(),
            created_ticks: 1,
            logical_expiration_ticks: 2,
            physical_expiration_ticks: 3,
            is_from_fail_safe: false,
            etag: Some("e".to_owned()),
            last_modified_ticks: None,
            tags: vec!["t".to_owned()],
        };
        let ser = JsonSerializer;
        let bytes = DistributedSerializer::<String>::serialize(&ser, &entry).unwrap();
        let back = DistributedSerializer::<String>::deserialize(&ser, &bytes).unwrap();
        assert_eq!(back.value, "v");
        assert_eq!(back.tags, vec!["t".to_owned()]);
    }
}
