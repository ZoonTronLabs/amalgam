//! Redis value storage, atomic invalidation, renewable leases, and an owned backplane.
//!
//! Values retain their ordinary physical keys except keys in the adapter's
//! private area, which are injectively escaped by every value operation. Control
//! hashes and ownership keys consequently cannot be overwritten by ordinary data.
//! V2 backplane frames accept legacy pipe input; old readers require namespace
//! migration. The subscriber owns bounded push delivery and explicit reconnect ACKs.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use redis::aio::{ConnectionManager, ConnectionManagerConfig, MultiplexedConnection};
use redis::{Client, Msg, ProtocolVersion, PushInfo, PushKind};
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, watch};

use crate::backplane::{
    Backplane, BackplaneAction, BackplaneMessage, BackplaneState, ContinuityEpoch, encode_hex,
};
use crate::distributed::{DistributedCache, InvalidationStore, LeasedMutation, LeasedWriteOutcome};
use crate::distributed_lock::{
    DistributedLocker, LeaseError, LeaseProof, LeaseReceipt, LeaseSupport, LeaseToken, LeaseTtl,
    RenewalOutcome, TokenAcquisition, WaitBudget,
};
use crate::error::{Error, Result};
use crate::tags::{
    CacheScope, MarkerAdvanceOutcome, MarkerError, MarkerKind, MarkerStoreLimits, MarkerVersion,
    StoredMarker,
};
use crate::time::{Timeout, Timestamp};

const BACKPLANE_CHANNEL: &str = "amalgam:backplane:v2";
const PRIVATE_AREA: &str = "\u{1f}amalgam/v2/";
const LOCK_POLL_INTERVAL: Duration = Duration::from_millis(25);
const RELEASE_LOCK_SCRIPT: &str = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
const RENEW_LOCK_SCRIPT: &str = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('pexpire', KEYS[1], ARGV[2]) else return 0 end";
const FENCED_WRITE_SCRIPT: &str = r#"
if redis.call('get', KEYS[1]) ~= ARGV[1] then return 0 end
if ARGV[2] == 'remove' then redis.call('del', KEYS[2])
elseif ARGV[4] == '0' then redis.call('del', KEYS[2])
elseif ARGV[4] == '' then redis.call('set', KEYS[2], ARGV[3])
else redis.call('set', KEYS[2], ARGV[3], 'PX', ARGV[4]) end
return 1
"#;

// Revisions are fixed-width sign-biased hex strings, never Lua floating-point numbers.
const ADVANCE_MARKER_SCRIPT: &str = r#"
if redis.call('sismember', KEYS[2], ARGV[1]) == 0 then
  if redis.call('scard', KEYS[2]) >= tonumber(ARGV[5]) then return {'!capacity', ARGV[5]} end
  redis.call('sadd', KEYS[2], ARGV[1])
end
local previous = redis.call('hget', KEYS[1], ARGV[2])
local revision = ARGV[3]
if previous and previous > revision then revision = previous end
redis.call('hset', KEYS[1], ARGV[2], revision)
local fields = redis.call('hgetall', KEYS[1])
local tags = 0
local through = ''
for index = 1, #fields, 2 do
  if string.sub(fields[index], 1, 2) == 't:' then
    tags = tags + 1
    if fields[index + 1] > through then through = fields[index + 1] end
  end
end
if tags <= tonumber(ARGV[4]) then return {revision, ''} end
local clear = redis.call('hget', KEYS[1], 'r')
if clear and clear > through then through = clear end
redis.call('hset', KEYS[1], 'r', through)
for index = 1, #fields, 2 do
  if string.sub(fields[index], 1, 2) == 't:' and fields[index + 1] <= through then
    redis.call('hdel', KEYS[1], fields[index])
  end
end
return {revision, through}
"#;

fn distributed_err(error: impl std::fmt::Display) -> Error {
    Error::Distributed(error.to_string())
}
fn backplane_err(error: impl std::fmt::Display) -> Error {
    Error::Backplane(error.to_string())
}
fn open_client(connection: impl Into<String>) -> Result<Client> {
    Client::open(connection.into()).map_err(distributed_err)
}

/// Positive native connection/request budgets, independent of the injected domain clock.
#[derive(Debug, Clone, Copy)]
pub struct RedisIoOptions {
    connection_timeout: Duration,
    response_timeout: Duration,
}

impl RedisIoOptions {
    /// Validates finite I/O budgets before opening a connection.
    pub fn new(
        connection_timeout: Duration,
        response_timeout: Duration,
    ) -> std::result::Result<Self, LeaseError> {
        if connection_timeout.is_zero() || response_timeout.is_zero() {
            return Err(LeaseError::InvalidDeadline);
        }
        WaitBudget::new(Timeout::After(connection_timeout))?;
        WaitBudget::new(Timeout::After(response_timeout))?;
        Ok(Self {
            connection_timeout,
            response_timeout,
        })
    }

    /// Maximum connection-attempt lifetime.
    #[must_use]
    pub const fn connection_timeout(self) -> Duration {
        self.connection_timeout
    }

    /// Maximum request lifetime for lease/control I/O.
    #[must_use]
    pub const fn response_timeout(self) -> Duration {
        self.response_timeout
    }
}

impl Default for RedisIoOptions {
    fn default() -> Self {
        Self {
            connection_timeout: Duration::from_secs(2),
            response_timeout: Duration::from_secs(2),
        }
    }
}

async fn connect_manager(client: &Client, options: RedisIoOptions) -> Result<ConnectionManager> {
    let config = ConnectionManagerConfig::new()
        .set_connection_timeout(Some(options.connection_timeout))
        .set_response_timeout(Some(options.response_timeout))
        .set_number_of_retries(2)
        .set_max_delay(Duration::from_millis(100));
    tokio::time::timeout(
        options.connection_timeout,
        client.get_connection_manager_with_config(config),
    )
    .await
    .map_err(distributed_err)?
    .map_err(distributed_err)
}

fn value_key(key: &str) -> String {
    if key.starts_with(PRIVATE_AREA) {
        format!("{PRIVATE_AREA}data/{}", encode_hex(key.as_bytes()))
    } else {
        key.to_owned()
    }
}
fn lease_key(key: &str) -> String {
    format!("{PRIVATE_AREA}lease/{}", encode_hex(key.as_bytes()))
}
fn marker_key(scope: &CacheScope) -> String {
    format!(
        "{PRIVATE_AREA}markers/{}",
        encode_hex(scope.storage_id().as_bytes())
    )
}
fn marker_field(kind: &MarkerKind) -> String {
    match kind {
        MarkerKind::Tag(tag) => format!("t:{}", encode_hex(tag.as_str().as_bytes())),
        MarkerKind::ClearExpire => "e".into(),
        MarkerKind::ClearRemove => "r".into(),
    }
}

/// Redis durable atomic marker maxima in an isolated, bounded control area.
#[derive(Clone)]
pub struct RedisInvalidationStore {
    manager: ConnectionManager,
    limits: MarkerStoreLimits,
    io: RedisIoOptions,
}

impl RedisInvalidationStore {
    /// Opens a standalone provider which may also accompany a custom value backend.
    pub async fn connect(connection: impl Into<String>, limits: MarkerStoreLimits) -> Result<Self> {
        let io = RedisIoOptions::default();
        let client = open_client(connection)?;
        Ok(Self {
            manager: connect_manager(&client, io).await?,
            limits,
            io,
        })
    }
}

#[async_trait]
impl InvalidationStore for RedisInvalidationStore {
    async fn read(
        &self,
        scope: &CacheScope,
        kind: &MarkerKind,
    ) -> std::result::Result<Option<MarkerVersion>, MarkerError> {
        let mut connection = self.manager.clone();
        let response = tokio::time::timeout(
            self.io.response_timeout,
            redis::cmd("HGET")
                .arg(marker_key(scope))
                .arg(marker_field(kind))
                .query_async::<Option<String>>(&mut connection),
        )
        .await
        .map_err(MarkerError::backend)?
        .map_err(MarkerError::backend)?;
        response
            .map(|encoded| MarkerVersion::from_ordered_hex(&encoded))
            .transpose()
    }

    async fn advance(
        &self,
        scope: &CacheScope,
        kind: MarkerKind,
        candidate: MarkerVersion,
    ) -> std::result::Result<MarkerAdvanceOutcome, MarkerError> {
        let mut connection = self.manager.clone();
        let response = tokio::time::timeout(
            self.io.response_timeout,
            redis::cmd("EVAL")
                .arg(ADVANCE_MARKER_SCRIPT)
                .arg(2)
                .arg(marker_key(scope))
                .arg(format!("{PRIVATE_AREA}marker-scopes"))
                .arg(scope.storage_id())
                .arg(marker_field(&kind))
                .arg(candidate.ordered_hex())
                .arg(self.limits.max_tags())
                .arg(self.limits.max_scopes())
                .query_async::<Vec<String>>(&mut connection),
        )
        .await
        .map_err(MarkerError::backend)?
        .map_err(MarkerError::backend)?;
        if response.first().is_some_and(|value| value == "!capacity") {
            return Err(MarkerError::ScopeCapacity {
                limit: self.limits.max_scopes(),
            });
        }
        if response.len() != 2 {
            return Err(MarkerError::Protocol {
                detail: "invalid marker advance response".into(),
            });
        }
        let marker = StoredMarker::new(kind, MarkerVersion::from_ordered_hex(&response[0])?);
        if response[1].is_empty() {
            Ok(MarkerAdvanceOutcome::Advanced(marker))
        } else {
            Ok(MarkerAdvanceOutcome::Compacted {
                marker,
                clear_remove: MarkerVersion::from_ordered_hex(&response[1])?,
            })
        }
    }

    async fn read_many(
        &self,
        scope: &CacheScope,
        kinds: &[MarkerKind],
    ) -> std::result::Result<Box<[StoredMarker]>, MarkerError> {
        if kinds.is_empty() {
            return Ok(Box::new([]));
        }
        let mut connection = self.manager.clone();
        let mut command = redis::cmd("HMGET");
        command.arg(marker_key(scope));
        for kind in kinds {
            command.arg(marker_field(kind));
        }
        let response = tokio::time::timeout(
            self.io.response_timeout,
            command.query_async::<Vec<Option<String>>>(&mut connection),
        )
        .await
        .map_err(MarkerError::backend)?
        .map_err(MarkerError::backend)?;
        if response.len() != kinds.len() {
            return Err(MarkerError::Protocol {
                detail: "invalid marker batch response".into(),
            });
        }
        kinds
            .iter()
            .zip(response)
            .filter_map(|(kind, response)| {
                response.map(|encoded| {
                    MarkerVersion::from_ordered_hex(&encoded)
                        .map(|version| StoredMarker::new(kind.clone(), version))
                })
            })
            .collect()
    }
}

/// Redis opaque bytes. Ordinary keys cannot overwrite control or ownership state.
#[derive(Clone)]
pub struct RedisDistributedCache {
    manager: ConnectionManager,
    invalidation: Arc<RedisInvalidationStore>,
}

impl RedisDistributedCache {
    /// Opens the value backend with default bounded transport I/O.
    pub async fn connect(connection: impl Into<String>) -> Result<Self> {
        Self::connect_with_options(
            connection,
            RedisIoOptions::default(),
            MarkerStoreLimits::default(),
        )
        .await
    }

    /// Opens value/marker storage with explicit positive budgets and resource bounds.
    pub async fn connect_with_options(
        connection: impl Into<String>,
        io: RedisIoOptions,
        limits: MarkerStoreLimits,
    ) -> Result<Self> {
        let client = open_client(connection)?;
        let manager = connect_manager(&client, io).await?;
        let invalidation = Arc::new(RedisInvalidationStore {
            manager: manager.clone(),
            limits,
            io,
        });
        Ok(Self {
            manager,
            invalidation,
        })
    }
}

#[async_trait]
impl DistributedCache for RedisDistributedCache {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let mut connection = self.manager.clone();
        redis::cmd("GET")
            .arg(value_key(key))
            .query_async(&mut connection)
            .await
            .map_err(distributed_err)
    }

    async fn set(&self, key: &str, bytes: Vec<u8>, ttl: Option<Duration>) -> Result<()> {
        if ttl.is_some_and(|duration| duration.is_zero()) {
            return self.remove(key).await;
        }
        let mut connection = self.manager.clone();
        let mut command = redis::cmd("SET");
        command.arg(value_key(key)).arg(bytes);
        if let Some(duration) = ttl {
            command.arg("PX").arg(duration_to_millis(duration));
        }
        command
            .query_async::<()>(&mut connection)
            .await
            .map_err(distributed_err)
    }

    async fn remove(&self, key: &str) -> Result<()> {
        let mut connection = self.manager.clone();
        redis::cmd("DEL")
            .arg(value_key(key))
            .query_async::<i64>(&mut connection)
            .await
            .map(|_| ())
            .map_err(distributed_err)
    }

    fn invalidation_store(&self) -> Option<Arc<dyn InvalidationStore>> {
        Some(self.invalidation.clone())
    }

    async fn write_with_lease(
        &self,
        key: &str,
        mutation: LeasedMutation,
        proof: &LeaseProof,
    ) -> std::result::Result<LeasedWriteOutcome, LeaseError> {
        let (action, bytes, ttl) = match mutation {
            LeasedMutation::Set { bytes, ttl } => (
                "set",
                bytes,
                ttl.map(|duration| {
                    if duration.is_zero() {
                        "0".into()
                    } else {
                        duration_to_millis(duration).to_string()
                    }
                })
                .unwrap_or_default(),
            ),
            LeasedMutation::Remove => ("remove", Vec::new(), String::new()),
        };
        let mut connection = self.manager.clone();
        let result = redis::cmd("EVAL")
            .arg(FENCED_WRITE_SCRIPT)
            .arg(2)
            .arg(lease_key(proof.key()))
            .arg(value_key(key))
            .arg(proof.token().as_str())
            .arg(action)
            .arg(bytes)
            .arg(ttl)
            .query_async::<i64>(&mut connection)
            .await
            .map_err(LeaseError::backend)?;
        Ok(if result == 1 {
            LeasedWriteOutcome::Committed
        } else {
            LeasedWriteOutcome::LeaseLost
        })
    }
}

/// Redis renewable token leases, with preselected nonce cancellation cleanup.
#[derive(Clone)]
pub struct RedisDistributedLocker {
    manager: ConnectionManager,
    io: RedisIoOptions,
}

impl RedisDistributedLocker {
    /// Opens native renewable ownership with explicit finite transport budgets.
    pub async fn connect(connection: impl Into<String>) -> Result<Self> {
        Self::connect_with_options(connection, RedisIoOptions::default()).await
    }

    /// Opens native ownership with custom validated transport budgets.
    pub async fn connect_with_options(
        connection: impl Into<String>,
        io: RedisIoOptions,
    ) -> Result<Self> {
        let client = open_client(connection)?;
        Ok(Self {
            manager: connect_manager(&client, io).await?,
            io,
        })
    }

    async fn try_acquire(
        &self,
        key: &str,
        token: &LeaseToken,
        ttl: LeaseTtl,
        budget: Duration,
    ) -> std::result::Result<bool, LeaseError> {
        let mut connection = self.manager.clone();
        let result = tokio::time::timeout(
            budget,
            redis::cmd("SET")
                .arg(lease_key(key))
                .arg(token.as_str())
                .arg("NX")
                .arg("PX")
                .arg(ttl.millis())
                .query_async::<Option<String>>(&mut connection),
        )
        .await
        .map_err(|_| LeaseError::AcquisitionTimeout)?
        .map_err(LeaseError::backend)?;
        Ok(result.is_some())
    }
}

struct AcquireCleanup {
    state: AcquireCleanupState,
}
enum AcquireCleanupState {
    Pending {
        manager: ConnectionManager,
        key: String,
        token: LeaseToken,
        budget: Duration,
        owner: Arc<dyn crate::distributed_lock::LeaseTaskOwner>,
    },
    Transferred,
}

impl AcquireCleanup {
    fn new(
        locker: &RedisDistributedLocker,
        key: &str,
        token: &LeaseToken,
        owner: Arc<dyn crate::distributed_lock::LeaseTaskOwner>,
    ) -> Self {
        Self {
            state: AcquireCleanupState::Pending {
                manager: locker.manager.clone(),
                key: lease_key(key),
                token: token.clone(),
                budget: locker.io.response_timeout,
                owner,
            },
        }
    }
    fn transfer(&mut self) {
        self.state = AcquireCleanupState::Transferred;
    }
}

impl Drop for AcquireCleanup {
    fn drop(&mut self) {
        let state = std::mem::replace(&mut self.state, AcquireCleanupState::Transferred);
        if let AcquireCleanupState::Pending {
            mut manager,
            key,
            token,
            budget,
            owner,
        } = state
        {
            owner.supervise(Box::pin(async move {
                tokio::time::timeout(
                    budget,
                    redis::cmd("EVAL")
                        .arg(RELEASE_LOCK_SCRIPT)
                        .arg(1)
                        .arg(key)
                        .arg(token.as_str())
                        .query_async::<i64>(&mut manager),
                )
                .await
                .map_err(|_| LeaseError::CleanupTimeout)?
                .map_err(LeaseError::backend)?;
                Ok(())
            }));
        }
    }
}

#[async_trait]
impl DistributedLocker for RedisDistributedLocker {
    async fn acquire(&self, key: &str, ttl: Duration, timeout: Timeout) -> Result<Option<String>> {
        let token = LeaseToken::random();
        let acquired = self
            .acquire_with_token(key, &token, LeaseTtl::new(ttl)?, timeout)
            .await?;
        Ok(acquired.then(|| token.as_str().to_owned()))
    }

    async fn release(&self, key: &str, token: &str) -> Result<()> {
        let mut connection = self.manager.clone();
        tokio::time::timeout(
            self.io.response_timeout,
            redis::cmd("EVAL")
                .arg(RELEASE_LOCK_SCRIPT)
                .arg(1)
                .arg(lease_key(key))
                .arg(token)
                .query_async::<i64>(&mut connection),
        )
        .await
        .map_err(distributed_err)?
        .map(|_| ())
        .map_err(distributed_err)
    }

    fn lease_support(&self) -> LeaseSupport {
        LeaseSupport::Renewable
    }
    fn token_acquisition(&self) -> TokenAcquisition {
        TokenAcquisition::CallerSelected
    }

    async fn acquire_with_token(
        &self,
        key: &str,
        token: &LeaseToken,
        ttl: LeaseTtl,
        timeout: Timeout,
    ) -> std::result::Result<bool, LeaseError> {
        self.acquire_receipt(key, token, ttl, timeout)
            .await
            .map(|receipt| receipt.is_some())
    }

    async fn acquire_receipt(
        &self,
        key: &str,
        token: &LeaseToken,
        ttl: LeaseTtl,
        timeout: Timeout,
    ) -> std::result::Result<Option<LeaseReceipt>, LeaseError> {
        self.acquire_receipt_supervised(
            key,
            token,
            ttl,
            timeout,
            crate::distributed_lock::StandaloneLeaseOwner::current()?,
        )
        .await
    }

    async fn acquire_receipt_supervised(
        &self,
        key: &str,
        token: &LeaseToken,
        ttl: LeaseTtl,
        timeout: Timeout,
        owner: Arc<dyn crate::distributed_lock::LeaseTaskOwner>,
    ) -> std::result::Result<Option<LeaseReceipt>, LeaseError> {
        let wait = WaitBudget::new(timeout)?;
        let mut first = true;
        let mut cleanup = AcquireCleanup::new(self, key, token, owner);
        loop {
            if wait.exhausted() && !(first && wait.immediate()) {
                return Ok(None);
            }
            let budget = if wait.immediate() {
                self.io.response_timeout
            } else {
                wait.remaining().map_or(self.io.response_timeout, |left| {
                    left.min(self.io.response_timeout)
                })
            };
            let started = tokio::time::Instant::now();
            let acquired = match self.try_acquire(key, token, ttl, budget).await {
                Ok(acquired) => acquired,
                Err(LeaseError::AcquisitionTimeout) => return Ok(None),
                Err(error) => return Err(error),
            };
            if acquired {
                if wait.exhausted() && !wait.immediate() {
                    self.release(key, token.as_str())
                        .await
                        .map_err(LeaseError::backend)?;
                    return Ok(None);
                }
                cleanup.transfer();
                return Ok(Some(LeaseReceipt::new(started)));
            }
            first = false;
            if wait.exhausted() {
                return Ok(None);
            }
            wait.pause(LOCK_POLL_INTERVAL).await;
        }
    }

    async fn renew(
        &self,
        key: &str,
        token: &LeaseToken,
        ttl: LeaseTtl,
    ) -> std::result::Result<RenewalOutcome, LeaseError> {
        let mut connection = self.manager.clone();
        let result = tokio::time::timeout(
            self.io.response_timeout.min(ttl.duration()),
            redis::cmd("EVAL")
                .arg(RENEW_LOCK_SCRIPT)
                .arg(1)
                .arg(lease_key(key))
                .arg(token.as_str())
                .arg(ttl.millis())
                .query_async::<i64>(&mut connection),
        )
        .await
        .map_err(LeaseError::backend)?
        .map_err(LeaseError::backend)?;
        Ok(if result == 1 {
            RenewalOutcome::Renewed
        } else {
            RenewalOutcome::Lost
        })
    }
}

/// Actual Redis connection identity, suitable for targeted disconnect tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RedisClientId(u64);
impl RedisClientId {
    /// The server-assigned positive connection ID.
    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }
}

/// Finite counters from the bounded subscriber relay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RedisBackplaneStats {
    /// Pushes missed through bounded local-buffer overflow.
    pub dropped_pushes: u64,
    /// Malformed remote frames which triggered conservative reconciliation.
    pub malformed_messages: u64,
    /// Acknowledged subscriber connections.
    pub acknowledged_connections: u64,
}

#[derive(Clone)]
struct SubscriberPush {
    incarnation: u64,
    info: PushInfo,
}

struct BackplaneInner {
    manager: tokio::sync::RwLock<Option<ConnectionManager>>,
    client: Client,
    channel: Arc<str>,
    name: Arc<str>,
    io: RedisIoOptions,
    sender: broadcast::Sender<BackplaneMessage>,
    state: watch::Sender<BackplaneState>,
    stop: watch::Sender<bool>,
    subscriber: Mutex<Option<MultiplexedConnection>>,
    subscriber_id: AtomicU64,
    worker: Mutex<Option<tokio::task::JoinHandle<()>>>,
    incarnation: Arc<AtomicU64>,
    liveness: Arc<Mutex<Option<u64>>>,
    dropped: AtomicU64,
    malformed: AtomicU64,
    acknowledged: AtomicU64,
    shutdown_gate: tokio::sync::Mutex<()>,
}

/// A bounded, explicitly supervised RESP3 subscriber. Drop closes its owned socket/task.
pub struct RedisBackplane {
    inner: Arc<BackplaneInner>,
}

impl RedisBackplane {
    /// Connects on the deliberate V2 control-protocol channel.
    pub async fn connect(connection: impl Into<String>) -> Result<Self> {
        Self::connect_with_channel(connection, BACKPLANE_CHANNEL).await
    }

    /// Connects using a channel chosen for an application's coordinated migration.
    pub async fn connect_with_channel(
        connection: impl Into<String>,
        channel: impl Into<String>,
    ) -> Result<Self> {
        Self::connect_named(
            connection,
            channel,
            format!("amalgam-backplane-{}", LeaseToken::random().as_str()),
            256,
            RedisIoOptions::default(),
        )
        .await
    }

    /// Connects with an identifiable fixture and bounded local push buffer.
    pub async fn connect_named(
        connection: impl Into<String>,
        channel: impl Into<String>,
        name: impl Into<String>,
        capacity: usize,
        io: RedisIoOptions,
    ) -> Result<Self> {
        let channel = channel.into();
        let name = name.into();
        if channel.trim().is_empty() || name.trim().is_empty() || capacity == 0 {
            return Err(Error::Backplane(
                "channel/name must be nonblank and push capacity positive".into(),
            ));
        }
        let connection = connection.into();
        let publish_client = open_client(connection.clone())?;
        let manager = connect_manager(&publish_client, io).await?;
        let mut publisher = manager.clone();
        redis::cmd("CLIENT")
            .arg("SETNAME")
            .arg(format!("{name}:publish"))
            .query_async::<()>(&mut publisher)
            .await
            .map_err(backplane_err)?;
        let info = connection
            .parse::<redis::ConnectionInfo>()
            .map_err(backplane_err)?;
        let settings = info
            .redis_settings()
            .clone()
            .set_protocol(ProtocolVersion::RESP3);
        let client = Client::open(info.set_redis_settings(settings)).map_err(backplane_err)?;
        let (sender, _) = broadcast::channel(capacity);
        let (push_sender, push_receiver) = broadcast::channel(capacity);
        let (state, _) = watch::channel(BackplaneState::Disconnected {
            epoch: ContinuityEpoch::INITIAL,
        });
        let (stop, _) = watch::channel(false);
        let inner = Arc::new(BackplaneInner {
            manager: tokio::sync::RwLock::new(Some(manager)),
            client,
            channel: channel.into(),
            name: name.into(),
            io,
            sender,
            state,
            stop,
            subscriber: Mutex::new(None),
            subscriber_id: AtomicU64::new(0),
            worker: Mutex::new(None),
            incarnation: Arc::new(AtomicU64::new(1)),
            liveness: Arc::new(Mutex::new(None)),
            dropped: AtomicU64::new(0),
            malformed: AtomicU64::new(0),
            acknowledged: AtomicU64::new(0),
            shutdown_gate: tokio::sync::Mutex::new(()),
        });
        let (subscriber, id) = open_subscriber(&inner, push_sender.clone(), 1).await?;
        *inner
            .subscriber
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(subscriber);
        inner.subscriber_id.store(id, Ordering::Release);
        inner.acknowledged.store(1, Ordering::Relaxed);
        if !admit_connected(&inner, ContinuityEpoch::INITIAL) {
            return Err(Error::Backplane(
                "subscriber disconnected before initial ACK admission".into(),
            ));
        }
        let worker = tokio::spawn(supervise_subscriber(
            Arc::downgrade(&inner),
            push_sender,
            push_receiver,
            inner.stop.subscribe(),
        ));
        *inner
            .worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(worker);
        Ok(Self { inner })
    }

    /// The actual current subscriber's server identity, never a synthetic health event.
    #[must_use]
    pub fn subscriber_client_id(&self) -> Option<RedisClientId> {
        if !matches!(*self.inner.state.borrow(), BackplaneState::Connected { .. }) {
            return None;
        }
        let id = self.inner.subscriber_id.load(Ordering::Acquire);
        (id != 0).then_some(RedisClientId(id))
    }

    /// Name reapplied to every new subscriber connection.
    #[must_use]
    pub fn subscriber_name(&self) -> String {
        format!("{}:subscriber", self.inner.name)
    }

    /// Current finite relay counters.
    #[must_use]
    pub fn stats(&self) -> RedisBackplaneStats {
        RedisBackplaneStats {
            dropped_pushes: self.inner.dropped.load(Ordering::Relaxed),
            malformed_messages: self.inner.malformed.load(Ordering::Relaxed),
            acknowledged_connections: self.inner.acknowledged.load(Ordering::Relaxed),
        }
    }
}

fn disconnect(state: &watch::Sender<BackplaneState>) {
    let epoch = match *state.borrow() {
        BackplaneState::Connected { epoch } | BackplaneState::Disconnected { epoch } => epoch,
        BackplaneState::Stopped => return,
    };
    state.send_replace(BackplaneState::Disconnected { epoch });
}

fn admit_connected(inner: &BackplaneInner, epoch: ContinuityEpoch) -> bool {
    let liveness = inner
        .liveness
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if *liveness != Some(inner.incarnation.load(Ordering::Acquire)) || *inner.stop.borrow() {
        return false;
    }
    inner
        .state
        .send_replace(BackplaneState::Connected { epoch });
    true
}

async fn open_subscriber(
    inner: &BackplaneInner,
    sender: broadcast::Sender<SubscriberPush>,
    incarnation: u64,
) -> Result<(MultiplexedConnection, u64)> {
    let io = inner.io;
    let state = inner.state.clone();
    let current = inner.incarnation.clone();
    let liveness = inner.liveness.clone();
    *liveness
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(incarnation);
    let config = redis::AsyncConnectionConfig::new()
        .set_connection_timeout(Some(io.connection_timeout))
        .set_response_timeout(Some(io.response_timeout))
        .set_push_sender(move |push: PushInfo| {
            if push.kind == PushKind::Disconnection {
                let mut live = liveness
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if current.load(Ordering::Acquire) == incarnation {
                    *live = None;
                    disconnect(&state);
                }
            }
            let _ = sender.send(SubscriberPush {
                incarnation,
                info: push,
            });
            Ok::<(), redis::aio::SendError>(())
        });
    let mut subscriber = tokio::time::timeout(
        io.connection_timeout,
        inner
            .client
            .get_multiplexed_async_connection_with_config(&config),
    )
    .await
    .map_err(backplane_err)?
    .map_err(backplane_err)?;
    redis::cmd("CLIENT")
        .arg("SETNAME")
        .arg(format!("{}:subscriber", inner.name))
        .query_async::<()>(&mut subscriber)
        .await
        .map_err(backplane_err)?;
    let id = redis::cmd("CLIENT")
        .arg("ID")
        .query_async::<u64>(&mut subscriber)
        .await
        .map_err(backplane_err)?;
    if id == 0 {
        return Err(Error::Backplane(
            "Redis supplied invalid client identity".into(),
        ));
    }
    // Only the matching SUBSCRIBE request's successful ACK admits Connected.
    subscriber
        .subscribe(&*inner.channel)
        .await
        .map_err(backplane_err)?;
    Ok((subscriber, id))
}

async fn restore_subscriber(
    inner: &BackplaneInner,
    pushes: broadcast::Sender<SubscriberPush>,
) -> Result<ContinuityEpoch> {
    let old = inner
        .incarnation
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |old| {
            old.checked_add(1)
        })
        .map_err(|_| Error::Backplane("subscriber incarnation exhausted".into()))?;
    let (subscriber, id) = open_subscriber(inner, pushes, old + 1).await?;
    let epoch = match *inner.state.borrow() {
        BackplaneState::Connected { epoch } | BackplaneState::Disconnected { epoch } => {
            epoch.next()?
        }
        BackplaneState::Stopped => {
            return Err(Error::Backplane(
                "subscriber stopped during reconnect".into(),
            ));
        }
    };
    *inner
        .subscriber
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(subscriber);
    inner.subscriber_id.store(id, Ordering::Release);
    inner
        .acknowledged
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            Some(value.saturating_add(1))
        })
        .ok();
    Ok(epoch)
}

async fn supervise_subscriber(
    weak: std::sync::Weak<BackplaneInner>,
    pushes: broadcast::Sender<SubscriberPush>,
    mut receiver: broadcast::Receiver<SubscriberPush>,
    mut stop: watch::Receiver<bool>,
) {
    loop {
        let received = tokio::select! { biased; _ = stop.changed() => return, received = receiver.recv() => received };
        let Some(inner) = weak.upgrade() else {
            return;
        };
        let needs_reconnect = match received {
            Ok(push) if push.incarnation != inner.incarnation.load(Ordering::Acquire) => false,
            Ok(push) => match push.info.kind {
                PushKind::Disconnection => true,
                PushKind::Message => {
                    match Msg::from_push_info(push.info)
                        .and_then(|message| decode_message(message.get_payload_bytes()))
                    {
                        Some(message) => {
                            let _ = inner.sender.send(message);
                            false
                        }
                        None => {
                            inner
                                .malformed
                                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                                    Some(value.saturating_add(1))
                                })
                                .ok();
                            true
                        }
                    }
                }
                PushKind::Subscribe
                | PushKind::PSubscribe
                | PushKind::SSubscribe
                | PushKind::Unsubscribe
                | PushKind::PUnsubscribe
                | PushKind::SUnsubscribe
                | PushKind::Invalidate
                | PushKind::PMessage
                | PushKind::SMessage
                | PushKind::Other(_) => false,
                // The SDK intentionally leaves its push family open. Unknown pushes
                // cannot certify continuity, so the boundary reconciles conservatively.
                _ => true,
            },
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                inner
                    .dropped
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                        Some(value.saturating_add(skipped))
                    })
                    .ok();
                true
            }
            Err(broadcast::error::RecvError::Closed) => {
                inner.state.send_replace(BackplaneState::Stopped);
                return;
            }
        };
        if !needs_reconnect {
            continue;
        }
        disconnect(&inner.state);
        inner
            .subscriber
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        loop {
            let restored = tokio::select! { biased; _ = stop.changed() => return, restored = restore_subscriber(&inner, pushes.clone()) => restored };
            match restored {
                Ok(epoch) => {
                    // Old socket pushes cannot force a newly acknowledged socket
                    // back into another reconnect storm. Overflow during the gap
                    // is already covered by the impending epoch reconciliation.
                    loop {
                        match receiver.try_recv() {
                            Ok(push)
                                if push.incarnation
                                    == inner.incarnation.load(Ordering::Acquire)
                                    && push.info.kind == PushKind::Message =>
                            {
                                if let Some(message) = Msg::from_push_info(push.info)
                                    .and_then(|message| decode_message(message.get_payload_bytes()))
                                {
                                    let _ = inner.sender.send(message);
                                }
                            }
                            Ok(_) => {}
                            Err(broadcast::error::TryRecvError::Lagged(skipped)) => {
                                inner
                                    .dropped
                                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                                        Some(value.saturating_add(skipped))
                                    })
                                    .ok();
                            }
                            Err(broadcast::error::TryRecvError::Empty) => break,
                            Err(broadcast::error::TryRecvError::Closed) => {
                                inner.state.send_replace(BackplaneState::Stopped);
                                return;
                            }
                        }
                    }
                    if !admit_connected(&inner, epoch) {
                        inner
                            .subscriber
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .take();
                        continue;
                    }
                    break;
                }
                Err(error) => {
                    tracing::debug!(%error, "Redis subscriber reconnect failed");
                    tokio::select! { biased; _ = stop.changed() => return, _ = tokio::time::sleep(Duration::from_millis(100)) => {} }
                }
            }
        }
    }
}

#[async_trait]
impl Backplane for RedisBackplane {
    async fn publish(&self, message: BackplaneMessage) -> Result<()> {
        if *self.inner.stop.borrow() {
            return Err(Error::Backplane("backplane has stopped".into()));
        }
        let publisher = self.inner.manager.read().await;
        let Some(mut connection) = publisher.as_ref().cloned() else {
            return Err(Error::Backplane("backplane publisher has stopped".into()));
        };
        if *self.inner.stop.borrow() {
            return Err(Error::Backplane("backplane has stopped".into()));
        }
        redis::cmd("PUBLISH")
            .arg(&*self.inner.channel)
            .arg(encode_message(&message))
            .query_async::<i64>(&mut connection)
            .await
            .map(|_| ())
            .map_err(backplane_err)
    }
    fn subscribe(&self) -> broadcast::Receiver<BackplaneMessage> {
        self.inner.sender.subscribe()
    }
    fn connection_state(&self) -> Option<watch::Receiver<BackplaneState>> {
        Some(self.inner.state.subscribe())
    }
    async fn shutdown(&self) -> Result<()> {
        self.inner.stop.send_replace(true);
        self.inner.state.send_replace(BackplaneState::Stopped);
        let _shutdown = self.inner.shutdown_gate.lock().await;
        self.inner
            .subscriber
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let worker = self
            .inner
            .worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let failure = if let Some(worker) = worker
            && let Err(error) = worker.await
            && !error.is_cancelled()
        {
            Some(backplane_err(error))
        } else {
            None
        };
        self.inner.manager.write().await.take();
        failure.map_or(Ok(()), Err)
    }
}

impl Drop for RedisBackplane {
    fn drop(&mut self) {
        self.inner.stop.send_replace(true);
        self.inner.state.send_replace(BackplaneState::Stopped);
        self.inner
            .subscriber
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(worker) = self
            .inner
            .worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            worker.abort();
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireMessage {
    version: u8,
    source: String,
    ticks: i64,
    action: u8,
    key: String,
}

fn action_byte(action: BackplaneAction) -> u8 {
    match action {
        BackplaneAction::Set => 1,
        BackplaneAction::Remove => 2,
        BackplaneAction::Expire => 3,
    }
}
fn action_from_byte(byte: u8) -> Option<BackplaneAction> {
    match byte {
        1 => Some(BackplaneAction::Set),
        2 => Some(BackplaneAction::Remove),
        3 => Some(BackplaneAction::Expire),
        _ => None,
    }
}

fn encode_message(message: &BackplaneMessage) -> String {
    // The fields are all infallible JSON primitives; writing to Vec cannot fail.
    serde_json::json!({"version":2,"source":&*message.source_id,"ticks":message.timestamp.ticks(),"action":action_byte(message.action),"key":&*message.key}).to_string()
}

fn decode_message(bytes: &[u8]) -> Option<BackplaneMessage> {
    if bytes.first() == Some(&b'{') {
        let frame: WireMessage = serde_json::from_slice(bytes).ok()?;
        if frame.version != 2 {
            return None;
        }
        return Some(BackplaneMessage {
            source_id: frame.source.into(),
            timestamp: Timestamp::from_ticks(frame.ticks),
            action: action_from_byte(frame.action)?,
            key: frame.key.into(),
        });
    }
    let text = std::str::from_utf8(bytes).ok()?;
    let mut fields = text.splitn(4, '|');
    Some(BackplaneMessage {
        source_id: fields.next()?.into(),
        timestamp: Timestamp::from_ticks(fields.next()?.parse().ok()?),
        action: action_from_byte(fields.next()?.parse().ok()?)?,
        key: fields.next()?.into(),
    })
}

fn duration_to_millis(duration: Duration) -> u64 {
    // Positive Redis PX; cap well below signed absolute-expiration overflow.
    duration
        .as_nanos()
        .div_ceil(1_000_000)
        .max(1)
        .min((i64::MAX / 2) as u128) as u64
}
#[cfg(test)]
mod tests {
    use super::*;

    mod fixture {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/support/redis_fixture.rs"
        ));
    }
    use fixture::redis_url;

    /// A unique key prefix so concurrent test runs never collide.
    fn unique_key(name: &str) -> String {
        format!("amalgam:test:{name}:{:016x}", fastrand::u64(..))
    }

    #[test]
    fn action_byte_round_trips() {
        for action in [
            BackplaneAction::Set,
            BackplaneAction::Remove,
            BackplaneAction::Expire,
        ] {
            assert_eq!(action_from_byte(action_byte(action)), Some(action));
        }
        assert_eq!(action_from_byte(0), None);
        assert_eq!(action_from_byte(99), None);
    }

    #[test]
    fn message_round_trips_through_wire_including_separator_in_key() {
        let original = BackplaneMessage {
            source_id: "node-a".into(),
            timestamp: Timestamp::from_ticks(123_456_789),
            action: BackplaneAction::Expire,
            key: "tenant|42|user:7".into(), // separator inside the key must survive
        };
        let decoded =
            decode_message(encode_message(&original).as_bytes()).expect("encoded message decodes");
        assert_eq!(&*decoded.source_id, "node-a");
        assert_eq!(decoded.timestamp, original.timestamp);
        assert_eq!(decoded.action, BackplaneAction::Expire);
        assert_eq!(&*decoded.key, "tenant|42|user:7");
    }

    #[test]
    fn decode_rejects_malformed_input() {
        assert!(decode_message(b"not-enough-fields").is_none());
        assert!(decode_message(b"src|not-a-number|1|key").is_none());
        assert!(decode_message(b"src|10|255|key").is_none()); // unknown action
        assert!(decode_message(&[0xff, 0xfe]).is_none()); // invalid UTF-8
    }

    #[test]
    fn duration_to_millis_floors_to_one() {
        assert_eq!(duration_to_millis(Duration::from_micros(1)), 1);
        assert_eq!(duration_to_millis(Duration::from_millis(250)), 250);
    }

    #[tokio::test]
    async fn cache_set_get_remove_round_trip() {
        let Some(url) = redis_url() else {
            return;
        };
        let cache = RedisDistributedCache::connect(url)
            .await
            .expect("connect to redis");
        let key = unique_key("cache");

        assert_eq!(cache.get(&key).await.unwrap(), None);

        cache
            .set(&key, b"hello".to_vec(), Some(Duration::from_secs(30)))
            .await
            .unwrap();
        assert_eq!(cache.get(&key).await.unwrap(), Some(b"hello".to_vec()));

        cache.remove(&key).await.unwrap();
        assert_eq!(cache.get(&key).await.unwrap(), None);
    }

    #[tokio::test]
    async fn cache_honours_px_expiry() {
        let Some(url) = redis_url() else {
            return;
        };
        let cache = RedisDistributedCache::connect(url)
            .await
            .expect("connect to redis");
        let key = unique_key("cache-ttl");

        cache
            .set(
                &key,
                b"transient".to_vec(),
                Some(Duration::from_millis(100)),
            )
            .await
            .unwrap();
        assert!(cache.get(&key).await.unwrap().is_some());

        tokio::time::sleep(Duration::from_millis(250)).await;
        assert_eq!(cache.get(&key).await.unwrap(), None);
    }

    #[tokio::test]
    async fn locker_is_exclusive_until_released() {
        let Some(url) = redis_url() else {
            return;
        };
        let locker = RedisDistributedLocker::connect(url)
            .await
            .expect("connect to redis");
        let key = unique_key("lock");

        let token = locker
            .acquire(&key, Duration::from_secs(30), Timeout::Infinite)
            .await
            .unwrap()
            .expect("first acquire succeeds");

        // A non-waiting second attempt must fail while the lock is held.
        let second = locker
            .acquire(
                &key,
                Duration::from_secs(30),
                Timeout::After(Duration::ZERO),
            )
            .await
            .unwrap();
        assert!(second.is_none(), "lock must be exclusive");

        locker.release(&key, &token).await.unwrap();

        // After release it can be acquired again.
        let third = locker
            .acquire(
                &key,
                Duration::from_secs(30),
                Timeout::After(Duration::ZERO),
            )
            .await
            .unwrap();
        assert!(
            third.is_some(),
            "lock should be re-acquirable after release"
        );
        locker.release(&key, &third.unwrap()).await.unwrap();
    }

    #[tokio::test]
    async fn release_with_wrong_token_is_noop() {
        let Some(url) = redis_url() else {
            return;
        };
        let locker = RedisDistributedLocker::connect(url)
            .await
            .expect("connect to redis");
        let key = unique_key("lock-wrong-token");

        let token = locker
            .acquire(&key, Duration::from_secs(30), Timeout::Infinite)
            .await
            .unwrap()
            .expect("acquire succeeds");

        // Releasing with someone else's token must not free the lock.
        locker.release(&key, "not-the-token").await.unwrap();
        let blocked = locker
            .acquire(
                &key,
                Duration::from_secs(30),
                Timeout::After(Duration::ZERO),
            )
            .await
            .unwrap();
        assert!(blocked.is_none(), "wrong-token release must be a no-op");

        locker.release(&key, &token).await.unwrap();
    }

    #[tokio::test]
    async fn backplane_publish_is_received_by_subscriber() {
        let Some(url) = redis_url() else {
            return;
        };
        let backplane = RedisBackplane::connect(url)
            .await
            .expect("connect to redis");
        let mut rx = backplane.subscribe();

        // Give the background SUBSCRIBE a moment to register on the server.
        tokio::time::sleep(Duration::from_millis(150)).await;

        let key: std::sync::Arc<str> = unique_key("backplane").into();
        let sent = BackplaneMessage {
            source_id: "publisher".into(),
            timestamp: Timestamp::from_ticks(987_654_321),
            action: BackplaneAction::Remove,
            key: key.clone(),
        };
        backplane.publish(sent).await.unwrap();

        let received = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("message arrives before timeout")
            .expect("broadcast channel delivers");

        assert_eq!(&*received.source_id, "publisher");
        assert_eq!(received.timestamp, Timestamp::from_ticks(987_654_321));
        assert_eq!(received.action, BackplaneAction::Remove);
        assert_eq!(received.key, key);
    }
}
