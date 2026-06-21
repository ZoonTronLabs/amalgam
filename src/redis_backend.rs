//! Redis-backed L2 cache, backplane, and distributed locker (feature `redis`).
//!
//! Three small adapters, each implementing the matching `amalgam` trait so they
//! drop straight into
//! `Cache::builder().distributed(..) / .backplane(..) / .distributed_locker(..)`:
//!
//! * [`RedisDistributedCache`] — an L2 byte store (`SET`/`GET`/`DEL`, TTL via `PX`).
//! * [`RedisDistributedLocker`] — cross-node single-flight (`SET key token NX PX`,
//!   released with an atomic compare-and-delete Lua script).
//! * [`RedisBackplane`] — multi-node invalidation over Redis pub/sub.
//!
//! All three are built on [`redis::aio::ConnectionManager`], which is cheap to
//! clone (it is an `Arc` internally) and transparently reconnects, so every
//! operation simply clones the manager to obtain the `&mut` the `redis` API
//! wants. Every [`redis::RedisError`] is mapped to [`Error::Distributed`] (or
//! [`Error::Backplane`] for the backplane) — a `redis` error never escapes.

use std::time::Duration;

use async_trait::async_trait;
use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use redis::{
    Client, ExistenceCheck, Msg, ProtocolVersion, PushInfo, PushKind, SetExpiry, SetOptions,
};
use tokio::sync::broadcast;

use crate::backplane::{Backplane, BackplaneAction, BackplaneMessage};
use crate::distributed::DistributedCache;
use crate::distributed_lock::DistributedLocker;
use crate::error::{Error, Result};
use crate::time::{Timeout, Timestamp};

/// The pub/sub channel every node publishes invalidation messages on.
const BACKPLANE_CHANNEL: &str = "amalgam:backplane";

/// Field separator for the compact backplane wire format
/// (`source_id|timestamp_ticks|action_byte|key`).
const WIRE_SEPARATOR: char = '|';

/// How long to wait between `SET NX` attempts while a distributed lock is held
/// by another node. Mirrors the polling cadence of the in-process locker.
const LOCK_POLL_INTERVAL: Duration = Duration::from_millis(25);

/// Lua for an atomic compare-and-delete lock release: delete the key only if it
/// still holds the caller's token, so a node never releases a lock that already
/// expired and was re-acquired by another node.
const RELEASE_LOCK_SCRIPT: &str = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";

/// Maps any `redis` error to a distributed-cache error.
fn distributed_err(err: redis::RedisError) -> Error {
    Error::Distributed(err.to_string())
}

/// Maps any `redis` error to a backplane error.
fn backplane_err(err: redis::RedisError) -> Error {
    Error::Backplane(err.to_string())
}

/// Opens a Redis [`Client`] from a connection string, mapping failures to
/// [`Error::Distributed`].
fn open_client(connection: impl Into<String>) -> Result<Client> {
    Client::open(connection.into()).map_err(distributed_err)
}

/// Builds an auto-reconnecting [`ConnectionManager`] for the given client.
async fn connect_manager(client: &Client) -> Result<ConnectionManager> {
    client
        .get_connection_manager()
        .await
        .map_err(distributed_err)
}

// ===========================================================================
// L2 distributed cache
// ===========================================================================

/// A Redis-backed L2 distributed cache.
///
/// Stores raw value bytes under each (already-prefixed) key. TTLs are honoured
/// server-side via Redis key expiration (`PX`), so an expired entry simply reads
/// back as a miss (`None`).
#[derive(Clone)]
pub struct RedisDistributedCache {
    manager: ConnectionManager,
}

impl RedisDistributedCache {
    /// Connects to Redis at `connection` (e.g. `redis://127.0.0.1/`).
    ///
    /// # Errors
    /// Returns [`Error::Distributed`] if the URL is invalid or the initial
    /// connection cannot be established.
    pub async fn connect(connection: impl Into<String>) -> Result<Self> {
        let client = open_client(connection)?;
        let manager = connect_manager(&client).await?;
        Ok(Self { manager })
    }
}

#[async_trait]
impl DistributedCache for RedisDistributedCache {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let mut conn = self.manager.clone();
        redis::cmd("GET")
            .arg(key)
            .query_async::<Option<Vec<u8>>>(&mut conn)
            .await
            .map_err(distributed_err)
    }

    async fn set(&self, key: &str, value: Vec<u8>, ttl: Option<Duration>) -> Result<()> {
        let mut conn = self.manager.clone();
        let mut options = SetOptions::default();
        if let Some(ttl) = ttl {
            options = options.with_expiration(SetExpiry::PX(duration_to_millis(ttl)));
        }
        // `SET key value [PX ms]`; the value is opaque bytes.
        redis::cmd("SET")
            .arg(key)
            .arg(value)
            .arg(options)
            .query_async::<()>(&mut conn)
            .await
            .map_err(distributed_err)
    }

    async fn remove(&self, key: &str) -> Result<()> {
        let mut conn = self.manager.clone();
        redis::cmd("DEL")
            .arg(key)
            .query_async::<i64>(&mut conn)
            .await
            .map(|_deleted| ())
            .map_err(distributed_err)
    }
}

// ===========================================================================
// Distributed locker
// ===========================================================================

/// A Redis-backed distributed locker.
///
/// Acquisition is the canonical `SET key token NX PX <ttl>`: it succeeds only
/// when the key is absent, and the `PX` TTL guarantees the lock is released even
/// if the holder dies. Release is an atomic compare-and-delete (a Lua script) so
/// a node can only delete the lock it still owns — never one that already expired
/// and was re-taken by someone else.
#[derive(Clone)]
pub struct RedisDistributedLocker {
    manager: ConnectionManager,
}

impl RedisDistributedLocker {
    /// Connects to Redis at `connection`.
    ///
    /// # Errors
    /// Returns [`Error::Distributed`] if the URL is invalid or the initial
    /// connection cannot be established.
    pub async fn connect(connection: impl Into<String>) -> Result<Self> {
        let client = open_client(connection)?;
        let manager = connect_manager(&client).await?;
        Ok(Self { manager })
    }

    /// One non-blocking `SET key token NX PX <ttl>` attempt.
    ///
    /// Returns `Ok(true)` if the lock was taken, `Ok(false)` if it is currently
    /// held by someone else (the `NX` made the SET a no-op, which Redis reports
    /// as a `Nil` reply → `None`).
    async fn try_acquire(&self, key: &str, token: &str, ttl: Duration) -> Result<bool> {
        let mut conn = self.manager.clone();
        let options = SetOptions::default()
            .conditional_set(ExistenceCheck::NX)
            .with_expiration(SetExpiry::PX(duration_to_millis(ttl)));
        let outcome: Option<String> = redis::cmd("SET")
            .arg(key)
            .arg(token)
            .arg(options)
            .query_async(&mut conn)
            .await
            .map_err(distributed_err)?;
        Ok(outcome.is_some())
    }
}

#[async_trait]
impl DistributedLocker for RedisDistributedLocker {
    async fn acquire(&self, key: &str, ttl: Duration, timeout: Timeout) -> Result<Option<String>> {
        let token = new_token();
        let deadline = match timeout {
            Timeout::After(d) => Some(tokio::time::Instant::now() + d),
            Timeout::Infinite => None,
        };
        loop {
            if self.try_acquire(key, &token, ttl).await? {
                return Ok(Some(token));
            }
            if let Some(deadline) = deadline
                && tokio::time::Instant::now() >= deadline
            {
                return Ok(None);
            }
            tokio::time::sleep(LOCK_POLL_INTERVAL).await;
        }
    }

    async fn release(&self, key: &str, token: &str) -> Result<()> {
        let mut conn = self.manager.clone();
        // Compare-and-delete: only drop the key if it still holds *our* token.
        // Releasing a token that already expired (or was re-taken) is a no-op.
        // Done as a raw `EVAL` (the `redis` crate's `Script` helper is behind the
        // `script` feature, which this crate does not enable).
        redis::cmd("EVAL")
            .arg(RELEASE_LOCK_SCRIPT)
            .arg(1) // numkeys
            .arg(key) // KEYS[1]
            .arg(token) // ARGV[1]
            .query_async::<i64>(&mut conn)
            .await
            .map(|_released| ())
            .map_err(distributed_err)
    }
}

/// Generates an opaque, hard-to-guess lock token (128 bits of randomness).
fn new_token() -> String {
    format!("{:016x}{:016x}", fastrand::u64(..), fastrand::u64(..))
}

// ===========================================================================
// Backplane (pub/sub)
// ===========================================================================

/// A Redis pub/sub backplane.
///
/// [`publish`](Backplane::publish) serialises a [`BackplaneMessage`] to a compact
/// `source_id|timestamp_ticks|action_byte|key` line and `PUBLISH`es it to
/// the shared backplane channel. On [`connect`](RedisBackplane::connect) a background
/// task subscribes (on its own RESP3 connection) to that channel, parses each
/// incoming line back into a [`BackplaneMessage`], and forwards it to an internal
/// [`broadcast::Sender`]; [`subscribe`](Backplane::subscribe) hands out receivers
/// off that sender. Messages that fail to parse are skipped, never fatal.
pub struct RedisBackplane {
    manager: ConnectionManager,
    sender: broadcast::Sender<BackplaneMessage>,
    channel: String,
    // Kept alive for the lifetime of the backplane so the subscriber connection
    // (and thus the relaying task) is not torn down early.
    _subscriber: ConnectionManager,
}

impl RedisBackplane {
    /// Connects to Redis at `connection` and starts relaying incoming backplane
    /// messages to local subscribers.
    ///
    /// # Errors
    /// Returns [`Error::Backplane`] if the URL is invalid, either connection
    /// cannot be established, or the channel subscription fails.
    pub async fn connect(connection: impl Into<String>) -> Result<Self> {
        Self::connect_with_channel(connection, BACKPLANE_CHANNEL).await
    }

    /// Like [`connect`](Self::connect) but on a specific pub/sub `channel`. Give
    /// each cache its own channel when several caches share one Redis server, so
    /// their invalidation messages don't cross-talk.
    ///
    /// # Errors
    /// Returns [`Error::Backplane`] if the URL is invalid, either connection
    /// cannot be established, or the channel subscription fails.
    pub async fn connect_with_channel(
        connection: impl Into<String>,
        channel: impl Into<String>,
    ) -> Result<Self> {
        let connection = connection.into();
        let channel = channel.into();

        // Connection used for publishing (RESP2 is fine for plain PUBLISH).
        let publish_client = Client::open(connection.clone()).map_err(backplane_err)?;
        let manager = publish_client
            .get_connection_manager()
            .await
            .map_err(backplane_err)?;

        let (sender, _) = broadcast::channel::<BackplaneMessage>(256);

        // Dedicated subscriber connection. Pub/sub pushes are only delivered to
        // the push sender over RESP3, so force the protocol regardless of what
        // the caller's URL requested.
        let subscriber = Self::spawn_subscriber(&connection, &channel, sender.clone()).await?;

        Ok(Self {
            manager,
            sender,
            channel,
            _subscriber: subscriber,
        })
    }

    /// Opens a RESP3 [`ConnectionManager`], wires a push channel into it,
    /// subscribes to the shared backplane channel, and spawns the relay task. Returns
    /// the manager so the caller can keep it (and the subscription) alive.
    async fn spawn_subscriber(
        connection: &str,
        channel: &str,
        sender: broadcast::Sender<BackplaneMessage>,
    ) -> Result<ConnectionManager> {
        let info = connection
            .parse::<redis::ConnectionInfo>()
            .map_err(backplane_err)?;
        let redis_settings = info
            .redis_settings()
            .clone()
            .set_protocol(ProtocolVersion::RESP3);
        let info = info.set_redis_settings(redis_settings);
        let client = Client::open(info).map_err(backplane_err)?;

        // `PushInfo`s (including pub/sub `message`s) are delivered onto this
        // plain tokio mpsc, which the relay task drains without needing any
        // `Stream` adapter.
        let (push_tx, push_rx) = tokio::sync::mpsc::unbounded_channel::<PushInfo>();
        let config = ConnectionManagerConfig::new()
            .set_push_sender(push_tx)
            // Re-establish the SUBSCRIBE automatically after a reconnect.
            .set_automatic_resubscription();
        let mut manager = client
            .get_connection_manager_with_config(config)
            .await
            .map_err(backplane_err)?;

        manager.subscribe(channel).await.map_err(backplane_err)?;

        tokio::spawn(relay_pushes(push_rx, sender));
        Ok(manager)
    }
}

#[async_trait]
impl Backplane for RedisBackplane {
    async fn publish(&self, message: BackplaneMessage) -> Result<()> {
        let payload = encode_message(&message);
        let mut conn = self.manager.clone();
        redis::cmd("PUBLISH")
            .arg(&self.channel)
            .arg(payload)
            .query_async::<i64>(&mut conn)
            .await
            .map(|_receivers| ())
            .map_err(backplane_err)
    }

    fn subscribe(&self) -> broadcast::Receiver<BackplaneMessage> {
        self.sender.subscribe()
    }
}

/// Drains pub/sub pushes, decoding each `message` push into a
/// [`BackplaneMessage`] and forwarding it to local subscribers. Runs until the
/// subscriber connection is dropped (which closes `push_rx`).
async fn relay_pushes(
    mut push_rx: tokio::sync::mpsc::UnboundedReceiver<PushInfo>,
    sender: broadcast::Sender<BackplaneMessage>,
) {
    while let Some(push) = push_rx.recv().await {
        // Only ordinary channel messages carry a payload we can decode.
        if push.kind != PushKind::Message {
            continue;
        }
        let Some(msg) = Msg::from_push_info(push) else {
            continue;
        };
        if let Some(message) = decode_message(msg.get_payload_bytes()) {
            // A send error just means "no live subscribers"; that is fine.
            let _ = sender.send(message);
        }
    }
}

// ===========================================================================
// Wire encoding
// ===========================================================================

/// Encodes the single-byte discriminant for a backplane action.
fn action_byte(action: BackplaneAction) -> u8 {
    match action {
        BackplaneAction::Set => 1,
        BackplaneAction::Remove => 2,
        BackplaneAction::Expire => 3,
    }
}

/// Decodes a single-byte action discriminant, or `None` if unrecognised.
fn action_from_byte(byte: u8) -> Option<BackplaneAction> {
    match byte {
        1 => Some(BackplaneAction::Set),
        2 => Some(BackplaneAction::Remove),
        3 => Some(BackplaneAction::Expire),
        _ => None,
    }
}

/// Serialises a [`BackplaneMessage`] to `source_id|timestamp_ticks|action_byte|key`.
///
/// The key is placed last and is the only field allowed to contain the separator
/// (decoding splits with a fixed field count), so arbitrary keys round-trip.
fn encode_message(message: &BackplaneMessage) -> String {
    format!(
        "{src}{sep}{ticks}{sep}{action}{sep}{key}",
        src = message.source_id,
        sep = WIRE_SEPARATOR,
        ticks = message.timestamp.ticks(),
        action = action_byte(message.action),
        key = message.key,
    )
}

/// Parses bytes produced by [`encode_message`] back into a [`BackplaneMessage`],
/// returning `None` on any malformed input (non-UTF-8, missing fields, bad
/// numbers, unknown action) so a corrupt message is skipped rather than fatal.
fn decode_message(bytes: &[u8]) -> Option<BackplaneMessage> {
    let text = std::str::from_utf8(bytes).ok()?;
    // Limit the split so a key containing '|' stays intact in the final field.
    let mut parts = text.splitn(4, WIRE_SEPARATOR);
    let source_id = parts.next()?;
    let ticks: i64 = parts.next()?.parse().ok()?;
    let action_raw: u8 = parts.next()?.parse().ok()?;
    let key = parts.next()?;
    let action = action_from_byte(action_raw)?;
    Some(BackplaneMessage {
        source_id: source_id.into(),
        timestamp: Timestamp::from_ticks(ticks),
        action,
        key: key.into(),
    })
}

// ===========================================================================
// Helpers
// ===========================================================================

/// Converts a [`Duration`] to whole milliseconds for Redis `PX`, saturating at
/// `u64::MAX` and flooring sub-millisecond TTLs to at least 1ms so a very short
/// TTL never collapses into "no expiry".
fn duration_to_millis(ttl: Duration) -> u64 {
    let millis = ttl.as_millis();
    if millis == 0 {
        1
    } else {
        u64::try_from(millis).unwrap_or(u64::MAX)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Integration tests that need a live Redis. They are skipped (return early)
    /// unless `AMALGAM_REDIS_URL` is set, so the suite is a no-op in CI without
    /// a server. Run locally with e.g.
    /// `AMALGAM_REDIS_URL=redis://127.0.0.1/ cargo test --features redis`.
    fn redis_url() -> Option<String> {
        std::env::var("AMALGAM_REDIS_URL").ok()
    }

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
