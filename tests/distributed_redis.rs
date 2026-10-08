//! Live, isolated Redis protocol evidence. CI exports AMALGAM_REQUIRE_REDIS=1.
#![cfg(feature = "redis")]

#[path = "support/redis_fixture.rs"]
mod fixture;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use redis::aio::MultiplexedConnection;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::Notify;
use {
    amalgam::Tag, amalgam::Timeout, amalgam::Timestamp, amalgam::advanced::CacheScope,
    amalgam::advanced::KeyModifierMode, amalgam::advanced::MarkerKind,
    amalgam::advanced::MarkerStoreLimits, amalgam::advanced::MarkerVersion,
    amalgam::provider::AcquisitionPolicy, amalgam::provider::Backplane,
    amalgam::provider::BackplaneAction, amalgam::provider::BackplaneCommand,
    amalgam::provider::BackplaneMessage, amalgam::provider::BackplaneState,
    amalgam::provider::DistributedCache, amalgam::provider::DistributedLocker,
    amalgam::provider::InvalidationStore, amalgam::provider::LeaseError,
    amalgam::provider::LeaseState, amalgam::provider::LeaseTtl, amalgam::provider::LeasedMutation,
    amalgam::provider::LeasedWriteOutcome, amalgam::provider::RedisBackplane,
    amalgam::provider::RedisDistributedCache, amalgam::provider::RedisDistributedLocker,
    amalgam::provider::RedisInvalidationStore, amalgam::provider::RedisIoOptions,
    amalgam::provider::acquire_owned,
};

fn unique(name: &str) -> String {
    format!("amalgam_protocol_{name}_{:016x}", fastrand::u64(..))
}
fn io() -> RedisIoOptions {
    RedisIoOptions::new(Duration::from_millis(500), Duration::from_millis(500)).unwrap()
}
async fn admin(url: &str) -> MultiplexedConnection {
    redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap()
}
async fn client_count(connection: &mut MultiplexedConnection, name: &str) -> usize {
    let listing: String = redis::cmd("CLIENT")
        .arg("LIST")
        .query_async(connection)
        .await
        .unwrap();
    listing
        .lines()
        .filter(|line| {
            line.split_whitespace()
                .any(|field| field == format!("name={name}"))
        })
        .count()
}
async fn await_no_clients(connection: &mut MultiplexedConnection, name: &str) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while client_count(connection, name).await != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}
fn message(key: &str) -> BackplaneMessage {
    BackplaneMessage {
        source_id: "remote|source".into(),
        timestamp: Timestamp::MIN,
        action: BackplaneAction::Set,
        key: key.into(),
    }
}
async fn await_connected(
    health: &mut tokio::sync::watch::Receiver<BackplaneState>,
    after: u64,
) -> u64 {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let BackplaneState::Connected { epoch } = *health.borrow_and_update()
                && epoch.value() > after
            {
                return epoch.value();
            }
            health.changed().await.unwrap();
        }
    })
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backplane_shutdown_remains_terminal_while_the_owned_subscriber_disconnects() {
    let Some(url) = fixture::redis_url() else {
        return;
    };
    let mut connection = admin(&url).await;
    for _ in 0..6 {
        let name = unique("shutdown_race");
        let backplane =
            RedisBackplane::connect_named(&url, unique("shutdown_channel"), &name, 8, io())
                .await
                .unwrap();
        let mut health = backplane.connection_state().unwrap();
        await_connected(&mut health, 0).await;
        let id = backplane.subscriber_client_id().unwrap();
        let disconnect = async {
            redis::cmd("CLIENT")
                .arg("KILL")
                .arg("ID")
                .arg(id.value())
                .query_async::<u64>(&mut connection)
                .await
                .unwrap();
        };
        let (_, stopped) = tokio::join!(disconnect, backplane.shutdown());
        stopped.unwrap();
        assert_eq!(*health.borrow(), BackplaneState::Stopped);
        assert!(backplane.publish(message("closed")).await.is_err());
        backplane.shutdown().await.unwrap();
        assert_eq!(*health.borrow(), BackplaneState::Stopped);
        await_no_clients(&mut connection, &backplane.subscriber_name()).await;
        await_no_clients(&mut connection, &format!("{name}:publish")).await;
    }
}
async fn acl(connection: &mut MultiplexedConnection, user: &str, password: &str, subscribe: bool) {
    let rule = if subscribe {
        "+subscribe"
    } else {
        "-subscribe"
    };
    redis::cmd("ACL")
        .arg("SETUSER")
        .arg(user)
        .arg("on")
        .arg(format!(">{password}"))
        .arg("~*")
        .arg("&*")
        .arg("+@all")
        .arg(rule)
        .query_async::<()>(connection)
        .await
        .unwrap();
}
fn user_url(url: &str, user: &str, password: &str) -> String {
    let tail = url
        .strip_prefix("redis://")
        .expect("live Redis fixture uses redis://");
    let tail = tail.rsplit_once('@').map_or(tail, |(_, tail)| tail);
    format!("redis://{user}:{password}@{tail}")
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
async fn remove_scope(connection: &mut MultiplexedConnection, scope: &CacheScope) {
    redis::pipe()
        .cmd("DEL")
        .arg(format!(
            "\u{1f}amalgam/v2/markers/{}",
            hex(scope.storage_id().as_bytes())
        ))
        .ignore()
        .cmd("SREM")
        .arg("\u{1f}amalgam/v2/marker-scopes")
        .arg(scope.storage_id())
        .ignore()
        .query_async::<()>(connection)
        .await
        .unwrap();
}

#[tokio::test]
async fn live_atomic_markers_survive_new_provider_and_ordinary_private_area_keys() {
    let Some(url) = fixture::redis_url() else {
        return;
    };
    let backend = RedisDistributedCache::connect_with_options(
        url.clone(),
        io(),
        MarkerStoreLimits::new(2, 1024).unwrap(),
    )
    .await
    .unwrap();
    let scope = CacheScope::new(unique("scope"), "v2", KeyModifierMode::None).unwrap();
    let store = backend.invalidation_store().unwrap();
    let kind = MarkerKind::Tag(Tag::new("tag").unwrap());
    let mut tasks = Vec::new();
    for tick in [
        9_007_199_254_740_993,
        9_007_199_254_740_995,
        i64::MIN,
        1,
        -9,
    ] {
        let store = store.clone();
        let scope = scope.clone();
        let kind = kind.clone();
        tasks.push(tokio::spawn(async move {
            store
                .advance(
                    &scope,
                    kind,
                    MarkerVersion::new(Timestamp::from_ticks(tick)),
                )
                .await
                .unwrap()
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    let expected = MarkerVersion::new(Timestamp::from_ticks(9_007_199_254_740_995));
    assert_eq!(
        store.read(&scope, &kind).await.unwrap(),
        Some(expected),
        "atomic max must retain precision above Lua's f64 range"
    );
    let physical_control = format!(
        "\u{1f}amalgam/v2/markers/{}",
        hex(scope.storage_id().as_bytes())
    );
    let already_escaped = format!("\u{1f}amalgam/v2/data/{}", hex(physical_control.as_bytes()));
    for (key, value) in [
        (&physical_control, vec![1]),
        (&already_escaped, vec![2]),
        (&"\u{1f}amalgam/v2/marker-scopes".to_owned(), vec![3]),
    ] {
        backend.set(key, value.clone(), None).await.unwrap();
        assert_eq!(backend.get(key).await.unwrap(), Some(value.into()));
    }
    assert_eq!(
        store.read(&scope, &kind).await.unwrap(),
        Some(expected),
        "ordinary data must never overwrite control storage"
    );
    store
        .advance(
            &scope,
            MarkerKind::Tag(Tag::new("b").unwrap()),
            MarkerVersion::new(Timestamp::from_ticks(2)),
        )
        .await
        .unwrap();
    store
        .advance(
            &scope,
            MarkerKind::Tag(Tag::new("c").unwrap()),
            MarkerVersion::new(Timestamp::from_ticks(3)),
        )
        .await
        .unwrap();
    let restarted =
        RedisInvalidationStore::connect(url.clone(), MarkerStoreLimits::new(2, 1024).unwrap())
            .await
            .unwrap();
    assert_eq!(
        restarted
            .read(&scope, &MarkerKind::ClearRemove)
            .await
            .unwrap(),
        Some(expected),
        "compaction must promote a durable conservative fence before deleting tags"
    );
    for key in [
        &physical_control,
        &already_escaped,
        &"\u{1f}amalgam/v2/marker-scopes".to_owned(),
    ] {
        backend.remove(key).await.unwrap();
    }
    remove_scope(&mut admin(&url).await, &scope).await;
}

#[tokio::test]
async fn live_lease_acquire_deadline_and_successful_attempt_receipt() {
    let Some(url) = fixture::redis_url() else {
        return;
    };
    let locker = Arc::new(
        RedisDistributedLocker::connect_with_options(url, io())
            .await
            .unwrap(),
    );
    let key = unique("deadline");
    let first = locker
        .acquire(
            &key,
            Duration::from_millis(80),
            Timeout::After(Duration::ZERO),
        )
        .await
        .unwrap()
        .unwrap();
    let started = tokio::time::Instant::now();
    assert!(
        locker
            .acquire(
                &key,
                Duration::from_secs(1),
                Timeout::After(Duration::from_millis(2))
            )
            .await
            .unwrap()
            .is_none()
    );
    assert!(started.elapsed() < Duration::from_millis(70));
    let lease = acquire_owned(
        locker.clone(),
        key.clone().into(),
        LeaseTtl::new(Duration::from_millis(100)).unwrap(),
        Timeout::After(Duration::from_millis(500)),
        AcquisitionPolicy::TokenOwned,
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        lease.proof().is_ok(),
        "contention waiting cannot consume the newly acquired TTL"
    );
    locker.release(&key, &first).await.unwrap();
    lease.release().await.unwrap();
}

#[tokio::test]
async fn live_renewal_fences_lost_ownership_and_drop_releases_the_current_token() {
    let Some(url) = fixture::redis_url() else {
        return;
    };
    let locker = Arc::new(
        RedisDistributedLocker::connect_with_options(url.clone(), io())
            .await
            .unwrap(),
    );
    let backend = RedisDistributedCache::connect(url).await.unwrap();
    let key = unique("renewal");
    let lease = acquire_owned(
        locker.clone(),
        key.clone().into(),
        LeaseTtl::new(Duration::from_millis(150)).unwrap(),
        Timeout::Infinite,
        AcquisitionPolicy::TokenOwned,
    )
    .await
    .unwrap()
    .unwrap();
    let proof = lease.proof().unwrap();
    tokio::time::sleep(Duration::from_millis(550)).await;
    assert!(
        lease.proof().is_ok(),
        "native ownership must renew beyond its initial TTL"
    );
    assert!(
        locker
            .acquire(&key, Duration::from_secs(1), Timeout::After(Duration::ZERO))
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        backend
            .write_with_lease(
                &key,
                LeasedMutation::Set {
                    bytes: vec![1],
                    ttl: None
                },
                &proof
            )
            .await
            .unwrap(),
        LeasedWriteOutcome::Committed
    );
    locker.release(&key, proof.token().as_str()).await.unwrap();
    let newer = locker
        .acquire(&key, Duration::from_secs(2), Timeout::After(Duration::ZERO))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        backend
            .write_with_lease(&key, LeasedMutation::Remove, &proof)
            .await
            .unwrap(),
        LeasedWriteOutcome::LeaseLost
    );
    assert_eq!(backend.get(&key).await.unwrap(), Some(vec![1].into()));
    let mut state = lease.state();
    tokio::time::timeout(Duration::from_secs(1), async {
        while *state.borrow_and_update() != LeaseState::Lost {
            state.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert!(matches!(lease.proof(), Err(LeaseError::Lost)));
    drop(lease);
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(
        locker
            .acquire(&key, Duration::from_secs(1), Timeout::After(Duration::ZERO))
            .await
            .unwrap()
            .is_none(),
        "old cleanup cannot delete a new token"
    );
    locker.release(&key, &newer).await.unwrap();
    let lease = acquire_owned(
        locker.clone(),
        key.clone().into(),
        LeaseTtl::new(Duration::from_secs(5)).unwrap(),
        Timeout::Infinite,
        AcquisitionPolicy::TokenOwned,
    )
    .await
    .unwrap()
    .unwrap();
    drop(lease);
    let acquired = locker
        .acquire(
            &key,
            Duration::from_secs(1),
            Timeout::After(Duration::from_millis(500)),
        )
        .await
        .unwrap()
        .unwrap();
    locker.release(&key, &acquired).await.unwrap();
    backend.remove(&key).await.unwrap();
}

struct ReplyGate {
    url: String,
    armed: Arc<AtomicBool>,
    arrived: Arc<Notify>,
    release: Arc<Notify>,
    task: tokio::task::JoinHandle<()>,
}
impl ReplyGate {
    async fn new(url: &str) -> Self {
        let info = url.parse::<redis::ConnectionInfo>().unwrap();
        let redis::ConnectionAddr::Tcp(host, port) = info.addr() else {
            panic!("reply gate fixture requires TCP Redis")
        };
        let upstream = (host.clone(), *port);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_url = format!(
            "redis://{}/{}",
            listener.local_addr().unwrap(),
            info.redis_settings().db()
        );
        let armed = Arc::new(AtomicBool::new(false));
        let arrived = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let task = {
            let armed = armed.clone();
            let arrived = arrived.clone();
            let release = release.clone();
            tokio::spawn(async move {
                let (client, _) = listener.accept().await.unwrap();
                let server = tokio::net::TcpStream::connect(upstream).await.unwrap();
                let (mut client_read, mut client_write) = client.into_split();
                let (server_read, mut server_write) = server.into_split();
                let request = async {
                    let _ = tokio::io::copy(&mut client_read, &mut server_write).await;
                };
                let response = async {
                    let mut server_read = BufReader::new(server_read);
                    let mut reply = Vec::with_capacity(256);
                    loop {
                        reply.clear();
                        match server_read.read_until(b'\n', &mut reply).await {
                            Ok(0) | Err(_) => return,
                            Ok(_) => {}
                        }
                        // This locker connection only grants SET with +OK.
                        // A previous idempotent cleanup returns an integer and
                        // must not consume a later acquisition's reply gate.
                        // Buffering through LF also handles TCP fragmentation.
                        if reply == b"+OK\r\n" && armed.swap(false, Ordering::SeqCst) {
                            arrived.notify_one();
                            release.notified().await;
                        }
                        if client_write.write_all(&reply).await.is_err() {
                            return;
                        }
                    }
                };
                tokio::select! { _ = request => {}, _ = response => {} }
            })
        };
        Self {
            url: proxy_url,
            armed,
            arrived,
            release,
            task,
        }
    }
}
impl Drop for ReplyGate {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[tokio::test]
async fn live_acquisition_cancellation_and_deadline_clean_up_a_granted_but_unreplied_token() {
    let Some(url) = fixture::redis_url() else {
        return;
    };
    let gate = ReplyGate::new(&url).await;
    let mut connection = admin(&url).await;
    let locker = Arc::new(
        RedisDistributedLocker::connect_with_options(gate.url.clone(), io())
            .await
            .unwrap(),
    );
    for cancel in [true, false] {
        let key = unique("unreplied");
        gate.armed.store(true, Ordering::SeqCst);
        let task = {
            let locker = locker.clone();
            let key = key.clone();
            tokio::spawn(async move {
                acquire_owned(
                    locker,
                    key.into(),
                    LeaseTtl::new(Duration::from_secs(2)).unwrap(),
                    if cancel {
                        Timeout::Infinite
                    } else {
                        Timeout::After(Duration::from_millis(200))
                    },
                    AcquisitionPolicy::TokenOwned,
                )
                .await
            })
        };
        tokio::time::timeout(Duration::from_secs(1), gate.arrived.notified())
            .await
            .unwrap();
        let physical = format!("\u{1f}amalgam/v2/lease/{}", hex(key.as_bytes()));
        let held: Option<String> = redis::cmd("GET")
            .arg(&physical)
            .query_async(&mut connection)
            .await
            .unwrap();
        assert!(
            held.is_some(),
            "Redis must have granted the real lease before cancellation"
        );
        if cancel {
            task.abort();
            assert!(matches!(task.await, Err(error) if error.is_cancelled()));
        } else {
            let result = tokio::time::timeout(Duration::from_secs(1), task)
                .await
                .unwrap()
                .unwrap();
            assert!(
                matches!(result, Err(LeaseError::AcquisitionTimeout) | Ok(None)),
                "finite acquisition I/O must terminate without transferring ownership"
            );
        }
        gate.release.notify_one();
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let held: Option<String> = redis::cmd("GET")
                    .arg(&physical)
                    .query_async(&mut connection)
                    .await
                    .unwrap();
                if held.is_none() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn live_subscription_requires_ack_recovers_targeted_disconnects_and_releases_clients() {
    let Some(url) = fixture::redis_url() else {
        return;
    };
    let mut connection = admin(&url).await;
    let user = unique("acl");
    let password = unique("nonce");
    let channel = unique("channel");
    let name = unique("subscriber");
    let restricted = user_url(&url, &user, &password);
    acl(&mut connection, &user, &password, false).await;
    let denied =
        RedisBackplane::connect_named(restricted.clone(), channel.clone(), name.clone(), 16, io())
            .await;
    assert!(
        denied.is_err(),
        "no successful SUBSCRIBE ACK means no connected backplane"
    );
    await_no_clients(&mut connection, &format!("{name}:subscriber")).await;
    await_no_clients(&mut connection, &format!("{name}:publish")).await;
    acl(&mut connection, &user, &password, true).await;
    let backplane = RedisBackplane::connect_named(restricted, channel, name.clone(), 16, io())
        .await
        .unwrap();
    let mut receiver = backplane.subscribe();
    let mut health = backplane.connection_state().unwrap();
    let mut epoch = await_connected(&mut health, 0).await;
    for _ in 0..3 {
        let id = backplane.subscriber_client_id().unwrap();
        assert_eq!(
            client_count(&mut connection, &backplane.subscriber_name()).await,
            1
        );
        acl(&mut connection, &user, &password, false).await;
        let killed: i64 = redis::cmd("CLIENT")
            .arg("KILL")
            .arg("ID")
            .arg(id.value())
            .query_async(&mut connection)
            .await
            .unwrap();
        assert_eq!(
            killed, 1,
            "the actual named subscriber connection must be killed"
        );
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if matches!(
                    *health.borrow_and_update(),
                    BackplaneState::Disconnected { .. }
                ) {
                    break;
                }
                health.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
        assert_eq!(backplane.subscriber_client_id(), None);
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert!(
            matches!(*health.borrow(), BackplaneState::Disconnected { .. }),
            "failed resubscribe ACK cannot announce Connected"
        );
        acl(&mut connection, &user, &password, true).await;
        epoch = await_connected(&mut health, epoch).await;
        assert_ne!(backplane.subscriber_client_id().unwrap(), id);
        backplane
            .publish_command(BackplaneCommand::Data(message("key|with|pipes")))
            .await
            .unwrap();
        let received = tokio::time::timeout(Duration::from_secs(1), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(received.source_id.as_ref(), "remote|source");
        assert_eq!(received.key.as_ref(), "key|with|pipes");
    }
    assert_eq!(backplane.stats().acknowledged_connections, 4);
    backplane.shutdown().await.unwrap();
    backplane.shutdown().await.unwrap();
    assert_eq!(*health.borrow(), BackplaneState::Stopped);
    assert_eq!(backplane.subscriber_client_id(), None);
    assert!(backplane.publish(message("stopped")).await.is_err());
    await_no_clients(&mut connection, &backplane.subscriber_name()).await;
    await_no_clients(&mut connection, &format!("{name}:publish")).await;
    drop(backplane);
    await_no_clients(&mut connection, &format!("{name}:publish")).await;
    redis::cmd("ACL")
        .arg("DELUSER")
        .arg(&user)
        .query_async::<i64>(&mut connection)
        .await
        .unwrap();
}

#[tokio::test]
async fn live_bounded_push_overflow_and_malformed_wire_create_new_continuity_epochs() {
    let Some(url) = fixture::redis_url() else {
        return;
    };
    let channel = unique("overflow");
    let name = unique("buffer");
    let mut connection = admin(&url).await;
    let backplane = RedisBackplane::connect_named(url, channel.clone(), name.clone(), 1, io())
        .await
        .unwrap();
    let mut health = backplane.connection_state().unwrap();
    let first = await_connected(&mut health, 0).await;
    let mut pipeline = redis::pipe();
    for _ in 0..4096 {
        pipeline
            .cmd("PUBLISH")
            .arg(&channel)
            .arg("remote|1|1|key")
            .ignore();
    }
    pipeline.query_async::<()>(&mut connection).await.unwrap();
    let overflow_epoch = await_connected(&mut health, first).await;
    assert!(
        backplane.stats().dropped_pushes > 0,
        "bounded relay overflow must be observed, never an unbounded hidden queue"
    );
    redis::cmd("PUBLISH")
        .arg(&channel)
        .arg("{\"version\":99}")
        .query_async::<i64>(&mut connection)
        .await
        .unwrap();
    let _ = await_connected(&mut health, overflow_epoch).await;
    assert!(backplane.stats().malformed_messages > 0);
    drop(backplane);
    await_no_clients(&mut connection, &format!("{name}:subscriber")).await;
    await_no_clients(&mut connection, &format!("{name}:publish")).await;
}

#[tokio::test]
async fn live_best_effort_retains_local_and_hydrated_l1_over_a_native_subscriber_gap() {
    use amalgam::{
        Cache, EntryOptions, RecoveryConfig, advanced::ReconciliationPolicy,
        provider::JsonSerializer,
    };
    let Some(url) = fixture::redis_url() else {
        return;
    };
    let mut connection = admin(&url).await;
    let user = unique("availability_acl");
    let password = unique("availability_nonce");
    let prefix = unique("availability_data");
    let name = unique("availability_subscriber");
    acl(&mut connection, &user, &password, true).await;
    let backplane = Arc::new(
        RedisBackplane::connect_named(
            user_url(&url, &user, &password),
            unique("availability_channel"),
            name,
            16,
            io(),
        )
        .await
        .unwrap(),
    );
    let backend = Arc::new(RedisDistributedCache::connect(url.clone()).await.unwrap());
    let recovery = RecoveryConfig {
        enabled: false,
        ..RecoveryConfig::default()
    };
    let opts = EntryOptions::new(Duration::from_secs(60));
    let writer = Cache::<u64>::builder()
        .key_prefix(&prefix)
        .distributed(backend.clone())
        .serializer(Arc::new(JsonSerializer))
        .default_options(opts.clone())
        .auto_recovery(recovery.clone())
        .try_build()
        .unwrap();
    let cache = Cache::<u64>::builder()
        .key_prefix(&prefix)
        .distributed(backend.clone())
        .serializer(Arc::new(JsonSerializer))
        .backplane(backplane.clone())
        .reconciliation_policy(ReconciliationPolicy::BackplaneBestEffort)
        .default_options(opts.clone())
        .auto_recovery(recovery)
        .try_build_ready()
        .await
        .unwrap();
    cache
        .set("local", 42)
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    writer
        .set("hydrated", 41)
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert_eq!(
        cache
            .get_or_set::<_, _>(
                "hydrated",
                typed_factory(|_| async { panic!("L2 must hydrate") })
            )
            .await
            .unwrap(),
        41
    );
    let mut health = backplane.connection_state().unwrap();
    let epoch = await_connected(&mut health, 0).await;
    let id = backplane.subscriber_client_id().unwrap();
    acl(&mut connection, &user, &password, false).await;
    let killed: i64 = redis::cmd("CLIENT")
        .arg("KILL")
        .arg("ID")
        .arg(id.value())
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(killed, 1);
    tokio::time::timeout(Duration::from_secs(1), async {
        while !matches!(
            *health.borrow_and_update(),
            BackplaneState::Disconnected { .. }
        ) {
            health.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    // Missed removes cannot be read back from L2 and must not provoke an origin.
    for key in ["local", "hydrated"] {
        backend.remove(&format!("v2:{prefix}{key}")).await.unwrap();
    }
    for key in ["local", "hydrated"] {
        let expected = if key == "local" { 42 } else { 41 };
        assert_eq!(
            cache
                .get_or_set::<_, _>(
                    key,
                    typed_factory(|_| async { panic!("gap must retain L1") })
                )
                .await
                .unwrap(),
            expected
        );
    }
    acl(&mut connection, &user, &password, true).await;
    await_connected(&mut health, epoch).await;
    for key in ["local", "hydrated"] {
        let expected = if key == "local" { 42 } else { 41 };
        assert_eq!(
            cache
                .get_or_set::<_, _>(
                    key,
                    typed_factory(|_| async { panic!("reconnect must retain L1") })
                )
                .await
                .unwrap(),
            expected
        );
    }
    backplane
        .publish(BackplaneMessage {
            source_id: "remote".into(),
            timestamp: Timestamp::MAX,
            action: BackplaneAction::Remove,
            key: format!("v2:{prefix}local").into(),
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while cache
            .try_get("local")
            .options(|_| opts.clone().with_skip_distributed(true, false))
            .await
            .unwrap()
            .is_some()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    cache.shutdown().await.unwrap();
    writer.shutdown().await.unwrap();
    backplane.shutdown().await.unwrap();
    await_no_clients(&mut connection, &backplane.subscriber_name()).await;
    redis::cmd("ACL")
        .arg("DELUSER")
        .arg(&user)
        .query_async::<i64>(&mut connection)
        .await
        .unwrap();
}

fn typed_factory<V, F, Fut>(factory: F) -> F
where
    F: FnOnce(amalgam::FactoryContext<V>) -> Fut,
    Fut: std::future::Future<Output = std::result::Result<V, amalgam::FactoryError>>,
{
    factory
}
