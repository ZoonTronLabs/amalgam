//! Component facts differ from logical acceptance, bypass and cancellation.
use amalgam::*;
use async_trait::async_trait;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{Notify, Semaphore, broadcast};

fn options() -> EntryOptions {
    EntryOptions::new(Duration::from_secs(60))
        .with_allow_background_distributed_operations(false)
        .with_allow_background_backplane_operations(false)
}
fn no_recovery() -> RecoveryConfig {
    RecoveryConfig {
        enabled: false,
        ..RecoveryConfig::default()
    }
}
fn drain(rx: &mut broadcast::Receiver<LayerEvent>) -> Vec<LayerEvent> {
    let mut result = Vec::new();
    loop {
        match rx.try_recv() {
            Ok(event) => result.push(event),
            Err(broadcast::error::TryRecvError::Empty | broadcast::error::TryRecvError::Closed) => {
                return result;
            }
            Err(broadcast::error::TryRecvError::Lagged(n)) => panic!("unexpected loss: {n}"),
        }
    }
}
fn key(s: &str) -> Arc<str> {
    Arc::from(s)
}

#[tokio::test]
async fn component_memory_attempts_do_not_change_the_legacy_stream() {
    let c = Cache::<u64>::builder().default_options(options()).build();
    let mut layer = c.events().subscribe_layers();
    let mut legacy = c.events().subscribe();
    let runs = Arc::new(AtomicUsize::new(0));
    let counter = runs.clone();
    assert_eq!(
        c.get_or_set("k", move |ctx| async move {
            counter.fetch_add(1, Ordering::SeqCst);
            Ok(ctx.value(17))
        })
        .await
        .unwrap(),
        17
    );
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    assert_eq!(
        drain(&mut layer),
        vec![
            LayerEvent::Memory(MemoryEvent::Miss { key: key("k") }),
            LayerEvent::Memory(MemoryEvent::Miss { key: key("k") }),
            LayerEvent::Memory(MemoryEvent::Set { key: key("k") }),
        ]
    );
    let mut logical_set = 0;
    while let Ok(event) = legacy.try_recv() {
        if matches!(event, CacheEvent::Set { .. }) {
            logical_set += 1;
        }
    }
    assert_eq!(logical_set, 1);
    assert_eq!(c.read("k", None).await.unwrap().value(), Some(&17));
    assert_eq!(
        drain(&mut layer),
        vec![LayerEvent::Memory(MemoryEvent::Hit {
            key: key("k"),
            stale: false
        })]
    );
    c.try_remove("absent").await.unwrap().wait().await.unwrap();
    assert_eq!(
        drain(&mut layer),
        vec![LayerEvent::Memory(MemoryEvent::Remove {
            key: key("absent")
        })]
    );
    c.try_expire("absent").await.unwrap().wait().await.unwrap();
    assert!(drain(&mut layer).is_empty());
    c.shutdown().await.unwrap();
}

#[tokio::test]
async fn rejected_and_skipped_memory_admission_never_claim_a_set() {
    let c = Cache::<u64>::builder().max_capacity(0).build();
    let mut events = c.events().subscribe_layers();
    let receipt = c
        .try_set("rejected", 1)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(matches!(
        receipt.local,
        LocalEffect::Stored(MemoryAdmission::Rejected(_))
    ));
    assert!(drain(&mut events).is_empty());
    let skipped = options().with_skip_memory(false, true);
    c.try_set_full("skipped", 2, Some(skipped), Box::from([]))
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(drain(&mut events).is_empty());
    c.shutdown().await.unwrap();
}

#[tokio::test]
async fn memory_hit_records_expiry_before_final_acceptance_and_secondary_tags() {
    let clock = Arc::new(ManualClock::default());
    let opts = options().with_fail_safe(true, Some(Duration::from_secs(120)), None);
    let c = Cache::<u64>::builder()
        .clock(clock.clone())
        .default_options(opts)
        .build();
    let mut events = c.events().subscribe_layers();
    c.try_set_full("tagged", 3, None, Box::from([Tag::new("group").unwrap()]))
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    drain(&mut events);
    clock.advance(Duration::from_millis(1));
    c.try_remove_by_tag(Tag::new("group").unwrap())
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    drain(&mut events);
    assert!(!c.read("tagged", None).await.unwrap().has_value());
    let facts = drain(&mut events);
    assert_eq!(
        facts.first(),
        Some(&LayerEvent::Memory(MemoryEvent::Hit {
            key: key("tagged"),
            stale: false
        }))
    );
    c.try_set("stale", 4).await.unwrap().wait().await.unwrap();
    drain(&mut events);
    c.try_expire("stale").await.unwrap().wait().await.unwrap();
    assert_eq!(
        drain(&mut events),
        vec![LayerEvent::Memory(MemoryEvent::Expire {
            key: key("stale")
        })]
    );
    assert!(!c.read("stale", None).await.unwrap().has_value());
    assert_eq!(
        drain(&mut events),
        vec![LayerEvent::Memory(MemoryEvent::Hit {
            key: key("stale"),
            stale: true
        })]
    );
    c.shutdown().await.unwrap();
}

fn hybrid(clock: Arc<ManualClock>, store: Arc<dyn DistributedCache>) -> Cache<u64> {
    Cache::builder()
        .clock(clock)
        .distributed(store)
        .serializer(Arc::new(JsonSerializer))
        .default_options(options())
        .distributed_circuit_breaker(Duration::from_secs(2))
        .auto_recovery(no_recovery())
        .build()
}

#[tokio::test]
async fn decoded_l2_hit_and_memory_promotion_are_distinct_actual_facts() {
    let clock = Arc::new(ManualClock::default());
    let store = Arc::new(InMemoryDistributedCache::new(clock.clone()));
    let a = hybrid(clock.clone(), store.clone());
    let b = hybrid(clock.clone(), store.clone());
    let mut wa = a.events().subscribe_layers();
    let mut rb = b.events().subscribe_layers();
    a.try_set("k", 5).await.unwrap().wait().await.unwrap();
    assert_eq!(
        drain(&mut wa),
        vec![
            LayerEvent::Memory(MemoryEvent::Set { key: key("k") }),
            LayerEvent::Distributed(DistributedEvent::Set { key: key("k") }),
        ]
    );
    assert_eq!(b.read("k", None).await.unwrap().value(), Some(&5));
    assert_eq!(
        drain(&mut rb),
        vec![
            LayerEvent::Memory(MemoryEvent::Miss { key: key("k") }),
            LayerEvent::Distributed(DistributedEvent::Hit {
                key: key("k"),
                stale: false
            }),
            LayerEvent::Memory(MemoryEvent::Set { key: key("k") }),
        ]
    );
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[tokio::test]
async fn l2_hit_precedes_durable_tag_rejection_without_a_fabricated_miss() {
    let clock = Arc::new(ManualClock::default());
    let store = Arc::new(InMemoryDistributedCache::new(clock.clone()));
    let a = hybrid(clock.clone(), store.clone());
    a.try_set_full("k", 5, None, Box::from([Tag::new("g").unwrap()]))
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    clock.advance(Duration::from_millis(1));
    a.try_remove_by_tag(Tag::new("g").unwrap())
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    let b = hybrid(clock, store);
    let mut events = b.events().subscribe_layers();
    assert!(!b.read("k", None).await.unwrap().has_value());
    let facts = drain(&mut events);
    assert!(
        facts.contains(&LayerEvent::Distributed(DistributedEvent::Hit {
            key: key("k"),
            stale: false
        }))
    );
    assert!(
        !facts
            .iter()
            .any(|e| matches!(e, LayerEvent::Distributed(DistributedEvent::Miss { .. })))
    );
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[derive(Clone, Copy)]
enum Phase {
    Healthy,
    Unavailable,
    Gated,
}
struct Store {
    inner: InMemoryDistributedCache,
    phase: Mutex<Phase>,
    reads: AtomicUsize,
    entered: Notify,
    release: Semaphore,
}
impl Store {
    fn new(clock: Arc<ManualClock>, phase: Phase) -> Self {
        Self {
            inner: InMemoryDistributedCache::new(clock),
            phase: Mutex::new(phase),
            reads: AtomicUsize::new(0),
            entered: Notify::new(),
            release: Semaphore::new(0),
        }
    }
    fn phase(&self) -> Phase {
        *self.phase.lock().unwrap()
    }
    fn recover(&self) {
        *self.phase.lock().unwrap() = Phase::Healthy;
    }
}
#[async_trait]
impl DistributedCache for Store {
    async fn get(&self, k: &str) -> Result<Option<Vec<u8>>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        match self.phase() {
            Phase::Healthy => {}
            Phase::Unavailable => {
                return Err(Error::distributed(std::io::Error::other(
                    "original unavailable",
                )));
            }
            Phase::Gated => {
                self.entered.notify_one();
                self.release.acquire().await.unwrap().forget();
            }
        }
        self.inner.get(k).await
    }
    async fn set(&self, k: &str, v: Vec<u8>, ttl: Option<Duration>) -> Result<()> {
        self.inner.set(k, v, ttl).await
    }
    async fn remove(&self, k: &str) -> Result<()> {
        self.inner.remove(k).await
    }
    fn invalidation_store(&self) -> Option<Arc<dyn InvalidationStore>> {
        self.inner.invalidation_store()
    }
}

#[tokio::test]
async fn suppressed_transport_fault_misses_once_but_open_circuit_skips_the_attempt() {
    let clock = Arc::new(ManualClock::default());
    let store = Arc::new(Store::new(clock.clone(), Phase::Unavailable));
    let c = hybrid(clock.clone(), store.clone());
    let mut events = c.events().subscribe_layers();
    assert_eq!(c.get_or_default("first", 7, None).await, 7);
    let first = drain(&mut events);
    assert!(first.contains(&LayerEvent::Distributed(
        DistributedEvent::CircuitBreakerChange { closed: false }
    )));
    assert_eq!(
        first
            .iter()
            .filter(|e| matches!(e, LayerEvent::Distributed(DistributedEvent::Miss { .. })))
            .count(),
        1
    );
    let reads = store.reads.load(Ordering::SeqCst);
    assert_eq!(c.get_or_default("second", 8, None).await, 8);
    assert_eq!(reads, store.reads.load(Ordering::SeqCst));
    assert!(
        !drain(&mut events)
            .iter()
            .any(|e| matches!(e, LayerEvent::Distributed(_)))
    );
    store.recover();
    clock.advance(Duration::from_secs(3));
    assert!(!c.read("recovered", None).await.unwrap().has_value());
    let after = drain(&mut events);
    assert!(after.contains(&LayerEvent::Distributed(
        DistributedEvent::CircuitBreakerChange { closed: true }
    )));
    assert!(
        after.contains(&LayerEvent::Distributed(DistributedEvent::Miss {
            key: key("recovered")
        }))
    );
    assert_eq!(store.reads.load(Ordering::SeqCst), reads + 1);
    c.shutdown().await.unwrap();
}

#[tokio::test]
async fn cache_read_deadline_is_typed_and_never_trips_the_transport_circuit() {
    let clock = Arc::new(ManualClock::default());
    let store = Arc::new(Store::new(clock.clone(), Phase::Gated));
    let c = hybrid(clock, store.clone());
    let mut events = c.events().subscribe_layers();
    let budget = Duration::from_millis(15);
    let opts = options().with_distributed_timeouts(Timeout::After(budget), Timeout::After(budget));
    assert!(
        matches!(c.read("timed", Some(opts.clone())).await, Err(Error::DistributedTimeout { elapsed }) if elapsed == budget)
    );
    assert!(!drain(&mut events).iter().any(|e| matches!(
        e,
        LayerEvent::Distributed(DistributedEvent::CircuitBreakerChange { .. })
    )));
    assert_eq!(c.get_or_default("suppressed", 9, Some(opts)).await, 9);
    let facts = drain(&mut events);
    assert_eq!(
        facts
            .iter()
            .filter(|e| matches!(e, LayerEvent::Distributed(DistributedEvent::Miss { .. })))
            .count(),
        1
    );
    assert!(!facts.iter().any(|e| matches!(
        e,
        LayerEvent::Distributed(DistributedEvent::CircuitBreakerChange { .. })
    )));
    store.recover();
    assert!(!c.read("healthy", None).await.unwrap().has_value());
    assert!(
        drain(&mut events).contains(&LayerEvent::Distributed(DistributedEvent::Miss {
            key: key("healthy")
        }))
    );
    c.shutdown().await.unwrap();
}

#[tokio::test]
async fn caller_cancellation_does_not_become_a_layer_miss_or_codec_failure() {
    let clock = Arc::new(ManualClock::default());
    let store = Arc::new(Store::new(clock.clone(), Phase::Gated));
    let c = hybrid(clock, store.clone());
    let mut events = c.events().subscribe_layers();
    let source = CancellationSource::new();
    let running = c.clone();
    let token = source.token();
    let task = tokio::spawn(async move { running.read_cancellable("cancel", None, token).await });
    tokio::time::timeout(Duration::from_secs(2), store.entered.notified())
        .await
        .unwrap();
    source.cancel();
    assert!(matches!(
        task.await.unwrap(),
        Err(Error::OperationCancelled {
            reason: FactoryCancellationReason::CallerCancelled
        })
    ));
    assert!(
        !drain(&mut events)
            .iter()
            .any(|e| matches!(e, LayerEvent::Distributed(_)))
    );
    c.shutdown().await.unwrap();
}

struct RejectEncode;
impl DistributedSerializer<u64> for RejectEncode {
    fn serialize(&self, _: &DistributedEntry<u64>) -> Result<Vec<u8>> {
        Err(Error::serialization(std::io::Error::other(
            "original codec failure",
        )))
    }
    fn deserialize(&self, b: &[u8]) -> Result<DistributedEntry<u64>> {
        JsonSerializer.deserialize(b)
    }
}

#[tokio::test]
async fn codec_failures_keep_causes_and_do_not_claim_transport_success_or_open_a_circuit() {
    let clock = Arc::new(ManualClock::default());
    let store = Arc::new(InMemoryDistributedCache::new(clock.clone()));
    let c = Cache::<u64>::builder()
        .clock(clock.clone())
        .distributed(store.clone())
        .serializer(Arc::new(RejectEncode))
        .default_options(options().with_rethrow_serialization_exceptions(false))
        .distributed_circuit_breaker(Duration::from_secs(2))
        .auto_recovery(no_recovery())
        .build();
    let mut events = c.events().subscribe_layers();
    let result = c.try_set("encode", 10).await.unwrap().wait().await.unwrap();
    assert!(matches!(
        result.distributed,
        EffectOutcome::FailedSuppressed {
            cause: Error::Codec(_)
        }
    ));
    let facts = drain(&mut events);
    assert!(facts.contains(&LayerEvent::Distributed(
        DistributedEvent::SerializationError { key: key("encode") }
    )));
    assert!(!facts.iter().any(|e| matches!(
        e,
        LayerEvent::Distributed(
            DistributedEvent::Set { .. } | DistributedEvent::CircuitBreakerChange { .. }
        )
    )));
    let read = hybrid(clock, store.clone());
    let mut reader = read.events().subscribe_layers();
    store
        .set("v2:decode", b"not-a-snapshot".to_vec(), None)
        .await
        .unwrap();
    let opts = options().with_rethrow_serialization_exceptions(false);
    assert_eq!(read.get_or_default("decode", 11, Some(opts)).await, 11);
    assert_eq!(
        drain(&mut reader),
        vec![
            LayerEvent::Memory(MemoryEvent::Miss { key: key("decode") }),
            LayerEvent::Distributed(DistributedEvent::DeserializationError { key: key("decode") }),
            LayerEvent::Distributed(DistributedEvent::Miss { key: key("decode") }),
        ]
    );
    c.shutdown().await.unwrap();
    read.shutdown().await.unwrap();
}

async fn receive(rx: &mut broadcast::Receiver<LayerEvent>) -> BackplaneMessage {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let LayerEvent::Backplane(BackplaneEvent::MessageReceived { message }) =
                rx.recv().await.unwrap()
            {
                return message;
            }
        }
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn rich_backplane_frames_preserve_source_revision_action_key_and_rejected_envelopes() {
    let clock = Arc::new(ManualClock::default());
    let bp = Arc::new(InProcessBackplane::default());
    let c = Cache::<u64>::builder()
        .clock(clock.clone())
        .backplane(bp.clone())
        .instance_id("local")
        .default_options(options())
        .reconciliation_policy(ReconciliationPolicy::BackplaneBestEffort)
        .build();
    let mut events = c.events().subscribe_layers();
    c.try_set("k", 12).await.unwrap().wait().await.unwrap();
    let facts = drain(&mut events);
    assert!(facts.iter().any(|e| matches!(e, LayerEvent::Backplane(BackplaneEvent::MessagePublished { command: BackplaneCommand::Data(m) }) if m.source_id.as_ref()=="local" && m.key.as_ref()=="v2:k" && m.action==BackplaneAction::Set && m.timestamp==clock.now())));
    assert!(!facts.iter().any(|e| matches!(
        e,
        LayerEvent::Backplane(BackplaneEvent::MessageReceived { .. })
    )));
    let foreign = BackplaneMessage {
        source_id: key("remote"),
        timestamp: clock.now(),
        action: BackplaneAction::Remove,
        key: key("v2:k"),
    };
    bp.publish(foreign.clone()).await.unwrap();
    assert_eq!(receive(&mut events).await, foreign);
    let invalid = BackplaneMessage {
        source_id: key("\u{1f}amalgam-control-v2:not-hex"),
        timestamp: clock.now(),
        action: BackplaneAction::Set,
        key: key("invalid"),
    };
    bp.publish(invalid.clone()).await.unwrap();
    assert_eq!(receive(&mut events).await, invalid);
    c.shutdown().await.unwrap();
}

#[tokio::test]
async fn explicit_layer_skips_and_ignore_incoming_emit_no_false_component_calls() {
    let clock = Arc::new(ManualClock::default());
    let store = Arc::new(InMemoryDistributedCache::new(clock.clone()));
    let bp = Arc::new(InProcessBackplane::default());
    let c = Cache::<u64>::builder()
        .clock(clock)
        .distributed(store)
        .serializer(Arc::new(JsonSerializer))
        .backplane(bp.clone())
        .ignore_incoming_backplane(true)
        .default_options(options())
        .build();
    let mut events = c.events().subscribe_layers();
    let skip = options()
        .with_skip_memory(true, true)
        .with_skip_distributed(true, true)
        .with_skip_backplane_notifications(true);
    c.try_set_full("skip", 13, Some(skip.clone()), Box::from([]))
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(!c.read("skip", Some(skip)).await.unwrap().has_value());
    assert!(drain(&mut events).is_empty());
    bp.publish(BackplaneMessage {
        source_id: key("remote"),
        timestamp: Timestamp::from_ticks(1),
        action: BackplaneAction::Remove,
        key: key("v2:skip"),
    })
    .await
    .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(30), events.recv())
            .await
            .is_err()
    );
    c.shutdown().await.unwrap();
}

#[tokio::test]
async fn layer_loss_and_close_are_independent_of_logical_delivery() {
    let events = Events::with_capacity(1);
    let mut rx = events.subscribe_layers_resilient();
    let mut legacy = events.subscribe();
    for i in 0..5 {
        events.emit_layer(LayerEvent::Memory(MemoryEvent::Miss {
            key: key(&i.to_string()),
        }));
    }
    events.emit(CacheEvent::Clear);
    assert_eq!(legacy.recv().await.unwrap(), CacheEvent::Clear);
    assert_eq!(
        rx.recv().await.unwrap(),
        LayerEvent::Memory(MemoryEvent::Miss { key: key("4") })
    );
    assert_eq!(rx.lost_events(), 4);
    drop(events);
    assert!(rx.recv().await.is_err());
}

#[cfg(feature = "redis")]
#[path = "support/redis_fixture.rs"]
mod redis_fixture;

#[cfg(feature = "redis")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_redis_layers_cover_fenced_factory_cold_peer_and_pubsub_remove() {
    let Some(url) = redis_fixture::redis_url() else {
        return;
    };
    let name = format!("layer_events_{:016x}", fastrand::u64(..));
    let prefix = format!("{name}:");
    let mut nodes = Vec::with_capacity(2);
    for id in ["a", "b"] {
        let l2 = Arc::new(RedisDistributedCache::connect(url.clone()).await.unwrap());
        let bp = Arc::new(RedisBackplane::connect(url.clone()).await.unwrap());
        let locker = Arc::new(RedisDistributedLocker::connect(url.clone()).await.unwrap());
        nodes.push(
            Cache::<u64>::builder()
                .name(&name)
                .key_prefix(&prefix)
                .instance_id(format!("{name}-{id}"))
                .distributed(l2)
                .serializer(Arc::new(JsonSerializer))
                .backplane(bp)
                .distributed_locker(locker)
                .default_options(options())
                .try_build_ready()
                .await
                .unwrap(),
        );
    }
    let a = &nodes[0];
    let b = &nodes[1];
    let mut ea = a.events().subscribe_layers();
    let mut eb = b.events().subscribe_layers();
    assert_eq!(
        a.get_or_set("k", |ctx| async move { Ok(ctx.value(19)) })
            .await
            .unwrap(),
        19
    );
    assert!(drain(&mut ea).iter().any(|e| matches!(e, LayerEvent::Distributed(DistributedEvent::Set { key }) if key.as_ref()==format!("{prefix}k"))));
    // Node B has not read before this point: incoming Set does not create a cold L1.
    assert_eq!(
        b.get_or_set(
            "k",
            |_| async move { panic!("cold peer must use actual L2") }
        )
        .await
        .unwrap(),
        19
    );
    assert!(drain(&mut eb).iter().any(|e| matches!(e, LayerEvent::Distributed(DistributedEvent::Hit { key, stale:false }) if key.as_ref()==format!("{prefix}k"))));
    a.try_remove("k").await.unwrap().wait().await.unwrap();
    let message = receive(&mut eb).await;
    // An older Set may still be queued in the diagnostic stream.
    let message = if message.action == BackplaneAction::Remove {
        message
    } else {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let m = receive(&mut eb).await;
                if m.action == BackplaneAction::Remove {
                    break m;
                }
            }
        })
        .await
        .unwrap()
    };
    assert_eq!(message.source_id.as_ref(), format!("{name}-a"));
    assert_eq!(message.key.as_ref(), format!("v2:{prefix}k"));
    assert!(
        drain(&mut ea)
            .iter()
            .any(|e| matches!(e, LayerEvent::Distributed(DistributedEvent::Remove { .. })))
    );
    let expected_remove = LayerEvent::Memory(MemoryEvent::Remove {
        key: key(&format!("{prefix}k")),
    });
    let mut removal_seen = false;
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let absent = !b.read("k", None).await.unwrap().has_value();
            // Consume the bounded stream while waiting for the received frame
            // to finish its physical effect; reads produce their own events.
            removal_seen |= drain(&mut eb).contains(&expected_remove);
            if absent && removal_seen {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(removal_seen);
    for node in nodes {
        node.shutdown().await.unwrap();
    }
}

struct FaultingBackplane {
    inner: InProcessBackplane,
    fail: AtomicBool,
}
#[async_trait]
impl Backplane for FaultingBackplane {
    async fn publish(&self, message: BackplaneMessage) -> Result<()> {
        if self.fail.swap(false, Ordering::SeqCst) {
            return Err(Error::backplane(std::io::Error::other(
                "original notification fault",
            )));
        }
        self.inner.publish(message).await
    }
    fn subscribe(&self) -> broadcast::Receiver<BackplaneMessage> {
        self.inner.subscribe()
    }
    fn connection_state(&self) -> Option<tokio::sync::watch::Receiver<BackplaneState>> {
        self.inner.connection_state()
    }
}

#[tokio::test]
async fn invalid_foreign_frame_closes_transport_circuit_before_validation_and_still_invalidates_strict_l1()
 {
    let bp = Arc::new(FaultingBackplane {
        inner: InProcessBackplane::default(),
        fail: AtomicBool::new(true),
    });
    let c = Cache::<u64>::builder()
        .backplane(bp.clone())
        .instance_id("local")
        .backplane_circuit_breaker(Duration::from_secs(60))
        .default_options(options())
        .auto_recovery(no_recovery())
        .build();
    let mut events = c.events().subscribe_layers();
    let receipt = c.try_set("warm", 23).await.unwrap().wait().await.unwrap();
    assert!(matches!(
        receipt.backplane,
        EffectOutcome::FailedSuppressed { .. }
    ));
    let before = drain(&mut events);
    assert!(before.contains(&LayerEvent::Backplane(
        BackplaneEvent::CircuitBreakerChange { closed: false }
    )));
    assert!(!before.iter().any(|e| matches!(
        e,
        LayerEvent::Backplane(BackplaneEvent::MessagePublished { .. })
    )));
    assert_eq!(c.read("warm", None).await.unwrap().value(), Some(&23));
    drain(&mut events);
    let invalid = BackplaneMessage {
        source_id: key("\u{1f}amalgam-control-v2:not-hex"),
        timestamp: Timestamp::from_ticks(1),
        action: BackplaneAction::Set,
        key: key("invalid"),
    };
    bp.inner.publish(invalid.clone()).await.unwrap();
    let closed = events.recv().await.unwrap();
    assert_eq!(
        closed,
        LayerEvent::Backplane(BackplaneEvent::CircuitBreakerChange { closed: true })
    );
    assert_eq!(
        events.recv().await.unwrap(),
        LayerEvent::Backplane(BackplaneEvent::MessageReceived { message: invalid })
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if !c.read("warm", None).await.unwrap().has_value() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    c.shutdown().await.unwrap();
}

struct PendingMarkers;
#[async_trait]
impl InvalidationStore for PendingMarkers {
    async fn read(
        &self,
        _: &CacheScope,
        _: &MarkerKind,
    ) -> std::result::Result<Option<MarkerVersion>, MarkerError> {
        std::future::pending().await
    }
    async fn advance(
        &self,
        _: &CacheScope,
        kind: MarkerKind,
        version: MarkerVersion,
    ) -> std::result::Result<MarkerAdvanceOutcome, MarkerError> {
        Ok(MarkerAdvanceOutcome::Advanced(StoredMarker::new(
            kind, version,
        )))
    }
}
#[tokio::test]
async fn decoded_hit_followed_by_marker_timeout_never_claims_an_additional_component_miss() {
    let clock = Arc::new(ManualClock::default());
    let store = Arc::new(InMemoryDistributedCache::new(clock.clone()));
    let writer = hybrid(clock.clone(), store.clone());
    writer
        .try_set("key", 28)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    writer.shutdown().await.unwrap();
    let reader = Cache::<u64>::builder()
        .clock(clock)
        .distributed(store)
        .serializer(Arc::new(JsonSerializer))
        .invalidation_store(Arc::new(PendingMarkers))
        .distributed_circuit_breaker(Duration::from_secs(2))
        .default_options(options())
        .auto_recovery(no_recovery())
        .build();
    let mut events = reader.events().subscribe_layers();
    let opts = options()
        .with_distributed_timeouts(Timeout::Infinite, Timeout::After(Duration::from_millis(15)));
    assert_eq!(reader.get_or_default("key", 29, Some(opts)).await, 29);
    assert_eq!(
        drain(&mut events),
        vec![
            LayerEvent::Memory(MemoryEvent::Miss { key: key("key") }),
            LayerEvent::Distributed(DistributedEvent::Hit {
                key: key("key"),
                stale: false
            }),
        ]
    );
    reader.shutdown().await.unwrap();
}
