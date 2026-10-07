use amalgam::*;
use async_trait::async_trait;
use std::future::Future;
use std::pin::Pin;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;
use tokio::sync::{Notify, Semaphore};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Direction {
    Encode,
    Decode,
}

#[derive(Default)]
struct Recorder(Mutex<Vec<(Direction, FactoryCancellation)>>);
impl Recorder {
    fn record(&self, direction: Direction, cancellation: FactoryCancellation) {
        self.0.lock().unwrap().push((direction, cancellation));
    }
    fn token(&self, direction: Direction) -> FactoryCancellation {
        self.0
            .lock()
            .unwrap()
            .iter()
            .rev()
            .find(|(recorded, _)| *recorded == direction)
            .unwrap()
            .1
            .clone()
    }
}
struct Gate {
    entered: Notify,
    release: Semaphore,
}
impl Gate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: Notify::new(),
            release: Semaphore::new(0),
        })
    }
    async fn wait(&self) {
        self.entered.notify_one();
        self.release.acquire().await.unwrap().forget();
    }
    async fn entered(&self) {
        tokio::time::timeout(Duration::from_secs(3), self.entered.notified())
            .await
            .unwrap();
    }
}
enum Step {
    Immediate,
    Parked(Arc<Gate>),
    ReportCancellation,
}
impl Step {
    async fn run(&self) -> Result<()> {
        match self {
            Self::Immediate => Ok(()),
            Self::Parked(gate) => {
                gate.wait().await;
                Ok(())
            }
            Self::ReportCancellation => Err(Error::OperationCancelled {
                reason: FactoryCancellationReason::CallerCancelled,
            }),
        }
    }
}
struct Codec {
    recorder: Arc<Recorder>,
    encode: Step,
    decode: Step,
}
#[async_trait]
impl AsyncDistributedSerializer<u64> for Codec {
    async fn serialize_snapshot(&self, snapshot: &DistributedSnapshot<u64>) -> Result<Vec<u8>> {
        JsonSerializer.serialize_snapshot(snapshot)
    }
    async fn deserialize_snapshot(&self, bytes: &[u8]) -> Result<DistributedSnapshot<u64>> {
        JsonSerializer.deserialize_snapshot(bytes)
    }
    async fn serialize_snapshot_with_cancellation(
        &self,
        snapshot: &DistributedSnapshot<u64>,
        cancellation: FactoryCancellation,
    ) -> Result<Vec<u8>> {
        self.recorder.record(Direction::Encode, cancellation);
        self.encode.run().await?;
        JsonSerializer.serialize_snapshot(snapshot)
    }
    async fn deserialize_snapshot_with_cancellation(
        &self,
        bytes: &[u8],
        cancellation: FactoryCancellation,
    ) -> Result<DistributedSnapshot<u64>> {
        self.recorder.record(Direction::Decode, cancellation);
        self.decode.run().await?;
        JsonSerializer.deserialize_snapshot(bytes)
    }
}
fn codec(recorder: &Arc<Recorder>, direction: Direction, step: Step) -> Arc<Codec> {
    let (encode, decode) = match direction {
        Direction::Encode => (step, Step::Immediate),
        Direction::Decode => (Step::Immediate, step),
    };
    Arc::new(Codec {
        recorder: recorder.clone(),
        encode,
        decode,
    })
}
async fn seed(backend: Arc<InMemoryDistributedCache>, clock: Arc<dyn Clock>, opts: EntryOptions) {
    let cache = Cache::<u64>::builder()
        .clock(clock)
        .distributed(backend)
        .serializer(Arc::new(JsonSerializer))
        .default_options(opts)
        .try_build()
        .unwrap();
    cache.try_set("key", 1).await.unwrap().wait().await.unwrap();
    cache.shutdown().await.unwrap();
}
fn assert_cancelled(token: &FactoryCancellation, expected: FactoryCancellationReason) {
    assert!(
        matches!(token.check(), Err(Error::OperationCancelled { reason }) if reason == expected)
    );
}
async fn poll_pending<T>(future: &mut Pin<Box<impl Future<Output = T>>>) {
    std::future::poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
}

#[tokio::test]
async fn caller_cancellation_reaches_both_codec_directions_before_repoll() {
    for direction in [Direction::Encode, Direction::Decode] {
        let clock = Arc::new(SystemClock);
        let backend = Arc::new(InMemoryDistributedCache::new(clock.clone()));
        seed(backend.clone(), clock, EntryOptions::default()).await;
        let before = backend.get("v2:key").await.unwrap();
        let recorder = Arc::new(Recorder::default());
        let gate = Gate::new();
        let cache = Cache::<u64>::builder()
            .distributed(backend.clone())
            .async_serializer(codec(&recorder, direction, Step::Parked(gate.clone())))
            .try_build()
            .unwrap();
        let source = CancellationSource::new();
        let mut operation = Box::pin(async {
            match direction {
                Direction::Encode => cache
                    .try_set_full_cancellable("key", 2, None, Box::from([]), source.token())
                    .await
                    .map(|_| ()),
                Direction::Decode => cache
                    .read_cancellable("key", None, source.token())
                    .await
                    .map(|_| ()),
            }
        });
        poll_pending(&mut operation).await;
        gate.entered().await;
        let token = recorder.token(direction);
        assert!(token.check().is_ok());
        source.cancel();
        assert_cancelled(&token, FactoryCancellationReason::CallerCancelled);
        assert!(matches!(
            operation.await,
            Err(Error::OperationCancelled {
                reason: FactoryCancellationReason::CallerCancelled
            })
        ));
        assert_eq!(backend.get("v2:key").await.unwrap(), before);
        assert!(
            !cache
                .read(
                    "key",
                    Some(EntryOptions::default().with_skip_distributed(true, false))
                )
                .await
                .unwrap()
                .has_value()
        );
        cache.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn dropped_caller_ends_the_codec_scope_without_another_poll() {
    let recorder = Arc::new(Recorder::default());
    let gate = Gate::new();
    let cache = Cache::<u64>::builder()
        .distributed(Arc::new(InMemoryDistributedCache::new(Arc::new(
            SystemClock,
        ))))
        .async_serializer(codec(
            &recorder,
            Direction::Encode,
            Step::Parked(gate.clone()),
        ))
        .try_build()
        .unwrap();
    let mut operation = Box::pin(cache.try_set("key", 2));
    poll_pending(&mut operation).await;
    gate.entered().await;
    let token = recorder.token(Direction::Encode);
    drop(operation);
    assert_cancelled(&token, FactoryCancellationReason::CallerDropped);
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn codec_cancellation_is_never_suppressed_as_a_miss_or_local_only_write() {
    for direction in [Direction::Encode, Direction::Decode] {
        let clock = Arc::new(SystemClock);
        let backend = Arc::new(InMemoryDistributedCache::new(clock.clone()));
        seed(backend.clone(), clock, EntryOptions::default()).await;
        let recorder = Arc::new(Recorder::default());
        let cache = Cache::<u64>::builder()
            .distributed(backend.clone())
            .async_serializer(codec(&recorder, direction, Step::ReportCancellation))
            .default_options(
                EntryOptions::default()
                    .with_rethrow_serialization_exceptions(false)
                    .with_rethrow_distributed_exceptions(false),
            )
            .auto_recovery(RecoveryConfig::default())
            .try_build()
            .unwrap();
        let result = match direction {
            Direction::Encode => cache.try_set("key", 2).await.map(|_| ()),
            Direction::Decode => cache
                .get_or_set("key", |ctx| async move {
                    Ok::<_, amalgam::FactoryError>(ctx.value(2))
                })
                .await
                .map(|_| ()),
        };
        assert!(matches!(
            result,
            Err(Error::OperationCancelled {
                reason: FactoryCancellationReason::CallerCancelled
            })
        ));
        assert_eq!(cache.pending_recovery(), 0);
        assert!(
            !cache
                .read(
                    "key",
                    Some(EntryOptions::default().with_skip_distributed(true, false))
                )
                .await
                .unwrap()
                .has_value()
        );
        cache.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn hard_factory_timeout_ends_the_owned_encoder_with_the_exact_reason() {
    let recorder = Arc::new(Recorder::default());
    let cache = Cache::<u64>::builder()
        .distributed(Arc::new(InMemoryDistributedCache::new(Arc::new(
            SystemClock,
        ))))
        .async_serializer(codec(
            &recorder,
            Direction::Encode,
            Step::Parked(Gate::new()),
        ))
        .default_options(EntryOptions::default().with_factory_timeouts(
            Timeout::Infinite,
            Timeout::After(Duration::from_millis(10)),
            false,
        ))
        .try_build()
        .unwrap();
    assert!(matches!(
        cache
            .get_or_set("key", |ctx| async move {
                Ok::<_, amalgam::FactoryError>(ctx.value(2))
            })
            .await,
        Err(Error::FactoryTimeout { .. })
    ));
    assert_cancelled(
        &recorder.token(Direction::Encode),
        FactoryCancellationReason::HardTimeout,
    );
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn permitted_background_encoder_survives_caller_completion_and_owns_shutdown() {
    for complete in [true, false] {
        let clock = Arc::new(ManualClock::default());
        let backend = Arc::new(InMemoryDistributedCache::new(clock.clone()));
        let opts = EntryOptions::new(Duration::from_secs(1))
            .with_fail_safe(true, Some(Duration::from_secs(60)), None)
            .with_factory_timeouts(
                Timeout::After(Duration::from_millis(10)),
                Timeout::After(Duration::from_secs(1)),
                true,
            );
        seed(backend.clone(), clock.clone(), opts.clone()).await;
        clock.advance(Duration::from_secs(2));
        let recorder = Arc::new(Recorder::default());
        let gate = Gate::new();
        let cache = Cache::<u64>::builder()
            .clock(clock)
            .distributed(backend)
            .async_serializer(codec(
                &recorder,
                Direction::Encode,
                Step::Parked(gate.clone()),
            ))
            .default_options(opts)
            .try_build()
            .unwrap();
        assert_eq!(
            cache
                .get_or_set("key", |ctx| async move {
                    Ok::<_, amalgam::FactoryError>(ctx.value(2))
                })
                .await
                .unwrap(),
            1
        );
        gate.entered().await;
        let token = recorder.token(Direction::Encode);
        assert!(
            token.check().is_ok(),
            "the origin token must outlive the caller's ScopeFinished"
        );
        if complete {
            gate.release.add_permits(1);
            tokio::time::timeout(Duration::from_secs(3), cache.flush_pending())
                .await
                .unwrap()
                .unwrap();
            assert_cancelled(&token, FactoryCancellationReason::ScopeFinished);
            assert_eq!(cache.read("key", None).await.unwrap().value(), Some(&2));
            cache.shutdown().await.unwrap();
        } else {
            tokio::time::timeout(Duration::from_secs(3), cache.shutdown())
                .await
                .unwrap()
                .unwrap();
            assert_cancelled(&token, FactoryCancellationReason::CacheShutdown);
        }
    }
}

#[tokio::test]
async fn distributed_decode_deadline_ends_only_its_phase_with_soft_or_hard_reason() {
    for soft in [true, false] {
        let clock = Arc::new(SystemClock);
        let backend = Arc::new(InMemoryDistributedCache::new(clock.clone()));
        seed(backend.clone(), clock, EntryOptions::default()).await;
        let recorder = Arc::new(Recorder::default());
        let cache = Cache::<u64>::builder()
            .distributed(backend)
            .async_serializer(codec(
                &recorder,
                Direction::Decode,
                Step::Parked(Gate::new()),
            ))
            .default_options(
                EntryOptions::default()
                    .with_fail_safe(true, Some(Duration::from_secs(60)), None)
                    .with_distributed_timeouts(
                        Timeout::After(Duration::from_millis(5)),
                        Timeout::After(Duration::from_millis(15)),
                    ),
            )
            .try_build()
            .unwrap();
        if soft {
            assert_eq!(
                cache
                    .get_or_set_full(
                        "key",
                        |ctx| async move { Ok::<_, amalgam::FactoryError>(ctx.value(2)) },
                        None,
                        Box::from([]),
                        MaybeValue::from_value(1)
                    )
                    .await
                    .unwrap(),
                2
            );
            assert_cancelled(
                &recorder.token(Direction::Decode),
                FactoryCancellationReason::SoftTimeout,
            );
            assert_cancelled(
                &recorder.token(Direction::Encode),
                FactoryCancellationReason::ScopeFinished,
            );
            assert_eq!(cache.read("key", None).await.unwrap().value(), Some(&2));
        } else {
            assert!(matches!(
                cache.read("key", None).await,
                Err(Error::DistributedTimeout { .. })
            ));
            assert_cancelled(
                &recorder.token(Direction::Decode),
                FactoryCancellationReason::HardTimeout,
            );
        }
        cache.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn expiry_decode_and_encode_receive_the_mutation_scope() {
    for direction in [Direction::Encode, Direction::Decode] {
        let clock = Arc::new(SystemClock);
        let backend = Arc::new(InMemoryDistributedCache::new(clock.clone()));
        seed(backend.clone(), clock, EntryOptions::default()).await;
        let before = backend.get("v2:key").await.unwrap();
        let recorder = Arc::new(Recorder::default());
        let gate = Gate::new();
        let cache = Cache::<u64>::builder()
            .distributed(backend.clone())
            .async_serializer(codec(&recorder, direction, Step::Parked(gate.clone())))
            .try_build()
            .unwrap();
        let source = CancellationSource::new();
        let mut operation = Box::pin(cache.try_expire_with_policy_cancellable(
            "key",
            None,
            DistributedExpirePolicy::RetainStale,
            source.token(),
        ));
        poll_pending(&mut operation).await;
        gate.entered().await;
        source.cancel();
        assert_cancelled(
            &recorder.token(direction),
            FactoryCancellationReason::CallerCancelled,
        );
        assert!(matches!(
            operation.await,
            Err(Error::OperationCancelled {
                reason: FactoryCancellationReason::CallerCancelled
            })
        ));
        assert_eq!(backend.get("v2:key").await.unwrap(), before);
        cache.shutdown().await.unwrap();
    }
}

struct LegacyCodec(Arc<Gate>);
#[async_trait]
impl AsyncDistributedSerializer<u64> for LegacyCodec {
    async fn serialize_snapshot(&self, snapshot: &DistributedSnapshot<u64>) -> Result<Vec<u8>> {
        self.0.wait().await;
        JsonSerializer.serialize_snapshot(snapshot)
    }
    async fn deserialize_snapshot(&self, bytes: &[u8]) -> Result<DistributedSnapshot<u64>> {
        JsonSerializer.deserialize_snapshot(bytes)
    }
}
#[tokio::test]
async fn direct_default_hook_preserves_cancellation_after_a_legacy_await() {
    let clock = Arc::new(SystemClock);
    let backend = Arc::new(InMemoryDistributedCache::new(clock.clone()));
    seed(backend.clone(), clock, EntryOptions::default()).await;
    let snapshot = JsonSerializer
        .deserialize_snapshot(&backend.get("v2:key").await.unwrap().unwrap())
        .unwrap();
    let gate = Gate::new();
    let codec = LegacyCodec(gate.clone());
    let source = CancellationSource::new();
    let mut operation =
        Box::pin(codec.serialize_snapshot_with_cancellation(&snapshot, source.token()));
    poll_pending(&mut operation).await;
    gate.entered().await;
    source.cancel();
    gate.release.add_permits(1);
    assert!(matches!(
        operation.await,
        Err(Error::OperationCancelled {
            reason: FactoryCancellationReason::CallerCancelled
        })
    ));
}

struct FailFirstWrite {
    backend: Arc<InMemoryDistributedCache>,
    fail: AtomicBool,
}
#[async_trait]
impl DistributedCache for FailFirstWrite {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.backend.get(key).await
    }
    async fn set(&self, key: &str, bytes: Vec<u8>, ttl: Option<Duration>) -> Result<()> {
        if self.fail.swap(false, Ordering::SeqCst) {
            return Err(Error::Distributed("transient write failure".into()));
        }
        self.backend.set(key, bytes, ttl).await
    }
    async fn remove(&self, key: &str) -> Result<()> {
        self.backend.remove(key).await
    }
    fn invalidation_store(&self) -> Option<Arc<dyn InvalidationStore>> {
        self.backend.invalidation_store()
    }
}
#[tokio::test]
async fn replay_codec_has_an_independent_scope_and_is_cancelled_by_shutdown() {
    let recorder = Arc::new(Recorder::default());
    let gate = Gate::new();
    let cache = Cache::<u64>::builder()
        .distributed(Arc::new(FailFirstWrite {
            backend: Arc::new(InMemoryDistributedCache::new(Arc::new(SystemClock))),
            fail: AtomicBool::new(true),
        }))
        .async_serializer(codec(
            &recorder,
            Direction::Decode,
            Step::Parked(gate.clone()),
        ))
        .default_options(EntryOptions::default().with_rethrow_distributed_exceptions(false))
        .auto_recovery(RecoveryConfig {
            delay: Duration::from_millis(5),
            ..RecoveryConfig::default()
        })
        .try_build()
        .unwrap();
    let report = cache.try_set("key", 2).await.unwrap().wait().await.unwrap();
    assert!(matches!(
        report.distributed,
        EffectOutcome::RecoveryQueued { .. }
    ));
    assert_cancelled(
        &recorder.token(Direction::Encode),
        FactoryCancellationReason::ScopeFinished,
    );
    gate.entered().await;
    let token = recorder.token(Direction::Decode);
    assert!(token.check().is_ok());
    tokio::time::timeout(Duration::from_secs(3), cache.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert_cancelled(&token, FactoryCancellationReason::CacheShutdown);
}

#[tokio::test]
async fn passive_refresh_codec_is_owned_and_cancelled_by_shutdown() {
    let clock = Arc::new(ManualClock::default());
    let backend = Arc::new(InMemoryDistributedCache::new(clock.clone()));
    let backplane = Arc::new(InProcessBackplane::default());
    let recorder = Arc::new(Recorder::default());
    let gate = Gate::new();
    let target = Cache::<u64>::builder()
        .clock(clock.clone())
        .distributed(backend.clone())
        .backplane(backplane.clone())
        .async_serializer(codec(
            &recorder,
            Direction::Decode,
            Step::Parked(gate.clone()),
        ))
        .try_build()
        .unwrap();
    target
        .try_set_full(
            "key",
            1,
            Some(EntryOptions::default().with_skip_distributed(false, true)),
            Box::from([]),
        )
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    let producer = Cache::<u64>::builder()
        .clock(clock.clone())
        .distributed(backend)
        .backplane(backplane)
        .serializer(Arc::new(JsonSerializer))
        .try_build()
        .unwrap();
    clock.advance(Duration::from_secs(1));
    producer
        .try_set("key", 2)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    gate.entered().await;
    let token = recorder.token(Direction::Decode);
    assert!(token.check().is_ok());
    tokio::time::timeout(Duration::from_secs(3), target.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert_cancelled(&token, FactoryCancellationReason::CacheShutdown);
    producer.shutdown().await.unwrap();
}

#[tokio::test]
async fn eager_encoder_outlives_the_hit_and_is_cancelled_by_shutdown() {
    let clock = Arc::new(ManualClock::default());
    let backend = Arc::new(InMemoryDistributedCache::new(clock.clone()));
    let opts = EntryOptions::new(Duration::from_secs(10))
        .with_eager_refresh(Some(EagerThreshold::new(0.5).unwrap()));
    seed(backend.clone(), clock.clone(), opts.clone()).await;
    let recorder = Arc::new(Recorder::default());
    let gate = Gate::new();
    let cache = Cache::<u64>::builder()
        .clock(clock.clone())
        .distributed(backend)
        .async_serializer(codec(
            &recorder,
            Direction::Encode,
            Step::Parked(gate.clone()),
        ))
        .default_options(opts)
        .try_build()
        .unwrap();
    assert_eq!(cache.read("key", None).await.unwrap().value(), Some(&1));
    clock.advance(Duration::from_secs(6));
    assert_eq!(
        cache
            .get_or_set("key", |ctx| async move {
                Ok::<_, amalgam::FactoryError>(ctx.value(2))
            })
            .await
            .unwrap(),
        1
    );
    gate.entered().await;
    let token = recorder.token(Direction::Encode);
    assert!(token.check().is_ok());
    tokio::time::timeout(Duration::from_secs(3), cache.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert_cancelled(&token, FactoryCancellationReason::CacheShutdown);
}

struct SyncCancellingCodec(CancellationSource);
impl DistributedSerializer<u64> for SyncCancellingCodec {
    fn serialize(&self, entry: &DistributedEntry<u64>) -> Result<Vec<u8>> {
        self.0.cancel();
        JsonSerializer.serialize(entry)
    }
    fn deserialize(&self, bytes: &[u8]) -> Result<DistributedEntry<u64>> {
        self.0.cancel();
        JsonSerializer.deserialize(bytes)
    }
}
#[tokio::test]
async fn cancellation_inside_synchronous_codec_callbacks_cannot_commit_or_return_data() {
    for direction in [Direction::Encode, Direction::Decode] {
        let clock = Arc::new(SystemClock);
        let backend = Arc::new(InMemoryDistributedCache::new(clock.clone()));
        seed(backend.clone(), clock, EntryOptions::default()).await;
        let before = backend.get("v2:key").await.unwrap();
        let source = CancellationSource::new();
        let cache = Cache::<u64>::builder()
            .distributed(backend.clone())
            .serializer(Arc::new(SyncCancellingCodec(source.clone())))
            .default_options(
                EntryOptions::default()
                    .with_rethrow_serialization_exceptions(false)
                    .with_rethrow_distributed_exceptions(false),
            )
            .try_build()
            .unwrap();
        let result = match direction {
            Direction::Encode => cache
                .try_set_full_cancellable("key", 2, None, Box::from([]), source.token())
                .await
                .map(|_| ()),
            Direction::Decode => cache
                .read_cancellable("key", None, source.token())
                .await
                .map(|_| ()),
        };
        assert!(matches!(
            result,
            Err(Error::OperationCancelled {
                reason: FactoryCancellationReason::CallerCancelled
            })
        ));
        assert_eq!(backend.get("v2:key").await.unwrap(), before);
        assert!(
            !cache
                .read(
                    "key",
                    Some(EntryOptions::default().with_skip_distributed(true, false))
                )
                .await
                .unwrap()
                .has_value()
        );
        cache.shutdown().await.unwrap();
    }
}
