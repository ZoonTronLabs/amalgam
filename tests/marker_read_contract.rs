//! Public independent secondary-read, authority and cancellation contracts.

use amalgam::*;
use async_trait::async_trait;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{Notify, Semaphore};

#[derive(Clone)]
enum Mode {
    Pass,
    Absent,
    Backend,
    Protocol,
    Unsupported,
    Park(Arc<Gate>),
    ReplyAfter { gate: Arc<Gate>, reply: Reply },
    Delay(Duration),
}

#[derive(Clone)]
enum Selection {
    Tags,
    Only(MarkerKind),
    All,
}

#[derive(Clone)]
enum Reply {
    Present(MarkerVersion),
    Absent,
    Backend,
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
}

#[derive(Debug, thiserror::Error)]
#[error("original marker provider cause")]
struct Cause;

struct Witness {
    token: FactoryCancellation,
    stopped: Arc<Mutex<Vec<Option<FactoryCancellationReason>>>>,
}
impl Drop for Witness {
    fn drop(&mut self) {
        let reason = match self.token.check() {
            Ok(()) => None,
            Err(Error::OperationCancelled { reason }) => Some(reason),
            Err(other) => panic!("unexpected cancellation contract: {other:?}"),
        };
        self.stopped.lock().unwrap().push(reason);
    }
}

struct Store {
    inner: InMemoryInvalidationStore,
    mode: Mutex<(Selection, Mode)>,
    reads: Mutex<Vec<MarkerKind>>,
    stopped: Arc<Mutex<Vec<Option<FactoryCancellationReason>>>>,
}
impl Store {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: InMemoryInvalidationStore::default(),
            mode: Mutex::new((Selection::Tags, Mode::Pass)),
            reads: Mutex::new(Vec::new()),
            stopped: Arc::new(Mutex::new(Vec::new())),
        })
    }
    fn mode(&self, mode: Mode) {
        *self.mode.lock().unwrap() = (Selection::Tags, mode);
    }
    fn mode_for(&self, kind: MarkerKind, mode: Mode) {
        *self.mode.lock().unwrap() = (Selection::Only(kind), mode);
    }
    fn mode_all(&self, mode: Mode) {
        *self.mode.lock().unwrap() = (Selection::All, mode);
    }
    fn count(&self, kind: &MarkerKind) -> usize {
        self.reads
            .lock()
            .unwrap()
            .iter()
            .filter(|read| *read == kind)
            .count()
    }
    fn tag_count(&self) -> usize {
        self.count(&MarkerKind::Tag(tag()))
    }
}
#[async_trait]
impl InvalidationStore for Store {
    async fn read(
        &self,
        scope: &CacheScope,
        kind: &MarkerKind,
    ) -> std::result::Result<Option<MarkerVersion>, MarkerError> {
        self.reads.lock().unwrap().push(kind.clone());
        let (selection, selected) = self.mode.lock().unwrap().clone();
        let selected_kind = match selection {
            Selection::Tags => matches!(kind, MarkerKind::Tag(_)),
            Selection::Only(selected) => selected == *kind,
            Selection::All => true,
        };
        let mode = if selected_kind { selected } else { Mode::Pass };
        match mode {
            Mode::Pass => {}
            Mode::Absent => return Ok(None),
            Mode::Backend => return Err(MarkerError::backend(Cause)),
            Mode::Protocol => return Err(MarkerError::protocol(Cause)),
            Mode::Unsupported => return Err(MarkerError::Unsupported),
            Mode::Park(gate) => {
                gate.entered.notify_one();
                gate.release.acquire().await.unwrap().forget();
            }
            Mode::ReplyAfter { gate, reply } => {
                gate.entered.notify_one();
                gate.release.acquire().await.unwrap().forget();
                return match reply {
                    Reply::Present(version) => Ok(Some(version)),
                    Reply::Absent => Ok(None),
                    Reply::Backend => Err(MarkerError::backend(Cause)),
                };
            }
            Mode::Delay(duration) => tokio::time::sleep(duration).await,
        }
        self.inner.read(scope, kind).await
    }
    async fn read_with_cancellation(
        &self,
        scope: &CacheScope,
        kind: &MarkerKind,
        cancellation: FactoryCancellation,
    ) -> std::result::Result<Option<MarkerVersion>, MarkerReadError> {
        let _witness = Witness {
            token: cancellation.clone(),
            stopped: self.stopped.clone(),
        };
        MarkerReadError::check_cancellation(&cancellation)?;
        let result = self.read(scope, kind).await;
        MarkerReadError::check_cancellation(&cancellation)?;
        Ok(result?)
    }
    async fn advance(
        &self,
        scope: &CacheScope,
        kind: MarkerKind,
        version: MarkerVersion,
    ) -> std::result::Result<MarkerAdvanceOutcome, MarkerError> {
        self.inner.advance(scope, kind, version).await
    }
}

fn tag() -> Tag {
    Tag::new("group").unwrap()
}
fn value_options() -> EntryOptions {
    EntryOptions::new(Duration::from_secs(3600)).with_allow_background_distributed_operations(false)
}
fn control_options() -> EntryOptions {
    EntryOptions::tag_defaults().with_duration(Duration::from_secs(1))
}
async fn seeded(clock: Arc<ManualClock>) -> Arc<InMemoryDistributedCache> {
    seeded_with_tags(clock, vec![tag()].into_boxed_slice()).await
}
async fn seeded_with_tags(
    clock: Arc<ManualClock>,
    tags: Box<[Tag]>,
) -> Arc<InMemoryDistributedCache> {
    let backend = Arc::new(InMemoryDistributedCache::new(clock.clone()));
    let writer = Cache::<u64>::builder()
        .clock(clock)
        .distributed(backend.clone())
        .serializer(Arc::new(JsonSerializer))
        .default_options(value_options())
        .try_build()
        .unwrap();
    writer
        .try_set_full("key", 7, None, tags)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    writer.shutdown().await.unwrap();
    backend
}
fn reader(
    clock: Arc<ManualClock>,
    backend: Arc<InMemoryDistributedCache>,
    store: Arc<dyn InvalidationStore>,
    tags: EntryOptions,
) -> Cache<u64> {
    Cache::<u64>::builder()
        .clock(clock)
        .distributed(backend)
        .invalidation_store(store)
        .serializer(Arc::new(JsonSerializer))
        .default_options(value_options())
        .tags_default_options(tags)
        .marker_read_policy(MarkerReadPolicy::OptionsControlled)
        .try_build()
        .unwrap()
}
fn clock() -> Arc<ManualClock> {
    Arc::new(ManualClock::new(Timestamp::from_ticks(10_000)))
}
fn marker_events(
    events: &mut tokio::sync::broadcast::Receiver<CacheEvent>,
) -> Vec<MarkerReadOutcome> {
    let mut outcomes = Vec::new();
    while let Ok(event) = events.try_recv() {
        if let CacheEvent::MarkerRead {
            kind: MarkerKind::Tag(_),
            outcome,
        } = event
        {
            outcomes.push(outcome);
        }
    }
    outcomes
}

struct CancelledRead(Store);

#[async_trait]
impl InvalidationStore for CancelledRead {
    async fn read(
        &self,
        scope: &CacheScope,
        kind: &MarkerKind,
    ) -> std::result::Result<Option<MarkerVersion>, MarkerError> {
        self.0.read(scope, kind).await
    }

    async fn read_with_cancellation(
        &self,
        scope: &CacheScope,
        kind: &MarkerKind,
        cancellation: FactoryCancellation,
    ) -> std::result::Result<Option<MarkerVersion>, MarkerReadError> {
        MarkerReadError::check_cancellation(&cancellation)?;
        if matches!(kind, MarkerKind::Tag(_)) {
            return Err(MarkerReadError::Cancelled {
                reason: FactoryCancellationReason::LeaseLost,
            });
        }
        Ok(self.read(scope, kind).await?)
    }

    async fn advance(
        &self,
        scope: &CacheScope,
        kind: MarkerKind,
        candidate: MarkerVersion,
    ) -> std::result::Result<MarkerAdvanceOutcome, MarkerError> {
        self.0.advance(scope, kind, candidate).await
    }
}

#[tokio::test]
async fn provider_reported_cancellation_cannot_be_suppressed_as_marker_failure() {
    let clock = clock();
    let backend = seeded(clock.clone()).await;
    let store = Arc::new(CancelledRead(Store {
        inner: InMemoryInvalidationStore::default(),
        mode: Mutex::new((Selection::Tags, Mode::Pass)),
        reads: Mutex::new(Vec::new()),
        stopped: Arc::default(),
    }));
    let cache = reader(
        clock,
        backend,
        store,
        control_options()
            .with_rethrow_distributed_exceptions(false)
            .with_rethrow_serialization_exceptions(false),
    );
    let mut events = cache.events().subscribe();
    let error = cache
        .get_or_set("key", |_| async {
            panic!("cancellation must not run origin")
        })
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        Error::OperationCancelled {
            reason: FactoryCancellationReason::LeaseLost
        }
    ));
    assert!(marker_events(&mut events).is_empty());
    assert!(
        !cache
            .read(
                "key",
                Some(value_options().with_skip_distributed(true, false))
            )
            .await
            .unwrap()
            .has_value(),
        "cancelled read must not hydrate L1"
    );
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn explicit_marker_skip_is_independent_of_value_read_and_does_not_fabricate_absence() {
    let clock = clock();
    let backend = seeded(clock.clone()).await;
    let store = Store::new();
    let cache = reader(
        clock,
        backend,
        store.clone(),
        control_options().with_skip_distributed(true, false),
    );
    let mut events = cache.events().subscribe();
    assert_eq!(cache.read("key", None).await.unwrap().into_value(), Some(7));
    assert!(store.reads.lock().unwrap().is_empty());
    assert_eq!(marker_events(&mut events), vec![MarkerReadOutcome::Skipped]);
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn confirmed_negative_observations_are_reused_and_expire_while_the_value_stays_fresh() {
    let clock = clock();
    let backend = seeded(clock.clone()).await;
    let store = Store::new();
    let cache = reader(clock.clone(), backend, store.clone(), control_options());
    for _ in 0..2 {
        assert_eq!(cache.read("key", None).await.unwrap().into_value(), Some(7));
    }
    assert_eq!(store.tag_count(), 1);
    clock.advance(Duration::from_secs(2));
    assert_eq!(cache.read("key", None).await.unwrap().into_value(), Some(7));
    assert_eq!(store.tag_count(), 2);
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn marker_memory_read_and_write_skips_affect_existing_value_l1_hits() {
    for tags in [
        control_options().with_skip_memory(true, false),
        control_options().with_skip_memory(false, true),
    ] {
        let clock = clock();
        let backend = seeded(clock.clone()).await;
        let store = Store::new();
        let cache = reader(clock, backend, store.clone(), tags);
        for _ in 0..2 {
            assert_eq!(cache.read("key", None).await.unwrap().into_value(), Some(7));
        }
        assert_eq!(store.tag_count(), 2);
        assert_eq!(store.count(&MarkerKind::ClearRemove), 2);
        cache.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn stale_marker_remote_skip_does_not_depend_on_value_staleness() {
    let clock = clock();
    let backend = seeded(clock.clone()).await;
    let store = Store::new();
    let cache = reader(
        clock.clone(),
        backend,
        store.clone(),
        control_options().with_skip_distributed_read_when_stale(true),
    );
    cache.read("key", None).await.unwrap();
    clock.advance(Duration::from_secs(2));
    store.mode(Mode::Backend);
    let mut events = cache.events().subscribe();
    assert_eq!(cache.read("key", None).await.unwrap().into_value(), Some(7));
    assert_eq!(store.tag_count(), 1);
    assert_eq!(marker_events(&mut events), vec![MarkerReadOutcome::Skipped]);
    cache.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn cold_marker_ignores_soft_budget_and_signals_hard_deadline_before_drop() {
    let clock = clock();
    let backend = seeded(clock.clone()).await;
    let store = Store::new();
    let gate = Gate::new();
    store.mode(Mode::Park(gate.clone()));
    let options = control_options().with_distributed_timeouts(
        Timeout::After(Duration::from_millis(5)),
        Timeout::After(Duration::from_millis(20)),
    );
    let cache = reader(clock, backend, store.clone(), options);
    let mut events = cache.events().subscribe();
    let task_cache = cache.clone();
    let task = tokio::spawn(async move { task_cache.read("key", None).await });
    gate.entered.notified().await;
    tokio::time::advance(Duration::from_millis(6)).await;
    assert!(!task.is_finished());
    tokio::time::advance(Duration::from_millis(20)).await;
    assert_eq!(task.await.unwrap().unwrap().into_value(), Some(7));
    assert!(
        store
            .stopped
            .lock()
            .unwrap()
            .contains(&Some(FactoryCancellationReason::HardTimeout))
    );
    assert_eq!(
        marker_events(&mut events),
        vec![MarkerReadOutcome::Unavailable(
            MarkerReadFailure::HardTimeout
        )]
    );
    cache.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn only_retained_marker_enables_soft_deadline_and_throttle_preserves_retention() {
    let clock = clock();
    let backend = seeded(clock.clone()).await;
    let store = Store::new();
    let options = control_options().with_distributed_timeouts(
        Timeout::After(Duration::from_millis(5)),
        Timeout::After(Duration::from_millis(20)),
    );
    let cache = reader(clock.clone(), backend, store.clone(), options);
    cache.read("key", None).await.unwrap();
    clock.advance(Duration::from_secs(2));
    let gate = Gate::new();
    store.mode(Mode::Park(gate.clone()));
    let mut events = cache.events().subscribe();
    let task_cache = cache.clone();
    let task = tokio::spawn(async move { task_cache.read("key", None).await });
    gate.entered.notified().await;
    tokio::time::advance(Duration::from_millis(6)).await;
    assert_eq!(task.await.unwrap().unwrap().into_value(), Some(7));
    assert!(
        store
            .stopped
            .lock()
            .unwrap()
            .contains(&Some(FactoryCancellationReason::SoftTimeout))
    );
    assert_eq!(
        marker_events(&mut events),
        vec![MarkerReadOutcome::StaleFallback(
            MarkerReadFailure::SoftTimeout
        )]
    );
    assert_eq!(cache.read("key", None).await.unwrap().into_value(), Some(7));
    assert_eq!(
        store.tag_count(),
        2,
        "throttled observation avoids a new remote read"
    );
    cache.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn marker_budget_runs_after_and_independently_of_value_deadline() {
    let clock = clock();
    let backend = seeded(clock.clone()).await;
    let store = Store::new();
    store.mode(Mode::Delay(Duration::from_millis(50)));
    let cache =
        Cache::<u64>::builder()
            .clock(clock)
            .distributed(backend)
            .invalidation_store(store.clone())
            .serializer(Arc::new(JsonSerializer))
            .default_options(value_options().with_distributed_timeouts(
                Timeout::Infinite,
                Timeout::After(Duration::from_millis(5)),
            ))
            .tags_default_options(control_options().with_distributed_timeouts(
                Timeout::Infinite,
                Timeout::After(Duration::from_millis(100)),
            ))
            .marker_read_policy(MarkerReadPolicy::OptionsControlled)
            .try_build()
            .unwrap();
    let mut events = cache.events().subscribe();
    assert_eq!(cache.read("key", None).await.unwrap().into_value(), Some(7));
    assert_eq!(
        marker_events(&mut events),
        vec![MarkerReadOutcome::Observed]
    );
    assert!(
        !store
            .stopped
            .lock()
            .unwrap()
            .contains(&Some(FactoryCancellationReason::HardTimeout))
    );
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn marker_fault_flags_are_independent_and_rethrow_original_cause_even_in_get_or_set() {
    for (mode, options) in [
        (
            Mode::Backend,
            control_options().with_rethrow_distributed_exceptions(true),
        ),
        (
            Mode::Protocol,
            control_options().with_rethrow_serialization_exceptions(true),
        ),
    ] {
        let clock = clock();
        let backend = seeded(clock.clone()).await;
        let store = Store::new();
        store.mode(mode);
        let cache = reader(clock, backend, store, options);
        let error = cache
            .get_or_set("key", |_| async {
                panic!("strict marker fault must not run origin")
            })
            .await
            .unwrap_err();
        let source = match error {
            Error::Marker(
                MarkerError::Backend { source } | MarkerError::ProtocolWithSource { source },
            ) => source,
            other => panic!("unexpected {other:?}"),
        };
        assert!(source.downcast_ref::<Cause>().is_some());
        cache.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn suppressed_fault_is_explicit_and_is_never_negative_cached() {
    for mode in [Mode::Backend, Mode::Protocol] {
        let clock = clock();
        let backend = seeded(clock.clone()).await;
        let store = Store::new();
        store.mode(mode);
        let cache = reader(clock, backend, store.clone(), control_options());
        let mut events = cache.events().subscribe();
        for _ in 0..2 {
            assert_eq!(cache.read("key", None).await.unwrap().into_value(), Some(7));
        }
        assert_eq!(store.tag_count(), 2);
        assert!(
            marker_events(&mut events)
                .iter()
                .all(|outcome| matches!(outcome, MarkerReadOutcome::Unavailable(_)))
        );
        cache.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn lost_or_expired_observation_cannot_forget_a_known_invalidation() {
    let clock = clock();
    let backend = seeded(clock.clone()).await;
    let store = Store::new();
    let cache = Cache::<u64>::builder()
        .clock(clock.clone())
        .distributed(backend)
        .invalidation_store(store.clone())
        .serializer(Arc::new(JsonSerializer))
        .default_options(value_options())
        .tags_default_options(control_options())
        .remove_by_tag_behavior(RemoveByTagBehavior::Remove)
        .marker_read_policy(MarkerReadPolicy::OptionsControlled)
        .try_build()
        .unwrap();
    assert_eq!(cache.read("key", None).await.unwrap().into_value(), Some(7));
    clock.advance(Duration::from_secs(2));
    let scope = CacheScope::new("", "v2", KeyModifierMode::Prefix).unwrap();
    store
        .advance(
            &scope,
            MarkerKind::Tag(tag()),
            MarkerVersion::new(clock.now()),
        )
        .await
        .unwrap();
    assert!(!cache.read("key", None).await.unwrap().has_value());
    clock.advance(Duration::from_secs(2));
    store.mode(Mode::Absent);
    assert!(
        !cache.read("key", None).await.unwrap().has_value(),
        "confirmed remote absence cannot lower the known maximum"
    );
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancelled_marker_read_never_hydrates_or_reports_degraded_success() {
    let clock = clock();
    let backend = seeded(clock.clone()).await;
    let store = Store::new();
    let gate = Gate::new();
    store.mode(Mode::Park(gate.clone()));
    let cache = reader(clock, backend, store.clone(), control_options());
    let source = CancellationSource::new();
    let cancellation = source.token();
    let task_cache = cache.clone();
    let task =
        tokio::spawn(async move { task_cache.read_cancellable("key", None, cancellation).await });
    gate.entered.notified().await;
    source.cancel();
    assert!(matches!(
        task.await.unwrap(),
        Err(Error::OperationCancelled {
            reason: FactoryCancellationReason::CallerCancelled
        })
    ));
    assert!(
        store
            .stopped
            .lock()
            .unwrap()
            .contains(&Some(FactoryCancellationReason::CallerCancelled))
    );
    store.mode(Mode::Pass);
    assert_eq!(cache.read("key", None).await.unwrap().into_value(), Some(7));
    assert_eq!(store.tag_count(), 2);
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn rejecting_observation_admission_cannot_change_value_admission_or_ledger() {
    let clock = clock();
    let backend = seeded(clock.clone()).await;
    let store = Store::new();
    let cache = Cache::<u64>::builder()
        .clock(clock)
        .distributed(backend)
        .invalidation_store(store.clone())
        .serializer(Arc::new(JsonSerializer))
        .default_options(value_options())
        .tags_default_options(control_options())
        .marker_read_policy(MarkerReadPolicy::OptionsControlled)
        .marker_read_limits(MemoryLimits::new(Some(0), None))
        .try_build()
        .unwrap();
    for _ in 0..2 {
        assert_eq!(cache.read("key", None).await.unwrap().into_value(), Some(7));
    }
    assert_eq!(store.tag_count(), 2);
    cache.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn default_durable_policy_preserves_its_existing_combined_budget() {
    let clock = clock();
    let backend = seeded(clock.clone()).await;
    let store = Store::new();
    let gate = Gate::new();
    store.mode(Mode::Park(gate.clone()));
    let cache =
        Cache::<u64>::builder()
            .clock(clock)
            .distributed(backend)
            .invalidation_store(store)
            .serializer(Arc::new(JsonSerializer))
            .default_options(value_options())
            .tags_default_options(control_options().with_distributed_timeouts(
                Timeout::Infinite,
                Timeout::After(Duration::from_millis(1)),
            ))
            .try_build()
            .unwrap();
    assert_eq!(
        cache.marker_read_policy(),
        MarkerReadPolicy::DurableRequired
    );
    let task_cache = cache.clone();
    let task = tokio::spawn(async move { task_cache.read("key", None).await });
    gate.entered.notified().await;
    tokio::time::advance(Duration::from_millis(2)).await;
    assert!(
        !task.is_finished(),
        "default durable reconciliation keeps the value budget"
    );
    gate.release.add_permits(1);
    assert_eq!(task.await.unwrap().unwrap().into_value(), Some(7));
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn initialized_clear_scalars_with_backplane_are_independent_of_marker_memory_skip() {
    let clock = clock();
    let backend = seeded(clock.clone()).await;
    let store = Store::new();
    let cache = Cache::<u64>::builder()
        .clock(clock)
        .distributed(backend)
        .invalidation_store(store.clone())
        .backplane(Arc::new(InProcessBackplane::default()))
        .serializer(Arc::new(JsonSerializer))
        .default_options(value_options())
        .tags_default_options(control_options().with_skip_memory(true, true))
        .marker_read_policy(MarkerReadPolicy::OptionsControlled)
        .try_build()
        .unwrap();
    for _ in 0..2 {
        assert_eq!(cache.read("key", None).await.unwrap().into_value(), Some(7));
    }
    assert_eq!(store.tag_count(), 2);
    assert_eq!(store.count(&MarkerKind::ClearRemove), 1);
    assert_eq!(store.count(&MarkerKind::ClearExpire), 1);
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn received_marker_seeding_respects_memory_write_without_losing_the_fact() {
    for skip_write in [false, true] {
        let clock = clock();
        let backend = seeded(clock.clone()).await;
        let store = Store::new();
        let backplane = Arc::new(InProcessBackplane::default());
        let cache = Cache::<u64>::builder()
            .clock(clock.clone())
            .distributed(backend)
            .invalidation_store(store.clone())
            .backplane(backplane.clone())
            .serializer(Arc::new(JsonSerializer))
            .default_options(value_options())
            .tags_default_options(control_options().with_skip_memory(false, skip_write))
            .marker_read_policy(MarkerReadPolicy::OptionsControlled)
            .try_build()
            .unwrap();
        cache.read("key", None).await.unwrap();
        clock.advance(Duration::from_secs(2));
        let mut events = cache.events().subscribe();
        let scope = CacheScope::new("", "v2", KeyModifierMode::Prefix).unwrap();
        let command = MarkerCommand::new(
            "remote",
            scope,
            StoredMarker::new(MarkerKind::Tag(tag()), MarkerVersion::new(Timestamp::MIN)),
        )
        .unwrap();
        backplane
            .publish_command(BackplaneCommand::Marker(command.clone()))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let CacheEvent::MarkerReceived { command: received } =
                    events.recv().await.unwrap()
                {
                    assert_eq!(received, command);
                    break;
                }
            }
        })
        .await
        .unwrap();
        store.mode(Mode::Backend);
        assert_eq!(cache.read("key", None).await.unwrap().into_value(), Some(7));
        assert_eq!(store.tag_count(), if skip_write { 2 } else { 1 });
        cache.shutdown().await.unwrap();
    }
}

struct Provider(Arc<Mutex<Vec<String>>>);
impl DefaultEntryOptionsProvider for Provider {
    fn options_for(&self, key: &str) -> Option<EntryOptions> {
        self.0.lock().unwrap().push(key.to_owned());
        Some(value_options())
    }
}

#[tokio::test]
async fn marker_reads_bypass_value_provider_and_value_operation_override() {
    let clock = clock();
    let backend = seeded(clock.clone()).await;
    let store = Store::new();
    let keys = Arc::new(Mutex::new(Vec::new()));
    let cache = Cache::<u64>::builder()
        .clock(clock)
        .distributed(backend)
        .invalidation_store(store.clone())
        .serializer(Arc::new(JsonSerializer))
        .default_options(value_options())
        .default_options_provider(Arc::new(Provider(keys.clone())))
        .tags_default_options(control_options().with_skip_distributed(true, false))
        .marker_read_policy(MarkerReadPolicy::OptionsControlled)
        .try_build()
        .unwrap();
    assert_eq!(cache.read("key", None).await.unwrap().into_value(), Some(7));
    assert_eq!(
        cache
            .read("key", Some(value_options().with_skip_memory(true, false)))
            .await
            .unwrap()
            .into_value(),
        Some(7)
    );
    assert_eq!(
        cache
            .get_or_set("key", |_| async { panic!("L2 hit") })
            .await
            .unwrap(),
        7
    );
    assert_eq!(*keys.lock().unwrap(), vec!["key", "key"]);
    assert!(store.reads.lock().unwrap().is_empty());
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn strict_capability_errors_cannot_be_suppressed_as_marker_absence() {
    let clock = clock();
    let backend = seeded(clock.clone()).await;
    let store = Store::new();
    store.mode(Mode::Unsupported);
    let cache = reader(clock, backend, store, control_options());
    assert!(matches!(
        cache.read("key", None).await,
        Err(Error::Marker(MarkerError::Unsupported))
    ));
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn dropping_or_shutting_down_a_marker_owner_retains_its_exact_reason() {
    for reason in [
        FactoryCancellationReason::CallerDropped,
        FactoryCancellationReason::CacheShutdown,
    ] {
        let clock = clock();
        let backend = seeded(clock.clone()).await;
        let store = Store::new();
        let gate = Gate::new();
        store.mode(Mode::Park(gate.clone()));
        let cache = reader(clock, backend, store.clone(), control_options());
        let task_cache = cache.clone();
        let task = tokio::spawn(async move { task_cache.read("key", None).await });
        gate.entered.notified().await;
        match reason {
            FactoryCancellationReason::CallerDropped => {
                task.abort();
                assert!(task.await.unwrap_err().is_cancelled());
            }
            FactoryCancellationReason::CacheShutdown => {
                cache.shutdown().await.unwrap();
                assert!(matches!(
                    task.await.unwrap(),
                    Err(Error::OperationCancelled {
                        reason: FactoryCancellationReason::CacheShutdown
                    })
                ));
            }
            _ => unreachable!(),
        }
        cache.shutdown().await.unwrap();
        assert!(store.stopped.lock().unwrap().contains(&Some(reason)));
    }
}

struct MaximumJitter;
impl JitterSource for MaximumJitter {
    fn sample(&self, maximum: Duration) -> Duration {
        maximum
    }
}

#[tokio::test]
async fn marker_memory_duration_and_injected_jitter_are_independent_of_value_lifetime() {
    let clock = clock();
    let backend = seeded(clock.clone()).await;
    let store = Store::new();
    let cache = Cache::<u64>::builder()
        .clock(clock.clone())
        .distributed(backend)
        .invalidation_store(store.clone())
        .serializer(Arc::new(JsonSerializer))
        .jitter_source(Arc::new(MaximumJitter))
        .default_options(value_options())
        .tags_default_options(
            control_options()
                .with_duration(Duration::from_secs(10))
                .with_memory_duration(Duration::from_secs(1))
                .with_jitter_max(Duration::from_secs(2)),
        )
        .marker_read_policy(MarkerReadPolicy::OptionsControlled)
        .try_build()
        .unwrap();
    cache.read("key", None).await.unwrap();
    clock.advance(Duration::from_secs(2));
    assert_eq!(cache.read("key", None).await.unwrap().into_value(), Some(7));
    assert_eq!(
        store.tag_count(),
        1,
        "injected jitter adds two seconds to the marker's L1 window"
    );
    clock.advance(Duration::from_secs(2));
    assert_eq!(cache.read("key", None).await.unwrap().into_value(), Some(7));
    assert_eq!(store.tag_count(), 2);
    cache.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn two_tags_and_two_clear_markers_each_have_their_own_complete_budget() {
    let clock = clock();
    let second = Tag::new("second").unwrap();
    let backend = seeded_with_tags(
        clock.clone(),
        vec![tag(), second.clone()].into_boxed_slice(),
    )
    .await;
    let store = Store::new();
    store.mode_all(Mode::Delay(Duration::from_millis(25)));
    let cache = reader(
        clock,
        backend,
        store.clone(),
        control_options().with_distributed_timeouts(
            Timeout::Infinite,
            Timeout::After(Duration::from_millis(30)),
        ),
    );
    let start = tokio::time::Instant::now();
    assert_eq!(cache.read("key", None).await.unwrap().into_value(), Some(7));
    assert!(start.elapsed() >= Duration::from_millis(100));
    assert_eq!(store.count(&MarkerKind::Tag(second)), 1);
    assert_eq!(store.tag_count(), 1);
    assert_eq!(store.count(&MarkerKind::ClearRemove), 1);
    assert_eq!(store.count(&MarkerKind::ClearExpire), 1);
    assert!(
        !store
            .stopped
            .lock()
            .unwrap()
            .contains(&Some(FactoryCancellationReason::HardTimeout))
    );
    cache.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn marker_hard_budget_wins_and_soft_budget_requires_retained_fail_safe() {
    for (enabled, soft, hard, expected) in [
        (
            false,
            2,
            8,
            MarkerReadOutcome::Unavailable(MarkerReadFailure::HardTimeout),
        ),
        (
            true,
            8,
            2,
            MarkerReadOutcome::StaleFallback(MarkerReadFailure::HardTimeout),
        ),
    ] {
        let clock = clock();
        let backend = seeded(clock.clone()).await;
        let store = Store::new();
        let cache = reader(
            clock.clone(),
            backend,
            store.clone(),
            control_options()
                .with_fail_safe(enabled, None, None)
                .with_distributed_timeouts(
                    Timeout::After(Duration::from_millis(soft)),
                    Timeout::After(Duration::from_millis(hard)),
                ),
        );
        cache.read("key", None).await.unwrap();
        clock.advance(Duration::from_secs(2));
        let gate = Gate::new();
        store.mode(Mode::Park(gate.clone()));
        let mut events = cache.events().subscribe();
        let task_cache = cache.clone();
        let task = tokio::spawn(async move { task_cache.read("key", None).await });
        gate.entered.notified().await;
        tokio::time::advance(Duration::from_millis(hard + 1)).await;
        assert_eq!(task.await.unwrap().unwrap().into_value(), Some(7));
        assert!(
            store
                .stopped
                .lock()
                .unwrap()
                .contains(&Some(FactoryCancellationReason::HardTimeout))
        );
        assert_eq!(marker_events(&mut events), vec![expected]);
        cache.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn physically_expired_marker_is_unavailable_and_failed_reads_do_not_renew_it() {
    let clock = clock();
    let backend = seeded(clock.clone()).await;
    let store = Store::new();
    let cache = reader(
        clock.clone(),
        backend,
        store.clone(),
        control_options().with_fail_safe(
            true,
            Some(Duration::from_secs(3)),
            Some(Duration::from_secs(1)),
        ),
    );
    cache.read("key", None).await.unwrap();
    clock.advance(Duration::from_secs(4));
    store.mode(Mode::Backend);
    let mut events = cache.events().subscribe();
    for _ in 0..2 {
        assert_eq!(cache.read("key", None).await.unwrap().into_value(), Some(7));
    }
    assert_eq!(store.tag_count(), 3);
    assert_eq!(
        marker_events(&mut events),
        vec![MarkerReadOutcome::Unavailable(MarkerReadFailure::Backend); 2]
    );
    cache.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn contended_marker_lock_uses_stale_fact_without_saving_and_honors_independent_timeout() {
    for options in [
        control_options()
            .with_lock_timeout(Timeout::Infinite)
            .with_memory_lock_timeout(Timeout::After(Duration::from_millis(5))),
        control_options()
            .with_lock_timeout(Timeout::Infinite)
            .with_factory_timeouts(
                Timeout::After(Duration::from_millis(5)),
                Timeout::Infinite,
                false,
            ),
    ] {
        let clock = clock();
        let backend = seeded(clock.clone()).await;
        let store = Store::new();
        let cache = reader(clock.clone(), backend, store.clone(), options);
        cache.read("key", None).await.unwrap();
        clock.advance(Duration::from_secs(2));
        let gate = Gate::new();
        store.mode(Mode::Park(gate.clone()));
        let task_cache = cache.clone();
        let owner = tokio::spawn(async move { task_cache.read("key", None).await });
        gate.entered.notified().await;
        let mut events = cache.events().subscribe();
        let task_cache = cache.clone();
        let contender = tokio::spawn(async move { task_cache.read("key", None).await });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(6)).await;
        assert_eq!(contender.await.unwrap().unwrap().into_value(), Some(7));
        assert_eq!(
            store.tag_count(),
            2,
            "contended fallback must not duplicate the remote read"
        );
        assert_eq!(
            marker_events(&mut events),
            vec![MarkerReadOutcome::StaleFallback(
                MarkerReadFailure::LockTimeout
            )]
        );
        owner.abort();
        assert!(owner.await.unwrap_err().is_cancelled());
        store.mode(Mode::Pass);
        assert_eq!(cache.read("key", None).await.unwrap().into_value(), Some(7));
        assert_eq!(
            store.tag_count(),
            3,
            "the contender did not save/renew the stale marker"
        );
        cache.shutdown().await.unwrap();
    }
}

async fn wait_for_marker(
    events: &mut tokio::sync::broadcast::Receiver<CacheEvent>,
    expected: &MarkerCommand,
) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let CacheEvent::MarkerReceived { command } = events.recv().await.unwrap() {
                assert_eq!(&command, expected);
                return;
            }
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn parked_reply_cannot_replace_a_newer_peer_observation_or_falsify_authority() {
    for reply in [
        Reply::Present(MarkerVersion::new(Timestamp::MIN)),
        Reply::Absent,
        Reply::Backend,
    ] {
        let clock = clock();
        let backend = seeded(clock.clone()).await;
        let store = Store::new();
        let backplane = Arc::new(InProcessBackplane::default());
        let scope = CacheScope::new("", "v2", KeyModifierMode::Prefix).unwrap();
        store
            .advance(
                &scope,
                MarkerKind::Tag(tag()),
                MarkerVersion::new(Timestamp::from_ticks(1)),
            )
            .await
            .unwrap();
        let cache = Cache::<u64>::builder()
            .clock(clock.clone())
            .distributed(backend)
            .invalidation_store(store.clone())
            .backplane(backplane.clone())
            .serializer(Arc::new(JsonSerializer))
            .default_options(value_options())
            .tags_default_options(control_options())
            .marker_read_policy(MarkerReadPolicy::OptionsControlled)
            .try_build()
            .unwrap();
        cache.read("key", None).await.unwrap();
        clock.advance(Duration::from_secs(2));
        let gate = Gate::new();
        let fault = matches!(reply, Reply::Backend);
        store.mode(Mode::ReplyAfter {
            gate: gate.clone(),
            reply,
        });
        let task_cache = cache.clone();
        let task = tokio::spawn(async move { task_cache.read("key", None).await });
        gate.entered.notified().await;
        let mut events = cache.events().subscribe();
        let command = MarkerCommand::new(
            "peer",
            scope,
            StoredMarker::new(
                MarkerKind::Tag(tag()),
                MarkerVersion::new(Timestamp::from_ticks(1000)),
            ),
        )
        .unwrap();
        backplane
            .publish_command(BackplaneCommand::Marker(command.clone()))
            .await
            .unwrap();
        wait_for_marker(&mut events, &command).await;
        gate.release.add_permits(1);
        assert_eq!(task.await.unwrap().unwrap().into_value(), Some(7));
        let expected = if fault {
            MarkerReadOutcome::StaleFallback(MarkerReadFailure::Backend)
        } else {
            MarkerReadOutcome::KnownMaximum
        };
        assert_eq!(marker_events(&mut events), vec![expected]);
        store.mode(Mode::Backend);
        assert_eq!(cache.read("key", None).await.unwrap().into_value(), Some(7));
        assert_eq!(store.tag_count(), 2);
        assert_eq!(
            marker_events(&mut events),
            vec![if fault {
                MarkerReadOutcome::Cached
            } else {
                MarkerReadOutcome::KnownMaximum
            }]
        );
        cache.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn gap_during_marker_read_revokes_old_observations_before_value_hydration() {
    let clock = clock();
    let backend = seeded(clock.clone()).await;
    let store = Store::new();
    let backplane = Arc::new(InProcessBackplane::default());
    let cache = Cache::<u64>::builder()
        .clock(clock.clone())
        .distributed(backend)
        .invalidation_store(store.clone())
        .backplane(backplane.clone())
        .serializer(Arc::new(JsonSerializer))
        .default_options(value_options())
        .tags_default_options(control_options())
        .marker_read_policy(MarkerReadPolicy::OptionsControlled)
        .try_build()
        .unwrap();
    cache
        .try_set_full(
            "proof",
            1,
            Some(
                value_options()
                    .with_skip_distributed(false, true)
                    .with_skip_backplane_notifications(true),
            ),
            Box::from([]),
        )
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    cache.read("key", None).await.unwrap();
    clock.advance(Duration::from_secs(2));
    let gate = Gate::new();
    store.mode(Mode::Park(gate.clone()));
    let task_cache = cache.clone();
    let task = tokio::spawn(async move { task_cache.read("key", None).await });
    gate.entered.notified().await;
    backplane
        .publish(BackplaneMessage {
            source_id: "\u{1f}amalgam-control-v2:zz".into(),
            timestamp: clock.now(),
            action: BackplaneAction::Set,
            key: "v2:key".into(),
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while cache
            .read(
                "proof",
                Some(value_options().with_skip_distributed(true, false)),
            )
            .await
            .unwrap()
            .has_value()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    store.mode(Mode::Pass);
    gate.release.add_permits(1);
    assert_eq!(task.await.unwrap().unwrap().into_value(), Some(7));
    assert_eq!(
        store.tag_count(),
        3,
        "post-gap value hydration must re-observe the marker"
    );
    assert_eq!(cache.read("key", None).await.unwrap().into_value(), Some(7));
    assert_eq!(store.tag_count(), 3);
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn value_freshness_is_rechecked_after_a_parked_marker_validation() {
    let clock = clock();
    let backend = seeded(clock.clone()).await;
    let store = Store::new();
    let cache = Cache::<u64>::builder()
        .clock(clock.clone())
        .distributed(backend)
        .invalidation_store(store.clone())
        .serializer(Arc::new(JsonSerializer))
        .default_options(value_options().with_memory_duration(Duration::from_secs(3)))
        .tags_default_options(control_options())
        .marker_read_policy(MarkerReadPolicy::OptionsControlled)
        .try_build()
        .unwrap();
    cache.read("key", None).await.unwrap();
    clock.advance(Duration::from_secs(2));
    let gate = Gate::new();
    store.mode(Mode::Park(gate.clone()));
    let task_cache = cache.clone();
    let task = tokio::spawn(async move {
        task_cache
            .read(
                "key",
                Some(value_options().with_skip_distributed(true, false)),
            )
            .await
    });
    gate.entered.notified().await;
    clock.advance(Duration::from_secs(2));
    gate.release.add_permits(1);
    assert!(
        !task.await.unwrap().unwrap().has_value(),
        "a value which expired during control validation is not a fresh L1 hit"
    );
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn old_marker_provider_adapter_and_marker_weight_admission_preserve_value_hits() {
    let clock = clock();
    let backend = seeded(clock.clone()).await;
    let cache = Cache::<u64>::builder()
        .clock(clock)
        .distributed(backend)
        .invalidation_store(Arc::new(InMemoryInvalidationStore::default()))
        .serializer(Arc::new(JsonSerializer))
        .default_options(value_options())
        .tags_default_options(control_options().with_size(2).with_priority(Priority::Low))
        .marker_read_limits(MemoryLimits::new(None, Some(1)))
        .marker_read_policy(MarkerReadPolicy::OptionsControlled)
        .try_build()
        .unwrap();
    let mut events = cache.events().subscribe();
    for _ in 0..2 {
        assert_eq!(cache.read("key", None).await.unwrap().into_value(), Some(7));
    }
    assert_eq!(
        marker_events(&mut events),
        vec![MarkerReadOutcome::Observed; 2]
    );
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn first_invalidating_control_stops_before_following_storage_faults() {
    let second = Tag::new("second").unwrap();
    for (invalidating, following) in [
        (MarkerKind::ClearRemove, MarkerKind::Tag(tag())),
        (MarkerKind::Tag(tag()), MarkerKind::Tag(second.clone())),
        (MarkerKind::Tag(second.clone()), MarkerKind::ClearExpire),
    ] {
        let clock = clock();
        let backend = seeded_with_tags(
            clock.clone(),
            vec![tag(), second.clone()].into_boxed_slice(),
        )
        .await;
        let store = Store::new();
        let scope = CacheScope::new("", "v2", KeyModifierMode::Prefix).unwrap();
        store
            .advance(&scope, invalidating, MarkerVersion::new(clock.now()))
            .await
            .unwrap();
        store.mode_for(following.clone(), Mode::Backend);
        let cache = Cache::<u64>::builder()
            .clock(clock)
            .distributed(backend)
            .invalidation_store(store.clone())
            .serializer(Arc::new(JsonSerializer))
            .default_options(value_options())
            .tags_default_options(control_options().with_rethrow_distributed_exceptions(true))
            .remove_by_tag_behavior(RemoveByTagBehavior::Remove)
            .marker_read_policy(MarkerReadPolicy::OptionsControlled)
            .try_build()
            .unwrap();
        assert!(!cache.read("key", None).await.unwrap().has_value());
        assert_eq!(
            store.count(&following),
            0,
            "an invalidated snapshot needs no later control lookup"
        );
        cache.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn retained_tag_expiry_never_bypasses_a_later_hard_clear_remove() {
    let clock = clock();
    let backend = seeded(clock.clone()).await;
    let store = Store::new();
    let cache = reader(clock.clone(), backend, store.clone(), control_options());
    cache.read("key", None).await.unwrap();
    cache
        .try_remove_by_tag(tag())
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    clock.advance(Duration::from_secs(2));
    let scope = CacheScope::new("", "v2", KeyModifierMode::Prefix).unwrap();
    store
        .advance(
            &scope,
            MarkerKind::ClearRemove,
            MarkerVersion::new(clock.now()),
        )
        .await
        .unwrap();
    assert!(
        !cache
            .read(
                "key",
                Some(value_options().with_allow_stale_on_read_only(true))
            )
            .await
            .unwrap()
            .has_value()
    );
    assert_eq!(
        store.count(&MarkerKind::ClearRemove),
        2,
        "hard clear is re-observed before the known stale tag, then reused by L2"
    );
    cache.shutdown().await.unwrap();
}

struct CodecWithoutCloner;
impl DistributedSerializer<u64> for CodecWithoutCloner {
    fn serialize(&self, entry: &DistributedEntry<u64>) -> Result<Vec<u8>> {
        JsonSerializer.serialize(entry)
    }
    fn deserialize(&self, bytes: &[u8]) -> Result<DistributedEntry<u64>> {
        JsonSerializer.deserialize(bytes)
    }
    fn serialize_snapshot(&self, snapshot: &DistributedSnapshot<u64>) -> Result<Vec<u8>> {
        JsonSerializer.serialize_snapshot(snapshot)
    }
    fn deserialize_snapshot(&self, bytes: &[u8]) -> Result<DistributedSnapshot<u64>> {
        JsonSerializer.deserialize_snapshot(bytes)
    }
}

#[tokio::test]
async fn marker_copy_does_not_require_the_ordinary_value_cloner() {
    let clock = clock();
    let backend = seeded(clock.clone()).await;
    let cache = Cache::<u64>::builder()
        .clock(clock)
        .distributed(backend)
        .serializer(Arc::new(CodecWithoutCloner))
        .default_options(value_options())
        .tags_default_options(control_options().with_enable_auto_clone(true))
        .marker_read_policy(MarkerReadPolicy::OptionsControlled)
        .try_build()
        .unwrap();
    assert_eq!(cache.read("key", None).await.unwrap().into_value(), Some(7));
    cache
        .try_remove_by_tag_with(tag(), Some(control_options().with_enable_auto_clone(true)))
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(matches!(
        cache
            .read("key", Some(value_options().with_enable_auto_clone(true)))
            .await,
        Err(Error::Config(ConfigError::AutoCloneWithoutCloner))
    ));
    cache.shutdown().await.unwrap();
}

struct ParkOnEager {
    store: Arc<Store>,
    gate: Arc<Gate>,
}
impl Plugin for ParkOnEager {
    fn name(&self) -> &str {
        "park-eager-marker"
    }
    fn on_event(&self, event: &CacheEvent) {
        if matches!(event, CacheEvent::EagerRefresh { .. }) {
            self.store.mode(Mode::Park(self.gate.clone()));
        }
    }
}

#[tokio::test]
async fn eager_marker_read_is_owned_by_refresh_and_shutdown() {
    let clock = clock();
    let backend = seeded(clock.clone()).await;
    let store = Store::new();
    let gate = Gate::new();
    let cache = Cache::<u64>::builder()
        .clock(clock.clone())
        .distributed(backend)
        .invalidation_store(store.clone())
        .serializer(Arc::new(JsonSerializer))
        .default_options(
            value_options()
                .with_memory_duration(Duration::from_secs(10))
                .with_eager_refresh(Some(EagerThreshold::new(0.5).unwrap())),
        )
        .tags_default_options(control_options().with_skip_memory(true, false))
        .marker_read_policy(MarkerReadPolicy::OptionsControlled)
        .plugin(Arc::new(ParkOnEager {
            store: store.clone(),
            gate: gate.clone(),
        }))
        .try_build()
        .unwrap();
    cache.read("key", None).await.unwrap();
    clock.advance(Duration::from_secs(6));
    assert_eq!(
        cache
            .get_or_set("key", |_| async {
                panic!("parked L2 preflight must not run origin")
            })
            .await
            .unwrap(),
        7
    );
    tokio::time::timeout(Duration::from_secs(2), gate.entered.notified())
        .await
        .unwrap();
    cache.shutdown().await.unwrap();
    assert!(
        store
            .stopped
            .lock()
            .unwrap()
            .contains(&Some(FactoryCancellationReason::CacheShutdown))
    );
}

#[tokio::test]
async fn passive_marker_read_is_owned_by_refresh_and_shutdown() {
    let clock = clock();
    let backend = seeded(clock.clone()).await;
    let store = Store::new();
    let backplane = Arc::new(InProcessBackplane::default());
    let cache = Cache::<u64>::builder()
        .clock(clock.clone())
        .distributed(backend.clone())
        .invalidation_store(store.clone())
        .backplane(backplane.clone())
        .serializer(Arc::new(JsonSerializer))
        .default_options(value_options())
        .tags_default_options(control_options())
        .marker_read_policy(MarkerReadPolicy::OptionsControlled)
        .try_build()
        .unwrap();
    cache.read("key", None).await.unwrap();
    clock.advance(Duration::from_secs(2));
    let writer = Cache::<u64>::builder()
        .clock(clock.clone())
        .distributed(backend)
        .serializer(Arc::new(JsonSerializer))
        .default_options(value_options())
        .try_build()
        .unwrap();
    writer
        .try_set_full("key", 9, None, vec![tag()].into_boxed_slice())
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    let gate = Gate::new();
    store.mode(Mode::Park(gate.clone()));
    backplane
        .publish(BackplaneMessage {
            source_id: "peer".into(),
            timestamp: clock.now(),
            action: BackplaneAction::Set,
            key: "v2:key".into(),
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), gate.entered.notified())
        .await
        .unwrap();
    cache.shutdown().await.unwrap();
    writer.shutdown().await.unwrap();
    assert!(
        store
            .stopped
            .lock()
            .unwrap()
            .contains(&Some(FactoryCancellationReason::CacheShutdown))
    );
}
