//! Original stored values, bounded cursors and producer reentrancy boundaries.
use amalgam::entry::Entry;
use amalgam::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::time::Duration;

fn clock() -> Arc<ManualClock> {
    Arc::new(ManualClock::new(Timestamp::from_ticks(10_000_000)))
}
fn builder<V: Clone + Send + Sync + 'static>(
    limit: Option<u64>,
    clock: Arc<ManualClock>,
) -> CacheBuilder<V> {
    let builder = Cache::builder()
        .clock(clock)
        .default_options(EntryOptions::new(Duration::from_secs(60)));
    match limit {
        Some(limit) => builder.max_capacity(limit),
        None => builder,
    }
}

#[tokio::test]
async fn replacement_and_removal_carry_exact_stored_payload_in_both_backends() {
    for limit in [None, Some(2)] {
        let cache = builder::<Arc<String>>(limit, clock()).try_build().unwrap();
        let mut events = cache.memory_evictions().subscribe();
        let first = Arc::new("first".to_owned());
        let second = Arc::new("second".to_owned());
        cache
            .try_set("key", first.clone())
            .await
            .unwrap()
            .wait()
            .await
            .unwrap();
        cache
            .try_set("key", second.clone())
            .await
            .unwrap()
            .wait()
            .await
            .unwrap();
        let event = events.try_recv().unwrap();
        assert_eq!(event.key(), "key");
        assert_eq!(event.reason(), MemoryEvictionReason::Replaced);
        assert!(Arc::ptr_eq(event.value(), &first));
        cache.try_remove("key").await.unwrap().wait().await.unwrap();
        let event = events.try_recv().unwrap();
        assert_eq!(event.reason(), MemoryEvictionReason::Removed);
        assert!(Arc::ptr_eq(event.value(), &second));
        cache.try_remove("key").await.unwrap().wait().await.unwrap();
        assert!(matches!(
            events.try_recv(),
            Err(EvictionReceiveError::Empty)
        ));
        cache.shutdown().await.unwrap();
    }
}

#[derive(Debug)]
struct Counted {
    value: u64,
    clones: Arc<AtomicUsize>,
}
impl Clone for Counted {
    fn clone(&self) -> Self {
        self.clones.fetch_add(1, Ordering::SeqCst);
        Self {
            value: self.value,
            clones: self.clones.clone(),
        }
    }
}
#[tokio::test]
async fn observation_and_multiple_receivers_never_clone_the_user_value() {
    for limit in [None, Some(2)] {
        let cache = builder::<Counted>(limit, clock()).try_build().unwrap();
        let mut a = cache.memory_evictions().subscribe();
        let mut b = cache.memory_evictions().subscribe();
        let clones = Arc::new(AtomicUsize::new(0));
        cache
            .try_set(
                "k",
                Counted {
                    value: 7,
                    clones: clones.clone(),
                },
            )
            .await
            .unwrap()
            .wait()
            .await
            .unwrap();
        let before = clones.load(Ordering::SeqCst);
        cache.try_remove("k").await.unwrap().wait().await.unwrap();
        let original = a.try_recv().unwrap();
        let peer = b.try_recv().unwrap();
        let copy = original.clone();
        assert!(std::ptr::eq(original.value(), peer.value()));
        assert!(std::ptr::eq(original.value(), copy.value()));
        assert_eq!(original.value().value, 7);
        assert_eq!(clones.load(Ordering::SeqCst), before);
        cache.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn insertion_capture_and_retirement_capture_have_explicit_late_subscription_behavior() {
    for limit in [None, Some(2)] {
        for capture in [EvictionCapture::AtInsertion, EvictionCapture::AtRetirement] {
            let cache = builder::<u64>(limit, clock())
                .memory_eviction_capture(capture)
                .try_build()
                .unwrap();
            cache.try_set("old", 9).await.unwrap().wait().await.unwrap();
            let mut events = cache.memory_evictions().subscribe();
            cache.try_remove("old").await.unwrap().wait().await.unwrap();
            match capture {
                EvictionCapture::AtInsertion => assert!(matches!(
                    events.try_recv(),
                    Err(EvictionReceiveError::Empty)
                )),
                EvictionCapture::AtRetirement => assert_eq!(*events.try_recv().unwrap().value(), 9),
            }
            cache.shutdown().await.unwrap();
        }
    }
}

#[tokio::test]
async fn logical_expire_is_metadata_only_and_preserves_original_capture_admission() {
    for limit in [None, Some(2)] {
        for subscribed_before in [false, true] {
            let cache = builder::<Arc<String>>(limit, clock())
                .default_options(EntryOptions::new(Duration::from_secs(60)).with_fail_safe(
                    true,
                    Some(Duration::from_secs(600)),
                    None,
                ))
                .try_build()
                .unwrap();
            let original = Arc::new("original".to_owned());
            let mut early = subscribed_before.then(|| cache.memory_evictions().subscribe());
            cache
                .try_set("k", original.clone())
                .await
                .unwrap()
                .wait()
                .await
                .unwrap();
            let mut late = (!subscribed_before).then(|| cache.memory_evictions().subscribe());
            let events = early.as_mut().or(late.as_mut()).unwrap();
            cache.try_expire("k").await.unwrap().wait().await.unwrap();
            assert!(matches!(
                events.try_recv(),
                Err(EvictionReceiveError::Empty)
            ));
            cache.try_remove("k").await.unwrap().wait().await.unwrap();
            if subscribed_before {
                assert!(Arc::ptr_eq(events.try_recv().unwrap().value(), &original));
            } else {
                assert!(matches!(
                    events.try_recv(),
                    Err(EvictionReceiveError::Empty)
                ));
            }
            cache.shutdown().await.unwrap();
        }
    }
}

#[tokio::test]
async fn physical_expiry_and_capacity_report_the_actual_cause_and_value() {
    for limit in [None, Some(2)] {
        let clock = clock();
        let cache = builder::<u64>(limit, clock.clone()).try_build().unwrap();
        let mut events = cache.memory_evictions().subscribe();
        cache
            .try_set("expiry", 8)
            .await
            .unwrap()
            .wait()
            .await
            .unwrap();
        clock.advance(Duration::from_secs(61));
        assert!(!cache.read("expiry", None).await.unwrap().has_value());
        let event = events.try_recv().unwrap();
        assert_eq!(event.reason(), MemoryEvictionReason::Expired);
        assert_eq!(*event.value(), 8);
        cache.shutdown().await.unwrap();
    }
    let cache = builder::<u64>(Some(1), clock()).try_build().unwrap();
    let mut events = cache.memory_evictions().subscribe();
    cache
        .try_set("victim", 11)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    cache
        .try_set("next", 22)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    let event = events.try_recv().unwrap();
    assert_eq!(event.key(), "victim");
    assert_eq!(event.reason(), MemoryEvictionReason::Capacity);
    assert_eq!(*event.value(), 11);
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn clear_reports_original_entries_and_rejected_candidates_do_not_invent_evictions() {
    for limit in [None, Some(2)] {
        let cache = builder::<u64>(limit, clock()).try_build().unwrap();
        let mut events = cache.memory_evictions().subscribe();
        for (k, v) in [("a", 1), ("b", 2)] {
            cache.try_set(k, v).await.unwrap().wait().await.unwrap();
        }
        cache
            .try_clear(ClearMode::Remove)
            .await
            .unwrap()
            .wait()
            .await
            .unwrap();
        // Clear visibility is immediate; unbounded physical extraction is lazy.
        assert!(!cache.read("a", None).await.unwrap().has_value());
        cache.run_pending_tasks().await;
        let mut values = Vec::new();
        for _ in 0..2 {
            let event = events.try_recv().unwrap();
            assert_eq!(event.reason(), MemoryEvictionReason::Removed);
            values.push(*event.value());
        }
        values.sort_unstable();
        assert_eq!(values, vec![1, 2]);
        cache.shutdown().await.unwrap();
    }
    let cache = builder::<u64>(Some(0), clock()).try_build().unwrap();
    let mut events = cache.memory_evictions().subscribe();
    cache
        .try_set("rejected", 99)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(matches!(
        events.try_recv(),
        Err(EvictionReceiveError::Empty)
    ));
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn lag_is_bounded_independent_and_remaining_records_drain_after_producer_close() {
    let cache = builder::<u64>(None, clock())
        .events_capacity(2)
        .try_build()
        .unwrap();
    let mut slow = cache.memory_evictions().subscribe();
    let mut fast = cache.memory_evictions().subscribe();
    for value in 1..=4 {
        cache
            .try_set("k", value)
            .await
            .unwrap()
            .wait()
            .await
            .unwrap();
        cache.try_remove("k").await.unwrap().wait().await.unwrap();
        assert_eq!(*fast.recv().await.unwrap().value(), value);
    }
    assert_eq!(*slow.try_recv().unwrap().value(), 3);
    assert_eq!(slow.lost_events(), 2);
    assert_eq!(fast.lost_events(), 0);
    cache.shutdown().await.unwrap();
    drop(cache);
    assert_eq!(*slow.recv().await.unwrap().value(), 4);
    assert!(matches!(slow.try_recv(), Err(EvictionReceiveError::Closed)));
}

struct Probe {
    on_drop: Option<Box<dyn Fn() + Send + Sync>>,
}
impl Drop for Probe {
    fn drop(&mut self) {
        if let Some(drop) = &self.on_drop {
            drop();
        }
    }
}
fn quiet() -> Arc<Probe> {
    Arc::new(Probe { on_drop: None })
}
fn bounded_run(work: impl FnOnce() + Send + 'static) {
    let (done, wait) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        work();
        done.send(()).unwrap();
    });
    wait.recv_timeout(Duration::from_secs(8))
        .expect("cache/queue value destruction must permit reentry");
    worker.join().unwrap();
}

#[test]
fn ordinary_remove_and_replace_destructors_reenter_the_same_public_native_key() {
    bounded_run(|| {
        for limit in [None, Some(2)] {
            for replace in [false, true] {
                let cache = Arc::new(
                    BlockingCache::from_builder(builder::<Arc<Probe>>(limit, clock())).unwrap(),
                );
                let owner = Arc::downgrade(&cache);
                let runs = Arc::new(AtomicUsize::new(0));
                let calls = runs.clone();
                cache
                    .try_set(
                        "k",
                        Arc::new(Probe {
                            on_drop: Some(Box::new(move || {
                                let cache = owner.upgrade().unwrap();
                                cache.try_remove("k").unwrap().wait().unwrap();
                                calls.fetch_add(1, Ordering::SeqCst);
                            })),
                        }),
                    )
                    .unwrap()
                    .wait()
                    .unwrap();
                if replace {
                    cache.try_set("k", quiet()).unwrap().wait().unwrap();
                } else {
                    cache.try_remove("k").unwrap().wait().unwrap();
                }
                assert_eq!(runs.load(Ordering::SeqCst), 1);
                cache.shutdown().unwrap();
            }
        }
    });
}

#[test]
fn factory_replacement_releases_origin_coordination_before_old_value_destructor() {
    bounded_run(|| {
        for limit in [None, Some(2)] {
            let clock = clock();
            let cache = Arc::new(
                BlockingCache::from_builder(
                    builder::<Arc<Probe>>(limit, clock.clone()).default_options(
                        EntryOptions::new(Duration::from_secs(60)).with_fail_safe(
                            true,
                            Some(Duration::from_secs(600)),
                            None,
                        ),
                    ),
                )
                .unwrap(),
            );
            let owner = Arc::downgrade(&cache);
            let calls = Arc::new(AtomicUsize::new(0));
            let runs = calls.clone();
            cache
                .try_set(
                    "k",
                    Arc::new(Probe {
                        on_drop: Some(Box::new(move || {
                            owner
                                .upgrade()
                                .unwrap()
                                .try_remove("k")
                                .unwrap()
                                .wait()
                                .unwrap();
                            runs.fetch_add(1, Ordering::SeqCst);
                        })),
                    }),
                )
                .unwrap()
                .wait()
                .unwrap();
            clock.advance(Duration::from_secs(61));
            cache.get_or_set("k", |ctx| Ok(ctx.value(quiet()))).unwrap();
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            cache.shutdown().unwrap();
        }
    });
}

#[test]
fn overwritten_queue_slots_reenter_after_outer_lane_release() {
    bounded_run(|| {
        for limit in [None, Some(2)] {
            let cache = Arc::new(
                BlockingCache::from_builder(
                    builder::<Arc<Probe>>(limit, clock()).events_capacity(1),
                )
                .unwrap(),
            );
            let mut events = cache.memory_evictions().subscribe();
            let owner = Arc::downgrade(&cache);
            let calls = Arc::new(AtomicUsize::new(0));
            let runs = calls.clone();
            cache
                .try_set(
                    "k",
                    Arc::new(Probe {
                        on_drop: Some(Box::new(move || {
                            owner
                                .upgrade()
                                .unwrap()
                                .try_remove("k")
                                .unwrap()
                                .wait()
                                .unwrap();
                            runs.fetch_add(1, Ordering::SeqCst);
                        })),
                    }),
                )
                .unwrap()
                .wait()
                .unwrap();
            cache.try_remove("k").unwrap().wait().unwrap();
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            cache.try_set("k", quiet()).unwrap().wait().unwrap();
            cache.try_remove("k").unwrap().wait().unwrap();
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert_eq!(
                events.try_recv().unwrap().reason(),
                MemoryEvictionReason::Removed
            );
            assert_eq!(events.lost_events(), 1);
            cache.shutdown().unwrap();
        }
    });
}

#[test]
fn last_receiver_releases_retained_values_after_queue_unlock() {
    bounded_run(|| {
        let cache =
            Arc::new(BlockingCache::from_builder(builder::<Arc<Probe>>(None, clock())).unwrap());
        let events = cache.memory_evictions().subscribe();
        let owner = Arc::downgrade(&cache);
        let calls = Arc::new(AtomicUsize::new(0));
        let runs = calls.clone();
        cache
            .try_set(
                "k",
                Arc::new(Probe {
                    on_drop: Some(Box::new(move || {
                        let cache = owner.upgrade().unwrap();
                        let _nested = cache.memory_evictions().subscribe();
                        cache.try_remove("k").unwrap().wait().unwrap();
                        runs.fetch_add(1, Ordering::SeqCst);
                    })),
                }),
            )
            .unwrap()
            .wait()
            .unwrap();
        cache.try_remove("k").unwrap().wait().unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        drop(events);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        cache.shutdown().unwrap();
    });
}

#[test]
fn standalone_store_preserves_original_entry_identity() {
    bounded_run(|| {
        for limits in [MemoryLimits::default(), MemoryLimits::new(Some(2), None)] {
            let clock = clock();
            let store = MemoryStore::with_clock(limits, Events::default(), clock.clone());
            let mut events = store.evictions().subscribe();
            let entry = Entry::fresh(
                7,
                &EntryOptions::new(Duration::from_secs(60)),
                clock.now(),
                Box::new([]),
                None,
                None,
            );
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(store.insert_at(Arc::from("k"), entry.clone(), clock.now()));
            drop(runtime.block_on(store.remove("k")));
            assert!(events.try_recv().unwrap().entry().is_same_instance(&entry));
        }
    });
}

struct PausedWrite {
    calls: AtomicUsize,
    entered: tokio::sync::Notify,
}
#[async_trait::async_trait]
impl DistributedCache for PausedWrite {
    async fn get(&self, _: &str) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }
    async fn set(&self, _: &str, _: Vec<u8>, _: Option<Duration>) -> Result<()> {
        if self.calls.fetch_add(1, Ordering::SeqCst) > 0 {
            self.entered.notify_one();
            std::future::pending::<()>().await;
        }
        Ok(())
    }
    async fn remove(&self, _: &str) -> Result<()> {
        Ok(())
    }
}
struct ProbeCodec;
impl DistributedSerializer<Arc<Probe>> for ProbeCodec {
    fn serialize(&self, _: &DistributedEntry<Arc<Probe>>) -> Result<Vec<u8>> {
        Ok(vec![0])
    }
    fn deserialize(&self, _: &[u8]) -> Result<DistributedEntry<Arc<Probe>>> {
        Err(Error::Deserialization(
            "read is unused by this write fixture".into(),
        ))
    }
}
#[test]
fn cancelling_a_suspended_foreground_write_releases_lane_before_retirement_destructor() {
    bounded_run(|| {
        for limit in [None, Some(2)] {
            let backend = Arc::new(PausedWrite {
                calls: AtomicUsize::new(0),
                entered: tokio::sync::Notify::new(),
            });
            let cache = Arc::new(
                BlockingCache::from_builder(
                    builder::<Arc<Probe>>(limit, clock())
                        .distributed(backend.clone())
                        .serializer(Arc::new(ProbeCodec))
                        .default_options(
                            EntryOptions::new(Duration::from_secs(60))
                                .with_allow_background_distributed_operations(false),
                        )
                        .auto_recovery(RecoveryConfig {
                            enabled: false,
                            ..RecoveryConfig::default()
                        }),
                )
                .unwrap(),
            );
            let owner = Arc::downgrade(&cache);
            let (dropped, wait) = mpsc::channel();
            cache
                .try_set(
                    "k",
                    Arc::new(Probe {
                        on_drop: Some(Box::new(move || {
                            owner
                                .upgrade()
                                .unwrap()
                                .try_remove("k")
                                .unwrap()
                                .wait()
                                .unwrap();
                            dropped.send(()).unwrap();
                        })),
                    }),
                )
                .unwrap()
                .wait()
                .unwrap();
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async {
                let source = CancellationSource::new();
                let caller = cache.as_async().clone();
                let token = source.token();
                let write = tokio::spawn(async move {
                    caller
                        .try_set_full_cancellable("k", quiet(), None, Box::new([]), token)
                        .await
                });
                tokio::time::timeout(Duration::from_secs(2), backend.entered.notified())
                    .await
                    .unwrap();
                assert!(
                    wait.try_recv().is_err(),
                    "old value must stay retained through the parked lane"
                );
                source.cancel();
                assert!(matches!(
                    tokio::time::timeout(Duration::from_secs(2), write)
                        .await
                        .unwrap()
                        .unwrap(),
                    Err(Error::OperationCancelled { .. })
                ));
            });
            wait.recv_timeout(Duration::from_secs(2))
                .expect("cancelled write must release all guards before destructor reentry");
            assert_eq!(backend.calls.load(Ordering::SeqCst), 2);
            cache.shutdown().unwrap();
        }
    });
}
