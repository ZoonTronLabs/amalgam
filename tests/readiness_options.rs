//! Initial subscription policy and explicit readiness have separate effects.

#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

use std::future;
use std::future::{Future, IntoFuture};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Poll;
use std::time::Duration;

use amalgam::advanced::{EffectOutcome, LocalEffect, SkipReason};
use amalgam::provider::{
    Backplane, BackplaneMessage, BackplaneState, ContinuityEpoch, InMemoryDistributedCache,
    InProcessBackplane, JsonSerializer, MemoryAdmission, SystemClock,
};
use amalgam::{
    BackplaneReadiness, Cache, CacheEvent, CloseOutcome, EntryOptions, Error,
    FactoryCancellationReason, FactoryError, RecoveryConfig, Result,
};
use async_trait::async_trait;
use tokio::sync::{broadcast, watch};

struct AwaitingAcknowledgement {
    messages: InProcessBackplane,
    state: watch::Sender<BackplaneState>,
    publications: AtomicUsize,
}

impl AwaitingAcknowledgement {
    fn new() -> Self {
        let (state, _) = watch::channel(BackplaneState::Disconnected {
            epoch: ContinuityEpoch::INITIAL,
        });
        Self {
            messages: InProcessBackplane::default(),
            state,
            publications: AtomicUsize::new(0),
        }
    }

    fn acknowledge(&self) {
        self.state.send_replace(BackplaneState::Connected {
            epoch: ContinuityEpoch::INITIAL,
        });
    }
}

#[async_trait]
impl Backplane for AwaitingAcknowledgement {
    async fn publish(&self, message: BackplaneMessage) -> Result<()> {
        self.publications.fetch_add(1, Ordering::SeqCst);
        self.messages.publish(message).await
    }

    fn subscribe(&self) -> broadcast::Receiver<BackplaneMessage> {
        self.messages.subscribe()
    }

    fn connection_state(&self) -> Option<watch::Receiver<BackplaneState>> {
        Some(self.state.subscribe())
    }
}

fn options() -> EntryOptions {
    EntryOptions::new(Duration::from_secs(60))
        .with_skip_backplane_notifications(true)
        .with_allow_background_backplane_operations(false)
}

#[tokio::test]
async fn default_initial_wait_allows_local_mutation_but_explicit_ready_still_needs_ack() {
    let backplane = Arc::new(AwaitingAcknowledgement::new());
    let cache = Cache::<i32>::builder()
        .backplane(backplane.clone())
        .default_options(options())
        .auto_recovery(RecoveryConfig {
            enabled: false,
            ..RecoveryConfig::default()
        })
        .try_build()
        .unwrap();
    let receipt = tokio::time::timeout(
        Duration::from_secs(1),
        cache.set("k", 42).with_receipt().into_future(),
    )
    .await
    .expect("disabled initial wait blocked local work")
    .unwrap();
    let report = receipt.wait().await.unwrap();
    assert!(
        matches!(report.local, LocalEffect::Stored(MemoryAdmission::Admitted)),
        "contract: default initial wait allows local mutation but explicit ready still needs ack"
    );
    assert!(
        matches!(report.backplane, EffectOutcome::Skipped(SkipReason::Policy)),
        "contract: default initial wait allows local mutation but explicit ready still needs ack"
    );
    assert_eq!(
        backplane.publications.load(Ordering::SeqCst),
        0,
        "contract: default initial wait allows local mutation but explicit ready still needs ack"
    );

    let mut readiness = Box::pin(cache.ready());
    future::poll_fn(|context| {
        assert!(readiness.as_mut().poll(context).is_pending(), "contract: default initial wait allows local mutation but explicit ready still needs ack");
        Poll::Ready(())
    })
    .await;
    backplane.acknowledge();
    assert!(
        matches!(
            readiness.await.unwrap(),
            BackplaneReadiness::Acknowledged(ContinuityEpoch::INITIAL)
        ),
        "contract: default initial wait allows local mutation but explicit ready still needs ack"
    );
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn close_cancels_initial_ack_admission_before_local_or_notification_effects() {
    let backplane = Arc::new(AwaitingAcknowledgement::new());
    let cache = Cache::<i32>::builder()
        .backplane(backplane.clone())
        .wait_for_initial_backplane_subscribe(true)
        .default_options(options())
        .auto_recovery(RecoveryConfig {
            enabled: false,
            ..RecoveryConfig::default()
        })
        .try_build()
        .unwrap();
    let mut events = cache.events().subscribe();
    let mut mutation = Box::pin(cache.set("k", 42).with_receipt().into_future());
    future::poll_fn(|context| {
        assert!(
            mutation.as_mut().poll(context).is_pending(),
            "contract: close cancels initial ack admission before local or notification effects"
        );
        Poll::Ready(())
    })
    .await;
    assert_eq!(
        cache.close(),
        CloseOutcome::Started,
        "contract: close cancels initial ack admission before local or notification effects"
    );
    cache.shutdown().await.unwrap();
    assert!(
        matches!(
            mutation.await,
            Err(Error::OperationCancelled {
                reason: FactoryCancellationReason::CacheShutdown
            })
        ),
        "contract: close cancels initial ack admission before local or notification effects"
    );
    assert_eq!(
        backplane.publications.load(Ordering::SeqCst),
        0,
        "contract: close cancels initial ack admission before local or notification effects"
    );
    while let Ok(event) = events.try_recv() {
        assert!(
            !matches!(event, CacheEvent::Set { .. }),
            "contract: close cancels initial ack admission before local or notification effects"
        );
    }
}

#[tokio::test]
async fn initial_subscription_wait_admits_a_cold_l2_read_after_acknowledgement() -> Result<()> {
    let backend = Arc::new(InMemoryDistributedCache::new(Arc::new(SystemClock)));
    let writer = Cache::<i32>::builder()
        .distributed(backend.clone())
        .serializer(Arc::new(JsonSerializer))
        .try_build()?;
    let _ = writer
        .set("shared", 42)
        .with_receipt()
        .await?
        .wait()
        .await?;
    let backplane = Arc::new(AwaitingAcknowledgement::new());
    let reader = Cache::<i32>::builder()
        .distributed(backend)
        .serializer(Arc::new(JsonSerializer))
        .backplane(backplane.clone())
        .wait_for_initial_backplane_subscribe(true)
        .default_options(options().with_skip_memory(true, false))
        .auto_recovery(RecoveryConfig {
            enabled: false,
            ..RecoveryConfig::default()
        })
        .try_build()?;
    let mut read = Box::pin(reader.try_get("shared").into_future());
    future::poll_fn(|context| {
        assert!(
            read.as_mut().poll(context).is_pending(),
            "contract: initial subscription wait admits a cold l2 read after acknowledgement"
        );
        Poll::Ready(())
    })
    .await;
    backplane.acknowledge();
    let value = tokio::time::timeout(Duration::from_secs(1), read)
        .await
        .expect("initial acknowledgement did not release the L2 read")?;
    assert_eq!(
        value.as_ref(),
        Some(&42),
        "contract: initial subscription wait admits a cold l2 read after acknowledgement"
    );
    assert_eq!(
        backplane.publications.load(Ordering::SeqCst),
        0,
        "contract: initial subscription wait admits a cold l2 read after acknowledgement"
    );
    reader.shutdown().await?;
    writer.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn admitted_initial_subscription_is_not_awaited_again_after_a_gap_or_stop() -> Result<()> {
    let backplane = Arc::new(AwaitingAcknowledgement::new());
    let cache = Cache::<i32>::builder()
        .backplane(backplane.clone())
        .wait_for_initial_backplane_subscribe(true)
        .default_options(options())
        .auto_recovery(RecoveryConfig {
            enabled: false,
            ..RecoveryConfig::default()
        })
        .try_build()?;
    let mut initial = Box::pin(cache.set("initial", 42).into_future());
    future::poll_fn(|context| {
        assert!(
            initial.as_mut().poll(context).is_pending(),
            "contract: admitted initial subscription is not awaited again after a gap or stop"
        );
        Poll::Ready(())
    })
    .await;
    backplane.acknowledge();
    initial.await?;
    for (key, state) in [
        (
            "after-gap",
            BackplaneState::Disconnected {
                epoch: ContinuityEpoch::INITIAL,
            },
        ),
        ("after-stop", BackplaneState::Stopped),
    ] {
        backplane.state.send_replace(state);
        let value = tokio::time::timeout(
            Duration::from_secs(1),
            cache
                .get_or_set(
                    key,
                    amalgam::source::factory(
                        |ctx| async move { Ok::<_, FactoryError>(ctx.value(43)) },
                    ),
                )
                .into_future(),
        )
        .await
        .expect("an admitted subscription started waiting again")?;
        assert_eq!(
            value, 43,
            "contract: admitted initial subscription is not awaited again after a gap or stop"
        );
    }
    assert_eq!(
        backplane.publications.load(Ordering::SeqCst),
        0,
        "contract: admitted initial subscription is not awaited again after a gap or stop"
    );
    cache.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn a_stopped_initial_subscription_preserves_its_error_without_running_the_factory() {
    let backplane = Arc::new(AwaitingAcknowledgement::new());
    backplane.state.send_replace(BackplaneState::Stopped);
    let cache = Cache::<i32>::builder()
        .backplane(backplane.clone())
        .wait_for_initial_backplane_subscribe(true)
        .default_options(options())
        .auto_recovery(RecoveryConfig {
            enabled: false,
            ..RecoveryConfig::default()
        })
        .try_build()
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let called = calls.clone();
    let result = cache
        .get_or_set(
            "blocked-origin",
            amalgam::source::factory(move |ctx| async move {
                called.fetch_add(1, Ordering::SeqCst);
                Ok::<_, FactoryError>(ctx.value(43))
            }),
        )
        .await;
    assert!(
        matches!(result, Err(Error::Backplane(cause)) if cause == "backplane provider stopped"),
        "contract: a stopped initial subscription preserves its error without running the factory"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "contract: a stopped initial subscription preserves its error without running the factory"
    );
    assert_eq!(
        backplane.publications.load(Ordering::SeqCst),
        0,
        "contract: a stopped initial subscription preserves its error without running the factory"
    );
    cache.shutdown().await.unwrap();
}
