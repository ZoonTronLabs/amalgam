//! Initial subscription policy and explicit readiness have separate effects.

use amalgam::{
    BackplaneReadiness, Cache, CacheEvent, CloseOutcome, EntryOptions, Error,
    FactoryCancellationReason, RecoveryConfig, Result, advanced::EffectOutcome,
    advanced::LocalEffect, advanced::SkipReason, provider::Backplane, provider::BackplaneMessage,
    provider::BackplaneState, provider::ContinuityEpoch, provider::InProcessBackplane,
    provider::MemoryAdmission,
};
use async_trait::async_trait;
use std::future::IntoFuture;
use std::future::{Future, poll_fn};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Poll;
use std::time::Duration;
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
    assert!(matches!(
        report.local,
        LocalEffect::Stored(MemoryAdmission::Admitted)
    ));
    assert!(matches!(
        report.backplane,
        EffectOutcome::Skipped(SkipReason::Policy)
    ));
    assert_eq!(backplane.publications.load(Ordering::SeqCst), 0);

    let mut readiness = Box::pin(cache.ready());
    poll_fn(|context| {
        assert!(readiness.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;
    backplane.acknowledge();
    assert!(matches!(
        readiness.await.unwrap(),
        BackplaneReadiness::Acknowledged(ContinuityEpoch::INITIAL)
    ));
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
    poll_fn(|context| {
        assert!(mutation.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;
    assert_eq!(cache.close(), CloseOutcome::Started);
    cache.shutdown().await.unwrap();
    assert!(matches!(
        mutation.await,
        Err(Error::OperationCancelled {
            reason: FactoryCancellationReason::CacheShutdown
        })
    ));
    assert_eq!(backplane.publications.load(Ordering::SeqCst), 0);
    while let Ok(event) = events.try_recv() {
        assert!(!matches!(event, CacheEvent::Set { .. }));
    }
}
