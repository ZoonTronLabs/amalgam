use amalgam::*;
use async_trait::async_trait;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{Notify, Semaphore};

#[derive(Clone, Copy)]
enum Acquisition {
    LostReply,
    Contended,
}
struct ReplyLostLocker {
    held: Mutex<Option<LeaseToken>>,
    releases: AtomicUsize,
    release_entered: Notify,
    release_gate: Semaphore,
    acquisition: Acquisition,
}
#[async_trait]
impl DistributedLocker for ReplyLostLocker {
    async fn acquire(&self, _: &str, _: Duration, _: Timeout) -> Result<Option<String>> {
        unreachable!("caller-selected ownership uses the canonical receipt hook")
    }
    async fn release(&self, _: &str, token: &str) -> Result<()> {
        self.releases.fetch_add(1, Ordering::SeqCst);
        self.release_entered.notify_one();
        self.release_gate.acquire().await.unwrap().forget();
        let mut held = self.held.lock().unwrap();
        if held.as_ref().is_some_and(|held| held.as_str() == token) {
            *held = None;
        }
        Err(Error::Distributed(
            "original uncertain release failure".into(),
        ))
    }
    fn lease_support(&self) -> LeaseSupport {
        LeaseSupport::FixedTtl
    }
    fn token_acquisition(&self) -> TokenAcquisition {
        TokenAcquisition::CallerSelected
    }
    async fn acquire_with_token(
        &self,
        _: &str,
        token: &LeaseToken,
        _: LeaseTtl,
        _: Timeout,
    ) -> std::result::Result<bool, LeaseError> {
        match self.acquisition {
            Acquisition::LostReply => {
                *self.held.lock().unwrap() = Some(token.clone());
                Err(LeaseError::backend(std::io::Error::other(
                    "original lost acquisition reply",
                )))
            }
            Acquisition::Contended => Ok(false),
        }
    }
}
fn locker(acquisition: Acquisition) -> Arc<ReplyLostLocker> {
    Arc::new(ReplyLostLocker {
        held: Mutex::new(None),
        releases: AtomicUsize::new(0),
        release_entered: Notify::new(),
        release_gate: Semaphore::new(0),
        acquisition,
    })
}
#[tokio::test]
async fn failed_caller_token_acquisition_is_cleaned_and_shutdown_retains_both_causes() {
    let clock = Arc::new(ManualClock::default());
    let locker = locker(Acquisition::LostReply);
    let cache: Cache<u64> = Cache::builder()
        .clock(clock.clone())
        .distributed(Arc::new(InMemoryDistributedCache::new(clock)))
        .serializer(Arc::new(JsonSerializer))
        .distributed_locker(locker.clone())
        .lease_policy(LeasePolicy::CooperativeLegacy)
        .auto_recovery(RecoveryConfig {
            enabled: false,
            ..RecoveryConfig::default()
        })
        .default_options(EntryOptions::default().with_rethrow_distributed_locker_exceptions(true))
        .try_build()
        .unwrap();
    let origin_calls = Arc::new(AtomicUsize::new(0));
    let calls = origin_calls.clone();
    let acquisition = cache
        .get_or_set("key", move |ctx| async move {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(ctx.value(7))
        })
        .await;
    assert!(
        matches!(acquisition, Err(Error::Lease(LeaseError::Backend { source })) if source.to_string() == "original lost acquisition reply")
    );
    assert_eq!(origin_calls.load(Ordering::SeqCst), 0);
    tokio::time::timeout(Duration::from_secs(1), locker.release_entered.notified())
        .await
        .expect("the known uncertain token was not scheduled for cleanup");
    let shutdown = tokio::spawn({
        let cache = cache.clone();
        async move { cache.shutdown().await }
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    let premature = shutdown.is_finished();
    locker.release_gate.add_permits(1);
    let first = shutdown.await.unwrap();
    let repeated = cache.shutdown().await;
    assert!(
        !premature,
        "shutdown did not retain the owned uncertain-token cleanup"
    );
    assert_eq!(locker.releases.load(Ordering::SeqCst), 1);
    assert!(locker.held.lock().unwrap().is_none());
    for result in [first, repeated] {
        assert!(
            matches!(result, Err(Error::Shutdown(error)) if error.failures().iter().any(|failure| matches!(failure, ShutdownFailure::Work(Error::Lease(LeaseError::Backend { source })) if source.to_string().contains("original uncertain release failure"))))
        );
    }
}
#[tokio::test]
async fn a_contended_receipt_does_not_fabricate_uncertain_ownership_cleanup() {
    let locker = locker(Acquisition::Contended);
    let receipt = acquire_owned(
        locker.clone(),
        Arc::from("key"),
        LeaseTtl::new(Duration::from_secs(1)).unwrap(),
        Timeout::After(Duration::from_millis(50)),
        AcquisitionPolicy::TokenOwned,
    )
    .await
    .unwrap();
    assert!(receipt.is_none());
    assert_eq!(locker.releases.load(Ordering::SeqCst), 0);
    assert!(locker.held.lock().unwrap().is_none());
}
