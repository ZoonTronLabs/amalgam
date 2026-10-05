//! Cancellation remains first-wins and wakes all waiters around registration.
use amalgam::{CancellationRequest, CancellationSource, FactoryCancellationReason};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Barrier;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_cancellation_records_one_request_and_wakes_every_waiter() {
    let source = CancellationSource::new();
    let barrier = Arc::new(Barrier::new(33));
    let mut waiters = Vec::with_capacity(32);
    for _ in 0..32 {
        let token = source.token();
        let barrier = barrier.clone();
        waiters.push(tokio::spawn(async move {
            barrier.wait().await;
            token.cancelled().await
        }));
    }
    barrier.wait().await;
    let mut requests = Vec::with_capacity(16);
    for _ in 0..16 {
        let source = source.clone();
        requests.push(tokio::spawn(async move { source.cancel() }));
    }
    let mut accepted = 0;
    for request in requests {
        accepted += usize::from(request.await.unwrap() == CancellationRequest::Cancelled);
    }
    assert_eq!(accepted, 1);
    for waiter in waiters {
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), waiter)
                .await
                .unwrap()
                .unwrap(),
            FactoryCancellationReason::CallerCancelled
        );
    }
    assert_eq!(source.cancel(), CancellationRequest::AlreadyCancelled);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancellation_before_or_during_wait_registration_never_loses_a_wakeup() {
    for round in 0..512 {
        let source = CancellationSource::new();
        let token = source.token();
        if round % 2 == 0 {
            source.cancel();
        }
        let waiter = tokio::spawn(async move { token.cancelled().await });
        if round % 2 != 0 {
            tokio::task::yield_now().await;
            source.cancel();
        }
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), waiter)
                .await
                .unwrap()
                .unwrap(),
            FactoryCancellationReason::CallerCancelled
        );
    }
}
