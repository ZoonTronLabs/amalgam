//! Cancellation must release an owned pending origin without another caller poll.

use std::future::{Future, pending, poll_fn};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Poll;
use std::time::Duration;

use amalgam::{Cache, CancellationSource, Error, FactoryCancellationReason};

struct OriginDrop(Arc<AtomicUsize>);

impl Drop for OriginDrop {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn explicit_caller_cancellation_releases_a_parked_origin_before_repoll() {
    let cache: Cache<u64> = Cache::builder().try_build().expect("valid cache");
    let source = CancellationSource::new();
    let entered = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let entered_factory = entered.clone();
    let dropped_factory = dropped.clone();
    let mut operation = Box::pin(cache.get_or_set_cancellable(
        "parked",
        move |ctx| async move {
            let _lifetime = OriginDrop(dropped_factory);
            entered_factory.fetch_add(1, Ordering::SeqCst);
            pending::<()>().await;
            Ok::<_, amalgam::FactoryError>(ctx.value(42))
        },
        source.token(),
    ));
    tokio::time::timeout(
        Duration::from_secs(2),
        poll_fn(|cx| {
            assert!(operation.as_mut().poll(cx).is_pending());
            if entered.load(Ordering::SeqCst) == 1 {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }),
    )
    .await
    .expect("origin entered");
    source.cancel();
    assert_eq!(
        dropped.load(Ordering::SeqCst),
        1,
        "cancellation itself must drop the parked origin"
    );
    assert!(matches!(
        operation.await,
        Err(Error::OperationCancelled {
            reason: FactoryCancellationReason::CallerCancelled
        })
    ));
    assert!(!cache.read("parked", None).await.expect("read").has_value());
    assert_eq!(
        cache
            .get_or_set("parked", |ctx| async move {
                Ok::<_, amalgam::FactoryError>(ctx.value(7))
            })
            .await
            .expect("new origin owns released key"),
        7
    );
    cache.shutdown().await.expect("cleanup");
}
