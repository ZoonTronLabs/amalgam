//! The lazy query may contain an unpinned input that opts out of Unpin.
use amalgam::{Cache, Error};
use std::future::Future;
use std::marker::PhantomPinned;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

struct PinnedKey {
    calls: Arc<AtomicUsize>,
    _pin: PhantomPinned,
}
impl AsRef<str> for PinnedKey {
    fn as_ref(&self) -> &str {
        self.calls.fetch_add(1, Ordering::SeqCst);
        "query"
    }
}
#[test]
fn creating_and_dropping_a_read_does_not_access_the_key_or_admit_work() {
    let cache = Cache::<u64>::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let key = || PinnedKey {
        calls: calls.clone(),
        _pin: PhantomPinned,
    };
    drop(cache.read(key(), None));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let query = cache.read(key(), None);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    cache.close();
    let mut query = std::pin::pin!(query);
    let mut context = Context::from_waker(Waker::noop());
    assert!(matches!(
        query.as_mut().poll(&mut context),
        Poll::Ready(Err(Error::CacheClosed))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
