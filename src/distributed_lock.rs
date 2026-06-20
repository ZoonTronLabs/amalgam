//! Cross-node single-flight: the [`DistributedLocker`] seam.
//!
//! In-process stampede protection ([`KeyedLock`](crate::locking::KeyedLock))
//! guarantees one factory per key *per node*. A distributed locker extends that
//! to *one factory per key across the cluster*, matching FusionCache's
//! `IFusionCacheDistributedLocker`. It is token-based: `acquire` returns an
//! opaque token that `release` consumes, so a Redis (`SET key token NX PX`) or
//! database implementation maps cleanly.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use dashmap::DashMap;

use crate::error::Result;
use crate::time::{Clock, Timeout, Timestamp};

/// A cross-node lock backend.
#[async_trait]
pub trait DistributedLocker: Send + Sync {
    /// Attempts to acquire the lock for `key`, waiting up to `timeout`.
    ///
    /// Returns `Some(token)` on success (the token must be passed back to
    /// [`release`](DistributedLocker::release)), or `None` if the wait elapsed.
    /// `ttl` bounds how long the lock is held even if the holder dies.
    ///
    /// # Errors
    /// Returns an error only on backend failure, not on a normal timeout.
    async fn acquire(&self, key: &str, ttl: Duration, timeout: Timeout) -> Result<Option<String>>;

    /// Releases a previously-acquired lock. Releasing a token that is no longer
    /// held (e.g. it already expired) is a no-op, not an error.
    ///
    /// # Errors
    /// Returns an error only on backend failure.
    async fn release(&self, key: &str, token: &str) -> Result<()>;
}

/// An in-process reference [`DistributedLocker`].
///
/// Share one instance (via `Arc`) between several caches to get cross-instance
/// single-flight within one process. A real cluster uses a Redis-backed locker.
pub struct InMemoryDistributedLocker {
    locks: Arc<DashMap<String, Held>>,
    clock: Arc<dyn Clock>,
}

#[derive(Clone)]
struct Held {
    token: String,
    expires_at: Timestamp,
}

impl InMemoryDistributedLocker {
    /// Creates a locker using `clock` for lock TTL accounting.
    #[must_use]
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        Self {
            locks: Arc::new(DashMap::new()),
            clock,
        }
    }

    /// Attempts a single non-blocking acquisition. Returns the token on success.
    fn try_once(&self, key: &str, ttl: Duration, now: Timestamp) -> Option<String> {
        use dashmap::mapref::entry::Entry;
        let expires_at = now.saturating_add(ttl);
        match self.locks.entry(key.to_owned()) {
            Entry::Occupied(mut occupied) => {
                if now.is_before(occupied.get().expires_at) {
                    None // still held by someone else
                } else {
                    let token = new_token();
                    occupied.insert(Held {
                        token: token.clone(),
                        expires_at,
                    });
                    Some(token)
                }
            }
            Entry::Vacant(vacant) => {
                let token = new_token();
                vacant.insert(Held {
                    token: token.clone(),
                    expires_at,
                });
                Some(token)
            }
        }
    }
}

fn new_token() -> String {
    format!("{:016x}{:016x}", fastrand::u64(..), fastrand::u64(..))
}

#[async_trait]
impl DistributedLocker for InMemoryDistributedLocker {
    async fn acquire(&self, key: &str, ttl: Duration, timeout: Timeout) -> Result<Option<String>> {
        let deadline = match timeout {
            Timeout::After(d) => Some(std::time::Instant::now() + d),
            Timeout::Infinite => None,
        };
        loop {
            if let Some(token) = self.try_once(key, ttl, self.clock.now()) {
                return Ok(Some(token));
            }
            if let Some(deadline) = deadline
                && std::time::Instant::now() >= deadline
            {
                return Ok(None);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn release(&self, key: &str, token: &str) -> Result<()> {
        self.locks.remove_if(key, |_, held| held.token == token);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::time::ManualClock;

    #[tokio::test]
    async fn second_acquire_blocks_until_release() {
        let clock = Arc::new(ManualClock::default());
        let locker = InMemoryDistributedLocker::new(clock);
        let token = locker
            .acquire("k", Duration::from_secs(30), Timeout::Infinite)
            .await
            .unwrap()
            .expect("first acquire succeeds");
        // A non-waiting second attempt fails while held.
        let second = locker
            .acquire("k", Duration::from_secs(30), Timeout::After(Duration::ZERO))
            .await
            .unwrap();
        assert!(second.is_none());
        locker.release("k", &token).await.unwrap();
        // Now it can be acquired again.
        assert!(
            locker
                .acquire("k", Duration::from_secs(30), Timeout::After(Duration::ZERO))
                .await
                .unwrap()
                .is_some()
        );
    }
}
