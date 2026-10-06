//! Pluggable local single-flight coordination with owned release.
//!
//! A custom locker receives cache identity, a collision-free coordination key,
//! a finite/infinite wait budget and an acquisition-only cancellation token.
//! The cache enforces the wait budget even if the provider does not. The token
//! ends before the pending provider future is dropped; it never owns the factory.
//! Successful guards move into background completion and release exactly once.

use crate::execution::{CancellationSource, FactoryCancellation};
use crate::locking::{KeyGuard, KeyedLock};
use crate::{FactoryCancellationReason as Reason, MarkerKind, Timeout};
use async_trait::async_trait;
use std::sync::Arc;

/// Failure of a custom local coordination provider.
#[derive(Debug, thiserror::Error)]
pub enum MemoryLockerError {
    /// The provider failed; its complete original cause remains available.
    #[error("local memory locker failed: {source}")]
    Provider {
        /// The original provider error.
        #[source]
        source: Arc<dyn std::error::Error + Send + Sync>,
    },
    /// Acquisition was cancelled, rather than failing or timing out.
    #[error("local memory lock cancelled: {}", reason.as_str())]
    Cancelled {
        /// The terminal acquisition reason.
        reason: Reason,
    },
}
impl MemoryLockerError {
    /// Preserves the provider's typed cause.
    pub fn from_source(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Provider {
            source: Arc::new(source),
        }
    }
}

/// Cache identity supplied to a shared provider and its per-cache shutdown hook.
#[derive(Debug, Clone)]
pub struct MemoryLockerContext {
    name: Arc<str>,
    instance_id: Arc<str>,
}
impl MemoryLockerContext {
    /// Diagnostic cache name.
    pub fn cache_name(&self) -> &str {
        &self.name
    }
    /// Owning cache instance, including when the provider is shared.
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }
}

/// The protected data family; marker keys cannot collide with ordinary entries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemoryLockKind {
    /// An ordinary cached value.
    Entry,
    /// A tag or cache-wide invalidation observation.
    Marker(MarkerKind),
}

/// An acquisition request created by the owning cache.
#[derive(Debug, Clone)]
pub struct MemoryLockRequest {
    context: MemoryLockerContext,
    key: Arc<str>,
    coordination_key: Arc<str>,
    kind: MemoryLockKind,
    timeout: Timeout,
    cancellation: FactoryCancellation,
}
impl MemoryLockRequest {
    /// Identity of the owning cache.
    pub fn context(&self) -> &MemoryLockerContext {
        &self.context
    }
    /// Processed entry key, or the marker observation key.
    pub fn key(&self) -> &str {
        &self.key
    }
    /// Key to use for coordination. Entry and marker namespaces are disjoint.
    ///
    /// A shared provider may additionally partition by cache name. Cache instance
    /// IDs describe ownership; partitioning by them prevents cross-cache sharing.
    pub fn coordination_key(&self) -> &str {
        &self.coordination_key
    }
    /// The protected data family.
    pub fn kind(&self) -> &MemoryLockKind {
        &self.kind
    }
    /// Maximum acquisition wait. Nonblocking eager attempts use zero.
    pub fn timeout(&self) -> Timeout {
        self.timeout
    }
    /// Acquisition-only token. It ends after success, timeout or cancellation.
    pub fn cancellation(&self) -> &FactoryCancellation {
        &self.cancellation
    }
}

/// Open release behavior for an owned local lock.
///
/// The callback consumes its guard and must release synchronously. Cache-owned
/// drop calls it once; release failures are logged and cannot replace an already
/// computed value. Use [`MemoryLock::release`] to observe release errors directly.
pub trait MemoryLockGuard: Send + Sync + 'static {
    /// Releases the owned resource. No guard can be reused after this call.
    fn release(self: Box<Self>) -> std::result::Result<(), MemoryLockerError>;
}
impl MemoryLockGuard for KeyGuard {
    fn release(self: Box<Self>) -> std::result::Result<(), MemoryLockerError> {
        drop(self);
        Ok(())
    }
}
enum ReleaseState {
    Held(Box<dyn MemoryLockGuard>),
    Released,
}
/// Move-only ownership token, released on every completion/cancellation path.
pub struct MemoryLock {
    state: ReleaseState,
}
impl MemoryLock {
    /// Takes ownership of an independently supplied release implementation.
    pub fn new(guard: impl MemoryLockGuard) -> Self {
        Self {
            state: ReleaseState::Held(Box::new(guard)),
        }
    }
    /// Releases once and returns the provider's actual release result.
    pub fn release(mut self) -> std::result::Result<(), MemoryLockerError> {
        self.release_inner()
    }
    fn release_inner(&mut self) -> std::result::Result<(), MemoryLockerError> {
        match std::mem::replace(&mut self.state, ReleaseState::Released) {
            ReleaseState::Held(guard) => guard.release(),
            ReleaseState::Released => Ok(()),
        }
    }
}
impl std::fmt::Debug for MemoryLock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryLock").finish_non_exhaustive()
    }
}
impl Drop for MemoryLock {
    fn drop(&mut self) {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.release_inner())) {
            Ok(Ok(())) => {}
            Ok(Err(error)) => tracing::warn!(%error, "local memory lock release failed"),
            Err(_) => tracing::warn!("local memory lock release panicked"),
        }
    }
}

/// Optional acquisition is distinct from a provider failure.
#[derive(Debug)]
pub enum MemoryLockOutcome {
    /// A move-only guard protects this key.
    Acquired(MemoryLock),
    /// No guard was available. Ordinary requests follow their configured stale
    /// fallback or unlocked factory behavior; eager refresh simply skips.
    Unavailable,
}

/// Independently supplied local coordination for entries and marker factories.
#[async_trait]
pub trait MemoryLocker: Send + Sync + 'static {
    /// Acquires a guard. The host enforces the supplied timeout and cancellation;
    /// providers should also observe the token for their own cooperative work.
    async fn acquire(
        &self,
        request: MemoryLockRequest,
    ) -> std::result::Result<MemoryLockOutcome, MemoryLockerError>;
    /// Attempts an eager acquisition without blocking the caller thread.
    fn try_acquire(
        &self,
        request: MemoryLockRequest,
    ) -> std::result::Result<MemoryLockOutcome, MemoryLockerError>;
    /// Drains resources belonging to this context, once per owning cache, after
    /// its factories and guards finish. Other caches can share this provider.
    /// Explicit cache shutdown waits for this hook and preserves its failure.
    async fn shutdown(
        &self,
        _context: MemoryLockerContext,
    ) -> std::result::Result<(), MemoryLockerError> {
        Ok(())
    }
}
#[async_trait]
impl MemoryLocker for KeyedLock {
    async fn acquire(
        &self,
        request: MemoryLockRequest,
    ) -> std::result::Result<MemoryLockOutcome, MemoryLockerError> {
        Ok(MemoryLockOutcome::Acquired(MemoryLock::new(
            self.lock(request.coordination_key()).await,
        )))
    }
    fn try_acquire(
        &self,
        request: MemoryLockRequest,
    ) -> std::result::Result<MemoryLockOutcome, MemoryLockerError> {
        Ok(match self.try_lock(request.coordination_key()) {
            Some(guard) => MemoryLockOutcome::Acquired(MemoryLock::new(guard)),
            None => MemoryLockOutcome::Unavailable,
        })
    }
}

mod runtime;
pub(crate) use runtime::{LocalGuard, LocalLocks};
