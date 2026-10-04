//! Token-owned distributed leases with explicit lifetime capabilities.
//!
//! Legacy acquire/release implementations compile unchanged. Their lifetime and
//! cancellation semantics remain opaque unless they declare a capability. Native
//! providers support caller-owned tokens, renewal, and atomic fenced mutations.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::{oneshot, watch};
use tokio::time::Instant;

use crate::error::Result;
use crate::time::{Clock, Timeout, Timestamp};

/// Expected lease rejection or infrastructure failure.
#[derive(Debug, thiserror::Error)]
pub enum LeaseError {
    /// Owned acquisition requires an active runtime before any effect.
    #[error("owned distributed acquisition requires a Tokio runtime")]
    MissingRuntime,
    /// A TTL was zero or could not be represented by supported providers.
    #[error("lease TTL must be positive and at most i64::MAX milliseconds")]
    InvalidTtl,
    /// An externally supplied token was blank.
    #[error("lease token must not be blank")]
    InvalidToken,
    /// A finite I/O deadline cannot be represented by the monotonic clock.
    #[error("lease deadline is outside the monotonic clock range")]
    InvalidDeadline,
    /// A legacy backend cannot provide token-owned cancellation cleanup.
    #[error("the locker does not support caller-owned acquisition tokens")]
    UnsupportedTokenAcquisition,
    /// The backend cannot renew an expiring lease.
    #[error("the locker does not support lease renewal")]
    UnsupportedRenewal,
    /// Legacy ownership lifetime is unknown and cannot authorize a strict commit.
    #[error("the locker has not declared its ownership lifetime")]
    OpaqueLifetime,
    /// Atomic partition-safe value writes require an explicit backend capability.
    #[error("the value backend does not support atomic lease-fenced writes")]
    UnsupportedFencing,
    /// Renewal or commit validation found that ownership was no longer held.
    #[error("distributed ownership lease was lost")]
    Lost,
    /// A bounded acquisition did not finish before its deadline.
    #[error("distributed lease acquisition deadline elapsed")]
    AcquisitionTimeout,
    /// Acquisition was cancelled before ownership was transferred.
    #[error("distributed lease acquisition was cancelled")]
    Cancelled,
    /// Explicit owned cleanup failed to complete within its finite budget.
    #[error("distributed lease cleanup deadline elapsed")]
    CleanupTimeout,
    /// A background acquisition task violated its execution contract.
    #[error("distributed acquisition task failed: {source}")]
    Task {
        /// Original task failure.
        #[source]
        source: tokio::task::JoinError,
    },
    /// The provider failed, preserving its original error chain.
    #[error("distributed lease backend failed: {source}")]
    Backend {
        /// The original backend cause.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

impl LeaseError {
    /// Retains an external backend cause.
    pub fn backend(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Backend {
            source: Box::new(source),
        }
    }
}

/// A positive TTL supported by native memory and Redis providers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseTtl(Duration);

impl LeaseTtl {
    /// Rejects invalid domain input before backend I/O.
    pub fn new(duration: Duration) -> std::result::Result<Self, LeaseError> {
        if duration.is_zero() || duration.as_millis() > i64::MAX as u128 {
            return Err(LeaseError::InvalidTtl);
        }
        Ok(Self(duration))
    }

    /// The configured lease lifetime, independent of cached-value TTL.
    #[must_use]
    pub const fn duration(self) -> Duration {
        self.0
    }

    /// Redis's positive integer millisecond representation, rounded upwards.
    #[must_use]
    pub fn millis(self) -> u64 {
        let millis = self.0.as_nanos().div_ceil(1_000_000).min(i64::MAX as u128);
        millis as u64
    }
}

/// An opaque nonblank ownership token. The locker never treats it as a key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LeaseToken(Arc<str>);

impl LeaseToken {
    /// Validates a token supplied by an application or provider.
    pub fn new(token: impl AsRef<str>) -> std::result::Result<Self, LeaseError> {
        if token.as_ref().trim().is_empty() {
            return Err(LeaseError::InvalidToken);
        }
        Ok(Self(Arc::from(token.as_ref())))
    }

    /// An ownership nonce generated at the orchestration boundary.
    #[must_use]
    pub fn random() -> Self {
        Self(format!("{:016x}{:016x}", fastrand::u64(..), fastrand::u64(..)).into())
    }

    /// The provider's exact token bytes.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A provider's declared ownership lifetime; nothing is inferred for legacy code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseSupport {
    /// Existing acquire/release methods make no additional lifetime declaration.
    OpaqueLegacy,
    /// Ownership remains held until explicit release, without a TTL expiry.
    HeldUntilRelease,
    /// Ownership may expire and cannot be extended.
    FixedTtl,
    /// Token-checked renewal extends ownership while work remains active.
    Renewable,
}

/// How a provider identifies an acquisition whose caller is cancelled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenAcquisition {
    /// The returned legacy token is not known before acquisition completes.
    BackendSelected,
    /// A caller's token permits safe cleanup even when its reply is lost.
    CallerSelected,
}

/// A token-checked renewal's complete expected outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RenewalOutcome {
    /// The currently held token was extended.
    Renewed,
    /// The token expired or belongs to a different owner.
    Lost,
    /// The provider has no renewal capability.
    Unsupported,
}

/// A synchronous ownership check, used by the controlled-clock reference backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnershipCheck {
    /// This exact token is still held.
    Held,
    /// Ownership is absent, expired, or belongs to a different token.
    Lost,
    /// A native synchronous check is unavailable; use the conservative deadline.
    Unknown,
}

/// Receipt anchored immediately before the successful backend attempt, so time
/// spent waiting for another owner cannot consume a newly granted lease.
#[derive(Debug, Clone, Copy)]
pub struct LeaseReceipt {
    started_at: Instant,
}

impl LeaseReceipt {
    /// Captures an injected monotonic attempt boundary.
    #[must_use]
    pub const fn new(started_at: Instant) -> Self {
        Self { started_at }
    }

    /// Conservative start of the successful request, before its response arrived.
    #[must_use]
    pub const fn started_at(self) -> Instant {
        self.started_at
    }
}

/// A lease worker whose original cleanup result must be observed.
pub type LeaseTask =
    Pin<Box<dyn Future<Output = std::result::Result<(), LeaseError>> + Send + 'static>>;
/// Owner of acquisition, uncertain-token cleanup and renewal drainage.
/// Implementations must be callable from an ordinary thread; retain a runtime
/// handle at construction/acquisition rather than relying on the dropping thread.
pub trait LeaseTaskOwner: Send + Sync {
    /// Registers work before ownership is transferred. The owner drains/errors it.
    fn supervise(&self, work: LeaseTask);
}
pub(crate) struct StandaloneLeaseOwner {
    runtime: tokio::runtime::Handle,
}
impl StandaloneLeaseOwner {
    pub(crate) fn current() -> std::result::Result<Arc<dyn LeaseTaskOwner>, LeaseError> {
        Ok(Arc::new(Self {
            runtime: tokio::runtime::Handle::try_current()
                .map_err(|_| LeaseError::MissingRuntime)?,
        }))
    }
}
impl LeaseTaskOwner for StandaloneLeaseOwner {
    fn supervise(&self, work: LeaseTask) {
        self.runtime.spawn(async move {
            if let Err(error) = work.await {
                tracing::warn!(%error,"standalone lease cleanup failed");
            }
        });
    }
}

/// A cross-node locker; legacy methods retain their public signatures.
#[async_trait]
pub trait DistributedLocker: Send + Sync {
    /// Attempts one acquisition, waiting at most `timeout` for contention.
    /// A zero timeout retains the legacy single nonblocking-attempt contract.
    async fn acquire(&self, key: &str, ttl: Duration, timeout: Timeout) -> Result<Option<String>>;

    /// Compare-releases the caller's token. An already-lost token is harmless.
    async fn release(&self, key: &str, token: &str) -> Result<()>;

    /// The explicitly supported lifetime protocol.
    fn lease_support(&self) -> LeaseSupport {
        LeaseSupport::OpaqueLegacy
    }

    /// Whether cleanup can identify an acquisition before a reply arrives.
    fn token_acquisition(&self) -> TokenAcquisition {
        TokenAcquisition::BackendSelected
    }

    /// Acquires using a known nonce. Native implementations are cancellation-safe.
    async fn acquire_with_token(
        &self,
        _key: &str,
        _token: &LeaseToken,
        _ttl: LeaseTtl,
        _timeout: Timeout,
    ) -> std::result::Result<bool, LeaseError> {
        Err(LeaseError::UnsupportedTokenAcquisition)
    }

    /// Native providers override this to capture the successful attempt boundary.
    /// The compatibility default conservatively includes the entire wait.
    async fn acquire_receipt(
        &self,
        key: &str,
        token: &LeaseToken,
        ttl: LeaseTtl,
        timeout: Timeout,
    ) -> std::result::Result<Option<LeaseReceipt>, LeaseError> {
        let started = Instant::now();
        self.acquire_with_token(key, token, ttl, timeout)
            .await
            .map(|held| held.then_some(LeaseReceipt::new(started)))
    }

    /// Additive owned-work hook. Native providers also supervise nested cleanup.
    /// The compatibility default requires the existing receipt implementation to
    /// honor its cancellation contract; it cannot account for private tasks.
    async fn acquire_receipt_supervised(
        &self,
        key: &str,
        token: &LeaseToken,
        ttl: LeaseTtl,
        timeout: Timeout,
        _owner: Arc<dyn LeaseTaskOwner>,
    ) -> std::result::Result<Option<LeaseReceipt>, LeaseError> {
        self.acquire_receipt(key, token, ttl, timeout).await
    }

    /// Extends only a currently held token; unsupported is an explicit outcome.
    async fn renew(
        &self,
        _key: &str,
        _token: &LeaseToken,
        _ttl: LeaseTtl,
    ) -> std::result::Result<RenewalOutcome, LeaseError> {
        Ok(RenewalOutcome::Unsupported)
    }

    /// Checks controlled-clock ownership without introducing network hot-path I/O.
    fn ownership_check(&self, _key: &str, _token: &LeaseToken) -> OwnershipCheck {
        OwnershipCheck::Unknown
    }

    /// Creates the proof consumed by an atomic native value mutation.
    fn lease_proof(&self, key: &str, token: &LeaseToken) -> LeaseProof {
        LeaseProof {
            key: Arc::from(key),
            token: token.clone(),
            authority: ProofAuthority::BackendAtomic,
        }
    }
}

/// A proof whose key/token are atomically checked by the value backend at commit.
#[derive(Clone)]
pub struct LeaseProof {
    key: Arc<str>,
    token: LeaseToken,
    authority: ProofAuthority,
}

#[derive(Clone)]
enum ProofAuthority {
    Memory(Arc<MemoryLeaseAuthority>),
    BackendAtomic,
}

impl std::fmt::Debug for LeaseProof {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeaseProof")
            .field("key", &self.key)
            .finish_non_exhaustive()
    }
}

impl LeaseProof {
    /// The logical ownership key (native Redis maps it into a private lease domain).
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    /// The ownership token, never a fencing claim by itself.
    #[must_use]
    pub fn token(&self) -> &LeaseToken {
        &self.token
    }

    pub(crate) fn with_memory_ownership<T>(
        &self,
        operation: impl FnOnce() -> T,
    ) -> std::result::Result<Option<T>, LeaseError> {
        match &self.authority {
            ProofAuthority::Memory(authority) => {
                Ok(authority.while_held(&self.key, &self.token, operation))
            }
            ProofAuthority::BackendAtomic => Err(LeaseError::UnsupportedFencing),
        }
    }
}

/// Finite/infinite wait accounting, using the real monotonic clock for I/O.
pub(crate) struct WaitBudget {
    deadline: Option<Instant>,
    immediate: bool,
}

impl WaitBudget {
    pub(crate) fn new(timeout: Timeout) -> std::result::Result<Self, LeaseError> {
        match timeout {
            Timeout::Infinite => Ok(Self {
                deadline: None,
                immediate: false,
            }),
            Timeout::After(duration) => Ok(Self {
                deadline: Some(
                    Instant::now()
                        .checked_add(duration)
                        .ok_or(LeaseError::InvalidDeadline)?,
                ),
                immediate: duration.is_zero(),
            }),
        }
    }

    pub(crate) fn exhausted(&self) -> bool {
        self.deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
    }
    pub(crate) fn immediate(&self) -> bool {
        self.immediate
    }
    pub(crate) fn remaining(&self) -> Option<Duration> {
        self.deadline
            .map(|deadline| deadline.saturating_duration_since(Instant::now()))
    }
    pub(crate) async fn pause(&self, interval: Duration) {
        tokio::time::sleep(
            self.remaining()
                .map_or(interval, |remaining| remaining.min(interval)),
        )
        .await;
    }
}

/// Controlled-clock reference ownership. Shared authority also fences value writes.
#[derive(Clone)]
pub struct InMemoryDistributedLocker {
    authority: Arc<MemoryLeaseAuthority>,
}

struct Held {
    token: LeaseToken,
    expires_at: Timestamp,
}

pub(crate) struct MemoryLeaseAuthority {
    locks: Mutex<HashMap<Arc<str>, Held>>,
    clock: Arc<dyn Clock>,
}

impl MemoryLeaseAuthority {
    fn try_once(&self, key: &str, token: &LeaseToken, ttl: LeaseTtl) -> bool {
        let mut locks = self
            .locks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let now = self.clock.now();
        if locks.get(key).is_some_and(|held| now < held.expires_at) {
            return false;
        }
        locks.insert(
            Arc::from(key),
            Held {
                token: token.clone(),
                expires_at: now.saturating_add(ttl.duration()),
            },
        );
        true
    }

    fn while_held<T>(
        &self,
        key: &str,
        token: &LeaseToken,
        operation: impl FnOnce() -> T,
    ) -> Option<T> {
        let locks = self
            .locks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !locks
            .get(key)
            .is_some_and(|held| &held.token == token && self.clock.now() < held.expires_at)
        {
            return None;
        }
        Some(operation())
    }
}

impl InMemoryDistributedLocker {
    /// Shares one authority across reference nodes, using injected domain expiry time.
    #[must_use]
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        Self {
            authority: Arc::new(MemoryLeaseAuthority {
                locks: Mutex::new(HashMap::new()),
                clock,
            }),
        }
    }

    /// Current physically held token count, including unobserved lazy expiry.
    #[must_use]
    pub fn held_count(&self) -> usize {
        let mut locks = self
            .authority
            .locks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let now = self.authority.clock.now();
        locks.retain(|_, held| now < held.expires_at);
        locks.len()
    }
}

#[async_trait]
impl DistributedLocker for InMemoryDistributedLocker {
    async fn acquire(&self, key: &str, ttl: Duration, timeout: Timeout) -> Result<Option<String>> {
        let token = LeaseToken::random();
        let acquired = self
            .acquire_with_token(key, &token, LeaseTtl::new(ttl)?, timeout)
            .await?;
        Ok(acquired.then(|| token.as_str().to_owned()))
    }

    async fn release(&self, key: &str, token: &str) -> Result<()> {
        let mut locks = self
            .authority
            .locks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if locks
            .get(key)
            .is_some_and(|held| held.token.as_str() == token)
        {
            locks.remove(key);
        }
        Ok(())
    }

    fn lease_support(&self) -> LeaseSupport {
        LeaseSupport::Renewable
    }
    fn token_acquisition(&self) -> TokenAcquisition {
        TokenAcquisition::CallerSelected
    }

    async fn acquire_with_token(
        &self,
        key: &str,
        token: &LeaseToken,
        ttl: LeaseTtl,
        timeout: Timeout,
    ) -> std::result::Result<bool, LeaseError> {
        self.acquire_receipt(key, token, ttl, timeout)
            .await
            .map(|receipt| receipt.is_some())
    }

    async fn acquire_receipt(
        &self,
        key: &str,
        token: &LeaseToken,
        ttl: LeaseTtl,
        timeout: Timeout,
    ) -> std::result::Result<Option<LeaseReceipt>, LeaseError> {
        let budget = WaitBudget::new(timeout)?;
        let mut first = true;
        loop {
            if budget.exhausted() && !(first && budget.immediate()) {
                return Ok(None);
            }
            let started = Instant::now();
            if self.authority.try_once(key, token, ttl) {
                return Ok(Some(LeaseReceipt::new(started)));
            }
            first = false;
            if budget.exhausted() {
                return Ok(None);
            }
            budget.pause(Duration::from_millis(10)).await;
        }
    }

    async fn renew(
        &self,
        key: &str,
        token: &LeaseToken,
        ttl: LeaseTtl,
    ) -> std::result::Result<RenewalOutcome, LeaseError> {
        let mut locks = self
            .authority
            .locks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let now = self.authority.clock.now();
        match locks.get_mut(key) {
            Some(held) if &held.token == token && now < held.expires_at => {
                held.expires_at = now.saturating_add(ttl.duration());
                Ok(RenewalOutcome::Renewed)
            }
            Some(_) | None => Ok(RenewalOutcome::Lost),
        }
    }

    fn ownership_check(&self, key: &str, token: &LeaseToken) -> OwnershipCheck {
        if self.authority.while_held(key, token, || ()).is_some() {
            OwnershipCheck::Held
        } else {
            OwnershipCheck::Lost
        }
    }

    fn lease_proof(&self, key: &str, token: &LeaseToken) -> LeaseProof {
        LeaseProof {
            key: Arc::from(key),
            token: token.clone(),
            authority: ProofAuthority::Memory(Arc::clone(&self.authority)),
        }
    }
}

/// Cancellation guarantees requested by canonical owned acquisition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcquisitionPolicy {
    /// Require a preselected token so uncertain acquisition can be cleaned up.
    TokenOwned,
    /// Explicitly trust a legacy provider to honor its acquire timeout contract.
    LegacyBackendContract,
}

/// Live ownership state. A lost lease cannot silently become valid again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseState {
    /// Ownership has an acknowledged token and a conservative local deadline.
    Held,
    /// Ownership was lost or renewal failed; commits must be fenced out.
    Lost,
    /// Explicit release/drop has ended this ownership scope.
    Released,
}

struct LeaseInner {
    locker: Arc<dyn DistributedLocker>,
    key: Arc<str>,
    token: LeaseToken,
    ttl: LeaseTtl,
    state: watch::Sender<LeaseState>,
    valid_until: Mutex<Option<Instant>>,
    renewal: Mutex<Option<tokio::task::JoinHandle<()>>>,
    owner: Arc<dyn LeaseTaskOwner>,
}

/// Owned acquisition handle. Drop initiates bounded compare-release cleanup.
pub struct DistributedLease {
    inner: Option<Arc<LeaseInner>>,
}

impl DistributedLease {
    fn new(
        locker: Arc<dyn DistributedLocker>,
        key: Arc<str>,
        token: LeaseToken,
        ttl: LeaseTtl,
        started: Instant,
        owner: Arc<dyn LeaseTaskOwner>,
    ) -> Self {
        let (state, _) = watch::channel(LeaseState::Held);
        let valid_until = match locker.lease_support() {
            LeaseSupport::HeldUntilRelease | LeaseSupport::OpaqueLegacy => None,
            LeaseSupport::FixedTtl | LeaseSupport::Renewable => started.checked_add(ttl.duration()),
        };
        let inner = Arc::new(LeaseInner {
            locker,
            key,
            token,
            ttl,
            state,
            valid_until: Mutex::new(valid_until),
            renewal: Mutex::new(None),
            owner,
        });
        if inner.locker.lease_support() == LeaseSupport::Renewable {
            let weak = Arc::downgrade(&inner);
            let interval = (ttl.duration() / 3).max(Duration::from_millis(1));
            let handle = tokio::spawn(async move {
                loop {
                    tokio::time::sleep(interval).await;
                    let Some(inner) = weak.upgrade() else {
                        return;
                    };
                    if *inner.state.borrow() != LeaseState::Held {
                        return;
                    }
                    if !ownership_is_locally_valid(&inner) {
                        inner.state.send_replace(LeaseState::Lost);
                        return;
                    }
                    let started = Instant::now();
                    let renewal = tokio::time::timeout(
                        inner.ttl.duration(),
                        inner.locker.renew(&inner.key, &inner.token, inner.ttl),
                    )
                    .await;
                    match renewal {
                        Ok(Ok(RenewalOutcome::Renewed)) => {
                            if *inner.state.borrow() != LeaseState::Held
                                || !ownership_is_locally_valid(&inner)
                            {
                                inner.state.send_replace(LeaseState::Lost);
                                return;
                            }
                            *inner
                                .valid_until
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner) =
                                started.checked_add(inner.ttl.duration());
                        }
                        Ok(Ok(RenewalOutcome::Lost | RenewalOutcome::Unsupported))
                        | Ok(Err(_))
                        | Err(_) => {
                            inner.state.send_replace(LeaseState::Lost);
                            return;
                        }
                    }
                }
            });
            *inner
                .renewal
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(handle);
        }
        Self { inner: Some(inner) }
    }

    /// A retained ownership-state receiver for cancellation context integration.
    #[must_use]
    pub fn state(&self) -> watch::Receiver<LeaseState> {
        self.inner.as_ref().map_or_else(
            || watch::channel(LeaseState::Released).1,
            |inner| inner.state.subscribe(),
        )
    }

    /// Checks current local validity; the value backend still atomically verifies it.
    pub fn proof(&self) -> std::result::Result<LeaseProof, LeaseError> {
        let Some(inner) = &self.inner else {
            return Err(LeaseError::Lost);
        };
        if *inner.state.borrow() != LeaseState::Held {
            return Err(LeaseError::Lost);
        }
        if inner.locker.lease_support() == LeaseSupport::OpaqueLegacy {
            return Err(LeaseError::OpaqueLifetime);
        }
        let valid = ownership_is_locally_valid(inner);
        if !valid {
            inner.state.send_replace(LeaseState::Lost);
            return Err(LeaseError::Lost);
        }
        Ok(inner.locker.lease_proof(&inner.key, &inner.token))
    }

    /// Deterministically stops renewal and awaits bounded compare-release.
    pub async fn release(mut self) -> std::result::Result<(), LeaseError> {
        let Some(inner) = self.inner.take() else {
            return Ok(());
        };
        let mut cleanup = LeaseCleanup::new(inner);
        let result = cleanup.run().await;
        cleanup.disarm();
        result
    }
}

fn ownership_is_locally_valid(inner: &LeaseInner) -> bool {
    match inner.locker.ownership_check(&inner.key, &inner.token) {
        OwnershipCheck::Held => true,
        OwnershipCheck::Lost => false,
        OwnershipCheck::Unknown => match inner.locker.lease_support() {
            LeaseSupport::HeldUntilRelease => true,
            LeaseSupport::OpaqueLegacy => false,
            LeaseSupport::FixedTtl | LeaseSupport::Renewable => inner
                .valid_until
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_some_and(|deadline| Instant::now() < deadline),
        },
    }
}

fn stop_renewal(inner: &LeaseInner) -> Option<tokio::task::JoinHandle<()>> {
    inner.state.send_replace(LeaseState::Released);
    let handle = inner
        .renewal
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take();
    if let Some(handle) = &handle {
        handle.abort();
    }
    handle
}
struct LeaseCleanup {
    inner: Option<Arc<LeaseInner>>,
    renewal: Option<tokio::task::JoinHandle<()>>,
}
impl LeaseCleanup {
    fn new(inner: Arc<LeaseInner>) -> Self {
        let renewal = stop_renewal(&inner);
        Self {
            inner: Some(inner),
            renewal,
        }
    }
    async fn run(&mut self) -> std::result::Result<(), LeaseError> {
        let Some(inner) = &self.inner else {
            return Ok(());
        };
        if let Some(handle) = &mut self.renewal
            && let Err(source) = handle.await
            && !source.is_cancelled()
        {
            inner
                .owner
                .supervise(Box::pin(async move { Err(LeaseError::Task { source }) }));
        }
        self.renewal.take();
        bounded_release(inner.locker.as_ref(), &inner.key, inner.token.as_str()).await
    }
    fn disarm(&mut self) {
        self.inner.take();
        self.renewal.take();
    }
}
impl Drop for LeaseCleanup {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            let owner = Arc::clone(&inner.owner);
            let renewal = self.renewal.take();
            owner.supervise(Box::pin(async move {
                let mut cleanup = LeaseCleanup {
                    inner: Some(inner),
                    renewal,
                };
                let result = cleanup.run().await;
                cleanup.disarm();
                result
            }));
        }
    }
}

pub(crate) async fn bounded_release(
    locker: &dyn DistributedLocker,
    key: &str,
    token: &str,
) -> std::result::Result<(), LeaseError> {
    tokio::time::timeout(Duration::from_secs(2), locker.release(key, token))
        .await
        .map_err(|_| LeaseError::CleanupTimeout)?
        .map_err(LeaseError::backend)
}

impl Drop for DistributedLease {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            drop(LeaseCleanup::new(inner));
        }
    }
}

/// Canonical token-owned acquisition. Caller cancellation never drops a native
/// acquire reply without arranging compare-release of any obtained ownership.
pub async fn acquire_owned(
    locker: Arc<dyn DistributedLocker>,
    key: Arc<str>,
    ttl: LeaseTtl,
    timeout: Timeout,
    policy: AcquisitionPolicy,
) -> std::result::Result<Option<DistributedLease>, LeaseError> {
    acquire_owned_supervised(
        locker,
        key,
        ttl,
        timeout,
        policy,
        StandaloneLeaseOwner::current()?,
    )
    .await
}

/// Token-owned acquisition registered with an owning cache/application supervisor.
/// Every library worker, failed transfer and native uncertain-token cleanup is
/// drained by `owner`. LegacyBackendContract still relies on the external
/// backend finishing its opaque-token acquire reply; it cannot bound a provider
/// that violates that explicitly weaker contract.
pub async fn acquire_owned_supervised(
    locker: Arc<dyn DistributedLocker>,
    key: Arc<str>,
    ttl: LeaseTtl,
    timeout: Timeout,
    policy: AcquisitionPolicy,
    owner: Arc<dyn LeaseTaskOwner>,
) -> std::result::Result<Option<DistributedLease>, LeaseError> {
    if tokio::runtime::Handle::try_current().is_err() {
        return Err(LeaseError::MissingRuntime);
    }
    if matches!(
        locker.lease_support(),
        LeaseSupport::FixedTtl | LeaseSupport::Renewable
    ) && Instant::now().checked_add(ttl.duration()).is_none()
    {
        return Err(LeaseError::InvalidDeadline);
    }
    if policy == AcquisitionPolicy::TokenOwned
        && locker.token_acquisition() != TokenAcquisition::CallerSelected
    {
        return Err(LeaseError::UnsupportedTokenAcquisition);
    }
    let budget = WaitBudget::new(timeout)?;
    let (mut sender, receiver) = oneshot::channel();
    let worker_owner = Arc::clone(&owner);
    owner.supervise(Box::pin(async move {
        let result=match locker.token_acquisition() {
            TokenAcquisition::CallerSelected=>{
                let token=LeaseToken::random();
                let acquired=tokio::select! {biased;
                    _=sender.closed()=>{return bounded_release(locker.as_ref(),&key,token.as_str()).await;},
                    acquired=locker.acquire_receipt_supervised(&key,&token,ttl,timeout,Arc::clone(&worker_owner))=>acquired,
                };
                if acquired.is_err() {
                    let cleanup_locker=Arc::clone(&locker);
                    let cleanup_key=Arc::clone(&key);
                    let cleanup_token=token.clone();
                    worker_owner.supervise(Box::pin(async move {
                        bounded_release(cleanup_locker.as_ref(),&cleanup_key,cleanup_token.as_str()).await
                    }));
                }
                acquired.map(|receipt|receipt.map(|receipt|DistributedLease::new(locker,key,token,ttl,receipt.started_at(),worker_owner)))
            },
            TokenAcquisition::BackendSelected=>{
                let started=Instant::now();
                locker.acquire(&key,ttl.duration(),timeout).await.map_err(LeaseError::backend).and_then(|token|token.map(LeaseToken::new).transpose()).map(|token|token.map(|token|DistributedLease::new(locker,key,token,ttl,started,worker_owner)))
            }
        };
        match sender.send(result) {
            Ok(())=>Ok(()),
            Err(Ok(Some(lease)))=>lease.release().await,
            Err(Ok(None))=>Ok(()),
            Err(Err(error))=>Err(error),
        }
    }));
    match budget.remaining() {
        Some(remaining) if !budget.immediate() => tokio::time::timeout(remaining, receiver)
            .await
            .map_err(|_| LeaseError::AcquisitionTimeout)?
            .map_err(|_| LeaseError::Cancelled)?,
        Some(_) | None => receiver.await.map_err(|_| LeaseError::Cancelled)?,
    }
}
