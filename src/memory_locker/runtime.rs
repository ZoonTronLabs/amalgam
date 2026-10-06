//! Cache-owned acquisition budgets, cancellation and provider selection.
use super::*;
use crate::events::Events;
use crate::lifecycle::Tasks;
use crate::{Error, Result};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

#[allow(dead_code, reason = "variants own RAII guards released on enum drop")]
pub(crate) enum LocalGuard {
    Builtin(KeyGuard),
    Custom(MemoryLock),
}
pub(crate) struct EagerLock {
    pub(crate) guard: LocalGuard,
    pub(crate) source: CancellationSource,
}
pub(crate) enum LocalLocks {
    Builtin(Box<KeyedLock>),
    Custom(Arc<ConfiguredLocker>),
}
pub(crate) struct ConfiguredLocker {
    provider: Arc<dyn MemoryLocker>,
    context: MemoryLockerContext,
    shutdown_started: AtomicBool,
}
impl LocalLocks {
    pub(crate) fn new(
        provider: Option<Arc<dyn MemoryLocker>>,
        name: Arc<str>,
        instance_id: Arc<str>,
        shards: usize,
    ) -> Self {
        match provider {
            Some(provider) => Self::Custom(Arc::new(ConfiguredLocker {
                provider,
                context: MemoryLockerContext { name, instance_id },
                shutdown_started: AtomicBool::new(false),
            })),
            None => Self::Builtin(Box::new(KeyedLock::new(shards))),
        }
    }
    pub(crate) fn for_markers(&self) -> Self {
        match self {
            Self::Builtin(_) => Self::Builtin(Box::new(KeyedLock::new(64))),
            Self::Custom(locker) => Self::Custom(Arc::clone(locker)),
        }
    }
    pub(crate) fn clean_idle(&self, budget: usize) {
        if let Self::Builtin(locker) = self {
            locker.clean_idle(budget);
        }
    }
    pub(crate) async fn acquire(
        &self,
        key: &Arc<str>,
        kind: MemoryLockKind,
        timeout: Timeout,
        parent: &FactoryCancellation,
    ) -> Result<Option<LocalGuard>> {
        parent.check()?;
        match self {
            Self::Builtin(locker) => Ok(crate::cache::bounded(timeout, locker.lock(key))
                .await?
                .map(LocalGuard::Builtin)),
            Self::Custom(locker) => Box::pin(locker.acquire(key, kind, timeout, parent)).await,
        }
    }
    pub(crate) fn try_acquire(
        &self,
        key: &Arc<str>,
        kind: MemoryLockKind,
        parent: &FactoryCancellation,
    ) -> Result<Option<LocalGuard>> {
        parent.check()?;
        match self {
            Self::Builtin(locker) => Ok(locker.try_lock(key).map(LocalGuard::Builtin)),
            Self::Custom(locker) => {
                let wait = WaitScope::new(parent, Reason::ScopeFinished);
                let request = locker.request(
                    key,
                    kind,
                    Timeout::After(std::time::Duration::ZERO),
                    wait.source.token(),
                );
                let result = locker.provider.try_acquire(request);
                wait.source.cancel_with(Reason::ScopeFinished);
                parent.check()?;
                custom_result(result)
            }
        }
    }
    pub(crate) fn try_eager(&self, key: &Arc<str>) -> Result<Option<EagerLock>> {
        // Allocate a factory scope only after the standard nonblocking claim.
        match self {
            Self::Builtin(locker) => Ok(locker.try_lock(key).map(|guard| EagerLock {
                guard: LocalGuard::Builtin(guard),
                source: CancellationSource::new(),
            })),
            Self::Custom(_) => {
                let source = CancellationSource::new();
                Ok(self
                    .try_acquire(key, MemoryLockKind::Entry, &source.token())?
                    .map(|guard| EagerLock { guard, source }))
            }
        }
    }
    pub(crate) fn shutdown(&self, tasks: &Arc<Tasks>, events: &Events) {
        if let Self::Custom(locker) = self {
            if locker.shutdown_started.swap(true, Ordering::AcqRel) {
                return;
            }
            let locker = Arc::clone(locker);
            tasks.cleanup_task(
                crate::ShutdownTask::MemoryLocker,
                Arc::from("memory-locker:shutdown"),
                events.clone(),
                async move {
                    locker
                        .provider
                        .shutdown(locker.context.clone())
                        .await
                        .map_err(Error::from)
                },
            );
        }
    }
}
impl ConfiguredLocker {
    fn request(
        &self,
        key: &Arc<str>,
        kind: MemoryLockKind,
        timeout: Timeout,
        cancellation: FactoryCancellation,
    ) -> MemoryLockRequest {
        let namespace = match &kind {
            MemoryLockKind::Entry => "entry",
            MemoryLockKind::Marker(_) => "marker",
        };
        MemoryLockRequest {
            context: self.context.clone(),
            key: Arc::clone(key),
            coordination_key: Arc::from(format!("{namespace}:{key}")),
            kind,
            timeout,
            cancellation,
        }
    }
    async fn acquire(
        &self,
        key: &Arc<str>,
        kind: MemoryLockKind,
        timeout: Timeout,
        parent: &FactoryCancellation,
    ) -> Result<Option<LocalGuard>> {
        crate::cache::validate_budget(timeout)?;
        if timeout == Timeout::After(std::time::Duration::ZERO) {
            return Ok(None);
        }
        let dropped = match timeout {
            Timeout::Infinite => Reason::ScopeFinished,
            Timeout::After(_) => Reason::HardTimeout,
        };
        let scope = WaitScope::new(parent, dropped);
        let request = self.request(key, kind, timeout, scope.source.token());
        let wait = GuardedWait {
            work: self.provider.acquire(request),
            scope,
        };
        let result = tokio::select! {
            biased;
            reason = parent.cancelled() => return Err(Error::OperationCancelled { reason }),
            result = crate::cache::bounded(timeout, wait) => result?,
        };
        parent.check()?;
        match result {
            Some(result) => custom_result(result),
            None => Ok(None),
        }
    }
}
fn custom_result(
    result: std::result::Result<MemoryLockOutcome, MemoryLockerError>,
) -> Result<Option<LocalGuard>> {
    match result {
        Ok(MemoryLockOutcome::Acquired(guard)) => Ok(Some(LocalGuard::Custom(guard))),
        Ok(MemoryLockOutcome::Unavailable) => Ok(None),
        Err(MemoryLockerError::Cancelled { reason }) => Err(Error::OperationCancelled { reason }),
        Err(error @ MemoryLockerError::Provider { .. }) => Err(error.into()),
    }
}
struct WaitScope {
    source: CancellationSource,
    parent: FactoryCancellation,
    dropped: Reason,
}
impl WaitScope {
    fn new(parent: &FactoryCancellation, dropped: Reason) -> Self {
        Self {
            source: CancellationSource::new(),
            parent: parent.clone(),
            dropped,
        }
    }
}
impl Drop for WaitScope {
    fn drop(&mut self) {
        self.source
            .cancel_with(self.parent.reason().unwrap_or(self.dropped));
    }
}
struct GuardedWait<F> {
    // Drop signals the acquisition scope before this owned provider future.
    work: F,
    scope: WaitScope,
}
impl<T> Future for GuardedWait<Pin<Box<dyn Future<Output = T> + Send + '_>>> {
    type Output = T;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let result = self.work.as_mut().poll(cx);
        if result.is_ready() {
            self.scope.source.cancel_with(Reason::ScopeFinished);
        }
        result
    }
}
impl<F> Drop for GuardedWait<F> {
    fn drop(&mut self) {
        self.scope
            .source
            .cancel_with(self.scope.parent.reason().unwrap_or(self.scope.dropped));
    }
}
