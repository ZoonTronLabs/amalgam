//! Actual typed observation storage and qualified authority namespaces.
use super::{Entry, MarkerKind, MarkerObservation};
use crate::memory::{CacheMemory, MemoryInvalidation};
use crate::{
    Clock, Error, Events, EvictionCapture, MemoryAdmission, MemoryExpiry, MemoryLimits,
    MemoryNamespace, MemoryStorage, MemoryStorageError, MemoryUsage, Result, Timestamp,
};
use std::sync::Arc;

pub(crate) enum MarkerMemoryNamespace {
    Local(Arc<str>),
    Durable(crate::CacheScope),
}
impl MarkerMemoryNamespace {
    fn storage_namespace(&self) -> MemoryNamespace {
        match self {
            Self::Local(prefix) => MemoryNamespace::local_markers(Arc::clone(prefix)),
            Self::Durable(scope) => MemoryNamespace::durable_markers(scope.clone()),
        }
    }
    fn key(&self, kind: &MarkerKind) -> Arc<str> {
        let kind = super::MarkerObservations::key(kind);
        let (authority, scope) = match self {
            Self::Local(prefix) => ("local", prefix.to_string()),
            Self::Durable(scope) => ("durable", scope.storage_id()),
        };
        Arc::from(format!(
            "\x1famalgam/marker-memory/{authority}/{}/{}",
            crate::backplane::encode_hex(scope.as_bytes()),
            crate::backplane::encode_hex(kind.as_bytes())
        ))
    }
}
pub(crate) struct MarkerMemory {
    store: CacheMemory<MarkerObservation>,
    namespace: MarkerMemoryNamespace,
}
impl MarkerMemory {
    pub(crate) fn new(
        provider: Option<Arc<dyn MemoryStorage<MarkerObservation>>>,
        limits: MemoryLimits,
        clock: Arc<dyn Clock>,
        expiry: MemoryExpiry,
        namespace: MarkerMemoryNamespace,
    ) -> Result<Self> {
        let store = CacheMemory::new_for_namespace(
            provider,
            limits,
            Events::with_capacity(16),
            clock,
            expiry,
            EvictionCapture::AtInsertion,
            namespace.storage_namespace(),
        )
        .map_err(|error| match error {
            Error::MemoryStorage(source) => Error::MarkerMemoryStorage(source),
            error => error,
        })?;
        Ok(Self { store, namespace })
    }
    pub(crate) fn key(&self, kind: &MarkerKind) -> Arc<str> {
        self.namespace.key(kind)
    }
    pub(crate) fn is_supplied(&self) -> bool {
        self.store.provider().is_some()
    }
    pub(crate) fn is_local_supplied(&self) -> bool {
        self.is_supplied() && matches!(self.namespace, MarkerMemoryNamespace::Local(_))
    }
    pub(crate) fn provider(&self) -> Option<&Arc<dyn MemoryStorage<MarkerObservation>>> {
        self.store.provider()
    }
    pub(crate) async fn get_at(
        &self,
        key: &str,
        now: Timestamp,
    ) -> Result<Option<Entry<MarkerObservation>>> {
        self.store
            .get_at(key, now)
            .await
            .map_err(Error::MarkerMemoryStorage)
    }
    pub(crate) fn ready_at(
        &self,
        key: &str,
        now: Timestamp,
    ) -> Result<Option<Entry<MarkerObservation>>> {
        self.store
            .ready_at(key, now)
            .map_err(Error::MarkerMemoryStorage)
    }
    pub(crate) async fn insert_if_unchanged(
        &self,
        key: Arc<str>,
        expected: Option<&Entry<MarkerObservation>>,
        entry: Entry<MarkerObservation>,
        now: Timestamp,
    ) -> Result<MemoryAdmission> {
        self.store
            .insert_if_unchanged(key, expected, entry, now)
            .await
            .map_err(Error::MarkerMemoryStorage)
    }
    pub(crate) fn begin_invalidation(
        &self,
    ) -> std::result::Result<MemoryInvalidation<'_, MarkerObservation>, MemoryStorageError> {
        self.store.begin_invalidation()
    }
    pub(crate) async fn run_pending_tasks(&self) -> Result<()> {
        self.store
            .run_pending_tasks()
            .await
            .map_err(Error::MarkerMemoryStorage)
    }
    pub(crate) fn usage(&self) -> Result<MemoryUsage> {
        self.store.usage().map_err(Error::MarkerMemoryStorage)
    }
}
