//! Construction-only advice; cache configuration is immutable after build.
use super::{CacheInner, MarkerAccess, Storage};
use crate::memory::CacheMemory;

enum Advice {
    SharedNameWithoutPrefix,
    BackplaneWithoutDistributed,
    LockerWithoutDistributed,
    OrdinaryMarkerWrites,
}
impl Advice {
    fn emit(self, name: &str) {
        match self {
            Self::SharedNameWithoutPrefix => tracing::warn!(
                cache = name,
                "named shared cache has no key prefix; cache names do not isolate stored keys"
            ),
            Self::BackplaneWithoutDistributed => tracing::warn!(
                cache = name,
                "backplane has no L2; peers invalidate values and recompute from their own source"
            ),
            Self::LockerWithoutDistributed => tracing::warn!(
                cache = name,
                "distributed locker has no L2; peers cannot reuse the completed value"
            ),
            Self::OrdinaryMarkerWrites => tracing::warn!(
                cache = name,
                "L2 has no atomic invalidation store; ordinary tag/clear writes can regress under concurrent or delayed writes and expire with their TTL"
            ),
        }
    }
}
impl<V: Clone + Send + Sync + 'static> CacheInner<V> {
    pub(super) fn advise_configuration(&self) {
        let distributed = matches!(self.storage, Storage::Hybrid { .. });
        if self.name.as_ref() != "amalgam"
            && self.key_prefix.as_deref().is_none_or(str::is_empty)
            && (distributed
                || matches!(self.memory, CacheMemory::Supplied(_))
                || self.distributed_locker.is_some())
        {
            Advice::SharedNameWithoutPrefix.emit(&self.name);
        }
        if !distributed
            && self.backplane.is_some()
            && !self.default_options.skip_backplane_notifications()
        {
            Advice::BackplaneWithoutDistributed.emit(&self.name);
        }
        if !distributed && self.distributed_locker.is_some() {
            Advice::LockerWithoutDistributed.emit(&self.name);
        }
        if !self.disable_tagging && matches!(self.markers, MarkerAccess::Ordinary(_)) {
            Advice::OrdinaryMarkerWrites.emit(&self.name);
        }
    }
}
