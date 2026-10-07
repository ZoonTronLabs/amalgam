//! Build-selected plain L1 execution, without the optional observation envelope.
use super::ready::RuntimeRequirement;
use super::{
    Cache, EntryOptions, Error, FactoryCancellation, InlinePermit, LookupMode, QuietObservation,
    Result, Storage, TagVerdict,
};
use crate::events::{
    CacheEvent, CacheLevel, CacheOperation, LayerEvent, MemoryEvent, OperationOutcome,
};
use crate::memory::CacheMemory;
use std::sync::Arc;

// Exactly two ready strategies: the general counted path and the
// build-proven primitive path. Neither depends on the target architecture.
#[derive(Clone, Copy)]
pub(super) enum ReadyPlan {
    General,
    CallbackFree,
}
impl ReadyPlan {
    pub(super) fn select<V: Clone + Send + Sync + 'static>(
        storage: &Storage<V>,
        memory: &CacheMemory<V>,
        options: &EntryOptions,
        runtime: RuntimeRequirement,
        clock: &crate::time::local::CacheClock,
    ) -> Self {
        if matches!(storage, Storage::MemoryOnly)
            && matches!(runtime, RuntimeRequirement::Inline)
            && !options.enable_auto_clone()
            && !options.skip_memory_read()
            && super::callback_free::primitive::<V>()
            && let (CacheMemory::Builtin(memory), crate::time::local::CacheClock::Local(_)) =
                (memory, clock)
            && memory.has_reader_slots()
        {
            Self::CallbackFree
        } else {
            Self::General
        }
    }
}

pub(super) enum QuietStart<'a, V> {
    General,
    Complete(Result<V>),
    Ready(QuietReady<'a, V>),
    Owned {
        observation: QuietObservation<'a>,
        permit: InlinePermit<'a>,
    },
    // A stored entry can opt into eager refresh even when defaults are plain.
    Recheck {
        observation: QuietObservation<'a>,
        permit: InlinePermit<'a>,
    },
}
pub(super) enum QuietCopy<V> {
    Value(V),
    EntryPolicy,
}
pub(super) struct QuietReady<'a, V> {
    pub(super) value: Result<V>,
    pub(super) observation: QuietObservation<'a>,
    pub(super) permit: InlinePermit<'a>,
}
impl<V: Clone + Send + Sync + 'static> Cache<V> {
    pub(super) fn quiet_lookup<'a>(
        &'a self,
        key: &str,
        options: Option<&EntryOptions>,
        token: Option<&FactoryCancellation>,
        operation: CacheOperation,
        mode: LookupMode,
        inputs: super::callback_free::Inputs,
    ) -> QuietStart<'a, V> {
        if matches!(self.inner.ready_plan, ReadyPlan::General)
            || options.is_some()
            || !self.inner.events.is_quiet()
            || tracing::level_filters::LevelFilter::current() >= tracing::Level::DEBUG
        {
            return QuietStart::General;
        }
        if matches!(self.inner.ready_plan, ReadyPlan::CallbackFree)
            && matches!(inputs, super::callback_free::Inputs::NoCallbacks)
            && token.is_none()
        {
            return self.callback_free_lookup(key, operation, mode);
        }
        QuietStart::General
    }
    pub(super) fn quiet_copy(
        &self,
        entry: &super::Entry<V>,
        freshness: crate::entry::Freshness,
        mode: LookupMode,
    ) -> Option<QuietCopy<V>> {
        if self.inner.tags(entry) != TagVerdict::Valid || !freshness.is_fresh() {
            return None;
        }
        Some(
            if matches!(mode, LookupMode::GetOrSet) && entry.meta().eager_refresh_at().is_some() {
                QuietCopy::EntryPolicy
            } else {
                QuietCopy::Value(entry.value().clone())
            },
        )
    }
}
impl<V> QuietReady<'_, V> {
    pub(super) fn finish<T>(
        self,
        key: &str,
        token: Option<&FactoryCancellation>,
        complete: impl FnOnce(V) -> T,
    ) -> Result<T> {
        let Self {
            value,
            observation,
            permit,
        } = self;
        let events = observation.events();
        let result = match value {
            Ok(value) => {
                events.emit_layer_lazy(|| {
                    LayerEvent::Memory(MemoryEvent::Hit {
                        key: Arc::from(key),
                        stale: false,
                    })
                });
                permit.status(token).map(|()| complete(value))
            }
            Err(error) => {
                drop(complete);
                Err(error)
            }
        };
        let result = match result {
            Err(Error::CacheClosed) => Err(Error::CacheClosed),
            result => permit.status(token).and(result),
        }
        .and_then(|value| {
            events.emit_lazy(|| CacheEvent::Hit {
                key: Arc::from(key),
                stale: false,
            });
            permit.status(token)?;
            Ok(value)
        });
        let (outcome, level) = match &result {
            Ok(_) => (OperationOutcome::Hit, Some(CacheLevel::Memory)),
            Err(error) => (OperationOutcome::from_error(error), None),
        };
        observation.finish(outcome, level);
        result
    }
}
