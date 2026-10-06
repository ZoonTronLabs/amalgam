//! Build-selected plain L1 execution, without the optional observation envelope.
use super::ready::RuntimeRequirement;
use super::{
    Cache, EntryOptions, Error, FactoryCancellation, InlinePermit, QuietObservation, Result,
    Storage, TagVerdict,
};
use crate::events::{
    CacheEvent, CacheLevel, CacheOperation, LayerEvent, MemoryEvent, OperationOutcome,
};
use crate::memory::CacheMemory;
use std::sync::Arc;

#[derive(Clone, Copy)]
pub(super) enum ReadyPlan {
    Plain,
    General,
}
impl ReadyPlan {
    pub(super) fn select<V: Clone + Send + Sync + 'static>(
        storage: &Storage<V>,
        memory: &CacheMemory<V>,
        options: &EntryOptions,
        runtime: RuntimeRequirement,
    ) -> Self {
        if matches!(storage, Storage::MemoryOnly)
            && matches!(memory, CacheMemory::Builtin(_))
            && matches!(runtime, RuntimeRequirement::Inline)
            && !options.enable_auto_clone()
            && !options.skip_memory_read()
        {
            Self::Plain
        } else {
            Self::General
        }
    }
}

pub(super) enum QuietStart<'a, V> {
    General,
    Ready(QuietReady<'a, V>),
    Owned {
        observation: QuietObservation<'a>,
        permit: InlinePermit<'a>,
    },
}
pub(super) struct QuietReady<'a, V> {
    value: Result<V>,
    observation: QuietObservation<'a>,
    permit: InlinePermit<'a>,
}
impl<V: Clone + Send + Sync + 'static> Cache<V> {
    pub(super) fn quiet_lookup<'a>(
        &'a self,
        key: &str,
        options: Option<&EntryOptions>,
        token: Option<&FactoryCancellation>,
        operation: CacheOperation,
    ) -> QuietStart<'a, V> {
        if !matches!(self.inner.ready_plan, ReadyPlan::Plain)
            || options.is_some()
            || !self.inner.events.is_quiet()
            || tracing::level_filters::LevelFilter::current() >= tracing::Level::DEBUG
        {
            return QuietStart::General;
        }
        let permit = self.inline();
        let observation = QuietObservation::new(&self.inner.events, operation);
        let value = (|| {
            permit.admit()?;
            permit.status(token)?;
            // The injected clock is caller code: it runs outside the slot and
            // inside the same counted admission as the eventual value Clone.
            let now = self.inner.clock.now();
            permit.status(token)?;
            let CacheMemory::Builtin(memory) = &self.inner.memory else {
                unreachable!("Plain plan requires built-in L1");
            };
            let copied = memory
                .with_ready(key, now, |entry| {
                    (self.inner.tags(entry) == TagVerdict::Valid && entry.freshness(now).is_fresh())
                        .then(|| entry.value().clone())
                })
                .flatten();
            permit.status(token)?;
            Ok(copied)
        })();
        match value {
            Ok(Some(value)) => QuietStart::Ready(QuietReady {
                value: Ok(value),
                observation,
                permit,
            }),
            Err(error) => QuietStart::Ready(QuietReady {
                value: Err(error),
                observation,
                permit,
            }),
            Ok(None) => QuietStart::Owned {
                observation,
                permit,
            },
        }
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
