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

#[derive(Clone, Copy)]
pub(super) enum ReadyPlan {
    Plain,
    #[cfg(target_arch = "x86_64")]
    LocalSlots,
    General,
}
impl ReadyPlan {
    pub(super) fn select<V: Clone + Send + Sync + 'static>(
        storage: &Storage<V>,
        memory: &CacheMemory<V>,
        options: &EntryOptions,
        runtime: RuntimeRequirement,
        #[cfg(target_arch = "x86_64")] clock: &crate::time::local::CacheClock,
    ) -> Self {
        if matches!(storage, Storage::MemoryOnly)
            && matches!(memory, CacheMemory::Builtin(_))
            && matches!(runtime, RuntimeRequirement::Inline)
            && !options.enable_auto_clone()
            && !options.skip_memory_read()
        {
            #[cfg(target_arch = "x86_64")]
            if let (CacheMemory::Builtin(memory), crate::time::local::CacheClock::Local(_)) =
                (memory, clock)
                && memory.has_reader_slots()
            {
                return Self::LocalSlots;
            }
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
    // A stored entry can opt into eager refresh even when defaults are plain.
    Recheck {
        observation: QuietObservation<'a>,
        permit: InlinePermit<'a>,
    },
}
enum QuietCopy<V> {
    Value(V),
    EntryPolicy,
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
        mode: LookupMode,
    ) -> QuietStart<'a, V> {
        if matches!(self.inner.ready_plan, ReadyPlan::General)
            || options.is_some()
            || !self.inner.events.is_quiet()
            || tracing::level_filters::LevelFilter::current() >= tracing::Level::DEBUG
        {
            return QuietStart::General;
        }
        #[cfg(target_arch = "x86_64")]
        if let Some(start) = self.quiet_slots_lookup(key, token, operation, mode) {
            return start;
        }
        let permit = self.inline();
        let observation = QuietObservation::new(&self.inner.events, operation);
        let value = (|| {
            permit.admit()?;
            permit.status(token)?;
            let CacheMemory::Builtin(memory) = &self.inner.memory else {
                unreachable!("Plain plan requires built-in L1");
            };
            let copy = |entry: &super::Entry<V>, freshness: crate::entry::Freshness| {
                self.quiet_copy(entry, freshness, mode)
            };
            let copied = match &self.inner.clock {
                crate::time::local::CacheClock::Local(clock) => {
                    memory.with_local_ready(key, clock, copy)
                }
                crate::time::local::CacheClock::Shared(clock) => {
                    // User clock code stays outside the slot and inside admission.
                    let now = clock.now();
                    permit.status(token)?;
                    memory.with_ready(key, now, |entry| copy(entry, entry.freshness(now)))
                }
            }
            .flatten();
            permit.status(token)?;
            Ok(copied)
        })();
        match value {
            Ok(Some(QuietCopy::EntryPolicy)) => QuietStart::Recheck {
                observation,
                permit,
            },
            Ok(Some(QuietCopy::Value(value))) => QuietStart::Ready(QuietReady {
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
    fn quiet_copy(
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
    // x86's SeqCst store emits a separate locked instruction; the already
    // required reader fence can publish operation admission instead. ARM keeps
    // its direct release-store path, which measured faster without this frame.
    #[cfg(target_arch = "x86_64")]
    fn quiet_slots_lookup<'a>(
        &'a self,
        key: &str,
        token: Option<&FactoryCancellation>,
        operation: CacheOperation,
        mode: LookupMode,
    ) -> Option<QuietStart<'a, V>> {
        let (
            ReadyPlan::LocalSlots,
            CacheMemory::Builtin(memory),
            crate::time::local::CacheClock::Local(_),
        ) = (self.inner.ready_plan, &self.inner.memory, &self.inner.clock)
        else {
            return None;
        };
        let reservation = self.deferred_inline()?;
        let observation = QuietObservation::new(&self.inner.events, operation);
        let (permit, copied) =
            memory.with_admitted_local_ready(key, reservation, token, |entry, freshness| {
                self.quiet_copy(entry, freshness, mode)
            });
        let value = match copied.map(Option::flatten) {
            Err(Error::CacheClosed) => Err(Error::CacheClosed),
            value => permit.status(token).and(value),
        };
        Some(match value {
            Ok(Some(QuietCopy::EntryPolicy)) => QuietStart::Recheck {
                observation,
                permit,
            },
            Ok(Some(QuietCopy::Value(value))) => QuietStart::Ready(QuietReady {
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
        })
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
