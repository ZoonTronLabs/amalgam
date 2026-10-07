//! The writer fence publishes callback-free preparation and counted completion.
use super::memory_inline::WritePlan;
use super::{Cache, CacheEvent, CacheOperation, LocalEffect, QuietObservation, Result, Tag};
use crate::entry::DefaultFreshPlan;
use crate::memory::CacheMemory;
use std::sync::Arc;

impl<V: Clone + Send + Sync + 'static> Cache<V> {
    pub(super) fn shared_slots_set(
        &self,
        key: &str,
        value: &V,
        options: &Option<Box<super::EntryOptions>>,
        tags: &std::result::Result<Box<[Tag]>, crate::TagError>,
        token: Option<&super::FactoryCancellation>,
    ) -> Option<Result<LocalEffect>> {
        if !matches!(self.inner.write_plan, WritePlan::SharedSlots)
            || options.is_some()
            || token.is_some()
            || !matches!(tags, Ok(tags) if tags.is_empty())
            || !self.inner.events.is_quiet()
            || tracing::level_filters::LevelFilter::current() >= tracing::Level::DEBUG
        {
            return None;
        }
        let (DefaultFreshPlan::Plain(plan), CacheMemory::Builtin(memory)) =
            (&self.inner.default_fresh_plan, &self.inner.memory)
        else {
            unreachable!("shared write plan requires primitive plain builtin L1");
        };
        if !memory.supports_shared_plain() {
            return None;
        }
        // Build selected primitive V, standard zero jitter and the concrete
        // local clock. No user callback or destructor executes before the fence.
        let time = self.inner.clock.write_time();
        let metadata = plan.metadata(time.now());
        if !time.now().is_before(metadata.physical()) {
            return None;
        }
        let reservation = self.deferred_inline()?;
        let observation = QuietObservation::new(&self.inner.events, CacheOperation::Set);
        let (permit, result) =
            memory.insert_admitted_plain(key, value.clone(), metadata, time, reservation);
        let result = result.map(LocalEffect::Stored).and_then(|local| {
            self.inner.events.emit_lazy(|| CacheEvent::Set {
                key: Arc::from(key),
            });
            permit.status(None)?;
            Ok(local)
        });
        observation.finish(super::memory_inline::set_outcome(&result), None);
        Some(result)
    }
}
