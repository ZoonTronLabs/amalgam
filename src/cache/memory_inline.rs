//! Synchronous built-in L1 commits. Payloads are prepared before coordination;
//! observations and destruction follow the short revision/storage critical section.
use super::{
    Arc, Cache, CacheEvent, CacheOperation, CommitReport, EffectOutcome, Entry, EntryOptions,
    FactoryCancellation, KeyMutation, LocalEffect, MemoryAdmission, MutationReceipt,
    OperationOutcome, OriginCommit, ReadyObservation, Result, Storage, Tag, Timestamp, Worker,
};
use crate::memory::{CacheMemory, MemoryStore};

pub(super) enum MutationStart<'a, T = MutationReceipt> {
    Ready(Result<T>),
    Pending(std::pin::Pin<Box<dyn std::future::Future<Output = Result<T>> + Send + 'a>>),
    Done,
}
impl<T: Unpin> std::future::Future for MutationStart<'_, T> {
    type Output = Result<T>;
    fn poll(
        self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let state = self.get_mut();
        match state {
            Self::Pending(work) => work.as_mut().poll(context),
            Self::Ready(_) => match std::mem::replace(state, Self::Done) {
                Self::Ready(result) => std::task::Poll::Ready(result),
                Self::Pending(_) | Self::Done => {
                    unreachable!("ready mutation changed without polling")
                }
            },
            Self::Done => panic!("a completed mutation must not be polled again"),
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum WritePlan {
    InlineMemory,
    General,
}
impl WritePlan {
    pub(super) fn select<V: Clone + Send + Sync + 'static>(
        storage: &Storage<V>,
        memory: &CacheMemory<V>,
        backplane: bool,
        locker: bool,
    ) -> Self {
        if matches!(storage, Storage::MemoryOnly)
            && matches!(memory, CacheMemory::Builtin(_))
            && !backplane
            && !locker
        {
            Self::InlineMemory
        } else {
            Self::General
        }
    }
    pub(super) fn is_inline(self) -> bool {
        matches!(self, Self::InlineMemory)
    }
}

pub(super) fn receipt(local: LocalEffect) -> MutationReceipt {
    MutationReceipt::Completed(CommitReport {
        local,
        distributed: EffectOutcome::NotConfigured,
        backplane: EffectOutcome::NotConfigured,
    })
}

impl<V: Clone + Send + Sync + 'static> Cache<V> {
    pub(super) fn inline_set(
        &self,
        raw: &str,
        value: V,
        options: Option<Box<EntryOptions>>,
        tags: std::result::Result<Box<[Tag]>, crate::TagError>,
        token: Option<&FactoryCancellation>,
    ) -> Result<LocalEffect> {
        let permit = self.inline();
        // Storage borrows the key and owns a copy only for a new entry.
        let joined;
        let key: &str = match super::KeyParts::new(self.inner.key_prefix.as_ref(), raw).prefix() {
            None => raw,
            Some(prefix) => {
                joined = super::PhysicalKey::joined(prefix, raw);
                &joined
            }
        };
        if self.inner.events.is_quiet()
            && tracing::level_filters::LevelFilter::current() < tracing::Level::DEBUG
        {
            let observation = super::QuietObservation::new(&self.inner.events, CacheOperation::Set);
            let result = self.inline_set_value(raw, key, value, options, tags, token, &permit);
            observation.finish(set_outcome(&result), None);
            return result;
        }
        let observation = ReadyObservation::new(
            &self.inner.events,
            &self.inner.name,
            &self.inner.instance_id,
            CacheOperation::Set,
            Some(key),
        );
        let result = self.inline_set_value(raw, key, value, options, tags, token, &permit);
        observation.finish(set_outcome(&result));
        result
    }
    #[allow(clippy::too_many_arguments)]
    fn inline_set_value(
        &self,
        raw: &str,
        key: &str,
        value: V,
        options: Option<Box<EntryOptions>>,
        tags: std::result::Result<Box<[Tag]>, crate::TagError>,
        token: Option<&FactoryCancellation>,
        permit: &super::InlinePermit<'_>,
    ) -> Result<LocalEffect> {
        (|| {
            permit.admit()?;
            permit.status(token)?;
            let tags = tags?;
            let resolved = options.or_else(|| {
                self.inner
                    .default_options_provider
                    .as_ref()
                    .and_then(|provider| {
                        provider
                            .options_for_with_defaults(raw, &self.inner.default_options)
                            .map(Box::new)
                    })
            });
            let (opts, copy) = match &resolved {
                Some(opts) => (opts.as_ref(), self.inner.validated_value_copy(opts)?),
                None => {
                    self.inner.default_runtime.validate()?;
                    (
                        &self.inner.default_options,
                        self.inner.default_copy.borrowed(),
                    )
                }
            };
            permit.status(token)?;
            let stored = copy.copy(&value)?;
            // No user Clone, random source or clock callback runs under revision/storage locks.
            let jitter = self.inner.jitter.sample(opts.jitter_max())?;
            let time = self.inner.clock.write_time();
            let now = time.now();
            permit.status(token)?;
            let CacheMemory::Builtin(memory) = &self.inner.memory else {
                unreachable!("inline write plan requires builtin L1");
            };
            use crate::entry::DefaultFreshPlan;
            let local = match (&resolved, &self.inner.default_fresh_plan) {
                (None, DefaultFreshPlan::Plain(plan))
                    if tags.is_empty() && !opts.skip_memory_write() =>
                {
                    LocalEffect::Stored(memory.insert_plain_borrowed(
                        key,
                        stored,
                        plan.metadata(now),
                        time,
                    ))
                }
                selected => {
                    let entry = match selected {
                        (None, DefaultFreshPlan::Plain(plan)) => plan.prepare(stored, now, tags),
                        (None, DefaultFreshPlan::Prepared(plan)) => plan.prepare(stored, now, tags),
                        _ => Entry::prepare_fresh_with_jitter(
                            stored, opts, now, now, jitter, tags, None, None,
                        )?,
                    };
                    if opts.skip_memory_write() {
                        memory.invalidate_origin(key);
                        drop(entry);
                        LocalEffect::Skipped
                    } else {
                        LocalEffect::Stored(memory.insert_value_borrowed(key, entry, time))
                    }
                }
            };
            self.inner.events.emit_lazy(|| CacheEvent::Set {
                key: Arc::from(key),
            });
            // The incoming representation can have its own destructor even
            // when the stored copy is pinned. Attribute a reentrant close now.
            drop(value);
            permit.status(token)?;
            Ok(local)
        })()
    }
}

fn set_outcome<T>(result: &Result<T>) -> OperationOutcome {
    match result {
        Ok(_) => OperationOutcome::Stored,
        Err(error) => OperationOutcome::from_error(error),
    }
}

impl<V: Clone + Send + Sync + 'static> Worker<V> {
    pub(super) fn inline_entry(
        &self,
        key: Arc<str>,
        entry: Entry<V>,
        opts: &EntryOptions,
        origin: Option<OriginCommit<V>>,
        cancellation: &FactoryCancellation,
    ) -> Result<MutationReceipt> {
        cancellation.check()?;
        let CacheMemory::Builtin(memory) = &self.memory else {
            unreachable!("inline write plan requires builtin L1");
        };
        let now = self.inner.clock.now();
        cancellation.check()?;
        let prepared = memory.prepare_insert(Arc::clone(&key), entry, now);
        if opts.skip_memory_write() {
            let current = match origin.as_ref().map(|origin| &origin.started_at) {
                None => {
                    memory.invalidate_origin(&key);
                    true
                }
                Some(super::OriginVersion::Memory(version)) => memory.skip_origin(&key, version),
                Some(super::OriginVersion::Ordered(_)) => {
                    unreachable!("inline origin carries a memory version")
                }
            };
            drop(origin);
            drop(prepared);
            return Ok(receipt(if current {
                LocalEffect::Skipped
            } else {
                LocalEffect::Stored(MemoryAdmission::Rejected(
                    crate::provider::CapacityRejection::VersionChanged,
                ))
            }));
        }
        let committed = match origin.as_ref().map(|origin| &origin.started_at) {
            None => memory.apply_insert(prepared),
            Some(super::OriginVersion::Memory(version)) => memory.apply_origin(prepared, version),
            Some(super::OriginVersion::Ordered(_)) => {
                unreachable!("inline origin carries a memory version")
            }
        };
        // Release local factory coordination before callbacks or value destruction.
        drop(origin);
        Ok(receipt(LocalEffect::Stored(
            memory.finish_insert(committed),
        )))
    }

    pub(super) fn inline_key_mutation(
        &self,
        key: &Arc<str>,
        opts: &EntryOptions,
        mutation: KeyMutation,
        now: Timestamp,
    ) -> Result<LocalEffect> {
        let CacheMemory::Builtin(memory) = &self.memory else {
            unreachable!("inline write plan requires builtin L1");
        };
        if opts.skip_memory_write() {
            memory.invalidate_origin(key);
            return Ok(LocalEffect::Skipped);
        }
        match mutation {
            KeyMutation::Remove => {
                let removed = memory.detach_remove(key);
                memory.finish_remove(removed);
                Ok(LocalEffect::Removed)
            }
            KeyMutation::Expire(_) => self.inline_expire(memory, key, now),
        }
    }

    fn inline_expire(
        &self,
        memory: &MemoryStore<V>,
        key: &Arc<str>,
        now: Timestamp,
    ) -> Result<LocalEffect> {
        // Metadata/value copying precedes revision coordination; storage still
        // conditionally targets this exact representation if another write wins.
        let previous = memory.ready_at_for_mutation(key, now);
        match previous {
            Some(previous) => {
                let expired = previous.with_logical_expiration(now);
                let prepared = memory.prepare_expire(Arc::clone(key), expired, now);
                let commit = memory.apply_expire(prepared, &previous);
                memory.finish_insert(commit);
            }
            None => {
                memory.invalidate_origin(key);
            }
        }
        Ok(LocalEffect::Expired)
    }
}
