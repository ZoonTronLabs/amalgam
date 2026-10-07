//! Ready standalone factories commit within borrowed admission. Only a genuinely
//! Pending factory registers cache-owned work and an observer future.
use super::{
    Arc, Cache, CacheEvent, CacheInner, CacheLevel, CacheOrigin, CacheValue, CommitReceipt, Entry,
    EntryOptions, Error, FactoryCancellation, FactoryContext, InlinePermit, LookupKey,
    MemoryAdmission, MutationReceipt, Observed, OperationOutcome, OriginKind, ReadyObservation,
    Result, Tag, TagVerdict, Timeout,
};
use crate::execution::LinkMode;
use crate::memory::CacheMemory;
use crate::single_flight::Completion;
use std::future::Future;
use std::pin::Pin;
use tracing::Instrument;

enum MemoryRead<V> {
    Fresh(V),
    Origin(Option<crate::factory::StaleInfo<V>>),
}

pub(super) type Value<V> = Observed<CacheValue<V>>;
pub(super) enum Start<V> {
    Ready(Result<CacheValue<V>>),
    Pending(Pin<Box<dyn Future<Output = Result<CacheValue<V>>> + Send + 'static>>),
}
pub(super) struct Input<O, V> {
    pub(super) keys: LookupKey,
    pub(super) origin: O,
    pub(super) options: EntryOptions,
    pub(super) tags: Box<[Tag]>,
    pub(super) caller: Option<FactoryCancellation>,
    pub(super) unused: super::MaybeValue<V>,
}
impl<V: Clone + Send + Sync + 'static> Cache<V> {
    pub(super) fn supports_inline_cold(&self, options: Option<&EntryOptions>) -> bool {
        let options = options.unwrap_or(&self.inner.default_options);
        self.inner.flights.is_some()
            && !options.enable_auto_clone()
            && !options.skip_memory_read()
            && !options.is_fail_safe_enabled()
            && options.eager_refresh_threshold().is_none()
            && options.factory_hard_timeout().is_infinite()
            && options.factory_soft_timeout().is_infinite()
            && options.memory_lock_timeout().is_infinite()
    }
    pub(super) fn prepare_inline_cold<O: CacheOrigin<V>>(
        &self,
        input: Input<O, V>,
        mut observation: ReadyObservation<'_>,
        permit: InlinePermit<'_>,
    ) -> Start<V> {
        let Input {
            keys,
            origin,
            options,
            tags,
            caller,
            unused,
        } = input;
        let span = observation.span();
        let _entered = span.enter();
        let flights = self
            .inner
            .flights
            .as_ref()
            .expect("inline plan owns flights");
        let mut claim = flights.acquire(Arc::clone(&keys.full), self.operation_scopes());
        let leader = claim.leader;
        let flight = Arc::clone(&claim.subscription.flight);
        let source = flight.source();
        self.inner.events.emit_lazy(|| CacheEvent::Miss {
            key: Arc::clone(&keys.full),
        });
        if leader {
            if let Some(token) = &caller {
                token.link_work(flight.clone(), LinkMode::Explicit);
            }
            let inner = Arc::clone(&self.inner);
            let token = source.token();
            let revision: Arc<dyn crate::memory::RevisionSource> = flight.clone();
            let work_options = options.clone();
            // The generic factory future is created inside this already pinned
            // body, so Pending never moves a previously polled !Unpin future.
            let action = match O::KIND {
                OriginKind::Factory => "factory",
                OriginKind::Constant => "constant",
            };
            let component =
                super::component_span(&inner.name, CacheLevel::Origin, action, Some(&keys.full));
            flight.install(Box::pin(
                async move {
                    inner
                        .inline_origin(keys, origin, work_options, tags, token, revision)
                        .await
                }
                .instrument(component),
            ));
            flight.poll_work();
        } else {
            // Unused user captures are dropped while borrowed admission is live.
            drop((origin, tags, keys));
        }
        drop(unused);
        if let Some(completion) = claim.subscription.try_complete() {
            let result = self
                .inner
                .complete_inline_value(completion, leader, &options);
            drop(claim.subscription);
            let result = permit.status(caller.as_ref()).and(result);
            match &result {
                Ok(value) => {
                    if let Some(level) = value.level {
                        observation.set_level(level);
                    }
                    observation.finish(value.outcome);
                }
                Err(error) => observation.finish(OperationOutcome::from_error(error)),
            }
            return Start::Ready(result.map(|value| value.value));
        }
        if leader {
            flight.register_pending();
            if self.inner.tasks.can_execute() {
                let _done = self.inner.tasks.spawn(
                    super::ShutdownTask::Factory,
                    Arc::clone(flight.key()),
                    self.inner.events.clone(),
                    Arc::clone(&flight).driver(),
                );
            }
        }
        let inner = Arc::clone(&self.inner);
        let subscription = claim.subscription;
        // The suspended observer has its own cancellable lifetime. Closing the
        // cache finishes its event even if the caller never polls again; dropping
        // that observer releases only the subscription, never the shared factory.
        let pending = self.execute_observed(
            observation.into_owned(),
            caller,
            super::CancellationSource::new(),
            super::ObservationAdmission::Inline(permit),
            async move {
                let completion = subscription.await;
                inner.complete_inline_value(completion, leader, &options)
            },
        );
        drop(_entered);
        Start::Pending(Box::pin(pending))
    }
}
impl<V: Clone + Send + Sync + 'static> CacheInner<V> {
    async fn inline_origin<O: CacheOrigin<V>>(
        self: Arc<Self>,
        keys: LookupKey,
        origin: O,
        options: EntryOptions,
        tags: Box<[Tag]>,
        token: FactoryCancellation,
        revision: Arc<dyn crate::memory::RevisionSource>,
    ) -> Result<Value<V>> {
        token.check()?;
        let CacheMemory::Builtin(memory) = &self.memory else {
            unreachable!("inline origin uses builtin L1");
        };
        let initial = self.inline_memory_read(memory, &keys.full, O::KIND);
        let stale = match initial {
            Some(MemoryRead::Fresh(value)) => {
                drop((origin, tags));
                token.check()?;
                return Ok(self.inline_hit(keys.full, value));
            }
            Some(MemoryRead::Origin(stale)) => stale,
            None => None,
        };
        // Reuse the same coordination as explicit options, native calls and
        // eager refresh during incremental migration of those policy paths.
        let local = self
            .assist_origin(
                &keys.full,
                self.locks.acquire(
                    &keys.full,
                    crate::MemoryLockKind::Entry,
                    Timeout::Infinite,
                    &token,
                    super::MemoryAcquireRoute::Asynchronous,
                ),
            )
            .await?;
        let version = memory.capture_origin_from(Arc::clone(&keys.full), Some(revision))?;
        token.check()?;
        let stale = match self.inline_memory_read(memory, &keys.full, O::KIND) {
            Some(MemoryRead::Fresh(value)) => {
                drop((local, version, origin, tags, stale));
                token.check()?;
                return Ok(self.inline_hit(keys.full, value));
            }
            Some(MemoryRead::Origin(Some(stale))) => Some(stale),
            Some(MemoryRead::Origin(None)) | None => stale,
        };
        let started = self.clock.now();
        token.check()?;
        let context =
            FactoryContext::with_cancellation(keys.clone(), options, tags, stale, token.clone());
        let product = origin
            .invoke(context)
            .await
            .map_err(|error| {
                let error = Error::from(error);
                if matches!(
                    error,
                    Error::Factory { .. } | Error::FactoryWithSource { .. }
                ) {
                    self.events.emit_lazy(|| CacheEvent::FactoryError {
                        key: Arc::clone(&keys.full),
                        message: error.to_string(),
                    });
                }
                error
            })?
            .into_payload()?;
        token.check()?;
        self.validate_options(&product.options)?;
        let value = self.copy(&product.value, &product.options)?;
        let stored = self.copy(&product.value, &product.options)?;
        let now = self.clock.now();
        let jitter = super::JitterSample::new(
            self.jitter.sample(product.options.jitter_max()),
            product.options.jitter_max(),
        )?;
        token.check()?;
        let entry = Entry::try_fresh_with_jitter(
            stored,
            &product.options,
            started,
            now,
            jitter,
            product.tags,
            product.etag,
            product.last_modified,
        )?;
        let local_effect = if product.options.skip_memory_write() {
            let current = memory.skip_origin(&keys.full, &version);
            drop((local, version, entry));
            if current {
                super::LocalEffect::Skipped
            } else {
                super::LocalEffect::Stored(MemoryAdmission::Rejected(
                    crate::CapacityRejection::VersionChanged,
                ))
            }
        } else {
            let prepared = memory.prepare_insert(Arc::clone(&keys.full), entry, now);
            let commit = memory.apply_origin(prepared, &version);
            // User Drop/observations never run while the factory lock is held.
            drop((local, version));
            super::LocalEffect::Stored(memory.finish_insert(commit))
        };
        drop(product.value);
        token.check()?;
        if !matches!(
            local_effect,
            super::LocalEffect::Stored(MemoryAdmission::Rejected(
                crate::CapacityRejection::VersionChanged
            ))
        ) {
            self.events.emit_lazy(|| CacheEvent::Set {
                key: Arc::clone(&keys.full),
            });
        }
        if matches!(O::KIND, OriginKind::Factory) {
            self.events
                .emit_lazy(|| CacheEvent::FactorySuccess { key: keys.full });
        }
        token.check()?;
        Ok(Observed::new(
            CacheValue {
                value,
                commit: CommitReceipt::Mutation(super::memory_inline::receipt(local_effect)),
            },
            OperationOutcome::Stored,
            match O::KIND {
                OriginKind::Factory => Some(CacheLevel::Origin),
                OriginKind::Constant => None,
            },
        ))
    }
    fn inline_memory_read(
        &self,
        memory: &crate::memory::MemoryStore<V>,
        key: &Arc<str>,
        kind: OriginKind,
    ) -> Option<MemoryRead<V>> {
        let now = self.clock.now();
        let read = memory.with_ready(key, now, |entry| {
            let candidate = match self.tags(entry) {
                TagVerdict::Valid if entry.freshness(now).is_fresh() => {
                    MemoryRead::Fresh(entry.value().clone())
                }
                TagVerdict::Valid | TagVerdict::Expire if matches!(kind, OriginKind::Factory) => {
                    MemoryRead::Origin(Some(crate::factory::StaleInfo {
                        value: entry.value().clone(),
                        etag: entry.meta().etag().map(str::to_owned),
                        last_modified: entry.meta().last_modified(),
                        tags: entry.meta().tags().into(),
                    }))
                }
                TagVerdict::Remove | TagVerdict::Valid | TagVerdict::Expire => {
                    MemoryRead::Origin(None)
                }
            };
            (entry.is_logically_expired(now), candidate)
        });
        memory.emit_layer_lazy(|| {
            super::LayerEvent::Memory(match &read {
                Some((stale, _)) => super::MemoryEvent::Hit {
                    key: Arc::clone(key),
                    stale: *stale,
                },
                None => super::MemoryEvent::Miss {
                    key: Arc::clone(key),
                },
            })
        });
        read.map(|(_, candidate)| candidate)
    }
    fn inline_hit(&self, key: Arc<str>, value: V) -> Value<V> {
        self.events
            .emit_lazy(|| CacheEvent::Hit { key, stale: false });
        Observed::new(
            CacheValue {
                value,
                commit: CommitReceipt::Unchanged,
            },
            OperationOutcome::Hit,
            Some(CacheLevel::Memory),
        )
    }
    fn complete_inline_value(
        &self,
        completion: Completion<Value<V>>,
        leader: bool,
        options: &EntryOptions,
    ) -> Result<Value<V>> {
        match completion {
            Completion::Owned(result) => result,
            Completion::Shared(result) => match result.as_ref() {
                Ok(observed) => {
                    let value = self.copy(&observed.value.value, options)?;
                    let commit = if leader {
                        match &observed.value.commit {
                            CommitReceipt::Unchanged => CommitReceipt::Unchanged,
                            CommitReceipt::Mutation(MutationReceipt::Completed(report)) => {
                                let local = report.local;
                                CommitReceipt::Mutation(super::memory_inline::receipt(local))
                            }
                            CommitReceipt::Mutation(MutationReceipt::Scheduled(_)) => {
                                unreachable!("inline receipt is complete")
                            }
                        }
                    } else {
                        CommitReceipt::Unchanged
                    };
                    Ok(Observed::new(
                        CacheValue { value, commit },
                        observed.outcome,
                        observed.level,
                    ))
                }
                Err(error) => Err(error.clone()),
            },
            Completion::Panicked(panic) => std::panic::resume_unwind(panic),
            Completion::FollowerPanicked => Err(Error::FactoryPanicked),
        }
    }
}
