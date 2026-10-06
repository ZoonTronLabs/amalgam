//! A ready lookup borrows state unless startup or health can retire values.
use super::{
    Cache, CacheEvent, CacheInner, ConfigError, Entry, EntryOptions, MarkerAccess, MarkerKind,
    MarkerReadOutcome, MarkerReadPolicy, MarkerReads, OptionsTarget, Ordering, Result,
    RuntimeComponent, Storage, Tag, TagVerdict, Timeout, Timestamp, Worker, validate_budget,
};

pub(super) enum ReadyContext<'a, V: Clone + Send + Sync + 'static> {
    // Constructed only with no backplane and no runtime maintenance startup.
    // ready_at never retires values; callbacks execute without a storage guard.
    Borrowed(&'a CacheInner<V>),
    // The operation collector survives every health retirement and callback.
    Retiring(Worker<V>),
}

impl<V: Clone + Send + Sync + 'static> ReadyContext<'_, V> {
    pub(super) fn inner(&self) -> &CacheInner<V> {
        match self {
            Self::Borrowed(inner) => inner,
            Self::Retiring(worker) => &worker.inner,
        }
    }

    pub(super) fn ensure_health(&self) {
        match self {
            Self::Borrowed(_) => {}
            Self::Retiring(worker) => worker.ensure_health(),
        }
    }
}

impl<V: Clone + Send + Sync + 'static> Cache<V> {
    pub(super) fn ready_context(&self) -> ReadyContext<'_, V> {
        let starts_maintenance = !self.inner.maintenance.load(Ordering::Acquire)
            && tokio::runtime::Handle::try_current().is_ok();
        if self.inner.backplane.is_some() || starts_maintenance {
            let worker = self.worker();
            worker.start_maintenance();
            ReadyContext::Retiring(worker)
        } else {
            ReadyContext::Borrowed(&self.inner)
        }
    }
}

impl<V: Clone + Send + Sync + 'static> CacheInner<V> {
    pub(super) fn validate_options(&self, opts: &EntryOptions) -> Result<()> {
        opts.validate_with_cloner(self.cloner.as_deref())?;
        self.validate_execution_options(opts, OptionsTarget::Value)
    }
    pub(super) fn validate_marker_options(&self, opts: &EntryOptions) -> Result<()> {
        opts.validate()?;
        self.validate_execution_options(opts, OptionsTarget::Marker)
    }
    pub(super) fn validate_execution_options(
        &self,
        opts: &EntryOptions,
        target: OptionsTarget,
    ) -> Result<()> {
        for timeout in [
            opts.memory_lock_timeout(),
            opts.distributed_lock_timeout(),
            opts.factory_soft_timeout(),
            opts.factory_hard_timeout(),
            opts.distributed_soft_timeout(),
            opts.distributed_hard_timeout(),
        ] {
            validate_budget(timeout)?;
        }
        if self.options_require_runtime(opts, target)
            && tokio::runtime::Handle::try_current().is_err()
        {
            return Err(ConfigError::MissingRuntime {
                component: RuntimeComponent::Execution,
            }
            .into());
        }
        Ok(())
    }
    pub(super) fn options_require_runtime(
        &self,
        opts: &EntryOptions,
        target: OptionsTarget,
    ) -> bool {
        let timers = [
            opts.memory_lock_timeout(),
            opts.factory_soft_timeout(),
            opts.factory_hard_timeout(),
            opts.distributed_soft_timeout(),
            opts.distributed_hard_timeout(),
        ]
        .into_iter()
        .any(|timeout| matches!(timeout,Timeout::After(duration) if !duration.is_zero()));
        let distributed = match target {
            OptionsTarget::Value => matches!(self.storage, Storage::Hybrid { .. }),
            OptionsTarget::Marker => matches!(self.markers, MarkerAccess::Durable(_)),
        };
        let background = opts.eager_refresh_threshold().is_some()
            || opts.allow_background_distributed_operations() && distributed
            || opts.allow_background_backplane_operations() && self.backplane.is_some();
        timers || background || !opts.skip_distributed_locker() && self.distributed_locker.is_some()
    }
    pub(super) fn copy(&self, value: &V, opts: &EntryOptions) -> Result<V> {
        crate::serializers::copy_value(value, opts, self.cloner.as_deref())
    }
    pub(super) fn tags(&self, entry: &Entry<V>) -> TagVerdict {
        if self.disable_tagging {
            TagVerdict::Valid
        } else {
            self.tags.evaluate(
                entry.meta().created(),
                entry.meta().tags(),
                self.remove_by_tag_behavior,
            )
        }
    }
    pub(super) fn marker_clear_shortcut(&self) -> bool {
        matches!(self.storage, Storage::Hybrid { .. }) && self.backplane.is_some()
    }

    pub(super) fn marker_reads_ready(&self, tags: &[Tag], now: Timestamp) -> bool {
        if self.disable_tagging
            || self.marker_reads.policy() == MarkerReadPolicy::DurableRequired
            || matches!(self.markers, MarkerAccess::Local)
        {
            return true;
        }
        if matches!(self.markers, MarkerAccess::Unavailable) {
            return false;
        }
        let MarkerReads::OptionsControlled(observations) = &self.marker_reads else {
            return true;
        };
        let options = &self.tags_default_options;
        let epoch = self.epoch.load(Ordering::Acquire);
        Worker::<V>::secondary_marker_kinds(tags).all(|kind| {
            observations.ready(&kind, options, now, epoch, self.marker_clear_shortcut())
        })
    }

    pub(super) fn marker_ready_events(&self, tags: &[Tag], now: Timestamp) {
        if self.disable_tagging {
            return;
        }
        let MarkerReads::OptionsControlled(observations) = &self.marker_reads else {
            return;
        };
        let epoch = self.epoch.load(Ordering::Acquire);
        for kind in Worker::<V>::secondary_marker_kinds(tags) {
            if let Some(outcome) = observations.ready_outcome(
                &kind,
                &self.tags_default_options,
                now,
                epoch,
                self.marker_clear_shortcut(),
            ) {
                self.marker_event(&kind, outcome);
            }
        }
    }

    pub(super) fn marker_event(&self, kind: &MarkerKind, outcome: MarkerReadOutcome) {
        self.events.emit_lazy(|| CacheEvent::MarkerRead {
            kind: kind.clone(),
            outcome,
        });
    }
}
