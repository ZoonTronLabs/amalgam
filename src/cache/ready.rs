//! A ready lookup borrows state unless startup or health can retire values.
use super::{
    Cache, CacheEvent, CacheInner, ConfigError, Entry, EntryOptions, MarkerAccess, MarkerKind,
    MarkerReadOutcome, MarkerReadPolicy, MarkerReads, OptionsTarget, Ordering, Result,
    RuntimeComponent, Storage, TagVerdict, Timestamp, Worker,
};

#[derive(Clone, Copy)]
pub(super) enum RuntimeRequirement {
    Inline,
    Runtime,
}
impl RuntimeRequirement {
    pub(super) fn for_options(
        options: &EntryOptions,
        distributed: bool,
        backplane: bool,
        locker: bool,
    ) -> Self {
        let background = options.eager_refresh_threshold().is_some()
            || options.allow_background_distributed_operations() && distributed
            || options.allow_background_backplane_operations() && backplane;
        if options.requires_timer_runtime()
            || background
            || !options.skip_distributed_locker() && locker
        {
            Self::Runtime
        } else {
            Self::Inline
        }
    }
    pub(super) fn validate(self) -> Result<()> {
        match self {
            Self::Inline => Ok(()),
            Self::Runtime if tokio::runtime::Handle::try_current().is_ok() => Ok(()),
            Self::Runtime => Err(ConfigError::MissingRuntime {
                component: RuntimeComponent::Execution,
            }
            .into()),
        }
    }
    fn requires_runtime(self) -> bool {
        match self {
            Self::Inline => false,
            Self::Runtime => true,
        }
    }
}

pub(super) enum ReadyContext<'a, V: Clone + Send + Sync + 'static> {
    // Constructed only with no backplane and no runtime maintenance startup.
    // Internal checks and ordinary value Clone borrow a thread-local reader slot.
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

    pub(super) fn ensure_health(&self) -> Result<()> {
        match self {
            Self::Borrowed(_) => Ok(()),
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
        opts.validate()?;
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
        let distributed = match target {
            OptionsTarget::Value => matches!(self.storage, Storage::Hybrid { .. }),
            OptionsTarget::Marker => matches!(self.markers, MarkerAccess::Durable(_)),
        };
        RuntimeRequirement::for_options(
            opts,
            distributed,
            self.backplane.is_some(),
            self.distributed_locker.is_some(),
        )
        .requires_runtime()
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

    pub(super) fn native_inline_read(&self) -> bool {
        matches!(self.storage, Storage::MemoryOnly)
            && matches!(self.memory, crate::memory::CacheMemory::Builtin(_))
            && matches!(self.default_runtime, RuntimeRequirement::Inline)
            && self.default_options_provider.is_none()
            && !self.default_options.enable_auto_clone()
    }

    /// Only internal checks and ordinary value Clone may run under an L1 slot.
    pub(super) fn ready_slot_copy(&self, opts: &EntryOptions) -> bool {
        !opts.enable_auto_clone()
            && matches!(self.memory, crate::memory::CacheMemory::Builtin(_))
            && (self.disable_tagging
                || !matches!(self.marker_reads, MarkerReads::OptionsControlled(_)))
    }

    pub(super) fn marker_reads_ready(&self, entry: &Entry<V>, now: Timestamp) -> Result<bool> {
        if self.disable_tagging || self.marker_reads.policy() == MarkerReadPolicy::DurableRequired {
            return Ok(true);
        }
        if matches!(self.markers, MarkerAccess::Unavailable) {
            return Ok(false);
        }
        let MarkerReads::OptionsControlled(observations) = &self.marker_reads else {
            return Ok(true);
        };
        if matches!(self.markers, MarkerAccess::Local) && !observations.memory.is_supplied() {
            return Ok(true);
        }
        let options = &self.tags_default_options;
        let epoch = self.epoch.load(Ordering::Acquire);
        for kind in Worker::<V>::secondary_marker_kinds(entry.meta().tags()) {
            let Some(probe) = observations.ready_value(
                &kind,
                options,
                now,
                epoch,
                self.marker_clear_shortcut(),
            )?
            else {
                return Ok(false);
            };
            if let Some(maximum) = probe.maximum() {
                self.tags.advance(kind.clone(), maximum);
            }
            self.marker_event(&kind, probe.outcome());
            if probe.maximum().is_some() && self.tags(entry) != TagVerdict::Valid {
                return Ok(false);
            }
        }
        Ok(self.tags(entry) == TagVerdict::Valid)
    }

    pub(super) fn local_marker_ready_events(
        &self,
        tags: &[super::Tag],
        now: Timestamp,
    ) -> Result<()> {
        if self.disable_tagging || !matches!(self.markers, MarkerAccess::Local) {
            return Ok(());
        }
        let MarkerReads::OptionsControlled(observations) = &self.marker_reads else {
            return Ok(());
        };
        if observations.memory.is_supplied() {
            return Ok(());
        }
        let epoch = self.epoch.load(Ordering::Acquire);
        for kind in Worker::<V>::secondary_marker_kinds(tags) {
            if let Some(probe) =
                observations.ready_value(&kind, &self.tags_default_options, now, epoch, false)?
            {
                self.marker_event(&kind, probe.outcome());
            }
        }
        Ok(())
    }

    pub(super) fn marker_event(&self, kind: &MarkerKind, outcome: MarkerReadOutcome) {
        self.events.emit_lazy(|| CacheEvent::MarkerRead {
            kind: kind.clone(),
            outcome,
        });
    }
}
