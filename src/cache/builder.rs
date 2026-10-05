//! Cache configuration, validation and construction.
use super::{
    Arc, AtomicBool, AtomicU64, AutoRecoveryService, Backplane, Cache, CacheInner, CacheScope,
    CircuitBreaker, Clock, ClockTiming, ConfigError, DefaultEntryOptionsProvider, DistributedCache,
    DistributedLocker, DistributedSerializer, Duration, EntryOptions, Events, IdentityField,
    Instant, InvalidationStore, JitterSource, KeyModifierMode, KeyedLock, Lanes, LeasePolicy,
    LeaseTtl, Lifecycle, MarkerAccess, MarkerLifecycleAccess, MarkerLifecyclePolicy,
    MarkerObservations, MarkerReadPolicy, MarkerReads, MemoryExpiry, MemoryLimits, MemoryStore,
    Plugin, PluginContext, PluginHost, PublicLifetime, RandomJitterSource, ReconciliationPolicy,
    RecoveryConfig, RecoveryExecutor, RemoveByTagBehavior, Result, RuntimeComponent, Scopes,
    Storage, SystemClock, TagRegistry, Tasks, Timeout, ValueCloner, validate_budget,
};

/// Builder for a [`Cache`].
///
/// ```
/// use amalgam::{Cache, EntryOptions};
/// use std::time::Duration;
///
/// let cache: Cache<String> = Cache::builder()
///     .name("users")
///     .key_prefix("u:")
///     .default_options(EntryOptions::new(Duration::from_secs(60)))
///     .build();
/// ```
#[must_use = "a builder does nothing until `.build()` is called"]
pub struct CacheBuilder<V> {
    name: Option<Arc<str>>,
    instance_id: Option<Arc<str>>,
    key_prefix: Option<Arc<str>>,
    default_options: EntryOptions,
    tags_default_options: EntryOptions,
    marker_read_policy: MarkerReadPolicy,
    marker_lifecycle_policy: MarkerLifecyclePolicy,
    marker_read_limits: MemoryLimits,
    clock: Option<Arc<dyn Clock>>,
    max_capacity: Option<u64>,
    max_weighted_capacity: Option<u64>,
    value_cloner: Option<Arc<dyn ValueCloner<V>>>,
    jitter: Arc<dyn JitterSource>,
    invalidation_store: Option<Arc<dyn InvalidationStore>>,
    lease_policy: LeasePolicy,
    lease_ttl: Duration,
    reconciliation: Option<ReconciliationPolicy>,
    lock_shards: usize,
    remove_by_tag_behavior: RemoveByTagBehavior,
    events_capacity: usize,
    distributed: Option<Arc<dyn DistributedCache>>,
    serializer: Option<crate::distributed::Serializer<V>>,
    serialization_mode: crate::distributed::SerializationMode,
    backplane: Option<Arc<dyn Backplane>>,
    distributed_locker: Option<Arc<dyn DistributedLocker>>,
    plugins: Vec<Arc<dyn Plugin>>,
    distributed_circuit_breaker: Duration,
    backplane_circuit_breaker: Duration,
    recovery_config: RecoveryConfig,
    default_options_provider: Option<Arc<dyn DefaultEntryOptionsProvider>>,
    ignore_incoming_backplane: bool,
    distributed_wire_version: Arc<str>,
    distributed_key_modifier_mode: KeyModifierMode,
    disable_tagging: bool,
    wait_for_initial_backplane_subscribe: bool,
    _marker: std::marker::PhantomData<fn() -> V>,
}

impl<V> CacheBuilder<V> {
    /// Creates a builder with default settings.
    pub fn new() -> Self {
        Self {
            name: None,
            instance_id: None,
            key_prefix: None,
            default_options: EntryOptions::default(),
            tags_default_options: EntryOptions::tag_defaults(),
            marker_read_policy: MarkerReadPolicy::default(),
            marker_lifecycle_policy: MarkerLifecyclePolicy::default(),
            marker_read_limits: MemoryLimits::new(Some(4096), None),
            clock: None,
            max_capacity: None,
            max_weighted_capacity: None,
            value_cloner: None,
            jitter: Arc::new(RandomJitterSource),
            invalidation_store: None,
            lease_policy: LeasePolicy::Fenced,
            lease_ttl: Duration::from_secs(30),
            reconciliation: None,
            lock_shards: 1024,
            remove_by_tag_behavior: RemoveByTagBehavior::default(),
            events_capacity: 256,
            distributed: None,
            serializer: None,
            serialization_mode: crate::distributed::SerializationMode::default(),
            backplane: None,
            distributed_locker: None,
            plugins: Vec::new(),
            distributed_circuit_breaker: Duration::ZERO,
            backplane_circuit_breaker: Duration::ZERO,
            recovery_config: RecoveryConfig::default(),
            default_options_provider: None,
            ignore_incoming_backplane: false,
            distributed_wire_version: Arc::from("v2"),
            distributed_key_modifier_mode: KeyModifierMode::default(),
            disable_tagging: false,
            wait_for_initial_backplane_subscribe: true,
            _marker: std::marker::PhantomData,
        }
    }

    /// Sets this instance's id (used to ignore its own backplane messages). A
    /// random id is generated if not set.
    pub fn instance_id(mut self, id: impl AsRef<str>) -> Self {
        self.instance_id = Some(Arc::from(id.as_ref()));
        self
    }

    /// Attaches an L2 distributed cache backend. Pair with
    /// [`serializer`](Self::serializer).
    pub fn distributed(mut self, distributed: Arc<dyn DistributedCache>) -> Self {
        self.distributed = Some(distributed);
        self
    }

    /// Sets the serializer used for the L2 wire format (e.g.
    /// [`JsonSerializer`](crate::JsonSerializer)).
    pub fn serializer(mut self, serializer: Arc<dyn DistributedSerializer<V>>) -> Self {
        self.serializer = Some(crate::distributed::Serializer::Sync(serializer));
        self
    }
    /// Sets an asynchronous full-snapshot codec, optionally offering a sync counterpart.
    pub fn async_serializer(
        mut self,
        serializer: Arc<dyn crate::distributed::AsyncDistributedSerializer<V>>,
    ) -> Self {
        self.serializer = Some(crate::distributed::Serializer::Async(serializer));
        self
    }
    /// Chooses the preferred available codec model. SyncPreferred preserves legacy behavior.
    pub fn serialization_mode(mut self, mode: crate::distributed::SerializationMode) -> Self {
        self.serialization_mode = mode;
        self
    }

    /// Attaches a backplane for multi-node L1 invalidation.
    ///
    /// Note: when a backplane is configured, [`build`](Self::build) spawns a
    /// listener task and so must be called from within a tokio runtime.
    pub fn backplane(mut self, backplane: Arc<dyn Backplane>) -> Self {
        self.backplane = Some(backplane);
        self
    }

    /// Sets the cache's name (used in events/diagnostics).
    pub fn name(mut self, name: impl AsRef<str>) -> Self {
        self.name = Some(Arc::from(name.as_ref()));
        self
    }

    /// Sets a prefix prepended to every key.
    pub fn key_prefix(mut self, prefix: impl AsRef<str>) -> Self {
        self.key_prefix = Some(Arc::from(prefix.as_ref()));
        self
    }

    /// Sets the default entry options merged into every operation that does not
    /// supply its own.
    pub fn default_options(mut self, options: EntryOptions) -> Self {
        self.default_options = options;
        self
    }
    /// Sets independent defaults for tag invalidation and cache-wide clear.
    /// Explicit mutation options take precedence; per-key providers are skipped.
    /// OptionsControlled secondary reads always use these cache-wide defaults.
    pub fn tags_default_options(mut self, options: EntryOptions) -> Self {
        self.tags_default_options = options;
        self
    }

    /// Selects independent secondary marker reads while preserving the existing
    /// durable contract by default. Skips/suppressed failures are explicit choices.
    pub fn marker_read_policy(mut self, policy: MarkerReadPolicy) -> Self {
        self.marker_read_policy = policy;
        self
    }

    /// Opts into independent expiring snapshots and nonzero read-miss repair.
    /// Existing durable providers remain supported by the default DurableOnly.
    pub fn marker_lifecycle_policy(mut self, policy: MarkerLifecyclePolicy) -> Self {
        self.marker_lifecycle_policy = policy;
        self
    }

    /// Limits the separate observation cache; never expires durable tombstones
    /// or discards known local invalidations. Tag size and priority select admission.
    pub fn marker_read_limits(mut self, limits: MemoryLimits) -> Self {
        self.marker_read_limits = limits;
        self
    }

    /// Injects a custom [`Clock`] (e.g. [`ManualClock`](crate::ManualClock) in
    /// tests).
    pub fn clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = Some(clock);
        self
    }

    /// Caps the number of entries held in L1 (`None` = unbounded).
    pub fn max_capacity(mut self, capacity: u64) -> Self {
        self.max_capacity = Some(capacity);
        self
    }

    /// Caps total admitted L1 weight, independently from the entry count.
    pub fn max_weighted_capacity(mut self, capacity: u64) -> Self {
        self.max_weighted_capacity = Some(capacity);
        self
    }
    /// Supplies an explicit deep-copy algorithm for auto-clone options.
    pub fn value_cloner(mut self, cloner: Arc<dyn ValueCloner<V>>) -> Self {
        self.value_cloner = Some(cloner);
        self
    }

    /// Uses the compiler-checked ordinary clone for auto-clone values.
    /// This bypasses codec copying without admitting shared mutable state.
    /// Later `value_cloner` calls may explicitly replace this strategy.
    pub fn immutable_values(mut self) -> Self
    where
        V: crate::serializers::ImmutableValue,
    {
        self.value_cloner = Some(Arc::new(crate::serializers::ImmutableCloner));
        self
    }
    /// Supplies expiration jitter outside pure entry construction.
    pub fn jitter_source(mut self, jitter: Arc<dyn JitterSource>) -> Self {
        self.jitter = jitter;
        self
    }
    /// Supplies genuine atomic invalidation storage for a custom byte backend.
    pub fn invalidation_store(mut self, store: Arc<dyn InvalidationStore>) -> Self {
        self.invalidation_store = Some(store);
        self
    }
    /// Chooses strict native fencing or explicitly cooperative legacy ownership.
    pub fn lease_policy(mut self, policy: LeasePolicy) -> Self {
        self.lease_policy = policy;
        self
    }
    /// Sets ownership lease duration independently from entry retention.
    pub fn lease_ttl(mut self, ttl: Duration) -> Self {
        self.lease_ttl = ttl;
        self
    }
    /// Declares notification/durable-marker reconciliation behavior.
    pub fn reconciliation_policy(mut self, policy: ReconciliationPolicy) -> Self {
        self.reconciliation = Some(policy);
        self
    }

    /// Retains the legacy shard setting for source compatibility.
    ///
    /// Flights are now per-key; this setting does not serialize distinct keys
    /// or select the internal lookup map's sharding.
    pub fn lock_shards(mut self, shards: usize) -> Self {
        self.lock_shards = shards;
        self
    }

    /// Sets what `remove_by_tag` does to matched entries.
    pub fn remove_by_tag_behavior(mut self, behavior: RemoveByTagBehavior) -> Self {
        self.remove_by_tag_behavior = behavior;
        self
    }

    /// Sets the event channel's buffer capacity.
    pub fn events_capacity(mut self, capacity: usize) -> Self {
        self.events_capacity = capacity;
        self
    }

    /// Attaches a cross-node distributed locker (e.g. Redis-backed) for
    /// cluster-wide single-flight.
    pub fn distributed_locker(mut self, locker: Arc<dyn DistributedLocker>) -> Self {
        self.distributed_locker = Some(locker);
        self
    }

    /// Registers a [`Plugin`] to observe events and lifecycle.
    pub fn plugin(mut self, plugin: Arc<dyn Plugin>) -> Self {
        self.plugins.push(plugin);
        self
    }

    /// Opens the L2 circuit breaker for `duration` after an L2 failure
    /// (`Duration::ZERO` disables it — the default).
    pub fn distributed_circuit_breaker(mut self, duration: Duration) -> Self {
        self.distributed_circuit_breaker = duration;
        self
    }

    /// Opens the backplane circuit breaker for `duration` after a backplane
    /// failure (`Duration::ZERO` disables it — the default).
    pub fn backplane_circuit_breaker(mut self, duration: Duration) -> Self {
        self.backplane_circuit_breaker = duration;
        self
    }

    /// Configures (or disables) auto-recovery of failed L2 / backplane operations.
    pub fn auto_recovery(mut self, config: RecoveryConfig) -> Self {
        self.recovery_config = config;
        self
    }

    /// Sets a provider of dynamic, per-key default options (consulted when a call
    /// supplies no explicit options).
    pub fn default_options_provider(
        mut self,
        provider: Arc<dyn DefaultEntryOptionsProvider>,
    ) -> Self {
        self.default_options_provider = Some(provider);
        self
    }

    /// Drops all incoming backplane notifications (dangerous; for testing).
    pub fn ignore_incoming_backplane(mut self, ignore: bool) -> Self {
        self.ignore_incoming_backplane = ignore;
        self
    }

    /// Sets the L2 wire-format version combined with distributed keys.
    pub fn distributed_wire_version(mut self, version: impl AsRef<str>) -> Self {
        self.distributed_wire_version = Arc::from(version.as_ref());
        self
    }

    /// Sets how the wire-format version is combined with the L2 key (prefix,
    /// suffix, or none).
    pub fn distributed_key_modifier_mode(mut self, mode: KeyModifierMode) -> Self {
        self.distributed_key_modifier_mode = mode;
        self
    }

    /// Disables tagging cache-wide (FusionCache `DisableTagging`).
    ///
    /// When enabled, the per-read tag/clear-marker check is skipped entirely (a
    /// small read-path saving for caches that never tag), and
    /// [`remove_by_tag`](Cache::remove_by_tag) /
    /// [`remove_by_tags`](Cache::remove_by_tags) / [`clear`](Cache::clear) are
    /// ignored and logged at `warn` (never silently) — the Rust-idiomatic
    /// counterpart of FusionCache throwing on tag use when tagging is disabled.
    /// Off by default.
    pub fn disable_tagging(mut self, disable: bool) -> Self {
        self.disable_tagging = disable;
        self
    }

    /// Gates operations on native subscription acknowledgement (default true).
    /// Construction installs the local receiver synchronously; use
    /// `try_build_ready` to await admission before returning the cache. Native
    /// Redis connect already awaits its first ACK. Healthless custom adapters
    /// report BestEffort and use bounded periodic durable reconciliation.
    pub fn wait_for_initial_backplane_subscribe(mut self, wait: bool) -> Self {
        self.wait_for_initial_backplane_subscribe = wait;
        self
    }
}

impl<V> Default for CacheBuilder<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V: Clone + Send + Sync + 'static> CacheBuilder<V> {
    /// Async construction that also waits native backplane acknowledgement.
    pub async fn try_build_ready(self) -> Result<Cache<V>> {
        let cache = self.try_build()?;
        cache.ready().await?;
        Ok(cache)
    }
    /// Diagnostic legacy construction adapter. Prefer try_build for expected
    /// configuration/plugin/runtime rejection.
    pub fn build(self) -> Cache<V> {
        match self.try_build() {
            Ok(cache) => cache,
            Err(error) => panic!("invalid legacy cache construction: {error}"),
        }
    }
    /// Constructs a fully valid cache before starting its services.
    pub fn try_build(self) -> Result<Cache<V>> {
        let name = self.name.unwrap_or_else(|| Arc::from("amalgam"));
        let instance_id = self
            .instance_id
            .unwrap_or_else(|| Arc::from(format!("amalgam-{:016x}", fastrand::u64(..))));
        for (value, field) in [
            (&*name, IdentityField::CacheName),
            (&*instance_id, IdentityField::InstanceId),
            (&*self.distributed_wire_version, IdentityField::WireVersion),
        ] {
            if value.trim().is_empty() {
                return Err(ConfigError::BlankIdentity { field }.into());
            }
        }
        if self.distributed.is_some() && self.serializer.is_none() {
            return Err(ConfigError::DistributedWithoutSerializer.into());
        }
        let cloner = self.value_cloner.or_else(|| {
            self.serializer
                .as_ref()
                .and_then(|serializer| serializer.value_cloner())
        });
        self.default_options
            .validate_with_cloner(cloner.as_deref())?;
        self.tags_default_options.validate()?;
        for options in [&self.default_options, &self.tags_default_options] {
            for timeout in [
                options.memory_lock_timeout(),
                options.distributed_lock_timeout(),
                options.factory_soft_timeout(),
                options.factory_hard_timeout(),
                options.distributed_soft_timeout(),
                options.distributed_hard_timeout(),
            ] {
                validate_budget(timeout)?;
            }
        }
        let recovery_enabled = self.recovery_config.enabled
            && (self.distributed.is_some() || self.backplane.is_some());
        if self.backplane.is_some() && tokio::runtime::Handle::try_current().is_err() {
            return Err(ConfigError::MissingRuntime {
                component: RuntimeComponent::Backplane,
            }
            .into());
        }
        if recovery_enabled && tokio::runtime::Handle::try_current().is_err() {
            return Err(ConfigError::MissingRuntime {
                component: RuntimeComponent::Recovery,
            }
            .into());
        }
        let lease_ttl = LeaseTtl::new(self.lease_ttl)?;
        let reconciliation = self.reconciliation.unwrap_or_else(|| {
            if self.backplane.is_none() && self.distributed.is_none() {
                ReconciliationPolicy::LocalOnly
            } else if self
                .backplane
                .as_ref()
                .and_then(|backplane| backplane.connection_state())
                .is_some()
            {
                ReconciliationPolicy::BackplaneContinuity
            } else {
                ReconciliationPolicy::Periodic(Duration::from_secs(1))
            }
        });
        match reconciliation {
            ReconciliationPolicy::LocalOnly => {
                if self.backplane.is_some() || self.distributed.is_some() {
                    return Err(ConfigError::LocalReconciliationWithExternalStorage.into());
                }
            }
            ReconciliationPolicy::Periodic(interval) => {
                if interval.is_zero() {
                    return Err(ConfigError::ZeroReconciliationInterval.into());
                }
                validate_budget(Timeout::After(interval))?;
            }
            ReconciliationPolicy::BackplaneContinuity => {
                if self
                    .backplane
                    .as_ref()
                    .and_then(|backplane| backplane.connection_state())
                    .is_none()
                {
                    return Err(ConfigError::UnavailableBackplaneContinuity.into());
                }
            }
        }
        let scope = CacheScope::new(
            self.key_prefix.clone().unwrap_or_else(|| Arc::from("")),
            Arc::clone(&self.distributed_wire_version),
            self.distributed_key_modifier_mode,
        )?;
        let markers = match self.invalidation_store.or_else(|| {
            self.distributed
                .as_ref()
                .and_then(|backend| backend.invalidation_store())
        }) {
            Some(store) => MarkerAccess::Durable(store),
            None if self.distributed.is_some() => MarkerAccess::Unavailable,
            None => MarkerAccess::Local,
        };
        let marker_lifecycle = match self.marker_lifecycle_policy {
            MarkerLifecyclePolicy::DurableOnly => MarkerLifecycleAccess::DurableOnly,
            MarkerLifecyclePolicy::CachedSnapshots => {
                if self.marker_read_policy != MarkerReadPolicy::OptionsControlled {
                    return Err(ConfigError::MarkerLifecycleRequiresControlledReads.into());
                }
                let cache = match &markers {
                    MarkerAccess::Durable(store) => store.snapshot_cache(),
                    MarkerAccess::Local | MarkerAccess::Unavailable => None,
                }
                .ok_or(ConfigError::MarkerSnapshotCapabilityUnavailable)?;
                MarkerLifecycleAccess::CachedSnapshots(cache)
            }
        };
        let storage = match (self.distributed, self.serializer) {
            (Some(backend), Some(serializer)) => Storage::Hybrid {
                backend,
                serializer,
            },
            (None, _) => Storage::MemoryOnly,
            (Some(_), None) => return Err(ConfigError::DistributedWithoutSerializer.into()),
        };
        let clock: Arc<dyn Clock> = self.clock.unwrap_or_else(|| Arc::new(SystemClock));
        let recovery = if recovery_enabled {
            Some(AutoRecoveryService::try_new(
                self.recovery_config,
                Arc::clone(&clock),
            )?)
        } else {
            None
        };
        let events = Events::with_capacity(self.events_capacity);
        let plugins = PluginHost::try_new(
            PluginContext::new(&*name, &*instance_id, events.clone())?,
            self.plugins,
        )?;
        let expiry = match clock.timing_model() {
            ClockTiming::RealTime => MemoryExpiry::RealTime,
            ClockTiming::Controlled => MemoryExpiry::ClockDriven,
        };
        let marker_reads = match self.marker_read_policy {
            MarkerReadPolicy::DurableRequired => MarkerReads::DurableRequired,
            MarkerReadPolicy::OptionsControlled => {
                MarkerReads::OptionsControlled(Box::new(MarkerObservations::new(
                    self.marker_read_limits,
                    Arc::clone(&clock),
                    expiry,
                    marker_lifecycle,
                )))
            }
        };
        let inner = Arc::new_cyclic(|owner| CacheInner {
            owner: owner.clone(),
            name,
            instance_id,
            memory: MemoryStore::with_clock_and_expiry(
                MemoryLimits::new(self.max_capacity, self.max_weighted_capacity),
                events.clone(),
                Arc::clone(&clock),
                expiry,
            ),
            locks: KeyedLock::new(self.lock_shards),
            lanes: Lanes::new(),
            tags: TagRegistry::new(),
            events,
            clock,
            default_options: self.default_options,
            tags_default_options: self.tags_default_options,
            marker_reads,
            key_prefix: self.key_prefix,
            remove_by_tag_behavior: self.remove_by_tag_behavior,
            storage,
            serialization_mode: self.serialization_mode,
            markers,
            scope,
            backplane: self.backplane,
            distributed_locker: self.distributed_locker,
            lease_policy: self.lease_policy,
            lease_ttl,
            circuit_l2: CircuitBreaker::new(self.distributed_circuit_breaker),
            circuit_backplane: CircuitBreaker::new(self.backplane_circuit_breaker),
            plugins,
            recovery: recovery.clone(),
            default_options_provider: self.default_options_provider,
            ignore_incoming_backplane: self.ignore_incoming_backplane,
            distributed_wire_version: self.distributed_wire_version,
            distributed_key_modifier_mode: self.distributed_key_modifier_mode,
            disable_tagging: self.disable_tagging,
            wait_for_initial_backplane_subscribe: self.wait_for_initial_backplane_subscribe,
            cloner,
            jitter: self.jitter,
            scopes: Scopes::new(),
            tasks: Tasks::new(),
            epoch: Arc::new(AtomicU64::new(0)),
            maintenance: AtomicBool::new(false),
            subscription_admitted: AtomicBool::new(false),
            reconciliation,
            lifecycle: std::sync::Mutex::new(Lifecycle::Running),
            shutdown_gate: tokio::sync::Mutex::new(()),
            marker_lane: Arc::new(tokio::sync::Mutex::new(())),
            health_seen: std::sync::Mutex::new(None),
            last_reconcile: std::sync::Mutex::new(Instant::now()),
        });
        if let Some(recovery) = &recovery {
            let executor: Arc<dyn RecoveryExecutor> = inner.clone();
            recovery.try_set_executor(Arc::downgrade(&executor))?;
            recovery.try_spawn()?;
        }
        if self.lease_policy == LeasePolicy::CooperativeLegacy && inner.distributed_locker.is_some()
        {
            tracing::warn!(cache=%inner.name,"explicit cooperative legacy lease policy: partition fencing is unavailable");
        }
        let cache = Cache {
            lifetime: Arc::new(PublicLifetime {
                inner: Arc::downgrade(&inner),
            }),
            inner,
        };
        cache.worker().start_listener();
        Ok(cache)
    }
}
