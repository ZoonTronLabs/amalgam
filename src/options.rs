//! Per-entry options ([`EntryOptions`]) and their supporting value types.
//!
//! This is the Rust counterpart of FusionCache's `FusionCacheEntryOptions`.
//! Every field carries the same default as FusionCache (see `docs/PARITY.md`),
//! with two deliberate, type-driven improvements:
//!
//! * timeouts are a [`Timeout`] enum rather than a `-1ms` negative sentinel;
//! * the eager-refresh threshold is an [`EagerThreshold`] newtype that can only
//!   hold a value in the open interval `(0, 1)`.

use std::time::Duration;

use crate::time::{Timeout, Timestamp, duration_to_ticks};

/// Memory-eviction priority hint, mirroring `CacheItemPriority`.
///
/// The in-memory backend (moka) uses a TinyLFU policy and does not honour an
/// explicit priority; this is retained for API parity and forwarded where a
/// backend can use it. `NeverRemove` entries (e.g. internal tag markers) are
/// kept in a dedicated never-evicting structure instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Priority {
    /// Evicted first under memory pressure.
    Low,
    /// The default priority.
    #[default]
    Normal,
    /// Evicted last under memory pressure.
    High,
    /// Never evicted by the size policy.
    NeverRemove,
}

/// What [`Cache::remove_by_tag`](crate::Cache::remove_by_tag) does to matched
/// entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RemoveByTagBehavior {
    /// Logically expire matched entries (fail-safe can still serve them).
    /// This is the FusionCache default.
    #[default]
    Expire,
    /// Hard-remove matched entries.
    Remove,
}

/// How the L2 wire-format version is combined with a cache key, mirroring
/// FusionCache's `CacheKeyModifierMode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum KeyModifierMode {
    /// Prepend the version: `v1:key` (the default — different cache versions can
    /// share one L2 without colliding).
    #[default]
    Prefix,
    /// Append the version: `key:v1`.
    Suffix,
    /// Leave the key unmodified.
    None,
}

/// A validated eager-refresh threshold: a fraction strictly between 0 and 1.
///
/// A threshold of `0.8` means "once 80% of the entry's duration has elapsed,
/// kick off a non-blocking background refresh". Values outside the open
/// interval `(0, 1)` are rejected (returning `None`) rather than silently
/// clamped, matching FusionCache's coercion of out-of-range values to "disabled".
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EagerThreshold(f32);

impl EagerThreshold {
    /// Creates a threshold, or `None` if `fraction` is not in `(0, 1)`.
    #[must_use]
    pub fn new(fraction: f32) -> Option<Self> {
        if fraction > 0.0 && fraction < 1.0 {
            Some(Self(fraction))
        } else {
            None
        }
    }

    /// The underlying fraction, guaranteed to be in `(0, 1)`.
    #[must_use]
    pub fn fraction(self) -> f32 {
        self.0
    }
}

/// Options controlling how a single entry is cached.
///
/// Build a baseline once (often via [`Cache::entry_options`](crate::Cache::entry_options),
/// which clones the cache's defaults) and tweak per call with the chainable
/// `with_*` methods.
#[derive(Debug, Clone)]
pub struct EntryOptions {
    // ---- expiration ----
    duration: Duration,
    memory_duration: Option<Duration>,
    distributed_duration: Option<Duration>,
    eager_refresh_threshold: Option<EagerThreshold>,
    jitter_max: Duration,

    // ---- locking ----
    lock_timeout: Timeout,
    // Specific overrides; each falls back to `lock_timeout` when `None`
    // (FusionCache `MemoryLockTimeout` / `DistributedLockTimeout`).
    memory_lock_timeout: Option<Timeout>,
    distributed_lock_timeout: Option<Timeout>,

    // ---- fail-safe ----
    is_fail_safe_enabled: bool,
    fail_safe_max_duration: Duration,
    fail_safe_throttle_duration: Duration,
    distributed_fail_safe_max_duration: Option<Duration>,

    // ---- factory timeouts ----
    factory_soft_timeout: Timeout,
    factory_hard_timeout: Timeout,
    allow_timed_out_factory_background_completion: bool,

    // ---- memory (L1) ----
    skip_memory_read: bool,
    skip_memory_write: bool,
    priority: Priority,
    size: Option<i64>,
    allow_stale_on_read_only: bool,

    // ---- distributed (L2) ----
    distributed_soft_timeout: Timeout,
    distributed_hard_timeout: Timeout,
    allow_background_distributed_operations: bool,
    rethrow_distributed_exceptions: bool,
    rethrow_serialization_exceptions: bool,
    skip_distributed_read: bool,
    skip_distributed_write: bool,
    skip_distributed_read_when_stale: bool,
    skip_distributed_locker: bool,
    enable_auto_clone: bool,

    // ---- backplane ----
    skip_backplane_notifications: bool,
    allow_background_backplane_operations: bool,
    rethrow_backplane_exceptions: bool,
}

impl Default for EntryOptions {
    fn default() -> Self {
        Self {
            duration: Duration::from_secs(30),
            memory_duration: None,
            distributed_duration: None,
            eager_refresh_threshold: None,
            jitter_max: Duration::ZERO,

            lock_timeout: Timeout::Infinite,
            memory_lock_timeout: None,
            distributed_lock_timeout: None,

            is_fail_safe_enabled: false,
            fail_safe_max_duration: Duration::from_secs(60 * 60 * 24), // 1 day
            fail_safe_throttle_duration: Duration::from_secs(30),
            distributed_fail_safe_max_duration: None,

            factory_soft_timeout: Timeout::Infinite,
            factory_hard_timeout: Timeout::Infinite,
            allow_timed_out_factory_background_completion: true,

            skip_memory_read: false,
            skip_memory_write: false,
            priority: Priority::Normal,
            size: None,
            allow_stale_on_read_only: false,

            distributed_soft_timeout: Timeout::Infinite,
            distributed_hard_timeout: Timeout::Infinite,
            allow_background_distributed_operations: false,
            rethrow_distributed_exceptions: false,
            rethrow_serialization_exceptions: true,
            skip_distributed_read: false,
            skip_distributed_write: false,
            skip_distributed_read_when_stale: false,
            skip_distributed_locker: false,
            enable_auto_clone: false,

            skip_backplane_notifications: false,
            allow_background_backplane_operations: true,
            rethrow_backplane_exceptions: false,
        }
    }
}

impl EntryOptions {
    /// Creates options with the given logical duration and all other fields at
    /// their defaults.
    #[must_use]
    pub fn new(duration: Duration) -> Self {
        Self {
            duration,
            ..Self::default()
        }
    }

    // ----------------------------------------------------------------- builders

    /// Sets the logical duration (the freshness window).
    #[must_use]
    pub fn with_duration(mut self, duration: Duration) -> Self {
        self.duration = duration;
        self
    }

    /// Overrides the L1-only logical duration (defaults to [`duration`](Self::duration)).
    #[must_use]
    pub fn with_memory_duration(mut self, duration: Duration) -> Self {
        self.memory_duration = Some(duration);
        self
    }

    /// Overrides the L2-only logical duration (defaults to [`duration`](Self::duration)).
    #[must_use]
    pub fn with_distributed_duration(mut self, duration: Duration) -> Self {
        self.distributed_duration = Some(duration);
        self
    }

    /// Enables eager (proactive, background) refresh at the given threshold.
    /// A `None` threshold disables eager refresh.
    #[must_use]
    pub fn with_eager_refresh(mut self, threshold: Option<EagerThreshold>) -> Self {
        self.eager_refresh_threshold = threshold;
        self
    }

    /// Sets the maximum random jitter added to a *fresh* entry's logical
    /// expiration (anti-stampede across nodes).
    #[must_use]
    pub fn with_jitter_max(mut self, jitter_max: Duration) -> Self {
        self.jitter_max = jitter_max;
        self
    }

    /// Sets the general maximum wait for the per-key single-flight lock — the
    /// fallback for the memory- and distributed-specific lock timeouts.
    #[must_use]
    pub fn with_lock_timeout(mut self, timeout: Timeout) -> Self {
        self.lock_timeout = timeout;
        self
    }

    /// Overrides the wait for the in-memory single-flight lock (defaults to
    /// [`with_lock_timeout`](Self::with_lock_timeout)). FusionCache `MemoryLockTimeout`.
    #[must_use]
    pub fn with_memory_lock_timeout(mut self, timeout: Timeout) -> Self {
        self.memory_lock_timeout = Some(timeout);
        self
    }

    /// Overrides the wait for the cross-node distributed lock (defaults to
    /// [`with_lock_timeout`](Self::with_lock_timeout)). FusionCache `DistributedLockTimeout`.
    #[must_use]
    pub fn with_distributed_lock_timeout(mut self, timeout: Timeout) -> Self {
        self.distributed_lock_timeout = Some(timeout);
        self
    }

    /// Enables or disables fail-safe and (optionally) tunes its windows.
    ///
    /// Mirrors FusionCache's `SetFailSafe(isEnabled, maxDuration?, throttleDuration?)`.
    #[must_use]
    pub fn with_fail_safe(
        mut self,
        enabled: bool,
        max_duration: Option<Duration>,
        throttle_duration: Option<Duration>,
    ) -> Self {
        self.is_fail_safe_enabled = enabled;
        if let Some(max) = max_duration {
            self.fail_safe_max_duration = max;
        }
        if let Some(throttle) = throttle_duration {
            self.fail_safe_throttle_duration = throttle;
        }
        self
    }

    /// Sets the factory soft/hard timeouts and whether a timed-out factory keeps
    /// running in the background to update the cache.
    #[must_use]
    pub fn with_factory_timeouts(
        mut self,
        soft: Timeout,
        hard: Timeout,
        allow_background_completion: bool,
    ) -> Self {
        self.factory_soft_timeout = soft;
        self.factory_hard_timeout = hard;
        self.allow_timed_out_factory_background_completion = allow_background_completion;
        self
    }

    /// Sets the memory-eviction priority.
    #[must_use]
    pub fn with_priority(mut self, priority: Priority) -> Self {
        self.priority = priority;
        self
    }

    /// Sets the entry's size weight (forwarded to the backend's size policy).
    #[must_use]
    pub fn with_size(mut self, size: i64) -> Self {
        self.size = Some(size);
        self
    }

    /// Allows read-only methods ([`try_get`](crate::Cache::try_get),
    /// [`get_or_default`](crate::Cache::get_or_default)) to return a stale value.
    #[must_use]
    pub fn with_allow_stale_on_read_only(mut self, allow: bool) -> Self {
        self.allow_stale_on_read_only = allow;
        self
    }

    /// Skips reading from / writing to the L1 memory cache for this operation.
    #[must_use]
    pub fn with_skip_memory(mut self, skip_read: bool, skip_write: bool) -> Self {
        self.skip_memory_read = skip_read;
        self.skip_memory_write = skip_write;
        self
    }

    /// Skips reading from / writing to the L2 distributed cache for this operation.
    #[must_use]
    pub fn with_skip_distributed(mut self, skip_read: bool, skip_write: bool) -> Self {
        self.skip_distributed_read = skip_read;
        self.skip_distributed_write = skip_write;
        self
    }

    /// When the L1 entry is stale, skip reading L2 (a multi-node freshness
    /// optimization).
    #[must_use]
    pub fn with_skip_distributed_read_when_stale(mut self, skip: bool) -> Self {
        self.skip_distributed_read_when_stale = skip;
        self
    }

    /// Sets the L2 soft/hard timeouts.
    #[must_use]
    pub fn with_distributed_timeouts(mut self, soft: Timeout, hard: Timeout) -> Self {
        self.distributed_soft_timeout = soft;
        self.distributed_hard_timeout = hard;
        self
    }

    /// Performs L2 writes in the background (fire-and-forget) instead of awaiting.
    #[must_use]
    pub fn with_allow_background_distributed_operations(mut self, allow: bool) -> Self {
        self.allow_background_distributed_operations = allow;
        self
    }

    /// Skips publishing a backplane notification for this operation.
    #[must_use]
    pub fn with_skip_backplane_notifications(mut self, skip: bool) -> Self {
        self.skip_backplane_notifications = skip;
        self
    }

    // ----------------------------------------------------------------- accessors

    /// The logical duration (freshness window).
    #[must_use]
    pub fn duration(&self) -> Duration {
        self.duration
    }

    /// `true` if fail-safe is enabled.
    #[must_use]
    pub fn is_fail_safe_enabled(&self) -> bool {
        self.is_fail_safe_enabled
    }

    /// The fail-safe throttle window: after a fail-safe activation, how long the
    /// stale value is served before the factory is retried.
    #[must_use]
    pub fn fail_safe_throttle_duration(&self) -> Duration {
        self.fail_safe_throttle_duration
    }

    /// The eager-refresh threshold, if enabled.
    #[must_use]
    pub fn eager_refresh_threshold(&self) -> Option<EagerThreshold> {
        self.eager_refresh_threshold
    }

    /// The general lock timeout — the fallback for the memory- and
    /// distributed-specific lock timeouts.
    #[must_use]
    pub fn lock_timeout(&self) -> Timeout {
        self.lock_timeout
    }

    /// The effective in-memory single-flight lock timeout (`memory_lock_timeout`,
    /// or [`lock_timeout`](Self::lock_timeout) when unset).
    #[must_use]
    pub fn memory_lock_timeout(&self) -> Timeout {
        self.memory_lock_timeout.unwrap_or(self.lock_timeout)
    }

    /// The effective cross-node distributed lock timeout
    /// (`distributed_lock_timeout`, or [`lock_timeout`](Self::lock_timeout) when unset).
    #[must_use]
    pub fn distributed_lock_timeout(&self) -> Timeout {
        self.distributed_lock_timeout.unwrap_or(self.lock_timeout)
    }

    /// `true` if a timed-out factory is allowed to finish in the background.
    #[must_use]
    pub fn allow_timed_out_factory_background_completion(&self) -> bool {
        self.allow_timed_out_factory_background_completion
    }

    /// `true` if L1 reads should be skipped.
    #[must_use]
    pub fn skip_memory_read(&self) -> bool {
        self.skip_memory_read
    }

    /// `true` if L1 writes should be skipped.
    #[must_use]
    pub fn skip_memory_write(&self) -> bool {
        self.skip_memory_write
    }

    /// `true` if read-only methods may return a stale value.
    #[must_use]
    pub fn allow_stale_on_read_only(&self) -> bool {
        self.allow_stale_on_read_only
    }

    /// The memory-eviction priority.
    #[must_use]
    pub fn priority(&self) -> Priority {
        self.priority
    }

    /// `true` if backplane notifications are suppressed for this operation.
    #[must_use]
    pub fn skip_backplane_notifications(&self) -> bool {
        self.skip_backplane_notifications
    }

    /// `true` if a backplane publish may run in the background.
    #[must_use]
    pub fn allow_background_backplane_operations(&self) -> bool {
        self.allow_background_backplane_operations
    }

    /// `true` if L2 reads should be skipped.
    #[must_use]
    pub fn skip_distributed_read(&self) -> bool {
        self.skip_distributed_read
    }

    /// `true` if L2 reads should be skipped when the L1 entry is stale.
    #[must_use]
    pub fn skip_distributed_read_when_stale(&self) -> bool {
        self.skip_distributed_read_when_stale
    }

    /// `true` if the cross-node distributed lock should be skipped for this op.
    #[must_use]
    pub fn skip_distributed_locker(&self) -> bool {
        self.skip_distributed_locker
    }

    /// Skips the cross-node distributed lock for this operation.
    #[must_use]
    pub fn with_skip_distributed_locker(mut self, skip: bool) -> Self {
        self.skip_distributed_locker = skip;
        self
    }

    /// `true` if L1 values should be (deep-)cloned out on read.
    ///
    /// In Rust this is effectively always true: reads return an owned `V`
    /// (`value_cloned`), so a caller mutating the returned value never affects the
    /// cached copy. The flag exists for API parity and to opt into the same
    /// guarantee explicitly.
    #[must_use]
    pub fn enable_auto_clone(&self) -> bool {
        self.enable_auto_clone
    }

    /// Enables auto-clone of L1 values on read (see [`enable_auto_clone`](Self::enable_auto_clone)).
    #[must_use]
    pub fn with_enable_auto_clone(mut self, enable: bool) -> Self {
        self.enable_auto_clone = enable;
        self
    }

    /// The L2 soft timeout (awaited L2 op when a fallback exists).
    #[must_use]
    pub fn distributed_soft_timeout(&self) -> Timeout {
        self.distributed_soft_timeout
    }

    /// The L2 hard timeout (always enforced on an awaited L2 op).
    #[must_use]
    pub fn distributed_hard_timeout(&self) -> Timeout {
        self.distributed_hard_timeout
    }

    /// `true` if L2 writes should be skipped.
    #[must_use]
    pub fn skip_distributed_write(&self) -> bool {
        self.skip_distributed_write
    }

    /// `true` if L2 writes may run in the background.
    #[must_use]
    pub fn allow_background_distributed_operations(&self) -> bool {
        self.allow_background_distributed_operations
    }

    /// `true` if L2 backend exceptions should bubble to the caller.
    #[must_use]
    pub fn rethrow_distributed_exceptions(&self) -> bool {
        self.rethrow_distributed_exceptions
    }

    /// `true` if (de)serialization errors should bubble to the caller (the
    /// FusionCache default is `true`).
    #[must_use]
    pub fn rethrow_serialization_exceptions(&self) -> bool {
        self.rethrow_serialization_exceptions
    }

    /// `true` if backplane exceptions should bubble to the caller.
    #[must_use]
    pub fn rethrow_backplane_exceptions(&self) -> bool {
        self.rethrow_backplane_exceptions
    }

    // ----------------------------------------------------------------- resolved

    /// The effective L1 logical duration (`memory_duration` or `duration`).
    #[must_use]
    pub fn resolved_memory_duration(&self) -> Duration {
        self.memory_duration.unwrap_or(self.duration)
    }

    /// The effective L2 logical duration (`distributed_duration` or `duration`).
    #[must_use]
    pub fn resolved_distributed_duration(&self) -> Duration {
        self.distributed_duration.unwrap_or(self.duration)
    }

    /// The effective L2 fail-safe max duration.
    #[must_use]
    pub fn resolved_distributed_fail_safe_max_duration(&self) -> Duration {
        self.distributed_fail_safe_max_duration
            .unwrap_or(self.fail_safe_max_duration)
    }

    /// The physical time-to-live handed to the L1 backend.
    ///
    /// With fail-safe enabled this is `max(duration, fail_safe_max_duration)`
    /// — **not** the sum — so an entry physically survives long enough to be
    /// reused as a stale fallback. Without fail-safe it is just the logical
    /// duration.
    #[must_use]
    pub fn physical_ttl(&self) -> Duration {
        let logical = self.resolved_memory_duration();
        if self.is_fail_safe_enabled {
            logical.max(self.fail_safe_max_duration)
        } else {
            logical
        }
    }

    /// The physical time-to-live handed to the L2 backend — the L2 analogue of
    /// [`physical_ttl`](Self::physical_ttl), using the distributed-specific
    /// duration and fail-safe-max overrides.
    #[must_use]
    pub fn distributed_physical_ttl(&self) -> Duration {
        let logical = self.resolved_distributed_duration();
        if self.is_fail_safe_enabled {
            logical.max(self.resolved_distributed_fail_safe_max_duration())
        } else {
            logical
        }
    }

    /// Selects the factory timeout to enforce for this call.
    ///
    /// The soft timeout only applies when fail-safe is on *and* a fallback value
    /// exists; the hard timeout always applies and wins when it is shorter.
    #[must_use]
    pub fn appropriate_factory_timeout(&self, has_fallback: bool) -> Timeout {
        let mut selected = Timeout::Infinite;
        if self.is_fail_safe_enabled && has_fallback && !self.factory_soft_timeout.is_infinite() {
            selected = self.factory_soft_timeout;
        }
        selected.min(self.factory_hard_timeout)
    }

    /// Selects the L2 (distributed) timeout to enforce for an awaited L2 read.
    ///
    /// Mirrors [`appropriate_factory_timeout`](Self::appropriate_factory_timeout):
    /// the distributed *soft* timeout only applies when fail-safe is on *and* a
    /// fallback value exists; the *hard* timeout always applies and wins when it
    /// is shorter (FusionCache `GetAppropriateDistributedCacheTimeout`).
    #[must_use]
    pub fn appropriate_distributed_timeout(&self, has_fallback: bool) -> Timeout {
        let mut selected = Timeout::Infinite;
        if self.is_fail_safe_enabled && has_fallback && !self.distributed_soft_timeout.is_infinite()
        {
            selected = self.distributed_soft_timeout;
        }
        selected.min(self.distributed_hard_timeout)
    }

    /// Computes the logical-expiration timestamp for a *fresh* entry created at
    /// `created`, applying jitter (if any).
    #[must_use]
    pub fn logical_expiration(&self, created: Timestamp) -> Timestamp {
        let base = created.saturating_add(self.resolved_memory_duration());
        if self.jitter_max.is_zero() {
            base
        } else {
            let max_ticks = duration_to_ticks(self.jitter_max);
            let extra = if max_ticks > 0 {
                fastrand::i64(0..=max_ticks)
            } else {
                0
            };
            base.saturating_add(crate::time::ticks_to_duration(extra))
        }
    }

    /// Computes the eager-refresh trigger timestamp for an entry created at
    /// `created`, or `None` if eager refresh is disabled.
    #[must_use]
    pub fn eager_refresh_at(&self, created: Timestamp) -> Option<Timestamp> {
        let threshold = self.eager_refresh_threshold?;
        let window = self.resolved_memory_duration();
        let elapsed = window.mul_f32(threshold.fraction());
        Some(created.saturating_add(elapsed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eager_threshold_rejects_out_of_range() {
        assert!(EagerThreshold::new(0.5).is_some());
        assert!(EagerThreshold::new(0.0).is_none());
        assert!(EagerThreshold::new(1.0).is_none());
        assert!(EagerThreshold::new(-0.1).is_none());
        assert!(EagerThreshold::new(1.5).is_none());
    }

    #[test]
    fn physical_ttl_uses_max_not_sum() {
        let opts = EntryOptions::new(Duration::from_secs(10)).with_fail_safe(
            true,
            Some(Duration::from_secs(100)),
            None,
        );
        assert_eq!(opts.physical_ttl(), Duration::from_secs(100));

        let opts2 = EntryOptions::new(Duration::from_secs(200)).with_fail_safe(
            true,
            Some(Duration::from_secs(100)),
            None,
        );
        // max(200, 100) == 200, never the 300 sum.
        assert_eq!(opts2.physical_ttl(), Duration::from_secs(200));
    }

    #[test]
    fn physical_ttl_without_fail_safe_is_logical_duration() {
        let opts = EntryOptions::new(Duration::from_secs(10));
        assert_eq!(opts.physical_ttl(), Duration::from_secs(10));
    }

    #[test]
    fn soft_timeout_ignored_without_fallback() {
        let opts = EntryOptions::new(Duration::from_secs(10)).with_factory_timeouts(
            Timeout::After(Duration::from_millis(50)),
            Timeout::Infinite,
            true,
        );
        // Soft timeout requires fail-safe; here fail-safe is off, so infinite.
        assert_eq!(opts.appropriate_factory_timeout(true), Timeout::Infinite);
    }

    #[test]
    fn soft_timeout_applies_with_fail_safe_and_fallback() {
        let opts = EntryOptions::new(Duration::from_secs(10))
            .with_fail_safe(true, None, None)
            .with_factory_timeouts(
                Timeout::After(Duration::from_millis(50)),
                Timeout::Infinite,
                true,
            );
        assert_eq!(
            opts.appropriate_factory_timeout(true),
            Timeout::After(Duration::from_millis(50))
        );
        // No fallback ⇒ soft timeout does not apply.
        assert_eq!(opts.appropriate_factory_timeout(false), Timeout::Infinite);
    }

    #[test]
    fn hard_timeout_wins_when_shorter() {
        let opts = EntryOptions::new(Duration::from_secs(10))
            .with_fail_safe(true, None, None)
            .with_factory_timeouts(
                Timeout::After(Duration::from_millis(50)),
                Timeout::After(Duration::from_millis(20)),
                true,
            );
        assert_eq!(
            opts.appropriate_factory_timeout(true),
            Timeout::After(Duration::from_millis(20))
        );
    }

    #[test]
    fn distributed_soft_timeout_applies_only_with_fail_safe_and_fallback() {
        let opts = EntryOptions::new(Duration::from_secs(10))
            .with_fail_safe(true, None, None)
            .with_distributed_timeouts(
                Timeout::After(Duration::from_millis(50)),
                Timeout::Infinite,
            );
        assert_eq!(
            opts.appropriate_distributed_timeout(true),
            Timeout::After(Duration::from_millis(50))
        );
        // No fallback ⇒ soft does not apply.
        assert_eq!(
            opts.appropriate_distributed_timeout(false),
            Timeout::Infinite
        );

        // Fail-safe off ⇒ soft does not apply even with a fallback.
        let no_fs = EntryOptions::new(Duration::from_secs(10)).with_distributed_timeouts(
            Timeout::After(Duration::from_millis(50)),
            Timeout::Infinite,
        );
        assert_eq!(
            no_fs.appropriate_distributed_timeout(true),
            Timeout::Infinite
        );
    }

    #[test]
    fn distributed_hard_timeout_wins_when_shorter() {
        let opts = EntryOptions::new(Duration::from_secs(10))
            .with_fail_safe(true, None, None)
            .with_distributed_timeouts(
                Timeout::After(Duration::from_millis(50)),
                Timeout::After(Duration::from_millis(20)),
            );
        assert_eq!(
            opts.appropriate_distributed_timeout(true),
            Timeout::After(Duration::from_millis(20))
        );
    }

    #[test]
    fn lock_timeouts_inherit_general_by_default() {
        let opts = EntryOptions::new(Duration::from_secs(10))
            .with_lock_timeout(Timeout::After(Duration::from_millis(100)));
        assert_eq!(
            opts.memory_lock_timeout(),
            Timeout::After(Duration::from_millis(100))
        );
        assert_eq!(
            opts.distributed_lock_timeout(),
            Timeout::After(Duration::from_millis(100))
        );
    }

    #[test]
    fn specific_lock_timeouts_override_general() {
        let opts = EntryOptions::new(Duration::from_secs(10))
            .with_lock_timeout(Timeout::After(Duration::from_millis(100)))
            .with_memory_lock_timeout(Timeout::After(Duration::from_millis(20)))
            .with_distributed_lock_timeout(Timeout::Infinite);
        assert_eq!(
            opts.memory_lock_timeout(),
            Timeout::After(Duration::from_millis(20))
        );
        assert_eq!(opts.distributed_lock_timeout(), Timeout::Infinite);
        // The general fallback is untouched.
        assert_eq!(
            opts.lock_timeout(),
            Timeout::After(Duration::from_millis(100))
        );
    }

    #[test]
    fn jitter_widens_logical_expiration_within_bound() {
        let base_dur = Duration::from_secs(100);
        let jitter = Duration::from_secs(10);
        let created = Timestamp::from_ticks(0);

        // No jitter ⇒ exactly base.
        let plain = EntryOptions::new(base_dur);
        let base_exp = plain.logical_expiration(created);
        assert_eq!(plain.logical_expiration(created).ticks(), base_exp.ticks());

        // With jitter ⇒ always in [base, base + jitter], never shorter, never over.
        let jittered = EntryOptions::new(base_dur).with_jitter_max(jitter);
        let upper = base_exp.saturating_add(jitter).ticks();
        for _ in 0..200 {
            let exp = jittered.logical_expiration(created).ticks();
            assert!(exp >= base_exp.ticks(), "jitter never shortens expiration");
            assert!(exp <= upper, "jitter never exceeds jitter_max");
        }
    }
}
