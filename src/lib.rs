//! `amalgam` — a hybrid cache with async and native sync APIs for Rust, inspired by .NET
//! [FusionCache](https://github.com/ZiggyCreatures/FusionCache).
//!
//! Local caching combines optional distributed storage, fail-safe retention,
//! controlled origin work and observable mutations. Same-key ownership prevents
//! ordinary stampedes; an explicitly finite lock wait can permit best-effort
//! origin work. Different keys have independent flights.
//!
//! New callers should use [`CacheBuilder::try_build`], fallible [`Cache::try_get`],
//! typed mutation receipts and [`Cache::shutdown`]. Ordinary origin failure can
//! activate an eligible stale fallback; cancellation remains a typed error.
//! Soft-timeout continuation and eager refresh have explicit ownership, while
//! distributed effects retain their actual completion and recovery stage.
//!
//! Stores, codecs, backplanes, plugins and value-copy strategies are extensible
//! traits. Durable invalidation and strict stale-owner rejection require their
//! corresponding atomic provider capabilities. `Clone` alone does not promise
//! isolation of shared mutable state.
//!
//! The 0.3 source version uses a v2 distributed namespace. New codecs accept
//! legacy payloads; older running readers need a coordinated fresh namespace.
//! See the packaged README, `docs/PARITY.md`, `docs/AUDIT.md` and `PORTING.md`
//! for supported contracts, migration and verification limits.

#![deny(unsafe_code)]
#![warn(missing_docs)]

pub mod advanced;
pub mod provider;

pub mod backplane;
pub mod cache;
pub mod circuit;
pub mod commit;
pub mod distributed;
pub mod distributed_lock;
pub mod entry;
pub mod error;
pub mod events;
mod execution;
pub mod factory;
mod lifecycle;
pub mod locking;
mod marker_leases;
pub mod marker_reads;
pub mod marker_snapshots;
pub mod memory;
pub mod memory_locker;
pub mod memory_storage;
pub mod observability;
pub mod options;
pub mod plugins;
mod retained_origin;
mod single_flight;
pub mod source;
// Owner-approved private performance boundary. Cache logic keeps the unsafe ban.
#[allow(unsafe_code)]
mod reader_slots;
pub mod recovery;
pub mod registry;
pub mod serializers;
pub mod tags;
pub mod time;

#[cfg(feature = "opentelemetry")]
pub mod otel;

#[cfg(feature = "redis")]
pub mod redis_backend;

pub use cache::{
    BackplaneReadiness, BlockingCache, BlockingCacheBuildError, Cache, CacheBuilder, ClearMode,
    CloseOutcome, ShutdownReport,
};

pub use error::{
    CloneError, CodecError, ConfigError, Error, FactoryCancellationReason, FactoryError, Result,
    ShutdownError, TransportError,
};

pub use events::{
    CacheEvent, CacheLevel, CacheOperation, EventStreamClosed, EventSubscription, Events,
    OperationOutcome,
};

pub use execution::{CancellationSource, FactoryCancellation};

pub(crate) use factory::FactoryProduct;
pub use factory::{ConditionalRefreshError, FactoryContext};

pub use options::{EagerThreshold, EntryOptions, EntryWeight, Priority, RemoveByTagBehavior};

pub use plugins::{
    Plugin, PluginContext, PluginError, PluginRegistration, PluginSession, PluginStage,
    PluginStopOutcome,
};

pub use recovery::RecoveryConfig;

pub use registry::{CacheRegistry, RegistryError};

pub use tags::{Tag, TagError};

pub use time::{Timeout, Timestamp};

#[cfg(feature = "metrics")]
pub use observability::MetricsPlugin;

#[cfg(feature = "opentelemetry")]
pub use observability::OtelMetricsPlugin;
