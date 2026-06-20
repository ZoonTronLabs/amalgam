//! `amalgam` — a robust, multi-level, fail-safe cache for Rust.
//!
//! An *amalgam* is a fusion of metals; this crate is a faithful, idiomatic Rust
//! port of the resiliency model pioneered by .NET's
//! [FusionCache](https://github.com/ZiggyCreatures/FusionCache). It fuses an
//! in-memory L1 cache with an optional distributed L2 cache and gives you the
//! features that make a cache *robust* rather than merely fast:
//!
//! * **Cache-stampede protection** — only one factory runs per key (single-flight).
//! * **Fail-safe** — serve a stale value when the factory fails, instead of erroring.
//! * **Soft / hard timeouts** — a slow factory can return a stale value immediately
//!   and finish in the background.
//! * **Eager refresh** — refresh proactively before expiration, off the hot path.
//! * **Adaptive caching** — the factory can change the entry's options per call.
//! * **Conditional refresh** — HTTP-style `NotModified` reuse of a stale value.
//! * **Tagging** — invalidate many entries at once, lazily, by tag.
//! * **L1 + L2 + backplane** — pluggable distributed cache and multi-node sync.
//!
//! See `PORTING.md` for the C#→Rust translation methodology and `docs/PARITY.md`
//! for the feature-by-feature mapping to FusionCache.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod backplane;
pub mod cache;
pub mod circuit;
pub mod distributed;
pub mod distributed_lock;
pub mod entry;
pub mod error;
pub mod events;
pub mod factory;
pub mod locking;
pub mod maybe;
pub mod memory;
pub mod observability;
pub mod options;
pub mod plugins;
pub mod recovery;
pub mod registry;
pub mod serializers;
pub mod tags;
pub mod time;

#[cfg(feature = "redis")]
pub mod redis_backend;

pub use backplane::{Backplane, BackplaneAction, BackplaneMessage, InProcessBackplane};
pub use cache::{Cache, CacheBuilder};
pub use circuit::CircuitBreaker;
pub use distributed::{
    DistributedCache, DistributedEntry, DistributedSerializer, InMemoryDistributedCache,
    JsonSerializer,
};
pub use distributed_lock::{DistributedLocker, InMemoryDistributedLocker};
pub use error::{Error, FactoryError, Result};
pub use events::{CacheEvent, CircuitComponent, Events};
pub use factory::{FactoryContext, FactoryProduct, ModifiedBuilder};
pub use maybe::MaybeValue;
pub use options::{EagerThreshold, EntryOptions, Priority, RemoveByTagBehavior};
pub use plugins::{Plugin, PluginHost};
pub use recovery::{
    AutoRecoveryService, RecoveryAction, RecoveryConfig, RecoveryExecutor, RecoveryItem,
};
pub use registry::{CacheRegistry, DefaultEntryOptionsProvider};
pub use tags::Tag;
pub use time::{Clock, ManualClock, SystemClock, Timeout, Timestamp};

#[cfg(feature = "messagepack")]
pub use serializers::MessagePackSerializer;

#[cfg(feature = "metrics")]
pub use observability::MetricsPlugin;

#[cfg(feature = "redis")]
pub use redis_backend::{RedisBackplane, RedisDistributedCache, RedisDistributedLocker};
