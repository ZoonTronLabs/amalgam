//! Named caches and dynamic default options.
//!
//! FusionCache supports multiple named caches resolved from a DI container, plus
//! a `DefaultEntryOptionsProvider` that supplies per-key defaults. The Rust
//! equivalents: a [`CacheRegistry`] of named caches and the
//! [`DefaultEntryOptionsProvider`] trait consulted when a call supplies no
//! explicit options.

use std::collections::HashMap;
use std::sync::RwLock;

use crate::cache::Cache;
use crate::options::EntryOptions;

/// Supplies default [`EntryOptions`] dynamically, keyed by cache key.
///
/// Returning `None` falls back to the cache's static default options.
pub trait DefaultEntryOptionsProvider: Send + Sync {
    /// The options to use for `key`, or `None` to fall back to the static default.
    fn options_for(&self, key: &str) -> Option<EntryOptions>;
}

/// A thread-safe registry of named caches of a single value type.
///
/// Use this where FusionCache uses keyed/named caches from DI: register a cache
/// under a name once, then resolve it anywhere.
pub struct CacheRegistry<V: Clone + Send + Sync + 'static> {
    caches: RwLock<HashMap<String, Cache<V>>>,
}

impl<V: Clone + Send + Sync + 'static> CacheRegistry<V> {
    /// Creates an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            caches: RwLock::new(HashMap::new()),
        }
    }

    /// Registers (or replaces) a named cache.
    pub fn register(&self, name: impl Into<String>, cache: Cache<V>) {
        if let Ok(mut map) = self.caches.write() {
            map.insert(name.into(), cache);
        }
    }

    /// Resolves a named cache.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<Cache<V>> {
        self.caches.read().ok()?.get(name).cloned()
    }

    /// Resolves a named cache, building and registering it on first use.
    pub fn get_or_create(&self, name: &str, build: impl FnOnce() -> Cache<V>) -> Cache<V> {
        if let Some(existing) = self.get(name) {
            return existing;
        }
        let cache = build();
        self.register(name.to_owned(), cache.clone());
        cache
    }

    /// The number of registered caches.
    #[must_use]
    pub fn len(&self) -> usize {
        self.caches.read().map(|m| m.len()).unwrap_or(0)
    }

    /// `true` if no caches are registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<V: Clone + Send + Sync + 'static> Default for CacheRegistry<V> {
    fn default() -> Self {
        Self::new()
    }
}
