//! Named caches with independent, exactly-once initialization slots.

use std::borrow::Borrow;
use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, RwLock};
use std::thread::ThreadId;

use crate::cache::Cache;
use crate::error::Error;
use crate::options::EntryOptions;

/// Supplies dynamic per-key defaults. None selects the static default options.
pub trait DefaultEntryOptionsProvider: Send + Sync {
    /// The options for this key, or no dynamic override.
    /// Legacy implementations may override this hook alone.
    fn options_for(&self, _key: &str) -> Option<EntryOptions> {
        None
    }

    /// Resolves an override against this cache's current default snapshot.
    /// Explicit operation options bypass this provider. The key is the raw
    /// application key, before the physical namespace is applied.
    fn options_for_with_defaults(
        &self,
        key: &str,
        _defaults: &EntryOptions,
    ) -> Option<EntryOptions> {
        self.options_for(key)
    }
}

/// A named-cache resolution failure.
#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    /// Registry names must identify a cache.
    #[error("registry name must not be blank")]
    BlankName,
    /// A builder recursively requested the name it is already initializing.
    #[error("recursive initialization of cache {name}")]
    RecursiveInitialization {
        /// The recursively requested name.
        name: String,
    },
    /// The cache builder failed; later callers may retry initialization.
    #[error("cache {name} could not be built: {source}")]
    Build {
        /// The requested registry name.
        name: String,
        /// The original typed cache failure.
        #[source]
        source: Box<Error>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RegistryName(Arc<str>);

impl RegistryName {
    fn new(name: &str) -> Result<Self, RegistryError> {
        if name.trim().is_empty() {
            return Err(RegistryError::BlankName);
        }
        Ok(Self(Arc::from(name)))
    }
}

impl Borrow<str> for RegistryName {
    fn borrow(&self) -> &str {
        &self.0
    }
}

enum Initialization<V: Clone + Send + Sync + 'static> {
    Vacant,
    Initializing(ThreadId),
    Ready(Cache<V>),
}

struct Slot<V: Clone + Send + Sync + 'static> {
    state: Mutex<Initialization<V>>,
    changed: Condvar,
}

impl<V: Clone + Send + Sync + 'static> Slot<V> {
    fn new() -> Self {
        Self {
            state: Mutex::new(Initialization::Vacant),
            changed: Condvar::new(),
        }
    }

    fn ready(&self) -> Option<Cache<V>> {
        match &*lock(&self.state) {
            Initialization::Ready(cache) => Some(cache.clone()),
            Initialization::Vacant | Initialization::Initializing(_) => None,
        }
    }
}

struct InitializationGuard<'a, V: Clone + Send + Sync + 'static> {
    slot: &'a Slot<V>,
    owner: ThreadId,
}

impl<V: Clone + Send + Sync + 'static> Drop for InitializationGuard<'_, V> {
    fn drop(&mut self) {
        let mut state = lock(&self.slot.state);
        if matches!(*state, Initialization::Initializing(owner) if owner == self.owner) {
            *state = Initialization::Vacant;
            self.slot.changed.notify_all();
        }
    }
}

/// A registry whose same-name builders coalesce while different names proceed
/// independently. User builders run outside all registry locks.
pub struct CacheRegistry<V: Clone + Send + Sync + 'static> {
    caches: RwLock<HashMap<RegistryName, Arc<Slot<V>>>>,
}

impl<V: Clone + Send + Sync + 'static> CacheRegistry<V> {
    /// Creates an empty registry without requiring an async runtime.
    #[must_use]
    pub fn new() -> Self {
        Self {
            caches: RwLock::new(HashMap::new()),
        }
    }

    fn slot(&self, name: &str) -> Result<Arc<Slot<V>>, RegistryError> {
        let key = RegistryName::new(name)?;
        if let Some(slot) = self
            .caches
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(name)
            .cloned()
        {
            return Ok(slot);
        }
        let mut map = self
            .caches
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok(Arc::clone(
            map.entry(key).or_insert_with(|| Arc::new(Slot::new())),
        ))
    }

    /// Registers/replaces a cache. Replacement supersedes an in-flight builder.
    /// Panics only for an invalid developer-supplied registry name; use
    /// try_register for expected configuration rejection.
    pub fn register(&self, name: impl Into<String>, cache: Cache<V>) {
        if let Err(error) = self.try_register(&name.into(), cache) {
            panic!("invalid registry configuration: {error}");
        }
    }

    /// Registers/replaces a cache with typed name validation.
    pub fn try_register(&self, name: &str, cache: Cache<V>) -> Result<(), RegistryError> {
        let slot = self.slot(name)?;
        let retired = {
            let mut state = lock(&slot.state);
            let retired = std::mem::replace(&mut *state, Initialization::Ready(cache));
            slot.changed.notify_all();
            retired
        };
        // Plugin teardown/destructors may reenter the registry.
        drop(retired);
        Ok(())
    }

    /// Resolves a fully initialized cache. An in-flight builder is not registered
    /// yet; get_or_create waits for its result instead.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<Cache<V>> {
        let slot = self
            .caches
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(name)
            .cloned()?;
        slot.ready()
    }

    /// Legacy infallible adapter. Same-name recursive initialization is a
    /// developer contract violation instead of a permanent deadlock.
    pub fn get_or_create(&self, name: &str, build: impl FnOnce() -> Cache<V>) -> Cache<V> {
        match self.try_get_or_create(name, || Ok(build())) {
            Ok(cache) => cache,
            Err(error) => panic!("cache registry initialization failed: {error}"),
        }
    }

    /// Coalesces a fallible builder, retaining only successful initialization.
    /// Failed or panicked builders release the slot so waiting callers can retry.
    pub fn try_get_or_create(
        &self,
        name: &str,
        build: impl FnOnce() -> crate::Result<Cache<V>>,
    ) -> Result<Cache<V>, RegistryError> {
        let slot = self.slot(name)?;
        let owner = std::thread::current().id();
        let mut state = lock(&slot.state);
        loop {
            match &*state {
                Initialization::Ready(cache) => return Ok(cache.clone()),
                Initialization::Initializing(initializer) if *initializer == owner => {
                    return Err(RegistryError::RecursiveInitialization {
                        name: name.to_owned(),
                    });
                }
                Initialization::Initializing(_) => {
                    state = slot
                        .changed
                        .wait(state)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                }
                Initialization::Vacant => {
                    *state = Initialization::Initializing(owner);
                    break;
                }
            }
        }
        drop(state);
        let _guard = InitializationGuard { slot: &slot, owner };
        let candidate = build().map_err(|source| RegistryError::Build {
            name: name.to_owned(),
            source: Box::new(source),
        })?;
        let mut state = lock(&slot.state);
        match &*state {
            Initialization::Ready(cache) => Ok(cache.clone()),
            Initialization::Initializing(initializer) if *initializer == owner => {
                *state = Initialization::Ready(candidate.clone());
                slot.changed.notify_all();
                Ok(candidate)
            }
            Initialization::Vacant | Initialization::Initializing(_) => {
                unreachable!("only the owner guard can clear an initializing slot")
            }
        }
    }

    /// Counts registered caches rather than unfinished/failed builder slots.
    #[must_use]
    pub fn len(&self) -> usize {
        let slots: Vec<_> = self
            .caches
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .cloned()
            .collect();
        slots.iter().filter(|slot| slot.ready().is_some()).count()
    }

    /// Whether no cache has successfully registered.
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

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
