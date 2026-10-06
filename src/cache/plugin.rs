//! Typed plugin access to the owning cache without a public lifetime cycle.
use super::{
    Arc, BlockingCache, BlockingRuntime, Cache, CacheInner, InlinePermit, PublicLifetime, Scopes,
    Weak, Worker,
};
use crate::plugins::{Plugin, PluginContext, PluginError, PluginSession};
use std::ops::Deref;
use std::sync::Mutex;

/// Open plugin behavior with access to the same typed cache as its caller.
/// Legacy [`Plugin`] implementations remain supported independently.
pub trait CachePlugin<V>: Send + Sync {
    /// The implementation's diagnostic name.
    fn name(&self) -> &str;
    /// Whether attachment deliberately requires an ambient Tokio runtime.
    fn requires_runtime(&self) -> bool {
        false
    }
    /// Attaches after cache components have been constructed. Sessions receive
    /// events and stop through the existing owned plugin lifecycle.
    fn attach(
        &self,
        context: &CachePluginContext<V>,
    ) -> Result<Box<dyn PluginSession>, PluginError>
    where
        V: Clone + Send + Sync + 'static;
}

/// Identity, events, shutdown signal and a weak operational cache capability.
/// Retaining this context does not retain the cache or its public lifetime.
pub struct CachePluginContext<V: Clone + Send + Sync + 'static> {
    context: PluginContext,
    owner: Weak<CacheInner<V>>,
    access: Arc<PluginAccess>,
}
impl<V: Clone + Send + Sync + 'static> Clone for CachePluginContext<V> {
    fn clone(&self) -> Self {
        Self {
            context: self.context.clone(),
            owner: self.owner.clone(),
            access: Arc::clone(&self.access),
        }
    }
}
impl<V: Clone + Send + Sync + 'static> std::fmt::Debug for CachePluginContext<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachePluginContext")
            .field("context", &self.context)
            .finish_non_exhaustive()
    }
}
impl<V: Clone + Send + Sync + 'static> Deref for CachePluginContext<V> {
    type Target = PluginContext;
    fn deref(&self) -> &Self::Target {
        &self.context
    }
}
impl<V: Clone + Send + Sync + 'static> CachePluginContext<V> {
    /// Acquires an operational view of the exact owning cache. It supports the
    /// full [`Cache`] API through Deref. Clones of this view do not count as public
    /// owners: the last application handle still closes the cache.
    ///
    /// During this session's synchronous `stop` hook, operations use a separate
    /// cleanup scope, even when normal cache operations have already closed.
    /// Await required commits before returning from `stop`: unfinished cleanup
    /// work is cancelled when the hook returns. Retained views work after manual
    /// detach while the application cache remains open.
    pub fn cache(&self) -> Result<PluginCache<V>, PluginError> {
        let inner = self.owner.upgrade().ok_or(PluginError::HostStopped)?;
        if self.access.scopes(&inner.scopes).is_closed() {
            return Err(PluginError::HostStopped);
        }
        Ok(PluginCache {
            cache: Cache {
                inner,
                lifetime: Arc::new(PublicLifetime::PluginAccess(Arc::clone(&self.access))),
            },
        })
    }
    /// Acquires the owner's typed original-value stream without retaining it.
    pub fn memory_evictions(&self) -> Result<crate::MemoryEvictions<V>, PluginError> {
        Ok(self.cache()?.memory_evictions().clone())
    }
}

/// A full operational view that never owns the application's cache lifetime.
/// Cache operations, options, events and providers refer to the same instance.
pub struct PluginCache<V: Clone + Send + Sync + 'static> {
    cache: Cache<V>,
}
impl<V: Clone + Send + Sync + 'static> Clone for PluginCache<V> {
    fn clone(&self) -> Self {
        Self {
            cache: self.cache.clone(),
        }
    }
}
impl<V: Clone + Send + Sync + 'static> Deref for PluginCache<V> {
    type Target = Cache<V>;
    fn deref(&self) -> &Self::Target {
        &self.cache
    }
}
impl<V: Clone + Send + Sync + 'static> PluginCache<V> {
    /// Uses an explicit driven executor for all synchronous cache operations.
    /// The resulting view retains the executor, but remains a non-owning cache
    /// view. This is suitable for synchronous Start/Event/Stop hooks.
    pub fn blocking(&self, runtime: BlockingRuntime) -> BlockingCache<V> {
        BlockingCache::plugin_view(self.cache.clone(), runtime)
    }
}

pub(super) enum InitialPlugin<V> {
    Legacy(Arc<dyn Plugin>),
    CacheAware(Arc<dyn CachePlugin<V>>),
}
impl<V: Clone + Send + Sync + 'static> InitialPlugin<V> {
    pub(super) fn bind(self, owner: &Arc<CacheInner<V>>) -> Arc<dyn Plugin> {
        match self {
            Self::Legacy(plugin) => plugin,
            Self::CacheAware(plugin) => adapter(plugin, owner),
        }
    }
}
pub(super) fn adapter<V: Clone + Send + Sync + 'static>(
    plugin: Arc<dyn CachePlugin<V>>,
    owner: &Arc<CacheInner<V>>,
) -> Arc<dyn Plugin> {
    Arc::new(CachePluginAdapter {
        plugin,
        owner: Arc::downgrade(owner),
    })
}
struct CachePluginAdapter<V: Clone + Send + Sync + 'static> {
    plugin: Arc<dyn CachePlugin<V>>,
    owner: Weak<CacheInner<V>>,
}
impl<V: Clone + Send + Sync + 'static> Plugin for CachePluginAdapter<V> {
    fn name(&self) -> &str {
        self.plugin.name()
    }
    fn requires_runtime(&self) -> bool {
        self.plugin.requires_runtime()
    }
    fn on_event(&self, _: &crate::CacheEvent) {}
    fn attach(
        &self,
        context: &PluginContext,
    ) -> Result<Option<Box<dyn PluginSession>>, PluginError> {
        let access = Arc::new(PluginAccess {
            state: Mutex::new(AccessState::Attached),
        });
        let context = CachePluginContext {
            context: context.clone(),
            owner: self.owner.clone(),
            access: Arc::clone(&access),
        };
        let session = self.plugin.attach(&context)?;
        Ok(Some(Box::new(CacheSession { session, access })))
    }
}
struct CacheSession {
    session: Box<dyn PluginSession>,
    access: Arc<PluginAccess>,
}
impl PluginSession for CacheSession {
    fn on_event(&self, event: &crate::CacheEvent) -> Result<(), PluginError> {
        self.session.on_event(event)
    }
    fn stop(&self) -> Result<(), PluginError> {
        let _cleanup = self.access.cleanup();
        self.session.stop()
    }
}
pub(super) struct PluginAccess {
    state: Mutex<AccessState>,
}
enum AccessState {
    Attached,
    Stopping(Arc<Scopes>),
    Detached,
}
impl PluginAccess {
    pub(super) fn scopes(&self, ordinary: &Arc<Scopes>) -> Arc<Scopes> {
        match &*crate::execution::lock(&self.state) {
            AccessState::Attached | AccessState::Detached => Arc::clone(ordinary),
            AccessState::Stopping(scopes) => Arc::clone(scopes),
        }
    }
    pub(super) fn is_stopping(&self) -> bool {
        matches!(
            *crate::execution::lock(&self.state),
            AccessState::Stopping(_)
        )
    }
    fn cleanup(&self) -> Cleanup<'_> {
        let scopes = Scopes::new();
        *crate::execution::lock(&self.state) = AccessState::Stopping(Arc::clone(&scopes));
        Cleanup {
            access: self,
            scopes,
        }
    }
}
struct Cleanup<'a> {
    access: &'a PluginAccess,
    scopes: Arc<Scopes>,
}
impl Drop for Cleanup<'_> {
    fn drop(&mut self) {
        // Never cancel/drop external work while holding the access-state lock.
        *crate::execution::lock(&self.access.state) = AccessState::Detached;
        self.scopes.close();
    }
}

#[derive(Clone)]
pub(super) enum WorkAdmission {
    Ordinary,
    Plugin(Arc<Scopes>),
}
impl<V: Clone + Send + Sync + 'static> Worker<V> {
    pub(super) fn ordinary(inner: Arc<CacheInner<V>>) -> Self {
        Self {
            memory: inner.memory.for_operation(),
            inner,
            admission: WorkAdmission::Ordinary,
        }
    }
    pub(super) fn scopes(&self) -> &Arc<Scopes> {
        match &self.admission {
            WorkAdmission::Ordinary => &self.inner.scopes,
            WorkAdmission::Plugin(scopes) => scopes,
        }
    }
}
impl<V: Clone + Send + Sync + 'static> Cache<V> {
    pub(super) fn operation_scopes(&self) -> Arc<Scopes> {
        match &*self.lifetime {
            PublicLifetime::External(_) | PublicLifetime::CacheOwned { .. } => {
                Arc::clone(&self.inner.scopes)
            }
            PublicLifetime::PluginAccess(access) => access.scopes(&self.inner.scopes),
        }
    }
    pub(super) fn inline(&self) -> InlinePermit<'_> {
        match &*self.lifetime {
            PublicLifetime::External(_) | PublicLifetime::CacheOwned { .. } => {
                self.inner.scopes.inline()
            }
            PublicLifetime::PluginAccess(access) => {
                access.scopes(&self.inner.scopes).inline_owned()
            }
        }
    }
    pub(super) fn check_plugin_drain(&self, operation: crate::DrainOperation) -> crate::Result<()> {
        match &*self.lifetime {
            PublicLifetime::PluginAccess(access) if access.is_stopping() => {
                Err(crate::Error::ReentrantDrain { operation })
            }
            PublicLifetime::External(_)
            | PublicLifetime::CacheOwned { .. }
            | PublicLifetime::PluginAccess(_) => Ok(()),
        }
    }
}
