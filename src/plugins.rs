//! The plugin seam.
//!
//! A [`Plugin`] observes the cache's lifecycle and event stream. This is the
//! Rust counterpart of FusionCache's `IFusionCachePlugin`. Plugins are notified
//! synchronously on the (already cheap, non-blocking) event path — a plugin must
//! not block; offload real work to its own task/channel.

use std::sync::Arc;

use crate::events::CacheEvent;

/// A cache plugin: observes events and lifecycle transitions.
///
/// Register plugins via [`CacheBuilder::plugin`](crate::CacheBuilder::plugin).
pub trait Plugin: Send + Sync {
    /// A short name used in diagnostics.
    fn name(&self) -> &str;

    /// Called once when the owning cache is built and the plugin is attached.
    fn on_start(&self) {}

    /// Called for every [`CacheEvent`] the cache emits.
    fn on_event(&self, event: &CacheEvent);
}

/// Holds the registered plugins and fans events out to them.
#[derive(Clone, Default)]
pub struct PluginHost {
    plugins: Arc<[Arc<dyn Plugin>]>,
}

impl PluginHost {
    /// Builds a host from a list of plugins, calling `on_start` for each.
    #[must_use]
    pub fn new(plugins: Vec<Arc<dyn Plugin>>) -> Self {
        for plugin in &plugins {
            plugin.on_start();
        }
        Self {
            plugins: plugins.into(),
        }
    }

    /// `true` if no plugins are registered (lets the cache skip work).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.plugins.is_empty()
    }

    /// Notifies every plugin of an event.
    pub fn notify(&self, event: &CacheEvent) {
        for plugin in self.plugins.iter() {
            plugin.on_event(event);
        }
    }
}

impl std::fmt::Debug for PluginHost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PluginHost")
            .field("count", &self.plugins.len())
            .finish()
    }
}
