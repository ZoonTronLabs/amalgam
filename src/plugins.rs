//! Extensible plugins with per-cache sessions and deterministic teardown.

use std::sync::{
    Arc, Mutex, MutexGuard, RwLock, Weak,
    atomic::{AtomicUsize, Ordering},
};

use tokio::sync::{Notify, watch};

use crate::error::{ConfigError, IdentityField};
use crate::events::{CacheEvent, CacheOperation, ComponentRead, Events, LayerEvent};
use crate::execution::{OwnedInlinePermit, Scopes};

/// The lifecycle stage at which an external plugin failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PluginStage {
    /// Attaching a session.
    Start,
    /// Handling an event.
    Event,
    /// Stopping a session.
    Stop,
}

/// A typed plugin failure with a preserved external source.
#[derive(Debug, Clone, thiserror::Error)]
pub enum PluginError {
    /// A plugin name must identify its implementation.
    #[error("plugin name must not be blank")]
    BlankName,
    /// The host has already been stopped.
    #[error("plugin host is stopped")]
    HostStopped,
    /// One event hub belongs to only one owning plugin host.
    #[error("event hub already has an owning plugin host")]
    EventsAlreadyAttached,
    /// The plugin deliberately requires a runtime.
    #[error("plugin {plugin} requires a Tokio runtime")]
    MissingRuntime {
        /// The plugin's diagnostic name.
        plugin: Arc<str>,
    },
    /// An implementation returned an expected failure.
    #[error("plugin {plugin} failed during {stage:?}: {source}")]
    Failure {
        /// The plugin's diagnostic name.
        plugin: Arc<str>,
        /// The failing lifecycle stage.
        stage: PluginStage,
        /// The original implementation failure.
        #[source]
        source: Arc<dyn std::error::Error + Send + Sync>,
    },
    /// An implementation violated its contract by panicking.
    #[error("plugin {plugin} panicked during {stage:?}")]
    Panicked {
        /// The plugin's diagnostic name.
        plugin: Arc<str>,
        /// The failing lifecycle stage.
        stage: PluginStage,
    },
}

impl PluginError {
    /// Preserves an implementation's source at the plugin boundary.
    pub fn from_source(
        plugin: impl AsRef<str>,
        stage: PluginStage,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::Failure {
            plugin: Arc::from(plugin.as_ref()),
            stage,
            source: Arc::new(source),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Lifecycle {
    Running,
    Stopped,
}

/// Cache identity, event hub and a shutdown signal supplied to one attachment.
/// It does not own the cache, so retaining it cannot keep the cache alive.
#[derive(Debug, Clone)]
pub struct PluginContext {
    cache_name: Arc<str>,
    instance_id: Arc<str>,
    events: Events,
    lifecycle: watch::Sender<Lifecycle>,
}

impl PluginContext {
    /// Creates a valid per-cache attachment context.
    pub fn new(
        cache_name: impl AsRef<str>,
        instance_id: impl AsRef<str>,
        events: Events,
    ) -> Result<Self, ConfigError> {
        if cache_name.as_ref().trim().is_empty() {
            return Err(ConfigError::BlankIdentity {
                field: IdentityField::CacheName,
            });
        }
        if instance_id.as_ref().trim().is_empty() {
            return Err(ConfigError::BlankIdentity {
                field: IdentityField::InstanceId,
            });
        }
        let (lifecycle, _) = watch::channel(Lifecycle::Running);
        Ok(Self {
            cache_name: Arc::from(cache_name.as_ref()),
            instance_id: Arc::from(instance_id.as_ref()),
            events,
            lifecycle,
        })
    }

    /// The owning cache's diagnostic name.
    #[must_use]
    pub fn cache_name(&self) -> &str {
        &self.cache_name
    }
    /// The owning cache's instance identity.
    #[must_use]
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }
    /// The common event route for this cache.
    #[must_use]
    pub fn events(&self) -> &Events {
        &self.events
    }
    /// Whether the owning host has begun shutting down.
    #[must_use]
    pub fn is_stopped(&self) -> bool {
        *self.lifecycle.borrow() == Lifecycle::Stopped
    }
    /// Waits for owning-cache shutdown. Already stopped contexts return immediately.
    pub async fn stopped(&self) {
        let mut receiver = self.lifecycle.subscribe();
        while *receiver.borrow_and_update() == Lifecycle::Running {
            if receiver.changed().await.is_err() {
                break;
            }
        }
    }

    fn stop(&self) {
        self.lifecycle.send_replace(Lifecycle::Stopped);
    }

    fn legacy() -> Self {
        let (lifecycle, _) = watch::channel(Lifecycle::Running);
        Self {
            cache_name: Arc::from("amalgam"),
            instance_id: Arc::from("legacy-plugin-host"),
            events: Events::default(),
            lifecycle,
        }
    }
}

/// Selects additional observation hooks without changing legacy callbacks.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum PluginObservations {
    /// Existing logical events only.
    #[default]
    Logical,
    /// Logical events, physical component facts and operation starts.
    All,
}

/// Open plugin behavior. Existing implementations keep their legacy hooks;
/// implementations needing per-cache state return an independent session.
pub trait Plugin: Send + Sync {
    /// The implementation's diagnostic name.
    fn name(&self) -> &str;
    /// Legacy attachment hook, called once for each owning cache.
    fn on_start(&self) {}
    /// Legacy event callback.
    fn on_event(&self, event: &CacheEvent);
    /// Selects additional callbacks; evaluated once on attachment.
    fn observations(&self) -> PluginObservations {
        PluginObservations::Logical
    }
    /// Observes a physical component fact when All was selected.
    fn on_layer_event(&self, _event: &LayerEvent) {}
    /// Observes a logical operation start when All was selected.
    fn on_operation_started(&self, _operation: CacheOperation) {}
    /// Observes an actual component read attempt when All was selected.
    fn on_component_read(&self, _component: ComponentRead) {}
    /// Legacy teardown hook, called once for each owning cache.
    fn on_stop(&self) {}
    /// Whether attachment requires a Tokio runtime.
    fn requires_runtime(&self) -> bool {
        false
    }
    /// Attaches independent behavior for this cache. None selects legacy hooks.
    fn attach(
        &self,
        _context: &PluginContext,
    ) -> Result<Option<Box<dyn PluginSession>>, PluginError> {
        self.on_start();
        Ok(None)
    }
}

/// Independent behavior/resources belonging to one cache attachment.
pub trait PluginSession: Send + Sync {
    /// Handles one event without blocking. Offload slow work to an owned task.
    fn on_event(&self, event: &CacheEvent) -> Result<(), PluginError>;
    /// Selects additional callbacks; evaluated once on attachment.
    fn observations(&self) -> PluginObservations {
        PluginObservations::Logical
    }
    /// Observes a component fact after cache coordination guards are released.
    fn on_layer_event(&self, _event: &LayerEvent) -> Result<(), PluginError> {
        Ok(())
    }
    /// Observes the start independently from final completion/cancellation.
    fn on_operation_started(&self, _operation: CacheOperation) -> Result<(), PluginError> {
        Ok(())
    }
    /// Observes an attempted component read, independent of its result.
    fn on_component_read(&self, _component: ComponentRead) -> Result<(), PluginError> {
        Ok(())
    }
    /// Releases this attachment's resources once.
    fn stop(&self) -> Result<(), PluginError> {
        Ok(())
    }
}

/// The explicit result of requesting registration teardown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PluginStopOutcome {
    /// Teardown completed.
    Stopped,
    /// A currently running callback or teardown is finishing.
    Pending,
    /// Teardown had already completed.
    AlreadyStopped,
}

enum Dispatch {
    Legacy(Arc<dyn Plugin>),
    Session(Box<dyn PluginSession>),
}

impl Dispatch {
    fn observations(&self) -> PluginObservations {
        match self {
            Self::Legacy(plugin) => plugin.observations(),
            Self::Session(session) => session.observations(),
        }
    }
    fn event(&self, notification: Notification<'_>) -> Result<(), PluginError> {
        match self {
            Self::Legacy(plugin) => {
                match notification {
                    Notification::Logical(event) => plugin.on_event(event),
                    Notification::Layer(event) => plugin.on_layer_event(event),
                    Notification::Started(operation) => plugin.on_operation_started(operation),
                    Notification::ComponentRead(component) => plugin.on_component_read(component),
                }
                Ok(())
            }
            Self::Session(session) => match notification {
                Notification::Logical(event) => session.on_event(event),
                Notification::Layer(event) => session.on_layer_event(event),
                Notification::Started(operation) => session.on_operation_started(operation),
                Notification::ComponentRead(component) => session.on_component_read(component),
            },
        }
    }
    fn stop(&self) -> Result<(), PluginError> {
        match self {
            Self::Legacy(plugin) => {
                plugin.on_stop();
                Ok(())
            }
            Self::Session(session) => session.stop(),
        }
    }
}

enum SessionState {
    Running {
        dispatch: Arc<Dispatch>,
        callbacks: usize,
    },
    Draining {
        dispatch: Arc<Dispatch>,
        callbacks: usize,
    },
    Stopping,
    Stopped(Result<(), PluginError>),
}

struct PluginSlot {
    name: Arc<str>,
    observations: PluginObservations,
    state: Mutex<SessionState>,
    stopped: Notify,
}

impl PluginSlot {
    fn is_stopped(&self) -> bool {
        matches!(*lock(&self.state), SessionState::Stopped(_))
    }
    fn accepts_events(&self) -> bool {
        matches!(*lock(&self.state), SessionState::Running { .. })
    }

    async fn wait_stopped(&self) -> Result<(), PluginError> {
        loop {
            let notified = self.stopped.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let SessionState::Stopped(result) = &*lock(&self.state) {
                return result.clone();
            }
            notified.await;
        }
    }

    fn callback(self: &Arc<Self>) -> Option<CallbackLease> {
        let mut state = lock(&self.state);
        match &mut *state {
            SessionState::Running {
                dispatch,
                callbacks,
            } => {
                *callbacks += 1;
                Some(CallbackLease {
                    dispatch: Arc::clone(dispatch),
                    _guard: CallbackGuard {
                        slot: Arc::clone(self),
                    },
                })
            }
            SessionState::Draining { .. } | SessionState::Stopping | SessionState::Stopped(_) => {
                None
            }
        }
    }

    fn request_stop(&self) -> Result<PluginStopOutcome, PluginError> {
        let dispatch = {
            let mut state = lock(&self.state);
            match &*state {
                SessionState::Running {
                    callbacks: 0,
                    dispatch,
                } => {
                    let dispatch = Arc::clone(dispatch);
                    *state = SessionState::Stopping;
                    Some(dispatch)
                }
                SessionState::Running {
                    dispatch,
                    callbacks,
                } => {
                    *state = SessionState::Draining {
                        dispatch: Arc::clone(dispatch),
                        callbacks: *callbacks,
                    };
                    None
                }
                SessionState::Draining { .. } | SessionState::Stopping => None,
                SessionState::Stopped(result) => {
                    return result.clone().map(|()| PluginStopOutcome::AlreadyStopped);
                }
            }
        };
        match dispatch {
            Some(dispatch) => self
                .finish_stop(dispatch)
                .map(|()| PluginStopOutcome::Stopped),
            None => Ok(PluginStopOutcome::Pending),
        }
    }

    fn finish_callback(&self) {
        let dispatch = {
            let mut state = lock(&self.state);
            match &mut *state {
                SessionState::Running { callbacks, .. } => {
                    *callbacks -= 1;
                    None
                }
                SessionState::Draining {
                    dispatch,
                    callbacks,
                } => {
                    *callbacks -= 1;
                    if *callbacks == 0 {
                        let dispatch = Arc::clone(dispatch);
                        *state = SessionState::Stopping;
                        Some(dispatch)
                    } else {
                        None
                    }
                }
                SessionState::Stopping | SessionState::Stopped(_) => {
                    unreachable!("callback leases drain before stopping")
                }
            }
        };
        if let Some(dispatch) = dispatch
            && let Err(error) = self.finish_stop(dispatch)
        {
            tracing::warn!(error = %error, "amalgam: deferred plugin teardown failed");
        }
    }

    fn finish_stop(&self, dispatch: Arc<Dispatch>) -> Result<(), PluginError> {
        let result = external(&self.name, PluginStage::Stop, || {
            let result = dispatch.stop();
            // Session destruction is still owned teardown, even when its
            // explicit stop hook returned an error. Publish Stopped afterward.
            drop(dispatch);
            result
        });
        *lock(&self.state) = SessionState::Stopped(result.clone());
        self.stopped.notify_waiters();
        result
    }
}

struct CallbackLease {
    // Field order is deliberate: release this callback's dispatch pin BEFORE
    // its guard finishes the final callback and publishes completed teardown.
    dispatch: Arc<Dispatch>,
    _guard: CallbackGuard,
}

struct CallbackGuard {
    slot: Arc<PluginSlot>,
}

impl Drop for CallbackGuard {
    fn drop(&mut self) {
        self.slot.finish_callback();
    }
}

enum HostState {
    Running(Vec<Arc<PluginSlot>>),
    Stopped(Vec<Arc<PluginSlot>>),
}

pub(crate) struct PluginHostInner {
    context: PluginContext,
    slots: RwLock<HostState>,
    // Published under the slots write lock. Counts potential recipients,
    // including draining slots; actual admission still belongs to each slot.
    listeners: Arc<AtomicUsize>,
    observers: Arc<AtomicUsize>,
    attachments: Arc<Scopes>,
}

#[derive(Clone, Copy)]
enum Notification<'a> {
    Logical(&'a CacheEvent),
    Layer(&'a LayerEvent),
    Started(CacheOperation),
    ComponentRead(ComponentRead),
}
#[derive(Clone, Copy)]
enum NotificationSelection {
    Every,
    Legacy,
    Observers,
}
impl NotificationSelection {
    fn selects(self, observations: PluginObservations) -> bool {
        match self {
            Self::Every => true,
            Self::Legacy => observations == PluginObservations::Logical,
            Self::Observers => observations == PluginObservations::All,
        }
    }
}
pub(crate) struct NotificationBatch {
    callbacks: Vec<CallbackLease>,
    _scope: OwnedInlinePermit,
}
impl NotificationBatch {
    fn notify(&self, event: Notification<'_>) -> Vec<PluginError> {
        let mut errors = Vec::new();
        for callback in &self.callbacks {
            let slot = &callback._guard.slot;
            if let Err(error) = external(&slot.name, PluginStage::Event, || {
                callback.dispatch.event(event)
            }) {
                errors.push(error);
            }
        }
        errors
    }
    pub(crate) fn logical(self, event: CacheEvent) -> PendingPluginEvent {
        PendingPluginEvent {
            batch: self,
            event: OwnedNotification::Logical(event),
        }
    }
    pub(crate) fn component_read(self, component: ComponentRead) -> PendingPluginEvent {
        PendingPluginEvent {
            batch: self,
            event: OwnedNotification::ComponentRead(component),
        }
    }
    pub(crate) fn layer(self, event: LayerEvent) -> PendingPluginEvent {
        PendingPluginEvent {
            batch: self,
            event: OwnedNotification::Layer(event),
        }
    }
}
enum OwnedNotification {
    Logical(CacheEvent),
    Layer(LayerEvent),
    ComponentRead(ComponentRead),
}
pub(crate) struct PendingPluginEvent {
    batch: NotificationBatch,
    event: OwnedNotification,
}
impl PendingPluginEvent {
    pub(crate) fn deliver(self) {
        let event = match &self.event {
            OwnedNotification::Logical(event) => Notification::Logical(event),
            OwnedNotification::Layer(event) => Notification::Layer(event),
            OwnedNotification::ComponentRead(component) => Notification::ComponentRead(*component),
        };
        for error in self.batch.notify(event) {
            tracing::warn!(%error, "amalgam: deferred plugin event failed");
        }
    }
}

/// The event hub can check recipient admission without pinning an idle host.
/// The shared counter owns no sessions, cache, or teardown work.
pub(crate) struct PluginEventRoute {
    host: Weak<PluginHostInner>,
    listeners: Arc<AtomicUsize>,
    observers: Arc<AtomicUsize>,
}

impl PluginEventRoute {
    #[inline]
    pub(crate) fn has_observers(&self) -> bool {
        self.observers.load(Ordering::Acquire) != 0
    }
    #[inline]
    pub(crate) fn has_listeners(&self) -> bool {
        self.listeners.load(Ordering::Acquire) != 0
    }
    #[inline]
    pub(crate) fn upgrade(&self) -> Option<Arc<PluginHostInner>> {
        if !self.has_listeners() {
            None
        } else {
            self.host.upgrade()
        }
    }
}

impl PluginHostInner {
    pub(crate) fn has_listeners(&self) -> bool {
        self.listeners.load(Ordering::Acquire) != 0
    }

    fn publish_listeners(&self, state: &HostState) {
        let count = match state {
            HostState::Running(slots) => slots.len(),
            HostState::Stopped(_) => 0,
        };
        let observers = match state {
            HostState::Running(slots) => slots
                .iter()
                .filter(|slot| slot.observations == PluginObservations::All)
                .count(),
            HostState::Stopped(_) => 0,
        };
        self.observers.store(observers, Ordering::Release);
        self.listeners.store(count, Ordering::Release);
    }

    pub(crate) fn notify(&self, event: &CacheEvent) -> Vec<PluginError> {
        self.capture(PluginObservations::Logical)
            .map_or_else(Vec::new, |batch| batch.notify(Notification::Logical(event)))
    }
    pub(crate) fn notify_layer(&self, event: &LayerEvent) -> Vec<PluginError> {
        self.capture(PluginObservations::All)
            .map_or_else(Vec::new, |batch| batch.notify(Notification::Layer(event)))
    }
    pub(crate) fn notify_started(&self, operation: CacheOperation) -> Vec<PluginError> {
        self.capture(PluginObservations::All)
            .map_or_else(Vec::new, |batch| {
                batch.notify(Notification::Started(operation))
            })
    }
    pub(crate) fn notify_legacy(&self, event: &CacheEvent) -> Vec<PluginError> {
        self.capture_selected(NotificationSelection::Legacy)
            .map_or_else(Vec::new, |batch| batch.notify(Notification::Logical(event)))
    }
    pub(crate) fn capture(&self, observations: PluginObservations) -> Option<NotificationBatch> {
        self.capture_selected(match observations {
            PluginObservations::Logical => NotificationSelection::Every,
            PluginObservations::All => NotificationSelection::Observers,
        })
    }
    fn capture_selected(&self, selection: NotificationSelection) -> Option<NotificationBatch> {
        let listeners = self.listeners.load(Ordering::Acquire);
        let observers = self.observers.load(Ordering::Acquire);
        let empty = match selection {
            NotificationSelection::Every => listeners == 0,
            NotificationSelection::Observers => observers == 0,
            NotificationSelection::Legacy => listeners == observers,
        };
        if empty {
            return None;
        }
        let scope = self.attachments.inline_owned();
        scope.admit().ok()?;
        let slots = {
            let state = self
                .slots
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match &*state {
                HostState::Running(slots) => slots.clone(),
                HostState::Stopped(_) => Vec::new(),
            }
        };
        let callbacks: Vec<_> = slots
            .into_iter()
            .filter(|slot| selection.selects(slot.observations))
            .filter_map(|slot| slot.callback())
            .collect();
        if callbacks.is_empty() {
            None
        } else {
            Some(NotificationBatch {
                callbacks,
                _scope: scope,
            })
        }
    }

    fn closing_slots(&self) -> Vec<Arc<PluginSlot>> {
        self.context.stop();
        self.attachments.close();
        let mut state = self
            .slots
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match &*state {
            HostState::Running(slots) => {
                let slots = slots.clone();
                *state = HostState::Stopped(slots.clone());
                self.publish_listeners(&state);
                slots
            }
            HostState::Stopped(slots) => slots.clone(),
        }
    }

    fn stop_all(&self) -> Vec<PluginError> {
        self.closing_slots()
            .into_iter()
            .filter_map(|slot| slot.request_stop().err())
            .collect()
    }

    fn prune_stopped(&self) {
        // Keep a detached but draining callback reachable by owning shutdown.
        // Completed registrations are retired outside the host's lock.
        let retired = {
            let mut state = self
                .slots
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut retired = Vec::new();
            if let HostState::Running(slots) = &mut *state {
                let mut index = 0;
                while index < slots.len() {
                    if slots[index].is_stopped() {
                        retired.push(slots.swap_remove(index));
                    } else {
                        index += 1;
                    }
                }
            }
            self.publish_listeners(&state);
            retired
        };
        drop(retired);
    }
}

impl Drop for PluginHostInner {
    fn drop(&mut self) {
        for error in self.stop_all() {
            tracing::warn!(error = %error, "amalgam: plugin teardown failed");
        }
    }
}

/// Holds one cache's attachments. Event hubs keep only a weak reference.
#[derive(Clone)]
pub struct PluginHost {
    inner: Arc<PluginHostInner>,
}

impl PluginHost {
    /// Legacy infallible host adapter with anonymous cache context.
    #[must_use]
    pub fn new(plugins: Vec<Arc<dyn Plugin>>) -> Self {
        match Self::try_new(PluginContext::legacy(), plugins) {
            Ok(host) => host,
            Err(error) => panic!("invalid plugin attachment: {error}"),
        }
    }

    /// Attaches all sessions transactionally, stopping earlier sessions on error.
    pub fn try_new(
        context: PluginContext,
        plugins: Vec<Arc<dyn Plugin>>,
    ) -> Result<Self, PluginError> {
        if context.is_stopped() {
            return Err(PluginError::HostStopped);
        }
        let host = Self {
            inner: Arc::new(PluginHostInner {
                context,
                slots: RwLock::new(HostState::Running(Vec::with_capacity(plugins.len()))),
                listeners: Arc::new(AtomicUsize::new(0)),
                observers: Arc::new(AtomicUsize::new(0)),
                attachments: Scopes::new(),
            }),
        };
        host.inner.context.events.attach_plugins(&host)?;
        for plugin in plugins {
            host.attach(plugin)?;
        }
        Ok(host)
    }

    pub(crate) fn attach_owned(&self, plugin: Arc<dyn Plugin>) -> Result<(), PluginError> {
        self.attach(plugin).map(|_| ())
    }

    fn attach(&self, plugin: Arc<dyn Plugin>) -> Result<Arc<PluginSlot>, PluginError> {
        let attachment = self.inner.attachments.inline();
        attachment.admit().map_err(|_| PluginError::HostStopped)?;
        let slot = self.attach_admitted(plugin)?;
        if attachment.status(None).is_err() {
            slot.request_stop()?;
            return Err(PluginError::HostStopped);
        }
        Ok(slot)
    }

    fn attach_admitted(&self, plugin: Arc<dyn Plugin>) -> Result<Arc<PluginSlot>, PluginError> {
        self.inner.prune_stopped();
        if matches!(
            *self
                .inner
                .slots
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            HostState::Stopped(_)
        ) {
            return Err(PluginError::HostStopped);
        }
        let unknown: Arc<str> = Arc::from("unidentified-plugin");
        let name = external(&unknown, PluginStage::Start, || {
            Ok(Arc::<str>::from(plugin.name()))
        })?;
        if name.trim().is_empty() {
            return Err(PluginError::BlankName);
        }
        let requires_runtime =
            external(&name, PluginStage::Start, || Ok(plugin.requires_runtime()))?;
        if requires_runtime && tokio::runtime::Handle::try_current().is_err() {
            return Err(PluginError::MissingRuntime { plugin: name });
        }
        // Attachment executes no user behavior under the host's registration lock.
        let session = external(&name, PluginStage::Start, || {
            plugin.attach(&self.inner.context)
        })?;
        let dispatch = match session {
            Some(session) => Dispatch::Session(session),
            None => Dispatch::Legacy(plugin),
        };
        let observations = match external(&name, PluginStage::Start, || Ok(dispatch.observations()))
        {
            Ok(observations) => observations,
            Err(error) => {
                if let Err(cleanup) = external(&name, PluginStage::Stop, || dispatch.stop()) {
                    tracing::warn!(%cleanup, "amalgam: rejected observer attachment cleanup failed");
                }
                return Err(error);
            }
        };
        let slot = Arc::new(PluginSlot {
            name,
            observations,
            state: Mutex::new(SessionState::Running {
                dispatch: Arc::new(dispatch),
                callbacks: 0,
            }),
            stopped: Notify::new(),
        });
        let attached = {
            let mut state = self
                .inner
                .slots
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let attached = match &mut *state {
                HostState::Running(slots) => {
                    slots.push(Arc::clone(&slot));
                    true
                }
                HostState::Stopped(slots) => {
                    // Preserve a late attachment's teardown result for awaited
                    // shutdown, which re-snapshots after startup has drained.
                    slots.push(Arc::clone(&slot));
                    false
                }
            };
            self.inner.publish_listeners(&state);
            attached
        };
        if !attached {
            slot.request_stop()?;
            return Err(PluginError::HostStopped);
        }
        Ok(slot)
    }

    /// Registers a dynamic attachment whose guard owns its lifetime.
    pub fn register(&self, plugin: Arc<dyn Plugin>) -> Result<PluginRegistration, PluginError> {
        Ok(PluginRegistration {
            host: Arc::downgrade(&self.inner),
            slot: self.attach(plugin)?,
        })
    }

    /// Whether no attachment can receive events.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        match &*self
            .inner
            .slots
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
        {
            HostState::Running(slots) => !slots.iter().any(|slot| slot.accepts_events()),
            HostState::Stopped(_) => true,
        }
    }

    /// Notifies attachments through the legacy standalone host API.
    pub fn notify(&self, event: &CacheEvent) {
        for error in self.try_notify(event) {
            tracing::warn!(error = %error, "amalgam: plugin event failed");
        }
    }

    /// Returns explicit implementation failures without changing cache outcomes.
    pub fn try_notify(&self, event: &CacheEvent) -> Vec<PluginError> {
        self.inner.notify(event)
    }

    /// Stops the owning cache's sessions once. In-flight callbacks drain safely.
    pub fn stop_all(&self) -> Vec<PluginError> {
        self.inner.stop_all()
    }

    /// Initiates teardown and waits for every owning or draining attachment.
    /// Repeated calls return the same preserved teardown failures. Call this
    /// from owning-cache shutdown; synchronous event callbacks use stop_all or
    /// their registration guard instead of awaiting their own completion.
    pub async fn shutdown(&self) -> Vec<PluginError> {
        let slots = self.inner.closing_slots();
        for slot in &slots {
            let _ = slot.request_stop();
        }
        self.inner.attachments.drained().await;
        let slots = self.inner.closing_slots();
        let mut errors = Vec::new();
        for slot in slots {
            let _ = slot.request_stop();
            if let Err(error) = slot.wait_stopped().await {
                errors.push(error);
            }
        }
        errors
    }

    pub(crate) fn event_route(&self) -> PluginEventRoute {
        PluginEventRoute {
            host: Arc::downgrade(&self.inner),
            listeners: Arc::clone(&self.inner.listeners),
            observers: Arc::clone(&self.inner.observers),
        }
    }
}

impl Default for PluginHost {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}

impl std::fmt::Debug for PluginHost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PluginHost")
            .field("cache_name", &self.inner.context.cache_name)
            .field("is_empty", &self.is_empty())
            .finish()
    }
}

/// Owns one dynamic registration. Drop detaches and requests teardown exactly
/// once; callbacks already executing finish before their session is stopped.
#[must_use = "dropping this guard detaches its plugin session"]
pub struct PluginRegistration {
    host: Weak<PluginHostInner>,
    slot: Arc<PluginSlot>,
}

impl PluginRegistration {
    /// Detaches and returns the explicit teardown state.
    pub fn stop(&self) -> Result<PluginStopOutcome, PluginError> {
        let result = self.slot.request_stop();
        if let Some(host) = self.host.upgrade() {
            host.prune_stopped();
        }
        result
    }

    /// Waits for any in-flight callback and the teardown hook to finish.
    pub async fn wait_stopped(&self) -> Result<(), PluginError> {
        self.slot.wait_stopped().await
    }
}

impl Drop for PluginRegistration {
    fn drop(&mut self) {
        if let Err(error) = self.stop() {
            tracing::warn!(error = %error, "amalgam: plugin registration teardown failed");
        }
    }
}

fn external<T>(
    name: &Arc<str>,
    stage: PluginStage,
    callback: impl FnOnce() -> Result<T, PluginError>,
) -> Result<T, PluginError> {
    // A plugin is an external infrastructure seam. Panic isolation here never
    // converts origin/cancellation failures into successful cache values.
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(callback)) {
        Ok(result) => result,
        Err(_) => Err(PluginError::Panicked {
            plugin: Arc::clone(name),
            stage,
        }),
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
