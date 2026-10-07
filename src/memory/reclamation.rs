//! An operation retains retired values until its coordination guards are gone.
use crate::entry::Entry;
use crate::plugins::PendingPluginEvent;
use std::sync::{Arc, Mutex};

pub(crate) trait ReclamationFence: Send + Sync {}
pub(super) struct Garbage<V> {
    state: Mutex<Retired<V>>,
}
struct Retired<V> {
    entries: Vec<Retained<V>>,
    notifications: Vec<PendingPluginEvent>,
}
enum Retained<V> {
    Entry(Entry<V>),
    Observer(crate::advanced::MemoryEvictions<V>),
}
impl<V> Drop for Garbage<V> {
    fn drop(&mut self) {
        let state = self
            .state
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for event in std::mem::take(&mut state.notifications) {
            event.deliver();
        }
        for retained in std::mem::take(&mut state.entries) {
            match retained {
                Retained::Entry(entry) => drop(entry),
                Retained::Observer(observer) => drop(observer),
            }
        }
        // All resource destruction follows callbacks without a queue lock.
    }
}
impl<V: Send + Sync> ReclamationFence for Garbage<V> {}

pub(super) enum Reclamation<V> {
    Immediate,
    Operation(Arc<Garbage<V>>),
}
impl<V> Clone for Reclamation<V> {
    fn clone(&self) -> Self {
        match self {
            Self::Immediate => Self::Immediate,
            Self::Operation(owner) => Self::Operation(Arc::clone(owner)),
        }
    }
}
impl<V: Send + Sync + 'static> Reclamation<V> {
    pub(super) fn operation() -> Self {
        Self::Operation(Arc::new(Garbage {
            state: Mutex::new(Retired {
                entries: Vec::new(),
                notifications: Vec::new(),
            }),
        }))
    }
    pub(super) fn is_deferred(&self) -> bool {
        match self {
            Self::Immediate => false,
            Self::Operation(_) => true,
        }
    }
    pub(super) fn pin_for_outer_guard(&self, entry: &Entry<V>) -> Option<Entry<V>> {
        match self {
            Self::Immediate => None,
            Self::Operation(_) => Some(entry.clone()),
        }
    }
    pub(super) fn retain(&self, entry: Entry<V>) {
        match self {
            Self::Immediate => drop(entry),
            Self::Operation(owner) => owner
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .entries
                .push(Retained::Entry(entry)),
        }
    }
    pub(super) fn retain_observer(&self, observer: crate::advanced::MemoryEvictions<V>) {
        match self {
            Self::Immediate => drop(observer),
            Self::Operation(owner) => owner
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .entries
                .push(Retained::Observer(observer)),
        }
    }
    pub(super) fn defer(&self, event: PendingPluginEvent) {
        match self {
            Self::Immediate => event.deliver(),
            Self::Operation(owner) => owner
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .notifications
                .push(event),
        }
    }
    pub(super) fn fence(&self) -> Option<Arc<dyn ReclamationFence>> {
        match self {
            Self::Immediate => None,
            Self::Operation(owner) => Some(owner.clone()),
        }
    }
}

/// Fields release coordination first, then the retirement owner, even if an
/// unpolled future is dropped. No destructor runs while the queue mutex is held.
pub(crate) struct ReclamationGuard<G> {
    guard: G,
    _owner: Option<Arc<dyn ReclamationFence>>,
}
impl<G> ReclamationGuard<G> {
    pub(super) fn new(guard: G, owner: Option<Arc<dyn ReclamationFence>>) -> Self {
        Self {
            guard,
            _owner: owner,
        }
    }
}
impl<G> std::ops::Deref for ReclamationGuard<G> {
    type Target = G;
    fn deref(&self) -> &G {
        &self.guard
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        EntryOptions, Events, Timestamp, provider::ManualClock, provider::MemoryLimits,
        provider::MemoryStore,
    };
    use std::future::Future;
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[derive(Clone)]
    struct Probe {
        lock: Arc<tokio::sync::Mutex<()>>,
        drops: Arc<AtomicUsize>,
    }
    impl Drop for Probe {
        fn drop(&mut self) {
            assert!(
                self.lock.try_lock().is_ok(),
                "reclamation must follow outer coordinator release"
            );
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }
    #[test]
    fn both_unpolled_and_suspended_future_drop_release_coordination_before_values() {
        for polled in [false, true] {
            let clock = Arc::new(ManualClock::new(Timestamp::from_ticks(0)));
            let memory = MemoryStore::<Arc<Probe>>::with_clock(
                MemoryLimits::default(),
                Events::default(),
                clock,
            )
            .for_operation();
            let lock = Arc::new(tokio::sync::Mutex::new(()));
            let drops = Arc::new(AtomicUsize::new(0));
            let entry = Entry::fresh(
                Arc::new(Probe {
                    lock: lock.clone(),
                    drops: drops.clone(),
                }),
                &EntryOptions::default(),
                Timestamp::from_ticks(0),
                Box::new([]),
                None,
                None,
            );
            memory.observer.reclamation.retain(entry);
            let runtime = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            let guard = memory.guard(runtime.block_on(lock.clone().lock_owned()));
            // Put the root capture first: correctness must not rely on a
            // compiler-generated future's ordering of captured field drops.
            let work = async move {
                let _root = memory;
                let _guard = guard;
                std::future::pending::<()>().await;
            };
            let mut work = Box::pin(work);
            if polled {
                assert!(
                    work.as_mut()
                        .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
                        .is_pending()
                );
            }
            drop(work);
            drop(runtime);
            assert_eq!(drops.load(Ordering::SeqCst), 1);
            assert!(lock.try_lock().is_ok());
        }
    }
    #[test]
    fn timestamp_adjusted_input_and_stored_value_survive_the_outer_guard() {
        let clock = Arc::new(ManualClock::new(Timestamp::from_ticks(0)));
        let memory =
            MemoryStore::<Probe>::with_clock(MemoryLimits::default(), Events::default(), clock)
                .for_operation();
        let lock = Arc::new(tokio::sync::Mutex::new(()));
        let drops = Arc::new(AtomicUsize::new(0));
        let entry = Entry::fresh(
            Probe {
                lock: lock.clone(),
                drops: drops.clone(),
            },
            &EntryOptions::default(),
            Timestamp::from_ticks(0),
            Box::new([]),
            None,
            None,
        );
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let guard = memory.guard(runtime.block_on(lock.clone().lock_owned()));
        assert_eq!(
            runtime.block_on(memory.insert_at(Arc::from("k"), entry, Timestamp::from_ticks(10))),
            crate::provider::MemoryAdmission::Admitted
        );
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(memory);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(guard);
        assert_eq!(drops.load(Ordering::SeqCst), 2);
    }
}
