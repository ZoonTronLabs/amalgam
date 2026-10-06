//! A shared bounded historical diagnostic label vocabulary.
use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, OnceLock};

/// A shared, historical cache-name label budget. Labels are never released:
/// repeatedly creating/dropping caches cannot grow exporter cardinality.
///
/// Both facade and native OTel plugins use a process-wide budget of 128 names, each at most
/// 64 bytes. Overflow/long names aggregate under the label "other".
#[derive(Debug)]
pub struct CacheLabelBudget {
    max_names: NonZeroUsize,
    max_bytes: NonZeroUsize,
    names: Mutex<HashSet<Arc<str>>>,
    overflow: Arc<str>,
}

impl CacheLabelBudget {
    /// Creates a bounded catalog with a 64-byte maximum label.
    #[must_use]
    pub fn new(max_names: NonZeroUsize) -> Self {
        Self::with_limits(
            max_names,
            NonZeroUsize::new(64).unwrap_or(NonZeroUsize::MIN),
        )
    }

    /// Creates explicit count/byte limits. Share one catalog across plugins
    /// that report to the same exporter.
    #[must_use]
    pub fn with_limits(max_names: NonZeroUsize, max_bytes: NonZeroUsize) -> Self {
        Self {
            max_names,
            max_bytes,
            names: Mutex::new(HashSet::with_capacity(max_names.get().min(128))),
            overflow: Arc::from("other"),
        }
    }

    /// Allocates a label at attachment time, never on a cache-key hot path.
    #[must_use]
    pub fn label_for(&self, cache_name: &str) -> Arc<str> {
        if cache_name.len() > self.max_bytes.get() {
            return Arc::clone(&self.overflow);
        }
        let mut names = self
            .names
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(name) = names.get(cache_name) {
            return Arc::clone(name);
        }
        if names.len() >= self.max_names.get() {
            return Arc::clone(&self.overflow);
        }
        let label: Arc<str> = Arc::from(cache_name);
        names.insert(Arc::clone(&label));
        label
    }

    /// Number of historically assigned distinct named labels.
    #[must_use]
    pub fn assigned_names(&self) -> usize {
        self.names
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }
}

pub(super) fn process_budget() -> Arc<CacheLabelBudget> {
    static BUDGET: OnceLock<Arc<CacheLabelBudget>> = OnceLock::new();
    Arc::clone(BUDGET.get_or_init(|| {
        Arc::new(CacheLabelBudget::new(
            NonZeroUsize::new(128).unwrap_or(NonZeroUsize::MIN),
        ))
    }))
}
