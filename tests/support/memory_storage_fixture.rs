//! Shared independent external storage used by value and marker contracts.
#![allow(
    dead_code,
    reason = "each integration binary exercises a different provider subset"
)]
use amalgam::advanced::*;
use amalgam::entry::Entry;
use amalgam::provider::*;
use amalgam::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum Fault {
    #[error("lookup source")]
    Get,
    #[error("admission source")]
    Insert,
    #[error("removal source")]
    Remove,
    #[error("clear source")]
    Clear,
    #[error("maintenance source")]
    Maintain,
    #[error("usage source")]
    Usage,
}
#[derive(Clone, Copy)]
pub(crate) enum Limit {
    Unbounded,
    Entries(usize),
    Weight(u128),
    Disabled,
}
pub(crate) struct State<V> {
    pub(crate) records: HashMap<Arc<str>, MemoryRecord<V>>,
    pub(crate) epochs: HashMap<MemoryNamespace, MemoryStorageEpoch>,
}
pub(crate) struct ClearGate {
    pub(crate) entered: Barrier,
    pub(crate) resume: Barrier,
}
pub(crate) struct MapStorage<V> {
    pub(crate) state: Mutex<State<V>>,
    pub(crate) fault: Mutex<Option<Fault>>,
    pub(crate) limit: Limit,
    pub(crate) lookups: AtomicUsize,
    pub(crate) ready: AtomicUsize,
    pub(crate) writes: AtomicUsize,
    pub(crate) removals: AtomicUsize,
    pub(crate) clear_gate: Mutex<Option<Arc<ClearGate>>>,
    pub(crate) write_gate: Mutex<Option<Arc<ClearGate>>>,
}
impl<V> MapStorage<V> {
    pub(crate) fn new() -> Arc<Self> {
        Self::limited(Limit::Unbounded)
    }
    pub(crate) fn limited(limit: Limit) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                records: HashMap::new(),
                epochs: HashMap::new(),
            }),
            fault: Mutex::new(None),
            limit,
            lookups: AtomicUsize::new(0),
            ready: AtomicUsize::new(0),
            writes: AtomicUsize::new(0),
            removals: AtomicUsize::new(0),
            clear_gate: Mutex::new(None),
            write_gate: Mutex::new(None),
        })
    }
    fn check(&self, fault: Fault) -> std::result::Result<(), MemoryStorageError> {
        if *self.fault.lock().unwrap() == Some(fault) {
            Err(MemoryStorageError::provider(fault))
        } else {
            Ok(())
        }
    }
    fn rejection(
        reason: CapacityRejection,
        retired: Vec<MemoryRetirement<V>>,
    ) -> MemoryStorageWrite<V> {
        MemoryStorageWrite::Rejected {
            reason,
            retired: retired.into_boxed_slice(),
        }
    }
    fn priority(record: &MemoryRecord<V>) -> u8 {
        match record.entry().meta().priority() {
            Priority::Low => 0,
            Priority::Normal => 1,
            Priority::High => 2,
            Priority::NeverRemove => 3,
        }
    }
}
impl<V: Clone + Send + Sync + 'static> MemoryStorage<V> for MapStorage<V> {
    fn epoch(&self, namespace: &MemoryNamespace) -> MemoryStorageEpoch {
        self.state
            .lock()
            .unwrap()
            .epochs
            .entry(namespace.clone())
            .or_default()
            .clone()
    }
    fn get(&self, key: &str) -> std::result::Result<Option<MemoryRecord<V>>, MemoryStorageError> {
        self.lookups.fetch_add(1, Ordering::SeqCst);
        self.check(Fault::Get)?;
        Ok(self.state.lock().unwrap().records.get(key).cloned())
    }
    fn try_get(
        &self,
        key: &str,
    ) -> std::result::Result<Option<MemoryRecord<V>>, MemoryStorageError> {
        self.ready.fetch_add(1, Ordering::SeqCst);
        self.check(Fault::Get)?;
        Ok(self
            .state
            .try_lock()
            .ok()
            .and_then(|state| state.records.get(key).cloned()))
    }
    fn insert(
        &self,
        candidate: MemoryRecord<V>,
        condition: MemoryCondition<'_, V>,
        now: Timestamp,
    ) -> std::result::Result<MemoryStorageWrite<V>, MemoryStorageError> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        self.check(Fault::Insert)?;
        let gate = self.write_gate.lock().unwrap().take();
        if let Some(gate) = gate {
            gate.entered.wait();
            gate.resume.wait();
        }
        let mut state = self.state.lock().unwrap();
        if !candidate.is_live_at(now) {
            return Ok(Self::rejection(
                match candidate.retirement_reason(now) {
                    Some(MemoryEvictionReason::Removed) => CapacityRejection::VersionChanged,
                    _ => CapacityRejection::PhysicallyExpired,
                },
                vec![],
            ));
        }
        if !condition.matches(state.records.get(candidate.key()), now) {
            return Ok(Self::rejection(CapacityRejection::VersionChanged, vec![]));
        }
        let mut retired = Vec::new();
        let dead: Vec<_> = state
            .records
            .iter()
            .filter_map(|(key, record)| {
                record
                    .retirement_reason(now)
                    .map(|reason| (key.clone(), reason))
            })
            .collect();
        for (key, reason) in dead {
            retired.push(MemoryRetirement::new(
                state.records.remove(&key).unwrap(),
                reason,
            ));
        }
        let weight = candidate.weight();
        if matches!(self.limit, Limit::Disabled | Limit::Entries(0))
            || matches!(self.limit, Limit::Weight(limit) if weight > limit)
        {
            return Ok(Self::rejection(CapacityRejection::Oversized, retired));
        }
        let current = state.records.get(candidate.key());
        let count = state.records.len() + usize::from(current.is_none());
        let total = state
            .records
            .values()
            .map(MemoryRecord::weight)
            .sum::<u128>()
            - current.map_or(0, MemoryRecord::weight)
            + weight;
        let full = match self.limit {
            Limit::Unbounded => false,
            Limit::Entries(limit) => count > limit,
            Limit::Weight(limit) => total > limit,
            Limit::Disabled => true,
        };
        if full {
            let victim = state
                .records
                .iter()
                .filter(|(key, record)| {
                    key.as_ref() != candidate.key()
                        && Self::priority(record) < 3
                        && Self::priority(record) <= Self::priority(&candidate)
                })
                .min_by_key(|(_, record)| Self::priority(record))
                .map(|(key, _)| key.clone());
            let Some(victim) = victim else {
                return Ok(Self::rejection(
                    CapacityRejection::ProtectedCapacity,
                    retired,
                ));
            };
            retired.push(MemoryRetirement::new(
                state.records.remove(&victim).unwrap(),
                MemoryEvictionReason::Capacity,
            ));
        }
        let key = Arc::from(candidate.key());
        let previous = state.records.insert(key, candidate);
        drop(state);
        Ok(match previous {
            Some(previous) => MemoryStorageWrite::Replaced {
                previous,
                retired: retired.into_boxed_slice(),
            },
            None => MemoryStorageWrite::Admitted {
                retired: retired.into_boxed_slice(),
            },
        })
    }
    fn remove(
        &self,
        key: &str,
        expected: Option<&Entry<V>>,
    ) -> std::result::Result<Option<MemoryRecord<V>>, MemoryStorageError> {
        self.removals.fetch_add(1, Ordering::SeqCst);
        self.check(Fault::Remove)?;
        let mut state = self.state.lock().unwrap();
        if expected.is_some_and(|entry| {
            !state
                .records
                .get(key)
                .is_some_and(|record| record.is_same_entry(entry))
        }) {
            return Ok(None);
        }
        Ok(state.records.remove(key))
    }
    fn clear_before(
        &self,
        barrier: &MemoryGeneration,
    ) -> std::result::Result<Box<[MemoryRecord<V>]>, MemoryStorageError> {
        self.check(Fault::Clear)?;
        let gate = self.clear_gate.lock().unwrap().take();
        if let Some(gate) = gate {
            gate.entered.wait();
            gate.resume.wait();
        }
        let mut state = self.state.lock().unwrap();
        let keys: Vec<_> = state
            .records
            .iter()
            .filter(|(_, record)| barrier.precedes(record))
            .map(|(key, _)| key.clone())
            .collect();
        let records = keys
            .into_iter()
            .map(|key| state.records.remove(&key).unwrap())
            .collect::<Vec<_>>()
            .into_boxed_slice();
        drop(state);
        Ok(records)
    }
    fn maintain(
        &self,
        now: Timestamp,
    ) -> std::result::Result<Box<[MemoryRetirement<V>]>, MemoryStorageError> {
        self.check(Fault::Maintain)?;
        let mut state = self.state.lock().unwrap();
        let dead: Vec<_> = state
            .records
            .iter()
            .filter_map(|(key, record)| {
                record
                    .retirement_reason(now)
                    .map(|reason| (key.clone(), reason))
            })
            .collect();
        let retired = dead
            .into_iter()
            .map(|(key, reason)| MemoryRetirement::new(state.records.remove(&key).unwrap(), reason))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        drop(state);
        Ok(retired)
    }
    fn usage(&self) -> std::result::Result<MemoryUsage, MemoryStorageError> {
        self.check(Fault::Usage)?;
        let state = self.state.lock().unwrap();
        Ok(MemoryUsage {
            entries: state.records.len() as u64,
            weight: state.records.values().map(MemoryRecord::weight).sum(),
        })
    }
}
