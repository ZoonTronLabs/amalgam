//! Reader slots for short, synchronous L1 decisions.
//!
//! Each thread changes only its own padded lock word. A writer holds every
//! slot, in the same order, before accessing the value. Slot counts grow with
//! available parallelism; they are not limited to eight cores.
//!
//! This is the crate's only unsafe implementation boundary. Its safe guards
//! cannot cross threads and are used only in synchronous storage decisions.
//! Cache callbacks and retired value
//! destruction must run after these guards are released. Ordinary value Clone
//! is the documented exception and must not reenter this cache.

use parking_lot::RawRwLock;
use parking_lot::lock_api::{GuardNoSend, RawRwLock as _};
use std::cell::{Cell, UnsafeCell};
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};

static NEXT_READER: AtomicUsize = AtomicUsize::new(0);
thread_local! {
    static READER: Cell<Option<usize>> = const { Cell::new(None) };
}
fn reader_index() -> usize {
    READER.with(|reader| match reader.get() {
        Some(index) => index,
        None => {
            let index = NEXT_READER.fetch_add(1, Ordering::Relaxed);
            reader.set(Some(index));
            index
        }
    })
}
fn slot_count() -> usize {
    static COUNT: OnceLock<usize> = OnceLock::new();
    *COUNT.get_or_init(|| {
        std::thread::available_parallelism()
            .map_or(8, usize::from)
            .max(8)
            .next_power_of_two()
    })
}

#[repr(align(128))]
struct Slot(RawRwLock);

pub(crate) struct ReaderSlots<T> {
    slots: Box<[Slot]>,
    value: UnsafeCell<T>,
}
// SAFETY: exclusive ownership of ReaderSlots also owns T. Guards borrow the
// lock, so moving it cannot overlap any active access.
unsafe impl<T: Send> Send for ReaderSlots<T> {}
// SAFETY: readers hold one shared slot and expose only &T. Writers acquire all
// slots exclusively before exposing &mut T. The fixed acquisition order also
// prevents two writers from accessing T at once.
unsafe impl<T: Send + Sync> Sync for ReaderSlots<T> {}

impl<T> ReaderSlots<T> {
    pub(crate) fn new(value: T) -> Self {
        Self::with_slots(value, slot_count())
    }
    fn with_slots(value: T, slots: usize) -> Self {
        assert!(
            slots.is_power_of_two(),
            "slot count is an internal invariant"
        );
        Self {
            slots: (0..slots).map(|_| Slot(RawRwLock::INIT)).collect(),
            value: UnsafeCell::new(value),
        }
    }
    pub(crate) fn read(&self) -> ReadGuard<'_, T> {
        let slot = &self.slots[reader_index() & (self.slots.len() - 1)];
        slot.0.lock_shared();
        ReadGuard {
            lock: self,
            slot,
            _thread: PhantomData,
        }
    }
    pub(crate) fn write(&self) -> WriteGuard<'_, T> {
        let mut hold = WriterHold::new(self);
        for slot in &self.slots {
            slot.0.lock_exclusive();
            hold.acquired += 1;
        }
        WriteGuard { hold }
    }
    pub(crate) fn try_write(&self) -> Option<WriteGuard<'_, T>> {
        let mut hold = WriterHold::new(self);
        for slot in &self.slots {
            if !slot.0.try_lock_exclusive() {
                return None;
            }
            hold.acquired += 1;
        }
        Some(WriteGuard { hold })
    }
}

pub(crate) struct ReadGuard<'a, T> {
    lock: &'a ReaderSlots<T>,
    slot: &'a Slot,
    _thread: PhantomData<GuardNoSend>,
}
impl<T> Deref for ReadGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        // SAFETY: this guard holds a shared slot, excluding every writer.
        unsafe { &*self.lock.value.get() }
    }
}
impl<T> Drop for ReadGuard<'_, T> {
    fn drop(&mut self) {
        // SAFETY: read acquired exactly this slot. The non-Send guard releases
        // it once, on the acquiring thread, after all borrowed access ends.
        unsafe { self.slot.0.unlock_shared() };
    }
}

// Partial acquisition has no Deref implementation. It exists solely to release
// earlier slots when try_write fails or acquisition unwinds.
struct WriterHold<'a, T> {
    lock: &'a ReaderSlots<T>,
    acquired: usize,
    _thread: PhantomData<GuardNoSend>,
}
impl<'a, T> WriterHold<'a, T> {
    fn new(lock: &'a ReaderSlots<T>) -> Self {
        Self {
            lock,
            acquired: 0,
            _thread: PhantomData,
        }
    }
}
impl<T> Drop for WriterHold<'_, T> {
    fn drop(&mut self) {
        for slot in self.lock.slots[..self.acquired].iter().rev() {
            // SAFETY: acquired counts only successfully locked slots, owned by
            // this non-Send reservation. Every slot is released exactly once.
            unsafe { slot.0.unlock_exclusive() };
        }
    }
}
pub(crate) struct WriteGuard<'a, T> {
    hold: WriterHold<'a, T>,
}
impl<T> Deref for WriteGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        // SAFETY: WriteGuard is constructed only after acquiring every slot.
        unsafe { &*self.hold.lock.value.get() }
    }
}
impl<T> DerefMut for WriteGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        // SAFETY: all slots exclude both readers and competing writers. The
        // mutable borrow of this guard prevents overlapping access through it.
        unsafe { &mut *self.hold.lock.value.get() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};

    #[test]
    fn slots_cover_available_parallelism() {
        let lock = ReaderSlots::new(());
        assert!(lock.slots.len() >= std::thread::available_parallelism().map_or(1, usize::from));
        assert!(lock.slots.len().is_power_of_two());
    }

    #[test]
    fn failed_writer_releases_every_partial_slot_and_readers_never_see_torn_values() {
        let lock = Arc::new(ReaderSlots::with_slots((0_u64, 0_u64), 32));
        // Hold the last slot: try_write must roll back 31 earlier slots.
        let previous = READER.with(|reader| reader.replace(Some(31)));
        let guard = lock.read();
        READER.with(|reader| reader.set(previous));
        assert!(lock.try_write().is_none());
        drop(guard);
        *lock.try_write().unwrap() = (1, 1);
        let barrier = Arc::new(Barrier::new(33));
        std::thread::scope(|scope| {
            for _ in 0..32 {
                let lock = &lock;
                let barrier = &barrier;
                scope.spawn(move || {
                    barrier.wait();
                    for _ in 0..2_000 {
                        let pair = lock.read();
                        assert_eq!(pair.0, pair.1);
                    }
                });
            }
            barrier.wait();
            for generation in 2..2_000 {
                *lock.write() = (generation, generation);
            }
        });
        assert_eq!(*lock.read(), (1999, 1999));
    }

    #[test]
    fn competing_writers_have_exclusive_access_and_panics_release_every_slot() {
        let lock = ReaderSlots::with_slots(0_u64, 16);
        let barrier = Barrier::new(8);
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let lock = &lock;
                let barrier = &barrier;
                scope.spawn(move || {
                    barrier.wait();
                    for _ in 0..2_000 {
                        *lock.write() += 1;
                    }
                });
            }
        });
        assert_eq!(*lock.read(), 16_000);
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let mut guard = lock.write();
                *guard += 1;
                panic!("writer callback");
            }))
            .is_err()
        );
        assert_eq!(*lock.try_write().unwrap(), 16_001);
    }

    #[test]
    fn panicking_reader_releases_its_slot() {
        let lock = ReaderSlots::new(7);
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _guard = lock.read();
                panic!("reader callback");
            }))
            .is_err()
        );
        *lock.try_write().unwrap() = 9;
        assert_eq!(*lock.read(), 9);
    }
}
