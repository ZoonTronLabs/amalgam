//! Private reader slots with a single writer admission gate.
//!
//! Readers publish a padded count, cross a SeqCst fence, then check the writer
//! gate. A writer closes the gate, crosses a SeqCst fence, then scans counts.
//! The fence order prevents both sides from overlooking each other: either the
//! writer observes the reservation or the reader observes the closed gate.
//! Release/Acquire count handoff orders completed reads before mutable access;
//! Release/Acquire gate handoff orders completed writes before admitted reads.
//!
//! Only a slot's permanent owner uses stores; colliding threads use a separate
//! RMW count. Counts are thread-bound. A writer serializes with one mutex and
//! waits for existing short readers to leave; ordinary value Clone must not reenter
//! the same cache. No user callback or value retirement is allowed under guards.
use parking_lot::{Condvar, Mutex, MutexGuard, lock_api::GuardNoSend};
use std::cell::{Cell, UnsafeCell};
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering, fence};

static NEXT_READER: AtomicUsize = AtomicUsize::new(0);
thread_local! {
    static READER: Cell<Option<usize>> = const { Cell::new(None) };
}
#[allow(deprecated, reason = "Atomic::try_update is unavailable on Rust 1.88")]
fn reader_index() -> usize {
    READER.with(|reader| match reader.get() {
        Some(index) => index,
        None => {
            initialize_parking();
            let index = NEXT_READER
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    current.checked_add(1)
                })
                .expect("reader identity exhausted");
            reader.set(Some(index));
            index
        }
    })
}
#[cold]
#[inline(never)]
fn initialize_parking() {
    let address = 0_usize;
    // SAFETY: an opaque live key; validation rejects before any enqueue and
    // callbacks neither panic nor reenter parking_lot.
    unsafe {
        parking_lot_core::park(
            &address as *const usize as usize,
            || false,
            || {},
            |_, _| {},
            parking_lot_core::DEFAULT_PARK_TOKEN,
            None,
        );
    }
}
fn slot_count() -> usize {
    static COUNT: OnceLock<usize> = OnceLock::new();
    *COUNT.get_or_init(|| {
        std::thread::available_parallelism()
            .map_or(64, usize::from)
            .max(64)
            .next_power_of_two()
    })
}
#[repr(align(128))]
struct Slot {
    owner: AtomicUsize,
    local: AtomicUsize,
    shared: AtomicUsize,
    // Only group-start slots use this bitmap. It fits existing slot padding.
    initialized: AtomicUsize,
}
enum Reservation<'a> {
    Local(&'a Slot),
    Shared(&'a Slot),
}
pub(crate) struct ReaderSlots<T> {
    slots: Box<[Slot]>,
    writer: AtomicBool,
    serial: Mutex<()>,
    waiters: Mutex<()>,
    waiting: AtomicUsize,
    changed: Condvar,
    value: UnsafeCell<T>,
}
// SAFETY: moving exclusive ownership also moves T, without outstanding guards.
unsafe impl<T: Send> Send for ReaderSlots<T> {}
// SAFETY: read reservations expose only &T. SeqCst admission excludes writers;
// the serial mutex excludes other writers. Only a fully admitted writer can
// expose &mut T, after every admitted reader has released its count.
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
            slots: (0..slots)
                .map(|_| Slot {
                    owner: AtomicUsize::new(0),
                    local: AtomicUsize::new(0),
                    shared: AtomicUsize::new(0),
                    initialized: AtomicUsize::new(0),
                })
                .collect(),
            writer: AtomicBool::new(false),
            serial: Mutex::new(()),
            waiters: Mutex::new(()),
            waiting: AtomicUsize::new(0),
            changed: Condvar::new(),
            value: UnsafeCell::new(value),
        }
    }
    #[allow(deprecated, reason = "Atomic::try_update is unavailable on Rust 1.88")]
    fn reserve(&self) -> Reservation<'_> {
        let reader = reader_index();
        let index = reader & (self.slots.len() - 1);
        let identity = reader + 1;
        let slot = &self.slots[index];
        let owner = slot.owner.load(Ordering::Acquire);
        // Publish this slot's bitmap bit BEFORE publishing its owner. A
        // colliding reader may immediately observe that owner and use shared.
        // Acquire of the owner then observes the preceding bitmap publication.
        if owner == 0 {
            let width = usize::BITS as usize;
            self.slots[index / width * width]
                .initialized
                .fetch_or(1 << (index % width), Ordering::Release);
        }
        let local = owner == identity
            || owner == 0
                && slot
                    .owner
                    .compare_exchange(0, identity, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok();
        if local {
            let count = slot.local.load(Ordering::Relaxed);
            let next = count.checked_add(1).expect("reader reservation exhausted");
            slot.local.store(next, Ordering::Release);
            Reservation::Local(slot)
        } else {
            slot.shared
                .fetch_update(Ordering::Release, Ordering::Relaxed, |count| {
                    count.checked_add(1)
                })
                .expect("shared reader reservations exhausted");
            Reservation::Shared(slot)
        }
    }
    pub(crate) fn read(&self) -> ReadGuard<'_, T> {
        loop {
            let reservation = self.reserve();
            fence(Ordering::SeqCst);
            if !self.writer.load(Ordering::Acquire) {
                return ReadGuard {
                    lock: self,
                    reservation,
                    _thread: PhantomData,
                };
            }
            drop(ReadGuard {
                lock: self,
                reservation,
                _thread: PhantomData,
            });
            self.wait_for_writer();
        }
    }
    fn wait_for_writer(&self) {
        // Waiting interest is published before checking the gate. A writer's
        // gate clear and interest check cross the same SeqCst fence. Its notification takes
        // this mutex, so it cannot pass the check-to-park registration window.
        self.waiting.fetch_add(1, Ordering::Release);
        fence(Ordering::SeqCst);
        {
            let mut wait = self.waiters.lock();
            while self.writer.load(Ordering::Acquire) {
                self.changed.wait(&mut wait);
            }
        }
        self.waiting.fetch_sub(1, Ordering::Release);
    }
    fn idle(&self) -> bool {
        // A sparse bitmap visits only slots that have actually been used,
        // regardless of the process-wide reader index or available core count.
        self.slots.chunks(usize::BITS as usize).all(|group| {
            let mut used = group[0].initialized.load(Ordering::Acquire);
            while used != 0 {
                let slot = &group[used.trailing_zeros() as usize];
                if slot.local.load(Ordering::Acquire) != 0
                    || slot.shared.load(Ordering::Acquire) != 0
                {
                    return false;
                }
                used &= used - 1;
            }
            true
        })
    }
    pub(crate) fn write(&self) -> WriteGuard<'_, T> {
        let hold = WriterHold::new(self, self.serial.lock());
        let mut spin = parking_lot_core::SpinWait::new();
        // Readers hold only short synchronous decisions. A waiting writer
        // yields after bounded spinning; it never requires reader-drop writes
        // to a shared wakeup word. Long user Clone can prolong this wait.
        while !self.idle() {
            if !spin.spin() {
                std::thread::yield_now();
            }
        }
        WriteGuard { hold }
    }
    pub(crate) fn try_write(&self) -> Option<WriteGuard<'_, T>> {
        let hold = WriterHold::new(self, self.serial.try_lock()?);
        self.idle().then_some(WriteGuard { hold })
    }
}
pub(crate) struct ReadGuard<'a, T> {
    lock: &'a ReaderSlots<T>,
    reservation: Reservation<'a>,
    _thread: PhantomData<GuardNoSend>,
}
impl<T> Deref for ReadGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        // SAFETY: the counted reservation passed the writer gate check. A
        // writer must see its count before obtaining exclusive value access.
        unsafe { &*self.lock.value.get() }
    }
}
impl<T> Drop for ReadGuard<'_, T> {
    fn drop(&mut self) {
        match self.reservation {
            Reservation::Local(slot) => {
                let count = slot.local.load(Ordering::Relaxed);
                slot.local.store(count - 1, Ordering::Release);
            }
            Reservation::Shared(slot) => {
                slot.shared.fetch_sub(1, Ordering::Release);
            }
        }
    }
}
struct WriterHold<'a, T> {
    lock: &'a ReaderSlots<T>,
    _serial: MutexGuard<'a, ()>,
}
impl<'a, T> WriterHold<'a, T> {
    fn new(lock: &'a ReaderSlots<T>, serial: MutexGuard<'a, ()>) -> Self {
        lock.writer.store(true, Ordering::Release);
        fence(Ordering::SeqCst);
        Self {
            lock,
            _serial: serial,
        }
    }
}
impl<T> Drop for WriterHold<'_, T> {
    fn drop(&mut self) {
        self.lock.writer.store(false, Ordering::Release);
        fence(Ordering::SeqCst);
        if self.lock.waiting.load(Ordering::Acquire) != 0 {
            let _wait = self.lock.waiters.lock();
            self.lock.changed.notify_all();
        }
    }
}
pub(crate) struct WriteGuard<'a, T> {
    hold: WriterHold<'a, T>,
}
impl<T> Deref for WriteGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        // SAFETY: all published readers left after the gate closed, and this
        // guard retains the serial mutex through its complete value access.
        unsafe { &*self.hold.lock.value.get() }
    }
}
impl<T> DerefMut for WriteGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        // SAFETY: the fully admitted exclusive guard and its mutable borrow
        // exclude every other access to T.
        unsafe { &mut *self.hold.lock.value.get() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};

    #[test]
    fn readers_never_see_torn_values_with_colliding_indices_and_competing_writers() {
        let lock = Arc::new(ReaderSlots::with_slots((0_u64, 0_u64), 2));
        let gate = Barrier::new(33);
        std::thread::scope(|scope| {
            for _ in 0..28 {
                let lock = &lock;
                let gate = &gate;
                scope.spawn(move || {
                    gate.wait();
                    for _ in 0..10_000 {
                        let value = lock.read();
                        assert_eq!(value.0, value.1);
                    }
                });
            }
            for _ in 0..4 {
                let lock = &lock;
                let gate = &gate;
                scope.spawn(move || {
                    gate.wait();
                    for _ in 0..10_000 {
                        let mut value = lock.write();
                        value.0 += 1;
                        value.1 += 1;
                    }
                });
            }
            gate.wait();
        });
        assert_eq!(*lock.read(), (40_000, 40_000));
    }
    #[test]
    fn rejected_writer_reopens_the_gate_and_panic_releases_both_kinds_of_guard() {
        let lock = ReaderSlots::new(7_u64);
        let read = lock.read();
        assert!(lock.try_write().is_none());
        assert_eq!(*lock.read(), 7);
        drop(read);
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let mut guard = lock.write();
                *guard = 9;
                panic!("writer panic");
            }))
            .is_err()
        );
        assert_eq!(*lock.read(), 9);
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _guard = lock.read();
                panic!("reader panic");
            }))
            .is_err()
        );
        assert!(lock.try_write().is_some());
    }
}

#[cfg(test)]
mod compatibility_tests {
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
        // A rejected writer must reopen admission for every reader slot.
        let guard = lock.read();
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

#[cfg(test)]
mod allocation;
