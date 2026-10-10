//! Private reader slots with a single writer admission gate.
//!
//! Readers publish a padded count, then check the writer gate. A writer closes
//! the gate, then scans the published counts. These are Dekker handshakes: each
//! side stores, then loads what the other side stores. Every access in them
//! (count publication and scan, gate store and load, bitmap of used slots,
//! waiter interest) is a `HANDSHAKE` access, SeqCst. The single total order of
//! SeqCst operations (C++20 [atomics.order]/4) prevents both sides from
//! overlooking each other: either the writer observes the reservation or the
//! reader observes the closed gate. No standalone fence is needed. That matters
//! on x86, where `fence(SeqCst)` is an `mfence`: ~21 ns per admission on a Zen 4
//! CI runner, against ~3 ns for the `xchg` of a SeqCst store. On AArch64 SeqCst
//! stores and loads are `stlr`/`ldar`; read-modify-writes are `swpal`/`casal`
//! only where LSE is enabled at compile time (Apple targets) and calls to
//! outline-atomics helpers on the default Linux targets.
//!
//! Loom models SeqCst accesses as AcqRel, so it cannot check this fence-free
//! form. Under `cfg(loom)` the `Gate` puts a SeqCst fence between each
//! handshake store and the load that follows it, and Loom checks that fenced
//! equivalent. Loom would not notice a handshake access weakened below SeqCst,
//! so a unit test counts the `HANDSHAKE` accesses instead.
//! Release/Acquire count handoff orders completed reads before mutable access;
//! Release/Acquire gate handoff orders completed writes before admitted reads.
//!
//! Only a slot's permanent owner uses stores; colliding threads use a separate
//! RMW count. Counts are thread-bound. A writer serializes with one mutex and
//! waits for existing short readers to leave; ordinary value Clone must not reenter
//! the same cache. No user callback or value retirement is allowed under guards.
#[cfg(loom)]
use loom::cell::UnsafeCell;
#[cfg(loom)]
use loom::sync::atomic::{AtomicBool, AtomicUsize, Ordering, fence};
#[cfg(loom)]
use loom::sync::{Condvar, Mutex, MutexGuard};
use parking_lot::lock_api::GuardNoSend;
#[cfg(not(loom))]
use parking_lot::{Condvar, Mutex, MutexGuard};
use std::cell::Cell;
#[cfg(not(loom))]
use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
#[cfg(not(loom))]
use std::sync::OnceLock;
#[cfg(not(loom))]
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[cfg(not(loom))]
static NEXT_READER: AtomicUsize = AtomicUsize::new(0);
#[cfg(loom)]
loom::lazy_static! { static ref NEXT_READER: AtomicUsize = AtomicUsize::new(0); }
#[cfg(not(loom))]
thread_local! { static READER: Cell<Option<usize>> = const { Cell::new(None) }; }
#[cfg(loom)]
loom::thread_local! { static READER: Cell<Option<usize>> = Cell::new(None); }

#[cfg(not(loom))]
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock()
}
#[cfg(loom)]
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().expect("model mutex")
}
#[cfg(not(loom))]
fn try_lock<T>(mutex: &Mutex<T>) -> Option<MutexGuard<'_, T>> {
    mutex.try_lock()
}
#[cfg(loom)]
fn try_lock<T>(mutex: &Mutex<T>) -> Option<MutexGuard<'_, T>> {
    mutex.try_lock().ok()
}
#[allow(deprecated, reason = "Atomic::try_update is unavailable on Rust 1.88")]
#[inline]
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
#[cfg(loom)]
fn initialize_parking() {}
#[cfg(not(loom))]
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
#[cfg(loom)]
fn slot_count() -> usize {
    2
}
#[cfg(not(loom))]
fn slot_count() -> usize {
    static COUNT: OnceLock<usize> = OnceLock::new();
    *COUNT.get_or_init(|| {
        std::thread::available_parallelism()
            .map_or(64, usize::from)
            .max(64)
            .next_power_of_two()
    })
}
/// The ordering of every access in the admission handshakes: SeqCst, never
/// weaker (module docs). Loom cannot check this; a unit test counts the uses.
const HANDSHAKE: Ordering = Ordering::SeqCst;

/// In the Loom model only: the SeqCst fence that stands in for the total order
/// of SeqCst accesses, which Loom does not model.
#[inline]
fn model_fence() {
    #[cfg(loom)]
    fence(Ordering::SeqCst);
}

/// The writer admission gate. Closing pairs with a reader's count publication,
/// reopening with a parked reader's waiter interest. The model fence sits
/// between each handshake store and the load that follows it.
struct Gate(AtomicBool);
impl Gate {
    fn new() -> Self {
        Self(AtomicBool::new(false))
    }
    /// Writer: close, then scan the published counts.
    fn close(&self) {
        self.0.store(true, HANDSHAKE);
        model_fence();
    }
    /// Writer: reopen, then check for waiter interest.
    fn open(&self) {
        self.0.store(false, HANDSHAKE);
        model_fence();
    }
    /// Reader: check after publishing a count or waiter interest.
    fn is_closed(&self) -> bool {
        model_fence();
        self.0.load(HANDSHAKE)
    }
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
    gate: Gate,
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
            gate: Gate::new(),
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
                .fetch_or(1 << (index % width), HANDSHAKE);
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
            // Only the owner writes `local`, so a store publishes the count.
            slot.local.store(next, HANDSHAKE);
            Reservation::Local(slot)
        } else {
            slot.shared
                .fetch_update(HANDSHAKE, Ordering::Relaxed, |count| count.checked_add(1))
                .expect("shared reader reservations exhausted");
            Reservation::Shared(slot)
        }
    }
    pub(crate) fn read(&self) -> ReadGuard<'_, T> {
        loop {
            let reservation = ReservationGuard {
                reservation: self.reserve(),
            };
            if !self.gate.is_closed() {
                return ReadGuard {
                    lock: self,
                    #[cfg(loom)]
                    access: self.value.get(),
                    reservation,
                    _thread: PhantomData,
                };
            }
            drop(reservation);
            self.wait_for_writer();
        }
    }
    fn wait_for_writer(&self) {
        // Waiting interest is published before checking the gate. A writer's
        // gate reopen and interest check are the other half of this
        // handshake. Its notification takes this mutex, so it cannot pass the
        // check-to-park registration window.
        self.waiting.fetch_add(1, HANDSHAKE);
        {
            let mut wait = lock(&self.waiters);
            while self.gate.is_closed() {
                #[cfg(not(loom))]
                self.changed.wait(&mut wait);
                #[cfg(loom)]
                {
                    wait = self.changed.wait(wait).expect("model condvar");
                }
            }
        }
        self.waiting.fetch_sub(1, Ordering::Release);
    }
    fn idle(&self) -> bool {
        // A sparse bitmap visits only slots that have actually been used,
        // regardless of the process-wide reader index or available core count.
        self.slots.chunks(usize::BITS as usize).all(|group| {
            let mut used = group[0].initialized.load(HANDSHAKE);
            while used != 0 {
                let slot = &group[used.trailing_zeros() as usize];
                if slot.local.load(HANDSHAKE) != 0 || slot.shared.load(HANDSHAKE) != 0 {
                    return false;
                }
                used &= used - 1;
            }
            true
        })
    }
    pub(crate) fn write(&self) -> WriteGuard<'_, T> {
        let hold = WriterHold::new(self, lock(&self.serial));
        #[cfg(not(loom))]
        let mut spin = parking_lot_core::SpinWait::new();
        // Readers hold only short synchronous decisions. A waiting writer
        // yields after bounded spinning; it never requires reader-drop writes
        // to a shared wakeup word. Long user Clone can prolong this wait.
        while !self.idle() {
            #[cfg(not(loom))]
            if !spin.spin() {
                std::thread::yield_now();
            }
            #[cfg(loom)]
            loom::thread::yield_now();
        }
        WriteGuard {
            #[cfg(loom)]
            access: self.value.get_mut(),
            hold,
        }
    }
    pub(crate) fn try_write(&self) -> Option<WriteGuard<'_, T>> {
        let hold = WriterHold::new(self, try_lock(&self.serial)?);
        if self.idle() {
            Some(WriteGuard {
                #[cfg(loom)]
                access: self.value.get_mut(),
                hold,
            })
        } else {
            None
        }
    }
}
pub(crate) struct ReadGuard<'a, T> {
    #[cfg_attr(loom, allow(dead_code))]
    lock: &'a ReaderSlots<T>,
    // Field order is deliberate: Loom's tracked access ends BEFORE the
    // reservation is released. Native layout has no tracking field.
    #[cfg(loom)]
    access: loom::cell::ConstPtr<T>,
    #[allow(dead_code, reason = "Drop releases the reader reservation")]
    reservation: ReservationGuard<'a>,
    _thread: PhantomData<GuardNoSend>,
}
struct ReservationGuard<'a> {
    reservation: Reservation<'a>,
}
impl<T> Deref for ReadGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        // SAFETY: the counted reservation passed the writer gate check. A
        // writer must see its count before obtaining exclusive value access.
        #[cfg(not(loom))]
        unsafe {
            &*self.lock.value.get()
        }
        #[cfg(loom)]
        unsafe {
            self.access.deref()
        }
    }
}
impl Drop for ReservationGuard<'_> {
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
        lock.gate.close();
        Self {
            lock,
            _serial: serial,
        }
    }
}
impl<T> Drop for WriterHold<'_, T> {
    fn drop(&mut self) {
        self.lock.gate.open();
        if self.lock.waiting.load(HANDSHAKE) != 0 {
            let _wait = lock(&self.lock.waiters);
            self.lock.changed.notify_all();
        }
    }
}
pub(crate) struct WriteGuard<'a, T> {
    // End tracked exclusive access before WriterHold reopens admission.
    #[cfg(loom)]
    access: loom::cell::MutPtr<T>,
    #[cfg_attr(loom, allow(dead_code))]
    hold: WriterHold<'a, T>,
}
impl<T> Deref for WriteGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        // SAFETY: all published readers left after the gate closed, and this
        // guard retains the serial mutex through its complete value access.
        #[cfg(not(loom))]
        unsafe {
            &*self.hold.lock.value.get()
        }
        #[cfg(loom)]
        unsafe {
            self.access.deref()
        }
    }
}
impl<T> DerefMut for WriteGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        // SAFETY: the fully admitted exclusive guard and its mutable borrow
        // exclude every other access to T.
        #[cfg(not(loom))]
        unsafe {
            &mut *self.hold.lock.value.get()
        }
        #[cfg(loom)]
        unsafe {
            self.access.deref()
        }
    }
}

#[cfg(all(test, not(loom)))]
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

#[cfg(all(test, not(loom)))]
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

    /// Loom checks only the fenced equivalent of the admission handshakes, so
    /// it cannot notice one of their accesses weakened below SeqCst. This
    /// tripwire counts them instead: the definition, three in `Gate`, three in
    /// `reserve`, one in `wait_for_writer`, three in `idle` and one when the
    /// writer reopens the gate.
    #[test]
    fn every_handshake_access_stays_sequentially_consistent() {
        assert_eq!(HANDSHAKE, Ordering::SeqCst);
        let primitive = include_str!("reader_slots.rs")
            .split("#[cfg(all(test, not(loom)))]")
            .next()
            .expect("primitive source precedes its tests");
        let uses: usize = primitive
            .lines()
            .filter(|line| !line.trim_start().starts_with("//"))
            .map(|line| line.matches("HANDSHAKE").count())
            .sum();
        assert_eq!(
            uses, 12,
            "a handshake access changed; each must use HANDSHAKE (module docs)"
        );
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

#[cfg(all(test, not(loom)))]
#[path = "reader_slots/allocation.rs"]
mod allocation;

#[cfg(all(test, loom))]
#[path = "reader_slots/loom_tests.rs"]
mod loom_tests;

#[cfg(all(test, not(loom)))]
#[path = "reader_slots/safety_contract.rs"]
mod safety_contract;
