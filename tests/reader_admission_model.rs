//! Model the private reader admission and parking protocols with weak ordering.
//! Keep the publication order and fences aligned with src/reader_slots.rs.
//! These bounded schedules supplement storage/drop stress tests; they do not
//! claim exhaustive verification of every execution or the complete cache.
use loom::cell::UnsafeCell;
use loom::sync::atomic::{AtomicBool, AtomicUsize, Ordering, fence};
use loom::sync::{Arc, Condvar, Mutex};

struct State {
    bound: AtomicUsize,
    owner: AtomicUsize,
    local: AtomicUsize,
    shared: AtomicUsize,
    writer: AtomicBool,
    value: UnsafeCell<usize>,
}
// SAFETY: the tested admission protocol excludes mutable/immutable overlap.
unsafe impl Sync for State {}

#[test]
fn first_use_and_colliding_readers_cannot_overlap_a_writer() {
    let mut model = loom::model::Builder::new();
    model.preemption_bound = Some(3);
    model.max_permutations = Some(20_000);
    model.check(|| {
        let state = Arc::new(State {
            bound: AtomicUsize::new(0),
            owner: AtomicUsize::new(0),
            local: AtomicUsize::new(0),
            shared: AtomicUsize::new(0),
            writer: AtomicBool::new(false),
            value: UnsafeCell::new(0),
        });
        let readers: Vec<_> = (1..=2)
            .map(|identity| {
                let state = state.clone();
                loom::thread::spawn(move || {
                    let owner = state.owner.load(Ordering::Acquire);
                    if owner == 0 {
                        state.bound.fetch_max(1, Ordering::Release);
                    }
                    let local = owner == identity
                        || owner == 0
                            && state
                                .owner
                                .compare_exchange(0, identity, Ordering::AcqRel, Ordering::Acquire)
                                .is_ok();
                    let count = if local { &state.local } else { &state.shared };
                    if local {
                        count.store(1, Ordering::Release);
                    } else {
                        count.fetch_add(1, Ordering::Release);
                    }
                    fence(Ordering::SeqCst);
                    if !state.writer.load(Ordering::Acquire) {
                        state.value.with(|pointer| {
                            // SAFETY: read reservation passed admission.
                            assert!(unsafe { *pointer } <= 1);
                        });
                    }
                    if local {
                        count.store(0, Ordering::Release);
                    } else {
                        count.fetch_sub(1, Ordering::Release);
                    }
                })
            })
            .collect();
        let writer_state = state.clone();
        let writer = loom::thread::spawn(move || {
            writer_state.writer.store(true, Ordering::Release);
            fence(Ordering::SeqCst);
            let idle = writer_state.bound.load(Ordering::Acquire) == 0
                || writer_state.local.load(Ordering::Acquire) == 0
                    && writer_state.shared.load(Ordering::Acquire) == 0;
            if idle {
                writer_state.value.with_mut(|pointer| {
                    // SAFETY: exclusive writer passed the count scan.
                    unsafe {
                        *pointer = 1;
                    }
                });
            }
            writer_state.writer.store(false, Ordering::Release);
        });
        for reader in readers {
            reader.join().unwrap();
        }
        writer.join().unwrap();
    });
}

#[test]
fn opening_a_writer_gate_cannot_lose_a_reader_wakeup() {
    let mut model = loom::model::Builder::new();
    model.preemption_bound = Some(3);
    model.check(|| {
        let state = Arc::new((
            AtomicBool::new(true),
            AtomicUsize::new(0),
            Mutex::new(()),
            Condvar::new(),
        ));
        let reader_state = state.clone();
        let reader = loom::thread::spawn(move || {
            let (gate, waiting, mutex, changed) = &*reader_state;
            waiting.fetch_add(1, Ordering::Release);
            fence(Ordering::SeqCst);
            let mut wait = mutex.lock().unwrap();
            while gate.load(Ordering::Acquire) {
                wait = changed.wait(wait).unwrap();
            }
            drop(wait);
            waiting.fetch_sub(1, Ordering::Release);
        });
        let (gate, waiting, mutex, changed) = &*state;
        gate.store(false, Ordering::Release);
        fence(Ordering::SeqCst);
        if waiting.load(Ordering::Acquire) != 0 {
            let _wait = mutex.lock().unwrap();
            changed.notify_all();
        }
        reader.join().unwrap();
    });
}

#[test]
fn one_storage_fence_also_publishes_shutdown_activity() {
    struct Shared {
        active: AtomicUsize,
        readers: AtomicUsize,
        closing: AtomicBool,
        writer: AtomicBool,
        stored: UnsafeCell<usize>,
        callback: UnsafeCell<usize>,
    }
    // SAFETY: model-check both storage exclusion and operation drainage.
    unsafe impl Sync for Shared {}
    let mut model = loom::model::Builder::new();
    model.preemption_bound = Some(3);
    model.max_permutations = Some(20_000);
    model.check(|| {
        let state = Arc::new(Shared {
            active: AtomicUsize::new(0),
            readers: AtomicUsize::new(0),
            closing: AtomicBool::new(false),
            writer: AtomicBool::new(false),
            stored: UnsafeCell::new(0),
            callback: UnsafeCell::new(0),
        });
        let reading = state.clone();
        let reader = loom::thread::spawn(move || {
            reading.active.store(1, Ordering::Release);
            reading.readers.store(1, Ordering::Release);
            fence(Ordering::SeqCst);
            if !reading.writer.load(Ordering::Acquire) && !reading.closing.load(Ordering::SeqCst) {
                reading.stored.with(|pointer| {
                    // SAFETY: the reader fence passed both admission gates.
                    assert!(unsafe { *pointer } <= 1);
                    reading.callback.with(|pointer| {
                        // SAFETY: shutdown cannot drain a started callback.
                        assert_eq!(unsafe { *pointer }, 0);
                        loom::thread::yield_now();
                        assert_eq!(unsafe { *pointer }, 0);
                    });
                });
            }
            reading.readers.store(0, Ordering::Release);
            reading.active.store(0, Ordering::Release);
        });
        let writing = state.clone();
        let writer = loom::thread::spawn(move || {
            writing.writer.store(true, Ordering::Release);
            fence(Ordering::SeqCst);
            if writing.readers.load(Ordering::Acquire) == 0 {
                writing.stored.with_mut(|pointer| {
                    // SAFETY: exclusive admission scanned the reader count.
                    unsafe {
                        *pointer = 1;
                    }
                });
            }
            writing.writer.store(false, Ordering::Release);
        });
        state.closing.swap(true, Ordering::SeqCst);
        fence(Ordering::SeqCst);
        if state.active.load(Ordering::SeqCst) == 0 {
            state.callback.with_mut(|pointer| {
                // SAFETY: either the callback completed or admission rejects it.
                unsafe {
                    *pointer = 1;
                }
            });
        }
        reader.join().unwrap();
        writer.join().unwrap();
    });
}
