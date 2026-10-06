//! A warmed reader must remain allocation-free when it first meets a writer.
use super::*;
use parking_lot_core::{DEFAULT_PARK_TOKEN, DEFAULT_UNPARK_TOKEN, FilterOp};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::{Arc, Barrier, mpsc};
use std::time::{Duration, Instant};

#[derive(Clone, Copy)]
enum Measurement {
    Inactive,
    Counting(usize),
}
thread_local! {
    static ALLOCATIONS: Cell<Measurement> = const { Cell::new(Measurement::Inactive) };
}
fn allocated() {
    let _ = ALLOCATIONS.try_with(|counter| {
        if let Measurement::Counting(count) = counter.get() {
            counter.set(Measurement::Counting(count + 1));
        }
    });
}
struct CountingSystem;
// SAFETY: every allocation is delegated to System unchanged. The additional
// counter is destructor-free TLS, with no allocation or synchronization.
unsafe impl GlobalAlloc for CountingSystem {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller supplied System's required valid layout.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            allocated();
        }
        pointer
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forward the original valid layout.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            allocated();
        }
        pointer
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: this System allocation retains its original layout.
        unsafe { System.dealloc(pointer, layout) };
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        // SAFETY: forward the original allocation and valid requested size.
        let pointer = unsafe { System.realloc(pointer, layout, size) };
        if !pointer.is_null() {
            allocated();
        }
        pointer
    }
}
#[global_allocator]
static ALLOCATOR: CountingSystem = CountingSystem;

fn queued<T>(lock: &ReaderSlots<T>) -> usize {
    let key = &lock.slots[0].0 as *const RawMutex as usize;
    let mut found = 0;
    // SAFETY: the address belongs to this live RawMutex. The filter only
    // observes its queue: Skip never removes or wakes a waiter. Both callbacks
    // are infallible and invoke no parking_lot operation while the queue locks.
    unsafe {
        parking_lot_core::unpark_filter(
            key,
            |_| {
                found += 1;
                FilterOp::Skip
            },
            |_| DEFAULT_UNPARK_TOKEN,
        );
    }
    found
}

#[test]
fn warmed_reader_does_not_allocate_on_its_first_contended_slot() {
    // Keep another initialized thread alive. In an isolated run the parking
    // table initially covers this thread alone, exposing lazy table growth in
    // the reader if its warmup did not initialize parking metadata.
    let address = 0_usize;
    // SAFETY: this owned live address is only an opaque queue key. Validation
    // always rejects before enqueue; no callback can panic, park or reenter.
    unsafe {
        parking_lot_core::park(
            &address as *const usize as usize,
            || false,
            || {},
            |_, _| {},
            DEFAULT_PARK_TOKEN,
            None,
        );
    }
    let lock = Arc::new(ReaderSlots::with_slots(1_u64, 1));
    const READERS: usize = 64;
    let barrier = Arc::new(Barrier::new(READERS + 1));
    let (warmed, ready) = mpsc::channel();
    let readers: Vec<_> = (0..READERS)
        .map(|_| {
            let lock = lock.clone();
            let barrier = barrier.clone();
            let warmed = warmed.clone();
            let thread = std::thread::spawn(move || {
                assert_eq!(*lock.read(), 1);
                warmed.send(()).unwrap();
                barrier.wait();
                ALLOCATIONS.with(|counter| counter.set(Measurement::Counting(0)));
                let value = *lock.read();
                let allocations =
                    ALLOCATIONS.with(|counter| match counter.replace(Measurement::Inactive) {
                        Measurement::Counting(count) => count,
                        Measurement::Inactive => unreachable!("counter was enabled"),
                    });
                (value, allocations)
            });
            // Each warm read is uncontended; first parking must happen during the
            // measured read rather than accidentally during concurrent warmup.
            ready.recv_timeout(Duration::from_secs(3)).unwrap();
            thread
        })
        .collect();
    let mut guard = lock.write();
    *guard = 17;
    barrier.wait();
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut was_queued = false;
    while Instant::now() < deadline {
        if queued(&lock) == READERS {
            was_queued = true;
            break;
        }
        std::thread::yield_now();
    }
    drop(guard);
    let results: Vec<_> = readers
        .into_iter()
        .map(|reader| reader.join().unwrap())
        .collect();
    assert!(
        was_queued,
        "the reader must actually enter the parking queue"
    );
    assert!(
        results.iter().all(|(value, _)| *value == 17),
        "contention must not become a miss"
    );
    let allocations: usize = results.iter().map(|(_, count)| count).sum();
    assert_eq!(allocations, 0, "first contended warm reads allocated");
}
