//! A warmed, unobserved scalar L1 read needs no heap-owned operation context.
//! Unsafe code is restricted to this test allocator forwarding System's contract.

use amalgam::{Cache, EntryOptions};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::future::Future;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

#[derive(Clone, Copy)]
enum Measurement {
    Inactive,
    Counting(usize),
}

thread_local! {
    static MEASUREMENT: Cell<Measurement> = const { Cell::new(Measurement::Inactive) };
}

fn allocated() {
    let _ = MEASUREMENT.try_with(|state| match state.get() {
        Measurement::Inactive => {}
        Measurement::Counting(count) => state.set(Measurement::Counting(count.saturating_add(1))),
    });
}

struct CountingSystem;

// SAFETY: every operation forwards the original pointer, layout and size to
// System. The allocation-free thread-local counter changes no allocator contract.
unsafe impl GlobalAlloc for CountingSystem {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: GlobalAlloc's caller supplies a valid nonzero layout.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            allocated();
        }
        pointer
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: System receives the unchanged caller-provided layout.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            allocated();
        }
        pointer
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: System owns the pointer allocated through this wrapper, with
        // the exact original layout required by GlobalAlloc::dealloc.
        unsafe { System.dealloc(pointer, layout) };
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: forward the System-owned allocation and valid original
        // layout/new size exactly as supplied by GlobalAlloc's caller.
        let pointer = unsafe { System.realloc(pointer, layout, new_size) };
        if !pointer.is_null() {
            allocated();
        }
        pointer
    }
}

#[global_allocator]
static ALLOCATOR: CountingSystem = CountingSystem;

struct MeasurementGuard;
impl Drop for MeasurementGuard {
    fn drop(&mut self) {
        MEASUREMENT.with(|state| state.set(Measurement::Inactive));
    }
}

fn measure_allocations(work: impl FnOnce()) -> usize {
    MEASUREMENT.with(|state| {
        assert!(matches!(state.get(), Measurement::Inactive));
        state.set(Measurement::Counting(0));
    });
    let guard = MeasurementGuard;
    work();
    let count = MEASUREMENT.with(|state| match state.get() {
        Measurement::Counting(count) => count,
        Measurement::Inactive => unreachable!("measurement remains active until guard release"),
    });
    drop(guard);
    count
}

#[test]
fn unobserved_warmed_scalar_ready_reads_do_not_allocate() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let cache = Cache::<u64>::builder()
        .default_options(EntryOptions::new(Duration::from_secs(3600)))
        .try_build()
        .unwrap();
    runtime.block_on(async {
        cache
            .set("key", 17)
            .with_receipt()
            .await
            .unwrap()
            .wait()
            .await
            .unwrap();
        for _ in 0..1000 {
            assert_eq!(
                cache.read("key", None).await.unwrap().into_value(),
                Some(17)
            );
        }
    });
    let mut context = Context::from_waker(Waker::noop());
    let allocations = measure_allocations(|| {
        for _ in 0..1000 {
            let mut lookup = std::pin::pin!(cache.read("key", None));
            match lookup.as_mut().poll(&mut context) {
                Poll::Ready(result) => assert_eq!(result.unwrap().into_value(), Some(17)),
                Poll::Pending => panic!("warmed scalar L1 read must complete on its first poll"),
            }
        }
    });
    runtime.block_on(cache.shutdown()).unwrap();
    assert_eq!(
        allocations, 0,
        "warmed ready reads allocated {allocations} blocks"
    );
}

#[test]
fn unobserved_warmed_native_scalar_factories_do_not_allocate() {
    let cache = amalgam::BlockingCache::<u64>::from_builder(
        Cache::builder().default_options(EntryOptions::new(Duration::from_secs(3600))),
    )
    .unwrap();
    cache
        .set("key", 19)
        .with_receipt()
        .execute()
        .unwrap()
        .wait()
        .unwrap();
    for _ in 0..1000 {
        assert_eq!(
            cache
                .get_or_set(
                    "key",
                    typed_blocking_factory(|_| panic!("warm value invoked factory"))
                )
                .execute()
                .unwrap(),
            19
        );
    }
    let allocations = measure_allocations(|| {
        for _ in 0..1000 {
            assert_eq!(
                cache
                    .get_or_set(
                        "key",
                        typed_blocking_factory(|_| panic!("warm value invoked factory"))
                    )
                    .execute()
                    .unwrap(),
                19
            );
        }
    });
    cache.shutdown().unwrap();
    assert_eq!(
        allocations, 0,
        "native warm factories allocated {allocations} unused blocks"
    );
}

fn typed_blocking_factory<V, F>(factory: F) -> F
where
    F: FnOnce(amalgam::FactoryContext<V>) -> std::result::Result<V, amalgam::FactoryError>,
{
    factory
}
