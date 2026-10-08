//! Untimed allocation stacks for the public L2 fixture.
//! Kept in a separate executable so tracing cannot affect the scaling allocator.
//! Run explicitly: cargo test --release --bench l2_ownership -- --ignored --nocapture
use amalgam::{
    Cache, EntryOptions, provider::InMemoryDistributedCache, provider::JsonSerializer,
    provider::SystemClock,
};
use std::alloc::{GlobalAlloc, Layout, System};
use std::backtrace::Backtrace;
use std::cell::Cell;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Copy)]
enum Probe {
    Inactive,
    Tracing(usize),
}
thread_local! { static PROBE: Cell<Probe> = const { Cell::new(Probe::Inactive) }; }

fn allocated(bytes: usize) {
    let _ = PROBE.try_with(|probe| {
        if let Probe::Tracing(count) = probe.get() {
            // Standard-library tracing also allocates. Suppress it recursively;
            // fallible output never invokes a user-supplied formatter.
            probe.set(Probe::Inactive);
            let trace = Backtrace::force_capture();
            let _ = writeln!(
                std::io::stderr().lock(),
                "allocation {count}: {bytes} bytes\n{trace}"
            );
            probe.set(Probe::Tracing(count + 1));
        }
    });
}
struct TracingSystem;
// SAFETY: all operations forward the original valid layout to System. Tracing
// uses destructor-free thread-local state and disables itself before capture.
unsafe impl GlobalAlloc for TracingSystem {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: GlobalAlloc's caller supplies a valid layout for System.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            allocated(layout.size());
        }
        pointer
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forward the original valid layout without modification.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            allocated(layout.size());
        }
        pointer
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: this allocation originated in System with this same layout.
        unsafe { System.dealloc(pointer, layout) };
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        // SAFETY: forward the System-owned allocation and caller's valid size.
        let pointer = unsafe { System.realloc(pointer, layout, size) };
        if !pointer.is_null() {
            allocated(size);
        }
        pointer
    }
}
#[global_allocator]
static ALLOCATOR: TracingSystem = TracingSystem;

#[derive(Clone, Copy)]
enum Lookup {
    Read,
    GetOrSet,
}
impl Lookup {
    async fn value(self, cache: &Cache<u64>) -> u64 {
        match self {
            Self::Read => cache.try_get("l2-json").await.unwrap().unwrap(),
            Self::GetOrSet => cache
                .get_or_set(
                    "l2-json",
                    amalgam::source::factory(|context| async move {
                        Err(context.fail("a warmed L2 factory must never run"))
                    }),
                )
                .await
                .unwrap(),
        }
    }
}
async fn fixture() -> Cache<u64> {
    let options = EntryOptions::new(Duration::from_secs(3600)).with_skip_memory(true, false);
    let cache = Cache::builder()
        .default_options(options.clone())
        .distributed(Arc::new(InMemoryDistributedCache::new(Arc::new(
            SystemClock,
        ))))
        .serializer(Arc::new(JsonSerializer))
        .try_build()
        .unwrap();
    cache
        .set("l2-json", 7)
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    cache
        .set("l2-json", 11)
        .options(|_| options.with_skip_distributed(false, true))
        .with_receipt()
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    cache
}
fn trace(lookup: Lookup) {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let cache = fixture().await;
            for _ in 0..20_000 {
                assert_eq!(lookup.value(&cache).await, 7);
            }
            PROBE.with(|probe| probe.set(Probe::Tracing(0)));
            let value = lookup.value(&cache).await;
            let count = PROBE.with(|probe| match probe.replace(Probe::Inactive) {
                Probe::Tracing(count) => count,
                Probe::Inactive => panic!("the allocation probe was not enabled"),
            });
            assert_eq!(value, 7);
            let _ = writeln!(
                std::io::stdout().lock(),
                "one untimed L2 lookup: {count} allocation stacks"
            );
            cache.shutdown().await.unwrap();
        });
}

#[test]
#[ignore = "untimed allocation-stack diagnostic"]
fn read_ownership() {
    trace(Lookup::Read);
}

#[test]
#[ignore = "untimed allocation-stack diagnostic"]
fn get_or_set_ownership() {
    trace(Lookup::GetOrSet);
}
