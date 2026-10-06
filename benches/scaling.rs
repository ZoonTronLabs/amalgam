//! Public-API scaling fixture. Timings are aggregate elapsed / completed operations.
//! Counting is thread-local; the instrumented allocator never shares a hot counter.
use amalgam::{BlockingCache, Cache, EntryOptions};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::hint::black_box;
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

const OPERATIONS: usize = 300_000;
const WARMUP: usize = 20_000;
#[derive(Clone, Copy)]
enum Measurement {
    Inactive,
    Counting(usize),
}
thread_local! { static ALLOCATIONS: Cell<Measurement> = const { Cell::new(Measurement::Inactive) }; }
fn allocated() {
    let _ = ALLOCATIONS.try_with(|counter| {
        if let Measurement::Counting(count) = counter.get() {
            counter.set(Measurement::Counting(count + 1));
        }
    });
}
struct CountingSystem;
// SAFETY: every operation delegates to System with the caller's original layout.
// The only additional effect is a destructor-free thread-local measurement.
unsafe impl GlobalAlloc for CountingSystem {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: GlobalAlloc's caller supplies a valid layout for System.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            allocated();
        }
        pointer
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forward the original valid layout without modification.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            allocated();
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
            allocated();
        }
        pointer
    }
}
#[global_allocator]
static ALLOCATOR: CountingSystem = CountingSystem;
fn begin_counting() {
    ALLOCATIONS.with(|counter| counter.set(Measurement::Counting(0)));
}
fn end_counting() -> usize {
    ALLOCATIONS.with(|counter| match counter.replace(Measurement::Inactive) {
        Measurement::Counting(count) => count,
        Measurement::Inactive => panic!("measurement was not enabled"),
    })
}
fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}
fn cache() -> Cache<u64> {
    Cache::builder()
        .default_options(EntryOptions::new(Duration::from_secs(3600)))
        .build()
}
fn scaling(cache: &Cache<u64>, keys: &[String], workers: usize, same: bool) {
    let barrier = Arc::new(Barrier::new(workers + 1));
    std::thread::scope(|scope| {
        let threads: Vec<_> = (0..workers)
            .map(|worker| {
                let gate = barrier.clone();
                let index = if same { 0 } else { worker };
                let key = &keys[index];
                scope.spawn(move || {
                    runtime().block_on(async {
                        for _ in 0..WARMUP {
                            assert_eq!(
                                cache.read(key, None).await.unwrap().into_value(),
                                Some(index as u64 + 1)
                            );
                        }
                        gate.wait();
                        gate.wait();
                        begin_counting();
                        let mut checksum = 0_u64;
                        for _ in 0..OPERATIONS {
                            checksum += black_box(
                                cache.read(key, None).await.unwrap().into_value().unwrap(),
                            );
                        }
                        let allocations = end_counting();
                        assert_eq!(checksum, OPERATIONS as u64 * (index as u64 + 1));
                        allocations
                    })
                })
            })
            .collect();
        barrier.wait();
        let began = Instant::now();
        barrier.wait();
        let allocations: usize = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .sum();
        let elapsed = began.elapsed();
        let label = if same { "same" } else { "distinct" };
        let operations = workers * OPERATIONS;
        println!(
            "{label},{workers},{operations},{:.3},{allocations}",
            elapsed.as_nanos() as f64 / operations as f64
        );
        assert_eq!(allocations, 0, "a warmed scalar hit allocated");
    });
}
fn synchronous_hit() {
    let cache = BlockingCache::from_builder(
        Cache::builder().default_options(EntryOptions::new(Duration::from_secs(3600))),
    )
    .unwrap();
    cache.try_set("sync", 1_u64).unwrap().wait().unwrap();
    for _ in 0..WARMUP {
        assert_eq!(cache.read("sync", None).unwrap().into_value(), Some(1));
    }
    begin_counting();
    let began = Instant::now();
    let mut checksum = 0_u64;
    for _ in 0..OPERATIONS {
        checksum += black_box(cache.read("sync", None).unwrap().into_value().unwrap());
    }
    let elapsed = began.elapsed();
    let allocations = end_counting();
    assert_eq!(checksum, OPERATIONS as u64);
    assert_eq!(allocations, 0, "a warmed native hit allocated");
    println!(
        "sync,1,{OPERATIONS},{:.3},{allocations}",
        elapsed.as_nanos() as f64 / OPERATIONS as f64
    );
    cache.shutdown().unwrap();
}
fn mutations(rt: &tokio::runtime::Runtime) {
    let writes = cache();
    rt.block_on(async {
        for value in 0..WARMUP {
            writes
                .try_set("replace", value as u64)
                .await
                .unwrap()
                .wait()
                .await
                .unwrap();
        }
        begin_counting();
        let began = Instant::now();
        for value in 1..=WARMUP {
            writes
                .try_set("replace", value as u64)
                .await
                .unwrap()
                .wait()
                .await
                .unwrap();
        }
        let elapsed = began.elapsed();
        let allocations = end_counting();
        println!(
            "set,1,{WARMUP},{:.3},{allocations}",
            elapsed.as_nanos() as f64 / WARMUP as f64
        );
        assert_eq!(
            writes.read("replace", None).await.unwrap().into_value(),
            Some(WARMUP as u64)
        );
        let warm = cache();
        for id in 0..WARMUP {
            assert_eq!(
                warm.get_or_set(format!("warm-{id}"), |context| async move {
                    Ok(context.value(7))
                })
                .await
                .unwrap(),
                7
            );
        }
        warm.shutdown().await.unwrap();
        let keys: Vec<_> = (0..WARMUP).map(|id| format!("cold-{id}")).collect();
        begin_counting();
        let began = Instant::now();
        for key in keys {
            assert_eq!(
                black_box(
                    writes
                        .get_or_set(key, |context| async move { Ok(context.value(7)) })
                        .await
                        .unwrap()
                ),
                7
            );
        }
        let elapsed = began.elapsed();
        let allocations = end_counting();
        println!(
            "cold,1,{WARMUP},{:.3},{allocations}",
            elapsed.as_nanos() as f64 / WARMUP as f64
        );
        writes.shutdown().await.unwrap();
    });
}
fn main() {
    if std::env::args().any(|argument| argument == "--costs") {
        ready_costs();
        return;
    }
    let rt = runtime();
    let cache = cache();
    let keys: Vec<_> = (0..8).map(|id| format!("key-{id}")).collect();
    rt.block_on(async {
        for (id, key) in keys.iter().enumerate() {
            cache
                .try_set(key, id as u64 + 1)
                .await
                .unwrap()
                .wait()
                .await
                .unwrap();
        }
    });
    println!("scenario,threads,operations,ns_per_op,allocations");
    for workers in [1, 2, 4, 8] {
        for same in [true, false] {
            scaling(&cache, &keys, workers, same);
        }
    }
    rt.block_on(cache.shutdown()).unwrap();
    synchronous_hit();
    mutations(&rt);
}

fn cost(label: &str, mut operation: impl FnMut() -> u64) {
    for _ in 0..WARMUP {
        black_box(operation());
    }
    let began = Instant::now();
    let mut checksum = 0_u64;
    for _ in 0..OPERATIONS {
        checksum = checksum.wrapping_add(black_box(operation()));
    }
    black_box(checksum);
    println!(
        "{label},{:.3}",
        began.elapsed().as_nanos() as f64 / OPERATIONS as f64
    );
}
struct CostClock(amalgam::ClockTiming);
impl amalgam::Clock for CostClock {
    fn now(&self) -> amalgam::Timestamp {
        amalgam::Timestamp::from_ticks(10_000_000_000)
    }
    fn timing_model(&self) -> amalgam::ClockTiming {
        self.0
    }
}
fn ready_costs() {
    use amalgam::Clock;
    println!("component,ns_per_op");
    cost("system_clock", || amalgam::SystemClock.now().ticks() as u64);
    cost("monotonic_clock", || {
        black_box(Instant::now());
        1
    });
    let duration = Duration::new(1_790_000_000, 123_456_789);
    cost("duration_ticks", || {
        amalgam::time::duration_to_ticks(black_box(duration)) as u64
    });
    let rt = runtime();
    for (label, clock) in [
        (
            "controlled",
            Arc::new(CostClock(amalgam::ClockTiming::Controlled)) as Arc<dyn Clock>,
        ),
        (
            "physical",
            Arc::new(CostClock(amalgam::ClockTiming::RealTime)) as Arc<dyn Clock>,
        ),
        ("system", Arc::new(amalgam::SystemClock) as Arc<dyn Clock>),
    ] {
        let builder = || {
            Cache::builder()
                .clock(clock.clone())
                .default_options(EntryOptions::new(Duration::from_secs(3600)))
        };
        let cache = builder().build();
        rt.block_on(async {
            cache
                .try_set("cost", 7_u64)
                .await
                .unwrap()
                .wait()
                .await
                .unwrap();
        });
        rt.block_on(async {
            cost(&format!("{label}_async"), || {
                let mut future = std::pin::pin!(cache.read("cost", None));
                let mut context = std::task::Context::from_waker(std::task::Waker::noop());
                match std::future::Future::poll(future.as_mut(), &mut context) {
                    std::task::Poll::Ready(value) => value.unwrap().into_value().unwrap(),
                    std::task::Poll::Pending => panic!("a warmed read must be ready"),
                }
            });
        });
        rt.block_on(cache.shutdown()).unwrap();
        let native = BlockingCache::from_builder(builder()).unwrap();
        native.try_set("cost", 7_u64).unwrap().wait().unwrap();
        cost(&format!("{label}_native"), || {
            native.read("cost", None).unwrap().into_value().unwrap()
        });
        native.shutdown().unwrap();
    }
}
