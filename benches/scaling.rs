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
const SET_OPERATIONS: usize = 1_000_000;
const COLD_OPERATIONS: usize = 100_000;
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
trait WarmHit {
    async fn value(cache: &Cache<u64>, key: &str) -> u64;
    fn native(cache: &BlockingCache<u64>, key: &str) -> u64;
}
struct ReadHit;
impl WarmHit for ReadHit {
    async fn value(cache: &Cache<u64>, key: &str) -> u64 {
        cache.read(key, None).await.unwrap().into_value().unwrap()
    }
    fn native(cache: &BlockingCache<u64>, key: &str) -> u64 {
        cache.read(key, None).unwrap().into_value().unwrap()
    }
}
struct OriginHit;
impl WarmHit for OriginHit {
    async fn value(cache: &Cache<u64>, key: &str) -> u64 {
        cache
            .get_or_set(key, |ctx| async move {
                Err(ctx.fail("a warmed factory must never run"))
            })
            .await
            .unwrap()
    }
    fn native(cache: &BlockingCache<u64>, key: &str) -> u64 {
        cache
            .get_or_set(key, |ctx| {
                Err(ctx.fail("a warmed native factory must never run"))
            })
            .unwrap()
    }
}
fn scaling<H: WarmHit>(cache: &Cache<u64>, keys: &[String], workers: usize, same: bool) {
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
                            assert_eq!(H::value(cache, key).await, index as u64 + 1);
                        }
                        gate.wait();
                        gate.wait();
                        begin_counting();
                        let mut checksum = 0_u64;
                        for _ in 0..OPERATIONS {
                            checksum += black_box(H::value(cache, key).await);
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
fn synchronous_hit<H: WarmHit>() {
    let cache = BlockingCache::from_builder(
        Cache::builder().default_options(EntryOptions::new(Duration::from_secs(3600))),
    )
    .unwrap();
    cache.try_set("sync", 1_u64).unwrap().wait().unwrap();
    for _ in 0..WARMUP {
        assert_eq!(H::native(&cache, "sync"), 1);
    }
    begin_counting();
    let began = Instant::now();
    let mut checksum = 0_u64;
    for _ in 0..OPERATIONS {
        checksum += black_box(H::native(&cache, "sync"));
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
            writes.set("replace", value as u64).await.unwrap();
        }
        begin_counting();
        let began = Instant::now();
        for value in 1..=SET_OPERATIONS {
            writes.set("replace", value as u64).await.unwrap();
        }
        let elapsed = began.elapsed();
        let allocations = end_counting();
        println!(
            "set,1,{SET_OPERATIONS},{:.3},{allocations}",
            elapsed.as_nanos() as f64 / SET_OPERATIONS as f64
        );
        assert_eq!(
            writes.read("replace", None).await.unwrap().into_value(),
            Some(SET_OPERATIONS as u64)
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
        drop(warm);
        let keys: Vec<_> = (0..COLD_OPERATIONS)
            .map(|id| format!("cold-{id}"))
            .collect();
        begin_counting();
        let began = Instant::now();
        for key in &keys {
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
            "cold,1,{COLD_OPERATIONS},{:.3},{allocations}",
            elapsed.as_nanos() as f64 / COLD_OPERATIONS as f64
        );
        writes.shutdown().await.unwrap();
    });
}
fn main() {
    if std::env::args().any(|argument| argument == "--metadata-costs") {
        metadata_costs();
        return;
    }
    if std::env::args().any(|argument| argument == "--costs") {
        ready_costs();
        return;
    }
    match std::env::args().skip(1).collect::<Vec<_>>().as_slice() {
        [flag] if flag == "--mutations" => {
            println!("scenario,threads,operations,ns_per_op,allocations");
            mutations(&runtime());
        }
        [] => run::<ReadHit>(),
        [flag, value] if flag == "--api" && value == "read" => run::<ReadHit>(),
        [flag, value] if flag == "--api" && value == "get-or-set" => run::<OriginHit>(),
        args => panic!("expected --api read|get-or-set or --mutations, got {args:?}"),
    }
}
fn run<H: WarmHit>() {
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
            scaling::<H>(&cache, &keys, workers, same);
        }
    }
    rt.block_on(cache.shutdown()).unwrap();
    synchronous_hit::<H>();
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
    cost("system_time", || {
        black_box(std::time::SystemTime::now());
        1
    });
    cost("system_duration", || {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    });
    let hash = std::collections::hash_map::RandomState::new();
    cost("key_hash", || {
        use std::hash::BuildHasher;
        hash.hash_one(black_box("key-0"))
    });
    let keyed = ahash::RandomState::new();
    cost("key_hash_ahash", || keyed.hash_one(black_box("key-0")));
    let active = std::sync::atomic::AtomicUsize::new(0);
    cost("counter_pair", || {
        active.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        active.fetch_sub(1, std::sync::atomic::Ordering::SeqCst) as u64
    });
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
            Some(Arc::new(CostClock(amalgam::ClockTiming::Controlled)) as Arc<dyn Clock>),
        ),
        (
            "physical",
            Some(Arc::new(CostClock(amalgam::ClockTiming::RealTime)) as Arc<dyn Clock>),
        ),
        (
            "system",
            Some(Arc::new(amalgam::SystemClock) as Arc<dyn Clock>),
        ),
        ("default", None),
    ] {
        let builder = || {
            let builder =
                Cache::builder().default_options(EntryOptions::new(Duration::from_secs(3600)));
            match &clock {
                Some(clock) => builder.clock(Arc::clone(clock)),
                None => builder,
            }
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

/// Separate enabled-feature costs from the default L1 performance budget.
/// All cases exercise the public entry or cache APIs and verify their facts.
#[derive(Clone, Copy)]
enum MetadataCase {
    Plain,
    Eager,
    Tagged,
}
impl MetadataCase {
    fn label(self) -> &'static str {
        match self {
            Self::Plain => "plain",
            Self::Eager => "eager",
            Self::Tagged => "tagged",
        }
    }
    fn options(self) -> EntryOptions {
        let options = EntryOptions::new(Duration::from_secs(3600));
        match self {
            Self::Plain | Self::Tagged => options,
            Self::Eager => options.with_eager_refresh(amalgam::EagerThreshold::new(0.5)),
        }
    }
    fn tags(self, tag: &amalgam::Tag) -> Box<[amalgam::Tag]> {
        match self {
            Self::Plain | Self::Eager => Box::new([]),
            Self::Tagged => Box::new([tag.clone()]),
        }
    }
}
fn metadata_cost(label: &str, mut operation: impl FnMut()) {
    // Owned scalar snapshots otherwise take only a few milliseconds. Use a
    // longer diagnostic sample without changing the paired workloads.
    const METADATA_WARMUP: usize = 100_000;
    const METADATA_OPERATIONS: usize = 3_000_000;
    for _ in 0..METADATA_WARMUP {
        operation();
    }
    begin_counting();
    let began = Instant::now();
    for _ in 0..METADATA_OPERATIONS {
        operation();
    }
    let elapsed = began.elapsed();
    let allocations = end_counting();
    println!(
        "{label},{METADATA_OPERATIONS},{:.3},{allocations}",
        elapsed.as_nanos() as f64 / METADATA_OPERATIONS as f64
    );
}
fn metadata_costs() {
    use amalgam::entry::Entry;
    let rt = runtime();
    let now = amalgam::Timestamp::from_ticks(1_000_000_000);
    let tag = amalgam::Tag::new("measurement-group").unwrap();
    eprintln!(
        "metadata_bytes={}",
        std::mem::size_of::<amalgam::entry::Metadata>()
    );
    println!("scenario,operations,ns_per_op,allocations");
    for case in [
        MetadataCase::Plain,
        MetadataCase::Eager,
        MetadataCase::Tagged,
    ] {
        let options = case.options();
        let fresh = || {
            Entry::try_fresh_with_jitter(
                7_u64,
                &options,
                now,
                now,
                amalgam::JitterSample::ZERO,
                case.tags(&tag),
                None,
                None,
            )
            .unwrap()
        };
        let source = fresh();
        assert_eq!(source.value(), &7);
        assert_eq!(source.meta().created(), now);
        match case {
            MetadataCase::Plain => {
                assert!(source.meta().tags().is_empty());
                assert!(source.meta().eager_refresh_at().is_none());
            }
            MetadataCase::Eager => assert!(source.meta().eager_refresh_at().is_some()),
            MetadataCase::Tagged => assert_eq!(source.meta().tags(), std::slice::from_ref(&tag)),
        }
        metadata_cost(&format!("entry_{}", case.label()), || {
            drop(black_box(fresh()));
        });
        metadata_cost(&format!("snapshot_{}", case.label()), || {
            drop(black_box(source.meta().clone()));
        });
        metadata_cost(&format!("expire_{}", case.label()), || {
            drop(black_box(source.with_logical_expiration(now)));
        });
        let cache = Cache::builder().default_options(options).build();
        rt.block_on(async {
            let replacement = || async {
                let request = cache.set("metadata-cost", 7_u64);
                let request = match case {
                    MetadataCase::Plain | MetadataCase::Eager => request,
                    MetadataCase::Tagged => request.tags(["measurement-group"]),
                };
                request.await.unwrap();
            };
            let mut replace = || {
                let mut operation = std::pin::pin!(replacement());
                let mut context = std::task::Context::from_waker(std::task::Waker::noop());
                match std::future::Future::poll(operation.as_mut(), &mut context) {
                    std::task::Poll::Ready(()) => {}
                    std::task::Poll::Pending => panic!("standalone replacement must be ready"),
                }
            };
            metadata_cost(&format!("set_{}", case.label()), &mut replace);
            assert_eq!(
                cache
                    .read("metadata-cost", None)
                    .await
                    .unwrap()
                    .into_value(),
                Some(7)
            );
            metadata_cost(&format!("set_after_read_{}", case.label()), &mut replace);
            cache.shutdown().await.unwrap();
        });
    }
}
