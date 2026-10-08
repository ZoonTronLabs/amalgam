//! Same public workloads for the published 0.3.1 and the candidate package.
use amalgam::{Cache, EntryOptions};
use std::hint::black_box;
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};
mod warmup;

#[cfg(not(feature = "baseline"))]
use amalgam::provider::{InMemoryDistributedCache, JsonSerializer, SystemClock};
#[cfg(feature = "baseline")]
use amalgam::{InMemoryDistributedCache, JsonSerializer, SystemClock};

#[cfg(feature = "baseline")]
async fn read(cache: &Cache<u64>, key: &str) -> u64 {
    cache.read(key, None).await.unwrap().into_value().unwrap()
}
#[cfg(not(feature = "baseline"))]
async fn read(cache: &Cache<u64>, key: &str) -> u64 {
    cache.try_get(key).await.unwrap().unwrap()
}
#[cfg(feature = "baseline")]
async fn get_or_set(cache: &Cache<u64>, key: &str) -> u64 {
    cache
        .get_or_set(key, |ctx| async move {
            Ok::<_, amalgam::FactoryError>(ctx.value(42))
        })
        .await
        .unwrap()
}
#[cfg(not(feature = "baseline"))]
async fn get_or_set(cache: &Cache<u64>, key: &str) -> u64 {
    cache
        .get_or_set(key, |_| async { Ok::<_, std::convert::Infallible>(42) })
        .await
        .unwrap()
}
#[cfg(feature = "baseline")]
async fn set(cache: &Cache<u64>, key: &str, value: u64) {
    cache.try_set(key, value).await.unwrap();
}
#[cfg(not(feature = "baseline"))]
async fn set(cache: &Cache<u64>, key: &str, value: u64) {
    cache.set(key, value).await.unwrap();
}
fn options() -> EntryOptions {
    EntryOptions::new(Duration::from_secs(3600))
}
fn cache() -> Cache<u64> {
    Cache::builder()
        .default_options(options())
        .try_build()
        .unwrap()
}
fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}
fn hot(retrieve: bool, workers: usize) {
    // Keep the seeding executor alive until its cache-owned work has drained.
    let rt = runtime();
    let _entered = rt.enter();
    let cache = cache();
    rt.block_on(async {
        for worker in 0..workers {
            set(&cache, &format!("key-{worker}"), 42).await;
        }
    });
    let gate = Arc::new(Barrier::new(workers + 1));
    std::thread::scope(|scope| {
        for worker in 0..workers {
            let cache = cache.clone();
            let gate = gate.clone();
            scope.spawn(move || {
                let rt = runtime();
                let key = format!("key-{worker}");
                let mut warmup = warmup::Warmup::new(format!("hot:{retrieve}:{workers}:{worker}"));
                loop {
                    let started = Instant::now();
                    rt.block_on(async {
                        for _ in 0..20_000 {
                            let value = if retrieve {
                                get_or_set(&cache, &key).await
                            } else {
                                read(&cache, &key).await
                            };
                            assert_eq!(black_box(value), 42);
                        }
                    });
                    if warmup.record(started.elapsed(), 20_000) {
                        break;
                    }
                }
                gate.wait();
                rt.block_on(async {
                    for _ in 0..200_000 {
                        let value = if retrieve {
                            get_or_set(&cache, &key).await
                        } else {
                            read(&cache, &key).await
                        };
                        assert_eq!(black_box(value), 42);
                    }
                });
                gate.wait();
            });
        }
        gate.wait();
        let started = Instant::now();
        gate.wait();
        let operations = workers * 200_000;
        println!(
            "{},{workers},{operations},{:.3}",
            if retrieve {
                "hot_get_or_set"
            } else {
                "hot_read"
            },
            started.elapsed().as_nanos() as f64 / operations as f64
        );
    });
    rt.block_on(cache.shutdown()).unwrap();
}
fn serial(case: &str) {
    let rt = runtime();
    let _entered = rt.enter();
    let cache = if case.starts_with("l2") {
        Cache::builder()
            .default_options(options().with_skip_memory(true, true))
            .distributed(Arc::new(InMemoryDistributedCache::new(Arc::new(
                SystemClock,
            ))))
            .serializer(Arc::new(JsonSerializer))
            .try_build()
            .unwrap()
    } else {
        cache()
    };
    if case != "cold" {
        rt.block_on(set(&cache, "key", 42));
    }
    let mut warmup = warmup::Warmup::new(case);
    let mut generation = 0usize;
    loop {
        let cold = if case == "cold" {
            Some(crate::cache())
        } else {
            None
        };
        let selected = cold.as_ref().unwrap_or(&cache);
        let started = Instant::now();
        rt.block_on(batch(selected, case, 20_000, generation));
        let elapsed = started.elapsed();
        if let Some(cold) = cold {
            rt.block_on(cold.shutdown()).unwrap();
        }
        generation += 1;
        if warmup.record(elapsed, 20_000) {
            break;
        }
    }
    let cold = if case == "cold" {
        Some(crate::cache())
    } else {
        None
    };
    let selected = cold.as_ref().unwrap_or(&cache);
    let operations = if case == "cold" { 20_000 } else { 200_000 };
    let started = Instant::now();
    rt.block_on(batch(selected, case, operations, generation));
    println!(
        "{case},1,{operations},{:.3}",
        started.elapsed().as_nanos() as f64 / operations as f64
    );
    if let Some(cold) = cold {
        rt.block_on(cold.shutdown()).unwrap();
    }
    rt.block_on(cache.shutdown()).unwrap();
}
async fn batch(cache: &Cache<u64>, case: &str, operations: usize, generation: usize) {
    for iteration in 0..operations {
        match case {
            "set" => set(cache, "key", black_box(42)).await,
            "cold" => assert_eq!(
                black_box(get_or_set(cache, &format!("cold-{generation}-{iteration}")).await),
                42
            ),
            "l2_read" => assert_eq!(black_box(read(cache, "key").await), 42),
            "l2_get_or_set" => assert_eq!(black_box(get_or_set(cache, "key").await), 42),
            _ => panic!("unknown benchmark case"),
        }
    }
}
fn main() {
    println!("scenario,threads,operations,ns_per_op");
    for retrieve in [false, true] {
        for workers in [1, 8] {
            hot(retrieve, workers);
        }
    }
    for case in ["set", "cold", "l2_read", "l2_get_or_set"] {
        serial(case);
    }
}
