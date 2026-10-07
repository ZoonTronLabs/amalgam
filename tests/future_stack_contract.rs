//! Public operation futures must compose on ordinary executor/thread stacks.

use amalgam::{Cache, ClearMode};
use std::future::IntoFuture;
use std::mem::size_of_val;

#[test]
fn ordinary_mutation_futures_fit_within_a_small_stack_budget() {
    let cache: Cache<u64> = Cache::builder().try_build().unwrap();
    const MAX_FUTURE_BYTES: usize = 16 * 1024;
    let sizes = [
        ("set", size_of_val(&cache.set("x", 42).into_future())),
        ("try_set", size_of_val(&cache.try_set("x", 42))),
        ("remove", size_of_val(&cache.remove("x").into_future())),
        (
            "try_remove",
            size_of_val(&std::future::IntoFuture::into_future(
                cache.remove("x").with_receipt(),
            )),
        ),
        ("expire", size_of_val(&cache.expire("x").into_future())),
        (
            "clear",
            size_of_val(&std::future::IntoFuture::into_future(
                cache.clear(ClearMode::Remove).with_receipt(),
            )),
        ),
    ];
    for (operation, size) in sizes {
        assert!(
            size <= MAX_FUTURE_BYTES,
            "{operation} embeds {size} bytes in its caller's future; budget {MAX_FUTURE_BYTES}"
        );
    }
}

#[test]
fn ordinary_lookup_futures_fit_within_a_small_stack_budget() {
    let cache: Cache<u64> = Cache::builder().try_build().unwrap();
    const MAX_FUTURE_BYTES: usize = 4 * 1024;
    let sizes = [
        ("read", size_of_val(&cache.read("x", None))),
        (
            "get_or_set",
            size_of_val(&cache.get_or_set("x", |ctx| {
                std::future::ready(Ok::<_, amalgam::FactoryError>(ctx.value(42)))
            })),
        ),
        (
            "get_or_set_value",
            size_of_val(&cache.get_or_set_value("x", 42, None)),
        ),
    ];
    for (operation, size) in sizes {
        assert!(
            size <= MAX_FUTURE_BYTES,
            "{operation} embeds {size} bytes before even reading L1; budget {MAX_FUTURE_BYTES}"
        );
    }
}

#[tracing::instrument(skip_all)]
async fn refresh(cache: &Cache<u64>, value: u64) {
    cache.set("x", value).await.unwrap();
}

#[tracing::instrument(skip_all)]
async fn read_then_refresh(cache: &Cache<u64>, value: u64) -> u64 {
    let current = cache
        .get_or_set("x", |ctx| async move {
            Ok::<_, amalgam::FactoryError>(ctx.value(1))
        })
        .await
        .unwrap();
    if current != value {
        refresh(cache, value).await;
    }
    cache.try_get("x", None).await.into_value().unwrap()
}

#[test]
fn traced_read_and_refresh_compose_on_a_two_mebibyte_thread_stack() {
    std::thread::Builder::new()
        .stack_size(2 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    let cache: Cache<u64> = Cache::builder().try_build().unwrap();
                    assert_eq!(read_then_refresh(&cache, 1).await, 1);
                    assert_eq!(read_then_refresh(&cache, 2).await, 2);
                    cache.remove("x").await.unwrap();
                    cache.shutdown().await.unwrap();
                });
        })
        .unwrap()
        .join()
        .unwrap();
}
