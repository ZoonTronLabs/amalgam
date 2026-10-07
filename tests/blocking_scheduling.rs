//! Public progress and cancellation evidence for nested native factories.
use amalgam::*;
use std::num::NonZeroUsize;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

fn nested(cache: BlockingCache<u64>, remaining: u32, token: FactoryCancellation) -> Result<u64> {
    let child = cache.clone();
    let next = token.clone();
    cache
        .get_or_set(
            format!("level/{remaining}"),
            typed_blocking_factory(move |ctx| {
                if remaining == 0 {
                    return Ok::<_, amalgam::FactoryError>(ctx.value(1));
                }
                nested(child, remaining - 1, next)
                    .map(|value| ctx.value(value + 1))
                    .map_err(FactoryError::from_source)
            }),
        )
        .cancellation(token)
        .execute()
}

#[test]
fn nested_cancellable_factories_progress_with_a_single_base_pool_thread() {
    let runtime =
        BlockingRuntime::with_workers(NonZeroUsize::new(1).unwrap(), NonZeroUsize::new(1).unwrap())
            .unwrap();
    let cache = BlockingCache::<u64>::on_runtime(Cache::builder(), runtime.clone()).unwrap();
    let source = CancellationSource::new();
    let token = source.token();
    let request = cache.clone();
    let (result_tx, result_rx) = mpsc::channel();
    let caller = thread::spawn(move || {
        result_tx.send(nested(request, 4, token)).unwrap();
    });
    runtime
        .run(async {
            tokio::time::timeout(
                Duration::from_secs(1),
                tokio::time::sleep(Duration::from_millis(5)),
            )
            .await
        })
        .unwrap();
    let result = result_rx.recv_timeout(Duration::from_secs(1));
    source.cancel(); // Also makes a failing probe release its queued child.
    caller.join().unwrap();
    cache.shutdown().unwrap();
    assert_eq!(
        result
            .expect("nested dispatch must progress without external cancellation")
            .unwrap(),
        5
    );
}

#[test]
fn opposite_cross_runtime_nested_calls_progress() {
    let make = || BlockingRuntime::with_workers(NonZeroUsize::MIN, NonZeroUsize::MIN).unwrap();
    let left = BlockingCache::<u64>::on_runtime(Cache::builder(), make()).unwrap();
    let right = BlockingCache::<u64>::on_runtime(Cache::builder(), make()).unwrap();
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let source = CancellationSource::new();
    let (sender, receiver) = mpsc::channel();
    let callers: Vec<_> = [
        (left.clone(), right.clone(), "left"),
        (right.clone(), left.clone(), "right"),
    ]
    .into_iter()
    .map(|(outer, inner, key)| {
        let barrier = barrier.clone();
        let token = source.token();
        let nested_token = token.clone();
        let sender = sender.clone();
        thread::spawn(move || {
            let result = outer
                .get_or_set(
                    key,
                    typed_blocking_factory(move |ctx| {
                        barrier.wait();
                        inner
                            .get_or_set(
                                format!("child/{key}"),
                                typed_blocking_factory(|child| Ok(child.value(7))),
                            )
                            .cancellation(nested_token)
                            .execute()
                            .map(|value| ctx.value(value + 1))
                            .map_err(FactoryError::from_source)
                    }),
                )
                .cancellation(token)
                .execute();
            sender.send(result).unwrap();
        })
    })
    .collect();
    drop(sender);
    let first = receiver.recv_timeout(Duration::from_secs(2));
    let second = receiver.recv_timeout(Duration::from_secs(2));
    source.cancel();
    for caller in callers {
        caller.join().unwrap();
    }
    left.shutdown().unwrap();
    right.shutdown().unwrap();
    assert_eq!(
        first
            .expect("left/right nesting must progress before cancellation")
            .unwrap(),
        8
    );
    assert_eq!(
        second
            .expect("both nesting directions must progress before cancellation")
            .unwrap(),
        8
    );
}

#[test]
fn nested_dispatch_limit_returns_its_original_typed_error_and_drains() {
    let driver = BlockingRuntime::with_workers(NonZeroUsize::MIN, NonZeroUsize::MIN).unwrap();
    let cache = BlockingCache::<u64>::on_runtime(Cache::builder(), driver).unwrap();
    let result = nested(cache.clone(), 32, CancellationSource::new().token());
    let error = result.unwrap_err();
    let mut cause: &(dyn std::error::Error + 'static) = &error;
    let found = loop {
        if let Some(dispatch) = cause.downcast_ref::<BlockingDispatchError>() {
            break matches!(dispatch, BlockingDispatchError::NestingLimit { limit: 32 });
        }
        let Some(next) = cause.source() else {
            break false;
        };
        cause = next;
    };
    assert!(
        found,
        "nesting rejection must retain the concrete typed cause: {error}"
    );
    cache.shutdown().unwrap();
}

fn typed_blocking_factory<V, F>(factory: F) -> F
where
    F: FnOnce(amalgam::FactoryContext<V>) -> std::result::Result<V, amalgam::FactoryError>,
{
    factory
}
