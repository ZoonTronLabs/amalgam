//! Ready L1 work retains cancellation, observation and synchronous drainage.
use amalgam::*;
use std::error::Error as _;
use std::future::{Future, IntoFuture, pending, poll_fn};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::task::Poll;
use std::time::Duration;
use tokio::sync::{Notify, broadcast};

#[derive(Default)]
struct Gate {
    armed: AtomicBool,
    entered: Notify,
    released: Mutex<bool>,
    release: Condvar,
}
impl Gate {
    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }
    fn block_once(&self) {
        if !self.armed.swap(false, Ordering::SeqCst) {
            return;
        }
        self.entered.notify_one();
        drop(
            self.release
                .wait_while(self.released.lock().unwrap(), |released| !*released)
                .unwrap(),
        );
    }
    async fn wait_entered(&self) {
        tokio::time::timeout(Duration::from_secs(2), self.entered.notified())
            .await
            .expect("synchronous callback entered");
    }
    fn unblock(&self) {
        *self.released.lock().unwrap() = true;
        self.release.notify_all();
    }
}

fn completions(events: &mut broadcast::Receiver<CacheEvent>) -> Vec<OperationOutcome> {
    let mut outcomes = Vec::new();
    while let Ok(event) = events.try_recv() {
        if let CacheEvent::OperationCompleted { outcome, .. } = event {
            outcomes.push(outcome);
        }
    }
    outcomes
}

async fn populated(cache: &Cache<i32>) {
    cache.try_set("k", 7).await.unwrap().wait().await.unwrap();
    assert_eq!(cache.read("k", None).await.unwrap().value_or(0), 7);
}
fn cancelled(result: std::result::Result<i32, Error>, reason: FactoryCancellationReason) {
    assert!(
        matches!(result, Err(Error::OperationCancelled { reason: actual }) if actual == reason)
    );
}

async fn close_while_blocked<V: Clone + Send + Sync + 'static>(
    cache: &Cache<V>,
    gate: &Gate,
    operation: tokio::task::JoinHandle<std::result::Result<i32, Error>>,
) {
    gate.wait_entered().await;
    let mut events = cache.events().subscribe();
    let closing = cache.clone();
    assert_eq!(
        std::thread::spawn(move || closing.close()).join().unwrap(),
        CloseOutcome::Started
    );
    let draining = cache.clone();
    let shutdown = tokio::spawn(async move { draining.shutdown().await });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        !shutdown.is_finished(),
        "shutdown must drain synchronous work"
    );
    gate.unblock();
    cancelled(
        operation.await.unwrap(),
        FactoryCancellationReason::CacheShutdown,
    );
    shutdown.await.unwrap().unwrap();
    assert_eq!(completions(&mut events), vec![OperationOutcome::Cancelled]);
}

struct CloneGate {
    number: i32,
    gate: Arc<Gate>,
}

#[derive(Clone)]
struct DefaultDrop {
    number: i32,
    gate: Option<Arc<Gate>>,
}
impl Drop for DefaultDrop {
    fn drop(&mut self) {
        if let Some(gate) = &self.gate {
            gate.block_once();
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unused_canonical_default_is_dropped_inside_counted_completion() {
    let gate = Arc::new(Gate::default());
    let cache = Cache::new();
    cache
        .try_set(
            "k",
            DefaultDrop {
                number: 7,
                gate: None,
            },
        )
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert_eq!(
        cache
            .read("k", None)
            .await
            .unwrap()
            .into_value()
            .unwrap()
            .number,
        7
    );
    gate.arm();
    let reading = cache.clone();
    let default = DefaultDrop {
        number: 99,
        gate: Some(gate.clone()),
    };
    let operation =
        tokio::spawn(async move { Ok(reading.read_or_default("k", default, None).await?.number) });
    close_while_blocked(&cache, &gate, operation).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unused_legacy_default_hit_is_dropped_under_transferred_count() {
    let gate = Arc::new(Gate::default());
    let cache = Cache::new();
    cache
        .try_set(
            "k",
            DefaultDrop {
                number: 7,
                gate: None,
            },
        )
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert_eq!(
        cache
            .read("k", None)
            .await
            .unwrap()
            .into_value()
            .unwrap()
            .number,
        7
    );
    gate.arm();
    let reading = cache.clone();
    let default = DefaultDrop {
        number: 99,
        gate: Some(gate.clone()),
    };
    let operation =
        tokio::spawn(async move { reading.get_or_default("k", default, None).await.number });
    gate.wait_entered().await;
    cache.close();
    let draining = cache.clone();
    let shutdown = tokio::spawn(async move { draining.shutdown().await });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        !shutdown.is_finished(),
        "legacy unused input is still being destroyed"
    );
    gate.unblock();
    assert_eq!(
        operation.await.unwrap(),
        7,
        "legacy signature retains produced values with diagnosed cancellation"
    );
    shutdown.await.unwrap().unwrap();
}
impl Clone for CloneGate {
    fn clone(&self) -> Self {
        self.gate.block_once();
        Self {
            number: self.number,
            gate: self.gate.clone(),
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordinary_value_clone_is_counted_as_user_work() {
    let gate = Arc::new(Gate::default());
    let cache = Cache::new();
    cache
        .try_set(
            "k",
            CloneGate {
                number: 7,
                gate: gate.clone(),
            },
        )
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert_eq!(
        cache
            .read("k", None)
            .await
            .unwrap()
            .into_value()
            .unwrap()
            .number,
        7
    );
    gate.arm();
    let reading = cache.clone();
    let operation =
        tokio::spawn(
            async move { Ok(reading.read("k", None).await?.into_value().unwrap().number) },
        );
    close_while_blocked(&cache, &gate, operation).await;
}

struct BlockingClock(Arc<Gate>);
impl Clock for BlockingClock {
    fn now(&self) -> Timestamp {
        self.0.block_once();
        Timestamp::from_ticks(10_000_000)
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blocked_ready_clock_is_cancelled_and_drained_from_another_thread() {
    let gate = Arc::new(Gate::default());
    let cache = Cache::builder()
        .clock(Arc::new(BlockingClock(gate.clone())))
        .try_build()
        .unwrap();
    populated(&cache).await;
    gate.arm();
    let reading = cache.clone();
    let operation = tokio::spawn(async move { Ok(reading.read("k", None).await?.value_or(0)) });
    close_while_blocked(&cache, &gate, operation).await;
}

struct BlockingCloner(Arc<Gate>);
impl ValueCloner<i32> for BlockingCloner {
    fn clone_value(&self, value: &i32) -> std::result::Result<i32, CloneError> {
        self.0.block_once();
        Ok(*value)
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blocked_ready_value_cloner_is_cancelled_and_drained() {
    let gate = Arc::new(Gate::default());
    let cache = Cache::builder()
        .default_options(EntryOptions::default().with_enable_auto_clone(true))
        .value_cloner(Arc::new(BlockingCloner(gate.clone())))
        .try_build()
        .unwrap();
    populated(&cache).await;
    gate.arm();
    let reading = cache.clone();
    let operation = tokio::spawn(async move { Ok(reading.read("k", None).await?.value_or(0)) });
    close_while_blocked(&cache, &gate, operation).await;
}

struct HitGate(Arc<Gate>, Arc<AtomicUsize>);
impl Plugin for HitGate {
    fn name(&self) -> &str {
        "ready-hit-gate"
    }
    fn on_event(&self, event: &CacheEvent) {
        if matches!(event, CacheEvent::Hit { .. }) {
            self.0.block_once();
        }
    }
    fn on_stop(&self) {
        self.1.fetch_add(1, Ordering::SeqCst);
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blocked_ready_hit_plugin_is_cancelled_and_drained_once() {
    let gate = Arc::new(Gate::default());
    let stops = Arc::new(AtomicUsize::new(0));
    let cache = Cache::builder()
        .plugin(Arc::new(HitGate(gate.clone(), stops.clone())))
        .try_build()
        .unwrap();
    populated(&cache).await;
    gate.arm();
    let reading = cache.clone();
    let operation = tokio::spawn(async move {
        reading
            .get_or_set(
                "k",
                |_ctx| async move { panic!("fresh value skips origin") },
            )
            .await
    });
    close_while_blocked(&cache, &gate, operation).await;
    assert_eq!(stops.load(Ordering::SeqCst), 1);
}

struct UnusedFactory(Arc<Gate>);
impl Drop for UnusedFactory {
    fn drop(&mut self) {
        self.0.block_once();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_waits_a_cancelled_parked_future_destructor_before_claiming_drainage() {
    let gate = Arc::new(Gate::default());
    let cache = Cache::<i32>::new();
    let origin_gate = gate.clone();
    let entered = Arc::new(AtomicBool::new(false));
    let origin_entered = entered.clone();
    let mut operation = Box::pin(
        cache
            .get_or_set("pending", move |ctx| async move {
                let _drop = UnusedFactory(origin_gate);
                origin_entered.store(true, Ordering::SeqCst);
                pending::<()>().await;
                Ok(ctx.value(99))
            })
            .into_future(),
    );
    poll_fn(|cx| {
        assert!(operation.as_mut().poll(cx).is_pending());
        if entered.load(Ordering::SeqCst) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
    gate.arm();
    let closing = cache.clone();
    let closer = std::thread::spawn(move || closing.close());
    gate.wait_entered().await;
    let draining = cache.clone();
    let shutdown = tokio::spawn(async move { draining.shutdown().await });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        !shutdown.is_finished(),
        "terminal state must not hide an active destructor"
    );
    gate.unblock();
    assert_eq!(closer.join().unwrap(), CloseOutcome::Started);
    cancelled(operation.await, FactoryCancellationReason::CacheShutdown);
    shutdown.await.unwrap().unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unused_factory_destructor_remains_inside_ready_drainage() {
    let gate = Arc::new(Gate::default());
    let cache = Cache::new();
    populated(&cache).await;
    gate.arm();
    let unused = UnusedFactory(gate.clone());
    let reading = cache.clone();
    let operation = tokio::spawn(async move {
        reading
            .get_or_set("k", move |ctx| async move {
                drop(unused);
                Ok(ctx.value(99))
            })
            .await
    });
    close_while_blocked(&cache, &gate, operation).await;
}

struct CallbackPlugin<F>(F);
impl<F: Fn(&CacheEvent) + Send + Sync> Plugin for CallbackPlugin<F> {
    fn name(&self) -> &str {
        "ready-callback"
    }
    fn on_event(&self, event: &CacheEvent) {
        (self.0)(event);
    }
}
#[tokio::test]
async fn ready_hit_plugin_explicit_cancellation_has_one_precise_observer() {
    let source = CancellationSource::new();
    let armed = Arc::new(AtomicBool::new(false));
    let callback_source = source.clone();
    let callback_armed = armed.clone();
    let cache = Cache::builder()
        .plugin(Arc::new(CallbackPlugin(move |event: &CacheEvent| {
            if matches!(event, CacheEvent::Hit { .. }) && callback_armed.load(Ordering::SeqCst) {
                callback_source.cancel();
            }
        })))
        .try_build()
        .unwrap();
    populated(&cache).await;
    armed.store(true, Ordering::SeqCst);
    let mut events = cache.events().subscribe();
    cancelled(
        cache
            .get_or_set_cancellable(
                "k",
                |_ctx| async move { panic!("origin must not run") },
                source.token(),
            )
            .await,
        FactoryCancellationReason::CallerCancelled,
    );
    assert_eq!(completions(&mut events), vec![OperationOutcome::Cancelled]);
    cache.shutdown().await.unwrap();
}

struct ClosingCloner(Arc<Mutex<Option<Cache<i32>>>>);
impl ValueCloner<i32> for ClosingCloner {
    fn clone_value(&self, value: &i32) -> std::result::Result<i32, CloneError> {
        let closing = self.0.lock().unwrap().take();
        if let Some(cache) = closing {
            cache.close();
        }
        Ok(*value)
    }
}
#[tokio::test]
async fn ready_cloner_reentrant_close_rejects_value_before_hit() {
    let closing = Arc::new(Mutex::new(None));
    let cache = Cache::builder()
        .default_options(EntryOptions::default().with_enable_auto_clone(true))
        .value_cloner(Arc::new(ClosingCloner(closing.clone())))
        .try_build()
        .unwrap();
    populated(&cache).await;
    *closing.lock().unwrap() = Some(cache.clone());
    let mut events = cache.events().subscribe();
    cancelled(
        cache.read("k", None).await.map(|value| value.value_or(0)),
        FactoryCancellationReason::CacheShutdown,
    );
    let mut hits = 0;
    let mut outcomes = Vec::new();
    while let Ok(event) = events.try_recv() {
        match event {
            CacheEvent::Hit { .. } => hits += 1,
            CacheEvent::OperationCompleted { outcome, .. } => outcomes.push(outcome),
            _ => {}
        }
    }
    assert_eq!(hits, 0);
    assert_eq!(outcomes, vec![OperationOutcome::Cancelled]);
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn ready_and_owned_fallback_each_keep_one_observer_and_copy_failure() {
    let cache = Cache::new();
    populated(&cache).await;
    let mut events = cache.events().subscribe();
    assert_eq!(cache.read("k", None).await.unwrap().value_or(0), 7);
    assert_eq!(
        cache
            .get_or_set("k", |_ctx| async move { panic!("origin must not run") })
            .await
            .unwrap(),
        7
    );
    assert_eq!(
        completions(&mut events),
        vec![OperationOutcome::Hit, OperationOutcome::Hit]
    );
    let missing = cache.read("missing", None).await.unwrap();
    assert!(!missing.has_value());
    assert_eq!(completions(&mut events), vec![OperationOutcome::Miss]);
    cache
        .get_or_set(
            "failure",
            |ctx| async move { Err(ctx.fail("origin failed")) },
        )
        .await
        .unwrap_err();
    assert_eq!(
        completions(&mut events),
        vec![OperationOutcome::FactoryError]
    );
    let invalid = EntryOptions::default().with_enable_auto_clone(true);
    assert!(matches!(
        cache.read("k", Some(invalid)).await,
        Err(Error::Config(ConfigError::AutoCloneWithoutCloner))
    ));
    assert_eq!(
        completions(&mut events),
        vec![OperationOutcome::ConfigurationError]
    );
    cache.shutdown().await.unwrap();
}

struct FailingCloner(AtomicBool);
impl ValueCloner<i32> for FailingCloner {
    fn clone_value(&self, value: &i32) -> std::result::Result<i32, CloneError> {
        if self.0.load(Ordering::SeqCst) {
            Err(CloneError::from_source(std::io::Error::other(
                "original copy failure",
            )))
        } else {
            Ok(*value)
        }
    }
}
#[tokio::test]
async fn ready_clone_failure_preserves_error_source_without_factory_or_hit() {
    let cloner = Arc::new(FailingCloner(AtomicBool::new(false)));
    let cache = Cache::builder()
        .default_options(EntryOptions::default().with_enable_auto_clone(true))
        .value_cloner(cloner.clone())
        .try_build()
        .unwrap();
    populated(&cache).await;
    cloner.0.store(true, Ordering::SeqCst);
    let mut events = cache.events().subscribe();
    let error = cache
        .get_or_set("k", |_ctx| async move {
            panic!("copy failure must not call origin")
        })
        .await
        .unwrap_err();
    assert!(matches!(&error, Error::Clone(CloneError::Custom { .. })));
    assert_eq!(error.source().unwrap().to_string(), "original copy failure");
    assert_eq!(completions(&mut events), vec![OperationOutcome::CloneError]);
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn already_cancelled_ready_read_skips_user_copy() {
    let cloner = Arc::new(FailingCloner(AtomicBool::new(false)));
    let cache = Cache::builder()
        .default_options(EntryOptions::default().with_enable_auto_clone(true))
        .value_cloner(cloner.clone())
        .try_build()
        .unwrap();
    populated(&cache).await;
    cloner.0.store(true, Ordering::SeqCst);
    let source = CancellationSource::new();
    source.cancel();
    let mut events = cache.events().subscribe();
    cancelled(
        cache
            .read_cancellable("k", None, source.token())
            .await
            .map(|value| value.value_or(0)),
        FactoryCancellationReason::CallerCancelled,
    );
    assert_eq!(completions(&mut events), vec![OperationOutcome::Cancelled]);
    cache.shutdown().await.unwrap();
}
