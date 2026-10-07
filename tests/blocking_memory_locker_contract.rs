//! Independent dual-method local locker, including uncooperative callbacks.
use amalgam::advanced::*;
use amalgam::provider::*;
use amalgam::*;
use async_trait::async_trait;
use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::thread;
use std::time::Duration;
use tokio::sync::Notify;

const WAIT: Duration = Duration::from_secs(3);
#[derive(Debug, thiserror::Error)]
#[error("original blocking acquisition cause")]
struct Cause;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Method {
    Async,
    Blocking,
    Try,
}
#[derive(Clone)]
enum Behavior {
    Lock,
    Unavailable,
    Error,
    Panic,
    IgnoreCancellation(Arc<Gate>),
}
struct Gate {
    open: Mutex<bool>,
    changed: Condvar,
}
impl Gate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            open: Mutex::new(false),
            changed: Condvar::new(),
        })
    }
    fn wait(&self) {
        let mut open = self.open.lock().unwrap();
        while !*open {
            open = self.changed.wait(open).unwrap();
        }
    }
    fn release(&self) {
        *self.open.lock().unwrap() = true;
        self.changed.notify_all();
    }
}
type Hook = Arc<dyn Fn() + Send + Sync>;
struct State {
    slots: Mutex<HashSet<String>>,
    released: Condvar,
    async_released: Notify,
    behavior: Mutex<Behavior>,
    requests: Mutex<Vec<(Method, MemoryLockRequest)>>,
    entered: Mutex<Option<mpsc::Sender<MemoryLockRequest>>>,
    acquired: AtomicUsize,
    returned: AtomicUsize,
    shutdowns: AtomicUsize,
    acquire_hook: Mutex<Option<Hook>>,
    release_hook: Mutex<Option<Hook>>,
}
impl State {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            slots: Mutex::new(HashSet::new()),
            released: Condvar::new(),
            async_released: Notify::new(),
            behavior: Mutex::new(Behavior::Lock),
            requests: Mutex::new(Vec::new()),
            entered: Mutex::new(None),
            acquired: AtomicUsize::new(0),
            returned: AtomicUsize::new(0),
            shutdowns: AtomicUsize::new(0),
            acquire_hook: Mutex::new(None),
            release_hook: Mutex::new(None),
        })
    }
    fn watch(&self) -> mpsc::Receiver<MemoryLockRequest> {
        let (send, receive) = mpsc::channel();
        *self.entered.lock().unwrap() = Some(send);
        receive
    }
    fn record(&self, method: Method, request: &MemoryLockRequest) {
        self.requests
            .lock()
            .unwrap()
            .push((method, request.clone()));
        let send = self.entered.lock().unwrap().clone();
        if let Some(send) = send {
            let _ = send.send(request.clone());
        }
        let hook = self.acquire_hook.lock().unwrap().clone();
        if let Some(hook) = hook {
            hook();
        }
    }
    fn count(&self, method: Method) -> usize {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|(kind, _)| *kind == method)
            .count()
    }
    fn guard(self: &Arc<Self>, key: String) -> MemoryLockOutcome {
        self.acquired.fetch_add(1, Ordering::SeqCst);
        MemoryLockOutcome::Acquired(MemoryLock::new(Guard {
            state: self.clone(),
            key,
        }))
    }
    fn try_guard(self: &Arc<Self>, key: &str) -> Option<MemoryLockOutcome> {
        let inserted = self.slots.lock().unwrap().insert(key.to_owned());
        inserted.then(|| self.guard(key.to_owned()))
    }
    fn blocking(
        self: &Arc<Self>,
        request: MemoryLockRequest,
    ) -> std::result::Result<MemoryLockOutcome, MemoryLockerError> {
        // Publishing entry must mean the behavior for this call is fixed.
        // Otherwise the test can switch it before this callback snapshots it.
        let behavior = self.behavior.lock().unwrap().clone();
        self.record(Method::Blocking, &request);
        match behavior {
            Behavior::Unavailable => return Ok(MemoryLockOutcome::Unavailable),
            Behavior::Error => return Err(MemoryLockerError::from_source(Cause)),
            Behavior::Panic => panic!("original blocking provider panic"),
            Behavior::IgnoreCancellation(gate) => {
                gate.wait();
                return Ok(self
                    .try_guard(request.coordination_key())
                    .expect("late probe uses a vacant key"));
            }
            Behavior::Lock => {}
        }
        let mut slots = self.slots.lock().unwrap();
        loop {
            check(&request)?;
            if slots.insert(request.coordination_key().to_owned()) {
                break;
            }
            (slots, _) = self
                .released
                .wait_timeout(slots, Duration::from_millis(2))
                .unwrap();
        }
        drop(slots);
        Ok(self.guard(request.coordination_key().to_owned()))
    }
    async fn asynchronous(
        self: &Arc<Self>,
        request: MemoryLockRequest,
    ) -> std::result::Result<MemoryLockOutcome, MemoryLockerError> {
        self.record(Method::Async, &request);
        loop {
            let changed = self.async_released.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            check(&request)?;
            if let Some(guard) = self.try_guard(request.coordination_key()) {
                return Ok(guard);
            }
            tokio::select! {
                reason=request.cancellation().cancelled()=>return Err(MemoryLockerError::Cancelled{reason}),
                ()=&mut changed=>{},
            }
        }
    }
    fn drained(&self) {
        assert!(self.slots.lock().unwrap().is_empty());
        assert_eq!(
            self.acquired.load(Ordering::SeqCst),
            self.returned.load(Ordering::SeqCst)
        );
    }
}
fn check(request: &MemoryLockRequest) -> std::result::Result<(), MemoryLockerError> {
    match request.cancellation().check() {
        Ok(()) => Ok(()),
        Err(Error::OperationCancelled { reason }) => Err(MemoryLockerError::Cancelled { reason }),
        Err(error) => Err(MemoryLockerError::from_source(error)),
    }
}
struct Guard {
    state: Arc<State>,
    key: String,
}
impl MemoryLockGuard for Guard {
    fn release(self: Box<Self>) -> std::result::Result<(), MemoryLockerError> {
        assert!(self.state.slots.lock().unwrap().remove(&self.key));
        self.state.returned.fetch_add(1, Ordering::SeqCst);
        self.state.released.notify_all();
        self.state.async_released.notify_waiters();
        let hook = self.state.release_hook.lock().unwrap().clone();
        if let Some(hook) = hook {
            hook();
        }
        Ok(())
    }
}
struct Blocking(Arc<State>);
impl BlockingMemoryLocker for Blocking {
    fn acquire(
        &self,
        request: MemoryLockRequest,
    ) -> std::result::Result<MemoryLockOutcome, MemoryLockerError> {
        self.0.blocking(request)
    }
}
enum Capability {
    Dual,
    AsyncOnly,
}
struct Provider {
    state: Arc<State>,
    capability: Capability,
}
#[async_trait]
impl MemoryLocker for Provider {
    fn blocking_acquirer(&self) -> Option<Arc<dyn BlockingMemoryLocker>> {
        match self.capability {
            Capability::Dual => Some(Arc::new(Blocking(self.state.clone()))),
            Capability::AsyncOnly => None,
        }
    }
    async fn acquire(
        &self,
        request: MemoryLockRequest,
    ) -> std::result::Result<MemoryLockOutcome, MemoryLockerError> {
        self.state.asynchronous(request).await
    }
    fn try_acquire(
        &self,
        request: MemoryLockRequest,
    ) -> std::result::Result<MemoryLockOutcome, MemoryLockerError> {
        self.state.record(Method::Try, &request);
        Ok(self
            .state
            .try_guard(request.coordination_key())
            .unwrap_or(MemoryLockOutcome::Unavailable))
    }
    async fn shutdown(&self, _: MemoryLockerContext) -> std::result::Result<(), MemoryLockerError> {
        self.state.drained();
        self.state.shutdowns.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
fn driver() -> BlockingRuntime {
    BlockingRuntime::with_workers(NonZeroUsize::MIN, NonZeroUsize::MIN).unwrap()
}
fn options() -> EntryOptions {
    EntryOptions::new(Duration::from_secs(60))
}
fn builder(state: &Arc<State>) -> CacheBuilder<u64> {
    Cache::builder()
        .name("native-profiles")
        .instance_id("native-one")
        .key_prefix("p:")
        .memory_locker(Arc::new(Provider {
            state: state.clone(),
            capability: Capability::Dual,
        }))
        .default_options(options())
}
fn native(state: &Arc<State>) -> BlockingCache<u64> {
    BlockingCache::on_runtime(builder(state), driver()).unwrap()
}
fn spawn<T: Send + 'static>(
    runtime: &BlockingRuntime,
    work: impl std::future::Future<Output = T> + Send + 'static,
) -> tokio::task::JoinHandle<T> {
    struct Started<T>(tokio::task::JoinHandle<T>);
    runtime.run(async { Started(tokio::spawn(work)) }).0
}
fn reason(request: &MemoryLockRequest, expected: FactoryCancellationReason) {
    assert!(
        matches!(request.cancellation().check(),Err(Error::OperationCancelled{reason}) if reason==expected)
    );
}

#[test]
fn native_and_async_views_select_distinct_methods_and_warm_hits_bypass_both() {
    let state = State::new();
    let cache = native(&state);
    assert_eq!(
        cache
            .get_or_set(
                "native",
                amalgam::source::factory(|ctx| Ok::<_, amalgam::FactoryError>(ctx.value(1)))
            )
            .execute()
            .unwrap(),
        1
    );
    assert_eq!(
        cache
            .runtime()
            .run(cache.as_async().get_or_set(
                "async",
                amalgam::source::factory(|ctx| async move {
                    Ok::<_, amalgam::FactoryError>(ctx.value(2))
                })
            ))
            .unwrap(),
        2
    );
    assert_eq!(state.count(Method::Blocking), 1);
    assert_eq!(state.count(Method::Async), 1);
    for (key, value) in [("native", 1), ("async", 2)] {
        assert_eq!(
            cache
                .get_or_set(key, typed_blocking_factory(|_| panic!("warm origin")))
                .execute()
                .unwrap(),
            value
        );
        assert_eq!(
            cache
                .runtime()
                .run(cache.as_async().get_or_set::<_, _>(
                    key,
                    typed_factory(|_| async { panic!("warm async origin") })
                ))
                .unwrap(),
            value
        );
    }
    let requests = state.requests.lock().unwrap();
    assert!(
        requests
            .iter()
            .all(|(_, r)| r.context().cache_name() == "native-profiles"
                && r.context().instance_id() == "native-one"
                && r.key().starts_with("p:")
                && r.kind() == &MemoryLockKind::Entry)
    );
    assert_eq!(requests.len(), 2);
    for (_, request) in requests.iter() {
        reason(request, FactoryCancellationReason::ScopeFinished);
    }
    drop(requests);
    cache.shutdown().unwrap();
    state.drained();
    assert_eq!(state.shutdowns.load(Ordering::SeqCst), 1);
}
#[test]
fn legacy_provider_without_blocking_capability_remains_compatible() {
    let state = State::new();
    let cache = BlockingCache::on_runtime(
        Cache::builder().memory_locker(Arc::new(Provider {
            state: state.clone(),
            capability: Capability::AsyncOnly,
        })),
        driver(),
    )
    .unwrap();
    assert_eq!(
        cache
            .get_or_set(
                "old",
                amalgam::source::factory(|ctx| Ok::<_, amalgam::FactoryError>(ctx.value(7)))
            )
            .execute()
            .unwrap(),
        7
    );
    assert_eq!(state.count(Method::Blocking), 0);
    assert_eq!(state.count(Method::Async), 1);
    cache.shutdown().unwrap();
}
#[test]
fn mixed_callers_share_one_factory_and_owned_guards() {
    let state = State::new();
    let cache = native(&state);
    let calls = Arc::new(AtomicUsize::new(0));
    let source = CancellationSource::new();
    let mut threads = Vec::with_capacity(12);
    let mut tasks = Vec::with_capacity(12);
    for _ in 0..12 {
        let c = cache.clone();
        let n = calls.clone();
        let token = source.token();
        threads.push(thread::spawn(move || {
            c.get_or_set(
                "same",
                typed_blocking_factory(move |ctx| {
                    n.fetch_add(1, Ordering::SeqCst);
                    thread::sleep(Duration::from_millis(40));
                    Ok::<_, amalgam::FactoryError>(ctx.value(42))
                }),
            )
            .cancellation(token)
            .execute()
        }));
        let c = cache.as_async().clone();
        let n = calls.clone();
        tasks.push(spawn(cache.runtime(), async move {
            c.get_or_set(
                "same",
                amalgam::source::factory(move |ctx| async move {
                    n.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(40)).await;
                    Ok::<_, amalgam::FactoryError>(ctx.value(42))
                }),
            )
            .await
        }));
    }
    for t in threads {
        assert_eq!(t.join().unwrap().unwrap(), 42);
    }
    for t in tasks {
        assert_eq!(cache.runtime().run(t).unwrap().unwrap(), 42);
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(state.count(Method::Blocking) > 0);
    assert!(state.count(Method::Async) > 0);
    cache.flush_pending().unwrap();
    state.drained();
    cache.shutdown().unwrap();
}
#[test]
fn blocking_waiter_cannot_starve_the_factory_with_one_callback_slot() {
    let state = State::new();
    let gate = Gate::new();
    *state.behavior.lock().unwrap() = Behavior::IgnoreCancellation(gate.clone());
    let entered = state.watch();
    let cache = native(&state);
    let source = CancellationSource::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let (send, receive) = mpsc::channel();
    let c = cache.clone();
    let token = source.token();
    let n = calls.clone();
    let a = send.clone();
    let first = thread::spawn(move || {
        let value = c
            .get_or_set(
                "shared",
                typed_blocking_factory(move |ctx| {
                    n.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, amalgam::FactoryError>(ctx.value(17))
                }),
            )
            .cancellation(token)
            .execute();
        a.send(value).unwrap();
    });
    entered.recv_timeout(WAIT).unwrap();
    *state.behavior.lock().unwrap() = Behavior::Lock;
    let mut events = cache.events().subscribe();
    let c = cache.clone();
    let token = source.token();
    let n = calls.clone();
    let second = thread::spawn(move || {
        let value = c
            .get_or_set(
                "shared",
                typed_blocking_factory(move |ctx| {
                    n.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, amalgam::FactoryError>(ctx.value(99))
                }),
            )
            .cancellation(token)
            .execute();
        send.send(value).unwrap();
    });
    cache
        .runtime()
        .run(async {
            tokio::time::timeout(WAIT, async {
                loop {
                    if matches!(events.recv().await.unwrap(), CacheEvent::Miss { .. }) {
                        break;
                    }
                }
            })
            .await
        })
        .unwrap();
    cache.runtime().run(async {
        tokio::time::sleep(Duration::from_millis(20)).await;
    });
    gate.release();
    let a = receive.recv_timeout(WAIT);
    let b = receive.recv_timeout(WAIT);
    source.cancel();
    first.join().unwrap();
    second.join().unwrap();
    cache.shutdown().unwrap();
    assert_eq!(
        a.expect("factory must progress while another lock acquisition waits")
            .unwrap(),
        17
    );
    assert_eq!(
        b.expect("waiter must consume the completed first factory")
            .unwrap(),
        17
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    state.drained();
}
#[test]
fn distinct_keys_enter_inline_factories_before_either_is_released() {
    let state = State::new();
    let cache = native(&state);
    let gate = Gate::new();
    let (send, receive) = mpsc::channel();
    let mut callers = Vec::with_capacity(2);
    for key in ["one", "two"] {
        let c = cache.clone();
        let g = gate.clone();
        let s = send.clone();
        callers.push(thread::spawn(move || {
            c.get_or_set(
                key,
                amalgam::source::factory(move |ctx| {
                    s.send(()).unwrap();
                    g.wait();
                    Ok::<_, amalgam::FactoryError>(ctx.value(7))
                }),
            )
            .execute()
        }));
    }
    let first = receive.recv_timeout(WAIT);
    let second = receive.recv_timeout(WAIT);
    gate.release();
    for t in callers {
        assert_eq!(t.join().unwrap().unwrap(), 7);
    }
    cache.shutdown().unwrap();
    assert!(
        first.is_ok() && second.is_ok(),
        "unrelated keys must progress independently"
    );
    state.drained();
}
#[test]
fn ignored_acquisition_deadline_serves_stale_and_flush_waits_for_late_release() {
    let state = State::new();
    let gate = Gate::new();
    *state.behavior.lock().unwrap() = Behavior::IgnoreCancellation(gate.clone());
    let entered = state.watch();
    let clock = Arc::new(ManualClock::new(Timestamp::from_ticks(10_000)));
    let opts = EntryOptions::new(Duration::from_secs(1))
        .with_fail_safe(true, Some(Duration::from_secs(60)), None)
        .with_memory_lock_timeout(Timeout::After(Duration::from_millis(40)));
    let cache = BlockingCache::on_runtime(
        builder(&state).clock(clock.clone()).default_options(opts),
        driver(),
    )
    .unwrap();
    cache
        .set("stale", 7)
        .with_receipt()
        .execute()
        .unwrap()
        .wait()
        .unwrap();
    clock.advance(Duration::from_secs(2));
    let c = cache.clone();
    let (send, receive) = mpsc::channel();
    let caller = thread::spawn(move || {
        send.send(
            c.get_or_set(
                "stale",
                typed_blocking_factory(|_| panic!("eligible stale must bypass origin")),
            )
            .execute(),
        )
        .unwrap()
    });
    let request = entered.recv_timeout(WAIT).unwrap();
    assert_eq!(receive.recv_timeout(WAIT).unwrap().unwrap(), 7);
    caller.join().unwrap();
    reason(&request, FactoryCancellationReason::HardTimeout);
    let c = cache.clone();
    let (send, receive) = mpsc::channel();
    let flush = thread::spawn(move || send.send(c.flush_pending()).unwrap());
    assert!(receive.recv_timeout(Duration::from_millis(30)).is_err());
    gate.release();
    receive.recv_timeout(WAIT).unwrap().unwrap();
    flush.join().unwrap();
    state.drained();
    assert_eq!(state.acquired.load(Ordering::SeqCst), 1);
    cache.shutdown().unwrap();
}
#[test]
fn caller_cancellation_is_prompt_while_an_opaque_callback_is_still_owned() {
    let state = State::new();
    let gate = Gate::new();
    *state.behavior.lock().unwrap() = Behavior::IgnoreCancellation(gate.clone());
    let entered = state.watch();
    let cache = native(&state);
    cache
        .set("hot", 8)
        .with_receipt()
        .execute()
        .unwrap()
        .wait()
        .unwrap();
    let source = CancellationSource::new();
    let c = cache.clone();
    let token = source.token();
    let (send, receive) = mpsc::channel();
    let caller = thread::spawn(move || {
        send.send(
            c.get_or_set(
                "cancel",
                typed_blocking_factory(|_| panic!("cancelled origin")),
            )
            .cancellation(token)
            .execute(),
        )
        .unwrap()
    });
    let request = entered.recv_timeout(WAIT).unwrap();
    source.cancel();
    assert!(matches!(
        receive.recv_timeout(WAIT).unwrap(),
        Err(Error::OperationCancelled {
            reason: FactoryCancellationReason::CallerCancelled
        })
    ));
    caller.join().unwrap();
    reason(&request, FactoryCancellationReason::CallerCancelled);
    assert_eq!(cache.read("hot", None).unwrap().into_value(), Some(8));
    cache
        .runtime()
        .run(async { tokio::time::sleep(Duration::from_millis(2)).await });
    gate.release();
    cache.flush_pending().unwrap();
    state.drained();
    assert_eq!(state.acquired.load(Ordering::SeqCst), 1);
    cache.shutdown().unwrap();
}
#[test]
fn queued_cancelled_acquisition_never_invokes_the_provider() {
    let state = State::new();
    let gate = Gate::new();
    *state.behavior.lock().unwrap() = Behavior::IgnoreCancellation(gate.clone());
    let entered = state.watch();
    let cache = native(&state);
    let c = cache.clone();
    let first = thread::spawn(move || {
        c.get_or_set(
            "holding-slot",
            amalgam::source::factory(|ctx| Ok::<_, amalgam::FactoryError>(ctx.value(1))),
        )
        .execute()
    });
    entered.recv_timeout(WAIT).unwrap();
    let source = CancellationSource::new();
    let c = cache.clone();
    let token = source.token();
    let (send, receive) = mpsc::channel();
    let second = thread::spawn(move || {
        send.send(
            c.get_or_set(
                "queued",
                typed_blocking_factory(|_| panic!("queued cancelled origin")),
            )
            .cancellation(token)
            .execute(),
        )
        .unwrap()
    });
    thread::sleep(Duration::from_millis(10));
    source.cancel();
    assert!(matches!(
        receive.recv_timeout(WAIT).unwrap(),
        Err(Error::OperationCancelled {
            reason: FactoryCancellationReason::CallerCancelled
        })
    ));
    second.join().unwrap();
    assert_eq!(state.count(Method::Blocking), 1);
    gate.release();
    assert_eq!(first.join().unwrap().unwrap(), 1);
    cache.flush_pending().unwrap();
    assert_eq!(state.count(Method::Blocking), 1);
    state.drained();
    cache.shutdown().unwrap();
}
#[test]
fn shutdown_cancels_wait_but_drains_the_real_callback_and_late_guard() {
    let state = State::new();
    let gate = Gate::new();
    *state.behavior.lock().unwrap() = Behavior::IgnoreCancellation(gate.clone());
    let entered = state.watch();
    let cache = native(&state);
    let c = cache.clone();
    let (send, receive) = mpsc::channel();
    let caller = thread::spawn(move || {
        send.send(
            c.get_or_set(
                "shutdown",
                typed_blocking_factory(|_| panic!("closed origin")),
            )
            .execute(),
        )
        .unwrap()
    });
    let request = entered.recv_timeout(WAIT).unwrap();
    let c = cache.clone();
    let (close_tx, close_rx) = mpsc::channel();
    let close = thread::spawn(move || close_tx.send(c.shutdown()).unwrap());
    assert!(matches!(
        receive.recv_timeout(WAIT).unwrap(),
        Err(Error::OperationCancelled {
            reason: FactoryCancellationReason::CacheShutdown
        }) | Err(Error::CacheClosed)
    ));
    caller.join().unwrap();
    reason(&request, FactoryCancellationReason::CacheShutdown);
    assert!(close_rx.recv_timeout(Duration::from_millis(30)).is_err());
    assert_eq!(state.shutdowns.load(Ordering::SeqCst), 0);
    gate.release();
    close_rx.recv_timeout(WAIT).unwrap().unwrap();
    close.join().unwrap();
    state.drained();
    assert_eq!(state.shutdowns.load(Ordering::SeqCst), 1);
    cache.shutdown().unwrap();
    assert_eq!(state.shutdowns.load(Ordering::SeqCst), 1);
}
#[test]
fn original_blocking_provider_error_is_not_a_miss_or_factory_failure() {
    let state = State::new();
    *state.behavior.lock().unwrap() = Behavior::Error;
    let cache = native(&state);
    let error = cache
        .get_or_set(
            "cause",
            typed_blocking_factory(|_| panic!("provider failure ran origin")),
        )
        .execute()
        .unwrap_err();
    let Error::MemoryLocker(MemoryLockerError::Provider { source }) = error else {
        panic!("lost provider error: {error:?}")
    };
    assert!(source.downcast_ref::<Cause>().is_some());
    cache.shutdown().unwrap();
    state.drained();
}
#[test]
fn blocking_provider_panic_keeps_its_join_cause_and_shutdown_stage() {
    let state = State::new();
    *state.behavior.lock().unwrap() = Behavior::Panic;
    let cache = native(&state);
    let error = cache
        .get_or_set(
            "panic",
            typed_blocking_factory(|_| panic!("panicked provider ran origin")),
        )
        .execute()
        .unwrap_err();
    let Error::MemoryLocker(MemoryLockerError::Provider { source }) = error else {
        panic!("lost callback panic: {error:?}")
    };
    assert!(
        source
            .downcast_ref::<tokio::task::JoinError>()
            .unwrap()
            .is_panic()
    );
    let Err(Error::Shutdown(report)) = cache.shutdown() else {
        panic!("owned callback panic must survive shutdown")
    };
    assert!(report.failures().iter().any(|e| matches!(
        e,
        ShutdownFailure::BackgroundTask {
            task: ShutdownTask::MemoryLockerAcquisition,
            ..
        }
    )));
    state.drained();
}
#[test]
fn unavailable_blocking_provider_permits_the_ordinary_unlocked_factory() {
    let state = State::new();
    *state.behavior.lock().unwrap() = Behavior::Unavailable;
    let cache = native(&state);
    assert_eq!(
        cache
            .get_or_set(
                "unlocked",
                amalgam::source::factory(|ctx| Ok::<_, amalgam::FactoryError>(ctx.value(4)))
            )
            .execute()
            .unwrap(),
        4
    );
    assert_eq!(state.acquired.load(Ordering::SeqCst), 0);
    cache.shutdown().unwrap();
}
#[test]
fn async_view_stays_async_inside_a_native_factory_and_after_native_drop() {
    let state = State::new();
    let cache = native(&state);
    let asynchronous = cache.as_async().clone();
    let driver = cache.runtime().clone();
    let child = asynchronous.clone();
    let run = driver.clone();
    assert_eq!(
        cache
            .get_or_set(
                "outer",
                amalgam::source::factory(move |ctx| run
                    .run(child.get_or_set(
                        "inner",
                        amalgam::source::factory(|ctx| async move {
                            Ok::<_, amalgam::FactoryError>(ctx.value(8))
                        })
                    ))
                    .map(|value| ctx.value(value + 1))
                    .map_err(FactoryError::from_source))
            )
            .execute()
            .unwrap(),
        9
    );
    assert_eq!(state.count(Method::Blocking), 1);
    assert_eq!(state.count(Method::Async), 1);
    drop(cache);
    assert_eq!(
        driver
            .run(asynchronous.get_or_set(
                "after",
                amalgam::source::factory(|ctx| async move {
                    Ok::<_, amalgam::FactoryError>(ctx.value(11))
                })
            ))
            .unwrap(),
        11
    );
    assert_eq!(state.count(Method::Blocking), 1);
    assert_eq!(state.count(Method::Async), 2);
    driver.run(asynchronous.shutdown()).unwrap();
    state.drained();
    assert_eq!(state.shutdowns.load(Ordering::SeqCst), 1);
}
#[test]
fn callbacks_and_late_release_reject_draining_their_own_cache() {
    let state = State::new();
    let gate = Gate::new();
    *state.behavior.lock().unwrap() = Behavior::IgnoreCancellation(gate.clone());
    let entered = state.watch();
    let cache = native(&state);
    let c = cache.clone();
    *state.acquire_hook.lock().unwrap() = Some(Arc::new(move || {
        assert!(matches!(
            c.flush_pending(),
            Err(Error::ReentrantDrain {
                operation: DrainOperation::FlushPending
            })
        ))
    }));
    let c = cache.clone();
    *state.release_hook.lock().unwrap() = Some(Arc::new(move || {
        assert!(matches!(
            c.shutdown(),
            Err(Error::ReentrantDrain {
                operation: DrainOperation::Shutdown
            })
        ))
    }));
    let source = CancellationSource::new();
    let c = cache.clone();
    let token = source.token();
    let (send, receive) = mpsc::channel();
    let caller = thread::spawn(move || {
        send.send(
            c.get_or_set(
                "late",
                typed_blocking_factory(|_| panic!("cancelled origin")),
            )
            .cancellation(token)
            .execute(),
        )
        .unwrap()
    });
    entered.recv_timeout(WAIT).unwrap();
    source.cancel();
    assert!(receive.recv_timeout(WAIT).unwrap().is_err());
    caller.join().unwrap();
    gate.release();
    cache.flush_pending().unwrap();
    state.acquire_hook.lock().unwrap().take();
    state.release_hook.lock().unwrap().take();
    state.drained();
    cache.shutdown().unwrap();
}
#[test]
fn soft_timeout_background_keeps_the_blocking_acquired_guard_until_commit() {
    let state = State::new();
    let clock = Arc::new(ManualClock::new(Timestamp::from_ticks(10_000)));
    let opts = EntryOptions::new(Duration::from_secs(1))
        .with_fail_safe(true, Some(Duration::from_secs(60)), None)
        .with_factory_timeouts(
            Timeout::After(Duration::from_millis(30)),
            Timeout::Infinite,
            true,
        );
    let cache = BlockingCache::on_runtime(
        builder(&state).clock(clock.clone()).default_options(opts),
        driver(),
    )
    .unwrap();
    cache
        .set("soft", 1)
        .with_receipt()
        .execute()
        .unwrap()
        .wait()
        .unwrap();
    clock.advance(Duration::from_secs(2));
    let gate = Gate::new();
    let blocked = gate.clone();
    let (send, receive) = mpsc::channel();
    assert_eq!(
        cache
            .get_or_set(
                "soft",
                amalgam::source::factory(move |ctx| {
                    send.send(()).unwrap();
                    blocked.wait();
                    Ok::<_, amalgam::FactoryError>(ctx.value(2))
                })
            )
            .execute()
            .unwrap(),
        1
    );
    receive.recv_timeout(WAIT).unwrap();
    assert_eq!(state.acquired.load(Ordering::SeqCst), 1);
    assert_eq!(state.returned.load(Ordering::SeqCst), 0);
    gate.release();
    cache.flush_pending().unwrap();
    assert_eq!(cache.read("soft", None).unwrap().into_value(), Some(2));
    state.drained();
    cache.shutdown().unwrap();
}
#[test]
fn eager_refresh_uses_nonblocking_try_and_never_blocking_acquisition() {
    let state = State::new();
    let clock = Arc::new(ManualClock::new(Timestamp::from_ticks(10_000)));
    let opts = EntryOptions::new(Duration::from_secs(10))
        .with_eager_refresh(Some(EagerThreshold::new(0.5).unwrap()));
    let cache = BlockingCache::on_runtime(
        builder(&state).clock(clock.clone()).default_options(opts),
        driver(),
    )
    .unwrap();
    assert_eq!(
        cache
            .get_or_set(
                "eager",
                amalgam::source::factory(|ctx| Ok::<_, amalgam::FactoryError>(ctx.value(1)))
            )
            .execute()
            .unwrap(),
        1
    );
    clock.advance(Duration::from_secs(6));
    assert_eq!(
        cache
            .get_or_set(
                "eager",
                amalgam::source::factory(|ctx| Ok::<_, amalgam::FactoryError>(ctx.value(2)))
            )
            .execute()
            .unwrap(),
        1
    );
    cache.flush_pending().unwrap();
    assert_eq!(cache.read("eager", None).unwrap().into_value(), Some(2));
    assert_eq!(state.count(Method::Blocking), 1);
    assert_eq!(state.count(Method::Try), 1);
    state.drained();
    cache.shutdown().unwrap();
}
#[test]
fn native_marker_factories_use_the_blocking_provider_and_disjoint_keys() {
    let state = State::new();
    let cache = BlockingCache::on_runtime(
        builder(&state)
            .invalidation_store(Arc::new(InMemoryInvalidationStore::default()))
            .marker_read_policy(MarkerReadPolicy::OptionsControlled)
            .marker_lifecycle_policy(MarkerLifecyclePolicy::CachedSnapshots),
        driver(),
    )
    .unwrap();
    let tag = Tag::new("group").unwrap();
    assert_eq!(
        cache
            .get_or_set(
                "tagged",
                typed_blocking_factory(|ctx| Ok::<_, amalgam::FactoryError>(ctx.value(7)))
            )
            .tags(vec![tag.clone()].into_boxed_slice())
            .execute()
            .unwrap(),
        7
    );
    assert_eq!(cache.read("tagged", None).unwrap().into_value(), Some(7));
    let requests = state.requests.lock().unwrap();
    assert!(
        requests
            .iter()
            .any(|(method, r)| *method == Method::Blocking
                && r.kind() == &MemoryLockKind::Marker(MarkerKind::Tag(tag.clone())))
    );
    assert!(
        requests
            .iter()
            .any(|(method, r)| *method == Method::Blocking && r.kind() == &MemoryLockKind::Entry)
    );
    assert!(!requests.iter().any(|(method, _)| *method == Method::Async));
    drop(requests);
    state.drained();
    cache.shutdown().unwrap();
}
#[test]
fn opposite_nested_native_calls_progress_with_one_slot_per_class() {
    let left_state = State::new();
    let right_state = State::new();
    let left = native(&left_state);
    let right = native(&right_state);
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let source = CancellationSource::new();
    let (send, receive) = mpsc::channel();
    let mut threads = Vec::with_capacity(2);
    for (outer, inner, key) in [
        (left.clone(), right.clone(), "left"),
        (right.clone(), left.clone(), "right"),
    ] {
        let barrier = barrier.clone();
        let token = source.token();
        let nested = token.clone();
        let send = send.clone();
        threads.push(thread::spawn(move || {
            let result = outer
                .get_or_set(
                    key,
                    typed_blocking_factory(move |ctx| {
                        barrier.wait();
                        inner
                            .get_or_set(
                                format!("child/{key}"),
                                typed_blocking_factory(|ctx| {
                                    Ok::<_, amalgam::FactoryError>(ctx.value(7))
                                }),
                            )
                            .cancellation(nested)
                            .execute()
                            .map(|value| ctx.value(value + 1))
                            .map_err(FactoryError::from_source)
                    }),
                )
                .cancellation(token)
                .execute();
            send.send(result).unwrap();
        }));
    }
    let a = receive.recv_timeout(WAIT);
    let b = receive.recv_timeout(WAIT);
    source.cancel();
    for t in threads {
        t.join().unwrap();
    }
    left.shutdown().unwrap();
    right.shutdown().unwrap();
    assert_eq!(a.expect("nested left must progress").unwrap(), 8);
    assert_eq!(b.expect("nested right must progress").unwrap(), 8);
    left_state.drained();
    right_state.drained();
}

fn typed_blocking_factory<V, F>(factory: F) -> F
where
    F: FnOnce(amalgam::FactoryContext<V>) -> std::result::Result<V, amalgam::FactoryError>,
{
    factory
}

fn typed_factory<V, F, Fut>(factory: F) -> F
where
    F: FnOnce(amalgam::FactoryContext<V>) -> Fut,
    Fut: std::future::Future<Output = std::result::Result<V, amalgam::FactoryError>>,
{
    factory
}
