//! Retains only general factory/commit work. Caller policies remain independent.
use crate::error::{Error, FactoryCancellationReason as Reason, Result};
use crate::execution::{CancelWork, CancellationSource, FactoryCancellation, LinkMode, Scopes};
use crate::single_flight::{Completion, Driver, FlightIdentity, Flights, Subscription};
use parking_lot::Mutex;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

#[derive(Clone, Copy)]
pub(crate) enum Audience {
    Foreground,
    Background,
}
enum Delivery {
    Active(Audience),
    Completed(Audience, Result<()>),
    BackgroundPanic(crate::single_flight::Panic),
}
struct Identity {
    key: Arc<str>,
    delivery: Mutex<Delivery>,
}
#[derive(Clone)]
pub(crate) struct OriginIdentity(Arc<Identity>);
impl PartialEq for OriginIdentity {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Eq for OriginIdentity {}
impl Hash for OriginIdentity {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.key.hash(state);
    }
}
impl FlightIdentity for OriginIdentity {
    fn key(&self) -> &Arc<str> {
        &self.0.key
    }
    fn panicked(&self, panic: crate::single_flight::Panic) -> Option<crate::single_flight::Panic> {
        let mut panic = Some(panic);
        let retired = {
            let mut delivery = self.0.delivery.lock();
            let next = match &*delivery {
                Delivery::Active(Audience::Foreground)
                | Delivery::Completed(Audience::Foreground, _) => {
                    Delivery::Completed(Audience::Foreground, Err(Error::FactoryPanicked))
                }
                Delivery::Active(Audience::Background)
                | Delivery::Completed(Audience::Background, _)
                | Delivery::BackgroundPanic(_) => {
                    Delivery::BackgroundPanic(panic.take().expect("panic payload is owned"))
                }
            };
            std::mem::replace(&mut *delivery, next)
        };
        drop(retired);
        panic
    }
}
impl OriginIdentity {
    pub(crate) fn complete<T>(&self, result: &Result<T>) -> Audience {
        let retired = {
            let mut delivery = self.0.delivery.lock();
            let audience = match &*delivery {
                Delivery::Active(audience) | Delivery::Completed(audience, _) => *audience,
                Delivery::BackgroundPanic(_) => Audience::Background,
            };
            let retired = std::mem::replace(
                &mut *delivery,
                Delivery::Completed(audience, result.as_ref().map(|_| ()).map_err(Clone::clone)),
            );
            (audience, retired)
        };
        // An error can retain a user-defined source: release it outside the guard.
        drop(retired.1);
        retired.0
    }
    fn detach(&self) {
        let mut delivery = self.0.delivery.lock();
        match &mut *delivery {
            Delivery::Active(audience) | Delivery::Completed(audience, _) => {
                *audience = Audience::Background
            }
            Delivery::BackgroundPanic(_) => {}
        }
    }
    fn background_result(&self) -> Result<()> {
        enum Completion {
            Result(Result<()>),
            Panic(crate::single_flight::Panic),
        }
        let completion = {
            let mut delivery = self.0.delivery.lock();
            match &*delivery {
                Delivery::Completed(Audience::Background, result) => {
                    Completion::Result(result.clone())
                }
                Delivery::Active(_) | Delivery::Completed(Audience::Foreground, _) => {
                    Completion::Result(Ok(()))
                }
                Delivery::BackgroundPanic(_) => {
                    match std::mem::replace(
                        &mut *delivery,
                        Delivery::Completed(Audience::Background, Err(Error::FactoryPanicked)),
                    ) {
                        Delivery::BackgroundPanic(panic) => Completion::Panic(panic),
                        Delivery::Active(_) | Delivery::Completed(_, _) => {
                            unreachable!("panic delivery owns its payload")
                        }
                    }
                }
            }
        };
        // The supervisor receives the original panic, outside every coordinator
        // guard. Waiting callers independently observe FactoryPanicked.
        match completion {
            Completion::Result(result) => result,
            Completion::Panic(panic) => std::panic::resume_unwind(panic),
        }
    }
    fn progress_result(&self) -> Result<()> {
        match &*self.0.delivery.lock() {
            Delivery::Completed(_, Err(Error::FactoryPanicked)) | Delivery::BackgroundPanic(_) => {
                Err(Error::FactoryPanicked)
            }
            Delivery::Active(_) | Delivery::Completed(_, Ok(_) | Err(_)) => Ok(()),
        }
    }
}

pub(crate) struct RetainedOrigins<T: Send + Sync + 'static> {
    index: Arc<Flights<T, OriginIdentity>>,
    changed: tokio::sync::Notify,
}
impl<T: Send + Sync + 'static> RetainedOrigins<T> {
    pub(crate) fn new() -> Self {
        Self {
            index: Flights::new(),
            changed: tokio::sync::Notify::new(),
        }
    }
    pub(crate) fn start(&self, key: Arc<str>, scopes: Arc<Scopes>) -> OriginClient<T> {
        let identity = OriginIdentity(Arc::new(Identity {
            key,
            delivery: Mutex::new(Delivery::Active(Audience::Foreground)),
        }));
        let claim = self.index.acquire(identity.clone(), scopes);
        debug_assert!(claim.leader, "a fresh identity is always independent work");
        self.changed.notify_waiters();
        OriginClient {
            subscription: claim.subscription,
            identity,
            state: ClientState::Waiting,
        }
    }
    pub(crate) fn changed(&self) -> tokio::sync::futures::Notified<'_> {
        self.changed.notified()
    }
    pub(crate) fn help(&self, key: &str) -> Option<OriginProgress<T>> {
        let flight = self.index.active(key)?;
        let identity = flight.identity().clone();
        Some(OriginProgress {
            driver: flight.driver(),
            identity,
        })
    }
}
enum ClientState {
    Waiting,
    Delivered,
}
pub(crate) struct OriginClient<T: Send + Sync + 'static> {
    subscription: Subscription<T, OriginIdentity>,
    identity: OriginIdentity,
    state: ClientState,
}
impl<T: Send + Sync + 'static> OriginClient<T> {
    pub(crate) fn source(&self) -> CancellationSource {
        self.subscription.flight.source()
    }
    pub(crate) fn reporter(&self) -> OriginIdentity {
        self.identity.clone()
    }
    pub(crate) fn link(&self, caller: &FactoryCancellation) {
        caller.link_work(self.subscription.flight.clone(), LinkMode::OriginCaller);
    }
    pub(crate) fn link_explicit(&self, caller: &FactoryCancellation) {
        // Explicit cancellation remains attached even after the caller's own
        // scope has ended with CallerDropped and can no longer change reason.
        caller.link_work(self.subscription.flight.clone(), LinkMode::Explicit);
    }
    pub(crate) fn begin(&self, work: impl Future<Output = Result<T>> + Send + 'static) {
        self.subscription.flight.install(Box::pin(work));
        self.subscription.flight.poll_work();
        if !self.subscription.flight.is_finished() {
            self.subscription.flight.register_pending();
        }
    }
    pub(crate) fn is_pending(&self) -> bool {
        !self.subscription.flight.is_finished()
    }
    pub(crate) fn cancel(&self, reason: Reason) {
        self.subscription.flight.cancel(reason);
    }
    pub(crate) fn driver(&self) -> impl Future<Output = Result<()>> + Send + 'static {
        let driver = self.subscription.flight.clone().driver();
        let identity = self.identity.clone();
        async move {
            driver.await?;
            identity.background_result()
        }
    }
}
impl<T: Send + Sync + 'static> Future for OriginClient<T> {
    type Output = Result<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match Pin::new(&mut this.subscription).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(completion) => {
                this.state = ClientState::Delivered;
                Poll::Ready(match completion {
                    Completion::Owned(result) => result,
                    Completion::Panicked(panic) => std::panic::resume_unwind(panic),
                    Completion::Shared(_) | Completion::FollowerPanicked => {
                        unreachable!("retained work has exactly one result consumer")
                    }
                })
            }
        }
    }
}
impl<T: Send + Sync + 'static> Drop for OriginClient<T> {
    fn drop(&mut self) {
        if matches!(self.state, ClientState::Waiting) {
            self.identity.detach();
        }
    }
}
pub(crate) struct OriginProgress<T: Send + Sync + 'static> {
    driver: Driver<T, OriginIdentity>,
    identity: OriginIdentity,
}
impl<T: Send + Sync + 'static> Future for OriginProgress<T> {
    type Output = Result<()>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.driver)
            .poll(cx)
            .map(|result| result.and_then(|()| this.identity.progress_result()))
    }
}
