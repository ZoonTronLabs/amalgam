//! A movable observer handle; cache-owned work stays behind its stable pointer.
use super::{Error, Execution, FactoryCancellation, LinkMode, Result};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

enum State<T: Send + 'static> {
    Unlinked {
        execution: Execution<T>,
        caller: FactoryCancellation,
    },
    Active {
        execution: Execution<T>,
        // Retain caller ownership through completion, including token owners
        // whose final destruction can retire a captured factory result.
        _caller: Option<FactoryCancellation>,
    },
    Rejected(Pin<Box<dyn Future<Output = Result<T>> + Send + 'static>>),
    Finished,
}
pub(super) struct ObservedExecution<T: Send + 'static> {
    state: State<T>,
}
impl<T: Send + 'static> ObservedExecution<T> {
    pub(super) fn new(prepared: Result<Execution<T>>, caller: Option<FactoryCancellation>) -> Self {
        let state = match (prepared, caller) {
            (Ok(execution), None) => State::Active {
                execution,
                _caller: None,
            },
            (Ok(execution), Some(caller)) => State::Unlinked { execution, caller },
            (Err(error), caller) => Self::rejected(error, caller),
        };
        Self { state }
    }
    fn rejected(error: Error, caller: Option<FactoryCancellation>) -> State<T> {
        // Preserve failed preparation's captures until poll/drop, as in the
        // previous observer adapter. No user work is moved.
        State::Rejected(Box::pin(async move {
            let prepared: Result<Execution<T>> = Err(error);
            super::drive(prepared?, caller).await
        }))
    }
    fn link(&mut self) {
        if !matches!(self.state, State::Unlinked { .. }) {
            return;
        }
        let State::Unlinked { execution, caller } =
            std::mem::replace(&mut self.state, State::Finished)
        else {
            unreachable!("the unlinked state was matched")
        };
        // Explicit links deliver every terminal reason and wake/cancel the
        // scope itself. Preserve the previous first-poll subscription.
        execution.link(&caller, LinkMode::Explicit);
        self.state = State::Active {
            execution,
            _caller: Some(caller),
        };
    }
}
impl<T: Send + 'static> Future for ObservedExecution<T> {
    type Output = Result<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.link();
        let result = match &mut this.state {
            State::Active { execution, .. } => Pin::new(execution).poll(cx),
            State::Rejected(work) => work.as_mut().poll(cx),
            State::Unlinked { .. } => unreachable!("linking completed before work polling"),
            State::Finished => panic!("completed cache observer was polled again"),
        };
        if result.is_ready() {
            this.state = State::Finished;
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::ObservedExecution;
    use crate::error::{Error, FactoryCancellationReason as Reason, Result};
    use crate::execution::{CancellationSource, FactoryCancellation, Scopes};
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};
    use std::time::Duration;

    struct Retirement {
        token: FactoryCancellation,
        reasons: Arc<Mutex<Vec<Reason>>>,
    }
    impl Drop for Retirement {
        fn drop(&mut self) {
            self.reasons
                .lock()
                .unwrap()
                .push(self.token.reason().unwrap());
        }
    }
    struct Waiting {
        _retirement: Retirement,
    }
    impl Future for Waiting {
        type Output = Result<u64>;
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Pending
        }
    }
    struct Ready {
        _retirement: Retirement,
    }
    impl Future for Ready {
        type Output = Result<u64>;
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Ready(Ok(7))
        }
    }

    #[tokio::test]
    async fn every_explicit_reason_retires_pending_work_before_another_caller_poll() {
        for reason in [
            Reason::CallerCancelled,
            Reason::CallerDropped,
            Reason::SoftTimeout,
            Reason::HardTimeout,
            Reason::CacheShutdown,
            Reason::LeaseLost,
            Reason::ScopeFinished,
        ] {
            let scopes = Scopes::new();
            let source = CancellationSource::new();
            let caller = CancellationSource::new();
            let reasons = Arc::new(Mutex::new(Vec::new()));
            let execution = scopes.execution(
                Waiting {
                    _retirement: Retirement {
                        token: source.token(),
                        reasons: Arc::clone(&reasons),
                    },
                },
                source,
            );
            let mut observer =
                std::pin::pin!(ObservedExecution::new(Ok(execution), Some(caller.token())));
            let first = std::future::poll_fn(|cx| Poll::Ready(observer.as_mut().poll(cx))).await;
            assert!(first.is_pending());
            caller.cancel_with(reason);
            assert_eq!(*reasons.lock().unwrap(), vec![reason]);
            assert!(
                matches!(observer.await, Err(Error::OperationCancelled { reason: actual }) if actual == reason)
            );
            scopes.close();
            scopes.drained().await;
        }
    }

    #[tokio::test]
    async fn plain_pending_observer_shutdown_retires_work_without_caller_repoll() {
        let scopes = Scopes::new();
        let source = CancellationSource::new();
        let reasons = Arc::new(Mutex::new(Vec::new()));
        let execution = scopes.execution(
            Waiting {
                _retirement: Retirement {
                    token: source.token(),
                    reasons: Arc::clone(&reasons),
                },
            },
            source,
        );
        let mut observer = std::pin::pin!(ObservedExecution::new(Ok(execution), None));
        let first = std::future::poll_fn(|cx| Poll::Ready(observer.as_mut().poll(cx))).await;
        assert!(first.is_pending());
        scopes.close();
        tokio::time::timeout(Duration::from_secs(1), scopes.drained())
            .await
            .unwrap();
        assert_eq!(*reasons.lock().unwrap(), vec![Reason::CacheShutdown]);
        assert!(matches!(
            observer.await,
            Err(Error::OperationCancelled {
                reason: Reason::CacheShutdown
            })
        ));
    }

    #[tokio::test]
    async fn plain_ready_observer_publishes_completion_before_work_retirement() {
        let scopes = Scopes::new();
        let source = CancellationSource::new();
        let reasons = Arc::new(Mutex::new(Vec::new()));
        let execution = scopes.execution(
            Ready {
                _retirement: Retirement {
                    token: source.token(),
                    reasons: Arc::clone(&reasons),
                },
            },
            source,
        );
        assert_eq!(
            ObservedExecution::new(Ok(execution), None).await.unwrap(),
            7
        );
        assert_eq!(*reasons.lock().unwrap(), vec![Reason::ScopeFinished]);
        scopes.close();
        scopes.drained().await;
    }
}
