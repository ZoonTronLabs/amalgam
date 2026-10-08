//! Build-selected inline completion of distributed reads over immediate providers.
//!
//! When every awaited read step answers from in-process state, a lookup that
//! misses L1 runs the unchanged read pipeline on the caller's stack and finishes
//! within its first poll. The operation then needs no cache-owned execution: no
//! pinned allocation, no registered scope and no observer future. Immediate
//! providers promise not to suspend; an unexpected suspension still falls back
//! to the owned pipeline, which repeats only in-process reads.
use super::{
    Cache, CacheMemory, CancellationSource, EntryOptions, Error, FactoryCancellation,
    InlinePermit, MarkerAccess, MarkerReads, OperationOutcome, Ordering, ReadyObservation, Result,
    Storage,
};
use crate::distributed::{ReadCompletion, SerializationMode};
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

/// How an L1 miss reaches distributed storage, fixed at construction.
#[derive(Clone, Copy)]
pub(super) enum DistributedReadPlan {
    /// Reads await providers inside cache-owned execution.
    Owned,
    /// Every read step completes immediately; warm reads finish inline.
    Immediate,
}

impl DistributedReadPlan {
    pub(super) fn select<V: Clone + Send + Sync + 'static>(
        storage: &Storage<V>,
        mode: SerializationMode,
        markers: &MarkerAccess,
        marker_reads: &MarkerReads,
        disable_tagging: bool,
        memory: &CacheMemory<V>,
        backplane: bool,
    ) -> Self {
        let Storage::Hybrid {
            backend,
            serializer,
        } = storage
        else {
            return Self::Owned;
        };
        let markers_immediate = disable_tagging
            || match markers {
                MarkerAccess::Durable(store) => {
                    matches!(marker_reads, MarkerReads::DurableRequired)
                        && store.read_completion() == ReadCompletion::Immediate
                }
                MarkerAccess::Local | MarkerAccess::Ordinary(_) | MarkerAccess::Unavailable => {
                    false
                }
            };
        if backend.read_completion() == ReadCompletion::Immediate
            && serializer.decodes_synchronously(mode)
            && markers_immediate
            && matches!(memory, CacheMemory::Builtin(_))
            && !backplane
        {
            Self::Immediate
        } else {
            Self::Owned
        }
    }
}

/// The inline attempt either finished the logical operation or hands its
/// still-pending observation back to the owned pipeline.
pub(super) enum InlineRead<'a, T> {
    Completed(Result<T>),
    Deferred(ReadyObservation<'a>),
}

impl<V: Clone + Send + Sync + 'static> Cache<V> {
    /// Completes an L1 miss inline under the caller's counted admission when
    /// the build-selected plan proves every distributed read step immediate.
    pub(super) fn immediate_read<'a>(
        &self,
        raw: &str,
        full: &str,
        options: Option<&EntryOptions>,
        token: Option<&FactoryCancellation>,
        permit: &InlinePermit<'_>,
        mut observation: ReadyObservation<'a>,
    ) -> InlineRead<'a, Option<V>> {
        if !matches!(
            self.inner.distributed_read_plan,
            DistributedReadPlan::Immediate
        ) || token.is_some()
            // The owned pipeline starts maintenance for the first operation.
            || !self.inner.maintenance.load(Ordering::Acquire)
        {
            return InlineRead::Deferred(observation);
        }
        let worker = self.worker();
        let key = self.lookup_key(raw, Arc::from(full));
        let source = CancellationSource::for_cache(self.operation_scopes());
        let cancellation = source.token();
        let span = observation.span();
        let _entered = span.enter();
        let read = worker.read(key, options.cloned().map(Box::new), &cancellation);
        let mut read = std::pin::pin!(read);
        let Poll::Ready(result) = read
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        else {
            return InlineRead::Deferred(observation);
        };
        // Cancellation recorded while the pipeline ran wins over its result,
        // exactly as completion of an owned execution reports it.
        let result = match cancellation.reason() {
            Some(reason) => Err(Error::OperationCancelled { reason }),
            None => result,
        }
        .and_then(|observed| permit.status(None).map(|()| observed));
        InlineRead::Completed(match result {
            Ok(observed) => {
                if let Some(level) = observed.level {
                    observation.set_level(level);
                }
                observation.finish(observed.outcome);
                Ok(observed.value)
            }
            Err(error) => {
                observation.finish(OperationOutcome::from_error(&error));
                Err(error)
            }
        })
    }
}
