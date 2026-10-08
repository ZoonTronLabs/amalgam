//! Native lazy mutation requests use the shared asynchronous operation engine.
use super::{BlockingCommitCompletion, BlockingMutationReceipt, BlockingRuntime};
use crate::cache::{
    ClearRequest, ExpireRequest, GetOrDefaultRequest, ReceiptInvalidationRequest,
    ReceiptSetRequest, RemoveRequest, SetRequest, TagInvalidationRequest,
};
use crate::{
    EntryOptions, FactoryCancellation, Result, advanced::DistributedExpirePolicy,
    advanced::MutationReceipt,
};
use std::future::IntoFuture;

/// A lazy native mutation. Execute it explicitly after configuring the request.
#[must_use = "a native cache request only executes through execute()"]
pub struct BlockingRequest<'a, R> {
    pub(super) runtime: &'a BlockingRuntime,
    pub(super) request: R,
}
/// A lazy native mutation returning actual synchronous commit evidence.
#[must_use = "a native cache request only executes through execute()"]
pub struct BlockingReceiptRequest<'a, R> {
    runtime: &'a BlockingRuntime,
    request: R,
}
impl<R: IntoFuture> BlockingRequest<'_, R> {
    /// Runs this operation and returns its typed result.
    pub fn execute(self) -> R::Output {
        self.runtime.run(self.request)
    }
}
impl<R: IntoFuture<Output = Result<MutationReceipt>>> BlockingReceiptRequest<'_, R> {
    /// Runs this operation and preserves scheduled completion on the same executor.
    pub fn execute(self) -> Result<BlockingMutationReceipt> {
        self.runtime.run(self.request).map(|receipt| match receipt {
            MutationReceipt::Completed(report) => BlockingMutationReceipt::Completed(report),
            MutationReceipt::Scheduled(completion) => {
                BlockingMutationReceipt::Scheduled(BlockingCommitCompletion {
                    completion,
                    runtime: self.runtime.clone(),
                })
            }
        })
    }
}
macro_rules! settings {
    ($wrapper:ident, $request:ident $(, $key:ident)?) => {
        impl<'a, $($key,)? V: Clone + Send + Sync + 'static>
            $wrapper<'a, $request<'a, $($key,)? V>> {
            /// Edits this operation's defaults, preserving untouched settings.
            pub fn options(mut self, update: impl FnOnce(EntryOptions) -> EntryOptions) -> Self {
                self.request = self.request.options(update);
                self
            }
            /// Links caller cancellation during execution.
            pub fn cancellation(mut self, token: FactoryCancellation) -> Self {
                self.request = self.request.cancellation(token);
                self
            }
        }
    };
}
settings!(BlockingRequest, RemoveRequest, K);
settings!(BlockingRequest, ExpireRequest, K);
settings!(BlockingRequest, SetRequest, K);
settings!(BlockingRequest, GetOrDefaultRequest, K);
settings!(BlockingRequest, TagInvalidationRequest);
settings!(BlockingRequest, ClearRequest);
settings!(BlockingReceiptRequest, ReceiptInvalidationRequest, K);
settings!(BlockingReceiptRequest, ReceiptSetRequest, K);
macro_rules! receipt {
    ($request:ident, $receipt:ident, $key:ident) => {
        impl<'a, $key, V: Clone + Send + Sync + 'static>
            BlockingRequest<'a, $request<'a, $key, V>>
        {
            /// Requests synchronous evidence of local and distributed effects.
            pub fn with_receipt(self) -> BlockingReceiptRequest<'a, $receipt<'a, $key, V>> {
                BlockingReceiptRequest {
                    runtime: self.runtime,
                    request: self.request.with_receipt(),
                }
            }
        }
    };
}
receipt!(SetRequest, ReceiptSetRequest, K);
receipt!(RemoveRequest, ReceiptInvalidationRequest, K);
receipt!(ExpireRequest, ReceiptInvalidationRequest, K);
impl<'a, V: Clone + Send + Sync + 'static> BlockingRequest<'a, TagInvalidationRequest<'a, V>> {
    /// Adds raw tags; one invalid tag rejects the entire batch.
    pub fn and_tags<I, S>(mut self, tags: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.request = self.request.and_tags(tags);
        self
    }
    /// Requests synchronous evidence of local and distributed effects.
    pub fn with_receipt(
        self,
    ) -> BlockingReceiptRequest<'a, ReceiptInvalidationRequest<'a, &'static str, V>> {
        BlockingReceiptRequest {
            runtime: self.runtime,
            request: self.request.with_receipt(),
        }
    }
}
impl<'a, V: Clone + Send + Sync + 'static> BlockingRequest<'a, ClearRequest<'a, V>> {
    /// Requests synchronous evidence of local and distributed effects.
    pub fn with_receipt(
        self,
    ) -> BlockingReceiptRequest<'a, ReceiptInvalidationRequest<'a, &'static str, V>> {
        BlockingReceiptRequest {
            runtime: self.runtime,
            request: self.request.with_receipt(),
        }
    }
}
impl<'a, K, V: Clone + Send + Sync + 'static> BlockingRequest<'a, ExpireRequest<'a, K, V>> {
    /// Explicit advanced L2 retention; ordinary expiration removes L2.
    pub fn distributed_policy(mut self, policy: DistributedExpirePolicy) -> Self {
        self.request = self.request.distributed_policy(policy);
        self
    }
}
macro_rules! tags {
    ($wrapper:ident, $request:ident) => {
        impl<'a, K, V: Clone + Send + Sync + 'static> $wrapper<'a, $request<'a, K, V>> {
            /// Assigns raw string tags; invalid input rejects the operation.
            pub fn tags<I, S>(mut self, tags: I) -> Self
            where
                I: IntoIterator<Item = S>,
                S: AsRef<str>,
            {
                self.request = self.request.tags(tags);
                self
            }
        }
    };
}
tags!(BlockingRequest, SetRequest);
tags!(BlockingReceiptRequest, ReceiptSetRequest);
