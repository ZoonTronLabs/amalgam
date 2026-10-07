//! Build-selected primitive copies have no callback or owned work to drain.
//! Storage admission still excludes writers and checks close before the copy.
//! Any actual observer or input destructor retains ordinary counted admission.
use super::plain_ready::{QuietCopy, QuietReady, QuietStart};
use super::{Cache, CacheMemory, CacheOperation, Error, LookupMode, MaybeValue, QuietObservation};
use std::any::TypeId;

pub(super) enum Inputs {
    NoCallbacks,
    Destructors,
}
impl Inputs {
    pub(super) fn origin<T, V>(_: &T, tags: &[super::Tag], fallback: &MaybeValue<V>) -> Self {
        if std::mem::needs_drop::<T>() || !tags.is_empty() || fallback.has_value() {
            Self::Destructors
        } else {
            Self::NoCallbacks
        }
    }
}
pub(super) fn primitive<V: 'static>() -> bool {
    let value = TypeId::of::<V>();
    [
        TypeId::of::<()>(),
        TypeId::of::<bool>(),
        TypeId::of::<char>(),
        TypeId::of::<i8>(),
        TypeId::of::<i16>(),
        TypeId::of::<i32>(),
        TypeId::of::<i64>(),
        TypeId::of::<i128>(),
        TypeId::of::<isize>(),
        TypeId::of::<u8>(),
        TypeId::of::<u16>(),
        TypeId::of::<u32>(),
        TypeId::of::<u64>(),
        TypeId::of::<u128>(),
        TypeId::of::<usize>(),
        TypeId::of::<f32>(),
        TypeId::of::<f64>(),
    ]
    .contains(&value)
}

impl<V: Clone + Send + Sync + 'static> Cache<V> {
    pub(super) fn callback_free_lookup<'a>(
        &'a self,
        key: &str,
        operation: CacheOperation,
        mode: LookupMode,
    ) -> QuietStart<'a, V> {
        let Some(scopes) = self.callback_free_scopes() else {
            return self.quiet_lookup(key, None, None, operation, mode, Inputs::Destructors);
        };
        let CacheMemory::Builtin(memory) = &self.inner.memory else {
            unreachable!("callback-free plan requires builtin unbounded L1");
        };
        let copied = memory
            .with_callback_free_local_ready(key, scopes, |entry, freshness| {
                self.quiet_copy(entry, freshness, mode)
            })
            .map(Option::flatten);
        let copied = match copied {
            Err(Error::CacheClosed) => Err(Error::CacheClosed),
            _ if scopes.is_closed() => Err(Error::OperationCancelled {
                reason: super::Reason::CacheShutdown,
            }),
            copied => copied,
        };
        match copied {
            Ok(Some(QuietCopy::Value(value))) if self.inner.events.is_quiet() => {
                QuietStart::Complete(Ok(value))
            }
            Err(error) if self.inner.events.is_quiet() => QuietStart::Complete(Err(error)),
            copied => self.counted_primitive_lookup(operation, copied),
        }
    }
    fn counted_primitive_lookup<'a>(
        &'a self,
        operation: CacheOperation,
        copied: super::Result<Option<QuietCopy<V>>>,
    ) -> QuietStart<'a, V> {
        let permit = self.inline();
        let observation = QuietObservation::new(&self.inner.events, operation);
        if copied.is_ok() {
            let CacheMemory::Builtin(memory) = &self.inner.memory else {
                unreachable!("callback-free plan requires builtin unbounded L1");
            };
            memory.component_read(crate::events::ComponentRead::Memory);
        }
        match copied {
            Ok(Some(QuietCopy::Value(value))) => QuietStart::Ready(QuietReady {
                value: Ok(value),
                observation,
                permit,
            }),
            Ok(Some(QuietCopy::EntryPolicy)) => QuietStart::Recheck {
                observation,
                permit,
            },
            Ok(None) => QuietStart::Owned {
                observation,
                permit,
            },
            Err(error) => QuietStart::Ready(QuietReady {
                value: Err(error),
                observation,
                permit,
            }),
        }
    }
}
