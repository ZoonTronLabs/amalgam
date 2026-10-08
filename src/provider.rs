//! Provider interfaces and implementations for optional cache infrastructure.
//!
//! Storage, serialization, backplanes, distributed/local locking and injected
//! time remain extensible behavior. Their typed capabilities and outcomes live
//! beside the corresponding interfaces. Import these when configuring a provider.
pub use crate::backplane::{
    Backplane, BackplaneAction, BackplaneCommand, BackplaneMessage, BackplaneState,
    ContinuityEpoch, InProcessBackplane, MarkerCommand,
};
pub use crate::distributed::{
    AsyncDistributedSerializer, DistributedBytes, DistributedCache, DistributedEntry,
    DistributedSerializer, DistributedSnapshot, FencedWriteSupport, ImmediateRead,
    InMemoryDistributedCache, InMemoryInvalidationStore, InvalidationStore, JsonSerializer,
    LeasedMutation, LeasedWriteOutcome, MarkedRead, MarkerReadError, ReadCompletion,
    SerializationMode, SnapshotRetention,
};
pub use crate::distributed_lock::{
    AcquisitionPolicy, DistributedLease, DistributedLocker, InMemoryDistributedLocker, LeaseError,
    LeaseProof, LeaseReceipt, LeaseState, LeaseSupport, LeaseTask, LeaseTaskOwner, LeaseToken,
    LeaseTtl, OwnershipCheck, RenewalOutcome, TokenAcquisition, acquire_owned,
    acquire_owned_supervised,
};
pub use crate::entry::Entry;
pub use crate::locking::{KeyGuard, KeyedLock};
pub use crate::memory::{
    CapacityRejection, MemoryAdmission, MemoryExpiry, MemoryLimits, MemoryStore, MemoryUsage,
};
pub use crate::memory_locker::{
    BlockingMemoryLocker, MemoryLock, MemoryLockGuard, MemoryLockKind, MemoryLockOutcome,
    MemoryLockRequest, MemoryLocker, MemoryLockerContext, MemoryLockerError,
};
pub use crate::memory_storage::{
    MemoryCondition, MemoryGeneration, MemoryInvalidationFailure, MemoryNamespace,
    MemoryNamespacePurpose, MemoryRecord, MemoryRecordViolation, MemoryRetirement, MemoryStorage,
    MemoryStorageEpoch, MemoryStorageError, MemoryStorageWrite,
};
pub use crate::options::{JitterSample, JitterSource, RandomJitterSource};
pub use crate::recovery::RecoveryExecutor;
pub use crate::registry::DefaultEntryOptionsProvider;
#[cfg(feature = "messagepack")]
pub use crate::serializers::MessagePackSerializer;
#[cfg(feature = "postcard")]
pub use crate::serializers::PostcardSerializer;
pub use crate::serializers::{ImmutableValue, ValueCloner};
pub use crate::time::{Clock, ClockTiming, ManualClock, SystemClock};
#[cfg(feature = "redis")]
pub use {
    crate::redis_backend::RedisBackplane, crate::redis_backend::RedisBackplaneStats,
    crate::redis_backend::RedisClientId, crate::redis_backend::RedisDistributedCache,
    crate::redis_backend::RedisDistributedLocker, crate::redis_backend::RedisInvalidationStore,
    crate::redis_backend::RedisIoOptions,
};
