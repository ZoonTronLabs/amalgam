//! Immutable control replay payloads with original policy and lifetime.
use super::RecoveryError;
use crate::{
    EntryOptions, Result, Timestamp, advanced::CacheScope, advanced::MarkerKind,
    advanced::MarkerSnapshot, provider::MarkerCommand,
};

/// Cluster authority which must be reacquired by an observation retry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarkerSnapshotParticipation {
    /// The original operation deliberately had no distributed lease.
    Unleased,
    /// A native token and atomic provider fence are mandatory.
    Fenced,
    /// The caller explicitly selected the legacy cooperative contract.
    Cooperative,
}

/// Captured observation repair; this never advances an invalidation fact.
#[derive(Debug, Clone)]
pub struct MarkerSnapshotReplay {
    scope: CacheScope,
    kind: MarkerKind,
    snapshot: MarkerSnapshot,
    options: EntryOptions,
    participation: MarkerSnapshotParticipation,
}

impl MarkerSnapshotReplay {
    pub(crate) fn capture(
        scope: CacheScope,
        kind: MarkerKind,
        snapshot: MarkerSnapshot,
        options: EntryOptions,
        participation: MarkerSnapshotParticipation,
    ) -> Result<Self> {
        options.validate()?;
        if options.skip_distributed_write() {
            return Err(RecoveryError::SnapshotWritesSkipped.into());
        }
        Ok(Self {
            scope,
            kind,
            snapshot,
            options,
            participation,
        })
    }

    /// Validated namespace, distinct from ordinary value keys.
    #[must_use]
    pub fn scope(&self) -> &CacheScope {
        &self.scope
    }
    /// The tag or clear observation being retried.
    #[must_use]
    pub fn kind(&self) -> &MarkerKind {
        &self.kind
    }
    /// Original immutable revision, age and absolute deadlines.
    #[must_use]
    pub const fn snapshot(&self) -> MarkerSnapshot {
        self.snapshot
    }
    /// Original operation policy; current defaults cannot extend its lifetime.
    #[must_use]
    pub fn options(&self) -> &EntryOptions {
        &self.options
    }
    /// Original authority contract, rather than a retained expired lease.
    #[must_use]
    pub const fn participation(&self) -> MarkerSnapshotParticipation {
        self.participation
    }
}

#[derive(Debug, Clone)]
enum MarkerMutationPhase {
    Advance {
        options: EntryOptions,
        snapshot: MarkerSnapshot,
    },
    Populate {
        options: EntryOptions,
        snapshot: MarkerSnapshot,
        additional: Box<[MarkerCommand]>,
    },
    Notify {
        additional: Box<[MarkerCommand]>,
    },
}

/// Immutable captured tag/clear mutation without nullable stage fields.
#[derive(Debug, Clone)]
pub struct MarkerMutationRecovery {
    command: MarkerCommand,
    phase: MarkerMutationPhase,
    // Legitimate independent debt from superseded work; factories cap depth at one.
    compaction: Option<std::sync::Arc<MarkerMutationRecovery>>,
}

/// A borrowed view carries only the fields valid for the remaining stage.
#[derive(Debug, Clone, Copy)]
pub enum MarkerMutationStage<'a> {
    /// Durable advancement, followed by optional observation population.
    Advance {
        /// Original mutation options, including notification/exception policy.
        options: &'a EntryOptions,
        /// Original age/deadlines; its revision is reconciled with actual advancement.
        snapshot: MarkerSnapshot,
    },
    /// Durable advancement succeeded; only observations and publication remain.
    Populate {
        /// Captured policy, including explicit notification exclusion.
        options: &'a EntryOptions,
        /// Original age and deadlines, reconciled with the actual durable revision.
        snapshot: MarkerSnapshot,
        /// Additional committed clear facts produced by bounded-store compaction.
        additional: &'a [MarkerCommand],
    },
    /// Storage succeeded; only these committed facts still need publication.
    Notify {
        /// Additional committed facts, never reconstructed from later provider state.
        additional: &'a [MarkerCommand],
    },
}

impl MarkerMutationRecovery {
    /// An already committed global clear retained across primary-tag supersession.
    /// This bounded child cannot itself contain another inherited obligation.
    #[must_use]
    pub fn pending_compaction(&self) -> Option<&Self> {
        self.compaction.as_deref()
    }

    pub(crate) fn with_compaction(
        &self,
        child: Option<Self>,
    ) -> std::result::Result<Self, RecoveryError> {
        if child.as_ref().is_some_and(|child| {
            child.command.scope() != self.command.scope()
                || *child.command.marker().kind() != MarkerKind::ClearRemove
                || child.compaction.is_some()
                || matches!(child.stage(), MarkerMutationStage::Advance { .. })
        }) {
            return Err(RecoveryError::MarkerIdentityChanged);
        }
        let mut next = self.clone();
        next.compaction = child.map(std::sync::Arc::new);
        Ok(next)
    }

    fn completed_compaction(&self) -> Option<Self> {
        let (command, phase) = match &self.phase {
            MarkerMutationPhase::Advance { .. } => return None,
            MarkerMutationPhase::Populate {
                options,
                snapshot,
                additional,
            } => {
                let command = additional.first()?.clone();
                let phase = MarkerMutationPhase::Populate {
                    options: options.clone(),
                    snapshot: MarkerSnapshot::fresh(
                        command.marker().version(),
                        options,
                        snapshot.created(),
                    ),
                    additional: Box::from([]),
                };
                (command, phase)
            }
            MarkerMutationPhase::Notify { additional } => (
                additional.first()?.clone(),
                MarkerMutationPhase::Notify {
                    additional: Box::from([]),
                },
            ),
        };
        Some(Self {
            command,
            phase,
            compaction: None,
        })
    }

    pub(crate) fn inherit_compaction(
        &self,
        previous: &Self,
    ) -> std::result::Result<Self, RecoveryError> {
        let own = previous.completed_compaction();
        let candidate = own
            .as_ref()
            .into_iter()
            .chain(previous.pending_compaction())
            .chain(self.pending_compaction())
            .max_by_key(|work| {
                (
                    work.command().marker().version(),
                    matches!(work.stage(), MarkerMutationStage::Notify { .. }),
                )
            });
        self.with_compaction(candidate.cloned())
    }
    pub(crate) fn committed_notification(command: MarkerCommand) -> Self {
        Self {
            command,
            phase: MarkerMutationPhase::Notify {
                additional: Box::from([]),
            },
            compaction: None,
        }
    }

    pub(crate) fn capture(
        command: MarkerCommand,
        options: EntryOptions,
        created: Timestamp,
    ) -> Result<Self> {
        options.validate()?;
        if options.skip_distributed_write() {
            return Err(RecoveryError::SnapshotWritesSkipped.into());
        }
        let snapshot = MarkerSnapshot::fresh(command.marker().version(), &options, created);
        Ok(Self {
            command,
            phase: MarkerMutationPhase::Advance { options, snapshot },
            compaction: None,
        })
    }

    /// Exact validated source, scope, kind and original revision.
    #[must_use]
    pub fn command(&self) -> &MarkerCommand {
        &self.command
    }
    /// Remaining stage, with no meaningless options on notification-only work.
    #[must_use]
    pub fn stage(&self) -> MarkerMutationStage<'_> {
        match &self.phase {
            MarkerMutationPhase::Advance { options, snapshot } => MarkerMutationStage::Advance {
                options,
                snapshot: *snapshot,
            },
            MarkerMutationPhase::Populate {
                options,
                snapshot,
                additional,
            } => MarkerMutationStage::Populate {
                options,
                snapshot: *snapshot,
                additional,
            },
            MarkerMutationPhase::Notify { additional } => {
                MarkerMutationStage::Notify { additional }
            }
        }
    }

    pub(crate) fn population(
        &self,
        command: MarkerCommand,
        additional: Box<[MarkerCommand]>,
    ) -> std::result::Result<Self, RecoveryError> {
        if self.command.scope() != command.scope()
            || self.command.marker().kind() != command.marker().kind()
            || self.command.marker().version() > command.marker().version()
        {
            return Err(RecoveryError::MarkerIdentityChanged);
        }
        if additional.len() > 1
            || (!additional.is_empty() && *command.marker().kind() == MarkerKind::ClearRemove)
        {
            return Err(RecoveryError::MarkerIdentityChanged);
        }
        if additional.iter().any(|other| {
            other.scope() != command.scope() || *other.marker().kind() != MarkerKind::ClearRemove
        }) {
            return Err(RecoveryError::MarkerIdentityChanged);
        }
        let MarkerMutationPhase::Advance { options, snapshot } = &self.phase else {
            return Err(RecoveryError::InvalidMarkerStage);
        };
        Ok(Self {
            command: command.clone(),
            phase: MarkerMutationPhase::Populate {
                options: options.clone(),
                snapshot: snapshot.with_maximum(Some(command.marker().version())),
                additional,
            },
            compaction: self.compaction.clone(),
        })
    }

    pub(crate) fn notification(&self) -> std::result::Result<Self, RecoveryError> {
        let MarkerMutationPhase::Populate {
            options,
            additional,
            ..
        } = &self.phase
        else {
            return Err(RecoveryError::InvalidMarkerStage);
        };
        if options.skip_backplane_notifications() {
            return Err(RecoveryError::InvalidMarkerStage);
        }
        Ok(Self {
            command: self.command.clone(),
            phase: MarkerMutationPhase::Notify {
                additional: additional.clone(),
            },
            compaction: self.compaction.clone(),
        })
    }
}
