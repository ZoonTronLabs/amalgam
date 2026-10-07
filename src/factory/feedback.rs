//! Adaptive metadata shares the existing origin execution allocation.
//! Plain factories never initialize or allocate a feedback container.
use super::{
    EntryOptions, FactoryCancellation, FactoryOutput, FactoryProduct, StaleInfo, Tag, TagError,
    Timestamp,
};
use std::sync::OnceLock;

#[derive(Debug)]
pub(crate) enum ProducedMetadata {
    Modified {
        etag: Option<String>,
        last_modified: Option<Timestamp>,
    },
    NotModified {
        etag: Option<String>,
        last_modified: Option<Timestamp>,
    },
}
#[derive(Debug)]
pub(crate) struct FactoryAdaptation {
    pub(crate) options: EntryOptions,
    pub(crate) tags: Result<Box<[Tag]>, TagError>,
    pub(crate) metadata: ProducedMetadata,
}
type AdaptationCell = Box<parking_lot::Mutex<Option<FactoryAdaptation>>>;
#[derive(Debug, Default)]
pub(crate) struct FactoryFeedback(OnceLock<AdaptationCell>);
impl FactoryFeedback {
    pub(crate) fn publish(&self, adaptation: FactoryAdaptation) {
        let cell = self
            .0
            .get_or_init(|| Box::new(parking_lot::Mutex::new(None)));
        let previous = cell.lock().replace(adaptation);
        // The lock's temporary guard ended before metadata retirement.
        drop(previous);
    }
    pub(crate) fn take(&self) -> Option<FactoryAdaptation> {
        self.0.get().and_then(|cell| cell.lock().take())
    }
}
pub(crate) struct FactoryCompletion {
    options: EntryOptions,
    tags: Box<[Tag]>,
    cancellation: FactoryCancellation,
}
impl FactoryCompletion {
    pub(crate) fn new(
        options: EntryOptions,
        tags: Box<[Tag]>,
        cancellation: FactoryCancellation,
    ) -> Self {
        Self {
            options,
            tags,
            cancellation,
        }
    }
    pub(crate) fn complete<V>(self, value: V) -> FactoryProduct<V> {
        let adaptation = self
            .cancellation
            .take_factory_adaptation()
            .unwrap_or(FactoryAdaptation {
                options: self.options,
                tags: Ok(self.tags),
                metadata: ProducedMetadata::Modified {
                    etag: None,
                    last_modified: None,
                },
            });
        let output = match adaptation.metadata {
            ProducedMetadata::Modified {
                etag,
                last_modified,
            } => FactoryOutput::Modified {
                value,
                etag,
                last_modified,
                tags: adaptation.tags,
            },
            ProducedMetadata::NotModified {
                etag,
                last_modified,
            } => FactoryOutput::NotModified {
                stale: StaleInfo {
                    value,
                    etag,
                    last_modified,
                    tags: Box::from([]),
                },
                tags: adaptation.tags,
            },
        };
        FactoryProduct {
            output,
            options: adaptation.options,
        }
    }
}
