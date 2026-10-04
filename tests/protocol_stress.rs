//! Seeded, reproducible wire/property checks; no external service required.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use amalgam::{
    BackplaneAction, BackplaneCommand, BackplaneMessage, CacheScope, DistributedEntry,
    DistributedSerializer, DistributedSnapshot, EntryWeight, InMemoryInvalidationStore,
    InvalidationStore, JsonSerializer, KeyModifierMode, MarkerCommand, MarkerKind,
    MarkerStoreLimits, MarkerVersion, Priority, SnapshotRetention, StoredMarker, Tag, Timestamp,
};

struct Sequence(u64);

impl Sequence {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        self.0
    }

    fn bytes(&mut self, length: usize) -> Vec<u8> {
        (0..length).map(|_| self.next().to_be_bytes()[0]).collect()
    }
}

type Codec = (&'static str, Arc<dyn DistributedSerializer<Vec<u8>>>);

fn codecs() -> Vec<Codec> {
    vec![
        ("json", Arc::new(JsonSerializer)),
        #[cfg(feature = "messagepack")]
        ("messagepack", Arc::new(amalgam::MessagePackSerializer)),
        #[cfg(feature = "postcard")]
        ("postcard", Arc::new(amalgam::PostcardSerializer)),
    ]
}

fn assert_snapshot_same(left: &DistributedSnapshot<Vec<u8>>, right: &DistributedSnapshot<Vec<u8>>) {
    assert_eq!(left.inserted_at(), right.inserted_at());
    assert_eq!(left.retention(), right.retention());
    assert_eq!(
        serde_json::to_value(left.entry()).expect("serializable fixture"),
        serde_json::to_value(right.entry()).expect("serializable fixture")
    );
}

#[test]
fn seeded_roundtrips_preserve_metadata_retention_and_remaining_lifetime() {
    let mut sequence = Sequence(0x1d2b_9e31_102a_00ff);
    let codecs = codecs();
    for iteration in 0..1024 {
        let inserted = Timestamp::from_ticks((sequence.next() as i64) / 4);
        let logical = inserted.saturating_add(Duration::from_millis(sequence.next() % 10_000));
        let physical = logical.saturating_add(Duration::from_secs(sequence.next() % 10_000));
        let length = (sequence.next() % 2049) as usize;
        let entry = DistributedEntry {
            value: sequence.bytes(length),
            created_ticks: inserted.ticks().saturating_sub(370_000_000),
            logical_expiration_ticks: logical.ticks(),
            physical_expiration_ticks: physical.ticks(),
            is_from_fail_safe: iteration % 2 == 0,
            etag: (iteration % 3 == 0).then(|| format!("\"Қазақша|etag:{iteration}\"")),
            last_modified_ticks: (iteration % 5 == 0).then_some(i64::MIN),
            tags: vec![format!("tag:{iteration}"), "Қазақша|\u{1f}".into()],
        };
        let retention = match iteration % 5 {
            0 => SnapshotRetention::Unspecified,
            1 => SnapshotRetention::Specified {
                size: None,
                priority: Priority::Low,
            },
            2 => SnapshotRetention::Specified {
                size: Some(EntryWeight::new(0)),
                priority: Priority::Normal,
            },
            3 => SnapshotRetention::Specified {
                size: Some(EntryWeight::new(u64::MAX)),
                priority: Priority::High,
            },
            _ => SnapshotRetention::Specified {
                size: Some(EntryWeight::new(1)),
                priority: Priority::NeverRemove,
            },
        };
        let snapshot =
            DistributedSnapshot::new(entry, inserted, retention).expect("valid snapshot");
        for (name, codec) in &codecs {
            let bytes = codec
                .serialize_snapshot(&snapshot)
                .expect("encode snapshot");
            let decoded = codec
                .deserialize_snapshot(&bytes)
                .unwrap_or_else(|error| panic!("{name}: {error}"));
            assert_snapshot_same(&snapshot, &decoded);
            let now = inserted.saturating_add(Duration::from_secs(1));
            assert_eq!(
                decoded.backend_ttl_at(now),
                physical.saturating_duration_since(now)
            );
            assert_eq!(decoded.backend_ttl_at(physical), Duration::ZERO);
            assert_eq!(decoded.backend_ttl_at(Timestamp::MAX), Duration::ZERO);

            let legacy = codec.serialize(snapshot.entry()).expect("encode legacy");
            let legacy = codec.deserialize_snapshot(&legacy).expect("read legacy");
            assert_eq!(legacy.retention(), SnapshotRetention::Unspecified);
            assert_eq!(legacy.entry().value, snapshot.entry().value);
        }
    }
}

#[test]
fn malformed_wire_never_panics_or_returns_an_invalid_snapshot() {
    let mut sequence = Sequence(0x9e10_711a_d733_8f00);
    let codecs = codecs();
    for iteration in 0..8192 {
        let length = (sequence.next() % 129) as usize;
        let mut bytes = sequence.bytes(length);
        if iteration % 3 == 0 {
            let mut framed = b"AMALGAM\0".to_vec();
            framed.extend(bytes);
            bytes = framed;
        }
        for (_, codec) in &codecs {
            if let Ok(snapshot) = codec.deserialize_snapshot(&bytes) {
                snapshot
                    .entry()
                    .validate()
                    .expect("decoded metadata must be valid");
            }
        }
    }
    for (_, codec) in &codecs {
        for version in [0, 1, 3, 255] {
            let mut bytes = b"AMALGAM\0".to_vec();
            bytes.extend([version, 255, 255, 255, 255]);
            assert!(codec.deserialize_snapshot(&bytes).is_err());
        }
        let mut huge_header = b"AMALGAM\0".to_vec();
        huge_header.extend([2, 255, 255, 255, 255]);
        assert!(codec.deserialize_snapshot(&huge_header).is_err());
    }
}

#[test]
fn seeded_notifications_preserve_data_control_separation_and_namespace() {
    let hostile = [
        "normal",
        "|",
        "Қазақша|🌍",
        "\u{1f}amalgam-control-v2:00",
        "__amalgam_clear_remove__",
        "\u{1f}amalgam/v2/markers",
        "v2:tenant:tag|clear",
    ];
    for iteration in 0..1024 {
        let source = format!("{}:{iteration}", hostile[iteration % hostile.len()]);
        let key = format!(
            "{}:{iteration}",
            hostile[(iteration / hostile.len()) % hostile.len()]
        );
        let timestamp = Timestamp::from_ticks([i64::MIN, -1, 0, 1, i64::MAX][iteration % 5]);
        let action = [
            BackplaneAction::Set,
            BackplaneAction::Remove,
            BackplaneAction::Expire,
        ][iteration % 3];
        let envelope = BackplaneCommand::Data(BackplaneMessage {
            source_id: source.clone().into(),
            timestamp,
            action,
            key: key.clone().into(),
        })
        .into_message()
        .expect("data frame");
        match BackplaneCommand::from_message(envelope).expect("data decode") {
            BackplaneCommand::Data(message) => {
                assert_eq!(message.source_id.as_ref(), source);
                assert_eq!(message.key.as_ref(), key);
                assert_eq!(message.timestamp, timestamp);
                assert_eq!(message.action, action);
            }
            BackplaneCommand::Marker(_) => panic!("ordinary data became control"),
        }
        let scope = CacheScope::new(
            &key,
            &source,
            [
                KeyModifierMode::Prefix,
                KeyModifierMode::Suffix,
                KeyModifierMode::None,
            ][iteration % 3],
        )
        .expect("valid namespace");
        let kind = match iteration % 3 {
            0 => MarkerKind::Tag(Tag::new(&key).expect("tag")),
            1 => MarkerKind::ClearExpire,
            _ => MarkerKind::ClearRemove,
        };
        let marker = StoredMarker::new(kind.clone(), MarkerVersion::new(timestamp));
        let envelope = BackplaneCommand::Marker(
            MarkerCommand::new(&source, scope.clone(), marker).expect("marker"),
        )
        .into_message()
        .expect("marker frame");
        match BackplaneCommand::from_message(envelope.clone()).expect("marker decode") {
            BackplaneCommand::Marker(command) => {
                assert_eq!(command.source_id(), source);
                assert_eq!(command.scope(), &scope);
                assert_eq!(command.marker().kind(), &kind);
                assert_eq!(command.marker().version().timestamp(), timestamp);
            }
            BackplaneCommand::Data(_) => panic!("control became data"),
        }
        let mut inconsistent = envelope;
        inconsistent.timestamp = if timestamp == Timestamp::MAX {
            Timestamp::MIN
        } else {
            Timestamp::MAX
        };
        assert!(BackplaneCommand::from_message(inconsistent).is_err());
    }
}

#[tokio::test]
async fn repeated_compaction_preserves_every_prior_tombstone_across_signed_revisions() {
    let store = InMemoryInvalidationStore::new(MarkerStoreLimits::new(8, 2).expect("limits"));
    let scope = CacheScope::new("tenant:", "v2", KeyModifierMode::Prefix).expect("scope");
    let mut model = HashMap::<Tag, MarkerVersion>::with_capacity(1024);
    let mut sequence = Sequence(0xbad1_0228_e731_1234);
    for iteration in 0..1024 {
        let tag = Tag::new(format!("tag:{}", sequence.next() % 512)).expect("tag");
        let candidate = MarkerVersion::new(Timestamp::from_ticks(sequence.next() as i64));
        model
            .entry(tag.clone())
            .and_modify(|old| *old = (*old).max(candidate))
            .or_insert(candidate);
        store
            .advance(&scope, MarkerKind::Tag(tag), candidate)
            .await
            .expect("advance");
        if iteration % 16 == 0 {
            let clear = store
                .read(&scope, &MarkerKind::ClearRemove)
                .await
                .expect("clear");
            for (tag, expected) in &model {
                let retained = store
                    .read(&scope, &MarkerKind::Tag(tag.clone()))
                    .await
                    .expect("tag read");
                assert!(
                    retained
                        .into_iter()
                        .chain(clear)
                        .any(|revision| revision >= *expected),
                    "prior tombstone vanished"
                );
            }
        }
    }
    let other = CacheScope::new("other:", "v2", KeyModifierMode::Prefix).expect("other scope");
    assert!(
        store
            .read(&other, &MarkerKind::ClearRemove)
            .await
            .expect("isolated read")
            .is_none()
    );
    assert_eq!(store.scope_count(), 1);
}
