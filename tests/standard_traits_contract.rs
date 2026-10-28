//! Standard conversions preserve existing validation and ownership contracts.

#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

use std::io::Error as IoError;
use std::ptr;
use std::sync::Arc;
use std::time::Duration;

use amalgam::backplane::{BackplaneCommand, MarkerCommand};
use amalgam::error::SharedSource;
use amalgam::options::KeyModifierMode;
use amalgam::tags::{CacheScope, MarkerError, MarkerKind, MarkerVersion, StoredMarker};
use amalgam::{Tag, TagError, Timeout, Timestamp};

#[test]
fn tag_parsing_preserves_spelling_and_rejects_blank_input() {
    let spelling = "  cache-tag  ";
    let parsed: Tag = spelling.parse().expect("a nonblank tag should parse");
    assert_eq!(
        parsed,
        Tag::new(spelling).unwrap(),
        "FromStr must preserve the validating factory's original spelling"
    );
    let rejected: Result<Tag, TagError> = " \n ".parse();
    assert_eq!(
        rejected,
        Err(TagError::Blank),
        "FromStr must reject whitespace-only tags with the existing typed error"
    );
}

#[test]
fn timeout_conversion_preserves_infinite_and_immediate_modes() {
    for duration in [None, Some(Duration::ZERO), Some(Duration::from_secs(3))] {
        assert_eq!(
            Timeout::from(duration),
            Timeout::from_option(duration),
            "From must preserve the existing timeout conversion for {duration:?}"
        );
    }
    assert_eq!(
        Timeout::from(None),
        Timeout::Infinite,
        "an absent duration must remain an infinite timeout"
    );
    assert_eq!(
        Timeout::from(Some(Duration::ZERO)),
        Timeout::After(Duration::ZERO),
        "a finite zero duration must remain immediate rather than infinite"
    );
}

#[test]
fn shared_source_conversion_preserves_the_original_error_allocation() {
    let original = Arc::new(IoError::other("original provider failure"));
    let shared: SharedSource<IoError> = Arc::clone(&original).into();
    assert!(
        ptr::eq(shared.as_ref(), original.as_ref()),
        "From must retain the original concrete error allocation"
    );
    assert_eq!(
        shared.to_string(),
        original.to_string(),
        "the shared wrapper must display the unchanged original cause"
    );
}

#[test]
fn backplane_conversion_preserves_marker_validation() {
    let version = MarkerVersion::new(Timestamp::from_ticks(7));
    let scope = CacheScope::new("style:", "v2", KeyModifierMode::Prefix).unwrap();
    let marker = StoredMarker::new(MarkerKind::ClearRemove, version);
    let command =
        BackplaneCommand::Marker(MarkerCommand::new("style-test", scope, marker).unwrap());
    let message = command.clone().into_message().unwrap();
    let decoded = BackplaneCommand::try_from(message.clone()).unwrap();
    assert_eq!(
        decoded, command,
        "TryFrom must preserve a valid marker command"
    );

    let mut invalid = message;
    invalid.timestamp = Timestamp::from_ticks(8);
    assert!(
        matches!(
            BackplaneCommand::from_message(invalid.clone()),
            Err(MarkerError::Protocol { .. })
        ),
        "the original decoder must reject a marker envelope with a mismatched revision"
    );
    assert!(
        matches!(
            BackplaneCommand::try_from(invalid),
            Err(MarkerError::Protocol { .. })
        ),
        "TryFrom must retain the decoder's typed protocol rejection"
    );
}
