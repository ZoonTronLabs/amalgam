//! Completed marker publication must become visible to every subsequent read.
use std::sync::{Arc, Barrier, mpsc};
use std::time::Duration;

use amalgam::tags::{MarkerKind, MarkerVersion, TagRegistry, TagVerdict};
use amalgam::{RemoveByTagBehavior, Tag, Timestamp};

#[test]
fn first_marker_is_visible_even_at_the_smallest_timestamp() {
    let at = Timestamp::from_ticks(i64::MIN);
    let tag = Tag::new("first").unwrap();
    for kind in [
        MarkerKind::Tag(tag.clone()),
        MarkerKind::ClearExpire,
        MarkerKind::ClearRemove,
    ] {
        let registry = TagRegistry::new();
        assert_eq!(
            registry.evaluate(at, std::slice::from_ref(&tag), RemoveByTagBehavior::Expire),
            TagVerdict::Valid
        );
        let expected = match &kind {
            MarkerKind::Tag(_) | MarkerKind::ClearExpire => TagVerdict::Expire,
            MarkerKind::ClearRemove => TagVerdict::Remove,
        };
        registry.advance(kind, MarkerVersion::new(at));
        assert_eq!(
            registry.evaluate(at, std::slice::from_ref(&tag), RemoveByTagBehavior::Expire),
            expected
        );
    }
}

#[test]
fn compacted_empty_tag_map_keeps_its_global_remove_fence() {
    let registry = TagRegistry::with_capacity(1).unwrap();
    let at = Timestamp::from_ticks(42);
    registry.mark_tag(Tag::new("a").unwrap(), at);
    registry.mark_tag(Tag::new("b").unwrap(), at);
    assert!(registry.is_empty());
    assert_eq!(
        registry.evaluate(at, &[], RemoveByTagBehavior::Expire),
        TagVerdict::Remove
    );
    assert_eq!(
        registry.evaluate(Timestamp::from_ticks(43), &[], RemoveByTagBehavior::Expire),
        TagVerdict::Valid
    );
}

#[test]
fn completed_clear_advances_are_visible_to_concurrent_readers() {
    let (done, wait) = mpsc::channel();
    std::thread::spawn(move || {
        let registry = TagRegistry::new();
        let barrier = Arc::new(Barrier::new(5));
        std::thread::scope(|scope| {
            for _ in 0..4 {
                let registry = &registry;
                let barrier = &barrier;
                scope.spawn(move || {
                    for revision in 0..1_000 {
                        barrier.wait();
                        assert_eq!(
                            registry.evaluate(
                                Timestamp::from_ticks(revision),
                                &[],
                                RemoveByTagBehavior::Expire,
                            ),
                            TagVerdict::Remove
                        );
                        barrier.wait();
                    }
                });
            }
            for revision in 0..1_000 {
                registry.mark_clear_remove(Timestamp::from_ticks(revision));
                barrier.wait();
                barrier.wait();
            }
        });
        done.send(()).unwrap();
    });
    wait.recv_timeout(Duration::from_secs(3))
        .expect("completed marker publication must remain visible under contention");
}
