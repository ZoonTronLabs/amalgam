//! Small actual-guard contracts suitable for Miri and TSan.
use super::*;
use std::sync::Arc;

#[test]
fn overlapping_guards_and_failed_try_write_preserve_borrowed_values() {
    let slots = ReaderSlots::with_slots(Box::new(7), 1);
    let first = slots.read();
    let second = slots.read();
    assert!(slots.try_write().is_none());
    assert_eq!(**first, 7);
    assert_eq!(**second, 7);
    drop(first);
    drop(second);
    let replacement = Box::new(9);
    *slots.write() = replacement;
    assert_eq!(**slots.read(), 9);
}

// Native TSan exercises this parking stress. Miri's stricter C-variadic
// shim rejects the released dependency's Linux futex ABI (upstream #539).
// Do not disable its UB/alias/race checks. Miri still checks actual guards,
// overlapping native readers and rejected/admitted writers below.
#[test]
#[cfg_attr(
    miri,
    ignore = "parking_lot_core 0.9.12 futex ABI; upstream parking_lot#539; native TSan covers parking"
)]
fn colliding_readers_and_writers_use_the_real_unsafe_cell() {
    let slots = Arc::new(ReaderSlots::with_slots((0, 0), 1));
    std::thread::scope(|scope| {
        for id in 0..4 {
            let slots = &slots;
            scope.spawn(move || {
                for _ in 0..4 {
                    if id % 2 == 0 {
                        let value = slots.read();
                        assert_eq!(value.0, value.1);
                        std::thread::yield_now();
                        assert_eq!(value.0, value.1);
                    } else {
                        let mut value = slots.write();
                        value.0 += 1;
                        std::thread::yield_now();
                        value.1 += 1;
                    }
                }
            });
        }
    });
    assert_eq!(*slots.read(), (8, 8));
}

#[test]
fn colliding_native_read_guards_prevent_exclusive_value_access() {
    let slots = ReaderSlots::with_slots(Box::new(7), 1);
    std::thread::scope(|scope| {
        let (entered, entered_rx) = std::sync::mpsc::channel();
        let mut releases = Vec::with_capacity(2);
        for _ in 0..2 {
            let slots = &slots;
            let entered = entered.clone();
            let (release, release_rx) = std::sync::mpsc::channel();
            releases.push(release);
            scope.spawn(move || {
                let value = slots.read();
                assert_eq!(**value, 7);
                entered.send(()).unwrap();
                release_rx.recv().unwrap();
                assert_eq!(**value, 7);
            });
            // Serialize first-use parking metadata; the read guards overlap.
            // Neither reader attempts admission under a closed writer gate.
            entered_rx.recv().unwrap();
        }
        assert!(slots.try_write().is_none());
        for release in releases {
            release.send(()).unwrap();
        }
    });
    let replacement = Box::new(9);
    *slots.write() = replacement;
    assert_eq!(**slots.read(), 9);
}
