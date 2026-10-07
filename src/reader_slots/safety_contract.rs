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
    *slots.write() = Box::new(9);
    assert_eq!(**slots.read(), 9);
}

#[test]
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
