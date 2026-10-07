//! Exercise the production ReaderSlots algorithm, not a duplicated model.
use super::*;
use loom::sync::Arc;

fn check(model: impl Fn() + Send + Sync + 'static) {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(3);
    builder.max_permutations = Some(20_000);
    builder.check(model);
}

#[test]
fn first_use_collisions_and_competing_writers_never_overlap_value_access() {
    check(|| {
        let slots = Arc::new(ReaderSlots::with_slots((0, 0), 1));
        let threads: Vec<_> = (0..3)
            .map(|id| {
                let slots = slots.clone();
                loom::thread::spawn(move || {
                    if id == 0 {
                        let value = slots.read();
                        assert_eq!(value.0, value.1);
                        loom::thread::yield_now();
                        assert_eq!(value.0, value.1);
                    } else if let Some(mut value) = slots.try_write() {
                        value.0 += 1;
                        loom::thread::yield_now();
                        value.1 += 1;
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
        let value = slots.read();
        assert_eq!(value.0, value.1);
    });
}

#[test]
fn colliding_readers_publish_their_slot_before_exclusive_access() {
    check(|| {
        let slots = Arc::new(ReaderSlots::with_slots(0, 1));
        let readers: Vec<_> = (0..2)
            .map(|_| {
                let slots = slots.clone();
                loom::thread::spawn(move || {
                    let value = slots.read();
                    assert!(*value <= 1);
                    loom::thread::yield_now();
                    assert!(*value <= 1);
                })
            })
            .collect();
        if let Some(mut value) = slots.try_write() {
            *value = 1;
        }
        for reader in readers {
            reader.join().unwrap();
        }
    });
}

#[test]
fn opening_writer_gate_does_not_lose_a_parked_reader() {
    check(|| {
        let slots = Arc::new(ReaderSlots::with_slots(0, 1));
        let hold = slots.write();
        let reader = slots.clone();
        let reader = loom::thread::spawn(move || assert_eq!(*reader.read(), 0));
        loom::thread::yield_now();
        drop(hold);
        reader.join().unwrap();
    });
}

#[test]
fn rejected_writer_and_nested_read_release_actual_reservations() {
    check(|| {
        let slots = ReaderSlots::new(7);
        let first = slots.read();
        let second = slots.read();
        assert!(slots.try_write().is_none());
        assert_eq!(*second, 7);
        drop(first);
        drop(second);
        *slots.try_write().unwrap() = 9;
        assert_eq!(*slots.read(), 9);
    });
}
