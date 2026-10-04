use amalgam::*;
use std::sync::{Arc, Weak, mpsc};
use std::time::Duration;

struct ReentrantFence {
    service: Weak<AutoRecoveryService>,
    generation: OperationGeneration,
    dropped: mpsc::Sender<usize>,
}
impl RecoveryFence for ReentrantFence {
    fn generation(&self) -> OperationGeneration {
        self.generation
    }
    fn is_current(&self) -> bool {
        true
    }
}
impl Drop for ReentrantFence {
    fn drop(&mut self) {
        if let Some(service) = self.service.upgrade() {
            let _ = self.dropped.send(service.len());
        }
    }
}
struct QueueReadingGeneration(ReentrantFence);
impl RecoveryFence for QueueReadingGeneration {
    fn generation(&self) -> OperationGeneration {
        let _ = self.0.service.upgrade().unwrap().len();
        self.0.generation
    }
    fn is_current(&self) -> bool {
        true
    }
}
fn work(key: &str, expiry_seconds: u64) -> RecoveryWork {
    RecoveryWork::Data {
        item: RecoveryItem {
            key: Arc::from(key),
            action: RecoveryAction::Remove,
            timestamp: Timestamp::from_ticks(0),
            expires_at: Timestamp::from_ticks(0)
                .saturating_add(Duration::from_secs(expiry_seconds)),
            remaining_retries: None,
        },
        mutation: PendingMutation::Legacy,
    }
}
#[derive(Clone, Copy, Debug)]
enum Retirement {
    Stop,
    Supersede,
    Cancel,
    Replace,
    CapacityEviction,
}
fn fence_retirement(case: Retirement) {
    let service = AutoRecoveryService::try_new(
        RecoveryConfig {
            max_items: Some(1),
            ..RecoveryConfig::default()
        },
        Arc::new(ManualClock::new(Timestamp::from_ticks(0))),
    )
    .unwrap();
    let (dropped, received) = mpsc::channel();
    // The queue alone retains this extension. No test-side Arc masks its Drop.
    service
        .enqueue_versioned(
            work("first", 60),
            Arc::new(ReentrantFence {
                service: Arc::downgrade(&service),
                generation: OperationGeneration::new(1),
                dropped,
            }),
        )
        .unwrap();
    let (finished, done) = mpsc::channel();
    let worker = std::thread::spawn({
        let service = service.clone();
        move || {
            match case {
                Retirement::Stop => service.stop(),
                Retirement::Supersede => {
                    service.supersede_through("first", OperationGeneration::new(1));
                }
                Retirement::Cancel => {
                    service.cancel_through("first", Timestamp::from_ticks(0));
                }
                Retirement::Replace | Retirement::CapacityEviction => {
                    let (dropped, _received) = mpsc::channel();
                    let key = match case {
                        Retirement::Replace => "first",
                        Retirement::CapacityEviction => "second",
                        Retirement::Stop | Retirement::Supersede | Retirement::Cancel => {
                            unreachable!()
                        }
                    };
                    service
                        .enqueue_versioned(
                            work(key, 120),
                            Arc::new(ReentrantFence {
                                service: Arc::downgrade(&service),
                                generation: OperationGeneration::new(2),
                                dropped,
                            }),
                        )
                        .unwrap();
                }
            }
            finished.send(()).unwrap();
        }
    });
    // A failing implementation blocks only the dedicated standard thread, so
    // the regression reports a bounded failure instead of hanging the suite.
    done.recv_timeout(Duration::from_secs(1))
        .expect("external fence callback/destructor deadlocked on the queue mutex");
    worker.join().unwrap();
    let remaining = received.recv_timeout(Duration::from_secs(1)).unwrap();
    let expected = match case {
        Retirement::Stop | Retirement::Supersede | Retirement::Cancel => 0,
        Retirement::Replace | Retirement::CapacityEviction => 1,
    };
    assert_eq!(remaining, expected);
    service.stop();
}
#[test]
fn recovery_stop_drops_external_fences_after_unlocking() {
    fence_retirement(Retirement::Stop);
}
#[test]
fn recovery_supersession_drops_external_fences_after_unlocking() {
    fence_retirement(Retirement::Supersede);
}
#[test]
fn recovery_cancellation_drops_external_fences_after_unlocking() {
    fence_retirement(Retirement::Cancel);
}
#[test]
fn recovery_replacement_drops_external_fences_after_unlocking() {
    fence_retirement(Retirement::Replace);
}
#[test]
fn recovery_capacity_eviction_drops_external_fences_after_unlocking() {
    fence_retirement(Retirement::CapacityEviction);
}

#[test]
fn recovery_reads_extension_generation_outside_queue_mutex() {
    let service = AutoRecoveryService::try_new(
        RecoveryConfig::default(),
        Arc::new(ManualClock::new(Timestamp::from_ticks(0))),
    )
    .unwrap();
    let (finished, done) = mpsc::channel();
    let worker = std::thread::spawn({
        let service = service.clone();
        move || {
            let (dropped, _received) = mpsc::channel();
            let admitted = service.enqueue_versioned(
                work("key", 60),
                Arc::new(QueueReadingGeneration(ReentrantFence {
                    service: Arc::downgrade(&service),
                    generation: OperationGeneration::new(1),
                    dropped,
                })),
            );
            finished.send(admitted.unwrap()).unwrap();
        }
    });
    assert!(matches!(
        done.recv_timeout(Duration::from_secs(1))
            .expect("generation callback ran under the queue mutex"),
        EnqueueOutcome::Queued(_)
    ));
    worker.join().unwrap();
    service.stop();
}
#[test]
fn enabled_recovery_rejects_unrepresentable_reconnect_delay_before_start() {
    let result = AutoRecoveryService::try_new(
        RecoveryConfig {
            delay: Duration::MAX,
            ..RecoveryConfig::default()
        },
        Arc::new(ManualClock::new(Timestamp::from_ticks(0))),
    );
    assert!(matches!(result, Err(RecoveryError::InvalidBarrier)));
}
#[test]
fn disabled_recovery_accepts_unused_delay_without_starting_work() {
    let service = AutoRecoveryService::try_new(
        RecoveryConfig {
            enabled: false,
            delay: Duration::MAX,
            ..RecoveryConfig::default()
        },
        Arc::new(ManualClock::new(Timestamp::from_ticks(0))),
    )
    .unwrap();
    let (dropped, _received) = mpsc::channel();
    assert!(matches!(
        service
            .enqueue_versioned(
                work("key", 60),
                Arc::new(ReentrantFence {
                    service: Arc::downgrade(&service),
                    generation: OperationGeneration::new(1),
                    dropped
                })
            )
            .unwrap(),
        EnqueueOutcome::Disabled
    ));
    assert!(service.is_empty());
}
