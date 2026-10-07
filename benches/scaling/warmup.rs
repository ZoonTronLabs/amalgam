//! Untimed settling, shared in policy with the .NET reference fixture.
use std::time::{Duration, Instant};

pub const BATCH: usize = 20_000;
const MINIMUM: Duration = Duration::from_secs(3);
const MAXIMUM: Duration = Duration::from_secs(15);
const WINDOW: Duration = Duration::from_millis(100);
const SAMPLES: usize = 5;

pub struct Warmup {
    label: String,
    started: Instant,
    window_time: Duration,
    window_operations: usize,
    operations: usize,
    windows: Vec<f64>,
}

impl Warmup {
    pub fn new(label: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            started: Instant::now(),
            window_time: Duration::ZERO,
            window_operations: 0,
            operations: 0,
            windows: Vec::with_capacity(150),
        }
    }

    // Only operation time contributes to a window; cold-cache setup/cleanup
    // is excluded. Wall time still bounds the total warmup and is recorded.
    pub fn record(&mut self, elapsed: Duration, operations: usize) -> bool {
        self.operations += operations;
        self.window_operations += operations;
        self.window_time += elapsed;
        if self.window_time >= WINDOW {
            self.windows
                .push(self.window_time.as_nanos() as f64 / self.window_operations as f64);
            self.window_time = Duration::ZERO;
            self.window_operations = 0;
        }
        let elapsed = self.started.elapsed();
        let stable = self.windows.len() >= SAMPLES && {
            let last = &self.windows[self.windows.len() - SAMPLES..];
            let min = last.iter().copied().fold(f64::INFINITY, f64::min);
            let max = last.iter().copied().fold(f64::NEG_INFINITY, f64::max);
            max / min <= 1.10
        };
        if elapsed >= MINIMUM && stable || elapsed >= MAXIMUM {
            eprintln!(
                "warmup {}",
                serde_json::json!({
                    "label": self.label, "seconds": elapsed.as_secs_f64(),
                    "operations": self.operations, "stable": stable,
                    "windows_ns": self.windows,
                })
            );
            true
        } else {
            false
        }
    }
}
