//! `plugins` — observe the cache's lifecycle with a `Plugin`.
//!
//! A [`Plugin`] is the synchronous observer seam: it is notified of every
//! [`CacheEvent`] on the cache's (cheap, non-blocking) event path. This is where
//! you'd wire metrics, structured logging, or tracing. Plugins must not block —
//! here we just print each event.
//!
//! We attach the plugin with `CacheBuilder::plugin(Arc::new(..))` and then run a
//! handful of operations (set, hit, miss, remove) so the plugin prints a small
//! event log.
//!
//! Run it with:
//!
//! ```text
//! cargo run --example plugins
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use amalgam::{Cache, CacheEvent, Plugin};

/// A tiny plugin that prints every event and counts how many it saw.
struct LoggingPlugin {
    seen: AtomicUsize,
}

impl Plugin for LoggingPlugin {
    fn name(&self) -> &str {
        "logging-plugin"
    }

    fn on_start(&self) {
        println!("[{}] attached and listening", self.name());
    }

    fn on_event(&self, event: &CacheEvent) {
        let n = self.seen.fetch_add(1, Ordering::SeqCst) + 1;
        println!("[{}] event #{n}: {event:?}", self.name());
    }
}

#[tokio::main]
async fn main() {
    let plugin = Arc::new(LoggingPlugin {
        seen: AtomicUsize::new(0),
    });

    // Register the plugin at build time. `on_start` fires immediately.
    let cache: Cache<i32> = Cache::builder().plugin(plugin.clone()).build();

    println!("\n-- set(\"answer\", 42) --");
    cache.set("answer", 42).await;

    println!("\n-- get_or_set(\"answer\", ..) => cache HIT, factory skipped --");
    let hit = cache
        .get_or_set("answer", |ctx| async move { Ok(ctx.value(0)) })
        .await
        .expect("served from cache");
    println!("   value = {hit}");

    println!("\n-- get_or_set(\"fresh\", ..) => MISS then factory runs --");
    let produced = cache
        .get_or_set("fresh", |ctx| async move { Ok(ctx.value(7)) })
        .await
        .expect("factory produces a value");
    println!("   value = {produced}");

    println!("\n-- remove(\"answer\") --");
    cache.remove("answer").await;

    // The event stream is best-effort and fanned out synchronously, so by now the
    // plugin has already seen everything above.
    let total = plugin.seen.load(Ordering::SeqCst);
    println!("\nplugin observed {total} event(s) in total.");
    assert!(total > 0, "the plugin should have observed several events");
    println!("OK: plugin received the cache's event stream.");
}
