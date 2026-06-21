//! Dependency-injection patterns (default features — no cache backend needed).
//!
//! FusionCache leans on .NET's global DI container: you register a (possibly
//! *named*) `IFusionCache` and the container hands the same instance to every
//! consumer. Rust has no global container, and it doesn't need one — ownership
//! and `Arc` give you the same "one shared instance, injected everywhere"
//! guarantee, explicitly and at compile time. This example shows the two shapes
//! that cover essentially every DI use of FusionCache:
//!
//! 1. **Constructor injection of one shared cache.** A service struct
//!    (`UserService`) is *given* its cache when constructed and stores it. A
//!    `Cache` is a cheap `Arc`-backed handle, so cloning it into background
//!    tasks shares the *same* underlying instance — a write through one clone is
//!    visible through another.
//! 2. **Named caches via [`CacheRegistry`].** Where FusionCache resolves caches
//!    by name from the container, [`CacheRegistry`] resolves them by name from a
//!    registry, and a [`DefaultEntryOptionsProvider`] supplies per-key default
//!    options — the Rust counterpart of FusionCache's options provider.
//!
//! ```text
//! cargo run --example di
//! ```

use std::sync::Arc;
use std::time::Duration;

use amalgam::{Cache, CacheRegistry, DefaultEntryOptionsProvider, EntryOptions};

// ---------------------------------------------------------------------------
// Pattern 1 — constructor injection of one shared cache.
// ---------------------------------------------------------------------------

/// An application service that depends on a cache. It does not *construct* the
/// cache; it receives one (this is "injection"), exactly as a FusionCache
/// consumer receives `IFusionCache` from the container. Cloning the held
/// `Cache` is cheap and yields a handle to the *same* instance.
struct UserService {
    cache: Cache<String>,
}

impl UserService {
    /// Inject the dependency. The caller owns the cache's lifetime and decides
    /// who shares it — the composition root, just like a DI container.
    fn new(cache: Cache<String>) -> Self {
        Self { cache }
    }

    /// Reads through the shared cache, populating it on a miss.
    async fn display_name(&self, user_id: &str) -> String {
        let key = format!("user:{user_id}:name");
        self.cache
            .get_or_set(key, {
                let user_id = user_id.to_owned();
                move |ctx| async move { Ok(ctx.value(format!("User #{user_id}"))) }
            })
            .await
            .unwrap_or_else(|_| "<unavailable>".to_owned())
    }
}

// ---------------------------------------------------------------------------
// Pattern 2 — a DefaultEntryOptionsProvider for the registry's caches.
// ---------------------------------------------------------------------------

/// Supplies per-key default options, the Rust analogue of FusionCache's
/// `DefaultEntryOptionsProvider`: short-lived freshness for volatile `session:*`
/// keys, long-lived for `config:*`, and a fallback for everything else.
struct KeyAwareOptionsProvider;

impl DefaultEntryOptionsProvider for KeyAwareOptionsProvider {
    fn options_for(&self, key: &str) -> Option<EntryOptions> {
        if key.starts_with("session:") {
            Some(EntryOptions::new(Duration::from_secs(30)))
        } else if key.starts_with("config:") {
            Some(EntryOptions::new(Duration::from_secs(3600)))
        } else {
            None // fall back to the cache's static default options
        }
    }
}

#[tokio::main]
async fn main() {
    println!("== Pattern 1: one shared cache, injected into a service and cloned into tasks ==");

    // Composition root: build the cache once. This is the single instance every
    // consumer will share — the role a DI container's singleton registration
    // plays in FusionCache.
    let cache: Cache<String> = Cache::builder().build();

    // Inject the SAME instance into a service (held by value) and keep a handle
    // here. `UserService` is wrapped in `Arc` to model a shared, injected
    // service that can be handed to many call sites / tasks.
    let service = Arc::new(UserService::new(cache.clone()));

    // Drive the service: a miss populates the cache …
    let name = service.display_name("42").await;
    println!("service.display_name(\"42\") -> {name}");

    // Spawn background tasks that each clone the shared service handle (and thus
    // the same cache). They read the value the service just cached — proving the
    // instance is shared, not copied.
    let mut handles = Vec::with_capacity(3);
    for task_id in 0..3 {
        let service = Arc::clone(&service);
        handles.push(tokio::spawn(async move {
            // No factory work should be needed: the entry is already present
            // because every clone points at the one underlying cache.
            let seen = service.display_name("42").await;
            println!("  task {task_id} read the SAME shared cache -> {seen}");
            seen
        }));
    }
    for handle in handles {
        let _ = handle.await;
    }

    // Write through the original handle, read through the service's clone:
    // same instance ⇒ the write is visible.
    cache
        .set(
            "user:42:name".to_owned(),
            "Renamed via another handle".to_owned(),
        )
        .await;
    let after = service.display_name("42").await;
    println!("after writing via a separate clone, service sees -> {after}");
    assert_eq!(
        after, "Renamed via another handle",
        "all clones share one instance"
    );
    println!("OK: every clone observed the same instance.\n");

    println!("== Pattern 2: a registry of independent NAMED caches + options provider ==");

    // The registry is the named-cache resolver: register once, resolve by name
    // anywhere. Shared via `Arc`, mirroring an injected singleton resolver.
    let registry: Arc<CacheRegistry<String>> = Arc::new(CacheRegistry::new());

    let options_provider: Arc<dyn DefaultEntryOptionsProvider> = Arc::new(KeyAwareOptionsProvider);

    // Two independent named caches. Each gets the same key-aware options
    // provider but its own isolated storage.
    registry.register(
        "sessions",
        Cache::builder()
            .default_options_provider(Arc::clone(&options_provider))
            .build(),
    );
    registry.register(
        "config",
        Cache::builder()
            .default_options_provider(Arc::clone(&options_provider))
            .build(),
    );
    println!(
        "registered {} named caches: \"sessions\", \"config\"",
        registry.len()
    );

    // Resolve named caches by name (the FusionCache "keyed service" move).
    let sessions = registry.get("sessions").expect("sessions cache registered");
    let config = registry.get("config").expect("config cache registered");

    // Same logical key in BOTH caches → independent values, proving isolation.
    let token = sessions
        .get_or_set("session:abc", |ctx| async move {
            Ok(ctx.value("session-token-for-abc".to_owned()))
        })
        .await
        .expect("factory is infallible");
    println!("sessions[\"session:abc\"] -> {token}");

    // The `config` cache has never seen `session:abc`. `try_get` is a pure read
    // (no factory): it returns `MaybeValue::none` here, demonstrating the two
    // named caches do not share storage.
    let config_view = config.try_get("session:abc", None).await;
    println!(
        "config[\"session:abc\"] present? {} (independent storage)",
        config_view.has_value()
    );
    assert!(
        !config_view.has_value(),
        "named caches must not share storage"
    );

    // Store something in `config` under a `config:*` key; the options provider
    // hands it the long (1 h) freshness window for that prefix.
    let setting = config
        .get_or_set("config:feature_x", |ctx| async move {
            Ok(ctx.value("enabled".to_owned()))
        })
        .await
        .expect("factory is infallible");
    println!("config[\"config:feature_x\"] -> {setting} (1 h freshness via options provider)");

    println!("OK: named caches are independent; the options provider drove per-key defaults.");
}
