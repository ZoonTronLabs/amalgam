//! Redis-backed L1 + L2 + backplane + distributed-locker example.
//!
//! Needs a running Redis and the `redis` feature:
//!
//! ```text
//! docker run --rm -p 6379:6379 redis
//! cargo run --example redis --features redis
//! ```
//!
//! Set `AMALGAM_REDIS_URL` to point at a different server.

#[cfg(feature = "redis")]
mod demo {
    use std::sync::Arc;

    use amalgam::{
        Backplane, Cache, DistributedCache, DistributedLocker, DistributedSerializer,
        JsonSerializer, RedisBackplane, RedisDistributedCache, RedisDistributedLocker,
    };

    pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
        let url = std::env::var("AMALGAM_REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1/".to_owned());
        println!("connecting to {url} …");

        let l2 = match RedisDistributedCache::connect(url.clone()).await {
            Ok(l2) => l2,
            Err(err) => {
                eprintln!("could not connect to Redis ({err}).");
                eprintln!("start one with: docker run --rm -p 6379:6379 redis");
                return Ok(());
            }
        };
        let backplane = RedisBackplane::connect(url.clone()).await?;
        let locker = RedisDistributedLocker::connect(url).await?;

        let cache: Cache<String> = Cache::builder()
            .distributed(Arc::new(l2) as Arc<dyn DistributedCache>)
            .serializer(Arc::new(JsonSerializer) as Arc<dyn DistributedSerializer<String>>)
            .backplane(Arc::new(backplane) as Arc<dyn Backplane>)
            .distributed_locker(Arc::new(locker) as Arc<dyn DistributedLocker>)
            .instance_id("redis-example")
            .build();

        let value = cache
            .get_or_set("greeting", |ctx| async move {
                println!("  factory ran (cache miss) — fetching from the source");
                Ok(ctx.value("hello from a Redis-backed amalgam".to_owned()))
            })
            .await?;
        println!("first call  → {value}");

        let again = cache
            .get_or_set("greeting", |ctx| async move {
                println!("  (this should NOT print — served from cache/L2)");
                Ok(ctx.value("unused".to_owned()))
            })
            .await?;
        println!("second call → {again}");

        Ok(())
    }
}

#[tokio::main]
async fn main() {
    #[cfg(feature = "redis")]
    if let Err(err) = demo::run().await {
        eprintln!("error: {err}");
    }

    #[cfg(not(feature = "redis"))]
    {
        eprintln!("This example requires the `redis` feature:");
        eprintln!("  cargo run --example redis --features redis");
    }
}
