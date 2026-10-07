//! Physical data keys and reverse backplane decoding preserve every wire mode.
use amalgam::{
    Cache, EntryOptions, advanced::KeyModifierMode, provider::DistributedCache,
    provider::InMemoryDistributedCache, provider::InProcessBackplane, provider::JsonSerializer,
    provider::SystemClock,
};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[tokio::test]
async fn unicode_data_keys_round_trip_and_peer_updates_use_the_same_namespace() {
    for (mode, physical) in [
        (KeyModifierMode::Prefix, "版:3:cache|订单:🙂"),
        (KeyModifierMode::Suffix, "cache|订单:🙂:版:3"),
        (KeyModifierMode::None, "cache|订单:🙂"),
    ] {
        let backend = Arc::new(InMemoryDistributedCache::new(Arc::new(SystemClock)));
        let backplane = Arc::new(InProcessBackplane::default());
        let node = |id| {
            Cache::<u64>::builder()
                .instance_id(id)
                .key_prefix("cache|")
                .distributed_wire_version("版:3")
                .distributed_key_modifier_mode(mode)
                .default_options(EntryOptions::new(Duration::from_secs(60)))
                .distributed(backend.clone())
                .serializer(Arc::new(JsonSerializer))
                .backplane(backplane.clone())
        };
        let a = node("encoding-a").try_build_ready().await.unwrap();
        let b = node("encoding-b").try_build_ready().await.unwrap();
        a.set("订单:🙂", 1)
            .with_receipt()
            .await
            .unwrap()
            .wait()
            .await
            .unwrap();
        assert!(backend.get(physical).await.unwrap().is_some());
        assert_eq!(b.read("订单:🙂", None).await.unwrap().into_value(), Some(1));
        a.set("订单:🙂", 2)
            .with_receipt()
            .await
            .unwrap()
            .wait()
            .await
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let value = b.read("订单:🙂", None).await.unwrap().into_value();
            if value == Some(2) {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "peer must decode the physical namespace"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        a.remove("订单:🙂")
            .with_receipt()
            .await
            .unwrap()
            .wait()
            .await
            .unwrap();
        assert!(backend.get(physical).await.unwrap().is_none());
        a.shutdown().await.unwrap();
        b.shutdown().await.unwrap();
    }
}
