//! A factory owns its starting version, rather than whichever version exists later.
use amalgam::{Cache, ClearMode, EntryOptions, ManualClock, Timestamp};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;

#[derive(Clone, Copy)]
enum LaterMutation {
    Set,
    Remove,
    Clear,
}

async fn paused_origin(mutation: LaterMutation) {
    let cache = Cache::<u64>::builder()
        .clock(Arc::new(ManualClock::new(Timestamp::from_ticks(10_000))))
        .default_options(EntryOptions::new(Duration::from_secs(60)))
        .build();
    let (started, entered) = oneshot::channel();
    let (release, resume) = oneshot::channel();
    let worker = cache.clone();
    let origin = tokio::spawn(async move {
        worker
            .get_or_set("key", |ctx| async move {
                started.send(()).unwrap();
                resume.await.unwrap();
                Ok(ctx.value(1))
            })
            .await
    });
    entered.await.unwrap();
    match mutation {
        LaterMutation::Set => cache.try_set("key", 2).await.unwrap().wait().await.unwrap(),
        LaterMutation::Remove => cache.try_remove("key").await.unwrap().wait().await.unwrap(),
        LaterMutation::Clear => cache
            .try_clear(ClearMode::Remove)
            .await
            .unwrap()
            .wait()
            .await
            .unwrap(),
    };
    release.send(()).unwrap();
    assert_eq!(
        origin.await.unwrap().unwrap(),
        1,
        "the original caller may use its computed result"
    );
    let current = cache.read("key", None).await.unwrap().into_value();
    let expected = match mutation {
        LaterMutation::Set => Some(2),
        LaterMutation::Remove | LaterMutation::Clear => None,
    };
    assert_eq!(
        current, expected,
        "a late factory must not overwrite a later completed mutation at the same clock tick"
    );
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_later_set_supersedes_the_paused_origin() {
    paused_origin(LaterMutation::Set).await;
}
#[tokio::test]
async fn removing_an_absent_key_supersedes_the_paused_origin() {
    paused_origin(LaterMutation::Remove).await;
}
#[tokio::test]
async fn a_later_clear_supersedes_the_paused_origin() {
    paused_origin(LaterMutation::Clear).await;
}
