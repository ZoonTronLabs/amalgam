use amalgam::*;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct UnexpectedCopy(Arc<AtomicUsize>);
impl ValueCloner<Vec<String>> for UnexpectedCopy {
    fn clone_value(&self, _: &Vec<String>) -> std::result::Result<Vec<String>, CloneError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(CloneError::from_source(std::io::Error::other(
            "codec copy should be bypassed",
        )))
    }
}

#[tokio::test]
async fn compiler_proven_owned_values_bypass_codec_copy_and_remain_isolated() {
    let calls = Arc::new(AtomicUsize::new(0));
    let cache = Cache::<Vec<String>>::builder()
        .value_cloner(Arc::new(UnexpectedCopy(calls.clone())))
        .immutable_values()
        .default_options(EntryOptions::default().with_enable_auto_clone(true))
        .try_build()
        .unwrap();
    let mut original = vec!["before".to_owned()];
    cache.try_set("value", original.clone()).await.unwrap();
    original[0].push_str(" input mutation");
    let mut read = cache
        .read("value", None)
        .await
        .unwrap()
        .value_or(Vec::new());
    read[0].push_str(" output mutation");
    assert_eq!(
        cache.read("value", None).await.unwrap().value().unwrap(),
        &["before"]
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn immutable_shared_allocation_retains_identity_and_null_is_a_present_value() {
    let cache = Cache::<Option<Arc<str>>>::builder()
        .immutable_values()
        .default_options(EntryOptions::default().with_enable_auto_clone(true))
        .try_build()
        .unwrap();
    let original: Arc<str> = Arc::from("immutable");
    cache
        .try_set("shared", Some(original.clone()))
        .await
        .unwrap();
    let read = cache.read("shared", None).await.unwrap();
    assert!(Arc::ptr_eq(
        &original,
        read.value().unwrap().as_ref().unwrap()
    ));
    cache.try_set("null", None).await.unwrap();
    assert_eq!(cache.read("null", None).await.unwrap().value(), Some(&None));
    assert!(!cache.read("absent", None).await.unwrap().has_value());
    cache.shutdown().await.unwrap();
}

#[tokio::test]
async fn explicit_later_copy_strategy_keeps_its_failure_instead_of_silently_bypassing() {
    let calls = Arc::new(AtomicUsize::new(0));
    let cache = Cache::<Vec<String>>::builder()
        .immutable_values()
        .value_cloner(Arc::new(UnexpectedCopy(calls.clone())))
        .default_options(EntryOptions::default().with_enable_auto_clone(true))
        .try_build()
        .unwrap();
    assert!(matches!(
        cache.try_set("value", vec![]).await,
        Err(Error::Clone(_))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    cache.shutdown().await.unwrap();
}
