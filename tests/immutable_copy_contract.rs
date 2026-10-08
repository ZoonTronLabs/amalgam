use amalgam::provider::*;
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
    cache
        .set("value", original.clone())
        .with_receipt()
        .await
        .unwrap();
    original[0].push_str(" input mutation");
    let mut read = cache.try_get("value").await.unwrap().unwrap_or(Vec::new());
    read[0].push_str(" output mutation");
    assert_eq!(
        cache.try_get("value").await.unwrap().as_ref().unwrap(),
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
        .set("shared", Some(original.clone()))
        .with_receipt()
        .await
        .unwrap();
    let read = cache.try_get("shared").await.unwrap();
    assert!(Arc::ptr_eq(
        &original,
        read.as_ref().unwrap().as_ref().unwrap()
    ));
    cache.set("null", None).with_receipt().await.unwrap();
    assert_eq!(cache.try_get("null").await.unwrap().as_ref(), Some(&None));
    assert!(cache.try_get("absent").await.unwrap().is_none());
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
        cache.set("value", vec![]).with_receipt().await,
        Err(Error::Clone(_))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    cache.shutdown().await.unwrap();
}
