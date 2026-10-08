//! Build-selected data-key encoding preserves the configured wire namespace.
use super::physical_key::PhysicalKey;
use crate::options::KeyModifierMode;
use std::sync::Arc;

pub(super) enum DistributedKey {
    Prefix(Arc<str>),
    Suffix(Arc<str>),
    Unmodified,
}
impl DistributedKey {
    pub(super) fn new(version: &str, mode: KeyModifierMode) -> Self {
        match mode {
            KeyModifierMode::Prefix => Self::Prefix(Arc::from(format!("{version}:"))),
            KeyModifierMode::Suffix => Self::Suffix(Arc::from(format!(":{version}"))),
            KeyModifierMode::None => Self::Unmodified,
        }
    }
    pub(super) fn physical<'a>(&self, key: &'a str) -> PhysicalKey<'a> {
        match self {
            Self::Prefix(prefix) => PhysicalKey::joined(prefix, key),
            Self::Suffix(suffix) => PhysicalKey::joined(key, suffix),
            Self::Unmodified => PhysicalKey::Borrowed(key),
        }
    }
    pub(super) fn logical<'a>(&self, physical: &'a str) -> Option<&'a str> {
        match self {
            Self::Prefix(prefix) => physical.strip_prefix(&**prefix),
            Self::Suffix(suffix) => physical.strip_suffix(&**suffix),
            Self::Unmodified => Some(physical),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::physical_key::INLINE_KEY;
    use super::*;

    #[test]
    fn physical_keys_join_inline_and_on_the_heap_identically() {
        let prefix = DistributedKey::new("v2", KeyModifierMode::Prefix);
        let suffix = DistributedKey::new("v2", KeyModifierMode::Suffix);
        let plain = DistributedKey::new("v2", KeyModifierMode::None);
        let long = "k".repeat(INLINE_KEY);
        let fits = "k".repeat(INLINE_KEY - 3);
        for key in ["", "key", "ключ ✓", fits.as_str(), long.as_str()] {
            let joined = prefix.physical(key);
            assert_eq!(&*joined, format!("v2:{key}"));
            assert_eq!(joined.to_string(), format!("v2:{key}"));
            assert_eq!(prefix.logical(&joined), Some(key));
            let joined = suffix.physical(key);
            assert_eq!(&*joined, format!("{key}:v2"));
            assert_eq!(suffix.logical(&joined), Some(key));
            assert_eq!(&*plain.physical(key), key);
        }
        assert!(matches!(prefix.physical(&fits), PhysicalKey::Inline { .. }));
        assert!(matches!(prefix.physical(&long), PhysicalKey::Owned(_)));
    }
}
