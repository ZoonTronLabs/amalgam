//! Build-selected data-key encoding preserves the configured wire namespace.
use crate::options::KeyModifierMode;
use std::borrow::Cow;
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
    pub(super) fn physical<'a>(&self, key: &'a str) -> Cow<'a, str> {
        match self {
            Self::Prefix(prefix) => {
                let mut physical = String::with_capacity(prefix.len() + key.len());
                physical.push_str(prefix);
                physical.push_str(key);
                Cow::Owned(physical)
            }
            Self::Suffix(suffix) => {
                let mut physical = String::with_capacity(key.len() + suffix.len());
                physical.push_str(key);
                physical.push_str(suffix);
                Cow::Owned(physical)
            }
            Self::Unmodified => Cow::Borrowed(key),
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
