//! Build-selected data-key encoding preserves the configured wire namespace.
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

const INLINE_KEY: usize = 64;

/// A physical L2 key. Ordinary keys are joined inline instead of allocating a
/// `String` for every distributed access; long keys keep the heap form.
pub(super) enum PhysicalKey<'a> {
    Borrowed(&'a str),
    Inline {
        length: usize,
        bytes: [u8; INLINE_KEY],
    },
    Owned(String),
}
impl PhysicalKey<'_> {
    fn joined(first: &str, second: &str) -> Self {
        let length = first.len() + second.len();
        if length > INLINE_KEY {
            let mut physical = String::with_capacity(length);
            physical.push_str(first);
            physical.push_str(second);
            return Self::Owned(physical);
        }
        let mut bytes = [0_u8; INLINE_KEY];
        bytes[..first.len()].copy_from_slice(first.as_bytes());
        bytes[first.len()..length].copy_from_slice(second.as_bytes());
        Self::Inline { length, bytes }
    }
}
impl std::ops::Deref for PhysicalKey<'_> {
    type Target = str;
    fn deref(&self) -> &str {
        match self {
            Self::Borrowed(key) => key,
            Self::Inline { length, bytes } => match std::str::from_utf8(&bytes[..*length]) {
                Ok(key) => key,
                Err(_) => unreachable!("an inline key joins two complete UTF-8 strings"),
            },
            Self::Owned(key) => key,
        }
    }
}
impl std::fmt::Display for PhysicalKey<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self)
    }
}
