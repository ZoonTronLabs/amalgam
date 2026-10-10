//! Physical keys: a caller key joined with a cache prefix or a distributed
//! key modifier.
//!
//! Keys of at most `INLINE_KEY` bytes are joined inline instead of allocating a
//! `String` for every access; longer keys allocate once. A ready L1 hit with a
//! prefixed key of that size allocates nothing, and an owned copy is made only
//! when work outlives the call.
use std::sync::Arc;

pub(super) const INLINE_KEY: usize = 64;

/// A joined key, inline when it fits.
pub(super) enum PhysicalKey<'a> {
    Borrowed(&'a str),
    Inline {
        length: usize,
        bytes: [u8; INLINE_KEY],
    },
    Owned(String),
}
impl PhysicalKey<'_> {
    #[inline]
    pub(super) fn joined(first: &str, second: &str) -> Self {
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
impl PhysicalKey<'_> {
    /// The owned text; a heap key moves instead of being copied.
    #[inline]
    pub(super) fn into_owned(self) -> String {
        match self {
            Self::Owned(key) => key,
            key => String::from(&*key),
        }
    }
}
impl std::ops::Deref for PhysicalKey<'_> {
    type Target = str;
    #[inline]
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

/// The cache prefix and the caller key of one operation. It stays two borrowed
/// words through a ready result: a hit needs the joined text only for the L1
/// probe, and events or eager refresh join it on demand.
#[derive(Clone, Copy)]
pub(super) struct KeyParts<'a> {
    // A thin reference keeps the parts as small as the `Cow` they replace.
    prefix: Option<&'a Arc<str>>,
    raw: &'a str,
}
impl<'a> KeyParts<'a> {
    /// An empty prefix is no prefix: the joined text is identical either way.
    #[inline]
    pub(super) fn new(prefix: Option<&'a Arc<str>>, raw: &'a str) -> Self {
        Self {
            prefix: prefix.filter(|prefix| !prefix.is_empty()),
            raw,
        }
    }
    #[inline]
    pub(super) fn prefix(self) -> Option<&'a str> {
        self.prefix.map(|prefix| &**prefix)
    }
    /// The joined L1 key.
    #[inline]
    pub(super) fn physical(self) -> PhysicalKey<'a> {
        match self.prefix() {
            None => PhysicalKey::Borrowed(self.raw),
            Some(prefix) => PhysicalKey::joined(prefix, self.raw),
        }
    }
    pub(super) fn to_shared(self) -> Arc<str> {
        Arc::from(&*self.physical())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_join_identically_inline_and_on_the_heap() {
        let long = "k".repeat(INLINE_KEY);
        let fits = "k".repeat(INLINE_KEY - 4);
        for (prefix, raw) in [
            (None, "currencies"),
            (Some(""), "currencies"),
            (Some("ref:"), "currencies"),
            (Some("справочник:"), "валюты-₸"),
            (Some("ref:"), fits.as_str()),
            (Some("ref:"), long.as_str()),
        ] {
            let expected = format!("{}{raw}", prefix.unwrap_or(""));
            let shared = prefix.map(Arc::<str>::from);
            let parts = KeyParts::new(shared.as_ref(), raw);
            assert_eq!(parts.prefix().is_some(), !prefix.unwrap_or("").is_empty());
            assert_eq!(&*parts.physical(), expected);
            assert_eq!(parts.physical().to_string(), expected);
            assert_eq!(&*parts.to_shared(), expected);
        }
        let prefix: Arc<str> = Arc::from("ref:");
        let ref_key = |raw| KeyParts::new(Some(&prefix), raw).physical();
        assert_eq!(
            std::mem::size_of::<KeyParts<'_>>(),
            std::mem::size_of::<std::borrow::Cow<'_, str>>()
        );
        assert_eq!(ref_key(&long).into_owned(), format!("ref:{long}"));
        assert_eq!(ref_key(&fits).into_owned(), format!("ref:{fits}"));
        assert!(matches!(ref_key(&fits), PhysicalKey::Inline { .. }));
        assert!(matches!(ref_key(&long), PhysicalKey::Owned(_)));
        assert!(matches!(
            KeyParts::new(None, "currencies").physical(),
            PhysicalKey::Borrowed(_)
        ));
    }
}
