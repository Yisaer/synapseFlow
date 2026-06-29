//! `SecretRef` (config-layer pointer) and `SecretString` (resolved runtime
//! value). These have OPPOSITE serialization behavior, by design (VF-51 §6.1):
//!
//! * `SecretRef::Serialize` is a faithful round-trip (so `store:NAME` survives a
//!   reload), while its `Debug`/`Display` redact (so logs never leak).
//! * `SecretString` never participates in config serialization; its
//!   `Debug`/`Display` always redact.

use std::fmt;

use serde::de::{self, Deserializer};
use serde::ser::Serializer;
use serde::{Deserialize, Serialize};
use zeroize::Zeroizing;

use super::root_key::SecretError;
use super::store::SecretStore;

/// Prefix marking a store reference in the compact wire form `store:NAME`.
pub const STORE_PREFIX: &str = "store:";

/// A resolved secret value. Zeroized on drop; `Debug`/`Display` always redact.
#[derive(Clone, PartialEq, Eq)]
pub struct SecretString(Zeroizing<String>);

impl SecretString {
    pub fn new(value: impl Into<String>) -> Self {
        Self(Zeroizing::new(value.into()))
    }

    /// Explicit, greppable exposure of the plaintext. Callers must not log it.
    pub fn expose(&self) -> &str {
        self.0.as_str()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

impl fmt::Display for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

/// A config-layer secret reference. v1 supports a named store reference and an
/// inline literal (the latter constrained by `secrets.policy`, see §6.2).
#[derive(Clone, PartialEq, Eq)]
pub enum SecretRef {
    /// Named secret resolved from the encrypted store: wire form `store:NAME`.
    Store { name: String },
    /// Literal value embedded in config (user's choice; warn/strict policy applies).
    Inline(SecretString),
}

impl SecretRef {
    /// Build a store reference from a name.
    pub fn store(name: impl Into<String>) -> Self {
        SecretRef::Store { name: name.into() }
    }

    /// Build an inline reference from a literal value.
    pub fn inline(value: impl Into<String>) -> Self {
        SecretRef::Inline(SecretString::new(value))
    }

    /// True if this reference embeds a literal secret in config.
    pub fn is_inline(&self) -> bool {
        matches!(self, SecretRef::Inline(_))
    }

    /// The store name for a `Store` reference, or `None` for inline. Useful as a
    /// stable, non-secret identifier (e.g. an encryption key id).
    pub fn store_name(&self) -> Option<&str> {
        match self {
            SecretRef::Store { name } => Some(name.as_str()),
            SecretRef::Inline(_) => None,
        }
    }

    /// Resolve to the concrete secret value, looking up the store when needed.
    pub fn resolve(&self, store: &SecretStore) -> Result<SecretString, SecretError> {
        match self {
            SecretRef::Store { name } => Ok(SecretString(store.get(name)?)),
            SecretRef::Inline(value) => Ok(value.clone()),
        }
    }

    /// The compact wire string: `store:NAME` for a store ref, plaintext for inline.
    fn to_wire(&self) -> String {
        match self {
            SecretRef::Store { name } => format!("{STORE_PREFIX}{name}"),
            SecretRef::Inline(value) => value.expose().to_string(),
        }
    }

    /// Parse the compact wire string. `store:` prefix selects a store ref;
    /// everything else is an inline literal.
    fn from_wire(s: &str) -> Self {
        if let Some(name) = s.strip_prefix(STORE_PREFIX) {
            SecretRef::Store {
                name: name.to_string(),
            }
        } else {
            SecretRef::Inline(SecretString::new(s))
        }
    }
}

impl fmt::Debug for SecretRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Store names are non-sensitive identifiers and are safe to show; inline
        // literals must redact.
        match self {
            SecretRef::Store { name } => f
                .debug_struct("SecretRef::Store")
                .field("name", name)
                .finish(),
            SecretRef::Inline(_) => f.write_str("SecretRef::Inline(<redacted>)"),
        }
    }
}

impl fmt::Display for SecretRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SecretRef::Store { name } => write!(f, "{STORE_PREFIX}{name}"),
            SecretRef::Inline(_) => f.write_str("<redacted>"),
        }
    }
}

impl Serialize for SecretRef {
    /// Faithful round-trip: persistence must be able to reload the pointer.
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_wire())
    }
}

impl<'de> Deserialize<'de> for SecretRef {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // Accept either the compact string form (`"store:NAME"` / literal) or a
        // structured object (`{"ref":"store:NAME"}` / `{"value":"..."}`).
        struct V;
        impl<'de> de::Visitor<'de> for V {
            type Value = SecretRef;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a secret reference string or object")
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<SecretRef, E> {
                Ok(SecretRef::from_wire(v))
            }

            fn visit_string<E: de::Error>(self, v: String) -> Result<SecretRef, E> {
                Ok(SecretRef::from_wire(&v))
            }

            fn visit_map<A: de::MapAccess<'de>>(self, mut map: A) -> Result<SecretRef, A::Error> {
                let mut reference: Option<String> = None;
                let mut value: Option<String> = None;
                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "ref" => reference = Some(map.next_value()?),
                        "value" => value = Some(map.next_value()?),
                        _ => {
                            let _: de::IgnoredAny = map.next_value()?;
                        }
                    }
                }
                match (reference, value) {
                    (Some(r), None) => Ok(SecretRef::from_wire(&r)),
                    (None, Some(v)) => Ok(SecretRef::inline(v)),
                    (Some(_), Some(_)) => Err(de::Error::custom(
                        "secret reference cannot set both `ref` and `value`",
                    )),
                    (None, None) => Err(de::Error::custom(
                        "secret reference object needs `ref` or `value`",
                    )),
                }
            }
        }
        deserializer.deserialize_any(V)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::secret::root_key::HardcodedRootKey;

    #[test]
    fn debug_and_display_redact_inline() {
        let r = SecretRef::inline("hunter2");
        assert!(!format!("{r:?}").contains("hunter2"));
        assert!(!format!("{r}").contains("hunter2"));
        let s = SecretString::new("hunter2");
        assert_eq!(format!("{s:?}"), "<redacted>");
        assert_eq!(format!("{s}"), "<redacted>");
    }

    #[test]
    fn store_ref_serialize_roundtrips() {
        let r = SecretRef::store("sink-key-v1");
        let json = serde_json::to_string(&r).unwrap();
        assert_eq!(json, "\"store:sink-key-v1\"");
        let back: SecretRef = serde_json::from_str(&json).unwrap();
        assert_eq!(back, r);
    }

    #[test]
    fn inline_serialize_is_faithful() {
        // warn mode: the user chose inline, so it must persist faithfully (not redacted).
        let r = SecretRef::inline("hunter2");
        let json = serde_json::to_string(&r).unwrap();
        assert_eq!(json, "\"hunter2\"");
        let back: SecretRef = serde_json::from_str(&json).unwrap();
        assert_eq!(back, r);
    }

    #[test]
    fn deserialize_object_forms() {
        let s: SecretRef = serde_json::from_str(r#"{"ref":"store:k"}"#).unwrap();
        assert_eq!(s, SecretRef::store("k"));
        let i: SecretRef = serde_json::from_str(r#"{"value":"pw"}"#).unwrap();
        assert_eq!(i, SecretRef::inline("pw"));
    }

    #[test]
    fn resolve_store_and_inline() {
        let provider = HardcodedRootKey::new();
        let mut store = SecretStore::empty();
        store.set("k", "v");
        let bytes = store.to_bytes(&provider).unwrap();
        let store = SecretStore::from_bytes(&bytes, &provider).unwrap();

        assert_eq!(SecretRef::store("k").resolve(&store).unwrap().expose(), "v");
        assert_eq!(
            SecretRef::inline("inline-val")
                .resolve(&store)
                .unwrap()
                .expose(),
            "inline-val"
        );
        assert!(SecretRef::store("missing").resolve(&store).is_err());
    }
}
