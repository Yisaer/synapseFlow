//! Shared Upon template support for connector and encoder profiles.

use crate::property::{validate_property_key, PropertyContext};
use crate::secret::SecretString;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// A fixed connector string that retains whether it contains sensitive data.
#[derive(Clone, PartialEq, Eq)]
pub enum ConnectorString {
    Plain(String),
    Sensitive(SecretString),
}

impl ConnectorString {
    pub fn plain(value: impl Into<String>) -> Self {
        Self::Plain(value.into())
    }

    pub fn sensitive(value: impl Into<String>) -> Self {
        Self::Sensitive(SecretString::new(value))
    }

    /// Explicitly expose the connector value at validation or wire boundaries.
    pub fn expose(&self) -> &str {
        match self {
            Self::Plain(value) => value,
            Self::Sensitive(value) => value.expose(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.expose().is_empty()
    }

    pub fn is_sensitive(&self) -> bool {
        matches!(self, Self::Sensitive(_))
    }
}

impl From<String> for ConnectorString {
    fn from(value: String) -> Self {
        Self::Plain(value)
    }
}

impl From<&str> for ConnectorString {
    fn from(value: &str) -> Self {
        Self::Plain(value.to_string())
    }
}

impl fmt::Debug for ConnectorString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Plain(value) => value.fmt(f),
            Self::Sensitive(_) => f.write_str("<redacted>"),
        }
    }
}

impl fmt::Display for ConnectorString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Plain(value) => value.fmt(f),
            Self::Sensitive(_) => f.write_str("<redacted>"),
        }
    }
}

pub(crate) struct PropertyTemplateProfile {
    used_property: Arc<AtomicBool>,
}

impl PropertyTemplateProfile {
    pub(crate) fn was_property_used(&self) -> bool {
        self.used_property.load(Ordering::Relaxed)
    }
}

/// Create an Upon engine with the process property function installed.
pub(crate) fn engine_with_properties(
    properties: PropertyContext,
) -> (upon::Engine<'static>, PropertyTemplateProfile) {
    let mut engine = upon::Engine::new();
    let used_property = Arc::new(AtomicBool::new(false));
    let used_property_for_function = Arc::clone(&used_property);
    engine.add_function("prop", move |key: &str| -> Result<String, String> {
        validate_property_key(key).map_err(|err| err.to_string())?;
        let value = properties
            .get(key)
            .ok_or_else(|| format!("property `{key}` is not defined"))?;
        used_property_for_function.store(true, Ordering::Relaxed);
        Ok(value.expose().to_string())
    });
    (engine, PropertyTemplateProfile { used_property })
}
