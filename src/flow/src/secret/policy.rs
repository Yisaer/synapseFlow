//! Strict/warn policy for inline secret literals (VF-51 §6.2).
//!
//! This governs the FIRST kind of gate: a secret put in the *correct*
//! `SecretRef` field but inline rather than referenced. Default is `warn`
//! (most deployments run on an internal network). The SECOND kind of gate —
//! a secret in the *wrong* place (URL userinfo, plain auth headers) — is always
//! rejected as invalid config and lives with each surface, not here.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use super::reference::SecretRef;
use super::root_key::SecretError;

/// How to handle an inline secret literal found in a `SecretRef` field.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SecretPolicy {
    /// Log a warning but allow loading. Default: internal-network deployments.
    #[default]
    Warn,
    /// Reject loading; startup fails. Hardening for high-exposure / compliance.
    Strict,
}

impl SecretPolicy {
    pub fn as_str(&self) -> &'static str {
        match self {
            SecretPolicy::Warn => "warn",
            SecretPolicy::Strict => "strict",
        }
    }
}

impl fmt::Display for SecretPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for SecretPolicy {
    type Err = SecretError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "warn" => Ok(SecretPolicy::Warn),
            "strict" => Ok(SecretPolicy::Strict),
            other => Err(SecretError::Reference(format!(
                "unknown secret policy `{other}` (expected `warn` or `strict`)"
            ))),
        }
    }
}

/// Outcome of checking a single `SecretRef` field against the policy.
#[derive(Debug)]
pub enum InlineCheck {
    /// Reference is a store pointer; nothing to do.
    Ok,
    /// Inline literal under `warn`: a warning the caller should log (no value).
    Warn(String),
}

/// Check one secret field. `field_label` is only the non-sensitive config-field
/// path shown in the message (e.g. `"mqtt.password"`), NOT the store key; the
/// secret value is NEVER included. Returns `Err` under `strict`.
pub fn check_inline(
    reference: &SecretRef,
    field_label: &str,
    policy: SecretPolicy,
) -> Result<InlineCheck, SecretError> {
    if !reference.is_inline() {
        return Ok(InlineCheck::Ok);
    }
    let message = format!(
        "field `{field_label}` contains an inline secret literal; use a `store:` reference instead"
    );
    match policy {
        SecretPolicy::Warn => Ok(InlineCheck::Warn(message)),
        SecretPolicy::Strict => Err(SecretError::Reference(message)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_ref_passes_any_policy() {
        let r = SecretRef::store("k");
        assert!(matches!(
            check_inline(&r, "f", SecretPolicy::Strict).unwrap(),
            InlineCheck::Ok
        ));
        assert!(matches!(
            check_inline(&r, "f", SecretPolicy::Warn).unwrap(),
            InlineCheck::Ok
        ));
    }

    #[test]
    fn inline_warns_in_warn_mode() {
        let r = SecretRef::inline("hunter2");
        match check_inline(&r, "mqtt.password", SecretPolicy::Warn).unwrap() {
            InlineCheck::Warn(msg) => {
                assert!(msg.contains("mqtt.password"));
                assert!(!msg.contains("hunter2"));
            }
            _ => panic!("expected warn"),
        }
    }

    #[test]
    fn inline_rejected_in_strict_mode() {
        let r = SecretRef::inline("hunter2");
        let err = check_inline(&r, "mqtt.password", SecretPolicy::Strict)
            .unwrap_err()
            .to_string();
        assert!(err.contains("mqtt.password"));
        assert!(!err.contains("hunter2"));
    }

    #[test]
    fn policy_parses_and_defaults() {
        assert_eq!(SecretPolicy::default(), SecretPolicy::Warn);
        assert_eq!(
            "strict".parse::<SecretPolicy>().unwrap(),
            SecretPolicy::Strict
        );
        assert!("nope".parse::<SecretPolicy>().is_err());
    }
}
