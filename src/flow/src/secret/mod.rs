//! Secret config management (VF-51).
//!
//! Converges all sensitive config onto references into a single envelope-encrypted
//! store, so that nothing scannable on disk (config files, logs) carries plaintext
//! secrets. See `ekwiki/issues/VF-51`.
//!
//! * [`SecretRef`] / [`SecretString`] — config pointer vs resolved value.
//! * [`SecretStore`] — the encrypted `name -> secret` file.
//! * [`RootKeyProvider`] — pluggable KEK source (hardcoded / env / future TPM/KMS).
//! * [`SecretPolicy`] — strict/warn handling of inline literals.

mod policy;
mod reference;
mod root_key;
mod store;

use std::sync::Arc;

pub use policy::{check_inline, InlineCheck, SecretPolicy};
pub use reference::{SecretRef, SecretString, STORE_PREFIX};
pub use root_key::{
    default_root_key_provider, EnvRootKey, HardcodedRootKey, RootKeyKind, RootKeyProvider,
    SecretError, DEFAULT_ENV_VAR,
};
pub use store::SecretStore;

/// Resolution context shared across config application: the decrypted store plus
/// the inline-secret policy. Cheap to clone (the store is behind an `Arc`).
#[derive(Clone)]
pub struct SecretContext {
    pub store: Arc<SecretStore>,
    pub policy: SecretPolicy,
}

impl SecretContext {
    pub fn new(store: Arc<SecretStore>, policy: SecretPolicy) -> Self {
        Self { store, policy }
    }

    /// An empty store with the default (`warn`) policy. Used when no secrets are
    /// configured, so existing inline configs keep working.
    pub fn empty() -> Self {
        Self {
            store: Arc::new(SecretStore::empty()),
            policy: SecretPolicy::default(),
        }
    }

    /// Resolve a reference, applying the policy to inline literals first. Returns
    /// any warning message for the caller to log (no secret value included).
    ///
    /// `field_label` is only the human-readable config-field path used in
    /// warning/error messages (e.g. `"mqtt.password"`); it is NOT the store key.
    /// The store key is the `NAME` in a `store:NAME` reference.
    pub fn resolve(
        &self,
        reference: &SecretRef,
        field_label: &str,
    ) -> Result<(SecretString, Option<String>), SecretError> {
        let warning = match check_inline(reference, field_label, self.policy)? {
            InlineCheck::Ok => None,
            InlineCheck::Warn(msg) => Some(msg),
        };
        let value = reference.resolve(&self.store)?;
        Ok((value, warning))
    }
}

impl Default for SecretContext {
    fn default() -> Self {
        Self::empty()
    }
}

/// Default secret store file name (under the data directory).
pub const DEFAULT_STORE_FILE: &str = "secrets.enc";
/// Env var selecting the inline-secret policy (`warn` | `strict`).
pub const POLICY_ENV_VAR: &str = "VELOFLUX_SECRETS_POLICY";

/// Load the encrypted secret store from `data_dir` using the default root key
/// provider (env > hardcoded), and resolve the policy from the environment.
/// A missing store file yields an empty store (no secrets configured).
///
/// Emits a warning when running on the hardcoded root key, which is scanner-safe
/// but NOT confidential (VF-51 §6.1.3).
pub fn bootstrap(data_dir: &std::path::Path) -> Result<SecretContext, SecretError> {
    let provider = default_root_key_provider()?;
    if provider.kind() == RootKeyKind::Hardcoded {
        tracing::warn!(
            target: "veloflux::secret",
            "secret store using the built-in hardcoded root key: scanner-safe but NOT confidential; \
             set the `{DEFAULT_ENV_VAR}` env var (base64 32-byte key) for real confidentiality"
        );
    }
    let path = data_dir.join(DEFAULT_STORE_FILE);
    let store = SecretStore::load(&path, provider.as_ref())?;
    let policy = match std::env::var(POLICY_ENV_VAR) {
        Ok(val) => val.parse::<SecretPolicy>()?,
        Err(_) => SecretPolicy::default(),
    };
    tracing::info!(
        target: "veloflux::secret",
        root_key = provider.kind().as_str(),
        policy = policy.as_str(),
        secret_count = store.names().len(),
        "secret store loaded"
    );
    Ok(SecretContext::new(Arc::new(store), policy))
}

#[cfg(test)]
mod context_tests {
    use super::*;

    fn ctx(policy: SecretPolicy) -> SecretContext {
        let mut store = SecretStore::empty();
        store.set("k", "v");
        SecretContext::new(Arc::new(store), policy)
    }

    #[test]
    fn warn_resolves_inline_with_warning() {
        let (value, warning) = ctx(SecretPolicy::Warn)
            .resolve(&SecretRef::inline("hunter2"), "f")
            .expect("warn allows inline");
        assert_eq!(value.expose(), "hunter2");
        assert!(warning.is_some());
    }

    #[test]
    fn strict_rejects_inline() {
        let err = ctx(SecretPolicy::Strict)
            .resolve(&SecretRef::inline("hunter2"), "mqtt.password")
            .unwrap_err()
            .to_string();
        assert!(err.contains("mqtt.password"));
        assert!(!err.contains("hunter2"), "error leaked secret: {err}");
    }

    #[test]
    fn strict_still_resolves_store_refs() {
        let (value, warning) = ctx(SecretPolicy::Strict)
            .resolve(&SecretRef::store("k"), "f")
            .expect("strict allows store refs");
        assert_eq!(value.expose(), "v");
        assert!(warning.is_none());
    }
}
