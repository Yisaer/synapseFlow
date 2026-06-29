//! Root key providers for the encrypted secret store.
//!
//! A [`RootKeyProvider`] holds the KEK (key-encryption key) used to wrap/unwrap
//! the store's DEK (data-encryption key). Envelope encryption means switching
//! providers (hardcoded -> env -> future TPM/KMS) only re-wraps the small DEK;
//! the encrypted store body never changes. See VF-51 §6.1.3.

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use base64::Engine;
use zeroize::Zeroizing;

use crate::codec::SecretBytes;

/// Errors produced by the secret store and its providers.
///
/// Messages NEVER include secret values, only non-sensitive identifiers
/// (secret names, env var names, error categories). See VF-51 §5.
#[derive(Debug, thiserror::Error)]
pub enum SecretError {
    #[error("secret store I/O error: {0}")]
    Io(String),
    #[error("secret store format error: {0}")]
    Format(String),
    #[error("root key error: {0}")]
    RootKey(String),
    #[error("secret crypto error: {0}")]
    Crypto(String),
    #[error("secret not found: `{0}`")]
    NotFound(String),
    #[error("invalid secret reference: {0}")]
    Reference(String),
}

/// Length of the AES-256 KEK / DEK in bytes.
pub(crate) const KEY_LEN: usize = 32;
/// Length of the AES-GCM nonce in bytes.
pub(crate) const NONCE_LEN: usize = 12;

/// Identifies which root key provider is in effect (for logging / upgrade hints).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RootKeyKind {
    /// Built-in application constant KEK. Scanner-safe but NOT confidential.
    Hardcoded,
    /// KEK supplied via environment variable. Real confidentiality.
    Env,
}

impl RootKeyKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            RootKeyKind::Hardcoded => "hardcoded",
            RootKeyKind::Env => "env",
        }
    }
}

/// Pluggable source of the root key (KEK). Holds both wrap and unwrap so that
/// for KMS/TPM the wrap happens inside the provider (KMS Encrypt / TPM seal).
pub trait RootKeyProvider: Send + Sync {
    /// Unwrap (decrypt) the DEK. Runtime read path.
    fn unwrap_dek(&self, wrapped: &[u8]) -> Result<SecretBytes, SecretError>;
    /// Wrap (encrypt) the DEK. Offline write / re-wrap path.
    fn wrap_dek(&self, dek: &SecretBytes) -> Result<Vec<u8>, SecretError>;
    /// Which provider this is, for diagnostics.
    fn kind(&self) -> RootKeyKind;
}

/// AES-256-GCM wrap: output = nonce(12) || ciphertext+tag.
fn aead_wrap(kek: &[u8], plaintext: &[u8]) -> Result<Vec<u8>, SecretError> {
    let cipher = Aes256Gcm::new_from_slice(kek)
        .map_err(|_| SecretError::RootKey("invalid root key length (expected 32 bytes)".into()))?;
    let mut nonce_bytes = [0u8; NONCE_LEN];
    getrandom::fill(&mut nonce_bytes)
        .map_err(|_| SecretError::Crypto("failed to generate wrap nonce".into()))?;
    let nonce = Nonce::from_slice(&nonce_bytes);
    let ciphertext = cipher
        .encrypt(nonce, plaintext)
        .map_err(|_| SecretError::Crypto("DEK wrap failed".into()))?;
    let mut out = Vec::with_capacity(NONCE_LEN + ciphertext.len());
    out.extend_from_slice(&nonce_bytes);
    out.extend_from_slice(&ciphertext);
    Ok(out)
}

/// AES-256-GCM unwrap of `aead_wrap` output.
fn aead_unwrap(kek: &[u8], wrapped: &[u8]) -> Result<SecretBytes, SecretError> {
    if wrapped.len() <= NONCE_LEN {
        return Err(SecretError::Format("wrapped DEK is too short".into()));
    }
    let cipher = Aes256Gcm::new_from_slice(kek)
        .map_err(|_| SecretError::RootKey("invalid root key length (expected 32 bytes)".into()))?;
    let (nonce_bytes, ciphertext) = wrapped.split_at(NONCE_LEN);
    let nonce = Nonce::from_slice(nonce_bytes);
    let plaintext = cipher.decrypt(nonce, ciphertext).map_err(|_| {
        SecretError::Crypto("DEK unwrap failed (wrong root key or corrupt store)".into())
    })?;
    Ok(SecretBytes::new(plaintext))
}

/// Built-in constant KEK. Zero-config default: makes the store ciphertext so a
/// static scanner finds nothing, but anyone holding the binary can recover it.
/// NOT confidential — set an env root key for real confidentiality. VF-51 §6.1.3.
pub struct HardcodedRootKey {
    kek: Zeroizing<[u8; KEY_LEN]>,
}

impl HardcodedRootKey {
    /// Application-constant KEK. Intentionally not protecting any user value; it
    /// only moves secrets out of scanner range by making the store ciphertext.
    // veloflux-secret-allowlist: build-time constant KEK, not a user secret.
    const KEK: [u8; KEY_LEN] = [
        0x56, 0x46, 0x2d, 0x35, 0x31, 0x3a, 0x68, 0x61, 0x72, 0x64, 0x63, 0x6f, 0x64, 0x65, 0x64,
        0x3a, 0x72, 0x6f, 0x6f, 0x74, 0x2d, 0x6b, 0x65, 0x79, 0x3a, 0x76, 0x31, 0x2e, 0x30, 0x2e,
        0x30, 0x21,
    ];

    pub fn new() -> Self {
        Self {
            kek: Zeroizing::new(Self::KEK),
        }
    }
}

impl Default for HardcodedRootKey {
    fn default() -> Self {
        Self::new()
    }
}

impl RootKeyProvider for HardcodedRootKey {
    fn unwrap_dek(&self, wrapped: &[u8]) -> Result<SecretBytes, SecretError> {
        aead_unwrap(self.kek.as_slice(), wrapped)
    }

    fn wrap_dek(&self, dek: &SecretBytes) -> Result<Vec<u8>, SecretError> {
        aead_wrap(self.kek.as_slice(), dek.as_slice())
    }

    fn kind(&self) -> RootKeyKind {
        RootKeyKind::Hardcoded
    }
}

/// Default env var holding the base64-encoded 32-byte root key.
pub const DEFAULT_ENV_VAR: &str = "VELOFLUX_SECRETS_KEY";

/// KEK supplied via an environment variable (raw 32-byte key, base64). No KDF:
/// the key is machine/systemd-provided full-entropy material. VF-51 §6.1.3.
pub struct EnvRootKey {
    kek: Zeroizing<[u8; KEY_LEN]>,
}

impl EnvRootKey {
    /// Read the KEK from `var_name`. Returns `Ok(None)` if the var is unset so
    /// callers can fall back to the hardcoded provider.
    pub fn from_env(var_name: &str) -> Result<Option<Self>, SecretError> {
        match std::env::var(var_name) {
            Ok(val) => Ok(Some(Self::from_base64(&val, var_name)?)),
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(std::env::VarError::NotUnicode(_)) => Err(SecretError::RootKey(format!(
                "env var `{var_name}` is not valid unicode"
            ))),
        }
    }

    /// Decode a base64 32-byte key. The value is never logged on error.
    pub fn from_base64(value: &str, var_name: &str) -> Result<Self, SecretError> {
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(value.trim())
            .map_err(|_| {
                SecretError::RootKey(format!("env var `{var_name}` is not valid base64"))
            })?;
        let kek: [u8; KEY_LEN] = decoded.as_slice().try_into().map_err(|_| {
            SecretError::RootKey(format!(
                "env var `{var_name}` must decode to exactly {KEY_LEN} bytes"
            ))
        })?;
        Ok(Self {
            kek: Zeroizing::new(kek),
        })
    }
}

impl RootKeyProvider for EnvRootKey {
    fn unwrap_dek(&self, wrapped: &[u8]) -> Result<SecretBytes, SecretError> {
        aead_unwrap(self.kek.as_slice(), wrapped)
    }

    fn wrap_dek(&self, dek: &SecretBytes) -> Result<Vec<u8>, SecretError> {
        aead_wrap(self.kek.as_slice(), dek.as_slice())
    }

    fn kind(&self) -> RootKeyKind {
        RootKeyKind::Env
    }
}

/// Resolve the default root key provider, env taking priority over hardcoded.
/// VF-51 §6.1.3 (priority env > hardcoded). Returns the provider plus its kind
/// so callers can emit the "scanner-safe but not confidential" warning.
pub fn default_root_key_provider() -> Result<Box<dyn RootKeyProvider>, SecretError> {
    match EnvRootKey::from_env(DEFAULT_ENV_VAR)? {
        Some(env_key) => Ok(Box::new(env_key)),
        None => Ok(Box::new(HardcodedRootKey::new())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hardcoded_wrap_unwrap_roundtrips() {
        let provider = HardcodedRootKey::new();
        let dek = SecretBytes::new(vec![7u8; KEY_LEN]);
        let wrapped = provider.wrap_dek(&dek).unwrap();
        let unwrapped = provider.unwrap_dek(&wrapped).unwrap();
        assert_eq!(unwrapped.as_slice(), dek.as_slice());
    }

    #[test]
    fn env_wrap_unwrap_roundtrips() {
        let key = base64::engine::general_purpose::STANDARD.encode([3u8; KEY_LEN]);
        let provider = EnvRootKey::from_base64(&key, "TEST").unwrap();
        let dek = SecretBytes::new(vec![9u8; KEY_LEN]);
        let wrapped = provider.wrap_dek(&dek).unwrap();
        assert_eq!(
            provider.unwrap_dek(&wrapped).unwrap().as_slice(),
            dek.as_slice()
        );
    }

    #[test]
    fn wrong_root_key_fails_unwrap() {
        let dek = SecretBytes::new(vec![1u8; KEY_LEN]);
        let wrapped = HardcodedRootKey::new().wrap_dek(&dek).unwrap();
        let other = EnvRootKey::from_base64(
            &base64::engine::general_purpose::STANDARD.encode([2u8; KEY_LEN]),
            "TEST",
        )
        .unwrap();
        assert!(other.unwrap_dek(&wrapped).is_err());
    }

    #[test]
    fn rewrap_dek_keeps_same_dek_across_providers() {
        // Envelope upgrade: unwrap with old provider, re-wrap with new; DEK is identical.
        let old = HardcodedRootKey::new();
        let new = EnvRootKey::from_base64(
            &base64::engine::general_purpose::STANDARD.encode([5u8; KEY_LEN]),
            "TEST",
        )
        .unwrap();
        let dek = SecretBytes::new(vec![42u8; KEY_LEN]);
        let wrapped_old = old.wrap_dek(&dek).unwrap();
        let dek_again = old.unwrap_dek(&wrapped_old).unwrap();
        let wrapped_new = new.wrap_dek(&dek_again).unwrap();
        assert_eq!(
            new.unwrap_dek(&wrapped_new).unwrap().as_slice(),
            dek.as_slice()
        );
    }

    #[test]
    fn errors_never_contain_key_bytes() {
        let err = match EnvRootKey::from_base64("!!!not-base64!!!", "MYVAR") {
            Err(e) => e.to_string(),
            Ok(_) => panic!("expected error"),
        };
        assert!(err.contains("MYVAR"));
        assert!(!err.contains("!!!not-base64!!!"));
    }
}
