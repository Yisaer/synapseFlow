//! Encrypted secret store: a single envelope-encrypted file holding all
//! `name -> secret` mappings. VF-51 §6.1.3.
//!
//! File layout (the header is cleartext; only the body is encrypted):
//! ```text
//! MAGIC "VFS1" (4) | version u8 | wrapped_dek_len u16 (BE) | wrapped_dek | nonce (12)
//! body: AES-256-GCM(plaintext = JSON(name -> secret), key = DEK, nonce = above, aad = header)
//! ```
//! The whole map is sealed in one AEAD pass (no VFE stream frames): the store is
//! small, bounded and read whole at startup, so framing buys nothing.

use std::collections::BTreeMap;
use std::path::Path;

use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use zeroize::Zeroizing;

use crate::codec::SecretBytes;

use super::root_key::{RootKeyProvider, SecretError, KEY_LEN, NONCE_LEN};

const MAGIC: &[u8; 4] = b"VFS1";
const VERSION: u8 = 1;

/// In-memory view of the decrypted secret store. The secret values live in
/// zeroizing strings and are never serialized in plaintext outside this file.
pub struct SecretStore {
    secrets: BTreeMap<String, Zeroizing<String>>,
}

impl SecretStore {
    /// An empty store.
    pub fn empty() -> Self {
        Self {
            secrets: BTreeMap::new(),
        }
    }

    /// Look up a secret by name. Returns the resolved value or `NotFound`.
    pub fn get(&self, name: &str) -> Result<Zeroizing<String>, SecretError> {
        self.secrets
            .get(name)
            .cloned()
            .ok_or_else(|| SecretError::NotFound(name.to_string()))
    }

    /// True if a secret with this name exists.
    pub fn contains(&self, name: &str) -> bool {
        self.secrets.contains_key(name)
    }

    /// Sorted list of secret names (no values).
    pub fn names(&self) -> Vec<String> {
        self.secrets.keys().cloned().collect()
    }

    /// Insert or replace a secret value.
    pub fn set(&mut self, name: impl Into<String>, value: impl Into<String>) {
        self.secrets
            .insert(name.into(), Zeroizing::new(value.into()));
    }

    /// Remove a secret. Returns true if it existed.
    pub fn remove(&mut self, name: &str) -> bool {
        self.secrets.remove(name).is_some()
    }

    pub fn is_empty(&self) -> bool {
        self.secrets.is_empty()
    }

    /// Load and decrypt a store from disk. Missing file -> empty store, so a
    /// fresh deployment with no secrets configured starts cleanly.
    pub fn load(path: &Path, provider: &dyn RootKeyProvider) -> Result<Self, SecretError> {
        let bytes = match std::fs::read(path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Self::empty()),
            Err(e) => return Err(SecretError::Io(format!("failed to read secret store: {e}"))),
        };
        Self::from_bytes(&bytes, provider)
    }

    /// Encrypt and atomically write the store to disk. Creates the parent
    /// directory if it does not exist (e.g. a fresh `--data-dir`).
    pub fn save(&self, path: &Path, provider: &dyn RootKeyProvider) -> Result<(), SecretError> {
        let bytes = self.to_bytes(provider)?;
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent).map_err(|e| {
                SecretError::Io(format!("failed to create secret store directory: {e}"))
            })?;
        }
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, &bytes)
            .map_err(|e| SecretError::Io(format!("failed to write secret store: {e}")))?;
        std::fs::rename(&tmp, path)
            .map_err(|e| SecretError::Io(format!("failed to persist secret store: {e}")))?;
        Ok(())
    }

    /// Decrypt a store from its on-disk byte representation.
    pub fn from_bytes(bytes: &[u8], provider: &dyn RootKeyProvider) -> Result<Self, SecretError> {
        let mut cur = 0usize;
        let take = |cur: &mut usize, n: usize| -> Result<&[u8], SecretError> {
            let end = cur.checked_add(n).filter(|e| *e <= bytes.len());
            match end {
                Some(end) => {
                    let slice = &bytes[*cur..end];
                    *cur = end;
                    Ok(slice)
                }
                None => Err(SecretError::Format("secret store is truncated".into())),
            }
        };

        if take(&mut cur, 4)? != MAGIC {
            return Err(SecretError::Format("bad secret store magic".into()));
        }
        let version = take(&mut cur, 1)?[0];
        if version != VERSION {
            return Err(SecretError::Format(format!(
                "unsupported secret store version {version}"
            )));
        }
        let wrapped_len = {
            let b = take(&mut cur, 2)?;
            u16::from_be_bytes([b[0], b[1]]) as usize
        };
        let wrapped_dek = take(&mut cur, wrapped_len)?.to_vec();
        let nonce_bytes = take(&mut cur, NONCE_LEN)?.to_vec();
        let header = &bytes[..cur];
        let ciphertext = &bytes[cur..];

        let dek = provider.unwrap_dek(&wrapped_dek)?;
        let plaintext = aead_decrypt(dek.as_slice(), &nonce_bytes, ciphertext, header)?;
        let map: BTreeMap<String, String> = serde_json::from_slice(plaintext.as_slice())
            .map_err(|_| SecretError::Format("corrupt secret store contents".into()))?;
        let secrets = map
            .into_iter()
            .map(|(k, v)| (k, Zeroizing::new(v)))
            .collect();
        Ok(Self { secrets })
    }

    /// Encrypt the store into its on-disk byte representation. Generates a fresh
    /// DEK and nonce each save (full re-encrypt; the store is small).
    pub fn to_bytes(&self, provider: &dyn RootKeyProvider) -> Result<Vec<u8>, SecretError> {
        let plain_map: BTreeMap<&str, &str> = self
            .secrets
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let plaintext = Zeroizing::new(
            serde_json::to_vec(&plain_map)
                .map_err(|_| SecretError::Format("failed to encode secret store".into()))?,
        );

        let mut dek_bytes = Zeroizing::new([0u8; KEY_LEN]);
        getrandom::fill(dek_bytes.as_mut_slice())
            .map_err(|_| SecretError::Crypto("failed to generate DEK".into()))?;
        let dek = SecretBytes::new(dek_bytes.to_vec());
        let wrapped_dek = provider.wrap_dek(&dek)?;

        let mut nonce_bytes = [0u8; NONCE_LEN];
        getrandom::fill(&mut nonce_bytes)
            .map_err(|_| SecretError::Crypto("failed to generate store nonce".into()))?;

        let wrapped_len = u16::try_from(wrapped_dek.len())
            .map_err(|_| SecretError::Format("wrapped DEK too large".into()))?;

        let mut header = Vec::with_capacity(4 + 1 + 2 + wrapped_dek.len() + NONCE_LEN);
        header.extend_from_slice(MAGIC);
        header.push(VERSION);
        header.extend_from_slice(&wrapped_len.to_be_bytes());
        header.extend_from_slice(&wrapped_dek);
        header.extend_from_slice(&nonce_bytes);

        let ciphertext = aead_encrypt(dek.as_slice(), &nonce_bytes, plaintext.as_slice(), &header)?;
        let mut out = header;
        out.extend_from_slice(&ciphertext);
        Ok(out)
    }
}

fn aead_encrypt(
    dek: &[u8],
    nonce_bytes: &[u8],
    plaintext: &[u8],
    aad: &[u8],
) -> Result<Vec<u8>, SecretError> {
    let cipher = Aes256Gcm::new_from_slice(dek)
        .map_err(|_| SecretError::Crypto("invalid DEK length".into()))?;
    let nonce = Nonce::from_slice(nonce_bytes);
    cipher
        .encrypt(
            nonce,
            Payload {
                msg: plaintext,
                aad,
            },
        )
        .map_err(|_| SecretError::Crypto("secret store encrypt failed".into()))
}

fn aead_decrypt(
    dek: &[u8],
    nonce_bytes: &[u8],
    ciphertext: &[u8],
    aad: &[u8],
) -> Result<Zeroizing<Vec<u8>>, SecretError> {
    let cipher = Aes256Gcm::new_from_slice(dek)
        .map_err(|_| SecretError::Crypto("invalid DEK length".into()))?;
    let nonce = Nonce::from_slice(nonce_bytes);
    cipher
        .decrypt(
            nonce,
            Payload {
                msg: ciphertext,
                aad,
            },
        )
        .map(Zeroizing::new)
        .map_err(|_| {
            SecretError::Crypto("secret store decrypt failed (corrupt or wrong key)".into())
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::secret::root_key::{EnvRootKey, HardcodedRootKey};
    use base64::Engine;

    #[test]
    fn roundtrips_through_bytes() {
        let provider = HardcodedRootKey::new();
        let mut store = SecretStore::empty();
        store.set("mqtt-pass", "s3cr3t");
        store.set("sink-key-v1", "BASE64KEY==");
        let bytes = store.to_bytes(&provider).unwrap();
        let loaded = SecretStore::from_bytes(&bytes, &provider).unwrap();
        assert_eq!(&*loaded.get("mqtt-pass").unwrap(), "s3cr3t");
        assert_eq!(&*loaded.get("sink-key-v1").unwrap(), "BASE64KEY==");
        assert_eq!(loaded.names(), vec!["mqtt-pass", "sink-key-v1"]);
    }

    #[test]
    fn ciphertext_does_not_contain_plaintext() {
        let provider = HardcodedRootKey::new();
        let mut store = SecretStore::empty();
        store.set("k", "super-secret-value-xyz");
        let bytes = store.to_bytes(&provider).unwrap();
        let haystack = String::from_utf8_lossy(&bytes);
        assert!(!haystack.contains("super-secret-value-xyz"));
        assert!(!haystack.contains("\"k\""));
    }

    #[test]
    fn missing_file_loads_empty() {
        let provider = HardcodedRootKey::new();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("secrets.enc");
        let store = SecretStore::load(&path, &provider).unwrap();
        assert!(store.is_empty());
    }

    #[test]
    fn save_then_load_file() {
        let provider = HardcodedRootKey::new();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("secrets.enc");
        let mut store = SecretStore::empty();
        store.set("token", "abc123");
        store.save(&path, &provider).unwrap();
        let loaded = SecretStore::load(&path, &provider).unwrap();
        assert_eq!(&*loaded.get("token").unwrap(), "abc123");
    }

    #[test]
    fn save_creates_missing_parent_dir() {
        let provider = HardcodedRootKey::new();
        let dir = tempfile::tempdir().unwrap();
        // Nested path whose parent does not exist yet (fresh --data-dir).
        let path = dir.path().join("does/not/exist/secrets.enc");
        let mut store = SecretStore::empty();
        store.set("k", "v");
        store.save(&path, &provider).unwrap();
        let loaded = SecretStore::load(&path, &provider).unwrap();
        assert_eq!(&*loaded.get("k").unwrap(), "v");
    }

    #[test]
    fn wrong_root_key_fails_to_decrypt() {
        let hard = HardcodedRootKey::new();
        let mut store = SecretStore::empty();
        store.set("k", "v");
        let bytes = store.to_bytes(&hard).unwrap();
        let env = EnvRootKey::from_base64(
            &base64::engine::general_purpose::STANDARD.encode([1u8; 32]),
            "TEST",
        )
        .unwrap();
        assert!(SecretStore::from_bytes(&bytes, &env).is_err());
    }

    #[test]
    fn rewrap_to_new_provider_preserves_secrets() {
        // The envelope upgrade in practice: same plaintext re-encrypted under a
        // new root key still decrypts to the same secrets.
        let hard = HardcodedRootKey::new();
        let mut store = SecretStore::empty();
        store.set("k", "v");
        let bytes = store.to_bytes(&hard).unwrap();
        let loaded = SecretStore::from_bytes(&bytes, &hard).unwrap();
        let env = EnvRootKey::from_base64(
            &base64::engine::general_purpose::STANDARD.encode([8u8; 32]),
            "TEST",
        )
        .unwrap();
        let rebytes = loaded.to_bytes(&env).unwrap();
        let reloaded = SecretStore::from_bytes(&rebytes, &env).unwrap();
        assert_eq!(&*reloaded.get("k").unwrap(), "v");
    }

    #[test]
    fn tampered_body_fails() {
        let provider = HardcodedRootKey::new();
        let mut store = SecretStore::empty();
        store.set("k", "v");
        let mut bytes = store.to_bytes(&provider).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 1;
        assert!(SecretStore::from_bytes(&bytes, &provider).is_err());
    }
}
