//! Sink delivery encryption transforms.
//!
//! Provides redacted inline key configuration and AES-GCM STREAM encryption for
//! connector-agnostic sink delivery bytes.

use aead::stream::{EncryptorBE32, Nonce, StreamBE32};
use aead::{KeyInit, Payload};
use aes::Aes192;
use aes_gcm::{Aes128Gcm, Aes256Gcm, AesGcm};
use base64::Engine;
use hkdf::Hkdf;
use sha2::Sha256;
use std::fmt;
use std::sync::Arc;
use zeroize::Zeroizing;

const MAGIC: &[u8; 4] = b"VFE1";
const VERSION: u8 = 1;
const ALGORITHM_AES_GCM_STREAM_BE32: u8 = 1;
const SALT_LEN: usize = 16;
const STREAM_NONCE_LEN: usize = 7;
const STREAM_NONCE: [u8; STREAM_NONCE_LEN] = [0; STREAM_NONCE_LEN];
const HKDF_INFO: &[u8] = b"veloflux:sink-encrypt:aes-gcm-stream:v1";

type Aes192Gcm = AesGcm<Aes192, aead::consts::U12>;

/// Supported sink delivery encryption algorithms.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EncryptionAlgorithm {
    AesGcm,
}

impl EncryptionAlgorithm {
    pub fn as_str(&self) -> &'static str {
        match self {
            EncryptionAlgorithm::AesGcm => "aes-gcm",
        }
    }
}

impl TryFrom<&str> for EncryptionAlgorithm {
    type Error = EncryptError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.trim().to_ascii_lowercase().as_str() {
            "aes-gcm" => Ok(EncryptionAlgorithm::AesGcm),
            other => Err(EncryptError::InvalidConfig(format!(
                "unsupported encryption algorithm `{other}`"
            ))),
        }
    }
}

/// Encoding used for inline secret material.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SecretEncoding {
    Base64,
    Hex,
}

impl SecretEncoding {
    pub fn as_str(&self) -> &'static str {
        match self {
            SecretEncoding::Base64 => "base64",
            SecretEncoding::Hex => "hex",
        }
    }
}

impl TryFrom<&str> for SecretEncoding {
    type Error = EncryptError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.trim().to_ascii_lowercase().as_str() {
            "base64" => Ok(SecretEncoding::Base64),
            "hex" => Ok(SecretEncoding::Hex),
            other => Err(EncryptError::InvalidConfig(format!(
                "unsupported secret encoding `{other}`"
            ))),
        }
    }
}

/// Inline key material as supplied by configuration.
#[derive(PartialEq, Eq)]
pub struct InlineEncryptionKey {
    value: Zeroizing<String>,
    pub encoding: SecretEncoding,
}

impl InlineEncryptionKey {
    pub fn new(value: impl Into<String>, encoding: SecretEncoding) -> Self {
        Self {
            value: Zeroizing::new(value.into()),
            encoding,
        }
    }

    pub fn decode(self, key_id: &str) -> Result<SecretBytes, EncryptError> {
        let value = self.value.trim_ascii();
        let decoded = match self.encoding {
            SecretEncoding::Base64 => base64::engine::general_purpose::STANDARD
                .decode(value)
                .map_err(|_| {
                    EncryptError::InvalidKey(format!(
                        "invalid base64 inline encryption key for key_id `{key_id}`"
                    ))
                })?,
            SecretEncoding::Hex => hex::decode(value).map_err(|_| {
                EncryptError::InvalidKey(format!(
                    "invalid hex inline encryption key for key_id `{key_id}`"
                ))
            })?,
        };
        Ok(SecretBytes::new(decoded))
    }
}

impl fmt::Debug for InlineEncryptionKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InlineEncryptionKey")
            .field("value", &"<redacted>")
            .field("encoding", &"<redacted>")
            .finish()
    }
}

/// Decoded secret bytes that are zeroized on drop.
#[derive(PartialEq, Eq)]
pub struct SecretBytes {
    bytes: Zeroizing<Vec<u8>>,
}

impl SecretBytes {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self {
            bytes: Zeroizing::new(bytes),
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        self.bytes.as_slice()
    }

    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

impl fmt::Debug for SecretBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("SecretBytes").field(&"<redacted>").finish()
    }
}

/// Sink delivery encryption configuration.
#[derive(Clone, PartialEq, Eq)]
pub struct SinkEncryptionConfig {
    pub algorithm: EncryptionAlgorithm,
    pub key_id: String,
    pub key_bits: u16,
    pub key: Arc<SecretBytes>,
}

impl SinkEncryptionConfig {
    pub fn new(
        algorithm: EncryptionAlgorithm,
        key_id: impl Into<String>,
        key: SecretBytes,
    ) -> Result<Self, EncryptError> {
        let key_id = key_id.into().trim().to_string();
        validate_encryption_config(algorithm, &key_id)?;
        let key_bits = resolve_aes_gcm_key_bits(&key_id, key.len())?;
        Ok(Self {
            algorithm,
            key_id,
            key_bits,
            key: Arc::new(key),
        })
    }

    pub fn from_inline_key(
        algorithm: EncryptionAlgorithm,
        key_id: impl Into<String>,
        key: InlineEncryptionKey,
    ) -> Result<Self, EncryptError> {
        let key_id = key_id.into().trim().to_string();
        validate_encryption_config(algorithm, &key_id)?;
        let key = key.decode(&key_id)?;
        Self::new(algorithm, key_id, key)
    }

    pub fn aes_gcm(
        key_id: impl Into<String>,
        key: InlineEncryptionKey,
    ) -> Result<Self, EncryptError> {
        Self::from_inline_key(EncryptionAlgorithm::AesGcm, key_id, key)
    }

    pub fn validate_and_resolve_key_bits(&self) -> Result<u16, EncryptError> {
        validate_encryption_config(self.algorithm, &self.key_id)?;
        let key_bits = resolve_aes_gcm_key_bits(&self.key_id, self.key.len())?;
        if key_bits != self.key_bits {
            return Err(EncryptError::InvalidKey(format!(
                "AES-GCM key_bits mismatch for key_id `{}`: config has {}, resolved {}",
                self.key_id, self.key_bits, key_bits
            )));
        }
        Ok(key_bits)
    }
}

impl fmt::Debug for SinkEncryptionConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SinkEncryptionConfig")
            .field("algorithm", &self.algorithm.as_str())
            .field("key_id", &self.key_id)
            .field("key_bits", &self.key_bits)
            .field("key", &self.key)
            .finish()
    }
}

/// Errors produced by encryption config and writers.
#[derive(Debug, thiserror::Error)]
pub enum EncryptError {
    #[error("invalid encryption config: {0}")]
    InvalidConfig(String),
    #[error("invalid encryption key: {0}")]
    InvalidKey(String),
    #[error("encrypt error: {0}")]
    Encrypt(String),
    #[error("encrypted delivery format error: {0}")]
    Format(String),
}

/// Streaming encryptor for one delivery at a time.
pub trait EncryptWriter: Send {
    /// Reset per-delivery state and append the cleartext encrypted-stream header.
    fn begin_delivery(&mut self, out: &mut Vec<u8>) -> Result<(), EncryptError>;
    /// Encrypt `input`, appending encrypted frame bytes to `out`.
    fn write(&mut self, input: &[u8], out: &mut Vec<u8>) -> Result<(), EncryptError>;
    /// Encrypt final bytes and close the stream, appending the final frame.
    fn finish(&mut self, input: &[u8], out: &mut Vec<u8>) -> Result<(), EncryptError>;
    /// Discard in-progress delivery state.
    fn abort_delivery(&mut self);
}

/// AES-GCM STREAM writer using a per-delivery HKDF subkey.
pub struct AesGcmStreamWriter {
    key_id: String,
    key_bits: u16,
    master_key: Arc<SecretBytes>,
    state: Option<AesGcmStreamState>,
}

impl AesGcmStreamWriter {
    pub fn from_config(config: &SinkEncryptionConfig) -> Result<Self, EncryptError> {
        let key_bits = config.validate_and_resolve_key_bits()?;
        Ok(Self {
            key_id: config.key_id.clone(),
            key_bits,
            master_key: Arc::clone(&config.key),
            state: None,
        })
    }

    pub fn key_bits(&self) -> u16 {
        self.key_bits
    }
}

impl EncryptWriter for AesGcmStreamWriter {
    fn begin_delivery(&mut self, out: &mut Vec<u8>) -> Result<(), EncryptError> {
        if self.state.is_some() {
            return Err(EncryptError::Encrypt(format!(
                "delivery already active for key_id `{}`",
                self.key_id
            )));
        }

        let mut salt = [0u8; SALT_LEN];
        getrandom::fill(&mut salt).map_err(|err| {
            EncryptError::Encrypt(format!(
                "failed to generate delivery salt for key_id `{}`: {err}",
                self.key_id
            ))
        })?;
        let header = encode_stream_header(self.key_bits, &self.key_id, &salt)?;
        let stream_key = derive_stream_key(self.master_key.as_slice(), &salt)?;
        let encryptor = AesGcmEncryptor::new(self.key_bits, stream_key.as_slice())?;
        self.state = Some(AesGcmStreamState {
            header,
            encryptor,
            segments_emitted: 0,
        });
        let state = self
            .state
            .as_ref()
            .expect("encryption state just initialized");
        out.extend_from_slice(&state.header);
        Ok(())
    }

    fn write(&mut self, input: &[u8], out: &mut Vec<u8>) -> Result<(), EncryptError> {
        let state = self.state.as_mut().ok_or_else(|| {
            EncryptError::Encrypt(format!(
                "write without active delivery for key_id `{}`",
                self.key_id
            ))
        })?;
        state.reserve_segment()?;
        let ciphertext = state.encryptor.encrypt_next(input, &state.header)?;
        append_frame(out, &ciphertext)?;
        Ok(())
    }

    fn finish(&mut self, input: &[u8], out: &mut Vec<u8>) -> Result<(), EncryptError> {
        let state = self.state.take().ok_or_else(|| {
            EncryptError::Encrypt(format!(
                "finish without active delivery for key_id `{}`",
                self.key_id
            ))
        })?;
        state.validate_segment_available()?;
        let ciphertext = state.encryptor.encrypt_last(input, &state.header)?;
        append_frame(out, &ciphertext)?;
        Ok(())
    }

    fn abort_delivery(&mut self) {
        self.state = None;
    }
}

struct AesGcmStreamState {
    header: Vec<u8>,
    encryptor: AesGcmEncryptor,
    segments_emitted: u32,
}

impl AesGcmStreamState {
    fn validate_segment_available(&self) -> Result<(), EncryptError> {
        if self.segments_emitted == u32::MAX {
            return Err(EncryptError::Encrypt(
                "AES-GCM STREAM segment counter exhausted".to_string(),
            ));
        }
        Ok(())
    }

    fn reserve_segment(&mut self) -> Result<(), EncryptError> {
        self.validate_segment_available()?;
        self.segments_emitted += 1;
        Ok(())
    }
}

enum AesGcmEncryptor {
    Aes128(EncryptorBE32<Aes128Gcm>),
    Aes192(EncryptorBE32<Aes192Gcm>),
    Aes256(EncryptorBE32<Aes256Gcm>),
}

impl AesGcmEncryptor {
    fn new(key_bits: u16, key: &[u8]) -> Result<Self, EncryptError> {
        match key_bits {
            128 => {
                let cipher = Aes128Gcm::new_from_slice(key).map_err(|_| {
                    EncryptError::InvalidKey("invalid AES-128-GCM key length".to_string())
                })?;
                let nonce = Nonce::<Aes128Gcm, StreamBE32<Aes128Gcm>>::from_slice(&STREAM_NONCE);
                Ok(Self::Aes128(EncryptorBE32::from_aead(cipher, nonce)))
            }
            192 => {
                let cipher = Aes192Gcm::new_from_slice(key).map_err(|_| {
                    EncryptError::InvalidKey("invalid AES-192-GCM key length".to_string())
                })?;
                let nonce = Nonce::<Aes192Gcm, StreamBE32<Aes192Gcm>>::from_slice(&STREAM_NONCE);
                Ok(Self::Aes192(EncryptorBE32::from_aead(cipher, nonce)))
            }
            256 => {
                let cipher = Aes256Gcm::new_from_slice(key).map_err(|_| {
                    EncryptError::InvalidKey("invalid AES-256-GCM key length".to_string())
                })?;
                let nonce = Nonce::<Aes256Gcm, StreamBE32<Aes256Gcm>>::from_slice(&STREAM_NONCE);
                Ok(Self::Aes256(EncryptorBE32::from_aead(cipher, nonce)))
            }
            _ => Err(EncryptError::InvalidKey(format!(
                "unsupported AES-GCM key_bits `{key_bits}`"
            ))),
        }
    }

    fn encrypt_next(&mut self, input: &[u8], aad: &[u8]) -> Result<Vec<u8>, EncryptError> {
        match self {
            Self::Aes128(encryptor) => encryptor.encrypt_next(Payload { msg: input, aad }),
            Self::Aes192(encryptor) => encryptor.encrypt_next(Payload { msg: input, aad }),
            Self::Aes256(encryptor) => encryptor.encrypt_next(Payload { msg: input, aad }),
        }
        .map_err(|_| EncryptError::Encrypt("AES-GCM STREAM encrypt_next failed".to_string()))
    }

    fn encrypt_last(self, input: &[u8], aad: &[u8]) -> Result<Vec<u8>, EncryptError> {
        match self {
            Self::Aes128(encryptor) => encryptor.encrypt_last(Payload { msg: input, aad }),
            Self::Aes192(encryptor) => encryptor.encrypt_last(Payload { msg: input, aad }),
            Self::Aes256(encryptor) => encryptor.encrypt_last(Payload { msg: input, aad }),
        }
        .map_err(|_| EncryptError::Encrypt("AES-GCM STREAM encrypt_last failed".to_string()))
    }
}

fn resolve_aes_gcm_key_bits(key_id: &str, len: usize) -> Result<u16, EncryptError> {
    match len {
        16 => Ok(128),
        24 => Ok(192),
        32 => Ok(256),
        _ => Err(EncryptError::InvalidKey(format!(
            "invalid AES-GCM key length {len} bytes for key_id `{key_id}`; expected 16, 24, or 32 bytes"
        ))),
    }
}

fn validate_encryption_config(
    algorithm: EncryptionAlgorithm,
    key_id: &str,
) -> Result<(), EncryptError> {
    if !matches!(algorithm, EncryptionAlgorithm::AesGcm) {
        return Err(EncryptError::InvalidConfig(format!(
            "unsupported encryption algorithm `{}`",
            algorithm.as_str()
        )));
    }
    if key_id.trim().is_empty() {
        return Err(EncryptError::InvalidConfig(
            "encryption key_id must be non-empty".to_string(),
        ));
    }
    Ok(())
}

fn derive_stream_key(
    master_key: &[u8],
    salt: &[u8; SALT_LEN],
) -> Result<Zeroizing<Vec<u8>>, EncryptError> {
    let hk = Hkdf::<Sha256>::new(Some(salt), master_key);
    let mut stream_key = Zeroizing::new(vec![0u8; master_key.len()]);
    hk.expand(HKDF_INFO, stream_key.as_mut_slice())
        .map_err(|_| EncryptError::Encrypt("failed to derive AES-GCM stream key".to_string()))?;
    Ok(stream_key)
}

fn encode_stream_header(
    key_bits: u16,
    key_id: &str,
    salt: &[u8; SALT_LEN],
) -> Result<Vec<u8>, EncryptError> {
    if key_id.is_empty() {
        return Err(EncryptError::InvalidConfig(
            "encryption key_id must be non-empty".to_string(),
        ));
    }
    let key_id_len = u16::try_from(key_id.len())
        .map_err(|_| EncryptError::InvalidConfig("encryption key_id is too long".to_string()))?;
    let mut out = Vec::with_capacity(4 + 1 + 1 + 2 + 2 + key_id.len() + 1 + SALT_LEN);
    out.extend_from_slice(MAGIC);
    out.push(VERSION);
    out.push(ALGORITHM_AES_GCM_STREAM_BE32);
    out.extend_from_slice(&key_bits.to_be_bytes());
    out.extend_from_slice(&key_id_len.to_be_bytes());
    out.extend_from_slice(key_id.as_bytes());
    out.push(SALT_LEN as u8);
    out.extend_from_slice(salt);
    Ok(out)
}

fn append_frame(out: &mut Vec<u8>, ciphertext: &[u8]) -> Result<(), EncryptError> {
    let len = u32::try_from(ciphertext.len()).map_err(|_| {
        EncryptError::Encrypt("encrypted frame exceeds u32 length limit".to_string())
    })?;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(ciphertext);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use aead::stream::DecryptorBE32;
    use base64::Engine;

    enum AesGcmDecryptor {
        Aes128(DecryptorBE32<Aes128Gcm>),
        Aes192(DecryptorBE32<Aes192Gcm>),
        Aes256(DecryptorBE32<Aes256Gcm>),
    }

    impl AesGcmDecryptor {
        fn new(key_bits: u16, key: &[u8]) -> Result<Self, String> {
            match key_bits {
                128 => {
                    let cipher = Aes128Gcm::new_from_slice(key)
                        .map_err(|_| "invalid aes-128 key".to_string())?;
                    let nonce =
                        Nonce::<Aes128Gcm, StreamBE32<Aes128Gcm>>::from_slice(&STREAM_NONCE);
                    Ok(Self::Aes128(DecryptorBE32::from_aead(cipher, nonce)))
                }
                192 => {
                    let cipher = Aes192Gcm::new_from_slice(key)
                        .map_err(|_| "invalid aes-192 key".to_string())?;
                    let nonce =
                        Nonce::<Aes192Gcm, StreamBE32<Aes192Gcm>>::from_slice(&STREAM_NONCE);
                    Ok(Self::Aes192(DecryptorBE32::from_aead(cipher, nonce)))
                }
                256 => {
                    let cipher = Aes256Gcm::new_from_slice(key)
                        .map_err(|_| "invalid aes-256 key".to_string())?;
                    let nonce =
                        Nonce::<Aes256Gcm, StreamBE32<Aes256Gcm>>::from_slice(&STREAM_NONCE);
                    Ok(Self::Aes256(DecryptorBE32::from_aead(cipher, nonce)))
                }
                other => Err(format!("unexpected key_bits {other}")),
            }
        }

        fn decrypt_next(&mut self, input: &[u8], aad: &[u8]) -> Result<Vec<u8>, aead::Error> {
            match self {
                Self::Aes128(decryptor) => decryptor.decrypt_next(Payload { msg: input, aad }),
                Self::Aes192(decryptor) => decryptor.decrypt_next(Payload { msg: input, aad }),
                Self::Aes256(decryptor) => decryptor.decrypt_next(Payload { msg: input, aad }),
            }
        }

        fn decrypt_last(self, input: &[u8], aad: &[u8]) -> Result<Vec<u8>, aead::Error> {
            match self {
                Self::Aes128(decryptor) => decryptor.decrypt_last(Payload { msg: input, aad }),
                Self::Aes192(decryptor) => decryptor.decrypt_last(Payload { msg: input, aad }),
                Self::Aes256(decryptor) => decryptor.decrypt_last(Payload { msg: input, aad }),
            }
        }
    }

    #[derive(Debug)]
    struct ParsedHeader {
        header_len: usize,
        key_bits: u16,
        key_id: String,
        salt: [u8; SALT_LEN],
    }

    fn parse_header(data: &[u8]) -> ParsedHeader {
        assert!(data.len() >= 4 + 1 + 1 + 2 + 2 + 1 + SALT_LEN);
        assert_eq!(&data[0..4], MAGIC);
        assert_eq!(data[4], VERSION);
        assert_eq!(data[5], ALGORITHM_AES_GCM_STREAM_BE32);
        let key_bits = u16::from_be_bytes([data[6], data[7]]);
        let key_id_len = u16::from_be_bytes([data[8], data[9]]) as usize;
        let key_id_start = 10;
        let key_id_end = key_id_start + key_id_len;
        let key_id = std::str::from_utf8(&data[key_id_start..key_id_end])
            .expect("key id utf8")
            .to_string();
        assert_eq!(data[key_id_end], SALT_LEN as u8);
        let salt_start = key_id_end + 1;
        let mut salt = [0u8; SALT_LEN];
        salt.copy_from_slice(&data[salt_start..salt_start + SALT_LEN]);
        ParsedHeader {
            header_len: salt_start + SALT_LEN,
            key_bits,
            key_id,
            salt,
        }
    }

    fn split_frames(data: &[u8], mut offset: usize) -> Vec<Vec<u8>> {
        let mut frames = Vec::new();
        while offset < data.len() {
            let len = u32::from_be_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]) as usize;
            offset += 4;
            frames.push(data[offset..offset + len].to_vec());
            offset += len;
        }
        frames
    }

    fn decrypt_delivery(data: &[u8], master_key: &[u8]) -> Result<Vec<u8>, String> {
        let parsed = parse_header(data);
        let stream_key =
            derive_stream_key(master_key, &parsed.salt).map_err(|err| err.to_string())?;
        let mut frames = split_frames(data, parsed.header_len);
        let last = frames
            .pop()
            .ok_or_else(|| "encrypted delivery has no frames".to_string())?;
        let header = &data[..parsed.header_len];
        let mut decryptor = AesGcmDecryptor::new(parsed.key_bits, stream_key.as_slice())?;
        let mut out = Vec::new();
        for frame in &frames {
            let plaintext = decryptor
                .decrypt_next(frame, header)
                .map_err(|_| "decrypt_next failed".to_string())?;
            out.extend_from_slice(&plaintext);
        }
        let plaintext = decryptor
            .decrypt_last(&last, header)
            .map_err(|_| "decrypt_last failed".to_string())?;
        out.extend_from_slice(&plaintext);
        Ok(out)
    }

    fn config_for_key(key: &[u8]) -> SinkEncryptionConfig {
        let value = base64::engine::general_purpose::STANDARD.encode(key);
        SinkEncryptionConfig::aes_gcm(
            "sink-aes-v1",
            InlineEncryptionKey::new(value, SecretEncoding::Base64),
        )
        .expect("encryption config")
    }

    fn encrypt_chunks(key: &[u8], chunks: &[&[u8]]) -> Vec<u8> {
        let config = config_for_key(key);
        let mut writer = AesGcmStreamWriter::from_config(&config).expect("writer");
        let mut out = Vec::new();
        writer.begin_delivery(&mut out).expect("begin");
        for chunk in &chunks[..chunks.len().saturating_sub(1)] {
            writer.write(chunk, &mut out).expect("write");
        }
        writer
            .finish(chunks.last().copied().unwrap_or_default(), &mut out)
            .expect("finish");
        out
    }

    // coverage-covers: sink.encrypt.inline_key_config
    #[test]
    fn inline_key_decodes_base64_hex_and_resolves_key_bits() {
        for len in [16usize, 24, 32] {
            let key = vec![7u8; len];
            let expected_bits = (len * 8) as u16;
            let b64 = base64::engine::general_purpose::STANDARD.encode(&key);
            let cfg = SinkEncryptionConfig::aes_gcm(
                "key-1",
                InlineEncryptionKey::new(format!(" \n{b64}\n "), SecretEncoding::Base64),
            )
            .expect("base64 config");
            assert_eq!(cfg.validate_and_resolve_key_bits().unwrap(), expected_bits);

            let hex = hex::encode(&key);
            let cfg = SinkEncryptionConfig::aes_gcm(
                "key-1",
                InlineEncryptionKey::new(format!(" {hex}\n"), SecretEncoding::Hex),
            )
            .expect("hex config");
            assert_eq!(cfg.validate_and_resolve_key_bits().unwrap(), expected_bits);
        }
    }

    // coverage-covers: sink.encrypt.inline_key_config
    #[test]
    fn config_trims_key_id_before_storage_and_header_encoding() {
        let cfg = SinkEncryptionConfig::aes_gcm(
            " \tsink-aes-v1\n",
            InlineEncryptionKey::new(hex::encode([7u8; 32]), SecretEncoding::Hex),
        )
        .expect("encryption config");
        assert_eq!(cfg.key_id, "sink-aes-v1");

        let mut writer = AesGcmStreamWriter::from_config(&cfg).expect("writer");
        let mut encrypted = Vec::new();
        writer.begin_delivery(&mut encrypted).expect("begin");
        writer.finish(b"payload", &mut encrypted).expect("finish");
        assert_eq!(parse_header(&encrypted).key_id, "sink-aes-v1");
    }

    // coverage-covers: sink.encrypt.inline_key_config
    #[test]
    fn inline_key_errors_are_redacted() {
        let raw = "not-secret-valid";
        let err = SinkEncryptionConfig::aes_gcm(
            "key-1",
            InlineEncryptionKey::new(raw, SecretEncoding::Base64),
        )
        .unwrap_err()
        .to_string();
        assert!(!err.contains(raw), "error leaked raw key: {err}");

        let valid_cfg = SinkEncryptionConfig::aes_gcm(
            "key-1",
            InlineEncryptionKey::new(hex::encode([1u8; 16]), SecretEncoding::Hex),
        )
        .expect("valid config");
        assert!(format!("{valid_cfg:?}").contains("<redacted>"));

        let short_hex = hex::encode([1u8; 15]);
        let err = SinkEncryptionConfig::aes_gcm(
            "key-1",
            InlineEncryptionKey::new(short_hex.clone(), SecretEncoding::Hex),
        )
        .unwrap_err()
        .to_string();
        assert!(!err.contains(&short_hex));
        assert!(err.contains("15 bytes"));
    }

    // coverage-covers: sink.encrypt.aes_gcm_delivery
    #[test]
    fn aes_gcm_stream_roundtrips_single_multi_and_empty_deliveries() {
        for key_len in [16usize, 24, 32] {
            let key = vec![key_len as u8; key_len];
            let encrypted = encrypt_chunks(&key, &[b"hello"]);
            assert_eq!(decrypt_delivery(&encrypted, &key).unwrap(), b"hello");

            let encrypted = encrypt_chunks(&key, &[b"hello", b" ", b"world"]);
            assert_eq!(decrypt_delivery(&encrypted, &key).unwrap(), b"hello world");

            let encrypted = encrypt_chunks(&key, &[b""]);
            assert_eq!(decrypt_delivery(&encrypted, &key).unwrap(), b"");
        }
    }

    // coverage-covers: sink.encrypt.aes_gcm_delivery
    #[test]
    fn deliveries_use_distinct_salt_and_ciphertext() {
        let key = vec![9u8; 32];
        let first = encrypt_chunks(&key, &[b"same plaintext"]);
        let second = encrypt_chunks(&key, &[b"same plaintext"]);
        assert_ne!(first, second);
        assert_ne!(parse_header(&first).salt, parse_header(&second).salt);
    }

    // coverage-covers: sink.encrypt.aes_gcm_delivery
    #[test]
    fn tamper_header_ciphertext_truncation_and_reorder_fail() {
        let key = vec![3u8; 32];
        let encrypted = encrypt_chunks(&key, &[b"one", b"two", b"three"]);
        assert_eq!(decrypt_delivery(&encrypted, &key).unwrap(), b"onetwothree");

        let parsed = parse_header(&encrypted);
        assert_eq!(parsed.key_id, "sink-aes-v1");

        let mut tampered = encrypted.clone();
        tampered[10] ^= 1;
        assert!(decrypt_delivery(&tampered, &key).is_err());

        let mut tampered = encrypted.clone();
        tampered[6] ^= 1;
        assert!(decrypt_delivery(&tampered, &key).is_err());

        let mut tampered = encrypted.clone();
        let last = tampered.len() - 1;
        tampered[last] ^= 1;
        assert!(decrypt_delivery(&tampered, &key).is_err());

        let frames = split_frames(&encrypted, parsed.header_len);
        let mut truncated = encrypted[..parsed.header_len].to_vec();
        append_frame(&mut truncated, &frames[0]).unwrap();
        append_frame(&mut truncated, &frames[1]).unwrap();
        assert!(decrypt_delivery(&truncated, &key).is_err());

        let mut reordered = encrypted[..parsed.header_len].to_vec();
        append_frame(&mut reordered, &frames[1]).unwrap();
        append_frame(&mut reordered, &frames[0]).unwrap();
        append_frame(&mut reordered, &frames[2]).unwrap();
        assert!(decrypt_delivery(&reordered, &key).is_err());
    }

    // coverage-covers: sink.encrypt.aes_gcm_delivery
    #[test]
    fn abort_resets_state_for_next_delivery() {
        let key = vec![5u8; 32];
        let config = config_for_key(&key);
        let mut writer = AesGcmStreamWriter::from_config(&config).expect("writer");
        let mut discarded = Vec::new();
        writer.begin_delivery(&mut discarded).expect("begin");
        writer.write(b"discard", &mut discarded).expect("write");
        writer.abort_delivery();

        let mut out = Vec::new();
        writer.begin_delivery(&mut out).expect("begin again");
        writer.finish(b"kept", &mut out).expect("finish");
        assert_eq!(decrypt_delivery(&out, &key).unwrap(), b"kept");
    }

    // coverage-covers: sink.encrypt.aes_gcm_delivery
    #[test]
    fn segment_counter_exhaustion_fails_delivery() {
        let key = vec![6u8; 32];
        let config = config_for_key(&key);
        let mut writer = AesGcmStreamWriter::from_config(&config).expect("writer");
        let mut out = Vec::new();
        writer.begin_delivery(&mut out).expect("begin");
        writer
            .state
            .as_mut()
            .expect("active state")
            .segments_emitted = u32::MAX;

        let err = writer.write(b"overflow", &mut out).unwrap_err().to_string();
        assert!(err.contains("segment counter exhausted"));
    }
}
