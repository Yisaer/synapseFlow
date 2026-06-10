use crate::codec::{EncryptionAlgorithm, SecretBytes};
use crate::planner::physical::BasePhysicalPlan;
use std::fmt;
use std::sync::Arc;

use super::PhysicalPlan;

/// Physical node for the sink delivery encryption transform.
///
/// Sits between `PhysicalSinkEncoder` / `PhysicalSinkCompress` and
/// `PhysicalSinkConnector`, encrypting each encoded delivery stream.
#[derive(Clone)]
pub struct PhysicalSinkEncrypt {
    pub base: BasePhysicalPlan,
    pub algorithm: EncryptionAlgorithm,
    pub key_id: String,
    pub key_bits: u16,
    pub key: Arc<SecretBytes>,
}

impl PhysicalSinkEncrypt {
    pub fn new(
        child: Arc<PhysicalPlan>,
        index: i64,
        algorithm: EncryptionAlgorithm,
        key_id: impl Into<String>,
        key_bits: u16,
        key: Arc<SecretBytes>,
    ) -> Self {
        Self {
            base: BasePhysicalPlan::new(vec![child], index),
            algorithm,
            key_id: key_id.into(),
            key_bits,
            key,
        }
    }
}

impl fmt::Debug for PhysicalSinkEncrypt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PhysicalSinkEncrypt")
            .field("index", &self.base.index())
            .field("algorithm", &self.algorithm.as_str())
            .field("key_id", &self.key_id)
            .field("key_bits", &self.key_bits)
            .field("key", &"<redacted>")
            .finish()
    }
}
