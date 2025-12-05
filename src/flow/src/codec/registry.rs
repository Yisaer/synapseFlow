use super::encoder::CollectionEncoder;
use super::CodecError;
use crate::codec::encoder::JsonEncoder;
use crate::planner::sink::SinkEncoderConfig;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

type EncoderFactory =
    Arc<dyn Fn(&SinkEncoderConfig) -> Result<Arc<dyn CollectionEncoder>, CodecError> + Send + Sync>;

/// Registry mapping encoder identifiers to factories.
pub struct EncoderRegistry {
    factories: RwLock<HashMap<String, EncoderFactory>>,
}

impl Default for EncoderRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl EncoderRegistry {
    pub fn new() -> Self {
        Self {
            factories: RwLock::new(HashMap::new()),
        }
    }

    pub fn with_builtin_encoders() -> Arc<Self> {
        let registry = Arc::new(Self::new());
        registry.register_builtin_encoders();
        registry
    }

    pub fn register_encoder(&self, kind: impl Into<String>, factory: EncoderFactory) {
        self.factories
            .write()
            .expect("encoder registry poisoned")
            .insert(kind.into(), factory);
    }

    pub fn instantiate(
        &self,
        kind: &str,
        config: &SinkEncoderConfig,
    ) -> Result<Arc<dyn CollectionEncoder>, CodecError> {
        let guard = self.factories.read().expect("encoder registry poisoned");
        let factory = guard
            .get(kind)
            .ok_or_else(|| CodecError::Other(format!("encoder kind `{kind}` not registered")))?;
        factory(config)
    }

    fn register_builtin_encoders(&self) {
        self.register_encoder(
            "json",
            Arc::new(|config| match config {
                SinkEncoderConfig::Json { encoder_id } => {
                    Ok(Arc::new(JsonEncoder::new(encoder_id.clone())) as Arc<_>)
                }
                other => Err(CodecError::Other(format!(
                    "encoder config mismatch, expected json but received {:?}",
                    other.kind()
                ))),
            }),
        );
    }
}
