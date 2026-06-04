use super::decoder::{JsonDecoder, RecordDecoder};
use super::encoder::SinkEncoderFactory;
use super::CodecError;
use crate::catalog::StreamDecoderConfig;
use crate::codec::encoder::JsonEncoder;
use crate::planner::sink::SinkEncoderConfig;
use datatypes::Schema;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

type EncoderFactory = Arc<
    dyn Fn(&SinkEncoderConfig) -> Result<Arc<dyn SinkEncoderFactory>, CodecError> + Send + Sync,
>;

struct EncoderEntry {
    factory: EncoderFactory,
    supports_by_index_projection: bool,
}
type DecoderFactory = Arc<
    dyn Fn(&StreamDecoderConfig, Arc<Schema>, &str) -> Result<Arc<dyn RecordDecoder>, CodecError>
        + Send
        + Sync,
>;

/// Registry mapping decoder identifiers to factories.
pub struct DecoderRegistry {
    factories: RwLock<HashMap<String, DecoderFactory>>,
}

impl Default for DecoderRegistry {
    fn default() -> Self {
        let registry = Self::new();
        registry.register_builtin_decoders();
        registry
    }
}

impl DecoderRegistry {
    pub fn new() -> Self {
        Self {
            factories: RwLock::new(HashMap::new()),
        }
    }

    pub fn with_builtin_decoders() -> Arc<Self> {
        let registry = Arc::new(Self::new());
        registry.register_builtin_decoders();
        registry
    }

    pub fn register_decoder(&self, kind: impl Into<String>, factory: DecoderFactory) {
        self.factories.write().insert(kind.into(), factory);
    }

    pub fn instantiate(
        &self,
        config: &StreamDecoderConfig,
        stream_name: &str,
        schema: Arc<Schema>,
    ) -> Result<Arc<dyn RecordDecoder>, CodecError> {
        let guard = self.factories.read();
        let factory = guard.get(config.kind()).ok_or_else(|| {
            CodecError::Other(format!("decoder kind `{}` not registered", config.kind()))
        })?;
        factory(config, schema, stream_name)
    }

    pub fn is_registered(&self, kind: &str) -> bool {
        let guard = self.factories.read();
        guard.contains_key(kind)
    }

    fn register_builtin_decoders(&self) {
        self.register_decoder(
            "json",
            Arc::new(|config, schema, stream_name| {
                Ok(Arc::new(JsonDecoder::new(
                    stream_name.to_string(),
                    schema,
                    config.props().clone(),
                )) as Arc<_>)
            }),
        );
    }
}

/// Registry mapping encoder identifiers to factories.
pub struct EncoderRegistry {
    factories: RwLock<HashMap<String, EncoderEntry>>,
}

impl Default for EncoderRegistry {
    fn default() -> Self {
        let registry = Self::new();
        registry.register_builtin_encoders();
        registry
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
        self.register_encoder_with_caps(kind, factory, false);
    }

    pub fn register_encoder_with_caps(
        &self,
        kind: impl Into<String>,
        factory: EncoderFactory,
        supports_by_index_projection: bool,
    ) {
        self.factories.write().insert(
            kind.into(),
            EncoderEntry {
                factory,
                supports_by_index_projection,
            },
        );
    }

    pub fn instantiate(
        &self,
        config: &SinkEncoderConfig,
    ) -> Result<Arc<dyn SinkEncoderFactory>, CodecError> {
        let guard = self.factories.read();
        let kind = config.kind_str();
        let factory = guard
            .get(kind)
            .ok_or_else(|| CodecError::Other(format!("encoder kind `{kind}` not registered")))?;
        (factory.factory)(config)
    }

    pub fn is_registered(&self, kind: &str) -> bool {
        let guard = self.factories.read();
        guard.contains_key(kind)
    }

    pub fn supports_by_index_projection(&self, kind: &str) -> bool {
        let guard = self.factories.read();
        guard
            .get(kind)
            .map(|entry| entry.supports_by_index_projection)
            .unwrap_or(false)
    }

    fn register_builtin_encoders(&self) {
        self.register_encoder_with_caps(
            "json",
            Arc::new(|config| {
                Ok(Arc::new(
                    JsonEncoder::new(config.kind_str().to_string(), config)
                        .map_err(|err| CodecError::Other(err.to_string()))?,
                ) as Arc<_>)
            }),
            true,
        );
    }
}

use super::Merger;
use serde_json::{Map, Value};

type MergerFactory =
    Arc<dyn Fn(&Map<String, Value>) -> Result<Box<dyn Merger>, CodecError> + Send + Sync>;

/// Registry mapping merger identifiers to factories.
pub struct MergerRegistry {
    factories: RwLock<HashMap<String, MergerFactory>>,
}

impl MergerRegistry {
    pub fn new() -> Self {
        Self {
            factories: RwLock::new(HashMap::new()),
        }
    }

    pub fn register<F>(&self, name: impl Into<String>, factory: F)
    where
        F: Fn(&Map<String, Value>) -> Result<Box<dyn Merger>, CodecError> + Send + Sync + 'static,
    {
        let mut map = self.factories.write();
        map.insert(name.into(), Arc::new(factory));
    }

    pub fn instantiate(
        &self,
        name: &str,
        props: &Map<String, Value>,
    ) -> Result<Box<dyn Merger>, CodecError> {
        let map = self.factories.read();
        if let Some(factory) = map.get(name) {
            factory(props)
        } else {
            Err(CodecError::Other(format!(
                "merger '{}' not registered",
                name
            )))
        }
    }
}

impl Default for MergerRegistry {
    fn default() -> Self {
        Self::new()
    }
}
