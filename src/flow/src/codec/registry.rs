use super::decoder::{JsonDecoder, RecordDecoder};
use super::encoder::CollectionEncoder;
use super::CodecError;
use crate::catalog::StreamDecoderConfig;
use crate::codec::encoder::JsonEncoder;
use crate::planner::sink::SinkEncoderConfig;
use datatypes::Schema;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

type EncoderFactory =
    Arc<dyn Fn(&SinkEncoderConfig) -> Result<Arc<dyn CollectionEncoder>, CodecError> + Send + Sync>;

struct EncoderEntry {
    factory: EncoderFactory,
    supports_streaming: bool,
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
        Self::new()
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
        self.factories
            .write()
            .expect("decoder registry poisoned")
            .insert(kind.into(), factory);
    }

    pub fn instantiate(
        &self,
        config: &StreamDecoderConfig,
        stream_name: &str,
        schema: Arc<Schema>,
    ) -> Result<Arc<dyn RecordDecoder>, CodecError> {
        let guard = self.factories.read().expect("decoder registry poisoned");
        let factory = guard.get(config.kind()).ok_or_else(|| {
            CodecError::Other(format!("decoder kind `{}` not registered", config.kind()))
        })?;
        factory(config, schema, stream_name)
    }

    fn register_builtin_decoders(&self) {
        self.register_decoder(
            "json",
            Arc::new(|_config, schema, stream_name| {
                Ok(Arc::new(JsonDecoder::new(stream_name.to_string(), schema)) as Arc<_>)
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

    pub fn register_encoder(
        &self,
        kind: impl Into<String>,
        factory: EncoderFactory,
        supports_streaming: bool,
    ) {
        self.factories
            .write()
            .expect("encoder registry poisoned")
            .insert(
                kind.into(),
                EncoderEntry {
                    factory,
                    supports_streaming,
                },
            );
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
        (factory.factory)(config)
    }

    pub fn supports_streaming(&self, kind: &str) -> bool {
        let guard = self.factories.read().expect("encoder registry poisoned");
        guard
            .get(kind)
            .map(|entry| entry.supports_streaming)
            .unwrap_or(false)
    }

    fn register_builtin_encoders(&self) {
        self.register_encoder(
            "json",
            Arc::new(|config| Ok(Arc::new(JsonEncoder::new(config.kind().to_string())) as Arc<_>)),
            true,
        );
    }
}
