use super::decoder::{proto::ProtobufDecoder, JsonDecoder, RecordDecoder};
use super::encoder::SinkEncoderFactory;
use super::CodecError;
use crate::catalog::StreamDecoderConfig;
use crate::codec::encoder::{CsvEncoder, JsonEncoder, ProtobufEncoder};
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
        self.register_decoder(
            "protobuf",
            Arc::new(|config, schema, stream_name| {
                let bundle = config.proto_bundle.as_ref().ok_or_else(|| {
                    CodecError::Other("protobuf decoder requires a proto descriptor bundle".into())
                })?;
                Ok(Arc::new(ProtobufDecoder::new(
                    stream_name.to_string(),
                    schema,
                    Arc::clone(bundle),
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
        self.factories
            .write()
            .insert(kind.into(), EncoderEntry { factory });
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

    fn register_builtin_encoders(&self) {
        self.register_encoder(
            "csv",
            Arc::new(|config| {
                Ok(Arc::new(
                    CsvEncoder::new(config.kind_str().to_string(), config)
                        .map_err(|err| CodecError::Other(err.to_string()))?,
                ) as Arc<_>)
            }),
        );
        self.register_encoder(
            "json",
            Arc::new(|config| {
                Ok(Arc::new(
                    JsonEncoder::new(config.kind_str().to_string(), config)
                        .map_err(|err| CodecError::Other(err.to_string()))?,
                ) as Arc<_>)
            }),
        );
        self.register_encoder(
            "protobuf",
            Arc::new(|config| {
                let bundle = config.proto_bundle().ok_or_else(|| {
                    CodecError::Other(
                        "protobuf encoder requires a proto descriptor bundle (schema_ref)"
                            .to_string(),
                    )
                })?;
                Ok(Arc::new(ProtobufEncoder::new(Arc::clone(bundle))) as Arc<_>)
            }),
        );
    }
}

use super::{Merger, MergerOutputKind};
use serde_json::{Map, Value};
use std::any::Any;

type MergerFactory = Arc<
    dyn Fn(
            &Map<String, Value>,
            Arc<Schema>,
            Option<Arc<dyn Any + Send + Sync>>,
        ) -> Result<Box<dyn Merger>, CodecError>
        + Send
        + Sync,
>;

struct MergerRegistration {
    factory: MergerFactory,
    output_kind: MergerOutputKind,
}

/// Registry mapping merger identifiers to factories.
pub struct MergerRegistry {
    registrations: RwLock<HashMap<String, MergerRegistration>>,
}

impl MergerRegistry {
    pub fn new() -> Self {
        Self {
            registrations: RwLock::new(HashMap::new()),
        }
    }

    pub fn register<F>(&self, name: impl Into<String>, output_kind: MergerOutputKind, factory: F)
    where
        F: Fn(&Map<String, Value>, Arc<Schema>) -> Result<Box<dyn Merger>, CodecError>
            + Send
            + Sync
            + 'static,
    {
        self.registrations.write().insert(
            name.into(),
            MergerRegistration {
                factory: Arc::new(move |props, schema, _| factory(props, schema)),
                output_kind,
            },
        );
    }

    pub fn register_with_schema_artifact<F>(
        &self,
        name: impl Into<String>,
        output_kind: MergerOutputKind,
        factory: F,
    ) where
        F: Fn(
                &Map<String, Value>,
                Arc<Schema>,
                Option<Arc<dyn Any + Send + Sync>>,
            ) -> Result<Box<dyn Merger>, CodecError>
            + Send
            + Sync
            + 'static,
    {
        self.registrations.write().insert(
            name.into(),
            MergerRegistration {
                factory: Arc::new(factory),
                output_kind,
            },
        );
    }

    pub fn output_kind(&self, name: &str) -> Result<MergerOutputKind, CodecError> {
        self.registrations
            .read()
            .get(name)
            .map(|registration| registration.output_kind)
            .ok_or_else(|| CodecError::Other(format!("merger '{name}' not registered")))
    }

    pub fn instantiate(
        &self,
        name: &str,
        props: &Map<String, Value>,
        schema: Arc<Schema>,
    ) -> Result<Box<dyn Merger>, CodecError> {
        let map = self.registrations.read();
        if let Some(registration) = map.get(name) {
            (registration.factory)(props, schema, None)
        } else {
            Err(CodecError::Other(format!(
                "merger '{}' not registered",
                name
            )))
        }
    }

    pub fn instantiate_with_schema_artifact(
        &self,
        name: &str,
        props: &Map<String, Value>,
        schema: Arc<Schema>,
        artifact: Option<Arc<dyn Any + Send + Sync>>,
    ) -> Result<Box<dyn Merger>, CodecError> {
        let map = self.registrations.read();
        if let Some(registration) = map.get(name) {
            (registration.factory)(props, schema, artifact)
        } else {
            Err(CodecError::Other(format!("merger '{name}' not registered")))
        }
    }
}

impl Default for MergerRegistry {
    fn default() -> Self {
        Self::new()
    }
}
