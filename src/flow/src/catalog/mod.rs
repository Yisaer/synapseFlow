use datatypes::Schema;
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

mod function_catalog;
mod functions;

pub use function_catalog::{
    AggregateFunctionSpec, FunctionArgSpec, FunctionContext, FunctionDef, FunctionKind,
    FunctionRequirement, FunctionSignatureSpec, StatefulFunctionSpec, StructFieldSpec, TypeSpec,
};
pub use functions::{describe_function_def, list_function_defs};

/// Errors that can occur when mutating the catalog.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CatalogError {
    #[error("stream already exists: {0}")]
    AlreadyExists(String),
    #[error("stream not found: {0}")]
    NotFound(String),
}

/// Additional metadata associated with a stream definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamProps {
    /// Stream is backed by an MQTT connector.
    Mqtt(MqttStreamProps),
    /// Stream is backed by an in-memory mock connector (tests only).
    Mock(MockStreamProps),
    /// Stream is backed by a History source (Parquet files).
    History(HistoryStreamProps),
}

/// Supported stream types recognized by the catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamType {
    /// Stream backed by an MQTT source.
    Mqtt,
    /// Stream backed by a mock source.
    Mock,
    /// Stream backed by a history source.
    History,
}

/// Properties for MQTT-backed streams.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MqttStreamProps {
    pub broker_url: String,
    pub topic: String,
    pub qos: u8,
    pub client_id: Option<String>,
    pub connector_key: Option<String>,
}

impl MqttStreamProps {
    pub fn new(broker_url: impl Into<String>, topic: impl Into<String>, qos: u8) -> Self {
        Self {
            broker_url: broker_url.into(),
            topic: topic.into(),
            qos,
            client_id: None,
            connector_key: None,
        }
    }

    pub fn with_client_id(mut self, id: impl Into<String>) -> Self {
        self.client_id = Some(id.into());
        self
    }

    pub fn with_connector_key(mut self, key: impl Into<String>) -> Self {
        self.connector_key = Some(key.into());
        self
    }
}

/// Properties for mock-backed streams.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MockStreamProps {}

/// Properties for history-backed streams.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HistoryStreamProps {
    pub datasource: String,
    pub topic: String,
    pub start: Option<i64>,
    pub end: Option<i64>,
    pub batch_size: Option<usize>,
    pub send_interval: Option<std::time::Duration>,
    pub decrypt_method: Option<String>,
    pub decrypt_props: Option<JsonMap<String, JsonValue>>,
}

/// Complete definition for a stream tracked by the catalog.
#[derive(Debug, Clone)]
pub struct StreamDefinition {
    id: String,
    stream_type: StreamType,
    schema: Arc<Schema>,
    props: StreamProps,
    decoder: StreamDecoderConfig,
    eventtime: Option<EventtimeDefinition>,
}

/// Event-time configuration for a stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventtimeDefinition {
    column: String,
    eventtime_type: String,
}

impl EventtimeDefinition {
    pub fn new(column: impl Into<String>, eventtime_type: impl Into<String>) -> Self {
        Self {
            column: column.into(),
            eventtime_type: eventtime_type.into(),
        }
    }

    pub fn column(&self) -> &str {
        &self.column
    }

    pub fn eventtime_type(&self) -> &str {
        &self.eventtime_type
    }
}

impl StreamDefinition {
    pub fn new(
        id: impl Into<String>,
        schema: Arc<Schema>,
        props: StreamProps,
        decoder: StreamDecoderConfig,
    ) -> Self {
        let stream_type = match props {
            StreamProps::Mqtt(_) => StreamType::Mqtt,
            StreamProps::Mock(_) => StreamType::Mock,
            StreamProps::History(_) => StreamType::History,
        };
        Self {
            id: id.into(),
            stream_type,
            schema,
            props,
            decoder,
            eventtime: None,
        }
    }

    pub fn with_eventtime(mut self, eventtime: EventtimeDefinition) -> Self {
        self.eventtime = Some(eventtime);
        self
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn stream_type(&self) -> StreamType {
        self.stream_type
    }

    pub fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    pub fn props(&self) -> &StreamProps {
        &self.props
    }

    pub fn decoder(&self) -> &StreamDecoderConfig {
        &self.decoder
    }

    pub fn eventtime(&self) -> Option<&EventtimeDefinition> {
        self.eventtime.as_ref()
    }
}

/// Configuration describing which decoder should be used for a stream's payloads.
#[derive(Debug, Clone)]
pub struct StreamDecoderConfig {
    pub decode_type: String,
    pub props: JsonMap<String, JsonValue>,
}

impl StreamDecoderConfig {
    pub fn new(decode_type: impl Into<String>, props: JsonMap<String, JsonValue>) -> Self {
        Self {
            decode_type: decode_type.into(),
            props,
        }
    }

    pub fn kind(&self) -> &str {
        &self.decode_type
    }

    pub fn props(&self) -> &JsonMap<String, JsonValue> {
        &self.props
    }

    pub fn json() -> Self {
        Self::new("json", JsonMap::new())
    }
}

#[derive(Default)]
pub struct Catalog {
    streams: RwLock<HashMap<String, Arc<StreamDefinition>>>,
}

impl Catalog {
    pub fn new() -> Self {
        Self {
            streams: RwLock::new(HashMap::new()),
        }
    }

    pub fn get(&self, stream_id: &str) -> Option<Arc<StreamDefinition>> {
        let guard = self.streams.read().expect("catalog poisoned");
        guard.get(stream_id).cloned()
    }

    pub fn list(&self) -> Vec<Arc<StreamDefinition>> {
        let guard = self.streams.read().expect("catalog poisoned");
        guard.values().cloned().collect()
    }

    pub fn insert(
        &self,
        definition: StreamDefinition,
    ) -> Result<Arc<StreamDefinition>, CatalogError> {
        let mut guard = self.streams.write().expect("catalog poisoned");
        let stream_id = definition.id().to_string();
        if guard.contains_key(&stream_id) {
            return Err(CatalogError::AlreadyExists(stream_id));
        }
        let definition = Arc::new(definition);
        guard.insert(stream_id, definition.clone());
        Ok(definition)
    }

    pub fn upsert(&self, definition: StreamDefinition) -> Arc<StreamDefinition> {
        let mut guard = self.streams.write().expect("catalog poisoned");
        let stream_id = definition.id().to_string();
        let definition = Arc::new(definition);
        guard.insert(stream_id, definition.clone());
        definition
    }

    pub fn remove(&self, stream_id: &str) -> Result<(), CatalogError> {
        let mut guard = self.streams.write().expect("catalog poisoned");
        guard
            .remove(stream_id)
            .map(|_| ())
            .ok_or_else(|| CatalogError::NotFound(stream_id.to_string()))
    }
}
