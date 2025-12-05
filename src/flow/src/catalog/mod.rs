use datatypes::Schema;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

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
}

/// Supported stream types recognized by the catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamType {
    /// Stream backed by an MQTT source.
    Mqtt,
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

/// Complete definition for a stream tracked by the catalog.
#[derive(Debug, Clone)]
pub struct StreamDefinition {
    id: String,
    stream_type: StreamType,
    schema: Arc<Schema>,
    props: StreamProps,
    decoder: StreamDecoderConfig,
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
        };
        Self {
            id: id.into(),
            stream_type,
            schema,
            props,
            decoder,
        }
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
}

/// Configuration describing which decoder should be used for a stream's payloads.
#[derive(Debug, Clone)]
pub struct StreamDecoderConfig {
    decoder_id: String,
}

impl StreamDecoderConfig {
    pub fn new(decoder_id: impl Into<String>) -> Self {
        Self {
            decoder_id: decoder_id.into(),
        }
    }

    pub fn decoder_id(&self) -> &str {
        &self.decoder_id
    }

    pub fn kind(&self) -> &str {
        "json"
    }

    pub fn json_default(stream_id: &str) -> Self {
        Self::new(format!("{stream_id}_decoder"))
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
