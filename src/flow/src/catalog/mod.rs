use crate::processor::SamplerConfig;
use datatypes::Schema;
use parking_lot::RwLock;
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

mod function_catalog;
mod functions;

pub use function_catalog::{
    AggregateFunctionSpec, FunctionArgSpec, FunctionContext, FunctionDef, FunctionKind,
    FunctionRequirement, FunctionSignatureSpec, StatefulFunctionSpec, StructFieldSpec, TypeSpec,
};
pub use functions::{describe_function_def, list_function_defs};

/// Message framing used by file-backed streams.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum FileSourceFraming {
    /// Emit all bytes observed during one file-change handling pass as one payload.
    #[default]
    AppendBatch,
    /// Split payloads using a byte delimiter.
    Delimiter {
        delimiter: Vec<u8>,
        include_delimiter: bool,
    },
}

/// Errors that can occur when mutating the catalog.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CatalogError {
    #[error("stream already exists: {0}")]
    AlreadyExists(String),
    #[error("stream not found: {0}")]
    NotFound(String),
    #[error("table already exists: {0}")]
    TableAlreadyExists(String),
    #[error("table not found: {0}")]
    TableNotFound(String),
    #[error("relation name is ambiguous between stream and table: {0}")]
    AmbiguousRelation(String),
}

/// Additional metadata associated with a stream definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamProps {
    /// Stream is backed by an MQTT connector.
    Mqtt(MqttStreamProps),
    /// Stream is backed by a video source.
    Video(VideoStreamProps),
    /// Stream is backed by an in-memory mock connector (tests only).
    Mock(MockStreamProps),
    /// Stream is backed by a History source (Parquet files).
    History(HistoryStreamProps),
    /// Stream is backed by an in-process memory pub/sub topic.
    Memory(MemoryStreamProps),
    /// Stream is backed by an NNG pub/sub subscriber.
    NngPubSub(NngPubSubStreamProps),
    /// Stream is backed by file append notifications.
    File(FileStreamProps),
}

/// Supported stream types recognized by the catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamType {
    /// Stream backed by an MQTT source.
    Mqtt,
    /// Stream backed by a video source.
    Video,
    /// Stream backed by a mock source.
    Mock,
    /// Stream backed by a history source.
    History,
    /// Stream backed by a memory source.
    Memory,
    /// Stream backed by an NNG pub/sub source.
    NngPubSub,
    /// Stream backed by file append notifications.
    File,
}

/// Additional metadata associated with a table definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableProps {
    /// Table backed by historical Parquet files.
    History(HistoryTableProps),
}

/// Supported table types recognized by the catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableType {
    /// Table backed by historical Parquet files.
    History,
}

/// Capabilities advertised by a table provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableCapabilities {
    pub scan: Option<TableScanCapability>,
}

impl TableCapabilities {
    pub fn history() -> Self {
        Self {
            scan: Some(TableScanCapability {}),
        }
    }
}

/// Scan capability metadata for a table provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableScanCapability {}

/// Properties for history-backed tables.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryTableProps {
    pub datasource: String,
    pub topic: String,
    pub time_column: String,
    pub batch_size: Option<usize>,
}

impl HistoryTableProps {
    pub fn new(datasource: impl Into<String>, topic: impl Into<String>) -> Self {
        Self {
            datasource: datasource.into(),
            topic: topic.into(),
            time_column: "ts".to_string(),
            batch_size: None,
        }
    }

    pub fn with_time_column(mut self, time_column: impl Into<String>) -> Self {
        self.time_column = time_column.into();
        self
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = Some(batch_size);
        self
    }
}

/// Properties for MQTT-backed streams.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MqttStreamProps {
    pub broker_url: String,
    pub topic: String,
    pub qos: u8,
    pub client_id: Option<String>,
    pub connector_key: Option<String>,
    pub protocol_version: Option<crate::connector::MqttProtocolVersion>,
}

impl MqttStreamProps {
    pub fn new(broker_url: impl Into<String>, topic: impl Into<String>, qos: u8) -> Self {
        Self {
            broker_url: broker_url.into(),
            topic: topic.into(),
            qos,
            client_id: None,
            connector_key: None,
            protocol_version: None,
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

    pub fn with_protocol_version(
        mut self,
        protocol_version: crate::connector::MqttProtocolVersion,
    ) -> Self {
        self.protocol_version = Some(protocol_version);
        self
    }
}

/// Properties for file-backed streams.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileStreamProps {
    pub path: String,
    pub framing: FileSourceFraming,
}

impl FileStreamProps {
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            framing: FileSourceFraming::default(),
        }
    }

    pub fn with_framing(mut self, framing: FileSourceFraming) -> Self {
        self.framing = framing;
        self
    }
}

/// Properties for video streams.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VideoStreamProps {
    pub url: String,
    pub rtsp_transport: VideoRtspTransport,
    pub reconnect: VideoReconnectConfig,
}

/// RTSP transport mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum VideoRtspTransport {
    #[default]
    Tcp,
    Udp,
}

impl VideoRtspTransport {
    pub fn as_str(&self) -> &'static str {
        match self {
            VideoRtspTransport::Tcp => "tcp",
            VideoRtspTransport::Udp => "udp",
        }
    }
}

/// Reconnect behavior for live video sources.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VideoReconnectConfig {
    pub enabled: bool,
    pub initial_delay: std::time::Duration,
    pub max_delay: std::time::Duration,
}

impl Default for VideoReconnectConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            initial_delay: std::time::Duration::from_secs(1),
            max_delay: std::time::Duration::from_secs(30),
        }
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

/// Properties for memory-backed streams.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryStreamProps {
    pub topic: String,
}

impl MemoryStreamProps {
    pub fn new(topic: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
        }
    }
}

/// Properties for NNG pub/sub-backed streams.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NngPubSubStreamProps {
    pub url: String,
    pub topic: String,
    pub topic_delimiter: String,
}

impl NngPubSubStreamProps {
    pub fn new(url: impl Into<String>, topic: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            topic: topic.into(),
            topic_delimiter: crate::connector::nng_pubsub::DEFAULT_TOPIC_DELIMITER.to_string(),
        }
    }

    pub fn with_topic_delimiter(mut self, topic_delimiter: impl Into<String>) -> Self {
        self.topic_delimiter = topic_delimiter.into();
        self
    }
}

/// Complete definition for a stream tracked by the catalog.
#[derive(Debug, Clone)]
pub struct StreamDefinition {
    id: String,
    stream_type: StreamType,
    schema: Arc<Schema>,
    /// Revision of the referenced named schema.
    ///
    /// Inline schemas do not have an independent revision and leave this unset.
    schema_version: Option<u64>,
    props: StreamProps,
    decoder: StreamDecoderConfig,
    eventtime: Option<EventtimeDefinition>,
    /// Optional sampler configuration for stream-level downsampling.
    sampler: Option<SamplerConfig>,
}

/// Complete definition for a table tracked by the catalog.
#[derive(Debug, Clone)]
pub struct TableDefinition {
    id: String,
    table_type: TableType,
    schema: Arc<Schema>,
    props: TableProps,
    decoder: StreamDecoderConfig,
    capabilities: TableCapabilities,
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
            StreamProps::Video(_) => StreamType::Video,
            StreamProps::Mock(_) => StreamType::Mock,
            StreamProps::History(_) => StreamType::History,
            StreamProps::Memory(_) => StreamType::Memory,
            StreamProps::NngPubSub(_) => StreamType::NngPubSub,
            StreamProps::File(_) => StreamType::File,
        };
        Self {
            id: id.into(),
            stream_type,
            schema,
            schema_version: None,
            props,
            decoder,
            eventtime: None,
            sampler: None,
        }
    }

    pub fn with_eventtime(mut self, eventtime: EventtimeDefinition) -> Self {
        self.eventtime = Some(eventtime);
        self
    }

    pub fn with_sampler(mut self, sampler: SamplerConfig) -> Self {
        self.sampler = Some(sampler);
        self
    }

    pub fn with_schema_version(mut self, schema_version: u64) -> Self {
        self.schema_version = Some(schema_version);
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

    pub fn schema_version(&self) -> Option<u64> {
        self.schema_version
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

    pub fn sampler(&self) -> Option<&SamplerConfig> {
        self.sampler.as_ref()
    }
}

impl TableDefinition {
    pub fn new(
        id: impl Into<String>,
        schema: Arc<Schema>,
        props: TableProps,
        decoder: StreamDecoderConfig,
    ) -> Self {
        let table_type = match props {
            TableProps::History(_) => TableType::History,
        };
        let capabilities = match table_type {
            TableType::History => TableCapabilities::history(),
        };
        Self {
            id: id.into(),
            table_type,
            schema,
            props,
            decoder,
            capabilities,
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn table_type(&self) -> TableType {
        self.table_type
    }

    pub fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    pub fn props(&self) -> &TableProps {
        &self.props
    }

    pub fn decoder(&self) -> &StreamDecoderConfig {
        &self.decoder
    }

    pub fn capabilities(&self) -> &TableCapabilities {
        &self.capabilities
    }
}

/// Configuration describing which decoder should be used for a stream's payloads.
#[derive(Clone)]
pub struct StreamDecoderConfig {
    pub decode_type: String,
    pub props: JsonMap<String, JsonValue>,
    /// Pre-built proto descriptor bundle, set by the manager when the decoder kind is
    /// `"protobuf"` and the schema is proto-based. Multiple streams referencing the same
    /// proto message type share the same `Arc`.
    pub proto_bundle: Option<Arc<crate::codec::ProtoDescriptorBundle>>,
    /// Parser-specific, immutable schema artifact shared by every stream that
    /// references the same named schema.
    pub schema_artifact: Option<Arc<dyn Any + Send + Sync>>,
}

impl std::fmt::Debug for StreamDecoderConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamDecoderConfig")
            .field("decode_type", &self.decode_type)
            .field("props", &self.props)
            .field(
                "proto_bundle",
                &self.proto_bundle.as_ref().map(|_| "<bundle>"),
            )
            .field(
                "schema_artifact",
                &self.schema_artifact.as_ref().map(|_| "<artifact>"),
            )
            .finish()
    }
}

impl StreamDecoderConfig {
    pub fn new(decode_type: impl Into<String>, props: JsonMap<String, JsonValue>) -> Self {
        Self {
            decode_type: decode_type.into(),
            props,
            proto_bundle: None,
            schema_artifact: None,
        }
    }

    pub fn with_proto_bundle(mut self, bundle: Arc<crate::codec::ProtoDescriptorBundle>) -> Self {
        self.proto_bundle = Some(bundle);
        self
    }

    pub fn with_schema_artifact(mut self, artifact: Arc<dyn Any + Send + Sync>) -> Self {
        self.schema_artifact = Some(artifact);
        self
    }

    pub fn schema_artifact<T: Any + Send + Sync>(&self) -> Option<Arc<T>> {
        Arc::clone(self.schema_artifact.as_ref()?)
            .downcast::<T>()
            .ok()
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

    pub fn none() -> Self {
        Self::new("none", JsonMap::new())
    }
}

#[derive(Default)]
pub struct Catalog {
    streams: RwLock<HashMap<String, Arc<StreamDefinition>>>,
    tables: RwLock<HashMap<String, Arc<TableDefinition>>>,
}

#[derive(Debug, Clone)]
pub enum CatalogRelation {
    Stream(Arc<StreamDefinition>),
    Table(Arc<TableDefinition>),
}

impl Catalog {
    pub fn new() -> Self {
        Self {
            streams: RwLock::new(HashMap::new()),
            tables: RwLock::new(HashMap::new()),
        }
    }

    pub fn get(&self, stream_id: &str) -> Option<Arc<StreamDefinition>> {
        let guard = self.streams.read();
        guard.get(stream_id).cloned()
    }

    pub fn list(&self) -> Vec<Arc<StreamDefinition>> {
        let guard = self.streams.read();
        guard.values().cloned().collect()
    }

    pub fn insert(
        &self,
        definition: StreamDefinition,
    ) -> Result<Arc<StreamDefinition>, CatalogError> {
        let mut guard = self.streams.write();
        let stream_id = definition.id().to_string();
        if guard.contains_key(&stream_id) {
            return Err(CatalogError::AlreadyExists(stream_id));
        }
        let definition = Arc::new(definition);
        guard.insert(stream_id, definition.clone());
        Ok(definition)
    }

    pub fn upsert(&self, definition: StreamDefinition) -> Arc<StreamDefinition> {
        let mut guard = self.streams.write();
        let stream_id = definition.id().to_string();
        let definition = Arc::new(definition);
        guard.insert(stream_id, definition.clone());
        definition
    }

    pub fn remove(&self, stream_id: &str) -> Result<(), CatalogError> {
        let mut guard = self.streams.write();
        guard
            .remove(stream_id)
            .map(|_| ())
            .ok_or_else(|| CatalogError::NotFound(stream_id.to_string()))
    }

    pub fn get_table(&self, table_id: &str) -> Option<Arc<TableDefinition>> {
        let guard = self.tables.read();
        guard.get(table_id).cloned()
    }

    pub fn list_tables(&self) -> Vec<Arc<TableDefinition>> {
        let guard = self.tables.read();
        guard.values().cloned().collect()
    }

    pub fn insert_table(
        &self,
        definition: TableDefinition,
    ) -> Result<Arc<TableDefinition>, CatalogError> {
        let mut guard = self.tables.write();
        let table_id = definition.id().to_string();
        if guard.contains_key(&table_id) {
            return Err(CatalogError::TableAlreadyExists(table_id));
        }
        let definition = Arc::new(definition);
        guard.insert(table_id, definition.clone());
        Ok(definition)
    }

    pub fn upsert_table(&self, definition: TableDefinition) -> Arc<TableDefinition> {
        let mut guard = self.tables.write();
        let table_id = definition.id().to_string();
        let definition = Arc::new(definition);
        guard.insert(table_id, definition.clone());
        definition
    }

    pub fn remove_table(&self, table_id: &str) -> Result<(), CatalogError> {
        let mut guard = self.tables.write();
        guard
            .remove(table_id)
            .map(|_| ())
            .ok_or_else(|| CatalogError::TableNotFound(table_id.to_string()))
    }

    pub fn resolve_relation(&self, name: &str) -> Result<Option<CatalogRelation>, CatalogError> {
        let stream = self.get(name);
        let table = self.get_table(name);
        match (stream, table) {
            (Some(_), Some(_)) => Err(CatalogError::AmbiguousRelation(name.to_string())),
            (Some(stream), None) => Ok(Some(CatalogRelation::Stream(stream))),
            (None, Some(table)) => Ok(Some(CatalogRelation::Table(table))),
            (None, None) => Ok(None),
        }
    }
}
