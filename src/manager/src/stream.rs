use crate::MQTT_QOS;
use crate::audit::ResourceMutationLog;
use crate::instances::DEFAULT_FLOW_INSTANCE_ID;
use crate::pipeline::AppState;
use crate::storage_bridge;
use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use flow::DecoderRegistry;
use flow::catalog::{
    CatalogError, EventtimeDefinition, HistoryStreamProps, MemoryStreamProps, MockStreamProps,
    MqttStreamProps, NngPubSubStreamProps, StreamDecoderConfig, VideoReconnectConfig,
    VideoRtspTransport, VideoStreamProps,
};
use flow::processor::ProcessorStatsEntry;
use flow::processor::SamplerConfig;
use flow::shared_stream::{SharedStreamError, SharedStreamInfo, SharedStreamStatus};
use flow::{FlowInstanceError, Schema, StreamDefinition, StreamProps, StreamRuntimeInfo};
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;
use tokio::sync::TryAcquireError;

use flow::{
    BooleanType, BytesType, ColumnSchema, ConcreteDatatype, Float32Type, Float64Type, Int8Type,
    Int16Type, Int32Type, Int64Type, ListType, StringType, StructField, StructType, TimestampType,
    Uint8Type, Uint16Type, Uint32Type, Uint64Type,
};
use storage::{StorageError, StorageManager, StoredMemoryTopicKind};

#[derive(Deserialize, Serialize, Clone)]
pub struct CreateStreamRequest {
    pub name: String,
    #[serde(rename = "type")]
    pub stream_type: String,
    #[serde(default)]
    pub schema: SchemaConfigRequest,
    #[serde(default)]
    pub props: StreamPropsRequest,
    #[serde(default)]
    pub shared: bool,
    #[serde(default)]
    pub decoder: DecoderConfigRequest,
    #[serde(default)]
    pub eventtime: Option<EventtimeConfigRequest>,
    #[serde(default)]
    pub sampler: Option<SamplerConfig>,
}

fn stream_busy_response(name: &str) -> axum::response::Response {
    (
        StatusCode::CONFLICT,
        format!("stream {name} is busy processing another command"),
    )
        .into_response()
}

impl CreateStreamRequest {
    pub(crate) fn normalize(&mut self) {
        if !self.stream_type.eq_ignore_ascii_case("mqtt") {
            return;
        }

        let normalized = match self.props.fields.get("connector_key") {
            Some(JsonValue::String(value)) => {
                let trimmed = value.trim();
                Some((!trimmed.is_empty()).then(|| JsonValue::String(trimmed.to_string())))
            }
            _ => None,
        };

        match normalized {
            Some(Some(value)) => {
                self.props.fields.insert("connector_key".to_string(), value);
            }
            Some(None) => {
                self.props.fields.remove("connector_key");
            }
            None => {}
        }
    }
}

#[derive(Deserialize, Serialize, Clone)]
pub struct EventtimeConfigRequest {
    pub column: String,
    #[serde(rename = "type")]
    pub eventtime_type: String,
}

#[derive(Deserialize, Serialize, Clone)]
#[serde(default)]
pub struct SchemaConfigRequest {
    #[serde(rename = "type")]
    pub schema_type: String,
    pub props: JsonMap<String, JsonValue>,
}

impl SchemaConfigRequest {
    fn new(schema_type: impl Into<String>, props: JsonMap<String, JsonValue>) -> Self {
        Self {
            schema_type: schema_type.into(),
            props,
        }
    }
}

impl Default for SchemaConfigRequest {
    fn default() -> Self {
        Self::new("json", JsonMap::new())
    }
}

#[derive(Deserialize, Serialize, Clone)]
#[serde(default)]
pub struct DecoderConfigRequest {
    #[serde(rename = "type")]
    pub decode_type: String,
    pub props: JsonMap<String, JsonValue>,
}

impl DecoderConfigRequest {
    fn new(decode_type: impl Into<String>, props: JsonMap<String, JsonValue>) -> Self {
        Self {
            decode_type: decode_type.into(),
            props,
        }
    }
}

impl Default for DecoderConfigRequest {
    fn default() -> Self {
        Self::new("json", JsonMap::new())
    }
}

pub type SchemaParser =
    dyn Fn(&str, &JsonMap<String, JsonValue>) -> Result<Schema, String> + Send + Sync;

/// Registry for schema parsers, enabling pluggable schema declaration formats.
pub struct SchemaRegistry {
    parsers: RwLock<HashMap<String, Arc<SchemaParser>>>,
}

impl Default for SchemaRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl SchemaRegistry {
    pub fn new() -> Self {
        Self {
            parsers: RwLock::new(HashMap::new()),
        }
    }

    pub fn with_builtin() -> Self {
        let registry = Self::new();
        registry.register_schema("json", Arc::new(parse_json_schema));
        registry
    }

    pub fn register_schema(&self, kind: impl Into<String>, parser: Arc<SchemaParser>) {
        self.parsers.write().insert(kind.into(), parser);
    }

    pub fn parse(
        &self,
        schema_type: &str,
        stream_name: &str,
        props: &JsonMap<String, JsonValue>,
    ) -> Result<Schema, String> {
        let guard = self.parsers.read();
        let parser = guard
            .get(schema_type)
            .ok_or_else(|| format!("schema type `{schema_type}` not registered"))?;
        parser(stream_name, props)
    }
}

static SCHEMA_REGISTRY: OnceLock<SchemaRegistry> = OnceLock::new();

/// Access the global schema registry (initialized with builtin schemas).
pub fn schema_registry() -> &'static SchemaRegistry {
    SCHEMA_REGISTRY.get_or_init(SchemaRegistry::with_builtin)
}

/// Register a custom schema parser into the global registry.
pub fn register_schema(kind: impl Into<String>, parser: Arc<SchemaParser>) {
    schema_registry().register_schema(kind, parser);
}

#[derive(Deserialize, Serialize, Clone)]
pub struct StreamSchemaRequest {
    pub columns: Vec<StreamColumnRequest>,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct StreamColumnRequest {
    pub name: String,
    pub data_type: String,
    #[serde(default)]
    pub fields: Option<Vec<StreamColumnRequest>>,
    #[serde(default)]
    pub element: Option<Box<StreamColumnRequest>>,
}

#[derive(Deserialize, Serialize, Default, Clone)]
#[serde(default)]
pub struct StreamPropsRequest {
    #[serde(flatten)]
    pub fields: JsonMap<String, JsonValue>,
}

impl StreamPropsRequest {
    fn to_value(&self) -> JsonValue {
        JsonValue::Object(self.fields.clone())
    }
}

#[derive(Deserialize, Serialize, Default, Clone)]
#[serde(default)]
pub struct MqttStreamPropsRequest {
    pub broker_url: Option<String>,
    pub topic: Option<String>,
    pub qos: Option<u8>,
    pub client_id: Option<String>,
    pub connector_key: Option<String>,
}

#[derive(Deserialize, Serialize, Default, Clone)]
#[serde(default)]
pub struct HistoryStreamPropsRequest {
    pub datasource: Option<String>,
    pub topic: Option<String>,
    pub start: Option<i64>,
    pub end: Option<i64>,
    pub batch_size: Option<usize>,
    pub send_interval_ms: Option<u64>,
}

#[derive(Deserialize, Serialize, Default, Clone)]
#[serde(default)]
pub struct MemoryStreamPropsRequest {
    pub topic: Option<String>,
}

#[derive(Deserialize, Serialize, Default, Clone)]
#[serde(default)]
pub struct NngPubSubStreamPropsRequest {
    pub url: Option<String>,
    pub topic: Option<String>,
    pub topic_delimiter: Option<String>,
    #[serde(rename = "topicDelimiter")]
    pub topic_delimiter_camel: Option<String>,
}

#[derive(Deserialize, Serialize, Default, Clone)]
#[serde(default)]
pub struct VideoStreamPropsRequest {
    pub url: Option<String>,
    pub rtsp_transport: Option<String>,
    pub reconnect: VideoReconnectRequest,
}

#[derive(Deserialize, Serialize, Clone)]
#[serde(default)]
pub struct VideoReconnectRequest {
    pub enabled: bool,
    pub initial_delay_ms: u64,
    pub max_delay_ms: u64,
}

impl Default for VideoReconnectRequest {
    fn default() -> Self {
        let defaults = VideoReconnectConfig::default();
        Self {
            enabled: defaults.enabled,
            initial_delay_ms: u64::try_from(defaults.initial_delay.as_millis()).unwrap_or(u64::MAX),
            max_delay_ms: u64::try_from(defaults.max_delay.as_millis()).unwrap_or(u64::MAX),
        }
    }
}

#[derive(Serialize)]
pub struct StreamInfo {
    pub name: String,
    pub shared: bool,
    pub schema: StreamSchemaInfo,
    pub shared_stream: Option<SharedStreamItem>,
}

#[derive(Serialize)]
pub struct StreamSchemaInfo {
    pub columns: Vec<StreamColumnInfo>,
}

#[derive(Serialize)]
pub struct StreamColumnInfo {
    pub name: String,
    pub data_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<StreamColumnInfo>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub element: Option<Box<StreamColumnInfo>>,
}

#[derive(Serialize)]
pub struct SharedStreamItem {
    pub id: String,
    pub status: String,
    pub status_message: Option<String>,
    pub connector_id: String,
    pub subscribers: usize,
    pub created_at_secs: u64,
}

#[derive(Serialize)]
pub struct DescribeStreamResponse {
    pub stream: String,
    pub spec_version: u32,
    pub spec: StreamDefinitionSpec,
}

#[derive(Serialize)]
pub struct StreamDefinitionSpec {
    #[serde(rename = "type")]
    pub stream_type: String,
    pub schema: StreamSchemaInfo,
    pub props: JsonValue,
    pub shared: bool,
    pub decoder: DecoderConfigRequest,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub eventtime: Option<EventtimeConfigRequest>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sampler: Option<SamplerConfig>,
}

#[derive(Deserialize, Serialize)]
pub struct SharedStreamStatsResponse {
    pub stream: String,
    pub flow_instance_id: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status_message: Option<String>,
    pub processors: Vec<ProcessorStatsEntry>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
pub struct SharedStreamStatsQuery {
    pub flow_instance_id: Option<String>,
}

pub async fn create_stream_handler(
    State(state): State<AppState>,
    Json(req): Json<CreateStreamRequest>,
) -> impl IntoResponse {
    let mut req = req;
    req.normalize();
    let audit = ResourceMutationLog::new("stream", "create", req.name.as_str(), None);
    if req.name.trim().is_empty() {
        let err = "stream name must not be empty".to_string();
        audit.log_failure(&err);
        return (StatusCode::BAD_REQUEST, err).into_response();
    }
    let _permit = match state.try_acquire_stream_op(&req.name).await {
        Ok(permit) => permit,
        Err(TryAcquireError::NoPermits) => return stream_busy_response(&req.name),
        Err(TryAcquireError::Closed) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "stream operation guard closed".to_string(),
            )
                .into_response();
        }
    };
    let schema = match build_schema_from_request(&req) {
        Ok(schema) => schema,
        Err(err) => {
            audit.log_failure(&err);
            return (StatusCode::BAD_REQUEST, err).into_response();
        }
    };

    let stream_props = match build_stream_props(&req.stream_type, &req.props) {
        Ok(props) => props,
        Err(err) => {
            audit.log_failure(&err);
            return (StatusCode::BAD_REQUEST, err).into_response();
        }
    };

    let decoder_registry = state.instances.default_instance().decoder_registry();
    let decoder = match build_stream_decoder(&req, decoder_registry.as_ref()) {
        Ok(config) => config,
        Err(err) => {
            audit.log_failure(&err);
            return (StatusCode::BAD_REQUEST, err).into_response();
        }
    };
    if let Err(err) = validate_stream_decoder_config(&req, &decoder) {
        audit.log_failure(&err);
        return (StatusCode::BAD_REQUEST, err).into_response();
    }
    if let StreamProps::Memory(memory_props) = &stream_props
        && let Err(err) = validate_memory_stream_topic(&req, memory_props)
    {
        audit.log_failure(&err);
        return (StatusCode::BAD_REQUEST, err).into_response();
    }
    if let StreamProps::Memory(memory_props) = &stream_props
        && let Err(err) =
            validate_memory_stream_binding(&req, memory_props, &decoder, &state.storage)
    {
        audit.log_failure(&err);
        return (StatusCode::BAD_REQUEST, err).into_response();
    }

    let stored = match storage_bridge::stored_stream_from_request(&req) {
        Ok(stored) => stored,
        Err(err) => {
            audit.log_failure(&err);
            return (StatusCode::BAD_REQUEST, err).into_response();
        }
    };
    match state.storage.create_stream(stored.clone()) {
        Ok(()) => {}
        Err(StorageError::AlreadyExists(_)) => {
            return (
                StatusCode::CONFLICT,
                format!("stream {} already exists", req.name),
            )
                .into_response();
        }
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to persist stream {}: {err}", req.name),
            )
                .into_response();
        }
    }

    let mut definition =
        StreamDefinition::new(req.name.clone(), Arc::new(schema), stream_props, decoder);
    if let Some(cfg) = &req.eventtime {
        definition = definition.with_eventtime(EventtimeDefinition::new(
            cfg.column.clone(),
            cfg.eventtime_type.clone(),
        ));
    }

    if let Some(sampler) = &req.sampler {
        definition = definition.with_sampler(sampler.clone());
    }

    let mut created = Vec::new();
    let mut first_info = None;
    let mut default_info = None;
    for (instance_id, instance) in state.instances.instances_snapshot() {
        match instance.create_stream(definition.clone(), req.shared).await {
            Ok(info) => {
                if first_info.is_none() {
                    first_info = Some(info.clone());
                }
                if instance_id == DEFAULT_FLOW_INSTANCE_ID {
                    default_info = Some(info);
                }
                created.push(instance);
            }
            Err(err) => {
                for instance in created {
                    let _ = instance.delete_stream(&req.name).await;
                }
                let _ = state.storage.delete_stream(&req.name);
                return map_flow_instance_error(err);
            }
        }
    }

    let Some(info) = default_info.or(first_info) else {
        // logically dead: stream was created in at least one instance above
        tracing::error!("no stream info returned after stream creation — audit mismatch");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "no stream info returned after creation",
        )
            .into_response();
    };
    audit.log_success();
    (StatusCode::CREATED, Json(build_stream_info(info))).into_response()
}

pub async fn list_streams(State(state): State<AppState>) -> impl IntoResponse {
    match state.storage.list_streams() {
        Ok(entries) => {
            let mut result = Vec::new();
            for entry in entries {
                let req: CreateStreamRequest = match serde_json::from_str(&entry.raw_json) {
                    Ok(req) => req,
                    Err(err) => {
                        return (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("decode stored stream {}: {err}", entry.id),
                        )
                            .into_response();
                    }
                };
                let schema = match build_schema_from_request(&req) {
                    Ok(s) => s,
                    Err(err) => {
                        return (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("build schema for stream {}: {err}", entry.id),
                        )
                            .into_response();
                    }
                };
                let columns = schema
                    .column_schemas()
                    .iter()
                    .map(|col| stream_column_info(&col.name, &col.data_type))
                    .collect();
                result.push(StreamInfo {
                    name: req.name,
                    shared: req.shared,
                    schema: StreamSchemaInfo { columns },
                    shared_stream: None,
                });
            }
            Json(result).into_response()
        }
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to list streams: {err}"),
        )
            .into_response(),
    }
}

pub async fn describe_stream_handler(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let stored = match state.storage.get_stream(&name) {
        Ok(Some(stream)) => stream,
        Ok(None) => {
            return (StatusCode::NOT_FOUND, format!("stream {name} not found")).into_response();
        }
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to load stream {name}: {err}"),
            )
                .into_response();
        }
    };

    let shared = match serde_json::from_str::<CreateStreamRequest>(&stored.raw_json) {
        Ok(req) => req.shared,
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("decode stored stream {name}: {err}"),
            )
                .into_response();
        }
    };

    let decoder_registry = state.instances.default_instance().decoder_registry();
    let definition =
        match storage_bridge::stream_definition_from_stored(&stored, decoder_registry.as_ref()) {
            Ok(definition) => definition,
            Err(err) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("rebuild stream definition {name}: {err}"),
                )
                    .into_response();
            }
        };

    let schema = definition.schema();
    let columns = schema
        .column_schemas()
        .iter()
        .map(|col| stream_column_info(&col.name, &col.data_type))
        .collect();
    let spec = StreamDefinitionSpec {
        stream_type: stream_type_label(definition.stream_type()).to_string(),
        schema: StreamSchemaInfo { columns },
        props: stream_props_value(definition.props()),
        shared,
        decoder: DecoderConfigRequest {
            decode_type: definition.decoder().kind().to_string(),
            props: definition.decoder().props().clone(),
        },
        eventtime: definition
            .eventtime()
            .map(|eventtime| EventtimeConfigRequest {
                column: eventtime.column().to_string(),
                eventtime_type: eventtime.eventtime_type().to_string(),
            }),
        sampler: definition.sampler().cloned(),
    };

    (
        StatusCode::OK,
        Json(DescribeStreamResponse {
            stream: name,
            spec_version: 1,
            spec,
        }),
    )
        .into_response()
}

pub async fn shared_stream_stats_handler(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(query): Query<SharedStreamStatsQuery>,
) -> impl IntoResponse {
    let stored = match state.storage.get_stream(&name) {
        Ok(Some(stream)) => stream,
        Ok(None) => {
            return (StatusCode::NOT_FOUND, format!("stream {name} not found")).into_response();
        }
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to load stream {name}: {err}"),
            )
                .into_response();
        }
    };

    let req = match serde_json::from_str::<CreateStreamRequest>(&stored.raw_json) {
        Ok(req) => req,
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("decode stored stream {name}: {err}"),
            )
                .into_response();
        }
    };

    if !req.shared {
        return (
            StatusCode::BAD_REQUEST,
            format!("stream {name} is not a shared stream"),
        )
            .into_response();
    }

    let flow_instance_id = match resolve_shared_stream_stats_flow_instance_id(&state, &query) {
        Ok(id) => id,
        Err(resp) => return *resp,
    };

    let instance = match state.local_instance(&flow_instance_id) {
        Some(instance) => instance,
        None => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("flow instance {flow_instance_id} is not available in runtime"),
            )
                .into_response();
        }
    };
    let stats = match instance.get_shared_stream_processor_stats(&name).await {
        Ok(stats) => stats,
        Err(FlowInstanceError::Catalog(CatalogError::NotFound(_))) => {
            return (StatusCode::NOT_FOUND, format!("stream {name} not found")).into_response();
        }
        Err(err) => return map_flow_instance_error(err),
    };
    let response = into_shared_stream_stats_response(&flow_instance_id, stats);

    (StatusCode::OK, Json(response)).into_response()
}

pub async fn delete_stream_handler(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let audit = ResourceMutationLog::new("stream", "delete", name.as_str(), None);
    let _permit = match state.try_acquire_stream_op(&name).await {
        Ok(permit) => permit,
        Err(TryAcquireError::NoPermits) => return stream_busy_response(&name),
        Err(TryAcquireError::Closed) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "stream operation guard closed".to_string(),
            )
                .into_response();
        }
    };
    let mut pipelines_using_stream = Vec::new();
    for (_, instance) in state.instances.instances_snapshot() {
        pipelines_using_stream.extend(
            instance
                .list_pipelines()
                .into_iter()
                .filter(|snapshot| snapshot.streams.iter().any(|stream| stream == &name))
                .map(|snapshot| snapshot.definition.id().to_string()),
        );
    }
    if !pipelines_using_stream.is_empty() {
        pipelines_using_stream.sort();
        let err = format!(
            "stream {name} still referenced by pipelines: {}",
            pipelines_using_stream.join(", ")
        );
        audit.log_failure(&err);
        return (StatusCode::CONFLICT, err).into_response();
    }

    for (_, instance) in state.instances.instances_snapshot() {
        match instance.delete_stream(&name).await {
            Ok(()) => {}
            Err(FlowInstanceError::Catalog(CatalogError::NotFound(_))) => {}
            Err(err) => return map_flow_instance_error(err),
        }
    }

    match state.storage.delete_stream(&name) {
        Ok(()) => {}
        Err(StorageError::NotFound(_)) => {
            let err = format!("stream {name} not found");
            audit.log_failure(&err);
            return (StatusCode::NOT_FOUND, err).into_response();
        }
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!(
                    "stream {name} deleted in runtime but failed to remove from storage: {err}"
                ),
            )
                .into_response();
        }
    }
    audit.log_success();
    (StatusCode::OK, format!("stream {name} deleted")).into_response()
}

fn build_stream_info(info: StreamRuntimeInfo) -> StreamInfo {
    let schema = info.definition.schema();
    let shared_item = info.shared_info.map(into_shared_stream_item);
    StreamInfo {
        name: info.definition.id().to_string(),
        shared: shared_item.is_some(),
        schema: StreamSchemaInfo {
            columns: schema
                .column_schemas()
                .iter()
                .map(|col| stream_column_info(&col.name, &col.data_type))
                .collect(),
        },
        shared_stream: shared_item,
    }
}

fn map_flow_instance_error(err: FlowInstanceError) -> axum::response::Response {
    let (status, message) = match err {
        FlowInstanceError::Catalog(CatalogError::AlreadyExists(name)) => (
            StatusCode::CONFLICT,
            format!("stream {name} already exists"),
        ),
        FlowInstanceError::Catalog(CatalogError::NotFound(name)) => {
            (StatusCode::NOT_FOUND, format!("stream {name} not found"))
        }
        FlowInstanceError::SharedStream(SharedStreamError::InUse(consumers)) => (
            StatusCode::CONFLICT,
            format!(
                "shared stream still referenced by pipelines: {}",
                consumers.join(", ")
            ),
        ),
        FlowInstanceError::SharedStream(SharedStreamError::NotFound(name)) => (
            StatusCode::NOT_FOUND,
            format!("shared stream {name} not found"),
        ),
        FlowInstanceError::StreamInUse { stream, pipelines } => (
            StatusCode::CONFLICT,
            format!("stream {stream} still referenced by pipelines: {pipelines}"),
        ),
        other => (StatusCode::BAD_REQUEST, other.to_string()),
    };

    (status, message).into_response()
}

fn resolve_shared_stream_stats_flow_instance_id(
    state: &AppState,
    query: &SharedStreamStatsQuery,
) -> Result<String, Box<axum::response::Response>> {
    match query.flow_instance_id.as_deref() {
        Some(id) => {
            let id = id.trim();
            if id.is_empty() {
                return Err(Box::new(
                    (
                        StatusCode::BAD_REQUEST,
                        "flow_instance_id must not be empty".to_string(),
                    )
                        .into_response(),
                ));
            }
            if !state.is_declared_instance(id) {
                return Err(Box::new(
                    (
                        StatusCode::BAD_REQUEST,
                        format!("flow instance {id} is not declared by config"),
                    )
                        .into_response(),
                ));
            }
            Ok(id.to_string())
        }
        None if state.declared_instances.len() > 1 => Err(Box::new(
            (
                StatusCode::BAD_REQUEST,
                "flow_instance_id is required when multiple flow instances are declared"
                    .to_string(),
            )
                .into_response(),
        )),
        None => Ok(DEFAULT_FLOW_INSTANCE_ID.to_string()),
    }
}

pub(crate) fn into_shared_stream_stats_response(
    flow_instance_id: &str,
    stats: flow::SharedStreamProcessorStats,
) -> SharedStreamStatsResponse {
    let (status, status_message) = shared_stream_status_label(&stats.status);
    SharedStreamStatsResponse {
        stream: stats.stream,
        flow_instance_id: flow_instance_id.to_string(),
        status,
        status_message,
        processors: stats.processors,
    }
}

fn stream_type_label(stream_type: flow::catalog::StreamType) -> &'static str {
    match stream_type {
        flow::catalog::StreamType::Mqtt => "mqtt",
        flow::catalog::StreamType::Video => "video",
        flow::catalog::StreamType::Mock => "mock",
        flow::catalog::StreamType::History => "history",
        flow::catalog::StreamType::Memory => "memory",
        flow::catalog::StreamType::NngPubSub => "nng_pubsub",
    }
}

fn normalized_optional_string(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_string())
    })
}

fn stream_props_value(props: &StreamProps) -> JsonValue {
    match props {
        StreamProps::Mqtt(mqtt) => {
            let mut map = JsonMap::new();
            if !mqtt.broker_url.trim().is_empty() {
                map.insert(
                    "broker_url".to_string(),
                    JsonValue::String(mqtt.broker_url.clone()),
                );
            }
            map.insert("topic".to_string(), JsonValue::String(mqtt.topic.clone()));
            map.insert("qos".to_string(), JsonValue::from(mqtt.qos));
            if let Some(client_id) = &mqtt.client_id {
                map.insert(
                    "client_id".to_string(),
                    JsonValue::String(client_id.clone()),
                );
            }
            if let Some(connector_key) = mqtt
                .connector_key
                .as_ref()
                .filter(|connector_key| !connector_key.trim().is_empty())
            {
                map.insert(
                    "connector_key".to_string(),
                    JsonValue::String(connector_key.clone()),
                );
            }
            JsonValue::Object(map)
        }
        StreamProps::Memory(memory) => {
            let mut map = JsonMap::new();
            map.insert("topic".to_string(), JsonValue::String(memory.topic.clone()));
            JsonValue::Object(map)
        }
        StreamProps::Video(video) => video_props_value(video),
        StreamProps::Mock(_) => JsonValue::Object(JsonMap::new()),
        StreamProps::History(_) => JsonValue::Object(JsonMap::new()),
        StreamProps::NngPubSub(nng) => {
            let mut map = JsonMap::new();
            map.insert("url".to_string(), JsonValue::String(nng.url.clone()));
            map.insert("topic".to_string(), JsonValue::String(nng.topic.clone()));
            map.insert(
                "topic_delimiter".to_string(),
                JsonValue::String(nng.topic_delimiter.clone()),
            );
            JsonValue::Object(map)
        }
    }
}

fn video_props_value(video: &VideoStreamProps) -> JsonValue {
    let mut map = JsonMap::new();
    map.insert("url".to_string(), JsonValue::String(video.url.clone()));
    if flow::pipeline::is_rtsp_video_url(&video.url) {
        map.insert(
            "rtsp_transport".to_string(),
            JsonValue::String(video.rtsp_transport.as_str().to_string()),
        );
    }
    let mut reconnect = JsonMap::new();
    reconnect.insert(
        "enabled".to_string(),
        JsonValue::Bool(video.reconnect.enabled),
    );
    reconnect.insert(
        "initial_delay_ms".to_string(),
        JsonValue::from(duration_millis_u64(video.reconnect.initial_delay)),
    );
    reconnect.insert(
        "max_delay_ms".to_string(),
        JsonValue::from(duration_millis_u64(video.reconnect.max_delay)),
    );
    map.insert("reconnect".to_string(), JsonValue::Object(reconnect));
    JsonValue::Object(map)
}

fn stream_column_info(name: &str, datatype: &ConcreteDatatype) -> StreamColumnInfo {
    let mut info = StreamColumnInfo {
        name: name.to_string(),
        data_type: datatype_name(datatype),
        fields: None,
        element: None,
    };

    match datatype {
        ConcreteDatatype::Struct(struct_type) => {
            let field_infos = struct_type
                .fields()
                .iter()
                .map(|field| stream_column_info(field.name(), field.data_type()))
                .collect();
            info.fields = Some(field_infos);
        }
        ConcreteDatatype::List(list_type) => {
            let element_info = stream_column_info("element", list_type.item_type());
            info.element = Some(Box::new(element_info));
        }
        _ => {}
    }

    info
}

fn duration_millis_u64(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn parse_json_schema(
    stream_name: &str,
    props: &JsonMap<String, JsonValue>,
) -> Result<Schema, String> {
    let schema_value = JsonValue::Object(props.clone());
    let schema_req: StreamSchemaRequest = serde_json::from_value(schema_value)
        .map_err(|err| format!("invalid json schema: {err}"))?;
    schema_from_columns(stream_name, &schema_req)
}

fn schema_from_columns(
    stream_name: &str,
    schema_req: &StreamSchemaRequest,
) -> Result<Schema, String> {
    let columns: Result<Vec<ColumnSchema>, String> = schema_req
        .columns
        .iter()
        .map(|col| column_schema_from_request(stream_name.to_string(), col))
        .collect();
    columns.map(Schema::new)
}

pub(crate) fn build_schema_from_request(req: &CreateStreamRequest) -> Result<Schema, String> {
    if req.stream_type.eq_ignore_ascii_case("video") {
        return Ok(flow::codec::default_video_schema(req.name.clone()));
    }
    schema_registry().parse(&req.schema.schema_type, &req.name, &req.schema.props)
}

pub(crate) fn build_stream_props(
    stream_type: &str,
    props: &StreamPropsRequest,
) -> Result<StreamProps, String> {
    match stream_type.to_ascii_lowercase().as_str() {
        "mqtt" => {
            let mqtt_props: MqttStreamPropsRequest = serde_json::from_value(props.to_value())
                .map_err(|err| format!("invalid mqtt props: {}", err))?;
            let connector_key = normalized_optional_string(mqtt_props.connector_key);
            let broker = normalized_optional_string(mqtt_props.broker_url);
            if connector_key.is_none() && broker.is_none() {
                return Err("mqtt stream requires broker_url".to_string());
            }
            let topic = mqtt_props
                .topic
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| "mqtt stream requires topic".to_string())?;
            let qos = mqtt_props.qos.unwrap_or(MQTT_QOS);
            Ok(StreamProps::Mqtt(MqttStreamProps {
                broker_url: broker.unwrap_or_default(),
                topic,
                qos,
                client_id: mqtt_props.client_id,
                connector_key,
            }))
        }
        "video" => {
            let video_props: VideoStreamPropsRequest = serde_json::from_value(props.to_value())
                .map_err(|err| format!("invalid video props: {err}"))?;
            build_video_stream_props(video_props)
        }
        "history" => {
            let history_props: HistoryStreamPropsRequest = serde_json::from_value(props.to_value())
                .map_err(|err| format!("invalid history props: {}", err))?;
            let datasource = history_props
                .datasource
                .ok_or("history stream requires datasource")?;
            let topic = history_props.topic.ok_or("history stream requires topic")?;
            Ok(StreamProps::History(HistoryStreamProps {
                datasource,
                topic,
                start: history_props.start,
                end: history_props.end,
                batch_size: history_props.batch_size,
                send_interval: history_props
                    .send_interval_ms
                    .map(std::time::Duration::from_millis),
                decrypt_method: None,
                decrypt_props: None,
            }))
        }
        "memory" => {
            let memory_props: MemoryStreamPropsRequest =
                serde_json::from_value(props.to_value())
                    .map_err(|err| format!("invalid memory props: {err}"))?;
            let topic = memory_props
                .topic
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| "memory stream requires topic".to_string())?;
            Ok(StreamProps::Memory(MemoryStreamProps::new(topic)))
        }
        "nng_pubsub" => {
            let nng_props: NngPubSubStreamPropsRequest =
                serde_json::from_value(props.to_value())
                    .map_err(|err| format!("invalid nng_pubsub props: {err}"))?;
            let url = nng_props
                .url
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| "nng_pubsub stream requires url".to_string())?;
            let topic = nng_props.topic.unwrap_or_default();
            let topic_delimiter = nng_props
                .topic_delimiter
                .or(nng_props.topic_delimiter_camel)
                .unwrap_or_else(|| {
                    flow::connector::nng_pubsub::DEFAULT_TOPIC_DELIMITER.to_string()
                });
            let stream_props =
                NngPubSubStreamProps::new(url, topic).with_topic_delimiter(topic_delimiter);
            flow::connector::NngPubSubSourceConfig::new(
                "nng_pubsub",
                stream_props.url.clone(),
                stream_props.topic.clone(),
            )
            .with_topic_delimiter(stream_props.topic_delimiter.clone())
            .validate()?;
            Ok(StreamProps::NngPubSub(stream_props))
        }
        "mock" => Ok(StreamProps::Mock(MockStreamProps::default())),
        other => Err(format!("unsupported stream type: {other}")),
    }
}

fn build_video_stream_props(req: VideoStreamPropsRequest) -> Result<StreamProps, String> {
    let url = req
        .url
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| "video stream requires props.url".to_string())?;
    let is_rtsp = flow::pipeline::is_rtsp_video_url(&url);
    if !is_rtsp && !flow::pipeline::is_hls_video_url(&url) {
        return Err(
            "video stream props.url must be rtsp://, rtsps://, or http(s)://...m3u8".to_string(),
        );
    }
    let rtsp_transport = match req.rtsp_transport {
        Some(value) if is_rtsp => match value.trim().to_ascii_lowercase().as_str() {
            "tcp" => VideoRtspTransport::Tcp,
            "udp" => VideoRtspTransport::Udp,
            other => {
                return Err(format!(
                    "invalid video stream rtsp_transport `{other}` (expected tcp|udp)"
                ));
            }
        },
        Some(_) => {
            return Err("video stream rtsp_transport is only valid for RTSP URLs".to_string());
        }
        None => VideoRtspTransport::Tcp,
    };
    if req.reconnect.initial_delay_ms == 0 || req.reconnect.max_delay_ms == 0 {
        return Err("video stream reconnect delays must be positive".to_string());
    }
    if req.reconnect.initial_delay_ms > req.reconnect.max_delay_ms {
        return Err(
            "video stream reconnect initial_delay_ms must be less than or equal to max_delay_ms"
                .to_string(),
        );
    }
    Ok(StreamProps::Video(VideoStreamProps {
        url,
        rtsp_transport,
        reconnect: VideoReconnectConfig {
            enabled: req.reconnect.enabled,
            initial_delay: std::time::Duration::from_millis(req.reconnect.initial_delay_ms),
            max_delay: std::time::Duration::from_millis(req.reconnect.max_delay_ms),
        },
    }))
}

pub(crate) fn build_stream_decoder(
    req: &CreateStreamRequest,
    decoder_registry: &DecoderRegistry,
) -> Result<StreamDecoderConfig, String> {
    if req.stream_type.eq_ignore_ascii_case("video")
        && req.decoder.decode_type.eq_ignore_ascii_case("json")
        && req.decoder.props.is_empty()
    {
        return Ok(StreamDecoderConfig::none());
    }
    let decoder_config = req.decoder.clone();
    if decoder_config.decode_type == "none" {
        return Ok(StreamDecoderConfig::new(
            decoder_config.decode_type,
            decoder_config.props,
        ));
    }
    if !decoder_registry.is_registered(&decoder_config.decode_type) {
        return Err(format!(
            "decoder kind `{}` not registered",
            decoder_config.decode_type
        ));
    }
    Ok(StreamDecoderConfig::new(
        decoder_config.decode_type,
        decoder_config.props,
    ))
}

pub(crate) fn validate_stream_decoder_config(
    req: &CreateStreamRequest,
    decoder: &StreamDecoderConfig,
) -> Result<(), String> {
    let stream_type = req.stream_type.to_ascii_lowercase();
    let is_none = decoder.kind() == "none";
    if stream_type == "video" {
        if req.shared {
            return Err(format!(
                "stream `{}` does not support shared=true for video streams",
                req.name
            ));
        }
        if !is_none {
            return Err(format!(
                "stream `{}` requires decoder type `none` for video streams",
                req.name
            ));
        }
        if req.eventtime.is_some() {
            return Err(format!(
                "stream `{}` does not support eventtime for video streams",
                req.name
            ));
        }
        if req.sampler.is_some() {
            return Err(format!(
                "stream `{}` does not support sampler for video streams",
                req.name
            ));
        }
        return Ok(());
    }
    if req.shared && stream_type == "memory" {
        return Err(format!(
            "shared stream `{}` does not support stream type `memory`",
            req.name
        ));
    }
    if req.shared && is_none {
        return Err(format!(
            "shared stream `{}` does not support decoder type `none`",
            req.name
        ));
    }
    if is_none && stream_type != "memory" {
        return Err(format!(
            "stream `{}` decoder type `none` only supported for memory streams",
            req.name
        ));
    }
    if is_none && req.eventtime.is_some() {
        return Err(format!(
            "stream `{}` eventtime requires a decoder (decoder type `none` unsupported)",
            req.name
        ));
    }
    Ok(())
}

pub(crate) fn validate_memory_stream_topic(
    req: &CreateStreamRequest,
    props: &MemoryStreamProps,
) -> Result<(), String> {
    if !req.stream_type.eq_ignore_ascii_case("memory") {
        return Ok(());
    }
    let topic = props.topic.trim();
    if topic.is_empty() {
        return Err(format!(
            "stream `{}` memory topic must not be empty",
            req.name
        ));
    }

    Ok(())
}

fn stored_memory_topic_kind_name(kind: &StoredMemoryTopicKind) -> &'static str {
    match kind {
        StoredMemoryTopicKind::Bytes => "bytes",
        StoredMemoryTopicKind::Collection => "collection",
    }
}

pub(crate) fn validate_memory_stream_binding(
    req: &CreateStreamRequest,
    props: &MemoryStreamProps,
    decoder: &StreamDecoderConfig,
    storage: &StorageManager,
) -> Result<(), String> {
    if !req.stream_type.eq_ignore_ascii_case("memory") {
        return Ok(());
    }

    let topic = props.topic.trim();
    let Some(stored_topic) = storage
        .get_memory_topic(topic)
        .map_err(|err| format!("failed to read memory topic `{topic}`: {err}"))?
    else {
        return Err(format!("memory topic `{topic}` not declared"));
    };

    let expected_kind = if decoder.kind() == "none" {
        StoredMemoryTopicKind::Collection
    } else {
        StoredMemoryTopicKind::Bytes
    };
    if stored_topic.kind != expected_kind {
        return Err(format!(
            "memory topic `{topic}` kind mismatch for stream `{}`: expected {}, got {}",
            req.name,
            stored_memory_topic_kind_name(&expected_kind),
            stored_memory_topic_kind_name(&stored_topic.kind)
        ));
    }

    Ok(())
}

fn column_schema_from_request(
    source: String,
    column: &StreamColumnRequest,
) -> Result<ColumnSchema, String> {
    parse_datatype(column).map(|datatype| ColumnSchema::new(source, column.name.clone(), datatype))
}

fn parse_datatype(column: &StreamColumnRequest) -> Result<ConcreteDatatype, String> {
    match column.data_type.to_ascii_lowercase().as_str() {
        "null" => Ok(ConcreteDatatype::Null),
        "bool" | "boolean" => Ok(ConcreteDatatype::Bool(BooleanType)),
        "int8" => Ok(ConcreteDatatype::Int8(Int8Type)),
        "int16" => Ok(ConcreteDatatype::Int16(Int16Type)),
        "int32" => Ok(ConcreteDatatype::Int32(Int32Type)),
        "int64" => Ok(ConcreteDatatype::Int64(Int64Type)),
        "uint8" => Ok(ConcreteDatatype::Uint8(Uint8Type)),
        "uint16" => Ok(ConcreteDatatype::Uint16(Uint16Type)),
        "uint32" => Ok(ConcreteDatatype::Uint32(Uint32Type)),
        "uint64" => Ok(ConcreteDatatype::Uint64(Uint64Type)),
        "float32" => Ok(ConcreteDatatype::Float32(Float32Type)),
        "float64" => Ok(ConcreteDatatype::Float64(Float64Type)),
        "string" => Ok(ConcreteDatatype::String(StringType)),
        "bytes" => Ok(ConcreteDatatype::Bytes(BytesType)),
        "timestamp" => Ok(ConcreteDatatype::Timestamp(TimestampType)),
        "list" => {
            let element = column.element.as_deref().ok_or_else(|| {
                format!("list column {} requires element definition", column.name)
            })?;
            let element_type = parse_datatype(element)?;
            Ok(ConcreteDatatype::List(ListType::new(Arc::new(
                element_type,
            ))))
        }
        "struct" => {
            let fields = column.fields.as_deref().ok_or_else(|| {
                format!("struct column {} requires fields definition", column.name)
            })?;
            let struct_fields: Result<Vec<StructField>, String> = fields
                .iter()
                .map(|field| {
                    let field_type = parse_datatype(field)?;
                    Ok(StructField::new(field.name.clone(), field_type, false))
                })
                .collect();
            Ok(ConcreteDatatype::Struct(StructType::new(Arc::new(
                struct_fields?,
            ))))
        }
        other => Err(format!("unsupported data type: {}", other)),
    }
}

fn datatype_name(datatype: &ConcreteDatatype) -> String {
    match datatype {
        ConcreteDatatype::Null => "null",
        ConcreteDatatype::Float32(_) => "float32",
        ConcreteDatatype::Float64(_) => "float64",
        ConcreteDatatype::Int8(_) => "int8",
        ConcreteDatatype::Int16(_) => "int16",
        ConcreteDatatype::Int32(_) => "int32",
        ConcreteDatatype::Int64(_) => "int64",
        ConcreteDatatype::Uint8(_) => "uint8",
        ConcreteDatatype::Uint16(_) => "uint16",
        ConcreteDatatype::Uint32(_) => "uint32",
        ConcreteDatatype::Uint64(_) => "uint64",
        ConcreteDatatype::String(_) => "string",
        ConcreteDatatype::Bytes(_) => "bytes",
        ConcreteDatatype::Timestamp(_) => "timestamp",
        ConcreteDatatype::Struct(_) => "struct",
        ConcreteDatatype::List(_) => "list",
        ConcreteDatatype::Bool(_) => "bool",
    }
    .to_string()
}

fn into_shared_stream_item(info: SharedStreamInfo) -> SharedStreamItem {
    let (status, status_message) = shared_stream_status_label(&info.status);
    SharedStreamItem {
        id: info.name,
        status,
        status_message,
        connector_id: info.connector_id,
        subscribers: info.subscriber_count,
        created_at_secs: unix_timestamp_secs(info.created_at),
    }
}

fn shared_stream_status_label(status: &SharedStreamStatus) -> (String, Option<String>) {
    match status {
        SharedStreamStatus::Starting => ("starting".to_string(), None),
        SharedStreamStatus::Running => ("running".to_string(), None),
        SharedStreamStatus::Stopped => ("stopped".to_string(), None),
        SharedStreamStatus::Failed(msg) => ("failed".to_string(), Some(msg.clone())),
    }
}

fn unix_timestamp_secs(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::{AppState, CreatePipelineRequest};
    use axum::{
        Json,
        body::to_bytes,
        extract::{Path, State},
        http::StatusCode,
        response::IntoResponse,
    };
    use serde_json::{Map as JsonMap, Value as JsonValue, json};
    use storage::StorageManager;
    use tempfile::TempDir;

    fn base_stream_request(stream_type: &str) -> CreateStreamRequest {
        CreateStreamRequest {
            name: "stream_test".to_string(),
            stream_type: stream_type.to_string(),
            schema: SchemaConfigRequest::default(),
            props: StreamPropsRequest::default(),
            shared: false,
            decoder: DecoderConfigRequest::default(),
            eventtime: None,
            sampler: None,
        }
    }

    fn mqtt_stream_request(name: &str) -> CreateStreamRequest {
        let schema_props: JsonMap<String, JsonValue> = json!({
            "columns": [
                { "name": "value", "data_type": "int64" }
            ]
        })
        .as_object()
        .expect("schema props object")
        .clone();

        let props_fields: JsonMap<String, JsonValue> = json!({
            "broker_url": "mqtt://localhost:1883",
            "topic": format!("{name}/topic"),
            "qos": 0
        })
        .as_object()
        .expect("stream props object")
        .clone();

        CreateStreamRequest {
            name: name.to_string(),
            stream_type: "mqtt".to_string(),
            schema: SchemaConfigRequest {
                schema_type: "json".to_string(),
                props: schema_props,
            },
            props: StreamPropsRequest {
                fields: props_fields,
            },
            shared: false,
            decoder: DecoderConfigRequest::default(),
            eventtime: None,
            sampler: None,
        }
    }

    fn mqtt_stream_request_with_connector_key(
        name: &str,
        connector_key: &str,
    ) -> CreateStreamRequest {
        let schema_props: JsonMap<String, JsonValue> = json!({
            "columns": [
                { "name": "value", "data_type": "int64" }
            ]
        })
        .as_object()
        .expect("schema props object")
        .clone();

        let props_fields: JsonMap<String, JsonValue> = json!({
            "topic": format!("{name}/topic"),
            "qos": 0,
            "connector_key": connector_key
        })
        .as_object()
        .expect("stream props object")
        .clone();

        CreateStreamRequest {
            name: name.to_string(),
            stream_type: "mqtt".to_string(),
            schema: SchemaConfigRequest {
                schema_type: "json".to_string(),
                props: schema_props,
            },
            props: StreamPropsRequest {
                fields: props_fields,
            },
            shared: false,
            decoder: DecoderConfigRequest::default(),
            eventtime: None,
            sampler: None,
        }
    }

    fn mock_stream_request(name: &str) -> CreateStreamRequest {
        let schema_props: JsonMap<String, JsonValue> = json!({
            "columns": [
                { "name": "value", "data_type": "int64" }
            ]
        })
        .as_object()
        .expect("schema props object")
        .clone();

        CreateStreamRequest {
            name: name.to_string(),
            stream_type: "mock".to_string(),
            schema: SchemaConfigRequest {
                schema_type: "json".to_string(),
                props: schema_props,
            },
            props: StreamPropsRequest::default(),
            shared: false,
            decoder: DecoderConfigRequest::default(),
            eventtime: None,
            sampler: None,
        }
    }

    fn sample_default_instance_spec() -> crate::FlowInstanceSpec {
        crate::FlowInstanceSpec {
            id: "default".to_string(),
            ..crate::FlowInstanceSpec::default()
        }
    }

    fn local_flow_instance_spec(id: &str) -> crate::FlowInstanceSpec {
        crate::FlowInstanceSpec {
            id: id.to_string(),
            ..crate::FlowInstanceSpec::default()
        }
    }

    fn build_state(temp_dir: &TempDir, flow_instances: Vec<crate::FlowInstanceSpec>) -> AppState {
        let storage = StorageManager::new(temp_dir.path()).expect("create storage");
        AppState::new(crate::new_default_flow_instance(), storage, flow_instances)
            .expect("build app state")
    }

    fn stream_definition_from_request(
        req: &CreateStreamRequest,
        decoder_registry: &flow::DecoderRegistry,
    ) -> StreamDefinition {
        let schema = build_schema_from_request(req).expect("build schema from request");
        let props =
            build_stream_props(&req.stream_type, &req.props).expect("build stream props from req");
        let decoder =
            build_stream_decoder(req, decoder_registry).expect("build stream decoder from req");
        validate_stream_decoder_config(req, &decoder)
            .expect("validate stream decoder configuration");

        let mut definition =
            StreamDefinition::new(req.name.clone(), Arc::new(schema), props, decoder);
        if let Some(cfg) = &req.eventtime {
            definition = definition.with_eventtime(EventtimeDefinition::new(
                cfg.column.clone(),
                cfg.eventtime_type.clone(),
            ));
        }
        if let Some(sampler) = &req.sampler {
            definition = definition.with_sampler(sampler.clone());
        }
        definition
    }

    fn pipeline_request(id: &str, stream_name: &str) -> CreatePipelineRequest {
        serde_json::from_value(json!({
            "id": id,
            "flow_instance_id": "default",
            "sql": format!("SELECT * FROM {stream_name}"),
            "sinks": [
                {
                    "id": format!("{id}_sink_0"),
                    "type": "nop",
                    "props": { "log": false },
                    "common_sink_props": {},
                    "encoder": { "type": "json", "props": {} }
                }
            ],
            "options": {
                "data_channel_capacity": 16,
                "eventtime": {
                    "enabled": false,
                    "late_tolerance_ms": 0
                }
            }
        }))
        .expect("deserialize pipeline request")
    }

    fn struct_request() -> StreamColumnRequest {
        StreamColumnRequest {
            name: "user".to_string(),
            data_type: "struct".to_string(),
            fields: Some(vec![
                StreamColumnRequest {
                    name: "id".to_string(),
                    data_type: "int64".to_string(),
                    fields: None,
                    element: None,
                },
                StreamColumnRequest {
                    name: "sessions".to_string(),
                    data_type: "list".to_string(),
                    fields: None,
                    element: Some(Box::new(StreamColumnRequest {
                        name: "session".to_string(),
                        data_type: "struct".to_string(),
                        fields: Some(vec![
                            StreamColumnRequest {
                                name: "session_id".to_string(),
                                data_type: "string".to_string(),
                                fields: None,
                                element: None,
                            },
                            StreamColumnRequest {
                                name: "events".to_string(),
                                data_type: "list".to_string(),
                                fields: None,
                                element: Some(Box::new(StreamColumnRequest {
                                    name: "event".to_string(),
                                    data_type: "string".to_string(),
                                    fields: None,
                                    element: None,
                                })),
                            },
                        ]),
                        element: None,
                    })),
                },
            ]),
            element: None,
        }
    }

    #[test]
    fn parse_datatype_nested_struct_list() {
        let column = struct_request();
        let datatype = parse_datatype(&column).expect("should parse nested struct");

        let expected = ConcreteDatatype::Struct(StructType::new(Arc::new(vec![
            StructField::new("id".to_string(), ConcreteDatatype::Int64(Int64Type), false),
            StructField::new(
                "sessions".to_string(),
                ConcreteDatatype::List(ListType::new(Arc::new(ConcreteDatatype::Struct(
                    StructType::new(Arc::new(vec![
                        StructField::new(
                            "session_id".to_string(),
                            ConcreteDatatype::String(StringType),
                            false,
                        ),
                        StructField::new(
                            "events".to_string(),
                            ConcreteDatatype::List(ListType::new(Arc::new(
                                ConcreteDatatype::String(StringType),
                            ))),
                            false,
                        ),
                    ])),
                )))),
                false,
            ),
        ])));

        assert_eq!(datatype, expected);
    }

    #[test]
    fn parse_datatype_timestamp() {
        let column = StreamColumnRequest {
            name: "event_time".to_string(),
            data_type: "timestamp".to_string(),
            fields: None,
            element: None,
        };
        let datatype = parse_datatype(&column).expect("should parse timestamp");

        assert!(matches!(datatype, ConcreteDatatype::Timestamp(_)));
        assert_eq!(datatype_name(&datatype), "timestamp");
    }

    #[test]
    fn parse_datatype_bytes() {
        let column = StreamColumnRequest {
            name: "payload".to_string(),
            data_type: "bytes".to_string(),
            fields: None,
            element: None,
        };
        let datatype = parse_datatype(&column).expect("should parse bytes");

        assert!(matches!(datatype, ConcreteDatatype::Bytes(_)));
        assert_eq!(datatype_name(&datatype), "bytes");
    }

    #[test]
    fn parse_datatype_errors_without_nested_payload() {
        let missing_fields = StreamColumnRequest {
            name: "bad_struct".to_string(),
            data_type: "struct".to_string(),
            fields: None,
            element: None,
        };
        assert!(parse_datatype(&missing_fields).is_err());

        let missing_element = StreamColumnRequest {
            name: "bad_list".to_string(),
            data_type: "list".to_string(),
            fields: None,
            element: None,
        };
        assert!(parse_datatype(&missing_element).is_err());
    }

    #[test]
    fn build_schema_from_request_rejects_unknown_schema_type() {
        let mut req = base_stream_request("mqtt");
        req.schema.schema_type = "avro".to_string();

        let err = build_schema_from_request(&req).unwrap_err();
        assert_eq!(err, "schema type `avro` not registered");
    }

    #[test]
    fn build_stream_decoder_rejects_unknown_non_none_decoder_type() {
        let mut req = base_stream_request("mqtt");
        req.decoder.decode_type = "unknown_decoder".to_string();
        let instance = crate::new_default_flow_instance();

        let err = build_stream_decoder(&req, instance.decoder_registry().as_ref()).unwrap_err();
        assert_eq!(err, "decoder kind `unknown_decoder` not registered");
    }

    #[test]
    fn build_stream_props_allows_shared_mqtt_without_broker_url() {
        let props = StreamPropsRequest {
            fields: json!({
                "topic": "shared/topic",
                "qos": 1,
                "connector_key": "shared_mqtt"
            })
            .as_object()
            .expect("mqtt props object")
            .clone(),
        };

        let built = build_stream_props("mqtt", &props).expect("build shared mqtt stream props");
        let StreamProps::Mqtt(mqtt) = built else {
            panic!("expected mqtt stream props");
        };
        assert_eq!(mqtt.broker_url, "");
        assert_eq!(mqtt.topic, "shared/topic");
        assert_eq!(mqtt.qos, 1);
        assert_eq!(mqtt.connector_key.as_deref(), Some("shared_mqtt"));
    }

    #[test]
    fn build_stream_props_rejects_missing_broker_url_without_connector_key() {
        let props = StreamPropsRequest {
            fields: json!({
                "topic": "shared/topic",
                "qos": 1
            })
            .as_object()
            .expect("mqtt props object")
            .clone(),
        };

        let err = build_stream_props("mqtt", &props).unwrap_err();
        assert_eq!(err, "mqtt stream requires broker_url");
    }

    #[test]
    fn build_stream_props_accepts_nng_pubsub_defaults() {
        let props = StreamPropsRequest {
            fields: json!({
                "url": "inproc://manager-nng-stream",
                "topic": "topic/can"
            })
            .as_object()
            .expect("nng props object")
            .clone(),
        };

        let built = build_stream_props("nng_pubsub", &props).expect("build nng stream props");
        let StreamProps::NngPubSub(nng) = built else {
            panic!("expected nng_pubsub stream props");
        };
        assert_eq!(nng.url, "inproc://manager-nng-stream");
        assert_eq!(nng.topic, "topic/can");
        assert_eq!(
            nng.topic_delimiter,
            flow::connector::nng_pubsub::DEFAULT_TOPIC_DELIMITER
        );
    }

    #[test]
    fn validate_stream_decoder_config_rejects_shared_memory_stream() {
        let mut req = base_stream_request("memory");
        req.shared = true;
        let decoder = StreamDecoderConfig::new("json", JsonMap::new());

        let err = validate_stream_decoder_config(&req, &decoder).unwrap_err();
        assert_eq!(
            err,
            "shared stream `stream_test` does not support stream type `memory`"
        );
    }

    #[test]
    fn validate_stream_decoder_config_rejects_shared_stream_with_decoder_none() {
        let mut req = base_stream_request("mqtt");
        req.shared = true;
        let decoder = StreamDecoderConfig::new("none", JsonMap::new());

        let err = validate_stream_decoder_config(&req, &decoder).unwrap_err();
        assert_eq!(
            err,
            "shared stream `stream_test` does not support decoder type `none`"
        );
    }

    #[test]
    fn validate_stream_decoder_config_rejects_decoder_none_for_non_memory_stream() {
        let req = base_stream_request("mqtt");
        let decoder = StreamDecoderConfig::new("none", JsonMap::new());

        let err = validate_stream_decoder_config(&req, &decoder).unwrap_err();
        assert_eq!(
            err,
            "stream `stream_test` decoder type `none` only supported for memory streams"
        );
    }

    #[test]
    fn validate_stream_decoder_config_rejects_eventtime_with_decoder_none() {
        let mut req = base_stream_request("memory");
        req.eventtime = Some(EventtimeConfigRequest {
            column: "event_ts".to_string(),
            eventtime_type: "unixtimestamp_ms".to_string(),
        });
        let decoder = StreamDecoderConfig::new("none", JsonMap::new());

        let err = validate_stream_decoder_config(&req, &decoder).unwrap_err();
        assert_eq!(
            err,
            "stream `stream_test` eventtime requires a decoder (decoder type `none` unsupported)"
        );
    }

    #[tokio::test]
    async fn create_stream_rolls_back_storage_when_late_instance_install_fails() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let state = build_state(
            &temp_dir,
            vec![
                sample_default_instance_spec(),
                local_flow_instance_spec("local_b"),
            ],
        );
        let req = mqtt_stream_request("stream_conflict");
        let later_instance = state
            .local_instance("local_b")
            .expect("local_b runtime instance");
        let existing_definition =
            stream_definition_from_request(&req, later_instance.decoder_registry().as_ref());
        later_instance
            .create_stream(existing_definition, req.shared)
            .await
            .expect("seed conflicting stream in local_b");

        let response = create_stream_handler(State(state.clone()), Json(req.clone()))
            .await
            .into_response();
        assert_eq!(response.status(), StatusCode::CONFLICT);

        let body = to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("read response body");
        assert_eq!(
            String::from_utf8(body.to_vec()).expect("utf8 body"),
            "stream stream_conflict already exists"
        );
        assert!(
            state
                .storage
                .get_stream(&req.name)
                .expect("read stored stream")
                .is_none(),
            "late instance failure must roll back persisted stream metadata",
        );

        let default_instance = state
            .local_instance("default")
            .expect("default runtime instance");
        let default_streams = default_instance
            .list_streams()
            .await
            .expect("list default streams");
        assert!(
            !default_streams
                .iter()
                .any(|stream| stream.definition.id() == req.name),
            "late instance failure must roll back earlier runtime installs",
        );

        let local_b_streams = later_instance
            .list_streams()
            .await
            .expect("list local_b streams");
        assert!(
            local_b_streams
                .iter()
                .any(|stream| stream.definition.id() == req.name),
            "pre-existing conflicting stream must remain in the failing instance",
        );
    }

    #[tokio::test]
    async fn describe_stream_returns_eventtime_and_shared_flags_from_stored_spec() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let state = build_state(&temp_dir, vec![sample_default_instance_spec()]);
        let mut req = mqtt_stream_request("shared_stream_describe");
        req.shared = true;
        req.eventtime = Some(EventtimeConfigRequest {
            column: "event_ts".to_string(),
            eventtime_type: "unixtimestamp_ms".to_string(),
        });

        state
            .storage
            .create_stream(
                crate::storage_bridge::stored_stream_from_request(&req)
                    .expect("serialize stored stream"),
            )
            .expect("store stream");

        let response = describe_stream_handler(State(state), Path(req.name.clone()))
            .await
            .into_response();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("read response body");
        let json: JsonValue = serde_json::from_slice(&body).expect("decode describe response");
        assert_eq!(json["stream"], req.name);
        assert_eq!(json["spec_version"], 1);
        assert_eq!(json["spec"]["type"], "mqtt");
        assert_eq!(json["spec"]["shared"], true);
        assert_eq!(json["spec"]["decoder"]["type"], "json");
        assert_eq!(json["spec"]["eventtime"]["column"], "event_ts");
        assert_eq!(json["spec"]["eventtime"]["type"], "unixtimestamp_ms");
    }

    #[tokio::test]
    async fn describe_stream_omits_empty_broker_url_for_shared_mqtt_stream() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let state = build_state(&temp_dir, vec![sample_default_instance_spec()]);
        let req = mqtt_stream_request_with_connector_key("shared_stream_no_broker", "shared_mqtt");

        state
            .storage
            .create_stream(
                crate::storage_bridge::stored_stream_from_request(&req)
                    .expect("serialize stored stream"),
            )
            .expect("store stream");

        let response = describe_stream_handler(State(state), Path(req.name.clone()))
            .await
            .into_response();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("read response body");
        let json: JsonValue = serde_json::from_slice(&body).expect("decode describe response");
        assert_eq!(
            json["spec"]["props"]["topic"],
            format!("{}/topic", req.name)
        );
        assert_eq!(json["spec"]["props"]["connector_key"], "shared_mqtt");
        assert!(
            json["spec"]["props"].get("broker_url").is_none(),
            "shared mqtt stream describe response should omit empty broker_url",
        );
    }

    #[tokio::test]
    async fn list_streams_round_trips_shared_flag_without_runtime_shared_stream_item() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let state = build_state(&temp_dir, vec![sample_default_instance_spec()]);
        let non_shared = mqtt_stream_request("stream_plain");
        let mut shared = mqtt_stream_request("stream_shared");
        shared.shared = true;

        for req in [&non_shared, &shared] {
            state
                .storage
                .create_stream(
                    crate::storage_bridge::stored_stream_from_request(req)
                        .expect("serialize stored stream"),
                )
                .expect("store stream");
        }

        let response = list_streams(State(state)).await.into_response();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("read response body");
        let json: JsonValue = serde_json::from_slice(&body).expect("decode list response");
        let streams = json.as_array().expect("stream list array");
        let plain = streams
            .iter()
            .find(|item| item["name"] == "stream_plain")
            .expect("plain stream in list response");
        let shared_item = streams
            .iter()
            .find(|item| item["name"] == "stream_shared")
            .expect("shared stream in list response");

        assert_eq!(plain["shared"], false);
        assert!(plain["shared_stream"].is_null());
        assert_eq!(shared_item["shared"], true);
        assert!(
            shared_item["shared_stream"].is_null(),
            "list endpoint currently exposes stored shared flag but not runtime shared-stream details",
        );
    }

    #[tokio::test]
    async fn delete_stream_returns_conflict_while_stream_is_still_referenced() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let state = build_state(&temp_dir, vec![sample_default_instance_spec()]);
        let mut stream_req = mock_stream_request("shared_stream_in_use");
        stream_req.shared = true;

        let create_resp = create_stream_handler(State(state.clone()), Json(stream_req.clone()))
            .await
            .into_response();
        assert_eq!(create_resp.status(), StatusCode::CREATED);

        let instance = state
            .local_instance("default")
            .expect("default runtime instance");
        let pipeline_req = pipeline_request("pipe_using_shared_stream", &stream_req.name);
        let definition = crate::pipeline::build_pipeline_definition(
            &pipeline_req,
            instance.encoder_registry().as_ref(),
            instance.as_ref(),
        )
        .expect("build pipeline definition");
        instance
            .create_pipeline(flow::CreatePipelineRequest::new(definition))
            .expect("create pipeline");

        let delete_resp =
            delete_stream_handler(State(state.clone()), Path(stream_req.name.clone()))
                .await
                .into_response();
        assert_eq!(delete_resp.status(), StatusCode::CONFLICT);

        let body = to_bytes(delete_resp.into_body(), 64 * 1024)
            .await
            .expect("read delete response body");
        assert_eq!(
            String::from_utf8(body.to_vec()).expect("utf8 response"),
            "stream shared_stream_in_use still referenced by pipelines: pipe_using_shared_stream"
        );
        assert!(
            state
                .storage
                .get_stream(&stream_req.name)
                .expect("read stored stream")
                .is_some(),
            "referenced stream must remain persisted after delete conflict",
        );
        assert!(
            instance.get_stream(&stream_req.name).await.is_ok(),
            "referenced stream must remain installed in runtime after delete conflict",
        );
    }
}
