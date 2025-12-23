use crate::{DEFAULT_BROKER_URL, MQTT_QOS, SINK_TOPIC, storage_bridge};
use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use flow::EncoderRegistry;
use flow::FlowInstance;
use flow::pipeline::{
    MqttSinkProps, PipelineDefinition, PipelineError, PipelineOptions, PipelineStatus,
    PlanCacheOptions, SinkDefinition, SinkProps, SinkType,
};
use flow::planner::sink::{CommonSinkProps, SinkEncoderConfig};
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use storage::{StorageError, StorageManager};

#[derive(Clone)]
pub struct AppState {
    pub instance: Arc<FlowInstance>,
    pub storage: Arc<StorageManager>,
}

impl AppState {
    pub fn new(instance: FlowInstance, storage: StorageManager) -> Self {
        Self {
            instance: Arc::new(instance),
            storage: Arc::new(storage),
        }
    }
}

#[derive(Deserialize, Serialize)]
pub struct CreatePipelineRequest {
    pub id: String,
    pub sql: String,
    #[serde(default)]
    pub sinks: Vec<CreatePipelineSinkRequest>,
    #[serde(default)]
    pub options: PipelineOptionsRequest,
}

#[derive(Deserialize, Serialize, Default, Clone)]
#[serde(default)]
pub struct PipelineOptionsRequest {
    #[serde(rename = "plan_cache")]
    pub plan_cache: PlanCacheOptionsRequest,
}

#[derive(Deserialize, Serialize, Default, Clone)]
#[serde(default)]
pub struct PlanCacheOptionsRequest {
    pub enabled: bool,
}

#[derive(Serialize)]
pub struct CreatePipelineResponse {
    pub id: String,
    pub status: String,
}

#[derive(Serialize)]
pub struct ListPipelineItem {
    pub id: String,
    pub status: String,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct CreatePipelineSinkRequest {
    pub id: Option<String>,
    #[serde(rename = "type")]
    pub sink_type: String,
    #[serde(default)]
    pub props: SinkPropsRequest,
    #[serde(rename = "commonSinkProps", default)]
    pub common: CommonSinkPropsRequest,
    #[serde(default)]
    pub encoder: EncoderConfigRequest,
}

#[derive(Deserialize, Serialize, Clone)]
#[serde(default)]
pub struct EncoderConfigRequest {
    #[serde(rename = "type")]
    pub encode_type: String,
    pub props: JsonMap<String, JsonValue>,
}

impl EncoderConfigRequest {
    fn new(encode_type: impl Into<String>, props: JsonMap<String, JsonValue>) -> Self {
        Self {
            encode_type: encode_type.into(),
            props,
        }
    }
}

impl Default for EncoderConfigRequest {
    fn default() -> Self {
        Self::new("json", JsonMap::new())
    }
}

#[derive(Deserialize, Serialize, Default, Clone)]
#[serde(default)]
pub struct SinkPropsRequest {
    #[serde(flatten)]
    fields: JsonMap<String, JsonValue>,
}

impl SinkPropsRequest {
    fn to_value(&self) -> JsonValue {
        JsonValue::Object(self.fields.clone())
    }
}

impl CommonSinkPropsRequest {
    fn to_common_props(&self) -> CommonSinkProps {
        let duration = self.batch_duration_ms.map(Duration::from_millis);
        CommonSinkProps {
            batch_count: self.batch_count,
            batch_duration: duration,
        }
    }
}

#[derive(Deserialize, Serialize, Default, Clone)]
#[serde(default)]
pub struct MqttSinkPropsRequest {
    pub broker_url: Option<String>,
    pub topic: Option<String>,
    pub qos: Option<u8>,
    pub retain: Option<bool>,
    pub client_id: Option<String>,
    pub connector_key: Option<String>,
}

#[derive(Deserialize, Serialize, Default, Clone)]
#[serde(default)]
pub struct CommonSinkPropsRequest {
    #[serde(rename = "batchCount")]
    pub batch_count: Option<usize>,
    #[serde(rename = "batchDuration")]
    pub batch_duration_ms: Option<u64>,
}

pub async fn create_pipeline_handler(
    State(state): State<AppState>,
    Json(req): Json<CreatePipelineRequest>,
) -> impl IntoResponse {
    if let Err(err) = validate_create_request(&req) {
        return (StatusCode::BAD_REQUEST, err).into_response();
    }
    let encoder_registry = state.instance.encoder_registry();
    let definition = match build_pipeline_definition(&req, encoder_registry.as_ref()) {
        Ok(def) => def,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let stored = match storage_bridge::stored_pipeline_from_request(&req) {
        Ok(stored) => stored,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    match state.storage.create_pipeline(stored.clone()) {
        Ok(()) => {}
        Err(StorageError::AlreadyExists(_)) => {
            return (
                StatusCode::CONFLICT,
                format!("pipeline {} already exists", req.id),
            )
                .into_response();
        }
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to persist pipeline {}: {err}", req.id),
            )
                .into_response();
        }
    }

    let build_result = match state.instance.create_pipeline_with_plan_cache(
        definition,
        flow::planner::plan_cache::PlanCacheInputs {
            pipeline_raw_json: stored.raw_json.clone(),
            streams_raw_json: Vec::new(),
            snapshot: None,
        },
    ) {
        Ok(result) => result,
        Err(PipelineError::AlreadyExists(_)) => {
            let _ = state.storage.delete_pipeline(&stored.id);
            return (
                StatusCode::CONFLICT,
                format!("pipeline {} already exists", req.id),
            )
                .into_response();
        }
        Err(err) => {
            let _ = state.storage.delete_pipeline(&stored.id);
            return (
                StatusCode::BAD_REQUEST,
                format!("failed to create pipeline {}: {err}", req.id),
            )
                .into_response();
        }
    };

    if let Some(logical_ir) = build_result.logical_plan_ir {
        match storage_bridge::build_plan_snapshot(
            state.storage.as_ref(),
            &stored.id,
            &stored.raw_json,
            &build_result.snapshot.streams,
            logical_ir,
        )
        .and_then(|record| {
            state
                .storage
                .put_plan_snapshot(record)
                .map_err(|e| e.to_string())
        }) {
            Ok(()) => {}
            Err(err) => {
                let _ = state.instance.delete_pipeline(&stored.id).await;
                let _ = state.storage.delete_pipeline(&stored.id);
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!(
                        "pipeline {} created but failed to persist plan cache: {err}",
                        req.id
                    ),
                )
                    .into_response();
            }
        }
    }

    let snapshot = build_result.snapshot;
    {
        println!("[manager] pipeline {} created", snapshot.definition.id());
        (
            StatusCode::CREATED,
            Json(CreatePipelineResponse {
                id: snapshot.definition.id().to_string(),
                status: status_label(snapshot.status),
            }),
        )
            .into_response()
    }
}

pub async fn start_pipeline_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.instance.start_pipeline(&id) {
        Ok(_) => {
            println!("[manager] pipeline {} started", id);
            (StatusCode::OK, format!("pipeline {id} started")).into_response()
        }
        Err(PipelineError::NotFound(_)) => {
            (StatusCode::NOT_FOUND, format!("pipeline {id} not found")).into_response()
        }
        Err(err) => (
            StatusCode::BAD_REQUEST,
            format!("failed to start pipeline {id}: {err}"),
        )
            .into_response(),
    }
}

pub async fn delete_pipeline_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.instance.delete_pipeline(&id).await {
        Ok(_) => {
            if let Err(err) = state.storage.delete_pipeline(&id) {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!(
                        "pipeline {id} deleted in runtime but failed to remove from storage: {err}"
                    ),
                )
                    .into_response();
            }
            (StatusCode::OK, format!("pipeline {id} deleted")).into_response()
        }
        Err(PipelineError::NotFound(_)) => {
            (StatusCode::NOT_FOUND, format!("pipeline {id} not found")).into_response()
        }
        Err(err) => (
            StatusCode::BAD_REQUEST,
            format!("failed to delete pipeline {id}: {err}"),
        )
            .into_response(),
    }
}

pub async fn list_pipelines(State(state): State<AppState>) -> impl IntoResponse {
    let runtime_status: HashMap<String, String> = state
        .instance
        .list_pipelines()
        .into_iter()
        .map(|snapshot| {
            (
                snapshot.definition.id().to_string(),
                status_label(snapshot.status),
            )
        })
        .collect();

    match state.storage.list_pipelines() {
        Ok(entries) => {
            let list = entries
                .into_iter()
                .map(|entry| {
                    let status = runtime_status
                        .get(&entry.id)
                        .cloned()
                        .unwrap_or_else(|| "created".to_string());
                    ListPipelineItem {
                        id: entry.id,
                        status,
                    }
                })
                .collect::<Vec<_>>();
            Json(list).into_response()
        }
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to list pipelines: {err}"),
        )
            .into_response(),
    }
}

fn validate_create_request(req: &CreatePipelineRequest) -> Result<(), String> {
    if req.id.trim().is_empty() {
        return Err("pipeline id must not be empty".to_string());
    }
    if req.sql.trim().is_empty() {
        return Err("pipeline sql must not be empty".to_string());
    }
    if req.sinks.is_empty() {
        return Err("pipeline must define at least one sink".to_string());
    }
    Ok(())
}

pub(crate) fn build_pipeline_definition(
    req: &CreatePipelineRequest,
    encoder_registry: &EncoderRegistry,
) -> Result<PipelineDefinition, String> {
    let mut sinks = Vec::with_capacity(req.sinks.len());
    for (index, sink_req) in req.sinks.iter().enumerate() {
        let sink_id = sink_req
            .id
            .clone()
            .unwrap_or_else(|| format!("{}_sink_{index}", req.id));
        let sink_type = sink_req.sink_type.to_ascii_lowercase();
        let sink_definition = match sink_type.as_str() {
            "mqtt" => {
                let mqtt_props: MqttSinkPropsRequest =
                    serde_json::from_value(sink_req.props.to_value())
                        .map_err(|err| format!("invalid mqtt sink props: {err}"))?;
                let broker = mqtt_props
                    .broker_url
                    .unwrap_or_else(|| DEFAULT_BROKER_URL.to_string());
                let topic = mqtt_props.topic.unwrap_or_else(|| SINK_TOPIC.to_string());
                let qos = mqtt_props.qos.unwrap_or(MQTT_QOS);
                let retain = mqtt_props.retain.unwrap_or(false);

                let mut props = MqttSinkProps::new(broker, topic, qos).with_retain(retain);
                if let Some(client_id) = mqtt_props.client_id {
                    props = props.with_client_id(client_id);
                }
                if let Some(connector_key) = mqtt_props.connector_key {
                    props = props.with_connector_key(connector_key);
                }
                SinkDefinition::new(sink_id.clone(), SinkType::Mqtt, SinkProps::Mqtt(props))
            }
            other => return Err(format!("unsupported sink type: {other}")),
        };
        let encoder_kind = sink_req.encoder.encode_type.clone();
        if !encoder_registry.is_registered(&encoder_kind) {
            return Err(format!("encoder kind `{encoder_kind}` not registered"));
        }
        let encoder_config = SinkEncoderConfig::new(encoder_kind, sink_req.encoder.props.clone());
        let sink_definition = sink_definition
            .with_encoder(encoder_config)
            .with_common_props(sink_req.common.to_common_props());
        sinks.push(sink_definition);
    }
    let options = PipelineOptions {
        plan_cache: PlanCacheOptions {
            enabled: req.options.plan_cache.enabled,
        },
    };
    Ok(PipelineDefinition::new(req.id.clone(), req.sql.clone(), sinks).with_options(options))
}

fn status_label(status: PipelineStatus) -> String {
    match status {
        PipelineStatus::Created => "created".to_string(),
        PipelineStatus::Running => "running".to_string(),
    }
}
