use crate::{DEFAULT_BROKER_URL, MQTT_QOS, SINK_TOPIC};
use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use flow::catalog::{StreamDefinition, StreamProps, global_catalog};
use flow::connector::{MqttSinkConfig, MqttSourceConfig, MqttSourceConnector};
use flow::pipeline::{
    MqttSinkProps, PipelineDefinition, PipelineError, PipelineManager, SinkDefinition, SinkProps,
    SinkType,
};
use flow::processor::Processor;
use flow::processor::processor_builder::{PlanProcessor, ProcessorPipeline};
use flow::{
    JsonDecoder, PipelineSink, PipelineSinkConnector, SinkConnectorConfig, SinkEncoderConfig,
    create_pipeline,
};
use parser::parse_sql;
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone, Debug, Serialize)]
pub enum PipelineStatus {
    Created,
    Running,
}

pub struct PipelineEntry {
    pub pipeline: ProcessorPipeline,
    pub status: PipelineStatus,
    pub streams: Vec<String>,
}

#[derive(Clone)]
pub struct AppState {
    pub pipelines: Arc<Mutex<HashMap<String, PipelineEntry>>>,
    pub pipeline_manager: Arc<PipelineManager>,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            pipelines: Arc::new(Mutex::new(HashMap::new())),
            pipeline_manager: Arc::new(PipelineManager::new()),
        }
    }
}

#[derive(Deserialize)]
pub struct CreatePipelineRequest {
    pub id: String,
    pub sql: String,
    #[serde(default)]
    pub sinks: Vec<CreatePipelineSinkRequest>,
}

#[derive(Serialize)]
pub struct CreatePipelineResponse {
    pub id: String,
    pub status: PipelineStatus,
}

#[derive(Serialize)]
pub struct ListPipelineItem {
    pub id: String,
    pub status: PipelineStatus,
}

#[derive(Deserialize, Clone)]
pub struct CreatePipelineSinkRequest {
    pub id: Option<String>,
    #[serde(rename = "type")]
    pub sink_type: String,
    #[serde(default)]
    pub props: SinkPropsRequest,
}

#[derive(Deserialize, Default, Clone)]
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

#[derive(Deserialize, Default, Clone)]
#[serde(default)]
pub struct MqttSinkPropsRequest {
    pub broker_url: Option<String>,
    pub topic: Option<String>,
    pub qos: Option<u8>,
    pub retain: Option<bool>,
    pub client_id: Option<String>,
    pub connector_key: Option<String>,
}

pub async fn create_pipeline_handler(
    State(state): State<AppState>,
    Json(req): Json<CreatePipelineRequest>,
) -> impl IntoResponse {
    if let Err(err) = validate_create_request(&req) {
        return (StatusCode::BAD_REQUEST, err).into_response();
    }
    let definition = match build_pipeline_definition(&req) {
        Ok(def) => def,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let pipeline_definition = match state.pipeline_manager.register(definition) {
        Ok(def) => def,
        Err(PipelineError::AlreadyExists(_)) => {
            return (
                StatusCode::CONFLICT,
                format!("pipeline {} already exists", req.id),
            )
                .into_response();
        }
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to register pipeline {}: {err}", req.id),
            )
                .into_response();
        }
    };

    let mut pipelines = state.pipelines.lock().await;
    if pipelines.contains_key(pipeline_definition.id()) {
        let _ = state.pipeline_manager.remove(pipeline_definition.id());
        return (
            StatusCode::CONFLICT,
            format!("pipeline {} already exists", pipeline_definition.id()),
        )
            .into_response();
    }

    let (pipeline, streams) = match build_pipeline(&pipeline_definition) {
        Ok(p) => p,
        Err(err) => {
            println!("[manager] failed to create pipeline {}: {}", req.id, err);
            let _ = state.pipeline_manager.remove(&req.id);
            return (StatusCode::BAD_REQUEST, err).into_response();
        }
    };

    pipelines.insert(
        pipeline_definition.id().to_string(),
        PipelineEntry {
            pipeline,
            status: PipelineStatus::Created,
            streams,
        },
    );
    println!("[manager] pipeline {} created", pipeline_definition.id());

    (
        StatusCode::CREATED,
        Json(CreatePipelineResponse {
            id: pipeline_definition.id().to_string(),
            status: PipelineStatus::Created,
        }),
    )
        .into_response()
}

pub async fn start_pipeline_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let mut pipelines = state.pipelines.lock().await;
    let entry = match pipelines.get_mut(&id) {
        Some(e) => e,
        None => return (StatusCode::NOT_FOUND, format!("pipeline {id} not found")).into_response(),
    };

    if let PipelineStatus::Running = entry.status {
        println!("[manager] pipeline {} already running", id);
        return (StatusCode::OK, format!("pipeline {id} already running")).into_response();
    }

    entry.pipeline.start();
    entry.status = PipelineStatus::Running;
    println!("[manager] pipeline {} started", id);
    (StatusCode::OK, format!("pipeline {id} started")).into_response()
}

pub async fn delete_pipeline_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let mut pipelines = state.pipelines.lock().await;
    let Some(mut entry) = pipelines.remove(&id) else {
        return (StatusCode::NOT_FOUND, format!("pipeline {id} not found")).into_response();
    };

    match entry.status {
        PipelineStatus::Running => {
            if let Err(err) = entry.pipeline.quick_close().await {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("failed to stop pipeline {id}: {err}"),
                )
                    .into_response();
            }
            println!("[manager] pipeline {id} quick close completed");
        }
        PipelineStatus::Created => {}
    }
    if let Err(err) = state.pipeline_manager.remove(&id) {
        if !matches!(err, PipelineError::NotFound(_)) {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to remove pipeline {id}: {err}"),
            )
                .into_response();
        }
    }
    (StatusCode::OK, format!("pipeline {id} deleted")).into_response()
}

pub async fn list_pipelines(State(state): State<AppState>) -> impl IntoResponse {
    let definitions = state.pipeline_manager.list();
    let pipelines = state.pipelines.lock().await;
    let mut list = Vec::with_capacity(definitions.len());
    for definition in definitions {
        let status = pipelines
            .get(definition.id())
            .map(|entry| entry.status.clone())
            .unwrap_or(PipelineStatus::Created);
        list.push(ListPipelineItem {
            id: definition.id().to_string(),
            status,
        });
    }
    Json(list)
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

fn build_pipeline_definition(req: &CreatePipelineRequest) -> Result<PipelineDefinition, String> {
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
                SinkDefinition::new(sink_id, SinkType::Mqtt, SinkProps::Mqtt(props))
            }
            other => return Err(format!("unsupported sink type: {other}")),
        };
        sinks.push(sink_definition);
    }
    Ok(PipelineDefinition::new(
        req.id.clone(),
        req.sql.clone(),
        sinks,
    ))
}

fn build_pipeline(
    definition: &PipelineDefinition,
) -> Result<(ProcessorPipeline, Vec<String>), String> {
    let select_stmt = parse_sql(definition.sql()).map_err(|err| err.to_string())?;
    let streams: Vec<String> = select_stmt
        .source_infos
        .iter()
        .map(|info| info.name.clone())
        .collect();
    let catalog = global_catalog();
    let mut stream_definitions = HashMap::new();
    for stream in &streams {
        let definition = catalog
            .get(stream)
            .ok_or_else(|| format!("stream {stream} not found in catalog"))?;
        stream_definitions.insert(stream.clone(), definition);
    }

    let sinks = build_sinks_from_definition(definition)?;
    let mut pipeline = create_pipeline(definition.sql(), sinks).map_err(|err| err.to_string())?;
    attach_sources_from_catalog(&mut pipeline, &stream_definitions)
        .map_err(|err| err.to_string())?;
    pipeline.set_pipeline_id(definition.id().to_string());
    Ok((pipeline, streams))
}

fn build_sinks_from_definition(
    definition: &PipelineDefinition,
) -> Result<Vec<PipelineSink>, String> {
    let mut sinks = Vec::with_capacity(definition.sinks().len());
    for sink in definition.sinks() {
        match sink.sink_type {
            SinkType::Mqtt => {
                let props = match &sink.props {
                    SinkProps::Mqtt(cfg) => cfg,
                };
                let mut config = MqttSinkConfig::new(
                    sink.sink_id.clone(),
                    props.broker_url.clone(),
                    if props.topic.is_empty() {
                        SINK_TOPIC.to_string()
                    } else {
                        props.topic.clone()
                    },
                    props.qos,
                );
                config = config.with_retain(props.retain);
                if let Some(client_id) = &props.client_id {
                    config = config.with_client_id(client_id.clone());
                }
                if let Some(conn_key) = &props.connector_key {
                    config = config.with_connector_key(conn_key.clone());
                }
                let connector = PipelineSinkConnector::new(
                    sink.sink_id.clone(),
                    SinkConnectorConfig::Mqtt(config),
                    SinkEncoderConfig::Json {
                        encoder_id: format!("{}_sink_encoder", sink.sink_id),
                    },
                );
                sinks.push(PipelineSink::new(sink.sink_id.clone(), vec![connector]));
            }
        }
    }
    Ok(sinks)
}

fn attach_sources_from_catalog(
    pipeline: &mut ProcessorPipeline,
    stream_defs: &HashMap<String, Arc<StreamDefinition>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut attached = false;
    for processor in pipeline.middle_processors.iter_mut() {
        if let PlanProcessor::DataSource(ds) = processor {
            let stream_name = ds.stream_name().to_string();
            let definition = stream_defs.get(&stream_name).ok_or_else(|| {
                format!("stream {stream_name} missing definition when attaching sources")
            })?;
            let stream_props = match definition.props() {
                StreamProps::Mqtt(props) => props,
            };

            let processor_id = ds.id().to_string();
            let schema = ds.schema();

            let mut config = MqttSourceConfig::new(
                processor_id.clone(),
                stream_props.broker_url.clone(),
                stream_props.topic.clone(),
                stream_props.qos,
            );
            if let Some(client_id) = &stream_props.client_id {
                config = config.with_client_id(client_id.clone());
            }
            if let Some(connector_key) = &stream_props.connector_key {
                config = config.with_connector_key(connector_key.clone());
            }
            let connector =
                MqttSourceConnector::new(format!("{processor_id}_source_connector"), config);
            let decoder = Arc::new(JsonDecoder::new(stream_name, schema));
            ds.add_connector(Box::new(connector), decoder);
            attached = true;
        }
    }

    if attached {
        Ok(())
    } else {
        Err("no datasource processors available to attach connectors".into())
    }
}
