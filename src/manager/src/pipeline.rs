use crate::{DEFAULT_BROKER_URL, MQTT_QOS, SINK_TOPIC, SOURCE_TOPIC};
use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use flow::connector::{MqttSinkConfig, MqttSourceConfig, MqttSourceConnector};
use flow::processor::Processor;
use flow::processor::processor_builder::{PlanProcessor, ProcessorPipeline};
use flow::{
    JsonDecoder, NopSinkConfig, PipelineSink, PipelineSinkConnector, SinkConnectorConfig,
    SinkEncoderConfig, create_pipeline,
};
use parser::parse_sql;
use serde::{Deserialize, Serialize};
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
}

impl AppState {
    pub fn new() -> Self {
        Self {
            pipelines: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[derive(Deserialize)]
pub struct CreatePipelineRequest {
    pub id: String,
    pub sql: String,
    #[serde(default)]
    pub sinks: Option<Vec<String>>,
    #[serde(default)]
    pub source_broker: Option<String>,
    #[serde(default)]
    pub source_topic: Option<String>,
    #[serde(default)]
    pub sink_topic: Option<String>,
    #[serde(default)]
    pub qos: Option<u8>,
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

pub async fn create_pipeline_handler(
    State(state): State<AppState>,
    Json(req): Json<CreatePipelineRequest>,
) -> impl IntoResponse {
    let mut pipelines = state.pipelines.lock().await;
    if pipelines.contains_key(&req.id) {
        return (
            StatusCode::CONFLICT,
            format!("pipeline {} already exists", req.id),
        )
            .into_response();
    }

    let (pipeline, streams) = match build_pipeline(&req) {
        Ok(p) => p,
        Err(err) => {
            println!("[manager] failed to create pipeline {}: {}", req.id, err);
            return (StatusCode::BAD_REQUEST, err).into_response();
        }
    };

    pipelines.insert(
        req.id.clone(),
        PipelineEntry {
            pipeline,
            status: PipelineStatus::Created,
            streams,
        },
    );
    println!("[manager] pipeline {} created", req.id);

    (
        StatusCode::CREATED,
        Json(CreatePipelineResponse {
            id: req.id,
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

    (StatusCode::OK, format!("pipeline {id} deleted")).into_response()
}

pub async fn list_pipelines(State(state): State<AppState>) -> impl IntoResponse {
    let pipelines = state.pipelines.lock().await;
    let list: Vec<ListPipelineItem> = pipelines
        .iter()
        .map(|(id, entry)| ListPipelineItem {
            id: id.clone(),
            status: entry.status.clone(),
        })
        .collect();
    Json(list)
}

fn build_pipeline(req: &CreatePipelineRequest) -> Result<(ProcessorPipeline, Vec<String>), String> {
    let select_stmt = parse_sql(&req.sql).map_err(|err| err.to_string())?;
    let streams: Vec<String> = select_stmt
        .source_infos
        .iter()
        .map(|info| info.name.clone())
        .collect();
    let broker = req.source_broker.as_deref().unwrap_or(DEFAULT_BROKER_URL);
    let source_topic = req.source_topic.as_deref().unwrap_or(SOURCE_TOPIC);
    let sink_topic = req.sink_topic.as_deref().unwrap_or(SINK_TOPIC);
    let qos = req.qos.unwrap_or(MQTT_QOS);

    let sinks = build_sinks(req, broker, sink_topic, qos)?;
    let mut pipeline = create_pipeline(&req.sql, sinks).map_err(|err| err.to_string())?;
    attach_mqtt_sources(&mut pipeline, broker, source_topic, qos).map_err(|err| err.to_string())?;
    pipeline.set_pipeline_id(req.id.clone());
    Ok((pipeline, streams))
}

fn build_sinks(
    req: &CreatePipelineRequest,
    broker: &str,
    sink_topic: &str,
    qos: u8,
) -> Result<Vec<PipelineSink>, String> {
    let sink_kinds = req
        .sinks
        .clone()
        .unwrap_or_else(|| vec!["mqtt".to_string()]);
    let mut sinks = Vec::new();
    for (idx, kind) in sink_kinds.iter().enumerate() {
        match kind.as_str() {
            "mqtt" => {
                let sink_id = format!("{}_sink_{idx}", req.id);
                let connector_id = format!("{}_sink_connector_{idx}", req.id);
                let config = MqttSinkConfig::new(sink_id.clone(), broker, sink_topic, qos);
                let connector = PipelineSinkConnector::new(
                    connector_id,
                    SinkConnectorConfig::Mqtt(config),
                    SinkEncoderConfig::Json {
                        encoder_id: format!("{}_sink_encoder_{idx}", req.id),
                    },
                );
                sinks.push(PipelineSink::new(sink_id, vec![connector]));
            }
            "nop" => {
                let sink_id = format!("{}_nop_sink_{idx}", req.id);
                let connector_id = format!("{}_nop_connector_{idx}", req.id);
                let connector = PipelineSinkConnector::new(
                    connector_id,
                    SinkConnectorConfig::Nop(NopSinkConfig),
                    SinkEncoderConfig::Json {
                        encoder_id: format!("{}_nop_encoder_{idx}", req.id),
                    },
                );
                sinks.push(PipelineSink::new(sink_id, vec![connector]));
            }
            other => return Err(format!("unsupported sink type: {}", other)),
        }
    }
    Ok(sinks)
}

fn attach_mqtt_sources(
    pipeline: &mut ProcessorPipeline,
    broker_url: &str,
    topic: &str,
    qos: u8,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut attached = false;
    for processor in pipeline.middle_processors.iter_mut() {
        if let PlanProcessor::DataSource(ds) = processor {
            let source_id = ds.id().to_string();
            let schema = ds.schema();
            let config = MqttSourceConfig::new(
                source_id.clone(),
                broker_url.to_string(),
                topic.to_string(),
                qos,
            );
            let connector =
                MqttSourceConnector::new(format!("{source_id}_source_connector"), config);
            let decoder = Arc::new(JsonDecoder::new(source_id, schema));
            ds.add_connector(Box::new(connector), decoder);
            attached = true;
        }
    }

    if attached {
        Ok(())
    } else {
        Err("no datasource processors available to attach MQTT source".into())
    }
}
