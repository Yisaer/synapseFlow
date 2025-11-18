use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, post},
};
use flow::connector::sink::nop::NopSinkConnector;
use flow::connector::{MqttSinkConfig, MqttSinkConnector, MqttSourceConfig, MqttSourceConnector};
use flow::processor::Processor;
use flow::processor::processor_builder::{PlanProcessor, ProcessorPipeline};
use flow::{JsonDecoder, JsonEncoder, SinkProcessor, create_pipeline};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::Mutex;

static DEFAULT_BROKER_URL: &str = "tcp://127.0.0.1:1883";
static SOURCE_TOPIC: &str = "/yisa/data";
static SINK_TOPIC: &str = "/yisa/data2";
static MQTT_QOS: u8 = 0;

#[derive(Clone, Debug, Serialize)]
enum PipelineStatus {
    Created,
    Running,
    // Stopped,
}

struct PipelineEntry {
    pipeline: ProcessorPipeline,
    status: PipelineStatus,
}

#[derive(Clone)]
struct AppState {
    pipelines: Arc<Mutex<HashMap<String, PipelineEntry>>>,
}

#[derive(Deserialize)]
struct CreatePipelineRequest {
    id: String,
    sql: String,
    // #[serde(default)]
    // forward_to_result: bool,
    #[serde(default)]
    sinks: Option<Vec<String>>,
    #[serde(default)]
    source_broker: Option<String>,
    #[serde(default)]
    source_topic: Option<String>,
    #[serde(default)]
    sink_topic: Option<String>,
    #[serde(default)]
    qos: Option<u8>,
}

#[derive(Serialize)]
struct CreatePipelineResponse {
    id: String,
    status: PipelineStatus,
}

#[derive(Serialize)]
struct ListPipelineItem {
    id: String,
    status: PipelineStatus,
}

pub async fn start_server(addr: String) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let state = AppState {
        pipelines: Arc::new(Mutex::new(HashMap::new())),
    };

    let app = Router::new()
        .route(
            "/pipelines",
            post(create_pipeline_handler).get(list_pipelines),
        )
        .route("/pipelines/:id/start", post(start_pipeline_handler))
        .route("/pipelines/:id", delete(delete_pipeline_handler))
        .with_state(state);

    let addr: SocketAddr = addr.parse()?;
    println!("Manager listening on http://{addr}");
    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app.into_make_service()).await?;
    Ok(())
}

async fn create_pipeline_handler(
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

    let pipeline = match build_pipeline(&req) {
        Ok(p) => p,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };

    pipelines.insert(
        req.id.clone(),
        PipelineEntry {
            pipeline,
            status: PipelineStatus::Created,
        },
    );

    (
        StatusCode::CREATED,
        Json(CreatePipelineResponse {
            id: req.id,
            status: PipelineStatus::Created,
        }),
    )
        .into_response()
}

async fn start_pipeline_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let mut pipelines = state.pipelines.lock().await;
    let entry = match pipelines.get_mut(&id) {
        Some(e) => e,
        None => return (StatusCode::NOT_FOUND, format!("pipeline {id} not found")).into_response(),
    };

    if let PipelineStatus::Running = entry.status {
        return (StatusCode::OK, format!("pipeline {id} already running")).into_response();
    }

    entry.pipeline.start();
    entry.status = PipelineStatus::Running;
    (StatusCode::OK, format!("pipeline {id} started")).into_response()
}

async fn delete_pipeline_handler(
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
        }
        PipelineStatus::Created => {}
    }

    (StatusCode::OK, format!("pipeline {id} deleted")).into_response()
}

async fn list_pipelines(State(state): State<AppState>) -> impl IntoResponse {
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

fn build_pipeline(req: &CreatePipelineRequest) -> Result<ProcessorPipeline, String> {
    let broker = req.source_broker.as_deref().unwrap_or(DEFAULT_BROKER_URL);
    let source_topic = req.source_topic.as_deref().unwrap_or(SOURCE_TOPIC);
    let sink_topic = req.sink_topic.as_deref().unwrap_or(SINK_TOPIC);
    let qos = req.qos.unwrap_or(MQTT_QOS);

    let sinks = build_sinks(req, broker, sink_topic, qos)?;
    let mut pipeline = create_pipeline(&req.sql, sinks).map_err(|err| err.to_string())?;
    attach_mqtt_sources(&mut pipeline, broker, source_topic, qos).map_err(|err| err.to_string())?;
    Ok(pipeline)
}

fn build_sinks(
    req: &CreatePipelineRequest,
    broker: &str,
    sink_topic: &str,
    qos: u8,
) -> Result<Vec<SinkProcessor>, String> {
    let sink_kinds = req
        .sinks
        .clone()
        .unwrap_or_else(|| vec!["mqtt".to_string()]);
    let mut sinks = Vec::new();
    for (idx, kind) in sink_kinds.iter().enumerate() {
        match kind.as_str() {
            "mqtt" => {
                let mut sink = SinkProcessor::new(format!("{}_sink_{idx}", req.id));
                sink.disable_result_forwarding();
                let sink_config = MqttSinkConfig::new(sink.id(), broker, sink_topic, qos);
                let sink_connector =
                    MqttSinkConnector::new(format!("{}_sink_connector_{idx}", req.id), sink_config);
                sink.add_connector(
                    Box::new(sink_connector),
                    Arc::new(JsonEncoder::new(format!("{}_sink_encoder_{idx}", req.id))),
                );
                sinks.push(sink);
            }
            "nop" => {
                let mut sink = SinkProcessor::new(format!("{}_nop_sink_{idx}", req.id));
                sink.disable_result_forwarding();
                sink.add_connector(
                    Box::new(NopSinkConnector::new(format!(
                        "{}_nop_connector_{idx}",
                        req.id
                    ))),
                    Arc::new(JsonEncoder::new(format!("{}_nop_encoder_{idx}", req.id))),
                );
                sinks.push(sink);
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
            let config = MqttSourceConfig::new(
                source_id.clone(),
                broker_url.to_string(),
                topic.to_string(),
                qos,
            );
            let connector =
                MqttSourceConnector::new(format!("{source_id}_source_connector"), config);
            let decoder = Arc::new(JsonDecoder::new(source_id));
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
