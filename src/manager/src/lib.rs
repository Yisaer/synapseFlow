use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, post},
};
use flow::catalog::global_catalog;
use flow::connector::sink::nop::NopSinkConnector;
use flow::connector::{MqttSinkConfig, MqttSinkConnector, MqttSourceConfig, MqttSourceConnector};
use flow::processor::Processor;
use flow::processor::processor_builder::{PlanProcessor, ProcessorPipeline};
use flow::{
    BooleanType, ColumnSchema, ConcreteDatatype, Float32Type, Float64Type, Int8Type, Int16Type,
    Int32Type, Int64Type, ListType, Schema, StringType, StructType, Uint8Type, Uint16Type,
    Uint32Type, Uint64Type,
};
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

#[derive(Deserialize)]
struct CreateSchemaRequest {
    source: String,
    columns: Vec<SchemaColumnRequest>,
}

#[derive(Deserialize)]
struct SchemaColumnRequest {
    name: String,
    data_type: String,
}

#[derive(Serialize)]
struct SchemaColumnInfo {
    name: String,
    data_type: String,
}

#[derive(Serialize)]
struct SchemaInfo {
    source: String,
    columns: Vec<SchemaColumnInfo>,
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
        .route("/schemas", post(create_schema_handler).get(list_schemas))
        .route("/schemas/:source", delete(delete_schema_handler))
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
        println!("[manager] pipeline {} already running", id);
        return (StatusCode::OK, format!("pipeline {id} already running")).into_response();
    }

    entry.pipeline.start();
    entry.status = PipelineStatus::Running;
    println!("[manager] pipeline {} started", id);
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
            println!("[manager] pipeline {id} quick close completed");
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

async fn create_schema_handler(Json(req): Json<CreateSchemaRequest>) -> impl IntoResponse {
    if req.source.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            "source must not be empty".to_string(),
        )
            .into_response();
    }
    if req.columns.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            "schema must contain at least one column".to_string(),
        )
            .into_response();
    }

    let schema = match build_schema_from_request(&req) {
        Ok(schema) => schema,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };

    let catalog = global_catalog();
    match catalog.insert(req.source.clone(), schema) {
        Ok(_) => {
            println!("[manager] schema {} created", req.source);
            (StatusCode::CREATED, Json(req.source)).into_response()
        }
        Err(err) => (
            StatusCode::CONFLICT,
            format!("failed to create schema: {}", err),
        )
            .into_response(),
    }
}

async fn list_schemas() -> impl IntoResponse {
    let catalog = global_catalog();
    let payload: Vec<SchemaInfo> = catalog
        .list()
        .into_iter()
        .map(|(source, schema)| SchemaInfo {
            source,
            columns: schema
                .column_schemas()
                .iter()
                .map(|col| SchemaColumnInfo {
                    name: col.name.clone(),
                    data_type: datatype_name(&col.data_type),
                })
                .collect(),
        })
        .collect();
    Json(payload)
}

async fn delete_schema_handler(Path(source): Path<String>) -> impl IntoResponse {
    let catalog = global_catalog();
    match catalog.remove(&source) {
        Ok(_) => (StatusCode::OK, format!("schema {source} deleted")).into_response(),
        Err(_) => (StatusCode::NOT_FOUND, format!("schema {source} not found")).into_response(),
    }
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

fn build_schema_from_request(req: &CreateSchemaRequest) -> Result<Schema, String> {
    let columns: Result<Vec<ColumnSchema>, String> = req
        .columns
        .iter()
        .map(|col| {
            parse_datatype(&col.data_type)
                .map(|datatype| ColumnSchema::new(req.source.clone(), col.name.clone(), datatype))
        })
        .collect();
    columns.map(Schema::new)
}

fn parse_datatype(datatype: &str) -> Result<ConcreteDatatype, String> {
    match datatype.to_ascii_lowercase().as_str() {
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
        "list" => Ok(ConcreteDatatype::List(ListType::new(Arc::new(
            ConcreteDatatype::Null,
        )))),
        "struct" => Ok(ConcreteDatatype::Struct(
            StructType::new(Default::default()),
        )),
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
        ConcreteDatatype::Struct(_) => "struct",
        ConcreteDatatype::List(_) => "list",
        ConcreteDatatype::Bool(_) => "bool",
    }
    .to_string()
}
