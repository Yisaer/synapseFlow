#![cfg_attr(
    not(test),
    deny(clippy::unwrap_used, clippy::unreachable, clippy::panic)
)]
#![forbid(unsafe_code)]

mod audit;
mod capabilities;
mod export;
mod function;
mod import;
mod init_process;
mod instances;
mod memory_topic;
mod mqtt_client;
mod pipeline;
mod resource_id;
mod schema;
mod startup;
mod status;
pub mod storage_bridge;
mod stream;
mod table;
#[cfg(feature = "wasm_udf")]
#[cfg(feature = "wasm_udf")]
mod udf_handler;
mod upload_handler;
use axum::Router;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{delete, get, post};
use pipeline::AppState;
use pipeline::state::DEFAULT_PATROL_INTERVAL_SECS;
use prometheus::{Encoder, TextEncoder};
use startup::StartupPhase;
use std::future::{Future, pending};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::mpsc::SyncSender;
use storage::StorageManager;
use tokio::net::TcpListener;
use tower_http::cors::CorsLayer;

pub use instances::{
    DEFAULT_FLOW_INSTANCE_ID, FlowInstanceSpec, build_in_process_flow_instance,
    find_default_flow_instance_spec, new_default_flow_instance,
};
pub use stream::{
    ParsedSchema, SchemaArtifact, SchemaParser, named_schema_store, register_schema,
    schema_registry,
};

pub(crate) static MQTT_QOS: u8 = 0;

pub struct ManagerStartupOptions {
    pub patrol_interval_secs: u64,
    pub init_dir: Option<PathBuf>,
}

fn build_app(state: AppState) -> Router {
    let app = Router::new()
        .route(
            "/pipelines",
            post(pipeline::create_pipeline_handler).get(pipeline::list_pipelines),
        )
        .route(
            "/pipelines/:id/start",
            post(pipeline::start_pipeline_handler),
        )
        .route("/pipelines/:id/stop", post(pipeline::stop_pipeline_handler))
        .route(
            "/pipelines/:id",
            get(pipeline::get_pipeline_handler)
                .put(pipeline::upsert_pipeline_handler)
                .delete(pipeline::delete_pipeline_handler),
        )
        .route(
            "/pipelines/:id/explain",
            get(pipeline::explain_pipeline_handler),
        )
        .route(
            "/pipelines/:id/stats",
            get(pipeline::collect_pipeline_stats_handler),
        )
        .route(
            "/streams",
            post(stream::create_stream_handler).get(stream::list_streams),
        )
        .route(
            "/streams/describe/:name",
            axum::routing::get(stream::describe_stream_handler),
        )
        .route(
            "/streams/:name/shared/stats",
            axum::routing::get(stream::shared_stream_stats_handler),
        )
        .route(
            "/functions",
            axum::routing::get(function::list_functions_handler),
        )
        .route(
            "/functions/describe/:name",
            axum::routing::get(function::describe_function_handler),
        )
        .route(
            "/capabilities/syntax",
            axum::routing::get(capabilities::get_syntax_capabilities_handler),
        )
        .route(
            "/import",
            post(import::import_storage_handler).layer(axum::extract::DefaultBodyLimit::max(
                import::MAX_ARCHIVE_BODY_SIZE,
            )),
        )
        .route("/storage/export", get(export::export_storage_handler))
        .route(
            "/streams/:name",
            delete(stream::delete_stream_handler).put(stream::upsert_stream_handler),
        )
        .route(
            "/schemas",
            post(schema::handler::create_schema_handler).get(schema::handler::list_schemas_handler),
        )
        .route(
            "/schemas/:name",
            get(schema::handler::get_schema_handler).delete(schema::handler::delete_schema_handler),
        )
        .route(
            "/memory/topics",
            post(memory_topic::create_memory_topic_handler)
                .get(memory_topic::list_memory_topics_handler),
        )
        .route(
            "/mqtt/clients",
            post(mqtt_client::create_shared_mqtt_client_handler)
                .get(mqtt_client::list_shared_mqtt_clients_handler),
        )
        .route(
            "/mqtt/clients/:key",
            get(mqtt_client::get_shared_mqtt_client_handler)
                .delete(mqtt_client::delete_shared_mqtt_client_handler),
        )
        .route("/files/upload", post(upload_handler::upload_file_handler))
        .route("/files", get(upload_handler::list_files_handler))
        .route(
            "/files/:name",
            get(upload_handler::get_file_handler).delete(upload_handler::delete_file_handler),
        )
        .route(
            "/tables",
            post(table::create_table_handler).get(table::list_tables),
        )
        .route("/tables/:name", delete(table::delete_table_handler))
        .route("/metrics", get(metrics_handler))
        .route("/status", get(status::status_handler))
        .layer(CorsLayer::permissive());
    add_udf_routes(app).with_state(state)
}

async fn metrics_handler() -> impl IntoResponse {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = vec![];
    match encoder.encode(&metric_families, &mut buffer) {
        Ok(()) => (
            StatusCode::OK,
            [(
                axum::http::header::CONTENT_TYPE,
                "text/plain; version=0.0.4",
            )],
            buffer,
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to encode metrics: {err}"),
        )
            .into_response(),
    }
}

#[cfg(feature = "wasm_udf")]
fn add_udf_routes(app: Router<AppState>) -> Router<AppState> {
    app.route("/udfs/upload", post(udf_handler::upload_udf_handler))
        .route("/udfs", get(udf_handler::list_udfs_handler))
        .route(
            "/udfs/:name",
            get(udf_handler::get_udf_handler).delete(udf_handler::delete_udf_handler),
        )
}

#[cfg(not(feature = "wasm_udf"))]
fn add_udf_routes(app: Router<AppState>) -> Router<AppState> {
    app
}

async fn serve_manager_with_listener<F>(
    listener: TcpListener,
    instance: flow::FlowInstance,
    storage: StorageManager,
    flow_instances: Vec<FlowInstanceSpec>,
    shutdown: F,
    startup_tx: Option<SyncSender<Result<(), String>>>,
    options: ManagerStartupOptions,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    F: Future<Output = ()> + Send + 'static,
{
    let state = AppState::new_with_init_dir(
        instance,
        storage,
        flow_instances,
        options.patrol_interval_secs,
        options.init_dir,
    )
    .map_err(|err| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("invalid config: {err}"),
        )
    })?;
    if let Err(err) = state.bootstrap_from_storage().await {
        let err = format!("failed to bootstrap from storage: {err}");
        if let Some(tx) = startup_tx {
            let _ = tx.send(Err(err.clone()));
        }
        return Err(err.into());
    }
    if let Some(tx) = startup_tx {
        let _ = tx.send(Ok(()));
    }
    status::init_uptime();
    let app = build_app(state);
    axum::serve(listener, app.into_make_service())
        .with_graceful_shutdown(shutdown)
        .await?;
    Ok(())
}

pub async fn start_server_with_listener(
    listener: TcpListener,
    instance: flow::FlowInstance,
    storage: StorageManager,
    flow_instances: Vec<FlowInstanceSpec>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    start_server_with_listener_and_shutdown(
        instance,
        storage,
        flow_instances,
        pending(),
        None,
        listener,
        ManagerStartupOptions {
            patrol_interval_secs: DEFAULT_PATROL_INTERVAL_SECS,
            init_dir: None,
        },
    )
    .await
}

pub async fn start_server_with_listener_and_shutdown<F>(
    instance: flow::FlowInstance,
    storage: StorageManager,
    flow_instances: Vec<FlowInstanceSpec>,
    shutdown: F,
    startup_tx: Option<SyncSender<Result<(), String>>>,
    listener: TcpListener,
    options: ManagerStartupOptions,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    F: Future<Output = ()> + Send + 'static,
{
    serve_manager_with_listener(
        listener,
        instance,
        storage,
        flow_instances,
        shutdown,
        startup_tx,
        options,
    )
    .await
}

pub async fn start_server(
    addr: String,
    instance: flow::FlowInstance,
    storage: StorageManager,
    flow_instances: Vec<FlowInstanceSpec>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr: SocketAddr = addr.parse()?;
    let bind_phase = StartupPhase::new("manager", "default", "server_bind");
    let listener = match TcpListener::bind(addr).await {
        Ok(listener) => listener,
        Err(err) => {
            bind_phase.log_failure(&err);
            return Err(err.into());
        }
    };
    tracing::info!(
        mode = "manager",
        flow_instance_id = "default",
        phase = "server_bind",
        result = "succeeded",
        elapsed_ms = bind_phase.elapsed_ms(),
        manager_addr = %addr,
        "manager listening"
    );
    start_server_with_listener(listener, instance, storage, flow_instances).await
}

pub async fn start_server_with_shutdown<F>(
    addr: String,
    instance: flow::FlowInstance,
    storage: StorageManager,
    flow_instances: Vec<FlowInstanceSpec>,
    shutdown: F,
    startup_tx: Option<SyncSender<Result<(), String>>>,
    options: ManagerStartupOptions,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    F: Future<Output = ()> + Send + 'static,
{
    let addr: SocketAddr = addr.parse()?;
    let bind_phase = StartupPhase::new("manager", "default", "server_bind");
    let listener = match TcpListener::bind(addr).await {
        Ok(listener) => listener,
        Err(err) => {
            if let Some(tx) = startup_tx {
                let _ = tx.send(Err(err.to_string()));
            }
            bind_phase.log_failure(&err);
            return Err(err.into());
        }
    };
    tracing::info!(
        mode = "manager",
        flow_instance_id = "default",
        phase = "server_bind",
        result = "succeeded",
        elapsed_ms = bind_phase.elapsed_ms(),
        manager_addr = %addr,
        "manager listening"
    );
    start_server_with_listener_and_shutdown(
        instance,
        storage,
        flow_instances,
        shutdown,
        startup_tx,
        listener,
        options,
    )
    .await
}
