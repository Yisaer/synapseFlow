mod capabilities;
mod function;
mod pipeline;
pub mod storage_bridge;
mod stream;

use axum::Router;
use axum::routing::{delete, post};
use pipeline::AppState;
use std::net::SocketAddr;
use storage::StorageManager;
use tokio::net::TcpListener;

pub use stream::{SchemaParser, register_schema, schema_registry};

pub(crate) static DEFAULT_BROKER_URL: &str = "tcp://127.0.0.1:1883";
pub(crate) static SOURCE_TOPIC: &str = "/yisa/data";
pub(crate) static SINK_TOPIC: &str = "/yisa/data2";
pub(crate) static MQTT_QOS: u8 = 0;

pub async fn start_server(
    addr: String,
    instance: flow::FlowInstance,
    storage: StorageManager,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let state = AppState::new(instance, storage);

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
        .route("/pipelines/:id", delete(pipeline::delete_pipeline_handler))
        .route(
            "/streams",
            post(stream::create_stream_handler).get(stream::list_streams),
        )
        .route(
            "/streams/describe/:name",
            axum::routing::get(stream::describe_stream_handler),
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
        .route("/streams/:name", delete(stream::delete_stream_handler))
        .with_state(state);

    let addr: SocketAddr = addr.parse()?;
    tracing::info!(manager_addr = %addr, "manager listening");
    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app.into_make_service()).await?;
    Ok(())
}
