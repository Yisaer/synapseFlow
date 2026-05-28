use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::Serialize;
use std::sync::OnceLock;
use std::time::Instant;

use crate::pipeline::AppState;
use crate::pipeline::status_label;

static PROCESS_START: OnceLock<Instant> = OnceLock::new();

/// Initialize the process startup timestamp. Must be called during server startup
/// before processing any requests, so that `uptime_seconds` reflects the true
/// server lifetime rather than the first-request lifetime.
pub(crate) fn init_uptime() {
    PROCESS_START.get_or_init(Instant::now);
}

fn process_start() -> Instant {
    *PROCESS_START.get_or_init(Instant::now)
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct StatusResponse {
    pub cpu_usage_percent: f64,
    pub memory_usage_bytes: i64,
    pub heap_in_use_bytes: i64,
    pub heap_in_allocator_bytes: i64,
    pub tokio_tasks_inflight: i64,
    pub uptime_seconds: u64,
    pub active_pipeline_count: usize,
    pub commit: String,
}

pub async fn status_handler(State(state): State<AppState>) -> impl IntoResponse {
    let cpu_usage_percent = veloflux_metrics::runtime_cpu_usage_percent().get();
    let memory_usage_bytes = veloflux_metrics::runtime_memory_usage_bytes().get();
    let heap_in_use_bytes = veloflux_metrics::runtime_heap_in_use_bytes().get();
    let heap_in_allocator_bytes = veloflux_metrics::runtime_heap_in_allocator_bytes().get();
    let tokio_tasks_inflight = veloflux_metrics::runtime_tokio_tasks_inflight().get();

    let uptime_seconds = process_start().elapsed().as_secs();

    let mut active_pipeline_count = 0usize;
    for (_, instance) in state.instances.instances_snapshot() {
        for snapshot in instance.list_pipelines() {
            if status_label(snapshot.status) == "running" {
                active_pipeline_count += 1;
            }
        }
    }

    let commit = build_info::git_sha().to_string();

    let response = StatusResponse {
        cpu_usage_percent,
        memory_usage_bytes,
        heap_in_use_bytes,
        heap_in_allocator_bytes,
        tokio_tasks_inflight,
        uptime_seconds,
        active_pipeline_count,
        commit,
    };

    (StatusCode::OK, Json(response)).into_response()
}
