use super::{bind_manager_listener_or_skip, default_flow_instances, http_client, random_suffix};
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Json, Router};
use serde_json::Value as JsonValue;
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Default)]
struct RowRecorder {
    rows: Mutex<Vec<JsonValue>>,
}

impl RowRecorder {
    fn record(&self, payload: JsonValue) -> Result<(), &'static str> {
        let rows = payload
            .as_array()
            .ok_or("HTTP sink payload must be a JSON array")?;
        self.rows
            .lock()
            .expect("lock row recorder")
            .extend(rows.clone());
        Ok(())
    }

    fn rows(&self) -> Vec<JsonValue> {
        self.rows.lock().expect("lock row recorder").clone()
    }

    fn clear(&self) {
        self.rows.lock().expect("lock row recorder").clear();
    }
}

async fn record_rows(
    State(recorder): State<Arc<RowRecorder>>,
    Json(payload): Json<JsonValue>,
) -> StatusCode {
    match recorder.record(payload) {
        Ok(()) => StatusCode::OK,
        Err(_) => StatusCode::BAD_REQUEST,
    }
}

async fn wait_for_manager(http: &reqwest::Client, base: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if http
            .get(format!("{base}/pipelines"))
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "manager did not start before timeout"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_for_row_count(recorder: &RowRecorder, expected: usize) -> Vec<JsonValue> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let rows = recorder.rows();
        if rows.len() >= expected {
            return rows;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {expected} HTTP sink rows; received {rows:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn assert_response_status(response: reqwest::Response, expected: StatusCode, action: &str) {
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    assert_eq!(status, expected, "{action} failed: {status}: {body}");
}

// coverage-covers: source.file.stream, pipeline.runtime.checkpoint_recovery
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn file_source_checkpoint_resumes_from_last_emitted_offset() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let source_path = temp_dir.path().join("app.log");
    std::fs::write(&source_path, b"committed\npart\n").expect("write initial source file");
    let source_path = std::fs::canonicalize(source_path).expect("canonical source path");

    let recorder = Arc::new(RowRecorder::default());
    let sink_app = Router::new()
        .route("/rows", post(record_rows))
        .with_state(Arc::clone(&recorder));
    let Some(sink_listener) = bind_manager_listener_or_skip().await else {
        return;
    };
    let sink_addr = sink_listener.local_addr().expect("read sink listener addr");
    let sink_server = tokio::spawn(async move {
        axum::serve(sink_listener, sink_app)
            .await
            .expect("start HTTP sink recorder");
    });

    let storage_dir = temp_dir.path().join("storage");
    std::fs::create_dir(&storage_dir).expect("create storage dir");
    let storage = storage::StorageManager::new(&storage_dir).expect("create storage manager");
    let instance = manager::new_default_flow_instance();
    let Some(manager_listener) = bind_manager_listener_or_skip().await else {
        sink_server.abort();
        let _ = sink_server.await;
        return;
    };
    let manager_addr = manager_listener
        .local_addr()
        .expect("read manager listener addr");
    let manager_server = tokio::spawn(async move {
        manager::start_server_with_listener(
            manager_listener,
            instance,
            storage,
            default_flow_instances(),
        )
        .await
        .expect("start manager server");
    });

    let http = http_client();
    let manager_base = format!("http://{manager_addr}");
    wait_for_manager(&http, &manager_base).await;

    let stream_name = format!("e2e_file_checkpoint_stream_{}", random_suffix());
    let create_stream = http
        .post(format!("{manager_base}/streams"))
        .json(&serde_json::json!({
            "name": stream_name,
            "revision": 1,
            "type": "file",
            "props": {
                "path": source_path.to_string_lossy()
            },
            "decoder": {
                "type": "file_line",
                "props": {}
            }
        }))
        .send()
        .await
        .expect("create file stream request");
    assert_response_status(create_stream, StatusCode::CREATED, "create file stream").await;

    let pipeline_id = format!("e2e_file_checkpoint_pipe_{}", random_suffix());
    let create_pipeline = http
        .post(format!("{manager_base}/pipelines"))
        .json(&serde_json::json!({
            "id": pipeline_id,
            "revision": 1,
            "sql": format!("SELECT line, filename FROM {stream_name}"),
            "sinks": [{
                "id": "http_out",
                "type": "http",
                "props": {
                    "url": format!("http://{sink_addr}/rows"),
                    "content_type": "application/json"
                },
                "encoder": {
                    "type": "json",
                    "props": {}
                }
            }],
            "options": {
                "checkpoint": {
                    "enabled": true
                }
            }
        }))
        .send()
        .await
        .expect("create checkpoint pipeline request");
    assert_response_status(
        create_pipeline,
        StatusCode::CREATED,
        "create checkpoint pipeline",
    )
    .await;

    let start_pipeline = http
        .post(format!("{manager_base}/pipelines/{pipeline_id}/start"))
        .send()
        .await
        .expect("start checkpoint pipeline request");
    assert_response_status(start_pipeline, StatusCode::OK, "start checkpoint pipeline").await;

    let initial_rows = wait_for_row_count(recorder.as_ref(), 2).await;
    assert_eq!(
        initial_rows,
        vec![
            serde_json::json!({"line": "committed", "filename": "app.log"}),
            serde_json::json!({"line": "part", "filename": "app.log"}),
        ]
    );

    let stop_pipeline = http
        .post(format!(
            "{manager_base}/pipelines/{pipeline_id}/stop?mode=graceful&timeout_ms=5000"
        ))
        .send()
        .await
        .expect("gracefully stop checkpoint pipeline request");
    assert_response_status(
        stop_pipeline,
        StatusCode::OK,
        "gracefully stop checkpoint pipeline",
    )
    .await;
    recorder.clear();

    let restart_pipeline = http
        .post(format!("{manager_base}/pipelines/{pipeline_id}/start"))
        .send()
        .await
        .expect("restart checkpoint pipeline request");
    assert_response_status(
        restart_pipeline,
        StatusCode::OK,
        "restart checkpoint pipeline",
    )
    .await;

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        recorder.rows().is_empty(),
        "restored file source replayed rows before new data was appended: {:?}",
        recorder.rows()
    );

    let mut source_file = OpenOptions::new()
        .append(true)
        .open(&source_path)
        .expect("open source file for append");
    source_file
        .write_all(b"new\n")
        .expect("append new source row");
    source_file.flush().expect("flush appended source row");
    drop(source_file);

    let resumed_rows = wait_for_row_count(recorder.as_ref(), 1).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        resumed_rows,
        vec![serde_json::json!({"line": "new", "filename": "app.log"})]
    );
    assert_eq!(
        recorder.rows(),
        resumed_rows,
        "restored file source emitted unexpected additional rows"
    );

    let stop_pipeline = http
        .post(format!(
            "{manager_base}/pipelines/{pipeline_id}/stop?mode=graceful&timeout_ms=5000"
        ))
        .send()
        .await
        .expect("stop restored checkpoint pipeline request");
    assert_response_status(
        stop_pipeline,
        StatusCode::OK,
        "stop restored checkpoint pipeline",
    )
    .await;

    let delete_pipeline = http
        .delete(format!("{manager_base}/pipelines/{pipeline_id}"))
        .send()
        .await
        .expect("delete checkpoint pipeline request");
    assert_response_status(
        delete_pipeline,
        StatusCode::OK,
        "delete checkpoint pipeline",
    )
    .await;

    let delete_stream = http
        .delete(format!("{manager_base}/streams/{stream_name}"))
        .send()
        .await
        .expect("delete file stream request");
    assert_response_status(delete_stream, StatusCode::OK, "delete file stream").await;

    manager_server.abort();
    let _ = manager_server.await;
    sink_server.abort();
    let _ = sink_server.await;
}
