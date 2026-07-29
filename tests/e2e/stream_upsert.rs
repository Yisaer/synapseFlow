use super::{bind_manager_listener_or_skip, default_flow_instances, http_client, random_suffix};
use reqwest::StatusCode;
use sdk::{PipelineCreateRequest, StreamCreateRequest, StreamUpsertRequest};
use std::time::Duration;

// ── Helpers ──

async fn start_manager(temp_dir: &tempfile::TempDir) -> (tokio::task::JoinHandle<()>, String) {
    let storage = storage::StorageManager::new(temp_dir.path()).expect("create storage manager");
    let instance = manager::new_default_flow_instance();
    let listener = bind_manager_listener_or_skip()
        .await
        .expect("bind manager listener");
    let addr = listener.local_addr().expect("read listener addr");
    let server = tokio::spawn(async move {
        manager::start_server_with_listener(listener, instance, storage, default_flow_instances())
            .await
            .expect("start manager server");
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    (server, format!("http://{addr}"))
}

fn pipeline_nop(id: &str, sql: String) -> PipelineCreateRequest {
    PipelineCreateRequest {
        id: id.to_string(),
        revision: 1,
        sql,
        sinks: vec![serde_json::json!({ "type": "nop" })],
        flow_instance_id: None,
    }
}

async fn delete_pipeline(http: &reqwest::Client, base: &str, pipeline_id: &str) {
    let resp = http
        .delete(format!("{base}/pipelines/{pipeline_id}"))
        .send()
        .await
        .expect("delete pipeline request");
    assert!(
        resp.status().is_success(),
        "delete pipeline {pipeline_id} failed: status={} body={}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );
}

async fn delete_stream(http: &reqwest::Client, base: &str, stream_name: &str) {
    let resp = http
        .delete(format!("{base}/streams/{stream_name}"))
        .send()
        .await
        .expect("delete stream request");
    assert!(
        resp.status().is_success(),
        "delete stream {stream_name} failed: status={} body={}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );
}

// ────────────────────────────────────────────────────────────────────
// Scenario 1 — Non-shared → non-shared: direct update, no pipeline check
// ────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upsert_non_shared_stream_succeeds() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let (server, manager_base) = start_manager(&temp_dir).await;
    let http = http_client();

    let stream_name = format!("e2e_ups_nonshared_{}", random_suffix());
    let create = StreamCreateRequest::mock_non_shared_i64_value(stream_name.clone());

    // Create
    let resp = http
        .post(format!("{manager_base}/streams"))
        .json(&create)
        .send()
        .await
        .expect("create non-shared stream");
    assert_eq!(
        resp.status(),
        StatusCode::CREATED,
        "create stream: {}",
        resp.text().await.unwrap_or_default()
    );

    // Update schema + decoder (keep non-shared, no shared field in request)
    let upsert = StreamUpsertRequest {
        revision: 2,
        schema: serde_json::json!({
            "type": "json",
            "props": { "columns": [{ "name": "x", "data_type": "float64" }] }
        }),
        props: serde_json::json!({}),
        decoder: serde_json::json!({ "type": "json", "props": {} }),
        shared: None,
    };

    let resp = http
        .put(format!("{manager_base}/streams/{stream_name}"))
        .json(&upsert)
        .send()
        .await
        .expect("upsert non-shared stream");
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "upsert non-shared stream: {}",
        resp.text().await.unwrap_or_default()
    );

    let resp = http
        .put(format!("{manager_base}/streams/{stream_name}"))
        .json(&upsert)
        .send()
        .await
        .expect("repeat equal stream revision");
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "equal revision with equal spec must be idempotent"
    );

    let mut older = upsert.clone();
    older.revision = 1;
    older.schema = serde_json::json!({
        "type": "json",
        "props": { "columns": [{ "name": "older", "data_type": "int64" }] }
    });
    let resp = http
        .put(format!("{manager_base}/streams/{stream_name}"))
        .json(&older)
        .send()
        .await
        .expect("send older stream revision");
    assert_eq!(resp.status(), StatusCode::CONFLICT);
    assert!(resp
        .text()
        .await
        .expect("older conflict body")
        .contains("older_revision"));

    let mut conflicting = upsert.clone();
    conflicting.schema = serde_json::json!({
        "type": "json",
        "props": { "columns": [{ "name": "conflict", "data_type": "int64" }] }
    });
    let resp = http
        .put(format!("{manager_base}/streams/{stream_name}"))
        .json(&conflicting)
        .send()
        .await
        .expect("send conflicting equal stream revision");
    assert_eq!(resp.status(), StatusCode::CONFLICT);
    assert!(resp
        .text()
        .await
        .expect("equal conflict body")
        .contains("same_revision_different_spec"));

    // Describe to verify the update took effect
    let resp = http
        .get(format!("{manager_base}/streams/describe/{stream_name}"))
        .send()
        .await
        .expect("describe stream");
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.json::<serde_json::Value>().await.expect("json body");
    assert_eq!(body["spec"]["shared"], false);
    assert_eq!(body["spec"]["schema"]["columns"][0]["name"], "x");
    assert_eq!(body["spec"]["schema"]["columns"][0]["data_type"], "float64");

    // Cleanup
    delete_stream(&http, &manager_base, &stream_name).await;
    server.abort();
    let _ = server.await;
}

// ────────────────────────────────────────────────────────────────────
// Scenario 2 — Non-shared → shared conversion
// ────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upsert_non_shared_to_shared_with_running_pipeline_is_rejected() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let (server, manager_base) = start_manager(&temp_dir).await;
    let http = http_client();

    let stream_name = format!("e2e_up_ns2s_run_{}", random_suffix());
    let create = StreamCreateRequest::mock_non_shared_i64_value(stream_name.clone());

    // Create non-shared stream
    let resp = http
        .post(format!("{manager_base}/streams"))
        .json(&create)
        .send()
        .await
        .expect("create non-shared stream");
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Create and start a pipeline that references this stream
    let pipeline_id = format!("e2e_up_ns2s_pipe_{}", random_suffix());
    let sql = format!("SELECT value FROM {stream_name}");
    let resp = http
        .post(format!("{manager_base}/pipelines"))
        .json(&pipeline_nop(&pipeline_id, sql))
        .send()
        .await
        .expect("create pipeline");
    assert_eq!(resp.status(), StatusCode::CREATED);

    let resp = http
        .post(format!("{manager_base}/pipelines/{pipeline_id}/start"))
        .send()
        .await
        .expect("start pipeline");
    assert_eq!(resp.status(), StatusCode::OK);

    // Try to convert to shared — must be rejected because pipeline is running
    let upsert = StreamUpsertRequest::from_create(&create).with_shared(true);
    let resp = http
        .put(format!("{manager_base}/streams/{stream_name}"))
        .json(&upsert)
        .send()
        .await
        .expect("upsert to shared with running pipeline");
    assert_eq!(resp.status(), StatusCode::CONFLICT);
    let body = resp.text().await.unwrap_or_default();
    assert!(
        body.contains("has running pipelines"),
        "unexpected conflict body: {body}"
    );

    // Cleanup
    let _ = http
        .post(format!(
            "{manager_base}/pipelines/{pipeline_id}/stop?mode=graceful&timeout_ms=5000"
        ))
        .send()
        .await;
    delete_pipeline(&http, &manager_base, &pipeline_id).await;
    delete_stream(&http, &manager_base, &stream_name).await;
    server.abort();
    let _ = server.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upsert_non_shared_to_shared_without_running_pipeline_succeeds() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let (server, manager_base) = start_manager(&temp_dir).await;
    let http = http_client();

    let stream_name = format!("e2e_up_ns2s_ok_{}", random_suffix());
    let create = StreamCreateRequest::mock_non_shared_i64_value(stream_name.clone());

    // Create non-shared stream
    let resp = http
        .post(format!("{manager_base}/streams"))
        .json(&create)
        .send()
        .await
        .expect("create non-shared stream");
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Create a pipeline but DO NOT start it
    let pipeline_id = format!("e2e_up_ns2s_pipe_{}", random_suffix());
    let sql = format!("SELECT value FROM {stream_name}");
    let resp = http
        .post(format!("{manager_base}/pipelines"))
        .json(&pipeline_nop(&pipeline_id, sql))
        .send()
        .await
        .expect("create pipeline");
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Convert to shared — no running pipeline, so it should succeed
    let upsert = StreamUpsertRequest::from_create(&create).with_shared(true);
    let resp = http
        .put(format!("{manager_base}/streams/{stream_name}"))
        .json(&upsert)
        .send()
        .await
        .expect("upsert to shared without running pipeline");
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "upsert to shared: {}",
        resp.text().await.unwrap_or_default()
    );

    // Verify conversion
    let resp = http
        .get(format!("{manager_base}/streams/describe/{stream_name}"))
        .send()
        .await
        .expect("describe stream");
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.json::<serde_json::Value>().await.expect("json body");
    assert_eq!(body["spec"]["shared"], true);

    // Cleanup
    delete_pipeline(&http, &manager_base, &pipeline_id).await;
    delete_stream(&http, &manager_base, &stream_name).await;
    server.abort();
    let _ = server.await;
}

// ────────────────────────────────────────────────────────────────────
// Scenario 3 — Shared → shared update
// ────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upsert_shared_stream_with_running_pipeline_is_rejected() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let (server, manager_base) = start_manager(&temp_dir).await;
    let http = http_client();

    let stream_name = format!("e2e_up_shared_run_{}", random_suffix());
    let create = StreamCreateRequest::mock_shared_i64_value(stream_name.clone());

    // Create shared stream
    let resp = http
        .post(format!("{manager_base}/streams"))
        .json(&create)
        .send()
        .await
        .expect("create shared stream");
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Create and start a pipeline
    let pipeline_id = format!("e2e_up_shared_pipe_{}", random_suffix());
    let sql = format!("SELECT value FROM {stream_name}");
    let resp = http
        .post(format!("{manager_base}/pipelines"))
        .json(&pipeline_nop(&pipeline_id, sql))
        .send()
        .await
        .expect("create pipeline");
    assert_eq!(resp.status(), StatusCode::CREATED);

    let resp = http
        .post(format!("{manager_base}/pipelines/{pipeline_id}/start"))
        .send()
        .await
        .expect("start pipeline");
    assert_eq!(resp.status(), StatusCode::OK);

    // Try to update shared stream — must be rejected because pipeline is running
    let upsert = StreamUpsertRequest::from_create(&create);
    let resp = http
        .put(format!("{manager_base}/streams/{stream_name}"))
        .json(&upsert)
        .send()
        .await
        .expect("upsert shared stream with running pipeline");
    assert_eq!(resp.status(), StatusCode::CONFLICT);
    let body = resp.text().await.unwrap_or_default();
    assert!(
        body.contains("has running pipelines"),
        "unexpected conflict body: {body}"
    );

    // Cleanup
    let _ = http
        .post(format!(
            "{manager_base}/pipelines/{pipeline_id}/stop?mode=graceful&timeout_ms=5000"
        ))
        .send()
        .await;
    delete_pipeline(&http, &manager_base, &pipeline_id).await;
    delete_stream(&http, &manager_base, &stream_name).await;
    server.abort();
    let _ = server.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upsert_shared_stream_without_running_pipeline_succeeds() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let (server, manager_base) = start_manager(&temp_dir).await;
    let http = http_client();

    let stream_name = format!("e2e_up_shared_ok_{}", random_suffix());
    let create = StreamCreateRequest::mock_shared_i64_value(stream_name.clone());

    // Create shared stream
    let resp = http
        .post(format!("{manager_base}/streams"))
        .json(&create)
        .send()
        .await
        .expect("create shared stream");
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Create pipeline, start it, then stop it — so it exists but is not running
    let pipeline_id = format!("e2e_up_shared_pipe_{}", random_suffix());
    let sql = format!("SELECT value FROM {stream_name}");
    let resp = http
        .post(format!("{manager_base}/pipelines"))
        .json(&pipeline_nop(&pipeline_id, sql))
        .send()
        .await
        .expect("create pipeline");
    assert_eq!(resp.status(), StatusCode::CREATED);

    let resp = http
        .post(format!("{manager_base}/pipelines/{pipeline_id}/start"))
        .send()
        .await
        .expect("start pipeline");
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = http
        .post(format!(
            "{manager_base}/pipelines/{pipeline_id}/stop?mode=graceful&timeout_ms=5000"
        ))
        .send()
        .await
        .expect("stop pipeline");
    assert_eq!(resp.status(), StatusCode::OK);

    // Now update the shared stream — pipeline is stopped, so update should succeed
    let upsert = StreamUpsertRequest {
        revision: 2,
        schema: serde_json::json!({
            "type": "json",
            "props": { "columns": [
                { "name": "value", "data_type": "int64" },
                { "name": "extra", "data_type": "string" }
            ]}
        }),
        props: serde_json::json!({}),
        decoder: serde_json::json!({ "type": "json", "props": {} }),
        shared: None,
    };
    let resp = http
        .put(format!("{manager_base}/streams/{stream_name}"))
        .json(&upsert)
        .send()
        .await
        .expect("upsert shared stream without running pipeline");
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "upsert shared stream: {}",
        resp.text().await.unwrap_or_default()
    );

    // Verify update
    let resp = http
        .get(format!("{manager_base}/streams/describe/{stream_name}"))
        .send()
        .await
        .expect("describe stream");
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.json::<serde_json::Value>().await.expect("json body");
    let columns = body["spec"]["schema"]["columns"]
        .as_array()
        .expect("columns array");
    assert_eq!(columns.len(), 2);
    assert_eq!(columns[1]["name"], "extra");
    assert_eq!(columns[1]["data_type"], "string");

    // Cleanup
    delete_pipeline(&http, &manager_base, &pipeline_id).await;
    delete_stream(&http, &manager_base, &stream_name).await;
    server.abort();
    let _ = server.await;
}

// ────────────────────────────────────────────────────────────────────
// Scenario 4 — Shared → non-shared: must be rejected
// ────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upsert_shared_to_non_shared_is_rejected() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let (server, manager_base) = start_manager(&temp_dir).await;
    let http = http_client();

    let stream_name = format!("e2e_up_s2ns_{}", random_suffix());
    let create = StreamCreateRequest::mock_shared_i64_value(stream_name.clone());

    // Create shared stream
    let resp = http
        .post(format!("{manager_base}/streams"))
        .json(&create)
        .send()
        .await
        .expect("create shared stream");
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Try to convert to non-shared — must be rejected
    let upsert = StreamUpsertRequest::from_create(&create).with_shared(false);
    let resp = http
        .put(format!("{manager_base}/streams/{stream_name}"))
        .json(&upsert)
        .send()
        .await
        .expect("upsert shared to non-shared");
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = resp.text().await.unwrap_or_default();
    assert!(
        body.contains("converting a shared stream to non-shared is not supported"),
        "unexpected body: {body}"
    );

    // Cleanup
    delete_stream(&http, &manager_base, &stream_name).await;
    server.abort();
    let _ = server.await;
}
