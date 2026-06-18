use super::{bind_manager_listener_or_skip, default_flow_instances, http_client, random_suffix};
use reqwest::StatusCode;
use sdk::{PipelineCreateRequest, StreamCreateRequest};
use serde_json::Value as JsonValue;
use std::time::Duration;

fn stats_value(entry: &JsonValue, field: &str) -> u64 {
    entry["stats"][field]
        .as_u64()
        .unwrap_or_else(|| panic!("missing numeric stats field {field} in {entry}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pipeline_stats_returns_400_for_stopped_pipeline_via_rest() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let storage = storage::StorageManager::new(temp_dir.path()).expect("create storage manager");
    let instance = manager::new_default_flow_instance();

    let Some(listener) = bind_manager_listener_or_skip().await else {
        return;
    };
    let addr = listener.local_addr().expect("read listener addr");

    let server = tokio::spawn(async move {
        manager::start_server_with_listener(listener, instance, storage, default_flow_instances())
            .await
            .expect("start manager server");
    });

    let http = http_client();
    let manager_base = format!("http://{addr}");
    tokio::time::sleep(Duration::from_millis(300)).await;

    let stream_name = format!("e2e_pipeline_stats_stream_{}", random_suffix());
    let create_stream_resp = http
        .post(format!("{manager_base}/streams"))
        .json(&StreamCreateRequest {
            name: stream_name.clone(),
            stream_type: "mock".to_string(),
            schema: serde_json::json!({
                "type": "json",
                "props": {
                    "columns": [
                        { "name": "value", "data_type": "int64" }
                    ]
                }
            }),
            props: serde_json::json!({}),
            shared: false,
            decoder: serde_json::json!({ "type": "json", "props": {} }),
        })
        .send()
        .await
        .expect("create stream request");
    assert_eq!(create_stream_resp.status(), StatusCode::CREATED);

    let pipeline_id = format!("e2e_pipeline_stats_stopped_{}", random_suffix());
    let create_pipeline_resp = http
        .post(format!("{manager_base}/pipelines"))
        .json(&PipelineCreateRequest::nop(
            pipeline_id.clone(),
            format!("SELECT value FROM {stream_name}"),
        ))
        .send()
        .await
        .expect("create pipeline request");
    assert_eq!(create_pipeline_resp.status(), StatusCode::CREATED);

    let start_pipeline_resp = http
        .post(format!("{manager_base}/pipelines/{pipeline_id}/start"))
        .send()
        .await
        .expect("start pipeline request");
    assert_eq!(start_pipeline_resp.status(), StatusCode::OK);

    let stop_pipeline_resp = http
        .post(format!(
            "{manager_base}/pipelines/{pipeline_id}/stop?mode=quick&timeout_ms=5000"
        ))
        .send()
        .await
        .expect("stop pipeline request");
    assert_eq!(stop_pipeline_resp.status(), StatusCode::OK);

    let stats_resp = http
        .get(format!("{manager_base}/pipelines/{pipeline_id}/stats"))
        .send()
        .await
        .expect("collect pipeline stats request");
    let stats_status = stats_resp.status();
    let stats_body = stats_resp.text().await.unwrap_or_default();
    assert_eq!(stats_status, StatusCode::BAD_REQUEST);
    assert!(
        stats_body.contains("is not running"),
        "unexpected stopped-pipeline stats body: {stats_body}"
    );

    let delete_pipeline_resp = http
        .delete(format!("{manager_base}/pipelines/{pipeline_id}"))
        .send()
        .await
        .expect("delete pipeline request");
    assert_eq!(delete_pipeline_resp.status(), StatusCode::OK);

    let delete_stream_resp = http
        .delete(format!("{manager_base}/streams/{stream_name}"))
        .send()
        .await
        .expect("delete stream request");
    assert_eq!(delete_stream_resp.status(), StatusCode::OK);

    server.abort();
    let _ = server.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pipeline_stats_excludes_internal_and_shared_ingest_processors_via_rest() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let storage = storage::StorageManager::new(temp_dir.path()).expect("create storage manager");
    let instance = manager::new_default_flow_instance();
    let injector = instance.clone();

    let Some(listener) = bind_manager_listener_or_skip().await else {
        return;
    };
    let addr = listener.local_addr().expect("read listener addr");

    let server = tokio::spawn(async move {
        manager::start_server_with_listener(listener, instance, storage, default_flow_instances())
            .await
            .expect("start manager server");
    });

    let http = http_client();
    let manager_base = format!("http://{addr}");
    tokio::time::sleep(Duration::from_millis(300)).await;

    let stream_name = format!("e2e_pipeline_stats_shared_{}", random_suffix());
    let create_stream_resp = http
        .post(format!("{manager_base}/streams"))
        .json(&StreamCreateRequest::mock_shared_i64_value(
            stream_name.clone(),
        ))
        .send()
        .await
        .expect("create shared stream request");
    assert_eq!(create_stream_resp.status(), StatusCode::CREATED);

    let pipeline_id = format!("e2e_pipeline_stats_running_{}", random_suffix());
    let create_pipeline_resp = http
        .post(format!("{manager_base}/pipelines"))
        .json(&PipelineCreateRequest::nop(
            pipeline_id.clone(),
            format!("SELECT value FROM {stream_name}"),
        ))
        .send()
        .await
        .expect("create pipeline request");
    assert_eq!(create_pipeline_resp.status(), StatusCode::CREATED);

    let start_pipeline_resp = http
        .post(format!("{manager_base}/pipelines/{pipeline_id}/start"))
        .send()
        .await
        .expect("start pipeline request");
    assert_eq!(start_pipeline_resp.status(), StatusCode::OK);

    tokio::time::sleep(Duration::from_millis(200)).await;
    injector
        .send_shared_mock_stream_payload(&stream_name, br#"{"value": 7}"#.as_ref())
        .await
        .expect("inject payload into shared mock stream");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let stats = loop {
        let stats_resp = http
            .get(format!("{manager_base}/pipelines/{pipeline_id}/stats"))
            .send()
            .await
            .expect("collect pipeline stats request");
        assert_eq!(stats_resp.status(), StatusCode::OK);
        let stats = stats_resp
            .json::<Vec<JsonValue>>()
            .await
            .expect("decode pipeline stats");
        if stats.iter().any(|entry| {
            stats_value(entry, "records_in") > 0 || stats_value(entry, "records_out") > 0
        }) {
            break stats;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "pipeline stats did not reflect runtime activity before timeout: {stats:#?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    assert!(
        !stats.is_empty(),
        "running pipeline stats should expose at least one processor entry"
    );
    assert!(
        stats
            .iter()
            .all(|entry| entry["processor_id"] != "control_source"),
        "pipeline stats should filter control_source: {stats:#?}"
    );
    assert!(
        stats.iter().all(|entry| {
            entry["processor_id"]
                .as_str()
                .is_some_and(|id| !id.starts_with("PhysicalResultCollect_"))
        }),
        "pipeline stats should filter result collect processors: {stats:#?}"
    );
    assert!(
        stats.iter().all(|entry| {
            entry["processor_id"]
                .as_str()
                .is_some_and(|id| !id.starts_with("shared/"))
        }),
        "pipeline stats should not include shared ingest runtime processors: {stats:#?}"
    );

    let stop_pipeline_resp = http
        .post(format!(
            "{manager_base}/pipelines/{pipeline_id}/stop?mode=graceful&timeout_ms=5000"
        ))
        .send()
        .await
        .expect("stop pipeline request");
    assert_eq!(stop_pipeline_resp.status(), StatusCode::OK);

    let delete_pipeline_resp = http
        .delete(format!("{manager_base}/pipelines/{pipeline_id}"))
        .send()
        .await
        .expect("delete pipeline request");
    assert_eq!(delete_pipeline_resp.status(), StatusCode::OK);

    let delete_stream_resp = http
        .delete(format!("{manager_base}/streams/{stream_name}"))
        .send()
        .await
        .expect("delete stream request");
    assert_eq!(delete_stream_resp.status(), StatusCode::OK);

    server.abort();
    let _ = server.await;
}
