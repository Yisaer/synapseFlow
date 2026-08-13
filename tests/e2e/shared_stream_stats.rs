use super::{bind_manager_listener_or_skip, default_flow_instances, http_client, random_suffix};
use reqwest::StatusCode;
use sdk::{PipelineCreateRequest, StreamCreateRequest};
use serde_json::Value as JsonValue;
use std::time::Duration;

fn find_processor<'a>(processors: &'a [JsonValue], suffix: &str) -> &'a JsonValue {
    processors
        .iter()
        .find(|entry| {
            entry["processor_id"]
                .as_str()
                .is_some_and(|id| id.ends_with(suffix))
        })
        .unwrap_or_else(|| panic!("missing processor with suffix {suffix}"))
}

fn stats_value(entry: &JsonValue, field: &str) -> u64 {
    entry["stats"][field]
        .as_u64()
        .unwrap_or_else(|| panic!("missing numeric stats field {field} in {entry}"))
}

// coverage-covers: stream.shared.stats
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_stream_stats_reflect_runtime_activity_via_rest() {
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

    let stream_name = format!("e2e_shared_stream_stats_{}", random_suffix());
    let create_stream_resp = http
        .post(format!("{manager_base}/streams"))
        .json(&StreamCreateRequest::mock_shared_i64_value(
            stream_name.clone(),
        ))
        .send()
        .await
        .expect("create shared stream request");
    let create_stream_status = create_stream_resp.status();
    let create_stream_body = create_stream_resp.text().await.unwrap_or_default();
    assert!(
        create_stream_status.is_success(),
        "create shared mock stream failed: status={} body={}",
        create_stream_status,
        create_stream_body
    );

    let pipeline_id = format!("e2e_shared_stream_pipeline_{}", random_suffix());
    let sql = format!("SELECT value FROM {stream_name}");
    let create_pipeline_resp = http
        .post(format!("{manager_base}/pipelines"))
        .json(&PipelineCreateRequest::nop(pipeline_id.clone(), sql))
        .send()
        .await
        .expect("create pipeline request");
    let create_pipeline_status = create_pipeline_resp.status();
    let create_pipeline_body = create_pipeline_resp.text().await.unwrap_or_default();
    assert!(
        create_pipeline_status.is_success(),
        "create pipeline failed: status={} body={}",
        create_pipeline_status,
        create_pipeline_body
    );

    let start_pipeline_resp = http
        .post(format!("{manager_base}/pipelines/{pipeline_id}/start"))
        .send()
        .await
        .expect("start pipeline request");
    let start_pipeline_status = start_pipeline_resp.status();
    let start_pipeline_body = start_pipeline_resp.text().await.unwrap_or_default();
    assert!(
        start_pipeline_status.is_success(),
        "start pipeline failed: status={} body={}",
        start_pipeline_status,
        start_pipeline_body
    );

    tokio::time::sleep(Duration::from_millis(200)).await;

    injector
        .send_shared_mock_stream_payload(&stream_name, br#"{"value": 42}"#.as_ref())
        .await
        .expect("inject payload into shared mock stream");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let stats = loop {
        let stats_resp = http
            .get(format!(
                "{manager_base}/streams/{stream_name}/shared/stats?flow_instance_id=default"
            ))
            .send()
            .await
            .expect("get shared stream stats request");
        assert_eq!(stats_resp.status(), StatusCode::OK);
        let stats = stats_resp
            .json::<JsonValue>()
            .await
            .expect("decode shared stream stats");
        let processors = stats["processors"]
            .as_array()
            .expect("processors should be an array");
        let decoder_ready = processors.iter().any(|entry| {
            entry["processor_id"]
                .as_str()
                .is_some_and(|id| id.ends_with("PhysicalDecoder_1"))
                && stats_value(entry, "messages_in") > 0
        });
        let collect_ready = processors.iter().any(|entry| {
            entry["processor_id"]
                .as_str()
                .is_some_and(|id| id.ends_with("PhysicalResultCollect_2"))
                && stats_value(entry, "records_in") > 0
        });
        if decoder_ready && collect_ready {
            break stats;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "shared stream stats did not reflect runtime activity before timeout: {stats}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    assert_eq!(stats["stream"], stream_name);
    assert_eq!(stats["flow_instance_id"], "default");
    assert_eq!(stats["status"], "running");

    let processors = stats["processors"]
        .as_array()
        .expect("processors should be an array");
    let data_source = find_processor(processors, "PhysicalDataSource_0");
    let decoder = find_processor(processors, "PhysicalDecoder_1");
    let result_collect = find_processor(processors, "PhysicalResultCollect_2");

    assert!(
        stats_value(data_source, "messages_out") > 0,
        "datasource stats should reflect emitted messages: {data_source}"
    );
    assert!(
        stats_value(decoder, "messages_in") > 0,
        "decoder stats should reflect consumed messages: {decoder}"
    );
    assert!(
        stats_value(result_collect, "records_in") > 0,
        "result collect stats should reflect consumed records: {result_collect}"
    );
    assert_eq!(
        stats_value(result_collect, "records_in"),
        stats_value(result_collect, "records_out"),
        "result collect should forward every observed row: {result_collect}"
    );
    assert_eq!(
        stats_value(result_collect, "collections_in"),
        stats_value(result_collect, "collections_out"),
        "result collect should forward every observed collection: {result_collect}"
    );
    assert!(
        result_collect["stats"].get("messages_in").is_none()
            && result_collect["stats"].get("messages_out").is_none(),
        "collection-domain result collect should not expose message counters: {result_collect}"
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_stream_stats_rejects_non_shared_stream_via_rest() {
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

    let stream_name = format!("e2e_non_shared_stream_stats_{}", random_suffix());
    let create_stream_resp = http
        .post(format!("{manager_base}/streams"))
        .json(&StreamCreateRequest {
            name: stream_name.clone(),
            revision: 1,
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
        .expect("create non-shared stream request");
    assert_eq!(create_stream_resp.status(), StatusCode::CREATED);

    let stats_resp = http
        .get(format!("{manager_base}/streams/{stream_name}/shared/stats"))
        .send()
        .await
        .expect("request shared stream stats for non-shared stream");
    let stats_status = stats_resp.status();
    let stats_body = stats_resp.text().await.unwrap_or_default();
    assert_eq!(stats_status, StatusCode::BAD_REQUEST);
    assert!(
        stats_body.contains("is not a shared stream"),
        "unexpected body: {stats_body}"
    );

    let delete_stream_resp = http
        .delete(format!("{manager_base}/streams/{stream_name}"))
        .send()
        .await
        .expect("delete non-shared stream request");
    assert_eq!(delete_stream_resp.status(), StatusCode::OK);

    server.abort();
    let _ = server.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_stream_stats_returns_404_for_unknown_stream() {
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

    let stream_name = format!("missing_shared_stream_stats_{}", random_suffix());
    let stats_resp = http
        .get(format!("{manager_base}/streams/{stream_name}/shared/stats"))
        .send()
        .await
        .expect("request shared stream stats for unknown stream");
    assert_eq!(stats_resp.status(), StatusCode::NOT_FOUND);

    server.abort();
    let _ = server.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_stream_stats_reports_stopped_status_after_last_consumer_stops() {
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

    let stream_name = format!("e2e_shared_stream_stopped_stats_{}", random_suffix());
    let create_stream_resp = http
        .post(format!("{manager_base}/streams"))
        .json(&StreamCreateRequest::mock_shared_i64_value(
            stream_name.clone(),
        ))
        .send()
        .await
        .expect("create shared stream request");
    let create_stream_status = create_stream_resp.status();
    let create_stream_body = create_stream_resp.text().await.unwrap_or_default();
    assert!(
        create_stream_status.is_success(),
        "create shared mock stream failed: status={} body={}",
        create_stream_status,
        create_stream_body
    );

    let pipeline_id = format!("e2e_shared_stream_stopped_pipeline_{}", random_suffix());
    let sql = format!("SELECT value FROM {stream_name}");
    let create_pipeline_resp = http
        .post(format!("{manager_base}/pipelines"))
        .json(&PipelineCreateRequest::nop(pipeline_id.clone(), sql))
        .send()
        .await
        .expect("create pipeline request");
    let create_pipeline_status = create_pipeline_resp.status();
    let create_pipeline_body = create_pipeline_resp.text().await.unwrap_or_default();
    assert!(
        create_pipeline_status.is_success(),
        "create pipeline failed: status={} body={}",
        create_pipeline_status,
        create_pipeline_body
    );

    let start_pipeline_resp = http
        .post(format!("{manager_base}/pipelines/{pipeline_id}/start"))
        .send()
        .await
        .expect("start pipeline request");
    let start_pipeline_status = start_pipeline_resp.status();
    let start_pipeline_body = start_pipeline_resp.text().await.unwrap_or_default();
    assert!(
        start_pipeline_status.is_success(),
        "start pipeline failed: status={} body={}",
        start_pipeline_status,
        start_pipeline_body
    );

    tokio::time::sleep(Duration::from_millis(200)).await;

    let stop_pipeline_resp = http
        .post(format!(
            "{manager_base}/pipelines/{pipeline_id}/stop?mode=graceful&timeout_ms=5000"
        ))
        .send()
        .await
        .expect("stop pipeline request");
    let stop_pipeline_status = stop_pipeline_resp.status();
    let stop_pipeline_body = stop_pipeline_resp.text().await.unwrap_or_default();
    assert!(
        stop_pipeline_status.is_success(),
        "stop pipeline failed: status={} body={}",
        stop_pipeline_status,
        stop_pipeline_body
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let stopped_stats = loop {
        let stats_resp = http
            .get(format!(
                "{manager_base}/streams/{stream_name}/shared/stats?flow_instance_id=default"
            ))
            .send()
            .await
            .expect("get shared stream stats request");
        assert_eq!(stats_resp.status(), StatusCode::OK);
        let stats = stats_resp
            .json::<JsonValue>()
            .await
            .expect("decode shared stream stats");
        if stats["status"] == "stopped"
            && stats["processors"]
                .as_array()
                .is_some_and(|processors| processors.is_empty())
        {
            break stats;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "shared stream stats did not reach stopped state before timeout: {stats}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    assert_eq!(stopped_stats["stream"], stream_name);
    assert_eq!(stopped_stats["flow_instance_id"], "default");
    assert_eq!(stopped_stats["status"], "stopped");
    assert_eq!(
        stopped_stats["processors"],
        serde_json::json!([]),
        "stopped shared stream runtime should not expose ingest processors",
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
