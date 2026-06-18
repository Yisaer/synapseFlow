use super::{bind_manager_listener_or_skip, default_flow_instances, http_client, random_suffix};
use sdk::{PipelineCreateRequest, StreamCreateRequest};
use serde_json::Value as JsonValue;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn status_endpoint_returns_expected_fields() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let storage = storage::StorageManager::new(temp_dir.path()).expect("create storage manager");
    let manager_instance = manager::new_default_flow_instance();
    let Some(listener) = bind_manager_listener_or_skip().await else {
        return;
    };
    let addr = listener.local_addr().expect("read listener addr");
    let flow_instances = default_flow_instances();

    let manager_server = tokio::spawn(async move {
        manager::start_server_with_listener(listener, manager_instance, storage, flow_instances)
            .await
            .expect("start manager server");
    });

    let http = http_client();
    let manager_base = format!("http://{addr}");

    // Wait for > 1 second so that uptime_seconds (whole-second resolution) is measurably > 0.
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;

    // --- 1. Verify stable fields without pipelines ---
    let resp = http
        .get(format!("{manager_base}/status"))
        .send()
        .await
        .expect("GET /status request");
    assert!(
        resp.status().is_success(),
        "/status should return success, got {}",
        resp.status()
    );

    let body = resp.json::<JsonValue>().await.expect("decode /status body");

    // Check all expected fields are present and have correct types.
    assert!(body["cpu_usage_percent"].is_f64() || body["cpu_usage_percent"].is_number());
    assert!(body["memory_usage_bytes"].is_i64() || body["memory_usage_bytes"].is_number());
    assert!(body["heap_in_use_bytes"].is_i64() || body["heap_in_use_bytes"].is_number());
    assert!(
        body["heap_in_allocator_bytes"].is_i64() || body["heap_in_allocator_bytes"].is_number()
    );
    assert!(body["tokio_tasks_inflight"].is_i64() || body["tokio_tasks_inflight"].is_number());

    let uptime = body["uptime_seconds"]
        .as_u64()
        .expect("uptime_seconds must be u64");
    assert!(
        uptime > 0,
        "uptime_seconds must be > 0 after initial delay, got {uptime}"
    );

    let active_pipelines = body["active_pipeline_count"]
        .as_u64()
        .expect("active_pipeline_count must be a number");
    assert_eq!(
        active_pipelines, 0,
        "active_pipeline_count must be 0 with no pipelines, got {active_pipelines}"
    );

    let commit = body["commit"].as_str().expect("commit must be a string");
    assert!(
        !commit.is_empty(),
        "commit must be non-empty, got '{commit}'"
    );

    let release_tag = body["release_tag"]
        .as_str()
        .expect("release_tag must be a string");
    assert!(
        !release_tag.is_empty(),
        "release_tag must be non-empty, got '{release_tag}'"
    );

    // --- 2. Create and start a pipeline, verify active_pipeline_count updates ---
    let stream_name = format!("status_test_stream_{}", random_suffix());
    let create_stream_resp = http
        .post(format!("{manager_base}/streams"))
        .json(&StreamCreateRequest::mock_shared_i64_value(
            stream_name.clone(),
        ))
        .send()
        .await
        .expect("create stream request");
    assert!(
        create_stream_resp.status().is_success(),
        "create stream failed: {}",
        create_stream_resp.status()
    );

    let pipeline_id = format!("status_test_pipe_{}", random_suffix());
    let sql = format!("SELECT value FROM {stream_name}");
    let create_pipeline_resp = http
        .post(format!("{manager_base}/pipelines"))
        .json(&PipelineCreateRequest::nop(pipeline_id.clone(), sql))
        .send()
        .await
        .expect("create pipeline request");
    assert!(
        create_pipeline_resp.status().is_success(),
        "create pipeline failed: {}",
        create_pipeline_resp.status()
    );

    let start_resp = http
        .post(format!("{manager_base}/pipelines/{pipeline_id}/start"))
        .send()
        .await
        .expect("start pipeline request");
    assert!(
        start_resp.status().is_success(),
        "start pipeline failed: {}",
        start_resp.status()
    );

    // Give the pipeline a moment to enter the running state.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let body_after = http
        .get(format!("{manager_base}/status"))
        .send()
        .await
        .expect("GET /status after pipeline start")
        .json::<JsonValue>()
        .await
        .expect("decode /status body after pipeline start");

    let active_after = body_after["active_pipeline_count"]
        .as_u64()
        .expect("active_pipeline_count must be a number");
    assert_eq!(
        active_after, 1,
        "active_pipeline_count must be 1 after starting a pipeline, got {active_after}"
    );

    let uptime_after = body_after["uptime_seconds"]
        .as_u64()
        .expect("uptime_seconds must be a number");
    assert!(
        uptime_after >= uptime,
        "uptime_seconds must be monotonic: before={uptime}, after={uptime_after}"
    );

    // Cleanup
    manager_server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn status_endpoint_uptime_monotonic() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let storage = storage::StorageManager::new(temp_dir.path()).expect("create storage manager");
    let manager_instance = manager::new_default_flow_instance();
    let Some(listener) = bind_manager_listener_or_skip().await else {
        return;
    };
    let addr = listener.local_addr().expect("read listener addr");
    let flow_instances = default_flow_instances();

    let manager_server = tokio::spawn(async move {
        manager::start_server_with_listener(listener, manager_instance, storage, flow_instances)
            .await
            .expect("start manager server");
    });

    let http = http_client();
    let manager_base = format!("http://{addr}");

    // First request after sufficient delay for uptime_seconds to cross the 1s boundary.
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
    let uptime1 = http
        .get(format!("{manager_base}/status"))
        .send()
        .await
        .expect("first /status")
        .json::<JsonValue>()
        .await
        .expect("decode first /status")["uptime_seconds"]
        .as_u64()
        .expect("uptime_seconds as u64");

    // Wait and request again.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let uptime2 = http
        .get(format!("{manager_base}/status"))
        .send()
        .await
        .expect("second /status")
        .json::<JsonValue>()
        .await
        .expect("decode second /status")["uptime_seconds"]
        .as_u64()
        .expect("uptime_seconds as u64");

    assert!(
        uptime2 >= uptime1 + 2,
        "uptime_seconds should increase by at least 2 seconds over a 2s sleep: first={uptime1}, second={uptime2}"
    );

    manager_server.abort();
}
