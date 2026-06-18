use super::{bind_manager_listener_or_skip, default_flow_instances, http_client, random_suffix};
use reqwest::StatusCode;
use sdk::{PipelineCreateRequest, StreamCreateRequest};
use serde_json::Value as JsonValue;
use std::time::Duration;
use tokio::time::Instant;

async fn wait_for_shared_stream_running(
    http: &reqwest::Client,
    manager_base: &str,
    stream_name: &str,
    flow_instance_id: Option<&str>,
) -> JsonValue {
    let deadline = Instant::now() + Duration::from_secs(5);

    let last_body = loop {
        let url = match flow_instance_id {
            Some(flow_instance_id) => {
                format!(
                    "{manager_base}/streams/{stream_name}/shared/stats?flow_instance_id={flow_instance_id}"
                )
            }
            None => format!("{manager_base}/streams/{stream_name}/shared/stats"),
        };
        let stats_resp = http
            .get(url)
            .send()
            .await
            .expect("shared stream stats request");
        assert!(
            stats_resp.status().is_success(),
            "shared stream stats should return success: status={}",
            stats_resp.status()
        );

        let stats_body = stats_resp
            .json::<JsonValue>()
            .await
            .expect("decode shared stream stats");
        if stats_body["status"] == "running" {
            return stats_body;
        }

        if Instant::now() >= deadline {
            break stats_body;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    panic!("shared stream stats did not become running: {last_body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_stream_stats_default_to_default_instance_in_single_instance_deployments() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let storage = storage::StorageManager::new(temp_dir.path()).expect("create storage manager");
    let manager_instance = manager::new_default_flow_instance();
    let _injector = manager_instance.clone();

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

    let stream_name = format!("shared_stream_stats_default_{}", random_suffix());
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
        "create shared stream failed: status={} body={}",
        create_stream_status,
        create_stream_body
    );

    let pipeline_id = format!("shared_stream_stats_pipe_{}", random_suffix());
    let sql = format!("SELECT value FROM {stream_name}");
    let create_pipeline_resp = http
        .post(format!("{manager_base}/pipelines"))
        .json(&PipelineCreateRequest::nop(pipeline_id.clone(), sql))
        .send()
        .await
        .expect("create shared stream stats pipeline request");
    let create_pipeline_status = create_pipeline_resp.status();
    let create_pipeline_body = create_pipeline_resp.text().await.unwrap_or_default();
    assert!(
        create_pipeline_status.is_success(),
        "create shared stream stats pipeline failed: status={} body={}",
        create_pipeline_status,
        create_pipeline_body
    );

    let start_pipeline_resp = http
        .post(format!("{manager_base}/pipelines/{pipeline_id}/start"))
        .send()
        .await
        .expect("start shared stream stats pipeline request");
    let start_pipeline_status = start_pipeline_resp.status();
    let start_pipeline_body = start_pipeline_resp.text().await.unwrap_or_default();
    assert!(
        start_pipeline_status.is_success(),
        "start shared stream stats pipeline failed: status={} body={}",
        start_pipeline_status,
        start_pipeline_body
    );

    let stats_body = wait_for_shared_stream_running(&http, &manager_base, &stream_name, None).await;
    assert_eq!(stats_body["stream"], stream_name);
    assert_eq!(stats_body["flow_instance_id"], "default");
    assert_eq!(stats_body["status"], "running");

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
        .expect("delete shared stream request");
    assert_eq!(delete_stream_resp.status(), StatusCode::OK);

    manager_server.abort();
    let _ = manager_server.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_stream_stats_require_explicit_flow_instance_id_in_multi_instance_deployments() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let storage = storage::StorageManager::new(temp_dir.path()).expect("create storage manager");
    let manager_instance = manager::new_default_flow_instance();

    let extra_spec = manager::FlowInstanceSpec {
        id: "extra_a".to_string(),
        ..manager::FlowInstanceSpec::default()
    };
    let Some(manager_listener) = bind_manager_listener_or_skip().await else {
        return;
    };
    let manager_addr = manager_listener
        .local_addr()
        .expect("manager listener addr");

    let mut flow_instances = default_flow_instances();
    flow_instances.push(extra_spec);

    let manager_server = tokio::spawn(async move {
        manager::start_server_with_listener(
            manager_listener,
            manager_instance,
            storage,
            flow_instances,
        )
        .await
        .expect("start manager server");
    });

    let http = http_client();
    let manager_base = format!("http://{manager_addr}");
    let stream_name = format!("extra_shared_stream_stats_{}", random_suffix());
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
        "create shared stream failed: status={} body={}",
        create_stream_status,
        create_stream_body
    );

    let pipeline_id = format!("extra_shared_stream_pipeline_{}", random_suffix());
    let sql = format!("SELECT value FROM {stream_name}");
    let create_pipeline_resp = http
        .post(format!("{manager_base}/pipelines"))
        .json(
            &PipelineCreateRequest::nop(pipeline_id.clone(), sql).with_flow_instance_id("extra_a"),
        )
        .send()
        .await
        .expect("create extra pipeline request");
    let create_pipeline_status = create_pipeline_resp.status();
    let create_pipeline_body = create_pipeline_resp.text().await.unwrap_or_default();
    assert!(
        create_pipeline_status.is_success(),
        "create extra pipeline failed: status={} body={}",
        create_pipeline_status,
        create_pipeline_body
    );

    let start_pipeline_resp = http
        .post(format!("{manager_base}/pipelines/{pipeline_id}/start"))
        .send()
        .await
        .expect("start extra pipeline request");
    let start_pipeline_status = start_pipeline_resp.status();
    let start_pipeline_body = start_pipeline_resp.text().await.unwrap_or_default();
    assert!(
        start_pipeline_status.is_success(),
        "start extra pipeline failed: status={} body={}",
        start_pipeline_status,
        start_pipeline_body
    );

    let missing_target_resp = http
        .get(format!("{manager_base}/streams/{stream_name}/shared/stats"))
        .send()
        .await
        .expect("shared stream stats without flow_instance_id");
    let missing_target_status = missing_target_resp.status();
    let missing_target_body = missing_target_resp.text().await.unwrap_or_default();
    assert_eq!(missing_target_status, StatusCode::BAD_REQUEST);
    assert!(
        missing_target_body.contains("flow_instance_id is required"),
        "unexpected body: {missing_target_body}"
    );

    let stats =
        wait_for_shared_stream_running(&http, &manager_base, &stream_name, Some("extra_a")).await;
    assert_eq!(stats["stream"], stream_name);
    assert_eq!(stats["flow_instance_id"], "extra_a");
    assert_eq!(stats["status"], "running");

    let stop_pipeline_resp = http
        .post(format!(
            "{manager_base}/pipelines/{pipeline_id}/stop?mode=graceful&timeout_ms=5000"
        ))
        .send()
        .await
        .expect("stop extra pipeline request");
    assert_eq!(stop_pipeline_resp.status(), StatusCode::OK);

    let delete_pipeline_resp = http
        .delete(format!("{manager_base}/pipelines/{pipeline_id}"))
        .send()
        .await
        .expect("delete extra pipeline request");
    assert_eq!(delete_pipeline_resp.status(), StatusCode::OK);

    let delete_stream_resp = http
        .delete(format!("{manager_base}/streams/{stream_name}"))
        .send()
        .await
        .expect("delete stream request");
    assert_eq!(delete_stream_resp.status(), StatusCode::OK);

    manager_server.abort();
    let _ = manager_server.await;
}
