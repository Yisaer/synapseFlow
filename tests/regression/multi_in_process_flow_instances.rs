use super::{bind_manager_listener_or_skip, make_client, random_suffix, wait_for_server};

use sdk::{PipelineCreateRequest, StopOptions, StreamCreateRequest};
use serde_json::Value as JsonValue;
use std::time::Duration;

fn multi_in_process_flow_instances() -> Vec<manager::FlowInstanceSpec> {
    vec![
        manager::FlowInstanceSpec {
            id: "default".to_string(),
            ..manager::FlowInstanceSpec::default()
        },
        manager::FlowInstanceSpec {
            id: "extra_a".to_string(),
            ..manager::FlowInstanceSpec::default()
        },
        manager::FlowInstanceSpec {
            id: "extra_b".to_string(),
            ..manager::FlowInstanceSpec::default()
        },
    ]
}

async fn wait_pipeline_status(client: &sdk::ManagerClient, pipeline_id: &str, expected: &str) {
    for _ in 0..30 {
        let list = client.list_pipelines().await.expect("list pipelines");
        if let Some(item) = list
            .iter()
            .find(|item| item["id"].as_str() == Some(pipeline_id))
        {
            if item["status"].as_str() == Some(expected) {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let list = client.list_pipelines().await.expect("list pipelines");
    panic!(
        "pipeline {} did not reach status {}, latest list: {:?}",
        pipeline_id, expected, list
    );
}

// coverage-covers: pipeline.lifecycle.start_stop
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_in_process_flow_instances_pipeline_lifecycle_via_rest() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let storage = storage::StorageManager::new(temp_dir.path()).expect("create storage manager");
    let instance = manager::new_default_flow_instance();

    let Some(listener) = bind_manager_listener_or_skip().await else {
        return;
    };
    let addr = listener.local_addr().expect("read listener addr");

    let server = tokio::spawn(async move {
        manager::start_server_with_listener(
            listener,
            instance,
            storage,
            multi_in_process_flow_instances(),
        )
        .await
        .expect("start manager server");
    });

    let client = make_client(addr);
    wait_for_server(&client).await;

    let stream_name = format!("multi_inst_stream_{}", random_suffix());
    let stream_req = StreamCreateRequest {
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
    };

    client
        .create_stream(&stream_req)
        .await
        .expect("create stream");

    let sql = format!("SELECT value FROM {stream_name}");
    let pipe_default = format!("multi_default_{}", random_suffix());
    let pipe_extra_a = format!("multi_extra_a_{}", random_suffix());
    let pipe_extra_b = format!("multi_extra_b_{}", random_suffix());

    let req_default = PipelineCreateRequest::nop(pipe_default.clone(), sql.clone());
    let req_extra_a = PipelineCreateRequest::nop(pipe_extra_a.clone(), sql.clone())
        .with_flow_instance_id("extra_a");
    let req_extra_b = PipelineCreateRequest::nop(pipe_extra_b.clone(), sql.clone())
        .with_flow_instance_id("extra_b");

    client
        .create_pipeline(&req_default)
        .await
        .expect("create default pipeline");
    client
        .create_pipeline(&req_extra_a)
        .await
        .expect("create extra_a pipeline");
    client
        .create_pipeline(&req_extra_b)
        .await
        .expect("create extra_b pipeline");

    client
        .start_pipeline(&pipe_default)
        .await
        .expect("start default pipeline");
    client
        .start_pipeline(&pipe_extra_a)
        .await
        .expect("start extra_a pipeline");
    client
        .start_pipeline(&pipe_extra_b)
        .await
        .expect("start extra_b pipeline");

    wait_pipeline_status(&client, &pipe_default, "running").await;
    wait_pipeline_status(&client, &pipe_extra_a, "running").await;
    wait_pipeline_status(&client, &pipe_extra_b, "running").await;

    let list = client.list_pipelines().await.expect("list pipelines");
    let assert_bound = |pipeline_id: &str, expected_instance: &str| {
        assert!(
            list.iter().any(|item| {
                item["id"].as_str() == Some(pipeline_id)
                    && item["flow_instance_id"].as_str() == Some(expected_instance)
            }),
            "pipeline {} not bound to {}, list: {:?}",
            pipeline_id,
            expected_instance,
            list
        );
    };
    assert_bound(&pipe_default, "default");
    assert_bound(&pipe_extra_a, "extra_a");
    assert_bound(&pipe_extra_b, "extra_b");

    let get_default: JsonValue = client
        .get_pipeline(&pipe_default)
        .await
        .expect("get default pipeline");
    let get_extra_a: JsonValue = client
        .get_pipeline(&pipe_extra_a)
        .await
        .expect("get extra_a pipeline");
    let get_extra_b: JsonValue = client
        .get_pipeline(&pipe_extra_b)
        .await
        .expect("get extra_b pipeline");

    assert_eq!(
        get_default["spec"]["flow_instance_id"].as_str(),
        Some("default")
    );
    assert_eq!(
        get_extra_a["spec"]["flow_instance_id"].as_str(),
        Some("extra_a")
    );
    assert_eq!(
        get_extra_b["spec"]["flow_instance_id"].as_str(),
        Some("extra_b")
    );

    let stop_opt = StopOptions::graceful(5_000);
    client
        .stop_pipeline(&pipe_default, stop_opt.clone())
        .await
        .expect("stop default pipeline");
    client
        .stop_pipeline(&pipe_extra_a, stop_opt.clone())
        .await
        .expect("stop extra_a pipeline");
    client
        .stop_pipeline(&pipe_extra_b, stop_opt)
        .await
        .expect("stop extra_b pipeline");

    wait_pipeline_status(&client, &pipe_default, "stopped").await;
    wait_pipeline_status(&client, &pipe_extra_a, "stopped").await;
    wait_pipeline_status(&client, &pipe_extra_b, "stopped").await;

    client
        .delete_pipeline(&pipe_default)
        .await
        .expect("delete default pipeline");
    client
        .delete_pipeline(&pipe_extra_a)
        .await
        .expect("delete extra_a pipeline");
    client
        .delete_pipeline(&pipe_extra_b)
        .await
        .expect("delete extra_b pipeline");

    client
        .delete_stream(&stream_name)
        .await
        .expect("delete stream");

    server.abort();
    let _ = server.await;
}
