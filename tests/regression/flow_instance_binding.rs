use super::{
    bind_manager_listener_or_skip, default_flow_instances, make_client, random_suffix,
    wait_for_server,
};
use sdk::types::{PipelineCreateRequest, StreamCreateRequest};
use sdk::SdkError;
use serde_json::Value as JsonValue;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flow_instance_pipeline_binding_lifecycle() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let storage = storage::StorageManager::new(temp_dir.path()).expect("create storage manager");
    let instance = manager::new_default_flow_instance();

    let Some(listener) = bind_manager_listener_or_skip().await else {
        return;
    };
    let addr = listener.local_addr().expect("read listener addr");

    let flow_instances = default_flow_instances();

    let server = tokio::spawn(async move {
        manager::start_server_with_listener(listener, instance, storage, flow_instances)
            .await
            .expect("start manager server");
    });

    let client = make_client(addr);
    wait_for_server(&client).await;

    let stream_name = format!("bind_stream_{}", random_suffix());
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

    let pipeline_id = format!("bind_pipe_{}", random_suffix());
    let sql = format!("SELECT value FROM {stream_name}");

    let pipe_req = PipelineCreateRequest::nop(pipeline_id.clone(), sql);

    client
        .create_pipeline(&pipe_req)
        .await
        .expect("create pipeline");

    client
        .start_pipeline(&pipeline_id)
        .await
        .expect("start pipeline");

    let list = client.list_pipelines().await.expect("list pipelines");
    assert!(
        list.iter().any(|item| {
            item["id"].as_str() == Some(pipeline_id.as_str())
                && item["flow_instance_id"].as_str() == Some("default")
        }),
        "pipelines list missing default binding"
    );

    let get_resp: JsonValue = client
        .get_pipeline(&pipeline_id)
        .await
        .expect("get pipeline");
    assert_eq!(get_resp["spec"]["id"].as_str(), Some(pipeline_id.as_str()));
    assert_eq!(
        get_resp["spec"]["flow_instance_id"].as_str(),
        Some("default")
    );

    let pipeline_id2 = format!("bind_pipe2_{}", random_suffix());
    let sql2 = format!("SELECT value FROM {stream_name}");
    let pipe_req2 = PipelineCreateRequest::nop(pipeline_id2.clone(), sql2)
        .with_flow_instance_id("extra_undeclared".to_string());

    let err = client
        .create_pipeline(&pipe_req2)
        .await
        .expect_err("create pipeline with undeclared flow_instance_id should fail");

    match err {
        SdkError::Http { status, body, .. } => {
            assert_eq!(status.as_u16(), 400, "body: {body}");
        }
        other => panic!("expected HTTP 400 error, got: {other}"),
    }

    client
        .delete_pipeline(&pipeline_id)
        .await
        .expect("delete pipeline");

    server.abort();
    let _ = server.await;
}
