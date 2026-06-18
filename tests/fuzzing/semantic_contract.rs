use super::{
    bind_manager_listener_or_skip, default_flow_instances, http_client, make_client, random_suffix,
    wait_for_server,
};

use reqwest::StatusCode;
use sdk::types::PipelineUpsertRequest;
use sdk::{ManagerClient, PipelineCreateRequest, SdkError, StopOptions, StreamCreateRequest};
use serde_json::{json, Value as JsonValue};
use std::net::SocketAddr;

struct TestHarness {
    _temp_dir: tempfile::TempDir,
    server: tokio::task::JoinHandle<()>,
    addr: SocketAddr,
    client: ManagerClient,
    http: reqwest::Client,
}

impl TestHarness {
    async fn new() -> Option<Self> {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let storage =
            storage::StorageManager::new(temp_dir.path()).expect("create storage manager");
        let instance = manager::new_default_flow_instance();

        let Some(listener) = bind_manager_listener_or_skip().await else {
            return None;
        };
        let addr = listener.local_addr().expect("read listener addr");

        let server = tokio::spawn(async move {
            manager::start_server_with_listener(
                listener,
                instance,
                storage,
                default_flow_instances(),
            )
            .await
            .expect("start manager server");
        });

        let client = make_client(addr);
        let http = http_client();

        wait_for_server(&client).await;

        Some(Self {
            _temp_dir: temp_dir,
            server,
            addr,
            client,
            http,
        })
    }

    fn base(&self) -> String {
        format!("http://{}", self.addr)
    }
}

impl Drop for TestHarness {
    fn drop(&mut self) {
        self.server.abort();
    }
}

fn basic_mock_stream_req(name: String) -> StreamCreateRequest {
    StreamCreateRequest {
        name,
        stream_type: "mock".to_string(),
        schema: json!({
            "type": "json",
            "props": {
                "columns": [
                    { "name": "value", "data_type": "int64" }
                ]
            }
        }),
        props: json!({}),
        shared: false,
        decoder: json!({ "type": "json", "props": {} }),
    }
}

fn mqtt_stream_req(
    name: String,
    broker_url: Option<&str>,
    topic: Option<&str>,
) -> StreamCreateRequest {
    let mut props = serde_json::Map::new();
    if let Some(v) = broker_url {
        props.insert("broker_url".to_string(), JsonValue::String(v.to_string()));
    }
    if let Some(v) = topic {
        props.insert("topic".to_string(), JsonValue::String(v.to_string()));
    }

    StreamCreateRequest {
        name,
        stream_type: "mqtt".to_string(),
        schema: json!({
            "type": "json",
            "props": {
                "columns": [
                    { "name": "value", "data_type": "int64" }
                ]
            }
        }),
        props: JsonValue::Object(props),
        shared: false,
        decoder: json!({ "type": "json", "props": {} }),
    }
}

fn pipeline_with_default_sink(id: String, sql: String) -> JsonValue {
    json!({
        "id": id,
        "sql": sql,
        "sinks": [
            {
                "type": "console",
                "props": {}
            }
        ]
    })
}

fn pipeline_with_sink_type(
    id: String,
    sql: String,
    sink_type: &str,
    sink_props: JsonValue,
) -> JsonValue {
    json!({
        "id": id,
        "sql": sql,
        "sinks": [
            {
                "type": sink_type,
                "props": sink_props
            }
        ]
    })
}

fn assert_http_status(err: SdkError, expected: u16) -> String {
    match err {
        SdkError::Http { status, body, .. } => {
            assert_eq!(status.as_u16(), expected, "unexpected body: {body}");
            body
        }
        other => panic!("expected HTTP {expected}, got: {other:?}"),
    }
}

async fn assert_pipeline_absent(http: &HttpClient, base: &str, id: &str) {
    let resp = http
        .get(format!("{base}/pipelines/{id}"))
        .send()
        .await
        .expect("get pipeline");
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_stream_rejects_missing_name() {
    let Some(h) = TestHarness::new().await else {
        return;
    };

    let err = h
        .client
        .create_stream(&basic_mock_stream_req("".to_string()))
        .await
        .expect_err("missing stream name should fail");

    let body = assert_http_status(err, 400);
    assert!(
        body.contains("name") || body.contains("empty"),
        "unexpected body: {body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_stream_rejects_missing_type() {
    let Some(h) = TestHarness::new().await else {
        return;
    };

    let stream_name = format!("missing_type_{}", random_suffix());
    let req = StreamCreateRequest {
        name: stream_name,
        stream_type: "".to_string(),
        schema: json!({
            "type": "json",
            "props": {
                "columns": [
                    { "name": "value", "data_type": "int64" }
                ]
            }
        }),
        props: json!({}),
        shared: false,
        decoder: json!({ "type": "json", "props": {} }),
    };

    let err = h
        .client
        .create_stream(&req)
        .await
        .expect_err("missing stream type should fail");

    match err {
        SdkError::Http { status, body, .. } => {
            assert_eq!(status.as_u16(), 400, "body: {body}");
        }
        other => panic!("expected HTTP 400 error, got: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_stream_rejects_missing_schema() {
    let Some(h) = TestHarness::new().await else {
        return;
    };

    let stream_name = format!("missing_schema_{}", random_suffix());
    let req = StreamCreateRequest {
        name: stream_name,
        stream_type: "mock".to_string(),
        schema: json!({}),
        props: json!({}),
        shared: false,
        decoder: json!({ "type": "json", "props": {} }),
    };

    let err = h
        .client
        .create_stream(&req)
        .await
        .expect_err("missing schema should fail");

    match err {
        SdkError::Http { status, body, .. } => {
            assert_eq!(status.as_u16(), 400, "body: {body}");
        }
        other => panic!("expected HTTP 400 error, got: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_stream_rejects_duplicate_name() {
    let Some(h) = TestHarness::new().await else {
        return;
    };

    let stream_name = format!("dup_stream_{}", random_suffix());
    h.client
        .create_stream(&basic_mock_stream_req(stream_name.clone()))
        .await
        .expect("create first stream");

    let err = h
        .client
        .create_stream(&basic_mock_stream_req(stream_name.clone()))
        .await
        .expect_err("duplicate stream create should fail");

    match err {
        SdkError::Http { status, body, .. } => {
            assert!(
                status.as_u16() == 400 || status.as_u16() == 409,
                "unexpected status={}, body={body}",
                status.as_u16()
            );
            assert!(
                body.contains("duplicate")
                    || body.contains("exists")
                    || body.contains("already exists"),
                "unexpected body: {body}"
            );
        }
        other => panic!("expected HTTP error, got: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_stream_does_not_reject_empty_columns() {
    let Some(h) = TestHarness::new().await else {
        return;
    };

    let stream_name = format!("empty_columns_{}", random_suffix());
    let req = StreamCreateRequest {
        name: stream_name.clone(),
        stream_type: "mock".to_string(),
        schema: json!({
            "type": "json",
            "props": { "columns": [] }
        }),
        props: json!({}),
        shared: false,
        decoder: json!({ "type": "json", "props": {} }),
    };

    let created = h
        .client
        .create_stream(&req)
        .await
        .expect("empty columns should be accepted");

    assert_eq!(created["name"].as_str(), Some(stream_name.as_str()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_stream_rejects_column_without_name() {
    let Some(h) = TestHarness::new().await else {
        return;
    };

    let stream_name = format!("column_without_name_{}", random_suffix());
    let req = StreamCreateRequest {
        name: stream_name,
        stream_type: "mock".to_string(),
        schema: json!({
            "type": "json",
            "props": {
                "columns": [
                    { "data_type": "int64" }
                ]
            }
        }),
        props: json!({}),
        shared: false,
        decoder: json!({ "type": "json", "props": {} }),
    };

    let err = h
        .client
        .create_stream(&req)
        .await
        .expect_err("column without name should fail");

    match err {
        SdkError::Http { status, body, .. } => {
            assert_eq!(status.as_u16(), 400, "body: {body}");
        }
        other => panic!("expected HTTP 400 error, got: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_stream_rejects_column_without_data_type() {
    let Some(h) = TestHarness::new().await else {
        return;
    };

    let stream_name = format!("column_without_dtype_{}", random_suffix());
    let req = StreamCreateRequest {
        name: stream_name,
        stream_type: "mock".to_string(),
        schema: json!({
            "type": "json",
            "props": {
                "columns": [
                    { "name": "value" }
                ]
            }
        }),
        props: json!({}),
        shared: false,
        decoder: json!({ "type": "json", "props": {} }),
    };

    let err = h
        .client
        .create_stream(&req)
        .await
        .expect_err("column without data type should fail");

    match err {
        SdkError::Http { status, body, .. } => {
            assert_eq!(status.as_u16(), 400, "body: {body}");
        }
        other => panic!("expected HTTP 400 error, got: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_stream_rejects_invalid_data_type() {
    let Some(h) = TestHarness::new().await else {
        return;
    };

    let stream_name = format!("invalid_dtype_{}", random_suffix());
    let req = StreamCreateRequest {
        name: stream_name,
        stream_type: "mock".to_string(),
        schema: json!({
            "type": "json",
            "props": {
                "columns": [
                    { "name": "value", "data_type": "definitely_not_a_type" }
                ]
            }
        }),
        props: json!({}),
        shared: false,
        decoder: json!({ "type": "json", "props": {} }),
    };

    let err = h
        .client
        .create_stream(&req)
        .await
        .expect_err("invalid data type should fail");

    match err {
        SdkError::Http { status, body, .. } => {
            assert_eq!(status.as_u16(), 400, "body: {body}");
        }
        other => panic!("expected HTTP 400 error, got: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_stream_rejects_missing_mqtt_broker_url() {
    let Some(h) = TestHarness::new().await else {
        return;
    };

    let stream_name = format!("mqtt_missing_broker_{}", random_suffix());
    let err = h
        .client
        .create_stream(&mqtt_stream_req(stream_name.clone(), None, Some("/topic")))
        .await
        .expect_err("missing mqtt broker_url should fail");

    let body = assert_http_status(err, 400);
    assert!(
        body.contains("broker") || body.contains("broker_url"),
        "unexpected body: {body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_stream_rejects_missing_mqtt_topic() {
    let Some(h) = TestHarness::new().await else {
        return;
    };

    let stream_name = format!("mqtt_missing_topic_{}", random_suffix());
    let err = h
        .client
        .create_stream(&mqtt_stream_req(
            stream_name.clone(),
            Some("tcp://127.0.0.1:1883"),
            None,
        ))
        .await
        .expect_err("missing mqtt topic should fail");

    let body = assert_http_status(err, 400);
    assert!(body.contains("topic"), "unexpected body: {body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_stream_rejects_empty_mqtt_topic() {
    let Some(h) = TestHarness::new().await else {
        return;
    };

    let stream_name = format!("mqtt_empty_topic_{}", random_suffix());
    let err = h
        .client
        .create_stream(&mqtt_stream_req(
            stream_name.clone(),
            Some("tcp://127.0.0.1:1883"),
            Some(""),
        ))
        .await
        .expect_err("empty mqtt topic should fail");

    let body = assert_http_status(err, 400);
    assert!(
        body.contains("topic") || body.contains("empty"),
        "unexpected body: {body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_pipeline_rejects_invalid_sql_syntax() {
    let Some(h) = TestHarness::new().await else {
        return;
    };

    let pipeline_id = format!("invalid_sql_{}", random_suffix());
    let err = h
        .client
        .create_pipeline(&PipelineCreateRequest::nop(
            pipeline_id.clone(),
            "SELECT FROM".to_string(),
        ))
        .await
        .expect_err("invalid SQL should fail");

    match err {
        SdkError::Http { status, body, .. } => {
            assert_eq!(status.as_u16(), 400, "body: {body}");
        }
        other => panic!("expected HTTP error, got: {other:?}"),
    }
    let get_err = h
        .client
        .get_pipeline(&pipeline_id)
        .await
        .expect_err("rejected pipeline should not be persisted");

    match get_err {
        SdkError::Http { status, body, .. } => {
            assert_eq!(status.as_u16(), 404, "body: {body}");
        }
        other => panic!("expected HTTP 404 for absent pipeline, got: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_pipeline_rejects_empty_sql() {
    let Some(h) = TestHarness::new().await else {
        return;
    };

    let pipeline_id = format!("empty_sql_{}", random_suffix());
    let err = h
        .client
        .create_pipeline(&PipelineCreateRequest::nop(pipeline_id, "".to_string()))
        .await
        .expect_err("empty SQL should fail");

    let _ = assert_http_status(err, 400);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_pipeline_rejects_unknown_stream_reference() {
    let Some(h) = TestHarness::new().await else {
        return;
    };

    let pipeline_id = format!("unknown_stream_{}", random_suffix());
    let err = h
        .client
        .create_pipeline(&PipelineCreateRequest::nop(
            pipeline_id,
            "SELECT value FROM nonexistent_stream".to_string(),
        ))
        .await
        .expect_err("unknown stream reference should fail");

    let _ = assert_http_status(err, 400);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_pipeline_rejects_unknown_column_reference() {
    let Some(h) = TestHarness::new().await else {
        return;
    };

    let stream_name = format!("pipe_col_stream_{}", random_suffix());
    h.client
        .create_stream(&basic_mock_stream_req(stream_name.clone()))
        .await
        .expect("create stream");

    let pipeline_id = format!("unknown_col_{}", random_suffix());
    let err = h
        .client
        .create_pipeline(&PipelineCreateRequest::nop(
            pipeline_id,
            format!("SELECT no_such_col FROM {stream_name}"),
        ))
        .await
        .expect_err("unknown column reference should fail");

    let _ = assert_http_status(err, 400);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_pipeline_rejects_missing_sinks() {
    let Some(h) = TestHarness::new().await else {
        return;
    };

    let stream_name = format!("missing_sinks_stream_{}", random_suffix());
    h.client
        .create_stream(&basic_mock_stream_req(stream_name.clone()))
        .await
        .expect("create stream");

    let pipeline_id = format!("missing_sinks_{}", random_suffix());
    let resp = h
        .http
        .post(format!("{}/pipelines", h.base()))
        .json(&json!({
            "id": pipeline_id,
            "sql": format!("SELECT value FROM {stream_name}")
        }))
        .send()
        .await
        .expect("create pipeline request");

    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_pipeline_rejects_empty_sinks() {
    let Some(h) = TestHarness::new().await else {
        return;
    };

    let stream_name = format!("empty_sinks_stream_{}", random_suffix());
    h.client
        .create_stream(&basic_mock_stream_req(stream_name.clone()))
        .await
        .expect("create stream");

    let pipeline_id = format!("empty_sinks_{}", random_suffix());
    let resp = h
        .http
        .post(format!("{}/pipelines", h.base()))
        .json(&json!({
            "id": pipeline_id,
            "sql": format!("SELECT value FROM {stream_name}"),
            "sinks": []
        }))
        .send()
        .await
        .expect("create pipeline request");

    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_pipeline_rejects_invalid_sink_type() {
    let Some(h) = TestHarness::new().await else {
        return;
    };

    let stream_name = format!("invalid_sink_stream_{}", random_suffix());
    h.client
        .create_stream(&basic_mock_stream_req(stream_name.clone()))
        .await
        .expect("create stream");

    let pipeline_id = format!("invalid_sink_type_{}", random_suffix());
    let resp = h
        .http
        .post(format!("{}/pipelines", h.base()))
        .json(&pipeline_with_sink_type(
            pipeline_id,
            format!("SELECT value FROM {stream_name}"),
            "not_a_real_sink",
            json!({}),
        ))
        .send()
        .await
        .expect("create pipeline request");

    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_pipeline_rejects_missing_required_sink_props() {
    let Some(h) = TestHarness::new().await else {
        return;
    };

    let stream_name = format!("missing_sink_props_stream_{}", random_suffix());
    h.client
        .create_stream(&basic_mock_stream_req(stream_name.clone()))
        .await
        .expect("create stream");

    let pipeline_id = format!("missing_sink_props_{}", random_suffix());
    let resp = h
        .http
        .post(format!("{}/pipelines", h.base()))
        .json(&pipeline_with_sink_type(
            pipeline_id,
            format!("SELECT value FROM {stream_name}"),
            "mqtt",
            json!({}),
        ))
        .send()
        .await
        .expect("create pipeline request");

    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_pipeline_rejects_valid_sql_with_invalid_sink() {
    let Some(h) = TestHarness::new().await else {
        return;
    };

    let stream_name = format!("valid_sql_invalid_sink_stream_{}", random_suffix());
    h.client
        .create_stream(&basic_mock_stream_req(stream_name.clone()))
        .await
        .expect("create stream");

    let pipeline_id = format!("valid_sql_invalid_sink_{}", random_suffix());
    let resp = h
        .http
        .post(format!("{}/pipelines", h.base()))
        .json(&pipeline_with_sink_type(
            pipeline_id,
            format!("SELECT value FROM {stream_name}"),
            "mqtt",
            json!({ "topic": "/missing/broker/url" }),
        ))
        .send()
        .await
        .expect("create pipeline request");

    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_pipeline_rejects_valid_sink_with_invalid_sql() {
    let Some(h) = TestHarness::new().await else {
        return;
    };

    let pipeline_id = format!("valid_sink_invalid_sql_{}", random_suffix());
    let err = h
        .client
        .create_pipeline(&PipelineCreateRequest::nop(
            pipeline_id,
            "SELECT definitely broken sql".to_string(),
        ))
        .await
        .expect_err("invalid SQL should fail even with otherwise valid default sink");

    let _ = assert_http_status(err, 400);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_stream_returns_conflict_when_pipeline_still_references_it() {
    let Some(h) = TestHarness::new().await else {
        return;
    };

    let stream_name = format!("delete_conflict_stream_{}", random_suffix());
    h.client
        .create_stream(&basic_mock_stream_req(stream_name.clone()))
        .await
        .expect("create stream");

    let pipeline_id = format!("delete_conflict_pipe_{}", random_suffix());
    let pipe_req = PipelineCreateRequest::nop(
        pipeline_id.clone(),
        format!("SELECT value FROM {stream_name}"),
    );
    h.client
        .create_pipeline(&pipe_req)
        .await
        .expect("create pipeline");

    let err = h
        .client
        .delete_stream(&stream_name)
        .await
        .expect_err("delete referenced stream should fail");

    let body = assert_http_status(err, 409);
    assert!(
        body.contains("refer") || body.contains("pipeline"),
        "unexpected body: {body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_pipeline_rejects_nonexistent_stream_reference() {
    let Some(h) = TestHarness::new().await else {
        return;
    };

    let pipeline_id = format!("missing_dep_create_{}", random_suffix());
    let err_resp = h
        .http
        .post(format!("{}/pipelines", h.base()))
        .json(&pipeline_with_default_sink(
            pipeline_id.clone(),
            "SELECT value FROM stream_that_does_not_exist".to_string(),
        ))
        .send()
        .await
        .expect("create pipeline request");

    let status = err_resp.status();
    let body = err_resp.text().await.unwrap_or_default();
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upsert_pipeline_rejects_nonexistent_stream_reference() {
    let Some(h) = TestHarness::new().await else {
        return;
    };

    let pipeline_id = format!("missing_dep_upsert_{}", random_suffix());
    let resp = h
        .http
        .put(format!("{}/pipelines/{pipeline_id}", h.base()))
        .json(&pipeline_with_default_sink(
            pipeline_id.clone(),
            "SELECT value FROM stream_that_does_not_exist".to_string(),
        ))
        .send()
        .await
        .expect("upsert pipeline request");

    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
    assert_pipeline_absent(&h.http, &h.base(), &pipeline_id).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_pipeline_after_deleting_referenced_stream_is_rejected() {
    let Some(h) = TestHarness::new().await else {
        return;
    };

    let stream_name = format!("deleted_stream_start_reject_{}", random_suffix());
    h.client
        .create_stream(&basic_mock_stream_req(stream_name.clone()))
        .await
        .expect("create stream");

    let pipeline_id = format!("deleted_stream_start_reject_pipe_{}", random_suffix());
    let pipe_req = PipelineCreateRequest::nop(
        pipeline_id.clone(),
        format!("SELECT value FROM {stream_name}"),
    );
    h.client
        .create_pipeline(&pipe_req)
        .await
        .expect("create pipeline");

    h.client
        .delete_pipeline(&pipeline_id)
        .await
        .expect("delete pipeline before removing stream");

    h.client
        .delete_stream(&stream_name)
        .await
        .expect("delete stream");

    let recreated = PipelineCreateRequest::nop(
        pipeline_id.clone(),
        format!("SELECT value FROM {stream_name}"),
    );
    let err = h
        .client
        .create_pipeline(&recreated)
        .await
        .expect_err("recreate pipeline against deleted stream should fail");
    let _ = assert_http_status(err, 400);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upsert_pipeline_rejects_invalid_sql() {
    let Some(h) = TestHarness::new().await else {
        return;
    };

    let pipeline_id = format!("upsert_invalid_sql_{}", random_suffix());
    let resp = h
        .http
        .put(format!("{}/pipelines/{pipeline_id}", h.base()))
        .json(&pipeline_with_default_sink(
            pipeline_id.clone(),
            "SELECT FROM".to_string(),
        ))
        .send()
        .await
        .expect("upsert pipeline request");

    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upsert_pipeline_rejects_missing_sinks() {
    let Some(h) = TestHarness::new().await else {
        return;
    };

    let stream_name = format!("upsert_missing_sinks_stream_{}", random_suffix());
    h.client
        .create_stream(&basic_mock_stream_req(stream_name.clone()))
        .await
        .expect("create stream");

    let pipeline_id = format!("upsert_missing_sinks_{}", random_suffix());
    let resp = h
        .http
        .put(format!("{}/pipelines/{pipeline_id}", h.base()))
        .json(&json!({
            "id": pipeline_id,
            "sql": format!("SELECT value FROM {stream_name}")
        }))
        .send()
        .await
        .expect("upsert pipeline request");

    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upsert_pipeline_rejects_invalid_stream_reference() {
    let Some(h) = TestHarness::new().await else {
        return;
    };

    let pipeline_id = format!("upsert_bad_ref_{}", random_suffix());
    let resp = h
        .http
        .put(format!("{}/pipelines/{pipeline_id}", h.base()))
        .json(&pipeline_with_default_sink(
            pipeline_id.clone(),
            "SELECT value FROM no_such_stream".to_string(),
        ))
        .send()
        .await
        .expect("upsert pipeline request");

    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upsert_pipeline_preserves_running_state() {
    let Some(h) = TestHarness::new().await else {
        return;
    };

    let stream_name = format!("upsert_running_stream_{}", random_suffix());
    h.client
        .create_stream(&basic_mock_stream_req(stream_name.clone()))
        .await
        .expect("create stream");

    let pipeline_id = format!("upsert_running_pipe_{}", random_suffix());
    let req = PipelineCreateRequest::nop(
        pipeline_id.clone(),
        format!("SELECT value FROM {stream_name}"),
    );
    h.client
        .create_pipeline(&req)
        .await
        .expect("create pipeline");

    h.client
        .start_pipeline(&pipeline_id)
        .await
        .expect("start pipeline");

    let upsert_req =
        PipelineUpsertRequest::nop(format!("SELECT value FROM {stream_name} WHERE value >= 0"));

    h.client
        .upsert_pipeline(&pipeline_id, &upsert_req)
        .await
        .expect("upsert pipeline");

    let pipeline = h
        .client
        .get_pipeline(&pipeline_id)
        .await
        .expect("get pipeline");

    assert!(
        pipeline["status"] == "running"
            || pipeline["desired_state"] == "running"
            || pipeline["run_state"] == "running",
        "pipeline should remain running after upsert: {pipeline}"
    );

    h.client
        .stop_pipeline(&pipeline_id, StopOptions::graceful(5000))
        .await
        .expect("stop pipeline");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upsert_pipeline_preserves_stopped_state() {
    let Some(h) = TestHarness::new().await else {
        return;
    };

    let stream_name = format!("upsert_stopped_stream_{}", random_suffix());
    h.client
        .create_stream(&basic_mock_stream_req(stream_name.clone()))
        .await
        .expect("create stream");

    let pipeline_id = format!("upsert_stopped_pipe_{}", random_suffix());
    let req = PipelineCreateRequest::nop(
        pipeline_id.clone(),
        format!("SELECT value FROM {stream_name}"),
    );
    h.client
        .create_pipeline(&req)
        .await
        .expect("create pipeline");

    let upsert_req =
        PipelineUpsertRequest::nop(format!("SELECT value FROM {stream_name} WHERE value >= 0"));

    h.client
        .upsert_pipeline(&pipeline_id, &upsert_req)
        .await
        .expect("upsert pipeline");

    let pipeline = h
        .client
        .get_pipeline(&pipeline_id)
        .await
        .expect("get pipeline");

    assert!(
        pipeline["status"] == "stopped"
            || pipeline["desired_state"] == "stopped"
            || pipeline["run_state"] == "stopped"
            || pipeline["status"].is_null(),
        "pipeline should remain stopped after upsert: {pipeline}"
    );
}
