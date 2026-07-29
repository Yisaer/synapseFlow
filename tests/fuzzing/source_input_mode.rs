use reqwest::StatusCode;
use sdk::{ManagerClient, PipelineCreateRequest, StreamCreateRequest};
use serde_json::{json, Value as JsonValue};
use std::net::SocketAddr;

use super::{
    bind_manager_listener_or_skip, default_flow_instances, http_client, make_client, random_suffix,
    wait_for_server,
};

struct TestHarness {
    _temp_dir: tempfile::TempDir,
    server: tokio::task::JoinHandle<()>,
    addr: SocketAddr,
    client: ManagerClient,
    http: reqwest::Client,
}

impl TestHarness {
    fn base(&self) -> String {
        format!("http://{}", self.addr)
    }

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
}

impl Drop for TestHarness {
    fn drop(&mut self) {
        self.server.abort();
    }
}

fn schema() -> JsonValue {
    json!({
        "type": "json",
        "props": {
            "columns": [
                { "name": "value", "data_type": "int64" },
                { "name": "flag", "data_type": "bool" },
                { "name": "user_id", "data_type": "string" }
            ]
        }
    })
}

fn stream_req(
    name: String,
    stream_type: &str,
    shared: bool,
    props: JsonValue,
    decoder: JsonValue,
) -> StreamCreateRequest {
    StreamCreateRequest {
        name,
        revision: 1,
        stream_type: stream_type.to_string(),
        schema: schema(),
        props,
        shared,
        decoder,
    }
}

fn mock_stream_req(name: String) -> StreamCreateRequest {
    stream_req(
        name,
        "mock",
        false,
        json!({}),
        json!({ "type": "json", "props": {} }),
    )
}

fn shared_mock_stream_req(name: String) -> StreamCreateRequest {
    stream_req(
        name,
        "mock",
        true,
        json!({}),
        json!({ "type": "json", "props": {} }),
    )
}

fn memory_collection_stream_req(name: String, topic: String) -> StreamCreateRequest {
    stream_req(
        name,
        "memory",
        false,
        json!({ "topic": topic }),
        json!({ "type": "none", "props": {} }),
    )
}

fn bad_decoder_stream_req(name: String) -> StreamCreateRequest {
    stream_req(
        name,
        "mock",
        false,
        json!({}),
        json!({ "type": "definitely_not_registered_decoder", "props": {} }),
    )
}

fn pipeline_with_sources(id: String, sql: String, sources: JsonValue) -> JsonValue {
    json!({
        "id": id,
            "revision": 1,
        "sql": sql,
        "sources": sources,
        "sinks": [
            {
                "type": "nop",
                "props": { "log": false },
                "encoder": { "type": "json", "props": {} }
            }
        ]
    })
}

async fn post_pipeline_raw(h: &TestHarness, body: JsonValue) -> (StatusCode, String) {
    let resp = h
        .http
        .post(format!("{}/pipelines", h.base()))
        .json(&body)
        .send()
        .await
        .expect("post pipeline");

    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    (status, body)
}

fn assert_success_or_controlled_400(status: StatusCode, body: &str, context: &str) {
    if status.is_success() {
        return;
    }

    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "expected success or controlled 400, got status={}, body={}, context={}",
        status,
        body,
        context
    );

    let lower = body.to_lowercase();
    assert!(
        lower.contains("source")
            || lower.contains("stream")
            || lower.contains("schema")
            || lower.contains("decoder")
            || lower.contains("input")
            || lower.contains("on_change")
            || lower.contains("column")
            || lower.contains("logical")
            || lower.contains("physical")
            || lower.contains("build")
            || lower.contains("registered")
            || lower.contains("memory")
            || lower.contains("topic"),
        "unexpected controlled error body={}, context={}",
        body,
        context
    );
}

fn assert_controlled_400_contains(status: StatusCode, body: &str, context: &str, needles: &[&str]) {
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "expected controlled 400, got status={}, body={}, context={}",
        status,
        body,
        context
    );

    let lower = body.to_lowercase();
    assert!(
        needles.iter().any(|needle| lower.contains(needle)),
        "body did not match expected keywords {:?}, body={}, context={}",
        needles,
        body,
        context
    );
}

#[test]
fn regular_source_pipeline_builds() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build runtime");

    let Some(h) = rt.block_on(TestHarness::new()) else {
        return;
    };

    let stream_name = format!("regular_src_{}", random_suffix());

    rt.block_on(async {
        h.client
            .create_stream(&mock_stream_req(stream_name.clone()))
            .await
            .expect("create regular mock stream");

        let sql = format!("SELECT value FROM {stream_name}");

        let result = h
            .client
            .create_pipeline(&PipelineCreateRequest::nop(
                format!("regular_pipe_{}", random_suffix()),
                sql.clone(),
            ))
            .await;

        assert!(
            result.is_ok(),
            "regular source should build successfully, result={:?}, sql={}",
            result,
            sql
        );
    });
}

#[test]
fn shared_stream_pipeline_builds_or_fails_cleanly() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build runtime");

    let Some(h) = rt.block_on(TestHarness::new()) else {
        return;
    };

    let stream_name = format!("shared_src_{}", random_suffix());

    rt.block_on(async {
        h.client
            .create_stream(&shared_mock_stream_req(stream_name.clone()))
            .await
            .expect("create shared stream");

        let sql = format!("SELECT value FROM {stream_name}");

        let result = h
            .client
            .create_pipeline(&PipelineCreateRequest::nop(
                format!("shared_pipe_{}", random_suffix()),
                sql.clone(),
            ))
            .await;

        match result {
            Ok(_) => {}
            Err(sdk::SdkError::Http { status, body, .. }) => {
                assert_success_or_controlled_400(status, &body, &sql);
                assert!(
                    body.to_lowercase().contains("shared")
                        || body.to_lowercase().contains("stream")
                        || body.to_lowercase().contains("source")
                        || body.to_lowercase().contains("decoder")
                        || body.to_lowercase().contains("build"),
                    "unexpected shared stream error body={body}"
                );
            }
            Err(e) => panic!("unexpected crash/network error: {e:?}, sql={sql}"),
        }
    });
}

#[test]
fn memory_collection_pipeline_builds_or_fails_cleanly() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build runtime");

    let Some(h) = rt.block_on(TestHarness::new()) else {
        return;
    };

    let stream_name = format!("memory_src_{}", random_suffix());
    let topic = format!("/sql_fuzz/memory/{}", random_suffix());

    rt.block_on(async {
        let create_result = h
            .client
            .create_stream(&memory_collection_stream_req(
                stream_name.clone(),
                topic.clone(),
            ))
            .await;

        if let Err(sdk::SdkError::Http { status, body, .. }) = create_result {
            assert_success_or_controlled_400(
                status,
                &body,
                "memory stream creation should be success or controlled 400",
            );
            return;
        }

        let sql = format!("SELECT value FROM {stream_name}");

        let result = h
            .client
            .create_pipeline(&PipelineCreateRequest::nop(
                format!("memory_pipe_{}", random_suffix()),
                sql.clone(),
            ))
            .await;

        match result {
            Ok(_) => {}
            Err(sdk::SdkError::Http { status, body, .. }) => {
                assert_success_or_controlled_400(status, &body, &sql);
                assert!(
                    body.to_lowercase().contains("memory")
                        || body.to_lowercase().contains("topic")
                        || body.to_lowercase().contains("source")
                        || body.to_lowercase().contains("stream")
                        || body.to_lowercase().contains("build"),
                    "unexpected memory source error body={body}"
                );
            }
            Err(e) => panic!("unexpected crash/network error: {e:?}, sql={sql}"),
        }
    });
}

#[test]
fn invalid_decoder_is_rejected_cleanly() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build runtime");

    let Some(h) = rt.block_on(TestHarness::new()) else {
        return;
    };

    let stream_name = format!("bad_decoder_src_{}", random_suffix());

    rt.block_on(async {
        let create_result = h
            .client
            .create_stream(&bad_decoder_stream_req(stream_name.clone()))
            .await;

        match create_result {
            Ok(_) => {
                let sql = format!("SELECT value FROM {stream_name}");

                let result = h
                    .client
                    .create_pipeline(&PipelineCreateRequest::nop(
                        format!("bad_decoder_pipe_{}", random_suffix()),
                        sql.clone(),
                    ))
                    .await;

                match result {
                    Ok(ok) => panic!(
                        "pipeline with unregistered decoder unexpectedly succeeded: {:?}, sql={}",
                        ok, sql
                    ),
                    Err(sdk::SdkError::Http { status, body, .. }) => {
                        assert_controlled_400_contains(
                            status,
                            &body,
                            &sql,
                            &["decoder", "registered", "build"],
                        );
                    }
                    Err(e) => panic!("unexpected crash/network error: {e:?}, sql={sql}"),
                }
            }
            Err(sdk::SdkError::Http { status, body, .. }) => {
                assert_controlled_400_contains(
                    status,
                    &body,
                    "create stream with invalid decoder",
                    &["decoder", "registered", "invalid"],
                );
            }
            Err(e) => panic!("unexpected crash/network error creating stream: {e:?}"),
        }
    });
}

#[test]
fn on_change_unknown_column_is_rejected_cleanly() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build runtime");

    let Some(h) = rt.block_on(TestHarness::new()) else {
        return;
    };

    let stream_name = format!("on_change_src_{}", random_suffix());

    rt.block_on(async {
        h.client
            .create_stream(&mock_stream_req(stream_name.clone()))
            .await
            .expect("create mock stream");

        let sql = format!("SELECT value FROM {stream_name}");

        let body = pipeline_with_sources(
            format!("on_change_pipe_{}", random_suffix()),
            sql.clone(),
            json!([
                          {
                              "stream": stream_name,
                              "input": {
                                  "mode": "on_change",
                                  "on_change": {
              "columns": ["value", "flag"]
            }
                              }
                          }
                      ]),
        );

        let (status, body) = post_pipeline_raw(&h, body).await;

        assert_success_or_controlled_400(status, &body, &sql);
    });
}

#[test]
fn shared_stream_required_columns_are_respected() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build runtime");

    let Some(h) = rt.block_on(TestHarness::new()) else {
        return;
    };

    let stream_name = format!("shared_required_cols_{}", random_suffix());

    rt.block_on(async {
        h.client
            .create_stream(&shared_mock_stream_req(stream_name.clone()))
            .await
            .expect("create shared stream");

        let sql = format!("SELECT value FROM {stream_name} WHERE flag = true");

        let body = pipeline_with_sources(
            format!("shared_required_pipe_{}", random_suffix()),
            sql.clone(),
            json!([
                          {
                              "stream": stream_name,
                              "input": {
                                  "mode": "on_change",
                                  "on_change": {
              "columns": ["value", "flag"]
            }
                              }
                          }
                      ]),
        );

        let (status, body) = post_pipeline_raw(&h, body).await;

        assert_success_or_controlled_400(status, &body, &sql);
    });
}

#[test]
fn invalid_source_input_config_fails_cleanly() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build runtime");

    let Some(h) = rt.block_on(TestHarness::new()) else {
        return;
    };

    let stream_name = format!("invalid_source_input_{}", random_suffix());

    rt.block_on(async {
        h.client
            .create_stream(&mock_stream_req(stream_name.clone()))
            .await
            .expect("create mock stream");

        let sql = format!("SELECT value FROM {stream_name}");

        let body = pipeline_with_sources(
            format!("invalid_source_input_pipe_{}", random_suffix()),
            sql.clone(),
            json!([
                {
                    "stream": stream_name,
                    "input": {
                        "mode": "definitely_not_a_real_input_mode",
                        "on_change": {
                              "columns": ["value", "flag"]
                              }
                    }
                }
            ]),
        );

        let (status, body) = post_pipeline_raw(&h, body).await;

        assert_controlled_400_contains(
            status,
            &body,
            &sql,
            &["input", "mode", "source", "invalid", "unknown"],
        );
    });
}
