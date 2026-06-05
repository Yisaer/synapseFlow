use proptest::prelude::*;
use reqwest::StatusCode;
use sdk::{ManagerClient, SdkError, StreamCreateRequest};
use serde_json::{json, Value as JsonValue};
use std::net::SocketAddr;

use super::{
    bind_manager_listener_or_skip, default_flow_instances, make_client, random_suffix,
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

        let http = reqwest::Client::builder()
            .no_proxy()
            .build()
            .expect("build reqwest client");

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

fn test_config(cases: u32) -> proptest::test_runner::Config {
    proptest::test_runner::Config {
        cases,
        failure_persistence: None,
        ..proptest::test_runner::Config::default()
    }
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

fn valid_schema_json() -> JsonValue {
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

fn invalid_struct_or_list_schema_json_cases() -> Vec<JsonValue> {
    vec![
        json!({
            "type": "json",
            "props": {
                "columns": [
                    {
                        "name": "bad_struct",
                        "data_type": {
                            "type": "struct",
                            "fields": [
                                { "data_type": "int64" }
                            ]
                        }
                    }
                ]
            }
        }),
        json!({
            "type": "json",
            "props": {
                "columns": [
                    {
                        "name": "bad_struct",
                        "data_type": {
                            "type": "struct",
                            "fields": "not_an_array"
                        }
                    }
                ]
            }
        }),
        json!({
            "type": "json",
            "props": {
                "columns": [
                    {
                        "name": "bad_list",
                        "data_type": {
                            "type": "list"
                        }
                    }
                ]
            }
        }),
        json!({
            "type": "json",
            "props": {
                "columns": [
                    {
                        "name": "bad_list",
                        "data_type": {
                            "type": "list",
                            "element_type": "definitely_not_a_type"
                        }
                    }
                ]
            }
        }),
    ]
}

fn invalid_data_type_schema_json_cases() -> Vec<JsonValue> {
    vec![
        json!({}),
        json!({
            "type": "json",
            "props": {
                "columns": [
                    { "name": "value" }
                ]
            }
        }),
        json!({
            "type": "json",
            "props": {
                "columns": [
                    { "data_type": "int64" }
                ]
            }
        }),
        json!({
            "type": "json",
            "props": {
                "columns": [
                    { "name": "value", "data_type": "definitely_not_a_type" }
                ]
            }
        }),
    ]
}

fn stream_req(
    name: String,
    stream_type: &str,
    schema: JsonValue,
    props: JsonValue,
) -> StreamCreateRequest {
    StreamCreateRequest {
        name,
        stream_type: stream_type.to_string(),
        schema,
        props,
        shared: false,
        decoder: json!({ "type": "json", "props": {} }),
    }
}

fn valid_mock_stream_req(name: String) -> StreamCreateRequest {
    stream_req(
        name,
        "mock",
        json!({
            "type": "json",
            "props": {
                "columns": [
                    { "name": "value", "data_type": "int64" }
                ]
            }
        }),
        json!({}),
    )
}

async fn post_pipeline_raw(h: &TestHarness, body: JsonValue) -> (StatusCode, String) {
    let resp = h
        .http
        .post(format!("{}/pipelines", h.base()))
        .json(&body)
        .send()
        .await
        .expect("send raw pipeline request");

    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if status.as_u16() == 422 {
        println!(
            "\n====== 422 PIPELINE ======\nSTATUS: {}\nBODY: {}\nREQUEST: {}\n===========================\n",
            status, text, body
        );
    }
    (status, text)
}

async fn export_bundle(h: &TestHarness) -> JsonValue {
    let resp = h
        .http
        .get(format!("{}/storage/export", h.base()))
        .send()
        .await
        .expect("export request");
    let status = resp.status();
    let body = resp.bytes().await.expect("export body");
    assert_eq!(status, reqwest::StatusCode::OK, "export failed");

    let gz = flate2::read::GzDecoder::new(body.as_ref());
    let mut archive = tar::Archive::new(gz);
    for entry in archive.entries().expect("tar entries") {
        let mut entry = entry.expect("tar entry");
        if entry.path().expect("entry path").to_string_lossy() == "metadata.json" {
            return serde_json::from_reader(&mut entry).expect("parse metadata.json");
        }
    }
    panic!("metadata.json not found in archive");
}

fn build_import_tar_gz(bundle: &JsonValue) -> Vec<u8> {
    let metadata_json = serde_json::to_vec(bundle).expect("serialize");
    let mut tar_gz = Vec::new();
    {
        let gz = flate2::write::GzEncoder::new(&mut tar_gz, flate2::Compression::default());
        let mut tar = tar::Builder::new(gz);
        let mut header = tar::Header::new_gnu();
        header.set_size(metadata_json.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        tar.append_data(&mut header, "metadata.json", metadata_json.as_slice())
            .expect("write tar");
        let gz = tar.into_inner().expect("finish tar");
        gz.finish().expect("finish gzip");
    }
    tar_gz
}

async fn import_bundle(
    h: &TestHarness,
    bundle: JsonValue,
) -> (StatusCode, String, Option<JsonValue>) {
    let body = build_import_tar_gz(&bundle);
    let resp = h
        .http
        .post(format!("{}/import", h.base()))
        .header("content-type", "application/gzip")
        .body(body)
        .send()
        .await
        .expect("import request");

    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    let json = serde_json::from_str::<JsonValue>(&text).ok();
    (status, text, json)
}

fn stream_resource(name: &str) -> JsonValue {
    json!({
        "name": name,
        "type": "mock",
        "schema": {
            "type": "json",
            "props": {
                "columns": [
                    { "name": "value", "data_type": "int64" }
                ]
            }
        },
        "props": {},
        "shared": false,
        "decoder": { "type": "json", "props": {} }
    })
}

fn pipeline_resource(id: &str, stream_name: &str) -> JsonValue {
    json!({
        "id": id,
        "sql": format!("SELECT value FROM {stream_name}"),
        "sinks": [
            {
                "type": "nop",
                "props": {},
                "encoder": { "type": "json", "props": {} }
            }
        ],
        "options": {
            "eventtime": {
                "enabled": false,
                "late_tolerance_ms": 0
            }
        }
    })
}

fn bundle_with_resources(
    streams: Vec<JsonValue>,
    pipelines: Vec<JsonValue>,
    run_states: Vec<JsonValue>,
) -> JsonValue {
    json!({
        "exported_at": 0,
        "resources": {
            "memory_topics": [],
            "shared_mqtt_clients": [],
            "streams": streams,
            "pipelines": pipelines,
            "pipeline_run_states": run_states,
            "udfs": []
        }
    })
}

#[test]
fn stream_schema_nested_struct_and_list_combinations() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build tokio runtime");

    let Some(h) = rt.block_on(TestHarness::new()) else {
        return;
    };

    rt.block_on(async {
        let name = format!("valid_nested_schema_{}", random_suffix());

        let result = h
            .client
            .create_stream(&stream_req(name, "mock", valid_schema_json(), json!({})))
            .await;

        assert!(
            result.is_ok(),
            "valid nested struct/list schema should be accepted, got {:?}",
            result.err()
        );
    });
}

#[test]
fn stream_schema_rejects_invalid_struct_and_list_shapes() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build tokio runtime");

    let Some(h) = rt.block_on(TestHarness::new()) else {
        return;
    };

    rt.block_on(async {
        for schema in invalid_struct_or_list_schema_json_cases() {
            let name = format!("invalid_nested_schema_{}", random_suffix());

            let err = h
                .client
                .create_stream(&stream_req(name, "mock", schema, json!({})))
                .await
                .expect_err("invalid struct/list schema should fail");

            let body = assert_http_status(err, 400);
            let lower = body.to_lowercase();

            assert!(
                lower.contains("schema")
                    || lower.contains("column")
                    || lower.contains("data_type")
                    || lower.contains("type")
                    || lower.contains("invalid")
                    || lower.contains("missing")
                    || lower.contains("list")
                    || lower.contains("struct"),
                "unexpected invalid struct/list schema error body: {body}"
            );
        }
    });
}

#[test]
fn stream_schema_rejects_invalid_data_type_combinations() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build tokio runtime");

    let Some(h) = rt.block_on(TestHarness::new()) else {
        return;
    };

    rt.block_on(async {
        for schema in invalid_data_type_schema_json_cases() {
            let name = format!("invalid_dtype_schema_{}", random_suffix());

            let err = h
                .client
                .create_stream(&stream_req(name, "mock", schema, json!({})))
                .await
                .expect_err("invalid data type schema should fail");

            let body = assert_http_status(err, 400);
            let lower = body.to_lowercase();

            assert!(
                lower.contains("schema")
                    || lower.contains("column")
                    || lower.contains("data_type")
                    || lower.contains("type")
                    || lower.contains("invalid")
                    || lower.contains("missing"),
                "unexpected invalid data type error body: {body}"
            );
        }
    });
}

#[test]
fn stream_type_props_consistency_for_mqtt_and_history() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build tokio runtime");

    let Some(h) = rt.block_on(TestHarness::new()) else {
        return;
    };

    let cases = vec![
        ("mqtt", json!({})),
        ("mqtt", json!({ "topic": "/topic" })),
        ("mqtt", json!({ "broker_url": "tcp://127.0.0.1:1883" })),
        ("history", json!({})),
    ];

    rt.block_on(async {
        for (stream_type, props) in cases {
            let name = format!("stream_bad_props_{}", random_suffix());

            let err = h
                .client
                .create_stream(&stream_req(
                    name,
                    stream_type,
                    json!({
                        "type": "json",
                        "props": {
                            "columns": [
                                { "name": "value", "data_type": "int64" }
                            ]
                        }
                    }),
                    props,
                ))
                .await
                .expect_err("invalid stream type props should fail");

            let body = assert_http_status(err, 400);
            let lower = body.to_lowercase();

            assert!(
                lower.contains("broker")
                    || lower.contains("topic")
                    || lower.contains("props")
                    || lower.contains("type")
                    || lower.contains("missing")
                    || lower.contains("stream"),
                "unexpected stream type props error body: {body}"
            );
        }
    });
}

fn sink_json_case() -> impl Strategy<Value = (JsonValue, bool)> {
    prop_oneof![
        Just((
            json!({
                "type": "nop",
                "props": {},
                "encoder": { "type": "json", "props": {} }
            }),
            true,
        )),
        Just((
            json!({
                "type": "mqtt",
                "props": {},
                "encoder": { "type": "json", "props": {} }
            }),
            false,
        )),
        Just((
            json!({
                "type": "mqtt",
                "props": { "topic": "/x" },
                "encoder": { "type": "json", "props": {} }
            }),
            false,
        )),
        Just((
            json!({
                "type": "not_a_real_sink",
                "props": {},
                "encoder": { "type": "json", "props": {} }
            }),
            false,
        )),
        Just((
            json!({
                "type": "kuksa",
                "props": { "addr": "127.0.0.1:55555" },
                "encoder": { "type": "json", "props": {} }
            }),
            false,
        )),
    ]
}

#[test]
fn proptest_pipeline_sink_type_specific_props_validation() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build tokio runtime");

    let Some(h) = rt.block_on(TestHarness::new()) else {
        return;
    };

    let stream_name = format!("pipe_sink_stream_{}", random_suffix());

    rt.block_on(async {
        h.client
            .create_stream(&valid_mock_stream_req(stream_name.clone()))
            .await
            .expect("create stream");
    });

    let mut runner = proptest::test_runner::TestRunner::new(test_config(32));

    runner
        .run(&sink_json_case(), |(sink, should_succeed)| {
            rt.block_on(async {
                let pipeline_id = format!("pipe_sink_{}", random_suffix());

                let body = json!({
                    "id": pipeline_id,
                    "sql": format!("SELECT value FROM {stream_name}"),
                    "sinks": [sink],
                });

                let (status, text) = post_pipeline_raw(&h, body).await;

                if should_succeed {
                    prop_assert!(
                        status.is_success(),
                        "expected valid sink to succeed, got status={}, body={}",
                        status,
                        text
                    );
                } else {
                    prop_assert_eq!(
                        status.as_u16(),
                        400,
                        "expected invalid sink to fail, got status={}, body={}",
                        status,
                        text
                    );
                }

                Ok(())
            })
        })
        .expect("pipeline sink props fuzz failed");
}

#[test]
fn pipeline_encoder_shapes_are_validated() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build tokio runtime");

    let Some(h) = rt.block_on(TestHarness::new()) else {
        return;
    };

    let stream_name = format!("pipe_encoder_options_stream_{}", random_suffix());

    rt.block_on(async {
        h.client
            .create_stream(&valid_mock_stream_req(stream_name.clone()))
            .await
            .expect("create stream");

        let cases = vec![json!({
            "id": format!("pipe_encoder_{}", random_suffix()),
            "sql": format!("SELECT value FROM {stream_name}"),
            "sinks": [
                {
                    "type": "nop",
                    "props": {},
                    "encoder": {
                        "type": "custom_encoder",
                        "props": {},
                        "transform": {
                            "template": "{\"x\":{{ json(.row.value) }} }"
                        }
                    }
                }
            ]
        })];

        for body in cases {
            let (status, text) = post_pipeline_raw(&h, body).await;

            assert_eq!(
                status.as_u16(),
                400,
                "expected bad encoder to fail, got status={}, body={}",
                status,
                text
            );
        }
    });
}

#[test]
fn invalid_import_never_mutates_existing_storage() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build tokio runtime");

    let Some(h) = rt.block_on(TestHarness::new()) else {
        return;
    };

    rt.block_on(async {
        h.client
            .create_stream(&valid_mock_stream_req("baseline_stream".to_string()))
            .await
            .expect("create baseline stream");

        let before = export_bundle(&h).await;

        let cases = vec![
            bundle_with_resources(
                vec![],
                vec![pipeline_resource("bad_pipeline", "missing_stream")],
                vec![],
            ),
            bundle_with_resources(
                vec![],
                vec![],
                vec![json!({
                    "pipeline_id": "missing_pipeline",
                    "desired_state": "Running"
                })],
            ),
            bundle_with_resources(vec![stream_resource("")], vec![], vec![]),
        ];

        for bundle in cases {
            let (status, body, _) = import_bundle(&h, bundle).await;

            assert_eq!(
                status.as_u16(),
                400,
                "invalid import should return 400, got status={}, body={}",
                status,
                body
            );

            let after = export_bundle(&h).await;

            assert_eq!(
                before["resources"], after["resources"],
                "invalid import should not mutate storage"
            );
        }
    });
}

#[test]
fn full_replace_removes_resources_missing_from_new_bundle() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build tokio runtime");

    let Some(h) = rt.block_on(TestHarness::new()) else {
        return;
    };

    let keep = format!("keep_stream_{}", random_suffix());
    let remove = format!("remove_stream_{}", random_suffix());

    rt.block_on(async {
        h.client
            .create_stream(&valid_mock_stream_req(keep.clone()))
            .await
            .expect("create keep stream");
        h.client
            .create_stream(&valid_mock_stream_req(remove.clone()))
            .await
            .expect("create remove stream");

        let bundle = bundle_with_resources(vec![stream_resource(&keep)], vec![], vec![]);
        let (status, body, _json) = import_bundle(&h, bundle).await;

        assert_eq!(status.as_u16(), 200, "import body={body}");

        let after = export_bundle(&h).await;
        let streams = after["resources"]["streams"]
            .as_array()
            .expect("streams array");

        let names = streams
            .iter()
            .filter_map(|s| s["name"].as_str())
            .collect::<Vec<_>>();

        assert!(
            names.contains(&keep.as_str()),
            "keep stream missing: {after}"
        );
        assert!(
            !names.contains(&remove.as_str()),
            "remove stream should be removed by full replace: {after}"
        );
    });
}

#[test]
fn previous_bundle_round_trip_restores_prior_state() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build tokio runtime");

    let Some(h) = rt.block_on(TestHarness::new()) else {
        return;
    };

    rt.block_on(async {
        h.client
            .create_stream(&valid_mock_stream_req("rollback_before".to_string()))
            .await
            .expect("create rollback baseline");

        let before = export_bundle(&h).await;

        let replacement =
            bundle_with_resources(vec![stream_resource("rollback_after")], vec![], vec![]);

        let (status, body, response_json) = import_bundle(&h, replacement).await;
        assert_eq!(status.as_u16(), 200, "import body={body}");

        let previous_bundle = response_json
            .as_ref()
            .and_then(|v| v.get("previous_bundle"))
            .cloned()
            .expect("import response should contain previous_bundle");

        let (rollback_status, rollback_body, _rollback_json) =
            import_bundle(&h, previous_bundle).await;

        assert_eq!(
            rollback_status.as_u16(),
            200,
            "rollback import body={rollback_body}"
        );

        let after = export_bundle(&h).await;
        assert_eq!(
            before["resources"], after["resources"],
            "previous_bundle rollback should restore state"
        );
    });
}

#[test]
fn import_rejects_pipeline_referencing_missing_stream() {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let Some(h) = rt.block_on(TestHarness::new()) else {
        return;
    };

    let bundle = bundle_with_resources(
        vec![stream_resource("valid_stream")],
        vec![pipeline_resource("bad_pipeline", "missing_stream")],
        vec![],
    );

    rt.block_on(async {
        let (status, body, _) = import_bundle(&h, bundle.clone()).await;

        println!("\n=== PIPELINE MISSING STREAM TEST ===");
        println!("STATUS: {}", status);
        println!("BODY: {}", body);
        println!("BUNDLE: {}", serde_json::to_string_pretty(&bundle).unwrap());

        assert_eq!(
            status.as_u16(),
            400,
            "pipeline referencing missing stream should be rejected"
        );
    });
}

#[test]
fn import_rejects_partially_valid_resource_bundles_atomically() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build tokio runtime");

    let Some(h) = rt.block_on(TestHarness::new()) else {
        return;
    };

    rt.block_on(async {
        h.client
            .create_stream(&valid_mock_stream_req("partial_baseline".to_string()))
            .await
            .expect("create partial baseline");

        let before = export_bundle(&h).await;

        let cases = vec![
            bundle_with_resources(
                vec![stream_resource("valid_stream"), stream_resource("")],
                vec![],
                vec![],
            ),
            bundle_with_resources(
                vec![stream_resource("valid_stream")],
                vec![pipeline_resource("bad_pipeline", "missing_stream")],
                vec![],
            ),
            bundle_with_resources(
                vec![stream_resource("valid_stream")],
                vec![],
                vec![json!({ "pipeline_id": "missing_pipeline", "desired_state": "Running" })],
            ),
        ];

        for bundle in cases {
            let (status, body, _json) = import_bundle(&h, bundle).await;

            assert_eq!(
                status.as_u16(),
                400,
                "partially valid import bundle should fail, got status={}, body={}",
                status,
                body
            );

            let after = export_bundle(&h).await;

            assert_eq!(
                before, after,
                "partially valid import should not mutate storage"
            );
        }
    });
}

#[test]
fn export_is_deterministic_under_repeated_reads() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build tokio runtime");

    let Some(h) = rt.block_on(TestHarness::new()) else {
        return;
    };

    rt.block_on(async {
        h.client
            .create_stream(&valid_mock_stream_req("deterministic_stream".to_string()))
            .await
            .expect("create stream");

        let first = export_bundle(&h).await;
        let second = export_bundle(&h).await;

        assert_eq!(
            first["resources"], second["resources"],
            "export resources should be deterministic"
        );
    });
}

#[test]
fn proptest_export_arrays_remain_sorted_under_randomized_insert_order() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build tokio runtime");

    let Some(h) = rt.block_on(TestHarness::new()) else {
        return;
    };

    let strategy = prop::collection::vec("[a-z]{1,8}", 3..8);

    let mut runner = proptest::test_runner::TestRunner::new(test_config(16));

    runner
        .run(&strategy, |names| {
            rt.block_on(async {
                let mut unique = names
                    .into_iter()
                    .map(|n| format!("sorted_{}_{}", n, random_suffix()))
                    .collect::<Vec<_>>();

                unique.sort();
                unique.dedup();

                for name in unique.iter().rev() {
                    h.client
                        .create_stream(&valid_mock_stream_req(name.clone()))
                        .await
                        .expect("create stream");
                }

                let exported = export_bundle(&h).await;
                let streams = exported["resources"]["streams"]
                    .as_array()
                    .expect("streams array");

                let exported_names = streams
                    .iter()
                    .filter_map(|s| s["name"].as_str())
                    .collect::<Vec<_>>();

                let mut sorted_names = exported_names.clone();
                sorted_names.sort();

                prop_assert_eq!(
                    exported_names,
                    sorted_names,
                    "exported streams should be sorted by stable identifier"
                );

                Ok(())
            })
        })
        .expect("export sorting fuzz failed");
}
