use super::{
    bind_manager_listener_or_skip, default_flow_instances, http_client, make_client, random_suffix,
    wait_for_server,
};
use reqwest::StatusCode;
use serde_json::{json, Value as JsonValue};
use std::io::{Cursor, Read, Write};
use std::net::SocketAddr;

struct ImportExportHarness {
    _temp_dir: tempfile::TempDir,
    server: tokio::task::JoinHandle<()>,
    addr: SocketAddr,
    http: reqwest::Client,
}

impl ImportExportHarness {
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

        let http = http_client();
        let client = make_client(addr);

        wait_for_server(&client).await;

        Some(Self {
            _temp_dir: temp_dir,
            server,
            addr,
            http,
        })
    }

    fn base(&self) -> String {
        format!("http://{}", self.addr)
    }
}

impl Drop for ImportExportHarness {
    fn drop(&mut self) {
        self.server.abort();
    }
}

fn bundle_empty() -> JsonValue {
    json!({
        "format_version": 1,
        "bundle_version": "test-bundle-1",
        "resources": {
            "memory_topics": [],
            "shared_mqtt_clients": [],
            "streams": [],
            "pipelines": [],
            "udfs": []
        }
    })
}

fn bundle_single_stream_and_pipeline(stream_name: &str, pipeline_id: &str) -> JsonValue {
    json!({
        "format_version": 1,
        "bundle_version": "test-bundle-1",
        "resources": {
            "memory_topics": [],
            "shared_mqtt_clients": [],
            "streams": [
                {
                    "name": stream_name,
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
                }
            ],
            "pipelines": [
                {
                    "id": pipeline_id,
                    "sql": format!("SELECT value FROM {stream_name}"),
                    "run_state": "Stopped",
                    "sinks": [
                        { "type": "nop" }
                    ]
                }
            ],
            "udfs": []
        }
    })
}

async fn export_bundle(http: &reqwest::Client, base: &str) -> JsonValue {
    let resp = http
        .get(format!(
            "{base}/storage/export?bundle_version=test-bundle-1"
        ))
        .send()
        .await
        .expect("export request");
    let status = resp.status();
    let body = resp.bytes().await.unwrap_or_default();
    assert_eq!(status, StatusCode::OK, "export failed");
    metadata_json_from_zip(&body).expect("decode export ZIP")
}

fn metadata_json_from_zip(data: &[u8]) -> Result<JsonValue, String> {
    let mut archive =
        zip::ZipArchive::new(Cursor::new(data)).map_err(|e| format!("open ZIP: {e}"))?;
    for index in 0..archive.len() {
        let mut entry = archive
            .by_index(index)
            .map_err(|e| format!("read ZIP entry: {e}"))?;
        if entry.name() == "manifest.json" {
            let mut metadata = Vec::new();
            entry
                .read_to_end(&mut metadata)
                .map_err(|e| format!("read manifest.json: {e}"))?;
            return serde_json::from_slice(&metadata)
                .map_err(|e| format!("parse manifest.json: {e}"));
        }
    }
    Err("manifest.json not found in archive".to_string())
}

fn build_zip_from_metadata(bundle: &JsonValue) -> Result<Vec<u8>, String> {
    let metadata_json = serde_json::to_vec(bundle).map_err(|e| format!("serialize: {e}"))?;
    let mut archive = zip::ZipWriter::new(Cursor::new(Vec::new()));
    archive
        .start_file(
            "manifest.json",
            zip::write::SimpleFileOptions::default()
                .compression_method(zip::CompressionMethod::Deflated),
        )
        .map_err(|e| format!("start manifest.json: {e}"))?;
    archive
        .write_all(&metadata_json)
        .map_err(|e| format!("write manifest.json: {e}"))?;
    archive
        .finish()
        .map(Cursor::into_inner)
        .map_err(|e| format!("finish ZIP: {e}"))
}

async fn import_bundle(
    http: &reqwest::Client,
    base: &str,
    bundle: &JsonValue,
) -> (StatusCode, String) {
    let zip_bytes = build_zip_from_metadata(bundle).expect("build ZIP");
    let resp = http
        .post(format!("{base}/import"))
        .header("content-type", "application/zip")
        .body(zip_bytes)
        .send()
        .await
        .expect("import request");

    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    (status, body)
}

fn sorted_ids(values: &[JsonValue], key: &str) -> Vec<String> {
    values
        .iter()
        .map(|v| {
            v[key]
                .as_str()
                .unwrap_or_else(|| panic!("missing string field {key} in {v}"))
                .to_string()
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn import_rejects_missing_resources() {
    let Some(h) = ImportExportHarness::new().await else {
        return;
    };

    let (status, body) = import_bundle(
        &h.http,
        &h.base(),
        &json!({
            "format_version": 1,
        "bundle_version": "test-bundle-1",
        }),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
    assert!(body.contains("missing field `resources`"), "body: {body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn import_rejects_invalid_json_shape() {
    let Some(h) = ImportExportHarness::new().await else {
        return;
    };

    let resp = h
        .http
        .post(format!("{}/import", h.base()))
        .body("{ definitely invalid json")
        .header("content-type", "application/json")
        .send()
        .await
        .expect("import request");

    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn import_rejects_duplicate_identifiers() {
    let Some(h) = ImportExportHarness::new().await else {
        return;
    };

    let stream_name = format!("dup_stream_{}", random_suffix());
    let pipeline_id = format!("dup_pipe_{}", random_suffix());

    let bundle = json!({
        "format_version": 1,
        "bundle_version": "test-bundle-1",
        "resources": {
            "memory_topics": [],
            "shared_mqtt_clients": [],
            "streams": [
                {
                    "name": stream_name,
                    "type": "mock",
                    "schema": {
                        "type": "json",
                        "props": { "columns": [ { "name": "value", "data_type": "int64" } ] }
                    },
                    "props": {},
                    "shared": false,
                    "decoder": { "type": "json", "props": {} }
                },
                {
                    "name": stream_name,
                    "type": "mock",
                    "schema": {
                        "type": "json",
                        "props": { "columns": [ { "name": "value", "data_type": "int64" } ] }
                    },
                    "props": {},
                    "shared": false,
                    "decoder": { "type": "json", "props": {} }
                }
            ],
            "pipelines": [
                {
                    "id": pipeline_id,
                    "sql": format!("SELECT value FROM {stream_name}"),
                    "sinks": [{ "type": "nop" }]
                }
            ],
            "udfs": []
        }
    });

    let (status, body) = import_bundle(&h.http, &h.base(), &bundle).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn import_rejects_invalid_resources() {
    let Some(h) = ImportExportHarness::new().await else {
        return;
    };

    let bundle = json!({
        "format_version": 1,
        "bundle_version": "test-bundle-1",
        "resources": {
            "memory_topics": [],
            "shared_mqtt_clients": [],
            "streams": [
                {
                    "name": "",
                    "type": "mock",
                    "schema": {
                        "type": "json",
                        "props": { "columns": [] }
                    },
                    "props": {},
                    "shared": false,
                    "decoder": { "type": "json", "props": {} }
                }
            ],
            "pipelines": [],
            "udfs": []
        }
    });

    let (status, body) = import_bundle(&h.http, &h.base(), &bundle).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn import_rejects_empty_name_and_zero_capacity() {
    let Some(h) = ImportExportHarness::new().await else {
        return;
    };

    let bundle = json!({
        "format_version": 1,
        "bundle_version": "test-bundle-1",
        "resources": {
            "memory_topics": [
                {
                    "topic": "",
                    "kind": "bytes",
                    "capacity": 0
                }
            ],
            "shared_mqtt_clients": [],
            "streams": [],
            "pipelines": [],
            "udfs": []
        }
    });

    let (status, body) = import_bundle(&h.http, &h.base(), &bundle).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn import_rejects_legacy_pipeline_run_states() {
    let Some(h) = ImportExportHarness::new().await else {
        return;
    };

    let bundle = json!({
        "format_version": 1,
        "bundle_version": "test-bundle-1",
        "resources": {
            "memory_topics": [],
            "shared_mqtt_clients": [],
            "streams": [],
            "pipelines": [],
            "pipeline_run_states": [
                {
                    "pipeline_id": "no_such_pipeline",
                    "desired_state": "Running"
                }
            ],
            "udfs": []
        }
    });

    let (status, body) = import_bundle(&h.http, &h.base(), &bundle).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
    assert!(
        body.contains("unknown field `pipeline_run_states`"),
        "body: {body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn import_empty_bundle_replaces_existing_snapshot() {
    let Some(h) = ImportExportHarness::new().await else {
        return;
    };

    let stream_name = format!("replace_empty_stream_{}", random_suffix());
    let pipeline_id = format!("replace_empty_pipe_{}", random_suffix());

    let bundle = bundle_single_stream_and_pipeline(&stream_name, &pipeline_id);
    let (status, body) = import_bundle(&h.http, &h.base(), &bundle).await;
    assert!(status.is_success(), "body: {body}");

    let before = export_bundle(&h.http, &h.base()).await;
    assert!(
        before["resources"]["streams"]
            .as_array()
            .is_some_and(|xs| !xs.is_empty()),
        "expected populated storage before empty import: {before}"
    );

    let (status2, body2) = import_bundle(&h.http, &h.base(), &bundle_empty()).await;
    assert!(status2.is_success(), "body: {body2}");

    let after = export_bundle(&h.http, &h.base()).await;
    assert_eq!(after["resources"]["streams"], json!([]));
    assert_eq!(after["resources"]["pipelines"], json!([]));
    assert!(after["resources"].get("pipeline_run_states").is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn import_full_replace_removes_missing_resources() {
    let Some(h) = ImportExportHarness::new().await else {
        return;
    };

    let stream_a = format!("replace_stream_a_{}", random_suffix());
    let pipe_a = format!("replace_pipe_a_{}", random_suffix());
    let full_bundle = bundle_single_stream_and_pipeline(&stream_a, &pipe_a);

    let (status1, body1) = import_bundle(&h.http, &h.base(), &full_bundle).await;
    assert!(status1.is_success(), "body: {body1}");

    let reduced_bundle = json!({
        "format_version": 1,
        "bundle_version": "test-bundle-1",
        "resources": {
            "memory_topics": [],
            "shared_mqtt_clients": [],
            "streams": [],
            "pipelines": [],
            "udfs": []
        }
    });

    let (status2, body2) = import_bundle(&h.http, &h.base(), &reduced_bundle).await;
    assert!(status2.is_success(), "body: {body2}");

    let exported = export_bundle(&h.http, &h.base()).await;
    assert_eq!(exported["resources"]["streams"], json!([]));
    assert_eq!(exported["resources"]["pipelines"], json!([]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn import_invalid_bundle_after_valid_import_preserves_previous_state() {
    let Some(h) = ImportExportHarness::new().await else {
        return;
    };

    let stream_name = format!("preserve_prev_stream_{}", random_suffix());
    let pipeline_id = format!("preserve_prev_pipe_{}", random_suffix());
    let bundle = bundle_single_stream_and_pipeline(&stream_name, &pipeline_id);

    let (status1, body1) = import_bundle(&h.http, &h.base(), &bundle).await;
    assert!(status1.is_success(), "body: {body1}");

    let before = export_bundle(&h.http, &h.base()).await;

    let bad_bundle = json!({
        "format_version": 1,
        "bundle_version": "test-bundle-1",
        "resources": {
            "memory_topics": [],
            "shared_mqtt_clients": [],
            "streams": [
                {
                    "name": "",
                    "type": "mock",
                    "schema": {
                        "type": "json",
                        "props": { "columns": [] }
                    },
                    "props": {},
                    "shared": false,
                    "decoder": { "type": "json", "props": {} }
                }
            ],
            "pipelines": [],
            "udfs": []
        }
    });

    let (status2, body2) = import_bundle(&h.http, &h.base(), &bad_bundle).await;
    assert_eq!(status2, StatusCode::BAD_REQUEST, "body: {body2}");

    let after = export_bundle(&h.http, &h.base()).await;
    let expected = before;
    assert_eq!(
        after, expected,
        "invalid import must preserve previous snapshot"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reimport_exported_snapshot_restores_prior_snapshot() {
    let Some(h) = ImportExportHarness::new().await else {
        return;
    };

    let stream_a = format!("restore_a_stream_{}", random_suffix());
    let pipe_a = format!("restore_a_pipe_{}", random_suffix());
    let bundle_a = bundle_single_stream_and_pipeline(&stream_a, &pipe_a);

    let stream_b = format!("restore_b_stream_{}", random_suffix());
    let pipe_b = format!("restore_b_pipe_{}", random_suffix());
    let bundle_b = bundle_single_stream_and_pipeline(&stream_b, &pipe_b);

    let (status1, body1) = import_bundle(&h.http, &h.base(), &bundle_a).await;
    assert!(status1.is_success(), "body: {body1}");

    let snapshot_a = export_bundle(&h.http, &h.base()).await;

    let (status2, body2) = import_bundle(&h.http, &h.base(), &bundle_b).await;
    assert!(status2.is_success(), "body: {body2}");

    let snapshot_b = export_bundle(&h.http, &h.base()).await;
    assert_ne!(snapshot_a, snapshot_b, "bundle_b should replace bundle_a");

    let (status3, body3) = import_bundle(&h.http, &h.base(), &snapshot_a).await;
    assert!(status3.is_success(), "body: {body3}");

    let restored = export_bundle(&h.http, &h.base()).await;
    let expected = snapshot_a;
    assert_eq!(restored, expected, "reimport should restore prior snapshot");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn export_empty_storage_returns_empty_bundle() {
    let Some(h) = ImportExportHarness::new().await else {
        return;
    };

    let bundle = export_bundle(&h.http, &h.base()).await;
    assert_eq!(bundle["resources"]["streams"], json!([]));
    assert_eq!(bundle["resources"]["pipelines"], json!([]));
    assert!(bundle["resources"].get("pipeline_run_states").is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn export_populated_storage_returns_expected_snapshot() {
    let Some(h) = ImportExportHarness::new().await else {
        return;
    };

    let stream_name = format!("export_stream_{}", random_suffix());
    let pipeline_id = format!("export_pipe_{}", random_suffix());
    let bundle = bundle_single_stream_and_pipeline(&stream_name, &pipeline_id);

    let (status, body) = import_bundle(&h.http, &h.base(), &bundle).await;
    assert!(status.is_success(), "body: {body}");

    let exported = export_bundle(&h.http, &h.base()).await;

    let streams = exported["resources"]["streams"]
        .as_array()
        .expect("streams array");
    let pipelines = exported["resources"]["pipelines"]
        .as_array()
        .expect("pipelines array");

    assert!(
        streams.iter().any(|s| s["name"] == stream_name),
        "exported={exported}"
    );
    assert!(
        pipelines.iter().any(|p| p["id"] == pipeline_id),
        "exported={exported}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn export_repeated_without_changes_is_identical() {
    let Some(h) = ImportExportHarness::new().await else {
        return;
    };

    let stream_name = format!("repeat_stream_{}", random_suffix());
    let pipeline_id = format!("repeat_pipe_{}", random_suffix());
    let bundle = bundle_single_stream_and_pipeline(&stream_name, &pipeline_id);

    let (status, body) = import_bundle(&h.http, &h.base(), &bundle).await;
    assert!(status.is_success(), "body: {body}");

    let export1 = export_bundle(&h.http, &h.base()).await;
    let export2 = export_bundle(&h.http, &h.base()).await;
    assert_eq!(
        export1, export2,
        "repeated export should be byte-equivalent in JSON value form"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn export_arrays_are_sorted_by_stable_identifiers() {
    let Some(h) = ImportExportHarness::new().await else {
        return;
    };

    let bundle = json!({
        "format_version": 1,
        "bundle_version": "test-bundle-1",
        "resources": {
            "memory_topics": [],
            "shared_mqtt_clients": [],
            "streams": [
                {
                    "name": "z_stream",
                    "type": "mock",
                    "schema": {
                        "type": "json",
                        "props": { "columns": [ { "name": "value", "data_type": "int64" } ] }
                    },
                    "props": {},
                    "shared": false,
                    "decoder": { "type": "json", "props": {} }
                },
                {
                    "name": "a_stream",
                    "type": "mock",
                    "schema": {
                        "type": "json",
                        "props": { "columns": [ { "name": "value", "data_type": "int64" } ] }
                    },
                    "props": {},
                    "shared": false,
                    "decoder": { "type": "json", "props": {} }
                }
            ],
            "pipelines": [
                {
                    "id": "z_pipe",
                    "sql": "SELECT value FROM z_stream",
                    "run_state": "Stopped",
                    "sinks": [{ "type": "nop" }]
                },
                {
                    "id": "a_pipe",
                    "sql": "SELECT value FROM a_stream",
                    "run_state": "Stopped",
                    "sinks": [{ "type": "nop" }]
                }
            ],
            "udfs": []
        }
    });

    let (status, body) = import_bundle(&h.http, &h.base(), &bundle).await;
    assert!(status.is_success(), "body: {body}");

    let exported = export_bundle(&h.http, &h.base()).await;

    let streams = exported["resources"]["streams"]
        .as_array()
        .expect("streams array");
    let pipelines = exported["resources"]["pipelines"]
        .as_array()
        .expect("pipelines array");
    assert_eq!(sorted_ids(streams, "name"), vec!["a_stream", "z_stream"]);
    assert_eq!(sorted_ids(pipelines, "id"), vec!["a_pipe", "z_pipe"]);
    assert_eq!(pipelines[0]["run_state"], "Stopped");
    assert_eq!(pipelines[1]["run_state"], "Stopped");
}
