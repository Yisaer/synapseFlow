use super::{
    bind_manager_listener_or_skip, default_flow_instances, http_client, make_client, random_suffix,
    wait_for_server,
};
use reqwest::StatusCode;
use serde_json::{json, Value as JsonValue};
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
        "exported_at": 0,
        "resources": {
            "memory_topics": [],
            "shared_mqtt_clients": [],
            "streams": [],
            "pipelines": [],
            "pipeline_run_states": [],
            "udfs": []
        }
    })
}

fn bundle_single_stream_and_pipeline(stream_name: &str, pipeline_id: &str) -> JsonValue {
    json!({
        "exported_at": 0,
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
                    "sinks": [
                        { "type": "nop" }
                    ]
                }
            ],
            "pipeline_run_states": [
                {
                    "pipeline_id": pipeline_id,
                    "desired_state": "Stopped"
                }
            ],
            "udfs": []
        }
    })
}

async fn export_bundle(http: &HttpClient, base: &str) -> JsonValue {
    let resp = http
        .get(format!("{base}/storage/export"))
        .send()
        .await
        .expect("export request");
    let status = resp.status();
    let body = resp.bytes().await.unwrap_or_default();
    assert_eq!(status, StatusCode::OK, "export failed");
    metadata_json_from_tar_gz(&body).expect("decode export tar.gz")
}

fn metadata_json_from_tar_gz(data: &[u8]) -> Result<JsonValue, String> {
    let gz = flate2::read::GzDecoder::new(data);
    let mut archive = tar::Archive::new(gz);
    for entry in archive.entries().map_err(|e| format!("tar: {e}"))? {
        let mut entry = entry.map_err(|e| format!("entry: {e}"))?;
        if entry
            .path()
            .map_err(|e| format!("path: {e}"))?
            .to_string_lossy()
            == "metadata.json"
        {
            return serde_json::from_reader(&mut entry)
                .map_err(|e| format!("parse metadata.json: {e}"));
        }
    }
    Err("metadata.json not found in archive".to_string())
}

fn build_tar_gz_from_metadata(bundle: &JsonValue) -> Result<Vec<u8>, String> {
    let metadata_json = serde_json::to_vec(bundle).map_err(|e| format!("serialize: {e}"))?;
    let mut tar_gz = Vec::new();
    {
        let gz = flate2::write::GzEncoder::new(&mut tar_gz, flate2::Compression::default());
        let mut tar = tar::Builder::new(gz);
        let mut header = tar::Header::new_gnu();
        header.set_size(metadata_json.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        tar.append_data(&mut header, "metadata.json", metadata_json.as_slice())
            .map_err(|e| format!("write tar: {e}"))?;
        let gz = tar.into_inner().map_err(|e| format!("finish tar: {e}"))?;
        gz.finish().map_err(|e| format!("finish gzip: {e}"))?;
    }
    Ok(tar_gz)
}

async fn import_bundle(http: &HttpClient, base: &str, bundle: &JsonValue) -> (StatusCode, String) {
    let tar_gz = build_tar_gz_from_metadata(bundle).expect("build tar.gz");
    let resp = http
        .post(format!("{base}/import"))
        .header("content-type", "application/gzip")
        .body(tar_gz)
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
            "exported_at": 0
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
        "exported_at": 0,
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
            "pipeline_run_states": [],
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
        "exported_at": 0,
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
            "pipeline_run_states": [],
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
        "exported_at": 0,
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
            "pipeline_run_states": [],
            "udfs": []
        }
    });

    let (status, body) = import_bundle(&h.http, &h.base(), &bundle).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn import_rejects_dangling_pipeline_run_state_reference() {
    let Some(h) = ImportExportHarness::new().await else {
        return;
    };

    let bundle = json!({
        "exported_at": 0,
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
    assert_eq!(after["resources"]["pipeline_run_states"], json!([]));
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
        "exported_at": 0,
        "resources": {
            "memory_topics": [],
            "shared_mqtt_clients": [],
            "streams": [],
            "pipelines": [],
            "pipeline_run_states": [],
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
        "exported_at": 0,
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
            "pipeline_run_states": [],
            "udfs": []
        }
    });

    let (status2, body2) = import_bundle(&h.http, &h.base(), &bad_bundle).await;
    assert_eq!(status2, StatusCode::BAD_REQUEST, "body: {body2}");

    let mut after = export_bundle(&h.http, &h.base()).await;
    let mut expected = before;
    after["exported_at"] = serde_json::json!(0);
    expected["exported_at"] = serde_json::json!(0);
    assert_eq!(
        after, expected,
        "invalid import must preserve previous snapshot"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reimport_previous_bundle_restores_prior_snapshot() {
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

    let mut restored = export_bundle(&h.http, &h.base()).await;
    let mut expected = snapshot_a;
    restored["exported_at"] = serde_json::json!(0);
    expected["exported_at"] = serde_json::json!(0);
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
    assert_eq!(bundle["resources"]["pipeline_run_states"], json!([]));
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

    let mut export1 = export_bundle(&h.http, &h.base()).await;
    let mut export2 = export_bundle(&h.http, &h.base()).await;
    export1["exported_at"] = serde_json::json!(0);
    export2["exported_at"] = serde_json::json!(0);
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
        "exported_at": 0,
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
                    "sinks": [{ "type": "nop" }]
                },
                {
                    "id": "a_pipe",
                    "sql": "SELECT value FROM a_stream",
                    "sinks": [{ "type": "nop" }]
                }
            ],
            "pipeline_run_states": [
                { "pipeline_id": "z_pipe", "desired_state": "Stopped" },
                { "pipeline_id": "a_pipe", "desired_state": "Stopped" }
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
    let run_states = exported["resources"]["pipeline_run_states"]
        .as_array()
        .expect("run states array");

    assert_eq!(sorted_ids(streams, "name"), vec!["a_stream", "z_stream"]);
    assert_eq!(sorted_ids(pipelines, "id"), vec!["a_pipe", "z_pipe"]);
    assert_eq!(
        sorted_ids(run_states, "pipeline_id"),
        vec!["a_pipe", "z_pipe"]
    );
}
