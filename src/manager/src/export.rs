use axum::{
    extract::{Query, State},
    http::{HeaderValue, StatusCode, header},
    response::IntoResponse,
};
use flow::connector::SharedMqttClientConfig;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::{Cursor, Write};
use storage::{StorageManager, StoredMemoryTopicKind, StoredPipelineDesiredState};
use tokio::sync::TryAcquireError;
use zip::write::SimpleFileOptions;

use crate::pipeline::{AppState, CreatePipelineRequest};
use crate::storage_bridge;
use crate::stream::CreateStreamRequest;

pub(crate) const RESOURCE_DIRECTORY_FORMAT_VERSION: u32 = 1;

#[derive(Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct ResourceManifestV1 {
    pub format_version: u32,
    pub bundle_version: String,
    pub resources: ExportResources,
}

#[derive(Deserialize)]
pub struct ExportStorageQuery {
    bundle_version: String,
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct ExportResources {
    pub memory_topics: Vec<ExportMemoryTopic>,
    pub shared_mqtt_clients: Vec<SharedMqttClientConfig>,
    #[serde(default)]
    pub schemas: Vec<ExportSchema>,
    pub streams: Vec<CreateStreamRequest>,
    pub pipelines: Vec<ExportPipeline>,
    pub udfs: Vec<ExportUdf>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ExportMemoryTopic {
    pub topic: String,
    pub kind: StoredMemoryTopicKind,
    pub capacity: usize,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ExportPipeline {
    #[serde(flatten)]
    pub definition: CreatePipelineRequest,
    #[serde(default = "default_pipeline_run_state")]
    pub run_state: StoredPipelineDesiredState,
}

fn default_pipeline_run_state() -> StoredPipelineDesiredState {
    StoredPipelineDesiredState::Stopped
}

/// UDF metadata included in the resource manifest. The actual `.wasm` binary is
/// stored under `wasm_files/` in the resource directory, keyed by SHA-256.
#[derive(Serialize, Deserialize, Clone)]
pub struct ExportUdf {
    pub name: String,
    pub wasm_sha256: String,
}

/// Schema metadata included in the export bundle.
#[derive(Serialize, Deserialize, Clone)]
pub struct ExportSchema {
    pub name: String,
    #[serde(rename = "type")]
    pub schema_type: String,
    pub props: serde_json::Map<String, serde_json::Value>,
}

pub async fn export_storage_handler(
    State(state): State<AppState>,
    Query(query): Query<ExportStorageQuery>,
) -> impl IntoResponse {
    let _import_export_permit = match state.try_acquire_import_export_op() {
        Ok(permit) => permit,
        Err(TryAcquireError::NoPermits) => {
            return (
                StatusCode::CONFLICT,
                "another import/export command is in progress".to_string(),
            )
                .into_response();
        }
        Err(TryAcquireError::Closed) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "import/export operation guard closed".to_string(),
            )
                .into_response();
        }
    };

    if let Err(err) = validate_bundle_version(&query.bundle_version) {
        return (StatusCode::BAD_REQUEST, err).into_response();
    }

    let manifest = match build_resource_manifest(
        state.storage.as_ref(),
        query.bundle_version.trim().to_string(),
    ) {
        Ok(bundle) => bundle,
        Err(err) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, err).into_response();
        }
    };

    let udf_shas: Vec<String> = manifest
        .resources
        .udfs
        .iter()
        .map(|u| u.wasm_sha256.clone())
        .collect();
    let wasm_dir = state.storage.wasm_files_dir();
    let schemas_dir = state.storage.schemas_dir();

    let zip = match build_zip(&manifest, &udf_shas, &wasm_dir, &schemas_dir) {
        Ok(data) => data,
        Err(err) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, err).into_response();
        }
    };

    let filename = "veloflux-export.zip";
    let disposition = format!("attachment; filename=\"{filename}\"");
    let disposition = match HeaderValue::from_str(&disposition) {
        Ok(value) => value,
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to build export response header: {err}"),
            )
                .into_response();
        }
    };

    (
        [
            (header::CONTENT_DISPOSITION, disposition),
            (
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/zip"),
            ),
        ],
        zip,
    )
        .into_response()
}

fn add_directory_to_zip<W: Write + std::io::Seek>(
    zip: &mut zip::ZipWriter<W>,
    dir: &std::path::Path,
    zip_prefix: &str,
) -> Result<(), String> {
    if !dir.exists() {
        return Ok(());
    }
    let mut entries = std::fs::read_dir(dir)
        .map_err(|e| format!("read resource directory: {e}"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("read resource entry: {e}"))?;
    entries.sort_by_key(std::fs::DirEntry::file_name);
    for entry in entries {
        let file_type = entry
            .file_type()
            .map_err(|e| format!("stat resource file: {e}"))?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with('.') && name.ends_with(".tmp") {
            continue;
        }
        let entry_name = format!("{zip_prefix}/{name}");
        if file_type.is_dir() {
            add_directory_to_zip(zip, &entry.path(), &entry_name)?;
        } else if file_type.is_file() {
            let data = std::fs::read(entry.path())
                .map_err(|e| format!("read resource file {}: {e}", entry.path().display()))?;
            zip.start_file(
                &entry_name,
                SimpleFileOptions::default()
                    .compression_method(zip::CompressionMethod::Deflated)
                    .unix_permissions(0o644),
            )
            .map_err(|e| format!("start {entry_name} in ZIP: {e}"))?;
            zip.write_all(&data)
                .map_err(|e| format!("write {entry_name} to ZIP: {e}"))?;
        } else {
            return Err(format!(
                "archive source entry {} is not a regular file or directory",
                entry.path().display()
            ));
        }
    }
    Ok(())
}

pub(crate) fn build_zip(
    manifest: &ResourceManifestV1,
    udf_shas: &[String],
    wasm_dir: &std::path::Path,
    schemas_dir: &std::path::Path,
) -> Result<Vec<u8>, String> {
    let manifest_json =
        serde_json::to_vec(manifest).map_err(|e| format!("serialize resource manifest: {e}"))?;

    let mut zip = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options = SimpleFileOptions::default()
        .compression_method(zip::CompressionMethod::Deflated)
        .unix_permissions(0o644);
    zip.start_file("manifest.json", options)
        .map_err(|e| format!("start manifest.json in ZIP: {e}"))?;
    zip.write_all(&manifest_json)
        .map_err(|e| format!("write manifest.json to ZIP: {e}"))?;

    for sha in udf_shas {
        let wasm_path = wasm_dir.join(format!("{sha}.wasm"));
        let wasm_bytes =
            std::fs::read(&wasm_path).map_err(|e| format!("read {}: {e}", wasm_path.display()))?;

        let entry_name = format!("wasm_files/{sha}.wasm");
        zip.start_file(&entry_name, options)
            .map_err(|e| format!("start {entry_name} in ZIP: {e}"))?;
        zip.write_all(&wasm_bytes)
            .map_err(|e| format!("write {entry_name} to ZIP: {e}"))?;
    }

    for schema in &manifest.resources.schemas {
        let schema_type = schema.schema_type.trim().to_ascii_lowercase();
        add_directory_to_zip(
            &mut zip,
            &schemas_dir.join(&schema_type).join(&schema.name),
            &format!("schemas/{schema_type}/{}", schema.name),
        )
        .map_err(|e| format!("write schema {} to ZIP: {e}", schema.name))?;
    }

    zip.finish()
        .map(Cursor::into_inner)
        .map_err(|e| format!("finish ZIP: {e}"))
}

pub(crate) fn build_export_resources(storage: &StorageManager) -> Result<ExportResources, String> {
    let snapshot = storage
        .export_metadata_snapshot()
        .map_err(|err| format!("read export snapshot from storage: {err}"))?;

    let mut memory_topics = snapshot
        .memory_topics
        .into_iter()
        .map(|topic| ExportMemoryTopic {
            topic: topic.topic,
            kind: topic.kind,
            capacity: topic.capacity,
        })
        .collect::<Vec<_>>();
    memory_topics.sort_by(|a, b| a.topic.cmp(&b.topic));

    let mut shared_mqtt_clients = Vec::with_capacity(snapshot.mqtt_configs.len());
    for stored in snapshot.mqtt_configs {
        let cfg: SharedMqttClientConfig =
            serde_json::from_str(&stored.raw_json).map_err(|err| {
                format!(
                    "decode stored shared mqtt client config {}: {err}",
                    stored.key
                )
            })?;
        if cfg.key != stored.key {
            return Err(format!(
                "stored shared mqtt client config {} key mismatch in raw_json: {}",
                stored.key, cfg.key
            ));
        }
        shared_mqtt_clients.push(cfg);
    }
    shared_mqtt_clients.sort_by(|a, b| a.key.cmp(&b.key));

    let mut schemas = Vec::with_capacity(snapshot.schemas.len());
    for stored in snapshot.schemas {
        let props: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(&stored.props_json)
                .map_err(|err| format!("decode stored schema {} props: {err}", stored.name))?;
        schemas.push(ExportSchema {
            name: stored.name,
            schema_type: stored.schema_type,
            props,
        });
    }
    schemas.sort_by(|a, b| a.name.cmp(&b.name));

    let mut streams = Vec::with_capacity(snapshot.streams.len());
    for stored in snapshot.streams {
        let req: CreateStreamRequest = serde_json::from_str(&stored.raw_json)
            .map_err(|err| format!("decode stored stream {}: {err}", stored.id))?;
        if req.name != stored.id {
            return Err(format!(
                "stored stream {} name mismatch in raw_json: {}",
                stored.id, req.name
            ));
        }
        streams.push(req);
    }
    streams.sort_by(|a, b| a.name.cmp(&b.name));

    let mut run_states = snapshot
        .pipeline_run_states
        .into_iter()
        .map(|state| (state.pipeline_id, state.desired_state))
        .collect::<BTreeMap<_, _>>();
    let mut pipelines = Vec::with_capacity(snapshot.pipelines.len());
    for stored in snapshot.pipelines {
        let req = storage_bridge::pipeline_request_from_stored(&stored)?;
        if req.id != stored.id {
            return Err(format!(
                "stored pipeline {} id mismatch in raw_json: {}",
                stored.id, req.id
            ));
        }
        let run_state = run_states
            .remove(&stored.id)
            .unwrap_or(StoredPipelineDesiredState::Stopped);
        pipelines.push(ExportPipeline {
            definition: req,
            run_state,
        });
    }
    pipelines.sort_by(|a, b| a.definition.id.cmp(&b.definition.id));
    if let Some((pipeline_id, _)) = run_states.first_key_value() {
        return Err(format!(
            "stored pipeline run state references missing pipeline: {pipeline_id}"
        ));
    }

    let mut udfs: Vec<ExportUdf> = snapshot
        .udfs
        .into_iter()
        .map(|u| ExportUdf {
            name: u.name,
            wasm_sha256: u.wasm_sha256,
        })
        .collect();
    udfs.sort_by(|a, b| a.name.cmp(&b.name));

    Ok(ExportResources {
        memory_topics,
        shared_mqtt_clients,
        schemas,
        streams,
        pipelines,
        udfs,
    })
}

pub(crate) fn build_resource_manifest(
    storage: &StorageManager,
    bundle_version: String,
) -> Result<ResourceManifestV1, String> {
    validate_bundle_version(&bundle_version)?;
    Ok(ResourceManifestV1 {
        format_version: RESOURCE_DIRECTORY_FORMAT_VERSION,
        bundle_version,
        resources: build_export_resources(storage)?,
    })
}

pub(crate) fn validate_resource_manifest(manifest: &ResourceManifestV1) -> Result<(), String> {
    if manifest.format_version != RESOURCE_DIRECTORY_FORMAT_VERSION {
        return Err(format!(
            "unsupported resource directory format_version: {}",
            manifest.format_version
        ));
    }
    validate_bundle_version(&manifest.bundle_version)
}

pub(crate) fn validate_bundle_version(bundle_version: &str) -> Result<(), String> {
    let version = bundle_version.trim();
    if version.is_empty() {
        return Err("bundle_version must not be empty".to_string());
    }
    if version.len() > 128 {
        return Err("bundle_version must not exceed 128 bytes".to_string());
    }
    if version.chars().any(char::is_control) {
        return Err("bundle_version must not contain control characters".to_string());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::instances::{DEFAULT_FLOW_INSTANCE_ID, FlowInstanceSpec};
    use crate::pipeline::AppState;
    use crate::storage_bridge::{
        stored_mqtt_from_config, stored_pipeline_from_request, stored_stream_from_request,
    };
    use axum::body::to_bytes;
    use axum::extract::State;
    use axum::http::StatusCode;
    use serde_json::Value as JsonValue;
    use storage::{
        StorageManager, StoredMemoryTopic, StoredMemoryTopicKind, StoredPipelineDesiredState,
        StoredPipelineRunState, StoredUdf,
    };
    use tempfile::tempdir;

    fn sample_stream_request_named(name: &str) -> CreateStreamRequest {
        serde_json::from_value(serde_json::json!({
            "name": name,
            "type": "mqtt",
            "schema": {
                "type": "json",
                "props": {
                    "columns": [
                        { "name": "value", "data_type": "int64" }
                    ]
                }
            },
            "props": {
                "broker_url": "mqtt://localhost:1883",
                "topic": format!("{name}/topic"),
                "qos": 0
            },
            "shared": false,
            "decoder": {
                "type": "json",
                "props": {}
            }
        }))
        .expect("deserialize stream request")
    }

    fn sample_stream_request() -> CreateStreamRequest {
        sample_stream_request_named("stream_1")
    }

    fn sample_pipeline_request_named(id: &str, stream_name: &str) -> CreatePipelineRequest {
        serde_json::from_value(serde_json::json!({
            "id": id,
            "flow_instance_id": DEFAULT_FLOW_INSTANCE_ID,
            "sql": format!("SELECT * FROM {stream_name}"),
            "sinks": [
                {
                    "id": format!("{id}_sink_0"),
                    "type": "nop",
                    "props": { "log": false },
                    "common_sink_props": {},
                    "encoder": { "type": "json", "props": {} }
                }
            ],
            "options": {
                "data_channel_capacity": 16,
                "eventtime": {
                    "enabled": false,
                    "late_tolerance_ms": 0
                }
            }
        }))
        .expect("deserialize pipeline request")
    }

    fn sample_pipeline_request() -> CreatePipelineRequest {
        sample_pipeline_request_named("pipe_1", "stream_1")
    }

    fn sample_stored_udf(name: &str, sha: &str) -> StoredUdf {
        StoredUdf {
            name: name.to_string(),
            wasm_sha256: sha.to_string(),
            raw_json: serde_json::json!({"name": name, "description": "test"}).to_string(),
        }
    }

    fn sample_default_instance_spec() -> FlowInstanceSpec {
        FlowInstanceSpec {
            id: DEFAULT_FLOW_INSTANCE_ID.to_string(),
            ..FlowInstanceSpec::default()
        }
    }

    #[tokio::test]
    async fn export_storage_handler_returns_zip() {
        let dir = tempdir().unwrap();
        let storage = StorageManager::new(dir.path()).unwrap();

        let stream = sample_stream_request();
        let pipeline = sample_pipeline_request();
        let mqtt = SharedMqttClientConfig {
            key: "shared_a".to_string(),
            broker_url: "tcp://localhost:1883".to_string(),
            topic: "foo/bar".to_string(),
            client_id: "client_a".to_string(),
            qos: 1,
            max_packet_size: None,
            username: None,
            password: None,
            resolved_password: None,
        };
        let memory_topic = StoredMemoryTopic {
            topic: "topic_1".to_string(),
            kind: StoredMemoryTopicKind::Bytes,
            capacity: 16,
        };
        let run_state = StoredPipelineRunState {
            pipeline_id: pipeline.id.clone(),
            desired_state: StoredPipelineDesiredState::Running,
        };

        // Write a dummy WASM file for the UDF
        let wasm_bytes = b"dummy wasm";
        let wasm_sha = "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2";
        let wasm_path = storage.wasm_files_dir().join(format!("{wasm_sha}.wasm"));
        std::fs::write(&wasm_path, wasm_bytes).unwrap();

        let udf = sample_stored_udf("my_udf", wasm_sha);

        storage
            .create_stream(stored_stream_from_request(&stream).unwrap())
            .unwrap();
        storage
            .create_pipeline(stored_pipeline_from_request(&pipeline).unwrap())
            .unwrap();
        storage
            .create_mqtt_config(stored_mqtt_from_config(&mqtt))
            .unwrap();
        storage.create_memory_topic(memory_topic).unwrap();
        storage.put_pipeline_run_state(run_state).unwrap();
        storage.create_udf(udf).unwrap();

        let state = AppState::new(
            crate::new_default_flow_instance(),
            storage,
            vec![sample_default_instance_spec()],
            0,
        )
        .unwrap();

        let response = export_storage_handler(
            State(state),
            Query(ExportStorageQuery {
                bundle_version: "test-bundle-1".to_string(),
            }),
        )
        .await
        .into_response();
        assert_eq!(response.status(), StatusCode::OK);

        let disposition = response
            .headers()
            .get(header::CONTENT_DISPOSITION)
            .expect("content disposition header")
            .to_str()
            .expect("header as str");
        assert_eq!(disposition, "attachment; filename=\"veloflux-export.zip\"");

        let content_type = response
            .headers()
            .get(header::CONTENT_TYPE)
            .expect("content type header")
            .to_str()
            .expect("header as str");
        assert_eq!(content_type, "application/zip");

        let body = to_bytes(response.into_body(), 10 * 1024 * 1024)
            .await
            .expect("read response body");

        let mut archive =
            zip::ZipArchive::new(Cursor::new(body.as_ref())).expect("open export ZIP");
        let mut entries: Vec<String> = Vec::new();
        let mut metadata_bytes: Option<Vec<u8>> = None;

        for index in 0..archive.len() {
            let mut entry = archive.by_index(index).expect("ZIP entry");
            let path = entry.name().to_string();
            entries.push(path.clone());
            if path == "manifest.json" {
                let mut buf = Vec::new();
                std::io::Read::read_to_end(&mut entry, &mut buf).expect("read entry");
                metadata_bytes = Some(buf);
            }
        }

        entries.sort();
        assert!(
            entries.contains(&"manifest.json".to_string()),
            "ZIP should contain manifest.json, got: {entries:?}"
        );
        assert!(
            entries.contains(&format!("wasm_files/{wasm_sha}.wasm")),
            "ZIP should contain wasm file, got: {entries:?}"
        );

        let metadata: JsonValue =
            serde_json::from_slice(&metadata_bytes.expect("manifest.json")).expect("parse json");
        assert_eq!(metadata["resources"]["streams"][0]["name"], "stream_1");
        assert_eq!(
            metadata["resources"]["pipelines"][0]["run_state"],
            "Running"
        );
        assert!(metadata["resources"].get("pipeline_run_states").is_none());
        assert_eq!(metadata["resources"]["udfs"][0]["name"], "my_udf",);
        assert_eq!(metadata["resources"]["udfs"][0]["wasm_sha256"], wasm_sha,);
    }

    #[test]
    fn build_resource_manifest_sorts_resource_collections_stably() {
        let dir = tempdir().expect("create tempdir");
        let storage = StorageManager::new(dir.path()).expect("create storage");

        for topic in [
            StoredMemoryTopic {
                topic: "topic_b".to_string(),
                kind: StoredMemoryTopicKind::Bytes,
                capacity: 16,
            },
            StoredMemoryTopic {
                topic: "topic_a".to_string(),
                kind: StoredMemoryTopicKind::Collection,
                capacity: 32,
            },
        ] {
            storage
                .create_memory_topic(topic)
                .expect("create memory topic");
        }

        for mqtt in [
            SharedMqttClientConfig {
                key: "shared_b".to_string(),
                broker_url: "tcp://localhost:1883".to_string(),
                topic: "b/topic".to_string(),
                client_id: "client_b".to_string(),
                qos: 1,
                max_packet_size: None,
                username: None,
                password: None,
                resolved_password: None,
            },
            SharedMqttClientConfig {
                key: "shared_a".to_string(),
                broker_url: "tcp://localhost:1883".to_string(),
                topic: "a/topic".to_string(),
                client_id: "client_a".to_string(),
                qos: 0,
                max_packet_size: Some(1024),
                username: None,
                password: None,
                resolved_password: None,
            },
        ] {
            storage
                .create_mqtt_config(stored_mqtt_from_config(&mqtt))
                .expect("create mqtt config");
        }

        for stream in [
            sample_stream_request_named("stream_b"),
            sample_stream_request_named("stream_a"),
        ] {
            storage
                .create_stream(stored_stream_from_request(&stream).expect("store stream"))
                .expect("create stream");
        }

        for pipeline in [
            sample_pipeline_request_named("pipe_b", "stream_b"),
            sample_pipeline_request_named("pipe_a", "stream_a"),
        ] {
            storage
                .create_pipeline(stored_pipeline_from_request(&pipeline).expect("store pipeline"))
                .expect("create pipeline");
        }

        for run_state in [
            StoredPipelineRunState {
                pipeline_id: "pipe_b".to_string(),
                desired_state: StoredPipelineDesiredState::RunningScheduled(123),
            },
            StoredPipelineRunState {
                pipeline_id: "pipe_a".to_string(),
                desired_state: StoredPipelineDesiredState::Stopped,
            },
        ] {
            storage
                .put_pipeline_run_state(run_state)
                .expect("create pipeline run state");
        }

        // Add UDFs in reverse order to test sorting
        storage
            .create_udf(sample_stored_udf("udf_b", "sha_b"))
            .expect("create udf b");
        storage
            .create_udf(sample_stored_udf("udf_a", "sha_a"))
            .expect("create udf a");

        let first_bundle =
            build_resource_manifest(&storage, "test-bundle-1".to_string()).expect("first bundle");
        let second_bundle =
            build_resource_manifest(&storage, "test-bundle-1".to_string()).expect("second bundle");

        assert_eq!(
            first_bundle
                .resources
                .memory_topics
                .iter()
                .map(|topic| topic.topic.as_str())
                .collect::<Vec<_>>(),
            vec!["topic_a", "topic_b"]
        );
        assert_eq!(
            first_bundle
                .resources
                .shared_mqtt_clients
                .iter()
                .map(|cfg| cfg.key.as_str())
                .collect::<Vec<_>>(),
            vec!["shared_a", "shared_b"]
        );
        assert_eq!(
            first_bundle
                .resources
                .streams
                .iter()
                .map(|stream| stream.name.as_str())
                .collect::<Vec<_>>(),
            vec!["stream_a", "stream_b"]
        );
        assert_eq!(
            first_bundle
                .resources
                .pipelines
                .iter()
                .map(|pipeline| pipeline.definition.id.as_str())
                .collect::<Vec<_>>(),
            vec!["pipe_a", "pipe_b"]
        );
        assert_eq!(
            first_bundle
                .resources
                .pipelines
                .iter()
                .map(|pipeline| &pipeline.run_state)
                .collect::<Vec<_>>(),
            vec![
                &StoredPipelineDesiredState::Stopped,
                &StoredPipelineDesiredState::RunningScheduled(123)
            ]
        );
        assert_eq!(
            first_bundle
                .resources
                .udfs
                .iter()
                .map(|u| u.name.as_str())
                .collect::<Vec<_>>(),
            vec!["udf_a", "udf_b"]
        );
        assert_eq!(
            serde_json::to_value(&first_bundle.resources).expect("serialize first resources"),
            serde_json::to_value(&second_bundle.resources).expect("serialize second resources")
        );
    }
}
