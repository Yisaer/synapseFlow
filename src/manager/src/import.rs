use axum::{extract::State, http::StatusCode, response::IntoResponse};
use serde::Serialize;
use std::collections::BTreeSet;
use storage::{
    MetadataExportSnapshot, StoredMemoryTopic, StoredMqttClientConfig, StoredPipelineRunState,
    StoredSchema, StoredUdf,
};
use tokio::sync::TryAcquireError;

use crate::audit::ResourceMutationLog;
use crate::export::{
    ExportBundleV1, ExportMemoryTopic, ExportPipelineRunState, ExportUdf, build_export_bundle,
};
use crate::instances::DEFAULT_FLOW_INSTANCE_ID;
use crate::pipeline::{AppState, CreatePipelineRequest, validate_create_request};
use crate::storage_bridge;
use crate::stream::CreateStreamRequest;

#[derive(Serialize)]
pub struct ImportStorageResponse {
    pub applied_to_runtime: bool,
    pub imported_resource_counts: ImportResourceCounts,
    pub previous_bundle: ExportBundleV1,
}

#[derive(Serialize)]
pub struct ImportResourceCounts {
    pub memory_topics: usize,
    pub shared_mqtt_clients: usize,
    pub schemas: usize,
    pub streams: usize,
    pub pipelines: usize,
    pub pipeline_run_states: usize,
    pub udfs: usize,
}

/// Accept a tar.gz body via `axum::body::Bytes`.
pub async fn import_storage_handler(
    State(state): State<AppState>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let audit = ResourceMutationLog::new("storage", "import", "tar_gz_bundle", None);
    let _import_export_permit = match state.try_acquire_import_export_op() {
        Ok(permit) => permit,
        Err(TryAcquireError::NoPermits) => return import_export_busy_response(),
        Err(TryAcquireError::Closed) => {
            let err = "import/export operation guard closed".to_string();
            audit.log_failure(&err);
            return (StatusCode::INTERNAL_SERVER_ERROR, err).into_response();
        }
    };

    // Unpack the tar.gz and extract metadata.json + wasm_files/
    let tmp = match tempfile::tempdir() {
        Ok(d) => d,
        Err(e) => {
            let err = format!("create temp dir: {e}");
            audit.log_failure(&err);
            return (StatusCode::INTERNAL_SERVER_ERROR, err).into_response();
        }
    };

    if let Err(err) = extract_tar_gz(&body, tmp.path()) {
        audit.log_failure(&err);
        return (StatusCode::BAD_REQUEST, err).into_response();
    }

    let metadata_path = tmp.path().join("metadata.json");
    let metadata_bytes = match std::fs::read(&metadata_path) {
        Ok(b) => b,
        Err(e) => {
            let err = format!("read metadata.json from archive: {e}");
            audit.log_failure(&err);
            return (StatusCode::BAD_REQUEST, err).into_response();
        }
    };

    let bundle: ExportBundleV1 = match serde_json::from_slice(&metadata_bytes) {
        Ok(b) => b,
        Err(e) => {
            let err = format!("parse metadata.json: {e}");
            audit.log_failure(&err);
            return (StatusCode::BAD_REQUEST, err).into_response();
        }
    };

    let snapshot = match validate_and_build_snapshot(&bundle, &|id| state.is_declared_instance(id))
    {
        Ok(snapshot) => snapshot,
        Err(err) => {
            audit.log_failure(&err);
            return (StatusCode::BAD_REQUEST, err).into_response();
        }
    };

    // Validate and copy UDF wasm files from the archive
    let udf_count = snapshot.udfs.len();
    if udf_count > 0 {
        let wasm_src = tmp.path().join("wasm_files");
        if let Err(err) =
            validate_and_copy_udfs(&snapshot.udfs, &wasm_src, &state.storage.wasm_files_dir())
        {
            audit.log_failure(&err);
            return (StatusCode::BAD_REQUEST, err).into_response();
        }
    }

    let previous_bundle = match build_export_bundle(state.storage.as_ref()) {
        Ok(bundle) => bundle,
        Err(err) => {
            audit.log_failure(&err);
            return (StatusCode::INTERNAL_SERVER_ERROR, err).into_response();
        }
    };

    let imported_resource_counts = ImportResourceCounts {
        memory_topics: snapshot.memory_topics.len(),
        shared_mqtt_clients: snapshot.mqtt_configs.len(),
        schemas: snapshot.schemas.len(),
        streams: snapshot.streams.len(),
        pipelines: snapshot.pipelines.len(),
        pipeline_run_states: snapshot.pipeline_run_states.len(),
        udfs: udf_count,
    };

    if let Err(err) = state.storage.replace_metadata_snapshot(snapshot) {
        let err = format!("replace metadata snapshot in storage: {err}");
        audit.log_failure(&err);
        return (StatusCode::INTERNAL_SERVER_ERROR, err).into_response();
    }

    audit.log_success();
    (
        StatusCode::OK,
        axum::Json(ImportStorageResponse {
            applied_to_runtime: false,
            imported_resource_counts,
            previous_bundle,
        }),
    )
        .into_response()
}

fn extract_tar_gz(data: &[u8], dest: &std::path::Path) -> Result<(), String> {
    let gz = flate2::read::GzDecoder::new(data);
    let mut archive = tar::Archive::new(gz);
    archive
        .unpack(dest)
        .map_err(|e| format!("unpack tar.gz: {e}"))
}

fn validate_pipeline_stream_references(
    req: &CreatePipelineRequest,
    stream_names: &BTreeSet<String>,
) -> Result<(), String> {
    let select_stmt = parser::parse_sql(&req.sql).map_err(|err| {
        format!(
            "pipeline {} sql parse failed during import validation: {}",
            req.id, err
        )
    })?;

    for source in &select_stmt.source_infos {
        if !stream_names.contains(&source.name) {
            return Err(format!(
                "pipeline {} references missing stream: {}",
                req.id, source.name
            ));
        }
    }

    Ok(())
}

fn validate_and_build_snapshot_inner<F>(
    bundle: &ExportBundleV1,
    existing_stream_names: &BTreeSet<String>,
    is_declared_instance: &F,
) -> Result<MetadataExportSnapshot, String>
where
    F: Fn(&str) -> bool,
{
    let mut memory_topics = Vec::with_capacity(bundle.resources.memory_topics.len());
    let mut memory_topic_names = BTreeSet::new();
    for topic in &bundle.resources.memory_topics {
        validate_memory_topic(topic, &mut memory_topic_names)?;
        memory_topics.push(StoredMemoryTopic {
            topic: topic.topic.trim().to_string(),
            kind: topic.kind.clone(),
            capacity: topic.capacity,
        });
    }

    let mut mqtt_configs = Vec::with_capacity(bundle.resources.shared_mqtt_clients.len());
    let mut mqtt_keys = BTreeSet::new();
    for cfg in &bundle.resources.shared_mqtt_clients {
        let key = cfg.key.trim();
        if key.is_empty() {
            return Err("shared mqtt client key must not be empty".to_string());
        }
        if !mqtt_keys.insert(key.to_string()) {
            return Err(format!("duplicate shared mqtt client key in bundle: {key}"));
        }
        let raw_json = serde_json::to_string(cfg)
            .map_err(|err| format!("serialize shared mqtt client config {key}: {err}"))?;
        mqtt_configs.push(StoredMqttClientConfig {
            key: cfg.key.clone(),
            raw_json,
        });
    }

    let mut schemas = Vec::with_capacity(bundle.resources.schemas.len());
    let mut schema_names = BTreeSet::new();
    for schema in &bundle.resources.schemas {
        let name = schema.name.trim();
        if name.is_empty() {
            return Err("schema name must not be empty".to_string());
        }
        if !schema_names.insert(name.to_string()) {
            return Err(format!("duplicate schema name in bundle: {name}"));
        }
        let props_json = serde_json::to_string(&schema.props)
            .map_err(|err| format!("serialize schema props for {name}: {err}"))?;
        schemas.push(StoredSchema {
            name: name.to_string(),
            schema_type: schema.schema_type.clone(),
            props_json,
        });
    }

    let mut streams = Vec::with_capacity(bundle.resources.streams.len());
    let mut bundle_stream_names = BTreeSet::new();
    let mut available_stream_names = existing_stream_names.clone();

    for req in &bundle.resources.streams {
        validate_stream_request(req, &mut bundle_stream_names)?;
        available_stream_names.insert(req.name.clone());
        streams.push(storage_bridge::stored_stream_from_request(req)?);
    }

    let mut pipelines = Vec::with_capacity(bundle.resources.pipelines.len());
    let mut pipeline_ids = BTreeSet::new();
    for req in &bundle.resources.pipelines {
        let normalized = normalize_pipeline_request(req)?;
        validate_pipeline_stream_references(&normalized, &available_stream_names)?;

        let flow_instance_id = normalized
            .flow_instance_id
            .as_deref()
            .ok_or_else(|| "flow_instance_id must not be empty".to_string())?;
        validate_declared_flow_instance(flow_instance_id, is_declared_instance)?;

        let id = normalized.id.clone();
        if !pipeline_ids.insert(id.clone()) {
            return Err(format!("duplicate pipeline id in bundle: {id}"));
        }

        pipelines.push(storage_bridge::stored_pipeline_from_request(&normalized)?);
    }

    let mut pipeline_run_states = Vec::with_capacity(bundle.resources.pipeline_run_states.len());
    let mut state_ids = BTreeSet::new();
    for run_state in &bundle.resources.pipeline_run_states {
        validate_pipeline_run_state(run_state, &pipeline_ids, &mut state_ids)?;
        pipeline_run_states.push(StoredPipelineRunState {
            pipeline_id: run_state.pipeline_id.clone(),
            desired_state: run_state.desired_state.clone(),
        });
    }

    let udfs: Vec<StoredUdf> = validate_import_udfs(&bundle.resources.udfs)?;

    Ok(MetadataExportSnapshot {
        streams,
        schemas,
        pipelines,
        pipeline_run_states,
        mqtt_configs,
        memory_topics,
        udfs,
    })
}

fn validate_import_udfs(udfs: &[ExportUdf]) -> Result<Vec<StoredUdf>, String> {
    let mut names = BTreeSet::new();
    let mut result = Vec::with_capacity(udfs.len());
    for udf in udfs {
        let name = udf.name.trim();
        if name.is_empty() {
            return Err("UDF name must not be empty".to_string());
        }
        let sha = udf.wasm_sha256.trim();
        if sha.is_empty() {
            return Err(format!("UDF '{name}' has empty wasm_sha256"));
        }
        if !names.insert(name.to_lowercase()) {
            return Err(format!("duplicate UDF name in bundle: {name}"));
        }
        result.push(StoredUdf {
            name: name.to_string(),
            wasm_sha256: sha.to_string(),
            raw_json: serde_json::json!({"name": name}).to_string(),
        });
    }
    Ok(result)
}

/// Validate each imported UDF's WASM file and copy it to the shared wasm directory.
/// Checks: file exists, SHA-256 matches, module is valid, metadata name matches.
#[cfg(feature = "wasm_udf")]
fn validate_and_copy_udfs(
    udfs: &[StoredUdf],
    wasm_src_dir: &std::path::Path,
    wasm_dst_dir: &std::path::Path,
) -> Result<(), String> {
    let engine = udf::WasmEngine::new()
        .map_err(|e| format!("create WASM engine for import validation: {e}"))?;
    for udf in udfs {
        let src = wasm_src_dir.join(format!("{}.wasm", udf.wasm_sha256));
        let wasm_bytes = std::fs::read(&src).map_err(|e| {
            format!(
                "UDF '{}': missing wasm file {} in archive: {e}",
                udf.name,
                src.display()
            )
        })?;

        // Recompute and verify SHA-256
        let actual_sha = sha256_hex(&wasm_bytes);
        if actual_sha != udf.wasm_sha256 {
            return Err(format!(
                "UDF '{}': SHA-256 mismatch (declared: {}, actual: {})",
                udf.name, udf.wasm_sha256, actual_sha
            ));
        }

        // Validate the WASM module
        let metadata = engine
            .validate(&wasm_bytes)
            .map_err(|e| format!("UDF '{}': invalid WASM module: {e}", udf.name))?;

        // Check metadata name matches declared name
        if metadata.name != udf.name {
            return Err(format!(
                "UDF '{}': metadata name '{}' does not match",
                udf.name, metadata.name
            ));
        }

        // Copy to shared directory (skip if already exists)
        let dst = wasm_dst_dir.join(format!("{}.wasm", udf.wasm_sha256));
        if !dst.exists() {
            std::fs::copy(&src, &dst)
                .map_err(|e| format!("copy wasm file {}: {e}", dst.display()))?;
        }
    }
    Ok(())
}

#[cfg(not(feature = "wasm_udf"))]
fn validate_and_copy_udfs(
    udfs: &[StoredUdf],
    wasm_src_dir: &std::path::Path,
    wasm_dst_dir: &std::path::Path,
) -> Result<(), String> {
    for udf in udfs {
        let src = wasm_src_dir.join(format!("{}.wasm", udf.wasm_sha256));
        let wasm_bytes = std::fs::read(&src).map_err(|e| {
            format!(
                "UDF '{}': missing wasm file {} in archive: {e}",
                udf.name,
                src.display()
            )
        })?;

        let actual_sha = sha256_hex(&wasm_bytes);
        if actual_sha != udf.wasm_sha256 {
            return Err(format!(
                "UDF '{}': SHA-256 mismatch (declared: {}, actual: {})",
                udf.name, udf.wasm_sha256, actual_sha
            ));
        }

        let dst = wasm_dst_dir.join(format!("{}.wasm", udf.wasm_sha256));
        if !dst.exists() {
            std::fs::copy(&src, &dst)
                .map_err(|e| format!("copy wasm file {}: {e}", dst.display()))?;
        }
    }
    Ok(())
}

fn sha256_hex(data: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(data);
    format!("{:x}", hasher.finalize())
}

pub(crate) fn validate_and_build_snapshot<F>(
    bundle: &ExportBundleV1,
    is_declared_instance: &F,
) -> Result<MetadataExportSnapshot, String>
where
    F: Fn(&str) -> bool,
{
    validate_and_build_snapshot_inner(bundle, &BTreeSet::new(), is_declared_instance)
}

pub(crate) fn validate_and_build_snapshot_with_existing_streams<F>(
    bundle: &ExportBundleV1,
    existing_stream_names: &BTreeSet<String>,
    is_declared_instance: &F,
) -> Result<MetadataExportSnapshot, String>
where
    F: Fn(&str) -> bool,
{
    validate_and_build_snapshot_inner(bundle, existing_stream_names, is_declared_instance)
}

fn validate_memory_topic(
    topic: &ExportMemoryTopic,
    names: &mut BTreeSet<String>,
) -> Result<(), String> {
    let topic_name = topic.topic.trim();
    if topic_name.is_empty() {
        return Err("memory topic name must not be empty".to_string());
    }
    if topic.capacity == 0 {
        return Err(format!(
            "memory topic {} capacity must be greater than 0",
            topic.topic
        ));
    }
    if !names.insert(topic_name.to_string()) {
        return Err(format!("duplicate memory topic in bundle: {topic_name}"));
    }
    Ok(())
}

fn validate_stream_request(
    req: &CreateStreamRequest,
    stream_names: &mut BTreeSet<String>,
) -> Result<(), String> {
    if req.name.trim().is_empty() {
        return Err("stream name must not be empty".to_string());
    }
    if !stream_names.insert(req.name.clone()) {
        return Err(format!("duplicate stream name in bundle: {}", req.name));
    }
    Ok(())
}

fn normalize_pipeline_request(
    req: &CreatePipelineRequest,
) -> Result<CreatePipelineRequest, String> {
    let mut normalized = req.clone();
    normalized.normalize();
    normalized.flow_instance_id =
        Some(canonical_flow_instance_id(req.flow_instance_id.as_deref())?);
    validate_create_request(&normalized)?;
    Ok(normalized)
}

fn validate_declared_flow_instance<F>(
    flow_instance_id: &str,
    is_declared_instance: &F,
) -> Result<(), String>
where
    F: Fn(&str) -> bool,
{
    if !is_declared_instance(flow_instance_id) {
        return Err(format!(
            "flow instance {flow_instance_id} is not declared by config"
        ));
    }
    Ok(())
}

fn validate_pipeline_run_state(
    run_state: &ExportPipelineRunState,
    pipeline_ids: &BTreeSet<String>,
    state_ids: &mut BTreeSet<String>,
) -> Result<(), String> {
    if run_state.pipeline_id.trim().is_empty() {
        return Err("pipeline_run_state pipeline_id must not be empty".to_string());
    }
    if !state_ids.insert(run_state.pipeline_id.clone()) {
        return Err(format!(
            "duplicate pipeline_run_state entry in bundle: {}",
            run_state.pipeline_id
        ));
    }
    if !pipeline_ids.contains(&run_state.pipeline_id) {
        return Err(format!(
            "pipeline_run_state references missing pipeline: {}",
            run_state.pipeline_id
        ));
    }
    Ok(())
}

fn canonical_flow_instance_id(value: Option<&str>) -> Result<String, String> {
    let id = value.unwrap_or(DEFAULT_FLOW_INSTANCE_ID).trim();
    if id.is_empty() {
        return Err("flow_instance_id must not be empty".to_string());
    }
    Ok(id.to_string())
}

fn import_export_busy_response() -> axum::response::Response {
    (
        StatusCode::CONFLICT,
        "another import/export command is in progress".to_string(),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::instances::{DEFAULT_FLOW_INSTANCE_ID, FlowInstanceSpec};
    use axum::body::to_bytes;
    use axum::http::StatusCode;
    use serde_json::Value as JsonValue;
    use storage::{StorageManager, StoredMemoryTopicKind, StoredPipelineDesiredState};
    use tempfile::tempdir;

    fn sample_default_instance_spec() -> FlowInstanceSpec {
        FlowInstanceSpec {
            id: DEFAULT_FLOW_INSTANCE_ID.to_string(),
            ..FlowInstanceSpec::default()
        }
    }

    fn sample_stream_request(name: &str) -> CreateStreamRequest {
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

    fn sample_pipeline_request(id: &str, stream_name: &str) -> CreatePipelineRequest {
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

    fn sample_bundle(
        stream_name: &str,
        pipeline_id: &str,
        mqtt_key: &str,
        topic_name: &str,
    ) -> ExportBundleV1 {
        ExportBundleV1 {
            exported_at: 1_700_000_000,
            resources: crate::export::ExportResources {
                memory_topics: vec![ExportMemoryTopic {
                    topic: topic_name.to_string(),
                    kind: StoredMemoryTopicKind::Bytes,
                    capacity: 16,
                }],
                shared_mqtt_clients: vec![flow::connector::SharedMqttClientConfig {
                    key: mqtt_key.to_string(),
                    broker_url: "tcp://localhost:1883".to_string(),
                    topic: format!("{mqtt_key}/topic"),
                    client_id: format!("{mqtt_key}_client"),
                    qos: 1,
                    max_packet_size: None,
                }],
                schemas: vec![],
                streams: vec![sample_stream_request(stream_name)],
                pipelines: vec![sample_pipeline_request(pipeline_id, stream_name)],
                pipeline_run_states: vec![ExportPipelineRunState {
                    pipeline_id: pipeline_id.to_string(),
                    desired_state: StoredPipelineDesiredState::Stopped,
                }],
                udfs: vec![],
            },
        }
    }

    fn build_tar_gz_for_test(bundle: &ExportBundleV1, wasm_dir: &std::path::Path) -> Vec<u8> {
        let metadata_json = serde_json::to_vec(bundle).unwrap();
        let udf_shas: Vec<String> = bundle
            .resources
            .udfs
            .iter()
            .map(|u| u.wasm_sha256.clone())
            .collect();

        let mut tar_gz = Vec::new();
        {
            let gz = flate2::write::GzEncoder::new(&mut tar_gz, flate2::Compression::default());
            let mut tar = tar::Builder::new(gz);

            let mut header = tar::Header::new_gnu();
            header.set_size(metadata_json.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            tar.append_data(&mut header, "metadata.json", metadata_json.as_slice())
                .unwrap();

            for sha in &udf_shas {
                let wasm_path = wasm_dir.join(format!("{sha}.wasm"));
                if wasm_path.exists() {
                    let data = std::fs::read(&wasm_path).unwrap();
                    let entry_name = format!("wasm_files/{sha}.wasm");
                    let mut header = tar::Header::new_gnu();
                    header.set_size(data.len() as u64);
                    header.set_mode(0o644);
                    header.set_cksum();
                    tar.append_data(&mut header, &entry_name, data.as_slice())
                        .unwrap();
                }
            }

            let gz = tar.into_inner().unwrap();
            gz.finish().unwrap();
        }
        tar_gz
    }

    fn is_default_instance(id: &str) -> bool {
        id == DEFAULT_FLOW_INSTANCE_ID
    }

    #[test]
    fn validate_snapshot_rejects_duplicate_memory_topics() {
        let mut bundle = sample_bundle("stream_a", "pipe_a", "mqtt_a", "topic_a");
        bundle
            .resources
            .memory_topics
            .push(bundle.resources.memory_topics[0].clone());

        let err = validate_and_build_snapshot(&bundle, &is_default_instance).unwrap_err();
        assert_eq!(err, "duplicate memory topic in bundle: topic_a");
    }

    #[test]
    fn validate_snapshot_rejects_duplicate_shared_mqtt_keys() {
        let mut bundle = sample_bundle("stream_a", "pipe_a", "mqtt_a", "topic_a");
        bundle
            .resources
            .shared_mqtt_clients
            .push(bundle.resources.shared_mqtt_clients[0].clone());

        let err = validate_and_build_snapshot(&bundle, &is_default_instance).unwrap_err();
        assert_eq!(err, "duplicate shared mqtt client key in bundle: mqtt_a");
    }

    #[test]
    fn validate_snapshot_rejects_duplicate_stream_names() {
        let mut bundle = sample_bundle("stream_a", "pipe_a", "mqtt_a", "topic_a");
        bundle
            .resources
            .streams
            .push(sample_stream_request("stream_a"));

        let err = validate_and_build_snapshot(&bundle, &is_default_instance).unwrap_err();
        assert_eq!(err, "duplicate stream name in bundle: stream_a");
    }

    #[test]
    fn validate_snapshot_rejects_duplicate_pipeline_ids() {
        let mut bundle = sample_bundle("stream_a", "pipe_a", "mqtt_a", "topic_a");
        bundle
            .resources
            .pipelines
            .push(sample_pipeline_request("pipe_a", "stream_a"));

        let err = validate_and_build_snapshot(&bundle, &is_default_instance).unwrap_err();
        assert_eq!(err, "duplicate pipeline id in bundle: pipe_a");
    }

    #[test]
    fn validate_snapshot_rejects_duplicate_pipeline_run_state_entries() {
        let mut bundle = sample_bundle("stream_a", "pipe_a", "mqtt_a", "topic_a");
        bundle
            .resources
            .pipeline_run_states
            .push(bundle.resources.pipeline_run_states[0].clone());

        let err = validate_and_build_snapshot(&bundle, &is_default_instance).unwrap_err();
        assert_eq!(err, "duplicate pipeline_run_state entry in bundle: pipe_a");
    }

    #[test]
    fn validate_snapshot_rejects_pipeline_run_state_for_missing_pipeline() {
        let mut bundle = sample_bundle("stream_a", "pipe_a", "mqtt_a", "topic_a");
        bundle.resources.pipeline_run_states[0].pipeline_id = "pipe_missing".to_string();

        let err = validate_and_build_snapshot(&bundle, &is_default_instance).unwrap_err();
        assert_eq!(
            err,
            "pipeline_run_state references missing pipeline: pipe_missing"
        );
    }

    #[test]
    fn validate_snapshot_rejects_undeclared_flow_instance() {
        let mut bundle = sample_bundle("stream_a", "pipe_a", "mqtt_a", "topic_a");
        bundle.resources.pipelines[0].flow_instance_id = Some("unknown".to_string());

        let err = validate_and_build_snapshot(&bundle, &is_default_instance).unwrap_err();
        assert_eq!(err, "flow instance unknown is not declared by config");
    }

    #[test]
    fn validate_snapshot_normalizes_missing_flow_instance_id_to_default() {
        let mut bundle = sample_bundle("stream_a", "pipe_a", "mqtt_a", "topic_a");
        bundle.resources.pipelines[0].flow_instance_id = None;

        let snapshot =
            validate_and_build_snapshot(&bundle, &is_default_instance).expect("build snapshot");
        let normalized =
            crate::storage_bridge::pipeline_request_from_stored(&snapshot.pipelines[0])
                .expect("decode normalized pipeline");

        assert_eq!(
            normalized.flow_instance_id.as_deref(),
            Some(DEFAULT_FLOW_INSTANCE_ID)
        );
    }

    #[test]
    fn validate_snapshot_rejects_duplicate_udf_name() {
        let mut bundle = sample_bundle("stream_a", "pipe_a", "mqtt_a", "topic_a");
        bundle.resources.udfs = vec![
            ExportUdf {
                name: "my_udf".to_string(),
                wasm_sha256: "aaaa".to_string(),
            },
            ExportUdf {
                name: "my_udf".to_string(),
                wasm_sha256: "bbbb".to_string(),
            },
        ];

        let err = validate_and_build_snapshot(&bundle, &is_default_instance).unwrap_err();
        assert_eq!(err, "duplicate UDF name in bundle: my_udf");
    }

    #[tokio::test]
    async fn import_storage_handler_replaces_snapshot_and_returns_previous_bundle() {
        let dir = tempdir().expect("create tempdir");
        let storage = StorageManager::new(dir.path()).expect("create storage");
        let old_bundle = sample_bundle("stream_old", "pipe_old", "mqtt_old", "topic_old");
        let new_bundle = sample_bundle("stream_new", "pipe_new", "mqtt_new", "topic_new");

        storage
            .replace_metadata_snapshot(
                validate_and_build_snapshot(&old_bundle, &is_default_instance)
                    .expect("build old snapshot"),
            )
            .expect("seed old snapshot");

        let state = AppState::new(
            crate::new_default_flow_instance(),
            storage,
            vec![sample_default_instance_spec()],
        )
        .expect("create app state");

        let tar_gz = build_tar_gz_for_test(&new_bundle, &dir.path().join("wasm_files"));
        let body = axum::body::Bytes::from(tar_gz);

        let response = import_storage_handler(State(state.clone()), body)
            .await
            .into_response();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), 1024 * 1024)
            .await
            .expect("read response body");
        let json: JsonValue = serde_json::from_slice(&body).expect("decode import response");

        assert_eq!(json["applied_to_runtime"], false);
        assert_eq!(json["imported_resource_counts"]["memory_topics"], 1);
        assert_eq!(json["imported_resource_counts"]["udfs"], 0);
        assert!(json["previous_bundle"]["exported_at"].as_u64().unwrap() > 0);

        assert!(state.storage.get_stream("stream_new").unwrap().is_some());
        assert!(state.storage.get_pipeline("pipe_new").unwrap().is_some());
    }

    #[tokio::test]
    async fn import_storage_handler_rejects_invalid_bundle_without_mutating_storage() {
        let dir = tempdir().expect("create tempdir");
        let storage = StorageManager::new(dir.path()).expect("create storage");
        let old_bundle = sample_bundle("stream_old", "pipe_old", "mqtt_old", "topic_old");
        let mut invalid_bundle = sample_bundle("stream_new", "pipe_new", "mqtt_new", "topic_new");
        invalid_bundle
            .resources
            .memory_topics
            .push(invalid_bundle.resources.memory_topics[0].clone());

        storage
            .replace_metadata_snapshot(
                validate_and_build_snapshot(&old_bundle, &is_default_instance)
                    .expect("build old snapshot"),
            )
            .expect("seed old snapshot");

        let state = AppState::new(
            crate::new_default_flow_instance(),
            storage,
            vec![sample_default_instance_spec()],
        )
        .expect("create app state");

        let tar_gz = build_tar_gz_for_test(&invalid_bundle, &dir.path().join("wasm_files"));
        let body = axum::body::Bytes::from(tar_gz);

        let response = import_storage_handler(State(state.clone()), body)
            .await
            .into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        // Verify storage was not mutated
        let bundle_after = build_export_bundle(state.storage.as_ref()).expect("export");
        assert_eq!(
            serde_json::to_value(&bundle_after.resources).unwrap(),
            serde_json::to_value(&old_bundle.resources).unwrap()
        );
    }

    #[test]
    fn validate_snapshot_rejects_pipeline_referencing_missing_stream() {
        let mut bundle = sample_bundle("stream_a", "pipe_a", "mqtt_a", "topic_a");
        bundle.resources.pipelines[0] = sample_pipeline_request("pipe_a", "missing_stream");

        let err = validate_and_build_snapshot(&bundle, &is_default_instance).unwrap_err();
        assert_eq!(
            err,
            "pipeline pipe_a references missing stream: missing_stream"
        );
    }

    #[test]
    fn validate_and_build_snapshot_with_existing_streams_allows_existing_stream_reference() {
        let mut bundle = sample_bundle("stream_a", "pipe_a", "mqtt_a", "topic_a");
        bundle.resources.streams.clear();
        bundle.resources.pipelines[0].sql = "SELECT * FROM stream_a".to_string();

        let existing_stream_names = BTreeSet::from(["stream_a".to_string()]);
        let snapshot = validate_and_build_snapshot_with_existing_streams(
            &bundle,
            &existing_stream_names,
            &is_default_instance,
        )
        .expect("should allow reference to existing stream");

        assert!(snapshot.streams.is_empty());
        assert_eq!(snapshot.pipelines.len(), 1);
    }
}
