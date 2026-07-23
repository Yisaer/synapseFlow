use axum::{extract::State, http::StatusCode, response::IntoResponse};
use serde::Serialize;
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::collections::BTreeSet;
use std::path::Path;
use storage::{
    MetadataExportSnapshot, StoredMemoryTopic, StoredMqttClientConfig, StoredPipelineRunState,
    StoredSchema, StoredUdf,
};
use tokio::sync::TryAcquireError;

use crate::audit::ResourceMutationLog;
use crate::export::{
    ExportBundleV1, ExportMemoryTopic, ExportPipelineRunState, ExportUdf, build_export_bundle,
};
use crate::pipeline::{AppState, CreatePipelineRequest, validate_create_request};
use crate::resource_id::{ResourceIdKind, defaulted_flow_instance_id, validate_resource_id};
use crate::schema::source::{PreparedSchemaTree, resolve_props_from_root};
use crate::storage_bridge;
use crate::stream::{CreateStreamRequest, named_schema_store, schema_registry};

/// Reload all schemas from persistent storage into the in-memory `NamedSchemaStore`.
fn reload_schemas_from_storage(storage: &storage::StorageManager) {
    named_schema_store().clear();
    let _ = crate::storage_bridge::hydrate_schemas_from_storage(storage);
}

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

    let schemas_src = tmp.path().join("schemas");
    let mut schema_tree = match PreparedSchemaTree::prepare(&state.storage, &schemas_src) {
        Ok(tree) => tree,
        Err(err) => {
            audit.log_failure(&err);
            return (StatusCode::BAD_REQUEST, err).into_response();
        }
    };
    let schema_validation_root = schema_tree.staged_root().unwrap_or(&schemas_src);
    let snapshot = match validate_and_build_snapshot(&bundle, Some(schema_validation_root), &|id| {
        state.is_declared_instance(id)
    }) {
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
        if let Err(err) = validate_and_copy_udfs_for_import(
            &snapshot.udfs,
            &wasm_src,
            &state.storage.wasm_files_dir(),
        ) {
            audit.log_failure(&err);
            return (StatusCode::BAD_REQUEST, err).into_response();
        }
    }

    // Copy upload files from the archive. This is best-effort: if the archive
    // has no uploads/ directory, no files are affected.
    let uploads_src = tmp.path().join("uploads");
    if let Err(err) = state.storage.copy_uploads_from_dir(&uploads_src) {
        audit.log_failure(&format!("copy uploads from archive: {err}"));
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to copy uploaded files: {err}"),
        )
            .into_response();
    }
    if let Err(err) = schema_tree.activate() {
        audit.log_failure(&err);
        return (StatusCode::INTERNAL_SERVER_ERROR, err).into_response();
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
        let mut err = format!("replace metadata snapshot in storage: {err}");
        if let Err(rollback_err) = schema_tree.rollback() {
            err = format!("{err}; rollback schema sources: {rollback_err}");
        }
        audit.log_failure(&err);
        return (StatusCode::INTERNAL_SERVER_ERROR, err).into_response();
    }
    schema_tree.finish();

    // Re-hydrate NamedSchemaStore from the newly imported storage
    reload_schemas_from_storage(state.storage.as_ref());

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
    schema_source_root: Option<&Path>,
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
        validate_resource_id(ResourceIdKind::SharedMqttClientKey, &cfg.key)?;
        let key = cfg.key.as_str();
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
        validate_resource_id(ResourceIdKind::SchemaName, &schema.name)?;
        let name = schema.name.as_str();
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

    // Validate each imported schema through the schema registry
    let mut available_schema_names: BTreeSet<String> = BTreeSet::new();
    for stored in &schemas {
        let mut props: JsonMap<String, JsonValue> = serde_json::from_str(&stored.props_json)
            .map_err(|err| format!("schema {} has invalid props JSON: {err}", stored.name))?;
        if let Some(root) = schema_source_root {
            resolve_props_from_root(root, &stored.name, &stored.schema_type, &mut props)
                .map_err(|err| format!("schema {} has invalid source: {err}", stored.name))?;
        }
        match schema_registry().parse(&stored.schema_type, &stored.name, &props) {
            Ok(_) => {}
            Err(err) => {
                return Err(format!("schema {} is invalid: {err}", stored.name));
            }
        }
        available_schema_names.insert(stored.name.clone());
    }

    let mut streams = Vec::with_capacity(bundle.resources.streams.len());
    let mut bundle_stream_names = BTreeSet::new();
    let mut available_stream_names = existing_stream_names.clone();

    for req in &bundle.resources.streams {
        validate_stream_request(req, &mut bundle_stream_names)?;
        if let Some(ref_name) = &req.schema.r#ref {
            let trimmed = ref_name.trim();
            if !available_schema_names.contains(trimmed) {
                return Err(format!(
                    "stream {} references schema '{}' which is not present in the import bundle",
                    req.name, trimmed
                ));
            }
        }
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
        validate_resource_id(ResourceIdKind::UdfName, &udf.name)?;
        let name = udf.name.as_str();
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
pub(crate) fn validate_and_copy_udfs_for_import(
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
pub(crate) fn validate_and_copy_udfs_for_import(
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
    schema_source_root: Option<&Path>,
    is_declared_instance: &F,
) -> Result<MetadataExportSnapshot, String>
where
    F: Fn(&str) -> bool,
{
    validate_and_build_snapshot_inner(
        bundle,
        &BTreeSet::new(),
        schema_source_root,
        is_declared_instance,
    )
}

pub(crate) fn validate_and_build_snapshot_with_existing_streams<F>(
    bundle: &ExportBundleV1,
    existing_stream_names: &BTreeSet<String>,
    schema_source_root: Option<&Path>,
    is_declared_instance: &F,
) -> Result<MetadataExportSnapshot, String>
where
    F: Fn(&str) -> bool,
{
    validate_and_build_snapshot_inner(
        bundle,
        existing_stream_names,
        schema_source_root,
        is_declared_instance,
    )
}

fn validate_memory_topic(
    topic: &ExportMemoryTopic,
    names: &mut BTreeSet<String>,
) -> Result<(), String> {
    validate_resource_id(ResourceIdKind::MemoryTopic, &topic.topic)?;
    let topic_name = topic.topic.as_str();
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
    validate_resource_id(ResourceIdKind::StreamName, &req.name)?;
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
        Some(defaulted_flow_instance_id(req.flow_instance_id.as_deref())?);
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
    validate_resource_id(ResourceIdKind::PipelineId, &run_state.pipeline_id)?;
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
                    username: None,
                    password: None,
                    resolved_password: None,
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

    fn add_file_backed_proto_schema(bundle: &mut ExportBundleV1, schemas_root: &std::path::Path) {
        let schema_dir = schemas_root.join("proto/simple_schema");
        std::fs::create_dir_all(&schema_dir).expect("create proto schema directory");
        std::fs::write(
            schema_dir.join("simple.proto"),
            b"syntax = \"proto3\"; message Simple { int64 value = 1; }",
        )
        .expect("write proto schema");
        bundle.resources.schemas.push(crate::export::ExportSchema {
            name: "simple_schema".to_string(),
            schema_type: "proto".to_string(),
            props: serde_json::from_value(serde_json::json!({
                "proto_path": "simple.proto",
                "message_type": "Simple"
            }))
            .expect("proto props"),
        });
        bundle.resources.streams[0].schema =
            serde_json::from_value(serde_json::json!({"ref": "simple_schema"}))
                .expect("schema ref");
    }

    fn build_tar_gz_for_test(
        bundle: &ExportBundleV1,
        wasm_dir: &std::path::Path,
        uploads_dir: &std::path::Path,
        schemas_dir: &std::path::Path,
    ) -> Vec<u8> {
        let udf_shas: Vec<String> = bundle
            .resources
            .udfs
            .iter()
            .map(|u| u.wasm_sha256.clone())
            .collect();
        crate::export::build_tar_gz(bundle, &udf_shas, wasm_dir, uploads_dir, schemas_dir)
            .expect("build test export")
    }

    #[cfg(unix)]
    fn build_tar_gz_with_symlinked_proto(
        bundle: &ExportBundleV1,
        target: &std::path::Path,
    ) -> Vec<u8> {
        let metadata = serde_json::to_vec(bundle).expect("serialize metadata");
        let mut tar_gz = Vec::new();
        {
            let gz = flate2::write::GzEncoder::new(&mut tar_gz, flate2::Compression::default());
            let mut builder = tar::Builder::new(gz);

            let mut metadata_header = tar::Header::new_gnu();
            metadata_header.set_mode(0o600);
            metadata_header.set_size(metadata.len() as u64);
            builder
                .append_data(&mut metadata_header, "metadata.json", metadata.as_slice())
                .expect("append metadata");

            let mut link_header = tar::Header::new_gnu();
            link_header.set_entry_type(tar::EntryType::Symlink);
            link_header.set_mode(0o777);
            link_header.set_size(0);
            builder
                .append_link(
                    &mut link_header,
                    "schemas/proto/simple_schema/simple.proto",
                    target,
                )
                .expect("append schema symlink");

            builder
                .into_inner()
                .expect("finish tar")
                .finish()
                .expect("finish gzip");
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

        let err = validate_and_build_snapshot(&bundle, None, &is_default_instance).unwrap_err();
        assert_eq!(err, "duplicate memory topic in bundle: topic_a");
    }

    #[test]
    fn validate_snapshot_rejects_duplicate_shared_mqtt_keys() {
        let mut bundle = sample_bundle("stream_a", "pipe_a", "mqtt_a", "topic_a");
        bundle
            .resources
            .shared_mqtt_clients
            .push(bundle.resources.shared_mqtt_clients[0].clone());

        let err = validate_and_build_snapshot(&bundle, None, &is_default_instance).unwrap_err();
        assert_eq!(err, "duplicate shared mqtt client key in bundle: mqtt_a");
    }

    #[test]
    fn validate_snapshot_rejects_duplicate_stream_names() {
        let mut bundle = sample_bundle("stream_a", "pipe_a", "mqtt_a", "topic_a");
        bundle
            .resources
            .streams
            .push(sample_stream_request("stream_a"));

        let err = validate_and_build_snapshot(&bundle, None, &is_default_instance).unwrap_err();
        assert_eq!(err, "duplicate stream name in bundle: stream_a");
    }

    #[test]
    fn validate_snapshot_rejects_duplicate_pipeline_ids() {
        let mut bundle = sample_bundle("stream_a", "pipe_a", "mqtt_a", "topic_a");
        bundle
            .resources
            .pipelines
            .push(sample_pipeline_request("pipe_a", "stream_a"));

        let err = validate_and_build_snapshot(&bundle, None, &is_default_instance).unwrap_err();
        assert_eq!(err, "duplicate pipeline id in bundle: pipe_a");
    }

    #[test]
    fn validate_snapshot_rejects_duplicate_pipeline_run_state_entries() {
        let mut bundle = sample_bundle("stream_a", "pipe_a", "mqtt_a", "topic_a");
        bundle
            .resources
            .pipeline_run_states
            .push(bundle.resources.pipeline_run_states[0].clone());

        let err = validate_and_build_snapshot(&bundle, None, &is_default_instance).unwrap_err();
        assert_eq!(err, "duplicate pipeline_run_state entry in bundle: pipe_a");
    }

    #[test]
    fn validate_snapshot_rejects_pipeline_run_state_for_missing_pipeline() {
        let mut bundle = sample_bundle("stream_a", "pipe_a", "mqtt_a", "topic_a");
        bundle.resources.pipeline_run_states[0].pipeline_id = "pipe_missing".to_string();

        let err = validate_and_build_snapshot(&bundle, None, &is_default_instance).unwrap_err();
        assert_eq!(
            err,
            "pipeline_run_state references missing pipeline: pipe_missing"
        );
    }

    #[test]
    fn validate_snapshot_rejects_undeclared_flow_instance() {
        let mut bundle = sample_bundle("stream_a", "pipe_a", "mqtt_a", "topic_a");
        bundle.resources.pipelines[0].flow_instance_id = Some("unknown".to_string());

        let err = validate_and_build_snapshot(&bundle, None, &is_default_instance).unwrap_err();
        assert_eq!(err, "flow instance unknown is not declared by config");
    }

    #[test]
    fn validate_snapshot_normalizes_missing_flow_instance_id_to_default() {
        let mut bundle = sample_bundle("stream_a", "pipe_a", "mqtt_a", "topic_a");
        bundle.resources.pipelines[0].flow_instance_id = None;

        let snapshot = validate_and_build_snapshot(&bundle, None, &is_default_instance)
            .expect("build snapshot");
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

        let err = validate_and_build_snapshot(&bundle, None, &is_default_instance).unwrap_err();
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
                validate_and_build_snapshot(&old_bundle, None, &is_default_instance)
                    .expect("build old snapshot"),
            )
            .expect("seed old snapshot");

        let state = AppState::new(
            crate::new_default_flow_instance(),
            storage,
            vec![sample_default_instance_spec()],
            0,
        )
        .expect("create app state");

        let tar_gz = build_tar_gz_for_test(
            &new_bundle,
            &dir.path().join("wasm_files"),
            &dir.path().join("uploads"),
            &dir.path().join("schemas"),
        );
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
                validate_and_build_snapshot(&old_bundle, None, &is_default_instance)
                    .expect("build old snapshot"),
            )
            .expect("seed old snapshot");

        let state = AppState::new(
            crate::new_default_flow_instance(),
            storage,
            vec![sample_default_instance_spec()],
            0,
        )
        .expect("create app state");

        let tar_gz = build_tar_gz_for_test(
            &invalid_bundle,
            &dir.path().join("wasm_files"),
            &dir.path().join("uploads"),
            &dir.path().join("schemas"),
        );
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
    fn validate_snapshot_rejects_invalid_stream_name() {
        let mut bundle = sample_bundle("stream_a", "pipe_a", "mqtt_a", "topic_a");
        bundle.resources.streams[0].name = "bad-stream".to_string();

        let err = validate_and_build_snapshot(&bundle, None, &is_default_instance).unwrap_err();
        assert!(err.contains("stream name"), "unexpected error: {err}");
        assert!(err.contains("invalid character"), "unexpected error: {err}");
    }

    #[test]
    fn validate_snapshot_rejects_invalid_pipeline_id() {
        let mut bundle = sample_bundle("stream_a", "pipe_a", "mqtt_a", "topic_a");
        bundle.resources.pipelines[0].id = "bad.pipe".to_string();

        let err = validate_and_build_snapshot(&bundle, None, &is_default_instance).unwrap_err();
        assert!(err.contains("pipeline id"), "unexpected error: {err}");
    }

    #[test]
    fn validate_snapshot_rejects_invalid_flow_instance_id_before_declared_lookup() {
        let mut bundle = sample_bundle("stream_a", "pipe_a", "mqtt_a", "topic_a");
        // Use a syntactically invalid id (hyphen): grammar must reject it before
        // the declared-instance lookup runs, so the error is about the grammar,
        // not "not declared by config".
        bundle.resources.pipelines[0].flow_instance_id = Some("bad-fi".to_string());

        let err = validate_and_build_snapshot(&bundle, None, &is_default_instance).unwrap_err();
        assert!(err.contains("flow_instance_id"), "unexpected error: {err}");
        assert!(
            !err.contains("not declared by config"),
            "grammar must be checked before declared lookup: {err}"
        );
    }

    #[test]
    fn validate_snapshot_rejects_invalid_shared_mqtt_key() {
        let mut bundle = sample_bundle("stream_a", "pipe_a", "mqtt_a", "topic_a");
        bundle.resources.shared_mqtt_clients[0].key = "bad/key".to_string();

        let err = validate_and_build_snapshot(&bundle, None, &is_default_instance).unwrap_err();
        assert!(
            err.contains("shared mqtt client key"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn validate_snapshot_rejects_invalid_memory_topic() {
        let mut bundle = sample_bundle("stream_a", "pipe_a", "mqtt_a", "topic_a");
        bundle.resources.memory_topics[0].topic = "bad-topic".to_string();

        let err = validate_and_build_snapshot(&bundle, None, &is_default_instance).unwrap_err();
        assert!(err.contains("memory topic"), "unexpected error: {err}");
    }

    #[test]
    fn validate_snapshot_rejects_invalid_schema_name() {
        let mut bundle = sample_bundle("stream_a", "pipe_a", "mqtt_a", "topic_a");
        bundle.resources.schemas.push(StoredSchemaSample::invalid());

        let err = validate_and_build_snapshot(&bundle, None, &is_default_instance).unwrap_err();
        assert!(err.contains("schema name"), "unexpected error: {err}");
    }

    #[test]
    fn validate_snapshot_rejects_invalid_udf_name() {
        let mut bundle = sample_bundle("stream_a", "pipe_a", "mqtt_a", "topic_a");
        bundle.resources.udfs = vec![ExportUdf {
            name: "bad-udf".to_string(),
            wasm_sha256: "aaaa".to_string(),
        }];

        let err = validate_and_build_snapshot(&bundle, None, &is_default_instance).unwrap_err();
        assert!(err.contains("UDF name"), "unexpected error: {err}");
    }

    /// Helper producing an `ExportSchema` with an invalid (hyphenated) name.
    struct StoredSchemaSample;
    impl StoredSchemaSample {
        fn invalid() -> crate::export::ExportSchema {
            crate::export::ExportSchema {
                name: "bad-schema".to_string(),
                schema_type: "json".to_string(),
                props: serde_json::Map::new(),
            }
        }
    }

    #[test]
    fn validate_snapshot_rejects_pipeline_referencing_missing_stream() {
        let mut bundle = sample_bundle("stream_a", "pipe_a", "mqtt_a", "topic_a");
        bundle.resources.pipelines[0] = sample_pipeline_request("pipe_a", "missing_stream");

        let err = validate_and_build_snapshot(&bundle, None, &is_default_instance).unwrap_err();
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
            None,
            &is_default_instance,
        )
        .expect("should allow reference to existing stream");

        assert!(snapshot.streams.is_empty());
        assert_eq!(snapshot.pipelines.len(), 1);
    }

    #[tokio::test]
    async fn export_import_uploads_roundtrip() {
        let dir = tempdir().unwrap();
        let storage = StorageManager::new(dir.path()).unwrap();

        // Seed storage with a stream and an upload file
        let bundle = sample_bundle("stream_1", "pipe_1", "mqtt_1", "topic_1");
        storage
            .replace_metadata_snapshot(
                validate_and_build_snapshot(&bundle, None, &is_default_instance)
                    .expect("build snapshot"),
            )
            .expect("seed snapshot");
        storage
            .save_upload("ca-cert.pem", b"certificate data")
            .unwrap();

        // Build tar.gz (same as export handler)
        let exported_bundle = crate::export::build_export_bundle(&storage).unwrap();
        let tar_gz = build_tar_gz_for_test(
            &exported_bundle,
            &storage.wasm_files_dir(),
            &storage.uploads_dir(),
            &storage.schemas_dir(),
        );

        // Create fresh storage and import
        let dir2 = tempdir().unwrap();
        let storage2 = StorageManager::new(dir2.path()).unwrap();
        let state = AppState::new(
            crate::new_default_flow_instance(),
            storage2,
            vec![sample_default_instance_spec()],
            0,
        )
        .unwrap();

        let body = axum::body::Bytes::from(tar_gz);
        let response = import_storage_handler(State(state.clone()), body)
            .await
            .into_response();
        assert_eq!(response.status(), StatusCode::OK);

        // Verify upload file survived
        let list = state.storage.list_uploads().unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].name, "ca-cert.pem");
        assert_eq!(list[0].size_bytes, 16);
        let data = state.storage.read_upload("ca-cert.pem").unwrap();
        assert_eq!(data, b"certificate data");

        // Verify stream also survived
        assert!(state.storage.get_stream("stream_1").unwrap().is_some());
    }

    #[tokio::test]
    async fn export_import_nested_uploads_roundtrip() {
        let dir = tempdir().unwrap();
        let storage = StorageManager::new(dir.path()).unwrap();

        let bundle = sample_bundle("stream_1", "pipe_1", "mqtt_1", "topic_1");
        storage
            .replace_metadata_snapshot(
                validate_and_build_snapshot(&bundle, None, &is_default_instance)
                    .expect("build snapshot"),
            )
            .expect("seed snapshot");
        storage
            .save_upload("proto/sensor.proto", b"message Sensor {}")
            .unwrap();
        storage
            .save_upload("proto/common/types.proto", b"message T {}")
            .unwrap();

        let exported_bundle = crate::export::build_export_bundle(&storage).unwrap();
        let tar_gz = build_tar_gz_for_test(
            &exported_bundle,
            &storage.wasm_files_dir(),
            &storage.uploads_dir(),
            &storage.schemas_dir(),
        );

        let dir2 = tempdir().unwrap();
        let storage2 = StorageManager::new(dir2.path()).unwrap();
        let state = AppState::new(
            crate::new_default_flow_instance(),
            storage2,
            vec![sample_default_instance_spec()],
            0,
        )
        .unwrap();

        let body = axum::body::Bytes::from(tar_gz);
        let response = import_storage_handler(State(state.clone()), body)
            .await
            .into_response();
        assert_eq!(response.status(), StatusCode::OK);

        let list = state.storage.list_uploads().unwrap();
        assert_eq!(list.len(), 2);
        assert_eq!(list[0].name, "proto/common/types.proto");
        assert_eq!(list[1].name, "proto/sensor.proto");
        assert_eq!(
            state.storage.read_upload("proto/sensor.proto").unwrap(),
            b"message Sensor {}"
        );
    }

    #[tokio::test]
    async fn export_import_file_backed_proto_schema_roundtrip() {
        let source_dir = tempdir().unwrap();
        let source_storage = StorageManager::new(source_dir.path()).unwrap();
        let mut bundle = sample_bundle("stream_1", "pipe_1", "mqtt_1", "topic_1");
        add_file_backed_proto_schema(&mut bundle, &source_storage.schemas_dir());
        source_storage
            .replace_metadata_snapshot(
                validate_and_build_snapshot(
                    &bundle,
                    Some(&source_storage.schemas_dir()),
                    &is_default_instance,
                )
                .expect("build source snapshot"),
            )
            .expect("seed source snapshot");

        let exported_bundle = crate::export::build_export_bundle(&source_storage).unwrap();
        assert_eq!(
            exported_bundle.resources.schemas[0]
                .props
                .get("proto_path")
                .and_then(JsonValue::as_str),
            Some("simple.proto")
        );
        let tar_gz = build_tar_gz_for_test(
            &exported_bundle,
            &source_storage.wasm_files_dir(),
            &source_storage.uploads_dir(),
            &source_storage.schemas_dir(),
        );

        let target_dir = tempdir().unwrap();
        let target_storage = StorageManager::new(target_dir.path()).unwrap();
        let state = AppState::new(
            crate::new_default_flow_instance(),
            target_storage,
            vec![sample_default_instance_spec()],
            0,
        )
        .unwrap();

        let response =
            import_storage_handler(State(state.clone()), axum::body::Bytes::from(tar_gz))
                .await
                .into_response();
        assert_eq!(response.status(), StatusCode::OK);

        let stored = state
            .storage
            .get_schema("simple_schema")
            .unwrap()
            .expect("stored schema");
        let props: JsonMap<String, JsonValue> =
            serde_json::from_str(&stored.props_json).expect("stored props");
        assert_eq!(
            props.get("proto_path").and_then(JsonValue::as_str),
            Some("simple.proto")
        );
        assert!(
            state
                .storage
                .schemas_dir()
                .join("proto/simple_schema/simple.proto")
                .is_file()
        );
        assert!(named_schema_store().get("simple_schema").is_some());
        assert!(state.storage.get_stream("stream_1").unwrap().is_some());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn import_rejects_symlinked_schema_before_parsing() {
        let source_dir = tempdir().unwrap();
        let source_storage = StorageManager::new(source_dir.path()).unwrap();
        let mut bundle = sample_bundle("stream_1", "pipe_1", "mqtt_1", "topic_1");
        add_file_backed_proto_schema(&mut bundle, &source_storage.schemas_dir());

        let external_dir = tempdir().unwrap();
        let external_proto = external_dir.path().join("external.proto");
        std::fs::write(
            &external_proto,
            b"syntax = \"proto3\"; message Simple { int64 value = 1; }",
        )
        .expect("write external proto");
        let tar_gz = build_tar_gz_with_symlinked_proto(&bundle, &external_proto);

        let target_dir = tempdir().unwrap();
        let target_storage = StorageManager::new(target_dir.path()).unwrap();
        let state = AppState::new(
            crate::new_default_flow_instance(),
            target_storage,
            vec![sample_default_instance_spec()],
            0,
        )
        .unwrap();

        let response =
            import_storage_handler(State(state.clone()), axum::body::Bytes::from(tar_gz))
                .await
                .into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read response body");
        let body = String::from_utf8(body.to_vec()).expect("response body is UTF-8");
        assert!(
            body.contains("is not a regular file or directory"),
            "{body}"
        );
        assert!(state.storage.list_schemas().unwrap().is_empty());
        assert!(state.storage.list_streams().unwrap().is_empty());
    }
}
