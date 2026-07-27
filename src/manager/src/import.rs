use axum::{extract::State, http::StatusCode, response::IntoResponse};
use serde::Serialize;
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::collections::{BTreeSet, HashSet};
use std::io::{Cursor, Read};
use std::path::Path;
use storage::{
    MetadataExportSnapshot, StoredMemoryTopic, StoredMqttClientConfig, StoredPipelineRunState,
    StoredSchema, StoredUdf,
};
use tokio::sync::TryAcquireError;
use zip::ZipArchive;

use crate::audit::ResourceMutationLog;
use crate::export::{
    ExportMemoryTopic, ExportResources, ExportUdf, ResourceManifestV1, build_export_resources,
    validate_resource_manifest,
};
use crate::pipeline::{AppState, CreatePipelineRequest, validate_create_request};
use crate::resource_id::{ResourceIdKind, defaulted_flow_instance_id, validate_resource_id};
use crate::schema::source::{PreparedSchemaTree, resolve_props_from_root};
use crate::storage_bridge;
use crate::stream::{CreateStreamRequest, named_schema_store, schema_registry};

const MAX_ARCHIVE_ENTRIES: usize = 4096;
const MAX_ARCHIVE_FILE_SIZE: u64 = 512 * 1024 * 1024;
const MAX_ARCHIVE_TOTAL_SIZE: u64 = 512 * 1024 * 1024;
pub(crate) const MAX_ARCHIVE_BODY_SIZE: usize = 512 * 1024 * 1024;

/// Reload all schemas from persistent storage into the in-memory `NamedSchemaStore`.
fn reload_schemas_from_storage(storage: &storage::StorageManager) {
    named_schema_store().clear();
    let _ = crate::storage_bridge::hydrate_schemas_from_storage(storage);
}

#[derive(Serialize)]
pub struct ImportStorageResponse {
    pub applied_to_runtime: bool,
    pub imported_resource_counts: ImportResourceCounts,
    pub previous_resources: ExportResources,
}

#[derive(Serialize)]
pub struct ImportResourceCounts {
    pub memory_topics: usize,
    pub shared_mqtt_clients: usize,
    pub schemas: usize,
    pub streams: usize,
    pub pipelines: usize,
    pub udfs: usize,
    pub tables: usize,
}

/// Accept a ZIP body via `axum::body::Bytes`.
pub async fn import_storage_handler(
    State(state): State<AppState>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let audit = ResourceMutationLog::new("storage", "import", "zip_bundle", None);
    let _import_export_permit = match state.try_acquire_import_export_op() {
        Ok(permit) => permit,
        Err(TryAcquireError::NoPermits) => return import_export_busy_response(),
        Err(TryAcquireError::Closed) => {
            let err = "import/export operation guard closed".to_string();
            audit.log_failure(&err);
            return (StatusCode::INTERNAL_SERVER_ERROR, err).into_response();
        }
    };

    let tmp = match tempfile::tempdir() {
        Ok(d) => d,
        Err(e) => {
            let err = format!("create temp dir: {e}");
            audit.log_failure(&err);
            return (StatusCode::INTERNAL_SERVER_ERROR, err).into_response();
        }
    };

    if let Err(err) = extract_zip(&body, tmp.path()) {
        audit.log_failure(&err);
        return (StatusCode::BAD_REQUEST, err).into_response();
    }

    let manifest_path = tmp.path().join("manifest.json");
    let manifest_bytes = match std::fs::read(&manifest_path) {
        Ok(b) => b,
        Err(e) => {
            let err = format!("read manifest.json from archive: {e}");
            audit.log_failure(&err);
            return (StatusCode::BAD_REQUEST, err).into_response();
        }
    };

    let manifest: ResourceManifestV1 = match serde_json::from_slice(&manifest_bytes) {
        Ok(b) => b,
        Err(e) => {
            let err = format!("parse manifest.json: {e}");
            audit.log_failure(&err);
            return (StatusCode::BAD_REQUEST, err).into_response();
        }
    };
    if let Err(err) = validate_resource_manifest(&manifest) {
        audit.log_failure(&err);
        return (StatusCode::BAD_REQUEST, err).into_response();
    }

    let schemas_src = tmp.path().join("schemas");
    let mut schema_tree = match PreparedSchemaTree::prepare(&state.storage, &schemas_src) {
        Ok(tree) => tree,
        Err(err) => {
            audit.log_failure(&err);
            return (StatusCode::BAD_REQUEST, err).into_response();
        }
    };
    let schema_validation_root = schema_tree.staged_root().unwrap_or(&schemas_src);
    let snapshot =
        match validate_and_build_snapshot(&manifest, Some(schema_validation_root), &|id| {
            state.is_declared_instance(id)
        }) {
            Ok(snapshot) => snapshot,
            Err(err) => {
                audit.log_failure(&err);
                return (StatusCode::BAD_REQUEST, err).into_response();
            }
        };

    // Validate UDF modules into import staging before installing managed files.
    let udf_count = snapshot.udfs.len();
    let validated_wasm_dir = tmp.path().join(".validated-wasm");
    if let Err(err) = std::fs::create_dir(&validated_wasm_dir) {
        let err = format!("create validated WASM staging directory: {err}");
        audit.log_failure(&err);
        return (StatusCode::INTERNAL_SERVER_ERROR, err).into_response();
    }
    if udf_count > 0 {
        let wasm_src = tmp.path().join("wasm_files");
        if let Err(err) =
            validate_and_copy_udfs_for_import(&snapshot.udfs, &wasm_src, &validated_wasm_dir)
        {
            audit.log_failure(&err);
            return (StatusCode::BAD_REQUEST, err).into_response();
        }
    }

    if let Err(err) = schema_tree.activate() {
        audit.log_failure(&err);
        return (StatusCode::INTERNAL_SERVER_ERROR, err).into_response();
    }
    let installed_wasm =
        match install_validated_wasm(&validated_wasm_dir, &state.storage.wasm_files_dir()) {
            Ok(paths) => paths,
            Err(err) => {
                let mut err = err;
                if let Err(rollback_err) = schema_tree.rollback() {
                    err = format!("{err}; rollback schema sources: {rollback_err}");
                }
                audit.log_failure(&err);
                return (StatusCode::INTERNAL_SERVER_ERROR, err).into_response();
            }
        };

    let previous_resources = match build_export_resources(state.storage.as_ref()) {
        Ok(resources) => resources,
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
        udfs: udf_count,
        tables: snapshot.tables.len(),
    };
    let referenced_wasm = snapshot
        .udfs
        .iter()
        .map(|udf| format!("{}.wasm", udf.wasm_sha256))
        .collect::<BTreeSet<_>>();

    if let Err(err) = state.storage.replace_metadata_snapshot(snapshot) {
        let mut err = format!("replace metadata snapshot in storage: {err}");
        for path in &installed_wasm {
            let _ = std::fs::remove_file(path);
        }
        if let Err(rollback_err) = schema_tree.rollback() {
            err = format!("{err}; rollback schema sources: {rollback_err}");
        }
        audit.log_failure(&err);
        return (StatusCode::INTERNAL_SERVER_ERROR, err).into_response();
    }
    schema_tree.finish();
    if let Err(err) = cleanup_unreferenced_wasm(&state.storage.wasm_files_dir(), &referenced_wasm) {
        tracing::warn!(error = %err, "failed to clean unreferenced imported WASM files");
    }

    // Re-hydrate NamedSchemaStore from the newly imported storage
    reload_schemas_from_storage(state.storage.as_ref());

    audit.log_success();
    (
        StatusCode::OK,
        axum::Json(ImportStorageResponse {
            applied_to_runtime: false,
            imported_resource_counts,
            previous_resources,
        }),
    )
        .into_response()
}

fn install_validated_wasm(
    source: &Path,
    destination: &Path,
) -> Result<Vec<std::path::PathBuf>, String> {
    let mut installed = Vec::new();
    for entry in std::fs::read_dir(source)
        .map_err(|err| format!("read validated WASM staging directory: {err}"))?
    {
        let entry = entry.map_err(|err| format!("read validated WASM entry: {err}"))?;
        let target = destination.join(entry.file_name());
        if !target.exists() {
            if let Err(err) = std::fs::rename(entry.path(), &target) {
                for installed_path in &installed {
                    let _ = std::fs::remove_file(installed_path);
                }
                return Err(format!(
                    "install validated WASM `{}`: {err}",
                    target.display()
                ));
            }
            installed.push(target);
        }
    }
    Ok(installed)
}

fn cleanup_unreferenced_wasm(wasm_dir: &Path, referenced: &BTreeSet<String>) -> Result<(), String> {
    for entry in
        std::fs::read_dir(wasm_dir).map_err(|err| format!("read managed WASM directory: {err}"))?
    {
        let entry = entry.map_err(|err| format!("read managed WASM entry: {err}"))?;
        let file_type = entry
            .file_type()
            .map_err(|err| format!("inspect managed WASM entry: {err}"))?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if file_type.is_file() && name.ends_with(".wasm") && !referenced.contains(&name) {
            std::fs::remove_file(entry.path())
                .map_err(|err| format!("remove unreferenced WASM `{name}`: {err}"))?;
        }
    }
    Ok(())
}

fn extract_zip(data: &[u8], dest: &std::path::Path) -> Result<(), String> {
    extract_zip_with_limits(
        data,
        dest,
        MAX_ARCHIVE_ENTRIES,
        MAX_ARCHIVE_FILE_SIZE,
        MAX_ARCHIVE_TOTAL_SIZE,
    )
}

fn extract_zip_with_limits(
    data: &[u8],
    dest: &std::path::Path,
    max_entries: usize,
    max_file_size: u64,
    max_total_size: u64,
) -> Result<(), String> {
    let mut archive =
        ZipArchive::new(Cursor::new(data)).map_err(|e| format!("open import ZIP: {e}"))?;
    let declared_entries = declared_zip_entry_count(data)?;
    if declared_entries != archive.len() {
        return Err(
            "import ZIP contains duplicate entries or an inconsistent central directory"
                .to_string(),
        );
    }
    if archive.len() > max_entries {
        return Err(format!(
            "import ZIP has too many entries: {} > {max_entries}",
            archive.len()
        ));
    }

    let mut paths = HashSet::with_capacity(archive.len());
    let mut total_size = 0u64;
    for index in 0..archive.len() {
        let file = archive
            .by_index(index)
            .map_err(|e| format!("read import ZIP entry {index}: {e}"))?;
        let name = file.name();
        if name.contains('\\') {
            return Err(format!("import ZIP entry `{name}` contains a backslash"));
        }
        let path = file
            .enclosed_name()
            .ok_or_else(|| format!("import ZIP entry `{name}` has an unsafe path"))?;
        if path.as_os_str().is_empty() {
            return Err("import ZIP contains an empty entry path".to_string());
        }
        if !paths.insert(path.clone()) {
            return Err(format!(
                "import ZIP contains duplicate entry `{}`",
                path.display()
            ));
        }
        if file.is_symlink() {
            return Err(format!(
                "import ZIP entry `{}` is not a regular file or directory",
                path.display()
            ));
        }
        if let Some(mode) = file.unix_mode()
            && mode & 0o170000 != 0
            && mode & 0o170000 != 0o040000
            && mode & 0o170000 != 0o100000
        {
            return Err(format!(
                "import ZIP entry `{}` is not a regular file or directory",
                path.display()
            ));
        }
        if !file.is_dir() {
            if file.size() > max_file_size {
                return Err(format!(
                    "import ZIP entry `{}` exceeds {max_file_size} bytes",
                    path.display()
                ));
            }
            total_size = total_size
                .checked_add(file.size())
                .ok_or_else(|| "import ZIP uncompressed size overflow".to_string())?;
            if total_size > max_total_size {
                return Err(format!(
                    "import ZIP uncompressed size exceeds {max_total_size} bytes"
                ));
            }
        }
    }

    for index in 0..archive.len() {
        let mut file = archive
            .by_index(index)
            .map_err(|e| format!("read import ZIP entry {index}: {e}"))?;
        let path = file
            .enclosed_name()
            .ok_or_else(|| format!("import ZIP entry `{}` has an unsafe path", file.name()))?;
        let target = dest.join(&path);
        if file.is_dir() {
            std::fs::create_dir_all(&target).map_err(|e| {
                format!(
                    "create directory for import ZIP entry `{}`: {e}",
                    path.display()
                )
            })?;
            continue;
        }
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                format!(
                    "create parent for import ZIP entry `{}`: {e}",
                    path.display()
                )
            })?;
        }
        let mut output = std::fs::File::create(&target)
            .map_err(|e| format!("create import ZIP entry `{}`: {e}", path.display()))?;
        let copied = std::io::copy(&mut file.by_ref().take(max_file_size + 1), &mut output)
            .map_err(|e| format!("extract import ZIP entry `{}`: {e}", path.display()))?;
        if copied != file.size() || copied > max_file_size {
            return Err(format!(
                "import ZIP entry `{}` size changed while extracting",
                path.display()
            ));
        }
    }
    Ok(())
}

fn declared_zip_entry_count(data: &[u8]) -> Result<usize, String> {
    const EOCD_LEN: usize = 22;
    const MAX_COMMENT_LEN: usize = u16::MAX as usize;
    if data.len() < EOCD_LEN {
        return Err("import ZIP is missing its end-of-central-directory record".to_string());
    }

    let search_start = data.len().saturating_sub(EOCD_LEN + MAX_COMMENT_LEN);
    for offset in (search_start..=data.len() - EOCD_LEN).rev() {
        if data[offset..offset + 4] != *b"PK\x05\x06" {
            continue;
        }
        let read_u16 = |relative: usize| {
            u16::from_le_bytes([data[offset + relative], data[offset + relative + 1]])
        };
        let comment_len = usize::from(read_u16(20));
        if offset + EOCD_LEN + comment_len != data.len() {
            continue;
        }
        let disk_number = read_u16(4);
        let central_directory_disk = read_u16(6);
        let entries_on_disk = read_u16(8);
        let total_entries = read_u16(10);
        if disk_number != 0 || central_directory_disk != 0 || entries_on_disk != total_entries {
            return Err("multi-disk import ZIP archives are not supported".to_string());
        }
        if total_entries == u16::MAX {
            return Err("ZIP64 import archives are not supported".to_string());
        }
        return Ok(usize::from(total_entries));
    }
    Err("import ZIP has an invalid end-of-central-directory record".to_string())
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
    bundle: &ResourceManifestV1,
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
    let mut pipeline_run_states = Vec::with_capacity(bundle.resources.pipelines.len());
    let mut pipeline_ids = BTreeSet::new();
    for pipeline in &bundle.resources.pipelines {
        let normalized = normalize_pipeline_request(&pipeline.definition)?;
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
        pipeline_run_states.push(StoredPipelineRunState {
            pipeline_id: id,
            desired_state: pipeline.run_state.clone(),
        });
    }

    let udfs: Vec<StoredUdf> = validate_import_udfs(&bundle.resources.udfs)?;

    let mut tables = Vec::with_capacity(bundle.resources.tables.len());
    let mut table_ids = BTreeSet::new();
    for table in &bundle.resources.tables {
        let req = &table.definition;
        validate_resource_id(ResourceIdKind::StreamName, &req.name)?;
        let id = req.name.clone();
        if !table_ids.insert(id.clone()) {
            return Err(format!("duplicate table name in bundle: {id}"));
        }
        // Validate that the table type is supported by parsing the props
        use crate::table::build_table_props;
        build_table_props(&req.table_type, &req.props)?;
        if let Some(ref_name) = &req.schema.r#ref {
            let trimmed = ref_name.trim();
            if !available_schema_names.contains(trimmed) {
                return Err(format!(
                    "table {} references schema '{}' which is not present in the import bundle",
                    req.name, trimmed
                ));
            }
        }
        let raw_json = serde_json::to_string(req)
            .map_err(|err| format!("serialize table {}: {err}", req.name))?;
        tables.push(storage::StoredTable { id, raw_json });
    }

    Ok(MetadataExportSnapshot {
        streams,
        schemas,
        pipelines,
        pipeline_run_states,
        mqtt_configs,
        memory_topics,
        udfs,
        tables,
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
    bundle: &ResourceManifestV1,
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
    use crate::export::{
        ExportPipeline, RESOURCE_DIRECTORY_FORMAT_VERSION, build_resource_manifest,
    };
    use crate::instances::{DEFAULT_FLOW_INSTANCE_ID, FlowInstanceSpec};
    use axum::body::to_bytes;
    use axum::http::StatusCode;
    use serde_json::Value as JsonValue;
    use std::io::{Cursor, Write};
    use storage::{
        StorageManager, StoredMemoryTopicKind, StoredPipelineDesiredState, StoredPipelineRunState,
    };
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
    ) -> ResourceManifestV1 {
        ResourceManifestV1 {
            format_version: RESOURCE_DIRECTORY_FORMAT_VERSION,
            bundle_version: "test-bundle-1".to_string(),
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
                pipelines: vec![ExportPipeline {
                    definition: sample_pipeline_request(pipeline_id, stream_name),
                    run_state: StoredPipelineDesiredState::Stopped,
                }],
                udfs: vec![],
                tables: vec![],
            },
        }
    }

    fn add_file_backed_proto_schema(
        bundle: &mut ResourceManifestV1,
        schemas_root: &std::path::Path,
    ) {
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

    fn build_zip_for_test(
        bundle: &ResourceManifestV1,
        wasm_dir: &std::path::Path,
        _uploads_dir: &std::path::Path,
        schemas_dir: &std::path::Path,
    ) -> Vec<u8> {
        let udf_shas: Vec<String> = bundle
            .resources
            .udfs
            .iter()
            .map(|u| u.wasm_sha256.clone())
            .collect();
        crate::export::build_zip(bundle, &udf_shas, wasm_dir, schemas_dir)
            .expect("build test export")
    }

    fn build_zip_entries(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let mut seen = std::collections::HashSet::new();
        let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
        let options = zip::write::SimpleFileOptions::default();
        for (name, data) in entries {
            if !seen.insert(*name) {
                continue;
            }
            writer.start_file(name, options).expect("start ZIP entry");
            writer.write_all(data).expect("write ZIP entry");
        }
        writer.finish().expect("finish ZIP").into_inner()
    }

    fn build_raw_zip_with_duplicate_entry() -> Vec<u8> {
        use std::io::Write;

        let name = b"duplicate";
        let data = b"x";
        let crc = 0x8cdc1683u32;

        let mut buf = Vec::new();

        // Local file header 1 (offset 0)
        buf.write_all(b"PK\x03\x04").unwrap();
        buf.write_all(&20u16.to_le_bytes()).unwrap();
        buf.write_all(&0u16.to_le_bytes()).unwrap();
        buf.write_all(&0u16.to_le_bytes()).unwrap();
        buf.write_all(&0u16.to_le_bytes()).unwrap();
        buf.write_all(&0u16.to_le_bytes()).unwrap();
        buf.write_all(&crc.to_le_bytes()).unwrap();
        buf.write_all(&(data.len() as u32).to_le_bytes()).unwrap();
        buf.write_all(&(data.len() as u32).to_le_bytes()).unwrap();
        buf.write_all(&(name.len() as u16).to_le_bytes()).unwrap();
        buf.write_all(&0u16.to_le_bytes()).unwrap();
        buf.write_all(name).unwrap();
        buf.write_all(data).unwrap();

        // Local file header 2
        let local_offset2 = buf.len() as u32;
        buf.write_all(b"PK\x03\x04").unwrap();
        buf.write_all(&20u16.to_le_bytes()).unwrap();
        buf.write_all(&0u16.to_le_bytes()).unwrap();
        buf.write_all(&0u16.to_le_bytes()).unwrap();
        buf.write_all(&0u16.to_le_bytes()).unwrap();
        buf.write_all(&0u16.to_le_bytes()).unwrap();
        buf.write_all(&crc.to_le_bytes()).unwrap();
        buf.write_all(&(data.len() as u32).to_le_bytes()).unwrap();
        buf.write_all(&(data.len() as u32).to_le_bytes()).unwrap();
        buf.write_all(&(name.len() as u16).to_le_bytes()).unwrap();
        buf.write_all(&0u16.to_le_bytes()).unwrap();
        buf.write_all(name).unwrap();
        buf.write_all(data).unwrap();

        let cd_offset = buf.len() as u32;

        // Central directory entry 1
        buf.write_all(b"PK\x01\x02").unwrap();
        buf.write_all(&20u16.to_le_bytes()).unwrap();
        buf.write_all(&20u16.to_le_bytes()).unwrap();
        buf.write_all(&0u16.to_le_bytes()).unwrap();
        buf.write_all(&0u16.to_le_bytes()).unwrap();
        buf.write_all(&0u16.to_le_bytes()).unwrap();
        buf.write_all(&0u16.to_le_bytes()).unwrap();
        buf.write_all(&crc.to_le_bytes()).unwrap();
        buf.write_all(&(data.len() as u32).to_le_bytes()).unwrap();
        buf.write_all(&(data.len() as u32).to_le_bytes()).unwrap();
        buf.write_all(&(name.len() as u16).to_le_bytes()).unwrap();
        buf.write_all(&0u16.to_le_bytes()).unwrap();
        buf.write_all(&0u16.to_le_bytes()).unwrap();
        buf.write_all(&0u16.to_le_bytes()).unwrap();
        buf.write_all(&0u16.to_le_bytes()).unwrap();
        buf.write_all(&0u32.to_le_bytes()).unwrap();
        buf.write_all(&0u32.to_le_bytes()).unwrap();
        buf.write_all(name).unwrap();

        // Central directory entry 2
        buf.write_all(b"PK\x01\x02").unwrap();
        buf.write_all(&20u16.to_le_bytes()).unwrap();
        buf.write_all(&20u16.to_le_bytes()).unwrap();
        buf.write_all(&0u16.to_le_bytes()).unwrap();
        buf.write_all(&0u16.to_le_bytes()).unwrap();
        buf.write_all(&0u16.to_le_bytes()).unwrap();
        buf.write_all(&0u16.to_le_bytes()).unwrap();
        buf.write_all(&crc.to_le_bytes()).unwrap();
        buf.write_all(&(data.len() as u32).to_le_bytes()).unwrap();
        buf.write_all(&(data.len() as u32).to_le_bytes()).unwrap();
        buf.write_all(&(name.len() as u16).to_le_bytes()).unwrap();
        buf.write_all(&0u16.to_le_bytes()).unwrap();
        buf.write_all(&0u16.to_le_bytes()).unwrap();
        buf.write_all(&0u16.to_le_bytes()).unwrap();
        buf.write_all(&0u16.to_le_bytes()).unwrap();
        buf.write_all(&0u32.to_le_bytes()).unwrap();
        buf.write_all(&local_offset2.to_le_bytes()).unwrap();
        buf.write_all(name).unwrap();

        let cd_size = buf.len() as u32 - cd_offset;

        // End of central directory
        buf.write_all(b"PK\x05\x06").unwrap();
        buf.write_all(&0u16.to_le_bytes()).unwrap();
        buf.write_all(&0u16.to_le_bytes()).unwrap();
        buf.write_all(&2u16.to_le_bytes()).unwrap();
        buf.write_all(&2u16.to_le_bytes()).unwrap();
        buf.write_all(&cd_size.to_le_bytes()).unwrap();
        buf.write_all(&cd_offset.to_le_bytes()).unwrap();
        buf.write_all(&0u16.to_le_bytes()).unwrap();

        buf
    }

    #[cfg(unix)]
    fn build_zip_with_symlinked_proto(
        bundle: &ResourceManifestV1,
        target: &std::path::Path,
    ) -> Vec<u8> {
        let metadata = serde_json::to_vec(bundle).expect("serialize metadata");
        let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
        let options = zip::write::SimpleFileOptions::default();
        writer
            .start_file("manifest.json", options)
            .expect("start metadata");
        writer.write_all(&metadata).expect("write metadata");
        writer
            .add_symlink(
                "schemas/proto/simple_schema/simple.proto",
                target.to_string_lossy(),
                options,
            )
            .expect("add schema symlink");
        writer.finish().expect("finish ZIP").into_inner()
    }

    fn is_default_instance(id: &str) -> bool {
        id == DEFAULT_FLOW_INSTANCE_ID
    }

    #[test]
    fn extract_zip_rejects_non_zip_body() {
        let dir = tempdir().expect("create tempdir");
        let err = extract_zip(b"not a ZIP archive", dir.path()).expect_err("reject non-ZIP body");
        assert!(err.contains("open import ZIP"), "unexpected error: {err}");
    }

    #[test]
    fn extract_zip_rejects_unsafe_path() {
        let dir = tempdir().expect("create tempdir");
        let zip = build_zip_entries(&[("../outside", b"data")]);
        let err = extract_zip(&zip, dir.path()).expect_err("reject unsafe path");
        assert!(err.contains("unsafe path"), "unexpected error: {err}");
    }

    #[test]
    fn extract_zip_rejects_duplicate_path() {
        let dir = tempdir().expect("create tempdir");
        let zip = build_raw_zip_with_duplicate_entry();
        let err = extract_zip(&zip, dir.path()).expect_err("reject duplicate path");
        assert!(err.contains("duplicate"), "unexpected error: {err}");
    }

    #[test]
    fn extract_zip_enforces_entry_and_size_limits() {
        let dir = tempdir().expect("create tempdir");
        let zip = build_zip_entries(&[("one", b"1234"), ("two", b"5678")]);

        let err = extract_zip_with_limits(&zip, dir.path(), 1, 8, 16)
            .expect_err("reject excessive entry count");
        assert!(err.contains("too many entries"), "unexpected error: {err}");

        let err =
            extract_zip_with_limits(&zip, dir.path(), 2, 3, 16).expect_err("reject oversized file");
        assert!(err.contains("exceeds 3 bytes"), "unexpected error: {err}");

        let err = extract_zip_with_limits(&zip, dir.path(), 2, 8, 7)
            .expect_err("reject excessive total size");
        assert!(
            err.contains("uncompressed size exceeds 7 bytes"),
            "unexpected error: {err}"
        );
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
        bundle.resources.pipelines.push(ExportPipeline {
            definition: sample_pipeline_request("pipe_a", "stream_a"),
            run_state: StoredPipelineDesiredState::Stopped,
        });

        let err = validate_and_build_snapshot(&bundle, None, &is_default_instance).unwrap_err();
        assert_eq!(err, "duplicate pipeline id in bundle: pipe_a");
    }

    #[test]
    fn deserialize_pipeline_without_run_state_defaults_to_stopped() {
        let mut value =
            serde_json::to_value(sample_bundle("stream_a", "pipe_a", "mqtt_a", "topic_a"))
                .expect("serialize bundle");
        value["resources"]["pipelines"][0]
            .as_object_mut()
            .expect("pipeline object")
            .remove("run_state");
        let bundle: ResourceManifestV1 = serde_json::from_value(value).expect("deserialize bundle");
        assert_eq!(
            bundle.resources.pipelines[0].run_state,
            StoredPipelineDesiredState::Stopped
        );
    }

    #[test]
    fn validate_snapshot_preserves_inline_pipeline_run_state() {
        let mut bundle = sample_bundle("stream_a", "pipe_a", "mqtt_a", "topic_a");
        bundle.resources.pipelines[0].run_state = StoredPipelineDesiredState::RunningScheduled(123);

        let snapshot = validate_and_build_snapshot(&bundle, None, &is_default_instance)
            .expect("build snapshot");
        assert_eq!(
            snapshot.pipeline_run_states,
            vec![StoredPipelineRunState {
                pipeline_id: "pipe_a".to_string(),
                desired_state: StoredPipelineDesiredState::RunningScheduled(123),
            }]
        );
    }

    #[test]
    fn validate_snapshot_rejects_undeclared_flow_instance() {
        let mut bundle = sample_bundle("stream_a", "pipe_a", "mqtt_a", "topic_a");
        bundle.resources.pipelines[0].definition.flow_instance_id = Some("unknown".to_string());

        let err = validate_and_build_snapshot(&bundle, None, &is_default_instance).unwrap_err();
        assert_eq!(err, "flow instance unknown is not declared by config");
    }

    #[test]
    fn validate_snapshot_normalizes_missing_flow_instance_id_to_default() {
        let mut bundle = sample_bundle("stream_a", "pipe_a", "mqtt_a", "topic_a");
        bundle.resources.pipelines[0].definition.flow_instance_id = None;

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
    async fn import_storage_handler_replaces_snapshot_and_returns_previous_resources() {
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

        let zip_bytes = build_zip_for_test(
            &new_bundle,
            &dir.path().join("wasm_files"),
            &dir.path().join("uploads"),
            &dir.path().join("schemas"),
        );
        let body = axum::body::Bytes::from(zip_bytes);

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
        assert!(json["previous_resources"]["streams"].is_array());

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

        let zip_bytes = build_zip_for_test(
            &invalid_bundle,
            &dir.path().join("wasm_files"),
            &dir.path().join("uploads"),
            &dir.path().join("schemas"),
        );
        let body = axum::body::Bytes::from(zip_bytes);

        let response = import_storage_handler(State(state.clone()), body)
            .await
            .into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        // Verify storage was not mutated
        let bundle_after =
            build_resource_manifest(state.storage.as_ref(), "test-bundle-1".to_string())
                .expect("export");
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
        bundle.resources.pipelines[0].definition.id = "bad.pipe".to_string();

        let err = validate_and_build_snapshot(&bundle, None, &is_default_instance).unwrap_err();
        assert!(err.contains("pipeline id"), "unexpected error: {err}");
    }

    #[test]
    fn validate_snapshot_rejects_invalid_flow_instance_id_before_declared_lookup() {
        let mut bundle = sample_bundle("stream_a", "pipe_a", "mqtt_a", "topic_a");
        // Use a syntactically invalid id (hyphen): grammar must reject it before
        // the declared-instance lookup runs, so the error is about the grammar,
        // not "not declared by config".
        bundle.resources.pipelines[0].definition.flow_instance_id = Some("bad-fi".to_string());

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
        bundle.resources.pipelines[0].definition =
            sample_pipeline_request("pipe_a", "missing_stream");

        let err = validate_and_build_snapshot(&bundle, None, &is_default_instance).unwrap_err();
        assert_eq!(
            err,
            "pipeline pipe_a references missing stream: missing_stream"
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

        let exported_bundle =
            crate::export::build_resource_manifest(&source_storage, "test-bundle-1".to_string())
                .unwrap();
        assert_eq!(
            exported_bundle.resources.schemas[0]
                .props
                .get("proto_path")
                .and_then(JsonValue::as_str),
            Some("simple.proto")
        );
        let zip_bytes = build_zip_for_test(
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
            import_storage_handler(State(state.clone()), axum::body::Bytes::from(zip_bytes))
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
        let zip_bytes = build_zip_with_symlinked_proto(&bundle, &external_proto);

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
            import_storage_handler(State(state.clone()), axum::body::Bytes::from(zip_bytes))
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
