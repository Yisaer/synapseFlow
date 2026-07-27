use std::collections::BTreeSet;
use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use storage::{MetadataExportSnapshot, StorageManager, StoredInitApplyMeta};

use crate::export::{
    ExportResources, RESOURCE_DIRECTORY_FORMAT_VERSION, ResourceManifestV1, build_export_resources,
    validate_bundle_version,
};
use crate::import::validate_and_build_snapshot;
use crate::instances::DEFAULT_FLOW_INSTANCE_ID;
use crate::schema::source::PreparedSchemaTree;
use crate::startup::StartupPhase;

const INIT_APPLY_PHASE: &str = "init_directory_apply";
const MANIFEST_FILE: &str = "manifest.json";
const MAX_SOURCE_ENTRIES: usize = 4096;
const MAX_SOURCE_FILE_SIZE: u64 = 512 * 1024 * 1024;
const MAX_SOURCE_TOTAL_SIZE: u64 = 512 * 1024 * 1024;

pub(crate) fn apply_init_directory_if_needed<F>(
    storage: &StorageManager,
    init_dir: Option<&Path>,
    is_declared_instance: &F,
) -> Result<(), String>
where
    F: Fn(&str) -> bool,
{
    let staging_root = storage.base_dir().join(".init-staging");
    fs::create_dir_all(&staging_root).map_err(|err| format!("create init staging root: {err}"))?;
    clean_staging_root(&staging_root)?;

    let Some(init_dir) = init_dir else {
        tracing::info!(
            phase = INIT_APPLY_PHASE,
            result = "skipped",
            reason = "not_configured",
            "startup init directory"
        );
        return Ok(());
    };

    let init_dir = match resolve_init_directory(storage, init_dir) {
        Ok(path) => path,
        Err(err) => {
            tracing::warn!(
                phase = INIT_APPLY_PHASE,
                result = "skipped",
                init_path = %init_dir.display(),
                reason = %err,
                "startup init directory"
            );
            return Ok(());
        }
    };
    let manifest_path = init_dir.join(MANIFEST_FILE);
    let raw = match fs::read(&manifest_path) {
        Ok(raw) => raw,
        Err(err) => {
            tracing::warn!(
                phase = INIT_APPLY_PHASE,
                result = "skipped",
                init_path = %init_dir.display(),
                reason = %format!("read manifest.json: {err}"),
                "startup init directory"
            );
            return Ok(());
        }
    };
    let manifest: ResourceManifestV1 = match serde_json::from_slice(&raw) {
        Ok(manifest) => manifest,
        Err(err) => {
            if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&raw)
                && value.is_object()
            {
                match value.get("bundle_version") {
                    None => return Err("bundle_version is required".to_string()),
                    Some(version) if !version.is_string() => {
                        return Err("bundle_version must be a string".to_string());
                    }
                    Some(_) => {}
                }
            }
            tracing::warn!(
                phase = INIT_APPLY_PHASE,
                result = "skipped",
                init_path = %init_dir.display(),
                reason = %format!("parse manifest.json: {err}"),
                "startup init directory"
            );
            return Ok(());
        }
    };
    if manifest.format_version != RESOURCE_DIRECTORY_FORMAT_VERSION {
        tracing::warn!(
            phase = INIT_APPLY_PHASE,
            result = "skipped",
            init_path = %init_dir.display(),
            format_version = manifest.format_version,
            reason = "unsupported_format_version",
            "startup init directory"
        );
        return Ok(());
    }
    validate_bundle_version(&manifest.bundle_version)?;

    if storage
        .get_init_apply_meta()
        .map_err(|err| format!("read init apply state: {err}"))?
        .is_some_and(|state| state.bundle_version == manifest.bundle_version)
    {
        tracing::info!(
            phase = INIT_APPLY_PHASE,
            result = "skipped_unchanged",
            init_path = %init_dir.display(),
            bundle_version = %manifest.bundle_version,
            "startup init directory"
        );
        return Ok(());
    }

    let phase = StartupPhase::new("manager", DEFAULT_FLOW_INSTANCE_ID, INIT_APPLY_PHASE);
    let result = apply_manifest(storage, &init_dir, manifest, is_declared_instance);
    match &result {
        Ok(summary) => {
            tracing::info!(
                phase = INIT_APPLY_PHASE,
                result = "applied",
                created = summary.created,
                kept_existing = summary.kept_existing,
                schema_files_installed = summary.schema_files,
                wasm_files_installed = summary.wasm_files,
                "startup init directory"
            );
            phase.log_success();
        }
        Err(err) => phase.log_failure(err),
    }
    result.map(|_| ())
}

fn resolve_init_directory(storage: &StorageManager, path: &Path) -> Result<PathBuf, String> {
    let init_dir = path
        .canonicalize()
        .map_err(|err| format!("resolve init directory: {err}"))?;
    if !fs::symlink_metadata(&init_dir)
        .map_err(|err| format!("inspect init directory: {err}"))?
        .file_type()
        .is_dir()
    {
        return Err("init path is not a directory".to_string());
    }
    let data_dir = storage
        .base_dir()
        .canonicalize()
        .map_err(|err| format!("resolve data directory: {err}"))?;
    if init_dir.starts_with(&data_dir) {
        return Err("init directory must not be inside data directory".to_string());
    }
    Ok(init_dir)
}

fn apply_manifest<F>(
    storage: &StorageManager,
    init_dir: &Path,
    manifest: ResourceManifestV1,
    is_declared_instance: &F,
) -> Result<ApplySummary, String>
where
    F: Fn(&str) -> bool,
{
    validate_incoming_identities(&manifest.resources)?;
    let live = build_export_resources(storage)?;
    let (candidate_resources, pending_resources, kept_existing) =
        build_apply_candidate(live, manifest.resources);
    let staging_root = storage.base_dir().join(".init-staging");
    let work = tempfile::Builder::new()
        .prefix("apply.")
        .tempdir_in(&staging_root)
        .map_err(|err| format!("create init staging directory: {err}"))?;

    let mut limits = SourceLimits::default();
    let combined_schemas = if pending_resources.schemas.is_empty() {
        storage.schemas_dir()
    } else {
        let combined = work.path().join("schemas");
        fs::create_dir(&combined).map_err(|err| format!("create schema staging root: {err}"))?;
        copy_tree_limited(&storage.schemas_dir(), &combined, &mut limits)?;
        for schema in &pending_resources.schemas {
            let source = init_dir
                .join("schemas")
                .join(schema.schema_type.trim().to_ascii_lowercase())
                .join(&schema.name);
            if source.exists() {
                let destination = combined
                    .join(schema.schema_type.trim().to_ascii_lowercase())
                    .join(&schema.name);
                fs::create_dir_all(
                    destination.parent().ok_or_else(|| {
                        format!("schema {} has no destination parent", schema.name)
                    })?,
                )
                .map_err(|err| format!("create schema type staging directory: {err}"))?;
                copy_tree_limited(&source, &destination, &mut limits)?;
            }
        }
        combined
    };

    let candidate = ResourceManifestV1 {
        format_version: RESOURCE_DIRECTORY_FORMAT_VERSION,
        bundle_version: manifest.bundle_version.clone(),
        resources: candidate_resources,
    };
    let candidate_snapshot =
        validate_and_build_snapshot(&candidate, Some(&combined_schemas), is_declared_instance)?;
    let pending_snapshot = filter_pending_snapshot(candidate_snapshot, &pending_resources);

    let wasm_source = work.path().join("wasm-source");
    fs::create_dir(&wasm_source).map_err(|err| format!("create WASM source staging: {err}"))?;
    for udf in &pending_resources.udfs {
        let file_name = format!("{}.wasm", udf.wasm_sha256);
        copy_regular_file_limited(
            &init_dir.join("wasm_files").join(&file_name),
            &wasm_source.join(&file_name),
            &mut limits,
        )?;
    }
    let staged_wasm = work.path().join("wasm_files");
    fs::create_dir(&staged_wasm).map_err(|err| format!("create WASM staging directory: {err}"))?;
    crate::import::validate_and_copy_udfs_for_import(
        &pending_snapshot.udfs,
        &wasm_source,
        &staged_wasm,
    )?;

    let mut schema_tree = if pending_resources.schemas.is_empty() {
        None
    } else {
        Some(PreparedSchemaTree::prepare(storage, &combined_schemas)?)
    };
    if let Some(tree) = &mut schema_tree {
        tree.activate()?;
    }
    let installed_wasm = install_staged_wasm(&staged_wasm, &storage.wasm_files_dir())?;
    let meta = StoredInitApplyMeta {
        bundle_version: manifest.bundle_version,
        applied_at_ms: unix_time_ms(SystemTime::now())?,
    };
    if let Err(err) = storage.apply_init_snapshot(pending_snapshot, meta) {
        for path in &installed_wasm {
            let _ = fs::remove_file(path);
        }
        let mut message = format!("commit init metadata: {err}");
        if let Some(tree) = &mut schema_tree
            && let Err(rollback_err) = tree.rollback()
        {
            message = format!("{message}; rollback schemas: {rollback_err}");
        }
        return Err(message);
    }
    if let Some(tree) = &mut schema_tree {
        tree.finish();
    }

    Ok(ApplySummary {
        created: count_resources(&pending_resources),
        kept_existing,
        schema_files: limits.entries,
        wasm_files: installed_wasm.len(),
    })
}

fn build_apply_candidate(
    mut live: ExportResources,
    incoming: ExportResources,
) -> (ExportResources, ExportResources, usize) {
    let mut pending = ExportResources {
        memory_topics: Vec::new(),
        shared_mqtt_clients: Vec::new(),
        schemas: Vec::new(),
        streams: Vec::new(),
        pipelines: Vec::new(),
        udfs: Vec::new(),
        tables: Vec::new(),
    };
    let mut kept = 0;
    macro_rules! merge {
        ($field:ident, $id:expr) => {{
            let mut ids: BTreeSet<String> = live.$field.iter().map($id).collect();
            for item in incoming.$field {
                let id = $id(&item);
                if ids.insert(id.clone()) {
                    pending.$field.push(item.clone());
                    live.$field.push(item);
                } else {
                    kept += 1;
                    tracing::warn!(
                        resource_kind = stringify!($field),
                        resource_id = %id,
                        reason = "data_dir_preferred",
                        "init resource kept existing"
                    );
                }
            }
        }};
    }
    merge!(memory_topics, |item: &crate::export::ExportMemoryTopic| {
        item.topic.clone()
    });
    merge!(
        shared_mqtt_clients,
        |item: &flow::connector::SharedMqttClientConfig| item.key.clone()
    );
    merge!(schemas, |item: &crate::export::ExportSchema| item
        .name
        .clone());
    merge!(streams, |item: &crate::stream::CreateStreamRequest| item
        .name
        .clone());
    merge!(pipelines, |item: &crate::export::ExportPipeline| item
        .definition
        .id
        .clone());
    merge!(udfs, |item: &crate::export::ExportUdf| item
        .name
        .to_ascii_lowercase());
    merge!(tables, |item: &crate::export::ExportTable| item
        .definition
        .name
        .clone());
    (live, pending, kept)
}

fn validate_incoming_identities(resources: &ExportResources) -> Result<(), String> {
    fn unique<'a>(kind: &str, identities: impl IntoIterator<Item = &'a str>) -> Result<(), String> {
        let mut seen = BTreeSet::new();
        for identity in identities {
            if !seen.insert(identity.to_string()) {
                return Err(format!(
                    "duplicate {kind} identity in resource manifest: {identity}"
                ));
            }
        }
        Ok(())
    }
    unique(
        "memory topic",
        resources
            .memory_topics
            .iter()
            .map(|item| item.topic.as_str()),
    )?;
    unique(
        "shared MQTT client",
        resources
            .shared_mqtt_clients
            .iter()
            .map(|item| item.key.as_str()),
    )?;
    unique(
        "schema",
        resources.schemas.iter().map(|item| item.name.as_str()),
    )?;
    unique(
        "stream",
        resources.streams.iter().map(|item| item.name.as_str()),
    )?;
    unique(
        "pipeline",
        resources
            .pipelines
            .iter()
            .map(|item| item.definition.id.as_str()),
    )?;
    unique("UDF", resources.udfs.iter().map(|item| item.name.as_str()))?;
    unique(
        "table",
        resources
            .tables
            .iter()
            .map(|item| item.definition.name.as_str()),
    )
}

fn filter_pending_snapshot(
    candidate: MetadataExportSnapshot,
    pending: &ExportResources,
) -> MetadataExportSnapshot {
    let stream_ids = pending
        .streams
        .iter()
        .map(|v| v.name.as_str())
        .collect::<BTreeSet<_>>();
    let schema_ids = pending
        .schemas
        .iter()
        .map(|v| v.name.as_str())
        .collect::<BTreeSet<_>>();
    let pipeline_ids = pending
        .pipelines
        .iter()
        .map(|v| v.definition.id.as_str())
        .collect::<BTreeSet<_>>();
    let mqtt_ids = pending
        .shared_mqtt_clients
        .iter()
        .map(|v| v.key.as_str())
        .collect::<BTreeSet<_>>();
    let topic_ids = pending
        .memory_topics
        .iter()
        .map(|v| v.topic.as_str())
        .collect::<BTreeSet<_>>();
    let udf_ids = pending
        .udfs
        .iter()
        .map(|v| v.name.as_str())
        .collect::<BTreeSet<_>>();
    let table_ids = pending
        .tables
        .iter()
        .map(|v| v.definition.name.as_str())
        .collect::<BTreeSet<_>>();
    MetadataExportSnapshot {
        streams: candidate
            .streams
            .into_iter()
            .filter(|v| stream_ids.contains(v.id.as_str()))
            .collect(),
        schemas: candidate
            .schemas
            .into_iter()
            .filter(|v| schema_ids.contains(v.name.as_str()))
            .collect(),
        pipelines: candidate
            .pipelines
            .into_iter()
            .filter(|v| pipeline_ids.contains(v.id.as_str()))
            .collect(),
        pipeline_run_states: candidate
            .pipeline_run_states
            .into_iter()
            .filter(|v| pipeline_ids.contains(v.pipeline_id.as_str()))
            .collect(),
        mqtt_configs: candidate
            .mqtt_configs
            .into_iter()
            .filter(|v| mqtt_ids.contains(v.key.as_str()))
            .collect(),
        memory_topics: candidate
            .memory_topics
            .into_iter()
            .filter(|v| topic_ids.contains(v.topic.as_str()))
            .collect(),
        udfs: candidate
            .udfs
            .into_iter()
            .filter(|v| udf_ids.contains(v.name.as_str()))
            .collect(),
        tables: candidate
            .tables
            .into_iter()
            .filter(|v| table_ids.contains(v.id.as_str()))
            .collect(),
    }
}

#[derive(Default)]
struct SourceLimits {
    entries: usize,
    bytes: u64,
}

fn copy_tree_limited(
    source: &Path,
    destination: &Path,
    limits: &mut SourceLimits,
) -> Result<(), String> {
    let metadata = match fs::symlink_metadata(source) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(format!("inspect source `{}`: {err}", source.display())),
    };
    if !metadata.file_type().is_dir() {
        return Err(format!("source `{}` is not a directory", source.display()));
    }
    fs::create_dir_all(destination)
        .map_err(|err| format!("create `{}`: {err}", destination.display()))?;
    for entry in
        fs::read_dir(source).map_err(|err| format!("read `{}`: {err}", source.display()))?
    {
        let entry = entry.map_err(|err| format!("read source entry: {err}"))?;
        let file_type = entry
            .file_type()
            .map_err(|err| format!("inspect source entry: {err}"))?;
        limits.entries += 1;
        if limits.entries > MAX_SOURCE_ENTRIES {
            return Err(format!(
                "init source has too many entries: {}",
                limits.entries
            ));
        }
        let target = destination.join(entry.file_name());
        if file_type.is_dir() {
            copy_tree_limited(&entry.path(), &target, limits)?;
        } else if file_type.is_file() {
            let size = entry
                .metadata()
                .map_err(|err| format!("inspect source file: {err}"))?
                .len();
            if size > MAX_SOURCE_FILE_SIZE {
                return Err(format!(
                    "init source file `{}` is too large",
                    entry.path().display()
                ));
            }
            limits.bytes = limits
                .bytes
                .checked_add(size)
                .ok_or_else(|| "init source size overflow".to_string())?;
            if limits.bytes > MAX_SOURCE_TOTAL_SIZE {
                return Err("init source total size exceeds limit".to_string());
            }
            fs::copy(entry.path(), &target)
                .map_err(|err| format!("copy source file `{}`: {err}", entry.path().display()))?;
        } else {
            return Err(format!(
                "init source entry `{}` is not a regular file or directory",
                entry.path().display()
            ));
        }
    }
    Ok(())
}

fn copy_regular_file_limited(
    source: &Path,
    destination: &Path,
    limits: &mut SourceLimits,
) -> Result<(), String> {
    let metadata = fs::symlink_metadata(source)
        .map_err(|err| format!("inspect source file `{}`: {err}", source.display()))?;
    if !metadata.file_type().is_file() {
        return Err(format!(
            "source `{}` is not a regular file",
            source.display()
        ));
    }
    let size = metadata.len();
    if size > MAX_SOURCE_FILE_SIZE {
        return Err(format!(
            "init source file `{}` is too large",
            source.display()
        ));
    }
    limits.entries += 1;
    limits.bytes = limits
        .bytes
        .checked_add(size)
        .ok_or_else(|| "init source size overflow".to_string())?;
    if limits.entries > MAX_SOURCE_ENTRIES || limits.bytes > MAX_SOURCE_TOTAL_SIZE {
        return Err("init source limits exceeded".to_string());
    }
    fs::copy(source, destination)
        .map_err(|err| format!("copy source file `{}`: {err}", source.display()))?;
    Ok(())
}

fn install_staged_wasm(source: &Path, destination: &Path) -> Result<Vec<PathBuf>, String> {
    fs::create_dir_all(destination).map_err(|err| format!("create WASM directory: {err}"))?;
    let mut entries = fs::read_dir(source)
        .map_err(|err| format!("read staged WASM directory: {err}"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| format!("read staged WASM entry: {err}"))?;
    entries.sort_by_key(fs::DirEntry::file_name);

    let mut installed = Vec::new();
    let install_result = (|| {
        for entry in entries {
            let source_path = entry.path();
            let target = destination.join(entry.file_name());
            if target.exists() {
                if !files_equal(&source_path, &target)? {
                    return Err(format!(
                        "existing managed WASM `{}` does not match staged content",
                        target.display()
                    ));
                }
                continue;
            }
            fs::rename(&source_path, &target)
                .map_err(|err| format!("install WASM `{}`: {err}", target.display()))?;
            installed.push(target);
        }
        Ok(())
    })();

    if let Err(err) = install_result {
        for path in &installed {
            let _ = fs::remove_file(path);
        }
        return Err(err);
    }

    Ok(installed)
}

fn files_equal(left: &Path, right: &Path) -> Result<bool, String> {
    let left_len = fs::metadata(left)
        .map_err(|err| format!("inspect staged WASM `{}`: {err}", left.display()))?
        .len();
    let right_len = fs::metadata(right)
        .map_err(|err| format!("inspect managed WASM `{}`: {err}", right.display()))?
        .len();
    if left_len != right_len {
        return Ok(false);
    }

    let mut left = fs::File::open(left)
        .map_err(|err| format!("open staged WASM `{}`: {err}", left.display()))?;
    let mut right = fs::File::open(right)
        .map_err(|err| format!("open managed WASM `{}`: {err}", right.display()))?;
    let mut left_buffer = [0_u8; 64 * 1024];
    let mut right_buffer = [0_u8; 64 * 1024];
    loop {
        let left_read = left
            .read(&mut left_buffer)
            .map_err(|err| format!("read staged WASM: {err}"))?;
        let right_read = right
            .read(&mut right_buffer)
            .map_err(|err| format!("read managed WASM: {err}"))?;
        if left_read != right_read || left_buffer[..left_read] != right_buffer[..right_read] {
            return Ok(false);
        }
        if left_read == 0 {
            return Ok(true);
        }
    }
}

fn clean_staging_root(path: &Path) -> Result<(), String> {
    for entry in fs::read_dir(path).map_err(|err| format!("read init staging root: {err}"))? {
        let entry = entry.map_err(|err| format!("read init staging entry: {err}"))?;
        let file_type = entry
            .file_type()
            .map_err(|err| format!("inspect init staging entry: {err}"))?;
        if file_type.is_dir() {
            fs::remove_dir_all(entry.path())
                .map_err(|err| format!("remove stale init staging: {err}"))?;
        } else {
            fs::remove_file(entry.path())
                .map_err(|err| format!("remove stale init staging file: {err}"))?;
        }
    }
    Ok(())
}

fn count_resources(resources: &ExportResources) -> usize {
    resources.memory_topics.len()
        + resources.shared_mqtt_clients.len()
        + resources.schemas.len()
        + resources.streams.len()
        + resources.pipelines.len()
        + resources.udfs.len()
        + resources.tables.len()
}

fn unix_time_ms(value: SystemTime) -> Result<u64, String> {
    let millis = value
        .duration_since(UNIX_EPOCH)
        .map_err(|err| format!("system time before unix epoch: {err}"))?
        .as_millis();
    u64::try_from(millis).map_err(|_| "unix time overflow".to_string())
}

struct ApplySummary {
    created: usize,
    kept_existing: usize,
    schema_files: usize,
    wasm_files: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use storage::{StorageManager, StoredMemoryTopicKind};
    use tempfile::tempdir;

    fn manifest(version: &str, topics: &[(&str, usize)]) -> ResourceManifestV1 {
        ResourceManifestV1 {
            format_version: RESOURCE_DIRECTORY_FORMAT_VERSION,
            bundle_version: version.to_string(),
            resources: ExportResources {
                memory_topics: topics
                    .iter()
                    .map(|(topic, capacity)| crate::export::ExportMemoryTopic {
                        topic: (*topic).to_string(),
                        kind: StoredMemoryTopicKind::Bytes,
                        capacity: *capacity,
                    })
                    .collect(),
                shared_mqtt_clients: Vec::new(),
                schemas: Vec::new(),
                streams: Vec::new(),
                pipelines: Vec::new(),
                udfs: Vec::new(),
                tables: Vec::new(),
            },
        }
    }

    fn write_manifest(init_dir: &Path, manifest: &ResourceManifestV1) {
        fs::create_dir_all(init_dir).expect("create init directory");
        fs::write(
            init_dir.join(MANIFEST_FILE),
            serde_json::to_vec(manifest).expect("serialize manifest"),
        )
        .expect("write manifest");
    }

    #[test]
    fn apply_creates_missing_resources_and_same_version_does_not_restore_deletion() {
        let root = tempdir().expect("root");
        let storage = StorageManager::new(root.path().join("data")).expect("storage");
        let init_dir = root.path().join("init");
        write_manifest(&init_dir, &manifest("v1", &[("topic_a", 16)]));

        apply_init_directory_if_needed(&storage, Some(&init_dir), &|_| true).expect("first apply");
        assert!(storage.get_memory_topic("topic_a").unwrap().is_some());
        assert_eq!(
            storage
                .get_init_apply_meta()
                .unwrap()
                .unwrap()
                .bundle_version,
            "v1"
        );

        storage
            .delete_memory_topic("topic_a")
            .expect("delete topic");
        apply_init_directory_if_needed(&storage, Some(&init_dir), &|_| true).expect("skip");
        assert!(storage.get_memory_topic("topic_a").unwrap().is_none());
    }

    #[test]
    fn new_version_keeps_existing_resource_and_creates_missing_resource() {
        let root = tempdir().expect("root");
        let storage = StorageManager::new(root.path().join("data")).expect("storage");
        let init_dir = root.path().join("init");
        write_manifest(&init_dir, &manifest("v1", &[("topic_a", 16)]));
        apply_init_directory_if_needed(&storage, Some(&init_dir), &|_| true).expect("first apply");

        write_manifest(
            &init_dir,
            &manifest("v2", &[("topic_a", 64), ("topic_b", 32)]),
        );
        apply_init_directory_if_needed(&storage, Some(&init_dir), &|_| true).expect("second apply");

        assert_eq!(
            storage
                .get_memory_topic("topic_a")
                .unwrap()
                .unwrap()
                .capacity,
            16
        );
        assert_eq!(
            storage
                .get_memory_topic("topic_b")
                .unwrap()
                .unwrap()
                .capacity,
            32
        );
    }

    #[test]
    fn malformed_manifest_is_skipped_without_advancing_apply_state() {
        let root = tempdir().expect("root");
        let storage = StorageManager::new(root.path().join("data")).expect("storage");
        let init_dir = root.path().join("init");
        fs::create_dir_all(&init_dir).expect("create init directory");
        fs::write(init_dir.join(MANIFEST_FILE), b"{").expect("write malformed manifest");

        apply_init_directory_if_needed(&storage, Some(&init_dir), &|_| true)
            .expect("malformed manifest is non-fatal");
        assert!(storage.get_init_apply_meta().unwrap().is_none());
    }

    #[test]
    fn missing_bundle_version_fails_startup() {
        let root = tempdir().expect("root");
        let storage = StorageManager::new(root.path().join("data")).expect("storage");
        let init_dir = root.path().join("init");
        fs::create_dir_all(&init_dir).expect("create init directory");
        fs::write(
            init_dir.join(MANIFEST_FILE),
            br#"{"format_version":1,"resources":{"memory_topics":[],"shared_mqtt_clients":[],"schemas":[],"streams":[],"pipelines":[],"udfs":[]}}"#,
        )
        .expect("write manifest");

        let err = apply_init_directory_if_needed(&storage, Some(&init_dir), &|_| true)
            .expect_err("missing bundle_version must fail");
        assert_eq!(err, "bundle_version is required");
    }

    #[test]
    fn install_staged_wasm_rejects_existing_file_with_different_content() {
        let root = tempdir().expect("root");
        let source = root.path().join("source");
        let destination = root.path().join("destination");
        fs::create_dir(&source).expect("create source");
        fs::create_dir(&destination).expect("create destination");
        fs::write(source.join("a.wasm"), b"new").expect("write staged WASM");
        fs::write(destination.join("a.wasm"), b"old").expect("write managed WASM");

        let err = install_staged_wasm(&source, &destination)
            .expect_err("different existing content must fail");

        assert!(err.contains("does not match staged content"));
        assert_eq!(
            fs::read(destination.join("a.wasm")).expect("read managed WASM"),
            b"old"
        );
    }

    #[test]
    fn install_staged_wasm_rolls_back_files_installed_before_failure() {
        let root = tempdir().expect("root");
        let source = root.path().join("source");
        let destination = root.path().join("destination");
        fs::create_dir(&source).expect("create source");
        fs::create_dir(&destination).expect("create destination");
        fs::write(source.join("a.wasm"), b"first").expect("write first staged WASM");
        fs::write(source.join("b.wasm"), b"new").expect("write second staged WASM");
        fs::write(destination.join("b.wasm"), b"old").expect("write conflicting managed WASM");

        install_staged_wasm(&source, &destination).expect_err("second install must fail");

        assert!(!destination.join("a.wasm").exists());
        assert_eq!(
            fs::read(destination.join("b.wasm")).expect("read conflicting managed WASM"),
            b"old"
        );
    }
}
