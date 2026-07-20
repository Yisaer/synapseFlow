use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use storage::{StorageManager, StoredInitApplyMeta};

use crate::export::{ExportBundleV1, ExportResources};
use crate::import::validate_and_build_snapshot_with_existing_streams;
use crate::instances::DEFAULT_FLOW_INSTANCE_ID;
use crate::startup::StartupPhase;

const INIT_JSON_FILE: &str = "init.json";
const INIT_TAR_GZ_FILE: &str = "init.tar.gz";
const INIT_JSON_APPLY_PHASE: &str = "init_json_apply";
const INIT_TAR_GZ_APPLY_PHASE: &str = "init_tar_gz_apply";

pub(crate) fn apply_init_json_if_needed<F>(
    storage: &StorageManager,
    is_declared_instance: &F,
) -> Result<(), String>
where
    F: Fn(&str) -> bool,
{
    // Prefer init.tar.gz over init.json
    let tar_gz_path = init_tar_gz_path(storage);
    if tar_gz_path.exists() {
        return apply_init_tar_gz(storage, &tar_gz_path, is_declared_instance);
    }

    apply_init_json_inner(storage, is_declared_instance)
}

fn apply_init_json_inner<F>(
    storage: &StorageManager,
    is_declared_instance: &F,
) -> Result<(), String>
where
    F: Fn(&str) -> bool,
{
    let init_path = init_json_path(storage);
    if !init_path.exists() {
        tracing::info!(
            mode = "manager",
            flow_instance_id = DEFAULT_FLOW_INSTANCE_ID,
            phase = INIT_JSON_APPLY_PHASE,
            result = "skipped",
            reason = "file_missing",
            init_json_path = %init_path.display(),
            "startup phase"
        );
        return Ok(());
    }

    let init_modified_at_ms = init_file_modified_at_ms(&init_path)?;
    let stored_meta = storage
        .get_init_apply_meta()
        .map_err(|err| format!("read init apply meta from storage: {err}"))?;
    if let Some(meta) = stored_meta
        && init_modified_at_ms <= meta.last_init_json_modified_at_ms
    {
        tracing::info!(
            mode = "manager",
            flow_instance_id = DEFAULT_FLOW_INSTANCE_ID,
            phase = INIT_JSON_APPLY_PHASE,
            result = "skipped",
            reason = "not_modified",
            init_json_path = %init_path.display(),
            init_json_modified_at_ms = init_modified_at_ms,
            last_applied_init_json_modified_at_ms = meta.last_init_json_modified_at_ms,
            "startup phase"
        );
        return Ok(());
    }

    let phase = StartupPhase::new("manager", DEFAULT_FLOW_INSTANCE_ID, INIT_JSON_APPLY_PHASE);
    let result = apply_init_json(
        storage,
        &init_path,
        init_modified_at_ms,
        is_declared_instance,
    );
    match result {
        Ok(summary) => {
            tracing::info!(
                mode = "manager",
                flow_instance_id = DEFAULT_FLOW_INSTANCE_ID,
                phase = INIT_JSON_APPLY_PHASE,
                result = "applied",
                init_json_path = %init_path.display(),
                init_json_modified_at_ms = init_modified_at_ms,
                applied_memory_topic_count = summary.memory_topics,
                applied_shared_mqtt_client_count = summary.shared_mqtt_clients,
                applied_stream_count = summary.streams,
                applied_pipeline_count = summary.pipelines,
                applied_pipeline_run_state_count = summary.pipeline_run_states,
                "startup phase"
            );
            phase.log_success();
            Ok(())
        }
        Err(err) => {
            phase.log_failure(&err);
            Err(err)
        }
    }
}

/// Apply init.tar.gz: unarchive, load metadata.json, apply to storage,
/// and copy any uploads/ files to the data directory.
fn apply_init_tar_gz<F>(
    storage: &StorageManager,
    tar_gz_path: &Path,
    is_declared_instance: &F,
) -> Result<(), String>
where
    F: Fn(&str) -> bool,
{
    let init_modified_at_ms = init_file_modified_at_ms(tar_gz_path)?;
    let stored_meta = storage
        .get_init_apply_meta()
        .map_err(|err| format!("read init apply meta from storage: {err}"))?;
    if let Some(meta) = stored_meta
        && init_modified_at_ms <= meta.last_init_json_modified_at_ms
    {
        tracing::info!(
            mode = "manager",
            flow_instance_id = DEFAULT_FLOW_INSTANCE_ID,
            phase = INIT_TAR_GZ_APPLY_PHASE,
            result = "skipped",
            reason = "not_modified",
            init_tar_gz_path = %tar_gz_path.display(),
            init_tar_gz_modified_at_ms = init_modified_at_ms,
            last_applied_init_modified_at_ms = meta.last_init_json_modified_at_ms,
            "startup phase"
        );
        return Ok(());
    }

    let phase = StartupPhase::new("manager", DEFAULT_FLOW_INSTANCE_ID, INIT_TAR_GZ_APPLY_PHASE);

    let tmp_dir =
        tempfile::tempdir().map_err(|e| format!("create temp dir for init.tar.gz: {e}"))?;

    // Unpack tar.gz
    {
        let data = fs::read(tar_gz_path)
            .map_err(|e| format!("read init.tar.gz {}: {e}", tar_gz_path.display()))?;
        let gz = flate2::read::GzDecoder::new(data.as_slice());
        let mut archive = tar::Archive::new(gz);
        archive
            .unpack(tmp_dir.path())
            .map_err(|e| format!("unpack init.tar.gz: {e}"))?;
    }

    // Read metadata.json
    let metadata_path = tmp_dir.path().join("metadata.json");
    let metadata_bytes = fs::read(&metadata_path)
        .map_err(|e| format!("read metadata.json from init.tar.gz: {e}"))?;
    let bundle: ExportBundleV1 = serde_json::from_slice(&metadata_bytes)
        .map_err(|e| format!("parse init.tar.gz metadata.json: {e}"))?;

    let summary = ApplySummary::from_resources(&bundle.resources);
    let existing_stream_names: BTreeSet<String> = storage
        .list_streams()
        .map_err(|err| format!("list existing streams from storage: {err}"))?
        .into_iter()
        .map(|stream| stream.id)
        .collect();
    let snapshot = validate_and_build_snapshot_with_existing_streams(
        &bundle,
        &existing_stream_names,
        is_declared_instance,
    )?;

    // Validate and copy UDF wasm files
    let udf_count = snapshot.udfs.len();
    if udf_count > 0 {
        let wasm_src = tmp_dir.path().join("wasm_files");
        crate::import::validate_and_copy_udfs_for_import(
            &snapshot.udfs,
            &wasm_src,
            &storage.wasm_files_dir(),
        )?;
    }

    // Write metadata snapshot to redb
    let meta = StoredInitApplyMeta {
        last_applied_at_ms: unix_time_ms(SystemTime::now())?,
        last_init_json_modified_at_ms: init_modified_at_ms,
    };
    storage
        .apply_init_snapshot(snapshot, meta)
        .map_err(|err| format!("apply init.tar.gz to storage: {err}"))?;

    // Copy uploads from archive
    let uploads_src = tmp_dir.path().join("uploads");
    let upload_count = storage
        .copy_uploads_from_dir(&uploads_src)
        .map_err(|e| format!("copy uploads from init.tar.gz: {e}"))?;

    tracing::info!(
        mode = "manager",
        flow_instance_id = DEFAULT_FLOW_INSTANCE_ID,
        phase = INIT_TAR_GZ_APPLY_PHASE,
        result = "applied",
        init_tar_gz_path = %tar_gz_path.display(),
        init_tar_gz_modified_at_ms = init_modified_at_ms,
        applied_memory_topic_count = summary.memory_topics,
        applied_shared_mqtt_client_count = summary.shared_mqtt_clients,
        applied_stream_count = summary.streams,
        applied_pipeline_count = summary.pipelines,
        applied_pipeline_run_state_count = summary.pipeline_run_states,
        applied_udf_count = udf_count,
        applied_upload_count = upload_count,
        "startup phase"
    );
    phase.log_success();
    Ok(())
}

fn apply_init_json<F>(
    storage: &StorageManager,
    init_path: &Path,
    init_modified_at_ms: u64,
    is_declared_instance: &F,
) -> Result<ApplySummary, String>
where
    F: Fn(&str) -> bool,
{
    let raw = fs::read(init_path)
        .map_err(|err| format!("read init.json {}: {err}", init_path.display()))?;
    let bundle: ExportBundleV1 = serde_json::from_slice(&raw)
        .map_err(|err| format!("parse init.json {}: {err}", init_path.display()))?;
    let summary = ApplySummary::from_resources(&bundle.resources);
    let existing_stream_names: BTreeSet<String> = storage
        .list_streams()
        .map_err(|err| format!("list existing streams from storage: {err}"))?
        .into_iter()
        .map(|stream| stream.id)
        .collect();
    let snapshot = validate_and_build_snapshot_with_existing_streams(
        &bundle,
        &existing_stream_names,
        is_declared_instance,
    )?;
    let meta = StoredInitApplyMeta {
        last_applied_at_ms: unix_time_ms(SystemTime::now())?,
        last_init_json_modified_at_ms: init_modified_at_ms,
    };
    storage
        .apply_init_snapshot(snapshot, meta)
        .map_err(|err| format!("apply init.json {} to storage: {err}", init_path.display()))?;
    Ok(summary)
}

fn init_json_path(storage: &StorageManager) -> PathBuf {
    storage.base_dir().join(INIT_JSON_FILE)
}

fn init_tar_gz_path(storage: &StorageManager) -> PathBuf {
    storage.base_dir().join(INIT_TAR_GZ_FILE)
}

fn init_file_modified_at_ms(path: &Path) -> Result<u64, String> {
    let metadata = fs::metadata(path)
        .map_err(|err| format!("read init.json metadata {}: {err}", path.display()))?;
    let modified = metadata
        .modified()
        .map_err(|err| format!("read init.json modified time {}: {err}", path.display()))?;
    unix_time_ms(modified)
}

fn unix_time_ms(value: SystemTime) -> Result<u64, String> {
    let duration = value
        .duration_since(UNIX_EPOCH)
        .map_err(|err| format!("system time before unix epoch: {err}"))?;
    u64::try_from(duration.as_millis()).map_err(|_| "unix time overflow".to_string())
}

struct ApplySummary {
    memory_topics: usize,
    shared_mqtt_clients: usize,
    streams: usize,
    pipelines: usize,
    pipeline_run_states: usize,
}

impl ApplySummary {
    fn from_resources(resources: &ExportResources) -> Self {
        Self {
            memory_topics: resources.memory_topics.len(),
            shared_mqtt_clients: resources.shared_mqtt_clients.len(),
            streams: resources.streams.len(),
            pipelines: resources.pipelines.len(),
            pipeline_run_states: resources.pipeline_run_states.len(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_bridge;
    use tempfile::tempdir;

    use crate::pipeline::CreatePipelineRequest;
    use crate::stream::{
        CreateStreamRequest, DecoderConfigRequest, SchemaConfigRequest, StreamPropsRequest,
    };
    use serde_json::json;

    fn sample_stream_request(name: &str) -> CreateStreamRequest {
        CreateStreamRequest {
            name: name.to_string(),
            stream_type: "mqtt".to_string(),
            schema: SchemaConfigRequest::default(),
            props: StreamPropsRequest::default(),
            shared: false,
            decoder: DecoderConfigRequest::default(),
            eventtime: None,
            sampler: None,
        }
    }

    fn sample_bundle(stream_name: &str) -> ExportBundleV1 {
        ExportBundleV1 {
            exported_at: 0,
            resources: ExportResources {
                memory_topics: Vec::new(),
                shared_mqtt_clients: Vec::new(),
                schemas: vec![],
                streams: vec![sample_stream_request(stream_name)],
                pipelines: Vec::new(),
                pipeline_run_states: Vec::new(),
                udfs: vec![],
            },
        }
    }

    fn sample_pipeline_request(
        id: &str,
        stream_name: &str,
        flow_instance_id: &str,
    ) -> CreatePipelineRequest {
        serde_json::from_value(json!({
            "id": id,
            "flow_instance_id": flow_instance_id,
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

    fn write_init_json(dir: &Path, bundle: &ExportBundleV1) {
        let path = dir.join(INIT_JSON_FILE);
        let json = serde_json::to_vec(bundle).expect("serialize init bundle");
        fs::write(path, json).expect("write init.json");
    }

    #[test]
    fn apply_init_json_skips_missing_file() {
        let dir = tempdir().unwrap();
        let storage = StorageManager::new(dir.path()).unwrap();

        apply_init_json_if_needed(&storage, &|id| id == DEFAULT_FLOW_INSTANCE_ID).unwrap();

        assert_eq!(
            storage.list_streams().unwrap(),
            Vec::<storage::StoredStream>::new()
        );
        assert_eq!(storage.get_init_apply_meta().unwrap(), None);
    }

    #[test]
    fn apply_init_json_writes_storage_and_skips_when_unchanged() {
        let dir = tempdir().unwrap();
        let storage = StorageManager::new(dir.path()).unwrap();
        write_init_json(dir.path(), &sample_bundle("stream_1"));

        apply_init_json_if_needed(&storage, &|id| id == DEFAULT_FLOW_INSTANCE_ID).unwrap();
        let meta = storage
            .get_init_apply_meta()
            .unwrap()
            .expect("init apply meta exists");

        assert_eq!(storage.list_streams().unwrap().len(), 1);

        apply_init_json_if_needed(&storage, &|id| id == DEFAULT_FLOW_INSTANCE_ID).unwrap();

        assert_eq!(storage.list_streams().unwrap().len(), 1);
        assert_eq!(storage.get_init_apply_meta().unwrap(), Some(meta));
    }

    #[test]
    fn apply_init_json_retries_when_meta_is_stale_and_fails_on_duplicate() {
        let dir = tempdir().unwrap();
        let storage = StorageManager::new(dir.path()).unwrap();
        write_init_json(dir.path(), &sample_bundle("stream_1"));

        apply_init_json_if_needed(&storage, &|id| id == DEFAULT_FLOW_INSTANCE_ID).unwrap();
        let meta = storage
            .get_init_apply_meta()
            .unwrap()
            .expect("init apply meta exists");
        let stale_meta = StoredInitApplyMeta {
            last_applied_at_ms: meta.last_applied_at_ms,
            last_init_json_modified_at_ms: meta.last_init_json_modified_at_ms.saturating_sub(1),
        };
        storage.put_init_apply_meta(stale_meta.clone()).unwrap();

        let err =
            apply_init_json_if_needed(&storage, &|id| id == DEFAULT_FLOW_INSTANCE_ID).unwrap_err();
        assert!(err.contains("already exists: stream_1"));
        assert_eq!(storage.list_streams().unwrap().len(), 1);
        assert_eq!(storage.get_init_apply_meta().unwrap(), Some(stale_meta));
    }

    #[test]
    fn apply_init_json_rejects_undeclared_flow_instance_without_partial_writes() {
        let dir = tempdir().unwrap();
        let storage = StorageManager::new(dir.path()).unwrap();
        let mut bundle = sample_bundle("stream_1");
        bundle
            .resources
            .pipelines
            .push(sample_pipeline_request("pipe_1", "stream_1", "worker_a"));
        write_init_json(dir.path(), &bundle);

        let err =
            apply_init_json_if_needed(&storage, &|id| id == DEFAULT_FLOW_INSTANCE_ID).unwrap_err();

        assert_eq!(err, "flow instance worker_a is not declared by config");
        assert!(storage.list_streams().unwrap().is_empty());
        assert!(storage.list_pipelines().unwrap().is_empty());
        assert_eq!(storage.get_init_apply_meta().unwrap(), None);
    }

    #[test]
    fn apply_init_json_allows_pipeline_referencing_existing_stream() {
        let dir = tempdir().unwrap();
        let storage = StorageManager::new(dir.path()).unwrap();

        storage
            .create_stream(
                storage_bridge::stored_stream_from_request(&sample_stream_request("stream_1"))
                    .unwrap(),
            )
            .unwrap();

        let mut bundle = sample_bundle("stream_2");
        bundle.resources.streams.clear();
        bundle.resources.pipelines.push(sample_pipeline_request(
            "pipe_1",
            "stream_1",
            DEFAULT_FLOW_INSTANCE_ID,
        ));

        write_init_json(dir.path(), &bundle);

        apply_init_json_if_needed(&storage, &|id| id == DEFAULT_FLOW_INSTANCE_ID).unwrap();

        assert_eq!(storage.list_streams().unwrap().len(), 1);
        assert_eq!(storage.list_pipelines().unwrap().len(), 1);
        assert!(storage.get_pipeline("pipe_1").unwrap().is_some());
    }

    #[test]
    fn apply_init_json_rejects_invalid_stream_name() {
        let dir = tempdir().unwrap();
        let storage = StorageManager::new(dir.path()).unwrap();
        let mut bundle = sample_bundle("stream_1");
        bundle.resources.streams[0].name = "bad-stream".to_string();
        write_init_json(dir.path(), &bundle);

        let err =
            apply_init_json_if_needed(&storage, &|id| id == DEFAULT_FLOW_INSTANCE_ID).unwrap_err();

        assert!(err.contains("stream name"), "unexpected error: {err}");
        assert!(err.contains("invalid character"), "unexpected error: {err}");
        assert!(storage.list_streams().unwrap().is_empty());
        assert_eq!(storage.get_init_apply_meta().unwrap(), None);
    }

    #[test]
    fn apply_init_tar_gz_applies_metadata_and_uploads() {
        let dir = tempdir().unwrap();
        let storage = StorageManager::new(dir.path()).unwrap();

        // Build init.tar.gz with metadata.json + uploads/
        let bundle = sample_bundle("stream_1");
        let metadata_json = serde_json::to_vec(&bundle).unwrap();

        let tar_gz_path = dir.path().join(INIT_TAR_GZ_FILE);
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
            let mut header = tar::Header::new_gnu();
            header.set_size(4);
            header.set_mode(0o644);
            header.set_cksum();
            tar.append_data(&mut header, "uploads/cert.pem", b"data".as_slice())
                .unwrap();
            let gz = tar.into_inner().unwrap();
            gz.finish().unwrap();
        }
        fs::write(&tar_gz_path, tar_gz).unwrap();

        // Apply
        apply_init_json_if_needed(&storage, &|id| id == DEFAULT_FLOW_INSTANCE_ID).unwrap();

        // Verify metadata was applied
        assert_eq!(storage.list_streams().unwrap().len(), 1);
        assert!(storage.get_init_apply_meta().unwrap().is_some());

        // Verify upload file was copied
        let cert_path = storage.uploads_dir().join("cert.pem");
        assert!(cert_path.exists());
        let contents = fs::read_to_string(&cert_path).unwrap();
        assert_eq!(contents, "data");
    }

    #[test]
    fn init_tar_gz_takes_priority_over_init_json() {
        let dir = tempdir().unwrap();
        let storage = StorageManager::new(dir.path()).unwrap();

        // Write both init.tar.gz and init.json with different stream names
        let tar_bundle = sample_bundle("stream_from_tar");
        let metadata_json = serde_json::to_vec(&tar_bundle).unwrap();
        let tar_gz_path = dir.path().join(INIT_TAR_GZ_FILE);
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
            let gz = tar.into_inner().unwrap();
            gz.finish().unwrap();
        }
        fs::write(&tar_gz_path, tar_gz).unwrap();

        write_init_json(dir.path(), &sample_bundle("stream_from_json"));

        apply_init_json_if_needed(&storage, &|id| id == DEFAULT_FLOW_INSTANCE_ID).unwrap();

        // Should get stream from tar, not json
        let streams = storage.list_streams().unwrap();
        assert_eq!(streams.len(), 1);
        assert_eq!(streams[0].id, "stream_from_tar");
    }

    #[test]
    fn apply_init_json_rejects_pipeline_referencing_missing_stream() {
        let dir = tempdir().unwrap();
        let storage = StorageManager::new(dir.path()).unwrap();

        let mut bundle = sample_bundle("stream_unused");
        bundle.resources.streams.clear();
        bundle.resources.pipelines.push(sample_pipeline_request(
            "pipe_1",
            "missing_stream",
            DEFAULT_FLOW_INSTANCE_ID,
        ));

        write_init_json(dir.path(), &bundle);

        let err =
            apply_init_json_if_needed(&storage, &|id| id == DEFAULT_FLOW_INSTANCE_ID).unwrap_err();

        assert!(
            err.contains("references missing stream"),
            "unexpected error: {err}"
        );
        assert!(storage.list_streams().unwrap().is_empty());
        assert!(storage.list_pipelines().unwrap().is_empty());
        assert_eq!(storage.get_init_apply_meta().unwrap(), None);
    }

    #[test]
    fn init_tar_gz_corrupted_gzip_body() {
        let dir = tempdir().unwrap();
        let storage = StorageManager::new(dir.path()).unwrap();

        let tar_gz_path = dir.path().join(INIT_TAR_GZ_FILE);
        // Write random bytes that are not valid gzip
        std::fs::write(&tar_gz_path, b"not a valid gzip stream").unwrap();

        let err =
            apply_init_json_if_needed(&storage, &|id| id == DEFAULT_FLOW_INSTANCE_ID).unwrap_err();
        assert!(
            err.contains("unpack init.tar.gz"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn init_tar_gz_missing_metadata_json() {
        let dir = tempdir().unwrap();
        let storage = StorageManager::new(dir.path()).unwrap();

        // Build tar.gz with uploads/ but no metadata.json
        let tar_gz_path = dir.path().join(INIT_TAR_GZ_FILE);
        let mut tar_gz = Vec::new();
        {
            let gz = flate2::write::GzEncoder::new(&mut tar_gz, flate2::Compression::default());
            let mut tar = tar::Builder::new(gz);
            let mut header = tar::Header::new_gnu();
            header.set_size(4);
            header.set_mode(0o644);
            header.set_cksum();
            tar.append_data(&mut header, "uploads/cert.pem", b"data".as_slice())
                .unwrap();
            let gz = tar.into_inner().unwrap();
            gz.finish().unwrap();
        }
        std::fs::write(&tar_gz_path, tar_gz).unwrap();

        let err =
            apply_init_json_if_needed(&storage, &|id| id == DEFAULT_FLOW_INSTANCE_ID).unwrap_err();
        assert!(err.contains("metadata.json"), "unexpected error: {err}");
    }

    #[test]
    fn init_tar_gz_applies_nested_uploads() {
        let dir = tempdir().unwrap();
        let storage = StorageManager::new(dir.path()).unwrap();

        let bundle = sample_bundle("stream_1");
        let metadata_json = serde_json::to_vec(&bundle).unwrap();

        let tar_gz_path = dir.path().join(INIT_TAR_GZ_FILE);
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
            let mut header = tar::Header::new_gnu();
            header.set_size(5);
            header.set_mode(0o644);
            header.set_cksum();
            tar.append_data(
                &mut header,
                "uploads/proto/sub/sensor.proto",
                b"msg S".as_slice(),
            )
            .unwrap();
            let gz = tar.into_inner().unwrap();
            gz.finish().unwrap();
        }
        std::fs::write(&tar_gz_path, tar_gz).unwrap();

        apply_init_json_if_needed(&storage, &|id| id == DEFAULT_FLOW_INSTANCE_ID).unwrap();

        let data = storage.read_upload("proto/sub/sensor.proto").unwrap();
        assert_eq!(data, b"msg S");
    }
}
