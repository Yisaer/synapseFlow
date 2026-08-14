use flow::pipeline::PipelineError;
use storage::{StorageManager, StoredPipelineRuntimeFailure};

pub(crate) fn matching_runtime_failure(
    storage: &StorageManager,
    pipeline_id: &str,
    revision: u64,
) -> Result<Option<StoredPipelineRuntimeFailure>, String> {
    storage
        .get_pipeline_runtime_failure(pipeline_id)
        .map_err(|err| format!("failed to read pipeline {pipeline_id} runtime failure: {err}"))
        .map(|failure| failure.filter(|failure| failure.revision == revision))
}

pub(crate) fn persist_generic_runtime_failure_marker(
    storage: &StorageManager,
    pipeline_id: &str,
    revision: u64,
    processor_kind: &str,
    reason: String,
) {
    match matching_runtime_failure(storage, pipeline_id, revision) {
        Ok(Some(existing)) => {
            tracing::warn!(
                pipeline_id = %pipeline_id,
                revision,
                processor_id = %existing.processor_id,
                processor_kind = %existing.processor_kind,
                "preserving existing pipeline runtime failure marker"
            );
            return;
        }
        Ok(None) => {}
        Err(err) => {
            tracing::warn!(
                pipeline_id = %pipeline_id,
                revision,
                error = %err,
                "failed to check existing pipeline runtime failure marker"
            );
        }
    }

    let marker = StoredPipelineRuntimeFailure {
        pipeline_id: pipeline_id.to_string(),
        revision,
        failed_at_ms: current_unix_timestamp_ms(),
        processor_id: "pipeline_runtime".to_string(),
        processor_kind: processor_kind.to_string(),
        reason,
    };
    if let Err(err) = storage.put_pipeline_runtime_failure(marker) {
        tracing::error!(
            pipeline_id = %pipeline_id,
            error = %err,
            "failed to persist pipeline runtime failure marker"
        );
    }
}

pub(crate) fn persist_start_failure_marker(
    storage: &StorageManager,
    pipeline_id: &str,
    revision: u64,
    err: &PipelineError,
) {
    persist_generic_runtime_failure_marker(
        storage,
        pipeline_id,
        revision,
        "pipeline_start",
        err.to_string(),
    );
}

fn current_unix_timestamp_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis().try_into().unwrap_or(u64::MAX))
        .unwrap_or(0)
}
