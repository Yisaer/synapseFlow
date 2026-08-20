//! Adapter between flow checkpoint contracts and opaque storage records.

use flow::{CheckpointError, CheckpointManifest, CheckpointStore, OperatorSnapshot};
use serde::Deserialize;
use storage::CheckpointStorage;

#[allow(dead_code)]
#[derive(Deserialize)]
struct LegacyCheckpointManifestV1 {
    checkpoint_format_version: u32,
    flow_instance_id: String,
    pipeline_id: String,
    checkpoint_id: u64,
    removed_spec_hash: String,
    created_at_unix_ms: i64,
    operator_snapshots: Vec<OperatorSnapshot>,
}

/// Manager-owned adapter that connects flow checkpoint contracts to durable storage.
pub struct DurableCheckpointStore {
    storage: CheckpointStorage,
}

impl DurableCheckpointStore {
    /// Create a durable checkpoint store from the storage-layer namespace.
    pub fn new(storage: CheckpointStorage) -> Self {
        Self { storage }
    }
}

impl CheckpointStore for DurableCheckpointStore {
    fn load_latest(
        &self,
        flow_instance_id: &str,
        pipeline_id: &str,
    ) -> Result<Option<CheckpointManifest>, CheckpointError> {
        let Some(record) = self
            .storage
            .load_latest_record(flow_instance_id, pipeline_id)
            .map_err(storage_error)?
        else {
            return Ok(None);
        };
        let manifest: CheckpointManifest = match bincode::deserialize(&record.payload) {
            Ok(manifest) => manifest,
            Err(current_error) => {
                if let Ok(legacy) =
                    bincode::deserialize::<LegacyCheckpointManifestV1>(&record.payload)
                {
                    return Err(CheckpointError::Incompatible(format!(
                        "checkpoint format version {} is no longer supported",
                        legacy.checkpoint_format_version
                    )));
                }
                return Err(CheckpointError::Store(current_error.to_string()));
            }
        };
        if manifest.flow_instance_id != flow_instance_id
            || manifest.pipeline_id != pipeline_id
            || manifest.checkpoint_id != record.checkpoint_id
        {
            return Err(CheckpointError::Store(
                "checkpoint record identity does not match its manifest".to_string(),
            ));
        }
        manifest.validate()?;
        Ok(Some(manifest))
    }

    fn commit(&self, manifest: CheckpointManifest) -> Result<(), CheckpointError> {
        manifest.validate()?;
        let payload =
            bincode::serialize(&manifest).map_err(|err| CheckpointError::Store(err.to_string()))?;
        self.storage
            .commit_record(
                &manifest.flow_instance_id,
                &manifest.pipeline_id,
                manifest.checkpoint_id,
                &payload,
            )
            .map_err(storage_error)
    }

    fn clear(&self, flow_instance_id: &str, pipeline_id: &str) -> Result<(), CheckpointError> {
        self.storage
            .clear_records(flow_instance_id, pipeline_id)
            .map_err(storage_error)
    }
}

fn storage_error(error: storage::StorageError) -> CheckpointError {
    CheckpointError::Store(error.to_string())
}
