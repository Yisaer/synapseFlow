//! Runtime checkpoint contracts and test-only in-memory persistence.
//!
//! This module defines the storage-independent checkpoint model used by the pipeline runtime.
//! Runtime coordination and processor integration are implemented by the flow runtime. Durable
//! persistence is provided by the storage crate, with the flow-level adapter owned by manager.

use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use thiserror::Error;

/// The checkpoint manifest format understood by the current runtime.
pub const CHECKPOINT_FORMAT_VERSION: u32 = 2;

/// Controls whether a checkpoint barrier leaves the pipeline running.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointMode {
    /// Snapshot state and continue processing after the barrier.
    Continue,
    /// Flush final state, snapshot it, and terminate after forwarding the barrier.
    Final,
}

/// An owned, storage-independent state tree captured by a checkpoint participant.
///
/// This value is intentionally kept in memory while a checkpoint is travelling through the
/// pipeline. The manager-side checkpoint store serializes the complete manifest only when it is
/// committed to durable storage.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CheckpointState {
    /// No state value.
    Null,
    /// Boolean state.
    Bool(bool),
    /// Signed integer state.
    Signed(i64),
    /// Unsigned integer state.
    Unsigned(u64),
    /// Floating-point state.
    Float(f64),
    /// UTF-8 string state.
    String(String),
    /// Binary state that does not need an additional processor-owned serialization format.
    Bytes(Vec<u8>),
    /// Ordered state values.
    Array(Vec<Self>),
    /// Deterministically ordered named state values.
    Map(BTreeMap<String, Self>),
}

/// A versioned snapshot owned by one checkpoint participant.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OperatorSnapshot {
    /// Stable semantic identity used to match the participant in a rebuilt physical plan.
    pub checkpoint_key: String,
    /// Implementation kind used to reject incompatible restores.
    pub operator_kind: String,
    /// State schema version owned by the participant.
    pub state_version: u32,
    /// Participant-defined state tree kept in memory until manifest persistence.
    pub state: CheckpointState,
}

impl OperatorSnapshot {
    /// Build a snapshot from an in-memory state tree.
    pub fn new(
        checkpoint_key: impl Into<String>,
        operator_kind: impl Into<String>,
        state_version: u32,
        state: CheckpointState,
    ) -> Self {
        Self {
            checkpoint_key: checkpoint_key.into(),
            operator_kind: operator_kind.into(),
            state_version,
            state,
        }
    }

    /// Validate identity and state version.
    pub fn validate(&self) -> Result<(), CheckpointError> {
        if self.checkpoint_key.is_empty() {
            return Err(CheckpointError::InvalidManifest(
                "operator snapshot checkpoint key must not be empty".to_string(),
            ));
        }
        if self.operator_kind.is_empty() {
            return Err(CheckpointError::InvalidManifest(format!(
                "operator `{}` snapshot kind must not be empty",
                self.checkpoint_key
            )));
        }
        if self.state_version == 0 {
            return Err(CheckpointError::InvalidManifest(format!(
                "operator `{}` snapshot state version must be greater than zero",
                self.checkpoint_key
            )));
        }
        Ok(())
    }
}

/// A complete, committed-or-committable checkpoint snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CheckpointManifest {
    /// Version of the manifest and runtime checkpoint contract.
    pub checkpoint_format_version: u32,
    /// Flow instance that owns the pipeline runtime.
    pub flow_instance_id: String,
    /// Stable pipeline identity.
    pub pipeline_id: String,
    /// Monotonically increasing checkpoint identifier within the pipeline.
    pub checkpoint_id: u64,
    /// Creation timestamp supplied by the runtime.
    pub created_at_unix_ms: i64,
    /// Snapshots for all registered checkpoint participants.
    pub operator_snapshots: Vec<OperatorSnapshot>,
}

impl CheckpointManifest {
    /// Validate manifest metadata and every participant snapshot.
    pub fn validate(&self) -> Result<(), CheckpointError> {
        if self.checkpoint_format_version == 0 {
            return Err(CheckpointError::InvalidManifest(
                "checkpoint format version must be greater than zero".to_string(),
            ));
        }
        if self.flow_instance_id.is_empty() {
            return Err(CheckpointError::InvalidManifest(
                "flow instance id must not be empty".to_string(),
            ));
        }
        if self.pipeline_id.is_empty() {
            return Err(CheckpointError::InvalidManifest(
                "pipeline id must not be empty".to_string(),
            ));
        }
        if self.checkpoint_id == 0 {
            return Err(CheckpointError::InvalidManifest(
                "checkpoint id must be greater than zero".to_string(),
            ));
        }
        let mut checkpoint_keys = HashSet::with_capacity(self.operator_snapshots.len());
        for snapshot in &self.operator_snapshots {
            snapshot.validate()?;
            if !checkpoint_keys.insert(&snapshot.checkpoint_key) {
                return Err(CheckpointError::InvalidManifest(format!(
                    "duplicate operator snapshot checkpoint key `{}`",
                    snapshot.checkpoint_key
                )));
            }
        }
        Ok(())
    }
}

/// Errors returned by checkpoint contracts and stores.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum CheckpointError {
    /// The checkpoint manifest or participant state is invalid.
    #[error("invalid checkpoint manifest: {0}")]
    InvalidManifest(String),
    /// The persisted checkpoint uses a format that cannot be restored by this runtime.
    #[error("incompatible checkpoint: {0}")]
    Incompatible(String),
    /// The backing store failed to load or commit checkpoint state.
    #[error("checkpoint store error: {0}")]
    Store(String),
}

/// Storage-independent checkpoint persistence boundary.
pub trait CheckpointStore: Send + Sync {
    /// Load the latest committed checkpoint for a pipeline.
    fn load_latest(
        &self,
        flow_instance_id: &str,
        pipeline_id: &str,
    ) -> Result<Option<CheckpointManifest>, CheckpointError>;

    /// Atomically commit a complete checkpoint manifest.
    fn commit(&self, manifest: CheckpointManifest) -> Result<(), CheckpointError>;

    /// Remove all checkpoints belonging to a pipeline.
    fn clear(&self, flow_instance_id: &str, pipeline_id: &str) -> Result<(), CheckpointError>;
}

/// In-memory collection point for snapshots produced while a checkpoint barrier is processed.
///
/// Processors may register a snapshot without serializing it. The pipeline drains the collection
/// after the barrier reaches the tail and commits the resulting manifest as one unit.
#[derive(Debug, Default)]
pub struct CheckpointSnapshotCollector {
    snapshots: Mutex<BTreeMap<u64, BTreeMap<String, OperatorSnapshot>>>,
}

impl CheckpointSnapshotCollector {
    /// Create an empty snapshot collector.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register one participant snapshot for a checkpoint.
    pub fn record(
        &self,
        checkpoint_id: u64,
        snapshot: OperatorSnapshot,
    ) -> Result<(), CheckpointError> {
        if checkpoint_id == 0 {
            return Err(CheckpointError::InvalidManifest(
                "checkpoint id must be greater than zero".to_string(),
            ));
        }
        snapshot.validate()?;
        let checkpoint_key = snapshot.checkpoint_key.clone();
        let mut snapshots = self.snapshots.lock();
        let checkpoint_snapshots = snapshots.entry(checkpoint_id).or_default();
        if let Some(existing) = checkpoint_snapshots.get(&checkpoint_key) {
            if existing != &snapshot {
                return Err(CheckpointError::InvalidManifest(format!(
                    "conflicting operator snapshot for `{checkpoint_key}`"
                )));
            }
            return Ok(());
        }
        checkpoint_snapshots.insert(checkpoint_key, snapshot);
        Ok(())
    }

    /// Read all snapshots collected for one checkpoint in stable checkpoint-key order.
    pub fn collect(&self, checkpoint_id: u64) -> Result<Vec<OperatorSnapshot>, CheckpointError> {
        if checkpoint_id == 0 {
            return Err(CheckpointError::InvalidManifest(
                "checkpoint id must be greater than zero".to_string(),
            ));
        }
        Ok(self
            .snapshots
            .lock()
            .get(&checkpoint_id)
            .map(|snapshots| snapshots.values().cloned().collect())
            .unwrap_or_default())
    }

    /// Remove snapshots after the corresponding manifest has been committed successfully.
    pub fn clear(&self, checkpoint_id: u64) -> Result<(), CheckpointError> {
        if checkpoint_id == 0 {
            return Err(CheckpointError::InvalidManifest(
                "checkpoint id must be greater than zero".to_string(),
            ));
        }
        self.snapshots.lock().remove(&checkpoint_id);
        Ok(())
    }
}

/// In-memory checkpoint store for runtime tests and local pipeline experiments.
///
/// This store intentionally has no retention policy. Retention and durable transactions belong to
/// the production storage adapter.
#[derive(Debug, Default)]
pub struct InMemoryCheckpointStore {
    manifests: RwLock<BTreeMap<(String, String, u64), CheckpointManifest>>,
}

impl InMemoryCheckpointStore {
    /// Create an empty in-memory checkpoint store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Return the number of stored checkpoint manifests.
    pub fn len(&self) -> usize {
        self.manifests.read().len()
    }

    /// Return whether the store has no checkpoint manifests.
    pub fn is_empty(&self) -> bool {
        self.manifests.read().is_empty()
    }
}

impl CheckpointStore for InMemoryCheckpointStore {
    fn load_latest(
        &self,
        flow_instance_id: &str,
        pipeline_id: &str,
    ) -> Result<Option<CheckpointManifest>, CheckpointError> {
        let manifests = self.manifests.read();
        Ok(manifests
            .iter()
            .filter(|((flow_id, pipeline, _), _)| {
                flow_id == flow_instance_id && pipeline == pipeline_id
            })
            .max_by_key(|((_, _, checkpoint_id), _)| *checkpoint_id)
            .map(|(_, manifest)| manifest.clone()))
    }

    fn commit(&self, manifest: CheckpointManifest) -> Result<(), CheckpointError> {
        manifest.validate()?;
        let key = (
            manifest.flow_instance_id.clone(),
            manifest.pipeline_id.clone(),
            manifest.checkpoint_id,
        );
        let mut manifests = self.manifests.write();
        if let Some(existing) = manifests.get(&key) {
            if existing != &manifest {
                return Err(CheckpointError::Store(format!(
                    "checkpoint {} is already committed with different contents",
                    manifest.checkpoint_id
                )));
            }
        }
        manifests.insert(key, manifest);
        Ok(())
    }

    fn clear(&self, flow_instance_id: &str, pipeline_id: &str) -> Result<(), CheckpointError> {
        let mut manifests = self.manifests.write();
        manifests.retain(|(flow_id, pipeline, _), _| {
            flow_id != flow_instance_id || pipeline != pipeline_id
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest(checkpoint_id: u64, state: CheckpointState) -> CheckpointManifest {
        CheckpointManifest {
            checkpoint_format_version: CHECKPOINT_FORMAT_VERSION,
            flow_instance_id: "flow_a".to_string(),
            pipeline_id: "pipeline_a".to_string(),
            checkpoint_id,
            created_at_unix_ms: 1,
            operator_snapshots: vec![OperatorSnapshot::new("operator_a", "test", 1, state)],
        }
    }

    #[test]
    fn operator_snapshot_validates_state_metadata() {
        let snapshot = OperatorSnapshot::new(
            "operator_a",
            "test",
            1,
            CheckpointState::Bytes(b"state".to_vec()),
        );
        assert!(snapshot.validate().is_ok());
    }

    #[test]
    fn in_memory_store_loads_latest_checkpoint() {
        let store = InMemoryCheckpointStore::new();
        store
            .commit(manifest(1, CheckpointState::String("one".to_string())))
            .unwrap();
        store
            .commit(manifest(2, CheckpointState::String("two".to_string())))
            .unwrap();

        let latest = store
            .load_latest("flow_a", "pipeline_a")
            .unwrap()
            .expect("latest checkpoint");
        assert_eq!(latest.checkpoint_id, 2);
        assert_eq!(
            latest.operator_snapshots[0].state,
            CheckpointState::String("two".to_string())
        );
    }

    #[test]
    fn in_memory_store_rejects_conflicting_duplicate_checkpoint() {
        let store = InMemoryCheckpointStore::new();
        store
            .commit(manifest(1, CheckpointState::String("one".to_string())))
            .unwrap();

        let error = store
            .commit(manifest(
                1,
                CheckpointState::String("different".to_string()),
            ))
            .unwrap_err();
        assert!(matches!(error, CheckpointError::Store(_)));
    }

    #[test]
    fn in_memory_store_clear_removes_only_requested_pipeline() {
        let store = InMemoryCheckpointStore::new();
        store
            .commit(manifest(1, CheckpointState::String("one".to_string())))
            .unwrap();
        let mut other = manifest(2, CheckpointState::String("other".to_string()));
        other.pipeline_id = "pipeline_b".to_string();
        store.commit(other).unwrap();

        store.clear("flow_a", "pipeline_a").unwrap();
        assert!(store.load_latest("flow_a", "pipeline_a").unwrap().is_none());
        assert_eq!(
            store
                .load_latest("flow_a", "pipeline_b")
                .unwrap()
                .expect("other pipeline checkpoint")
                .checkpoint_id,
            2
        );
    }
}
