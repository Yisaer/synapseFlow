//! Checkpoint persistence on the shared redb backend.

use crate::RedbBackend;
use redb::{ReadableTable, TableDefinition};
use std::sync::Arc;

const CHECKPOINTS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("checkpoints");

/// A checkpoint record with a storage-defined identity and opaque payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredCheckpoint {
    pub flow_instance_id: String,
    pub pipeline_id: String,
    pub checkpoint_id: u64,
    pub payload: Vec<u8>,
}

/// Independent checkpoint namespace backed by the metadata redb file.
///
/// This type shares only the low-level database backend and access lock with metadata storage.
/// Checkpoint records and metadata records have separate tables and APIs.
#[derive(Clone)]
pub struct CheckpointStorage {
    backend: Arc<RedbBackend>,
}

impl CheckpointStorage {
    pub(crate) fn from_backend(backend: Arc<RedbBackend>) -> Result<Self, crate::StorageError> {
        let storage = Self { backend };
        storage.ensure_table()?;
        Ok(storage)
    }

    /// Load the latest record for a pipeline without interpreting its payload.
    pub fn load_latest_record(
        &self,
        flow_instance_id: &str,
        pipeline_id: &str,
    ) -> Result<Option<StoredCheckpoint>, crate::StorageError> {
        validate_checkpoint_identity(flow_instance_id, pipeline_id)?;
        let _guard = self.lock_db()?;
        let txn = self
            .backend
            .db
            .begin_read()
            .map_err(crate::StorageError::backend)?;
        let table = txn
            .open_table(CHECKPOINTS_TABLE)
            .map_err(crate::StorageError::backend)?;
        let mut latest = None;
        for entry in table
            .range::<&str>(..)
            .map_err(crate::StorageError::backend)?
        {
            let (key, value) = entry.map_err(crate::StorageError::backend)?;
            let (record_flow_instance_id, record_pipeline_id, checkpoint_id) =
                parse_checkpoint_key(key.value())?;
            if record_flow_instance_id != flow_instance_id || record_pipeline_id != pipeline_id {
                continue;
            }
            let record = StoredCheckpoint {
                flow_instance_id: record_flow_instance_id,
                pipeline_id: record_pipeline_id,
                checkpoint_id,
                payload: value.value().to_vec(),
            };
            if latest
                .as_ref()
                .is_none_or(|current: &StoredCheckpoint| current.checkpoint_id < checkpoint_id)
            {
                latest = Some(record);
            }
        }
        Ok(latest)
    }

    /// Commit an opaque checkpoint payload at the storage boundary.
    pub fn commit_record(
        &self,
        flow_instance_id: &str,
        pipeline_id: &str,
        checkpoint_id: u64,
        payload: &[u8],
    ) -> Result<(), crate::StorageError> {
        let key = checkpoint_key(flow_instance_id, pipeline_id, checkpoint_id)?;
        let _guard = self.lock_db()?;
        let txn = self
            .backend
            .db
            .begin_write()
            .map_err(crate::StorageError::backend)?;
        {
            let mut table = txn
                .open_table(CHECKPOINTS_TABLE)
                .map_err(crate::StorageError::backend)?;
            let existing = table
                .get(key.as_str())
                .map_err(crate::StorageError::backend)?
                .map(|value| value.value().to_vec());
            if let Some(existing) = existing {
                if existing != payload {
                    return Err(crate::StorageError::AlreadyExists(format!(
                        "checkpoint {checkpoint_id} is already committed with different contents"
                    )));
                }
            } else {
                table
                    .insert(key.as_str(), payload)
                    .map_err(crate::StorageError::backend)?;
            }
        }
        txn.commit().map_err(crate::StorageError::backend)?;
        Ok(())
    }

    /// Remove all records belonging to a pipeline.
    pub fn clear_records(
        &self,
        flow_instance_id: &str,
        pipeline_id: &str,
    ) -> Result<(), crate::StorageError> {
        validate_checkpoint_identity(flow_instance_id, pipeline_id)?;
        let _guard = self.lock_db()?;
        let txn = self
            .backend
            .db
            .begin_write()
            .map_err(crate::StorageError::backend)?;
        let mut keys = Vec::new();
        {
            let table = txn
                .open_table(CHECKPOINTS_TABLE)
                .map_err(crate::StorageError::backend)?;
            for entry in table
                .range::<&str>(..)
                .map_err(crate::StorageError::backend)?
            {
                let (key, _) = entry.map_err(crate::StorageError::backend)?;
                let (record_flow_instance_id, record_pipeline_id, _) =
                    parse_checkpoint_key(key.value())?;
                if record_flow_instance_id == flow_instance_id && record_pipeline_id == pipeline_id
                {
                    keys.push(key.value().to_string());
                }
            }
        }
        {
            let mut table = txn
                .open_table(CHECKPOINTS_TABLE)
                .map_err(crate::StorageError::backend)?;
            for key in keys {
                table
                    .remove(key.as_str())
                    .map_err(crate::StorageError::backend)?;
            }
        }
        txn.commit().map_err(crate::StorageError::backend)?;
        Ok(())
    }

    fn ensure_table(&self) -> Result<(), crate::StorageError> {
        let _guard = self.lock_db()?;
        let txn = self
            .backend
            .db
            .begin_write()
            .map_err(crate::StorageError::backend)?;
        txn.open_table(CHECKPOINTS_TABLE)
            .map_err(crate::StorageError::backend)?;
        txn.commit().map_err(crate::StorageError::backend)?;
        Ok(())
    }

    fn lock_db(&self) -> Result<std::sync::MutexGuard<'_, ()>, crate::StorageError> {
        self.backend.db_access_lock.lock().map_err(|err| {
            crate::StorageError::backend(format!("checkpoint storage lock poisoned: {err}"))
        })
    }
}

fn checkpoint_key(
    flow_instance_id: &str,
    pipeline_id: &str,
    checkpoint_id: u64,
) -> Result<String, crate::StorageError> {
    validate_checkpoint_identity(flow_instance_id, pipeline_id)?;
    if checkpoint_id == 0 {
        return Err(crate::StorageError::InvalidInput(
            "checkpoint id must be greater than zero".to_string(),
        ));
    }
    Ok(format!(
        "{flow_instance_id}\0{pipeline_id}\0{checkpoint_id:020}"
    ))
}

fn validate_checkpoint_identity(
    flow_instance_id: &str,
    pipeline_id: &str,
) -> Result<(), crate::StorageError> {
    if flow_instance_id.is_empty() || pipeline_id.is_empty() {
        return Err(crate::StorageError::InvalidInput(
            "checkpoint identity must not be empty".to_string(),
        ));
    }
    if flow_instance_id.contains('\0') || pipeline_id.contains('\0') {
        return Err(crate::StorageError::InvalidInput(
            "checkpoint identity must not contain NUL".to_string(),
        ));
    }
    Ok(())
}

fn parse_checkpoint_key(key: &str) -> Result<(String, String, u64), crate::StorageError> {
    let mut parts = key.split('\0');
    let flow_instance_id = parts.next().unwrap_or_default();
    let pipeline_id = parts.next().unwrap_or_default();
    let checkpoint_id = parts.next().unwrap_or_default();
    if parts.next().is_some()
        || flow_instance_id.is_empty()
        || pipeline_id.is_empty()
        || checkpoint_id.is_empty()
    {
        return Err(crate::StorageError::Corrupted("invalid checkpoint key"));
    }
    let checkpoint_id = checkpoint_id
        .parse::<u64>()
        .map_err(|_| crate::StorageError::Corrupted("invalid checkpoint id"))?;
    if checkpoint_id == 0 {
        return Err(crate::StorageError::Corrupted("invalid checkpoint id"));
    }
    Ok((
        flow_instance_id.to_string(),
        pipeline_id.to_string(),
        checkpoint_id,
    ))
}
