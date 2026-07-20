#![cfg_attr(
    not(test),
    deny(clippy::unwrap_used, clippy::unreachable, clippy::panic)
)]
#![forbid(unsafe_code)]

use std::fs;
use std::path::{Path, PathBuf};

use redb::{Database, ReadTransaction, ReadableTable, TableDefinition, WriteTransaction};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use thiserror::Error;

const STREAMS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("streams");
const PIPELINES_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("pipelines");
const PIPELINE_RUN_STATES_TABLE: TableDefinition<&str, &[u8]> =
    TableDefinition::new("pipeline_run_states");
const SCHEMAS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("schemas");
const SHARED_MQTT_CONFIGS_TABLE: TableDefinition<&str, &[u8]> =
    TableDefinition::new("shared_mqtt_client_configs");
const MEMORY_TOPICS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("memory_topics");
const INIT_APPLY_META_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("init_apply_meta");
const INIT_APPLY_META_KEY: &str = "init.json";
const UDFS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("udfs");

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageNamespace {
    Metadata,
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("io error: {0}")]
    Io(String),
    #[error("backend error: {0}")]
    Backend(String),
    #[error("serialization error: {0}")]
    Serialization(String),
    #[error("corrupted record: {0}")]
    Corrupted(&'static str),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("already exists: {0}")]
    AlreadyExists(String),
}

impl StorageError {
    fn io(err: std::io::Error) -> Self {
        StorageError::Io(err.to_string())
    }

    fn backend(err: impl std::fmt::Display) -> Self {
        StorageError::Backend(err.to_string())
    }

    fn serialization(err: impl std::fmt::Display) -> Self {
        StorageError::Serialization(err.to_string())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredStream {
    pub id: String,
    /// Original create-stream request serialized as JSON.
    pub raw_json: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredSchema {
    pub name: String,
    /// Schema type ("proto", "json", etc.)
    pub schema_type: String,
    /// Schema-type-specific properties serialized as JSON.
    pub props_json: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredPipeline {
    pub id: String,
    /// Original create-pipeline request serialized as JSON.
    pub raw_json: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum StoredPipelineDesiredState {
    Stopped,
    Running,
    /// Running because of a scheduler auto-start. The i64 is the
    /// unix timestamp (ms) at which the scheduler should auto-stop.
    RunningScheduled(i64),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredPipelineRunState {
    pub pipeline_id: String,
    pub desired_state: StoredPipelineDesiredState,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredMqttClientConfig {
    pub key: String,
    /// Original request serialized as JSON.
    pub raw_json: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum StoredMemoryTopicKind {
    Bytes,
    Collection,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredMemoryTopic {
    pub topic: String,
    pub kind: StoredMemoryTopicKind,
    pub capacity: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredInitApplyMeta {
    pub last_applied_at_ms: u64,
    pub last_init_json_modified_at_ms: u64,
}

/// Persisted record for a WASM UDF.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredUdf {
    /// Canonical function name (case-insensitive in SQL, stored lowercase).
    pub name: String,
    /// SHA-256 hex digest of the uploaded WASM file contents.
    pub wasm_sha256: String,
    /// Original upload metadata serialized as JSON for export portability.
    pub raw_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataExportSnapshot {
    pub streams: Vec<StoredStream>,
    pub schemas: Vec<StoredSchema>,
    pub pipelines: Vec<StoredPipeline>,
    pub pipeline_run_states: Vec<StoredPipelineRunState>,
    pub mqtt_configs: Vec<StoredMqttClientConfig>,
    pub memory_topics: Vec<StoredMemoryTopic>,
    pub udfs: Vec<StoredUdf>,
}

/// Information about an uploaded file returned by the list API.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct UploadFileInfo {
    pub name: String,
    pub size_bytes: u64,
    pub modified_at: u64,
}

pub struct MetadataStorage {
    db: Database,
    db_path: PathBuf,
}

impl MetadataStorage {
    /// Open (or create) the metadata namespace.
    pub fn open(base_dir: impl AsRef<Path>) -> Result<Self, StorageError> {
        let db_path = Self::db_path(base_dir.as_ref());
        if let Some(parent) = db_path.parent() {
            fs::create_dir_all(parent).map_err(StorageError::io)?;
        }

        let db = if db_path.exists() {
            Database::builder()
                .open(db_path.as_path())
                .map_err(StorageError::backend)?
        } else {
            Database::builder()
                .create(db_path.as_path())
                .map_err(StorageError::backend)?
        };

        let storage = Self {
            db,
            db_path: db_path.clone(),
        };
        storage.ensure_tables()?;
        Ok(storage)
    }

    pub fn namespace(&self) -> StorageNamespace {
        StorageNamespace::Metadata
    }

    pub fn path(&self) -> &Path {
        &self.db_path
    }

    pub fn create_stream(&self, stream: StoredStream) -> Result<(), StorageError> {
        self.insert_if_absent(STREAMS_TABLE, &stream.id, &stream)
    }

    pub fn get_stream(&self, id: &str) -> Result<Option<StoredStream>, StorageError> {
        self.get_entry(STREAMS_TABLE, id)
    }

    pub fn list_streams(&self) -> Result<Vec<StoredStream>, StorageError> {
        self.list_entries(STREAMS_TABLE)
    }

    pub fn delete_stream(&self, id: &str) -> Result<(), StorageError> {
        self.delete_entry(STREAMS_TABLE, id)
    }

    pub fn create_schema(&self, schema: StoredSchema) -> Result<(), StorageError> {
        self.insert_if_absent(SCHEMAS_TABLE, &schema.name, &schema)
    }

    pub fn get_schema(&self, name: &str) -> Result<Option<StoredSchema>, StorageError> {
        self.get_entry(SCHEMAS_TABLE, name)
    }

    pub fn list_schemas(&self) -> Result<Vec<StoredSchema>, StorageError> {
        self.list_entries(SCHEMAS_TABLE)
    }

    pub fn delete_schema(&self, name: &str) -> Result<(), StorageError> {
        self.delete_entry(SCHEMAS_TABLE, name)
    }

    pub fn create_pipeline(&self, pipeline: StoredPipeline) -> Result<(), StorageError> {
        self.insert_if_absent(PIPELINES_TABLE, &pipeline.id, &pipeline)
    }

    pub fn get_pipeline(&self, id: &str) -> Result<Option<StoredPipeline>, StorageError> {
        self.get_entry(PIPELINES_TABLE, id)
    }

    pub fn list_pipelines(&self) -> Result<Vec<StoredPipeline>, StorageError> {
        self.list_entries(PIPELINES_TABLE)
    }

    pub fn delete_pipeline(&self, id: &str) -> Result<(), StorageError> {
        let txn = self.db.begin_write().map_err(StorageError::backend)?;
        {
            let mut pipelines = txn
                .open_table(PIPELINES_TABLE)
                .map_err(StorageError::backend)?;
            let removed = pipelines.remove(id).map_err(StorageError::backend)?;
            if removed.is_none() {
                return Err(StorageError::NotFound(id.to_string()));
            }

            let mut run_states = txn
                .open_table(PIPELINE_RUN_STATES_TABLE)
                .map_err(StorageError::backend)?;
            let _ = run_states.remove(id).map_err(StorageError::backend)?;
        }
        txn.commit().map_err(StorageError::backend)?;
        Ok(())
    }

    pub fn put_pipeline_run_state(
        &self,
        state: StoredPipelineRunState,
    ) -> Result<(), StorageError> {
        let txn = self.db.begin_write().map_err(StorageError::backend)?;
        {
            let mut table = txn
                .open_table(PIPELINE_RUN_STATES_TABLE)
                .map_err(StorageError::backend)?;
            let encoded = encode_record(&state)?;
            table
                .insert(state.pipeline_id.as_str(), encoded.as_slice())
                .map_err(StorageError::backend)?;
        }
        txn.commit().map_err(StorageError::backend)?;
        Ok(())
    }

    pub fn get_pipeline_run_state(
        &self,
        pipeline_id: &str,
    ) -> Result<Option<StoredPipelineRunState>, StorageError> {
        self.get_entry(PIPELINE_RUN_STATES_TABLE, pipeline_id)
    }

    pub fn delete_pipeline_run_state(&self, pipeline_id: &str) -> Result<(), StorageError> {
        self.delete_entry(PIPELINE_RUN_STATES_TABLE, pipeline_id)
    }

    pub fn create_mqtt_config(&self, config: StoredMqttClientConfig) -> Result<(), StorageError> {
        self.insert_if_absent(SHARED_MQTT_CONFIGS_TABLE, &config.key, &config)
    }

    pub fn get_mqtt_config(
        &self,
        key: &str,
    ) -> Result<Option<StoredMqttClientConfig>, StorageError> {
        self.get_entry(SHARED_MQTT_CONFIGS_TABLE, key)
    }

    pub fn list_mqtt_configs(&self) -> Result<Vec<StoredMqttClientConfig>, StorageError> {
        self.list_entries(SHARED_MQTT_CONFIGS_TABLE)
    }

    pub fn delete_mqtt_config(&self, key: &str) -> Result<(), StorageError> {
        self.delete_entry(SHARED_MQTT_CONFIGS_TABLE, key)
    }

    pub fn create_memory_topic(&self, topic: StoredMemoryTopic) -> Result<(), StorageError> {
        self.insert_if_absent(MEMORY_TOPICS_TABLE, &topic.topic, &topic)
    }

    pub fn get_memory_topic(&self, topic: &str) -> Result<Option<StoredMemoryTopic>, StorageError> {
        self.get_entry(MEMORY_TOPICS_TABLE, topic)
    }

    pub fn list_memory_topics(&self) -> Result<Vec<StoredMemoryTopic>, StorageError> {
        self.list_entries(MEMORY_TOPICS_TABLE)
    }

    pub fn delete_memory_topic(&self, topic: &str) -> Result<(), StorageError> {
        self.delete_entry(MEMORY_TOPICS_TABLE, topic)
    }

    pub fn create_udf(&self, udf: StoredUdf) -> Result<(), StorageError> {
        self.insert_if_absent(UDFS_TABLE, &udf.name, &udf)
    }

    pub fn get_udf(&self, name: &str) -> Result<Option<StoredUdf>, StorageError> {
        self.get_entry(UDFS_TABLE, name)
    }

    pub fn list_udfs(&self) -> Result<Vec<StoredUdf>, StorageError> {
        self.list_entries(UDFS_TABLE)
    }

    pub fn delete_udf(&self, name: &str) -> Result<(), StorageError> {
        self.delete_entry(UDFS_TABLE, name)
    }

    pub fn export_snapshot(&self) -> Result<MetadataExportSnapshot, StorageError> {
        let txn = self.db.begin_read().map_err(StorageError::backend)?;
        Ok(MetadataExportSnapshot {
            streams: Self::list_entries_in_read_txn(&txn, STREAMS_TABLE)?,
            schemas: Self::list_entries_in_read_txn(&txn, SCHEMAS_TABLE)?,
            pipelines: Self::list_entries_in_read_txn(&txn, PIPELINES_TABLE)?,
            pipeline_run_states: Self::list_entries_in_read_txn(&txn, PIPELINE_RUN_STATES_TABLE)?,
            mqtt_configs: Self::list_entries_in_read_txn(&txn, SHARED_MQTT_CONFIGS_TABLE)?,
            memory_topics: Self::list_entries_in_read_txn(&txn, MEMORY_TOPICS_TABLE)?,
            udfs: Self::list_entries_in_read_txn(&txn, UDFS_TABLE)?,
        })
    }

    pub fn replace_snapshot(&self, snapshot: MetadataExportSnapshot) -> Result<(), StorageError> {
        let txn = self.db.begin_write().map_err(StorageError::backend)?;
        Self::replace_table_entries(&txn, STREAMS_TABLE, &snapshot.streams, |stream| {
            stream.id.as_str()
        })?;
        Self::replace_table_entries(&txn, SCHEMAS_TABLE, &snapshot.schemas, |schema| {
            schema.name.as_str()
        })?;
        Self::replace_table_entries(&txn, PIPELINES_TABLE, &snapshot.pipelines, |pipeline| {
            pipeline.id.as_str()
        })?;
        Self::replace_table_entries(
            &txn,
            PIPELINE_RUN_STATES_TABLE,
            &snapshot.pipeline_run_states,
            |state| state.pipeline_id.as_str(),
        )?;
        Self::replace_table_entries(
            &txn,
            SHARED_MQTT_CONFIGS_TABLE,
            &snapshot.mqtt_configs,
            |cfg| cfg.key.as_str(),
        )?;
        Self::replace_table_entries(
            &txn,
            MEMORY_TOPICS_TABLE,
            &snapshot.memory_topics,
            |topic| topic.topic.as_str(),
        )?;
        Self::replace_table_entries(&txn, UDFS_TABLE, &snapshot.udfs, |udf| udf.name.as_str())?;
        txn.commit().map_err(StorageError::backend)?;
        Ok(())
    }

    pub fn get_init_apply_meta(&self) -> Result<Option<StoredInitApplyMeta>, StorageError> {
        self.get_entry(INIT_APPLY_META_TABLE, INIT_APPLY_META_KEY)
    }

    pub fn put_init_apply_meta(&self, meta: StoredInitApplyMeta) -> Result<(), StorageError> {
        self.put_entry(INIT_APPLY_META_TABLE, INIT_APPLY_META_KEY, &meta)
    }

    pub fn apply_init_snapshot(
        &self,
        snapshot: MetadataExportSnapshot,
        meta: StoredInitApplyMeta,
    ) -> Result<(), StorageError> {
        let txn = self.db.begin_write().map_err(StorageError::backend)?;
        Self::ensure_entries_absent(&txn, STREAMS_TABLE, &snapshot.streams, |stream| {
            stream.id.as_str()
        })?;
        Self::ensure_entries_absent(&txn, SCHEMAS_TABLE, &snapshot.schemas, |schema| {
            schema.name.as_str()
        })?;
        Self::ensure_entries_absent(&txn, PIPELINES_TABLE, &snapshot.pipelines, |pipeline| {
            pipeline.id.as_str()
        })?;
        Self::ensure_entries_absent(
            &txn,
            PIPELINE_RUN_STATES_TABLE,
            &snapshot.pipeline_run_states,
            |state| state.pipeline_id.as_str(),
        )?;
        Self::ensure_entries_absent(
            &txn,
            SHARED_MQTT_CONFIGS_TABLE,
            &snapshot.mqtt_configs,
            |cfg| cfg.key.as_str(),
        )?;
        Self::ensure_entries_absent(
            &txn,
            MEMORY_TOPICS_TABLE,
            &snapshot.memory_topics,
            |topic| topic.topic.as_str(),
        )?;
        Self::ensure_entries_absent(&txn, UDFS_TABLE, &snapshot.udfs, |udf| udf.name.as_str())?;

        Self::insert_entries(&txn, STREAMS_TABLE, &snapshot.streams, |stream| {
            stream.id.as_str()
        })?;
        Self::insert_entries(&txn, SCHEMAS_TABLE, &snapshot.schemas, |schema| {
            schema.name.as_str()
        })?;
        Self::insert_entries(&txn, PIPELINES_TABLE, &snapshot.pipelines, |pipeline| {
            pipeline.id.as_str()
        })?;
        Self::insert_entries(
            &txn,
            PIPELINE_RUN_STATES_TABLE,
            &snapshot.pipeline_run_states,
            |state| state.pipeline_id.as_str(),
        )?;
        Self::insert_entries(
            &txn,
            SHARED_MQTT_CONFIGS_TABLE,
            &snapshot.mqtt_configs,
            |cfg| cfg.key.as_str(),
        )?;
        Self::insert_entries(
            &txn,
            MEMORY_TOPICS_TABLE,
            &snapshot.memory_topics,
            |topic| topic.topic.as_str(),
        )?;
        Self::insert_entries(&txn, UDFS_TABLE, &snapshot.udfs, |udf| udf.name.as_str())?;
        Self::put_entry_in_txn(&txn, INIT_APPLY_META_TABLE, INIT_APPLY_META_KEY, &meta)?;
        txn.commit().map_err(StorageError::backend)?;
        Ok(())
    }

    fn db_path(base_dir: &Path) -> PathBuf {
        base_dir.join("metadata.redb")
    }

    fn ensure_tables(&self) -> Result<(), StorageError> {
        let txn = self.db.begin_write().map_err(StorageError::backend)?;
        txn.open_table(STREAMS_TABLE)
            .map_err(StorageError::backend)?;
        txn.open_table(PIPELINES_TABLE)
            .map_err(StorageError::backend)?;
        txn.open_table(PIPELINE_RUN_STATES_TABLE)
            .map_err(StorageError::backend)?;
        txn.open_table(SHARED_MQTT_CONFIGS_TABLE)
            .map_err(StorageError::backend)?;
        txn.open_table(MEMORY_TOPICS_TABLE)
            .map_err(StorageError::backend)?;
        txn.open_table(INIT_APPLY_META_TABLE)
            .map_err(StorageError::backend)?;
        txn.open_table(UDFS_TABLE).map_err(StorageError::backend)?;
        txn.open_table(SCHEMAS_TABLE)
            .map_err(StorageError::backend)?;
        txn.commit().map_err(StorageError::backend)?;
        Ok(())
    }

    fn insert_if_absent<T: Serialize>(
        &self,
        table: TableDefinition<&str, &[u8]>,
        key: &str,
        value: &T,
    ) -> Result<(), StorageError> {
        let txn = self.db.begin_write().map_err(StorageError::backend)?;
        {
            let mut table = txn.open_table(table).map_err(StorageError::backend)?;
            if table.get(key).map_err(StorageError::backend)?.is_some() {
                return Err(StorageError::AlreadyExists(key.to_string()));
            }
            let encoded = encode_record(value)?;
            table
                .insert(key, encoded.as_slice())
                .map_err(StorageError::backend)?;
        }
        txn.commit().map_err(StorageError::backend)?;
        Ok(())
    }

    fn put_entry<T: Serialize>(
        &self,
        table: TableDefinition<&str, &[u8]>,
        key: &str,
        value: &T,
    ) -> Result<(), StorageError> {
        let txn = self.db.begin_write().map_err(StorageError::backend)?;
        Self::put_entry_in_txn(&txn, table, key, value)?;
        txn.commit().map_err(StorageError::backend)?;
        Ok(())
    }

    fn get_entry<T: DeserializeOwned>(
        &self,
        table: TableDefinition<&str, &[u8]>,
        key: &str,
    ) -> Result<Option<T>, StorageError> {
        let txn = self.db.begin_read().map_err(StorageError::backend)?;
        let table = txn.open_table(table).map_err(StorageError::backend)?;
        let result = match table.get(key).map_err(StorageError::backend)? {
            Some(value) => {
                let raw = value.value();
                Some(decode_record(raw)?)
            }
            None => None,
        };
        Ok(result)
    }

    fn delete_entry(
        &self,
        table: TableDefinition<&str, &[u8]>,
        key: &str,
    ) -> Result<(), StorageError> {
        let txn = self.db.begin_write().map_err(StorageError::backend)?;
        {
            let mut table = txn.open_table(table).map_err(StorageError::backend)?;
            let removed = table.remove(key).map_err(StorageError::backend)?;
            if removed.is_none() {
                return Err(StorageError::NotFound(key.to_string()));
            }
        }
        txn.commit().map_err(StorageError::backend)?;
        Ok(())
    }

    fn list_entries<T: DeserializeOwned>(
        &self,
        table: TableDefinition<&str, &[u8]>,
    ) -> Result<Vec<T>, StorageError> {
        let txn = self.db.begin_read().map_err(StorageError::backend)?;
        Self::list_entries_in_read_txn(&txn, table)
    }

    fn list_entries_in_read_txn<T: DeserializeOwned>(
        txn: &ReadTransaction,
        table: TableDefinition<&str, &[u8]>,
    ) -> Result<Vec<T>, StorageError> {
        let table = txn.open_table(table).map_err(StorageError::backend)?;
        let mut items = Vec::new();
        for entry in table.range::<&str>(..).map_err(StorageError::backend)? {
            let (_, value) = entry.map_err(StorageError::backend)?;
            items.push(decode_record(value.value())?);
        }
        Ok(items)
    }

    fn replace_table_entries<T, F>(
        txn: &WriteTransaction,
        table: TableDefinition<&str, &[u8]>,
        values: &[T],
        key_fn: F,
    ) -> Result<(), StorageError>
    where
        T: Serialize,
        F: Fn(&T) -> &str,
    {
        let mut table = txn.open_table(table).map_err(StorageError::backend)?;
        drop(table.drain::<&str>(..).map_err(StorageError::backend)?);
        for value in values {
            let encoded = encode_record(value)?;
            table
                .insert(key_fn(value), encoded.as_slice())
                .map_err(StorageError::backend)?;
        }
        Ok(())
    }

    fn ensure_entries_absent<T, F>(
        txn: &WriteTransaction,
        table: TableDefinition<&str, &[u8]>,
        values: &[T],
        key_fn: F,
    ) -> Result<(), StorageError>
    where
        F: Fn(&T) -> &str,
    {
        let table = txn.open_table(table).map_err(StorageError::backend)?;
        for value in values {
            let key = key_fn(value);
            if table.get(key).map_err(StorageError::backend)?.is_some() {
                return Err(StorageError::AlreadyExists(key.to_string()));
            }
        }
        Ok(())
    }

    fn insert_entries<T, F>(
        txn: &WriteTransaction,
        table: TableDefinition<&str, &[u8]>,
        values: &[T],
        key_fn: F,
    ) -> Result<(), StorageError>
    where
        T: Serialize,
        F: Fn(&T) -> &str,
    {
        let mut table = txn.open_table(table).map_err(StorageError::backend)?;
        for value in values {
            let encoded = encode_record(value)?;
            table
                .insert(key_fn(value), encoded.as_slice())
                .map_err(StorageError::backend)?;
        }
        Ok(())
    }

    fn put_entry_in_txn<T: Serialize>(
        txn: &WriteTransaction,
        table: TableDefinition<&str, &[u8]>,
        key: &str,
        value: &T,
    ) -> Result<(), StorageError> {
        let mut table = txn.open_table(table).map_err(StorageError::backend)?;
        let encoded = encode_record(value)?;
        table
            .insert(key, encoded.as_slice())
            .map_err(StorageError::backend)?;
        Ok(())
    }
}

/// Recursively copy upload files from src to dst. Returns the count of copied files.
fn copy_uploads_recursive(src: &Path, dst: &Path) -> Result<usize, StorageError> {
    let mut copied = 0usize;
    for entry in std::fs::read_dir(src).map_err(StorageError::io)? {
        let entry = entry.map_err(StorageError::io)?;
        let file_type = entry.file_type().map_err(StorageError::io)?;
        let raw_name = entry.file_name().to_string_lossy().into_owned();
        if raw_name.starts_with('.') && raw_name.ends_with(".tmp") {
            continue;
        }
        if file_type.is_dir() {
            let sub_dst = dst.join(&raw_name);
            std::fs::create_dir_all(&sub_dst).map_err(StorageError::io)?;
            copied += copy_uploads_recursive(&entry.path(), &sub_dst)?;
        } else if file_type.is_file() {
            let target = dst.join(&raw_name);
            std::fs::copy(entry.path(), &target).map_err(StorageError::io)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o600))
                    .map_err(StorageError::io)?;
            }
            copied += 1;
        }
    }
    Ok(copied)
}

/// Helper: recursively collect files from an uploads subdirectory.
fn collect_upload_entries(
    dir: &Path,
    prefix: &str,
    files: &mut Vec<UploadFileInfo>,
) -> Result<(), StorageError> {
    for entry in std::fs::read_dir(dir).map_err(StorageError::io)? {
        let entry = entry.map_err(StorageError::io)?;
        let file_type = entry.file_type().map_err(StorageError::io)?;
        let raw_name = entry.file_name().to_string_lossy().into_owned();
        if raw_name.starts_with('.') && raw_name.ends_with(".tmp") {
            continue;
        }
        if file_type.is_dir() {
            let sub_prefix = if prefix.is_empty() {
                raw_name.clone()
            } else {
                format!("{prefix}/{raw_name}")
            };
            collect_upload_entries(&entry.path(), &sub_prefix, files)?;
        } else if file_type.is_file() {
            let metadata = entry.metadata().map_err(StorageError::io)?;
            let name = if prefix.is_empty() {
                raw_name
            } else {
                format!("{prefix}/{raw_name}")
            };
            let modified_at = metadata
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_secs())
                .unwrap_or(0);
            files.push(UploadFileInfo {
                name,
                size_bytes: metadata.len(),
                modified_at,
            });
        }
    }
    Ok(())
}

/// Facade that exposes storage capabilities; currently only metadata, future namespaces can be added.
pub struct StorageManager {
    metadata: MetadataStorage,
    base_dir: PathBuf,
}

impl StorageManager {
    /// Create a storage manager; initializes the metadata namespace under base_dir/metadata.
    /// Also creates base_dir/wasm_files/ for WASM UDF binaries.
    pub fn new(base_dir: impl AsRef<Path>) -> Result<Self, StorageError> {
        let base_dir = base_dir.as_ref().to_path_buf();
        let metadata = MetadataStorage::open(base_dir.join("metadata"))?;
        let wasm_dir = base_dir.join("wasm_files");
        fs::create_dir_all(&wasm_dir).map_err(StorageError::io)?;
        Ok(Self { metadata, base_dir })
    }

    pub fn metadata(&self) -> &MetadataStorage {
        &self.metadata
    }

    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    /// Returns the directory where WASM UDF binaries are persisted.
    pub fn wasm_files_dir(&self) -> PathBuf {
        self.base_dir.join("wasm_files")
    }

    /// Returns the directory where uploaded user files are persisted.
    pub fn uploads_dir(&self) -> PathBuf {
        self.base_dir.join("uploads")
    }

    /// Save an uploaded file to the uploads directory. Overwrites if a file with
    /// the same name already exists. Uses a temp-file-and-rename pattern to avoid
    /// partial writes.
    pub fn save_upload(&self, name: &str, data: &[u8]) -> Result<(), StorageError> {
        let target = self.uploads_dir().join(name);
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent).map_err(StorageError::io)?;
        }
        let file_stem = target.file_name().ok_or_else(|| {
            StorageError::io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "empty file name",
            ))
        })?;
        let tmp = target.with_file_name(format!(".{}.tmp", file_stem.to_string_lossy()));
        fs::write(&tmp, data).map_err(StorageError::io)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&tmp, fs::Permissions::from_mode(0o600))
                .map_err(StorageError::io)?;
        }
        fs::rename(&tmp, &target).map_err(StorageError::io)?;
        Ok(())
    }

    /// Read an uploaded file from the uploads directory.
    pub fn read_upload(&self, name: &str) -> Result<Vec<u8>, StorageError> {
        let path = self.uploads_dir().join(name);
        fs::read(&path).map_err(StorageError::io)
    }

    /// Delete an uploaded file. Returns Ok(true) if deleted, Ok(false) if not found.
    pub fn delete_upload(&self, name: &str) -> Result<bool, StorageError> {
        let path = self.uploads_dir().join(name);
        match fs::remove_file(&path) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(StorageError::io(e)),
        }
    }

    /// List all files in the uploads directory recursively with metadata.
    pub fn list_uploads(&self) -> Result<Vec<UploadFileInfo>, StorageError> {
        let uploads_dir = self.uploads_dir();
        if !uploads_dir.exists() {
            return Ok(Vec::new());
        }
        let mut files = Vec::new();
        collect_upload_entries(&uploads_dir, "", &mut files)?;
        files.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(files)
    }

    /// Copy files from a source uploads directory into the data-dir uploads
    /// directory. Handles nested subdirectories. Skips temp files. Returns the
    /// count of copied files.
    pub fn copy_uploads_from_dir(&self, src_dir: &Path) -> Result<usize, StorageError> {
        if !src_dir.exists() || !src_dir.is_dir() {
            return Ok(0);
        }
        let dst_dir = self.uploads_dir();
        fs::create_dir_all(&dst_dir).map_err(StorageError::io)?;
        copy_uploads_recursive(src_dir, &dst_dir)
    }

    pub fn create_stream(&self, stream: StoredStream) -> Result<(), StorageError> {
        self.metadata.create_stream(stream)
    }

    pub fn get_stream(&self, id: &str) -> Result<Option<StoredStream>, StorageError> {
        self.metadata.get_stream(id)
    }

    pub fn list_streams(&self) -> Result<Vec<StoredStream>, StorageError> {
        self.metadata.list_streams()
    }

    pub fn delete_stream(&self, id: &str) -> Result<(), StorageError> {
        self.metadata.delete_stream(id)
    }

    pub fn create_schema(&self, schema: StoredSchema) -> Result<(), StorageError> {
        self.metadata.create_schema(schema)
    }

    pub fn get_schema(&self, name: &str) -> Result<Option<StoredSchema>, StorageError> {
        self.metadata.get_schema(name)
    }

    pub fn list_schemas(&self) -> Result<Vec<StoredSchema>, StorageError> {
        self.metadata.list_schemas()
    }

    pub fn delete_schema(&self, name: &str) -> Result<(), StorageError> {
        self.metadata.delete_schema(name)
    }

    pub fn create_pipeline(&self, pipeline: StoredPipeline) -> Result<(), StorageError> {
        self.metadata.create_pipeline(pipeline)
    }

    pub fn get_pipeline(&self, id: &str) -> Result<Option<StoredPipeline>, StorageError> {
        self.metadata.get_pipeline(id)
    }

    pub fn list_pipelines(&self) -> Result<Vec<StoredPipeline>, StorageError> {
        self.metadata.list_pipelines()
    }

    pub fn delete_pipeline(&self, id: &str) -> Result<(), StorageError> {
        self.metadata.delete_pipeline(id)
    }

    pub fn put_pipeline_run_state(
        &self,
        state: StoredPipelineRunState,
    ) -> Result<(), StorageError> {
        self.metadata.put_pipeline_run_state(state)
    }

    pub fn get_pipeline_run_state(
        &self,
        pipeline_id: &str,
    ) -> Result<Option<StoredPipelineRunState>, StorageError> {
        self.metadata.get_pipeline_run_state(pipeline_id)
    }

    pub fn delete_pipeline_run_state(&self, pipeline_id: &str) -> Result<(), StorageError> {
        self.metadata.delete_pipeline_run_state(pipeline_id)
    }

    pub fn create_mqtt_config(&self, config: StoredMqttClientConfig) -> Result<(), StorageError> {
        self.metadata.create_mqtt_config(config)
    }

    pub fn get_mqtt_config(
        &self,
        key: &str,
    ) -> Result<Option<StoredMqttClientConfig>, StorageError> {
        self.metadata.get_mqtt_config(key)
    }

    pub fn list_mqtt_configs(&self) -> Result<Vec<StoredMqttClientConfig>, StorageError> {
        self.metadata.list_mqtt_configs()
    }

    pub fn delete_mqtt_config(&self, key: &str) -> Result<(), StorageError> {
        self.metadata.delete_mqtt_config(key)
    }

    pub fn create_memory_topic(&self, topic: StoredMemoryTopic) -> Result<(), StorageError> {
        self.metadata.create_memory_topic(topic)
    }

    pub fn get_memory_topic(&self, topic: &str) -> Result<Option<StoredMemoryTopic>, StorageError> {
        self.metadata.get_memory_topic(topic)
    }

    pub fn list_memory_topics(&self) -> Result<Vec<StoredMemoryTopic>, StorageError> {
        self.metadata.list_memory_topics()
    }

    pub fn delete_memory_topic(&self, topic: &str) -> Result<(), StorageError> {
        self.metadata.delete_memory_topic(topic)
    }

    pub fn create_udf(&self, udf: StoredUdf) -> Result<(), StorageError> {
        self.metadata.create_udf(udf)
    }

    pub fn get_udf(&self, name: &str) -> Result<Option<StoredUdf>, StorageError> {
        self.metadata.get_udf(name)
    }

    pub fn list_udfs(&self) -> Result<Vec<StoredUdf>, StorageError> {
        self.metadata.list_udfs()
    }

    pub fn delete_udf(&self, name: &str) -> Result<(), StorageError> {
        self.metadata.delete_udf(name)
    }

    pub fn export_metadata_snapshot(&self) -> Result<MetadataExportSnapshot, StorageError> {
        self.metadata.export_snapshot()
    }

    pub fn replace_metadata_snapshot(
        &self,
        snapshot: MetadataExportSnapshot,
    ) -> Result<(), StorageError> {
        self.metadata.replace_snapshot(snapshot)
    }

    pub fn get_init_apply_meta(&self) -> Result<Option<StoredInitApplyMeta>, StorageError> {
        self.metadata.get_init_apply_meta()
    }

    pub fn put_init_apply_meta(&self, meta: StoredInitApplyMeta) -> Result<(), StorageError> {
        self.metadata.put_init_apply_meta(meta)
    }

    pub fn apply_init_snapshot(
        &self,
        snapshot: MetadataExportSnapshot,
        meta: StoredInitApplyMeta,
    ) -> Result<(), StorageError> {
        self.metadata.apply_init_snapshot(snapshot, meta)
    }
}

fn encode_record<T: Serialize>(value: &T) -> Result<Vec<u8>, StorageError> {
    bincode::serialize(value).map_err(StorageError::serialization)
}

fn decode_record<T: DeserializeOwned>(raw: &[u8]) -> Result<T, StorageError> {
    bincode::deserialize(raw).map_err(StorageError::serialization)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn sample_stream() -> StoredStream {
        StoredStream {
            id: "stream_1".to_string(),
            raw_json: r#"{"name":"stream_1","type":"mqtt","schema":{"columns":[]}}"#.to_string(),
        }
    }

    fn sample_pipeline() -> StoredPipeline {
        StoredPipeline {
            id: "pipe_1".to_string(),
            raw_json: r#"{"id":"pipe_1","sql":"SELECT * FROM stream_1","sinks":[]}"#.to_string(),
        }
    }

    fn sample_mqtt_config() -> StoredMqttClientConfig {
        StoredMqttClientConfig {
            key: "shared_a".to_string(),
            raw_json: r#"{"key":"shared_a","broker_url":"tcp://localhost:1883","topic":"foo/bar","client_id":"client_a","qos":1}"#.to_string(),
        }
    }

    fn sample_pipeline_run_state() -> StoredPipelineRunState {
        StoredPipelineRunState {
            pipeline_id: "pipe_1".to_string(),
            desired_state: StoredPipelineDesiredState::Running,
        }
    }

    fn sample_memory_topic() -> StoredMemoryTopic {
        StoredMemoryTopic {
            topic: "topic_1".to_string(),
            kind: StoredMemoryTopicKind::Bytes,
            capacity: 16,
        }
    }

    fn sample_udf(name: &str) -> StoredUdf {
        StoredUdf {
            name: name.to_string(),
            wasm_sha256: "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2"
                .to_string(),
            raw_json: format!(r#"{{"name":"{name}","description":"test udf"}}"#),
        }
    }

    fn sample_init_apply_meta() -> StoredInitApplyMeta {
        StoredInitApplyMeta {
            last_applied_at_ms: 1234,
            last_init_json_modified_at_ms: 5678,
        }
    }

    #[test]
    fn udf_roundtrip() {
        let dir = tempdir().unwrap();
        let storage = MetadataStorage::open(dir.path()).unwrap();

        let udf = sample_udf("my_udf");
        storage.create_udf(udf.clone()).unwrap();
        assert_eq!(storage.get_udf("my_udf").unwrap(), Some(udf.clone()));

        let all = storage.list_udfs().unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].name, "my_udf");

        storage.delete_udf("my_udf").unwrap();
        assert!(storage.get_udf("my_udf").unwrap().is_none());
    }

    #[test]
    fn udf_create_fails_on_duplicate() {
        let dir = tempdir().unwrap();
        let storage = MetadataStorage::open(dir.path()).unwrap();

        let udf = sample_udf("my_udf");
        storage.create_udf(udf.clone()).unwrap();
        match storage.create_udf(udf) {
            Err(StorageError::AlreadyExists(name)) => assert_eq!(name, "my_udf"),
            other => panic!("expected AlreadyExists, got {other:?}"),
        }
    }

    #[test]
    fn udf_delete_nonexistent() {
        let dir = tempdir().unwrap();
        let storage = MetadataStorage::open(dir.path()).unwrap();

        match storage.delete_udf("nonexistent") {
            Err(StorageError::NotFound(name)) => assert_eq!(name, "nonexistent"),
            other => panic!("expected NotFound, got {other:?}"),
        }
    }

    #[test]
    fn stream_pipeline_mqtt_roundtrip() {
        let dir = tempdir().unwrap();
        let storage = MetadataStorage::open(dir.path()).unwrap();

        let stream = sample_stream();
        storage.create_stream(stream.clone()).unwrap();
        assert_eq!(
            storage.get_stream(&stream.id).unwrap(),
            Some(stream.clone())
        );
        let streams = storage.list_streams().unwrap();
        assert_eq!(streams.len(), 1);

        let pipeline = sample_pipeline();
        storage.create_pipeline(pipeline.clone()).unwrap();
        assert_eq!(
            storage.get_pipeline(&pipeline.id).unwrap(),
            Some(pipeline.clone())
        );
        let pipelines = storage.list_pipelines().unwrap();
        assert_eq!(pipelines.len(), 1);

        let mqtt = sample_mqtt_config();
        storage.create_mqtt_config(mqtt.clone()).unwrap();
        assert_eq!(
            storage.get_mqtt_config(&mqtt.key).unwrap(),
            Some(mqtt.clone())
        );
        let mqtts = storage.list_mqtt_configs().unwrap();
        assert_eq!(mqtts.len(), 1);

        storage.delete_stream(&stream.id).unwrap();
        storage.delete_pipeline(&pipeline.id).unwrap();
        storage.delete_mqtt_config(&mqtt.key).unwrap();

        assert!(storage.get_stream(&stream.id).unwrap().is_none());
        assert!(storage.get_pipeline(&pipeline.id).unwrap().is_none());
        assert!(storage.get_mqtt_config(&mqtt.key).unwrap().is_none());
    }

    #[test]
    fn create_fails_on_duplicate_keys() {
        let dir = tempdir().unwrap();
        let storage = MetadataStorage::open(dir.path()).unwrap();

        let stream = sample_stream();
        storage.create_stream(stream.clone()).unwrap();
        match storage.create_stream(stream) {
            Err(StorageError::AlreadyExists(id)) => assert_eq!(id, "stream_1"),
            other => panic!("expected AlreadyExists, got {other:?}"),
        }
    }

    #[test]
    fn storage_manager_forwards_to_metadata() {
        let dir = tempdir().unwrap();
        let manager = StorageManager::new(dir.path()).unwrap();

        let stream = sample_stream();
        manager.create_stream(stream.clone()).unwrap();
        assert!(manager.get_stream(&stream.id).unwrap().is_some());

        let pipeline = sample_pipeline();
        manager.create_pipeline(pipeline.clone()).unwrap();
        assert!(manager.get_pipeline(&pipeline.id).unwrap().is_some());

        let mqtt = sample_mqtt_config();
        manager.create_mqtt_config(mqtt.clone()).unwrap();
        assert!(manager.get_mqtt_config(&mqtt.key).unwrap().is_some());

        manager.delete_stream(&stream.id).unwrap();
        manager.delete_pipeline(&pipeline.id).unwrap();
        manager.delete_mqtt_config(&mqtt.key).unwrap();
    }

    #[test]
    fn export_snapshot_captures_all_metadata_tables() {
        let dir = tempdir().unwrap();
        let storage = StorageManager::new(dir.path()).unwrap();

        let stream = sample_stream();
        let pipeline = sample_pipeline();
        let mqtt = sample_mqtt_config();
        let run_state = sample_pipeline_run_state();
        let memory_topic = sample_memory_topic();

        storage.create_stream(stream.clone()).unwrap();
        storage.create_pipeline(pipeline.clone()).unwrap();
        storage.create_mqtt_config(mqtt.clone()).unwrap();
        storage.put_pipeline_run_state(run_state.clone()).unwrap();
        storage.create_memory_topic(memory_topic.clone()).unwrap();
        let udf = sample_udf("udf_1");
        storage.create_udf(udf.clone()).unwrap();

        let snapshot = storage.export_metadata_snapshot().unwrap();

        assert_eq!(snapshot.streams, vec![stream]);
        assert_eq!(snapshot.pipelines, vec![pipeline]);
        assert_eq!(snapshot.mqtt_configs, vec![mqtt]);
        assert_eq!(snapshot.pipeline_run_states, vec![run_state]);
        assert_eq!(snapshot.memory_topics, vec![memory_topic]);
        assert_eq!(snapshot.udfs, vec![udf]);
    }

    #[test]
    fn init_apply_snapshot_is_atomic_on_duplicate_conflict() {
        let dir = tempdir().unwrap();
        let storage = StorageManager::new(dir.path()).unwrap();
        let existing_stream = sample_stream();
        storage.create_stream(existing_stream).unwrap();

        let snapshot = MetadataExportSnapshot {
            streams: vec![sample_stream()],
            schemas: vec![],
            pipelines: vec![sample_pipeline()],
            pipeline_run_states: vec![sample_pipeline_run_state()],
            mqtt_configs: vec![sample_mqtt_config()],
            memory_topics: vec![sample_memory_topic()],
            udfs: vec![],
        };

        let result = storage.apply_init_snapshot(snapshot, sample_init_apply_meta());
        match result {
            Err(StorageError::AlreadyExists(id)) => assert_eq!(id, "stream_1"),
            other => panic!("expected AlreadyExists, got {other:?}"),
        }

        assert_eq!(
            storage.list_pipelines().unwrap(),
            Vec::<StoredPipeline>::new()
        );
        assert_eq!(
            storage.list_mqtt_configs().unwrap(),
            Vec::<StoredMqttClientConfig>::new()
        );
        assert_eq!(
            storage.list_memory_topics().unwrap(),
            Vec::<StoredMemoryTopic>::new()
        );
        assert_eq!(storage.get_init_apply_meta().unwrap(), None);
    }

    #[test]
    fn init_apply_snapshot_persists_resources_and_meta() {
        let dir = tempdir().unwrap();
        let storage = StorageManager::new(dir.path()).unwrap();
        let stream = sample_stream();
        let pipeline = sample_pipeline();
        let mqtt = sample_mqtt_config();
        let run_state = sample_pipeline_run_state();
        let memory_topic = sample_memory_topic();
        let meta = sample_init_apply_meta();

        let snapshot = MetadataExportSnapshot {
            streams: vec![stream.clone()],
            schemas: vec![],
            pipelines: vec![pipeline.clone()],
            pipeline_run_states: vec![run_state.clone()],
            mqtt_configs: vec![mqtt.clone()],
            memory_topics: vec![memory_topic.clone()],
            udfs: vec![],
        };

        storage.apply_init_snapshot(snapshot, meta.clone()).unwrap();

        assert_eq!(storage.list_streams().unwrap(), vec![stream]);
        assert_eq!(storage.list_pipelines().unwrap(), vec![pipeline]);
        assert_eq!(storage.list_mqtt_configs().unwrap(), vec![mqtt]);
        assert_eq!(storage.list_memory_topics().unwrap(), vec![memory_topic]);
        assert_eq!(
            storage.get_pipeline_run_state("pipe_1").unwrap(),
            Some(run_state)
        );
        assert_eq!(storage.get_init_apply_meta().unwrap(), Some(meta));
    }

    #[test]
    fn upload_save_read_roundtrip() {
        let dir = tempdir().unwrap();
        let storage = StorageManager::new(dir.path()).unwrap();

        storage.save_upload("test.txt", b"hello world").unwrap();
        let data = storage.read_upload("test.txt").unwrap();
        assert_eq!(data, b"hello world");
    }

    #[test]
    fn upload_overwrite() {
        let dir = tempdir().unwrap();
        let storage = StorageManager::new(dir.path()).unwrap();

        storage.save_upload("conf.txt", b"old").unwrap();
        storage.save_upload("conf.txt", b"new").unwrap();
        let data = storage.read_upload("conf.txt").unwrap();
        assert_eq!(data, b"new");
    }

    #[test]
    fn upload_delete_nonexistent_returns_false() {
        let dir = tempdir().unwrap();
        let storage = StorageManager::new(dir.path()).unwrap();

        assert!(!storage.delete_upload("missing.txt").unwrap());
    }

    #[test]
    fn upload_list_empty_and_nonempty() {
        let dir = tempdir().unwrap();
        let storage = StorageManager::new(dir.path()).unwrap();

        let list = storage.list_uploads().unwrap();
        assert!(list.is_empty());

        storage.save_upload("a.txt", b"a").unwrap();
        storage.save_upload("b.txt", b"bb").unwrap();

        let list = storage.list_uploads().unwrap();
        assert_eq!(list.len(), 2);
        assert_eq!(list[0].name, "a.txt");
        assert_eq!(list[0].size_bytes, 1);
        assert_eq!(list[1].name, "b.txt");
        assert_eq!(list[1].size_bytes, 2);
    }

    #[test]
    fn upload_list_includes_nested_files() {
        let dir = tempdir().unwrap();
        let storage = StorageManager::new(dir.path()).unwrap();

        storage
            .save_upload("proto/sensor.proto", b"syntax proto3")
            .unwrap();
        storage
            .save_upload("proto/common/types.proto", b"message T {}")
            .unwrap();
        storage.save_upload("dbc/can.dbc", b"VERSION").unwrap();

        let list = storage.list_uploads().unwrap();
        assert_eq!(list.len(), 3);
        assert_eq!(list[0].name, "dbc/can.dbc");
        assert_eq!(list[1].name, "proto/common/types.proto");
        assert_eq!(list[2].name, "proto/sensor.proto");
    }

    #[test]
    fn upload_copy_uploads_from_dir_copies_files() {
        let dir = tempdir().unwrap();
        let storage = StorageManager::new(dir.path()).unwrap();

        let src = dir.path().join("src_uploads");
        std::fs::create_dir_all(src.join("sub")).unwrap();
        std::fs::write(src.join("cert.pem"), b"cert-data").unwrap();
        std::fs::write(src.join("sub/key.pem"), b"key-data").unwrap();

        let copied = storage.copy_uploads_from_dir(&src).unwrap();
        assert_eq!(copied, 2);

        assert_eq!(
            storage.read_upload("cert.pem").unwrap(),
            b"cert-data".to_vec()
        );
        assert_eq!(
            storage.read_upload("sub/key.pem").unwrap(),
            b"key-data".to_vec()
        );
    }

    #[test]
    fn upload_save_read_nested_path() {
        let dir = tempdir().unwrap();
        let storage = StorageManager::new(dir.path()).unwrap();

        storage
            .save_upload("proto/sub/sensor.proto", b"message Sensor {}")
            .unwrap();
        let data = storage.read_upload("proto/sub/sensor.proto").unwrap();
        assert_eq!(data, b"message Sensor {}");

        let list = storage.list_uploads().unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].name, "proto/sub/sensor.proto");
    }
}
