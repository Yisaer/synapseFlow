use std::fs;
use std::path::{Path, PathBuf};

use redb::{Database, ReadableTable, TableDefinition};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use thiserror::Error;

const STREAMS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("streams");
const PIPELINES_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("pipelines");
const PLAN_SNAPSHOTS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("plan_snapshots");
const SHARED_MQTT_CONFIGS_TABLE: TableDefinition<&str, &[u8]> =
    TableDefinition::new("shared_mqtt_client_configs");

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
pub struct StoredPipeline {
    pub id: String,
    /// Original create-pipeline request serialized as JSON.
    pub raw_json: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredPlanSnapshot {
    /// Owning pipeline identifier (also used as the storage key).
    pub pipeline_id: String,
    /// Fingerprint over pipeline/streams/build-id used for cache validation.
    pub fingerprint: String,
    /// Hash of the pipeline creation JSON (`StoredPipeline.raw_json`).
    pub pipeline_json_hash: String,
    /// Hash of each referenced stream creation JSON (`StoredStream.raw_json`).
    ///
    /// Stored as `(stream_id, stream_json_hash)` pairs.
    pub stream_json_hashes: Vec<(String, String)>,
    /// Flow build identifier (e.g. commit id + git tag).
    pub flow_build_id: String,
    /// Serialized optimized logical plan IR.
    pub logical_plan_ir: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredMqttClientConfig {
    pub key: String,
    /// Original request serialized as JSON.
    pub raw_json: String,
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
            let mut pipelines = txn.open_table(PIPELINES_TABLE).map_err(StorageError::backend)?;
            let removed = pipelines.remove(id).map_err(StorageError::backend)?;
            if removed.is_none() {
                return Err(StorageError::NotFound(id.to_string()));
            }

            let mut snapshots = txn
                .open_table(PLAN_SNAPSHOTS_TABLE)
                .map_err(StorageError::backend)?;
            let _ = snapshots.remove(id).map_err(StorageError::backend)?;
        }
        txn.commit().map_err(StorageError::backend)?;
        Ok(())
    }

    pub fn put_plan_snapshot(&self, snapshot: StoredPlanSnapshot) -> Result<(), StorageError> {
        let txn = self.db.begin_write().map_err(StorageError::backend)?;
        {
            let mut table = txn
                .open_table(PLAN_SNAPSHOTS_TABLE)
                .map_err(StorageError::backend)?;
            let encoded = encode_record(&snapshot)?;
            table
                .insert(snapshot.pipeline_id.as_str(), encoded.as_slice())
                .map_err(StorageError::backend)?;
        }
        txn.commit().map_err(StorageError::backend)?;
        Ok(())
    }

    pub fn get_plan_snapshot(
        &self,
        pipeline_id: &str,
    ) -> Result<Option<StoredPlanSnapshot>, StorageError> {
        self.get_entry(PLAN_SNAPSHOTS_TABLE, pipeline_id)
    }

    pub fn delete_plan_snapshot(&self, pipeline_id: &str) -> Result<(), StorageError> {
        self.delete_entry(PLAN_SNAPSHOTS_TABLE, pipeline_id)
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

    fn db_path(base_dir: &Path) -> PathBuf {
        base_dir.join("metadata.redb")
    }

    fn ensure_tables(&self) -> Result<(), StorageError> {
        let txn = self.db.begin_write().map_err(StorageError::backend)?;
        txn.open_table(STREAMS_TABLE)
            .map_err(StorageError::backend)?;
        txn.open_table(PIPELINES_TABLE)
            .map_err(StorageError::backend)?;
        txn.open_table(PLAN_SNAPSHOTS_TABLE)
            .map_err(StorageError::backend)?;
        txn.open_table(SHARED_MQTT_CONFIGS_TABLE)
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
        let table = txn.open_table(table).map_err(StorageError::backend)?;
        let mut items = Vec::new();
        for entry in table.range::<&str>(..).map_err(StorageError::backend)? {
            let (_, value) = entry.map_err(StorageError::backend)?;
            items.push(decode_record(value.value())?);
        }
        Ok(items)
    }
}

/// Facade that exposes storage capabilities; currently only metadata, future namespaces can be added.
pub struct StorageManager {
    metadata: MetadataStorage,
    base_dir: PathBuf,
}

impl StorageManager {
    /// Create a storage manager; initializes the metadata namespace under base_dir/metadata.
    pub fn new(base_dir: impl AsRef<Path>) -> Result<Self, StorageError> {
        let base_dir = base_dir.as_ref().to_path_buf();
        let metadata = MetadataStorage::open(base_dir.join("metadata"))?;
        Ok(Self { metadata, base_dir })
    }

    pub fn metadata(&self) -> &MetadataStorage {
        &self.metadata
    }

    pub fn base_dir(&self) -> &Path {
        &self.base_dir
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

    pub fn put_plan_snapshot(&self, snapshot: StoredPlanSnapshot) -> Result<(), StorageError> {
        self.metadata.put_plan_snapshot(snapshot)
    }

    pub fn get_plan_snapshot(
        &self,
        pipeline_id: &str,
    ) -> Result<Option<StoredPlanSnapshot>, StorageError> {
        self.metadata.get_plan_snapshot(pipeline_id)
    }

    pub fn delete_plan_snapshot(&self, pipeline_id: &str) -> Result<(), StorageError> {
        self.metadata.delete_plan_snapshot(pipeline_id)
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

    fn sample_plan_snapshot() -> StoredPlanSnapshot {
        StoredPlanSnapshot {
            pipeline_id: "pipe_1".to_string(),
            fingerprint: "fp_a".to_string(),
            pipeline_json_hash: "pipe_json_a".to_string(),
            stream_json_hashes: vec![("stream_1".to_string(), "stream_json_a".to_string())],
            flow_build_id: "sha:deadbeef tag:v0.0.0".to_string(),
            logical_plan_ir: vec![1, 2, 3],
        }
    }

    fn sample_mqtt_config() -> StoredMqttClientConfig {
        StoredMqttClientConfig {
        key: "shared_a".to_string(),
        raw_json: r#"{"key":"shared_a","broker_url":"tcp://localhost:1883","topic":"foo/bar","client_id":"client_a","qos":1}"#.to_string(),
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

        let snapshot = sample_plan_snapshot();
        storage.put_plan_snapshot(snapshot.clone()).unwrap();
        assert_eq!(
            storage.get_plan_snapshot(&snapshot.pipeline_id).unwrap(),
            Some(snapshot.clone())
        );

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
        assert!(storage.get_plan_snapshot(&snapshot.pipeline_id).unwrap().is_none());
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

        let snapshot = sample_plan_snapshot();
        manager.put_plan_snapshot(snapshot.clone()).unwrap();
        assert!(manager.get_plan_snapshot(&snapshot.pipeline_id).unwrap().is_some());

        let mqtt = sample_mqtt_config();
        manager.create_mqtt_config(mqtt.clone()).unwrap();
        assert!(manager.get_mqtt_config(&mqtt.key).unwrap().is_some());

        manager.delete_stream(&stream.id).unwrap();
        manager.delete_pipeline(&pipeline.id).unwrap();
        manager.delete_mqtt_config(&mqtt.key).unwrap();

        assert!(manager.get_plan_snapshot(&snapshot.pipeline_id).unwrap().is_none());
    }

    #[test]
    fn delete_pipeline_cascades_plan_snapshot() {
        let dir = tempdir().unwrap();
        let storage = StorageManager::new(dir.path()).unwrap();

        let pipeline = sample_pipeline();
        storage.create_pipeline(pipeline.clone()).unwrap();

        let snapshot = sample_plan_snapshot();
        storage.put_plan_snapshot(snapshot.clone()).unwrap();
        assert!(storage.get_plan_snapshot(&snapshot.pipeline_id).unwrap().is_some());

        storage.delete_pipeline(&pipeline.id).unwrap();
        assert!(storage.get_pipeline(&pipeline.id).unwrap().is_none());
        assert!(storage.get_plan_snapshot(&snapshot.pipeline_id).unwrap().is_none());
    }
}
