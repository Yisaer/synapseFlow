use crate::codec::RecordDecoder;
use crate::connector::SourceConnector;
use crate::processor::base::DEFAULT_CHANNEL_CAPACITY;
use crate::processor::{
    ControlSignal, DataSourceProcessor, DecoderProcessor, Processor, ProcessorError, StreamData,
};
use datatypes::Schema;
use once_cell::sync::Lazy;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::SystemTime;
use thiserror::Error;
use tokio::runtime::Handle;
use tokio::sync::{broadcast, Mutex, RwLock};
use tokio::task::JoinHandle;

/// Accessor for the global shared stream registry.
pub fn registry() -> &'static SharedStreamRegistry {
    static REGISTRY: Lazy<SharedStreamRegistry> = Lazy::new(SharedStreamRegistry::new);
    &REGISTRY
}

/// Central registry that tracks shared source streams that are owned by the process.
pub struct SharedStreamRegistry {
    streams: RwLock<HashMap<String, Arc<SharedStreamInner>>>,
}

impl SharedStreamRegistry {
    fn new() -> Self {
        Self {
            streams: RwLock::new(HashMap::new()),
        }
    }

    /// Create and start a new shared stream.
    pub async fn create_stream(
        &self,
        config: SharedStreamConfig,
    ) -> Result<SharedStreamInfo, SharedStreamError> {
        if config.connector.is_none() {
            return Err(SharedStreamError::Internal(
                "shared stream requires exactly one connector".into(),
            ));
        }
        let mut streams = self.streams.write().await;
        if streams.contains_key(&config.stream_name) {
            return Err(SharedStreamError::AlreadyExists(config.stream_name));
        }

        let inner = SharedStreamInner::start(config)?;
        let info = inner.snapshot().await;
        streams.insert(info.name.clone(), Arc::new(inner));
        Ok(info)
    }

    /// List all registered shared streams.
    pub async fn list_streams(&self) -> Vec<SharedStreamInfo> {
        let entries: Vec<Arc<SharedStreamInner>> = {
            let guard = self.streams.read().await;
            guard.values().cloned().collect()
        };

        let mut infos = Vec::with_capacity(entries.len());
        for entry in entries {
            infos.push(entry.snapshot().await);
        }
        infos
    }

    /// Fetch information about a single shared stream.
    pub async fn get_stream(&self, name: &str) -> Result<SharedStreamInfo, SharedStreamError> {
        let entry = {
            let guard = self.streams.read().await;
            guard
                .get(name)
                .cloned()
                .ok_or_else(|| SharedStreamError::NotFound(name.to_string()))?
        };
        Ok(entry.snapshot().await)
    }

    /// Drop a shared stream if it is not referenced by any pipelines.
    pub async fn drop_stream(&self, name: &str) -> Result<(), SharedStreamError> {
        let entry = {
            let mut guard = self.streams.write().await;
            guard
                .remove(name)
                .ok_or_else(|| SharedStreamError::NotFound(name.to_string()))?
        };

        let consumers = entry.consumer_ids().await;
        if !consumers.is_empty() {
            let mut guard = self.streams.write().await;
            guard.insert(name.to_string(), entry);
            return Err(SharedStreamError::InUse(consumers));
        }

        let stream_name = entry.name().to_string();
        tokio::spawn(async move {
            if let Err(err) = entry.shutdown().await {
                println!("[SharedStream:{stream_name}] shutdown error: {err}");
            }
        });

        Ok(())
    }

    /// Register a consumer for a shared stream and obtain receivers.
    pub async fn subscribe(
        &self,
        name: &str,
        consumer_id: impl Into<String>,
    ) -> Result<SharedStreamSubscription, SharedStreamError> {
        let entry = {
            let guard = self.streams.read().await;
            guard
                .get(name)
                .cloned()
                .ok_or_else(|| SharedStreamError::NotFound(name.to_string()))?
        };
        entry.register_consumer(consumer_id.into()).await
    }

    /// Update the required columns for a registered consumer.
    ///
    /// This does not change what is currently decoded yet; it only updates registry state and
    /// recomputes the union required columns. The decoder will apply it in a later step.
    pub async fn set_consumer_required_columns(
        &self,
        name: &str,
        consumer_id: &str,
        required_columns: Vec<String>,
    ) -> Result<Vec<String>, SharedStreamError> {
        let entry = {
            let guard = self.streams.read().await;
            guard
                .get(name)
                .cloned()
                .ok_or_else(|| SharedStreamError::NotFound(name.to_string()))?
        };
        entry
            .set_consumer_required_columns(consumer_id, required_columns)
            .await
    }

    /// Return the current union required columns across all registered consumers.
    ///
    /// This is an internal planning/debugging aid; pipelines should normally rely on the
    /// applied `SharedStreamInfo.decoding_columns` when waiting for readiness.
    pub async fn union_required_columns(
        &self,
        name: &str,
    ) -> Result<Vec<String>, SharedStreamError> {
        let entry = {
            let guard = self.streams.read().await;
            guard
                .get(name)
                .cloned()
                .ok_or_else(|| SharedStreamError::NotFound(name.to_string()))?
        };
        Ok(entry.union_required_columns().await)
    }

    /// Whether the given stream name corresponds to a registered shared stream.
    pub async fn is_registered(&self, name: &str) -> bool {
        let guard = self.streams.read().await;
        guard.contains_key(name)
    }
}

/// Errors that can occur while operating a shared stream.
#[derive(Debug, Error)]
pub enum SharedStreamError {
    #[error("shared stream already exists: {0}")]
    AlreadyExists(String),
    #[error("shared stream not found: {0}")]
    NotFound(String),
    #[error("shared stream still referenced by pipelines: {0:?}")]
    InUse(Vec<String>),
    #[error("shared stream encountered internal error: {0}")]
    Internal(String),
}

/// Connector binding used while constructing a shared stream.
pub struct SharedSourceConnectorConfig {
    pub connector: Box<dyn SourceConnector>,
    pub decoder: Arc<dyn RecordDecoder>,
}

/// Configuration used to create a shared stream instance.
pub struct SharedStreamConfig {
    pub stream_name: String,
    pub schema: Arc<Schema>,
    pub connector: Option<SharedSourceConnectorConfig>,
    pub channel_capacity: usize,
}

impl SharedStreamConfig {
    pub fn new(stream_name: impl Into<String>, schema: Arc<Schema>) -> Self {
        Self {
            stream_name: stream_name.into(),
            schema,
            connector: None,
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
        }
    }

    pub fn with_channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = capacity.max(1);
        self
    }

    pub fn with_connector(
        mut self,
        connector: Box<dyn SourceConnector>,
        decoder: Arc<dyn RecordDecoder>,
    ) -> Self {
        self.connector = Some(SharedSourceConnectorConfig { connector, decoder });
        self
    }

    pub fn set_connector(
        &mut self,
        connector: Box<dyn SourceConnector>,
        decoder: Arc<dyn RecordDecoder>,
    ) {
        self.connector = Some(SharedSourceConnectorConfig { connector, decoder });
    }
}

/// Runtime status for a shared stream.
#[derive(Clone, Debug)]
pub enum SharedStreamStatus {
    Starting,
    Running,
    Stopped,
    Failed(String),
}

/// Public information returned to management APIs.
#[derive(Clone, Debug)]
pub struct SharedStreamInfo {
    pub name: String,
    pub schema: Arc<Schema>,
    pub created_at: SystemTime,
    pub status: SharedStreamStatus,
    pub connector_id: String,
    pub subscriber_count: usize,
    /// Columns that the shared stream decoder is currently decoding (applied state).
    ///
    /// This is used by pipelines to wait until the shared decoder has applied a required
    /// projection before the pipeline starts consuming data.
    pub decoding_columns: Vec<String>,
}

/// Handle returned to pipeline consumers.
pub struct SharedStreamSubscription {
    stream: Arc<SharedStreamInner>,
    consumer_id: String,
    data_receiver: Option<broadcast::Receiver<StreamData>>,
    control_receiver: Option<broadcast::Receiver<ControlSignal>>,
}

impl SharedStreamSubscription {
    pub fn stream_name(&self) -> &str {
        &self.stream.name
    }

    pub fn take_receivers(
        &mut self,
    ) -> (
        broadcast::Receiver<StreamData>,
        broadcast::Receiver<ControlSignal>,
    ) {
        let data = self
            .data_receiver
            .take()
            .expect("shared stream data receiver already taken");
        let control = self
            .control_receiver
            .take()
            .expect("shared stream control receiver already taken");
        (data, control)
    }

    pub async fn release(mut self) {
        let _ = self.data_receiver.take();
        let _ = self.control_receiver.take();
        let consumer_id = self.consumer_id.clone();
        self.stream.unregister_consumer(&consumer_id).await;
    }
}

impl Drop for SharedStreamSubscription {
    fn drop(&mut self) {
        if Handle::try_current().is_err() {
            return;
        }
        let stream = Arc::clone(&self.stream);
        let consumer_id = self.consumer_id.clone();
        tokio::spawn(async move {
            stream.unregister_consumer(&consumer_id).await;
        });
    }
}

/// Internal structure that holds state for a single shared stream.
struct SharedStreamInner {
    name: String,
    schema: Arc<Schema>,
    created_at: SystemTime,
    connector_id: String,
    data_sender: broadcast::Sender<StreamData>,
    control_sender: broadcast::Sender<ControlSignal>,
    control_input: broadcast::Sender<ControlSignal>,
    handles: Mutex<SharedStreamHandles>,
    state: Mutex<SharedStreamState>,
}

/// Join handles that should be awaited when shutting down the stream.
struct SharedStreamHandles {
    datasource: Option<JoinHandle<Result<(), ProcessorError>>>,
    decoder: Option<JoinHandle<Result<(), ProcessorError>>>,
    forward: Option<JoinHandle<()>>,
    control_forward: Option<JoinHandle<()>>,
    data_anchor: Option<broadcast::Receiver<StreamData>>,
    control_anchor: Option<broadcast::Receiver<ControlSignal>>,
}

/// Mutable status tracked for each stream.
struct SharedStreamState {
    status: SharedStreamStatus,
    subscribers: HashSet<String>,
    decoding_columns: Vec<String>,
    consumer_required_columns: HashMap<String, Vec<String>>,
    union_required_columns: Vec<String>,
}

impl SharedStreamInner {
    fn start(config: SharedStreamConfig) -> Result<Self, SharedStreamError> {
        let SharedStreamConfig {
            stream_name,
            schema,
            connector,
            channel_capacity,
        } = config;

        let SharedSourceConnectorConfig { connector, decoder } = connector.ok_or_else(|| {
            SharedStreamError::Internal(
                "shared stream requires exactly one source connector".into(),
            )
        })?;
        let datasource_id = format!("shared:{}/PhysicalDataSource_0", stream_name);
        let decoder_id = format!("shared:{}/PhysicalDecoder_1", stream_name);
        let mut datasource = DataSourceProcessor::with_custom_id(
            None,
            datasource_id,
            stream_name.clone(),
            Arc::clone(&schema),
        );
        let connector_id = connector.id().to_string();
        datasource.add_connector(connector);

        let datasource_data_rx = datasource
            .subscribe_output()
            .ok_or_else(|| SharedStreamError::Internal("datasource output unavailable".into()))?;
        let mut control_rx = datasource.subscribe_control_output().ok_or_else(|| {
            SharedStreamError::Internal("datasource control output unavailable".into())
        })?;

        let (control_input_tx, control_input_rx) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        datasource.add_control_input(control_input_rx);

        let mut decoder_processor = DecoderProcessor::with_custom_id(decoder_id, decoder);
        decoder_processor.add_input(datasource_data_rx);
        decoder_processor.add_control_input(control_input_tx.subscribe());
        let mut data_rx = decoder_processor
            .subscribe_output()
            .ok_or_else(|| SharedStreamError::Internal("decoder output unavailable".into()))?;

        let datasource_handle = datasource.start();
        let decoder_handle = decoder_processor.start();

        let (data_sender, _) = broadcast::channel(channel_capacity);
        let (control_sender, _) = broadcast::channel(channel_capacity);

        let name_for_data = stream_name.clone();
        let data_tx = data_sender.clone();
        let forward = tokio::spawn(async move {
            use tokio::sync::broadcast::error::RecvError;
            loop {
                match data_rx.recv().await {
                    Ok(data) => {
                        if data_tx.send(data).is_err() {
                            break;
                        }
                    }
                    Err(RecvError::Lagged(skipped)) => {
                        println!(
                            "[SharedStream:{name_for_data}] datasource output lagged by {skipped} messages"
                        );
                    }
                    Err(RecvError::Closed) => break,
                }
            }
        });

        let name_for_control = stream_name.clone();
        let control_tx = control_sender.clone();
        let control_forward = tokio::spawn(async move {
            use tokio::sync::broadcast::error::RecvError;
            loop {
                match control_rx.recv().await {
                    Ok(signal) => {
                        if control_tx.send(signal).is_err() {
                            break;
                        }
                    }
                    Err(RecvError::Lagged(skipped)) => {
                        println!(
                            "[SharedStream:{name_for_control}] control channel lagged by {skipped} messages"
                        );
                    }
                    Err(RecvError::Closed) => break,
                }
            }
        });

        let data_anchor = data_sender.subscribe();
        let control_anchor = control_sender.subscribe();

        let initial_decoding_columns: Vec<String> = schema
            .column_schemas()
            .iter()
            .map(|col| col.name.clone())
            .collect();

        Ok(Self {
            name: stream_name,
            schema,
            created_at: SystemTime::now(),
            connector_id,
            data_sender,
            control_sender,
            control_input: control_input_tx,
            handles: Mutex::new(SharedStreamHandles {
                datasource: Some(datasource_handle),
                decoder: Some(decoder_handle),
                forward: Some(forward),
                control_forward: Some(control_forward),
                data_anchor: Some(data_anchor),
                control_anchor: Some(control_anchor),
            }),
            state: Mutex::new(SharedStreamState {
                status: SharedStreamStatus::Running,
                subscribers: HashSet::new(),
                decoding_columns: initial_decoding_columns,
                consumer_required_columns: HashMap::new(),
                union_required_columns: Vec::new(),
            }),
        })
    }

    fn name(&self) -> &str {
        &self.name
    }

    async fn snapshot(&self) -> SharedStreamInfo {
        let state = self.state.lock().await;
        SharedStreamInfo {
            name: self.name.clone(),
            schema: Arc::clone(&self.schema),
            created_at: self.created_at,
            status: state.status.clone(),
            connector_id: self.connector_id.clone(),
            subscriber_count: state.subscribers.len(),
            decoding_columns: state.decoding_columns.clone(),
        }
    }

    async fn consumer_ids(&self) -> Vec<String> {
        let state = self.state.lock().await;
        state.subscribers.iter().cloned().collect()
    }

    async fn register_consumer(
        self: &Arc<Self>,
        consumer_id: String,
    ) -> Result<SharedStreamSubscription, SharedStreamError> {
        {
            let mut state = self.state.lock().await;
            if matches!(state.status, SharedStreamStatus::Stopped) {
                return Err(SharedStreamError::Internal(format!(
                    "shared stream {} is stopped",
                    self.name()
                )));
            }
            if !state.subscribers.insert(consumer_id.clone()) {
                return Err(SharedStreamError::Internal(format!(
                    "consumer {consumer_id} already registered for {}",
                    self.name()
                )));
            }
        }

        let data_receiver = self.data_sender.subscribe();
        let control_receiver = self.control_sender.subscribe();

        Ok(SharedStreamSubscription {
            stream: Arc::clone(self),
            consumer_id,
            data_receiver: Some(data_receiver),
            control_receiver: Some(control_receiver),
        })
    }

    async fn set_consumer_required_columns(
        &self,
        consumer_id: &str,
        required_columns: Vec<String>,
    ) -> Result<Vec<String>, SharedStreamError> {
        let mut state = self.state.lock().await;
        if !state.subscribers.contains(consumer_id) {
            return Err(SharedStreamError::Internal(format!(
                "consumer {consumer_id} not registered for {}",
                self.name()
            )));
        }
        state
            .consumer_required_columns
            .insert(consumer_id.to_string(), required_columns);

        let union_set: HashSet<String> = state
            .consumer_required_columns
            .values()
            .flat_map(|cols| cols.iter().cloned())
            .collect();

        let union: Vec<String> = self
            .schema
            .column_schemas()
            .iter()
            .map(|col| col.name.clone())
            .filter(|name| union_set.contains(name))
            .collect();

        state.union_required_columns = union.clone();
        if union.is_empty() {
            state.decoding_columns = self
                .schema
                .column_schemas()
                .iter()
                .map(|col| col.name.clone())
                .collect();
        } else {
            state.decoding_columns = union.clone();
        }
        Ok(union)
    }

    async fn union_required_columns(&self) -> Vec<String> {
        let state = self.state.lock().await;
        state.union_required_columns.clone()
    }

    async fn unregister_consumer(&self, consumer_id: &str) {
        let mut state = self.state.lock().await;
        state.subscribers.remove(consumer_id);
        state.consumer_required_columns.remove(consumer_id);

        let union_set: HashSet<String> = state
            .consumer_required_columns
            .values()
            .flat_map(|cols| cols.iter().cloned())
            .collect();

        let union: Vec<String> = self
            .schema
            .column_schemas()
            .iter()
            .map(|col| col.name.clone())
            .filter(|name| union_set.contains(name))
            .collect();

        state.union_required_columns = union.clone();
        if union.is_empty() {
            state.decoding_columns = self
                .schema
                .column_schemas()
                .iter()
                .map(|col| col.name.clone())
                .collect();
        } else {
            state.decoding_columns = union;
        }
    }

    async fn shutdown(self: Arc<Self>) -> Result<(), SharedStreamError> {
        {
            let mut state = self.state.lock().await;
            state.status = SharedStreamStatus::Stopped;
            state.subscribers.clear();
            state.consumer_required_columns.clear();
            state.union_required_columns.clear();
        }

        let _ = self.control_input.send(ControlSignal::StreamQuickEnd);

        let mut handles = self.handles.lock().await;

        if let Some(handle) = handles.forward.take() {
            let _ = handle.await;
        }
        if let Some(handle) = handles.control_forward.take() {
            let _ = handle.await;
        }
        handles.data_anchor.take();
        handles.control_anchor.take();
        if let Some(handle) = handles.decoder.take() {
            match handle.await {
                Ok(result) => result.map_err(|err| SharedStreamError::Internal(err.to_string()))?,
                Err(join_err) => {
                    return Err(SharedStreamError::Internal(format!(
                        "decoder join error: {join_err}"
                    )))
                }
            }
        }
        if let Some(handle) = handles.datasource.take() {
            match handle.await {
                Ok(result) => result.map_err(|err| SharedStreamError::Internal(err.to_string()))?,
                Err(join_err) => {
                    return Err(SharedStreamError::Internal(format!(
                        "datasource join error: {join_err}"
                    )))
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::JsonDecoder;
    use crate::connector::MockSourceConnector;
    use datatypes::{ColumnSchema, ConcreteDatatype, Int64Type, Schema};
    use serde_json::Map as JsonMap;
    use uuid::Uuid;

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![ColumnSchema::new(
            "shared_stream".to_string(),
            "value".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        )]))
    }

    #[tokio::test]
    async fn create_list_drop_shared_stream() {
        let name = format!("shared_stream_test_{}", Uuid::new_v4());
        let schema = test_schema();
        let (connector, _handle) = MockSourceConnector::new(format!("{name}_connector"));
        let decoder = Arc::new(JsonDecoder::new(
            name.clone(),
            Arc::clone(&schema),
            JsonMap::new(),
        ));

        let mut config = SharedStreamConfig::new(name.clone(), Arc::clone(&schema));
        config.set_connector(Box::new(connector), decoder);

        let info = registry().create_stream(config).await.unwrap();
        assert_eq!(info.name, name);
        assert_eq!(info.connector_id, format!("{name}_connector"));

        let listed = registry().list_streams().await;
        assert!(listed.iter().any(|entry| entry.name == name));

        registry().drop_stream(&name).await.unwrap();

        let listed = registry().list_streams().await;
        assert!(
            listed.iter().all(|entry| entry.name != name),
            "shared stream should be removed after drop"
        );
    }

    #[tokio::test]
    async fn shared_stream_tracks_consumer_required_columns_union() {
        let name = format!("shared_stream_req_cols_test_{}", Uuid::new_v4().simple());
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new(
                name.clone(),
                "a".to_string(),
                ConcreteDatatype::Int64(Int64Type),
            ),
            ColumnSchema::new(
                name.clone(),
                "b".to_string(),
                ConcreteDatatype::Int64(Int64Type),
            ),
        ]));
        let (connector, _handle) = MockSourceConnector::new(format!("{name}_connector"));
        let decoder = Arc::new(JsonDecoder::new(
            name.clone(),
            Arc::clone(&schema),
            JsonMap::new(),
        ));

        let config = SharedStreamConfig::new(name.clone(), Arc::clone(&schema))
            .with_connector(Box::new(connector), decoder);
        let info = registry().create_stream(config).await.unwrap();
        assert_eq!(info.name, name);

        let sub_a = registry()
            .subscribe(&name, "consumer_a")
            .await
            .expect("subscribe consumer_a");
        let sub_b = registry()
            .subscribe(&name, "consumer_b")
            .await
            .expect("subscribe consumer_b");

        registry()
            .set_consumer_required_columns(&name, "consumer_a", vec!["a".to_string()])
            .await
            .expect("set required columns for a");
        registry()
            .set_consumer_required_columns(&name, "consumer_b", vec!["b".to_string()])
            .await
            .expect("set required columns for b");

        let union = registry()
            .union_required_columns(&name)
            .await
            .expect("union required columns");
        assert_eq!(union, vec!["a".to_string(), "b".to_string()]);

        sub_a.release().await;
        let union = registry()
            .union_required_columns(&name)
            .await
            .expect("union required columns");
        assert_eq!(union, vec!["b".to_string()]);

        sub_b.release().await;
        let union = registry()
            .union_required_columns(&name)
            .await
            .expect("union required columns");
        assert!(union.is_empty());

        registry().drop_stream(&name).await.unwrap();
    }
}
