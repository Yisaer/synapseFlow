use crate::codec::RecordDecoder;
use crate::connector::SourceConnector;
use crate::processor::base::DEFAULT_CHANNEL_CAPACITY;
use crate::processor::{
    ControlSignal, DataSourceProcessor, DecoderProcessor, InstantControlSignal, Processor,
    ProcessorError, StreamData,
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

type ConnectorDecoderPair = (Box<dyn SourceConnector>, Arc<dyn RecordDecoder>);

/// Factory that can (re)build a connector + decoder pair for a shared stream runtime.
pub trait SharedStreamConnectorFactory: Send + Sync + 'static {
    /// Return a stable identifier for management/logging even when the stream runtime is stopped.
    fn connector_id(&self) -> String;

    /// Build a fresh connector + decoder for starting (or restarting) the stream runtime.
    fn build(&self) -> Result<ConnectorDecoderPair, SharedStreamError>;
}

struct OneShotConnectorFactory {
    connector_id: String,
    inner: std::sync::Mutex<Option<ConnectorDecoderPair>>,
}

impl OneShotConnectorFactory {
    fn new(connector: Box<dyn SourceConnector>, decoder: Arc<dyn RecordDecoder>) -> Self {
        let connector_id = connector.id().to_string();
        Self {
            connector_id,
            inner: std::sync::Mutex::new(Some((connector, decoder))),
        }
    }
}

impl SharedStreamConnectorFactory for OneShotConnectorFactory {
    fn connector_id(&self) -> String {
        self.connector_id.clone()
    }

    fn build(&self) -> Result<ConnectorDecoderPair, SharedStreamError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| SharedStreamError::Internal("connector factory lock poisoned".into()))?;
        guard.take().ok_or_else(|| {
            SharedStreamError::Internal(
                "shared stream connector factory cannot rebuild connector/decoder".into(),
            )
        })
    }
}

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
                "shared stream requires a connector factory or instance".into(),
            ));
        }
        let mut streams = self.streams.write().await;
        if streams.contains_key(&config.stream_name) {
            return Err(SharedStreamError::AlreadyExists(config.stream_name));
        }

        let inner = SharedStreamInner::new(config)?;
        let info = inner.snapshot().await;
        streams.insert(info.name.clone(), inner);
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
            if let Err(err) = entry.stop_runtime().await {
                tracing::error!(stream_name = %stream_name, error = %err, "shared stream shutdown error");
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
        entry.ensure_started().await?;
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

/// Connector specification for creating a shared stream.
pub enum SharedStreamConnectorSpec {
    /// Use the provided connector/decoder instance (cannot be restarted once stopped).
    Instance(SharedSourceConnectorConfig),
    /// Use a restartable factory that can build fresh connector/decoder instances on demand.
    Factory(Arc<dyn SharedStreamConnectorFactory>),
}

/// Configuration used to create a shared stream instance.
pub struct SharedStreamConfig {
    pub stream_name: String,
    pub schema: Arc<Schema>,
    pub connector: Option<SharedStreamConnectorSpec>,
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
        self.connector = Some(SharedStreamConnectorSpec::Instance(
            SharedSourceConnectorConfig { connector, decoder },
        ));
        self
    }

    pub fn set_connector(
        &mut self,
        connector: Box<dyn SourceConnector>,
        decoder: Arc<dyn RecordDecoder>,
    ) {
        self.connector = Some(SharedStreamConnectorSpec::Instance(
            SharedSourceConnectorConfig { connector, decoder },
        ));
    }

    pub fn with_connector_factory(
        mut self,
        factory: Arc<dyn SharedStreamConnectorFactory>,
    ) -> Self {
        self.connector = Some(SharedStreamConnectorSpec::Factory(factory));
        self
    }

    pub fn set_connector_factory(&mut self, factory: Arc<dyn SharedStreamConnectorFactory>) {
        self.connector = Some(SharedStreamConnectorSpec::Factory(factory));
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
    receivers_taken: bool,
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
        if self.receivers_taken {
            panic!("shared stream receivers already taken");
        }
        self.receivers_taken = true;
        (
            self.stream.data_sender.subscribe(),
            self.stream.control_sender.subscribe(),
        )
    }

    pub async fn release(self) {
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
    connector_factory: Arc<dyn SharedStreamConnectorFactory>,
    runtime_lock: Mutex<()>,
    decoding_columns: Arc<std::sync::RwLock<Vec<String>>>,
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
    consumer_required_columns: HashMap<String, Vec<String>>,
    union_required_columns: Vec<String>,
}

impl SharedStreamInner {
    fn new(config: SharedStreamConfig) -> Result<Arc<Self>, SharedStreamError> {
        let SharedStreamConfig {
            stream_name,
            schema,
            connector,
            channel_capacity,
        } = config;

        let connector_factory: Arc<dyn SharedStreamConnectorFactory> = match connector {
            Some(SharedStreamConnectorSpec::Factory(factory)) => factory,
            Some(SharedStreamConnectorSpec::Instance(SharedSourceConnectorConfig {
                connector,
                decoder,
            })) => Arc::new(OneShotConnectorFactory::new(connector, decoder)),
            None => {
                return Err(SharedStreamError::Internal(
                    "shared stream requires a connector factory or instance".into(),
                ))
            }
        };
        let connector_id = connector_factory.connector_id();

        let (control_input_tx, _control_input_rx) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        let initial_decoding_columns: Vec<String> = schema
            .column_schemas()
            .iter()
            .map(|col| col.name.clone())
            .collect();
        let decoding_columns = Arc::new(std::sync::RwLock::new(initial_decoding_columns));

        let (data_sender, _) = broadcast::channel(channel_capacity);
        let (control_sender, _) = broadcast::channel(channel_capacity);

        let data_anchor = data_sender.subscribe();
        let control_anchor = control_sender.subscribe();

        Ok(Arc::new(Self {
            name: stream_name,
            schema,
            created_at: SystemTime::now(),
            connector_id,
            connector_factory,
            runtime_lock: Mutex::new(()),
            decoding_columns: Arc::clone(&decoding_columns),
            data_sender,
            control_sender,
            control_input: control_input_tx,
            handles: Mutex::new(SharedStreamHandles {
                datasource: None,
                decoder: None,
                forward: None,
                control_forward: None,
                data_anchor: Some(data_anchor),
                control_anchor: Some(control_anchor),
            }),
            state: Mutex::new(SharedStreamState {
                status: SharedStreamStatus::Stopped,
                subscribers: HashSet::new(),
                consumer_required_columns: HashMap::new(),
                union_required_columns: Vec::new(),
            }),
        }))
    }

    fn name(&self) -> &str {
        &self.name
    }

    async fn ensure_started(self: &Arc<Self>) -> Result<(), SharedStreamError> {
        let _guard = self.runtime_lock.lock().await;
        {
            let state = self.state.lock().await;
            if matches!(state.status, SharedStreamStatus::Running) {
                return Ok(());
            }
        }
        self.start_runtime().await
    }

    async fn start_runtime(self: &Arc<Self>) -> Result<(), SharedStreamError> {
        let (connector, decoder) = self.connector_factory.build()?;

        let datasource_id = format!("shared:{}/PhysicalDataSource_0", self.name);
        let decoder_id = format!("shared:{}/PhysicalDecoder_1", self.name);

        let mut datasource = DataSourceProcessor::with_custom_id(
            None,
            datasource_id,
            self.name.clone(),
            Arc::clone(&self.schema),
        );
        datasource.add_connector(connector);

        let datasource_data_rx = datasource
            .subscribe_output()
            .ok_or_else(|| SharedStreamError::Internal("datasource output unavailable".into()))?;
        let mut control_rx = datasource.subscribe_control_output().ok_or_else(|| {
            SharedStreamError::Internal("datasource control output unavailable".into())
        })?;

        datasource.add_control_input(self.control_input.subscribe());

        {
            let applied = self.current_decoding_columns().await;
            *self
                .decoding_columns
                .write()
                .expect("shared stream decoding columns lock poisoned") = applied;
        }

        let mut decoder_processor = DecoderProcessor::with_custom_id(decoder_id, decoder)
            .with_projection(Arc::clone(&self.decoding_columns));
        decoder_processor.add_input(datasource_data_rx);
        decoder_processor.add_control_input(self.control_input.subscribe());
        let mut data_rx = decoder_processor
            .subscribe_output()
            .ok_or_else(|| SharedStreamError::Internal("decoder output unavailable".into()))?;

        let datasource_handle = datasource.start();
        let decoder_handle = decoder_processor.start();

        let name_for_data = self.name.clone();
        let data_tx = self.data_sender.clone();
        let forward = tokio::spawn(async move {
            use tokio::sync::broadcast::error::RecvError;
            loop {
                match data_rx.recv().await {
                    Ok(data) => {
                        if crate::processor::base::send_with_backpressure(&data_tx, data)
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(RecvError::Lagged(skipped)) => {
                        crate::processor::base::log_broadcast_lagged(
                            &name_for_data,
                            skipped,
                            "shared stream datasource output",
                        );
                    }
                    Err(RecvError::Closed) => break,
                }
            }
        });

        let name_for_control = self.name.clone();
        let control_tx = self.control_sender.clone();
        let control_forward = tokio::spawn(async move {
            use tokio::sync::broadcast::error::RecvError;
            loop {
                match control_rx.recv().await {
                    Ok(signal) => {
                        if crate::processor::base::send_control_with_backpressure(
                            &control_tx,
                            signal,
                        )
                        .await
                        .is_err()
                        {
                            break;
                        }
                    }
                    Err(RecvError::Lagged(skipped)) => {
                        crate::processor::base::log_broadcast_lagged(
                            &name_for_control,
                            skipped,
                            "shared stream control output",
                        );
                    }
                    Err(RecvError::Closed) => break,
                }
            }
        });

        let mut handles = self.handles.lock().await;
        if handles.datasource.is_some() || handles.decoder.is_some() {
            return Err(SharedStreamError::Internal(format!(
                "shared stream {} runtime already started",
                self.name
            )));
        }
        handles.datasource = Some(datasource_handle);
        handles.decoder = Some(decoder_handle);
        handles.forward = Some(forward);
        handles.control_forward = Some(control_forward);
        drop(handles);

        let mut state = self.state.lock().await;
        state.status = SharedStreamStatus::Running;
        Ok(())
    }

    async fn current_decoding_columns(&self) -> Vec<String> {
        let state = self.state.lock().await;
        if state.union_required_columns.is_empty() {
            self.schema
                .column_schemas()
                .iter()
                .map(|col| col.name.clone())
                .collect()
        } else {
            state.union_required_columns.clone()
        }
    }

    async fn snapshot(&self) -> SharedStreamInfo {
        let state = self.state.lock().await;
        let decoding_columns = self
            .decoding_columns
            .read()
            .expect("shared stream decoding columns lock poisoned")
            .clone();
        SharedStreamInfo {
            name: self.name.clone(),
            schema: Arc::clone(&self.schema),
            created_at: self.created_at,
            status: state.status.clone(),
            connector_id: self.connector_id.clone(),
            subscriber_count: state.subscribers.len(),
            decoding_columns,
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

        Ok(SharedStreamSubscription {
            stream: Arc::clone(self),
            consumer_id,
            receivers_taken: false,
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
        let applied = if union.is_empty() {
            self.schema
                .column_schemas()
                .iter()
                .map(|col| col.name.clone())
                .collect()
        } else {
            union.clone()
        };
        *self
            .decoding_columns
            .write()
            .expect("shared stream decoding columns lock poisoned") = applied;
        Ok(union)
    }

    async fn union_required_columns(&self) -> Vec<String> {
        let state = self.state.lock().await;
        state.union_required_columns.clone()
    }

    async fn unregister_consumer(self: &Arc<Self>, consumer_id: &str) {
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
        let applied = if union.is_empty() {
            self.schema
                .column_schemas()
                .iter()
                .map(|col| col.name.clone())
                .collect()
        } else {
            union
        };
        *self
            .decoding_columns
            .write()
            .expect("shared stream decoding columns lock poisoned") = applied;

        let should_stop = state.subscribers.is_empty();
        drop(state);
        if should_stop {
            if let Err(err) = Arc::clone(self).stop_runtime().await {
                tracing::error!(stream_name = %self.name(), error = %err, "shared stream shutdown error");
            }
        }
    }

    async fn stop_runtime(self: Arc<Self>) -> Result<(), SharedStreamError> {
        let _guard = self.runtime_lock.lock().await;
        {
            let mut state = self.state.lock().await;
            if !state.subscribers.is_empty() {
                return Ok(());
            }
            state.union_required_columns.clear();
            state.consumer_required_columns.clear();
            state.status = SharedStreamStatus::Stopped;
        }
        *self
            .decoding_columns
            .write()
            .expect("shared stream decoding columns lock poisoned") = self
            .schema
            .column_schemas()
            .iter()
            .map(|col| col.name.clone())
            .collect();

        let _ = self.control_input.send(ControlSignal::Instant(
            InstantControlSignal::StreamQuickEnd { signal_id: 0 },
        ));

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
    use tokio::time::{timeout, Duration};
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

    #[tokio::test]
    async fn shared_stream_decodes_only_union_required_columns() {
        let name = format!("shared_stream_projection_test_{}", Uuid::new_v4().simple());
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
            ColumnSchema::new(
                name.clone(),
                "c".to_string(),
                ConcreteDatatype::Int64(Int64Type),
            ),
        ]));

        let (connector, handle) = MockSourceConnector::new(format!("{name}_connector"));
        let decoder = Arc::new(JsonDecoder::new(
            name.clone(),
            Arc::clone(&schema),
            JsonMap::new(),
        ));

        let config = SharedStreamConfig::new(name.clone(), Arc::clone(&schema))
            .with_connector(Box::new(connector), decoder);
        registry().create_stream(config).await.unwrap();

        let mut sub_a = registry()
            .subscribe(&name, "consumer_a")
            .await
            .expect("subscribe consumer_a");
        let mut sub_b = registry()
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

        let info = registry().get_stream(&name).await.expect("get stream");
        assert_eq!(
            info.decoding_columns,
            vec!["a".to_string(), "b".to_string()]
        );

        let mut rx_a = sub_a.take_receivers().0;
        let mut rx_b = sub_b.take_receivers().0;

        handle
            .send(br#"{"a":1,"b":2,"c":3}"#.as_ref())
            .await
            .expect("send payload");

        async fn read_one(
            rx: &mut broadcast::Receiver<StreamData>,
        ) -> Box<dyn crate::model::Collection> {
            loop {
                let data = timeout(Duration::from_secs(2), rx.recv())
                    .await
                    .expect("recv timeout")
                    .expect("recv");
                if let StreamData::Collection(collection) = data {
                    return collection;
                }
            }
        }

        let collection_a = read_one(&mut rx_a).await;
        let collection_b = read_one(&mut rx_b).await;

        for collection in [collection_a, collection_b] {
            assert_eq!(collection.num_rows(), 1);
            let row = &collection.rows()[0];
            assert_eq!(
                row.value_by_name(name.as_str(), "a"),
                Some(&datatypes::Value::Int64(1))
            );
            assert_eq!(
                row.value_by_name(name.as_str(), "b"),
                Some(&datatypes::Value::Int64(2))
            );
            assert_eq!(
                row.value_by_name(name.as_str(), "c"),
                Some(&datatypes::Value::Null)
            );
        }

        sub_a.release().await;
        sub_b.release().await;
        handle.close().await.ok();
        registry().drop_stream(&name).await.unwrap();
    }
}
