use crate::backpressure_hub::BackpressureHub;
use crate::catalog::StreamDecoderConfig;
use crate::codec::{MergerRegistry, RecordDecoder};
use crate::connector::SourceConnector;
use crate::planner::decode_projection::DecodeProjection;
use crate::planner::shared_stream_plan::create_physical_plan_for_shared_stream;
use crate::processor::base::{
    normalize_channel_capacity, DEFAULT_CONTROL_CHANNEL_CAPACITY, DEFAULT_DATA_CHANNEL_CAPACITY,
};
use crate::processor::processor_builder::{
    create_processor_pipeline_for_shared_stream, PlanProcessor, SharedStreamPipelineOptions,
};
use crate::processor::SamplerConfig;
use crate::processor::{ControlSignal, ProcessorPipeline, ProcessorStatsEntry, StreamData};
use crate::runtime::TaskSpawner;
use datatypes::Schema;
use parking_lot::{Mutex as SyncMutex, RwLock as SyncRwLock};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::SystemTime;
use thiserror::Error;
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::task::JoinHandle;
use tokio::time::Duration;

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
    inner: SyncMutex<Option<ConnectorDecoderPair>>,
}

impl OneShotConnectorFactory {
    fn new(connector: Box<dyn SourceConnector>, decoder: Arc<dyn RecordDecoder>) -> Self {
        let connector_id = connector.id().to_string();
        Self {
            connector_id,
            inner: SyncMutex::new(Some((connector, decoder))),
        }
    }
}

impl SharedStreamConnectorFactory for OneShotConnectorFactory {
    fn connector_id(&self) -> String {
        self.connector_id.clone()
    }

    fn build(&self) -> Result<ConnectorDecoderPair, SharedStreamError> {
        let mut guard = self.inner.lock();
        guard.take().ok_or_else(|| {
            SharedStreamError::Internal(
                "shared stream connector factory cannot rebuild connector/decoder".into(),
            )
        })
    }
}

/// Central registry that tracks shared source streams that are owned by the process.
pub struct SharedStreamRegistry {
    streams: RwLock<HashMap<String, Arc<SharedStreamInner>>>,
    spawner: TaskSpawner,
    merger_registry: Arc<MergerRegistry>,
}

impl SharedStreamRegistry {
    #[cfg(test)]
    pub(crate) fn new(spawner: TaskSpawner) -> Self {
        Self::new_with_merger_registry(spawner, Arc::new(MergerRegistry::new()))
    }

    pub(crate) fn new_with_merger_registry(
        spawner: TaskSpawner,
        merger_registry: Arc<MergerRegistry>,
    ) -> Self {
        Self {
            streams: RwLock::new(HashMap::new()),
            spawner,
            merger_registry,
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

        let inner = SharedStreamInner::new(
            config,
            self.spawner.clone(),
            Arc::clone(&self.merger_registry),
        )?;
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

    /// Fetch processor stats for the internal ingest pipeline of one shared stream.
    pub async fn get_stream_processor_stats(
        &self,
        name: &str,
    ) -> Result<SharedStreamProcessorStats, SharedStreamError> {
        let entry = {
            let guard = self.streams.read().await;
            guard
                .get(name)
                .cloned()
                .ok_or_else(|| SharedStreamError::NotFound(name.to_string()))?
        };
        Ok(entry.processor_stats_snapshot().await)
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
        let _task = self.spawner.spawn(async move {
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
    #[error("shared stream receivers already taken: stream={stream} consumer={consumer_id}")]
    ReceiversAlreadyTaken { stream: String, consumer_id: String },
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
    pub flow_instance_id: Arc<str>,
    pub schema: Arc<Schema>,
    pub decoder: StreamDecoderConfig,
    pub connector: Option<SharedStreamConnectorSpec>,
    pub channel_capacity: usize,
    pub sampler: Option<SamplerConfig>,
}

impl SharedStreamConfig {
    pub fn new(stream_name: impl Into<String>, schema: Arc<Schema>) -> Self {
        Self {
            stream_name: stream_name.into(),
            flow_instance_id: Arc::<str>::from("default"),
            schema,
            decoder: StreamDecoderConfig::json(),
            connector: None,
            channel_capacity: DEFAULT_DATA_CHANNEL_CAPACITY,
            sampler: None,
        }
    }

    pub fn with_decoder(mut self, decoder: StreamDecoderConfig) -> Self {
        self.decoder = decoder;
        self
    }

    pub fn set_decoder(&mut self, decoder: StreamDecoderConfig) {
        self.decoder = decoder;
    }

    pub fn with_channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = normalize_channel_capacity(capacity);
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

    pub fn with_sampler(mut self, sampler: SamplerConfig) -> Self {
        self.sampler = Some(sampler);
        self
    }

    pub fn set_sampler(&mut self, sampler: SamplerConfig) {
        self.sampler = Some(sampler);
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

/// Processor stats snapshot for one shared stream's internal ingest pipeline.
#[derive(Clone, Debug)]
pub struct SharedStreamProcessorStats {
    pub stream: String,
    pub status: SharedStreamStatus,
    pub processors: Vec<ProcessorStatsEntry>,
}

#[derive(Clone, Debug)]
pub(crate) struct AppliedDecodeState {
    pub decoding_columns: Vec<String>,
    pub decode_projection: Arc<DecodeProjection>,
}

/// Handle returned to pipeline consumers.
pub struct SharedStreamSubscription {
    stream: Arc<SharedStreamInner>,
    consumer_id: String,
    receivers_taken: bool,
    data_receiver: Option<mpsc::Receiver<StreamData>>,
    control_receiver: Option<mpsc::Receiver<ControlSignal>>,
    spawner: TaskSpawner,
}

impl SharedStreamSubscription {
    pub fn stream_name(&self) -> &str {
        &self.stream.name
    }

    pub fn take_receivers(
        &mut self,
    ) -> Result<(mpsc::Receiver<StreamData>, mpsc::Receiver<ControlSignal>), SharedStreamError>
    {
        if self.receivers_taken {
            return Err(SharedStreamError::ReceiversAlreadyTaken {
                stream: self.stream.name.clone(),
                consumer_id: self.consumer_id.clone(),
            });
        }
        self.receivers_taken = true;
        let data_receiver = self.data_receiver.take().ok_or_else(|| {
            SharedStreamError::Internal(format!(
                "shared stream {} data receiver missing",
                self.stream.name()
            ))
        })?;
        let control_receiver = self.control_receiver.take().ok_or_else(|| {
            SharedStreamError::Internal(format!(
                "shared stream {} control receiver missing",
                self.stream.name()
            ))
        })?;
        Ok((data_receiver, control_receiver))
    }

    pub async fn release(self) {
        let consumer_id = self.consumer_id.clone();
        self.stream.unregister_consumer(&consumer_id).await;
    }
}

impl Drop for SharedStreamSubscription {
    fn drop(&mut self) {
        let stream = Arc::clone(&self.stream);
        let consumer_id = self.consumer_id.clone();
        let spawner = self.spawner.clone();
        let _task = spawner.spawn(async move {
            stream.unregister_consumer(&consumer_id).await;
        });
    }
}

/// Internal structure that holds state for a single shared stream.
struct SharedStreamInner {
    name: String,
    flow_instance_id: Arc<str>,
    schema: Arc<Schema>,
    decoder_config: StreamDecoderConfig,
    created_at: SystemTime,
    connector_id: String,
    connector_factory: Arc<dyn SharedStreamConnectorFactory>,
    sampler: Option<SamplerConfig>,
    merger_registry: Arc<MergerRegistry>,
    spawner: TaskSpawner,
    runtime_lock: Mutex<()>,
    applied_decode_state: Arc<SyncRwLock<AppliedDecodeState>>,
    data_hub: Arc<BackpressureHub<StreamData>>,
    control_hub: Arc<BackpressureHub<ControlSignal>>,
    handles: Mutex<SharedStreamHandles>,
    state: Mutex<SharedStreamState>,
}

/// Join handles that should be awaited when shutting down the stream.
struct SharedStreamHandles {
    pipeline: Option<ProcessorPipeline>,
    forward: Option<JoinHandle<()>>,
}

/// Mutable status tracked for each stream.
struct SharedStreamState {
    status: SharedStreamStatus,
    subscribers: HashSet<String>,
    consumer_required_columns: HashMap<String, Vec<String>>,
    union_required_columns: Vec<String>,
}

impl SharedStreamInner {
    fn metric_labels(&self) -> [&str; 2] {
        [self.flow_instance_id.as_ref(), self.name.as_str()]
    }

    fn set_running_metric(&self, running: bool) {
        let value = if running { 1 } else { 0 };
        veloflux_metrics::shared_stream_running()
            .with_label_values(&self.metric_labels())
            .set(value);
    }

    fn set_active_subscribers_metric(&self, subscribers: usize) {
        let value = i64::try_from(subscribers).unwrap_or(i64::MAX);
        veloflux_metrics::shared_stream_active_subscribers()
            .with_label_values(&self.metric_labels())
            .set(value);
    }

    fn record_start_metric(&self) {
        veloflux_metrics::shared_stream_starts_total()
            .with_label_values(&self.metric_labels())
            .inc();
    }

    fn record_error_metric(&self, kind: &'static str) {
        veloflux_metrics::shared_stream_errors_total()
            .with_label_values(&[self.flow_instance_id.as_ref(), self.name.as_str(), kind])
            .inc();
    }

    fn record_message_in_metric(&self) {
        veloflux_metrics::shared_stream_messages_in_total()
            .with_label_values(&self.metric_labels())
            .inc();
    }

    fn record_message_out_metric(&self) {
        veloflux_metrics::shared_stream_messages_out_total()
            .with_label_values(&self.metric_labels())
            .inc();
    }

    fn new(
        config: SharedStreamConfig,
        spawner: TaskSpawner,
        merger_registry: Arc<MergerRegistry>,
    ) -> Result<Arc<Self>, SharedStreamError> {
        let SharedStreamConfig {
            stream_name,
            flow_instance_id,
            schema,
            decoder,
            connector,
            channel_capacity,
            sampler,
        } = config;
        let data_channel_capacity = normalize_channel_capacity(channel_capacity);
        let control_channel_capacity = DEFAULT_CONTROL_CHANNEL_CAPACITY;

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

        let initial_decoding_columns: Vec<String> = schema
            .column_schemas()
            .iter()
            .map(|col| col.name.clone())
            .collect();
        let decode_projection = Arc::new(DecodeProjection::from_top_level_columns_with_version(
            initial_decoding_columns.as_slice(),
            1,
        ));
        let applied_decode_state = Arc::new(SyncRwLock::new(AppliedDecodeState {
            decoding_columns: initial_decoding_columns,
            decode_projection,
        }));

        let data_hub = Arc::new(BackpressureHub::new(data_channel_capacity));
        let control_hub = Arc::new(BackpressureHub::new(control_channel_capacity));

        let entry = Arc::new(Self {
            name: stream_name,
            flow_instance_id,
            schema,
            decoder_config: decoder,
            created_at: SystemTime::now(),
            connector_id,
            connector_factory,
            sampler,
            merger_registry,
            spawner,
            runtime_lock: Mutex::new(()),
            applied_decode_state: Arc::clone(&applied_decode_state),
            data_hub,
            control_hub,
            handles: Mutex::new(SharedStreamHandles {
                pipeline: None,
                forward: None,
            }),
            state: Mutex::new(SharedStreamState {
                status: SharedStreamStatus::Stopped,
                subscribers: HashSet::new(),
                consumer_required_columns: HashMap::new(),
                union_required_columns: Vec::new(),
            }),
        });
        entry.set_running_metric(false);
        entry.set_active_subscribers_metric(0);
        Ok(entry)
    }

    fn name(&self) -> &str {
        &self.name
    }

    /// Atomically update the shared stream decoding columns and its cached decode projection.
    ///
    /// This avoids rebuilding the projection on the decoder hot path. The projection version is
    /// bumped only when the applied decoding columns actually change.
    fn set_applied_decoding_columns(&self, applied: Vec<String>) {
        let mut guard = self.applied_decode_state.write();
        if guard.decoding_columns == applied {
            return;
        }
        guard.decoding_columns = applied.clone();

        let next_version = guard.decode_projection.version().saturating_add(1);
        guard.decode_projection = Arc::new(DecodeProjection::from_top_level_columns_with_version(
            applied.as_slice(),
            next_version,
        ));
    }

    async fn ensure_started(self: &Arc<Self>) -> Result<(), SharedStreamError> {
        let _guard = self.runtime_lock.lock().await;
        {
            let state = self.state.lock().await;
            if matches!(state.status, SharedStreamStatus::Running) {
                return Ok(());
            }
        }
        match self.start_runtime().await {
            Ok(()) => Ok(()),
            Err(err) => {
                self.record_error_metric("start");
                self.set_running_metric(false);
                Err(err)
            }
        }
    }

    async fn start_runtime(self: &Arc<Self>) -> Result<(), SharedStreamError> {
        let (connector, decoder) = self.connector_factory.build()?;

        {
            let applied = self.current_decoding_columns().await;
            self.set_applied_decoding_columns(applied);
        }

        let physical_plan = create_physical_plan_for_shared_stream(
            &self.name,
            Arc::clone(&self.schema),
            self.decoder_config.clone(),
            self.sampler.clone(),
        );

        let options = SharedStreamPipelineOptions {
            stream_name: self.name.clone(),
            flow_instance_id: Arc::clone(&self.flow_instance_id),
            decoder,
            applied_decode_state: Arc::clone(&self.applied_decode_state),
            merger_registry: Arc::clone(&self.merger_registry),
        };
        let mut pipeline = create_processor_pipeline_for_shared_stream(
            physical_plan,
            options,
            self.spawner.clone(),
        )
        .map_err(|err| SharedStreamError::Internal(err.to_string()))?;
        pipeline.set_pipeline_id(format!("shared:{}", self.name));

        let mut attached = false;
        for processor in &mut pipeline.middle_processors {
            if let PlanProcessor::DataSource(ds) = processor {
                ds.add_connector(connector);
                attached = true;
                break;
            }
        }
        if !attached {
            return Err(SharedStreamError::Internal(
                "shared stream pipeline missing datasource processor".into(),
            ));
        }

        let mut output_rx = pipeline.take_output().ok_or_else(|| {
            SharedStreamError::Internal("shared stream pipeline output unavailable".into())
        })?;

        pipeline.start();

        let data_hub = Arc::clone(&self.data_hub);
        let control_hub = Arc::clone(&self.control_hub);
        let stream = Arc::clone(self);
        let forward = self.spawner.spawn(async move {
            while let Some(item) = output_rx.recv().await {
                match item {
                    StreamData::Control(signal) => {
                        if control_hub.send(signal).await.is_err() {
                            stream.record_error_metric("forward");
                            break;
                        }
                    }
                    other => {
                        stream.record_message_in_metric();
                        if data_hub.send(other).await.is_err() {
                            stream.record_error_metric("forward");
                            break;
                        }
                        stream.record_message_out_metric();
                    }
                }
            }
        });

        let mut handles = self.handles.lock().await;
        if handles.pipeline.is_some() {
            return Err(SharedStreamError::Internal(format!(
                "shared stream {} runtime already started",
                self.name
            )));
        }
        handles.pipeline = Some(pipeline);
        handles.forward = Some(forward);
        drop(handles);

        let mut state = self.state.lock().await;
        state.status = SharedStreamStatus::Running;
        self.set_running_metric(true);
        self.record_start_metric();
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
        let decoding_columns = self.applied_decode_state.read().decoding_columns.clone();
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

    async fn processor_stats_snapshot(&self) -> SharedStreamProcessorStats {
        let status = {
            let state = self.state.lock().await;
            state.status.clone()
        };

        let processors = {
            let handles = self.handles.lock().await;
            handles
                .pipeline
                .as_ref()
                .map(|pipeline| {
                    pipeline
                        .processor_stats()
                        .iter()
                        .map(|handle| handle.snapshot())
                        .collect()
                })
                .unwrap_or_default()
        };

        SharedStreamProcessorStats {
            stream: self.name.clone(),
            status,
            processors,
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
        let data_receiver = self.data_hub.subscribe().await.map_err(|_| {
            SharedStreamError::Internal(format!("shared stream {} data hub is closed", self.name()))
        })?;
        let control_receiver = self.control_hub.subscribe().await.map_err(|_| {
            SharedStreamError::Internal(format!(
                "shared stream {} control hub is closed",
                self.name()
            ))
        })?;
        {
            let mut state = self.state.lock().await;
            if matches!(state.status, SharedStreamStatus::Stopped) {
                self.record_error_metric("consumer");
                return Err(SharedStreamError::Internal(format!(
                    "shared stream {} is stopped",
                    self.name()
                )));
            }
            if !state.subscribers.insert(consumer_id.clone()) {
                self.record_error_metric("consumer");
                return Err(SharedStreamError::Internal(format!(
                    "consumer {consumer_id} already registered for {}",
                    self.name()
                )));
            }
            self.set_active_subscribers_metric(state.subscribers.len());
        }

        Ok(SharedStreamSubscription {
            stream: Arc::clone(self),
            consumer_id,
            receivers_taken: false,
            data_receiver: Some(data_receiver),
            control_receiver: Some(control_receiver),
            spawner: self.spawner.clone(),
        })
    }

    async fn set_consumer_required_columns(
        &self,
        consumer_id: &str,
        required_columns: Vec<String>,
    ) -> Result<Vec<String>, SharedStreamError> {
        let mut state = self.state.lock().await;
        if !state.subscribers.contains(consumer_id) {
            self.record_error_metric("consumer");
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
        self.set_applied_decoding_columns(applied);
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
        self.set_applied_decoding_columns(applied);

        let should_stop = state.subscribers.is_empty();
        self.set_active_subscribers_metric(state.subscribers.len());
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
        self.set_applied_decoding_columns(
            self.schema
                .column_schemas()
                .iter()
                .map(|col| col.name.clone())
                .collect(),
        );

        let (mut pipeline, forward) = {
            let mut handles = self.handles.lock().await;
            (handles.pipeline.take(), handles.forward.take())
        };

        if let Some(pipeline) = &mut pipeline {
            pipeline
                .quick_close(Duration::from_secs(5))
                .await
                .map_err(|err| {
                    self.record_error_metric("stop");
                    SharedStreamError::Internal(err.to_string())
                })?;
        }

        if let Some(handle) = forward {
            let _ = handle.await;
        }

        self.set_running_metric(false);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::JsonDecoder;
    use crate::connector::MockSourceConnector;
    use crate::runtime::TaskSpawner;
    use datatypes::{ColumnSchema, ConcreteDatatype, Int64Type, Schema};
    use serde_json::Map as JsonMap;
    use std::sync::Arc;
    use tokio::time::{timeout, Duration};
    use uuid::Uuid;

    fn test_spawner() -> TaskSpawner {
        TaskSpawner::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("build test tokio runtime"),
        )
    }

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![ColumnSchema::new(
            "shared_stream".to_string(),
            "value".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        )]))
    }

    #[tokio::test]
    async fn create_list_drop_shared_stream() {
        let registry = SharedStreamRegistry::new(test_spawner());
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

        let info = registry.create_stream(config).await.unwrap();
        assert_eq!(info.name, name);
        assert_eq!(info.connector_id, format!("{name}_connector"));

        let listed = registry.list_streams().await;
        assert!(listed.iter().any(|entry| entry.name == name));

        registry.drop_stream(&name).await.unwrap();

        let listed = registry.list_streams().await;
        assert!(
            listed.iter().all(|entry| entry.name != name),
            "shared stream should be removed after drop"
        );
    }

    #[tokio::test]
    async fn shared_stream_tracks_consumer_required_columns_union() {
        let registry = SharedStreamRegistry::new(test_spawner());
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
        let info = registry.create_stream(config).await.unwrap();
        assert_eq!(info.name, name);

        let sub_a = registry
            .subscribe(&name, "consumer_a")
            .await
            .expect("subscribe consumer_a");
        let sub_b = registry
            .subscribe(&name, "consumer_b")
            .await
            .expect("subscribe consumer_b");

        registry
            .set_consumer_required_columns(&name, "consumer_a", vec!["a".to_string()])
            .await
            .expect("set required columns for a");
        registry
            .set_consumer_required_columns(&name, "consumer_b", vec!["b".to_string()])
            .await
            .expect("set required columns for b");

        let union = registry
            .union_required_columns(&name)
            .await
            .expect("union required columns");
        assert_eq!(union, vec!["a".to_string(), "b".to_string()]);

        sub_a.release().await;
        let union = registry
            .union_required_columns(&name)
            .await
            .expect("union required columns");
        assert_eq!(union, vec!["b".to_string()]);

        sub_b.release().await;
        let union = registry
            .union_required_columns(&name)
            .await
            .expect("union required columns");
        assert!(union.is_empty());

        registry.drop_stream(&name).await.unwrap();
    }

    #[tokio::test]
    async fn shared_stream_decodes_only_union_required_columns() {
        let registry = SharedStreamRegistry::new(test_spawner());
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
        registry.create_stream(config).await.unwrap();

        let mut sub_a = registry
            .subscribe(&name, "consumer_a")
            .await
            .expect("subscribe consumer_a");
        let mut sub_b = registry
            .subscribe(&name, "consumer_b")
            .await
            .expect("subscribe consumer_b");

        registry
            .set_consumer_required_columns(&name, "consumer_a", vec!["a".to_string()])
            .await
            .expect("set required columns for a");
        registry
            .set_consumer_required_columns(&name, "consumer_b", vec!["b".to_string()])
            .await
            .expect("set required columns for b");

        let info = registry.get_stream(&name).await.expect("get stream");
        assert_eq!(
            info.decoding_columns,
            vec!["a".to_string(), "b".to_string()]
        );

        let mut rx_a = sub_a.take_receivers().unwrap().0;
        let mut rx_b = sub_b.take_receivers().unwrap().0;

        handle
            .send(br#"{"a":1,"b":2,"c":3}"#.as_ref())
            .await
            .expect("send payload");

        async fn read_one(
            rx: &mut mpsc::Receiver<StreamData>,
        ) -> Box<dyn crate::model::Collection> {
            loop {
                let data = timeout(Duration::from_secs(2), rx.recv())
                    .await
                    .expect("recv timeout")
                    .expect("recv closed");
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
        registry.drop_stream(&name).await.unwrap();
    }
}
