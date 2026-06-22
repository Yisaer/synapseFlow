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
use std::sync::atomic::{AtomicBool, Ordering};
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

/// Column→slot registry for a shared stream's output slot schema (VF-56).
///
/// Slots are assigned the first time a column is referenced by any consumer and a
/// retained slot is **never reassigned or shifted**. This is what lets a consumer's
/// `ColumnRef::ByIndex`, resolved at plan time, stay valid as later consumers
/// attach: existing slots never move, new columns append at the tail.
///
/// The registry can shrink, but **only from the tail and only slots nobody uses**.
/// Each slot carries a reference count (how many consumers rely on it); on detach a
/// consumer releases its columns and any trailing zero-refcount slots are popped.
/// Removing from the tail keeps every retained slot's index, so surviving
/// consumers' by-index slots are untouched. A zero-refcount slot that is *not* at
/// the tail (a stranded "hole") is kept until it becomes the tail, so the schema is
/// not guaranteed to shrink to the minimum — only opportunistically.
///
/// Refcounts are incremented when a column is assigned (plan time), so a slot is
/// protected from the moment it is assigned — before the consumer registers its
/// required columns — closing the plan-assign / start-register race against a
/// concurrent detach. Decrements saturate at 0, so any accounting mismatch can only
/// under-shrink (leak a slot), never pop a slot still in use.
///
/// The slot schema (output row layout) is independent from the parse *whitelist*
/// (which signals the decoder actually parses, = the live union of consumers'
/// required columns). The whitelist shrinks to the union on detach; the slot schema
/// shrinks only its unused tail and a not-yet-reclaimed freed column is emitted as
/// Null at its stable slot. See `set_applied_decoding_columns`.
///
/// Lives behind a synchronous lock so the (sync) planner can resolve slots without
/// awaiting the async stream map.
#[derive(Debug, Default)]
pub(crate) struct SliceRegistry {
    name_to_slot: HashMap<Arc<str>, u32>,
    /// slot index → column name (insertion order == slot order).
    keys: Vec<Arc<str>>,
    /// Per-slot reference count (parallel to `keys`): how many consumers currently
    /// rely on the slot. A trailing slot with count 0 can be popped safely.
    refcounts: Vec<u32>,
    /// Bumped on every slot-layout change (new assignment or tail shrink) so the
    /// decoder can rebuild its slice layout lazily.
    version: u64,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct SliceRegistrySnapshot {
    pub keys: Arc<[Arc<str>]>,
    pub version: u64,
}

impl SliceRegistry {
    /// Return the column's slot, assigning the next free slot if unseen. Each call
    /// adds one reference to the slot (a consumer's plan-time hold), so a freshly
    /// assigned slot is protected against tail-shrink before the consumer registers.
    pub(crate) fn get_or_assign(&mut self, name: &str) -> u32 {
        if let Some(&slot) = self.name_to_slot.get(name) {
            let rc = &mut self.refcounts[slot as usize];
            *rc = rc.saturating_add(1);
            return slot;
        }
        let slot = self.keys.len() as u32;
        let arc: Arc<str> = Arc::from(name);
        self.name_to_slot.insert(Arc::clone(&arc), slot);
        self.keys.push(arc);
        self.refcounts.push(1);
        self.version += 1;
        slot
    }

    /// Release one reference for each named column, then pop any trailing slots
    /// that have no references left. Tail-only: retained slots keep their index, so
    /// surviving consumers' by-index slots stay valid. A zero-refcount slot that is
    /// not at the tail is left in place (a hole) until it becomes the tail. Columns
    /// not present are ignored; refcounts saturate at 0, so an accounting mismatch
    /// can only under-shrink, never pop a slot still referenced. Returns whether the
    /// layout changed (and thus `version` was bumped).
    pub(crate) fn release_and_shrink<'a>(
        &mut self,
        names: impl IntoIterator<Item = &'a str>,
    ) -> bool {
        for name in names {
            if let Some(&slot) = self.name_to_slot.get(name) {
                let rc = &mut self.refcounts[slot as usize];
                *rc = rc.saturating_sub(1);
            }
        }
        let mut shrunk = false;
        while let Some(&0) = self.refcounts.last() {
            self.refcounts.pop();
            if let Some(key) = self.keys.pop() {
                self.name_to_slot.remove(&key);
            }
            shrunk = true;
        }
        if shrunk {
            self.version += 1;
        }
        shrunk
    }

    /// Assign the given columns in order, then return a slot-ordered snapshot.
    pub(crate) fn assign_all<'a>(
        &mut self,
        names: impl IntoIterator<Item = &'a str>,
    ) -> SliceRegistrySnapshot {
        for name in names {
            self.get_or_assign(name);
        }
        self.snapshot()
    }

    pub(crate) fn snapshot(&self) -> SliceRegistrySnapshot {
        SliceRegistrySnapshot {
            keys: Arc::from(self.keys.to_vec()),
            version: self.version,
        }
    }
}

/// Central registry that tracks shared source streams that are owned by the process.
pub struct SharedStreamRegistry {
    streams: RwLock<HashMap<String, Arc<SharedStreamInner>>>,
    /// Per-stream append-only slice registries, kept in a **synchronous** side map
    /// (populated at `create_stream`) so plan-time slot resolution needs no `await`.
    slice_registries: SyncRwLock<HashMap<String, Arc<SyncRwLock<SliceRegistry>>>>,
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
            slice_registries: SyncRwLock::new(HashMap::new()),
            spawner,
            merger_registry,
        }
    }

    /// Get (or lazily create) the append-only slice registry for a stream.
    ///
    /// Synchronous so the planner can resolve consumer slots at plan time. The
    /// entry is created on first access; `create_stream` also seeds it.
    pub(crate) fn slice_registry(&self, name: &str) -> Arc<SyncRwLock<SliceRegistry>> {
        if let Some(reg) = self.slice_registries.read().get(name) {
            return Arc::clone(reg);
        }
        let mut guard = self.slice_registries.write();
        Arc::clone(
            guard
                .entry(name.to_string())
                .or_insert_with(|| Arc::new(SyncRwLock::new(SliceRegistry::default()))),
        )
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

        // Resolve the canonical slice registry first (the planner may have already
        // created and assigned slots into it via `slice_registry()`), then hand that
        // SAME Arc to the inner so the planner and the runtime decoder can never
        // diverge onto two registries (VF-56). `or_insert_with` adopts an existing
        // entry instead of overwriting it.
        let slice_registry = Arc::clone(
            self.slice_registries
                .write()
                .entry(config.stream_name.clone())
                .or_insert_with(|| Arc::new(SyncRwLock::new(SliceRegistry::default()))),
        );
        let inner = SharedStreamInner::new(
            config,
            self.spawner.clone(),
            Arc::clone(&self.merger_registry),
            slice_registry,
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

    /// Register a consumer after applying its shared-stream decode requirement.
    ///
    /// Slot-schema consumers must not receive rows emitted before their required output slot
    /// schema is active. This path updates the shared decode state first and only attaches the
    /// receiver to the fan-out hub afterwards.
    pub async fn subscribe_with_requirements(
        &self,
        name: &str,
        consumer_id: impl Into<String>,
        required_columns: Vec<String>,
        required_slot_version: u64,
    ) -> Result<SharedStreamSubscription, SharedStreamError> {
        let entry = {
            let guard = self.streams.read().await;
            guard
                .get(name)
                .cloned()
                .ok_or_else(|| SharedStreamError::NotFound(name.to_string()))?
        };
        let consumer_id = consumer_id.into();
        entry.ensure_started().await?;
        entry
            .apply_consumer_requirement(&consumer_id, required_columns, required_slot_version > 0)
            .await?;
        match entry.register_consumer(consumer_id.clone()).await {
            Ok(subscription) => Ok(subscription),
            Err(err) => {
                // VF-56: register failed after the plan-time `assign_all` already added
                // this consumer's slot holds, and the consumer never entered the
                // subscriber set, so `unregister_consumer` will never release them.
                // Release here to keep refcounts balanced; otherwise the trailing slots
                // would leak and never reclaim for the stream's lifetime.
                if let Some(columns) = entry.remove_consumer_requirement(&consumer_id).await {
                    entry
                        .slice_registry
                        .write()
                        .release_and_shrink(columns.iter().map(String::as_str));
                }
                Err(err)
            }
        }
    }

    /// Update the required columns for a registered consumer.
    ///
    /// This updates registry state, recomputes the union required columns, and applies the
    /// shared decoder projection immediately.
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
            .set_registered_consumer_required_columns(consumer_id, required_columns)
            .await
    }

    /// Enable VF-56 slot-schema output for this shared stream.
    pub async fn enable_slot_schema_projection(&self, name: &str) -> Result<(), SharedStreamError> {
        self.streams
            .read()
            .await
            .get(name)
            .ok_or_else(|| SharedStreamError::NotFound(name.to_string()))?
            .enable_slot_schema_projection();
        Ok(())
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
    /// Slot-schema version applied by the shared decoder.
    pub slot_version: u64,
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
    pub slot_version: u64,
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
    /// Append-only output-slice registry (VF-56). Shared (same `Arc`) with the
    /// `SharedStreamRegistry.slice_registries` entry so the planner and the runtime
    /// decoder agree on slot assignments.
    slice_registry: Arc<SyncRwLock<SliceRegistry>>,
    slot_schema_projection_enabled: AtomicBool,
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
        slice_registry: Arc<SyncRwLock<SliceRegistry>>,
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
            slot_version: 0,
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
            slice_registry,
            slot_schema_projection_enabled: AtomicBool::new(false),
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

    fn enable_slot_schema_projection(&self) {
        self.slot_schema_projection_enabled
            .store(true, Ordering::Release);
    }

    /// Atomically update the shared stream decoding columns and its cached decode projection.
    ///
    /// This avoids rebuilding the projection on the decoder hot path. The projection version is
    /// bumped only when the applied decoding columns actually change.
    fn set_applied_decoding_columns(&self, applied: Vec<String>) {
        // VF-56: the parse whitelist and the output slot schema are two independent
        // things. `applied` is the *whitelist* — the live union of consumers'
        // required columns — and it shrinks fully when a consumer detaches. The slot
        // schema comes from the slot registry, which shrinks only its unused tail
        // (see `SliceRegistry`), so a surviving consumer's plan-time
        // `ColumnRef::ByIndex` stays valid. We decode only the union but still emit
        // the full slot-width row: a freed column still in the slot schema (not yet
        // reclaimed from the tail) becomes a Null at its stable slot position. The
        // union is always a subset of the slot keys (every slot key came from some
        // consumer's used columns; see `build_shared_slice_schema`), so every
        // decoded column lands in a slot.
        let slot_snapshot = {
            let reg = self.slice_registry.read();
            reg.snapshot()
        };
        let slot_keys = if self.slot_schema_projection_enabled.load(Ordering::Acquire)
            && !slot_snapshot.keys.is_empty()
        {
            Some(Arc::clone(&slot_snapshot.keys))
        } else {
            None
        };

        let mut guard = self.applied_decode_state.write();
        if guard.decoding_columns == applied && guard.slot_version == slot_snapshot.version {
            return;
        }
        guard.decoding_columns = applied.clone();
        guard.slot_version = slot_snapshot.version;

        let next_version = guard.decode_projection.version().saturating_add(1);
        // Whitelist (what to parse) = the union; slot schema (output layout) = the
        // append-only registry keys. Built from independent inputs so a detach
        // narrows the parse set without disturbing slot positions.
        let mut projection =
            DecodeProjection::from_top_level_columns_with_version(applied.as_slice(), next_version);
        if let Some(slots) = slot_keys {
            projection = projection.with_output_slots(slots);
        }
        guard.decode_projection = Arc::new(projection);
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

        pipeline
            .start()
            .await
            .map_err(|err| SharedStreamError::Internal(err.to_string()))?;

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
        let applied = self.applied_decode_state.read();
        let decoding_columns = applied.decoding_columns.clone();
        let slot_version = applied.slot_version;
        drop(applied);
        SharedStreamInfo {
            name: self.name.clone(),
            schema: Arc::clone(&self.schema),
            created_at: self.created_at,
            status: state.status.clone(),
            connector_id: self.connector_id.clone(),
            subscriber_count: state.subscribers.len(),
            decoding_columns,
            slot_version,
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

    async fn set_registered_consumer_required_columns(
        &self,
        consumer_id: &str,
        required_columns: Vec<String>,
    ) -> Result<Vec<String>, SharedStreamError> {
        {
            let state = self.state.lock().await;
            if !state.subscribers.contains(consumer_id) {
                self.record_error_metric("consumer");
                return Err(SharedStreamError::Internal(format!(
                    "consumer {consumer_id} not registered for {}",
                    self.name()
                )));
            }
        }
        self.apply_consumer_requirement(consumer_id, required_columns, false)
            .await
    }

    async fn apply_consumer_requirement(
        &self,
        consumer_id: &str,
        required_columns: Vec<String>,
        enable_slot_schema_projection: bool,
    ) -> Result<Vec<String>, SharedStreamError> {
        if enable_slot_schema_projection {
            self.enable_slot_schema_projection();
        }
        let mut state = self.state.lock().await;
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

    /// Drop a consumer's recorded requirement and recompute the applied whitelist.
    /// Returns the columns that were recorded (if any) so the caller can release the
    /// matching slot-registry hold; this does NOT itself touch slot refcounts.
    async fn remove_consumer_requirement(&self, consumer_id: &str) -> Option<Vec<String>> {
        let mut state = self.state.lock().await;
        let removed = state.consumer_required_columns.remove(consumer_id);

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
        removed
    }

    async fn union_required_columns(&self) -> Vec<String> {
        let state = self.state.lock().await;
        state.union_required_columns.clone()
    }

    async fn unregister_consumer(self: &Arc<Self>, consumer_id: &str) {
        let mut state = self.state.lock().await;
        state.subscribers.remove(consumer_id);
        let released_columns = state.consumer_required_columns.remove(consumer_id);

        // VF-56: release this consumer's hold on its slots and reclaim any now-unused
        // trailing slots. Tail-only, so surviving consumers' by-index slots keep
        // their index; a stranded middle slot is left until it becomes the tail. The
        // released set equals what was assigned at plan time (both are this
        // consumer's used columns), so refcounts balance.
        if let Some(columns) = released_columns.as_ref() {
            self.slice_registry
                .write()
                .release_and_shrink(columns.iter().map(String::as_str));
        }

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
    #[test]
    fn slice_registry_assigns_append_only_stable_slots() {
        let mut reg = SliceRegistry::default();
        // First-seen order defines slots; repeats reuse.
        assert_eq!(reg.get_or_assign("A"), 0);
        assert_eq!(reg.get_or_assign("C"), 1);
        assert_eq!(reg.get_or_assign("A"), 0, "repeat reuses slot");
        assert_eq!(reg.version, 2, "version bumps only on new assignment");

        // A later consumer's new column appends at the tail; existing slots never move.
        assert_eq!(reg.get_or_assign("B"), 2);
        assert_eq!(reg.name_to_slot.get("A").copied(), Some(0));
        assert_eq!(reg.name_to_slot.get("C").copied(), Some(1));
        let snapshot = reg.snapshot();
        assert_eq!(snapshot.keys.len(), 3);
        assert_eq!(
            snapshot.keys.iter().map(|k| k.as_ref()).collect::<Vec<_>>(),
            vec!["A", "C", "B"],
            "keys are slot-ordered (insertion order), not schema/alpha order"
        );
        assert_eq!(reg.name_to_slot.get("missing").copied(), None);
    }

    #[test]
    fn release_and_shrink_pops_unused_tail_only() {
        let mut reg = SliceRegistry::default();
        reg.get_or_assign("a"); // slot 0
        reg.get_or_assign("b"); // slot 1
        reg.get_or_assign("c"); // slot 2
        assert_eq!(reg.version, 3);

        // Releasing a middle column leaves a stranded hole; the live tail blocks shrink.
        assert!(!reg.release_and_shrink(["b"]));
        assert_eq!(reg.keys.len(), 3, "stranded middle slot is not removed");
        assert_eq!(reg.name_to_slot.get("a").copied(), Some(0));
        assert_eq!(
            reg.name_to_slot.get("c").copied(),
            Some(2),
            "tail slot index is unchanged"
        );
        assert_eq!(reg.version, 3, "no pop => no version bump");

        // Releasing the tail pops it AND reclaims the now-trailing stranded hole,
        // stopping at the still-referenced slot.
        assert!(reg.release_and_shrink(["c"]));
        assert_eq!(reg.keys.len(), 1, "c and the stranded b are both reclaimed");
        assert_eq!(
            reg.name_to_slot.get("a").copied(),
            Some(0),
            "live slot keeps its index"
        );
        assert_eq!(reg.name_to_slot.get("b").copied(), None);
        assert_eq!(reg.name_to_slot.get("c").copied(), None);
        assert_eq!(reg.version, 4, "a pop bumps version once");

        // A second reference holds the slot against a single release.
        reg.get_or_assign("a"); // a now referenced twice
        assert!(!reg.release_and_shrink(["a"]));
        assert_eq!(reg.keys.len(), 1, "still referenced once, not popped");
        assert!(reg.release_and_shrink(["a"]));
        assert_eq!(reg.keys.len(), 0, "last reference released, a popped");

        // Over-release saturates at 0 (no underflow/panic) and then pops the slot.
        reg.get_or_assign("x"); // slot 0
        reg.get_or_assign("x"); // referenced twice
        assert!(reg.release_and_shrink(["x", "x", "x"]));
        assert_eq!(reg.keys.len(), 0, "x saturated to 0 and was popped");
    }

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
    async fn detach_shrinks_whitelist_and_unused_tail_slots() {
        // VF-56: with slot-schema projection enabled, a detach narrows the parse
        // whitelist (decoding_columns) to the surviving union, and reclaims unused
        // *trailing* slots while leaving live slots' indices untouched. Slots a@0,
        // b@1, c@2: detaching the middle consumer (b) cannot shrink (c still tails);
        // detaching the tail consumer (c) pops c and reclaims the stranded b.
        let registry = SharedStreamRegistry::new(test_spawner());
        let name = format!("shared_stream_detach_slot_test_{}", Uuid::new_v4().simple());
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
        let (connector, _handle) = MockSourceConnector::new(format!("{name}_connector"));
        let decoder = Arc::new(JsonDecoder::new(
            name.clone(),
            Arc::clone(&schema),
            JsonMap::new(),
        ));

        let config = SharedStreamConfig::new(name.clone(), Arc::clone(&schema))
            .with_connector(Box::new(connector), decoder);
        registry.create_stream(config).await.unwrap();
        registry
            .enable_slot_schema_projection(&name)
            .await
            .expect("enable slot schema");

        let sub_a = registry
            .subscribe(&name, "consumer_a")
            .await
            .expect("subscribe consumer_a");
        let sub_b = registry
            .subscribe(&name, "consumer_b")
            .await
            .expect("subscribe consumer_b");
        let sub_c = registry
            .subscribe(&name, "consumer_c")
            .await
            .expect("subscribe consumer_c");

        // Plan-time slot assignment: one reference per slot (a@0, b@1, c@2).
        registry
            .slice_registry(&name)
            .write()
            .assign_all(["a", "b", "c"]);

        registry
            .set_consumer_required_columns(&name, "consumer_a", vec!["a".to_string()])
            .await
            .expect("set required columns for a");
        registry
            .set_consumer_required_columns(&name, "consumer_b", vec!["b".to_string()])
            .await
            .expect("set required columns for b");
        registry
            .set_consumer_required_columns(&name, "consumer_c", vec!["c".to_string()])
            .await
            .expect("set required columns for c");

        // All attached: whitelist == union {a,b,c}; slot schema has 3 slots.
        let info = registry.get_stream(&name).await.expect("get stream");
        assert_eq!(
            info.decoding_columns,
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
        );
        assert_eq!(info.slot_version, 3, "three slots assigned");

        // Detach the middle consumer b: whitelist drops b, but the slot schema is
        // unchanged because the live tail (c) blocks the shrink.
        sub_b.release().await;
        let info = registry.get_stream(&name).await.expect("get stream");
        assert_eq!(
            info.decoding_columns,
            vec!["a".to_string(), "c".to_string()],
            "whitelist narrows to the surviving union a,c"
        );
        assert_eq!(
            info.slot_version, 3,
            "a stranded middle slot does not shrink the schema"
        );

        // Detach the tail consumer c: c is popped and the now-trailing stranded b is
        // reclaimed, leaving only the live a@0. Slot version bumps.
        sub_c.release().await;
        let info = registry.get_stream(&name).await.expect("get stream");
        assert_eq!(
            info.decoding_columns,
            vec!["a".to_string()],
            "whitelist narrows to a"
        );
        assert_eq!(
            info.slot_version, 4,
            "tail shrink reclaims c and b; version bumps"
        );

        sub_a.release().await;
        registry.drop_stream(&name).await.ok();
    }

    #[tokio::test]
    async fn subscribe_with_requirements_applies_projection_before_attach() {
        let registry = SharedStreamRegistry::new(test_spawner());
        let name = format!(
            "shared_stream_subscribe_requirements_test_{}",
            Uuid::new_v4().simple()
        );
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
        registry.create_stream(config).await.unwrap();
        registry
            .slice_registry(&name)
            .write()
            .assign_all(["a", "b"]);

        let sub = registry
            .subscribe_with_requirements(&name, "consumer_a", vec!["b".to_string()], 2)
            .await
            .expect("subscribe with requirements");

        let info = registry.get_stream(&name).await.expect("get stream");
        assert_eq!(info.subscriber_count, 1);
        assert_eq!(info.decoding_columns, vec!["b".to_string()]);
        assert_eq!(info.slot_version, 2);

        sub.release().await;
        registry.drop_stream(&name).await.ok();
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
