//! Processor builder - creates and connects processors from PhysicalPlan
//!
//! This module provides utilities to build processor pipelines from PhysicalPlan,
//! connecting ControlSourceProcessor outputs to leaf nodes (nodes without children).

use crate::aggregation::AggregateFunctionRegistry;
use crate::checkpoint::{
    CheckpointError, CheckpointManifest, CheckpointSnapshotCollector, CheckpointStore,
    OperatorSnapshot, CHECKPOINT_FORMAT_VERSION,
};
use crate::codec::{
    AesGcmStreamWriter, DecoderRegistry, EncoderRegistry, MergerRegistry, RecordDecoder,
    SinkEncryptionConfig,
};
use crate::connector::{ConnectorRegistry, MqttClientManager};
use crate::pipeline::PipelineRuntimeFailure;
use crate::planner::physical::{DataDomain, PhysicalPlan};
use crate::processor::base::{
    normalize_channel_capacity, LinkKind, LinkReceiver, ProcessorChannelCapacities,
    DEFAULT_CONTROL_CHANNEL_CAPACITY, DEFAULT_DATA_CHANNEL_CAPACITY,
};
use crate::processor::data_metrics::DataMetricDomains;
use crate::processor::decoder_processor::EventtimeDecodeConfig;
use crate::processor::result_collect_processor::{AckHook, AckManager, ErrorLoggingHook};
use crate::processor::EventtimePipelineContext;
use crate::processor::{
    AggregationProcessor, BarrierControlSignalKind, BarrierProcessor, BatchProcessor,
    CheckpointCoordinator, CheckpointTrigger, CollectionLayoutNormalizeProcessor, ComputeProcessor,
    ControlSignal, ControlSourceProcessor, DataSourceProcessor, DecoderProcessor,
    EmptySuppressProcessor, EosWindowProcessor, FilterProcessor, Ingress, InstantControlSignal,
    MemoryCollectionMaterializeProcessor, OrderProcessor, Processor, ProcessorError,
    ProcessorStart, ProjectProcessor, ResultCollectProcessor, RowDiffProcessor, SamplerProcessor,
    SharedStreamProcessor, SinkCompressProcessor, SinkEncoderProcessor, SinkEncryptProcessor,
    SinkProcessor, SlidingWindowProcessor, SourceChangeGateProcessor, StateWindowProcessor,
    StatefulFunctionProcessor, StreamData, StreamingAggregationProcessor, TableScanProcessor,
    TumblingWindowProcessor, WatermarkProcessor,
};
use crate::processor::{MetricKind, MetricSpec, ProcessorStats, ProcessorStatsHandle};
use crate::runtime::TaskSpawner;
use crate::shared_stream::{AppliedDecodeState, SharedStreamRegistry};
use crate::stateful::StatefulFunctionRegistry;
use crate::PipelineRegistries;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::{AbortHandle, JoinHandle};
use tokio::time::{timeout, Duration};
use uuid::Uuid;

const PROCESSOR_START_READY_TIMEOUT: Duration = Duration::from_secs(10);

type PipelineFailureHandler = Arc<dyn Fn(PipelineRuntimeFailure) + Send + Sync>;

#[derive(Debug)]
enum ProcessorTaskExitKind {
    Completed,
    Failed(String),
    Panicked(String),
}

#[derive(Debug)]
struct ProcessorTaskExit {
    processor_id: String,
    processor_kind: &'static str,
    kind: ProcessorTaskExitKind,
}

impl ProcessorTaskExit {
    fn reason(&self) -> String {
        match &self.kind {
            ProcessorTaskExitKind::Completed => {
                "processor task exited unexpectedly with Ok(())".to_string()
            }
            ProcessorTaskExitKind::Failed(err) => err.clone(),
            ProcessorTaskExitKind::Panicked(err) => format!("Join error: {err}"),
        }
    }
}

struct ProcessorTaskMonitor {
    flow_instance_id: Arc<str>,
    pipeline_id: String,
    processor_id: String,
    processor_kind: &'static str,
    allow_normal_completion: bool,
}

#[derive(Clone)]
pub(crate) struct SharedStreamPipelineOptions {
    pub stream_name: String,
    pub flow_instance_id: Arc<str>,
    pub decoder: Arc<dyn RecordDecoder>,
    pub applied_decode_state: Arc<parking_lot::RwLock<AppliedDecodeState>>,
    pub merger_registry: Arc<MergerRegistry>,
}

/// Enum for all processor types created from PhysicalPlan
///
/// This enum allows storing different types of processors in a unified way.
/// All processors are created through PhysicalPlan.
pub(crate) enum PlanProcessor {
    /// AggregationProcessor created from PhysicalAggregation
    Aggregation(AggregationProcessor),
    /// DataSourceProcessor created from PhysicalDatasource
    DataSource(DataSourceProcessor),
    /// TableScanProcessor created from PhysicalTableScan
    TableScan(TableScanProcessor),
    /// DecoderProcessor created from PhysicalDecoder
    Decoder(DecoderProcessor),
    /// CollectionLayoutNormalizeProcessor inserted for collection sources that must preserve full schema.
    CollectionLayoutNormalize(CollectionLayoutNormalizeProcessor),
    /// MemoryCollectionMaterializeProcessor inserted before direct collection sinks.
    MemoryCollectionMaterialize(MemoryCollectionMaterializeProcessor),
    /// SharedStreamProcessor created from PhysicalSharedStream
    SharedSource(SharedStreamProcessor),
    /// SourceChangeGateProcessor created from PhysicalSourceChangeGate
    SourceChangeGate(SourceChangeGateProcessor),
    /// ComputeProcessor created from PhysicalCompute
    Compute(ComputeProcessor),
    /// OrderProcessor created from PhysicalOrder
    Order(OrderProcessor),
    /// ProjectProcessor created from PhysicalProject
    Project(ProjectProcessor),
    /// RowDiffProcessor created from PhysicalRowDiff
    RowDiff(RowDiffProcessor),
    /// EmptySuppressProcessor created from PhysicalEmptySuppress
    EmptySuppress(EmptySuppressProcessor),
    /// StatefulFunctionProcessor created from PhysicalStatefulFunction
    StatefulFunction(StatefulFunctionProcessor),
    /// FilterProcessor created from PhysicalFilter
    Filter(FilterProcessor),
    /// BatchProcessor used by standalone batch/window nodes
    Batch(BatchProcessor),
    /// Sink encoder processor combining batch + encoder
    SinkEncoder(SinkEncoderProcessor),
    /// Sink compress processor applying delivery-boundary compression
    SinkCompress(SinkCompressProcessor),
    /// Sink encrypt processor applying delivery-boundary encryption
    SinkEncrypt(SinkEncryptProcessor),
    /// Streaming aggregation combining window + aggregation
    StreamingAggregation(StreamingAggregationProcessor),
    /// Watermark processor used to drive time progression
    Watermark(WatermarkProcessor),
    /// Tumbling window processor driven by watermarks
    TumblingWindow(TumblingWindowProcessor),
    /// Sliding window processor driven by watermarks (for lookahead windows)
    SlidingWindow(SlidingWindowProcessor),
    /// State window processor driven by open/emit conditions
    StateWindow(StateWindowProcessor),
    /// EOS window processor driven by data-path graceful end
    EosWindow(EosWindowProcessor),
    /// SinkProcessor created from PhysicalDataSink
    Sink(SinkProcessor),
    /// ResultCollectProcessor created from PhysicalResultCollect
    ResultCollect(ResultCollectProcessor),
    /// BarrierProcessor created from PhysicalBarrier
    Barrier(BarrierProcessor),
    /// SamplerProcessor for rate limiting
    Sampler(SamplerProcessor),
}

#[derive(Clone)]
pub(crate) struct ProcessorPipelineDependencies {
    flow_instance_id: Arc<str>,
    mqtt_clients: MqttClientManager,
    connector_registry: Arc<ConnectorRegistry>,
    encoder_registry: Arc<EncoderRegistry>,
    decoder_registry: Arc<DecoderRegistry>,
    aggregate_registry: Arc<AggregateFunctionRegistry>,
    stateful_registry: Arc<StatefulFunctionRegistry>,
    shared_stream_registry: Arc<SharedStreamRegistry>,
    spawner: TaskSpawner,

    eventtime: Option<EventtimePipelineContext>,
    merger_registry: Arc<MergerRegistry>,
    checkpoint_store: Option<Arc<dyn CheckpointStore>>,
}

impl ProcessorPipelineDependencies {
    pub(crate) fn new(
        flow_instance_id: impl Into<Arc<str>>,
        mqtt_clients: MqttClientManager,
        shared_stream_registry: Arc<SharedStreamRegistry>,
        registries: &PipelineRegistries,
        eventtime: Option<EventtimePipelineContext>,
        spawner: TaskSpawner,
    ) -> Self {
        Self {
            flow_instance_id: flow_instance_id.into(),
            mqtt_clients,
            connector_registry: registries.connector_registry(),
            encoder_registry: registries.encoder_registry(),
            decoder_registry: registries.decoder_registry(),
            aggregate_registry: registries.aggregate_registry(),
            stateful_registry: registries.stateful_registry(),
            shared_stream_registry,
            spawner,
            eventtime,
            merger_registry: registries.merger_registry(),
            checkpoint_store: None,
        }
    }

    pub(crate) fn with_checkpoint_store(mut self, store: Option<Arc<dyn CheckpointStore>>) -> Self {
        self.checkpoint_store = store;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProcessorPipelineOptions {
    pub(crate) data_channel_capacity: usize,
    pub(crate) checkpoint_enabled: bool,
}

impl Default for ProcessorPipelineOptions {
    fn default() -> Self {
        Self {
            data_channel_capacity: DEFAULT_DATA_CHANNEL_CAPACITY,
            checkpoint_enabled: false,
        }
    }
}

impl ProcessorPipelineOptions {
    pub(crate) fn with_data_channel_capacity(mut self, capacity: usize) -> Self {
        self.data_channel_capacity = normalize_channel_capacity(capacity);
        self
    }

    pub(crate) fn with_checkpoint_enabled(mut self, enabled: bool) -> Self {
        self.checkpoint_enabled = enabled;
        self
    }

    fn channel_capacities(&self) -> ProcessorChannelCapacities {
        ProcessorChannelCapacities::new(
            normalize_channel_capacity(self.data_channel_capacity),
            DEFAULT_CONTROL_CHANNEL_CAPACITY,
        )
    }
}

#[derive(Clone)]
struct ProcessorBuilderContext {
    flow_instance_id: Arc<str>,
    mqtt_clients: Option<MqttClientManager>,
    connector_registry: Option<Arc<ConnectorRegistry>>,
    encoder_registry: Option<Arc<EncoderRegistry>>,
    decoder_registry: Option<Arc<DecoderRegistry>>,
    aggregate_registry: Option<Arc<AggregateFunctionRegistry>>,
    stateful_registry: Option<Arc<StatefulFunctionRegistry>>,
    shared_stream_registry: Option<Arc<SharedStreamRegistry>>,
    eventtime: Option<EventtimePipelineContext>,
    merger_registry: Option<Arc<MergerRegistry>>,
    shared_stream: Option<SharedStreamPipelineOptions>,
    channel_capacities: ProcessorChannelCapacities,
    checkpoint_enabled: bool,
    checkpoint_store: Option<Arc<dyn CheckpointStore>>,
    checkpoint_keys: HashMap<String, String>,
    output_link_kinds: HashMap<i64, LinkKind>,
    spawner: TaskSpawner,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PipelineLinkKindCounts {
    mpsc_links: u64,
    broadcast_links: u64,
}

impl ProcessorBuilderContext {
    fn flow_instance_id(&self) -> &str {
        self.flow_instance_id.as_ref()
    }

    fn spawner(&self) -> &TaskSpawner {
        &self.spawner
    }

    fn mqtt_clients_ref(&self) -> Result<&MqttClientManager, ProcessorError> {
        self.mqtt_clients.as_ref().ok_or_else(|| {
            ProcessorError::InvalidConfiguration("mqtt client manager unavailable".into())
        })
    }

    fn connector_registry(&self) -> Result<Arc<ConnectorRegistry>, ProcessorError> {
        self.connector_registry
            .as_ref()
            .map(Arc::clone)
            .ok_or_else(|| {
                ProcessorError::InvalidConfiguration("connector registry unavailable".into())
            })
    }

    fn encoder_registry(&self) -> Result<Arc<EncoderRegistry>, ProcessorError> {
        self.encoder_registry
            .as_ref()
            .map(Arc::clone)
            .ok_or_else(|| {
                ProcessorError::InvalidConfiguration("encoder registry unavailable".into())
            })
    }

    fn decoder_registry(&self) -> Result<Arc<DecoderRegistry>, ProcessorError> {
        self.decoder_registry
            .as_ref()
            .map(Arc::clone)
            .ok_or_else(|| {
                ProcessorError::InvalidConfiguration("decoder registry unavailable".into())
            })
    }

    fn aggregate_registry(&self) -> Result<Arc<AggregateFunctionRegistry>, ProcessorError> {
        self.aggregate_registry
            .as_ref()
            .map(Arc::clone)
            .ok_or_else(|| {
                ProcessorError::InvalidConfiguration(
                    "aggregate function registry unavailable".into(),
                )
            })
    }

    fn stateful_registry(&self) -> Result<Arc<StatefulFunctionRegistry>, ProcessorError> {
        self.stateful_registry
            .as_ref()
            .map(Arc::clone)
            .ok_or_else(|| {
                ProcessorError::InvalidConfiguration(
                    "stateful function registry unavailable".into(),
                )
            })
    }

    fn shared_stream_registry(&self) -> Result<Arc<SharedStreamRegistry>, ProcessorError> {
        self.shared_stream_registry
            .as_ref()
            .map(Arc::clone)
            .ok_or_else(|| {
                ProcessorError::InvalidConfiguration("shared stream registry unavailable".into())
            })
    }

    fn merger_registry(&self) -> Result<Arc<MergerRegistry>, ProcessorError> {
        self.merger_registry
            .as_ref()
            .map(Arc::clone)
            .ok_or_else(|| {
                ProcessorError::InvalidConfiguration("merger registry unavailable".into())
            })
    }

    fn eventtime(&self) -> Option<EventtimePipelineContext> {
        self.eventtime.clone()
    }

    fn shared_stream(&self) -> Option<&SharedStreamPipelineOptions> {
        self.shared_stream.as_ref()
    }

    fn channel_capacities_for(&self, plan: &PhysicalPlan) -> ProcessorChannelCapacities {
        let kind = self
            .output_link_kinds
            .get(&plan.get_plan_index())
            .copied()
            .unwrap_or(LinkKind::Broadcast);
        self.channel_capacities.with_link_kind(kind)
    }
}

impl PlanProcessor {
    /// Get the processor ID
    pub fn id(&self) -> &str {
        match self {
            PlanProcessor::Aggregation(p) => p.id(),
            PlanProcessor::DataSource(p) => p.id(),
            PlanProcessor::TableScan(p) => p.id(),
            PlanProcessor::Decoder(p) => p.id(),
            PlanProcessor::CollectionLayoutNormalize(p) => p.id(),
            PlanProcessor::MemoryCollectionMaterialize(p) => p.id(),
            PlanProcessor::SharedSource(p) => p.id(),
            PlanProcessor::SourceChangeGate(p) => p.id(),
            PlanProcessor::Compute(p) => p.id(),
            PlanProcessor::Order(p) => p.id(),
            PlanProcessor::Project(p) => p.id(),
            PlanProcessor::RowDiff(p) => p.id(),
            PlanProcessor::EmptySuppress(p) => p.id(),
            PlanProcessor::StatefulFunction(p) => p.id(),
            PlanProcessor::Filter(p) => p.id(),
            PlanProcessor::Batch(p) => p.id(),
            PlanProcessor::SinkEncoder(p) => p.id(),
            PlanProcessor::SinkCompress(p) => p.id(),
            PlanProcessor::SinkEncrypt(p) => p.id(),
            PlanProcessor::StreamingAggregation(p) => p.id(),
            PlanProcessor::Watermark(p) => p.id(),
            PlanProcessor::TumblingWindow(p) => p.id(),
            PlanProcessor::SlidingWindow(p) => p.id(),
            PlanProcessor::StateWindow(p) => p.id(),
            PlanProcessor::EosWindow(p) => p.id(),
            PlanProcessor::Sink(p) => p.id(),
            PlanProcessor::ResultCollect(p) => p.id(),
            PlanProcessor::Barrier(p) => p.id(),
            PlanProcessor::Sampler(p) => p.id(),
        }
    }

    pub fn checkpoint_key(&self) -> &str {
        match self {
            PlanProcessor::Aggregation(p) => p.checkpoint_key(),
            PlanProcessor::DataSource(p) => p.checkpoint_key(),
            PlanProcessor::TableScan(p) => p.checkpoint_key(),
            PlanProcessor::Decoder(p) => p.checkpoint_key(),
            PlanProcessor::CollectionLayoutNormalize(p) => p.checkpoint_key(),
            PlanProcessor::MemoryCollectionMaterialize(p) => p.checkpoint_key(),
            PlanProcessor::SharedSource(p) => p.checkpoint_key(),
            PlanProcessor::SourceChangeGate(p) => p.checkpoint_key(),
            PlanProcessor::Compute(p) => p.checkpoint_key(),
            PlanProcessor::Order(p) => p.checkpoint_key(),
            PlanProcessor::Project(p) => p.checkpoint_key(),
            PlanProcessor::RowDiff(p) => p.checkpoint_key(),
            PlanProcessor::EmptySuppress(p) => p.checkpoint_key(),
            PlanProcessor::StatefulFunction(p) => p.checkpoint_key(),
            PlanProcessor::Filter(p) => p.checkpoint_key(),
            PlanProcessor::Batch(p) => p.checkpoint_key(),
            PlanProcessor::SinkEncoder(p) => p.checkpoint_key(),
            PlanProcessor::SinkCompress(p) => p.checkpoint_key(),
            PlanProcessor::SinkEncrypt(p) => p.checkpoint_key(),
            PlanProcessor::StreamingAggregation(p) => p.checkpoint_key(),
            PlanProcessor::Watermark(p) => p.checkpoint_key(),
            PlanProcessor::TumblingWindow(p) => p.checkpoint_key(),
            PlanProcessor::SlidingWindow(p) => p.checkpoint_key(),
            PlanProcessor::StateWindow(p) => p.checkpoint_key(),
            PlanProcessor::EosWindow(p) => p.checkpoint_key(),
            PlanProcessor::Sink(p) => p.checkpoint_key(),
            PlanProcessor::ResultCollect(p) => p.checkpoint_key(),
            PlanProcessor::Barrier(p) => p.checkpoint_key(),
            PlanProcessor::Sampler(p) => p.checkpoint_key(),
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            PlanProcessor::Aggregation(_) => "aggregation",
            PlanProcessor::DataSource(_) => "datasource",
            PlanProcessor::TableScan(_) => "table_scan",
            PlanProcessor::Decoder(_) => "decoder",
            PlanProcessor::CollectionLayoutNormalize(_) => "collection_layout_normalize",
            PlanProcessor::MemoryCollectionMaterialize(_) => "memory_collection_materialize",
            PlanProcessor::SharedSource(_) => "shared_source",
            PlanProcessor::SourceChangeGate(_) => "source_change_gate",
            PlanProcessor::Compute(_) => "compute",
            PlanProcessor::Order(_) => "order",
            PlanProcessor::Project(_) => "project",
            PlanProcessor::RowDiff(_) => "row_diff",
            PlanProcessor::EmptySuppress(_) => "empty_suppress",
            PlanProcessor::StatefulFunction(_) => "stateful_function",
            PlanProcessor::Filter(_) => "filter",
            PlanProcessor::Batch(_) => "batch",
            PlanProcessor::SinkEncoder(_) => "sink_encoder",
            PlanProcessor::SinkCompress(_) => "sink_compress",
            PlanProcessor::SinkEncrypt(_) => "sink_encrypt",
            PlanProcessor::StreamingAggregation(_) => "streaming_aggregation",
            PlanProcessor::Watermark(_) => "watermark",
            PlanProcessor::TumblingWindow(_) => "tumbling_window",
            PlanProcessor::SlidingWindow(_) => "sliding_window",
            PlanProcessor::StateWindow(_) => "state_window",
            PlanProcessor::EosWindow(_) => "eos_window",
            PlanProcessor::Sink(_) => "sink",
            PlanProcessor::ResultCollect(_) => "result_collect",
            PlanProcessor::Barrier(_) => "barrier",
            PlanProcessor::Sampler(_) => "sampler",
        }
    }

    pub fn set_checkpoint_snapshot_collector(
        &mut self,
        collector: Option<Arc<CheckpointSnapshotCollector>>,
    ) {
        match self {
            PlanProcessor::Aggregation(p) => p.set_checkpoint_snapshot_collector(collector),
            PlanProcessor::DataSource(p) => p.set_checkpoint_snapshot_collector(collector),
            PlanProcessor::TableScan(p) => p.set_checkpoint_snapshot_collector(collector),
            PlanProcessor::Decoder(p) => p.set_checkpoint_snapshot_collector(collector),
            PlanProcessor::CollectionLayoutNormalize(p) => {
                p.set_checkpoint_snapshot_collector(collector)
            }
            PlanProcessor::MemoryCollectionMaterialize(p) => {
                p.set_checkpoint_snapshot_collector(collector)
            }
            PlanProcessor::SharedSource(p) => p.set_checkpoint_snapshot_collector(collector),
            PlanProcessor::SourceChangeGate(p) => p.set_checkpoint_snapshot_collector(collector),
            PlanProcessor::Compute(p) => p.set_checkpoint_snapshot_collector(collector),
            PlanProcessor::Order(p) => p.set_checkpoint_snapshot_collector(collector),
            PlanProcessor::Project(p) => p.set_checkpoint_snapshot_collector(collector),
            PlanProcessor::RowDiff(p) => p.set_checkpoint_snapshot_collector(collector),
            PlanProcessor::EmptySuppress(p) => p.set_checkpoint_snapshot_collector(collector),
            PlanProcessor::StatefulFunction(p) => p.set_checkpoint_snapshot_collector(collector),
            PlanProcessor::Filter(p) => p.set_checkpoint_snapshot_collector(collector),
            PlanProcessor::Batch(p) => p.set_checkpoint_snapshot_collector(collector),
            PlanProcessor::SinkEncoder(p) => p.set_checkpoint_snapshot_collector(collector),
            PlanProcessor::SinkCompress(p) => p.set_checkpoint_snapshot_collector(collector),
            PlanProcessor::SinkEncrypt(p) => p.set_checkpoint_snapshot_collector(collector),
            PlanProcessor::StreamingAggregation(p) => {
                p.set_checkpoint_snapshot_collector(collector)
            }
            PlanProcessor::Watermark(p) => p.set_checkpoint_snapshot_collector(collector),
            PlanProcessor::TumblingWindow(p) => p.set_checkpoint_snapshot_collector(collector),
            PlanProcessor::SlidingWindow(p) => p.set_checkpoint_snapshot_collector(collector),
            PlanProcessor::StateWindow(p) => p.set_checkpoint_snapshot_collector(collector),
            PlanProcessor::EosWindow(p) => p.set_checkpoint_snapshot_collector(collector),
            PlanProcessor::Sink(p) => p.set_checkpoint_snapshot_collector(collector),
            PlanProcessor::ResultCollect(p) => p.set_checkpoint_snapshot_collector(collector),
            PlanProcessor::Barrier(p) => p.set_checkpoint_snapshot_collector(collector),
            PlanProcessor::Sampler(p) => p.set_checkpoint_snapshot_collector(collector),
        }
    }

    pub fn restore_checkpoint(
        &mut self,
        snapshot: &OperatorSnapshot,
    ) -> Result<(), ProcessorError> {
        match self {
            PlanProcessor::Aggregation(p) => p.restore_checkpoint(snapshot),
            PlanProcessor::DataSource(p) => p.restore_checkpoint(snapshot),
            PlanProcessor::TableScan(p) => p.restore_checkpoint(snapshot),
            PlanProcessor::Decoder(p) => p.restore_checkpoint(snapshot),
            PlanProcessor::CollectionLayoutNormalize(p) => p.restore_checkpoint(snapshot),
            PlanProcessor::MemoryCollectionMaterialize(p) => p.restore_checkpoint(snapshot),
            PlanProcessor::SharedSource(p) => p.restore_checkpoint(snapshot),
            PlanProcessor::SourceChangeGate(p) => p.restore_checkpoint(snapshot),
            PlanProcessor::Compute(p) => p.restore_checkpoint(snapshot),
            PlanProcessor::Order(p) => p.restore_checkpoint(snapshot),
            PlanProcessor::Project(p) => p.restore_checkpoint(snapshot),
            PlanProcessor::RowDiff(p) => p.restore_checkpoint(snapshot),
            PlanProcessor::EmptySuppress(p) => p.restore_checkpoint(snapshot),
            PlanProcessor::StatefulFunction(p) => p.restore_checkpoint(snapshot),
            PlanProcessor::Filter(p) => p.restore_checkpoint(snapshot),
            PlanProcessor::Batch(p) => p.restore_checkpoint(snapshot),
            PlanProcessor::SinkEncoder(p) => p.restore_checkpoint(snapshot),
            PlanProcessor::SinkCompress(p) => p.restore_checkpoint(snapshot),
            PlanProcessor::SinkEncrypt(p) => p.restore_checkpoint(snapshot),
            PlanProcessor::StreamingAggregation(p) => p.restore_checkpoint(snapshot),
            PlanProcessor::Watermark(p) => p.restore_checkpoint(snapshot),
            PlanProcessor::TumblingWindow(p) => p.restore_checkpoint(snapshot),
            PlanProcessor::SlidingWindow(p) => p.restore_checkpoint(snapshot),
            PlanProcessor::StateWindow(p) => p.restore_checkpoint(snapshot),
            PlanProcessor::EosWindow(p) => p.restore_checkpoint(snapshot),
            PlanProcessor::Sink(p) => p.restore_checkpoint(snapshot),
            PlanProcessor::ResultCollect(p) => p.restore_checkpoint(snapshot),
            PlanProcessor::Barrier(p) => p.restore_checkpoint(snapshot),
            PlanProcessor::Sampler(p) => p.restore_checkpoint(snapshot),
        }
    }

    pub fn validate_checkpoint(&self, snapshot: &OperatorSnapshot) -> Result<(), ProcessorError> {
        match self {
            PlanProcessor::Aggregation(p) => p.validate_checkpoint(snapshot),
            PlanProcessor::DataSource(p) => p.validate_checkpoint(snapshot),
            PlanProcessor::TableScan(p) => p.validate_checkpoint(snapshot),
            PlanProcessor::Decoder(p) => p.validate_checkpoint(snapshot),
            PlanProcessor::CollectionLayoutNormalize(p) => p.validate_checkpoint(snapshot),
            PlanProcessor::MemoryCollectionMaterialize(p) => p.validate_checkpoint(snapshot),
            PlanProcessor::SharedSource(p) => p.validate_checkpoint(snapshot),
            PlanProcessor::SourceChangeGate(p) => p.validate_checkpoint(snapshot),
            PlanProcessor::Compute(p) => p.validate_checkpoint(snapshot),
            PlanProcessor::Order(p) => p.validate_checkpoint(snapshot),
            PlanProcessor::Project(p) => p.validate_checkpoint(snapshot),
            PlanProcessor::RowDiff(p) => p.validate_checkpoint(snapshot),
            PlanProcessor::EmptySuppress(p) => p.validate_checkpoint(snapshot),
            PlanProcessor::StatefulFunction(p) => p.validate_checkpoint(snapshot),
            PlanProcessor::Filter(p) => p.validate_checkpoint(snapshot),
            PlanProcessor::Batch(p) => p.validate_checkpoint(snapshot),
            PlanProcessor::SinkEncoder(p) => p.validate_checkpoint(snapshot),
            PlanProcessor::SinkCompress(p) => p.validate_checkpoint(snapshot),
            PlanProcessor::SinkEncrypt(p) => p.validate_checkpoint(snapshot),
            PlanProcessor::StreamingAggregation(p) => p.validate_checkpoint(snapshot),
            PlanProcessor::Watermark(p) => p.validate_checkpoint(snapshot),
            PlanProcessor::TumblingWindow(p) => p.validate_checkpoint(snapshot),
            PlanProcessor::SlidingWindow(p) => p.validate_checkpoint(snapshot),
            PlanProcessor::StateWindow(p) => p.validate_checkpoint(snapshot),
            PlanProcessor::EosWindow(p) => p.validate_checkpoint(snapshot),
            PlanProcessor::Sink(p) => p.validate_checkpoint(snapshot),
            PlanProcessor::ResultCollect(p) => p.validate_checkpoint(snapshot),
            PlanProcessor::Barrier(p) => p.validate_checkpoint(snapshot),
            PlanProcessor::Sampler(p) => p.validate_checkpoint(snapshot),
        }
    }

    pub fn clear_checkpoint_restore(&mut self) {
        match self {
            PlanProcessor::Aggregation(p) => p.clear_checkpoint_restore(),
            PlanProcessor::DataSource(p) => p.clear_checkpoint_restore(),
            PlanProcessor::TableScan(p) => p.clear_checkpoint_restore(),
            PlanProcessor::Decoder(p) => p.clear_checkpoint_restore(),
            PlanProcessor::CollectionLayoutNormalize(p) => p.clear_checkpoint_restore(),
            PlanProcessor::MemoryCollectionMaterialize(p) => p.clear_checkpoint_restore(),
            PlanProcessor::SharedSource(p) => p.clear_checkpoint_restore(),
            PlanProcessor::SourceChangeGate(p) => p.clear_checkpoint_restore(),
            PlanProcessor::Compute(p) => p.clear_checkpoint_restore(),
            PlanProcessor::Order(p) => p.clear_checkpoint_restore(),
            PlanProcessor::Project(p) => p.clear_checkpoint_restore(),
            PlanProcessor::RowDiff(p) => p.clear_checkpoint_restore(),
            PlanProcessor::EmptySuppress(p) => p.clear_checkpoint_restore(),
            PlanProcessor::StatefulFunction(p) => p.clear_checkpoint_restore(),
            PlanProcessor::Filter(p) => p.clear_checkpoint_restore(),
            PlanProcessor::Batch(p) => p.clear_checkpoint_restore(),
            PlanProcessor::SinkEncoder(p) => p.clear_checkpoint_restore(),
            PlanProcessor::SinkCompress(p) => p.clear_checkpoint_restore(),
            PlanProcessor::SinkEncrypt(p) => p.clear_checkpoint_restore(),
            PlanProcessor::StreamingAggregation(p) => p.clear_checkpoint_restore(),
            PlanProcessor::Watermark(p) => p.clear_checkpoint_restore(),
            PlanProcessor::TumblingWindow(p) => p.clear_checkpoint_restore(),
            PlanProcessor::SlidingWindow(p) => p.clear_checkpoint_restore(),
            PlanProcessor::StateWindow(p) => p.clear_checkpoint_restore(),
            PlanProcessor::EosWindow(p) => p.clear_checkpoint_restore(),
            PlanProcessor::Sink(p) => p.clear_checkpoint_restore(),
            PlanProcessor::ResultCollect(p) => p.clear_checkpoint_restore(),
            PlanProcessor::Barrier(p) => p.clear_checkpoint_restore(),
            PlanProcessor::Sampler(p) => p.clear_checkpoint_restore(),
        }
    }

    pub fn set_pipeline_id(&mut self, pipeline_id: &str) {
        if let PlanProcessor::SharedSource(proc) = self {
            proc.set_pipeline_id(pipeline_id);
        }
    }

    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        match self {
            PlanProcessor::Aggregation(p) => p.set_stats(stats),
            PlanProcessor::DataSource(p) => p.set_stats(stats),
            PlanProcessor::TableScan(p) => p.set_stats(stats),
            PlanProcessor::Decoder(p) => p.set_stats(stats),
            PlanProcessor::CollectionLayoutNormalize(p) => p.set_stats(stats),
            PlanProcessor::MemoryCollectionMaterialize(p) => p.set_stats(stats),
            PlanProcessor::SharedSource(p) => p.set_stats(stats),
            PlanProcessor::SourceChangeGate(p) => p.set_stats(stats),
            PlanProcessor::Compute(p) => p.set_stats(stats),
            PlanProcessor::Order(p) => p.set_stats(stats),
            PlanProcessor::Project(p) => p.set_stats(stats),
            PlanProcessor::RowDiff(p) => p.set_stats(stats),
            PlanProcessor::EmptySuppress(p) => p.set_stats(stats),
            PlanProcessor::StatefulFunction(p) => p.set_stats(stats),
            PlanProcessor::Filter(p) => p.set_stats(stats),
            PlanProcessor::Batch(p) => p.set_stats(stats),
            PlanProcessor::SinkEncoder(p) => p.set_stats(stats),
            PlanProcessor::SinkCompress(p) => p.set_stats(stats),
            PlanProcessor::SinkEncrypt(p) => p.set_stats(stats),
            PlanProcessor::StreamingAggregation(p) => p.set_stats(stats),
            PlanProcessor::Watermark(p) => p.set_stats(stats),
            PlanProcessor::TumblingWindow(p) => p.set_stats(stats),
            PlanProcessor::SlidingWindow(p) => p.set_stats(stats),
            PlanProcessor::StateWindow(p) => p.set_stats(stats),
            PlanProcessor::EosWindow(p) => p.set_stats(stats),
            PlanProcessor::Sink(p) => p.set_stats(stats),
            PlanProcessor::ResultCollect(p) => p.set_stats(stats),
            PlanProcessor::Barrier(p) => p.set_stats(stats),
            PlanProcessor::Sampler(p) => p.set_stats(stats),
        }
    }

    /// Start the processor
    pub fn start(&mut self, spawner: &TaskSpawner) -> ProcessorStart {
        match self {
            PlanProcessor::Aggregation(p) => p.start(spawner),
            PlanProcessor::DataSource(p) => p.start(spawner),
            PlanProcessor::TableScan(p) => p.start(spawner),
            PlanProcessor::Decoder(p) => p.start(spawner),
            PlanProcessor::CollectionLayoutNormalize(p) => p.start(spawner),
            PlanProcessor::MemoryCollectionMaterialize(p) => p.start(spawner),
            PlanProcessor::SharedSource(p) => p.start(spawner),
            PlanProcessor::SourceChangeGate(p) => p.start(spawner),
            PlanProcessor::Compute(p) => p.start(spawner),
            PlanProcessor::Order(p) => p.start(spawner),
            PlanProcessor::Project(p) => p.start(spawner),
            PlanProcessor::RowDiff(p) => p.start(spawner),
            PlanProcessor::EmptySuppress(p) => p.start(spawner),
            PlanProcessor::StatefulFunction(p) => p.start(spawner),
            PlanProcessor::Filter(p) => p.start(spawner),
            PlanProcessor::Batch(p) => p.start(spawner),
            PlanProcessor::SinkEncoder(p) => p.start(spawner),
            PlanProcessor::SinkCompress(p) => p.start(spawner),
            PlanProcessor::SinkEncrypt(p) => p.start(spawner),
            PlanProcessor::StreamingAggregation(p) => p.start(spawner),
            PlanProcessor::Watermark(p) => p.start(spawner),
            PlanProcessor::TumblingWindow(p) => p.start(spawner),
            PlanProcessor::SlidingWindow(p) => p.start(spawner),
            PlanProcessor::StateWindow(p) => p.start(spawner),
            PlanProcessor::EosWindow(p) => p.start(spawner),
            PlanProcessor::Sink(p) => p.start(spawner),
            PlanProcessor::ResultCollect(p) => p.start(spawner),
            PlanProcessor::Barrier(p) => p.start(spawner),
            PlanProcessor::Sampler(p) => p.start(spawner),
        }
    }

    /// Subscribe to the processor's output stream
    pub fn subscribe_output(&self) -> Option<LinkReceiver<crate::processor::StreamData>> {
        match self {
            PlanProcessor::Aggregation(p) => p.subscribe_output(),
            PlanProcessor::DataSource(p) => p.subscribe_output(),
            PlanProcessor::TableScan(p) => p.subscribe_output(),
            PlanProcessor::Decoder(p) => p.subscribe_output(),
            PlanProcessor::CollectionLayoutNormalize(p) => p.subscribe_output(),
            PlanProcessor::MemoryCollectionMaterialize(p) => p.subscribe_output(),
            PlanProcessor::SharedSource(p) => p.subscribe_output(),
            PlanProcessor::SourceChangeGate(p) => p.subscribe_output(),
            PlanProcessor::Compute(p) => p.subscribe_output(),
            PlanProcessor::Order(p) => p.subscribe_output(),
            PlanProcessor::Project(p) => p.subscribe_output(),
            PlanProcessor::RowDiff(p) => p.subscribe_output(),
            PlanProcessor::EmptySuppress(p) => p.subscribe_output(),
            PlanProcessor::StatefulFunction(p) => p.subscribe_output(),
            PlanProcessor::Filter(p) => p.subscribe_output(),
            PlanProcessor::Batch(p) => p.subscribe_output(),
            PlanProcessor::SinkEncoder(p) => p.subscribe_output(),
            PlanProcessor::SinkCompress(p) => p.subscribe_output(),
            PlanProcessor::SinkEncrypt(p) => p.subscribe_output(),
            PlanProcessor::StreamingAggregation(p) => p.subscribe_output(),
            PlanProcessor::Watermark(p) => p.subscribe_output(),
            PlanProcessor::TumblingWindow(p) => p.subscribe_output(),
            PlanProcessor::SlidingWindow(p) => p.subscribe_output(),
            PlanProcessor::StateWindow(p) => p.subscribe_output(),
            PlanProcessor::EosWindow(p) => p.subscribe_output(),
            PlanProcessor::Sink(p) => p.subscribe_output(),
            PlanProcessor::ResultCollect(p) => p.subscribe_output(),
            PlanProcessor::Barrier(p) => p.subscribe_output(),
            PlanProcessor::Sampler(p) => p.subscribe_output(),
        }
    }

    /// Subscribe to the processor's control output stream
    pub fn subscribe_control_output(&self) -> Option<LinkReceiver<ControlSignal>> {
        match self {
            PlanProcessor::Aggregation(p) => p.subscribe_control_output(),
            PlanProcessor::DataSource(p) => p.subscribe_control_output(),
            PlanProcessor::TableScan(p) => p.subscribe_control_output(),
            PlanProcessor::Decoder(p) => p.subscribe_control_output(),
            PlanProcessor::CollectionLayoutNormalize(p) => p.subscribe_control_output(),
            PlanProcessor::MemoryCollectionMaterialize(p) => p.subscribe_control_output(),
            PlanProcessor::SharedSource(p) => p.subscribe_control_output(),
            PlanProcessor::SourceChangeGate(p) => p.subscribe_control_output(),
            PlanProcessor::Compute(p) => p.subscribe_control_output(),
            PlanProcessor::Order(p) => p.subscribe_control_output(),
            PlanProcessor::Project(p) => p.subscribe_control_output(),
            PlanProcessor::RowDiff(p) => p.subscribe_control_output(),
            PlanProcessor::EmptySuppress(p) => p.subscribe_control_output(),
            PlanProcessor::StatefulFunction(p) => p.subscribe_control_output(),
            PlanProcessor::Filter(p) => p.subscribe_control_output(),
            PlanProcessor::Batch(p) => p.subscribe_control_output(),
            PlanProcessor::SinkEncoder(p) => p.subscribe_control_output(),
            PlanProcessor::SinkCompress(p) => p.subscribe_control_output(),
            PlanProcessor::SinkEncrypt(p) => p.subscribe_control_output(),
            PlanProcessor::StreamingAggregation(p) => p.subscribe_control_output(),
            PlanProcessor::Watermark(p) => p.subscribe_control_output(),
            PlanProcessor::TumblingWindow(p) => p.subscribe_control_output(),
            PlanProcessor::SlidingWindow(p) => p.subscribe_control_output(),
            PlanProcessor::StateWindow(p) => p.subscribe_control_output(),
            PlanProcessor::EosWindow(p) => p.subscribe_control_output(),
            PlanProcessor::Sink(p) => p.subscribe_control_output(),
            PlanProcessor::ResultCollect(p) => p.subscribe_control_output(),
            PlanProcessor::Barrier(p) => p.subscribe_control_output(),
            PlanProcessor::Sampler(p) => p.subscribe_control_output(),
        }
    }

    /// Add an input channel
    pub fn add_input(&mut self, receiver: LinkReceiver<crate::processor::StreamData>) {
        match self {
            PlanProcessor::Aggregation(p) => p.add_input(receiver),
            PlanProcessor::DataSource(p) => p.add_input(receiver),
            PlanProcessor::TableScan(p) => p.add_input(receiver),
            PlanProcessor::Decoder(p) => p.add_input(receiver),
            PlanProcessor::CollectionLayoutNormalize(p) => p.add_input(receiver),
            PlanProcessor::MemoryCollectionMaterialize(p) => p.add_input(receiver),
            PlanProcessor::SharedSource(p) => p.add_input(receiver),
            PlanProcessor::SourceChangeGate(p) => p.add_input(receiver),
            PlanProcessor::Compute(p) => p.add_input(receiver),
            PlanProcessor::Order(p) => p.add_input(receiver),
            PlanProcessor::Project(p) => p.add_input(receiver),
            PlanProcessor::RowDiff(p) => p.add_input(receiver),
            PlanProcessor::EmptySuppress(p) => p.add_input(receiver),
            PlanProcessor::StatefulFunction(p) => p.add_input(receiver),
            PlanProcessor::Filter(p) => p.add_input(receiver),
            PlanProcessor::Batch(p) => p.add_input(receiver),
            PlanProcessor::SinkEncoder(p) => p.add_input(receiver),
            PlanProcessor::SinkCompress(p) => p.add_input(receiver),
            PlanProcessor::SinkEncrypt(p) => p.add_input(receiver),
            PlanProcessor::StreamingAggregation(p) => p.add_input(receiver),
            PlanProcessor::Watermark(p) => p.add_input(receiver),
            PlanProcessor::TumblingWindow(p) => p.add_input(receiver),
            PlanProcessor::SlidingWindow(p) => p.add_input(receiver),
            PlanProcessor::StateWindow(p) => p.add_input(receiver),
            PlanProcessor::EosWindow(p) => p.add_input(receiver),
            PlanProcessor::Sink(p) => p.add_input(receiver),
            PlanProcessor::ResultCollect(p) => p.add_input(receiver),
            PlanProcessor::Barrier(p) => p.add_input(receiver),
            PlanProcessor::Sampler(p) => p.add_input(receiver),
        }
    }

    /// Add a control input channel
    pub fn add_control_input<R>(&mut self, receiver: R)
    where
        R: Into<LinkReceiver<ControlSignal>>,
    {
        match self {
            PlanProcessor::Aggregation(p) => p.add_control_input(receiver),
            PlanProcessor::DataSource(p) => p.add_control_input(receiver),
            PlanProcessor::TableScan(p) => p.add_control_input(receiver),
            PlanProcessor::Decoder(p) => p.add_control_input(receiver),
            PlanProcessor::CollectionLayoutNormalize(p) => p.add_control_input(receiver),
            PlanProcessor::MemoryCollectionMaterialize(p) => p.add_control_input(receiver),
            PlanProcessor::SharedSource(p) => p.add_control_input(receiver),
            PlanProcessor::SourceChangeGate(p) => p.add_control_input(receiver),
            PlanProcessor::Compute(p) => p.add_control_input(receiver),
            PlanProcessor::Order(p) => p.add_control_input(receiver),
            PlanProcessor::Project(p) => p.add_control_input(receiver),
            PlanProcessor::RowDiff(p) => p.add_control_input(receiver),
            PlanProcessor::EmptySuppress(p) => p.add_control_input(receiver),
            PlanProcessor::StatefulFunction(p) => p.add_control_input(receiver),
            PlanProcessor::Filter(p) => p.add_control_input(receiver),
            PlanProcessor::Batch(p) => p.add_control_input(receiver),
            PlanProcessor::SinkEncoder(p) => p.add_control_input(receiver),
            PlanProcessor::SinkCompress(p) => p.add_control_input(receiver),
            PlanProcessor::SinkEncrypt(p) => p.add_control_input(receiver),
            PlanProcessor::StreamingAggregation(p) => p.add_control_input(receiver),
            PlanProcessor::Watermark(p) => p.add_control_input(receiver),
            PlanProcessor::TumblingWindow(p) => p.add_control_input(receiver),
            PlanProcessor::SlidingWindow(p) => p.add_control_input(receiver),
            PlanProcessor::StateWindow(p) => p.add_control_input(receiver),
            PlanProcessor::EosWindow(p) => p.add_control_input(receiver),
            PlanProcessor::Sink(p) => p.add_control_input(receiver),
            PlanProcessor::ResultCollect(p) => p.add_control_input(receiver),
            PlanProcessor::Barrier(p) => p.add_control_input(receiver),
            PlanProcessor::Sampler(p) => p.add_control_input(receiver),
        }
    }
}

/// Complete processor pipeline structure
///
/// Contains all processors in the pipeline:
/// - ControlSourceProcessor: data flow starting point
/// - Middle processors: created from PhysicalPlan nodes (can be various types)
/// - ResultCollectProcessor: data flow ending point
pub struct ProcessorPipeline {
    /// Pipeline ingress channel (send data/control into ControlSourceProcessor)
    pub(crate) ingress: mpsc::Sender<Ingress>,
    /// Pipeline output channel (receive data from ResultCollectProcessor)
    pub(crate) output: Option<mpsc::Receiver<StreamData>>,
    /// Control source processor (data head)
    pub(crate) control_source: ControlSourceProcessor,
    /// Middle processors created from PhysicalPlan (various types)
    pub(crate) middle_processors: Vec<PlanProcessor>,
    /// Result sink processor (data tail) if downstream forwarding is enabled
    pub(crate) result_sink: Option<ResultCollectProcessor>,
    /// Join handles for all running processors
    handles: Vec<JoinHandle<Result<(), ProcessorError>>>,
    supervisor_abort_handles: Vec<AbortHandle>,
    supervisor_shutdown: Arc<AtomicBool>,
    supervisor_handle: Option<JoinHandle<()>>,
    /// Logical pipeline identifier used for diagnostics/subscriptions
    pipeline_id: String,
    flow_instance_id: Arc<str>,
    /// Allows callers to wait for specific control signals to reach the tail.
    ack_manager: Arc<AckManager>,
    /// Runtime handle for injecting data-channel checkpoint barriers.
    checkpoint_coordinator: CheckpointCoordinator,
    /// In-memory snapshots collected while checkpoint barriers pass through participants.
    checkpoint_snapshot_collector: Arc<CheckpointSnapshotCollector>,
    checkpoint_enabled: bool,
    checkpoint_store: Option<Arc<dyn CheckpointStore>>,
    /// Processor-local stats handles (one per processor instance).
    processor_stats: Vec<ProcessorStatsHandle>,
    channel_capacities: ProcessorChannelCapacities,
    spawner: TaskSpawner,
}

impl ProcessorPipeline {
    fn wrap_processor_handle(
        spawner: &TaskSpawner,
        monitor: ProcessorTaskMonitor,
        handle: JoinHandle<Result<(), ProcessorError>>,
        task_exit_tx: mpsc::UnboundedSender<ProcessorTaskExit>,
    ) -> JoinHandle<Result<(), ProcessorError>> {
        spawner.spawn(async move {
            let ProcessorTaskMonitor {
                flow_instance_id,
                pipeline_id,
                processor_id,
                processor_kind,
                allow_normal_completion,
            } = monitor;
            match handle.await {
                Ok(Ok(())) => {
                    if !allow_normal_completion {
                        let _ = task_exit_tx.send(ProcessorTaskExit {
                            processor_id,
                            processor_kind,
                            kind: ProcessorTaskExitKind::Completed,
                        });
                    }
                    Ok(())
                }
                Ok(Err(err)) => {
                    tracing::error!(
                        flow_instance_id = %flow_instance_id,
                        pipeline_id = %pipeline_id,
                        processor_id = %processor_id,
                        processor_kind,
                        error = %err,
                        "processor task failed"
                    );
                    let _ = task_exit_tx.send(ProcessorTaskExit {
                        processor_id,
                        processor_kind,
                        kind: ProcessorTaskExitKind::Failed(err.to_string()),
                    });
                    Err(err)
                }
                Err(join_err) => {
                    let err = join_err.to_string();
                    tracing::error!(
                        flow_instance_id = %flow_instance_id,
                        pipeline_id = %pipeline_id,
                        processor_id = %processor_id,
                        processor_kind,
                        error = %join_err,
                        "processor task panicked"
                    );
                    let _ = task_exit_tx.send(ProcessorTaskExit {
                        processor_id,
                        processor_kind,
                        kind: ProcessorTaskExitKind::Panicked(err.clone()),
                    });
                    Err(ProcessorError::ProcessingError(format!(
                        "Join error: {err}"
                    )))
                }
            }
        })
    }

    async fn await_processor_ready(
        processor_id: &str,
        processor_kind: &'static str,
        start: &mut ProcessorStart,
    ) -> Result<(), ProcessorError> {
        let Some(ready) = start.take_ready() else {
            return Ok(());
        };
        match timeout(PROCESSOR_START_READY_TIMEOUT, ready).await {
            Ok(Ok(Ok(()))) => Ok(()),
            Ok(Ok(Err(err))) => Err(err),
            Ok(Err(_)) => Err(ProcessorError::ChannelClosed),
            Err(_) => Err(ProcessorError::ProcessingError(format!(
                "processor `{processor_id}` ({processor_kind}) startup readiness timeout"
            ))),
        }
    }

    async fn start_processor(
        &mut self,
        processor_id: String,
        processor_kind: &'static str,
        mut start: ProcessorStart,
        allow_normal_completion: bool,
        task_exit_tx: mpsc::UnboundedSender<ProcessorTaskExit>,
    ) -> Result<(), ProcessorError> {
        if let Err(err) =
            Self::await_processor_ready(&processor_id, processor_kind, &mut start).await
        {
            start.handle.abort();
            let _ = start.handle.await;
            return Err(err);
        }
        self.supervisor_abort_handles
            .push(start.handle.abort_handle());
        self.handles.push(Self::wrap_processor_handle(
            &self.spawner,
            ProcessorTaskMonitor {
                flow_instance_id: Arc::clone(&self.flow_instance_id),
                pipeline_id: self.pipeline_id.clone(),
                processor_id,
                processor_kind,
                allow_normal_completion,
            },
            start.handle,
            task_exit_tx,
        ));
        Ok(())
    }

    fn allows_normal_processor_completion(&self) -> bool {
        self.middle_processors
            .iter()
            .any(|processor| matches!(processor, PlanProcessor::TableScan(_)))
    }

    async fn abort_started_processors(&mut self) {
        for handle in &self.supervisor_abort_handles {
            handle.abort();
        }
        while let Some(handle) = self.handles.pop() {
            handle.abort();
            let _ = handle.await;
        }
        self.supervisor_abort_handles.clear();
        if let Some(handle) = self.supervisor_handle.take() {
            handle.abort();
            let _ = handle.await;
        }
    }

    /// Start all processors in the pipeline. Subsequent calls are no-ops.
    pub async fn start(&mut self) -> Result<(), ProcessorError> {
        self.start_with_failure_handler(Arc::new(|_| {})).await
    }

    pub(crate) async fn start_with_failure_handler(
        &mut self,
        failure_handler: PipelineFailureHandler,
    ) -> Result<(), ProcessorError> {
        if !self.handles.is_empty() {
            return Ok(());
        }
        self.supervisor_abort_handles.clear();
        self.supervisor_shutdown.store(false, Ordering::Release);
        let (task_exit_tx, task_exit_rx) = mpsc::unbounded_channel();
        if let Err(err) = self.start_inner(task_exit_tx).await {
            self.abort_started_processors().await;
            return Err(err);
        }
        self.spawn_supervisor(task_exit_rx, failure_handler);
        Ok(())
    }

    fn spawn_supervisor(
        &mut self,
        mut task_exit_rx: mpsc::UnboundedReceiver<ProcessorTaskExit>,
        failure_handler: PipelineFailureHandler,
    ) {
        let abort_handles = self.supervisor_abort_handles.clone();
        let shutdown = Arc::clone(&self.supervisor_shutdown);
        let flow_instance_id = Arc::clone(&self.flow_instance_id);
        let pipeline_id = self.pipeline_id.clone();
        self.supervisor_handle = Some(self.spawner.spawn(async move {
            while let Some(exit) = task_exit_rx.recv().await {
                if shutdown.load(Ordering::Acquire) {
                    continue;
                }
                let reason = exit.reason();
                tracing::error!(
                    flow_instance_id = %flow_instance_id,
                    pipeline_id = %pipeline_id,
                    processor_id = %exit.processor_id,
                    processor_kind = exit.processor_kind,
                    reason = %reason,
                    "pipeline supervisor detected processor task exit"
                );
                for handle in &abort_handles {
                    handle.abort();
                }
                failure_handler(PipelineRuntimeFailure {
                    pipeline_id,
                    failed_at_ms: unix_timestamp_ms(),
                    processor_id: exit.processor_id,
                    processor_kind: exit.processor_kind.to_string(),
                    reason,
                });
                return;
            }
        }));
    }

    async fn start_inner(
        &mut self,
        task_exit_tx: mpsc::UnboundedSender<ProcessorTaskExit>,
    ) -> Result<(), ProcessorError> {
        let allow_normal_completion = self.allows_normal_processor_completion();

        if let Some(result_sink) = &mut self.result_sink {
            let processor_id = result_sink.id().to_string();
            let start = result_sink.start(&self.spawner);
            self.start_processor(
                processor_id,
                "result_collect",
                start,
                allow_normal_completion,
                task_exit_tx.clone(),
            )
            .await?;
        }

        let len = self.middle_processors.len();
        for idx in (0..len).rev() {
            if !matches!(
                self.middle_processors[idx],
                PlanProcessor::DataSource(_) | PlanProcessor::TableScan(_)
            ) {
                let processor_id = self.middle_processors[idx].id().to_string();
                let processor_kind = self.middle_processors[idx].kind();
                let start = self.middle_processors[idx].start(&self.spawner);
                self.start_processor(
                    processor_id,
                    processor_kind,
                    start,
                    allow_normal_completion,
                    task_exit_tx.clone(),
                )
                .await?;
            }
        }

        for idx in (0..len).rev() {
            if matches!(
                self.middle_processors[idx],
                PlanProcessor::DataSource(_) | PlanProcessor::TableScan(_)
            ) {
                let processor_id = self.middle_processors[idx].id().to_string();
                let processor_kind = self.middle_processors[idx].kind();
                let start = self.middle_processors[idx].start(&self.spawner);
                self.start_processor(
                    processor_id,
                    processor_kind,
                    start,
                    allow_normal_completion,
                    task_exit_tx.clone(),
                )
                .await?;
            }
        }

        let processor_id = self.control_source.id().to_string();
        let start = self.control_source.start(&self.spawner);
        self.start_processor(
            processor_id,
            "control_source",
            start,
            allow_normal_completion,
            task_exit_tx,
        )
        .await
    }

    pub async fn send_ingress(&self, item: Ingress) -> Result<(), ProcessorError> {
        self.ingress
            .send(item)
            .await
            .map_err(|_| ProcessorError::ChannelClosed)
    }

    pub async fn send_data(&self, data: StreamData) -> Result<(), ProcessorError> {
        self.send_ingress(Ingress::data(data)).await
    }

    pub async fn send_control_signal(&self, signal: ControlSignal) -> Result<(), ProcessorError> {
        self.send_ingress(Ingress::control(signal)).await
    }

    async fn await_control_ack(
        &self,
        signal_id: u64,
        rx: tokio::sync::oneshot::Receiver<ControlSignal>,
        timeout_duration: std::time::Duration,
    ) -> Result<ControlSignal, ProcessorError> {
        match timeout(timeout_duration, rx).await {
            Ok(Ok(signal)) => Ok(signal),
            Ok(Err(_)) => Err(ProcessorError::ChannelClosed),
            Err(_) => {
                self.ack_manager.unregister(signal_id);
                Err(ProcessorError::Timeout)
            }
        }
    }

    pub async fn send_quick_end_via_control_with_ack(
        &self,
        timeout_duration: std::time::Duration,
    ) -> Result<u64, ProcessorError> {
        let signal_id = self.control_source.allocate_control_signal_id();
        let rx = self.ack_manager.register(signal_id)?;
        let signal = ControlSignal::Instant(InstantControlSignal::StreamQuickEnd { signal_id });
        if let Err(err) = self.send_control_signal(signal).await {
            self.ack_manager.unregister(signal_id);
            return Err(err);
        }
        let _ = self
            .await_control_ack(signal_id, rx, timeout_duration)
            .await?;
        Ok(signal_id)
    }

    pub async fn send_barrier_via_control_with_ack(
        &self,
        kind: BarrierControlSignalKind,
        timeout_duration: std::time::Duration,
    ) -> Result<u64, ProcessorError> {
        let barrier_id = self.control_source.allocate_control_signal_id();
        let rx = self.ack_manager.register(barrier_id)?;
        if let Err(err) = self
            .send_control_signal(ControlSignal::Barrier(kind.with_id(barrier_id)))
            .await
        {
            self.ack_manager.unregister(barrier_id);
            return Err(err);
        }
        let _ = self
            .await_control_ack(barrier_id, rx, timeout_duration)
            .await?;
        Ok(barrier_id)
    }

    pub async fn send_barrier_via_data_with_ack(
        &self,
        kind: BarrierControlSignalKind,
        timeout_duration: std::time::Duration,
    ) -> Result<u64, ProcessorError> {
        let barrier_id = self.control_source.allocate_control_signal_id();
        let rx = self.ack_manager.register(barrier_id)?;
        if let Err(err) = self
            .send_data(StreamData::control(ControlSignal::Barrier(
                kind.with_id(barrier_id),
            )))
            .await
        {
            self.ack_manager.unregister(barrier_id);
            return Err(err);
        }
        let _ = self
            .await_control_ack(barrier_id, rx, timeout_duration)
            .await?;
        Ok(barrier_id)
    }

    /// Send a barrier control signal into the pipeline via the control channel.
    ///
    /// The returned `barrier_id` is globally unique within the pipeline instance.
    pub async fn send_barrier_via_control(
        &self,
        kind: BarrierControlSignalKind,
    ) -> Result<u64, ProcessorError> {
        let barrier_id = self.control_source.allocate_control_signal_id();
        self.send_control_signal(ControlSignal::Barrier(kind.with_id(barrier_id)))
            .await?;
        Ok(barrier_id)
    }

    /// Send a barrier control signal into the pipeline via the data channel.
    ///
    /// The returned `barrier_id` is globally unique within the pipeline instance.
    pub async fn send_barrier_via_data(
        &self,
        kind: BarrierControlSignalKind,
    ) -> Result<u64, ProcessorError> {
        let barrier_id = self.control_source.allocate_control_signal_id();
        self.send_data(StreamData::control(ControlSignal::Barrier(
            kind.with_id(barrier_id),
        )))
        .await?;
        Ok(barrier_id)
    }

    pub fn set_pipeline_id(&mut self, id: impl Into<String>) {
        let id = id.into();
        self.pipeline_id = id.clone();
        for stats in &self.processor_stats {
            stats.stats.set_pipeline_id(&id);
        }
        for processor in &mut self.middle_processors {
            processor.set_pipeline_id(&id);
        }
    }

    pub fn pipeline_id(&self) -> &str {
        &self.pipeline_id
    }

    /// Load and restore the latest committed checkpoint before processor tasks start.
    pub fn restore_latest_checkpoint(&mut self) -> Result<(), ProcessorError> {
        if !self.checkpoint_enabled {
            return Ok(());
        }
        let store = Arc::clone(self.checkpoint_store.as_ref().ok_or_else(|| {
            ProcessorError::InvalidConfiguration(
                "checkpoint store is not configured for this pipeline".to_string(),
            )
        })?);
        let manifest = match store.load_latest(&self.flow_instance_id, &self.pipeline_id) {
            Ok(Some(manifest)) => manifest,
            Ok(None) => return Ok(()),
            Err(
                CheckpointError::InvalidManifest(reason) | CheckpointError::Incompatible(reason),
            ) => {
                self.clear_incompatible_checkpoint(store.as_ref(), &reason)?;
                return Ok(());
            }
            Err(err) => return Err(ProcessorError::ProcessingError(err.to_string())),
        };
        if let Err(err) = manifest.validate() {
            self.clear_incompatible_checkpoint(store.as_ref(), &err.to_string())?;
            return Ok(());
        }
        if manifest.checkpoint_format_version != CHECKPOINT_FORMAT_VERSION {
            self.clear_incompatible_checkpoint(
                store.as_ref(),
                &format!(
                    "checkpoint format version mismatch: expected {}, got {}",
                    CHECKPOINT_FORMAT_VERSION, manifest.checkpoint_format_version
                ),
            )?;
            return Ok(());
        }
        if manifest.flow_instance_id != self.flow_instance_id.as_ref()
            || manifest.pipeline_id != self.pipeline_id
        {
            return Err(ProcessorError::InvalidConfiguration(
                "checkpoint identity does not match the pipeline runtime".to_string(),
            ));
        }

        for snapshot in &manifest.operator_snapshots {
            if let Err(err) = self.validate_operator_snapshot(snapshot) {
                self.clear_incompatible_checkpoint(store.as_ref(), &err.to_string())?;
                return Ok(());
            }
        }
        for snapshot in &manifest.operator_snapshots {
            if let Err(err) = self.restore_operator_snapshot(snapshot) {
                self.clear_incompatible_checkpoint(store.as_ref(), &err.to_string())?;
                return Ok(());
            }
        }
        let Some(next_signal_id) = manifest.checkpoint_id.checked_add(1) else {
            self.clear_incompatible_checkpoint(
                store.as_ref(),
                "checkpoint id space is exhausted for this pipeline",
            )?;
            return Ok(());
        };
        self.control_source
            .advance_control_signal_id_to_at_least(next_signal_id);
        Ok(())
    }

    fn validate_operator_snapshot(
        &self,
        snapshot: &OperatorSnapshot,
    ) -> Result<(), ProcessorError> {
        let key = snapshot.checkpoint_key.as_str();
        let control_matches = usize::from(self.control_source.checkpoint_key() == key);
        let middle_matches = self
            .middle_processors
            .iter()
            .filter(|processor| processor.checkpoint_key() == key)
            .count();
        let result_matches = usize::from(
            self.result_sink
                .as_ref()
                .is_some_and(|processor| processor.checkpoint_key() == key),
        );
        let match_count = control_matches + middle_matches + result_matches;
        if match_count != 1 {
            return Err(ProcessorError::InvalidConfiguration(format!(
                "checkpoint key `{key}` matched {match_count} processors in the physical plan"
            )));
        }

        let expected_kind = if control_matches == 1 {
            "control_source"
        } else if let Some(processor) = self
            .middle_processors
            .iter()
            .find(|processor| processor.checkpoint_key() == key)
        {
            processor.kind()
        } else {
            "result_collect"
        };
        if expected_kind != snapshot.operator_kind {
            return Err(ProcessorError::InvalidConfiguration(format!(
                "checkpoint operator kind mismatch for `{key}`: expected {expected_kind}, got {}",
                snapshot.operator_kind
            )));
        }

        if control_matches == 1 {
            return self.control_source.validate_checkpoint(snapshot);
        }
        if let Some(processor) = self
            .middle_processors
            .iter()
            .find(|processor| processor.checkpoint_key() == key)
        {
            return processor.validate_checkpoint(snapshot);
        }
        if let Some(processor) = self.result_sink.as_ref() {
            return processor.validate_checkpoint(snapshot);
        }
        Err(ProcessorError::InvalidConfiguration(format!(
            "checkpoint processor `{key}` disappeared during validation"
        )))
    }

    fn restore_operator_snapshot(
        &mut self,
        snapshot: &OperatorSnapshot,
    ) -> Result<(), ProcessorError> {
        let key = snapshot.checkpoint_key.as_str();
        if self.control_source.checkpoint_key() == key {
            return self.control_source.restore_checkpoint(snapshot);
        }
        if let Some(processor) = self
            .middle_processors
            .iter_mut()
            .find(|processor| processor.checkpoint_key() == key)
        {
            return processor.restore_checkpoint(snapshot);
        }
        if let Some(processor) = self
            .result_sink
            .as_mut()
            .filter(|processor| processor.checkpoint_key() == key)
        {
            return processor.restore_checkpoint(snapshot);
        }
        Err(ProcessorError::InvalidConfiguration(format!(
            "checkpoint processor `{}` disappeared during restore",
            snapshot.checkpoint_key
        )))
    }

    fn clear_incompatible_checkpoint(
        &mut self,
        store: &dyn CheckpointStore,
        reason: &str,
    ) -> Result<(), ProcessorError> {
        self.control_source.clear_checkpoint_restore();
        for processor in &mut self.middle_processors {
            processor.clear_checkpoint_restore();
        }
        if let Some(processor) = &mut self.result_sink {
            processor.clear_checkpoint_restore();
        }
        tracing::warn!(
            pipeline_id = %self.pipeline_id,
            reason,
            "clearing incompatible checkpoint and starting without restored state"
        );
        store
            .clear(&self.flow_instance_id, &self.pipeline_id)
            .map_err(|err| ProcessorError::ProcessingError(err.to_string()))
    }

    /// Inject a regular checkpoint barrier through the data channel and wait
    /// until it reaches the pipeline tail.
    pub async fn request_checkpoint(
        &self,
        timeout_duration: std::time::Duration,
    ) -> Result<u64, ProcessorError> {
        if !self.checkpoint_enabled {
            return Err(ProcessorError::InvalidConfiguration(
                "checkpointing is disabled for this pipeline".to_string(),
            ));
        }
        let checkpoint_id = self
            .checkpoint_coordinator
            .request_checkpoint(timeout_duration)
            .await?;
        self.commit_checkpoint(checkpoint_id).await?;
        Ok(checkpoint_id)
    }

    /// Inject the terminal checkpoint barrier used by graceful pipeline end.
    pub async fn request_final_checkpoint(
        &self,
        timeout_duration: std::time::Duration,
    ) -> Result<u64, ProcessorError> {
        if !self.checkpoint_enabled {
            return Err(ProcessorError::InvalidConfiguration(
                "checkpointing is disabled for this pipeline".to_string(),
            ));
        }
        let checkpoint_id = self
            .checkpoint_coordinator
            .request_final_checkpoint(timeout_duration)
            .await?;
        self.commit_checkpoint(checkpoint_id).await?;
        Ok(checkpoint_id)
    }

    /// Return the in-memory collector used by checkpoint-aware processors.
    pub fn checkpoint_snapshot_collector(&self) -> Arc<CheckpointSnapshotCollector> {
        Arc::clone(&self.checkpoint_snapshot_collector)
    }

    async fn commit_checkpoint(&self, checkpoint_id: u64) -> Result<(), ProcessorError> {
        let store = self.checkpoint_store.as_ref().ok_or_else(|| {
            ProcessorError::InvalidConfiguration(
                "checkpoint store is not configured for this pipeline".to_string(),
            )
        })?;
        let created_at_unix_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|err| ProcessorError::ProcessingError(err.to_string()))?
            .as_millis() as i64;
        let operator_snapshots = self
            .checkpoint_snapshot_collector
            .collect(checkpoint_id)
            .map_err(|err| ProcessorError::ProcessingError(err.to_string()))?;
        let manifest = CheckpointManifest {
            checkpoint_format_version: CHECKPOINT_FORMAT_VERSION,
            flow_instance_id: self.flow_instance_id.to_string(),
            pipeline_id: self.pipeline_id.clone(),
            checkpoint_id,
            created_at_unix_ms,
            operator_snapshots,
        };
        store
            .commit(manifest)
            .map_err(|err| ProcessorError::ProcessingError(err.to_string()))?;
        self.checkpoint_snapshot_collector
            .clear(checkpoint_id)
            .map_err(|err| ProcessorError::ProcessingError(err.to_string()))
    }

    pub fn data_channel_capacity(&self) -> usize {
        self.channel_capacities.data
    }

    pub fn control_channel_capacity(&self) -> usize {
        self.channel_capacities.control
    }

    pub fn processor_stats(&self) -> &[ProcessorStatsHandle] {
        &self.processor_stats
    }

    /// Close the pipeline gracefully using the data path.
    pub async fn close(
        &mut self,
        timeout_duration: std::time::Duration,
    ) -> Result<(), ProcessorError> {
        self.graceful_close(timeout_duration).await
    }

    /// Gracefully close the pipeline by sending one final checkpoint via the data channel.
    pub async fn graceful_close(
        &mut self,
        timeout_duration: std::time::Duration,
    ) -> Result<(), ProcessorError> {
        self.supervisor_shutdown.store(true, Ordering::Release);
        let terminal_result = if self.checkpoint_enabled {
            self.request_final_checkpoint(timeout_duration)
                .await
                .map(|_| ())
        } else {
            self.send_barrier_via_data_with_ack(
                BarrierControlSignalKind::StreamGracefulEnd,
                timeout_duration,
            )
            .await
            .map(|_| ())
        };
        self.replace_ingress_sender();
        if let Err(err) = terminal_result {
            self.abort_started_processors().await;
            return Err(err);
        }
        self.await_all_handles().await
    }

    /// Quickly close the pipeline by delivering StreamQuickEnd to the control channel.
    pub async fn quick_close(
        &mut self,
        timeout_duration: std::time::Duration,
    ) -> Result<(), ProcessorError> {
        self.supervisor_shutdown.store(true, Ordering::Release);
        let _ = self
            .send_quick_end_via_control_with_ack(timeout_duration)
            .await?;
        self.replace_ingress_sender();
        self.await_all_handles().await
    }

    fn replace_ingress_sender(&mut self) {
        let (dummy_tx, _) = mpsc::channel(self.channel_capacities.control);
        let old = std::mem::replace(&mut self.ingress, dummy_tx);
        drop(old);
    }

    async fn await_all_handles(&mut self) -> Result<(), ProcessorError> {
        while let Some(handle) = self.handles.pop() {
            match handle.await {
                Ok(result) => result?,
                Err(join_err) => {
                    return Err(ProcessorError::ProcessingError(format!(
                        "Join error: {}",
                        join_err
                    )));
                }
            }
        }
        self.supervisor_abort_handles.clear();
        if let Some(handle) = self.supervisor_handle.take() {
            let _ = handle.await;
        }
        Ok(())
    }

    /// Send StreamData to a specific downstream processor by id
    ///
    /// This method directly delegates to ControlSourceProcessor's send_stream_data method,
    /// providing a convenient interface for sending data to specific processors in the pipeline.
    ///
    /// # Arguments
    /// * `processor_id` - The ID of the target processor
    /// * `data` - The StreamData to send
    ///
    /// # Returns
    /// * `Ok(())` if the data was sent successfully
    /// * `Err(ProcessorError)` if the processor was not found or channel error occurred
    pub async fn send_stream_data(
        &self,
        processor_id: &str,
        data: StreamData,
    ) -> Result<(), ProcessorError> {
        self.control_source
            .send_stream_data(processor_id, data)
            .await
    }

    /// Take ownership of the pipeline's output receiver.
    pub fn take_output(&mut self) -> Option<mpsc::Receiver<StreamData>> {
        self.output.take()
    }
}

fn unix_timestamp_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis().try_into().unwrap_or(u64::MAX))
        .unwrap_or(0)
}

/// Create a processor from a PhysicalPlan node
///
/// This function dispatches to the appropriate processor creation function
/// based on the PhysicalPlan type. All processors are created through PhysicalPlan.
///
/// # Arguments
/// * `plan` - The PhysicalPlan node to create a processor from
///
/// # Returns
/// A ProcessorBuildOutput containing the created processor
struct ProcessorBuildOutput {
    processor: Option<PlanProcessor>,
}

impl ProcessorBuildOutput {
    fn with_processor(processor: PlanProcessor) -> Self {
        Self {
            processor: Some(processor),
        }
    }
}

fn create_processor_from_plan_node(
    plan: &Arc<PhysicalPlan>,
    context: &ProcessorBuilderContext,
) -> Result<ProcessorBuildOutput, ProcessorError> {
    let plan_name = plan.get_plan_name();
    let channel_capacities = context.channel_capacities_for(plan.as_ref());
    let processor_id = context
        .shared_stream()
        .map(|opts| format!("shared:{}/{}", opts.stream_name, plan_name))
        .unwrap_or_else(|| plan_name.clone());
    match plan.as_ref() {
        PhysicalPlan::DataSource(ds) => {
            let schema = ds.schema();
            let checkpoint_key = context.checkpoint_keys.get(&plan_name).ok_or_else(|| {
                ProcessorError::InvalidConfiguration(format!(
                    "missing checkpoint key for datasource `{plan_name}`"
                ))
            })?;
            let mut processor = DataSourceProcessor::with_custom_id_and_channel_capacities(
                None, // plan_index is no longer needed as we use plan_name for ID
                processor_id.clone(),
                checkpoint_key,
                ds.source_name().to_string(),
                schema,
                channel_capacities,
            );
            processor.set_metric_domains(DataMetricDomains::NONE.passthrough(ds.output_domain()));
            Ok(ProcessorBuildOutput::with_processor(
                PlanProcessor::DataSource(processor),
            ))
        }
        PhysicalPlan::TableScan(scan) => {
            let crate::catalog::TableProps::History(props) = scan.props().clone();
            let decoder = context
                .decoder_registry()?
                .instantiate(scan.decoder(), scan.table_name(), scan.schema())
                .map_err(|err| ProcessorError::InvalidConfiguration(err.to_string()))?;
            let processor = TableScanProcessor::new_with_channel_capacities(
                processor_id.clone(),
                scan.table_name().to_string(),
                props,
                decoder,
                channel_capacities,
            );
            Ok(ProcessorBuildOutput::with_processor(
                PlanProcessor::TableScan(processor),
            ))
        }
        PhysicalPlan::Decoder(decoder_plan) => {
            let schema = decoder_plan.schema();
            let mut processor = match context.shared_stream() {
                Some(opts) if opts.stream_name == decoder_plan.source_name() => {
                    DecoderProcessor::with_custom_id_and_channel_capacities(
                        processor_id.clone(),
                        Arc::clone(&opts.decoder),
                        channel_capacities,
                    )
                    .with_shared_decode_state(Arc::clone(&opts.applied_decode_state))
                }
                _ => {
                    let decoder = context
                        .decoder_registry()?
                        .instantiate(
                            decoder_plan.decoder(),
                            decoder_plan.source_name(),
                            Arc::clone(&schema),
                        )
                        .map_err(|err| ProcessorError::InvalidConfiguration(err.to_string()))?;
                    let mut processor = DecoderProcessor::with_custom_id_and_channel_capacities(
                        processor_id.clone(),
                        decoder,
                        channel_capacities,
                    );
                    if let Some(projection) = decoder_plan.decode_projection().cloned() {
                        processor = processor.with_decode_projection(projection);
                    }
                    processor
                }
            };
            if let (Some(eventtime_ctx), Some(eventtime_spec)) =
                (context.eventtime(), decoder_plan.eventtime())
            {
                let parser = eventtime_ctx
                    .registry
                    .resolve(eventtime_spec.type_key.as_str())
                    .map_err(|err| {
                        ProcessorError::InvalidConfiguration(format!(
                            "eventtime.type `{}` not registered: {}",
                            eventtime_spec.type_key, err
                        ))
                    })?;
                processor = processor.with_eventtime(EventtimeDecodeConfig {
                    source_name: decoder_plan.source_name().to_string(),
                    column_name: eventtime_spec.column_name.clone(),
                    column_index: eventtime_spec.column_index,
                    type_key: eventtime_spec.type_key.clone(),
                    parser,
                });
            }
            let input_domain = plan_input_domain(plan, context.merger_registry()?.as_ref())?;
            processor.set_metric_domains(
                DataMetricDomains::NONE
                    .with_input(input_domain)
                    .with_output(DataDomain::Collection),
            );
            Ok(ProcessorBuildOutput::with_processor(
                PlanProcessor::Decoder(processor),
            ))
        }
        PhysicalPlan::CollectionLayoutNormalize(spec) => {
            let processor = CollectionLayoutNormalizeProcessor::new_with_channel_capacities(
                processor_id.clone(),
                Arc::new(spec.clone()),
                channel_capacities,
            );
            Ok(ProcessorBuildOutput::with_processor(
                PlanProcessor::CollectionLayoutNormalize(processor),
            ))
        }
        PhysicalPlan::MemoryCollectionMaterialize(spec) => {
            let processor = MemoryCollectionMaterializeProcessor::new_with_channel_capacities(
                processor_id.clone(),
                Arc::new(spec.clone()),
                channel_capacities,
            );
            Ok(ProcessorBuildOutput::with_processor(
                PlanProcessor::MemoryCollectionMaterialize(processor),
            ))
        }
        PhysicalPlan::SharedStream(shared) => {
            let shared_stream_registry = context.shared_stream_registry()?;
            let mut processor = SharedStreamProcessor::new_with_channel_capacities(
                &processor_id,
                shared.stream_name().to_string(),
                shared_stream_registry,
                channel_capacities,
            );
            processor.set_required_columns(shared.required_columns().to_vec());
            processor.set_required_slot_version(shared.required_slot_version());
            Ok(ProcessorBuildOutput::with_processor(
                PlanProcessor::SharedSource(processor),
            ))
        }
        PhysicalPlan::SourceChangeGate(gate) => {
            let processor = SourceChangeGateProcessor::new_with_channel_capacities(
                processor_id.clone(),
                Arc::new(gate.clone()),
                channel_capacities,
            );
            Ok(ProcessorBuildOutput::with_processor(
                PlanProcessor::SourceChangeGate(processor),
            ))
        }
        PhysicalPlan::Compute(compute) => {
            let processor = ComputeProcessor::new_with_channel_capacities(
                processor_id.clone(),
                Arc::new(compute.clone()),
                channel_capacities,
            );
            Ok(ProcessorBuildOutput::with_processor(
                PlanProcessor::Compute(processor),
            ))
        }
        PhysicalPlan::Order(order) => {
            let processor = OrderProcessor::new_with_channel_capacities(
                processor_id.clone(),
                Arc::new(order.clone()),
                channel_capacities,
            );
            Ok(ProcessorBuildOutput::with_processor(PlanProcessor::Order(
                processor,
            )))
        }
        PhysicalPlan::Project(project) => {
            let processor = ProjectProcessor::new_with_channel_capacities(
                processor_id.clone(),
                Arc::new(project.clone()),
                channel_capacities,
            );
            Ok(ProcessorBuildOutput::with_processor(
                PlanProcessor::Project(processor),
            ))
        }
        PhysicalPlan::RowDiff(spec) => {
            let processor = RowDiffProcessor::new_with_channel_capacities(
                processor_id.clone(),
                Arc::new(spec.clone()),
                channel_capacities,
            )?;
            Ok(ProcessorBuildOutput::with_processor(
                PlanProcessor::RowDiff(processor),
            ))
        }
        PhysicalPlan::ColumnFilter(_) => Err(ProcessorError::InvalidConfiguration(
            "optimized physical plan still contains planner-only PhysicalColumnFilter".to_string(),
        )),
        PhysicalPlan::EmptySuppress(spec) => {
            let processor = EmptySuppressProcessor::new_with_channel_capacities(
                processor_id.clone(),
                Arc::new(spec.clone()),
                channel_capacities,
            );
            Ok(ProcessorBuildOutput::with_processor(
                PlanProcessor::EmptySuppress(processor),
            ))
        }
        PhysicalPlan::StatefulFunction(stateful) => {
            let processor = StatefulFunctionProcessor::new_with_channel_capacities(
                processor_id.clone(),
                Arc::new(stateful.clone()),
                context.stateful_registry()?,
                channel_capacities,
            )?;
            Ok(ProcessorBuildOutput::with_processor(
                PlanProcessor::StatefulFunction(processor),
            ))
        }
        PhysicalPlan::Aggregation(aggregation) => {
            let processor = AggregationProcessor::new_with_channel_capacities(
                processor_id.clone(),
                Arc::new(aggregation.clone()),
                context.aggregate_registry()?,
                channel_capacities,
            );
            Ok(ProcessorBuildOutput::with_processor(
                PlanProcessor::Aggregation(processor),
            ))
        }
        PhysicalPlan::Filter(filter) => {
            let processor = FilterProcessor::new_with_channel_capacities(
                processor_id.clone(),
                Arc::new(filter.clone()),
                channel_capacities,
            );
            Ok(ProcessorBuildOutput::with_processor(PlanProcessor::Filter(
                processor,
            )))
        }
        PhysicalPlan::Batch(batch) => {
            BatchProcessor::validate_batch_config(
                batch.common.batch_count,
                batch.common.batch_duration,
            )?;
            let processor = BatchProcessor::new_with_channel_capacities(
                processor_id.clone(),
                batch.common.batch_count,
                batch.common.batch_duration,
                channel_capacities,
            );
            Ok(ProcessorBuildOutput::with_processor(PlanProcessor::Batch(
                processor,
            )))
        }
        PhysicalPlan::SinkCompress(compress) => {
            let writer = compress
                .codec
                .build_writer()
                .map_err(|err| ProcessorError::InvalidConfiguration(err.to_string()))?;
            let processor = SinkCompressProcessor::new_with_channel_capacities(
                processor_id.clone(),
                writer,
                channel_capacities,
            );
            Ok(ProcessorBuildOutput::with_processor(
                PlanProcessor::SinkCompress(processor),
            ))
        }
        PhysicalPlan::SinkEncrypt(encrypt) => {
            let config = SinkEncryptionConfig {
                algorithm: encrypt.algorithm,
                key_id: encrypt.key_id.clone(),
                key_bits: encrypt.key_bits,
                key: Arc::clone(&encrypt.key),
            };
            let writer = AesGcmStreamWriter::from_config(&config)
                .map_err(|err| ProcessorError::InvalidConfiguration(err.to_string()))?;
            if writer.key_bits() != encrypt.key_bits {
                return Err(ProcessorError::InvalidConfiguration(format!(
                    "sink encrypt key_bits mismatch for key_id `{}`",
                    encrypt.key_id
                )));
            }
            let processor = SinkEncryptProcessor::new_with_channel_capacities(
                processor_id.clone(),
                Box::new(writer),
                channel_capacities,
            );
            Ok(ProcessorBuildOutput::with_processor(
                PlanProcessor::SinkEncrypt(processor),
            ))
        }
        PhysicalPlan::SinkEncoder(encoder) => {
            validate_encoder_input_child(encoder.base.children.len())?;
            let encoder_impl = context
                .encoder_registry()?
                .instantiate(&encoder.encoder)
                .map_err(|err| ProcessorError::InvalidConfiguration(err.to_string()))?;
            let encoder_impl =
                attach_encoder_output_layout(encoder.output_layout.as_ref(), encoder_impl)?;
            SinkEncoderProcessor::validate_batch_config(
                encoder.common.batch_count,
                encoder.common.batch_duration,
            )?;
            let encoder_runtime = encoder_impl
                .start_encoder()
                .map_err(|err| ProcessorError::InvalidConfiguration(err.to_string()))?;
            let processor = SinkEncoderProcessor::new_with_channel_capacities(
                processor_id.clone(),
                encoder_runtime,
                encoder.common.batch_count,
                encoder.common.batch_duration,
                channel_capacities,
            );
            Ok(ProcessorBuildOutput::with_processor(
                PlanProcessor::SinkEncoder(processor),
            ))
        }
        PhysicalPlan::IncSinkEncoder(encoder) => {
            validate_encoder_input_child(encoder.base.children.len())?;
            let encoder_impl = context
                .encoder_registry()?
                .instantiate(&encoder.encoder)
                .map_err(|err| ProcessorError::InvalidConfiguration(err.to_string()))?;
            let encoder_impl =
                attach_encoder_output_layout(encoder.output_layout.as_ref(), encoder_impl)?;
            // PhysicalIncSinkEncoder (fused) uses batch params from the original PhysicalBatch.
            SinkEncoderProcessor::validate_batch_config(
                encoder.common.batch_count,
                encoder.common.batch_duration,
            )?;
            let encoder_runtime = encoder_impl
                .start_encoder()
                .map_err(|err| ProcessorError::InvalidConfiguration(err.to_string()))?;
            let processor = SinkEncoderProcessor::new_with_channel_capacities(
                processor_id.clone(),
                encoder_runtime,
                encoder.common.batch_count,
                encoder.common.batch_duration,
                channel_capacities,
            );
            Ok(ProcessorBuildOutput::with_processor(
                PlanProcessor::SinkEncoder(processor),
            ))
        }
        PhysicalPlan::StreamingAggregation(agg) => {
            let processor = StreamingAggregationProcessor::new_with_channel_capacities(
                processor_id.clone(),
                Arc::new(agg.clone()),
                context.aggregate_registry()?,
                channel_capacities,
            )?;
            Ok(ProcessorBuildOutput::with_processor(
                PlanProcessor::StreamingAggregation(processor),
            ))
        }
        PhysicalPlan::ProcessTimeWatermark(_) | PhysicalPlan::EventtimeWatermark(_) => {
            let processor = WatermarkProcessor::from_physical_plan_with_channel_capacities(
                processor_id.clone(),
                Arc::clone(plan),
                channel_capacities,
            )?
            .ok_or_else(|| {
                ProcessorError::InvalidConfiguration(
                    "Unsupported watermark configuration".to_string(),
                )
            })?;
            Ok(ProcessorBuildOutput::with_processor(
                PlanProcessor::Watermark(processor),
            ))
        }
        PhysicalPlan::Watermark(_) => Err(ProcessorError::InvalidConfiguration(
            "PhysicalWatermark is deprecated; use PhysicalProcessTimeWatermark".to_string(),
        )),
        PhysicalPlan::CountWindow(count_window) => {
            BatchProcessor::validate_batch_config(Some(count_window.count as usize), None)?;
            let processor = BatchProcessor::new_partitioned_with_channel_capacities(
                processor_id.clone(),
                Some(count_window.count as usize),
                None,
                count_window.partition_by_scalars.clone(),
                channel_capacities,
            );
            Ok(ProcessorBuildOutput::with_processor(PlanProcessor::Batch(
                processor,
            )))
        }
        PhysicalPlan::TumblingWindow(window) => {
            let processor = TumblingWindowProcessor::new_with_channel_capacities(
                processor_id.clone(),
                Arc::new(window.clone()),
                channel_capacities,
            );
            Ok(ProcessorBuildOutput::with_processor(
                PlanProcessor::TumblingWindow(processor),
            ))
        }
        PhysicalPlan::SlidingWindow(window) => {
            let processor = SlidingWindowProcessor::new_with_channel_capacities(
                processor_id.clone(),
                Arc::new(window.clone()),
                channel_capacities,
            );
            Ok(ProcessorBuildOutput::with_processor(
                PlanProcessor::SlidingWindow(processor),
            ))
        }
        PhysicalPlan::StateWindow(window) => {
            let processor = StateWindowProcessor::new_with_channel_capacities(
                processor_id.clone(),
                Arc::new(*window.clone()),
                channel_capacities,
            );
            Ok(ProcessorBuildOutput::with_processor(
                PlanProcessor::StateWindow(processor),
            ))
        }
        PhysicalPlan::EosWindow(_) => {
            let processor = EosWindowProcessor::new_with_channel_capacities(
                processor_id.clone(),
                channel_capacities,
            );
            Ok(ProcessorBuildOutput::with_processor(
                PlanProcessor::EosWindow(processor),
            ))
        }
        PhysicalPlan::DataSink(sink_plan) | PhysicalPlan::SinkConnector(sink_plan) => {
            let processor_id = format!("{}_{}", processor_id, sink_plan.connector.sink_id);
            let mut processor = SinkProcessor::new_with_channel_capacities(
                processor_id.clone(),
                channel_capacities,
            );
            processor.set_input_domain(plan_input_domain(
                plan,
                context.merger_registry()?.as_ref(),
            )?);
            if sink_plan.connector.forward_to_result {
                processor.enable_result_forwarding();
            } else {
                processor.disable_result_forwarding();
            }
            let connector_impl = context
                .connector_registry()?
                .instantiate_sink(
                    sink_plan.connector.connector.kind(),
                    &sink_plan.connector.sink_id,
                    &sink_plan.connector.connector,
                    context.flow_instance_id(),
                    context.mqtt_clients_ref()?,
                    context.spawner(),
                )
                .map_err(|err| ProcessorError::InvalidConfiguration(err.to_string()))?;
            processor.add_connector(connector_impl);
            sink_plan
                .connector
                .retry
                .validate()
                .map_err(ProcessorError::InvalidConfiguration)?;
            processor.set_retry_config(sink_plan.connector.retry.clone());
            Ok(ProcessorBuildOutput::with_processor(PlanProcessor::Sink(
                processor,
            )))
        }
        PhysicalPlan::ResultCollect(_result_collect) => {
            let mut processor = ResultCollectProcessor::new(processor_id.clone());
            processor.set_input_domain(plan_input_domain(
                plan,
                context.merger_registry()?.as_ref(),
            )?);
            Ok(ProcessorBuildOutput::with_processor(
                PlanProcessor::ResultCollect(processor),
            ))
        }
        PhysicalPlan::Barrier(barrier) => {
            let expected_upstreams = barrier.base.children.len();
            let mut processor = BarrierProcessor::new_with_channel_capacities(
                processor_id.clone(),
                expected_upstreams,
                channel_capacities,
            );
            processor.set_input_domain(plan_input_domain(
                plan,
                context.merger_registry()?.as_ref(),
            )?);
            Ok(ProcessorBuildOutput::with_processor(
                PlanProcessor::Barrier(processor),
            ))
        }
        PhysicalPlan::Sampler(sampler) => {
            let mut processor = SamplerProcessor::new_with_channel_capacities(
                plan_name.clone(),
                crate::processor::SamplerConfig {
                    interval: sampler.interval,
                    strategy: sampler.strategy.clone(),
                },
                channel_capacities,
            );
            let merger_registry = context.merger_registry()?;
            processor.set_metric_domains(
                DataMetricDomains::NONE
                    .with_input(plan_input_domain(plan, merger_registry.as_ref())?)
                    .with_output(plan_output_domain(plan, merger_registry.as_ref())?),
            );
            if let crate::processor::SamplingStrategy::Packer { .. } = &sampler.strategy {
                processor.set_merger_registry(merger_registry);
                // The Packer merger may build an embedded decoder for the fused
                // decode path, which needs the stream's output schema. Derive it
                // from the sampler's input (the datasource/decoder child).
                let schema = sampler
                    .base
                    .children
                    .first()
                    .and_then(sampler_input_schema)
                    .ok_or_else(|| {
                        ProcessorError::InvalidConfiguration(
                            "Packer sampler requires a schema-bearing input".to_string(),
                        )
                    })?;
                processor.set_merger_schema(schema);
                if let Some(artifact) = sampler.schema_artifact() {
                    processor.set_merger_schema_artifact(artifact);
                }
                // Share decode state for projection pushdown into fused decode.
                if let Some(opts) = context.shared_stream() {
                    processor.set_applied_decode_state(Arc::clone(&opts.applied_decode_state));
                }
            } else if let Ok(registry) = context.merger_registry() {
                processor.set_merger_registry(registry);
            }
            Ok(ProcessorBuildOutput::with_processor(
                PlanProcessor::Sampler(processor),
            ))
        }
    }
}

fn plan_input_domain(
    plan: &PhysicalPlan,
    merger_registry: &MergerRegistry,
) -> Result<DataDomain, ProcessorError> {
    let children = plan.children();
    let first_child = children.first().ok_or_else(|| {
        ProcessorError::InvalidConfiguration(format!(
            "{} requires a data input child",
            plan.get_plan_type()
        ))
    })?;
    let expected = plan_output_domain(first_child.as_ref(), merger_registry)?;
    for child in children.iter().skip(1) {
        let actual = plan_output_domain(child.as_ref(), merger_registry)?;
        if actual != expected {
            return Err(ProcessorError::InvalidConfiguration(format!(
                "{} requires all input children to use the same data domain; expected {:?}, got {:?} from {}",
                plan.get_plan_type(),
                expected,
                actual,
                child.get_plan_type(),
            )));
        }
    }
    Ok(expected)
}

fn plan_output_domain(
    plan: &PhysicalPlan,
    merger_registry: &MergerRegistry,
) -> Result<DataDomain, ProcessorError> {
    match plan {
        PhysicalPlan::DataSource(source) => Ok(source.output_domain()),
        PhysicalPlan::SinkEncoder(_)
        | PhysicalPlan::IncSinkEncoder(_)
        | PhysicalPlan::SinkCompress(_)
        | PhysicalPlan::SinkEncrypt(_) => Ok(DataDomain::Message),
        PhysicalPlan::Sampler(sampler) => match &sampler.strategy {
            crate::processor::SamplingStrategy::Latest => plan_input_domain(plan, merger_registry),
            crate::processor::SamplingStrategy::Packer { props } => merger_registry
                .output_kind(&props.merger.merger_type)
                .map(|kind| match kind {
                    crate::codec::MergerOutputKind::Bytes => DataDomain::Message,
                    crate::codec::MergerOutputKind::Collection => DataDomain::Collection,
                })
                .map_err(|err| ProcessorError::InvalidConfiguration(err.to_string())),
        },
        PhysicalPlan::DataSink(_) | PhysicalPlan::SinkConnector(_) => Ok(DataDomain::Message),
        PhysicalPlan::ResultCollect(_) | PhysicalPlan::Barrier(_) => {
            plan_input_domain(plan, merger_registry)
        }
        PhysicalPlan::TableScan(_)
        | PhysicalPlan::Decoder(_)
        | PhysicalPlan::CollectionLayoutNormalize(_)
        | PhysicalPlan::MemoryCollectionMaterialize(_)
        | PhysicalPlan::StatefulFunction(_)
        | PhysicalPlan::Filter(_)
        | PhysicalPlan::Compute(_)
        | PhysicalPlan::Order(_)
        | PhysicalPlan::Project(_)
        | PhysicalPlan::RowDiff(_)
        | PhysicalPlan::ColumnFilter(_)
        | PhysicalPlan::EmptySuppress(_)
        | PhysicalPlan::Aggregation(_)
        | PhysicalPlan::SharedStream(_)
        | PhysicalPlan::SourceChangeGate(_)
        | PhysicalPlan::Batch(_)
        | PhysicalPlan::StreamingAggregation(_)
        | PhysicalPlan::TumblingWindow(_)
        | PhysicalPlan::CountWindow(_)
        | PhysicalPlan::SlidingWindow(_)
        | PhysicalPlan::StateWindow(_)
        | PhysicalPlan::EosWindow(_)
        | PhysicalPlan::ProcessTimeWatermark(_)
        | PhysicalPlan::EventtimeWatermark(_)
        | PhysicalPlan::Watermark(_) => Ok(DataDomain::Collection),
    }
}

/// Return the `datatypes::Schema` produced by a sampler input node, if the node
/// carries one (datasource / decoder / shared-stream). Used to build a fused
/// Packer merger that needs the stream's output schema.
fn sampler_input_schema(plan: &Arc<PhysicalPlan>) -> Option<Arc<datatypes::Schema>> {
    match plan.as_ref() {
        PhysicalPlan::DataSource(ds) => Some(ds.schema()),
        PhysicalPlan::TableScan(scan) => Some(scan.schema()),
        PhysicalPlan::Decoder(dec) => Some(dec.schema()),
        PhysicalPlan::SharedStream(ss) => Some(ss.schema()),
        _ => None,
    }
}

fn validate_encoder_input_child(child_count: usize) -> Result<(), ProcessorError> {
    if child_count != 1 {
        return Err(ProcessorError::InvalidConfiguration(format!(
            "encoder processor requires exactly one input child, got {child_count}",
        )));
    }
    Ok(())
}

fn attach_encoder_output_layout(
    output_layout: Option<&Arc<crate::planner::physical::output_layout::OutputLayout>>,
    encoder_impl: Arc<dyn crate::codec::encoder::SinkEncoderFactory>,
) -> Result<Arc<dyn crate::codec::encoder::SinkEncoderFactory>, ProcessorError> {
    let output_layout = output_layout.cloned().ok_or_else(|| {
        ProcessorError::InvalidConfiguration(
            "optimized encoder plan is missing output_layout; encoder.output_layout must be \
             attached before processor construction"
                .to_string(),
        )
    })?;
    encoder_impl
        .with_output_layout(output_layout)
        .map_err(|err| ProcessorError::InvalidConfiguration(err.to_string()))
}

/// Internal structure to track processors created from PhysicalPlan nodes
struct ProcessorMap {
    /// Map from plan name to processor
    processors: std::collections::HashMap<String, PlanProcessor>,
    /// Tracks whether a plan node has already been visited
    visited: std::collections::HashSet<String>,
}

impl ProcessorMap {
    fn new() -> Self {
        Self {
            processors: std::collections::HashMap::new(),
            visited: std::collections::HashSet::new(),
        }
    }

    fn get_processor(&self, plan_name: &str) -> Option<&PlanProcessor> {
        self.processors.get(plan_name)
    }

    fn get_processor_mut(&mut self, plan_name: &str) -> Option<&mut PlanProcessor> {
        self.processors.get_mut(plan_name)
    }

    fn insert_processor(&mut self, plan_name: String, processor: PlanProcessor) {
        self.processors.insert(plan_name, processor);
    }

    fn get_all_processors(self) -> Vec<PlanProcessor> {
        self.processors.into_values().collect()
    }

    fn mark_visited(&mut self, plan_name: &str) -> bool {
        self.visited.insert(plan_name.to_string())
    }
}

/// Recursively build processors from PhysicalPlan tree
///
/// This function:
/// 1. Creates a processor for the current plan node
/// 2. Recursively processes all children
/// 3. Connects children's outputs to parent's input
fn build_processors_recursive(
    plan: Arc<PhysicalPlan>,
    processor_map: &mut ProcessorMap,
    context: &ProcessorBuilderContext,
) -> Result<(), ProcessorError> {
    let plan_name = plan.get_plan_name();
    if !processor_map.mark_visited(&plan_name) {
        return Ok(());
    }

    // Create processor for current node
    let creation = create_processor_from_plan_node(&plan, context)?;
    if let Some(processor) = creation.processor {
        processor_map.insert_processor(plan_name, processor);
    }

    // Recursively process children
    for child in plan.children() {
        build_processors_recursive(Arc::clone(child), processor_map, context)?;
    }

    Ok(())
}

fn build_checkpoint_keys(physical_plan: &Arc<PhysicalPlan>) -> HashMap<String, String> {
    fn visit(
        plan: &Arc<PhysicalPlan>,
        visited: &mut HashSet<String>,
        source_occurrences: &mut HashMap<String, usize>,
        checkpoint_keys: &mut HashMap<String, String>,
    ) {
        let plan_name = plan.get_plan_name();
        if !visited.insert(plan_name.clone()) {
            return;
        }
        if let PhysicalPlan::DataSource(source) = plan.as_ref() {
            let occurrence = source_occurrences
                .entry(source.source_name().to_string())
                .or_default();
            checkpoint_keys.insert(
                plan_name,
                format!("datasource:{}:{}", source.source_name(), *occurrence),
            );
            *occurrence += 1;
        }
        for child in plan.children() {
            visit(child, visited, source_occurrences, checkpoint_keys);
        }
    }

    let mut checkpoint_keys = HashMap::new();
    visit(
        physical_plan,
        &mut HashSet::new(),
        &mut HashMap::new(),
        &mut checkpoint_keys,
    );
    checkpoint_keys
}

/// Collect leaf node indices from PhysicalPlan tree
fn collect_leaf_indices(plan: Arc<PhysicalPlan>) -> Vec<i64> {
    use std::collections::HashSet;
    fn helper(plan: Arc<PhysicalPlan>, leaves: &mut HashSet<i64>, visited: &mut HashSet<i64>) {
        let index = plan.get_plan_index();
        if !visited.insert(index) {
            return;
        }
        if plan.children().is_empty() {
            leaves.insert(index);
        } else {
            for child in plan.children() {
                helper(Arc::clone(child), leaves, visited);
            }
        }
    }

    let mut leaves = HashSet::new();
    let mut visited = HashSet::new();
    helper(plan, &mut leaves, &mut visited);
    leaves.into_iter().collect()
}

/// Collect parent-child relationships from PhysicalPlan tree
fn collect_parent_child_relations(plan: Arc<PhysicalPlan>) -> Vec<(i64, i64)> {
    use std::collections::HashSet;
    fn helper(
        plan: Arc<PhysicalPlan>,
        relations: &mut HashSet<(i64, i64)>,
        visited: &mut HashSet<i64>,
    ) {
        let parent_index = plan.get_plan_index();
        if !visited.insert(parent_index) {
            return;
        }
        for child in plan.children() {
            let child_index = child.get_plan_index();
            relations.insert((parent_index, child_index));
            helper(Arc::clone(child), relations, visited);
        }
    }

    let mut relations = HashSet::new();
    let mut visited = HashSet::new();
    helper(plan, &mut relations, &mut visited);
    relations.into_iter().collect()
}

fn collect_plan_refs_by_index(
    plan: &Arc<PhysicalPlan>,
    refs: &mut HashMap<i64, Arc<PhysicalPlan>>,
    visited: &mut HashSet<i64>,
) {
    let index = plan.get_plan_index();
    if !visited.insert(index) {
        return;
    }
    refs.insert(index, Arc::clone(plan));
    for child in plan.children() {
        collect_plan_refs_by_index(child, refs, visited);
    }
}

fn classify_output_link_kinds(physical_plan: &Arc<PhysicalPlan>) -> HashMap<i64, LinkKind> {
    let mut downstream_counts: HashMap<i64, usize> = HashMap::new();
    for (_parent_index, child_index) in collect_parent_child_relations(Arc::clone(physical_plan)) {
        *downstream_counts.entry(child_index).or_insert(0) += 1;
    }

    let mut refs = HashMap::new();
    collect_plan_refs_by_index(physical_plan, &mut refs, &mut HashSet::new());

    refs.into_iter()
        .map(|(index, plan)| {
            let kind = if downstream_counts.get(&index).copied() == Some(1)
                && !matches!(plan.as_ref(), PhysicalPlan::SharedStream(_))
            {
                LinkKind::Mpsc
            } else {
                LinkKind::Broadcast
            };
            (index, kind)
        })
        .collect()
}

fn count_pipeline_link_kinds(
    physical_plan: &Arc<PhysicalPlan>,
    output_link_kinds: &HashMap<i64, LinkKind>,
) -> PipelineLinkKindCounts {
    let mut linked_outputs = HashSet::new();
    for (_parent_index, child_index) in collect_parent_child_relations(Arc::clone(physical_plan)) {
        linked_outputs.insert(child_index);
    }

    let mut counts = PipelineLinkKindCounts {
        mpsc_links: 0,
        broadcast_links: 0,
    };

    if !collect_leaf_indices(Arc::clone(physical_plan)).is_empty() {
        counts.broadcast_links += 1;
    }

    for plan_index in linked_outputs {
        match output_link_kinds
            .get(&plan_index)
            .copied()
            .unwrap_or(LinkKind::Broadcast)
        {
            LinkKind::Mpsc => counts.mpsc_links += 1,
            LinkKind::Broadcast => counts.broadcast_links += 1,
        }
    }

    counts
}

fn record_pipeline_link_kind_counts(stats: &ProcessorStats, counts: PipelineLinkKindCounts) {
    const MPSC_LINKS: MetricSpec = MetricSpec {
        id: "pipeline_mpsc_links",
        flat_name: "mpsc_links",
        kind: MetricKind::Gauge,
    };
    const BROADCAST_LINKS: MetricSpec = MetricSpec {
        id: "pipeline_broadcast_links",
        flat_name: "broadcast_links",
        kind: MetricKind::Gauge,
    };

    stats.register_gauge(MPSC_LINKS).set(counts.mpsc_links);
    stats
        .register_gauge(BROADCAST_LINKS)
        .set(counts.broadcast_links);
}

/// Build a mapping from plan index to plan name for all nodes in the PhysicalPlan tree
fn build_index_to_name_mapping(
    plan: &Arc<PhysicalPlan>,
    mapping: &mut std::collections::HashMap<i64, String>,
) {
    let plan_index = plan.get_plan_index();
    let plan_name = plan.get_plan_name();
    mapping.insert(plan_index, plan_name);

    // Recursively process children
    for child in plan.children() {
        build_index_to_name_mapping(child, mapping);
    }
}

/// Connect processors based on PhysicalPlan tree structure
///
/// This function connects:
/// - ControlSourceProcessor outputs to leaf node inputs
/// - Children outputs to parent inputs
fn connect_processors(
    physical_plan: Arc<PhysicalPlan>,
    processor_map: &mut ProcessorMap,
    control_source: &mut ControlSourceProcessor,
) -> Result<(), ProcessorError> {
    // Build index to name mapping for quick lookup
    let mut index_to_name_map: std::collections::HashMap<i64, String> =
        std::collections::HashMap::new();
    build_index_to_name_mapping(&physical_plan, &mut index_to_name_map);

    // 1. Connect ControlSourceProcessor to all leaf nodes
    let leaf_indices = collect_leaf_indices(Arc::clone(&physical_plan));
    for leaf_index in leaf_indices {
        if let Some(leaf_plan_name) = index_to_name_map.get(&leaf_index) {
            if let Some(processor) = processor_map.get_processor_mut(leaf_plan_name) {
                let receiver = control_source.subscribe_output().ok_or_else(|| {
                    ProcessorError::InvalidConfiguration("control source output unavailable".into())
                })?;
                processor.add_input(receiver);
                if let Some(control_rx) = control_source.subscribe_control_output() {
                    processor.add_control_input(control_rx);
                }
            }
        }
    }

    // 2. Connect children outputs to parent inputs
    let relations = collect_parent_child_relations(Arc::clone(&physical_plan));

    // Debug: Print connection relationships
    // println!("=== Processor Connection Relationships ===");
    // let mut relation_counts: std::collections::HashMap<i64, usize> = std::collections::HashMap::new();
    // for (parent_idx, child_idx) in &relations {
    //     *relation_counts.entry(*child_idx).or_insert(0) += 1;
    //     if let (Some(child_name), Some(parent_name)) = (index_to_name_map.get(child_idx), index_to_name_map.get(parent_idx)) {
    //         println!("  {} (index: {}) -> {} (index: {})", child_name, child_idx, parent_name, parent_idx);
    //     }
    // }
    // println!("Child processor subscription counts:");
    // for (child_idx, count) in relation_counts {
    //     if let Some(child_name) = index_to_name_map.get(&child_idx) {
    //         println!("  {} (index: {}): {} parent(s)", child_name, child_idx, count);
    //     }
    // }
    // println!("=========================================");

    for (parent_index, child_index) in relations {
        if let (Some(child_plan_name), Some(parent_plan_name)) = (
            index_to_name_map.get(&child_index),
            index_to_name_map.get(&parent_index),
        ) {
            // println!("Connecting {} -> {}", child_plan_name, parent_plan_name);

            let receiver = processor_map
                .get_processor(child_plan_name)
                .and_then(|proc| proc.subscribe_output())
                .ok_or_else(|| {
                    ProcessorError::InvalidConfiguration(format!(
                        "Processor {} has no broadcast output",
                        child_plan_name
                    ))
                })?;

            let control_receiver = processor_map
                .get_processor(child_plan_name)
                .and_then(|proc| proc.subscribe_control_output());
            if let Some(parent_processor) = processor_map.get_processor_mut(parent_plan_name) {
                parent_processor.add_input(receiver);
                if let Some(control_rx) = control_receiver {
                    parent_processor.add_control_input(control_rx);
                }
            }
        }
    }

    Ok(())
}

/// Create a complete processor pipeline from a PhysicalPlan tree.
///
/// The provided plan is expected to terminate in a `PhysicalDataSink` node that
/// carries the declarative sink configuration.
fn create_processor_pipeline_with_context(
    physical_plan: Arc<PhysicalPlan>,
    mut context: ProcessorBuilderContext,
) -> Result<ProcessorPipeline, ProcessorError> {
    if context.checkpoint_enabled && context.checkpoint_store.is_none() {
        return Err(ProcessorError::InvalidConfiguration(
            "checkpoint store is required when checkpointing is enabled".to_string(),
        ));
    }
    context.checkpoint_keys = build_checkpoint_keys(&physical_plan);
    context.output_link_kinds = classify_output_link_kinds(&physical_plan);
    let link_kind_counts = count_pipeline_link_kinds(&physical_plan, &context.output_link_kinds);
    let channel_capacities = context.channel_capacities;
    let mut control_source =
        ControlSourceProcessor::new_with_channel_capacities("control_source", channel_capacities);
    let (ingress_sender, ingress_receiver) = mpsc::channel(channel_capacities.control);
    control_source.set_ingress_input(ingress_receiver);
    let ack_manager = Arc::new(AckManager::default());
    let checkpoint_coordinator = CheckpointCoordinator::new(CheckpointTrigger::new(
        ingress_sender.clone(),
        Arc::clone(&ack_manager),
        control_source.control_signal_id_allocator(),
    ));

    let mut processor_map = ProcessorMap::new();
    build_processors_recursive(Arc::clone(&physical_plan), &mut processor_map, &context)?;

    connect_processors(
        Arc::clone(&physical_plan),
        &mut processor_map,
        &mut control_source,
    )?;

    // Set up pipeline output from ResultCollect processor if present
    let mut pipeline_output_receiver = None;
    let mut result_sink = None;

    // Get all processors first
    let mut middle_processors = processor_map.get_all_processors();
    let checkpoint_snapshot_collector = Arc::new(CheckpointSnapshotCollector::new());
    control_source
        .set_checkpoint_snapshot_collector(Some(Arc::clone(&checkpoint_snapshot_collector)));
    for processor in &mut middle_processors {
        processor
            .set_checkpoint_snapshot_collector(Some(Arc::clone(&checkpoint_snapshot_collector)));
    }

    // Extract ResultCollect processor (if any) to serve as pipeline output
    // In multi-sink scenarios, there should be only one top-level ResultCollect processor
    if let Some(pos) = middle_processors
        .iter()
        .position(|p| matches!(p, PlanProcessor::ResultCollect(_)))
    {
        if let PlanProcessor::ResultCollect(mut collector) = middle_processors.swap_remove(pos) {
            let (result_output_sender, pipeline_output_rx) =
                mpsc::channel(channel_capacities.control);
            collector.set_output(result_output_sender);
            collector.add_bus_hook(Arc::new(ErrorLoggingHook));
            collector.add_bus_hook(Arc::new(AckHook::new(Arc::clone(&ack_manager))));
            pipeline_output_receiver = Some(pipeline_output_rx);
            result_sink = Some(collector);
        }
    }

    let mut processor_stats = Vec::new();
    let mut seen_ids = HashSet::new();

    let control_id = control_source.id().to_string();
    if !seen_ids.insert(control_id.clone()) {
        return Err(ProcessorError::InvalidConfiguration(format!(
            "duplicate processor id: {control_id}"
        )));
    }
    let stats = Arc::new(ProcessorStats::new(
        context.flow_instance_id(),
        control_id.as_str(),
        "control_source",
    ));
    record_pipeline_link_kind_counts(stats.as_ref(), link_kind_counts);
    control_source.set_stats(Arc::clone(&stats));
    processor_stats.push(ProcessorStatsHandle {
        processor_id: control_id,
        stats,
    });

    for processor in &mut middle_processors {
        let id = processor.id().to_string();
        if !seen_ids.insert(id.clone()) {
            return Err(ProcessorError::InvalidConfiguration(format!(
                "duplicate processor id: {id}"
            )));
        }
        let stats = Arc::new(ProcessorStats::new(
            context.flow_instance_id(),
            id.as_str(),
            processor.kind(),
        ));
        processor.set_stats(Arc::clone(&stats));
        processor_stats.push(ProcessorStatsHandle {
            processor_id: id,
            stats,
        });
    }

    if let Some(collector) = &mut result_sink {
        collector
            .set_checkpoint_snapshot_collector(Some(Arc::clone(&checkpoint_snapshot_collector)));
        let id = collector.id().to_string();
        if !seen_ids.insert(id.clone()) {
            return Err(ProcessorError::InvalidConfiguration(format!(
                "duplicate processor id: {id}"
            )));
        }
        let stats = Arc::new(ProcessorStats::new(
            context.flow_instance_id(),
            id.as_str(),
            "result_collect",
        ));
        collector.set_stats(Arc::clone(&stats));
        processor_stats.push(ProcessorStatsHandle {
            processor_id: id,
            stats,
        });
    }

    let pipeline_id = Uuid::new_v4().to_string();
    for processor in &mut middle_processors {
        processor.set_pipeline_id(&pipeline_id);
    }

    Ok(ProcessorPipeline {
        ingress: ingress_sender,
        output: pipeline_output_receiver,
        control_source,
        middle_processors,
        result_sink,
        handles: Vec::new(),
        supervisor_abort_handles: Vec::new(),
        supervisor_shutdown: Arc::new(AtomicBool::new(false)),
        supervisor_handle: None,
        pipeline_id,
        flow_instance_id: Arc::clone(&context.flow_instance_id),
        ack_manager,
        checkpoint_coordinator,
        checkpoint_snapshot_collector,
        checkpoint_enabled: context.checkpoint_enabled,
        checkpoint_store: context.checkpoint_store,
        processor_stats,
        channel_capacities,
        spawner: context.spawner.clone(),
    })
}

pub(crate) fn create_processor_pipeline_for_shared_stream(
    physical_plan: Arc<PhysicalPlan>,
    options: SharedStreamPipelineOptions,
    spawner: TaskSpawner,
) -> Result<ProcessorPipeline, ProcessorError> {
    create_processor_pipeline_with_context(
        physical_plan,
        ProcessorBuilderContext {
            flow_instance_id: Arc::clone(&options.flow_instance_id),
            mqtt_clients: None,
            connector_registry: None,
            encoder_registry: None,
            decoder_registry: None,
            aggregate_registry: None,
            stateful_registry: None,
            shared_stream_registry: None,
            eventtime: None,
            merger_registry: Some(Arc::clone(&options.merger_registry)),
            shared_stream: Some(options),
            output_link_kinds: HashMap::new(),
            channel_capacities: ProcessorChannelCapacities::new(
                DEFAULT_DATA_CHANNEL_CAPACITY,
                DEFAULT_CONTROL_CHANNEL_CAPACITY,
            ),
            checkpoint_enabled: false,
            checkpoint_store: None,
            checkpoint_keys: HashMap::new(),
            spawner,
        },
    )
}

pub(crate) fn create_processor_pipeline(
    physical_plan: Arc<PhysicalPlan>,
    dependencies: ProcessorPipelineDependencies,
    options: ProcessorPipelineOptions,
) -> Result<ProcessorPipeline, ProcessorError> {
    create_processor_pipeline_with_context(
        physical_plan,
        ProcessorBuilderContext {
            flow_instance_id: dependencies.flow_instance_id,
            mqtt_clients: Some(dependencies.mqtt_clients),
            connector_registry: Some(dependencies.connector_registry),
            encoder_registry: Some(dependencies.encoder_registry),
            decoder_registry: Some(dependencies.decoder_registry),
            aggregate_registry: Some(dependencies.aggregate_registry),
            stateful_registry: Some(dependencies.stateful_registry),
            shared_stream_registry: Some(dependencies.shared_stream_registry),
            eventtime: dependencies.eventtime,
            merger_registry: Some(dependencies.merger_registry),
            shared_stream: None,
            output_link_kinds: HashMap::new(),
            channel_capacities: options.channel_capacities(),
            checkpoint_enabled: options.checkpoint_enabled,
            checkpoint_store: dependencies.checkpoint_store,
            checkpoint_keys: HashMap::new(),
            spawner: dependencies.spawner,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::StreamDecoderConfig;
    use crate::expr::ScalarExpr;
    use crate::planner::physical::{
        PhysicalDataSource, PhysicalDecoder, PhysicalProject, PhysicalProjectField,
        PhysicalSharedStream, PhysicalSharedStreamRequirement,
    };
    use datatypes::{ConcreteDatatype, Schema, Value};
    use sqlparser::ast::{Expr, Value as SqlValue};
    use std::sync::Arc;
    use std::sync::Mutex;
    use tokio::sync::oneshot;
    use tokio::time::Duration as TokioDuration;

    fn test_spawner() -> TaskSpawner {
        TaskSpawner::from_handle(tokio::runtime::Handle::current())
    }

    fn empty_test_pipeline(spawner: TaskSpawner) -> ProcessorPipeline {
        let channel_capacities = ProcessorChannelCapacities::new(4, 4);
        let mut control_source = ControlSourceProcessor::new_with_channel_capacities(
            "control_source",
            channel_capacities,
        );
        let (ingress, ingress_rx) = mpsc::channel(channel_capacities.control);
        control_source.set_ingress_input(ingress_rx);
        let ack_manager = Arc::new(AckManager::default());
        let checkpoint_coordinator = CheckpointCoordinator::new(CheckpointTrigger::new(
            ingress.clone(),
            Arc::clone(&ack_manager),
            control_source.control_signal_id_allocator(),
        ));
        ProcessorPipeline {
            ingress,
            output: None,
            control_source,
            middle_processors: Vec::new(),
            result_sink: None,
            handles: Vec::new(),
            supervisor_abort_handles: Vec::new(),
            supervisor_shutdown: Arc::new(AtomicBool::new(false)),
            supervisor_handle: None,
            pipeline_id: "test_pipe".to_string(),
            flow_instance_id: Arc::<str>::from("default"),
            ack_manager,
            checkpoint_coordinator,
            checkpoint_snapshot_collector: Arc::new(CheckpointSnapshotCollector::new()),
            checkpoint_enabled: false,
            checkpoint_store: None,
            processor_stats: Vec::new(),
            channel_capacities,
            spawner,
        }
    }

    async fn start_fake_processor(
        pipeline: &mut ProcessorPipeline,
        processor_id: &str,
        processor_kind: &'static str,
        handle: JoinHandle<Result<(), ProcessorError>>,
        allow_normal_completion: bool,
        task_exit_tx: mpsc::UnboundedSender<ProcessorTaskExit>,
    ) {
        pipeline
            .start_processor(
                processor_id.to_string(),
                processor_kind,
                ProcessorStart::ready(handle),
                allow_normal_completion,
                task_exit_tx,
            )
            .await
            .expect("start fake processor");
    }

    async fn run_supervisor_failure_case(
        processor_id: &str,
        processor_kind: &'static str,
        handle: JoinHandle<Result<(), ProcessorError>>,
    ) -> PipelineRuntimeFailure {
        let spawner = test_spawner();
        let mut pipeline = empty_test_pipeline(spawner.clone());
        let (task_exit_tx, task_exit_rx) = mpsc::unbounded_channel();
        start_fake_processor(
            &mut pipeline,
            processor_id,
            processor_kind,
            handle,
            false,
            task_exit_tx,
        )
        .await;
        let (failure_tx, failure_rx) = oneshot::channel();
        let failure_tx = Arc::new(Mutex::new(Some(failure_tx)));
        pipeline.spawn_supervisor(
            task_exit_rx,
            Arc::new(move |failure| {
                if let Some(tx) = failure_tx.lock().unwrap().take() {
                    let _ = tx.send(failure);
                }
            }),
        );
        let failure = tokio::time::timeout(TokioDuration::from_secs(2), failure_rx)
            .await
            .expect("timeout waiting for supervisor failure")
            .expect("failure sender dropped");
        pipeline.abort_started_processors().await;
        failure
    }

    #[tokio::test]
    async fn supervisor_reports_processor_err_as_failure() {
        let spawner = test_spawner();
        let handle = spawner.spawn(async {
            Err(ProcessorError::ProcessingError(
                "processor returned err".to_string(),
            ))
        });

        let failure = run_supervisor_failure_case("err_processor", "test_kind", handle).await;

        assert_eq!(failure.pipeline_id, "test_pipe");
        assert_eq!(failure.processor_id, "err_processor");
        assert_eq!(failure.processor_kind, "test_kind");
        assert!(failure.reason.contains("processor returned err"));
    }

    #[tokio::test]
    async fn supervisor_reports_processor_panic_as_failure() {
        let spawner = test_spawner();
        let handle: JoinHandle<Result<(), ProcessorError>> =
            spawner.spawn(async { panic!("processor panic") });

        let failure = run_supervisor_failure_case("panic_processor", "test_kind", handle).await;

        assert_eq!(failure.processor_id, "panic_processor");
        assert_eq!(failure.processor_kind, "test_kind");
        assert!(failure.reason.contains("Join error"));
    }

    #[tokio::test]
    async fn supervisor_reports_unexpected_ok_exit_as_failure() {
        let spawner = test_spawner();
        let handle = spawner.spawn(async { Ok(()) });

        let failure = run_supervisor_failure_case("ok_processor", "test_kind", handle).await;

        assert_eq!(failure.processor_id, "ok_processor");
        assert_eq!(failure.processor_kind, "test_kind");
        assert!(failure.reason.contains("unexpectedly with Ok"));
    }

    #[tokio::test]
    async fn supervisor_ignores_expected_ok_exit() {
        let spawner = test_spawner();
        let mut pipeline = empty_test_pipeline(spawner.clone());
        let handle = spawner.spawn(async { Ok(()) });
        let (task_exit_tx, task_exit_rx) = mpsc::unbounded_channel();
        start_fake_processor(
            &mut pipeline,
            "bounded_processor",
            "table_scan",
            handle,
            true,
            task_exit_tx,
        )
        .await;
        let (failure_tx, mut failure_rx) = mpsc::unbounded_channel();
        pipeline.spawn_supervisor(
            task_exit_rx,
            Arc::new(move |failure| {
                let _ = failure_tx.send(failure);
            }),
        );

        let result = tokio::time::timeout(TokioDuration::from_millis(100), failure_rx.recv()).await;

        if let Ok(Some(failure)) = result {
            panic!("expected completion should not report failure: {failure:?}");
        }
        pipeline.abort_started_processors().await;
    }

    #[tokio::test]
    async fn supervisor_ignores_task_exit_during_shutdown() {
        let spawner = test_spawner();
        let mut pipeline = empty_test_pipeline(spawner.clone());
        let (release_tx, release_rx) = oneshot::channel();
        let handle = spawner.spawn(async {
            let _ = release_rx.await;
            Ok(())
        });
        let (task_exit_tx, task_exit_rx) = mpsc::unbounded_channel();
        start_fake_processor(
            &mut pipeline,
            "shutdown_processor",
            "test_kind",
            handle,
            false,
            task_exit_tx,
        )
        .await;
        let (failure_tx, mut failure_rx) = mpsc::unbounded_channel();
        pipeline.spawn_supervisor(
            task_exit_rx,
            Arc::new(move |failure| {
                let _ = failure_tx.send(failure);
            }),
        );

        pipeline.supervisor_shutdown.store(true, Ordering::Release);
        release_tx.send(()).expect("release fake processor");
        let result = tokio::time::timeout(TokioDuration::from_millis(100), failure_rx.recv()).await;

        if let Ok(Some(failure)) = result {
            panic!("shutdown exit should not report failure: {failure:?}");
        }
        pipeline.abort_started_processors().await;
    }

    #[tokio::test]
    async fn graceful_close_aborts_runtime_when_final_checkpoint_fails() {
        struct TaskDropGuard(Arc<AtomicBool>);

        impl Drop for TaskDropGuard {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }

        let spawner = test_spawner();
        let mut pipeline = empty_test_pipeline(spawner.clone());
        pipeline.checkpoint_enabled = true;

        let task_dropped = Arc::new(AtomicBool::new(false));
        let task_dropped_on_abort = Arc::clone(&task_dropped);
        let (started_tx, started_rx) = oneshot::channel();
        let handle = spawner.spawn(async move {
            let _guard = TaskDropGuard(task_dropped_on_abort);
            let _ = started_tx.send(());
            std::future::pending::<Result<(), ProcessorError>>().await
        });
        let (task_exit_tx, task_exit_rx) = mpsc::unbounded_channel();
        start_fake_processor(
            &mut pipeline,
            "checkpoint_processor",
            "test_kind",
            handle,
            false,
            task_exit_tx,
        )
        .await;
        pipeline.spawn_supervisor(task_exit_rx, Arc::new(|_| {}));
        started_rx.await.expect("fake processor should start");

        let err = pipeline
            .graceful_close(TokioDuration::from_millis(10))
            .await
            .expect_err("final checkpoint should time out");

        assert!(matches!(err, ProcessorError::Timeout));
        assert!(task_dropped.load(Ordering::Acquire));
        assert!(pipeline.handles.is_empty());
        assert!(pipeline.supervisor_abort_handles.is_empty());
        assert!(pipeline.supervisor_handle.is_none());
    }

    #[test]
    fn test_create_processor_from_physical_project() {
        // Create a simple data source
        let schema = Arc::new(Schema::new(vec![]));
        let data_source = Arc::new(PhysicalPlan::DataSource(PhysicalDataSource::new(
            "test_source".to_string(),
            Arc::clone(&schema),
            None,
            0,
        )));
        let decoded_source = Arc::new(PhysicalPlan::Decoder(PhysicalDecoder::new(
            "test_source".to_string(),
            StreamDecoderConfig::json(),
            Arc::clone(&schema),
            None,
            None,
            vec![data_source],
            1,
        )));

        // Create a projection field
        let project_field = PhysicalProjectField::new(
            "projected_field".to_string(),
            Expr::Value(SqlValue::Number("42".to_string(), false)),
            ScalarExpr::Literal(
                Value::Int64(42),
                ConcreteDatatype::Int64(datatypes::Int64Type),
            ),
        );

        // Create a PhysicalProject
        let physical_project = Arc::new(PhysicalPlan::Project(PhysicalProject::with_single_child(
            vec![project_field],
            decoded_source,
            2,
        )));

        // Try to create a processor from the PhysicalProject
        let spawner = crate::runtime::TaskSpawner::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("runtime"),
        );
        let connector_registry =
            ConnectorRegistry::with_builtin_sinks(crate::connector::MemoryPubSubRegistry::new());
        let encoder_registry = EncoderRegistry::with_builtin_encoders();
        let decoder_registry = DecoderRegistry::with_builtin_decoders();
        let aggregate_registry = AggregateFunctionRegistry::with_builtins();
        let stateful_registry = StatefulFunctionRegistry::with_builtins();
        let context = ProcessorBuilderContext {
            flow_instance_id: Arc::<str>::from("default"),
            mqtt_clients: Some(MqttClientManager::new("default", spawner.clone())),
            connector_registry: Some(connector_registry),
            encoder_registry: Some(encoder_registry),
            decoder_registry: Some(decoder_registry),
            aggregate_registry: Some(aggregate_registry),
            stateful_registry: Some(stateful_registry),
            shared_stream_registry: None,
            eventtime: None,
            merger_registry: None,
            shared_stream: None,
            output_link_kinds: HashMap::new(),
            channel_capacities: ProcessorChannelCapacities::new(
                DEFAULT_DATA_CHANNEL_CAPACITY,
                DEFAULT_CONTROL_CHANNEL_CAPACITY,
            ),
            checkpoint_enabled: false,
            checkpoint_store: None,
            checkpoint_keys: HashMap::new(),
            spawner,
        };
        let result = create_processor_from_plan_node(&physical_project, &context)
            .expect("processor creation failed");

        let processor = result
            .processor
            .expect("expected processor for physical project node");
        assert_eq!(processor.id(), "PhysicalProject_2");
        tracing::info!(
            processor_id = %processor.id(),
            "PhysicalProject processor created successfully"
        );
    }

    #[test]
    fn sealed_linear_links_use_mpsc() {
        let schema = Arc::new(Schema::new(vec![]));
        let data_source = Arc::new(PhysicalPlan::DataSource(PhysicalDataSource::new(
            "test_source".to_string(),
            Arc::clone(&schema),
            None,
            0,
        )));
        let decoder = Arc::new(PhysicalPlan::Decoder(PhysicalDecoder::new(
            "test_source".to_string(),
            StreamDecoderConfig::json(),
            Arc::clone(&schema),
            None,
            None,
            vec![Arc::clone(&data_source)],
            1,
        )));
        let project = Arc::new(PhysicalPlan::Project(PhysicalProject::with_single_child(
            Vec::new(),
            Arc::clone(&decoder),
            2,
        )));

        let kinds = classify_output_link_kinds(&project);

        assert_eq!(kinds.get(&0), Some(&LinkKind::Mpsc));
        assert_eq!(kinds.get(&1), Some(&LinkKind::Mpsc));
        assert_eq!(kinds.get(&2), Some(&LinkKind::Broadcast));
    }

    #[test]
    fn shared_stream_output_stays_broadcast_even_with_one_downstream() {
        let schema = Arc::new(Schema::new(vec![]));
        let shared = Arc::new(PhysicalPlan::SharedStream(PhysicalSharedStream::new(
            "shared_source".to_string(),
            Arc::clone(&schema),
            PhysicalSharedStreamRequirement::new(Vec::new(), 0),
            StreamDecoderConfig::json(),
            None,
            10,
        )));
        let project = Arc::new(PhysicalPlan::Project(PhysicalProject::with_single_child(
            Vec::new(),
            Arc::clone(&shared),
            11,
        )));

        let kinds = classify_output_link_kinds(&project);

        assert_eq!(kinds.get(&10), Some(&LinkKind::Broadcast));
        assert_eq!(kinds.get(&11), Some(&LinkKind::Broadcast));
    }
}
