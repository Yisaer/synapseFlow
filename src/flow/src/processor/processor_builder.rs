//! Processor builder - creates and connects processors from PhysicalPlan
//!
//! This module provides utilities to build processor pipelines from PhysicalPlan,
//! connecting ControlSourceProcessor outputs to leaf nodes (nodes without children).

use crate::aggregation::AggregateFunctionRegistry;
use crate::codec::{DecoderRegistry, EncoderRegistry, MergerRegistry, RecordDecoder};
use crate::connector::{ConnectorRegistry, MqttClientManager};
use crate::planner::physical::PhysicalPlan;
use crate::processor::base::{
    normalize_channel_capacity, ProcessorChannelCapacities, DEFAULT_CONTROL_CHANNEL_CAPACITY,
    DEFAULT_DATA_CHANNEL_CAPACITY,
};
use crate::processor::decoder_processor::EventtimeDecodeConfig;
use crate::processor::result_collect_processor::{AckHook, AckManager, ErrorLoggingHook};
use crate::processor::EventtimePipelineContext;
use crate::processor::{
    AggregationProcessor, BarrierControlSignalKind, BarrierProcessor, BatchProcessor,
    CollectionLayoutNormalizeProcessor, ComputeProcessor, ControlSignal, ControlSourceProcessor,
    DataSourceProcessor, DecoderProcessor, EmptySuppressProcessor, FilterProcessor, Ingress,
    InstantControlSignal, MemoryCollectionMaterializeProcessor, OrderProcessor, Processor,
    ProcessorError, ProjectProcessor, ResultCollectProcessor, RowDiffProcessor, SamplerProcessor,
    SharedStreamProcessor, SinkEncoderProcessor, SinkProcessor, SlidingWindowProcessor,
    SourceChangeGateProcessor, StateWindowProcessor, StatefulFunctionProcessor, StreamData,
    StreamingAggregationProcessor, TumblingWindowProcessor, WatermarkProcessor,
};
use crate::processor::{ProcessorStats, ProcessorStatsHandle};
use crate::runtime::TaskSpawner;
use crate::shared_stream::{AppliedDecodeState, SharedStreamRegistry};
use crate::stateful::StatefulFunctionRegistry;
use crate::PipelineRegistries;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use uuid::Uuid;

#[derive(Clone)]
pub(crate) struct SharedStreamPipelineOptions {
    pub stream_name: String,
    pub flow_instance_id: Arc<str>,
    pub decoder: Arc<dyn RecordDecoder>,
    pub applied_decode_state: Arc<parking_lot::RwLock<AppliedDecodeState>>,
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
    /// DecoderProcessor created from PhysicalDecoder
    Decoder(DecoderProcessor),
    /// CollectionLayoutNormalizeProcessor inserted for collection sources that must preserve full schema.
    CollectionLayoutNormalize(CollectionLayoutNormalizeProcessor),
    /// MemoryCollectionMaterializeProcessor inserted before memory collection sinks.
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
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ProcessorPipelineOptions {
    pub(crate) data_channel_capacity: usize,
}

impl Default for ProcessorPipelineOptions {
    fn default() -> Self {
        Self {
            data_channel_capacity: DEFAULT_DATA_CHANNEL_CAPACITY,
        }
    }
}

impl ProcessorPipelineOptions {
    pub(crate) fn with_data_channel_capacity(mut self, capacity: usize) -> Self {
        self.data_channel_capacity = normalize_channel_capacity(capacity);
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
    spawner: TaskSpawner,
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
}

impl PlanProcessor {
    /// Get the processor ID
    pub fn id(&self) -> &str {
        match self {
            PlanProcessor::Aggregation(p) => p.id(),
            PlanProcessor::DataSource(p) => p.id(),
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
            PlanProcessor::StreamingAggregation(p) => p.id(),
            PlanProcessor::Watermark(p) => p.id(),
            PlanProcessor::TumblingWindow(p) => p.id(),
            PlanProcessor::SlidingWindow(p) => p.id(),
            PlanProcessor::StateWindow(p) => p.id(),
            PlanProcessor::Sink(p) => p.id(),
            PlanProcessor::ResultCollect(p) => p.id(),
            PlanProcessor::Barrier(p) => p.id(),
            PlanProcessor::Sampler(p) => p.id(),
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            PlanProcessor::Aggregation(_) => "aggregation",
            PlanProcessor::DataSource(_) => "datasource",
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
            PlanProcessor::StreamingAggregation(_) => "streaming_aggregation",
            PlanProcessor::Watermark(_) => "watermark",
            PlanProcessor::TumblingWindow(_) => "tumbling_window",
            PlanProcessor::SlidingWindow(_) => "sliding_window",
            PlanProcessor::StateWindow(_) => "state_window",
            PlanProcessor::Sink(_) => "sink",
            PlanProcessor::ResultCollect(_) => "result_collect",
            PlanProcessor::Barrier(_) => "barrier",
            PlanProcessor::Sampler(_) => "sampler",
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
            PlanProcessor::StreamingAggregation(p) => p.set_stats(stats),
            PlanProcessor::Watermark(p) => p.set_stats(stats),
            PlanProcessor::TumblingWindow(p) => p.set_stats(stats),
            PlanProcessor::SlidingWindow(p) => p.set_stats(stats),
            PlanProcessor::StateWindow(p) => p.set_stats(stats),
            PlanProcessor::Sink(p) => p.set_stats(stats),
            PlanProcessor::ResultCollect(p) => p.set_stats(stats),
            PlanProcessor::Barrier(p) => p.set_stats(stats),
            PlanProcessor::Sampler(p) => p.set_stats(stats),
        }
    }

    /// Start the processor
    pub fn start(
        &mut self,
        spawner: &TaskSpawner,
    ) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        match self {
            PlanProcessor::Aggregation(p) => p.start(spawner),
            PlanProcessor::DataSource(p) => p.start(spawner),
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
            PlanProcessor::StreamingAggregation(p) => p.start(spawner),
            PlanProcessor::Watermark(p) => p.start(spawner),
            PlanProcessor::TumblingWindow(p) => p.start(spawner),
            PlanProcessor::SlidingWindow(p) => p.start(spawner),
            PlanProcessor::StateWindow(p) => p.start(spawner),
            PlanProcessor::Sink(p) => p.start(spawner),
            PlanProcessor::ResultCollect(p) => p.start(spawner),
            PlanProcessor::Barrier(p) => p.start(spawner),
            PlanProcessor::Sampler(p) => p.start(spawner),
        }
    }

    /// Subscribe to the processor's output stream
    pub fn subscribe_output(&self) -> Option<broadcast::Receiver<crate::processor::StreamData>> {
        match self {
            PlanProcessor::Aggregation(p) => p.subscribe_output(),
            PlanProcessor::DataSource(p) => p.subscribe_output(),
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
            PlanProcessor::StreamingAggregation(p) => p.subscribe_output(),
            PlanProcessor::Watermark(p) => p.subscribe_output(),
            PlanProcessor::TumblingWindow(p) => p.subscribe_output(),
            PlanProcessor::SlidingWindow(p) => p.subscribe_output(),
            PlanProcessor::StateWindow(p) => p.subscribe_output(),
            PlanProcessor::Sink(p) => p.subscribe_output(),
            PlanProcessor::ResultCollect(p) => p.subscribe_output(),
            PlanProcessor::Barrier(p) => p.subscribe_output(),
            PlanProcessor::Sampler(p) => p.subscribe_output(),
        }
    }

    /// Subscribe to the processor's control output stream
    pub fn subscribe_control_output(&self) -> Option<broadcast::Receiver<ControlSignal>> {
        match self {
            PlanProcessor::Aggregation(p) => p.subscribe_control_output(),
            PlanProcessor::DataSource(p) => p.subscribe_control_output(),
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
            PlanProcessor::StreamingAggregation(p) => p.subscribe_control_output(),
            PlanProcessor::Watermark(p) => p.subscribe_control_output(),
            PlanProcessor::TumblingWindow(p) => p.subscribe_control_output(),
            PlanProcessor::SlidingWindow(p) => p.subscribe_control_output(),
            PlanProcessor::StateWindow(p) => p.subscribe_control_output(),
            PlanProcessor::Sink(p) => p.subscribe_control_output(),
            PlanProcessor::ResultCollect(p) => p.subscribe_control_output(),
            PlanProcessor::Barrier(p) => p.subscribe_control_output(),
            PlanProcessor::Sampler(p) => p.subscribe_control_output(),
        }
    }

    /// Add an input channel
    pub fn add_input(&mut self, receiver: broadcast::Receiver<crate::processor::StreamData>) {
        match self {
            PlanProcessor::Aggregation(p) => p.add_input(receiver),
            PlanProcessor::DataSource(p) => p.add_input(receiver),
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
            PlanProcessor::StreamingAggregation(p) => p.add_input(receiver),
            PlanProcessor::Watermark(p) => p.add_input(receiver),
            PlanProcessor::TumblingWindow(p) => p.add_input(receiver),
            PlanProcessor::SlidingWindow(p) => p.add_input(receiver),
            PlanProcessor::StateWindow(p) => p.add_input(receiver),
            PlanProcessor::Sink(p) => p.add_input(receiver),
            PlanProcessor::ResultCollect(p) => p.add_input(receiver),
            PlanProcessor::Barrier(p) => p.add_input(receiver),
            PlanProcessor::Sampler(p) => p.add_input(receiver),
        }
    }

    /// Add a control input channel
    pub fn add_control_input(&mut self, receiver: broadcast::Receiver<ControlSignal>) {
        match self {
            PlanProcessor::Aggregation(p) => p.add_control_input(receiver),
            PlanProcessor::DataSource(p) => p.add_control_input(receiver),
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
            PlanProcessor::StreamingAggregation(p) => p.add_control_input(receiver),
            PlanProcessor::Watermark(p) => p.add_control_input(receiver),
            PlanProcessor::TumblingWindow(p) => p.add_control_input(receiver),
            PlanProcessor::SlidingWindow(p) => p.add_control_input(receiver),
            PlanProcessor::StateWindow(p) => p.add_control_input(receiver),
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
    /// Logical pipeline identifier used for diagnostics/subscriptions
    pipeline_id: String,
    flow_instance_id: Arc<str>,
    /// Allows callers to wait for specific control signals to reach the tail.
    ack_manager: Arc<AckManager>,
    /// Processor-local stats handles (one per processor instance).
    processor_stats: Vec<ProcessorStatsHandle>,
    channel_capacities: ProcessorChannelCapacities,
    spawner: TaskSpawner,
}

impl ProcessorPipeline {
    fn wrap_processor_handle(
        spawner: &TaskSpawner,
        flow_instance_id: Arc<str>,
        pipeline_id: String,
        processor_id: String,
        processor_kind: &'static str,
        handle: JoinHandle<Result<(), ProcessorError>>,
    ) -> JoinHandle<Result<(), ProcessorError>> {
        spawner.spawn(async move {
            match handle.await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(err)) => {
                    tracing::error!(
                        flow_instance_id = %flow_instance_id,
                        pipeline_id = %pipeline_id,
                        processor_id = %processor_id,
                        processor_kind,
                        error = %err,
                        "processor task failed"
                    );
                    Err(err)
                }
                Err(join_err) => {
                    tracing::error!(
                        flow_instance_id = %flow_instance_id,
                        pipeline_id = %pipeline_id,
                        processor_id = %processor_id,
                        processor_kind,
                        error = %join_err,
                        "processor task panicked"
                    );
                    Err(ProcessorError::ProcessingError(format!(
                        "Join error: {}",
                        join_err
                    )))
                }
            }
        })
    }

    /// Start all processors in the pipeline. Subsequent calls are no-ops.
    pub fn start(&mut self) {
        if !self.handles.is_empty() {
            return;
        }
        let flow_instance_id = Arc::clone(&self.flow_instance_id);
        let pipeline_id = self.pipeline_id.clone();
        // Start from downstream to upstream so that consumers are ready before producers.
        if let Some(result_sink) = &mut self.result_sink {
            let processor_id = result_sink.id().to_string();
            let handle = result_sink.start(&self.spawner);
            self.handles.push(Self::wrap_processor_handle(
                &self.spawner,
                Arc::clone(&flow_instance_id),
                pipeline_id.clone(),
                processor_id,
                "result_collect",
                handle,
            ));
        }
        let len = self.middle_processors.len();
        for idx in (0..len).rev() {
            if !matches!(self.middle_processors[idx], PlanProcessor::DataSource(_)) {
                let processor_id = self.middle_processors[idx].id().to_string();
                let processor_kind = self.middle_processors[idx].kind();
                let handle = self.middle_processors[idx].start(&self.spawner);
                self.handles.push(Self::wrap_processor_handle(
                    &self.spawner,
                    Arc::clone(&flow_instance_id),
                    pipeline_id.clone(),
                    processor_id,
                    processor_kind,
                    handle,
                ));
            }
        }
        for idx in (0..len).rev() {
            if matches!(self.middle_processors[idx], PlanProcessor::DataSource(_)) {
                let processor_id = self.middle_processors[idx].id().to_string();
                let processor_kind = self.middle_processors[idx].kind();
                let handle = self.middle_processors[idx].start(&self.spawner);
                self.handles.push(Self::wrap_processor_handle(
                    &self.spawner,
                    Arc::clone(&flow_instance_id),
                    pipeline_id.clone(),
                    processor_id,
                    processor_kind,
                    handle,
                ));
            }
        }
        let handle = self.control_source.start(&self.spawner);
        self.handles.push(Self::wrap_processor_handle(
            &self.spawner,
            flow_instance_id,
            pipeline_id,
            self.control_source.id().to_string(),
            "control_source",
            handle,
        ));
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

    /// Gracefully close the pipeline by sending StreamEnd via the data channel.
    pub async fn graceful_close(
        &mut self,
        timeout_duration: std::time::Duration,
    ) -> Result<(), ProcessorError> {
        let _ = self
            .send_barrier_via_data_with_ack(
                BarrierControlSignalKind::StreamGracefulEnd,
                timeout_duration,
            )
            .await?;
        self.replace_ingress_sender();
        self.await_all_handles().await
    }

    /// Quickly close the pipeline by delivering StreamQuickEnd to the control channel.
    pub async fn quick_close(
        &mut self,
        timeout_duration: std::time::Duration,
    ) -> Result<(), ProcessorError> {
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
    let channel_capacities = context.channel_capacities;
    let processor_id = context
        .shared_stream()
        .map(|opts| format!("shared:{}/{}", opts.stream_name, plan_name))
        .unwrap_or_else(|| plan_name.clone());
    match plan.as_ref() {
        PhysicalPlan::DataSource(ds) => {
            let schema = ds.schema();
            let processor = DataSourceProcessor::with_custom_id_and_channel_capacities(
                None, // plan_index is no longer needed as we use plan_name for ID
                processor_id.clone(),
                ds.source_name().to_string(),
                schema,
                channel_capacities,
            );
            Ok(ProcessorBuildOutput::with_processor(
                PlanProcessor::DataSource(processor),
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
        PhysicalPlan::SinkEncoder(encoder) => {
            let encoder_impl = context
                .encoder_registry()?
                .instantiate(&encoder.encoder)
                .map_err(|err| ProcessorError::InvalidConfiguration(err.to_string()))?;
            let encoder_impl = attach_encoder_output_schema(plan, encoder_impl)?;
            let encoder_impl = if let Some(spec) = encoder.by_index_projection.as_ref() {
                if !encoder_impl.supports_index_lazy_materialization() {
                    return Err(ProcessorError::InvalidConfiguration(format!(
                        "encoder kind `{}` does not support index lazy materialization",
                        encoder.encoder.kind_str()
                    )));
                }
                encoder_impl
                    .with_by_index_projection(Arc::clone(spec))
                    .map_err(|err| ProcessorError::InvalidConfiguration(err.to_string()))?
            } else {
                encoder_impl
            };
            SinkEncoderProcessor::validate_batch_config(
                encoder.common.batch_count,
                encoder.common.batch_duration,
            )?;
            let processor = SinkEncoderProcessor::new_with_channel_capacities(
                processor_id.clone(),
                encoder_impl,
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
            let processor = BatchProcessor::new_with_channel_capacities(
                processor_id.clone(),
                Some(count_window.count as usize),
                None,
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
        PhysicalPlan::DataSink(sink_plan) | PhysicalPlan::SinkConnector(sink_plan) => {
            let processor_id = format!("{}_{}", processor_id, sink_plan.connector.sink_id);
            let mut processor = SinkProcessor::new_with_channel_capacities(
                processor_id.clone(),
                channel_capacities,
            );
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
            Ok(ProcessorBuildOutput::with_processor(PlanProcessor::Sink(
                processor,
            )))
        }
        PhysicalPlan::ResultCollect(_result_collect) => {
            let processor = ResultCollectProcessor::new(processor_id.clone());
            Ok(ProcessorBuildOutput::with_processor(
                PlanProcessor::ResultCollect(processor),
            ))
        }
        PhysicalPlan::Barrier(barrier) => {
            let expected_upstreams = barrier.base.children.len();
            let processor = BarrierProcessor::new_with_channel_capacities(
                processor_id.clone(),
                expected_upstreams,
                channel_capacities,
            );
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
            if let crate::processor::SamplingStrategy::Packer { .. } = &sampler.strategy {
                let registry = context.merger_registry()?;
                processor.set_merger_registry(registry);
            } else if let Ok(registry) = context.merger_registry() {
                processor.set_merger_registry(registry);
            }
            Ok(ProcessorBuildOutput::with_processor(
                PlanProcessor::Sampler(processor),
            ))
        }
    }
}

fn attach_encoder_output_schema(
    plan: &Arc<PhysicalPlan>,
    encoder_impl: Arc<dyn crate::codec::encoder::CollectionEncoder>,
) -> Result<Arc<dyn crate::codec::encoder::CollectionEncoder>, ProcessorError> {
    let input_child = plan.children().first().ok_or_else(|| {
        ProcessorError::InvalidConfiguration(
            "encoder processor requires exactly one input child".to_string(),
        )
    })?;
    let output_schema = input_child.output_schema().map_err(|err| {
        ProcessorError::InvalidConfiguration(format!(
            "encoder processor failed to resolve output schema: {err}"
        ))
    })?;
    encoder_impl
        .with_output_schema(Arc::new(output_schema))
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
    context: ProcessorBuilderContext,
) -> Result<ProcessorPipeline, ProcessorError> {
    let channel_capacities = context.channel_capacities;
    let mut control_source =
        ControlSourceProcessor::new_with_channel_capacities("control_source", channel_capacities);
    let (ingress_sender, ingress_receiver) = mpsc::channel(channel_capacities.control);
    control_source.set_ingress_input(ingress_receiver);
    let ack_manager = Arc::new(AckManager::default());

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
        pipeline_id,
        flow_instance_id: Arc::clone(&context.flow_instance_id),
        ack_manager,
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
            merger_registry: None,
            shared_stream: Some(options),
            channel_capacities: ProcessorChannelCapacities::new(
                DEFAULT_DATA_CHANNEL_CAPACITY,
                DEFAULT_CONTROL_CHANNEL_CAPACITY,
            ),
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
            channel_capacities: options.channel_capacities(),
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
    };
    use datatypes::{ConcreteDatatype, Schema, Value};
    use sqlparser::ast::{Expr, Value as SqlValue};
    use std::sync::Arc;

    #[test]
    fn test_create_processor_from_physical_project() {
        // Create a simple data source
        let schema = Arc::new(Schema::new(vec![]));
        let data_source = Arc::new(PhysicalPlan::DataSource(PhysicalDataSource::new(
            "test_source".to_string(),
            None,
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
            channel_capacities: ProcessorChannelCapacities::new(
                DEFAULT_DATA_CHANNEL_CAPACITY,
                DEFAULT_CONTROL_CHANNEL_CAPACITY,
            ),
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
}
