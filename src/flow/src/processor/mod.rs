//! Stream processing operators
//!
//! New architecture with tokio mspc channels:
//! - Multi-input, multi-output processors
//! - ControlSourceProcessor as data flow starting point
//! - DataSourceProcessor for data generation
//! - ResultCollectProcessor as final destination
//! - All processors communicate via StreamData through tokio mspc channels

pub mod aggregation_processor;
pub mod barrier;
pub mod barrier_processor;
pub mod base;
pub mod batch_processor;
pub mod collection_layout_normalize_processor;
pub mod compute_processor;
pub mod control_source_processor;
pub mod datasource_processor;
pub mod decoder_processor;
pub mod empty_suppress_processor;
pub mod eos_window_processor;
pub mod eventtime;
pub mod filter_processor;
pub mod memory_collection_materialize_processor;
pub mod order_processor;
pub(crate) mod output_row_accessor;
pub(crate) mod pipeline_state_runtime;
pub mod processor_builder;
pub mod processor_state;
pub mod project_processor;
pub mod result_collect_processor;
pub mod row_diff_processor;
pub mod sampler_processor;
pub mod shared_stream_processor;
pub mod sink_compress_processor;
pub mod sink_encoder_processor;
pub mod sink_encrypt_processor;
pub mod sink_processor;
pub mod sliding_window_processor;
pub(crate) mod sliding_window_runtime;
pub mod source_change_gate_processor;
pub mod state_window_processor;
pub mod stateful_function_processor;
pub mod stats;
pub mod stream_data;
pub mod streaming_aggregation_processor;
pub mod table_scan_processor;
pub mod tumbling_window_processor;
pub mod watermark_processor;
pub(crate) mod window_metadata;
pub(crate) mod window_partition;

pub use aggregation_processor::AggregationProcessor;
pub use barrier_processor::BarrierProcessor;
pub use base::ProcessorError;
pub(crate) use base::{Processor, ProcessorStart};
pub use batch_processor::BatchProcessor;
pub use collection_layout_normalize_processor::CollectionLayoutNormalizeProcessor;
pub use compute_processor::ComputeProcessor;
pub use control_source_processor::{ControlSourceProcessor, Ingress, IngressTarget};
pub use datasource_processor::DataSourceProcessor;
pub use decoder_processor::DecoderProcessor;
pub use empty_suppress_processor::EmptySuppressProcessor;
pub use eos_window_processor::EosWindowProcessor;
pub use eventtime::EventtimePipelineContext;
pub use filter_processor::FilterProcessor;
pub use memory_collection_materialize_processor::MemoryCollectionMaterializeProcessor;
pub use order_processor::OrderProcessor;
pub use processor_builder::ProcessorPipeline;
pub(crate) use processor_builder::{
    create_processor_pipeline, ProcessorPipelineDependencies, ProcessorPipelineOptions,
};
pub use processor_state::ProcessorState;
pub use project_processor::ProjectProcessor;
pub use result_collect_processor::ResultCollectProcessor;
pub use row_diff_processor::RowDiffProcessor;
pub use sampler_processor::{SamplerConfig, SamplerProcessor, SamplingStrategy};
pub use shared_stream_processor::SharedStreamProcessor;
pub use sink_compress_processor::SinkCompressProcessor;
pub use sink_encoder_processor::SinkEncoderProcessor;
pub use sink_encrypt_processor::SinkEncryptProcessor;
pub use sink_processor::SinkProcessor;
pub use sliding_window_processor::SlidingWindowProcessor;
pub use source_change_gate_processor::SourceChangeGateProcessor;
pub use state_window_processor::StateWindowProcessor;
pub use stateful_function_processor::StatefulFunctionProcessor;
pub use stats::{
    CounterHandle, GaugeHandle, MetricKind, MetricSpec, ProcessorStats, ProcessorStatsEntry,
    ProcessorStatsHandle, ProcessorStatsSnapshot,
};
pub use stream_data::{
    BarrierControlSignal, BarrierControlSignalKind, ControlSignal, EncodedDeliveryFlags,
    InstantControlSignal, StreamData, StreamError,
};
pub use streaming_aggregation_processor::{
    StreamingAggregationProcessor, StreamingCountAggregationProcessor,
    StreamingTumblingAggregationProcessor,
};
pub use table_scan_processor::TableScanProcessor;
pub use tumbling_window_processor::TumblingWindowProcessor;
pub use watermark_processor::WatermarkProcessor;
