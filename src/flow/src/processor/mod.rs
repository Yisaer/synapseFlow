//! Stream processing operators
//!
//! New architecture with tokio mspc channels:
//! - Multi-input, multi-output processors
//! - ControlSourceProcessor as data flow starting point
//! - DataSourceProcessor for data generation
//! - ResultCollectProcessor as final destination
//! - All processors communicate via StreamData through tokio mspc channels

pub mod aggregation_processor;
pub mod base;
pub mod batch_processor;
pub mod control_source_processor;
pub mod datasource_processor;
pub mod decoder_processor;
pub mod encoder_processor;
pub mod filter_processor;
pub mod processor_builder;
pub mod project_processor;
pub mod result_collect_processor;
pub mod shared_stream_processor;
pub mod sink_processor;
pub mod sliding_window_processor;
pub mod state_window_processor;
pub mod stream_data;
pub mod streaming_aggregation_processor;
pub mod streaming_encoder_processor;
pub mod tumbling_window_processor;
pub mod watermark_processor;

pub use aggregation_processor::AggregationProcessor;
pub use base::{Processor, ProcessorError};
pub use batch_processor::BatchProcessor;
pub use control_source_processor::ControlSourceProcessor;
pub use datasource_processor::DataSourceProcessor;
pub use decoder_processor::DecoderProcessor;
pub use encoder_processor::EncoderProcessor;
pub use filter_processor::FilterProcessor;
pub use processor_builder::{create_processor_pipeline, ProcessorPipeline};
pub use project_processor::ProjectProcessor;
pub use result_collect_processor::ResultCollectProcessor;
pub use shared_stream_processor::SharedStreamProcessor;
pub use sink_processor::SinkProcessor;
pub use sliding_window_processor::SlidingWindowProcessor;
pub use state_window_processor::StateWindowProcessor;
pub use stream_data::{ControlSignal, StreamData, StreamError};
pub use streaming_aggregation_processor::{
    StreamingAggregationProcessor, StreamingCountAggregationProcessor,
    StreamingTumblingAggregationProcessor,
};
pub use streaming_encoder_processor::StreamingEncoderProcessor;
pub use tumbling_window_processor::TumblingWindowProcessor;
pub use watermark_processor::WatermarkProcessor;
