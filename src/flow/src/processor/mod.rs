//! Stream processing operators
//!
//! New architecture with tokio mspc channels:
//! - Multi-input, multi-output processors
//! - ControlSourceProcessor as data flow starting point
//! - DataSourceProcessor for data generation
//! - ResultCollectProcessor as final destination
//! - All processors communicate via StreamData through tokio mspc channels

pub mod base;
pub mod control_source_processor;
pub mod datasource_processor;
pub mod filter_processor;
pub mod processor_builder;
pub mod project_processor;
pub mod result_collect_processor;
pub mod shared_stream_processor;
pub mod sink_processor;
pub mod stream_data;

pub use base::{Processor, ProcessorError};
pub use control_source_processor::ControlSourceProcessor;
pub use datasource_processor::DataSourceProcessor;
pub use filter_processor::FilterProcessor;
pub use processor_builder::{create_processor_pipeline, ProcessorPipeline};
pub use project_processor::ProjectProcessor;
pub use result_collect_processor::ResultCollectProcessor;
pub use shared_stream_processor::SharedStreamProcessor;
pub use sink_processor::SinkProcessor;
pub use stream_data::{ControlSignal, StreamData, StreamError};
