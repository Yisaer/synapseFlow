//! Stream processing operators
//! 
//! New architecture with tokio mspc channels:
//! - Multi-input, multi-output processors
//! - ControlSourceProcessor as data flow starting point
//! - DataSourceProcessor for data generation
//! - ResultSinkProcessor as final destination
//! - All processors communicate via StreamData through tokio mspc channels

pub mod stream_data;
pub mod stream_processor;
pub mod control_source_processor;
pub mod datasource_processor;
pub mod result_sink_processor;
pub mod processor_builder;

pub use stream_data::{StreamData, StreamError, ControlSignal};
pub use stream_processor::{StreamProcessor, ProcessorHandle, utils};
pub use control_source_processor::ControlSourceProcessor;
pub use datasource_processor::DataSourceProcessor;
pub use result_sink_processor::ResultSinkProcessor;
pub use processor_builder::{SimpleChainBuilder, SimpleProcessorChain};