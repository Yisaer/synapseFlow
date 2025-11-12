//! Stream processing operators
//! 
//! New architecture with tokio mspc channels:
//! - Multi-input, multi-output processors
//! - ControlSourceProcessor as data flow starting point
//! - DataSourceProcessor for data generation
//! - ResultSinkProcessor as final destination
//! - All processors communicate via StreamData through tokio mspc channels

pub mod stream_data;
pub mod processor;
pub mod control_source_processor;
pub mod datasource_processor;
pub mod result_sink_processor;
pub mod processor_builder;

pub use stream_data::{StreamData, StreamError, ControlSignal};
pub use processor::{Processor, ProcessorError};
pub use control_source_processor::ControlSourceProcessor;
pub use datasource_processor::DataSourceProcessor;
pub use result_sink_processor::ResultSinkProcessor;
pub use processor_builder::{
    connect_control_source_to_leaf_nodes, 
    find_leaf_nodes, 
    create_processor_from_physical_plan,
    create_processor_pipeline,
    ProcessorPipeline,
};