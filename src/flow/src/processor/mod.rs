//! Stream processing operators
//! 
//! Each processor corresponds to a PhysicalPlan and handles data flow through tokio channels.
//! Processors receive data from upstream operators, process it, and broadcast to downstream operators.
//! 
//! Design inspired by rstream's Executor pattern:
//! - Each processor runs in its own tokio task
//! - Uses tokio::select! for handling multiple input sources and control signals
//! - Supports graceful shutdown via stop channels
//! - Handles backpressure and flow control

pub mod stream_data;
pub mod datasource_processor;
pub mod filter_processor;
pub mod project_processor;
pub mod processor_view;
pub mod stream_processor;
pub mod pipeline_builder;
pub mod processor_builder;
pub mod control_source_processor;
pub mod result_sink_processor;

pub use stream_data::{StreamData, StreamError, ControlSignal};
pub use datasource_processor::DataSourceProcessor;
pub use filter_processor::FilterProcessor;
pub use project_processor::ProjectProcessor;
pub use processor_view::{ProcessorView, ProcessorHandle};
pub use stream_processor::{StreamProcessor, utils};
pub use pipeline_builder::{build_processor_pipeline, build_connected_pipeline, build_connected_pipeline_with_external_io,
                          build_pipeline_with_external_control, build_pipeline_with_external_io, execute_pipeline,
                          ConnectedProcessorNode, ConnectedExternalPipeline, execute_connected_pipeline};
pub use processor_builder::{ProcessorChainBuilder, ProcessorChainResult};
pub use control_source_processor::ControlSourceProcessor;
pub use result_sink_processor::ResultSinkProcessor;