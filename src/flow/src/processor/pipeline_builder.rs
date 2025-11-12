//! Stream processing pipeline builder
//! 
//! Builds a pipeline of StreamProcessors from a PhysicalPlan tree.
//! Each processor runs in its own tokio task and communicates via broadcast channels.
//! 
//! Architecture: Single input, multiple output design
//! - Each processor receives data from upstream via input_receiver
//! - Processors broadcast results to multiple output channels
//! - Proper channel connection between upstream and downstream processors
//! 
//! Now uses StreamData::Control signals for all control operations,
//! eliminating the need for separate stop channels.

use std::sync::Arc;
use crate::planner::physical::{PhysicalPlan, PhysicalDataSource, PhysicalFilter, PhysicalProject};
use crate::processor::{DataSourceProcessor, FilterProcessor, ProjectProcessor, ProcessorView, StreamData, stream_processor::StreamProcessor};
use crate::processor::control_source_processor::ControlSourceProcessor;
use crate::processor::result_sink_processor::ResultSinkProcessor;
use crate::processor::stream_processor::utils;
use tokio::sync::broadcast;

/// Represents a processor node in the execution pipeline with proper connections
pub struct ConnectedProcessorNode {
    /// The processor instance
    pub processor: Arc<dyn crate::processor::StreamProcessor>,
    /// View for controlling and monitoring the processor
    pub processor_view: ProcessorView,
    /// Input receiver - connected to upstream output
    pub input_receiver: broadcast::Receiver<StreamData>,
}

impl std::fmt::Debug for ConnectedProcessorNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectedProcessorNode")
            .field("processor", &"<dyn StreamProcessor>")
            .field("processor_view", &self.processor_view)
            .finish()
    }
}

/// Build a stream processing pipeline from a physical plan with proper connections
/// 
/// This function walks through the physical plan tree and creates corresponding processor nodes.
/// Each processor is properly connected to its upstream via broadcast channels.
pub fn build_connected_pipeline(
    physical_plan: Arc<dyn PhysicalPlan>,
) -> Result<ConnectedProcessorNode, String> {
    // Build the pipeline recursively with proper connections
    build_connected_processor_node(physical_plan, None)
}

/// Build a processor node and its subtree with proper upstream connections
/// 
/// # Arguments
/// * `physical_plan` - The physical plan to build
/// * `upstream_output` - Optional upstream output receiver (None for source processors)
fn build_connected_processor_node(
    physical_plan: Arc<dyn PhysicalPlan>,
    upstream_output: Option<broadcast::Receiver<StreamData>>,
) -> Result<ConnectedProcessorNode, String> {
    // Use downcast_ref for type-safe matching of concrete plan types
    if let Some(data_source) = physical_plan.as_any().downcast_ref::<PhysicalDataSource>() {
        build_connected_data_source_processor(data_source, physical_plan.clone(), upstream_output)
    } else if let Some(filter) = physical_plan.as_any().downcast_ref::<PhysicalFilter>() {
        build_connected_filter_processor(filter, physical_plan.clone(), upstream_output)
    } else if let Some(project) = physical_plan.as_any().downcast_ref::<PhysicalProject>() {
        build_connected_project_processor(project, physical_plan.clone(), upstream_output)
    } else {
        Err(format!(
            "Unsupported physical plan type: {:?}", 
            std::any::type_name_of_val(physical_plan.as_ref())
        ))
    }
}

/// Build a connected DataSource processor
/// 
/// DataSource processors don't have upstream inputs, so they create their own input channel
fn build_connected_data_source_processor(
    _data_source: &PhysicalDataSource,
    physical_plan: Arc<dyn PhysicalPlan>,
    _upstream_output: Option<broadcast::Receiver<StreamData>>,
) -> Result<ConnectedProcessorNode, String> {
    let downstream_count = 1; // Default to 1 downstream
    
    // DataSource creates its own input channel (mainly for control signals)
    let (input_sender, input_receiver) = utils::create_input_channel();
    
    let processor = Arc::new(DataSourceProcessor::new(
        physical_plan,
        downstream_count,
    ));
    
    let processor_view = processor.start(input_receiver.resubscribe());
    
    Ok(ConnectedProcessorNode {
        processor,
        processor_view,
        input_receiver,
    })
}

/// Build a connected Filter processor
/// 
/// Filter processors need to connect to their upstream output
fn build_connected_filter_processor(
    _filter: &PhysicalFilter,
    physical_plan: Arc<dyn PhysicalPlan>,
    upstream_output: Option<broadcast::Receiver<StreamData>>,
) -> Result<ConnectedProcessorNode, String> {
    let downstream_count = 1; // Default to 1 downstream
    
    // Filter processor must have exactly one upstream
    let upstream_receiver = upstream_output.ok_or("Filter processor requires exactly 1 upstream")?;
    
    let processor = Arc::new(FilterProcessor::new(
        physical_plan,
        downstream_count,
    ));
    
    let processor_view = processor.start(upstream_receiver.resubscribe());
    
    Ok(ConnectedProcessorNode {
        processor,
        processor_view,
        input_receiver: upstream_receiver,
    })
}

/// Build a connected Project processor
/// 
/// Project processors need to connect to their upstream output
fn build_connected_project_processor(
    _project: &PhysicalProject,
    physical_plan: Arc<dyn PhysicalPlan>,
    upstream_output: Option<broadcast::Receiver<StreamData>>,
) -> Result<ConnectedProcessorNode, String> {
    let downstream_count = 1; // Default to 1 downstream
    
    // Project processor must have exactly one upstream
    let upstream_receiver = upstream_output.ok_or("Project processor requires exactly 1 upstream")?;
    
    let processor = Arc::new(ProjectProcessor::new(
        physical_plan,
        downstream_count,
    ));
    
    let processor_view = processor.start(upstream_receiver.resubscribe());
    
    Ok(ConnectedProcessorNode {
        processor,
        processor_view,
        input_receiver: upstream_receiver,
    })
}

/// Build a complete pipeline with external control and result collection
/// 
/// Creates a pipeline with:
/// 1. Control source node (start) - receives external control signals
/// 2. Main processing chain - processes data according to physical plan
/// 3. Result sink node (end) - forwards results to external consumers
/// 
/// Returns a complete pipeline setup with external interaction channels.
pub fn build_connected_pipeline_with_external_io(
    physical_plan: Arc<dyn PhysicalPlan>,
) -> Result<ConnectedExternalPipeline, String> {
    // Create external interaction channels
    let (control_tx, control_rx) = broadcast::channel(1024);
    let (result_tx, result_rx) = broadcast::channel(1024);
    
    // Build the main processing chain
    let processing_pipeline = build_connected_pipeline(physical_plan)?;
    
    // Create control source node (pipeline entry point)
    let control_source = Arc::new(ControlSourceProcessor::with_name(
        "PipelineEntry".to_string(),
        control_rx,
        1, // Send to processing chain
    ));
    
    // Control source needs its own input channel
    let (control_input_sender, control_input_receiver) = control_source.create_input_channel();
    let control_source_view = control_source.start(control_input_receiver.resubscribe());
    
    // Create result sink node (pipeline exit point)
    // Connect it to the processing chain's output
    let processing_output = processing_pipeline.processor_view.output_sender(0)
        .ok_or("Processing pipeline has no outputs")?
        .subscribe();
    
    let result_sink = Arc::new(ResultSinkProcessor::with_name(
        "PipelineExit".to_string(),
        processing_output,
        result_tx,
        0, // No downstream for sink
    ));
    
    let (sink_input_sender, sink_input_receiver) = result_sink.create_input_channel();
    let result_sink_view = result_sink.start(sink_input_receiver.resubscribe());
    
    Ok(ConnectedExternalPipeline {
        control_sender: control_tx,
        result_receiver: result_rx,
        control_source: ConnectedProcessorNode {
            processor: control_source,
            processor_view: control_source_view,
            input_receiver: control_input_receiver,
        },
        processing_chain: processing_pipeline,
        result_sink: ConnectedProcessorNode {
            processor: result_sink,
            processor_view: result_sink_view,
            input_receiver: sink_input_receiver,
        },
    })
}

/// Complete pipeline with external I/O capabilities and proper connections
pub struct ConnectedExternalPipeline {
    /// Sender for external control signal injection
    pub control_sender: broadcast::Sender<StreamData>,
    /// Receiver for external result collection
    pub result_receiver: broadcast::Receiver<StreamData>,
    /// Control source node (pipeline entry)
    pub control_source: ConnectedProcessorNode,
    /// Main processing chain
    pub processing_chain: ConnectedProcessorNode,
    /// Result sink node (pipeline exit)
    pub result_sink: ConnectedProcessorNode,
}

impl ConnectedExternalPipeline {
    /// Send a control signal to the pipeline start
    pub fn send_control(&self, signal: StreamData) -> Result<usize, broadcast::error::SendError<StreamData>> {
        self.control_sender.send(signal)
    }
    
    /// Receive processed data from pipeline end
    pub async fn receive_result(&mut self) -> Result<StreamData, broadcast::error::RecvError> {
        self.result_receiver.recv().await
    }
    
    /// Get the main processing chain view
    pub fn processing_view(&self) -> &ProcessorView {
        &self.processing_chain.processor_view
    }
    
    /// Stop the entire pipeline using StreamData::Control signals
    pub fn stop_all(&self) -> Result<usize, broadcast::error::SendError<StreamData>> {
        self.control_sender.send(StreamData::stream_end())
    }
}

/// Execute a complete connected processor pipeline
pub fn execute_connected_pipeline(
    physical_plan: Arc<dyn PhysicalPlan>,
) -> Result<ConnectedProcessorNode, String> {
    build_connected_pipeline(physical_plan)
}

/// Backward compatibility: Original function names that delegate to new implementation

/// Build a stream processing pipeline from a physical plan (backward compatibility)
pub fn build_processor_pipeline(
    physical_plan: Arc<dyn PhysicalPlan>,
) -> Result<ConnectedProcessorNode, String> {
    build_connected_pipeline(physical_plan)
}

/// Build a pipeline with external control and result collection (backward compatibility)
pub fn build_pipeline_with_external_control(
    physical_plan: Arc<dyn PhysicalPlan>,
) -> Result<(broadcast::Sender<StreamData>, broadcast::Receiver<StreamData>, ConnectedProcessorNode), String> {
    let external_pipeline = build_connected_pipeline_with_external_io(physical_plan)?;
    
    Ok((
        external_pipeline.control_sender,
        external_pipeline.result_receiver,
        external_pipeline.processing_chain,
    ))
}

/// Build a pipeline with external I/O capabilities (backward compatibility)
pub fn build_pipeline_with_external_io(
    physical_plan: Arc<dyn PhysicalPlan>,
) -> Result<ConnectedExternalPipeline, String> {
    build_connected_pipeline_with_external_io(physical_plan)
}

/// Execute a complete processor pipeline (backward compatibility)
pub fn execute_pipeline(
    physical_plan: Arc<dyn PhysicalPlan>,
) -> Result<ProcessorView, String> {
    let pipeline = build_connected_pipeline(physical_plan)?;
    Ok(pipeline.processor_view)
}