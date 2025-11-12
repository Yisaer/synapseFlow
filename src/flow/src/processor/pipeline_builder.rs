//! Stream processing pipeline builder
//! 
//! Builds a pipeline of StreamProcessors from a PhysicalPlan tree.
//! Each processor runs in its own tokio task and communicates via broadcast channels.

use std::sync::Arc;
use crate::planner::physical::PhysicalPlan;
use crate::processor::{DataSourceProcessor, FilterProcessor, ProjectProcessor, ProcessorView, StreamData, stream_processor::StreamProcessor};
use tokio::sync::broadcast;

/// Represents a processor node in the execution pipeline
pub struct ProcessorNode {
    /// The processor instance
    pub processor: Arc<dyn crate::processor::StreamProcessor>,
    /// View for controlling and monitoring the processor
    pub processor_view: ProcessorView,
}

impl std::fmt::Debug for ProcessorNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProcessorNode")
            .field("processor", &"<dyn StreamProcessor>")
            .field("processor_view", &self.processor_view)
            .finish()
    }
}

/// Build a stream processing pipeline from a physical plan
/// 
/// This function walks through the physical plan tree and creates corresponding processor nodes.
/// Each processor runs in its own tokio task and communicates via broadcast channels.
pub fn build_processor_pipeline(
    physical_plan: Arc<dyn PhysicalPlan>,
) -> Result<ProcessorNode, String> {
    // Build the pipeline recursively
    build_processor_node(physical_plan, Vec::new())
}

/// Build a processor node and its subtree
fn build_processor_node(
    physical_plan: Arc<dyn PhysicalPlan>,
    _upstream_receivers: Vec<broadcast::Receiver<StreamData>>,
) -> Result<ProcessorNode, String> {
    // Determine downstream count by counting direct children
    let downstream_count = physical_plan.children().len();
    
    if downstream_count == 0 {
        // Leaf node (usually DataSource) - create with no upstream
        return build_leaf_processor(physical_plan, downstream_count.max(1)); // At least 1 for final output
    }
    
    // Non-leaf node - first build children, then create this processor
    let mut child_receivers = Vec::new();
    
    // Build all child processors and collect their output receivers
    for child in physical_plan.children() {
        let child_node = build_processor_node(child.clone(), Vec::new())?;
        // Get the output receiver from child's processor view - this gives us StreamData directly
        let result_rx = child_node.processor_view.result_resubscribe();
        
        // Create a wrapper channel that unwraps the Result<StreamData, String> to StreamData
        // For now, we'll pass the result receiver directly and let processors handle Results
        // In a real implementation, we might want to handle errors at the channel level
        child_receivers.push(result_rx);
    }
    
    // Create this processor with child receivers as upstream
    build_non_leaf_processor(physical_plan, child_receivers, downstream_count.max(1))
}

/// Build a leaf processor (no children, typically DataSource)
fn build_leaf_processor(
    physical_plan: Arc<dyn PhysicalPlan>,
    downstream_count: usize,
) -> Result<ProcessorNode, String> {
    match physical_plan.get_plan_type() {
        "PhysicalDataSource" => {
            let processor = Arc::new(DataSourceProcessor::new(
                physical_plan.clone(),
                Vec::new(), // No upstream for data source
                downstream_count,
            ));
            
            let processor_view = processor.start();
            Ok(ProcessorNode {
                processor,
                processor_view,
            })
        }
        plan_type => Err(format!("Unsupported leaf processor type: {}", plan_type))
    }
}

/// Build a non-leaf processor (has children)
fn build_non_leaf_processor(
    physical_plan: Arc<dyn PhysicalPlan>,
    upstream_receivers: Vec<broadcast::Receiver<Result<StreamData, String>>>,
    downstream_count: usize,
) -> Result<ProcessorNode, String> {
    match physical_plan.get_plan_type() {
        "PhysicalFilter" => {
            // For FilterProcessor, we need to convert from Result<StreamData, String> receivers to StreamData receivers
            // For now, we'll create a simple wrapper, but in reality processors should handle Results directly
            let processor = Arc::new(FilterProcessor::new(
                physical_plan.clone(),
                Vec::new(), // We'll handle the result wrapping in the processor
                downstream_count,
            ));
            
            let processor_view = processor.start();
            Ok(ProcessorNode {
                processor,
                processor_view,
            })
        }
        "PhysicalProject" => {
            let processor = Arc::new(ProjectProcessor::new(
                physical_plan.clone(),
                Vec::new(), // We'll handle the result wrapping in the processor
                downstream_count,
            ));
            
            let processor_view = processor.start();
            Ok(ProcessorNode {
                processor,
                processor_view,
            })
        }
        plan_type => Err(format!("Unsupported non-leaf processor type: {}", plan_type))
    }
}

/// Execute a complete processor pipeline
/// 
/// This is a convenience function that builds and starts the pipeline.
/// Returns the view of the root processor for result consumption and control.
pub fn execute_pipeline(
    physical_plan: Arc<dyn PhysicalPlan>,
) -> Result<ProcessorView, String> {
    let pipeline = build_processor_pipeline(physical_plan)?;
    Ok(pipeline.processor_view)
}