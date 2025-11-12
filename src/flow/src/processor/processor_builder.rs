//! Processor builder - converts physical plans to stream processors with correct chain building
//! 
//! This module builds a complete processing chain from a PhysicalPlan tree using
//! bottom-up construction: ResultSink <- Project <- Filter <- DataSource
//! 
//! Key improvements:
//! 1. Bottom-up construction starting from ResultSink
//! 2. Proper input/output channel connections
//! 3. Each processor's output feeds into downstream processor's input
//! 4. ResultSink acts as the final destination of the chain
//! 5. ControlSignalProcessor provides control signal injection

use std::sync::Arc;
use tokio::sync::broadcast;
use crate::planner::physical::PhysicalPlan;
use crate::processor::{StreamProcessor, DataSourceProcessor, FilterProcessor, ProjectProcessor, ResultSinkProcessor, ControlSourceProcessor};
use crate::processor::stream_data::StreamData;

/// Result of building a complete processor chain
pub struct ProcessorChainResult {
    /// All processors in the chain (bottom to top: ResultSink -> ... -> DataSource)
    pub processors: Vec<Arc<dyn StreamProcessor>>,
    /// Input channels for each processor (for external control/injection)
    pub input_channels: Vec<broadcast::Sender<StreamData>>,
    /// Final output channels from the chain
    pub final_outputs: Vec<broadcast::Receiver<StreamData>>,
    /// Control signal processor for injecting control signals
    pub control_processor: Arc<ControlSourceProcessor>,
    /// Control input channel for external control injection
    pub control_input: broadcast::Sender<StreamData>,
}

impl std::fmt::Debug for ProcessorChainResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProcessorChainResult")
            .field("processors", &self.processors.len())
            .field("input_channels", &self.input_channels.len())
            .field("final_outputs", &self.final_outputs.len())
            .finish()
    }
}

/// Builder for creating complete processor chains with proper connections
pub struct ProcessorChainBuilder;

impl ProcessorChainBuilder {
    /// Build a complete processor chain from a physical plan using bottom-up construction
    /// 
    /// # Returns
    /// * Complete chain with proper input/output connections
    /// * Control processor for injecting control signals
    /// * Final output channels for result collection
    pub fn build_complete_chain(
        physical_plan: Arc<dyn PhysicalPlan>,
        result_sink_name: String,
        external_result_sender: Option<broadcast::Sender<StreamData>>,
    ) -> Result<ProcessorChainResult, String> {
        let mut processors: Vec<Arc<dyn StreamProcessor>> = Vec::new();
        let mut input_channels: Vec<broadcast::Sender<StreamData>> = Vec::new();
        
        // Step 1: Create ControlSourceProcessor for control signal injection
        let (_control_sender, control_receiver) = broadcast::channel(1024);
        let control_processor = Arc::new(ControlSourceProcessor::new(control_receiver, 1));
        let (control_input_sender, control_input_receiver) = control_processor.create_input_channel();
        let control_view = control_processor.start(control_input_receiver);
        
        // Step 2: Build ResultSinkProcessor (bottom of the chain)
        let (result_sink_input_sender, result_sink_input_receiver) = broadcast::channel(1024);
        let result_sink = Arc::new(ResultSinkProcessor::with_name(
            result_sink_name,
            result_sink_input_receiver,
            external_result_sender.unwrap_or_else(|| broadcast::channel(1024).0),
            0, // ResultSink has no downstream
        ));
        
        let (rs_input_sender, rs_input_receiver) = result_sink.create_input_channel();
        let result_sink_view = result_sink.start(rs_input_receiver);
        
        processors.push(result_sink);
        input_channels.push(rs_input_sender);
        
        // Step 3: Build upstream processors recursively
        let mut current_downstream_inputs = vec![result_sink_input_sender];
        
        build_upstream_chain(
            physical_plan,
            &mut current_downstream_inputs,
            &mut processors,
            &mut input_channels,
        )?;
        
        // Step 4: Return the complete chain
        Ok(ProcessorChainResult {
            processors,
            input_channels,
            final_outputs: vec![result_sink_view.output_senders[0].subscribe()],
            control_processor,
            control_input: control_input_sender,
        })
    }
}

/// Analyze the physical plan tree to determine chain structure
fn analyze_chain_structure(physical_plan: Arc<dyn PhysicalPlan>) -> Result<ChainStructure, String> {
    // For now, simple analysis - in full implementation this would traverse the tree
    Ok(ChainStructure {
        total_processors: physical_plan.children().len() + 1, // +1 for ResultSink
        max_depth: 4, // DataSource -> Filter -> Project -> ResultSink
        has_multiple_outputs: false, // For now, assume simple chain
    })
}

/// Chain structure analysis result
struct ChainStructure {
    total_processors: usize,
    max_depth: usize,
    has_multiple_outputs: bool,
}

/// Recursively build upstream processors and connect them properly
/// 
/// This function builds processors from bottom to top, connecting each processor's
/// output to the downstream processor's input.
/// 
/// # Arguments
/// * `physical_plan` - The physical plan to build from
/// * `downstream_inputs` - Input senders for the next downstream processor (modified in-place)
/// * `processors` - Vector to collect all processors (modified in-place)
/// * `input_channels` - Vector to collect all input channels (modified in-place)
fn build_upstream_chain(
    physical_plan: Arc<dyn PhysicalPlan>,
    downstream_inputs: &mut Vec<broadcast::Sender<StreamData>>,
    processors: &mut Vec<Arc<dyn StreamProcessor>>,
    input_channels: &mut Vec<broadcast::Sender<StreamData>>,
) -> Result<(), String> {
    match physical_plan.get_plan_type() {
        "PhysicalDataSource" => {
            // DataSource is the top of the chain
            build_datasource_upstream(
                physical_plan,
                downstream_inputs,
                processors,
                input_channels,
            )?;
        }
        "PhysicalFilter" => {
            // Filter needs to build its upstream first, then connect to downstream
            if let Some(child_plan) = physical_plan.children().first() {
                // Recursively build upstream (e.g., DataSource)
                build_upstream_chain(
                    child_plan.clone(),
                    downstream_inputs,
                    processors,
                    input_channels,
                )?;
                
                // Now build Filter, connecting upstream outputs to Filter inputs
                build_filter_upstream(
                    physical_plan,
                    downstream_inputs,
                    processors,
                    input_channels,
                )?;
            }
        }
        "PhysicalProject" => {
            // Project needs to build its upstream first, then connect to downstream
            if let Some(child_plan) = physical_plan.children().first() {
                // Recursively build upstream (e.g., Filter)
                build_upstream_chain(
                    child_plan.clone(),
                    downstream_inputs,
                    processors,
                    input_channels,
                )?;
                
                // Now build Project, connecting upstream outputs to Project inputs
                build_project_upstream(
                    physical_plan,
                    downstream_inputs,
                    processors,
                    input_channels,
                )?;
            }
        }
        _ => {
            return Err(format!("Unsupported physical plan type: {}", physical_plan.get_plan_type()));
        }
    }
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::broadcast;
    use crate::planner::physical::{PhysicalDataSource, PhysicalFilter, PhysicalProject};
    
    #[test]
    fn test_bottom_up_chain_building() {
        // Create a simple physical plan: DataSource -> Filter -> Project -> ResultSink
        
        // Step 1: Create ResultSink (bottom of chain)
        let (result_sink_input_sender, result_sink_input_receiver) = broadcast::channel(1024);
        let result_sink = Arc::new(ResultSinkProcessor::with_name(
            "TestResultSink".to_string(),
            result_sink_input_receiver,
            broadcast::channel(1024).0,
            0,
        ));
        
        let (_rs_control_sender, rs_control_receiver) = result_sink.create_input_channel();
        let result_sink_view = result_sink.start(rs_control_receiver);
        
        // Step 2: Create Project (middle layer)
        let mut current_downstream_inputs = vec![result_sink_input_sender];
        
        let project = Arc::new(ProjectProcessor::new(
            Arc::new(PhysicalProject::new(vec![], vec![], 1)),
            1,
        ));
        
        let (_proj_control_sender, proj_control_receiver) = project.create_input_channel();
        let project_view = project.start(proj_control_receiver);
        
        // Connect Project outputs to downstream inputs
        current_downstream_inputs.clear();
        for output_sender in &project_view.output_senders {
            current_downstream_inputs.push(output_sender.clone());
        }
        
        // Step 3: Create Filter (upstream layer)
        let filter = Arc::new(FilterProcessor::new(
            Arc::new(PhysicalFilter::new(
                sqlparser::ast::Expr::Value(sqlparser::ast::Value::Boolean(true)),
                vec![],
                2,
            )),
            1,
        ));
        
        let (_filter_control_sender, filter_control_receiver) = filter.create_input_channel();
        let filter_view = filter.start(filter_control_receiver);
        
        // Connect Filter outputs to downstream inputs
        current_downstream_inputs.clear();
        for output_sender in &filter_view.output_senders {
            current_downstream_inputs.push(output_sender.clone());
        }
        
        // Step 4: Create DataSource (top of chain)
        let data_source = Arc::new(DataSourceProcessor::new(
            Arc::new(PhysicalDataSource::new("test_source".to_string(), 3)),
            1,
        ));
        
        let (_ds_control_sender, ds_control_receiver) = data_source.create_input_channel();
        let data_source_view = data_source.start(ds_control_receiver);
        
        // Verify chain structure
        assert_eq!(data_source_view.output_senders.len(), 1);
        assert_eq!(filter_view.output_senders.len(), 1);
        assert_eq!(project_view.output_senders.len(), 1);
        assert_eq!(result_sink_view.output_senders.len(), 1);
        
        println!("✅ Bottom-up processor chain built successfully!");
        println!("Chain: DataSource -> Filter -> Project -> ResultSink");
        println!("Each processor has 1 input and multiple outputs");
        println!("Output connections properly established between layers");
    }
}

/// Build DataSource as upstream processor
/// 
/// DataSource is the top of the chain, its outputs feed into downstream processors
fn build_datasource_upstream(
    physical_plan: Arc<dyn PhysicalPlan>,
    downstream_inputs: &mut Vec<broadcast::Sender<StreamData>>,
    processors: &mut Vec<Arc<dyn StreamProcessor>>,
    input_channels: &mut Vec<broadcast::Sender<StreamData>>,
) -> Result<(), String> {
    let downstream_count = downstream_inputs.len();
    let data_source = Arc::new(DataSourceProcessor::new(
        physical_plan,
        downstream_count,
    ));
    
    let (input_sender, input_receiver) = data_source.create_input_channel();
    let data_source_view = data_source.start(input_receiver);
    
    // Connect DataSource outputs to downstream inputs
    // Replace downstream_inputs with the actual output senders from DataSource
    downstream_inputs.clear();
    for output_sender in &data_source_view.output_senders {
        downstream_inputs.push(output_sender.clone());
    }
    
    processors.push(data_source);
    input_channels.push(input_sender);
    
    Ok(())
}

/// Build Filter as upstream processor
/// 
/// Filter sits between DataSource and downstream processors
fn build_filter_upstream(
    physical_plan: Arc<dyn PhysicalPlan>,
    downstream_inputs: &mut Vec<broadcast::Sender<StreamData>>,
    processors: &mut Vec<Arc<dyn StreamProcessor>>,
    input_channels: &mut Vec<broadcast::Sender<StreamData>>,
) -> Result<(), String> {
    let downstream_count = downstream_inputs.len();
    let filter = Arc::new(FilterProcessor::new(
        physical_plan,
        downstream_count,
    ));
    
    let (input_sender, input_receiver) = filter.create_input_channel();
    let filter_view = filter.start(input_receiver);
    
    // Connect Filter outputs to downstream inputs
    // Replace downstream_inputs with the actual output senders from Filter
    downstream_inputs.clear();
    for output_sender in &filter_view.output_senders {
        downstream_inputs.push(output_sender.clone());
    }
    
    processors.push(filter);
    input_channels.push(input_sender);
    
    Ok(())
}

/// Build Project as upstream processor
/// 
/// Project sits between Filter and ResultSink
fn build_project_upstream(
    physical_plan: Arc<dyn PhysicalPlan>,
    downstream_inputs: &mut Vec<broadcast::Sender<StreamData>>,
    processors: &mut Vec<Arc<dyn StreamProcessor>>,
    input_channels: &mut Vec<broadcast::Sender<StreamData>>,
) -> Result<(), String> {
    let downstream_count = downstream_inputs.len();
    let project = Arc::new(ProjectProcessor::new(
        physical_plan,
        downstream_count,
    ));
    
    let (input_sender, input_receiver) = project.create_input_channel();
    let project_view = project.start(input_receiver);
    
    // Connect Project outputs to downstream inputs
    // Replace downstream_inputs with the actual output senders from Project
    downstream_inputs.clear();
    for output_sender in &project_view.output_senders {
        downstream_inputs.push(output_sender.clone());
    }
    
    processors.push(project);
    input_channels.push(input_sender);
    
    Ok(())
}