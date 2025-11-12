//! Processor builder - creates and connects processors from PhysicalPlan
//!
//! This module provides utilities to build processor pipelines from PhysicalPlan,
//! connecting ControlSourceProcessor outputs to leaf nodes (nodes without children).

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use std::sync::Arc;
use crate::processor::{
    Processor, ProcessorError, 
    ControlSourceProcessor, DataSourceProcessor, ResultSinkProcessor, StreamData,
};
use crate::planner::physical::{PhysicalPlan, PhysicalDataSource};

/// Enum for all processor types created from PhysicalPlan
///
/// This enum allows storing different types of processors in a unified way.
/// Currently only DataSourceProcessor is implemented, but more types can be added.
pub enum PlanProcessor {
    /// DataSourceProcessor created from PhysicalDatasource
    DataSource(DataSourceProcessor),
    // Future processor types can be added here:
    // Filter(FilterProcessor),
    // Project(ProjectProcessor),
    // etc.
}

impl PlanProcessor {
    /// Get the processor ID
    pub fn id(&self) -> &str {
        match self {
            PlanProcessor::DataSource(p) => p.id(),
        }
    }
    
    /// Start the processor
    pub fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        match self {
            PlanProcessor::DataSource(p) => p.start(),
        }
    }
    
    /// Get output channel senders
    pub fn output_senders(&self) -> Vec<mpsc::Sender<crate::processor::StreamData>> {
        match self {
            PlanProcessor::DataSource(p) => p.output_senders(),
        }
    }
    
    /// Add an input channel
    pub fn add_input(&mut self, receiver: mpsc::Receiver<crate::processor::StreamData>) {
        match self {
            PlanProcessor::DataSource(p) => p.add_input(receiver),
        }
    }
    
    /// Add an output channel
    pub fn add_output(&mut self, sender: mpsc::Sender<crate::processor::StreamData>) {
        match self {
            PlanProcessor::DataSource(p) => p.add_output(sender),
        }
    }
}

/// Complete processor pipeline structure
///
/// Contains all processors in the pipeline:
/// - ControlSourceProcessor: data flow starting point
/// - Middle processors: created from PhysicalPlan nodes (can be various types)
/// - ResultSinkProcessor: data flow ending point
pub struct ProcessorPipeline {
    /// Pipeline input channel (send data into ControlSourceProcessor)
    pub input: mpsc::Sender<StreamData>,
    /// Pipeline output channel (receive data from ResultSinkProcessor)
    pub output: mpsc::Receiver<StreamData>,
    /// Control source processor (data head)
    pub control_source: ControlSourceProcessor,
    /// Middle processors created from PhysicalPlan (various types)
    pub middle_processors: Vec<PlanProcessor>,
    /// Result sink processor (data tail)
    pub result_sink: ResultSinkProcessor,
    /// Join handles for all running processors
    handles: Vec<JoinHandle<Result<(), ProcessorError>>>,
}

impl ProcessorPipeline {
    /// Start all processors in the pipeline. Subsequent calls are no-ops.
    pub fn start(&mut self) {
        if !self.handles.is_empty() {
            return;
        }
        self.handles.push(self.control_source.start());
        for processor in &mut self.middle_processors {
            self.handles.push(processor.start());
        }
        self.handles.push(self.result_sink.start());
    }

    /// Gracefully close the pipeline by sending StreamEnd and awaiting all tasks.
    pub async fn close(&mut self) -> Result<(), ProcessorError> {
        // Send StreamEnd to signal shutdown
        self.input
            .send(StreamData::stream_end())
            .await
            .map_err(|_| ProcessorError::ChannelClosed)?;

        // Await all processor tasks
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
}

/// Connect ControlSourceProcessor outputs to all leaf nodes in PhysicalPlan
///
/// Leaf nodes are PhysicalPlan nodes that have no children.
/// Currently, only PhysicalDatasource is supported as a leaf node.
///
/// # Arguments
/// * `control_source` - The ControlSourceProcessor whose outputs will be connected
/// * `physical_plan` - The PhysicalPlan to traverse and find leaf nodes
///
/// # Returns
/// A vector of DataSourceProcessor created from leaf nodes, with their inputs connected to control_source outputs
pub fn connect_control_source_to_leaf_nodes(
    control_source: &mut ControlSourceProcessor,
    physical_plan: Arc<dyn PhysicalPlan>,
) -> Result<Vec<DataSourceProcessor>, ProcessorError> {
    let mut processors = Vec::new();
    let leaf_nodes = find_leaf_nodes(physical_plan);
    
    for (idx, leaf) in leaf_nodes.iter().enumerate() {
        // Create processor from leaf node
        if let Some(ds) = leaf.as_any().downcast_ref::<PhysicalDataSource>() {
            let processor_id = format!("datasource_{}", idx);
            let mut datasource_processor = DataSourceProcessor::new(
                processor_id.clone(),
                Arc::new(ds.clone()),
            );
            
            // Create channel to connect control_source output to datasource_processor input
            let (sender, receiver) = mpsc::channel(100);
            control_source.add_output(sender);
            datasource_processor.add_input(receiver);
            
            processors.push(datasource_processor);
        } else {
            return Err(ProcessorError::InvalidConfiguration(format!(
                "Unsupported leaf node type: {}",
                leaf.get_plan_type()
            )));
        }
    }
    
    Ok(processors)
}

/// Find all leaf nodes (nodes without children) in a PhysicalPlan
///
/// This function recursively traverses the PhysicalPlan tree and collects
/// all nodes that have no children.
pub fn find_leaf_nodes(plan: Arc<dyn PhysicalPlan>) -> Vec<Arc<dyn PhysicalPlan>> {
    let mut leaf_nodes = Vec::new();
    
    if plan.children().is_empty() {
        // This is a leaf node
        leaf_nodes.push(plan);
    } else {
        // Recursively find leaf nodes in children
        for child in plan.children() {
            leaf_nodes.extend(find_leaf_nodes(Arc::clone(child)));
        }
    }
    
    leaf_nodes
}

/// Create a processor from a PhysicalPlan node
///
/// This function dispatches to the appropriate processor creation function
/// based on the PhysicalPlan type. Currently only PhysicalDatasource is supported.
///
/// # Arguments
/// * `plan` - The PhysicalPlan node to create a processor from
/// * `idx` - Index for generating processor ID
///
/// # Returns
/// A PlanProcessor enum variant corresponding to the plan type
pub fn create_processor_from_plan_node(
    plan: &Arc<dyn PhysicalPlan>,
    idx: usize,
) -> Result<PlanProcessor, ProcessorError> {
    if let Some(ds) = plan.as_any().downcast_ref::<PhysicalDataSource>() {
        let processor_id = format!("datasource_{}", idx);
        let processor = DataSourceProcessor::new(
            processor_id,
            Arc::new(ds.clone()),
        );
        Ok(PlanProcessor::DataSource(processor))
    } else {
        Err(ProcessorError::InvalidConfiguration(format!(
            "Unsupported PhysicalPlan type: {}",
            plan.get_plan_type()
        )))
    }
}

/// Create a DataSourceProcessor from a PhysicalPlan if it's a PhysicalDatasource
///
/// This is a convenience function for creating DataSourceProcessor specifically.
/// For general use, prefer `create_processor_from_plan_node`.
pub fn create_processor_from_physical_plan(
    id: impl Into<String>,
    plan: Arc<dyn PhysicalPlan>,
) -> Option<DataSourceProcessor> {
    if let Some(ds) = plan.as_any().downcast_ref::<PhysicalDataSource>() {
        Some(DataSourceProcessor::new(
            id,
            Arc::new(ds.clone()),
        ))
    } else {
        None
    }
}

/// Internal structure to track processors created from PhysicalPlan nodes
struct ProcessorMap {
    /// Map from plan index to processor
    processors: std::collections::HashMap<i64, PlanProcessor>,
    /// Counter for generating unique processor IDs
    processor_counter: usize,
}

impl ProcessorMap {
    fn new() -> Self {
        Self {
            processors: std::collections::HashMap::new(),
            processor_counter: 0,
        }
    }
    
    fn get_processor_mut(&mut self, plan_index: i64) -> Option<&mut PlanProcessor> {
        self.processors.get_mut(&plan_index)
    }
    
    fn insert_processor(&mut self, plan_index: i64, processor: PlanProcessor) {
        self.processors.insert(plan_index, processor);
    }
    
    fn get_all_processors(self) -> Vec<PlanProcessor> {
        self.processors.into_values().collect()
    }
}

/// Recursively build processors from PhysicalPlan tree
///
/// This function:
/// 1. Creates a processor for the current plan node
/// 2. Recursively processes all children
/// 3. Connects children's outputs to parent's input
fn build_processors_recursive(
    plan: Arc<dyn PhysicalPlan>,
    processor_map: &mut ProcessorMap,
) -> Result<(), ProcessorError> {
    let plan_index = *plan.get_plan_index();
    
    // Create processor for current node
    let processor = create_processor_from_plan_node(&plan, processor_map.processor_counter)?;
    processor_map.processor_counter += 1;
    processor_map.insert_processor(plan_index, processor);
    
    // Recursively process children
    for child in plan.children() {
        build_processors_recursive(Arc::clone(child), processor_map)?;
    }
    
    Ok(())
}

/// Collect leaf node indices from PhysicalPlan tree
fn collect_leaf_indices(plan: Arc<dyn PhysicalPlan>) -> Vec<i64> {
    let mut leaf_indices = Vec::new();
    
    if plan.children().is_empty() {
        leaf_indices.push(*plan.get_plan_index());
    } else {
        for child in plan.children() {
            leaf_indices.extend(collect_leaf_indices(Arc::clone(child)));
        }
    }
    
    leaf_indices
}

/// Collect parent-child relationships from PhysicalPlan tree
fn collect_parent_child_relations(plan: Arc<dyn PhysicalPlan>) -> Vec<(i64, i64)> {
    let mut relations = Vec::new();
    let parent_index = *plan.get_plan_index();
    
    for child in plan.children() {
        let child_index = *child.get_plan_index();
        relations.push((parent_index, child_index));
        // Recursively collect from children
        relations.extend(collect_parent_child_relations(Arc::clone(child)));
    }
    
    relations
}

/// Connect processors based on PhysicalPlan tree structure
///
/// This function connects:
/// - ControlSourceProcessor outputs to leaf node inputs
/// - Children outputs to parent inputs
fn connect_processors(
    physical_plan: Arc<dyn PhysicalPlan>,
    processor_map: &mut ProcessorMap,
    control_source: &mut ControlSourceProcessor,
) -> Result<(), ProcessorError> {
    // 1. Connect ControlSourceProcessor to all leaf nodes
    let leaf_indices = collect_leaf_indices(Arc::clone(&physical_plan));
    for leaf_index in leaf_indices {
        if let Some(processor) = processor_map.get_processor_mut(leaf_index) {
            let (sender, receiver) = mpsc::channel(100);
            control_source.add_output(sender);
            processor.add_input(receiver);
        }
    }
    
    // 2. Connect children outputs to parent inputs
    let relations = collect_parent_child_relations(Arc::clone(&physical_plan));
    for (parent_index, child_index) in relations {
        // Create channel
        let (sender, receiver) = mpsc::channel(100);
        
        // Connect child output
        if let Some(child_processor) = processor_map.get_processor_mut(child_index) {
            child_processor.add_output(sender);
        }
        
        // Connect parent input
        if let Some(parent_processor) = processor_map.get_processor_mut(parent_index) {
            parent_processor.add_input(receiver);
        }
    }
    
    Ok(())
}

/// Create a complete processor pipeline from a PhysicalPlan tree
///
/// This function:
/// 1. Recursively traverses the PhysicalPlan tree
/// 2. Creates a processor for each PhysicalPlan node
/// 3. Connects processors based on tree structure:
///    - ControlSourceProcessor output -> leaf nodes input
///    - Children outputs -> parent input
/// 4. Connects root node output -> ResultSinkProcessor input
///
/// # Arguments
/// * `physical_plan` - The root PhysicalPlan node (data flow end point)
///
/// # Returns
/// A ProcessorPipeline containing all connected processors
pub fn create_processor_pipeline(
    physical_plan: Arc<dyn PhysicalPlan>,
) -> Result<ProcessorPipeline, ProcessorError> {
    let mut control_source = ControlSourceProcessor::new("control_source");
    let (pipeline_input_sender, control_input_receiver) = mpsc::channel(100);
    control_source.add_input(control_input_receiver);

    let mut processor_map = ProcessorMap::new();
    build_processors_recursive(Arc::clone(&physical_plan), &mut processor_map)?;

    connect_processors(Arc::clone(&physical_plan), &mut processor_map, &mut control_source)?;

    let mut result_sink = ResultSinkProcessor::new("result_sink");

    let root_index = *physical_plan.get_plan_index();
    if let Some(root_processor) = processor_map.get_processor_mut(root_index) {
        let (sender, receiver) = mpsc::channel(100);
        root_processor.add_output(sender);
        result_sink.add_input(receiver);
    } else {
        return Err(ProcessorError::InvalidConfiguration(
            "Root processor not found".to_string()
        ));
    }

    let (result_output_sender, pipeline_output_receiver) = mpsc::channel(100);
    result_sink.add_output(result_output_sender);

    let middle_processors = processor_map.get_all_processors();
    
    Ok(ProcessorPipeline {
        input: pipeline_input_sender,
        output: pipeline_output_receiver,
        control_source,
        middle_processors,
        result_sink,
        handles: Vec::new(),
    })
}
