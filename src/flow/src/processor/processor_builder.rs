
//! Processor builder - creates and connects processors from PhysicalPlan
//!
//! This module provides utilities to build processor pipelines from PhysicalPlan,
//! connecting ControlSourceProcessor outputs to leaf nodes (nodes without children).

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use std::sync::Arc;
use crate::processor::{
    Processor, ProcessorError, 
    ControlSourceProcessor, DataSourceProcessor, ProjectProcessor, FilterProcessor, ResultSinkProcessor, StreamData,
};
use crate::planner::physical::{PhysicalPlan, PhysicalDataSource, PhysicalProject, PhysicalFilter};

/// Enum for all processor types created from PhysicalPlan
///
/// This enum allows storing different types of processors in a unified way.
/// Currently supports DataSourceProcessor, ProjectProcessor, and FilterProcessor.
pub enum PlanProcessor {
    /// DataSourceProcessor created from PhysicalDatasource
    DataSource(DataSourceProcessor),
    /// ProjectProcessor created from PhysicalProject
    Project(ProjectProcessor),
    /// FilterProcessor created from PhysicalFilter
    Filter(FilterProcessor),
}

impl PlanProcessor {
    /// Get the processor ID
    pub fn id(&self) -> &str {
        match self {
            PlanProcessor::DataSource(p) => p.id(),
            PlanProcessor::Project(p) => p.id(),
            PlanProcessor::Filter(p) => p.id(),
        }
    }
    
    /// Start the processor
    pub fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        match self {
            PlanProcessor::DataSource(p) => p.start(),
            PlanProcessor::Project(p) => p.start(),
            PlanProcessor::Filter(p) => p.start(),
        }
    }
    
    /// Get output channel senders
    pub fn output_senders(&self) -> Vec<mpsc::Sender<crate::processor::StreamData>> {
        match self {
            PlanProcessor::DataSource(p) => p.output_senders(),
            PlanProcessor::Project(p) => p.output_senders(),
            PlanProcessor::Filter(p) => p.output_senders(),
        }
    }
    
    /// Add an input channel
    pub fn add_input(&mut self, receiver: mpsc::Receiver<crate::processor::StreamData>) {
        match self {
            PlanProcessor::DataSource(p) => p.add_input(receiver),
            PlanProcessor::Project(p) => p.add_input(receiver),
            PlanProcessor::Filter(p) => p.add_input(receiver),
        }
    }
    
    /// Add an output channel
    pub fn add_output(&mut self, sender: mpsc::Sender<crate::processor::StreamData>) {
        match self {
            PlanProcessor::DataSource(p) => p.add_output(sender),
            PlanProcessor::Project(p) => p.add_output(sender),
            PlanProcessor::Filter(p) => p.add_output(sender),
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
    
    /// Send StreamData to a specific downstream processor by id
    ///
    /// This method directly delegates to ControlSourceProcessor's send_stream_data method,
    /// providing a convenient interface for sending data to specific processors in the pipeline.
    ///
    /// # Arguments
    /// * `processor_id` - The ID of the target processor
    /// * `data` - The StreamData to send
    ///
    /// # Returns
    /// * `Ok(())` if the data was sent successfully
    /// * `Err(ProcessorError)` if the processor was not found or channel error occurred
    pub async fn send_stream_data(
        &self,
        processor_id: &str,
        data: StreamData,
    ) -> Result<(), ProcessorError> {
        self.control_source.send_stream_data(processor_id, data).await
    }
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
    _idx: usize,
) -> Result<PlanProcessor, ProcessorError> {
    if let Some(ds) = plan.as_any().downcast_ref::<PhysicalDataSource>() {
        let processor_id = ds.source_name();
        let processor = DataSourceProcessor::new(
            processor_id,
        );
        Ok(PlanProcessor::DataSource(processor))
    } else if let Some(proj) = plan.as_any().downcast_ref::<PhysicalProject>() {
        let processor_id = format!("project_{}", _idx);
        let processor = ProjectProcessor::new(
            processor_id,
            Arc::new(proj.clone()),
        );
        Ok(PlanProcessor::Project(processor))
    } else if let Some(filter) = plan.as_any().downcast_ref::<PhysicalFilter>() {
        let processor_id = format!("filter_{}", _idx);
        let processor = FilterProcessor::new(
            processor_id,
            Arc::new(filter.clone()),
        );
        Ok(PlanProcessor::Filter(processor))
    } else {
        Err(ProcessorError::InvalidConfiguration(format!(
            "Unsupported PhysicalPlan type: {}",
            plan.get_plan_type()
        )))
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
            let processor_id = processor.id().to_string();
            let (sender, receiver) = mpsc::channel(100);
            control_source.add_output_for_processor(processor_id, sender);
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use crate::planner::physical::{PhysicalProject, PhysicalProjectField, PhysicalDataSource};
    use sqlparser::ast::{Expr, Value as SqlValue};
    use crate::expr::ScalarExpr;
    use datatypes::{Value, ConcreteDatatype};

    #[test]
    fn test_create_processor_from_physical_project() {
        // Create a simple data source
        let data_source: Arc<dyn crate::planner::physical::PhysicalPlan> =
            Arc::new(PhysicalDataSource::new("test_source".to_string(), 0));

        // Create a projection field
        let project_field = PhysicalProjectField::new(
            "projected_field".to_string(),
            Expr::Value(SqlValue::Number("42".to_string(), false)),
            ScalarExpr::Literal(Value::Int64(42), ConcreteDatatype::Int64(datatypes::Int64Type)),
        );

        // Create a PhysicalProject
        let physical_project: Arc<dyn crate::planner::physical::PhysicalPlan> =
            Arc::new(PhysicalProject::with_single_child(
                vec![project_field],
                data_source,
                1,
            ));

        // Try to create a processor from the PhysicalProject
        let result = create_processor_from_plan_node(&physical_project, 0);
        
        assert!(result.is_ok(), "Should successfully create processor from PhysicalProject");
        
        match result {
            Ok(processor) => {
                assert_eq!(processor.id(), "project_0");
                println!("✅ SUCCESS: PhysicalProject processor created with ID: {}", processor.id());
            }
            Err(e) => {
                panic!("Failed to create PhysicalProject processor: {}", e);
            }
        }
    }
}
