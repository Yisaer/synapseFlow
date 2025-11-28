//! Processor builder - creates and connects processors from PhysicalPlan
//!
//! This module provides utilities to build processor pipelines from PhysicalPlan,
//! connecting ControlSourceProcessor outputs to leaf nodes (nodes without children).

use crate::codec::encoder::JsonEncoder;
use crate::connector::MockSinkConnector;
use crate::planner::physical::PhysicalPlan;
use crate::processor::{
    ControlSignal, ControlSourceProcessor, DataSourceProcessor, FilterProcessor, Processor,
    ProcessorError, ProjectProcessor, ResultCollectProcessor, SharedStreamProcessor, SinkProcessor,
    StreamData,
};
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use uuid::Uuid;

/// Enum for all processor types created from PhysicalPlan
///
/// This enum allows storing different types of processors in a unified way.
/// Currently supports DataSourceProcessor, ProjectProcessor, and FilterProcessor.
pub enum PlanProcessor {
    /// DataSourceProcessor created from PhysicalDatasource
    DataSource(DataSourceProcessor),
    /// SharedStreamProcessor created from PhysicalSharedStream
    SharedSource(SharedStreamProcessor),
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
            PlanProcessor::SharedSource(p) => p.id(),
            PlanProcessor::Project(p) => p.id(),
            PlanProcessor::Filter(p) => p.id(),
        }
    }

    pub fn set_pipeline_id(&mut self, pipeline_id: &str) {
        if let PlanProcessor::SharedSource(proc) = self {
            proc.set_pipeline_id(pipeline_id);
        }
    }

    /// Start the processor
    pub fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        match self {
            PlanProcessor::DataSource(p) => p.start(),
            PlanProcessor::SharedSource(p) => p.start(),
            PlanProcessor::Project(p) => p.start(),
            PlanProcessor::Filter(p) => p.start(),
        }
    }

    /// Subscribe to the processor's output stream
    pub fn subscribe_output(&self) -> Option<broadcast::Receiver<crate::processor::StreamData>> {
        match self {
            PlanProcessor::DataSource(p) => p.subscribe_output(),
            PlanProcessor::SharedSource(p) => p.subscribe_output(),
            PlanProcessor::Project(p) => p.subscribe_output(),
            PlanProcessor::Filter(p) => p.subscribe_output(),
        }
    }

    /// Subscribe to the processor's control output stream
    pub fn subscribe_control_output(
        &self,
    ) -> Option<broadcast::Receiver<crate::processor::StreamData>> {
        match self {
            PlanProcessor::DataSource(p) => p.subscribe_control_output(),
            PlanProcessor::SharedSource(p) => p.subscribe_control_output(),
            PlanProcessor::Project(p) => p.subscribe_control_output(),
            PlanProcessor::Filter(p) => p.subscribe_control_output(),
        }
    }

    /// Add an input channel
    pub fn add_input(&mut self, receiver: broadcast::Receiver<crate::processor::StreamData>) {
        match self {
            PlanProcessor::DataSource(p) => p.add_input(receiver),
            PlanProcessor::SharedSource(p) => p.add_input(receiver),
            PlanProcessor::Project(p) => p.add_input(receiver),
            PlanProcessor::Filter(p) => p.add_input(receiver),
        }
    }

    /// Add a control input channel
    pub fn add_control_input(
        &mut self,
        receiver: broadcast::Receiver<crate::processor::StreamData>,
    ) {
        match self {
            PlanProcessor::DataSource(p) => p.add_control_input(receiver),
            PlanProcessor::SharedSource(p) => p.add_control_input(receiver),
            PlanProcessor::Project(p) => p.add_control_input(receiver),
            PlanProcessor::Filter(p) => p.add_control_input(receiver),
        }
    }
}

/// Complete processor pipeline structure
///
/// Contains all processors in the pipeline:
/// - ControlSourceProcessor: data flow starting point
/// - Middle processors: created from PhysicalPlan nodes (can be various types)
/// - ResultCollectProcessor: data flow ending point
pub struct ProcessorPipeline {
    /// Pipeline input channel (send data into ControlSourceProcessor)
    pub input: mpsc::Sender<StreamData>,
    /// Pipeline output channel (receive data from ResultCollectProcessor)
    pub output: Option<mpsc::Receiver<StreamData>>,
    /// Control source processor (data head)
    pub control_source: ControlSourceProcessor,
    /// Middle processors created from PhysicalPlan (various types)
    pub middle_processors: Vec<PlanProcessor>,
    /// Sink processors wired to the PhysicalPlan root (fan-out for connectors)
    pub sink_processors: Vec<SinkProcessor>,
    /// Result sink processor (data tail) if downstream forwarding is enabled
    pub result_sink: Option<ResultCollectProcessor>,
    /// Broadcast sender feeding the control source input
    control_input_sender: broadcast::Sender<StreamData>,
    /// Buffered receiver that bridges external input into the control input sender
    control_input_buffer: Option<mpsc::Receiver<StreamData>>,
    /// Join handles for all running processors
    handles: Vec<JoinHandle<Result<(), ProcessorError>>>,
    /// Logical pipeline identifier used for diagnostics/subscriptions
    pipeline_id: String,
}

impl ProcessorPipeline {
    /// Start all processors in the pipeline. Subsequent calls are no-ops.
    pub fn start(&mut self) {
        if !self.handles.is_empty() {
            return;
        }
        if let Some(buffer) = self.control_input_buffer.take() {
            let sender = self.control_input_sender.clone();
            self.handles.push(tokio::spawn(async move {
                let mut receiver = buffer;
                while let Some(data) = receiver.recv().await {
                    sender
                        .send(data)
                        .map_err(|_| ProcessorError::ChannelClosed)?;
                }
                Ok(())
            }));
        }
        self.handles.push(self.control_source.start());
        for processor in &mut self.middle_processors {
            self.handles.push(processor.start());
        }
        for sink in &mut self.sink_processors {
            self.handles.push(sink.start());
        }
        if let Some(result_sink) = &mut self.result_sink {
            self.handles.push(result_sink.start());
        }
    }

    /// Broadcast a control signal into the pipeline, respecting its channel target.
    pub fn broadcast_control_signal(&self, signal: ControlSignal) -> Result<(), ProcessorError> {
        self.control_input_sender
            .send(StreamData::control(signal))
            .map(|_| ())
            .map_err(|_| ProcessorError::ChannelClosed)
    }

    pub fn set_pipeline_id(&mut self, id: impl Into<String>) {
        let id = id.into();
        self.pipeline_id = id.clone();
        for processor in &mut self.middle_processors {
            processor.set_pipeline_id(&id);
        }
    }

    pub fn pipeline_id(&self) -> &str {
        &self.pipeline_id
    }

    /// Close the pipeline gracefully using the data path.
    pub async fn close(&mut self) -> Result<(), ProcessorError> {
        self.graceful_close().await
    }

    /// Gracefully close the pipeline by sending StreamEnd via the data channel.
    pub async fn graceful_close(&mut self) -> Result<(), ProcessorError> {
        self.send_stream_end_via_data().await?;
        self.await_all_handles().await
    }

    /// Quickly close the pipeline by delivering StreamQuickEnd to the control channel.
    pub async fn quick_close(&mut self) -> Result<(), ProcessorError> {
        self.broadcast_control_signal(ControlSignal::StreamQuickEnd)?;
        self.replace_input_sender();
        self.await_all_handles().await
    }

    async fn send_stream_end_via_data(&mut self) -> Result<(), ProcessorError> {
        self.input
            .send(StreamData::stream_end())
            .await
            .map_err(|_| ProcessorError::ChannelClosed)?;
        self.replace_input_sender();
        Ok(())
    }

    fn replace_input_sender(&mut self) {
        let (dummy_tx, _) = mpsc::channel(1);
        let old_input = std::mem::replace(&mut self.input, dummy_tx);
        drop(old_input);
    }

    async fn await_all_handles(&mut self) -> Result<(), ProcessorError> {
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
        self.control_source
            .send_stream_data(processor_id, data)
            .await
    }

    /// Take ownership of the pipeline's output receiver.
    pub fn take_output(&mut self) -> Option<mpsc::Receiver<StreamData>> {
        self.output.take()
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
    plan: &Arc<PhysicalPlan>,
) -> Result<PlanProcessor, ProcessorError> {
    let plan_index = plan.get_plan_index();
    match plan.as_ref() {
        PhysicalPlan::DataSource(ds) => {
            let processor =
                DataSourceProcessor::new(plan_index, ds.source_name().to_string(), ds.schema());
            Ok(PlanProcessor::DataSource(processor))
        }
        PhysicalPlan::SharedStream(shared) => {
            let processor =
                SharedStreamProcessor::new(plan_index, shared.stream_name().to_string());
            Ok(PlanProcessor::SharedSource(processor))
        }
        PhysicalPlan::Project(project) => {
            let processor_id = format!("project_{}", plan_index);
            let processor = ProjectProcessor::new(processor_id, Arc::new(project.clone()));
            Ok(PlanProcessor::Project(processor))
        }
        PhysicalPlan::Filter(filter) => {
            let processor_id = format!("filter_{}", plan_index);
            let processor = FilterProcessor::new(processor_id, Arc::new(filter.clone()));
            Ok(PlanProcessor::Filter(processor))
        }
    }
}

/// Internal structure to track processors created from PhysicalPlan nodes
struct ProcessorMap {
    /// Map from plan index to processor
    processors: std::collections::HashMap<i64, PlanProcessor>,
}

impl ProcessorMap {
    fn new() -> Self {
        Self {
            processors: std::collections::HashMap::new(),
        }
    }

    fn get_processor(&self, plan_index: i64) -> Option<&PlanProcessor> {
        self.processors.get(&plan_index)
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
    plan: Arc<PhysicalPlan>,
    processor_map: &mut ProcessorMap,
) -> Result<(), ProcessorError> {
    let plan_index = plan.get_plan_index();

    // Create processor for current node
    let processor = create_processor_from_plan_node(&plan)?;
    processor_map.insert_processor(plan_index, processor);

    // Recursively process children
    for child in plan.children() {
        build_processors_recursive(Arc::clone(child), processor_map)?;
    }

    Ok(())
}

/// Collect leaf node indices from PhysicalPlan tree
fn collect_leaf_indices(plan: Arc<PhysicalPlan>) -> Vec<i64> {
    let mut leaf_indices = Vec::new();

    if plan.children().is_empty() {
        leaf_indices.push(plan.get_plan_index());
    } else {
        for child in plan.children() {
            leaf_indices.extend(collect_leaf_indices(Arc::clone(child)));
        }
    }

    leaf_indices
}

/// Collect parent-child relationships from PhysicalPlan tree
fn collect_parent_child_relations(plan: Arc<PhysicalPlan>) -> Vec<(i64, i64)> {
    let mut relations = Vec::new();
    let parent_index = plan.get_plan_index();

    for child in plan.children() {
        let child_index = child.get_plan_index();
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
    physical_plan: Arc<PhysicalPlan>,
    processor_map: &mut ProcessorMap,
    control_source: &mut ControlSourceProcessor,
) -> Result<(), ProcessorError> {
    // 1. Connect ControlSourceProcessor to all leaf nodes
    let leaf_indices = collect_leaf_indices(Arc::clone(&physical_plan));
    for leaf_index in leaf_indices {
        if let Some(processor) = processor_map.get_processor_mut(leaf_index) {
            let receiver = control_source.subscribe_output().ok_or_else(|| {
                ProcessorError::InvalidConfiguration("control source output unavailable".into())
            })?;
            processor.add_input(receiver);
            if let Some(control_rx) = control_source.subscribe_control_output() {
                processor.add_control_input(control_rx);
            }
        }
    }

    // 2. Connect children outputs to parent inputs
    let relations = collect_parent_child_relations(Arc::clone(&physical_plan));
    for (parent_index, child_index) in relations {
        let receiver = processor_map
            .get_processor(child_index)
            .and_then(|proc| proc.subscribe_output())
            .ok_or_else(|| {
                ProcessorError::InvalidConfiguration(format!(
                    "Processor {} has no broadcast output",
                    child_index
                ))
            })?;

        let control_receiver = processor_map
            .get_processor(child_index)
            .and_then(|proc| proc.subscribe_control_output());
        if let Some(parent_processor) = processor_map.get_processor_mut(parent_index) {
            parent_processor.add_input(receiver);
            if let Some(control_rx) = control_receiver {
                parent_processor.add_control_input(control_rx);
            }
        }
    }

    Ok(())
}

/// Create a complete processor pipeline from a PhysicalPlan tree and custom sinks.
///
/// This function:
/// 1. Recursively traverses the PhysicalPlan tree
/// 2. Creates a processor for each PhysicalPlan node
/// 3. Connects processors based on tree structure:
///    - ControlSourceProcessor output -> leaf nodes input
///    - Children outputs -> parent input
/// 4. Connects root node output -> provided SinkProcessors -> ResultCollectProcessor inputs
pub fn create_processor_pipeline(
    physical_plan: Arc<PhysicalPlan>,
    mut sink_processors: Vec<SinkProcessor>,
) -> Result<ProcessorPipeline, ProcessorError> {
    if sink_processors.is_empty() {
        return Err(ProcessorError::InvalidConfiguration(
            "At least one SinkProcessor is required".to_string(),
        ));
    }

    let mut control_source = ControlSourceProcessor::new("control_source");
    let (pipeline_input_sender, pipeline_input_receiver) = mpsc::channel(100);
    let (control_input_sender, control_input_receiver) =
        broadcast::channel(crate::processor::base::DEFAULT_CHANNEL_CAPACITY);
    control_source.add_input(control_input_receiver);

    let mut processor_map = ProcessorMap::new();
    build_processors_recursive(Arc::clone(&physical_plan), &mut processor_map)?;

    connect_processors(
        Arc::clone(&physical_plan),
        &mut processor_map,
        &mut control_source,
    )?;

    let root_index = physical_plan.get_plan_index();
    if processor_map.get_processor(root_index).is_none() {
        return Err(ProcessorError::InvalidConfiguration(
            "Root processor not found".to_string(),
        ));
    }

    for sink in sink_processors.iter_mut() {
        let receiver = processor_map
            .get_processor(root_index)
            .and_then(|proc| proc.subscribe_output())
            .ok_or_else(|| {
                ProcessorError::InvalidConfiguration(
                    "Root processor is missing broadcast output".to_string(),
                )
            })?;
        sink.add_input(receiver);
        if let Some(control_rx) = processor_map
            .get_processor(root_index)
            .and_then(|proc| proc.subscribe_control_output())
        {
            sink.add_control_input(control_rx);
        }
    }

    let mut result_sink = None;
    let mut pipeline_output_receiver = None;
    let mut sink_outputs = Vec::new();
    let mut sink_control_outputs = Vec::new();
    for sink in sink_processors.iter_mut() {
        if let Some(receiver) = sink.subscribe_output() {
            sink_outputs.push(receiver);
        }
        if let Some(control_receiver) = sink.subscribe_control_output() {
            sink_control_outputs.push(control_receiver);
        }
    }

    if !sink_outputs.is_empty() {
        let mut collector = ResultCollectProcessor::new("result_sink");
        for receiver in sink_outputs {
            collector.add_input(receiver);
        }
        for control_receiver in sink_control_outputs {
            collector.add_control_input(control_receiver);
        }
        let (result_output_sender, pipeline_output_rx) = mpsc::channel(100);
        collector.set_output(result_output_sender);
        result_sink = Some(collector);
        pipeline_output_receiver = Some(pipeline_output_rx);
    }

    let mut middle_processors = processor_map.get_all_processors();
    let pipeline_id = Uuid::new_v4().to_string();
    for processor in &mut middle_processors {
        processor.set_pipeline_id(&pipeline_id);
    }

    Ok(ProcessorPipeline {
        input: pipeline_input_sender,
        output: pipeline_output_receiver,
        control_source,
        middle_processors,
        sink_processors,
        result_sink,
        control_input_sender,
        control_input_buffer: Some(pipeline_input_receiver),
        handles: Vec::new(),
        pipeline_id,
    })
}

/// Convenience helper that wires a PhysicalPlan into a pipeline backed by a logging mock sink.
pub fn create_processor_pipeline_with_log_sink(
    physical_plan: Arc<PhysicalPlan>,
    forward_to_result: bool,
) -> Result<ProcessorPipeline, ProcessorError> {
    let mut log_sink = SinkProcessor::new("log_sink");
    if forward_to_result {
        log_sink.enable_result_forwarding();
    }
    let (connector, _handle) = MockSinkConnector::new("log_sink_connector");
    let encoder = Arc::new(JsonEncoder::new("log_sink_encoder"));
    log_sink.add_connector(Box::new(connector), encoder);

    create_processor_pipeline(physical_plan, vec![log_sink])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::ScalarExpr;
    use crate::planner::physical::{PhysicalDataSource, PhysicalProject, PhysicalProjectField};
    use datatypes::{ConcreteDatatype, Schema, Value};
    use sqlparser::ast::{Expr, Value as SqlValue};
    use std::sync::Arc;

    #[test]
    fn test_create_processor_from_physical_project() {
        // Create a simple data source
        let schema = Arc::new(Schema::new(vec![]));
        let data_source = Arc::new(PhysicalPlan::DataSource(PhysicalDataSource::new(
            "test_source".to_string(),
            None,
            schema,
            0,
        )));

        // Create a projection field
        let project_field = PhysicalProjectField::new(
            "projected_field".to_string(),
            Expr::Value(SqlValue::Number("42".to_string(), false)),
            ScalarExpr::Literal(
                Value::Int64(42),
                ConcreteDatatype::Int64(datatypes::Int64Type),
            ),
        );

        // Create a PhysicalProject
        let physical_project = Arc::new(PhysicalPlan::Project(PhysicalProject::with_single_child(
            vec![project_field],
            data_source,
            1,
        )));

        // Try to create a processor from the PhysicalProject
        let result = create_processor_from_plan_node(&physical_project);

        assert!(
            result.is_ok(),
            "Should successfully create processor from PhysicalProject"
        );

        match result {
            Ok(processor) => {
                assert_eq!(processor.id(), "project_1");
                println!(
                    "✅ SUCCESS: PhysicalProject processor created with ID: {}",
                    processor.id()
                );
            }
            Err(e) => {
                panic!("Failed to create PhysicalProject processor: {}", e);
            }
        }
    }
}
