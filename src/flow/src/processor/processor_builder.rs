//! Processor builder - creates and connects processors from PhysicalPlan
//!
//! This module provides utilities to build processor pipelines from PhysicalPlan,
//! connecting ControlSourceProcessor outputs to leaf nodes (nodes without children).

use crate::codec::{CollectionEncoder, JsonEncoder};
use crate::connector::sink::mqtt::MqttSinkConnector;
use crate::connector::sink::nop::NopSinkConnector;
use crate::connector::sink::SinkConnector;
use crate::planner::physical::{PhysicalPlan};
use crate::planner::sink::{SinkConnectorConfig, SinkEncoderConfig};
use crate::processor::{
    BatchProcessor, ControlSignal, ControlSourceProcessor, DataSourceProcessor, EncoderProcessor,
    FilterProcessor, Processor, ProcessorError, ProjectProcessor, ResultCollectProcessor,
    SharedStreamProcessor, SinkProcessor, StreamData, StreamingEncoderProcessor,
};
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use uuid::Uuid;

/// Enum for all processor types created from PhysicalPlan
///
/// This enum allows storing different types of processors in a unified way.
/// All processors are created through PhysicalPlan.
pub enum PlanProcessor {
    /// DataSourceProcessor created from PhysicalDatasource
    DataSource(DataSourceProcessor),
    /// SharedStreamProcessor created from PhysicalSharedStream
    SharedSource(SharedStreamProcessor),
    /// ProjectProcessor created from PhysicalProject
    Project(ProjectProcessor),
    /// FilterProcessor created from PhysicalFilter
    Filter(FilterProcessor),
    /// BatchProcessor inserted before encoders when batching enabled
    Batch(BatchProcessor),
    /// EncoderProcessor inserted before sinks
    Encoder(EncoderProcessor),
    /// Streaming encoder processor combining batch + encoder
    StreamingEncoder(StreamingEncoderProcessor),
    /// SinkProcessor created from PhysicalDataSink
    Sink(SinkProcessor),
    /// ResultCollectProcessor created from PhysicalResultCollect
    ResultCollect(ResultCollectProcessor),
}

impl PlanProcessor {
    /// Get the processor ID
    pub fn id(&self) -> &str {
        match self {
            PlanProcessor::DataSource(p) => p.id(),
            PlanProcessor::SharedSource(p) => p.id(),
            PlanProcessor::Project(p) => p.id(),
            PlanProcessor::Filter(p) => p.id(),
            PlanProcessor::Batch(p) => p.id(),
            PlanProcessor::Encoder(p) => p.id(),
            PlanProcessor::StreamingEncoder(p) => p.id(),
            PlanProcessor::Sink(p) => p.id(),
            PlanProcessor::ResultCollect(p) => p.id(),
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
            PlanProcessor::Batch(p) => p.start(),
            PlanProcessor::Encoder(p) => p.start(),
            PlanProcessor::StreamingEncoder(p) => p.start(),
            PlanProcessor::Sink(p) => p.start(),
            PlanProcessor::ResultCollect(p) => p.start(),
        }
    }

    /// Subscribe to the processor's output stream
    pub fn subscribe_output(&self) -> Option<broadcast::Receiver<crate::processor::StreamData>> {
        match self {
            PlanProcessor::DataSource(p) => p.subscribe_output(),
            PlanProcessor::SharedSource(p) => p.subscribe_output(),
            PlanProcessor::Project(p) => p.subscribe_output(),
            PlanProcessor::Filter(p) => p.subscribe_output(),
            PlanProcessor::Batch(p) => p.subscribe_output(),
            PlanProcessor::Encoder(p) => p.subscribe_output(),
            PlanProcessor::StreamingEncoder(p) => p.subscribe_output(),
            PlanProcessor::Sink(p) => p.subscribe_output(),
            PlanProcessor::ResultCollect(p) => p.subscribe_output(),
        }
    }

    /// Subscribe to the processor's control output stream
    pub fn subscribe_control_output(&self) -> Option<broadcast::Receiver<ControlSignal>> {
        match self {
            PlanProcessor::DataSource(p) => p.subscribe_control_output(),
            PlanProcessor::SharedSource(p) => p.subscribe_control_output(),
            PlanProcessor::Project(p) => p.subscribe_control_output(),
            PlanProcessor::Filter(p) => p.subscribe_control_output(),
            PlanProcessor::Batch(p) => p.subscribe_control_output(),
            PlanProcessor::Encoder(p) => p.subscribe_control_output(),
            PlanProcessor::StreamingEncoder(p) => p.subscribe_control_output(),
            PlanProcessor::Sink(p) => p.subscribe_control_output(),
            PlanProcessor::ResultCollect(p) => p.subscribe_control_output(),
        }
    }

    /// Add an input channel
    pub fn add_input(&mut self, receiver: broadcast::Receiver<crate::processor::StreamData>) {
        match self {
            PlanProcessor::DataSource(p) => p.add_input(receiver),
            PlanProcessor::SharedSource(p) => p.add_input(receiver),
            PlanProcessor::Project(p) => p.add_input(receiver),
            PlanProcessor::Filter(p) => p.add_input(receiver),
            PlanProcessor::Batch(p) => p.add_input(receiver),
            PlanProcessor::Encoder(p) => p.add_input(receiver),
            PlanProcessor::StreamingEncoder(p) => p.add_input(receiver),
            PlanProcessor::Sink(p) => p.add_input(receiver),
            PlanProcessor::ResultCollect(p) => p.add_input(receiver),
        }
    }

    /// Add a control input channel
    pub fn add_control_input(&mut self, receiver: broadcast::Receiver<ControlSignal>) {
        match self {
            PlanProcessor::DataSource(p) => p.add_control_input(receiver),
            PlanProcessor::SharedSource(p) => p.add_control_input(receiver),
            PlanProcessor::Project(p) => p.add_control_input(receiver),
            PlanProcessor::Filter(p) => p.add_control_input(receiver),
            PlanProcessor::Batch(p) => p.add_control_input(receiver),
            PlanProcessor::Encoder(p) => p.add_control_input(receiver),
            PlanProcessor::StreamingEncoder(p) => p.add_control_input(receiver),
            PlanProcessor::Sink(p) => p.add_control_input(receiver),
            PlanProcessor::ResultCollect(p) => p.add_control_input(receiver),
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
    /// Result sink processor (data tail) if downstream forwarding is enabled
    pub result_sink: Option<ResultCollectProcessor>,
    /// Broadcast sender feeding the control source data input
    data_input_sender: broadcast::Sender<StreamData>,
    /// Buffered receiver that bridges external input into the data input sender
    data_input_buffer: Option<mpsc::Receiver<StreamData>>,
    /// Broadcast sender delivering external control signals
    control_signal_sender: broadcast::Sender<ControlSignal>,
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
        // Start from downstream to upstream so that consumers are ready before producers.
        if let Some(result_sink) = &mut self.result_sink {
            self.handles.push(result_sink.start());
        }
        let len = self.middle_processors.len();
        for idx in (0..len).rev() {
            if !matches!(self.middle_processors[idx], PlanProcessor::DataSource(_)) {
                self.handles.push(self.middle_processors[idx].start());
            }
        }
        for idx in (0..len).rev() {
            if matches!(self.middle_processors[idx], PlanProcessor::DataSource(_)) {
                self.handles.push(self.middle_processors[idx].start());
            }
        }
        self.handles.push(self.control_source.start());
        if let Some(buffer) = self.data_input_buffer.take() {
            let sender = self.data_input_sender.clone();
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
    }

    /// Broadcast a control signal into the pipeline, respecting its channel target.
    pub fn broadcast_control_signal(&self, signal: ControlSignal) -> Result<(), ProcessorError> {
        self.control_signal_sender
            .send(signal)
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
/// based on the PhysicalPlan type. All processors are created through PhysicalPlan.
///
/// # Arguments
/// * `plan` - The PhysicalPlan node to create a processor from
///
/// # Returns
/// A ProcessorBuildOutput containing the created processor
struct ProcessorBuildOutput {
    processor: Option<PlanProcessor>,
}

impl ProcessorBuildOutput {
    fn with_processor(processor: PlanProcessor) -> Self {
        Self {
            processor: Some(processor),
        }
    }
}

fn create_processor_from_plan_node(
    plan: &Arc<PhysicalPlan>,
) -> Result<ProcessorBuildOutput, ProcessorError> {
    let plan_name = plan.get_plan_name();
    match plan.as_ref() {
        PhysicalPlan::DataSource(ds) => {
            let processor =
                DataSourceProcessor::new(&plan_name, ds.source_name().to_string(), ds.schema());
            Ok(ProcessorBuildOutput::with_processor(
                PlanProcessor::DataSource(processor),
            ))
        }
        PhysicalPlan::SharedStream(shared) => {
            let processor =
                SharedStreamProcessor::new(&plan_name, shared.stream_name().to_string());
            Ok(ProcessorBuildOutput::with_processor(
                PlanProcessor::SharedSource(processor),
            ))
        }
        PhysicalPlan::Project(project) => {
            let processor = ProjectProcessor::new(plan_name.clone(), Arc::new(project.clone()));
            Ok(ProcessorBuildOutput::with_processor(
                PlanProcessor::Project(processor),
            ))
        }
        PhysicalPlan::Filter(filter) => {
            let processor = FilterProcessor::new(plan_name.clone(), Arc::new(filter.clone()));
            Ok(ProcessorBuildOutput::with_processor(PlanProcessor::Filter(
                processor,
            )))
        }
        PhysicalPlan::Batch(batch) => {
            let processor = BatchProcessor::new(
                plan_name.clone(),
                batch.common.batch_count,
                batch.common.batch_duration,
            );
            Ok(ProcessorBuildOutput::with_processor(PlanProcessor::Batch(
                processor,
            )))
        }
        PhysicalPlan::Encoder(encoder) => {
            let encoder_impl = instantiate_encoder(&encoder.encoder);
            let processor = EncoderProcessor::new(plan_name.clone(), encoder_impl);
            Ok(ProcessorBuildOutput::with_processor(
                PlanProcessor::Encoder(processor),
            ))
        }
        PhysicalPlan::StreamingEncoder(streaming) => {
            let encoder_impl = instantiate_encoder(&streaming.encoder);
            let processor = StreamingEncoderProcessor::new(
                plan_name.clone(),
                encoder_impl,
                streaming.common.batch_count,
                streaming.common.batch_duration,
            );
            Ok(ProcessorBuildOutput::with_processor(
                PlanProcessor::StreamingEncoder(processor),
            ))
        }
        PhysicalPlan::DataSink(sink_plan) => {
            let processor_id = format!(
                "{}_{}",
                plan_name,
                sink_plan.connector.sink_id
            );
            let mut processor = SinkProcessor::new(processor_id);
            if sink_plan.connector.forward_to_result {
                processor.enable_result_forwarding();
            } else {
                processor.disable_result_forwarding();
            }
            let connector_impl = instantiate_connector(
                &sink_plan.connector.sink_id,
                &sink_plan.connector.connector,
            )?;
            processor.add_connector(connector_impl);
            Ok(ProcessorBuildOutput::with_processor(PlanProcessor::Sink(
                processor,
            )))
        }
        PhysicalPlan::ResultCollect(_result_collect) => {
            let processor = ResultCollectProcessor::new(plan_name.clone());
            Ok(ProcessorBuildOutput::with_processor(
                PlanProcessor::ResultCollect(processor),
            ))
        }
    }
}

/// Internal structure to track processors created from PhysicalPlan nodes
struct ProcessorMap {
    /// Map from plan name to processor
    processors: std::collections::HashMap<String, PlanProcessor>,
    /// Tracks whether a plan node has already been visited
    visited: std::collections::HashSet<String>,
}

impl ProcessorMap {
    fn new() -> Self {
        Self {
            processors: std::collections::HashMap::new(),
            visited: std::collections::HashSet::new(),
        }
    }

    fn get_processor(&self, plan_name: &str) -> Option<&PlanProcessor> {
        self.processors.get(plan_name)
    }

    fn get_processor_mut(&mut self, plan_name: &str) -> Option<&mut PlanProcessor> {
        self.processors.get_mut(plan_name)
    }

    fn insert_processor(&mut self, plan_name: String, processor: PlanProcessor) {
        self.processors.insert(plan_name, processor);
    }

    fn get_all_processors(self) -> Vec<PlanProcessor> {
        self.processors.into_values().collect()
    }

    fn mark_visited(&mut self, plan_name: &str) -> bool {
        self.visited.insert(plan_name.to_string())
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
    let plan_name = plan.get_plan_name();
    if !processor_map.mark_visited(&plan_name) {
        return Ok(());
    }

    // Create processor for current node
    let creation = create_processor_from_plan_node(&plan)?;
    if let Some(processor) = creation.processor {
        processor_map.insert_processor(plan_name, processor);
    }

    // Recursively process children
    for child in plan.children() {
        build_processors_recursive(Arc::clone(child), processor_map)?;
    }

    Ok(())
}

/// Collect leaf node indices from PhysicalPlan tree
fn collect_leaf_indices(plan: Arc<PhysicalPlan>) -> Vec<i64> {
    use std::collections::HashSet;
    fn helper(
        plan: Arc<PhysicalPlan>,
        leaves: &mut HashSet<i64>,
        visited: &mut HashSet<i64>,
    ) {
        let index = plan.get_plan_index();
        if !visited.insert(index) {
            return;
        }
        if plan.children().is_empty() {
            leaves.insert(index);
        } else {
            for child in plan.children() {
                helper(Arc::clone(child), leaves, visited);
            }
        }
    }

    let mut leaves = HashSet::new();
    let mut visited = HashSet::new();
    helper(plan, &mut leaves, &mut visited);
    leaves.into_iter().collect()
}

/// Collect parent-child relationships from PhysicalPlan tree
fn collect_parent_child_relations(plan: Arc<PhysicalPlan>) -> Vec<(i64, i64)> {
    use std::collections::HashSet;
    fn helper(
        plan: Arc<PhysicalPlan>,
        relations: &mut HashSet<(i64, i64)>,
        visited: &mut HashSet<i64>,
    ) {
        let parent_index = plan.get_plan_index();
        if !visited.insert(parent_index) {
            return;
        }
        for child in plan.children() {
            let child_index = child.get_plan_index();
            relations.insert((parent_index, child_index));
            helper(Arc::clone(child), relations, visited);
        }
    }

    let mut relations = HashSet::new();
    let mut visited = HashSet::new();
    helper(plan, &mut relations, &mut visited);
    relations.into_iter().collect()
}

/// Build a mapping from plan index to plan name for all nodes in the PhysicalPlan tree
fn build_index_to_name_mapping(
    plan: &Arc<PhysicalPlan>,
    mapping: &mut std::collections::HashMap<i64, String>,
) {
    let plan_index = plan.get_plan_index();
    let plan_name = plan.get_plan_name();
    mapping.insert(plan_index, plan_name);
    
    // Recursively process children
    for child in plan.children() {
        build_index_to_name_mapping(child, mapping);
    }
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
    // Build index to name mapping for quick lookup
    let mut index_to_name_map: std::collections::HashMap<i64, String> = std::collections::HashMap::new();
    build_index_to_name_mapping(&physical_plan, &mut index_to_name_map);

    // 1. Connect ControlSourceProcessor to all leaf nodes
    let leaf_indices = collect_leaf_indices(Arc::clone(&physical_plan));
    for leaf_index in leaf_indices {
        if let Some(leaf_plan_name) = index_to_name_map.get(&leaf_index) {
            if let Some(processor) = processor_map.get_processor_mut(leaf_plan_name) {
                let receiver = control_source.subscribe_output().ok_or_else(|| {
                    ProcessorError::InvalidConfiguration("control source output unavailable".into())
                })?;
                processor.add_input(receiver);
                if let Some(control_rx) = control_source.subscribe_control_output() {
                    processor.add_control_input(control_rx);
                }
            }
        }
    }

    // 2. Connect children outputs to parent inputs
    let relations = collect_parent_child_relations(Arc::clone(&physical_plan));
    
    // Debug: Print connection relationships
    // println!("=== Processor Connection Relationships ===");
    // let mut relation_counts: std::collections::HashMap<i64, usize> = std::collections::HashMap::new();
    // for (parent_idx, child_idx) in &relations {
    //     *relation_counts.entry(*child_idx).or_insert(0) += 1;
    //     if let (Some(child_name), Some(parent_name)) = (index_to_name_map.get(child_idx), index_to_name_map.get(parent_idx)) {
    //         println!("  {} (index: {}) -> {} (index: {})", child_name, child_idx, parent_name, parent_idx);
    //     }
    // }
    // println!("Child processor subscription counts:");
    // for (child_idx, count) in relation_counts {
    //     if let Some(child_name) = index_to_name_map.get(&child_idx) {
    //         println!("  {} (index: {}): {} parent(s)", child_name, child_idx, count);
    //     }
    // }
    // println!("=========================================");
    
    for (parent_index, child_index) in relations {
        if let (Some(child_plan_name), Some(parent_plan_name)) = (
            index_to_name_map.get(&child_index),
            index_to_name_map.get(&parent_index)
        ) {
            println!("Connecting {} -> {}", child_plan_name, parent_plan_name);
            
            let receiver = processor_map
                .get_processor(child_plan_name)
                .and_then(|proc| proc.subscribe_output())
                .ok_or_else(|| {
                    ProcessorError::InvalidConfiguration(format!(
                        "Processor {} has no broadcast output",
                        child_plan_name
                    ))
                })?;

            let control_receiver = processor_map
                .get_processor(child_plan_name)
                .and_then(|proc| proc.subscribe_control_output());
            if let Some(parent_processor) = processor_map.get_processor_mut(parent_plan_name) {
                parent_processor.add_input(receiver);
                if let Some(control_rx) = control_receiver {
                    parent_processor.add_control_input(control_rx);
                }
            }
        }
    }

    Ok(())
}

/// Create a complete processor pipeline from a PhysicalPlan tree.
///
/// The provided plan is expected to terminate in a `PhysicalDataSink` node that
/// carries the declarative sink configuration.
pub fn create_processor_pipeline(
    physical_plan: Arc<PhysicalPlan>,
) -> Result<ProcessorPipeline, ProcessorError> {
    // Print the PhysicalPlan topology structure for debugging
    println!("=== PhysicalPlan Topology Structure ===");
    physical_plan.print_topology(0);
    println!("======================================");

    let mut control_source = ControlSourceProcessor::new("control_source");
    let (pipeline_input_sender, pipeline_input_receiver) = mpsc::channel(100);
    let (data_input_sender, data_input_receiver) =
        broadcast::channel(crate::processor::base::DEFAULT_CHANNEL_CAPACITY);
    control_source.add_input(data_input_receiver);
    let (control_signal_sender, control_signal_receiver) =
        broadcast::channel(crate::processor::base::DEFAULT_CHANNEL_CAPACITY);
    control_source.add_control_input(control_signal_receiver);

    let mut processor_map = ProcessorMap::new();
    build_processors_recursive(Arc::clone(&physical_plan), &mut processor_map)?;

    connect_processors(
        Arc::clone(&physical_plan),
        &mut processor_map,
        &mut control_source,
    )?;

    // Set up pipeline output from ResultCollect processor if present
    let mut pipeline_output_receiver = None;
    let mut result_sink = None;

    // Get all processors first
    let mut middle_processors = processor_map.get_all_processors();

    // Extract ResultCollect processor (if any) to serve as pipeline output
    // In multi-sink scenarios, there should be only one top-level ResultCollect processor
    if let Some(pos) = middle_processors
        .iter()
        .position(|p| matches!(p, PlanProcessor::ResultCollect(_)))
    {
        if let PlanProcessor::ResultCollect(mut collector) = middle_processors.swap_remove(pos) {
            let (result_output_sender, pipeline_output_rx) = mpsc::channel(100);
            collector.set_output(result_output_sender);
            pipeline_output_receiver = Some(pipeline_output_rx);
            result_sink = Some(collector);
        }
    }
    let pipeline_id = Uuid::new_v4().to_string();
    for processor in &mut middle_processors {
        processor.set_pipeline_id(&pipeline_id);
    }

    Ok(ProcessorPipeline {
        input: pipeline_input_sender,
        output: pipeline_output_receiver,
        control_source,
        middle_processors,
        result_sink,
        data_input_sender,
        data_input_buffer: Some(pipeline_input_receiver),
        control_signal_sender,
        handles: Vec::new(),
        pipeline_id,
    })
}

fn instantiate_encoder(cfg: &SinkEncoderConfig) -> Arc<dyn CollectionEncoder> {
    match cfg {
        SinkEncoderConfig::Json { encoder_id } => Arc::new(JsonEncoder::new(encoder_id.clone())),
    }
}

fn instantiate_connector(
    sink_id: &str,
    cfg: &SinkConnectorConfig,
) -> Result<Box<dyn SinkConnector>, ProcessorError> {
    match cfg {
        SinkConnectorConfig::Mqtt(mqtt_cfg) => Ok(Box::new(MqttSinkConnector::new(
            sink_id.to_string(),
            mqtt_cfg.clone(),
        ))),
        SinkConnectorConfig::Nop(_) => {
            Ok(Box::new(NopSinkConnector::new(sink_id.to_string())))
        }
    }
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
        let result =
            create_processor_from_plan_node(&physical_project).expect("processor creation failed");

        let processor = result
            .processor
            .expect("expected processor for physical project node");
        assert_eq!(processor.id(), "PhysicalProject_1");
        println!(
            "✅ SUCCESS: PhysicalProject processor created with ID: {}",
            processor.id()
        );
    }
}
