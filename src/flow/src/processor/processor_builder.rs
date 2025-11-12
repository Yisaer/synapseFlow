//! Processor builder - converts physical plans to stream processors
//! 
//! This module builds a tree of StreamProcessors from a PhysicalPlan tree,
//! following the pattern of rstream's ExecutorBuilder.

use std::sync::Arc;
use tokio::sync::broadcast;
use crate::planner::physical::PhysicalPlan;
use crate::processor::{StreamProcessor, DataSourceProcessor, FilterProcessor, ProjectProcessor};
use crate::processor::stream_data::StreamData;

/// Result of building a processor - contains the processor and its output channel
pub struct ProcessorBuildResult {
    /// The built processor
    pub processor: Arc<dyn StreamProcessor>,
    /// Channel for receiving data from this processor
    pub output_receiver: broadcast::Receiver<Result<StreamData, String>>,
}

impl std::fmt::Debug for ProcessorBuildResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProcessorBuildResult")
            .field("processor", &"<dyn StreamProcessor>")
            .field("output_receiver", &"<Receiver>")
            .finish()
    }
}

/// Builder for creating StreamProcessors from PhysicalPlans
pub struct ProcessorBuilder;

impl ProcessorBuilder {
    /// Build a processor from a physical plan
    pub fn build_processor(&self, physical_plan: Arc<dyn PhysicalPlan>) -> Result<ProcessorBuildResult, String> {
        match physical_plan.get_plan_type() {
            "PhysicalDataSource" => self.build_datasource_processor(physical_plan),
            "PhysicalFilter" => self.build_filter_processor(physical_plan),
            "PhysicalProject" => self.build_project_processor(physical_plan),
            _ => Err(format!("Unknown physical plan type: {}", physical_plan.get_plan_type())),
        }
    }
    
    /// Build a DataSourceProcessor from PhysicalDataSource
    fn build_datasource_processor(&self, physical_plan: Arc<dyn PhysicalPlan>) -> Result<ProcessorBuildResult, String> {
        // For now, assume 1 downstream processor
        let downstream_count = 1;
        let input_receivers = Vec::new(); // Data source has no inputs
        
        let processor = Arc::new(DataSourceProcessor::new(
            physical_plan.clone(),
            input_receivers,
            downstream_count,
        ));
        
        let processor_view = processor.start();
        
        Ok(ProcessorBuildResult {
            processor,
            output_receiver: processor_view.result_resubscribe(),
        })
    }
    
    /// Build a FilterProcessor from PhysicalFilter
    fn build_filter_processor(&self, physical_plan: Arc<dyn PhysicalPlan>) -> Result<ProcessorBuildResult, String> {
        // For now, assume 1 downstream processor
        let downstream_count = 1;
        // For now, no input receivers - this will be connected later
        let input_receivers = Vec::new();
        
        let processor = Arc::new(FilterProcessor::new(
            physical_plan.clone(),
            input_receivers,
            downstream_count,
        ));
        
        let processor_view = processor.start();
        
        Ok(ProcessorBuildResult {
            processor,
            output_receiver: processor_view.result_resubscribe(),
        })
    }
    
    /// Build a ProjectProcessor from PhysicalProject
    fn build_project_processor(&self, physical_plan: Arc<dyn PhysicalPlan>) -> Result<ProcessorBuildResult, String> {
        // For now, assume 1 downstream processor
        let downstream_count = 1;
        // For now, no input receivers - this will be connected later
        let input_receivers = Vec::new();
        
        let processor = Arc::new(ProjectProcessor::new(
            physical_plan.clone(),
            input_receivers,
            downstream_count,
        ));
        
        let processor_view = processor.start();
        
        Ok(ProcessorBuildResult {
            processor,
            output_receiver: processor_view.result_resubscribe(),
        })
    }
}