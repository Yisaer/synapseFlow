use std::time::Duration;

use crate::pipeline::{PipelineDefinition, PipelineError, PipelineSnapshot, PipelineStopMode};
use crate::processor::ProcessorPipeline;
use crate::processor::ProcessorStatsEntry;
use crate::{create_pipeline, create_pipeline_with_log_sink};
use crate::{PipelineExplain, PipelineSink};

use super::FlowInstance;

impl FlowInstance {
    pub fn create_pipeline_with_plan_cache(
        &self,
        definition: PipelineDefinition,
        inputs: crate::planner::plan_cache::PlanCacheInputs,
    ) -> Result<crate::planner::plan_cache::PlanCacheBuildResult, PipelineError> {
        self.pipeline_manager
            .create_pipeline_with_plan_cache(definition, inputs)
    }

    /// Internal API used by the plan cache write-back path.
    ///
    /// Prefer `create_pipeline_with_plan_cache` for user-facing pipeline creation.
    #[doc(hidden)]
    pub fn create_pipeline_with_logical_ir(
        &self,
        definition: PipelineDefinition,
    ) -> Result<(PipelineSnapshot, Vec<u8>), PipelineError> {
        self.pipeline_manager
            .create_pipeline_with_logical_ir(definition)
    }

    /// Start a pipeline by identifier.
    pub fn start_pipeline(&self, id: &str) -> Result<(), PipelineError> {
        self.pipeline_manager.start_pipeline(id)
    }

    /// Stop a pipeline by identifier.
    pub async fn stop_pipeline(
        &self,
        id: &str,
        mode: PipelineStopMode,
        timeout: Duration,
    ) -> Result<(), PipelineError> {
        self.pipeline_manager.stop_pipeline(id, mode, timeout).await
    }

    /// Stop and delete a pipeline.
    pub async fn delete_pipeline(&self, id: &str) -> Result<(), PipelineError> {
        self.pipeline_manager.delete_pipeline(id).await
    }

    pub async fn collect_pipeline_stats(
        &self,
        id: &str,
        timeout: Duration,
    ) -> Result<Vec<ProcessorStatsEntry>, PipelineError> {
        self.pipeline_manager.collect_stats(id, timeout).await
    }

    /// Retrieve pipeline snapshots.
    pub fn list_pipelines(&self) -> Vec<PipelineSnapshot> {
        self.pipeline_manager.list()
    }

    /// Explain an existing pipeline by id (logical + physical plans).
    pub fn explain_pipeline(&self, id: &str) -> Result<PipelineExplain, PipelineError> {
        self.pipeline_manager.explain_pipeline(id)
    }

    /// Explain a pipeline definition without registering it.
    pub fn explain_pipeline_definition(
        &self,
        definition: &PipelineDefinition,
    ) -> Result<PipelineExplain, PipelineError> {
        self.pipeline_manager
            .explain_pipeline_definition(definition)
    }

    /// Build a processor pipeline directly without registering it.
    pub fn build_pipeline(
        &self,
        sql: &str,
        sinks: Vec<PipelineSink>,
    ) -> Result<ProcessorPipeline, Box<dyn std::error::Error>> {
        let registries = self.pipeline_registries();
        create_pipeline(
            sql,
            sinks,
            &self.catalog,
            self.shared_stream_registry,
            self.mqtt_client_manager.clone(),
            &registries,
        )
    }

    /// Build a processor pipeline wired to a default logging sink.
    pub fn build_pipeline_with_log_sink(
        &self,
        sql: &str,
        forward_to_result: bool,
    ) -> Result<ProcessorPipeline, Box<dyn std::error::Error>> {
        let registries = self.pipeline_registries();
        create_pipeline_with_log_sink(
            sql,
            forward_to_result,
            &self.catalog,
            self.shared_stream_registry,
            self.mqtt_client_manager.clone(),
            &registries,
        )
    }
}
