use std::time::Duration;

use crate::pipeline::{
    CreatePipelineRequest, CreatePipelineResult, ExplainPipelineTarget, PipelineError,
    PipelineRuntimeFailure, PipelineSnapshot, PipelineStopMode,
};
use crate::processor::ProcessorPipeline;
use crate::processor::ProcessorStatsEntry;
use crate::{PipelineExplain, PipelineSink};
use std::sync::Arc;

use super::FlowInstance;

impl FlowInstance {
    pub fn create_pipeline(
        &self,
        request: CreatePipelineRequest,
    ) -> Result<CreatePipelineResult, PipelineError> {
        self.pipeline_manager.create_pipeline(request)
    }

    /// Start a pipeline by identifier.
    pub async fn start_pipeline(&self, id: &str) -> Result<(), PipelineError> {
        self.pipeline_manager.start_pipeline(id).await
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

    pub fn mark_pipeline_failed(&self, id: &str) -> Result<(), PipelineError> {
        self.pipeline_manager.mark_pipeline_failed(id)
    }

    pub fn set_pipeline_failure_reporter(
        &self,
        reporter: Arc<dyn Fn(PipelineRuntimeFailure) + Send + Sync>,
    ) {
        self.pipeline_manager
            .set_pipeline_failure_reporter(reporter);
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

    pub fn explain_pipeline(
        &self,
        target: ExplainPipelineTarget<'_>,
    ) -> Result<PipelineExplain, PipelineError> {
        self.pipeline_manager.explain_pipeline(target)
    }

    /// Build a processor pipeline directly without registering it.
    pub fn build_pipeline(
        &self,
        sql: &str,
        sinks: Vec<PipelineSink>,
    ) -> Result<ProcessorPipeline, Box<dyn std::error::Error>> {
        let registries = self.pipeline_registries();
        crate::create_pipeline(
            sql,
            sinks,
            &self.catalog,
            self.shared_stream_registry.clone(),
            self.mqtt_client_manager.clone(),
            self.spawner.clone(),
            &registries,
        )
    }
}
