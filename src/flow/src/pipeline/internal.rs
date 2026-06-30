use super::*;
use crate::catalog::{Catalog, StreamDefinition, StreamProps};
use crate::connector::sink::file::{
    FileRetentionConfig as FileSinkRetentionConfig, FileSinkConfig,
};
use crate::connector::sink::http::{HttpMethod, HttpRetryConfig, HttpSinkConfig};
use crate::connector::sink::video::{
    VideoCodecConfig, VideoContainerConfig, VideoFileSinkConfig,
    VideoRollingConfig as VideoSinkRollingConfig, VideoSinkTargetConfig,
};
use crate::connector::source::video::{VideoReconnectRuntimeConfig, VideoRtspTransportConfig};
use crate::connector::{
    HistorySourceConfig, HistorySourceConnector, KuksaSinkConfig, KuraSinkConfig, MemorySinkConfig,
    MemorySourceConfig, MemorySourceConnector, MemoryTopicKind, MockSourceConnector,
    MqttSinkConfig, MqttSourceConfig, MqttSourceConnector, NngPubSubSourceConfig, VideoSinkConfig,
    VideoSourceConfig, VideoSourceConnector,
};
#[cfg(feature = "nng_pubsub")]
use crate::connector::{NngPubSubSinkConfig, NngPubSubSourceConnector};
use crate::expr::sql_conversion::{SchemaBinding, SchemaBindingEntry, SourceBindingKind};
use crate::planner::logical::create_logical_plan_with_source_inputs;
use crate::planner::sink::SinkEncoderKind;
use crate::processor::processor_builder::{PlanProcessor, ProcessorPipeline};
use crate::processor::EventtimePipelineContext;
use crate::processor::Processor;
use crate::processor::ProcessorStatsEntry;
use crate::processor::{
    create_processor_pipeline, ProcessorPipelineDependencies, ProcessorPipelineOptions,
};
use crate::{
    explain_pipeline_definition_with_options, optimize_physical_plan, PipelineExplain,
    PipelineRegistries, PipelineSink, PipelineSinkConnector, SinkConnectorConfig,
};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn validate_eventtime_enabled(
    stream_definitions: &HashMap<String, Arc<StreamDefinition>>,
    registries: &PipelineRegistries,
) -> Result<(), String> {
    let registry = registries.eventtime_type_registry();
    let mut saw_eventtime = false;
    for (stream, definition) in stream_definitions {
        let Some(eventtime) = definition.eventtime() else {
            continue;
        };
        saw_eventtime = true;
        let column = eventtime.column();
        if !definition.schema().contains_column(column) {
            return Err(format!(
                "eventtime.column `{}` not found in stream `{}` schema",
                column, stream
            ));
        }
        let type_key = eventtime.eventtime_type();
        if !registry.is_registered(type_key) {
            let available = registry.list().join(", ");
            return Err(format!(
                "eventtime.type `{}` not registered (available: {})",
                type_key, available
            ));
        }
    }
    if !saw_eventtime {
        return Err("eventtime.enabled=true but no stream declares eventtime".to_string());
    }
    Ok(())
}

const DEFAULT_SINK_TOPIC: &str = "/yisa/data2";

struct PipelineBuildLog {
    flow_instance_id: String,
    pipeline_id: String,
    sink_count: usize,
    sql_len: usize,
    eventtime_enabled: bool,
    data_channel_capacity: usize,
    started_at: Instant,
}

impl PipelineBuildLog {
    fn new(definition: &PipelineDefinition, context: &PipelineContext) -> Self {
        let log = Self {
            flow_instance_id: context.flow_instance_id().to_string(),
            pipeline_id: definition.id().to_string(),
            sink_count: definition.sinks().len(),
            sql_len: definition.sql().len(),
            eventtime_enabled: definition.options().eventtime.enabled,
            data_channel_capacity: definition.options().data_channel_capacity,
            started_at: Instant::now(),
        };
        tracing::info!(
            flow_instance_id = %log.flow_instance_id,
            pipeline_id = %log.pipeline_id,
            phase = "build_pipeline_runtime",
            result = "started",
            sink_count = log.sink_count,
            sql_len = log.sql_len,
            eventtime_enabled = log.eventtime_enabled,
            data_channel_capacity = log.data_channel_capacity,
            "pipeline build"
        );
        log
    }

    fn run_phase<T, E, F>(&self, phase: &'static str, f: F) -> Result<T, E>
    where
        E: Display,
        F: FnOnce() -> Result<T, E>,
    {
        let started_at = Instant::now();
        match f() {
            Ok(value) => {
                tracing::info!(
                    flow_instance_id = %self.flow_instance_id,
                    pipeline_id = %self.pipeline_id,
                    phase,
                    result = "succeeded",
                    elapsed_ms = started_at.elapsed().as_millis(),
                    "pipeline build phase"
                );
                Ok(value)
            }
            Err(err) => {
                tracing::error!(
                    flow_instance_id = %self.flow_instance_id,
                    pipeline_id = %self.pipeline_id,
                    phase,
                    result = "failed",
                    elapsed_ms = started_at.elapsed().as_millis(),
                    error = %err,
                    "pipeline build phase"
                );
                Err(err)
            }
        }
    }

    fn log_success(&self, stream_count: usize, processor_count: usize) {
        tracing::info!(
            flow_instance_id = %self.flow_instance_id,
            pipeline_id = %self.pipeline_id,
            phase = "build_pipeline_runtime",
            result = "succeeded",
            elapsed_ms = self.started_at.elapsed().as_millis(),
            stream_count,
            sink_count = self.sink_count,
            processor_count,
            sql_len = self.sql_len,
            eventtime_enabled = self.eventtime_enabled,
            data_channel_capacity = self.data_channel_capacity,
            "pipeline build"
        );
    }

    fn log_failure(&self, error: &(impl Display + ?Sized)) {
        tracing::error!(
            flow_instance_id = %self.flow_instance_id,
            pipeline_id = %self.pipeline_id,
            phase = "build_pipeline_runtime",
            result = "failed",
            elapsed_ms = self.started_at.elapsed().as_millis(),
            sink_count = self.sink_count,
            sql_len = self.sql_len,
            eventtime_enabled = self.eventtime_enabled,
            data_channel_capacity = self.data_channel_capacity,
            error = %error,
            "pipeline build"
        );
    }
}

pub(super) struct ManagedPipeline {
    definition: Arc<PipelineDefinition>,
    pipeline: Option<ProcessorPipeline>,
    streams: Vec<String>,
    status: PipelineStatus,
}

impl ManagedPipeline {
    fn snapshot(&self) -> PipelineSnapshot {
        PipelineSnapshot {
            definition: Arc::clone(&self.definition),
            streams: self.streams.clone(),
            status: self.status,
        }
    }
}

impl PipelineManager {
    pub(crate) fn new(
        catalog: Arc<Catalog>,
        context: PipelineContext,
        registries: PipelineRegistries,
    ) -> Self {
        Self {
            pipelines: RwLock::new(HashMap::new()),
            catalog,
            context,
            registries: RwLock::new(registries),
        }
    }

    pub(crate) fn replace_registries(&self, registries: PipelineRegistries) {
        *self.registries.write() = registries;
    }

    pub(crate) fn create_pipeline(
        &self,
        request: CreatePipelineRequest,
    ) -> Result<CreatePipelineResult, PipelineError> {
        let definition = request.definition;
        let pipeline_id = definition.id().to_string();

        {
            let guard = self.pipelines.read();
            if guard.contains_key(&pipeline_id) {
                return Err(PipelineError::AlreadyExists(pipeline_id));
            }
        }

        let snapshot = self.create_pipeline_from_sql(definition)?;
        Ok(CreatePipelineResult { snapshot })
    }

    fn create_pipeline_from_sql(
        &self,
        definition: PipelineDefinition,
    ) -> Result<PipelineSnapshot, PipelineError> {
        let pipeline_id = definition.id().to_string();
        let registries = self.registries.read();
        let (pipeline, streams) =
            build_pipeline_runtime(&definition, &self.catalog, &self.context, &registries)
                .map_err(PipelineError::BuildFailure)?;
        let mut guard = self.pipelines.write();
        if guard.contains_key(&pipeline_id) {
            return Err(PipelineError::AlreadyExists(pipeline_id));
        }
        let entry = ManagedPipeline {
            definition: Arc::new(definition),
            pipeline: Some(pipeline),
            streams,
            status: PipelineStatus::Stopped,
        };
        let snapshot = entry.snapshot();
        guard.insert(pipeline_id, entry);
        Ok(snapshot)
    }

    /// List all managed pipelines.
    pub fn list(&self) -> Vec<PipelineSnapshot> {
        let guard = self.pipelines.read();
        guard.values().map(|entry| entry.snapshot()).collect()
    }

    pub(crate) fn explain_pipeline(
        &self,
        target: ExplainPipelineTarget<'_>,
    ) -> Result<PipelineExplain, PipelineError> {
        let definition = match target {
            ExplainPipelineTarget::Id(pipeline_id) => {
                let guard = self.pipelines.read();
                let entry = guard
                    .get(pipeline_id)
                    .ok_or_else(|| PipelineError::NotFound(pipeline_id.to_string()))?;
                Arc::clone(&entry.definition)
            }
            ExplainPipelineTarget::Definition(definition) => Arc::new(definition.clone()),
        };

        let sinks = build_sinks_from_definition(definition.as_ref())
            .map_err(PipelineError::BuildFailure)?;

        let registries_snapshot = self.registries.read();
        explain_pipeline_definition_with_options(
            definition.as_ref(),
            sinks,
            &self.catalog,
            Some(self.context.shared_stream_registry().as_ref()),
            &registries_snapshot,
            definition.options(),
        )
        .map_err(|err| PipelineError::BuildFailure(err.to_string()))
    }

    /// Start the pipeline runtime if not already running.
    pub async fn start_pipeline(&self, pipeline_id: &str) -> Result<(), PipelineError> {
        let mut pipeline = {
            let mut guard = self.pipelines.write();
            let entry = guard
                .get_mut(pipeline_id)
                .ok_or_else(|| PipelineError::NotFound(pipeline_id.to_string()))?;
            if matches!(entry.status, PipelineStatus::Running) {
                return Ok(());
            }

            if entry.pipeline.is_none() {
                let definition = Arc::clone(&entry.definition);
                let registries = self.registries.read().clone();
                let (pipeline, streams) =
                    build_pipeline_runtime(&definition, &self.catalog, &self.context, &registries)
                        .map_err(PipelineError::BuildFailure)?;
                entry.pipeline = Some(pipeline);
                entry.streams = streams;
            }

            entry
                .pipeline
                .take()
                .ok_or_else(|| PipelineError::Runtime("pipeline runtime missing".to_string()))?
        };

        let start_result = pipeline
            .start()
            .await
            .map_err(|err| PipelineError::Runtime(err.to_string()));

        let mut guard = self.pipelines.write();
        let entry = guard
            .get_mut(pipeline_id)
            .ok_or_else(|| PipelineError::NotFound(pipeline_id.to_string()))?;
        match start_result {
            Ok(()) => {
                entry.pipeline = Some(pipeline);
                entry.status = PipelineStatus::Running;
                Ok(())
            }
            Err(err) => {
                entry.pipeline = None;
                Err(err)
            }
        }
    }

    pub async fn stop_pipeline(
        &self,
        pipeline_id: &str,
        mode: PipelineStopMode,
        timeout: Duration,
    ) -> Result<(), PipelineError> {
        let maybe_pipeline = {
            let mut guard = self.pipelines.write();
            let entry = guard
                .get_mut(pipeline_id)
                .ok_or_else(|| PipelineError::NotFound(pipeline_id.to_string()))?;
            if !matches!(entry.status, PipelineStatus::Running) {
                entry.status = PipelineStatus::Stopped;
                return Ok(());
            }
            entry.status = PipelineStatus::Stopped;
            entry.pipeline.take()
        };

        let pipeline = maybe_pipeline
            .ok_or_else(|| PipelineError::Runtime("pipeline runtime missing".to_string()))?;
        close_pipeline(pipeline, mode, timeout).await
    }

    /// Remove a pipeline runtime and close it if running.
    pub async fn delete_pipeline(&self, pipeline_id: &str) -> Result<(), PipelineError> {
        let maybe_entry = {
            let mut guard = self.pipelines.write();
            guard.remove(pipeline_id)
        };
        let entry = maybe_entry.ok_or_else(|| PipelineError::NotFound(pipeline_id.to_string()))?;
        if matches!(entry.status, PipelineStatus::Running) {
            let pipeline = entry.pipeline.ok_or_else(|| {
                PipelineError::Runtime("pipeline runtime missing for running pipeline".to_string())
            })?;
            close_pipeline(pipeline, PipelineStopMode::Quick, Duration::from_secs(5)).await?;
        }
        Ok(())
    }

    pub async fn collect_stats(
        &self,
        pipeline_id: &str,
        _timeout: Duration,
    ) -> Result<Vec<ProcessorStatsEntry>, PipelineError> {
        let guard = self.pipelines.read();
        let entry = guard
            .get(pipeline_id)
            .ok_or_else(|| PipelineError::NotFound(pipeline_id.to_string()))?;
        if !matches!(entry.status, PipelineStatus::Running) {
            return Err(PipelineError::Runtime(format!(
                "pipeline {pipeline_id} is not running"
            )));
        }
        let pipeline = entry
            .pipeline
            .as_ref()
            .ok_or_else(|| PipelineError::Runtime("pipeline runtime missing".to_string()))?;
        Ok(pipeline
            .processor_stats()
            .iter()
            .map(crate::processor::ProcessorStatsHandle::snapshot)
            .collect())
    }
}

async fn close_pipeline(
    mut pipeline: ProcessorPipeline,
    mode: PipelineStopMode,
    timeout: Duration,
) -> Result<(), PipelineError> {
    let result = match mode {
        PipelineStopMode::Graceful => pipeline.graceful_close(timeout).await,
        PipelineStopMode::Quick => pipeline.quick_close(timeout).await,
    };
    result.map_err(|err| PipelineError::Runtime(err.to_string()))
}

fn build_pipeline_runtime(
    definition: &PipelineDefinition,
    catalog: &Catalog,
    context: &PipelineContext,
    registries: &PipelineRegistries,
) -> Result<(ProcessorPipeline, Vec<String>), String> {
    let build_log = PipelineBuildLog::new(definition, context);
    let result = (|| {
        let select_stmt = build_log.run_phase("parse_sql", || {
            parser::parse_sql_with_registries(
                definition.sql(),
                registries.aggregate_registry(),
                registries.stateful_registry(),
            )
            .map_err(|err| err.to_string())
        })?;
        let streams: Vec<String> = select_stmt
            .source_infos
            .iter()
            .map(|info| info.name.clone())
            .collect();

        let shared_stream_registry = context.shared_stream_registry();
        let mqtt_client_manager = context.mqtt_client_manager();
        let (stream_definitions, binding_entries) = build_log.run_phase("resolve_streams", || {
            let mut stream_definitions = HashMap::new();
            let mut binding_entries = Vec::new();

            for source in &select_stmt.source_infos {
                let definition = catalog
                    .get(&source.name)
                    .ok_or_else(|| format!("stream '{}' not found in catalog", source.name))?;

                if definition.stream_type() == crate::catalog::StreamType::Memory {
                    let StreamProps::Memory(props) = definition.props() else {
                        return Err(format!(
                            "stream '{}' expected memory props but received {:?}",
                            definition.id(),
                            definition.stream_type()
                        ));
                    };

                    let topic = props.topic.trim();
                    if topic.is_empty() {
                        return Err(format!(
                            "stream '{}' memory topic must not be empty",
                            definition.id()
                        ));
                    }

                    let expected_kind = if definition.decoder().kind() == "none" {
                        MemoryTopicKind::Collection
                    } else {
                        MemoryTopicKind::Bytes
                    };
                    let actual_kind = context
                        .memory_pubsub_registry()
                        .topic_kind(topic)
                        .ok_or_else(|| {
                            format!(
                                "memory topic '{}' required by stream '{}' not declared in instance",
                                topic,
                                definition.id()
                            )
                        })?;
                    if actual_kind != expected_kind {
                        return Err(format!(
                            "memory topic '{}' kind mismatch for stream '{}': expected {}, got {}",
                            topic,
                            definition.id(),
                            expected_kind,
                            actual_kind
                        ));
                    }
                }

                let schema = definition.schema();
                let kind = if shared_stream_registry.is_registered_sync(&source.name) {
                    SourceBindingKind::Shared
                } else if definition.stream_type() == crate::catalog::StreamType::Memory
                    && definition.decoder().kind() == "none"
                {
                    SourceBindingKind::MemoryCollection
                } else {
                    SourceBindingKind::Regular
                };
                binding_entries.push(SchemaBindingEntry {
                    source_name: source.name.clone(),
                    alias: source.alias.clone(),
                    schema: Arc::clone(&schema),
                    kind,
                });
                stream_definitions.insert(source.name.clone(), definition);
            }

            Ok((stream_definitions, binding_entries))
        })?;

        let sinks =
            build_log.run_phase("build_sinks", || build_sinks_from_definition(definition))?;
        let source_inputs: HashMap<String, crate::pipeline::SourceInputConfig> = definition
            .sources()
            .iter()
            .map(|source| (source.stream.clone(), source.input.clone()))
            .collect();
        let schema_binding = SchemaBinding::new(binding_entries);
        let (logical_plan, pruned_binding) = build_log.run_phase("build_logical_plan", || {
            let logical_plan = create_logical_plan_with_source_inputs(
                select_stmt,
                sinks,
                &stream_definitions,
                &source_inputs,
            )?;
            Ok::<_, String>(crate::planner::optimize_logical_plan_with_options(
                logical_plan,
                &schema_binding,
                &crate::planner::LogicalOptimizerOptions {
                    eventtime_enabled: definition.options().eventtime.enabled,
                },
            ))
        })?;

        let slot_projection_enabled = definition
            .options()
            .shared_slice_projection_enabled
            .unwrap_or_else(|| {
                crate::planner::physical_plan_builder::shared_slice_projection_enabled()
            });
        let (slot_versions, slot_binding) = if slot_projection_enabled {
            crate::planner::apply_shared_stream_slot_schemas(
                &logical_plan,
                &pruned_binding,
                shared_stream_registry.as_ref(),
                true,
            )
        } else {
            (HashMap::new(), pruned_binding)
        };

        let optimized_plan = build_log.run_phase("build_physical_plan", || {
            let build_options = crate::planner::PhysicalPlanBuildOptions {
                eventtime_enabled: definition.options().eventtime.enabled,
                eventtime_late_tolerance: definition.options().eventtime.late_tolerance,
                shared_slot_versions: slot_versions,
            };
            let physical_plan = crate::planner::create_physical_plan_with_build_options(
                Arc::clone(&logical_plan),
                &slot_binding,
                registries,
                &build_options,
            )?;
            Ok::<_, String>(optimize_physical_plan(
                Arc::clone(&physical_plan),
                registries.encoder_registry().as_ref(),
                registries.aggregate_registry(),
            ))
        })?;

        let eventtime = build_log.run_phase("prepare_eventtime", || {
            if definition.options().eventtime.enabled {
                validate_eventtime_enabled(&stream_definitions, registries)?;
            }
            let eventtime = if definition.options().eventtime.enabled {
                let mut per_source = HashMap::new();
                for (name, def) in &stream_definitions {
                    if let Some(cfg) = def.eventtime() {
                        per_source.insert(name.clone(), cfg.clone());
                    }
                }
                Some(EventtimePipelineContext {
                    enabled: true,
                    registry: registries.eventtime_type_registry(),
                    per_source,
                })
            } else {
                None
            };
            Ok::<_, String>(eventtime)
        })?;
        // // Pipeline explain is exposed via REST; avoid generating/logging it in release builds.
        // #[cfg(debug_assertions)]
        // {
        //     let shared_stream_decode_applied = shared_stream_decode_applied_snapshot(
        //         &optimized_plan,
        //         shared_stream_registry.as_ref(),
        //     );
        //     let explain = PipelineExplain::new(
        //         Arc::clone(&logical_plan),
        //         Arc::clone(&optimized_plan),
        //         PipelineExplainConfig {
        //             pipeline_options: Some(crate::planner::explain::PipelineExplainOptions {
        //                 eventtime_enabled: definition.options().eventtime.enabled,
        //                 eventtime_late_tolerance_ms: definition
        //                     .options()
        //                     .eventtime
        //                     .late_tolerance
        //                     .as_millis(),
        //             }),
        //             shared_stream_decode_applied,
        //         },
        //     );
        //     tracing::info!(explain = %explain.to_pretty_string(), "pipeline explain");
        // }

        let mut pipeline = build_log.run_phase("create_processor_pipeline", || {
            create_processor_pipeline(
                optimized_plan,
                ProcessorPipelineDependencies::new(
                    context.flow_instance_id(),
                    mqtt_client_manager.clone(),
                    Arc::clone(&shared_stream_registry),
                    registries,
                    eventtime,
                    context.spawner().clone(),
                ),
                ProcessorPipelineOptions::default()
                    .with_data_channel_capacity(definition.options().data_channel_capacity),
            )
            .map_err(|err| err.to_string())
        })?;
        pipeline.set_pipeline_id(definition.id().to_string());
        build_log.run_phase("attach_sources", || {
            attach_sources_from_catalog(&mut pipeline, &stream_definitions, context)
        })?;
        let processor_count = pipeline.processor_stats().len();
        Ok((pipeline, streams, processor_count))
    })();

    match result {
        Ok((pipeline, streams, processor_count)) => {
            build_log.log_success(streams.len(), processor_count);
            Ok((pipeline, streams))
        }
        Err(err) => {
            build_log.log_failure(&err);
            Err(err)
        }
    }
}

fn build_sinks_from_definition(
    definition: &PipelineDefinition,
) -> Result<Vec<PipelineSink>, String> {
    let mut sinks = Vec::with_capacity(definition.sinks().len());
    for sink in definition.sinks() {
        match sink.sink_type {
            SinkType::Mqtt => {
                let props = match &sink.props {
                    SinkProps::Mqtt(props) => props,
                    other => {
                        return Err(format!(
                            "sink {} expected mqtt props but received {other:?}",
                            sink.sink_id
                        ));
                    }
                };
                let mut config = MqttSinkConfig::new(
                    sink.sink_id.clone(),
                    props.broker_url.clone(),
                    if props.topic.is_empty() {
                        DEFAULT_SINK_TOPIC.to_string()
                    } else {
                        props.topic.clone()
                    },
                    props.qos,
                );
                config = config.with_retain(props.retain);
                let client_id = props
                    .client_id
                    .clone()
                    .unwrap_or_else(|| format!("{}-{}", definition.id(), sink.sink_id));
                config = config.with_client_id(client_id);
                if let Some(conn_key) = &props.connector_key {
                    config = config.with_connector_key(conn_key.clone());
                }
                if let Some(max_packet_size) = props.max_packet_size {
                    config = config.with_max_packet_size(max_packet_size);
                }
                let connector = PipelineSinkConnector::new(
                    sink.sink_id.clone(),
                    SinkConnectorConfig::Mqtt(config),
                    sink.encoder.clone(),
                )
                .with_compression(sink.compression.clone())
                .with_encryption(sink.encryption.clone());
                let pipeline_sink = PipelineSink::new(sink.sink_id.clone(), connector)
                    .with_common_props(sink.common.clone())
                    .with_output(sink.output.clone());
                sinks.push(pipeline_sink);
            }
            SinkType::Nop => {
                let props = match &sink.props {
                    SinkProps::Nop(props) => props,
                    _ => {
                        return Err(format!(
                            "sink {} expected nop props but received different variant",
                            sink.sink_id
                        ));
                    }
                };
                let connector = PipelineSinkConnector::new(
                    sink.sink_id.clone(),
                    SinkConnectorConfig::Nop(crate::planner::sink::NopSinkConfig {
                        log: props.log,
                    }),
                    sink.encoder.clone(),
                )
                .with_compression(sink.compression.clone())
                .with_encryption(sink.encryption.clone());
                let pipeline_sink = PipelineSink::new(sink.sink_id.clone(), connector)
                    .with_common_props(sink.common.clone())
                    .with_output(sink.output.clone());
                sinks.push(pipeline_sink);
            }
            SinkType::Kuksa => {
                let props = match &sink.props {
                    SinkProps::Kuksa(props) => props,
                    other => {
                        return Err(format!(
                            "sink {} expected kuksa props but received {other:?}",
                            sink.sink_id
                        ));
                    }
                };
                if !matches!(sink.encoder.kind(), SinkEncoderKind::None) {
                    return Err(format!(
                        "sink {} expected encoder `none` for kuksa but received {}",
                        sink.sink_id,
                        sink.encoder.kind_str()
                    ));
                }
                let connector = PipelineSinkConnector::new(
                    sink.sink_id.clone(),
                    SinkConnectorConfig::Kuksa(KuksaSinkConfig {
                        sink_name: sink.sink_id.clone(),
                        addr: props.addr.clone(),
                        vss_path: props.vss_path.clone(),
                    }),
                    sink.encoder.clone(),
                )
                .with_compression(sink.compression.clone())
                .with_encryption(sink.encryption.clone());
                let pipeline_sink = PipelineSink::new(sink.sink_id.clone(), connector)
                    .with_common_props(sink.common.clone())
                    .with_output(sink.output.clone());
                sinks.push(pipeline_sink);
            }
            SinkType::Kura => {
                let props = match &sink.props {
                    SinkProps::Kura(props) => props,
                    other => {
                        return Err(format!(
                            "sink {} expected kura props but received {other:?}",
                            sink.sink_id
                        ));
                    }
                };
                if !matches!(sink.encoder.kind(), SinkEncoderKind::None) {
                    return Err(format!(
                        "sink {} expected encoder `none` for kura but received {}",
                        sink.sink_id,
                        sink.encoder.kind_str()
                    ));
                }
                let connector = PipelineSinkConnector::new(
                    sink.sink_id.clone(),
                    SinkConnectorConfig::Kura(KuraSinkConfig {
                        sink_name: sink.sink_id.clone(),
                        addr: props.addr.clone(),
                        mapping_path: props.mapping_path.clone(),
                    }),
                    sink.encoder.clone(),
                )
                .with_compression(sink.compression.clone())
                .with_encryption(sink.encryption.clone());
                let pipeline_sink = PipelineSink::new(sink.sink_id.clone(), connector)
                    .with_common_props(sink.common.clone())
                    .with_output(sink.output.clone());
                sinks.push(pipeline_sink);
            }
            SinkType::Video => {
                let props = match &sink.props {
                    SinkProps::Video(props) => props,
                    other => {
                        return Err(format!(
                            "sink {} expected video props but received {other:?}",
                            sink.sink_id
                        ));
                    }
                };
                if !matches!(sink.encoder.kind(), SinkEncoderKind::None) {
                    return Err(format!(
                        "sink {} expected encoder `none` for video but received {}",
                        sink.sink_id,
                        sink.encoder.kind_str()
                    ));
                }
                let filename_prefix = props
                    .filename_prefix
                    .as_deref()
                    .unwrap_or_else(|| definition.id());
                validate_video_filename_prefix(filename_prefix).map_err(|err| {
                    format!(
                        "invalid video sink filename_prefix for {}: {err}",
                        sink.sink_id
                    )
                })?;
                let rolling = match &props.rolling {
                    VideoRollingConfig::Duration { seconds } => {
                        if *seconds == 0 {
                            return Err(format!(
                                "sink {} video rolling duration requires positive seconds",
                                sink.sink_id
                            ));
                        }
                        VideoSinkRollingConfig::Duration {
                            duration: Duration::from_secs(*seconds),
                        }
                    }
                };
                let connector = PipelineSinkConnector::new(
                    sink.sink_id.clone(),
                    SinkConnectorConfig::Video(VideoSinkConfig {
                        target: VideoSinkTargetConfig::File(VideoFileSinkConfig {
                            path: props.path.clone(),
                            filename_prefix: filename_prefix.to_string(),
                        }),
                        codec: match props.codec {
                            VideoCodec::H264 => VideoCodecConfig::H264,
                            VideoCodec::H265 => VideoCodecConfig::H265,
                        },
                        container: match props.container {
                            VideoContainer::Mp4 => VideoContainerConfig::Mp4,
                        },
                        rolling,
                    }),
                    sink.encoder.clone(),
                )
                .with_compression(sink.compression.clone())
                .with_encryption(sink.encryption.clone());
                let pipeline_sink = PipelineSink::new(sink.sink_id.clone(), connector)
                    .with_common_props(sink.common.clone())
                    .with_output(sink.output.clone());
                sinks.push(pipeline_sink);
            }
            SinkType::Memory => {
                let props = match &sink.props {
                    SinkProps::Memory(props) => props,
                    other => {
                        return Err(format!(
                            "sink {} expected memory props but received {other:?}",
                            sink.sink_id
                        ));
                    }
                };
                let topic = props.topic.trim();
                if topic.is_empty() {
                    return Err(format!("sink {} requires topic", sink.sink_id));
                }
                let kind = if matches!(sink.encoder.kind(), SinkEncoderKind::None) {
                    MemoryTopicKind::Collection
                } else {
                    MemoryTopicKind::Bytes
                };
                let connector = PipelineSinkConnector::new(
                    sink.sink_id.clone(),
                    SinkConnectorConfig::Memory(MemorySinkConfig::new(
                        sink.sink_id.clone(),
                        topic.to_string(),
                        kind,
                    )),
                    sink.encoder.clone(),
                )
                .with_compression(sink.compression.clone())
                .with_encryption(sink.encryption.clone());
                let pipeline_sink = PipelineSink::new(sink.sink_id.clone(), connector)
                    .with_common_props(sink.common.clone())
                    .with_output(sink.output.clone());
                sinks.push(pipeline_sink);
            }
            SinkType::File => {
                let props = match &sink.props {
                    SinkProps::File(props) => props,
                    other => {
                        return Err(format!(
                            "sink {} expected file props but received {other:?}",
                            sink.sink_id
                        ));
                    }
                };
                if matches!(sink.encoder.kind(), SinkEncoderKind::None) {
                    return Err(format!(
                        "sink {} expected encoded bytes for file but received encoder `none`",
                        sink.sink_id
                    ));
                }
                let connector = PipelineSinkConnector::new(
                    sink.sink_id.clone(),
                    SinkConnectorConfig::File(FileSinkConfig {
                        sink_name: sink.sink_id.clone(),
                        pipeline_id: definition.id().to_string(),
                        path: props.path.clone(),
                        filename_prefix: props.filename_prefix.clone(),
                        filename_suffix: props.filename_suffix.clone(),
                        retention: FileSinkRetentionConfig {
                            max_file_count: props.retention.max_file_count,
                            max_file_age_days: props.retention.max_file_age_days,
                        },
                    }),
                    sink.encoder.clone(),
                )
                .with_compression(sink.compression.clone())
                .with_encryption(sink.encryption.clone());
                let pipeline_sink = PipelineSink::new(sink.sink_id.clone(), connector)
                    .with_common_props(sink.common.clone())
                    .with_output(sink.output.clone());
                sinks.push(pipeline_sink);
            }
            SinkType::NngPubSub => {
                #[cfg(feature = "nng_pubsub")]
                {
                    let props = match &sink.props {
                        SinkProps::NngPubSub(props) => props,
                        other => {
                            return Err(format!(
                                "sink {} expected nng_pubsub props but received {other:?}",
                                sink.sink_id
                            ));
                        }
                    };
                    let config = NngPubSubSinkConfig::new(
                        sink.sink_id.clone(),
                        props.url.clone(),
                        props.topic.clone(),
                    )
                    .with_topic_delimiter(props.topic_delimiter.clone());
                    config.validate().map_err(|err| {
                        format!("invalid nng_pubsub sink {}: {err}", sink.sink_id)
                    })?;
                    let connector = PipelineSinkConnector::new(
                        sink.sink_id.clone(),
                        SinkConnectorConfig::NngPubSub(config),
                        sink.encoder.clone(),
                    )
                    .with_compression(sink.compression.clone())
                    .with_encryption(sink.encryption.clone());
                    let pipeline_sink = PipelineSink::new(sink.sink_id.clone(), connector)
                        .with_common_props(sink.common.clone())
                        .with_output(sink.output.clone());
                    sinks.push(pipeline_sink);
                }
                #[cfg(not(feature = "nng_pubsub"))]
                {
                    return Err(format!(
                        "sink {} uses nng_pubsub but flow was built without the nng_pubsub feature",
                        sink.sink_id
                    ));
                }
            }
            SinkType::Http => {
                let props = match &sink.props {
                    SinkProps::Http(props) => props,
                    other => {
                        return Err(format!(
                            "sink {} expected http props but received {other:?}",
                            sink.sink_id
                        ));
                    }
                };
                if matches!(sink.encoder.kind(), SinkEncoderKind::None) {
                    return Err(format!(
                        "sink {} expected an encoder for http but received encoder `none`",
                        sink.sink_id
                    ));
                }
                let mut http_cfg = HttpSinkConfig::new(&props.url);
                if let Some(method) = &props.method {
                    http_cfg = http_cfg.with_method(match method.to_uppercase().as_str() {
                        "GET" => HttpMethod::Get,
                        "PUT" => HttpMethod::Put,
                        "PATCH" => HttpMethod::Patch,
                        "DELETE" => HttpMethod::Delete,
                        _ => HttpMethod::Post,
                    });
                }
                if let Some(timeout_secs) = props.timeout_secs {
                    http_cfg = http_cfg.with_timeout(Duration::from_secs(timeout_secs));
                }
                for (key, value) in &props.headers {
                    http_cfg = http_cfg.with_header(key, value);
                }
                if let Some(ref ct) = props.content_type {
                    http_cfg = http_cfg.with_content_type(ct);
                }
                if let Some(max_size) = props.max_body_size {
                    http_cfg = http_cfg.with_max_body_size(max_size);
                }
                let mut retry_cfg = HttpRetryConfig::default();
                if let Some(max_attempts) = props.retry_max_attempts {
                    retry_cfg.max_attempts = Some(max_attempts);
                }
                if let Some(backoff_ms) = props.retry_backoff_ms {
                    retry_cfg.initial_backoff_ms = backoff_ms;
                }
                if let Some(max_backoff_ms) = props.retry_max_backoff_ms {
                    retry_cfg.max_backoff_ms = max_backoff_ms;
                }
                http_cfg = http_cfg.with_retry(retry_cfg);
                let connector = PipelineSinkConnector::new(
                    sink.sink_id.clone(),
                    SinkConnectorConfig::Http(http_cfg),
                    sink.encoder.clone(),
                )
                .with_compression(sink.compression.clone())
                .with_encryption(sink.encryption.clone());
                let pipeline_sink = PipelineSink::new(sink.sink_id.clone(), connector)
                    .with_common_props(sink.common.clone())
                    .with_output(sink.output.clone());
                sinks.push(pipeline_sink);
            }
        }
    }
    Ok(sinks)
}

pub(super) fn attach_sources_from_catalog(
    pipeline: &mut ProcessorPipeline,
    stream_defs: &HashMap<String, Arc<StreamDefinition>>,
    context: &PipelineContext,
) -> Result<(), String> {
    let mut has_source_processor = false;
    let pipeline_id = pipeline.pipeline_id().to_string();
    let data_channel_capacity = pipeline.data_channel_capacity();
    let mqtt_client_manager = context.mqtt_client_manager();
    let memory_pubsub_registry = context.memory_pubsub_registry();
    for processor in pipeline.middle_processors.iter_mut() {
        if let PlanProcessor::DataSource(ds) = processor {
            has_source_processor = true;
            let stream_name = ds.stream_name().to_string();
            let definition = stream_defs.get(&stream_name).ok_or_else(|| {
                format!("stream {stream_name} missing definition when attaching sources")
            })?;
            let processor_id = ds.id().to_string();

            match definition.props() {
                StreamProps::Mqtt(stream_props) => {
                    let mut config = MqttSourceConfig::new(
                        processor_id.clone(),
                        stream_props.broker_url.clone(),
                        stream_props.topic.clone(),
                        stream_props.qos,
                    );
                    if let Some(client_id) = &stream_props.client_id {
                        config = config.with_client_id(client_id.clone());
                    }
                    if let Some(connector_key) = &stream_props.connector_key {
                        config = config.with_connector_key(connector_key.clone());
                    }
                    let connector = MqttSourceConnector::new(
                        format!("{processor_id}_source_connector"),
                        config,
                        context.flow_instance_id(),
                        mqtt_client_manager.clone(),
                        context.spawner().clone(),
                    )
                    .with_channel_capacity(data_channel_capacity);
                    ds.add_connector(Box::new(connector));
                }
                StreamProps::Video(stream_props) => {
                    let connector = VideoSourceConnector::new(
                        format!("{processor_id}_video_source_connector"),
                        VideoSourceConfig {
                            stream_name: stream_name.clone(),
                            url: stream_props.url.clone(),
                            rtsp_transport: match stream_props.rtsp_transport {
                                crate::catalog::VideoRtspTransport::Tcp => {
                                    VideoRtspTransportConfig::Tcp
                                }
                                crate::catalog::VideoRtspTransport::Udp => {
                                    VideoRtspTransportConfig::Udp
                                }
                            },
                            reconnect: VideoReconnectRuntimeConfig {
                                enabled: stream_props.reconnect.enabled,
                                initial_delay: stream_props.reconnect.initial_delay,
                                max_delay: stream_props.reconnect.max_delay,
                            },
                        },
                    )
                    .with_channel_capacity(data_channel_capacity);
                    ds.add_connector(Box::new(connector));
                }
                StreamProps::History(props) => {
                    let mut config = HistorySourceConfig::new(&props.datasource, &props.topic);
                    config.start = props.start;
                    config.end = props.end;
                    if let Some(bs) = props.batch_size {
                        config.batch_size = bs;
                    }
                    config.send_interval = props.send_interval;

                    let connector = HistorySourceConnector::new(
                        processor_id.clone(),
                        config,
                        context.spawner().clone(),
                    )
                    .with_channel_capacity(data_channel_capacity);
                    ds.add_connector(Box::new(connector));
                }
                StreamProps::Mock(_) => {
                    let (connector, handle) = MockSourceConnector::new_with_channel_capacity(
                        format!("{processor_id}_mock_source_connector"),
                        data_channel_capacity,
                    );
                    let key = format!("{pipeline_id}:{stream_name}:{processor_id}");
                    context.mock_source_handle_registry().register(key, handle);
                    ds.add_connector(Box::new(connector));
                }
                StreamProps::Memory(props) => {
                    let kind = if definition.decoder().kind() == "none" {
                        MemoryTopicKind::Collection
                    } else {
                        MemoryTopicKind::Bytes
                    };
                    let config = MemorySourceConfig::new(props.topic.clone(), kind);
                    let connector = MemorySourceConnector::new(
                        format!("{processor_id}_memory_source_connector"),
                        config,
                        memory_pubsub_registry.clone(),
                    );
                    ds.add_connector(Box::new(connector));
                }
                StreamProps::NngPubSub(props) => {
                    let config = NngPubSubSourceConfig::new(
                        stream_name.clone(),
                        props.url.clone(),
                        props.topic.clone(),
                    )
                    .with_topic_delimiter(props.topic_delimiter.clone());
                    config
                        .validate()
                        .map_err(|err| format!("invalid nng_pubsub stream {stream_name}: {err}"))?;
                    #[cfg(feature = "nng_pubsub")]
                    {
                        let connector = NngPubSubSourceConnector::new(
                            format!("{processor_id}_nng_pubsub_source_connector"),
                            config,
                            context.flow_instance_id(),
                            context.spawner().clone(),
                        )
                        .with_channel_capacity(data_channel_capacity);
                        ds.add_connector(Box::new(connector));
                    }
                    #[cfg(not(feature = "nng_pubsub"))]
                    {
                        let _ = config;
                        return Err(format!(
                            "stream {stream_name} uses nng_pubsub but flow was built without the nng_pubsub feature"
                        ));
                    }
                }
            }
            continue;
        }

        if matches!(processor, PlanProcessor::SharedSource(_)) {
            has_source_processor = true;
        }
    }

    if has_source_processor {
        Ok(())
    } else {
        Err("no datasource processors available to attach connectors".into())
    }
}

// General pipeline tests live in `pipeline/tests.rs`; focused lowering tests stay close to these helpers.

#[cfg(test)]
mod file_sink_tests {
    use super::*;
    use crate::pipeline::{FileRetentionConfig, FileSinkProps};
    use crate::planner::sink::{CommonSinkProps, SinkEncoderConfig, SinkEncoderKind};
    use serde_json::Map as JsonMap;

    fn file_sink_definition(encoder: SinkEncoderConfig) -> PipelineDefinition {
        let props = FileSinkProps::new("/tmp/vf-file", "speed_")
            .with_filename_suffix(".json")
            .with_retention(FileRetentionConfig {
                max_file_count: 10,
                max_file_age_days: 7,
            });
        let sink = SinkDefinition::new("sink_1", SinkType::File, SinkProps::File(props))
            .with_encoder(encoder)
            .with_common_props(CommonSinkProps {
                batch_count: Some(1000),
                batch_duration: Some(Duration::from_millis(1000)),
            });
        PipelineDefinition::new("pipe_1", "SELECT 1 AS a", vec![sink])
    }

    #[test]
    fn file_sink_lowering_preserves_config_and_common_props() {
        let definition = file_sink_definition(SinkEncoderConfig::json());

        let sinks = build_sinks_from_definition(&definition).expect("build sinks");

        assert_eq!(sinks.len(), 1);
        assert_eq!(sinks[0].common.batch_count, Some(1000));
        assert_eq!(
            sinks[0].common.batch_duration,
            Some(Duration::from_millis(1000))
        );
        let SinkConnectorConfig::File(config) = &sinks[0].connector.connector else {
            panic!("expected file sink connector config");
        };
        assert_eq!(config.sink_name, "sink_1");
        assert_eq!(config.pipeline_id, "pipe_1");
        assert_eq!(config.path, "/tmp/vf-file");
        assert_eq!(config.filename_prefix, "speed_");
        assert_eq!(config.filename_suffix, ".json");
        assert_eq!(config.retention.max_file_count, 10);
        assert_eq!(config.retention.max_file_age_days, 7);
    }

    #[test]
    fn file_sink_lowering_rejects_encoder_none() {
        let definition = file_sink_definition(SinkEncoderConfig::new(
            SinkEncoderKind::None,
            JsonMap::new(),
        ));

        let err = build_sinks_from_definition(&definition).expect_err("encoder none should fail");

        assert!(
            err.contains("expected encoded bytes for file"),
            "unexpected error: {err}"
        );
    }
}
