use super::*;
use crate::catalog::{Catalog, StreamDefinition, StreamProps};
use crate::connector::{
    register_mock_source_handle, HistorySourceConfig, HistorySourceConnector, KuksaSinkConfig,
    MemorySinkConfig, MemorySourceConfig, MemorySourceConnector, MemoryTopicKind,
    MockSourceConnector, MqttClientManager, MqttSinkConfig, MqttSourceConfig, MqttSourceConnector,
};
use crate::expr::sql_conversion::{SchemaBinding, SchemaBindingEntry, SourceBindingKind};
use crate::planner::logical::create_logical_plan;
use crate::planner::plan_cache::{logical_plan_from_ir, sources_from_logical_ir, LogicalPlanIR};
use crate::planner::sink::SinkEncoderKind;
use crate::processor::processor_builder::{PlanProcessor, ProcessorPipeline};
use crate::processor::EventtimePipelineContext;
use crate::processor::Processor;
use crate::processor::ProcessorStatsEntry;
use crate::processor::{create_processor_pipeline, ProcessorPipelineDependencies};
use crate::shared_stream::SharedStreamRegistry;
use crate::{
    explain_pipeline_with_options, optimize_physical_plan, PipelineExplain, PipelineRegistries,
    PipelineSink, PipelineSinkConnector, SinkConnectorConfig,
};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

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
    pub fn new(
        catalog: Arc<Catalog>,
        shared_stream_registry: &'static SharedStreamRegistry,
        mqtt_client_manager: MqttClientManager,
        registries: PipelineRegistries,
    ) -> Self {
        Self {
            pipelines: RwLock::new(HashMap::new()),
            catalog,
            shared_stream_registry,
            mqtt_client_manager,
            registries,
        }
    }

    /// Create a pipeline runtime from definition and store it.
    pub fn create_pipeline(
        &self,
        definition: PipelineDefinition,
    ) -> Result<PipelineSnapshot, PipelineError> {
        let pipeline_id = definition.id().to_string();
        let (pipeline, streams) = build_pipeline_runtime(
            &definition,
            &self.catalog,
            self.shared_stream_registry,
            &self.mqtt_client_manager,
            &self.registries,
        )
        .map_err(PipelineError::BuildFailure)?;
        let mut guard = self.pipelines.write().expect("pipeline manager poisoned");
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

    /// Internal API used by the plan cache write-back path.
    ///
    /// Prefer `create_pipeline_with_plan_cache` for user-facing pipeline creation.
    pub(crate) fn create_pipeline_with_logical_ir(
        &self,
        definition: PipelineDefinition,
    ) -> Result<(PipelineSnapshot, Vec<u8>), PipelineError> {
        let pipeline_id = definition.id().to_string();
        let (pipeline, streams, logical_ir) = build_pipeline_runtime_with_logical_ir(
            &definition,
            &self.catalog,
            self.shared_stream_registry,
            &self.mqtt_client_manager,
            &self.registries,
        )
        .map_err(PipelineError::BuildFailure)?;
        let mut guard = self.pipelines.write().expect("pipeline manager poisoned");
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
        Ok((snapshot, logical_ir))
    }

    /// Create a pipeline with optional plan-cache support.
    ///
    /// When `options.plan_cache.enabled` is `true`, this tries to reuse a persisted logical-plan
    /// snapshot (hit) and falls back to building from SQL (miss). On miss, it returns
    /// `logical_plan_ir` for the caller to persist as a plan snapshot.
    pub fn create_pipeline_with_plan_cache(
        &self,
        definition: PipelineDefinition,
        inputs: crate::planner::plan_cache::PlanCacheInputs,
    ) -> Result<crate::planner::plan_cache::PlanCacheBuildResult, PipelineError> {
        if !definition.options().plan_cache.enabled {
            let snapshot = self.create_pipeline(definition)?;
            return Ok(crate::planner::plan_cache::PlanCacheBuildResult {
                snapshot,
                hit: false,
                logical_plan_ir: None,
            });
        }

        if crate::planner::plan_cache::snapshot_matches_inputs(&inputs) {
            if let Some(snapshot_record) = inputs.snapshot {
                if let Ok(snapshot) = self.create_pipeline_from_logical_ir(
                    definition.clone(),
                    &snapshot_record.logical_plan_ir,
                ) {
                    tracing::debug!(pipeline_id = %definition.id(), "plan_cache hit");
                    return Ok(crate::planner::plan_cache::PlanCacheBuildResult {
                        snapshot,
                        hit: true,
                        logical_plan_ir: None,
                    });
                }
            }
        }

        tracing::debug!(pipeline_id = %definition.id(), "plan_cache miss");
        let (snapshot, logical_ir) = self.create_pipeline_with_logical_ir(definition)?;
        Ok(crate::planner::plan_cache::PlanCacheBuildResult {
            snapshot,
            hit: false,
            logical_plan_ir: Some(logical_ir),
        })
    }

    /// Internal API used by the plan cache hit path.
    ///
    /// Prefer `create_pipeline_with_plan_cache` for user-facing pipeline creation.
    pub(crate) fn create_pipeline_from_logical_ir(
        &self,
        definition: PipelineDefinition,
        logical_plan_ir: &[u8],
    ) -> Result<PipelineSnapshot, PipelineError> {
        let pipeline_id = definition.id().to_string();

        {
            let guard = self.pipelines.read().expect("pipeline manager poisoned");
            if guard.contains_key(&pipeline_id) {
                return Err(PipelineError::AlreadyExists(pipeline_id));
            }
        }

        let (pipeline, streams) = build_pipeline_runtime_from_logical_ir(
            &definition,
            logical_plan_ir,
            &self.catalog,
            self.shared_stream_registry,
            &self.mqtt_client_manager,
            &self.registries,
        )
        .map_err(PipelineError::BuildFailure)?;

        let mut guard = self.pipelines.write().expect("pipeline manager poisoned");
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

    /// Retrieve a snapshot for a specific pipeline.
    pub fn get(&self, pipeline_id: &str) -> Option<PipelineSnapshot> {
        let guard = self.pipelines.read().expect("pipeline manager poisoned");
        guard.get(pipeline_id).map(|entry| entry.snapshot())
    }

    /// List all managed pipelines.
    pub fn list(&self) -> Vec<PipelineSnapshot> {
        let guard = self.pipelines.read().expect("pipeline manager poisoned");
        guard.values().map(|entry| entry.snapshot()).collect()
    }

    /// Explain an existing pipeline by id (logical + physical plans).
    pub fn explain_pipeline(&self, pipeline_id: &str) -> Result<PipelineExplain, PipelineError> {
        let definition = {
            let guard = self.pipelines.read().expect("pipeline manager poisoned");
            let entry = guard
                .get(pipeline_id)
                .ok_or_else(|| PipelineError::NotFound(pipeline_id.to_string()))?;
            Arc::clone(&entry.definition)
        };

        self.explain_pipeline_definition(definition.as_ref())
    }

    /// Explain a pipeline definition without registering it.
    pub fn explain_pipeline_definition(
        &self,
        definition: &PipelineDefinition,
    ) -> Result<PipelineExplain, PipelineError> {
        let sinks = build_sinks_from_definition(definition).map_err(PipelineError::BuildFailure)?;

        explain_pipeline_with_options(
            definition.sql(),
            sinks,
            &self.catalog,
            self.shared_stream_registry,
            &self.registries,
            definition.options(),
        )
        .map_err(|err| PipelineError::BuildFailure(err.to_string()))
    }

    /// Start the pipeline runtime if not already running.
    pub fn start_pipeline(&self, pipeline_id: &str) -> Result<(), PipelineError> {
        let mut guard = self.pipelines.write().expect("pipeline manager poisoned");
        let entry = guard
            .get_mut(pipeline_id)
            .ok_or_else(|| PipelineError::NotFound(pipeline_id.to_string()))?;
        if matches!(entry.status, PipelineStatus::Running) {
            return Ok(());
        }

        if entry.pipeline.is_none() {
            let definition = Arc::clone(&entry.definition);
            let (pipeline, streams) = build_pipeline_runtime(
                &definition,
                &self.catalog,
                self.shared_stream_registry,
                &self.mqtt_client_manager,
                &self.registries,
            )
            .map_err(PipelineError::BuildFailure)?;
            entry.pipeline = Some(pipeline);
            entry.streams = streams;
        }

        let pipeline = entry
            .pipeline
            .as_mut()
            .ok_or_else(|| PipelineError::Runtime("pipeline runtime missing".to_string()))?;
        pipeline.start();
        entry.status = PipelineStatus::Running;
        Ok(())
    }

    pub async fn stop_pipeline(
        &self,
        pipeline_id: &str,
        mode: PipelineStopMode,
        timeout: Duration,
    ) -> Result<(), PipelineError> {
        let maybe_pipeline = {
            let mut guard = self.pipelines.write().expect("pipeline manager poisoned");
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
            let mut guard = self.pipelines.write().expect("pipeline manager poisoned");
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
        let guard = self.pipelines.read().expect("pipeline manager poisoned");
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
    shared_stream_registry: &SharedStreamRegistry,
    mqtt_client_manager: &MqttClientManager,
    registries: &PipelineRegistries,
) -> Result<(ProcessorPipeline, Vec<String>), String> {
    let (pipeline, streams, _) = build_pipeline_runtime_with_logical_ir(
        definition,
        catalog,
        shared_stream_registry,
        mqtt_client_manager,
        registries,
    )?;
    Ok((pipeline, streams))
}

fn build_pipeline_runtime_with_logical_ir(
    definition: &PipelineDefinition,
    catalog: &Catalog,
    shared_stream_registry: &SharedStreamRegistry,
    mqtt_client_manager: &MqttClientManager,
    registries: &PipelineRegistries,
) -> Result<(ProcessorPipeline, Vec<String>, Vec<u8>), String> {
    let select_stmt = parser::parse_sql_with_registries(
        definition.sql(),
        registries.aggregate_registry(),
        registries.stateful_registry(),
    )
    .map_err(|err| err.to_string())?;
    let streams: Vec<String> = select_stmt
        .source_infos
        .iter()
        .map(|info| info.name.clone())
        .collect();

    let mut stream_definitions = HashMap::new();
    let mut binding_entries = Vec::new();

    for source in &select_stmt.source_infos {
        let definition = catalog
            .get(&source.name)
            .ok_or_else(|| format!("stream '{}' not found in catalog", source.name))?;
        let schema = definition.schema();
        let kind =
            if futures::executor::block_on(shared_stream_registry.is_registered(&source.name)) {
                SourceBindingKind::Shared
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

    let schema_binding = SchemaBinding::new(binding_entries);
    let sinks = build_sinks_from_definition(definition)?;
    let logical_plan = create_logical_plan(select_stmt, sinks, &stream_definitions)?;
    let (logical_plan, pruned_binding) = crate::planner::optimize_logical_plan_with_options(
        logical_plan,
        &schema_binding,
        &crate::planner::LogicalOptimizerOptions {
            eventtime_enabled: definition.options().eventtime.enabled,
        },
    );
    let logical_ir = LogicalPlanIR::from_plan(&logical_plan)
        .encode()
        .map_err(|err| err.to_string())?;

    let build_options = crate::planner::PhysicalPlanBuildOptions {
        eventtime_enabled: definition.options().eventtime.enabled,
        eventtime_late_tolerance: definition.options().eventtime.late_tolerance,
    };
    let physical_plan = crate::planner::create_physical_plan_with_build_options(
        Arc::clone(&logical_plan),
        &pruned_binding,
        registries,
        &build_options,
    )?;
    let physical_plan = physical_plan;
    let optimized_plan = optimize_physical_plan(
        Arc::clone(&physical_plan),
        registries.encoder_registry().as_ref(),
        registries.aggregate_registry(),
    );
    if definition.options().eventtime.enabled {
        validate_eventtime_enabled(&stream_definitions, registries)?;
    }
    let explain = PipelineExplain::new_with_pipeline_options(
        crate::planner::explain::PipelineExplainOptions {
            eventtime_enabled: definition.options().eventtime.enabled,
            eventtime_late_tolerance_ms: definition.options().eventtime.late_tolerance.as_millis(),
        },
        Arc::clone(&logical_plan),
        Arc::clone(&optimized_plan),
    );
    tracing::info!(explain = %explain.to_pretty_string(), "pipeline explain");

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

    let mut pipeline = create_processor_pipeline(
        optimized_plan,
        ProcessorPipelineDependencies::new(
            mqtt_client_manager.clone(),
            registries.connector_registry(),
            registries.encoder_registry(),
            registries.decoder_registry(),
            registries.aggregate_registry(),
            registries.stateful_registry(),
            eventtime,
        ),
    )
    .map_err(|err| err.to_string())?;
    pipeline.set_pipeline_id(definition.id().to_string());
    attach_sources_from_catalog(&mut pipeline, &stream_definitions, mqtt_client_manager)?;
    Ok((pipeline, streams, logical_ir))
}

fn build_pipeline_runtime_from_logical_ir(
    definition: &PipelineDefinition,
    logical_plan_ir: &[u8],
    catalog: &Catalog,
    shared_stream_registry: &SharedStreamRegistry,
    mqtt_client_manager: &MqttClientManager,
    registries: &PipelineRegistries,
) -> Result<(ProcessorPipeline, Vec<String>), String> {
    let logical_ir = LogicalPlanIR::decode(logical_plan_ir).map_err(|e| e.to_string())?;

    let sources = sources_from_logical_ir(&logical_ir);
    let mut streams: Vec<String> = sources.iter().map(|(s, _)| s.clone()).collect();
    streams.sort();
    streams.dedup();

    let mut stream_definitions = HashMap::new();
    let mut binding_entries = Vec::new();
    let mut datasource_inputs = HashMap::new();

    for (stream, alias) in sources {
        let definition = catalog
            .get(&stream)
            .ok_or_else(|| format!("stream {stream} not found in catalog"))?;
        let schema = definition.schema();
        let kind = if futures::executor::block_on(shared_stream_registry.is_registered(&stream)) {
            SourceBindingKind::Shared
        } else {
            SourceBindingKind::Regular
        };
        binding_entries.push(SchemaBindingEntry {
            source_name: stream.clone(),
            alias,
            schema: Arc::clone(&schema),
            kind,
        });
        stream_definitions.insert(stream.clone(), Arc::clone(&definition));
        datasource_inputs.insert(stream, (definition.decoder().clone(), schema));
    }

    let schema_binding = SchemaBinding::new(binding_entries);
    let logical_plan =
        logical_plan_from_ir(&logical_ir, &datasource_inputs).map_err(|e| e.to_string())?;
    let (logical_plan, pruned_binding) = crate::planner::optimize_logical_plan_with_options(
        logical_plan,
        &schema_binding,
        &crate::planner::LogicalOptimizerOptions {
            eventtime_enabled: definition.options().eventtime.enabled,
        },
    );

    let build_options = crate::planner::PhysicalPlanBuildOptions {
        eventtime_enabled: definition.options().eventtime.enabled,
        eventtime_late_tolerance: definition.options().eventtime.late_tolerance,
    };
    let physical_plan = crate::planner::create_physical_plan_with_build_options(
        Arc::clone(&logical_plan),
        &pruned_binding,
        registries,
        &build_options,
    )
    .map_err(|err| err.to_string())?;
    let optimized_plan = optimize_physical_plan(
        Arc::clone(&physical_plan),
        registries.encoder_registry().as_ref(),
        registries.aggregate_registry(),
    );
    if definition.options().eventtime.enabled {
        validate_eventtime_enabled(&stream_definitions, registries)?;
    }
    let explain = PipelineExplain::new(Arc::clone(&logical_plan), Arc::clone(&optimized_plan));
    tracing::info!(explain = %explain.to_pretty_string(), "pipeline explain");

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

    let mut pipeline = create_processor_pipeline(
        optimized_plan,
        ProcessorPipelineDependencies::new(
            mqtt_client_manager.clone(),
            registries.connector_registry(),
            registries.encoder_registry(),
            registries.decoder_registry(),
            registries.aggregate_registry(),
            registries.stateful_registry(),
            eventtime,
        ),
    )
    .map_err(|err| err.to_string())?;

    pipeline.set_pipeline_id(definition.id().to_string());
    attach_sources_from_catalog(&mut pipeline, &stream_definitions, mqtt_client_manager)?;

    Ok((pipeline, streams))
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
                let connector = PipelineSinkConnector::new(
                    sink.sink_id.clone(),
                    SinkConnectorConfig::Mqtt(config),
                    sink.encoder.clone(),
                );
                let pipeline_sink = PipelineSink::new(sink.sink_id.clone(), connector)
                    .with_common_props(sink.common.clone());
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
                );
                let pipeline_sink = PipelineSink::new(sink.sink_id.clone(), connector)
                    .with_common_props(sink.common.clone());
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
                );
                let pipeline_sink = PipelineSink::new(sink.sink_id.clone(), connector)
                    .with_common_props(sink.common.clone());
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
                );
                let pipeline_sink = PipelineSink::new(sink.sink_id.clone(), connector)
                    .with_common_props(sink.common.clone());
                sinks.push(pipeline_sink);
            }
        }
    }
    Ok(sinks)
}

pub(super) fn attach_sources_from_catalog(
    pipeline: &mut ProcessorPipeline,
    stream_defs: &HashMap<String, Arc<StreamDefinition>>,
    mqtt_client_manager: &MqttClientManager,
) -> Result<(), String> {
    let mut has_source_processor = false;
    let pipeline_id = pipeline.pipeline_id().to_string();
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
                        mqtt_client_manager.clone(),
                    );
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

                    let connector = HistorySourceConnector::new(processor_id.clone(), config);
                    ds.add_connector(Box::new(connector));
                }
                StreamProps::Mock(_) => {
                    let (connector, handle) =
                        MockSourceConnector::new(format!("{processor_id}_mock_source_connector"));
                    let key = format!("{pipeline_id}:{stream_name}:{processor_id}");
                    register_mock_source_handle(key, handle);
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
                    );
                    ds.add_connector(Box::new(connector));
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

// Tests live in `pipeline/tests.rs`.
