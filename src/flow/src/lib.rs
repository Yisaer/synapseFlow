#![cfg_attr(
    not(test),
    deny(clippy::unwrap_used, clippy::unreachable, clippy::panic)
)]
#![deny(unsafe_code)]

pub mod aggregation;
mod backpressure_hub;
pub mod catalog;
pub mod codec;
pub mod connector;
mod deadlock;
pub mod eventtime;
mod explain_shared_stream;
pub mod expr;
pub mod instance;
pub mod model;
pub mod pipeline;
pub mod planner;
pub mod processor;
mod runtime;
pub mod shared_stream;
pub mod stateful;
pub mod test_proto;
#[cfg(test)]
mod test_support;

pub use aggregation::AggregateFunctionRegistry;
pub use catalog::{
    Catalog, CatalogError, EventtimeDefinition, MqttStreamProps, NngPubSubStreamProps,
    StreamDecoderConfig, StreamDefinition, StreamProps, StreamType, VideoReconnectConfig,
    VideoRtspTransport, VideoStreamProps,
};
pub use codec::{
    AesGcmStreamWriter, CodecError, CompressError, CompressWriter, CompressionCodec,
    DecoderRegistry, EncodeError, EncoderRegistry, EncryptError, EncryptWriter,
    EncryptionAlgorithm, InlineEncryptionKey, JsonDecoder, JsonEncoder, Merger, MergerRegistry,
    ProtoDescriptorBundle, ProtoFieldInfo, ProtobufDecoder, RecordDecoder, SecretBytes,
    SecretEncoding, SinkEncoder, SinkEncoderFactory, SinkEncryptionConfig,
};
pub use datatypes::{
    BooleanType, BytesType, ColumnSchema, ConcreteDatatype, Float32Type, Float64Type, Int16Type,
    Int32Type, Int64Type, Int8Type, ListType, Schema, StringType, StructField, StructType,
    TimestampType, TimestampValue, Uint16Type, Uint32Type, Uint64Type, Uint8Type,
};
pub use eventtime::{
    BuiltinEventtimeType, EventtimeParseError, EventtimeTypeParser, EventtimeTypeRegistry,
};
pub use expr::custom_func::{CustomFunc, CustomFuncRegistry};
pub use expr::sql_conversion;
pub use expr::{
    convert_expr_to_scalar, convert_select_stmt_to_scalar, extract_select_expressions, BinaryFunc,
    ConversionError, EvalContext, ScalarExpr, StreamSqlConverter, UnaryFunc,
};
pub use instance::{
    FlowInstance, FlowInstanceCpuMetricsError, FlowInstanceDedicatedRuntimeOptions,
    FlowInstanceError, FlowInstanceOptions, FlowInstanceRuntimeOptions,
    FlowInstanceSharedRegistries, StreamRuntimeInfo,
};
pub use model::{Collection, RecordBatch};
pub use pipeline::{
    CreatePipelineRequest, CreatePipelineResult, ExplainPipelineTarget, KuksaSinkProps,
    KuraSinkProps, MemorySinkProps, MqttSinkProps, NngPubSubSinkProps, NopSinkProps,
    PipelineDefinition, PipelineError, PipelineOptions, PipelineScheduleConfig, PipelineSnapshot,
    PipelineStatus, PipelineStopMode, SinkDefinition, SinkProps, SinkType, SourceDefinition,
    SourceInputConfig, SourceInputMode, SourceOnChangeConfig, VideoCodec, VideoContainer,
    VideoRollingConfig, VideoSinkProps,
};
pub use planner::create_physical_plan;
pub use planner::explain::{ExplainReport, ExplainRow, PipelineExplain, PipelineExplainConfig};
pub use planner::logical::{
    BaseLogicalPlan, DataSinkPlan, DataSource, Filter, LogicalPlan, Project,
};
pub use planner::optimize_logical_plan;
pub use planner::optimize_physical_plan;
pub use planner::sink::{
    CommonSinkProps, NopSinkConfig, PipelineSink, PipelineSinkConnector, SinkConnectorConfig,
    SinkDeltaOutputConfig, SinkEncoderConfig, SinkOutputConfig, SinkOutputMode,
};
pub use processor::{ControlSignal, ProcessorError, StreamData};
pub use shared_stream::{
    SharedSourceConnectorConfig, SharedStreamConfig, SharedStreamError, SharedStreamInfo,
    SharedStreamProcessorStats, SharedStreamStatus, SharedStreamSubscription,
};
pub use stateful::StatefulFunctionRegistry;

/// Initialize process-global utilities used by the `flow` crate.
///
/// This should be called once during process startup (e.g. from the application's `main`),
/// especially when running multiple [`FlowInstance`] runtimes inside the same process.
pub fn init_process_once() {
    deadlock::start_deadlock_detector_once();
}

/// Sample and update per-instance CPU metrics for all registered in-process flow instances.
pub fn collect_flow_instance_cpu_metrics_once() {
    instance::collect_registered_flow_instance_cpu_metrics();
}

use connector::{ConnectorRegistry, MqttClientManager};
use explain_shared_stream::shared_stream_decode_applied_snapshot;
use planner::logical::{create_logical_plan, create_logical_plan_with_source_inputs};
use processor::processor_builder::{
    create_processor_pipeline, ProcessorPipeline, ProcessorPipelineDependencies,
    ProcessorPipelineOptions,
};
use shared_stream::SharedStreamRegistry;
use std::collections::HashMap;
use std::sync::Arc;

type SchemaBindingResult = (
    crate::expr::sql_conversion::SchemaBinding,
    HashMap<String, Arc<StreamDefinition>>,
);

/// Bundle of registries required for building pipelines.
#[derive(Clone)]
pub struct PipelineRegistries {
    connector_registry: Arc<ConnectorRegistry>,
    encoder_registry: Arc<EncoderRegistry>,
    decoder_registry: Arc<DecoderRegistry>,
    aggregate_registry: Arc<AggregateFunctionRegistry>,
    stateful_registry: Arc<StatefulFunctionRegistry>,
    custom_func_registry: Arc<CustomFuncRegistry>,
    eventtime_type_registry: Arc<EventtimeTypeRegistry>,
    merger_registry: Arc<MergerRegistry>,
}

impl PipelineRegistries {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        connector_registry: Arc<ConnectorRegistry>,
        encoder_registry: Arc<EncoderRegistry>,
        decoder_registry: Arc<DecoderRegistry>,
        aggregate_registry: Arc<AggregateFunctionRegistry>,
        stateful_registry: Arc<StatefulFunctionRegistry>,
        custom_func_registry: Arc<CustomFuncRegistry>,
        eventtime_type_registry: Arc<EventtimeTypeRegistry>,
        merger_registry: Arc<MergerRegistry>,
    ) -> Self {
        Self {
            connector_registry,
            encoder_registry,
            decoder_registry,
            aggregate_registry,
            stateful_registry,
            custom_func_registry,
            eventtime_type_registry,
            merger_registry,
        }
    }

    pub fn new_with_builtin() -> Self {
        Self::new(
            ConnectorRegistry::with_builtin_sinks(connector::MemoryPubSubRegistry::new()),
            EncoderRegistry::with_builtin_encoders(),
            DecoderRegistry::with_builtin_decoders(),
            AggregateFunctionRegistry::with_builtins(),
            StatefulFunctionRegistry::with_builtins(),
            CustomFuncRegistry::with_builtins(),
            EventtimeTypeRegistry::with_builtin_types(),
            Arc::new(MergerRegistry::new()),
        )
    }

    pub fn connector_registry(&self) -> Arc<ConnectorRegistry> {
        Arc::clone(&self.connector_registry)
    }

    pub fn encoder_registry(&self) -> Arc<EncoderRegistry> {
        Arc::clone(&self.encoder_registry)
    }

    pub fn decoder_registry(&self) -> Arc<DecoderRegistry> {
        Arc::clone(&self.decoder_registry)
    }

    pub fn aggregate_registry(&self) -> Arc<AggregateFunctionRegistry> {
        Arc::clone(&self.aggregate_registry)
    }

    pub fn stateful_registry(&self) -> Arc<StatefulFunctionRegistry> {
        Arc::clone(&self.stateful_registry)
    }

    pub fn merger_registry(&self) -> Arc<MergerRegistry> {
        Arc::clone(&self.merger_registry)
    }

    pub fn custom_func_registry(&self) -> Arc<CustomFuncRegistry> {
        Arc::clone(&self.custom_func_registry)
    }

    pub fn eventtime_type_registry(&self) -> Arc<EventtimeTypeRegistry> {
        Arc::clone(&self.eventtime_type_registry)
    }
}

impl Default for PipelineRegistries {
    fn default() -> Self {
        Self::new_with_builtin()
    }
}

fn build_physical_plan_from_sql(
    sql: &str,
    sinks: Vec<PipelineSink>,
    catalog: &Catalog,
    shared_stream_registry: &SharedStreamRegistry,
    registries: &PipelineRegistries,
) -> Result<Arc<planner::physical::PhysicalPlan>, Box<dyn std::error::Error>> {
    let select_stmt = parser::parse_sql_with_registries(
        sql,
        registries.aggregate_registry(),
        registries.stateful_registry(),
    )?;
    let (schema_binding, stream_defs) =
        build_schema_binding(&select_stmt, catalog, Some(shared_stream_registry))?;
    let logical_plan = create_logical_plan(select_stmt, sinks, &stream_defs)?;
    let (logical_plan, pruned_binding) =
        optimize_logical_plan(Arc::clone(&logical_plan), &schema_binding);
    let physical_plan =
        create_physical_plan(Arc::clone(&logical_plan), &pruned_binding, registries)?;
    let optimized_plan = optimize_physical_plan(
        Arc::clone(&physical_plan),
        registries.encoder_registry().as_ref(),
        registries.aggregate_registry(),
    );
    let explain = PipelineExplain::new(
        Arc::clone(&logical_plan),
        Arc::clone(&optimized_plan),
        PipelineExplainConfig::default(),
    );
    tracing::info!(explain = %explain.to_pretty_string(), "pipeline explain");
    Ok(optimized_plan)
}

fn build_schema_binding(
    select_stmt: &parser::SelectStmt,
    catalog: &Catalog,
    shared_stream_registry: Option<&SharedStreamRegistry>,
) -> Result<SchemaBindingResult, Box<dyn std::error::Error>> {
    use crate::expr::sql_conversion::{SchemaBinding, SchemaBindingEntry, SourceBindingKind};
    let mut entries = Vec::new();
    let mut definitions = HashMap::new();
    for source in &select_stmt.source_infos {
        let definition = catalog
            .get(&source.name)
            .ok_or_else(|| format!("stream '{}' not found in catalog", source.name))?;
        let schema = definition.schema();
        let is_shared = shared_stream_registry.is_some_and(|registry| {
            futures::executor::block_on(registry.is_registered(&source.name))
        });
        let kind = if is_shared {
            SourceBindingKind::Shared
        } else if definition.stream_type() == crate::catalog::StreamType::Memory
            && definition.decoder().kind() == "none"
        {
            // Memory collection sources must preserve the full schema for ByIndex correctness.
            SourceBindingKind::MemoryCollection
        } else {
            SourceBindingKind::Regular
        };
        entries.push(SchemaBindingEntry {
            source_name: source.name.clone(),
            alias: source.alias.clone(),
            schema,
            kind,
        });
        definitions.insert(source.name.clone(), definition);
    }
    Ok((SchemaBinding::new(entries), definitions))
}

/// Create a processor pipeline from SQL, wiring it to the provided sink descriptors.
///
/// Use this function when you want to declaratively configure one or more sinks
/// (each with its own connectors/encoders) and plug them into the compiled physical plan.
///
/// # Example
/// ```no_run
/// use flow::{
///     FlowInstance,
///     FlowInstanceOptions,
///     planner::sink::{
///         NopSinkConfig, PipelineSink, PipelineSinkConnector, SinkConnectorConfig, SinkEncoderConfig,
///     },
/// };
///
/// # fn demo() -> Result<(), Box<dyn std::error::Error>> {
/// let instance = FlowInstance::new(FlowInstanceOptions::shared_current_runtime("default", None))?;
/// let connector = PipelineSinkConnector::new(
///     "custom_connector",
///     SinkConnectorConfig::Nop(NopSinkConfig { log: false }),
///     SinkEncoderConfig::json(),
/// );
/// let sink = PipelineSink::new("custom_sink", connector);
/// let pipeline = instance.build_pipeline("SELECT a FROM stream", vec![sink])?;
/// # Ok(()) }
/// ```
pub(crate) fn create_pipeline(
    sql: &str,
    sinks: Vec<PipelineSink>,
    catalog: &Catalog,
    shared_stream_registry: Arc<SharedStreamRegistry>,
    mqtt_client_manager: MqttClientManager,
    spawner: crate::runtime::TaskSpawner,
    registries: &PipelineRegistries,
) -> Result<ProcessorPipeline, Box<dyn std::error::Error>> {
    tracing::info!(sql = sql, "create pipeline");
    let physical_plan = build_physical_plan_from_sql(
        sql,
        sinks,
        catalog,
        shared_stream_registry.as_ref(),
        registries,
    )?;
    let pipeline = create_processor_pipeline(
        physical_plan,
        ProcessorPipelineDependencies::new(
            veloflux_metrics::default_flow_instance_id(),
            mqtt_client_manager,
            Arc::clone(&shared_stream_registry),
            registries,
            None,
            spawner,
        ),
        ProcessorPipelineOptions::default(),
    )?;
    Ok(pipeline)
}

fn validate_eventtime_enabled(
    stream_defs: &HashMap<String, Arc<StreamDefinition>>,
    registries: &PipelineRegistries,
) -> Result<(), Box<dyn std::error::Error>> {
    let registry = registries.eventtime_type_registry();
    let mut saw_eventtime = false;
    for (stream, def) in stream_defs {
        let Some(eventtime) = def.eventtime() else {
            continue;
        };
        saw_eventtime = true;
        let column = eventtime.column();
        if !def.schema().contains_column(column) {
            return Err(format!(
                "eventtime.column `{}` not found in stream `{}` schema",
                column, stream
            )
            .into());
        }
        let type_key = eventtime.eventtime_type();
        if !registry.is_registered(type_key) {
            let available = registry.list().join(", ");
            return Err(format!(
                "eventtime.type `{}` not registered (available: {})",
                type_key, available
            )
            .into());
        }
    }
    if !saw_eventtime {
        return Err("eventtime.enabled=true but no stream declares eventtime".into());
    }
    Ok(())
}

pub fn explain_pipeline_with_options(
    sql: &str,
    sinks: Vec<PipelineSink>,
    catalog: &Catalog,
    shared_stream_registry: Option<&SharedStreamRegistry>,
    registries: &PipelineRegistries,
    options: &crate::pipeline::PipelineOptions,
) -> Result<PipelineExplain, Box<dyn std::error::Error>> {
    let select_stmt = parser::parse_sql_with_registries(
        sql,
        registries.aggregate_registry(),
        registries.stateful_registry(),
    )?;
    let (schema_binding, stream_defs) =
        build_schema_binding(&select_stmt, catalog, shared_stream_registry)?;
    let logical_plan = create_logical_plan(select_stmt, sinks, &stream_defs)?;
    let (logical_plan, pruned_binding) = crate::planner::optimize_logical_plan_with_options(
        Arc::clone(&logical_plan),
        &schema_binding,
        &crate::planner::LogicalOptimizerOptions {
            eventtime_enabled: options.eventtime.enabled,
            shared_slice_registry: shared_stream_registry,
            shared_slice_projection_enabled: options.shared_slice_projection_enabled,
        },
    );
    if options.eventtime.enabled {
        validate_eventtime_enabled(&stream_defs, registries)?;
    }

    let build_options = crate::planner::PhysicalPlanBuildOptions {
        eventtime_enabled: options.eventtime.enabled,
        eventtime_late_tolerance: options.eventtime.late_tolerance,
    };
    let physical_plan = crate::planner::create_physical_plan_with_build_options(
        Arc::clone(&logical_plan),
        &pruned_binding,
        registries,
        &build_options,
    )?;
    let optimized_plan = optimize_physical_plan(
        Arc::clone(&physical_plan),
        registries.encoder_registry().as_ref(),
        registries.aggregate_registry(),
    );

    let shared_stream_decode_applied = shared_stream_registry.map_or_else(HashMap::new, |r| {
        shared_stream_decode_applied_snapshot(&optimized_plan, r)
    });

    Ok(PipelineExplain::new(
        logical_plan,
        optimized_plan,
        PipelineExplainConfig {
            pipeline_options: Some(crate::planner::explain::PipelineExplainOptions {
                eventtime_enabled: options.eventtime.enabled,
                eventtime_late_tolerance_ms: options.eventtime.late_tolerance.as_millis(),
            }),
            shared_stream_decode_applied,
        },
    ))
}

pub(crate) fn explain_pipeline_definition_with_options(
    definition: &crate::pipeline::PipelineDefinition,
    sinks: Vec<PipelineSink>,
    catalog: &Catalog,
    shared_stream_registry: Option<&SharedStreamRegistry>,
    registries: &PipelineRegistries,
    options: &crate::pipeline::PipelineOptions,
) -> Result<PipelineExplain, Box<dyn std::error::Error>> {
    let select_stmt = parser::parse_sql_with_registries(
        definition.sql(),
        registries.aggregate_registry(),
        registries.stateful_registry(),
    )?;
    let (schema_binding, stream_defs) =
        build_schema_binding(&select_stmt, catalog, shared_stream_registry)?;
    let source_inputs = definition
        .sources()
        .iter()
        .map(|source| (source.stream.clone(), source.input.clone()))
        .collect::<HashMap<_, _>>();
    let logical_plan =
        create_logical_plan_with_source_inputs(select_stmt, sinks, &stream_defs, &source_inputs)?;
    let (logical_plan, pruned_binding) = crate::planner::optimize_logical_plan_with_options(
        Arc::clone(&logical_plan),
        &schema_binding,
        &crate::planner::LogicalOptimizerOptions {
            eventtime_enabled: options.eventtime.enabled,
            shared_slice_registry: shared_stream_registry,
            shared_slice_projection_enabled: options.shared_slice_projection_enabled,
        },
    );
    if options.eventtime.enabled {
        validate_eventtime_enabled(&stream_defs, registries)?;
    }

    let build_options = crate::planner::PhysicalPlanBuildOptions {
        eventtime_enabled: options.eventtime.enabled,
        eventtime_late_tolerance: options.eventtime.late_tolerance,
    };
    let physical_plan = crate::planner::create_physical_plan_with_build_options(
        Arc::clone(&logical_plan),
        &pruned_binding,
        registries,
        &build_options,
    )?;
    let optimized_plan = optimize_physical_plan(
        Arc::clone(&physical_plan),
        registries.encoder_registry().as_ref(),
        registries.aggregate_registry(),
    );

    let shared_stream_decode_applied = shared_stream_registry.map_or_else(HashMap::new, |r| {
        shared_stream_decode_applied_snapshot(&optimized_plan, r)
    });

    Ok(PipelineExplain::new(
        logical_plan,
        optimized_plan,
        PipelineExplainConfig {
            pipeline_options: Some(crate::planner::explain::PipelineExplainOptions {
                eventtime_enabled: options.eventtime.enabled,
                eventtime_late_tolerance_ms: options.eventtime.late_tolerance.as_millis(),
            }),
            shared_stream_decode_applied,
        },
    ))
}
