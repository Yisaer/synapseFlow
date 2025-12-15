pub mod aggregation;
pub mod catalog;
pub mod codec;
pub mod connector;
pub mod expr;
pub mod instance;
pub mod model;
pub mod pipeline;
pub mod planner;
pub mod processor;
pub mod shared_stream;

pub use aggregation::AggregateFunctionRegistry;
pub use catalog::{
    Catalog, CatalogError, MqttStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps,
    StreamType,
};
pub use codec::{
    CodecError, CollectionEncoder, CollectionEncoderStream, DecoderRegistry, EncodeError,
    EncoderRegistry, JsonDecoder, JsonEncoder, RecordDecoder,
};
pub use datatypes::{
    BooleanType, ColumnSchema, ConcreteDatatype, Float32Type, Float64Type, Int16Type, Int32Type,
    Int64Type, Int8Type, ListType, Schema, StringType, StructField, StructType, Uint16Type,
    Uint32Type, Uint64Type, Uint8Type,
};
pub use expr::sql_conversion;
pub use expr::{
    convert_expr_to_scalar, convert_select_stmt_to_scalar, extract_select_expressions, BinaryFunc,
    ConcatFunc, ConversionError, EvalContext, ScalarExpr, StreamSqlConverter, UnaryFunc,
};
pub use instance::{FlowInstance, FlowInstanceError, StreamRuntimeInfo};
pub use model::{Collection, RecordBatch};
pub use pipeline::{
    MqttSinkProps, PipelineDefinition, PipelineError, PipelineManager, PipelineSnapshot,
    PipelineStatus, SinkDefinition, SinkProps, SinkType,
};
pub use planner::create_physical_plan;
pub use planner::explain::{ExplainReport, ExplainRow, PipelineExplain};
pub use planner::logical::{
    BaseLogicalPlan, DataSinkPlan, DataSource, Filter, LogicalPlan, Project,
};
pub use planner::optimize_physical_plan;
pub use planner::sink::{
    CommonSinkProps, NopSinkConfig, PipelineSink, PipelineSinkConnector, SinkConnectorConfig,
    SinkEncoderConfig,
};
pub use processor::{
    ControlSignal, ControlSourceProcessor, DataSourceProcessor, Processor, ProcessorError,
    ResultCollectProcessor, SinkProcessor, StreamData,
};
pub use shared_stream::{
    registry as shared_stream_registry, SharedSourceConnectorConfig, SharedStreamConfig,
    SharedStreamError, SharedStreamInfo, SharedStreamStatus, SharedStreamSubscription,
};

use connector::{ConnectorRegistry, MqttClientManager};
use planner::logical::create_logical_plan;
use processor::{create_processor_pipeline, ProcessorPipeline};
use shared_stream::SharedStreamRegistry;
use std::collections::HashMap;
use std::sync::Arc;

type SchemaBindingResult = (
    crate::expr::sql_conversion::SchemaBinding,
    HashMap<String, Arc<StreamDefinition>>,
);

/// Bundle of registries required for building pipelines.
pub struct PipelineRegistries {
    connector_registry: Arc<ConnectorRegistry>,
    encoder_registry: Arc<EncoderRegistry>,
    decoder_registry: Arc<DecoderRegistry>,
    aggregate_registry: Arc<AggregateFunctionRegistry>,
}

impl PipelineRegistries {
    pub fn new(
        connector_registry: Arc<ConnectorRegistry>,
        encoder_registry: Arc<EncoderRegistry>,
        decoder_registry: Arc<DecoderRegistry>,
        aggregate_registry: Arc<AggregateFunctionRegistry>,
    ) -> Self {
        Self {
            connector_registry,
            encoder_registry,
            decoder_registry,
            aggregate_registry,
        }
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
}

fn build_physical_plan_from_sql(
    sql: &str,
    sinks: Vec<PipelineSink>,
    catalog: &Catalog,
    shared_stream_registry: &SharedStreamRegistry,
    registries: &PipelineRegistries,
) -> Result<Arc<planner::physical::PhysicalPlan>, Box<dyn std::error::Error>> {
    let select_stmt = parser::parse_sql_with_registry(sql, registries.aggregate_registry())?;
    let (schema_binding, stream_defs) =
        build_schema_binding(&select_stmt, catalog, shared_stream_registry)?;
    let logical_plan = create_logical_plan(select_stmt, sinks, &stream_defs)?;
    let physical_plan =
        create_physical_plan(Arc::clone(&logical_plan), &schema_binding, registries)?;
    let optimized_plan = optimize_physical_plan(
        Arc::clone(&physical_plan),
        registries.encoder_registry().as_ref(),
    );
    let explain = PipelineExplain::new(Arc::clone(&logical_plan), Arc::clone(&optimized_plan));
    println!("[Pipeline Explain]\n{}", explain.to_pretty_string());
    Ok(optimized_plan)
}

fn build_schema_binding(
    select_stmt: &parser::SelectStmt,
    catalog: &Catalog,
    shared_stream_registry: &SharedStreamRegistry,
) -> Result<SchemaBindingResult, Box<dyn std::error::Error>> {
    use crate::expr::sql_conversion::{SchemaBinding, SchemaBindingEntry, SourceBindingKind};
    let mut entries = Vec::new();
    let mut definitions = HashMap::new();
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
///     aggregation::AggregateFunctionRegistry,
///     catalog::Catalog,
///     connector::MqttClientManager,
///     connector::ConnectorRegistry,
///     codec::EncoderRegistry,
///     codec::DecoderRegistry,
///     create_pipeline,
///     planner::sink::{
///         NopSinkConfig, PipelineSink, PipelineSinkConnector, SinkConnectorConfig, SinkEncoderConfig,
///     },
///     shared_stream_registry,
///     PipelineRegistries,
/// };
///
/// # fn demo() -> Result<(), Box<dyn std::error::Error>> {
/// let catalog = Catalog::new();
/// let registry = shared_stream_registry();
/// let mqtt_clients = MqttClientManager::new();
/// let aggregate_registry = AggregateFunctionRegistry::with_builtins();
/// let registries = PipelineRegistries::new(
///     ConnectorRegistry::with_builtin_sinks(),
///     EncoderRegistry::with_builtin_encoders(),
///     DecoderRegistry::with_builtin_decoders(),
///     aggregate_registry,
/// );
/// let connector = PipelineSinkConnector::new(
///     "custom_connector",
///     SinkConnectorConfig::Nop(NopSinkConfig),
///     SinkEncoderConfig::json(),
/// );
/// let sink = PipelineSink::new("custom_sink", connector);
/// let pipeline = create_pipeline(
///     "SELECT a FROM stream",
///     vec![sink],
///     &catalog,
///     registry,
///     mqtt_clients,
///     &registries,
/// )?;
/// # Ok(()) }
/// ```
pub fn create_pipeline(
    sql: &str,
    sinks: Vec<PipelineSink>,
    catalog: &Catalog,
    shared_stream_registry: &SharedStreamRegistry,
    mqtt_client_manager: MqttClientManager,
    registries: &PipelineRegistries,
) -> Result<ProcessorPipeline, Box<dyn std::error::Error>> {
    println!("create pipeline sql:{}", sql);
    let physical_plan =
        build_physical_plan_from_sql(sql, sinks, catalog, shared_stream_registry, registries)?;
    let pipeline = create_processor_pipeline(
        physical_plan,
        mqtt_client_manager,
        registries.connector_registry(),
        registries.encoder_registry(),
        registries.decoder_registry(),
        registries.aggregate_registry(),
    )?;
    Ok(pipeline)
}

pub fn explain_pipeline(
    sql: &str,
    sinks: Vec<PipelineSink>,
    catalog: &Catalog,
    shared_stream_registry: &SharedStreamRegistry,
    registries: &PipelineRegistries,
) -> Result<PipelineExplain, Box<dyn std::error::Error>> {
    let select_stmt = parser::parse_sql_with_registry(sql, registries.aggregate_registry())?;
    let (schema_binding, stream_defs) =
        build_schema_binding(&select_stmt, catalog, shared_stream_registry)?;
    let logical_plan = create_logical_plan(select_stmt, sinks, &stream_defs)?;
    let physical_plan =
        create_physical_plan(Arc::clone(&logical_plan), &schema_binding, registries)?;
    let optimized_plan = optimize_physical_plan(
        Arc::clone(&physical_plan),
        registries.encoder_registry().as_ref(),
    );
    Ok(PipelineExplain::new(logical_plan, optimized_plan))
}

/// Convenience helper for tests and demos that just need a logging mock sink.
pub fn create_pipeline_with_log_sink(
    sql: &str,
    forward_to_result: bool,
    catalog: &Catalog,
    shared_stream_registry: &SharedStreamRegistry,
    mqtt_client_manager: MqttClientManager,
    registries: &PipelineRegistries,
) -> Result<ProcessorPipeline, Box<dyn std::error::Error>> {
    let connector = PipelineSinkConnector::new(
        "log_sink_connector",
        SinkConnectorConfig::Nop(NopSinkConfig),
        SinkEncoderConfig::json(),
    );
    let sink = PipelineSink::new("log_sink", connector).with_forward_to_result(forward_to_result);
    create_pipeline(
        sql,
        vec![sink],
        catalog,
        shared_stream_registry,
        mqtt_client_manager,
        registries,
    )
}
