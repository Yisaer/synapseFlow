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

pub use catalog::{
    Catalog, CatalogError, MqttStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps,
    StreamType,
};
pub use codec::{
    CodecError, CollectionEncoder, CollectionEncoderStream, DecoderRegistry, EncodeError,
    JsonDecoder, JsonEncoder, RecordDecoder,
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
pub use planner::logical::{
    BaseLogicalPlan, DataSinkPlan, DataSource, Filter, LogicalPlan, Project,
};
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

use codec::EncoderRegistry;
use connector::{ConnectorRegistry, MqttClientManager};
use planner::logical::create_logical_plan;
use processor::{create_processor_pipeline, ProcessorPipeline};
use shared_stream::SharedStreamRegistry;
use std::sync::Arc;

fn build_physical_plan_from_sql(
    sql: &str,
    sinks: Vec<PipelineSink>,
    catalog: &Catalog,
    shared_stream_registry: &SharedStreamRegistry,
    encoder_registry: &EncoderRegistry,
) -> Result<Arc<planner::physical::PhysicalPlan>, Box<dyn std::error::Error>> {
    let select_stmt = parser::parse_sql(sql)?;
    let (schema_binding, stream_defs) =
        build_schema_binding(&select_stmt, catalog, shared_stream_registry)?;
    let logical_plan = create_logical_plan(select_stmt, sinks, &stream_defs)?;
    println!("[LogicalPlan] topology:");
    logical_plan.print_topology(0);
    let physical_plan =
        create_physical_plan(Arc::clone(&logical_plan), &schema_binding, encoder_registry)?;
    Ok(physical_plan)
}

fn build_schema_binding(
    select_stmt: &parser::SelectStmt,
    catalog: &Catalog,
    shared_stream_registry: &SharedStreamRegistry,
) -> Result<
    (
        crate::expr::sql_conversion::SchemaBinding,
        std::collections::HashMap<String, Arc<StreamDefinition>>,
    ),
    Box<dyn std::error::Error>,
> {
    use crate::expr::sql_conversion::{SchemaBinding, SchemaBindingEntry, SourceBindingKind};
    let mut entries = Vec::new();
    let mut definitions = std::collections::HashMap::new();
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
/// };
///
/// # fn demo() -> Result<(), Box<dyn std::error::Error>> {
/// let catalog = Catalog::new();
/// let registry = shared_stream_registry();
/// let mqtt_clients = MqttClientManager::new();
/// let connector_registry = ConnectorRegistry::with_builtin_sinks();
/// let encoder_registry = EncoderRegistry::with_builtin_encoders();
/// let decoder_registry = DecoderRegistry::with_builtin_decoders();
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
///     connector_registry,
///     encoder_registry,
///     decoder_registry,
/// )?;
/// # Ok(()) }
/// ```
pub fn create_pipeline(
    sql: &str,
    sinks: Vec<PipelineSink>,
    catalog: &Catalog,
    shared_stream_registry: &SharedStreamRegistry,
    mqtt_client_manager: MqttClientManager,
    connector_registry: Arc<ConnectorRegistry>,
    encoder_registry: Arc<EncoderRegistry>,
    decoder_registry: Arc<DecoderRegistry>,
) -> Result<ProcessorPipeline, Box<dyn std::error::Error>> {
    let physical_plan = build_physical_plan_from_sql(
        sql,
        sinks,
        catalog,
        shared_stream_registry,
        encoder_registry.as_ref(),
    )?;
    let pipeline = create_processor_pipeline(
        physical_plan,
        mqtt_client_manager,
        connector_registry,
        encoder_registry,
        decoder_registry,
    )?;
    Ok(pipeline)
}

/// Convenience helper for tests and demos that just need a logging mock sink.
pub fn create_pipeline_with_log_sink(
    sql: &str,
    forward_to_result: bool,
    catalog: &Catalog,
    shared_stream_registry: &SharedStreamRegistry,
    mqtt_client_manager: MqttClientManager,
    connector_registry: Arc<ConnectorRegistry>,
    encoder_registry: Arc<EncoderRegistry>,
    decoder_registry: Arc<DecoderRegistry>,
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
        connector_registry,
        encoder_registry,
        decoder_registry,
    )
}
