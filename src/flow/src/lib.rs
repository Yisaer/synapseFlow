pub mod catalog;
pub mod codec;
pub mod connector;
pub mod expr;
pub mod model;
pub mod planner;
pub mod processor;
pub mod shared_stream;

pub use catalog::{global_catalog, Catalog, CatalogError};
pub use codec::{
    CodecError, CollectionEncoder, EncodeError, JsonDecoder, JsonEncoder, RecordDecoder,
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
pub use model::{Collection, RecordBatch};
pub use planner::create_physical_plan;
pub use planner::logical::{
    BaseLogicalPlan, DataSinkPlan, DataSource, Filter, LogicalPlan, Project,
};
pub use planner::sink::{
    NopSinkConfig, PipelineSink, PipelineSinkConnector, SinkConnectorConfig, SinkEncoderConfig,
};
pub use processor::{
    ControlSignal, ControlSourceProcessor, DataSourceProcessor, Processor, ProcessorError,
    ResultCollectProcessor, SinkProcessor, StreamData,
};
pub use shared_stream::{
    registry as shared_stream_registry, SharedSourceConnectorConfig, SharedStreamConfig,
    SharedStreamError, SharedStreamInfo, SharedStreamStatus, SharedStreamSubscription,
};

use planner::logical::create_logical_plan;
use processor::{create_processor_pipeline, ProcessorPipeline};
use std::sync::Arc;

fn build_physical_plan_from_sql(
    sql: &str,
    sinks: Vec<PipelineSink>,
) -> Result<Arc<planner::physical::PhysicalPlan>, Box<dyn std::error::Error>> {
    let select_stmt = parser::parse_sql(sql)?;
    let schema_binding = build_schema_binding(&select_stmt)?;
    let logical_plan = create_logical_plan(select_stmt, sinks)?;
    let physical_plan = create_physical_plan(logical_plan, &schema_binding)?;
    Ok(physical_plan)
}

fn build_schema_binding(
    select_stmt: &parser::SelectStmt,
) -> Result<crate::expr::sql_conversion::SchemaBinding, Box<dyn std::error::Error>> {
    use crate::expr::sql_conversion::{SchemaBinding, SchemaBindingEntry, SourceBindingKind};
    let catalog = catalog::global_catalog();
    let registry = shared_stream_registry();
    let mut entries = Vec::new();
    for source in &select_stmt.source_infos {
        let schema = catalog
            .get(&source.name)
            .ok_or_else(|| format!("schema for source '{}' not found", source.name))?;
        let kind = if futures::executor::block_on(registry.is_registered(&source.name)) {
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
    }
    Ok(SchemaBinding::new(entries))
}

/// Create a processor pipeline from SQL, wiring it to the provided sink descriptors.
///
/// Use this function when you want to declaratively configure one or more sinks
/// (each with its own connectors/encoders) and plug them into the compiled physical plan.
///
/// # Example
/// ```no_run
/// use flow::{
///     create_pipeline,
///     planner::sink::{
///         NopSinkConfig, PipelineSink, PipelineSinkConnector, SinkConnectorConfig, SinkEncoderConfig,
///     },
/// };
///
/// # fn demo() -> Result<(), Box<dyn std::error::Error>> {
/// let connector = PipelineSinkConnector::new(
///     "custom_connector",
///     SinkConnectorConfig::Nop(NopSinkConfig),
///     SinkEncoderConfig::Json { encoder_id: "json".into() },
/// );
/// let sink = PipelineSink::new("custom_sink", vec![connector]);
/// let pipeline = create_pipeline("SELECT a FROM stream", vec![sink])?;
/// # Ok(()) }
/// ```
pub fn create_pipeline(
    sql: &str,
    sinks: Vec<PipelineSink>,
) -> Result<ProcessorPipeline, Box<dyn std::error::Error>> {
    let physical_plan = build_physical_plan_from_sql(sql, sinks)?;
    let pipeline = create_processor_pipeline(physical_plan)?;
    Ok(pipeline)
}

/// Convenience helper for tests and demos that just need a logging mock sink.
pub fn create_pipeline_with_log_sink(
    sql: &str,
    forward_to_result: bool,
) -> Result<ProcessorPipeline, Box<dyn std::error::Error>> {
    let connector = PipelineSinkConnector::new(
        "log_sink_connector",
        SinkConnectorConfig::Nop(NopSinkConfig),
        SinkEncoderConfig::Json {
            encoder_id: "log_sink_encoder".into(),
        },
    );
    let sink =
        PipelineSink::new("log_sink", vec![connector]).with_forward_to_result(forward_to_result);
    create_pipeline(sql, vec![sink])
}
