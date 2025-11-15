pub mod codec;
pub mod connector;
pub mod expr;
pub mod model;
pub mod planner;
pub mod processor;

pub use codec::{
    CodecError, CollectionEncoder, EncodeError, JsonDecoder, JsonEncoder, RawStringDecoder,
    RecordDecoder,
};
pub use datatypes::Schema;
pub use expr::sql_conversion;
pub use expr::{
    convert_expr_to_scalar, convert_select_stmt_to_scalar, extract_select_expressions, BinaryFunc,
    ConcatFunc, ConversionError, EvalContext, ScalarExpr, StreamSqlConverter, UnaryFunc,
};
pub use model::{Collection, RecordBatch};
pub use planner::create_physical_plan;
pub use planner::logical::{BaseLogicalPlan, DataSource, Filter, LogicalPlan, Project};
pub use processor::{
    ControlSignal, ControlSourceProcessor, DataSourceProcessor, Processor, ProcessorError,
    ResultCollectProcessor, SinkProcessor, StreamData,
};

use planner::logical::create_logical_plan;
use processor::{
    create_processor_pipeline, create_processor_pipeline_with_log_sink, ProcessorPipeline,
};
use std::sync::Arc;

fn build_physical_plan_from_sql(
    sql: &str,
) -> Result<Arc<dyn planner::physical::PhysicalPlan>, Box<dyn std::error::Error>> {
    let select_stmt = parser::parse_sql(sql)?;
    let logical_plan = create_logical_plan(select_stmt)?;
    let physical_plan = create_physical_plan(logical_plan)?;
    Ok(physical_plan)
}

/// Create a processor pipeline from SQL, wiring it to the provided sink processors.
///
/// Use this function when you want to configure one or more [`SinkProcessor`] instances
/// (each with its own connectors/encoders) and plug them into the compiled physical plan.
///
/// # Example
/// ```no_run
/// use flow::{connector::MockSinkConnector, create_pipeline, JsonEncoder, processor::SinkProcessor};
/// use std::sync::Arc;
///
/// # fn demo() -> Result<(), Box<dyn std::error::Error>> {
/// let mut sink = SinkProcessor::new("custom_sink");
/// let (connector, _handle) = MockSinkConnector::new("custom");
/// sink.add_connector(Box::new(connector), Arc::new(JsonEncoder::new("json")));
/// let pipeline = create_pipeline("SELECT a FROM stream", vec![sink])?;
/// # Ok(()) }
/// ```
pub fn create_pipeline(
    sql: &str,
    sink_processors: Vec<SinkProcessor>,
) -> Result<ProcessorPipeline, Box<dyn std::error::Error>> {
    let physical_plan = build_physical_plan_from_sql(sql)?;
    let pipeline = create_processor_pipeline(physical_plan, sink_processors)?;
    Ok(pipeline)
}

/// Convenience helper for tests and demos that just need a logging mock sink.
pub fn create_pipeline_with_log_sink(
    sql: &str,
    forward_to_result: bool,
) -> Result<ProcessorPipeline, Box<dyn std::error::Error>> {
    let physical_plan = build_physical_plan_from_sql(sql)?;
    let pipeline = create_processor_pipeline_with_log_sink(physical_plan, forward_to_result)?;
    Ok(pipeline)
}
