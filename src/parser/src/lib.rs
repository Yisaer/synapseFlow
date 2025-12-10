pub mod aggregate_transformer;
pub mod aggregate_registry;
pub mod dialect;
pub mod expression_extractor;
pub mod parser;
pub mod select_stmt;
pub mod visitor;
pub mod window;

pub use aggregate_transformer::transform_aggregate_functions;
pub use aggregate_registry::{
    default_aggregate_registry, AggregateRegistry, StaticAggregateRegistry,
};
pub use dialect::StreamDialect;
pub use expression_extractor::{
    ExpressionAnalysis, analyze_sql_expressions, extract_expressions_from_sql,
    extract_select_expressions_simple,
};
pub use parser::{parse_sql, parse_sql_with_registry, StreamSqlParser};
pub use select_stmt::{SelectField, SelectStmt};
pub use visitor::{
    AggregateVisitor, SourceInfo, TableInfoVisitor, contains_aggregates_with_visitor,
    extract_aggregates_with_visitor,
};
pub use window::Window;
