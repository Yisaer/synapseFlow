pub mod aggregate_registry;
pub mod aggregate_transformer;
pub mod capabilities;
pub mod col_placeholder_allocator;
pub mod dialect;
pub mod expression_extractor;
pub mod parser;
pub mod select_stmt;
pub mod stateful_registry;
pub mod stateful_transformer;
pub mod visitor;
pub mod window;

pub use aggregate_registry::{
    AggregateRegistry, StaticAggregateRegistry, default_aggregate_registry,
};
pub use aggregate_transformer::transform_aggregate_functions;
pub use col_placeholder_allocator::ColPlaceholderAllocator;
pub use dialect::StreamDialect;
pub use expression_extractor::{
    ExpressionAnalysis, analyze_sql_expressions, extract_expressions_from_sql,
    extract_select_expressions_simple,
};
pub use parser::{StreamSqlParser, parse_sql, parse_sql_with_registries};
pub use select_stmt::{SelectField, SelectStmt};
pub use stateful_registry::{StatefulRegistry, StaticStatefulRegistry, default_stateful_registry};
pub use stateful_transformer::transform_stateful_functions;
pub use visitor::{
    AggregateVisitor, SourceInfo, TableInfoVisitor, contains_aggregates_with_visitor,
    extract_aggregates_with_visitor,
};
pub use window::Window;

pub use capabilities::{
    SyntaxCapabilities, SyntaxConstruct, SyntaxConstructKind, SyntaxFeatureStatus, SyntaxPlacement,
    syntax_capabilities, syntax_capabilities_owned,
};
