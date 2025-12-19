use sqlparser::ast::{Expr, Ident, Query, Select, SelectItem, SetExpr, Statement, Visit};
use sqlparser::parser::Parser;

use crate::aggregate_registry::{AggregateRegistry, default_aggregate_registry};
use crate::aggregate_transformer::transform_aggregate_functions;
use crate::dialect::StreamDialect;
use crate::select_stmt::{SelectField, SelectStmt};
use crate::visitor::TableInfoVisitor;
use std::sync::Arc;

/// SQL Parser based on StreamDialect
pub struct StreamSqlParser {
    dialect: StreamDialect,
    aggregate_registry: Arc<dyn AggregateRegistry>,
}

impl StreamSqlParser {
    /// Create a new StreamSqlParser
    pub fn new() -> Self {
        Self::with_registry(default_aggregate_registry())
    }

    /// Create a StreamSqlParser with a specific aggregate registry
    pub fn with_registry(aggregate_registry: Arc<dyn AggregateRegistry>) -> Self {
        Self {
            dialect: StreamDialect::new(),
            aggregate_registry,
        }
    }

    /// Parse SQL string and return SelectStmt containing select fields and aggregate mappings
    /// This is the main entry point for parsing SQL with StreamDialect
    /// Automatically transforms aggregate functions during parsing
    pub fn parse(&self, sql: &str) -> Result<SelectStmt, String> {
        // Create a parser with StreamDialect
        let parser =
            Parser::parse_sql(&self.dialect, sql).map_err(|e| format!("Parse error: {}", e))?;

        if parser.len() != 1 {
            return Err("Expected exactly one SQL statement".to_string());
        }

        let statement = &parser[0];

        // Collect window + non-window GROUP BY expressions before we move on
        let (window, group_by_exprs) = crate::dialect::collect_window_and_group_by_exprs(statement)
            .map_err(|e| format!("Dialect processing error: {}", e))?;

        // Extract raw select fields from the statement (before transformation)
        let mut select_stmt = self.extract_select_fields(statement)?;
        select_stmt.window = window;
        select_stmt.group_by_exprs = group_by_exprs;

        // Transform aggregate functions in one step (search + replace)
        let (transformed_stmt, _aggregate_mappings) =
            transform_aggregate_functions(select_stmt, Arc::clone(&self.aggregate_registry))?;

        Ok(transformed_stmt)
    }

    /// Extract select fields from a parsed SQL statement
    fn extract_select_fields(&self, statement: &Statement) -> Result<SelectStmt, String> {
        match statement {
            Statement::Query(query) => self.extract_from_query(query),
            _ => Err("Expected a SELECT query".to_string()),
        }
    }

    /// Extract select fields from a query
    fn extract_from_query(&self, query: &Query) -> Result<SelectStmt, String> {
        match &*query.body {
            SetExpr::Select(select) => self.extract_from_select(select),
            _ => Err("Expected a simple SELECT query".to_string()),
        }
    }

    /// Extract select fields from a SELECT statement
    fn extract_from_select(&self, select: &Select) -> Result<SelectStmt, String> {
        let mut select_fields = Vec::new();

        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    let field_name = expr.to_string();
                    select_fields.push(SelectField::new(expr.clone(), None, field_name));
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let field_name = alias.value.clone();
                    select_fields.push(SelectField::new(
                        expr.clone(),
                        Some(alias.value.clone()),
                        field_name,
                    ));
                }
                SelectItem::Wildcard(_) => {
                    let expr = Expr::Identifier(Ident::new("*"));
                    let field_name = expr.to_string();
                    select_fields.push(SelectField::new(expr, None, field_name));
                }
                SelectItem::QualifiedWildcard(object_name, _) => {
                    let mut idents = object_name.0.clone();
                    idents.push(Ident::new("*"));
                    let expr = Expr::CompoundIdentifier(idents);
                    let field_name = expr.to_string();
                    select_fields.push(SelectField::new(expr, None, field_name));
                }
            }
        }

        // Extract WHERE and HAVING clauses if present
        let where_condition = select.selection.clone();
        let having = select.having.clone();

        // Use visitor pattern to extract table (source) information
        let mut table_visitor = TableInfoVisitor::new();
        let _ = select.visit(&mut table_visitor);
        let source_infos = table_visitor.get_sources();

        let mut select_stmt =
            SelectStmt::with_fields_and_conditions(select_fields, where_condition, having);
        select_stmt.source_infos = source_infos;

        Ok(select_stmt)
    }

    // Window validation (e.g. only allowed in GROUP BY) is intentionally not enforced here.
}

impl Default for StreamSqlParser {
    fn default() -> Self {
        Self::new()
    }
}

/// Convenience function to parse SQL and return SelectStmt
pub fn parse_sql(sql: &str) -> Result<SelectStmt, String> {
    let parser = StreamSqlParser::new();
    parser.parse(sql)
}

/// Convenience function to parse SQL with an explicit aggregate registry
pub fn parse_sql_with_registry(
    sql: &str,
    aggregate_registry: Arc<dyn AggregateRegistry>,
) -> Result<SelectStmt, String> {
    let parser = StreamSqlParser::with_registry(aggregate_registry);
    parser.parse(sql)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_agg_replacement_expr_field_name() {
        let parser = StreamSqlParser::new();
        let result = parser.parse("SELECT sum(a) + 1");
        assert!(result.is_ok());
        let select_stmt = result.unwrap();
        assert_eq!(select_stmt.select_fields.len(), 1);

        let field = &select_stmt.select_fields[0];
        assert_eq!(field.alias, Some("sum(a) + 1".to_string()));
        assert_eq!(field.field_name, "sum(a) + 1".to_string());
        assert_eq!(field.expr.to_string(), "col_1 + 1".to_string());
    }

    #[test]
    fn test_parse_simple_select() {
        let parser = StreamSqlParser::new();
        let result = parser.parse("SELECT a + b");

        assert!(result.is_ok());
        let select_stmt = result.unwrap();
        assert_eq!(select_stmt.select_fields.len(), 1);

        let field = &select_stmt.select_fields[0];
        assert!(field.alias.is_none());
        assert_eq!(field.field_name, "a + b".to_string());
    }

    #[test]
    fn test_parse_select_with_alias() {
        let parser = StreamSqlParser::new();
        let result = parser.parse("SELECT a + b AS total");

        assert!(result.is_ok());
        let select_stmt = result.unwrap();
        assert_eq!(select_stmt.select_fields.len(), 1);

        let field = &select_stmt.select_fields[0];
        assert_eq!(field.alias, Some("total".to_string()));
        assert_eq!(field.field_name, "total".to_string());
    }

    #[test]
    fn test_parse_multiple_fields() {
        let parser = StreamSqlParser::new();
        let result = parser.parse("SELECT a, b + c, CONCAT(name, 'test') AS full_name");

        assert!(result.is_ok());
        let select_stmt = result.unwrap();
        assert_eq!(select_stmt.select_fields.len(), 3);

        // First field: a
        assert_eq!(select_stmt.select_fields[0].alias, None);
        assert_eq!(select_stmt.select_fields[0].field_name, "a");
        // Second field: b + c
        assert_eq!(select_stmt.select_fields[1].alias, None);
        assert_eq!(select_stmt.select_fields[1].field_name, "b + c");
        // Third field: CONCAT with alias
        assert_eq!(
            select_stmt.select_fields[2].alias,
            Some("full_name".to_string())
        );
        assert_eq!(select_stmt.select_fields[2].field_name, "full_name");
    }

    #[test]
    fn test_parse_invalid_sql() {
        let parser = StreamSqlParser::new();
        let result = parser.parse("INVALID SQL");

        assert!(result.is_err());
    }

    #[test]
    fn test_parse_non_select() {
        let parser = StreamSqlParser::new();
        let result = parser.parse("INSERT INTO table VALUES (1)");

        assert!(result.is_err());
        let error_msg = result.unwrap_err();
        assert!(error_msg.contains("Parse error"));
    }

    #[test]
    fn test_convenience_function() {
        let result = parse_sql("SELECT a * b");
        assert!(result.is_ok());

        let select_stmt = result.unwrap();
        assert_eq!(select_stmt.select_fields.len(), 1);
    }
}

#[cfg(test)]
mod source_info_tests {
    use super::*;
    use crate::Window;

    #[test]
    fn test_parse_select_with_single_table() {
        let parser = StreamSqlParser::new();
        let result = parser.parse("SELECT a, b FROM users");

        assert!(result.is_ok());
        let select_stmt = result.unwrap();
        assert_eq!(select_stmt.source_infos.len(), 1);

        let source = &select_stmt.source_infos[0];
        assert_eq!(source.name, "users");
        assert_eq!(source.alias, None);
    }

    #[test]
    fn test_parse_select_with_table_alias() {
        let parser = StreamSqlParser::new();
        let result = parser.parse("SELECT a, b FROM users AS u");

        assert!(result.is_ok());
        let select_stmt = result.unwrap();
        assert_eq!(select_stmt.source_infos.len(), 1);

        let source = &select_stmt.source_infos[0];
        assert_eq!(source.name, "users");
        assert_eq!(source.alias, Some("u".to_string()));
    }

    #[test]
    fn test_parse_select_with_multiple_tables() {
        let parser = StreamSqlParser::new();
        let result = parser.parse("SELECT a, b FROM users AS u, orders AS o");

        assert!(result.is_ok());
        let select_stmt = result.unwrap();
        assert_eq!(select_stmt.source_infos.len(), 2);

        assert_eq!(select_stmt.source_infos[0].name, "users");
        assert_eq!(select_stmt.source_infos[0].alias, Some("u".to_string()));
        assert_eq!(select_stmt.source_infos[1].name, "orders");
        assert_eq!(select_stmt.source_infos[1].alias, Some("o".to_string()));
    }

    #[test]
    fn test_parse_select_with_where_clause() {
        let parser = StreamSqlParser::new();
        let result = parser.parse("SELECT a, b FROM users WHERE a > 10");

        assert!(result.is_ok());
        let select_stmt = result.unwrap();
        assert_eq!(select_stmt.source_infos.len(), 1);
        assert_eq!(select_stmt.source_infos[0].name, "users");
        assert!(select_stmt.where_condition.is_some());
    }

    #[test]
    fn parse_group_by_window() {
        let parser = StreamSqlParser::new();
        let result = parser.parse("SELECT * FROM stream GROUP BY tumblingwindow('ss', 10)");

        assert!(result.is_ok());
        let select_stmt = result.unwrap();
        assert_eq!(select_stmt.group_by_exprs.len(), 0);
        assert!(select_stmt.window.is_some());

        match select_stmt.window {
            Some(Window::Tumbling { time_unit, length }) => {
                assert_eq!(time_unit, crate::window::TimeUnit::Seconds);
                assert_eq!(length, 10);
            }
            _ => panic!("Expected tumbling window"),
        }
    }

    #[test]
    fn parse_group_by_sliding_window() {
        let parser = StreamSqlParser::new();
        let result = parser.parse("SELECT * FROM stream GROUP BY slidingwindow('ss', 10)");

        assert!(result.is_ok());
        let select_stmt = result.unwrap();
        assert_eq!(select_stmt.group_by_exprs.len(), 0);
        assert!(select_stmt.window.is_some());

        match select_stmt.window {
            Some(Window::Sliding {
                time_unit,
                lookback,
                lookahead,
            }) => {
                assert_eq!(time_unit, crate::window::TimeUnit::Seconds);
                assert_eq!(lookback, 10);
                assert_eq!(lookahead, None);
            }
            other => panic!("Expected sliding window, got {:?}", other),
        }
    }

    #[test]
    fn reject_multiple_windows() {
        let parser = StreamSqlParser::new();
        let result =
            parser.parse("SELECT * FROM stream GROUP BY tumblingwindow('ss', 10), countwindow(3)");

        assert!(result.is_err());
        let msg = result.unwrap_err();
        assert!(
            msg.contains("Only one window function is allowed"),
            "unexpected error: {}",
            msg
        );
    }

    #[test]
    fn parse_group_by_with_column_and_window() {
        let parser = StreamSqlParser::new();
        let sql = "SELECT * FROM stream GROUP BY tumblingwindow('ss', 10), b";
        let result = parser.parse(sql);

        assert!(result.is_ok(), "parse failed: {:?}", result);
        let select_stmt = result.unwrap();

        // window should be parsed
        match select_stmt.window {
            Some(Window::Tumbling { time_unit, length }) => {
                assert_eq!(time_unit, crate::window::TimeUnit::Seconds);
                assert_eq!(length, 10);
            }
            other => panic!("expected tumbling window, got {:?}", other),
        }

        // group_by_exprs should only contain the column, not the window function
        assert_eq!(select_stmt.group_by_exprs.len(), 1);
        let expr = &select_stmt.group_by_exprs[0];
        match expr {
            Expr::Identifier(ident) => assert_eq!(ident.to_string(), "b"),
            other => panic!("expected group by identifier `b`, got {:?}", other),
        }
    }

    #[test]
    fn parse_group_by_with_column_and_sliding_window() {
        let parser = StreamSqlParser::new();
        let sql = "SELECT * FROM stream GROUP BY slidingwindow('ss', 10, 15), b";
        let result = parser.parse(sql);

        assert!(result.is_ok(), "parse failed: {:?}", result);
        let select_stmt = result.unwrap();

        match select_stmt.window {
            Some(Window::Sliding {
                time_unit,
                lookback,
                lookahead,
            }) => {
                assert_eq!(time_unit, crate::window::TimeUnit::Seconds);
                assert_eq!(lookback, 10);
                assert_eq!(lookahead, Some(15));
            }
            other => panic!("expected sliding window, got {:?}", other),
        }

        assert_eq!(select_stmt.group_by_exprs.len(), 1);
        let expr = &select_stmt.group_by_exprs[0];
        match expr {
            Expr::Identifier(ident) => assert_eq!(ident.to_string(), "b"),
            other => panic!("expected group by identifier `b`, got {:?}", other),
        }
    }

    #[test]
    fn parse_group_by_state_window() {
        let parser = StreamSqlParser::new();
        let sql = "SELECT * FROM stream GROUP BY statewindow(a > 0, b = 1)";
        let result = parser.parse(sql);

        assert!(result.is_ok(), "parse failed: {:?}", result);
        let select_stmt = result.unwrap();

        assert!(select_stmt.window.is_some());
        assert_eq!(select_stmt.group_by_exprs.len(), 0);

        match select_stmt.window {
            Some(Window::State { open, emit }) => {
                assert_eq!(open.as_ref().to_string(), "a > 0");
                assert_eq!(emit.as_ref().to_string(), "b = 1");
            }
            other => panic!("expected state window, got {:?}", other),
        }
    }

    #[test]
    fn parse_group_by_with_column_and_state_window() {
        let parser = StreamSqlParser::new();
        let sql = "SELECT * FROM stream GROUP BY statewindow(a > 0, b = 1), c";
        let result = parser.parse(sql);

        assert!(result.is_ok(), "parse failed: {:?}", result);
        let select_stmt = result.unwrap();

        match select_stmt.window {
            Some(Window::State { open, emit }) => {
                assert_eq!(open.as_ref().to_string(), "a > 0");
                assert_eq!(emit.as_ref().to_string(), "b = 1");
            }
            other => panic!("expected state window, got {:?}", other),
        }

        assert_eq!(select_stmt.group_by_exprs.len(), 1);
        let expr = &select_stmt.group_by_exprs[0];
        match expr {
            Expr::Identifier(ident) => assert_eq!(ident.to_string(), "c"),
            other => panic!("expected group by identifier `c`, got {:?}", other),
        }
    }
}
