use sqlparser::ast::{Expr, GroupByExpr, SetExpr, Statement};
use sqlparser::parser::ParserError;

use super::window;
pub use window::{Window, parse_window_expr, window_to_expr};

/// Stream processing dialect that supports window functions in GROUP BY clauses
/// Currently supports tumblingwindow (time-based) and countwindow (row-count based)
#[derive(Debug, Clone)]
pub struct StreamDialect {}

impl StreamDialect {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for StreamDialect {
    fn default() -> Self {
        Self::new()
    }
}

impl sqlparser::dialect::Dialect for StreamDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_alphabetic() || ch == '_'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_alphabetic() || ch.is_ascii_digit() || ch == '_'
    }

    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '`' || ch == '"'
    }

    fn supports_filter_during_aggregation(&self) -> bool {
        true
    }

    fn supports_group_by_expr(&self) -> bool {
        true
    }
}

/// Collect window + remaining GROUP BY expressions present in a parsed statement
/// Enforces at most one window per statement
pub fn collect_window_and_group_by_exprs(
    statement: &Statement,
) -> Result<(Option<Window>, Vec<Expr>), ParserError> {
    if let Statement::Query(query) = statement
        && let SetExpr::Select(select) = &*query.body
    {
        return split_group_by_window(&select.group_by);
    }

    Ok((None, Vec::new()))
}

/// Parse GROUP BY clause to extract supported window (if any) and keep the remaining expressions
/// Errors if multiple window functions are present
pub fn split_group_by_window(
    group_by: &GroupByExpr,
) -> Result<(Option<Window>, Vec<Expr>), ParserError> {
    let mut found: Option<Window> = None;
    let mut remaining_exprs: Vec<Expr> = Vec::new();

    if let GroupByExpr::Expressions(exprs) = group_by {
        for expr in exprs {
            if let Some(window) = parse_window_expr(expr)? {
                if found.is_some() {
                    return Err(ParserError::ParserError(
                        "Only one window function is allowed in GROUP BY".to_string(),
                    ));
                }
                found = Some(window);
            } else {
                remaining_exprs.push(expr.clone());
            }
        }
    };

    Ok((found, remaining_exprs))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::parser::Parser;

    #[test]
    fn parse_single_tumbling_window() {
        let sql = "SELECT * FROM stream GROUP BY tumblingwindow('ss', 10)";
        let dialect = StreamDialect::new();

        let statements = Parser::parse_sql(&dialect, sql).unwrap();
        assert_eq!(statements.len(), 1);

        // Collect the window + remaining GROUP BY expressions
        let (window, remaining) = collect_window_and_group_by_exprs(&statements[0]).unwrap();
        assert!(window.is_some());
        assert!(remaining.is_empty());

        assert!(matches!(
            window.unwrap(),
            window::Window::Tumbling {
                ref time_unit,
                length: 10
            } if *time_unit == window::TimeUnit::Seconds
        ));
    }

    #[test]
    fn split_group_by_keeps_non_window_exprs() {
        let sql = "SELECT * FROM stream GROUP BY tumblingwindow('ss', 10), b";
        let dialect = StreamDialect::new();

        let statements = Parser::parse_sql(&dialect, sql).unwrap();
        let (window, remaining) = collect_window_and_group_by_exprs(&statements[0]).unwrap();

        assert!(window.is_some());
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].to_string(), "b");
    }

    #[test]
    fn split_group_by_keeps_non_window_exprs_for_sliding_window() {
        let sql = "SELECT * FROM stream GROUP BY slidingwindow('ss', 10), b";
        let dialect = StreamDialect::new();

        let statements = Parser::parse_sql(&dialect, sql).unwrap();
        let (window, remaining) = collect_window_and_group_by_exprs(&statements[0]).unwrap();

        assert!(window.is_some());
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].to_string(), "b");
    }
}
