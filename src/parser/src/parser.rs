use sqlparser::ast::{Expr, Ident, Query, Select, SelectItem, SetExpr, Statement, Visit};
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Location, Token, TokenWithLocation, Tokenizer};

use crate::aggregate_registry::{AggregateRegistry, default_aggregate_registry};
use crate::aggregate_transformer::transform_aggregate_functions;
use crate::col_placeholder_allocator::ColPlaceholderAllocator;
use crate::dialect::StreamDialect;
use crate::select_stmt::{OrderByItem, SelectField, SelectStmt};
use crate::stateful_registry::{StatefulRegistry, default_stateful_registry};
use crate::stateful_transformer::transform_stateful_functions;
use crate::visitor::TableInfoVisitor;
use crate::window::Window;
use std::sync::Arc;

/// SQL Parser based on StreamDialect
pub struct StreamSqlParser {
    dialect: StreamDialect,
    aggregate_registry: Arc<dyn AggregateRegistry>,
    stateful_registry: Arc<dyn StatefulRegistry>,
}

impl StreamSqlParser {
    /// Create a new StreamSqlParser
    pub fn new() -> Self {
        Self::with_registries(default_aggregate_registry(), default_stateful_registry())
    }

    /// Create a StreamSqlParser with a specific aggregate registry
    pub fn with_registry(aggregate_registry: Arc<dyn AggregateRegistry>) -> Self {
        Self::with_registries(aggregate_registry, default_stateful_registry())
    }

    pub fn with_registries(
        aggregate_registry: Arc<dyn AggregateRegistry>,
        stateful_registry: Arc<dyn StatefulRegistry>,
    ) -> Self {
        Self {
            dialect: StreamDialect::new(),
            aggregate_registry,
            stateful_registry,
        }
    }

    /// Parse SQL string and return SelectStmt containing select fields and aggregate mappings
    /// This is the main entry point for parsing SQL with StreamDialect
    /// Automatically transforms aggregate functions during parsing
    pub fn parse(&self, sql: &str) -> Result<SelectStmt, String> {
        let sliding_over = preprocess_sliding_window_over(sql, &self.dialect)?;

        // Create a parser with StreamDialect
        let parser = Parser::parse_sql(&self.dialect, &sliding_over.sql)
            .map_err(|e| format!("Parse error: {}", e))?;

        if parser.len() != 1 {
            return Err("Expected exactly one SQL statement".to_string());
        }

        let statement = &parser[0];

        // Collect window + non-window GROUP BY expressions before we move on
        let (mut window, group_by_exprs) =
            crate::dialect::collect_window_and_group_by_exprs(statement)
                .map_err(|e| format!("Dialect processing error: {}", e))?;
        if let Some(trigger_condition) = sliding_over.trigger_condition {
            window = match window {
                Some(sliding @ Window::Sliding { .. }) => {
                    Some(sliding.with_sliding_trigger_condition(Some(Box::new(trigger_condition))))
                }
                Some(other) => {
                    return Err(format!(
                        "slidingwindow OVER WHEN was parsed but no GROUP BY slidingwindow was found; found {:?}",
                        other,
                    ));
                }
                None => {
                    return Err(
                        "slidingwindow OVER WHEN was parsed but no GROUP BY window function was found"
                            .to_string(),
                    );
                }
            };
        }

        // Extract raw select fields from the statement (before transformation)
        let mut select_stmt = self.extract_select_fields(statement)?;
        select_stmt.window = window;
        select_stmt.group_by_exprs = group_by_exprs;

        let mut allocator = ColPlaceholderAllocator::new();

        // Transform stateful functions first (search + replace, with dedup).
        // This enables cases like last_row(lag(a)) where a stateful call appears inside an aggregate.
        let (select_stmt, _stateful_mappings) = transform_stateful_functions(
            select_stmt,
            Arc::clone(&self.aggregate_registry),
            Arc::clone(&self.stateful_registry),
            &mut allocator,
        )?;

        // Transform aggregate functions after stateful rewrite.
        let (transformed_stmt, _aggregate_mappings) = transform_aggregate_functions(
            select_stmt,
            Arc::clone(&self.aggregate_registry),
            &mut allocator,
        )?;

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
        let mut select_stmt = match &*query.body {
            SetExpr::Select(select) => self.extract_from_select(select)?,
            _ => return Err("Expected a simple SELECT query".to_string()),
        };

        let mut order_by_items = Vec::with_capacity(query.order_by.len());
        for item in &query.order_by {
            if item.nulls_first.is_some() {
                return Err("NULLS FIRST/LAST in ORDER BY is not supported yet".to_string());
            }
            order_by_items.push(OrderByItem {
                expr: item.expr.clone(),
                asc: item.asc.unwrap_or(true),
            });
        }
        select_stmt.order_by = order_by_items;
        Ok(select_stmt)
    }

    /// Extract select fields from a SELECT statement
    fn extract_from_select(&self, select: &Select) -> Result<SelectStmt, String> {
        let mut select_fields = Vec::new();

        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    let field_name = projection_field_name(expr);
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

fn projection_field_name(expr: &Expr) -> String {
    match expr {
        Expr::Identifier(ident) => ident.value.clone(),
        Expr::CompoundIdentifier(idents) => idents
            .iter()
            .map(|ident| ident.value.as_str())
            .collect::<Vec<_>>()
            .join("."),
        _ => expr.to_string(),
    }
}

struct SlidingOverPreprocessResult {
    sql: String,
    trigger_condition: Option<Expr>,
}

struct Replacement {
    start: usize,
    end: usize,
    text: String,
}

/// Preprocess SQL to extract `OVER (WHEN <expr>)` from slidingwindow and remove it
/// from the SQL text so the standard parser can handle the rest.
///
/// Handles three cases:
/// - `slidingwindow(...) OVER (WHEN <expr> PARTITION BY ...)` → extracts WHEN, keeps PARTITION BY
/// - `slidingwindow(...) OVER (WHEN <expr>)` → extracts WHEN, removes entire OVER clause
/// - `slidingwindow(...)` (no OVER, or OVER without WHEN) → no-op
fn preprocess_sliding_window_over(
    sql: &str,
    dialect: &StreamDialect,
) -> Result<SlidingOverPreprocessResult, String> {
    let mut tokenizer = Tokenizer::new(dialect, sql);
    let tokens = tokenizer
        .tokenize_with_location()
        .map_err(|err| format!("Parse error: {err}"))?;
    let non_ws = non_whitespace_token_indexes(&tokens);
    let mut replacements = Vec::new();
    let mut trigger_condition = None;

    for &token_idx in &non_ws {
        if !is_word(&tokens[token_idx].token, "slidingwindow") {
            continue;
        }

        let Some(fn_lparen) = next_non_whitespace(&tokens, token_idx + 1) else {
            continue;
        };
        if !matches!(tokens[fn_lparen].token, Token::LParen) {
            continue;
        }
        let fn_rparen = matching_rparen(&tokens, fn_lparen)?;

        let mut cursor = next_non_whitespace(&tokens, fn_rparen + 1);
        if let Some(filter_idx) = cursor
            && is_word(&tokens[filter_idx].token, "filter")
        {
            let Some(filter_lparen) = next_non_whitespace(&tokens, filter_idx + 1) else {
                return Err("slidingwindow FILTER requires parentheses".to_string());
            };
            if !matches!(tokens[filter_lparen].token, Token::LParen) {
                return Err("slidingwindow FILTER requires parentheses".to_string());
            }
            let filter_rparen = matching_rparen(&tokens, filter_lparen)?;
            cursor = next_non_whitespace(&tokens, filter_rparen + 1);
        }

        let Some(over_idx) = cursor else {
            continue;
        };
        if !is_word(&tokens[over_idx].token, "over") {
            continue;
        }
        let Some(over_lparen) = next_non_whitespace(&tokens, over_idx + 1) else {
            return Err("slidingwindow OVER requires parentheses".to_string());
        };
        if !matches!(tokens[over_lparen].token, Token::LParen) {
            return Err("slidingwindow OVER requires parentheses".to_string());
        }
        let over_rparen = matching_rparen(&tokens, over_lparen)?;
        let Some(first_over_token) = next_non_whitespace(&tokens, over_lparen + 1) else {
            continue;
        };
        if !is_word(&tokens[first_over_token].token, "when") {
            continue;
        }
        if trigger_condition.is_some() {
            return Err("Only one slidingwindow OVER WHEN clause is allowed".to_string());
        }

        let Some(expr_start_token) = next_non_whitespace(&tokens, first_over_token + 1) else {
            return Err("slidingwindow OVER WHEN requires an expression".to_string());
        };
        if expr_start_token >= over_rparen {
            return Err("slidingwindow OVER WHEN requires an expression".to_string());
        }

        let partition_token = find_top_level_partition_by(&tokens, expr_start_token, over_rparen)?;
        let expr_end_token = partition_token.unwrap_or(over_rparen);
        let expr_start = token_start_offset(sql, &tokens[expr_start_token])?;
        let expr_end = token_start_offset(sql, &tokens[expr_end_token])?;
        let expr_sql = sql
            .get(expr_start..expr_end)
            .ok_or_else(|| "failed to slice slidingwindow OVER WHEN expression".to_string())?
            .trim();
        if expr_sql.is_empty() {
            return Err("slidingwindow OVER WHEN requires an expression".to_string());
        }
        trigger_condition = Some(parse_over_when_expr(expr_sql, dialect)?);

        if partition_token.is_some() {
            // Remove `WHEN <expr>` portion (including the WHEN keyword),
            // leaving `OVER (PARTITION BY ...)` intact.
            let when_start = token_start_offset(sql, &tokens[first_over_token])?;
            replacements.push(Replacement {
                start: when_start,
                end: expr_end,
                text: String::new(),
            });
        } else {
            // Remove entire `OVER (WHEN <expr>)`
            let over_start = token_start_offset(sql, &tokens[over_idx])?;
            let over_end = token_start_offset(sql, &tokens[over_rparen])?.saturating_add(1);
            replacements.push(Replacement {
                start: over_start,
                end: over_end,
                text: String::new(),
            });
        }
    }

    let mut normalized = sql.to_string();
    for replacement in replacements.iter().rev() {
        normalized.replace_range(replacement.start..replacement.end, &replacement.text);
    }

    Ok(SlidingOverPreprocessResult {
        sql: normalized,
        trigger_condition,
    })
}

fn parse_over_when_expr(expr_sql: &str, dialect: &StreamDialect) -> Result<Expr, String> {
    let sql = format!("SELECT {expr_sql}");
    let statements =
        Parser::parse_sql(dialect, &sql).map_err(|err| format!("Parse error: {err}"))?;
    let Some(Statement::Query(query)) = statements.first() else {
        return Err("failed to parse slidingwindow OVER WHEN expression".to_string());
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err("failed to parse slidingwindow OVER WHEN expression".to_string());
    };
    let Some(item) = select.projection.first() else {
        return Err("failed to parse slidingwindow OVER WHEN expression".to_string());
    };
    match item {
        SelectItem::UnnamedExpr(expr) => Ok(expr.clone()),
        _ => Err("slidingwindow OVER WHEN must be a scalar expression".to_string()),
    }
}

fn non_whitespace_token_indexes(tokens: &[TokenWithLocation]) -> Vec<usize> {
    tokens
        .iter()
        .enumerate()
        .filter_map(|(idx, token)| (!is_whitespace(&token.token)).then_some(idx))
        .collect()
}

fn next_non_whitespace(tokens: &[TokenWithLocation], start: usize) -> Option<usize> {
    tokens
        .iter()
        .enumerate()
        .skip(start)
        .find_map(|(idx, token)| (!is_whitespace(&token.token)).then_some(idx))
}

fn is_whitespace(token: &Token) -> bool {
    matches!(token, Token::Whitespace(_))
}

fn is_word(token: &Token, expected: &str) -> bool {
    match token {
        Token::Word(word) => word.value.eq_ignore_ascii_case(expected),
        _ => false,
    }
}

fn matching_rparen(tokens: &[TokenWithLocation], lparen: usize) -> Result<usize, String> {
    let mut depth = 0usize;
    for (idx, token) in tokens.iter().enumerate().skip(lparen) {
        match token.token {
            Token::LParen => depth = depth.saturating_add(1),
            Token::RParen => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Ok(idx);
                }
            }
            _ => {}
        }
    }
    Err("unclosed parentheses in slidingwindow clause".to_string())
}

fn find_top_level_partition_by(
    tokens: &[TokenWithLocation],
    start: usize,
    end: usize,
) -> Result<Option<usize>, String> {
    let mut depth = 0usize;
    let mut idx = start;
    while idx < end {
        match tokens[idx].token {
            Token::LParen | Token::LBracket | Token::LBrace => depth = depth.saturating_add(1),
            Token::RParen | Token::RBracket | Token::RBrace => depth = depth.saturating_sub(1),
            _ => {
                if depth == 0 && is_word(&tokens[idx].token, "partition") {
                    let Some(by_idx) = next_non_whitespace(tokens, idx + 1) else {
                        return Ok(None);
                    };
                    if by_idx < end && is_word(&tokens[by_idx].token, "by") {
                        return Ok(Some(idx));
                    }
                }
            }
        }
        idx += 1;
    }
    Ok(None)
}

fn token_start_offset(sql: &str, token: &TokenWithLocation) -> Result<usize, String> {
    location_to_byte_offset(sql, token.location).ok_or_else(|| {
        format!(
            "failed to resolve SQL token location at line {}, column {}",
            token.location.line, token.location.column
        )
    })
}

fn location_to_byte_offset(sql: &str, location: Location) -> Option<usize> {
    let mut line = 1u64;
    let mut column = 1u64;
    for (offset, ch) in sql.char_indices() {
        if line == location.line && column == location.column {
            return Some(offset);
        }
        if ch == '\n' {
            line = line.saturating_add(1);
            column = 1;
        } else {
            column = column.saturating_add(1);
        }
    }
    (line == location.line && column == location.column).then_some(sql.len())
}

/// Convenience function to parse SQL and return SelectStmt
pub fn parse_sql(sql: &str) -> Result<SelectStmt, String> {
    let parser = StreamSqlParser::new();
    parser.parse(sql)
}

pub fn parse_sql_with_registries(
    sql: &str,
    aggregate_registry: Arc<dyn AggregateRegistry>,
    stateful_registry: Arc<dyn StatefulRegistry>,
) -> Result<SelectStmt, String> {
    let parser = StreamSqlParser::with_registries(aggregate_registry, stateful_registry);
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
    fn test_parse_quoted_identifier_projection_names_are_unquoted() {
        let parser = StreamSqlParser::new();
        let result = parser.parse("SELECT `TDU_1.TMInletWaterTempFltSts`, `msg`.`sig`");

        assert!(result.is_ok());
        let select_stmt = result.unwrap();
        assert_eq!(select_stmt.select_fields.len(), 2);
        assert_eq!(
            select_stmt.select_fields[0].field_name,
            "TDU_1.TMInletWaterTempFltSts"
        );
        assert_eq!(select_stmt.select_fields[1].field_name, "msg.sig");
        assert_eq!(
            select_stmt.select_fields[0].expr.to_string(),
            "`TDU_1.TMInletWaterTempFltSts`"
        );
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

    #[test]
    fn test_parse_order_by_basic() {
        let parser = StreamSqlParser::new();
        let result = parser.parse("SELECT a FROM t ORDER BY b DESC, c");
        assert!(result.is_ok());
        let select_stmt = result.unwrap();
        assert_eq!(select_stmt.order_by.len(), 2);
        assert_eq!(select_stmt.order_by[0].expr.to_string(), "b");
        assert_eq!(select_stmt.order_by[0].asc, false);
        assert_eq!(select_stmt.order_by[1].expr.to_string(), "c");
        assert_eq!(select_stmt.order_by[1].asc, true);
    }

    #[test]
    fn test_parse_order_by_nulls_first_rejected() {
        let parser = StreamSqlParser::new();
        let result = parser.parse("SELECT a FROM t ORDER BY b NULLS FIRST");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("NULLS FIRST/LAST in ORDER BY is not supported yet")
        );
    }

    #[test]
    fn test_parse_order_by_aggregate_rewrite() {
        let parser = StreamSqlParser::new();
        let result = parser.parse("SELECT a FROM t ORDER BY sum(b)");
        assert!(result.is_ok());
        let select_stmt = result.unwrap();
        assert_eq!(select_stmt.order_by.len(), 1);

        // Aggregate is rewritten to a placeholder column.
        assert_eq!(select_stmt.order_by[0].expr.to_string(), "col_1");
        assert_eq!(select_stmt.aggregate_mappings.len(), 1);
        assert_eq!(
            select_stmt
                .aggregate_mappings
                .get("col_1")
                .unwrap()
                .to_string(),
            "sum(b)"
        );
    }

    #[test]
    fn test_parse_order_by_stateful_rewrite() {
        let parser = StreamSqlParser::new();
        let result = parser.parse("SELECT a FROM t ORDER BY lag(a)");
        assert!(result.is_ok());
        let select_stmt = result.unwrap();
        assert_eq!(select_stmt.order_by.len(), 1);
        assert_eq!(select_stmt.order_by[0].expr.to_string(), "col_1");
        assert_eq!(select_stmt.stateful_mappings.len(), 1);
        assert_eq!(
            select_stmt
                .stateful_mappings
                .iter()
                .find(|entry| entry.output_column == "col_1")
                .unwrap()
                .spec
                .original_expr
                .to_string(),
            "lag(a)"
        );
    }

    #[test]
    fn test_parse_acc_rewrite() {
        let parser = StreamSqlParser::new();
        let result = parser.parse("SELECT acc_sum(a) + 1 FROM t");
        assert!(result.is_ok());
        let select_stmt = result.unwrap();

        assert_eq!(select_stmt.select_fields.len(), 1);
        assert_eq!(select_stmt.select_fields[0].expr.to_string(), "col_1 + 1");
        assert_eq!(select_stmt.select_fields[0].alias, None);
        assert_eq!(select_stmt.acc_mappings.len(), 0);
        assert_eq!(select_stmt.stateful_mappings.len(), 1);

        let mapping = &select_stmt.stateful_mappings[0];
        assert_eq!(mapping.output_column, "col_1");
        assert_eq!(mapping.spec.func_name, "acc_sum");
        assert_eq!(
            mapping
                .spec
                .args
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            vec!["a"]
        );
        assert_eq!(mapping.spec.original_expr.to_string(), "acc_sum(a)");
    }

    #[test]
    fn test_parse_acc_reuses_same_expression() {
        let parser = StreamSqlParser::new();
        let result = parser.parse("SELECT acc_count(a), acc_count(a) FROM t");
        assert!(result.is_ok());
        let select_stmt = result.unwrap();

        assert_eq!(select_stmt.select_fields[0].expr.to_string(), "col_1");
        assert_eq!(select_stmt.select_fields[1].expr.to_string(), "col_1");
        assert_eq!(select_stmt.acc_mappings.len(), 0);
        assert_eq!(select_stmt.stateful_mappings.len(), 1);
    }

    #[test]
    fn test_parse_acc_does_not_alias_unrelated_fields() {
        let parser = StreamSqlParser::new();
        let result = parser.parse("SELECT acc_count(a), b FROM t");
        assert!(result.is_ok());
        let select_stmt = result.unwrap();

        assert_eq!(select_stmt.select_fields[0].alias, None);
        assert_eq!(select_stmt.select_fields[1].alias, None);
        assert_eq!(select_stmt.select_fields[1].field_name, "b");
    }

    #[test]
    fn test_parse_acc_allows_multiple_distinct_calls() {
        let parser = StreamSqlParser::new();
        let result = parser.parse("SELECT acc_count(a), acc_sum(b) FROM t");
        assert!(result.is_ok());
        let select_stmt = result.unwrap();
        assert_eq!(select_stmt.stateful_mappings.len(), 2);
        assert_eq!(select_stmt.stateful_mappings[0].spec.func_name, "acc_count");
        assert_eq!(select_stmt.stateful_mappings[1].spec.func_name, "acc_sum");
    }

    #[test]
    fn test_parse_acc_supports_filter_and_over_partition_by() {
        let parser = StreamSqlParser::new();
        let result =
            parser.parse("SELECT acc_sum(a) FILTER (WHERE flag) OVER (PARTITION BY k) FROM t");
        assert!(result.is_ok());
        let select_stmt = result.unwrap();
        assert_eq!(select_stmt.stateful_mappings.len(), 1);
        let mapping = &select_stmt.stateful_mappings[0];
        assert_eq!(mapping.spec.func_name, "acc_sum");
        assert_eq!(mapping.spec.when.as_ref().unwrap().to_string(), "flag");
        assert_eq!(mapping.spec.partition_by[0].to_string(), "k");
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
            Some(Window::Tumbling {
                time_unit, length, ..
            }) => {
                assert_eq!(time_unit, crate::window::TimeUnit::Seconds);
                assert_eq!(length, 10);
            }
            _ => panic!("Expected tumbling window"),
        }
    }

    #[test]
    fn parse_group_by_tumbling_window_over_partition_by() {
        let parser = StreamSqlParser::new();
        let sql =
            "SELECT * FROM stream GROUP BY tumblingwindow('ss', 10) OVER (PARTITION BY k1, k2)";
        let result = parser.parse(sql);

        assert!(result.is_ok(), "parse failed: {:?}", result);
        let select_stmt = result.unwrap();
        assert_eq!(select_stmt.group_by_exprs.len(), 0);

        match select_stmt.window {
            Some(Window::Tumbling {
                time_unit,
                length,
                partition_by,
                ..
            }) => {
                assert_eq!(time_unit, crate::window::TimeUnit::Seconds);
                assert_eq!(length, 10);
                assert_eq!(partition_by.len(), 2);
                assert_eq!(partition_by[0].to_string(), "k1");
                assert_eq!(partition_by[1].to_string(), "k2");
            }
            other => panic!("expected tumbling window, got {:?}", other),
        }
    }

    #[test]
    fn parse_group_by_count_window_over_partition_by() {
        let parser = StreamSqlParser::new();
        let sql = "SELECT * FROM stream GROUP BY countwindow(3) OVER (PARTITION BY k)";
        let result = parser.parse(sql);

        assert!(result.is_ok(), "parse failed: {:?}", result);
        let select_stmt = result.unwrap();
        assert_eq!(select_stmt.group_by_exprs.len(), 0);

        match select_stmt.window {
            Some(Window::Count {
                count,
                partition_by,
                ..
            }) => {
                assert_eq!(count, 3);
                assert_eq!(partition_by.len(), 1);
                assert_eq!(partition_by[0].to_string(), "k");
            }
            other => panic!("expected count window, got {:?}", other),
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
                ..
            }) => {
                assert_eq!(time_unit, crate::window::TimeUnit::Seconds);
                assert_eq!(lookback, 10);
                assert_eq!(lookahead, None);
            }
            other => panic!("Expected sliding window, got {:?}", other),
        }
    }

    #[test]
    fn parse_group_by_sliding_window_over_partition_by() {
        let parser = StreamSqlParser::new();
        let sql = "SELECT * FROM stream GROUP BY slidingwindow('ss', 10, 15) OVER (PARTITION BY k)";
        let result = parser.parse(sql);

        assert!(result.is_ok(), "parse failed: {:?}", result);
        let select_stmt = result.unwrap();
        assert_eq!(select_stmt.group_by_exprs.len(), 0);

        match select_stmt.window {
            Some(Window::Sliding {
                time_unit,
                lookback,
                lookahead,
                partition_by,
                ..
            }) => {
                assert_eq!(time_unit, crate::window::TimeUnit::Seconds);
                assert_eq!(lookback, 10);
                assert_eq!(lookahead, Some(15));
                assert_eq!(partition_by.len(), 1);
                assert_eq!(partition_by[0].to_string(), "k");
            }
            other => panic!("expected sliding window, got {:?}", other),
        }
    }

    #[test]
    fn parse_group_by_eos_window() {
        let parser = StreamSqlParser::new();
        let result = parser.parse("SELECT sum(a) FROM history_table GROUP BY eoswindow()");

        assert!(result.is_ok());
        let select_stmt = result.unwrap();
        assert_eq!(select_stmt.group_by_exprs.len(), 0);
        assert_eq!(select_stmt.window, Some(Window::eos()));
    }

    #[test]
    fn reject_eos_window_over_partition_by() {
        let parser = StreamSqlParser::new();
        let result = parser
            .parse("SELECT sum(a) FROM history_table GROUP BY eoswindow() OVER (PARTITION BY k)");

        assert!(result.is_err());
        let msg = result.unwrap_err();
        assert!(
            msg.contains("eoswindow does not support OVER"),
            "unexpected error: {}",
            msg
        );
    }

    #[test]
    fn reject_window_over_order_by() {
        let parser = StreamSqlParser::new();
        let result = parser.parse("SELECT * FROM stream GROUP BY countwindow(3) OVER (ORDER BY k)");

        assert!(result.is_err());
        let msg = result.unwrap_err();
        assert!(
            msg.contains("countwindow OVER does not support ORDER BY"),
            "unexpected error: {}",
            msg
        );
    }

    #[test]
    fn select_eos_window_function_does_not_set_group_window() {
        let parser = StreamSqlParser::new();
        let result = parser.parse("SELECT eoswindow() FROM history_table");

        assert!(result.is_ok());
        let select_stmt = result.unwrap();
        assert!(select_stmt.window.is_none());
        assert_eq!(select_stmt.select_fields.len(), 1);
        assert_eq!(select_stmt.select_fields[0].expr.to_string(), "eoswindow()");
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
            Some(Window::Tumbling {
                time_unit, length, ..
            }) => {
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
                ..
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
            Some(Window::State {
                open,
                emit,
                partition_by,
                ..
            }) => {
                assert_eq!(open.as_ref().to_string(), "a > 0");
                assert_eq!(emit.as_ref().to_string(), "b = 1");
                assert!(partition_by.is_empty());
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
            Some(Window::State {
                open,
                emit,
                partition_by,
                ..
            }) => {
                assert_eq!(open.as_ref().to_string(), "a > 0");
                assert_eq!(emit.as_ref().to_string(), "b = 1");
                assert!(partition_by.is_empty());
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

    #[test]
    fn parse_group_by_state_window_over_partition_by() {
        let parser = StreamSqlParser::new();
        let sql =
            "SELECT * FROM stream GROUP BY statewindow(a > 0, b = 1) OVER (PARTITION BY k1, k2)";
        let result = parser.parse(sql);

        assert!(result.is_ok(), "parse failed: {:?}", result);
        let select_stmt = result.unwrap();

        assert!(select_stmt.window.is_some());
        assert_eq!(select_stmt.group_by_exprs.len(), 0);

        match select_stmt.window {
            Some(Window::State {
                open,
                emit,
                partition_by,
                ..
            }) => {
                assert_eq!(open.as_ref().to_string(), "a > 0");
                assert_eq!(emit.as_ref().to_string(), "b = 1");
                assert_eq!(partition_by.len(), 2);
                assert_eq!(partition_by[0].to_string(), "k1");
                assert_eq!(partition_by[1].to_string(), "k2");
            }
            other => panic!("expected state window, got {:?}", other),
        }
    }

    #[test]
    fn parse_group_by_sliding_window_over_when() {
        let parser = StreamSqlParser::new();
        let sql = "SELECT * FROM stream GROUP BY slidingwindow('ss', 10) OVER (WHEN a > 1)";
        let result = parser.parse(sql);

        assert!(result.is_ok(), "parse failed: {:?}", result);
        let select_stmt = result.unwrap();
        assert!(select_stmt.group_by_exprs.is_empty());

        match select_stmt.window {
            Some(Window::Sliding {
                lookback,
                lookahead,
                partition_by,
                trigger_condition,
                ..
            }) => {
                assert_eq!(lookback, 10);
                assert_eq!(lookahead, None);
                assert!(partition_by.is_empty());
                assert_eq!(
                    trigger_condition.as_ref().map(|e| e.to_string()),
                    Some("a > 1".to_string()),
                );
            }
            other => panic!("expected sliding window with trigger condition, got {other:?}"),
        }
    }

    #[test]
    fn parse_group_by_sliding_window_over_when_partition_by() {
        let parser = StreamSqlParser::new();
        let sql = "SELECT * FROM stream GROUP BY slidingwindow('ss', 10) OVER (WHEN a > 1 PARTITION BY k)";
        let result = parser.parse(sql);

        assert!(result.is_ok(), "parse failed: {:?}", result);
        let select_stmt = result.unwrap();
        assert!(select_stmt.group_by_exprs.is_empty());

        match select_stmt.window {
            Some(Window::Sliding {
                lookback,
                lookahead,
                partition_by,
                trigger_condition,
                ..
            }) => {
                assert_eq!(lookback, 10);
                assert_eq!(lookahead, None);
                assert_eq!(partition_by.len(), 1);
                assert_eq!(partition_by[0].to_string(), "k");
                assert_eq!(
                    trigger_condition.as_ref().map(|e| e.to_string()),
                    Some("a > 1".to_string()),
                );
            }
            other => panic!(
                "expected sliding window with trigger condition and partition, got {other:?}"
            ),
        }
    }

    #[test]
    fn parse_group_by_sliding_window_over_when_complex_expr() {
        let parser = StreamSqlParser::new();
        let sql =
            "SELECT * FROM stream GROUP BY slidingwindow('ss', 10) OVER (WHEN a > 1 AND b = 0)";
        let result = parser.parse(sql);

        assert!(result.is_ok(), "parse failed: {:?}", result);
        let select_stmt = result.unwrap();

        match select_stmt.window {
            Some(Window::Sliding {
                trigger_condition, ..
            }) => {
                assert_eq!(
                    trigger_condition.as_ref().map(|e| e.to_string()),
                    Some("a > 1 AND b = 0".to_string()),
                );
            }
            other => panic!("expected sliding window, got {other:?}"),
        }
    }

    #[test]
    fn parse_group_by_sliding_window_over_when_no_expr() {
        let parser = StreamSqlParser::new();
        let sql = "SELECT * FROM stream GROUP BY slidingwindow('ss', 10) OVER (WHEN)";
        let result = parser.parse(sql);

        assert!(result.is_err(), "expected error for empty WHEN expression");
        assert!(result.unwrap_err().contains("requires an expression"));
    }

    #[test]
    fn parse_sliding_window_over_when_with_lookahead() {
        let parser = StreamSqlParser::new();
        let sql = "SELECT * FROM stream GROUP BY slidingwindow('ss', 10, 15) OVER (WHEN a > 1 PARTITION BY k1, k2)";
        let result = parser.parse(sql);

        assert!(result.is_ok(), "parse failed: {:?}", result);
        let select_stmt = result.unwrap();

        match select_stmt.window {
            Some(Window::Sliding {
                lookback,
                lookahead,
                partition_by,
                trigger_condition,
                ..
            }) => {
                assert_eq!(lookback, 10);
                assert_eq!(lookahead, Some(15));
                assert_eq!(partition_by.len(), 2);
                assert_eq!(partition_by[0].to_string(), "k1");
                assert_eq!(partition_by[1].to_string(), "k2");
                assert_eq!(
                    trigger_condition.as_ref().map(|e| e.to_string()),
                    Some("a > 1".to_string()),
                );
            }
            other => panic!(
                "expected sliding window with lookahead, trigger, and partition, got {other:?}"
            ),
        }
    }

    #[test]
    fn parse_sliding_window_with_filter_and_over_when() {
        let parser = StreamSqlParser::new();
        let sql = "SELECT * FROM stream GROUP BY slidingwindow('ss', 10) FILTER (WHERE x > 0) OVER (WHEN a > 1)";
        let result = parser.parse(sql);

        assert!(result.is_ok(), "parse failed: {:?}", result);
        let select_stmt = result.unwrap();

        match select_stmt.window {
            Some(Window::Sliding {
                trigger_condition,
                filter,
                ..
            }) => {
                assert_eq!(
                    trigger_condition.as_ref().map(|e| e.to_string()),
                    Some("a > 1".to_string()),
                );
                assert_eq!(
                    filter.as_ref().map(|e| e.to_string()),
                    Some("x > 0".to_string()),
                );
            }
            other => panic!("expected sliding window, got {other:?}"),
        }
    }

    #[test]
    fn parse_group_by_sliding_window_over_partition_by_without_when() {
        let parser = StreamSqlParser::new();
        let sql = "SELECT * FROM stream GROUP BY slidingwindow('ss', 10, 15) OVER (PARTITION BY k)";
        let result = parser.parse(sql);

        assert!(result.is_ok(), "parse failed: {:?}", result);
        let select_stmt = result.unwrap();

        match select_stmt.window {
            Some(Window::Sliding {
                lookback,
                lookahead,
                partition_by,
                trigger_condition,
                ..
            }) => {
                assert_eq!(lookback, 10);
                assert_eq!(lookahead, Some(15));
                assert_eq!(partition_by.len(), 1);
                assert_eq!(partition_by[0].to_string(), "k");
                assert!(trigger_condition.is_none());
            }
            other => panic!("expected sliding window with partition, got {:?}", other),
        }
    }
}
