use sqlparser::ast::{Query, Select, SelectItem, SetExpr, Statement};
use sqlparser::parser::Parser;

use crate::dialect::StreamDialect;
use crate::select_stmt::{SelectStmt, SelectField};
use crate::aggregate_transformer::transform_aggregate_functions;

/// SQL Parser based on StreamDialect
pub struct StreamSqlParser {
    dialect: StreamDialect,
}

impl StreamSqlParser {
    /// Create a new StreamSqlParser
    pub fn new() -> Self {
        Self {
            dialect: StreamDialect::new(),
        }
    }

    /// Parse SQL string and return SelectStmt containing select fields and aggregate mappings
    /// This is the main entry point for parsing SQL with StreamDialect
    /// Automatically transforms aggregate functions during parsing
    pub fn parse(&self, sql: &str) -> Result<SelectStmt, String> {
        // Create a parser with StreamDialect
        let mut parser = Parser::parse_sql(&self.dialect, sql)
            .map_err(|e| format!("Parse error: {}", e))?;

        if parser.len() != 1 {
            return Err("Expected exactly one SQL statement".to_string());
        }

        let statement = &mut parser[0];
        
        // Process the statement to handle any dialect-specific features
        if let Err(e) = crate::dialect::process_tumblingwindow_in_statement(statement) {
            return Err(format!("Dialect processing error: {}", e));
        }

        // Extract raw select fields from the statement (before transformation)
        let select_stmt = self.extract_select_fields(statement)?;
        
        // Transform aggregate functions in one step (search + replace)
        let (transformed_stmt, _aggregate_mappings) = transform_aggregate_functions(select_stmt)?;
        
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
                    select_fields.push(SelectField::new(expr.clone(), None));
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    select_fields.push(SelectField::new(
                        expr.clone(), 
                        Some(alias.value.clone())
                    ));
                }
                SelectItem::Wildcard(_) => {
                    return Err("Wildcard (*) is not supported in select fields".to_string());
                }
                SelectItem::QualifiedWildcard(_, _) => {
                    return Err("Qualified wildcard is not supported in select fields".to_string());
                }
            }
        }

        // Extract WHERE and HAVING clauses if present
        let where_condition = select.selection.clone();
        let having = select.having.clone();

        Ok(SelectStmt::with_fields_and_conditions(select_fields, where_condition, having))
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_select() {
        let parser = StreamSqlParser::new();
        let result = parser.parse("SELECT a + b");
        
        assert!(result.is_ok());
        let select_stmt = result.unwrap();
        assert_eq!(select_stmt.select_fields.len(), 1);
        
        let field = &select_stmt.select_fields[0];
        assert!(field.alias.is_none());
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
        // Second field: b + c
        assert_eq!(select_stmt.select_fields[1].alias, None);
        // Third field: CONCAT with alias
        assert_eq!(select_stmt.select_fields[2].alias, Some("full_name".to_string()));
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

/// Demo test showing the complete StreamDialect parser workflow
#[cfg(test)]
mod demo_tests {
    use super::*;
    
    #[test]
    fn demo_stream_dialect_parsing() {
        println!("\n=== StreamDialect Parser Demo ===\n");
        
        let parser = StreamSqlParser::new();
        
        // Demo 1: Simple SELECT
        println!("1. Simple SELECT:");
        let sql1 = "SELECT a + b, c * 2, CONCAT(name, '_test') AS full_name";
        println!("   SQL: {}", sql1);
        
        match parser.parse(sql1) {
            Ok(select_stmt) => {
                println!("   ✓ Parse successful!");
                println!("   ✓ Found {} select fields", select_stmt.select_fields.len());
                
                for (i, field) in select_stmt.select_fields.iter().enumerate() {
                    println!("   Field {}: {:?}", i + 1, field.expr);
                    if let Some(alias) = &field.alias {
                        println!("     Alias: {}", alias);
                    }
                }
            }
            Err(e) => {
                println!("   ✗ Parse failed: {}", e);
            }
        }
        
        // Demo 2: Complex expression
        println!("\n2. Complex Expression:");
        let sql2 = "SELECT (a + b) * c AS result";
        println!("   SQL: {}", sql2);
        
        match parse_sql(sql2) {
            Ok(select_stmt) => {
                println!("   ✓ Parse successful!");
                println!("   ✓ Found {} select fields", select_stmt.select_fields.len());
                
                for field in &select_stmt.select_fields {
                    println!("   Field: {:?}", field.expr);
                    if let Some(alias) = &field.alias {
                        println!("     Alias: {}", alias);
                    }
                }
            }
            Err(e) => {
                println!("   ✗ Parse failed: {}", e);
            }
        }
        
        println!("\n✅ StreamDialect Parser Demo Complete!");
    }
}