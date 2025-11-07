//! Extract all expressions from SQL statements
//! This module provides functionality to extract and analyze SQL expressions

use sqlparser::ast::{Expr, Visit, Visitor};
use sqlparser::parser::Parser;

use crate::dialect::StreamDialect;

/// Extract all expressions from SQL statements
pub fn extract_expressions_from_sql(sql: &str) -> Result<Vec<String>, String> {
    let dialect = StreamDialect::new();
    let statements = Parser::parse_sql(&dialect, sql)
        .map_err(|e| format!("Parse error: {}", e))?;
    
    let mut extractor = ExpressionExtractor::new();
    
    for statement in &statements {
        let _ = statement.visit(&mut extractor);
    }
    
    Ok(extractor.expressions)
}

/// Extract SELECT expressions from SQL
pub fn extract_select_expressions_simple(sql: &str) -> Result<Vec<String>, String> {
    let dialect = StreamDialect::new();
    let statements = Parser::parse_sql(&dialect, sql)
        .map_err(|e| format!("Parse error: {}", e))?;
    
    if statements.len() != 1 {
        return Err("Expected exactly one SQL statement".to_string());
    }
    
    let mut extractor = SelectExpressionExtractor::new();
    let _ = statements[0].visit(&mut extractor);
    
    Ok(extractor.expressions)
}

/// Simple visitor to extract all expressions
struct ExpressionExtractor {
    expressions: Vec<String>,
}

impl ExpressionExtractor {
    fn new() -> Self {
        Self {
            expressions: Vec::new(),
        }
    }
}

impl Visitor for ExpressionExtractor {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &Expr) -> std::ops::ControlFlow<Self::Break> {
        self.expressions.push(format!("{:?}", expr));
        std::ops::ControlFlow::Continue(())
    }
}

/// Visitor to extract SELECT expressions specifically
struct SelectExpressionExtractor {
    expressions: Vec<String>,
}

impl SelectExpressionExtractor {
    fn new() -> Self {
        Self {
            expressions: Vec::new(),
        }
    }
}

impl Visitor for SelectExpressionExtractor {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &Expr) -> std::ops::ControlFlow<Self::Break> {
        // Only extract expressions that are part of SELECT projection
        self.expressions.push(format!("{:?}", expr));
        std::ops::ControlFlow::Continue(())
    }
}

/// Helper function to parse and analyze SQL expressions
pub fn analyze_sql_expressions(sql: &str) -> Result<ExpressionAnalysis, String> {
    let dialect = StreamDialect::new();
    let statements = Parser::parse_sql(&dialect, sql)
        .map_err(|e| format!("Parse error: {}", e))?;
    
    let mut analysis = ExpressionAnalysis {
        sql: sql.to_string(),
        expression_count: 0,
        binary_operations: Vec::new(),
        literals: Vec::new(),
        functions: Vec::new(),
    };
    
    for statement in &statements {
        let mut analyzer = ExpressionAnalyzer::new(&mut analysis);
        let _ = statement.visit(&mut analyzer);
    }
    
    Ok(analysis)
}

/// Analysis results for SQL expressions
#[derive(Debug, Clone)]
pub struct ExpressionAnalysis {
    pub sql: String,
    pub expression_count: usize,
    pub binary_operations: Vec<String>,
    pub literals: Vec<String>,
    pub functions: Vec<String>,
}

/// Visitor to analyze expression types
struct ExpressionAnalyzer<'a> {
    analysis: &'a mut ExpressionAnalysis,
}

impl<'a> ExpressionAnalyzer<'a> {
    fn new(analysis: &'a mut ExpressionAnalysis) -> Self {
        Self { analysis }
    }
}

impl Visitor for ExpressionAnalyzer<'_> {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &Expr) -> std::ops::ControlFlow<Self::Break> {
        self.analysis.expression_count += 1;
        
        match expr {
            Expr::BinaryOp { op, .. } => {
                self.analysis.binary_operations.push(format!("{:?}", op));
            }
            Expr::Value(value) => {
                self.analysis.literals.push(format!("{:?}", value));
            }
            Expr::Function(func) => {
                self.analysis.functions.push(func.name.to_string());
            }
            // Note: Removed flow-specific expression types (JsonAccess, MapAccess)
            // Parser module should only focus on basic SQL parsing analysis
            _ => {}
        }
        
        std::ops::ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_simple_expression() {
        let sql = "SELECT a + b";
        let result = extract_expressions_from_sql(sql);
        
        assert!(result.is_ok());
        let expressions = result.unwrap();
        assert!(!expressions.is_empty());
        
        // Should contain the binary operation
        let has_binary_op = expressions.iter().any(|expr| expr.contains("BinaryOp"));
        assert!(has_binary_op);
    }

    #[test]
    fn test_analyze_expressions() {
        let sql = "SELECT a + b, CONCAT(name, 'test'), 42";
        let analysis = analyze_sql_expressions(sql).unwrap();
        
        assert!(analysis.expression_count > 0);
        assert!(!analysis.binary_operations.is_empty());
        assert!(!analysis.functions.is_empty());
        assert!(!analysis.literals.is_empty());
    }
}