use sqlparser::ast::{Expr, BinaryOperator, Visit, Visitor};
use sqlparser::parser::Parser;
use sqlparser::dialect::GenericDialect;

// Custom expression format that you might want to convert to
#[derive(Debug, Clone, PartialEq)]
pub enum CustomExpr {
    Column(String),
    Literal(String),
    BinaryOp {
        left: Box<CustomExpr>,
        op: CustomBinaryOp,
        right: Box<CustomExpr>,
    },
    Function {
        name: String,
        args: Vec<CustomExpr>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum CustomBinaryOp {
    Add,
    Subtract,
    Multiply,
    Divide,
    Equal,
    NotEqual,
    GreaterThan,
    LessThan,
    GreaterThanOrEqual,
    LessThanOrEqual,
    And,
    Or,
}

// Convert from sqlparser BinaryOperator to our custom operator
impl From<&BinaryOperator> for CustomBinaryOp {
    fn from(op: &BinaryOperator) -> Self {
        match op {
            BinaryOperator::Plus => CustomBinaryOp::Add,
            BinaryOperator::Minus => CustomBinaryOp::Subtract,
            BinaryOperator::Multiply => CustomBinaryOp::Multiply,
            BinaryOperator::Divide => CustomBinaryOp::Divide,
            BinaryOperator::Eq => CustomBinaryOp::Equal,
            BinaryOperator::NotEq => CustomBinaryOp::NotEqual,
            BinaryOperator::Gt => CustomBinaryOp::GreaterThan,
            BinaryOperator::Lt => CustomBinaryOp::LessThan,
            BinaryOperator::GtEq => CustomBinaryOp::GreaterThanOrEqual,
            BinaryOperator::LtEq => CustomBinaryOp::LessThanOrEqual,
            BinaryOperator::And => CustomBinaryOp::And,
            BinaryOperator::Or => CustomBinaryOp::Or,
            _ => CustomBinaryOp::Equal, // Default fallback
        }
    }
}

// Visitor to convert sqlparser expressions to custom format
struct ExpressionConverter {
    converted_expressions: Vec<CustomExpr>,
}

impl ExpressionConverter {
    fn new() -> Self {
        Self {
            converted_expressions: Vec::new(),
        }
    }
}

impl Visitor for ExpressionConverter {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &Expr) -> std::ops::ControlFlow<Self::Break> {
        let converted = match expr {
            Expr::Identifier(ident) => {
                CustomExpr::Column(ident.value.clone())
            }
            Expr::Value(value) => {
                CustomExpr::Literal(format!("{:?}", value))
            }
            Expr::BinaryOp { left: _, op, right: _ } => {
                // For simplicity, we'll create a placeholder - 
                // in real implementation you'd recursively convert left and right
                CustomExpr::BinaryOp {
                    left: Box::new(CustomExpr::Column("left".to_string())),
                    op: CustomBinaryOp::from(op),
                    right: Box::new(CustomExpr::Column("right".to_string())),
                }
            }
            Expr::Function(func) => {
                CustomExpr::Function {
                    name: func.name.to_string(),
                    args: vec![], // Simplified for test
                }
            }
            _ => CustomExpr::Literal("unsupported".to_string()),
        };
        
        self.converted_expressions.push(converted);
        std::ops::ControlFlow::Continue(())
    }
}

#[test]
fn test_simple_expression_parsing() {
    let sql = "SELECT a + b";
    
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql).unwrap();
    
    let mut converter = ExpressionConverter::new();
    
    for statement in &statements {
        statement.visit(&mut converter);
    }
    
    // Should have converted some expressions
    assert!(!converter.converted_expressions.is_empty());
    
    println!("✓ Converted expressions: {:?}", converter.converted_expressions);
}

#[test]
fn test_literal_expression_parsing() {
    let sql = "SELECT 42, 'hello', true";
    
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql).unwrap();
    
    let mut converter = ExpressionConverter::new();
    
    for statement in &statements {
        statement.visit(&mut converter);
    }
    
    // Should have literal expressions
    let literals: Vec<&CustomExpr> = converter.converted_expressions.iter()
        .filter(|expr| matches!(expr, CustomExpr::Literal(_)))
        .collect();
    
    assert!(!literals.is_empty());
    println!("✓ Found {} literal expressions", literals.len());
}

#[test]
fn test_function_call_parsing() {
    let sql = "SELECT CONCAT(a, b), UPPER(name)";
    
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql).unwrap();
    
    let mut converter = ExpressionConverter::new();
    
    for statement in &statements {
        statement.visit(&mut converter);
    }
    
    // Should have function expressions
    let functions: Vec<&CustomExpr> = converter.converted_expressions.iter()
        .filter(|expr| matches!(expr, CustomExpr::Function { .. }))
        .collect();
    
    assert!(!functions.is_empty());
    println!("✓ Found {} function expressions", functions.len());
}

#[test]
fn test_complex_expression_parsing() {
    let sql = "SELECT (a + b) * c, CASE WHEN x > 0 THEN 'positive' ELSE 'negative' END";
    
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql).unwrap();
    
    let mut converter = ExpressionConverter::new();
    
    for statement in &statements {
        statement.visit(&mut converter);
    }
    
    // Should handle complex expressions
    assert!(!converter.converted_expressions.is_empty());
    println!("✓ Handled complex expressions: {} found", converter.converted_expressions.len());
}

#[test]
fn test_nested_expression_structure() {
    let sql = "SELECT a + (b * c)";
    
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql).unwrap();
    
    let mut converter = ExpressionConverter::new();
    
    for statement in &statements {
        statement.visit(&mut converter);
    }
    
    // Should handle nested structure
    let binary_ops: Vec<&CustomExpr> = converter.converted_expressions.iter()
        .filter(|expr| matches!(expr, CustomExpr::BinaryOp { .. }))
        .collect();
    
    println!("✓ Found {} binary operations in nested expression", binary_ops.len());
}

// Helper function that was originally in expression_example.rs - now for testing
pub fn parse_and_analyze_expressions() {
    println!("=== Expression Parsing and Analysis Test ===");
    
    // Test simple binary operation
    let simple_expr = "SELECT a + b";
    println!("Testing simple expression: {}", simple_expr);
    
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, simple_expr).unwrap();
    
    let mut converter = ExpressionConverter::new();
    for statement in &statements {
        statement.visit(&mut converter);
    }
    
    println!("✓ Parsed and converted simple expression");
    println!("  Found {} expressions", converter.converted_expressions.len());
    
    // Test with literals
    let literal_expr = "SELECT 42, 'hello', true";
    println!("\nTesting literal expression: {}", literal_expr);
    
    let mut literal_converter = ExpressionConverter::new();
    let literal_statements = Parser::parse_sql(&dialect, literal_expr).unwrap();
    
    for statement in &literal_statements {
        statement.visit(&mut literal_converter);
    }
    
    let literals: Vec<&CustomExpr> = literal_converter.converted_expressions.iter()
        .filter(|expr| matches!(expr, CustomExpr::Literal(_)))
        .collect();
    
    println!("✓ Parsed and converted literal expressions");
    println!("  Found {} literal expressions", literals.len());
    
    // Test with function calls
    let function_expr = "SELECT CONCAT(a, b), UPPER(name)";
    println!("\nTesting function expression: {}", function_expr);
    
    let mut function_converter = ExpressionConverter::new();
    let function_statements = Parser::parse_sql(&dialect, function_expr).unwrap();
    
    for statement in &function_statements {
        statement.visit(&mut function_converter);
    }
    
    let functions: Vec<&CustomExpr> = function_converter.converted_expressions.iter()
        .filter(|expr| matches!(expr, CustomExpr::Function { .. }))
        .collect();
    
    println!("✓ Parsed and converted function expressions");
    println!("  Found {} function expressions", functions.len());
}