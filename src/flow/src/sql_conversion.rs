use sqlparser::ast::{Expr, BinaryOperator, UnaryOperator, Value as SqlValue, Function, FunctionArg, FunctionArgExpr};
use crate::expr::{ScalarExpr, BinaryFunc, UnaryFunc};
use datatypes::{Value, ConcreteDatatype, BooleanType, StringType, Int64Type, Float64Type};

/// Error types for expression conversion
#[derive(Debug, Clone)]
pub enum ConversionError {
    UnsupportedExpression(String),
    UnsupportedOperator(String),
    TypeConversionError(String),
}

impl std::fmt::Display for ConversionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConversionError::UnsupportedExpression(expr) => write!(f, "Unsupported expression: {}", expr),
            ConversionError::UnsupportedOperator(op) => write!(f, "Unsupported operator: {}", op),
            ConversionError::TypeConversionError(msg) => write!(f, "Type conversion error: {}", msg),
        }
    }
}

impl std::error::Error for ConversionError {}

/// Convert sqlparser Expression to flow ScalarExpr
pub fn convert_expr_to_scalar(expr: &Expr) -> Result<ScalarExpr, ConversionError> {
    match expr {
        // Column references like "a", "table.column"
        Expr::Identifier(_ident) => {
            // For now, assume it's column 0 - in real implementation you'd need column mapping
            Ok(ScalarExpr::column(0))
        }
        
        // Qualified column references like "table.column"
        Expr::CompoundIdentifier(_idents) => {
            // For now, assume it's column 0 - in real implementation you'd need column mapping
            Ok(ScalarExpr::column(0))
        }
        
        // Literals like 1, 'hello', true
        Expr::Value(value) => convert_sql_value_to_scalar(value),
        
        // Binary operations like a + b, a * b
        Expr::BinaryOp { left, op, right } => {
            let left_expr = convert_expr_to_scalar(left)?;
            let right_expr = convert_expr_to_scalar(right)?;
            let binary_func = convert_binary_op(op)?;
            
            Ok(ScalarExpr::CallBinary {
                func: binary_func,
                expr1: Box::new(left_expr),
                expr2: Box::new(right_expr),
            })
        }
        
        // Unary operations like -a, NOT b
        Expr::UnaryOp { op, expr: operand } => {
            let operand_expr = convert_expr_to_scalar(operand)?;
            let unary_func = convert_unary_op(op)?;
            
            Ok(ScalarExpr::CallUnary {
                func: unary_func,
                expr: Box::new(operand_expr),
            })
        }
        
        // Function calls like CONCAT(a, b), UPPER(name)
        Expr::Function(Function { name, args, .. }) => {
            let function_name = name.to_string();
            let mut scalar_args = Vec::new();
            
            for arg in args {
                match arg {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                        scalar_args.push(convert_expr_to_scalar(expr)?);
                    }
                    FunctionArg::Named { arg: FunctionArgExpr::Expr(arg), .. } => {
                        scalar_args.push(convert_expr_to_scalar(arg)?);
                    }
                    _ => return Err(ConversionError::UnsupportedExpression("Unsupported function argument type".to_string())),
                }
            }
            
            Ok(ScalarExpr::CallDf {
                function_name,
                args: scalar_args,
            })
        }
        
        // Parenthesized expressions like (a + b)
        Expr::Nested(inner_expr) => convert_expr_to_scalar(inner_expr),
        
        // BETWEEN expressions like a BETWEEN 1 AND 10
        Expr::Between { expr, low, high, negated } => {
            let value_expr = convert_expr_to_scalar(expr)?;
            let low_expr = convert_expr_to_scalar(low)?;
            let high_expr = convert_expr_to_scalar(high)?;
            
            let lower_bound = ScalarExpr::CallBinary {
                func: BinaryFunc::Gte,
                expr1: Box::new(value_expr.clone()),
                expr2: Box::new(low_expr),
            };
            
            let upper_bound = ScalarExpr::CallBinary {
                func: BinaryFunc::Lte,
                expr1: Box::new(value_expr),
                expr2: Box::new(high_expr),
            };
            
            let between_expr = ScalarExpr::CallBinary {
                func: BinaryFunc::Mul, // Using Mul as AND logic
                expr1: Box::new(lower_bound),
                expr2: Box::new(upper_bound),
            };
            
            if *negated {
                Ok(ScalarExpr::CallUnary {
                    func: UnaryFunc::Not,
                    expr: Box::new(between_expr),
                })
            } else {
                Ok(between_expr)
            }
        }
        
        // IN expressions like a IN (1, 2, 3)
        Expr::InList { expr, list, negated } => {
            if list.is_empty() {
                return Ok(ScalarExpr::Literal(Value::Bool(false), ConcreteDatatype::Bool(BooleanType)));
            }
            
            let value_expr = convert_expr_to_scalar(expr)?;
            let mut result_expr = None;
            
            for list_item in list {
                let item_expr = convert_expr_to_scalar(list_item)?;
                let comparison = ScalarExpr::CallBinary {
                    func: BinaryFunc::Eq,
                    expr1: Box::new(value_expr.clone()),
                    expr2: Box::new(item_expr),
                };
                
                result_expr = match result_expr {
                    Some(prev) => Some(ScalarExpr::CallBinary {
                        func: BinaryFunc::Add, // Using Add as OR logic
                        expr1: Box::new(prev),
                        expr2: Box::new(comparison),
                    }),
                    None => Some(comparison),
                };
            }
            
            let final_expr = result_expr.unwrap();
            
            if *negated {
                Ok(ScalarExpr::CallUnary {
                    func: UnaryFunc::Not,
                    expr: Box::new(final_expr),
                })
            } else {
                Ok(final_expr)
            }
        }
        
        // CASE expressions
        Expr::Case { operand, conditions, results, else_result } => {
            // For simplicity, convert to a chain of IF-THEN-ELSE
            // In a real implementation, you might want to handle this more efficiently
            let mut current_expr = if let Some(else_expr) = else_result {
                convert_expr_to_scalar(else_expr)?
            } else {
                ScalarExpr::Literal(Value::Null, ConcreteDatatype::Int64(Int64Type))
            };
            
            // Process conditions in reverse order
            for i in (0..conditions.len()).rev() {
                let condition = &conditions[i];
                let result = &results[i];
                
                let condition_expr = if let Some(operand_expr) = operand {
                    // Simple CASE: operand WHEN value THEN result
                    let operand_scalar = convert_expr_to_scalar(operand_expr)?;
                    let value_scalar = convert_expr_to_scalar(condition)?;
                    ScalarExpr::CallBinary {
                        func: BinaryFunc::Eq,
                        expr1: Box::new(operand_scalar),
                        expr2: Box::new(value_scalar),
                    }
                } else {
                    // Searched CASE: WHEN condition THEN result
                    convert_expr_to_scalar(condition)?
                };
                
                let result_expr = convert_expr_to_scalar(result)?;
                
                // This is a simplified implementation - in practice you'd need proper conditional evaluation
                current_expr = ScalarExpr::CallBinary {
                    func: BinaryFunc::Add, // Using Add as a placeholder for conditional logic
                    expr1: Box::new(condition_expr),
                    expr2: Box::new(result_expr),
                };
            }
            
            Ok(current_expr)
        }
        
        _ => Err(ConversionError::UnsupportedExpression(format!("{:?}", expr))),
    }
}

/// Convert SQL Value to ScalarExpr literal
fn convert_sql_value_to_scalar(value: &SqlValue) -> Result<ScalarExpr, ConversionError> {
    match value {
        SqlValue::Number(s, _) => {
            // Try to parse as integer first, then float
            if let Ok(i) = s.parse::<i64>() {
                Ok(ScalarExpr::Literal(Value::Int64(i), ConcreteDatatype::Int64(Int64Type)))
            } else if let Ok(f) = s.parse::<f64>() {
                Ok(ScalarExpr::Literal(Value::Float64(f), ConcreteDatatype::Float64(Float64Type)))
            } else {
                Err(ConversionError::TypeConversionError(format!("Cannot parse number: {}", s)))
            }
        }
        SqlValue::SingleQuotedString(s) => {
            Ok(ScalarExpr::Literal(Value::String(s.clone()), ConcreteDatatype::String(StringType)))
        }
        SqlValue::DoubleQuotedString(s) => {
            Ok(ScalarExpr::Literal(Value::String(s.clone()), ConcreteDatatype::String(StringType)))
        }
        SqlValue::Boolean(b) => {
            Ok(ScalarExpr::Literal(Value::Bool(*b), ConcreteDatatype::Bool(BooleanType)))
        }
        SqlValue::Null => {
            // For now, use Int64 as a placeholder for NULL - this should be improved
            Ok(ScalarExpr::Literal(Value::Null, ConcreteDatatype::Int64(Int64Type)))
        }
        _ => Err(ConversionError::TypeConversionError(format!("Unsupported value type: {:?}", value))),
    }
}

/// Convert SQL BinaryOperator to flow BinaryFunc
fn convert_binary_op(op: &BinaryOperator) -> Result<BinaryFunc, ConversionError> {
    match op {
        BinaryOperator::Plus => Ok(BinaryFunc::Add),
        BinaryOperator::Minus => Ok(BinaryFunc::Sub),
        BinaryOperator::Multiply => Ok(BinaryFunc::Mul),
        BinaryOperator::Divide => Ok(BinaryFunc::Div),
        BinaryOperator::Modulo => Ok(BinaryFunc::Mod),
        BinaryOperator::Eq => Ok(BinaryFunc::Eq),
        BinaryOperator::NotEq => Ok(BinaryFunc::NotEq),
        BinaryOperator::Lt => Ok(BinaryFunc::Lt),
        BinaryOperator::LtEq => Ok(BinaryFunc::Lte),
        BinaryOperator::Gt => Ok(BinaryFunc::Gt),
        BinaryOperator::GtEq => Ok(BinaryFunc::Gte),
        _ => Err(ConversionError::UnsupportedOperator(format!("{:?}", op))),
    }
}

/// Convert SQL UnaryOperator to flow UnaryFunc
fn convert_unary_op(op: &UnaryOperator) -> Result<UnaryFunc, ConversionError> {
    match op {
        UnaryOperator::Not => Ok(UnaryFunc::Not),
        UnaryOperator::Minus => Ok(UnaryFunc::Cast(ConcreteDatatype::Int64(Int64Type))), // Convert to negative
        _ => Err(ConversionError::UnsupportedOperator(format!("{:?}", op))),
    }
}

/// Extract expressions from a SELECT statement
pub fn extract_select_expressions(sql: &str) -> Result<Vec<ScalarExpr>, ConversionError> {
    use sqlparser::parser::Parser;
    use sqlparser::dialect::GenericDialect;
    
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql)
        .map_err(|e| ConversionError::UnsupportedExpression(format!("Parse error: {}", e)))?;
    
    if statements.len() != 1 {
        return Err(ConversionError::UnsupportedExpression(
            "Expected exactly one SQL statement".to_string()
        ));
    }
    
    let statement = &statements[0];
    
    match statement {
        sqlparser::ast::Statement::Query(query) => {
            match &*query.body {
                sqlparser::ast::SetExpr::Select(select) => {
                    let mut expressions = Vec::new();
                    
                    for item in &select.projection {
                        match item {
                            sqlparser::ast::SelectItem::UnnamedExpr(expr) => {
                                expressions.push(convert_expr_to_scalar(expr)?);
                            }
                            sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } => {
                                expressions.push(convert_expr_to_scalar(expr)?);
                            }
                            _ => return Err(ConversionError::UnsupportedExpression(
                                "Unsupported SELECT item type".to_string()
                            )),
                        }
                    }
                    
                    Ok(expressions)
                }
                _ => Err(ConversionError::UnsupportedExpression(
                    "Expected SELECT statement".to_string()
                )),
            }
        }
        _ => Err(ConversionError::UnsupportedExpression(
            "Expected SELECT statement".to_string()
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::DataFusionEvaluator;
    use crate::tuple::Tuple;
    use crate::row::Row;
    use datatypes::Schema;
    
    #[test]
    fn test_convert_simple_binary_op() {
        let sql = "SELECT a + b";
        let expressions = extract_select_expressions(sql).unwrap();
        
        assert_eq!(expressions.len(), 1);
        match &expressions[0] {
            ScalarExpr::CallBinary { func, .. } => {
                assert_eq!(*func, BinaryFunc::Add);
            }
            _ => panic!("Expected binary operation"),
        }
    }
    
    #[test]
    fn test_convert_complex_expression() {
        let sql = "SELECT (a + b) * c";
        let expressions = extract_select_expressions(sql).unwrap();
        
        assert_eq!(expressions.len(), 1);
        // Should be a multiplication of (a+b) and c
        match &expressions[0] {
            ScalarExpr::CallBinary { func, expr1, .. } => {
                assert_eq!(*func, BinaryFunc::Mul);
                // First operand should be the addition
                match expr1.as_ref() {
                    ScalarExpr::CallBinary { func: add_func, .. } => {
                        assert_eq!(*add_func, BinaryFunc::Add);
                    }
                    _ => panic!("Expected addition as first operand"),
                }
            }
            _ => panic!("Expected binary operation"),
        }
    }
    
    #[test]
    fn test_convert_literals() {
        let sql = "SELECT 42, 'hello', true";
        let expressions = extract_select_expressions(sql).unwrap();
        
        assert_eq!(expressions.len(), 3);
        
        // Check integer literal
        match &expressions[0] {
            ScalarExpr::Literal(value, _) => {
                assert_eq!(*value, Value::Int64(42));
            }
            _ => panic!("Expected integer literal"),
        }
        
        // Check string literal
        match &expressions[1] {
            ScalarExpr::Literal(value, _) => {
                assert_eq!(*value, Value::String("hello".to_string()));
            }
            _ => panic!("Expected string literal"),
        }
        
        // Check boolean literal
        match &expressions[2] {
            ScalarExpr::Literal(value, _) => {
                assert_eq!(*value, Value::Bool(true));
            }
            _ => panic!("Expected boolean literal"),
        }
    }
    
    #[test]
    fn test_convert_function_call() {
        let sql = "SELECT CONCAT(a, b)";
        let expressions = extract_select_expressions(sql).unwrap();
        
        assert_eq!(expressions.len(), 1);
        match &expressions[0] {
            ScalarExpr::CallDf { function_name, args } => {
                assert_eq!(function_name, "CONCAT");
                assert_eq!(args.len(), 2);
            }
            _ => panic!("Expected function call"),
        }
    }
}