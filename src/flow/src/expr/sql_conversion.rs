use super::custom_func::CUSTOM_FUNCTIONS;
use super::func::{BinaryFunc, UnaryFunc};
use super::scalar::ScalarExpr;
use datatypes::{BooleanType, ConcreteDatatype, Float64Type, Int64Type, StringType, Value};
use sqlparser::ast::{
    BinaryOperator, Expr, Function, FunctionArg, FunctionArgExpr, Ident, UnaryOperator,
    Value as SqlValue,
};

/// Enhanced error types for expression conversion with schema support
#[derive(Debug, Clone)]
pub enum ConversionError {
    UnsupportedExpression(String),
    UnsupportedOperator(String),
    TypeConversionError(String),
    ColumnNotFound(String),
    InvalidColumnReference(String),
}

impl std::fmt::Display for ConversionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConversionError::UnsupportedExpression(expr) => {
                write!(f, "Unsupported expression: {}", expr)
            }
            ConversionError::UnsupportedOperator(op) => write!(f, "Unsupported operator: {}", op),
            ConversionError::TypeConversionError(msg) => {
                write!(f, "Type conversion error: {}", msg)
            }
            ConversionError::ColumnNotFound(name) => write!(f, "Column not found: {}", name),
            ConversionError::InvalidColumnReference(ref_str) => {
                write!(f, "Invalid column reference: {}", ref_str)
            }
        }
    }
}

impl std::error::Error for ConversionError {}

/// Convert SQL Value to ScalarExpr literal
fn convert_sql_value_to_scalar(value: &SqlValue) -> Result<ScalarExpr, ConversionError> {
    match value {
        SqlValue::Number(s, _) => {
            if let Ok(i) = s.parse::<i64>() {
                Ok(ScalarExpr::Literal(
                    Value::Int64(i),
                    ConcreteDatatype::Int64(Int64Type),
                ))
            } else if let Ok(f) = s.parse::<f64>() {
                Ok(ScalarExpr::Literal(
                    Value::Float64(f),
                    ConcreteDatatype::Float64(Float64Type),
                ))
            } else {
                Err(ConversionError::TypeConversionError(format!(
                    "Cannot parse number: {}",
                    s
                )))
            }
        }
        SqlValue::SingleQuotedString(s) => Ok(ScalarExpr::Literal(
            Value::String(s.clone()),
            ConcreteDatatype::String(StringType),
        )),
        SqlValue::DoubleQuotedString(s) => Ok(ScalarExpr::Literal(
            Value::String(s.clone()),
            ConcreteDatatype::String(StringType),
        )),
        SqlValue::Boolean(b) => Ok(ScalarExpr::Literal(
            Value::Bool(*b),
            ConcreteDatatype::Bool(BooleanType),
        )),
        SqlValue::Null => Ok(ScalarExpr::Literal(
            Value::Null,
            ConcreteDatatype::Int64(Int64Type),
        )),
        _ => Err(ConversionError::TypeConversionError(format!(
            "Unsupported value type: {:?}",
            value
        ))),
    }
}

/// Convert SQL BinaryOperator to flow BinaryFunc
fn convert_binary_op(op: &BinaryOperator) -> Result<BinaryFunc, ConversionError> {
    use sqlparser::ast::BinaryOperator;

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
    use sqlparser::ast::UnaryOperator;

    match op {
        UnaryOperator::Not => Ok(UnaryFunc::Not),
        UnaryOperator::Minus => Ok(UnaryFunc::Cast(ConcreteDatatype::Int64(Int64Type))),
        _ => Err(ConversionError::UnsupportedOperator(format!("{:?}", op))),
    }
}

/// Convert sqlparser Expression to flow ScalarExpr
pub fn convert_expr_to_scalar(expr: &Expr) -> Result<ScalarExpr, ConversionError> {
    match expr {
        // Simple column reference like "a"
        Expr::Identifier(ident) => convert_identifier_to_column(ident),

        // Compound identifier like "table.column"
        Expr::CompoundIdentifier(idents) => convert_compound_identifier_to_column(idents),

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
        Expr::Function(Function { name, args, .. }) => convert_function_call(name, args),

        // Parenthesized expressions like (a + b)
        Expr::Nested(inner_expr) => convert_expr_to_scalar(inner_expr),

        // BETWEEN expressions like a BETWEEN 1 AND 10
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => convert_between_expression(expr, low, high, *negated),

        // IN expressions like a IN (1, 2, 3)
        Expr::InList {
            expr,
            list,
            negated,
        } => convert_in_list_expression(expr, list, *negated),

        // CASE expressions
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => convert_case_expression(operand, conditions, results, else_result),

        // Struct field access like a->b
        Expr::JsonAccess {
            left,
            operator,
            right,
        } => convert_json_access(left, operator, right),

        // List indexing like a[0]
        Expr::MapAccess { column, keys } => convert_map_access(column, keys),

        _ => Err(ConversionError::UnsupportedExpression(format!(
            "{:?}",
            expr
        ))),
    }
}

/// Convert simple Identifier to Column reference
fn convert_identifier_to_column(ident: &Ident) -> Result<ScalarExpr, ConversionError> {
    let column_name = &ident.value;
    if column_name == "*" {
        return Ok(ScalarExpr::wildcard_all());
    }
    Ok(ScalarExpr::column("", column_name))
}

/// Convert CompoundIdentifier to Column reference
/// Only handles cases 1 and 2 as specified:
/// - Case 1: simple identifier (already handled by convert_identifier_to_column)
/// - Case 2: table.column format where we use both source_name and column_name
fn convert_compound_identifier_to_column(idents: &[Ident]) -> Result<ScalarExpr, ConversionError> {
    if let Some(last_ident) = idents.last() {
        if last_ident.value == "*" {
            if idents.len() == 1 {
                return Ok(ScalarExpr::wildcard_all());
            }
            let qualifier = idents[..idents.len() - 1]
                .iter()
                .map(|ident| ident.value.clone())
                .collect::<Vec<_>>()
                .join(".");
            return Ok(ScalarExpr::wildcard_for(qualifier));
        }
    }

    match idents.len() {
        1 => {
            // Simple identifier case - delegate to existing function
            convert_identifier_to_column(&idents[0])
        }
        2 => {
            // table.column format - use both source_name and column_name directly
            let source_name = &idents[0].value;
            let column_name = &idents[1].value;

            // No need to validate - just create the column reference
            Ok(ScalarExpr::column(source_name, column_name))
        }
        _ => Err(ConversionError::InvalidColumnReference(format!(
            "Unsupported compound identifier with {} parts. Only 1 or 2 parts are supported.",
            idents.len()
        ))),
    }
}

/// Convert JsonAccess (struct field access like a->b) to ScalarExpr
fn convert_json_access(
    left: &Expr,
    operator: &sqlparser::ast::JsonOperator,
    right: &Expr,
) -> Result<ScalarExpr, ConversionError> {
    // Only support Arrow operator for now
    match operator {
        sqlparser::ast::JsonOperator::Arrow => {
            // Convert the struct container (left side)
            let struct_expr = convert_expr_to_scalar(left)?;

            // Convert the field name (right side) - should be an identifier
            let field_name = match right {
                Expr::Identifier(ident) => ident.value.clone(),
                _ => {
                    return Err(ConversionError::UnsupportedExpression(
                        "Struct field access right side must be an identifier".to_string(),
                    ))
                }
            };

            // Use the proper FieldAccess variant instead of CallDf
            Ok(ScalarExpr::field_access(struct_expr, field_name))
        }
        _ => Err(ConversionError::UnsupportedOperator(format!(
            "{:?}",
            operator
        ))),
    }
}

/// Convert MapAccess (list indexing like a[0]) to ScalarExpr
fn convert_map_access(column: &Expr, keys: &[Expr]) -> Result<ScalarExpr, ConversionError> {
    if keys.is_empty() {
        return Err(ConversionError::UnsupportedExpression(
            "MapAccess requires at least one key".to_string(),
        ));
    }

    if keys.len() > 1 {
        return Err(ConversionError::UnsupportedExpression(
            "Multiple keys in MapAccess not yet supported".to_string(),
        ));
    }

    // Convert the container (column)
    let container_expr = convert_expr_to_scalar(column)?;

    // Convert the key (index) - should be a literal value
    let key_expr = convert_expr_to_scalar(&keys[0])?;

    // Use the proper ListIndex variant instead of CallDf
    Ok(ScalarExpr::list_index(container_expr, key_expr))
}

/// Convert function call
fn convert_function_call(
    name: &sqlparser::ast::ObjectName,
    args: &[FunctionArg],
) -> Result<ScalarExpr, ConversionError> {
    use crate::expr::custom_func::ConcatFunc;
    use std::sync::Arc;

    let function_name = name.to_string();

    let is_custom_function = CUSTOM_FUNCTIONS.contains(&function_name.as_str());

    if !is_custom_function {
        return Err(ConversionError::UnsupportedExpression(format!(
            "Unknown function: '{}'. Available custom functions: {:?}",
            function_name, CUSTOM_FUNCTIONS
        )));
    }

    let mut scalar_args = Vec::new();

    for arg in args {
        match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                scalar_args.push(convert_expr_to_scalar(expr)?);
            }
            FunctionArg::Unnamed(FunctionArgExpr::QualifiedWildcard(object_name)) => {
                let qualifier = object_name
                    .0
                    .iter()
                    .map(|ident| ident.value.clone())
                    .collect::<Vec<_>>()
                    .join(".");
                scalar_args.push(ScalarExpr::wildcard_for(qualifier));
            }
            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {
                scalar_args.push(ScalarExpr::wildcard_all());
            }
            FunctionArg::Named {
                arg: FunctionArgExpr::Expr(arg),
                ..
            } => {
                scalar_args.push(convert_expr_to_scalar(arg)?);
            }
            FunctionArg::Named {
                arg: FunctionArgExpr::QualifiedWildcard(object_name),
                ..
            } => {
                let qualifier = object_name
                    .0
                    .iter()
                    .map(|ident| ident.value.clone())
                    .collect::<Vec<_>>()
                    .join(".");
                scalar_args.push(ScalarExpr::wildcard_for(qualifier));
            }
            FunctionArg::Named {
                arg: FunctionArgExpr::Wildcard,
                ..
            } => {
                scalar_args.push(ScalarExpr::wildcard_all());
            }
        }
    }

    let custom_func: Arc<dyn crate::expr::custom_func::CustomFunc> = match function_name.as_str() {
        "concat" => Arc::new(ConcatFunc),
        _ => {
            return Err(ConversionError::UnsupportedExpression(format!(
                "Function '{}' is in CUSTOM_FUNCTIONS but not implemented",
                function_name
            )));
        }
    };

    Ok(ScalarExpr::CallFunc {
        func: custom_func,
        args: scalar_args,
    })
}

/// Convert BETWEEN expression
fn convert_between_expression(
    expr: &Expr,
    low: &Expr,
    high: &Expr,
    negated: bool,
) -> Result<ScalarExpr, ConversionError> {
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

    if negated {
        Ok(ScalarExpr::CallUnary {
            func: UnaryFunc::Not,
            expr: Box::new(between_expr),
        })
    } else {
        Ok(between_expr)
    }
}

/// Convert IN LIST expression
fn convert_in_list_expression(
    expr: &Expr,
    list: &[Expr],
    negated: bool,
) -> Result<ScalarExpr, ConversionError> {
    if list.is_empty() {
        return Ok(ScalarExpr::Literal(
            Value::Bool(false),
            ConcreteDatatype::Bool(BooleanType),
        ));
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

    if negated {
        Ok(ScalarExpr::CallUnary {
            func: UnaryFunc::Not,
            expr: Box::new(final_expr),
        })
    } else {
        Ok(final_expr)
    }
}

/// Convert CASE expression
fn convert_case_expression(
    operand: &Option<Box<Expr>>,
    conditions: &[Expr],
    results: &[Expr],
    else_result: &Option<Box<Expr>>,
) -> Result<ScalarExpr, ConversionError> {
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

/// Extract expressions from SQL SELECT statement
pub fn extract_select_expressions(sql: &str) -> Result<Vec<ScalarExpr>, ConversionError> {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql)
        .map_err(|e| ConversionError::UnsupportedExpression(format!("Parse error: {}", e)))?;

    if statements.len() != 1 {
        return Err(ConversionError::UnsupportedExpression(
            "Expected exactly one SQL statement".to_string(),
        ));
    }

    let statement = &statements[0];

    match statement {
        sqlparser::ast::Statement::Query(query) => match &*query.body {
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
                        _ => {
                            return Err(ConversionError::UnsupportedExpression(
                                "Unsupported SELECT item type".to_string(),
                            ))
                        }
                    }
                }

                Ok(expressions)
            }
            _ => Err(ConversionError::UnsupportedExpression(
                "Expected SELECT statement".to_string(),
            )),
        },
        _ => Err(ConversionError::UnsupportedExpression(
            "Expected SELECT statement".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn convert_identifier_wildcard_becomes_scalar_wildcard() {
        let expr = Expr::Identifier(Ident::new("*"));
        let scalar = convert_expr_to_scalar(&expr).expect("conversion");
        assert!(matches!(scalar, ScalarExpr::Wildcard { source_name: None }));
    }

    #[test]
    fn convert_compound_wildcard_tracks_prefix() {
        let expr = Expr::CompoundIdentifier(vec![Ident::new("orders"), Ident::new("*")]);
        let scalar = convert_expr_to_scalar(&expr).expect("conversion");
        match scalar {
            ScalarExpr::Wildcard {
                source_name: Some(prefix),
            } => assert_eq!(prefix, "orders"),
            other => panic!("unexpected scalar expr: {:?}", other),
        }
    }
}

/// Convert SelectStmt to ScalarExpr with aliases
pub fn convert_select_stmt_to_scalar(
    select_stmt: &parser::SelectStmt,
) -> Result<Vec<(ScalarExpr, Option<String>)>, ConversionError> {
    let mut results = Vec::new();

    for field in &select_stmt.select_fields {
        let scalar_expr = convert_expr_to_scalar(&field.expr)?;
        results.push((scalar_expr, field.alias.clone()));
    }

    Ok(results)
}

/// High-level API: StreamDialect SQL to ScalarExpr conversion
pub struct StreamSqlConverter {}

impl StreamSqlConverter {
    pub fn new() -> Self {
        Self {}
    }
    pub fn convert_select_stmt(
        &self,
        select_stmt: &parser::SelectStmt,
    ) -> Result<Vec<(ScalarExpr, Option<String>)>, ConversionError> {
        // 核心转换：SelectStmt → ScalarExpr
        convert_select_stmt_to_scalar(select_stmt)
    }

    pub fn convert_select_stmt_to_scalar(
        &self,
        select_stmt: &parser::SelectStmt,
    ) -> Result<Vec<ScalarExpr>, ConversionError> {
        let results = self.convert_select_stmt(select_stmt)?;
        Ok(results.into_iter().map(|(expr, _)| expr).collect())
    }
}

impl Default for StreamSqlConverter {
    fn default() -> Self {
        Self::new()
    }
}
