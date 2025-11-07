use sqlparser::ast::{Expr, BinaryOperator, UnaryOperator, Value as SqlValue, Function, FunctionArg, FunctionArgExpr, Ident};
use crate::expr::{ScalarExpr, BinaryFunc, UnaryFunc};
use datatypes::{Value, ConcreteDatatype, BooleanType, StringType, Int64Type, Float64Type, Schema};

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
            ConversionError::UnsupportedExpression(expr) => write!(f, "Unsupported expression: {}", expr),
            ConversionError::UnsupportedOperator(op) => write!(f, "Unsupported operator: {}", op),
            ConversionError::TypeConversionError(msg) => write!(f, "Type conversion error: {}", msg),
            ConversionError::ColumnNotFound(name) => write!(f, "Column not found: {}", name),
            ConversionError::InvalidColumnReference(ref_str) => write!(f, "Invalid column reference: {}", ref_str),
        }
    }
}

impl std::error::Error for ConversionError {}

/// Convert SQL Value to ScalarExpr literal
fn convert_sql_value_to_scalar(value: &SqlValue) -> Result<ScalarExpr, ConversionError> {
    match value {
        SqlValue::Number(s, _) => {
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
            Ok(ScalarExpr::Literal(Value::Null, ConcreteDatatype::Int64(Int64Type)))
        }
        _ => Err(ConversionError::TypeConversionError(format!("Unsupported value type: {:?}", value))),
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

/// Convert sqlparser Expression to flow ScalarExpr with schema support
pub fn convert_expr_to_scalar(
    expr: &Expr,
    schema: &Schema,
) -> Result<ScalarExpr, ConversionError> {
    match expr {
        // Simple column reference like "a"
        Expr::Identifier(ident) => {
            convert_identifier_to_column(ident, schema)
        }
        
        // Compound identifier like "table.column" or "db.table.column"
        Expr::CompoundIdentifier(idents) => {
            convert_compound_identifier_to_column(idents, schema)
        }
        
        // Literals like 1, 'hello', true
        Expr::Value(value) => convert_sql_value_to_scalar(value),
        
        // Binary operations like a + b, a * b
        Expr::BinaryOp { left, op, right } => {
            let left_expr = convert_expr_to_scalar(left, schema)?;
            let right_expr = convert_expr_to_scalar(right, schema)?;
            let binary_func = convert_binary_op(op)?;
            
            Ok(ScalarExpr::CallBinary {
                func: binary_func,
                expr1: Box::new(left_expr),
                expr2: Box::new(right_expr),
            })
        }
        
        // Unary operations like -a, NOT b
        Expr::UnaryOp { op, expr: operand } => {
            let operand_expr = convert_expr_to_scalar(operand, schema)?;
            let unary_func = convert_unary_op(op)?;
            
            Ok(ScalarExpr::CallUnary {
                func: unary_func,
                expr: Box::new(operand_expr),
            })
        }
        
        // Function calls like CONCAT(a, b), UPPER(name)
        Expr::Function(Function { name, args, .. }) => {
            convert_function_call_with_schema(name, args, schema)
        }
        
        // Parenthesized expressions like (a + b)
        Expr::Nested(inner_expr) => convert_expr_to_scalar(inner_expr, schema),
        
        // BETWEEN expressions like a BETWEEN 1 AND 10
        Expr::Between { expr, low, high, negated } => {
            convert_between_expression_with_schema(expr, low, high, *negated, schema)
        }
        
        // IN expressions like a IN (1, 2, 3)
        Expr::InList { expr, list, negated } => {
            convert_in_list_expression_with_schema(expr, list, *negated, schema)
        }
        
        // CASE expressions
        Expr::Case { operand, conditions, results, else_result } => {
            convert_case_expression_with_schema(operand, conditions, results, else_result, schema)
        }
        
        // Struct field access like a->b
        Expr::JsonAccess { left, operator, right } => {
            convert_json_access_with_schema(left, operator, right, schema)
        }
        
        // List indexing like a[0]
        Expr::MapAccess { column, keys } => {
            convert_map_access_with_schema(column, keys, schema)
        }
        
        _ => Err(ConversionError::UnsupportedExpression(format!("{:?}", expr))),
    }
}

/// Convert simple Identifier to Column reference using schema
fn convert_identifier_to_column(ident: &Ident, schema: &Schema) -> Result<ScalarExpr, ConversionError> {
    let column_name = &ident.value;
    
    // 在 schema 中查找列
    if let Some(index) = schema.column_schemas().iter().position(|col| col.name == *column_name) {
        Ok(ScalarExpr::column(index))
    } else {
        Err(ConversionError::ColumnNotFound(column_name.clone()))
    }
}

/// Convert CompoundIdentifier to Column reference using schema
fn convert_compound_identifier_to_column(idents: &[Ident], schema: &Schema) -> Result<ScalarExpr, ConversionError> {
    match idents.len() {
        1 => {
            // 只有一个部分，退化为简单标识符
            convert_identifier_to_column(&idents[0], schema)
        }
        2 => {
            // table.column 格式
            let table_name = &idents[0].value;
            let column_name = &idents[1].value;
            
            // 为了简单起见，我们只检查列名是否存在
            // 在实际应用中，你可能还需要验证表名
            if let Some(index) = schema.column_schemas().iter().position(|col| col.name == *column_name) {
                Ok(ScalarExpr::column(index))
            } else {
                Err(ConversionError::ColumnNotFound(format!("{}. {}", table_name, column_name)))
            }
        }
        3 => {
            // db.table.column 格式
            let _db_name = &idents[0].value;
            let table_name = &idents[1].value;
            let column_name = &idents[2].value;
            
            // 简化处理：只检查列名
            if let Some(index) = schema.column_schemas().iter().position(|col| col.name == *column_name) {
                Ok(ScalarExpr::column(index))
            } else {
                Err(ConversionError::ColumnNotFound(format!("{}.{}", _db_name, table_name)))
            }
        }
        _ => {
            // 更复杂的限定符，暂时不支持
            Err(ConversionError::InvalidColumnReference(
                idents.iter().map(|i| i.value.as_str()).collect::<Vec<_>>().join(".")
            ))
        }
    }
}

/// Convert JsonAccess (struct field access like a->b) to ScalarExpr
fn convert_json_access_with_schema(
    left: &Expr,
    operator: &sqlparser::ast::JsonOperator,
    right: &Expr,
    schema: &Schema,
) -> Result<ScalarExpr, ConversionError> {
    // Only support Arrow operator for now
    match operator {
        sqlparser::ast::JsonOperator::Arrow => {
            // Convert the struct container (left side)
            let struct_expr = convert_expr_to_scalar(left, schema)?;
            
            // Convert the field name (right side) - should be an identifier
            let field_name = match right {
                Expr::Identifier(ident) => ident.value.clone(),
                _ => return Err(ConversionError::UnsupportedExpression(
                    "Struct field access right side must be an identifier".to_string()
                )),
            };
            
            // Use the proper FieldAccess variant instead of CallDf
            Ok(ScalarExpr::field_access(struct_expr, field_name))
        }
        _ => Err(ConversionError::UnsupportedOperator(format!("{:?}", operator))),
    }
}

/// Convert MapAccess (list indexing like a[0]) to ScalarExpr
fn convert_map_access_with_schema(
    column: &Expr,
    keys: &[Expr],
    schema: &Schema,
) -> Result<ScalarExpr, ConversionError> {
    if keys.is_empty() {
        return Err(ConversionError::UnsupportedExpression(
            "MapAccess requires at least one key".to_string()
        ));
    }
    
    if keys.len() > 1 {
        return Err(ConversionError::UnsupportedExpression(
            "Multiple keys in MapAccess not yet supported".to_string()
        ));
    }
    
    // Convert the container (column)
    let container_expr = convert_expr_to_scalar(column, schema)?;
    
    // Convert the key (index) - should be a literal value
    let key_expr = convert_expr_to_scalar(&keys[0], schema)?;
    
    // Use the proper ListIndex variant instead of CallDf
    Ok(ScalarExpr::list_index(container_expr, key_expr))
}

/// Convert function call with schema context
fn convert_function_call_with_schema(
    name: &sqlparser::ast::ObjectName,
    args: &[FunctionArg],
    schema: &Schema,
) -> Result<ScalarExpr, ConversionError> {
    let function_name = name.to_string();
    let mut scalar_args = Vec::new();
    
    for arg in args {
        match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                scalar_args.push(convert_expr_to_scalar(expr, schema)?);
            }
            FunctionArg::Named { arg: FunctionArgExpr::Expr(arg), .. } => {
                scalar_args.push(convert_expr_to_scalar(arg, schema)?);
            }
            _ => return Err(ConversionError::UnsupportedExpression("Unsupported function argument type".to_string())),
        }
    }
    
    Ok(ScalarExpr::CallDf {
        function_name,
        args: scalar_args,
    })
}

/// Convert BETWEEN expression with schema
fn convert_between_expression_with_schema(
    expr: &Expr,
    low: &Expr,
    high: &Expr,
    negated: bool,
    schema: &Schema,
) -> Result<ScalarExpr, ConversionError> {
    let value_expr = convert_expr_to_scalar(expr, schema)?;
    let low_expr = convert_expr_to_scalar(low, schema)?;
    let high_expr = convert_expr_to_scalar(high, schema)?;
    
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

/// Convert IN LIST expression with schema
fn convert_in_list_expression_with_schema(
    expr: &Expr,
    list: &[Expr],
    negated: bool,
    schema: &Schema,
) -> Result<ScalarExpr, ConversionError> {
    if list.is_empty() {
        return Ok(ScalarExpr::Literal(Value::Bool(false), ConcreteDatatype::Bool(BooleanType)));
    }
    
    let value_expr = convert_expr_to_scalar(expr, schema)?;
    let mut result_expr = None;
    
    for list_item in list {
        let item_expr = convert_expr_to_scalar(list_item, schema)?;
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

/// Convert CASE expression with schema
fn convert_case_expression_with_schema(
    operand: &Option<Box<Expr>>,
    conditions: &[Expr],
    results: &[Expr],
    else_result: &Option<Box<Expr>>,
    schema: &Schema,
) -> Result<ScalarExpr, ConversionError> {
    // For simplicity, convert to a chain of IF-THEN-ELSE
    // In a real implementation, you might want to handle this more efficiently
    let mut current_expr = if let Some(else_expr) = else_result {
        convert_expr_to_scalar(else_expr, schema)?
    } else {
        ScalarExpr::Literal(Value::Null, ConcreteDatatype::Int64(Int64Type))
    };
    
    // Process conditions in reverse order
    for i in (0..conditions.len()).rev() {
        let condition = &conditions[i];
        let result = &results[i];
        
        let condition_expr = if let Some(operand_expr) = operand {
            // Simple CASE: operand WHEN value THEN result
            let operand_scalar = convert_expr_to_scalar(operand_expr, schema)?;
            let value_scalar = convert_expr_to_scalar(condition, schema)?;
            ScalarExpr::CallBinary {
                func: BinaryFunc::Eq,
                expr1: Box::new(operand_scalar),
                expr2: Box::new(value_scalar),
            }
        } else {
            // Searched CASE: WHEN condition THEN result
            convert_expr_to_scalar(condition, schema)?
        };
        
        let result_expr = convert_expr_to_scalar(result, schema)?;
        
        // This is a simplified implementation - in practice you'd need proper conditional evaluation
        current_expr = ScalarExpr::CallBinary {
            func: BinaryFunc::Add, // Using Add as a placeholder for conditional logic
            expr1: Box::new(condition_expr),
            expr2: Box::new(result_expr),
        };
    }
    
    Ok(current_expr)
}

/// Enhanced version of extract_select_expressions with schema support
pub fn extract_select_expressions(
    sql: &str,
    schema: &Schema,
) -> Result<Vec<ScalarExpr>, ConversionError> {
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
                                expressions.push(convert_expr_to_scalar(expr, schema)?);
                            }
                            sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } => {
                                expressions.push(convert_expr_to_scalar(expr, schema)?);
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

/// Convert SelectStmt to ScalarExpr with aliases
pub fn convert_select_stmt_to_scalar(
    select_stmt: &parser::SelectStmt, 
    schema: &Schema
) -> Result<Vec<(ScalarExpr, Option<String>)>, ConversionError> {
    let mut results = Vec::new();
    
    for field in &select_stmt.select_fields {
        let scalar_expr = convert_expr_to_scalar(&field.expr, schema)?;
        results.push((scalar_expr, field.alias.clone()));
    }
    
    Ok(results)
}

/// High-level API: StreamDialect SQL to ScalarExpr conversion
pub struct StreamSqlConverter {
}

impl StreamSqlConverter {
    pub fn new() -> Self {
        Self {}
    }
    pub fn convert_select_stmt(
        &self, 
        select_stmt: &parser::SelectStmt, 
        schema: &Schema
    ) -> Result<Vec<(ScalarExpr, Option<String>)>, ConversionError> {
        // 核心转换：SelectStmt → ScalarExpr
        convert_select_stmt_to_scalar(select_stmt, schema)
    }

    pub fn convert_select_stmt_to_scalar(
        &self,
        select_stmt: &parser::SelectStmt, 
        schema: &Schema
    ) -> Result<Vec<ScalarExpr>, ConversionError> {
        let results = self.convert_select_stmt(select_stmt, schema)?;
        Ok(results.into_iter().map(|(expr, _)| expr).collect())
    }
}

impl Default for StreamSqlConverter {
    fn default() -> Self {
        Self::new()
    }
}

/// 便捷函数：SQL字符串 → SelectStmt → ScalarExpr（带别名）
/// 完整流程：先解析SQL得到SelectStmt，然后转换为ScalarExpr
pub fn extract_select_expressions_with_aliases(sql: &str, schema: &Schema) -> Result<Vec<(ScalarExpr, Option<String>)>, ConversionError> {
    // 步骤1: 使用StreamDialect解析SQL得到SelectStmt
    let select_stmt = parser::parse_sql(sql)
        .map_err(|e| ConversionError::UnsupportedExpression(format!("Parse error: {}", e)))?;
    
    // 步骤2: 使用StreamSqlConverter转换SelectStmt为ScalarExpr
    let converter = StreamSqlConverter::new();
    converter.convert_select_stmt(&select_stmt, schema)
}

/// Backward compatibility: convert single expression
pub fn convert_expr_to_scalar_with_schema(
    expr: &Expr,
    schema: &Schema,
) -> Result<ScalarExpr, ConversionError> {
    convert_expr_to_scalar(expr, schema)
}

/// 便捷函数：SQL字符串 → SelectStmt → ScalarExpr（不带别名）
/// 完整流程：先解析SQL得到SelectStmt，然后转换为ScalarExpr
pub fn parse_sql_to_scalar_expr(
    sql: &str,
    schema: &Schema
) -> Result<Vec<ScalarExpr>, ConversionError> {
    // 步骤1: 使用StreamDialect解析SQL得到SelectStmt
    let select_stmt = parser::parse_sql(sql)
        .map_err(|e| ConversionError::UnsupportedExpression(format!("Parse error: {}", e)))?;
    
    // 步骤2: 使用StreamSqlConverter转换SelectStmt为ScalarExpr
    let converter = StreamSqlConverter::new();
    converter.convert_select_stmt_to_scalar(&select_stmt, schema)
}