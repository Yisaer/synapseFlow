use flow::sql_conversion::extract_select_expressions;
use flow::expr::{ScalarExpr, BinaryFunc, DataFusionEvaluator};
use flow::tuple::Tuple;
use flow::row::Row;
use datatypes::{Value, ConcreteDatatype, Int64Type, Schema, ColumnSchema};

#[test]
fn test_basic_expression_conversion() {
    // Test simple addition: a + b
    let sql = "SELECT a + b";
    let schema = Schema::new(vec![
        ColumnSchema::new("a".to_string(), ConcreteDatatype::Int64(Int64Type)),
        ColumnSchema::new("b".to_string(), ConcreteDatatype::Int64(Int64Type)),
    ]);
    let expressions = extract_select_expressions(sql, &schema).unwrap();
    
    assert_eq!(expressions.len(), 1);
    
    match &expressions[0] {
        ScalarExpr::CallBinary { func, expr1, expr2 } => {
            assert_eq!(*func, BinaryFunc::Add);
            // Both operands should be column references
            assert!(matches!(expr1.as_ref(), ScalarExpr::Column(0)));
            assert!(matches!(expr2.as_ref(), ScalarExpr::Column(1)));
        }
        _ => panic!("Expected binary operation"),
    }
}

#[test]
fn test_literal_expressions() {
    // Test literal expressions
    let test_cases = vec![
        ("SELECT 42", "integer literal"),
        ("SELECT 'hello'", "string literal"),
        ("SELECT 3.14", "float literal"),
        ("SELECT true", "boolean literal"),
    ];
    
    for (sql, description) in test_cases {
        let schema = Schema::new(vec![]);  // 字面量不需要schema
        let expressions = extract_select_expressions(sql, &schema).unwrap();
        assert_eq!(expressions.len(), 1, "Failed for: {}", description);
        assert!(matches!(expressions[0], ScalarExpr::Literal(_, _)), 
                "Expected literal for: {}", description);
    }
}

#[test]
fn test_complex_expression() {
    // Test complex expression: (a + b) * c
    let sql = "SELECT (a + b) * c";
    let schema = Schema::new(vec![
        ColumnSchema::new("a".to_string(), ConcreteDatatype::Int64(Int64Type)),
        ColumnSchema::new("b".to_string(), ConcreteDatatype::Int64(Int64Type)),
        ColumnSchema::new("c".to_string(), ConcreteDatatype::Int64(Int64Type)),
    ]);
    let expressions = extract_select_expressions(sql, &schema).unwrap();
    
    assert_eq!(expressions.len(), 1);
    
    // Should be a multiplication operation
    match &expressions[0] {
        ScalarExpr::CallBinary { func, expr1, .. } => {
            assert_eq!(*func, BinaryFunc::Mul);
            // First operand should be the addition (a + b)
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
fn test_function_call() {
    // Test function call: CONCAT(a, b)
    let sql = "SELECT CONCAT(a, b)";
    let schema = Schema::new(vec![
        ColumnSchema::new("a".to_string(), ConcreteDatatype::Int64(Int64Type)),
        ColumnSchema::new("b".to_string(), ConcreteDatatype::Int64(Int64Type)),
    ]);
    let expressions = extract_select_expressions(sql, &schema).unwrap();
    
    assert_eq!(expressions.len(), 1);
    
    match &expressions[0] {
        ScalarExpr::CallDf { function_name, args } => {
            assert_eq!(function_name, "CONCAT");
            assert_eq!(args.len(), 2);
            // Both arguments should be column references
            assert!(matches!(args[0], ScalarExpr::Column(0)));
            assert!(matches!(args[1], ScalarExpr::Column(1)));
        }
        _ => panic!("Expected function call"),
    }
}

#[test]
fn test_expression_evaluation() {
    // Test actual evaluation of converted expressions
    let evaluator = DataFusionEvaluator::new();
    let schema = Schema::new(vec![
        datatypes::ColumnSchema::new("a".to_string(), ConcreteDatatype::Int64(Int64Type)),
        datatypes::ColumnSchema::new("b".to_string(), ConcreteDatatype::Int64(Int64Type))
    ]);
    let row = Row::from(vec![Value::Int64(5), Value::Int64(3)]);
    let tuple = Tuple::new(schema, row);
    
    // Create expression: 5 + 3 (using literals)
    let expr = ScalarExpr::Literal(Value::Int64(5), ConcreteDatatype::Int64(Int64Type))
        .call_binary(
            ScalarExpr::Literal(Value::Int64(3), ConcreteDatatype::Int64(Int64Type)),
            BinaryFunc::Add
        );
    
    let result = expr.eval(&evaluator, &tuple).unwrap();
    assert_eq!(result, Value::Int64(8));
}

#[test]
fn test_multiple_expressions() {
    // Test multiple expressions in one SELECT
    let sql = "SELECT a + b, a * b, 42";
    let schema = Schema::new(vec![
        ColumnSchema::new("a".to_string(), ConcreteDatatype::Int64(Int64Type)),
        ColumnSchema::new("b".to_string(), ConcreteDatatype::Int64(Int64Type)),
    ]);
    let expressions = extract_select_expressions(sql, &schema).unwrap();
    
    assert_eq!(expressions.len(), 3);
    
    // First should be addition
    assert!(matches!(&expressions[0], ScalarExpr::CallBinary { func: BinaryFunc::Add, .. }));
    
    // Second should be multiplication
    assert!(matches!(&expressions[1], ScalarExpr::CallBinary { func: BinaryFunc::Mul, .. }));
    
    // Third should be literal
    assert!(matches!(&expressions[2], ScalarExpr::Literal(_, _)));
}

#[test]
fn test_error_handling() {
    // Test invalid SQL
    let sql = "INVALID SQL";
    let schema = Schema::new(vec![]);
    let result = extract_select_expressions(sql, &schema);
    assert!(result.is_err());
    
    // Test unsupported expression (for now)
    let sql = "SELECT * FROM table"; // Wildcard not supported
    let schema = Schema::new(vec![]);
    let result = extract_select_expressions(sql, &schema);
    assert!(result.is_err());
}

#[test]
fn test_nested_expressions() {
    // Test deeply nested expressions
    let sql = "SELECT ((a + b) * c) - d";
    let schema = Schema::new(vec![
        ColumnSchema::new("a".to_string(), ConcreteDatatype::Int64(Int64Type)),
        ColumnSchema::new("b".to_string(), ConcreteDatatype::Int64(Int64Type)),
        ColumnSchema::new("c".to_string(), ConcreteDatatype::Int64(Int64Type)),
        ColumnSchema::new("d".to_string(), ConcreteDatatype::Int64(Int64Type)),
    ]);
    let expressions = extract_select_expressions(sql, &schema).unwrap();
    
    assert_eq!(expressions.len(), 1);
    
    // Should be a subtraction operation at the top level
    match &expressions[0] {
        ScalarExpr::CallBinary { func, .. } => {
            assert_eq!(*func, BinaryFunc::Sub);
        }
        _ => panic!("Expected subtraction operation"),
    }
}