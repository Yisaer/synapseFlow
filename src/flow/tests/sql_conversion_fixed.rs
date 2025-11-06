//! SQL conversion tests - fixed with schema support

use flow::sql_conversion::extract_select_expressions;
use flow::expr::{ScalarExpr, BinaryFunc, DataFusionEvaluator};
use flow::tuple::Tuple;
use flow::row::Row;
use datatypes::{Value, ConcreteDatatype, Int64Type, StringType, Float64Type, Schema, ColumnSchema};

/// Helper function to create a basic schema for testing
fn create_test_schema() -> Schema {
    Schema::new(vec![
        ColumnSchema::new("a".to_string(), ConcreteDatatype::Int64(Int64Type)),
        ColumnSchema::new("b".to_string(), ConcreteDatatype::Int64(Int64Type)),
        ColumnSchema::new("c".to_string(), ConcreteDatatype::Int64(Int64Type)),
        ColumnSchema::new("name".to_string(), ConcreteDatatype::String(StringType)),
        ColumnSchema::new("value".to_string(), ConcreteDatatype::Float64(Float64Type)),
    ])
}

#[test]
fn test_basic_expression_conversion() {
    println!("\n=== Basic Expression Conversion Test ===");
    
    // Test simple addition: a + b
    let sql = "SELECT a + b";
    println!("Testing SQL: {}", sql);
    
    let schema = create_test_schema();
    let expressions = extract_select_expressions(sql, &schema).unwrap();
    
    assert_eq!(expressions.len(), 1);
    println!("✓ Parsed 1 expression");
    
    match &expressions[0] {
        ScalarExpr::CallBinary { func, expr1, expr2 } => {
            assert_eq!(*func, BinaryFunc::Add);
            // Both operands should be column references
            if let (ScalarExpr::Column(idx1), ScalarExpr::Column(idx2)) = (expr1.as_ref(), expr2.as_ref()) {
                assert_eq!(*idx1, 0); // a is at index 0
                assert_eq!(*idx2, 1); // b is at index 1
                println!("✓ Both operands correctly mapped to columns 0 + 1");
            }
        }
        _ => panic!("Expected binary operation"),
    }
}

#[test]
fn test_literal_expressions() {
    println!("\n=== Literal Expressions Test ===");
    
    let schema = create_test_schema();
    
    let test_cases = vec![
        ("SELECT 42", "integer literal"),
        ("SELECT 'hello'", "string literal"),
        ("SELECT 3.14", "float literal"),
        ("SELECT true", "boolean literal"),
    ];
    
    for (sql, description) in test_cases {
        println!("Testing {}: {}", description, sql);
        
        let expressions = extract_select_expressions(sql, &schema).unwrap();
        assert_eq!(expressions.len(), 1, "Failed for: {}", description);
        
        match &expressions[0] {
            ScalarExpr::Literal(val, _) => {
                match description {
                    "integer literal" => {
                        if let Value::Int64(i) = val {
                            assert_eq!(*i, 42);
                            println!("✓ Integer literal: {}", i);
                        }
                    }
                    "string literal" => {
                        if let Value::String(s) = val {
                            assert_eq!(s, "hello");
                            println!("✓ String literal: {}", s);
                        }
                    }
                    "float literal" => {
                        if let Value::Float64(f) = val {
                            println!("✓ Float literal: {}", f);
                        }
                    }
                    "boolean literal" => {
                        if let Value::Bool(b) = val {
                            assert_eq!(*b, true);
                            println!("✓ Boolean literal: {}", b);
                        }
                    }
                    _ => panic!("Unexpected description"),
                }
            }
            _ => panic!("Expected literal for: {}", description),
        }
    }
}

#[test]
fn test_complex_expression() {
    println!("\n=== Complex Expression Test ===");
    
    // Test complex expression: (a + b) * c
    let sql = "SELECT (a + b) * c";
    println!("Testing SQL: {}", sql);
    
    let schema = create_test_schema();
    let expressions = extract_select_expressions(sql, &schema).unwrap();
    
    assert_eq!(expressions.len(), 1);
    println!("✓ Parsed 1 expression");
    
    // Should be a multiplication of (a+b) and c
    match &expressions[0] {
        ScalarExpr::CallBinary { func, expr1, expr2 } => {
            assert_eq!(*func, BinaryFunc::Mul);
            println!("✓ Top level is multiplication");
            
            // First operand should be the addition (a + b)
            if let ScalarExpr::CallBinary { func: add_func, expr1: inner_expr1, expr2: inner_expr2 } = expr1.as_ref() {
                assert_eq!(*add_func, BinaryFunc::Add);
                println!("✓ First operand is addition");
                
                // Verify addition operands
                if let (ScalarExpr::Column(idx1), ScalarExpr::Column(idx2)) = (inner_expr1.as_ref(), inner_expr2.as_ref()) {
                    assert_eq!(*idx1, 0); // a is at index 0
                    assert_eq!(*idx2, 1); // b is at index 1
                    println!("✓ Addition operands: columns 0 + 1");
                }
            }
            
            // Second operand should be column c
            if let ScalarExpr::Column(idx) = expr2.as_ref() {
                assert_eq!(*idx, 2); // c is at index 2
                println!("✓ Second operand: column 2");
            }
        }
        _ => panic!("Expected binary operation"),
    }
}

#[test]
fn test_function_call() {
    println!("\n=== Function Call Test ===");
    
    // Test function call: CONCAT(a, b)
    let sql = "SELECT CONCAT(a, b)";
    println!("Testing SQL: {}", sql);
    
    let schema = create_test_schema();
    let expressions = extract_select_expressions(sql, &schema).unwrap();
    
    assert_eq!(expressions.len(), 1);
    println!("✓ Parsed 1 expression");
    
    match &expressions[0] {
        ScalarExpr::CallDf { function_name, args } => {
            assert_eq!(function_name, "CONCAT");
            assert_eq!(args.len(), 2);
            println!("✓ Function CONCAT with 2 arguments");
            
            // Verify function arguments
            if let (ScalarExpr::Column(idx1), ScalarExpr::Column(idx2)) = (&args[0], &args[1]) {
                assert_eq!(*idx1, 0); // a is at index 0
                assert_eq!(*idx2, 1); // b is at index 1
                println!("✓ Function arguments: columns 0 + 1");
            }
        }
        _ => panic!("Expected function call"),
    }
}

#[test]
fn test_multiple_expressions() {
    println!("\n=== Multiple Expressions Test ===");
    
    let schema = create_test_schema();
    
    // Test multiple expressions in one SELECT
    let sql = "SELECT a + b, a * b, 42";
    println!("Testing SQL: {}", sql);
    
    let expressions = extract_select_expressions(sql, &schema).unwrap();
    
    assert_eq!(expressions.len(), 3);
    println!("✓ Parsed 3 expressions");
    
    // First should be addition
    if let ScalarExpr::CallBinary { func, .. } = &expressions[0] {
        assert_eq!(*func, BinaryFunc::Add);
        println!("✓ First expression: addition");
    }
    
    // Second should be multiplication
    if let ScalarExpr::CallBinary { func, .. } = &expressions[1] {
        assert_eq!(*func, BinaryFunc::Mul);
        println!("✓ Second expression: multiplication");
    }
    
    // Third should be literal
    if let ScalarExpr::Literal(_, _) = &expressions[2] {
        println!("✓ Third expression: literal");
    }
}

#[test]
fn test_nested_expressions() {
    println!("\n=== Nested Expressions Test ===");
    
    let schema = create_test_schema();
    
    // Test nested expressions: a + (b * c)
    let sql = "SELECT a + (b * c)";
    println!("Testing SQL: {}", sql);
    
    let expressions = extract_select_expressions(sql, &schema).unwrap();
    
    assert_eq!(expressions.len(), 1);
    println!("✓ Parsed 1 expression");
    
    // Should be addition with multiplication as second operand
    match &expressions[0] {
        ScalarExpr::CallBinary { func, expr1, expr2 } => {
            assert_eq!(*func, BinaryFunc::Add);
            println!("✓ Top level is addition");
            
            // First operand should be column a
            if let ScalarExpr::Column(idx) = expr1.as_ref() {
                assert_eq!(*idx, 0); // a is at index 0
                println!("✓ First operand: column 0 (a)");
            }
            
            // Second operand should be multiplication (b * c)
            if let ScalarExpr::CallBinary { func: mul_func, expr1: inner_expr1, expr2: inner_expr2 } = expr2.as_ref() {
                assert_eq!(*mul_func, BinaryFunc::Mul);
                println!("✓ Second operand is multiplication");
                
                // Verify multiplication operands
                if let (ScalarExpr::Column(idx1), ScalarExpr::Column(idx2)) = (inner_expr1.as_ref(), inner_expr2.as_ref()) {
                    assert_eq!(*idx1, 1); // b is at index 1
                    assert_eq!(*idx2, 2); // c is at index 2
                    println!("✓ Multiplication operands: columns 1 * 2");
                }
            }
        }
        _ => panic!("Expected binary operation"),
    }
}

#[test]
fn test_error_handling() {
    println!("\n=== Error Handling Test ===");
    
    // Test invalid SQL
    let invalid_sql = "INVALID SQL EXPRESSION";
    println!("Testing invalid SQL: {}", invalid_sql);
    
    let schema = create_test_schema();
    let result = extract_select_expressions(invalid_sql, &schema);
    assert!(result.is_err());
    println!("✓ Invalid SQL properly rejected: {:?}", result.err().unwrap());
    
    // Test unsupported expression (wildcard)
    let unsupported_sql = "SELECT * FROM table";
    println!("Testing unsupported SQL: {}", unsupported_sql);
    
    let result = extract_select_expressions(unsupported_sql, &schema);
    assert!(result.is_err());
    println!("✓ Unsupported expression properly rejected");
}

#[test]
fn test_expression_evaluation() {
    println!("\n=== Expression Evaluation Test ===");
    
    // Test actual evaluation of converted expressions
    let evaluator = DataFusionEvaluator::new();
    let schema = Schema::new(vec![
        ColumnSchema::new("dummy1".to_string(), ConcreteDatatype::Int64(Int64Type)),
        ColumnSchema::new("dummy2".to_string(), ConcreteDatatype::Int64(Int64Type)),
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
    println!("✓ Evaluation successful: 5 + 3 = {:?}", result);
}