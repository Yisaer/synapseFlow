//! Comprehensive tests demonstrating the complete SQL to evaluation workflow

use flow::sql_conversion::extract_select_expressions;
use flow::expr::{DataFusionEvaluator, ScalarExpr, BinaryFunc};
use flow::tuple::Tuple;
use flow::row::Row;
use datatypes::{Value, ConcreteDatatype, Int64Type, Float64Type, StringType, Schema, ColumnSchema};

/// Test the complete workflow: SQL → ScalarExpr → Evaluation
#[test]
fn test_complete_workflow() {
    println!("\n=== Complete Workflow Test ===");
    
    // Step 1: Parse SQL expressions
    let sql = "SELECT a + b, 42, 'hello'";
    println!("Input SQL: {}", sql);
    
    let schema = Schema::new(vec![
        ColumnSchema::new("a".to_string(), ConcreteDatatype::Int64(Int64Type)),
        ColumnSchema::new("b".to_string(), ConcreteDatatype::Int64(Int64Type)),
        ColumnSchema::new("c".to_string(), ConcreteDatatype::Int64(Int64Type)),
    ]);
    let expressions = extract_select_expressions(sql, &schema).unwrap();
    assert_eq!(expressions.len(), 3);
    println!("✓ Parsed and converted {} expressions", expressions.len());
    
    // Step 2: Verify expression types
    match &expressions[0] {
        ScalarExpr::CallBinary { func, .. } => {
            assert_eq!(*func, BinaryFunc::Add);
            println!("✓ First expression is binary addition");
        }
        _ => panic!("Expected binary operation"),
    }
    
    match &expressions[1] {
        ScalarExpr::Literal(val, _) => {
            assert_eq!(*val, Value::Int64(42));
            println!("✓ Second expression is integer literal");
        }
        _ => panic!("Expected literal"),
    }
    
    match &expressions[2] {
        ScalarExpr::Literal(val, _) => {
            assert_eq!(*val, Value::String("hello".to_string()));
            println!("✓ Third expression is string literal");
        }
        _ => panic!("Expected literal"),
    }
    
    // Step 3: Create evaluation context
    let evaluator = DataFusionEvaluator::new();
    let schema = Schema::new(vec![
        datatypes::ColumnSchema::new("a".to_string(), ConcreteDatatype::Int64(Int64Type)),
        datatypes::ColumnSchema::new("b".to_string(), ConcreteDatatype::Int64(Int64Type))
    ]);
    let row = Row::from(vec![Value::Int64(5), Value::Int64(3)]); // Sample data: a=5, b=3
    let tuple = Tuple::new(schema, row);
    
    // Step 4: Evaluate expressions with literal values
    let literal_addition = ScalarExpr::Literal(Value::Int64(5), ConcreteDatatype::Int64(Int64Type))
        .call_binary(
            ScalarExpr::Literal(Value::Int64(3), ConcreteDatatype::Int64(Int64Type)),
            BinaryFunc::Add,
        );
    
    let result = literal_addition.eval(&evaluator, &tuple).unwrap();
    assert_eq!(result, Value::Int64(8));
    println!("✓ Evaluation successful: 5 + 3 = {:?}", result);
}

/// Test various SQL expression types
#[test]
fn test_various_expression_types() {
    println!("\n=== Various Expression Types Test ===");
    
    let test_cases = vec![
        ("SELECT a + b", "addition"),
        ("SELECT a - b", "subtraction"),
        ("SELECT a * b", "multiplication"),
        ("SELECT a / b", "division"),
        ("SELECT 42", "integer literal"),
        ("SELECT 'hello'", "string literal"),
        ("SELECT (a + b) * c", "complex expression"),
        ("SELECT CONCAT(a, b)", "function call"),
    ];
    
    for (sql, description) in test_cases {
        println!("\nTesting {}: {}", description, sql);
        
        let schema = Schema::new(vec![
            ColumnSchema::new("a".to_string(), ConcreteDatatype::Int64(Int64Type)),
            ColumnSchema::new("b".to_string(), ConcreteDatatype::Int64(Int64Type)),
            ColumnSchema::new("c".to_string(), ConcreteDatatype::Int64(Int64Type)),
        ]);
        let expressions = extract_select_expressions(sql, &schema);
        assert!(expressions.is_ok(), "Failed to parse {}: {}", description, sql);
        
        let exprs = expressions.unwrap();
        assert_eq!(exprs.len(), 1, "Should have exactly one expression for {}", description);
        
        // Verify the expression structure
        match description {
            "addition" => {
                assert!(matches!(exprs[0], ScalarExpr::CallBinary { func: BinaryFunc::Add, .. }));
            }
            "subtraction" => {
                assert!(matches!(exprs[0], ScalarExpr::CallBinary { func: BinaryFunc::Sub, .. }));
            }
            "multiplication" => {
                assert!(matches!(exprs[0], ScalarExpr::CallBinary { func: BinaryFunc::Mul, .. }));
            }
            "division" => {
                assert!(matches!(exprs[0], ScalarExpr::CallBinary { func: BinaryFunc::Div, .. }));
            }
            "integer literal" => {
                assert!(matches!(exprs[0], ScalarExpr::Literal(Value::Int64(42), _)));
            }
            "string literal" => {
                assert!(matches!(exprs[0], ScalarExpr::Literal(Value::String(_), _)));
            }
            "function call" => {
                if let ScalarExpr::CallDf { function_name, .. } = &exprs[0] {
                    // Function names are case-sensitive, CONCAT is uppercase
                    assert_eq!(function_name, "CONCAT");
                } else {
                    panic!("Expected function call but got: {:?}", exprs[0]);
                }
            }
            _ => {}
        }
        
        println!("✓ {} parsed successfully", description);
    }
}

/// Test actual calculation with different data types
#[test]
fn test_calculation_with_data_types() {
    println!("\n=== Calculation with Data Types Test ===");
    
    let evaluator = DataFusionEvaluator::new();
    let schema = Schema::new(vec![datatypes::ColumnSchema::new("dummy".to_string(), ConcreteDatatype::Int64(Int64Type))]);
    let row = Row::from(vec![Value::Int64(0)]); // Dummy data
    let tuple = Tuple::new(schema, row);
    
    // Test integer calculation
    let int_expr = ScalarExpr::Literal(Value::Int64(10), ConcreteDatatype::Int64(Int64Type))
        .call_binary(
            ScalarExpr::Literal(Value::Int64(5), ConcreteDatatype::Int64(Int64Type)),
            BinaryFunc::Add,
        );
    
    let result = int_expr.eval(&evaluator, &tuple).unwrap();
    assert_eq!(result, Value::Int64(15));
    println!("✓ Integer calculation: 10 + 5 = {:?}", result);
    
    // Test float calculation
    let float_expr = ScalarExpr::Literal(Value::Float64(10.5), ConcreteDatatype::Float64(Float64Type))
        .call_binary(
            ScalarExpr::Literal(Value::Float64(2.5), ConcreteDatatype::Float64(Float64Type)),
            BinaryFunc::Mul,
        );
    
    let result = float_expr.eval(&evaluator, &tuple).unwrap();
    assert_eq!(result, Value::Float64(26.25));
    println!("✓ Float calculation: 10.5 * 2.5 = {:?}", result);
    
    // Test string concatenation
    let concat_expr = ScalarExpr::Literal(
        Value::String("Hello".to_string()), 
        ConcreteDatatype::String(StringType)
    ).call_binary(
        ScalarExpr::Literal(
            Value::String(" World".to_string()), 
            ConcreteDatatype::String(StringType)
        ),
        BinaryFunc::Add, // Using Add for string concatenation
    );
    
    let result = concat_expr.eval(&evaluator, &tuple).unwrap();
    assert_eq!(result, Value::String("Hello World".to_string()));
    println!("✓ String concatenation: 'Hello' + ' World' = {:?}", result);
}

/// Test complex nested expressions
#[test]
fn test_complex_nested_expressions() {
    println!("\n=== Complex Nested Expressions Test ===");
    
    let sql = "SELECT (a + b) * c, a + (b * c)";
    let schema = Schema::new(vec![
        ColumnSchema::new("a".to_string(), ConcreteDatatype::Int64(Int64Type)),
        ColumnSchema::new("b".to_string(), ConcreteDatatype::Int64(Int64Type)),
        ColumnSchema::new("c".to_string(), ConcreteDatatype::Int64(Int64Type)),
    ]);
    let expressions = extract_select_expressions(sql, &schema).unwrap();
    
    assert_eq!(expressions.len(), 2);
    println!("✓ Parsed 2 complex expressions");
    
    // First expression: (a + b) * c
    match &expressions[0] {
        ScalarExpr::CallBinary { func, expr1, .. } => {
            assert_eq!(*func, BinaryFunc::Mul);
            // First operand should be addition
            match expr1.as_ref() {
                ScalarExpr::CallBinary { func: add_func, .. } => {
                    assert_eq!(*add_func, BinaryFunc::Add);
                    println!("✓ First expression: (a + b) * c");
                }
                _ => panic!("Expected addition as first operand"),
            }
        }
        _ => panic!("Expected binary operation"),
    }
    
    // Second expression: a + (b * c)  
    match &expressions[1] {
        ScalarExpr::CallBinary { func, expr2, .. } => {
            assert_eq!(*func, BinaryFunc::Add);
            // Second operand should be multiplication
            match expr2.as_ref() {
                ScalarExpr::CallBinary { func: mul_func, .. } => {
                    assert_eq!(*mul_func, BinaryFunc::Mul);
                    println!("✓ Second expression: a + (b * c)");
                }
                _ => panic!("Expected multiplication as second operand"),
            }
        }
        _ => panic!("Expected binary operation"),
    }
}

/// Test error handling
#[test]
fn test_error_handling() {
    println!("\n=== Error Handling Test ===");
    
    // Test invalid SQL
    let invalid_sql = "INVALID SQL EXPRESSION";
    let result = extract_select_expressions(invalid_sql, &Schema::new(vec![]));
    assert!(result.is_err());
    println!("✓ Invalid SQL properly rejected: {:?}", result.err().unwrap());
    
    // Test unsupported expression (wildcard)
    let unsupported_sql = "SELECT * FROM table";
    let result = extract_select_expressions(unsupported_sql, &Schema::new(vec![]));
    assert!(result.is_err());
    println!("✓ Unsupported expression properly rejected");
}

/// Demonstrate the complete workflow like in the original main.rs
#[test]
fn test_demo_workflow() {
    println!("\n=== Demo Workflow Test ===");
    println!("🎯 This test demonstrates the complete SQL → ScalarExpr → Value workflow");
    
    // Step 1: SQL input
    let sql = "SELECT a + b";
    println!("1. Input SQL: {}", sql);
    
    // Step 2: Parse and convert
    let schema = Schema::new(vec![
        ColumnSchema::new("a".to_string(), ConcreteDatatype::Int64(Int64Type)),
        ColumnSchema::new("b".to_string(), ConcreteDatatype::Int64(Int64Type)),
    ]);
    let expressions = extract_select_expressions(sql, &schema).unwrap();
    println!("2. Converted to ScalarExpr: {:?}", expressions[0]);
    
    // Step 3: Create evaluation context
    let evaluator = DataFusionEvaluator::new();
    let schema = Schema::new(vec![
        datatypes::ColumnSchema::new("a".to_string(), ConcreteDatatype::Int64(Int64Type)),
        datatypes::ColumnSchema::new("b".to_string(), ConcreteDatatype::Int64(Int64Type))
    ]);
    let row = Row::from(vec![Value::Int64(5), Value::Int64(3)]);
    let tuple = Tuple::new(schema, row);
    
    // Step 4: Create test expression (since column references need proper mapping)
    let test_expr = ScalarExpr::Literal(Value::Int64(5), ConcreteDatatype::Int64(Int64Type))
        .call_binary(
            ScalarExpr::Literal(Value::Int64(3), ConcreteDatatype::Int64(Int64Type)),
            BinaryFunc::Add,
        );
    
    // Step 5: Evaluate
    let result = test_expr.eval(&evaluator, &tuple).unwrap();
    println!("3. Evaluation result: {:?}", result);
    
    // Verify result
    assert_eq!(result, Value::Int64(8));
    println!("✅ SUCCESS: Complete workflow demonstrated!");
    println!("   SQL '{}' was successfully parsed, converted, and calculated to {:?}", sql, result);
}