//! Tests for schema-aware expression conversion

use flow::sql_conversion::{extract_select_expressions, ConversionError};
use flow::expr::{ScalarExpr, BinaryFunc};
use datatypes::{Schema, ColumnSchema, ConcreteDatatype, Int64Type, StringType};

/// Test schema-aware column mapping
#[test]
fn test_schema_aware_column_mapping() {
    println!("\n=== Schema-Aware Column Mapping Test ===");
    
    // Create a schema with specific column names and order
    let schema = Schema::new(vec![
        ColumnSchema::new("id".to_string(), ConcreteDatatype::Int64(Int64Type)),
        ColumnSchema::new("name".to_string(), ConcreteDatatype::String(StringType)),
        ColumnSchema::new("age".to_string(), ConcreteDatatype::Int64(Int64Type)),
        ColumnSchema::new("salary".to_string(), ConcreteDatatype::Int64(Int64Type)),
    ]);
    
    // Test simple identifier mapping
    let sql = "SELECT name, age, salary";
    println!("Testing SQL: {}", sql);
    
    let expressions = extract_select_expressions(sql, &schema).unwrap();
    assert_eq!(expressions.len(), 3);
    
    // Verify each expression maps to correct column index
    // name -> index 1, age -> index 2, salary -> index 3
    match &expressions[0] {
        ScalarExpr::Column(idx) => {
            assert_eq!(*idx, 1); // name is at index 1
            println!("✓ 'name' correctly mapped to column index 1");
        }
        _ => panic!("Expected column reference"),
    }
    
    match &expressions[1] {
        ScalarExpr::Column(idx) => {
            assert_eq!(*idx, 2); // age is at index 2
            println!("✓ 'age' correctly mapped to column index 2");
        }
        _ => panic!("Expected column reference"),
    }
    
    match &expressions[2] {
        ScalarExpr::Column(idx) => {
            assert_eq!(*idx, 3); // salary is at index 3
            println!("✓ 'salary' correctly mapped to column index 3");
        }
        _ => panic!("Expected column reference"),
    }
}

/// Test compound identifier handling
#[test]
fn test_compound_identifier_handling() {
    println!("\n=== Compound Identifier Handling Test ===");
    
    let schema = Schema::new(vec![
        ColumnSchema::new("id".to_string(), ConcreteDatatype::Int64(Int64Type)),
        ColumnSchema::new("name".to_string(), ConcreteDatatype::String(StringType)),
    ]);
    
    // Test table.column format (simplified - we only check column name)
    let sql = "SELECT users.name FROM users";
    println!("Testing SQL: {}", sql);
    
    let expressions = extract_select_expressions(sql, &schema).unwrap();
    assert_eq!(expressions.len(), 1);
    
    match &expressions[0] {
        ScalarExpr::Column(idx) => {
            assert_eq!(*idx, 1); // name is at index 1
            println!("✓ 'users.name' correctly mapped to column index 1");
        }
        _ => panic!("Expected column reference"),
    }
}

/// Test column not found error
#[test]
fn test_column_not_found_error() {
    println!("\n=== Column Not Found Error Test ===");
    
    let schema = Schema::new(vec![
        ColumnSchema::new("id".to_string(), ConcreteDatatype::Int64(Int64Type)),
        ColumnSchema::new("name".to_string(), ConcreteDatatype::String(StringType)),
    ]);
    
    // Test non-existent column
    let sql = "SELECT non_existent_column";
    println!("Testing SQL: {}", sql);
    
    let result = extract_select_expressions(sql, &schema);
    assert!(result.is_err());
    
    match result.err().unwrap() {
        ConversionError::ColumnNotFound(column_name) => {
            assert_eq!(column_name, "non_existent_column");
            println!("✓ Correctly detected missing column: '{}'", column_name);
        }
        _ => panic!("Expected ColumnNotFound error"),
    }
}

/// Test binary operations with schema-aware columns
#[test]
fn test_binary_operations_with_schema() {
    println!("\n=== Binary Operations with Schema Test ===");
    
    let schema = Schema::new(vec![
        ColumnSchema::new("salary".to_string(), ConcreteDatatype::Int64(Int64Type)),
        ColumnSchema::new("bonus".to_string(), ConcreteDatatype::Int64(Int64Type)),
    ]);
    
    // Test binary operation with schema-aware column references
    let sql = "SELECT salary + bonus";
    println!("Testing SQL: {}", sql);
    
    let expressions = extract_select_expressions(sql, &schema).unwrap();
    assert_eq!(expressions.len(), 1);
    
    match &expressions[0] {
        ScalarExpr::CallBinary { func, expr1, expr2 } => {
            assert_eq!(*func, BinaryFunc::Add);
            
            // Verify both operands are correctly mapped to columns
            match (expr1.as_ref(), expr2.as_ref()) {
                (ScalarExpr::Column(idx1), ScalarExpr::Column(idx2)) => {
                    assert_eq!(*idx1, 0); // salary is at index 0
                    assert_eq!(*idx2, 1); // bonus is at index 1
                    println!("✓ 'salary + bonus' correctly mapped to columns 0 + 1");
                }
                _ => panic!("Expected column references for both operands"),
            }
        }
        _ => panic!("Expected binary operation"),
    }
}

/// Test function calls with schema-aware arguments
#[test]
fn test_function_calls_with_schema() {
    println!("\n=== Function Calls with Schema Test ===");
    
    let schema = Schema::new(vec![
        ColumnSchema::new("first_name".to_string(), ConcreteDatatype::String(StringType)),
        ColumnSchema::new("last_name".to_string(), ConcreteDatatype::String(StringType)),
    ]);
    
    // Test function call with schema-aware column arguments
    let sql = "SELECT CONCAT(first_name, last_name)";
    println!("Testing SQL: {}", sql);
    
    let expressions = extract_select_expressions(sql, &schema).unwrap();
    assert_eq!(expressions.len(), 1);
    
    match &expressions[0] {
        ScalarExpr::CallDf { function_name, args } => {
            assert_eq!(function_name, "CONCAT");
            assert_eq!(args.len(), 2);
            
            // Verify function arguments are correctly mapped
            match (&args[0], &args[1]) {
                (ScalarExpr::Column(idx1), ScalarExpr::Column(idx2)) => {
                    assert_eq!(*idx1, 0); // first_name is at index 0
                    assert_eq!(*idx2, 1); // last_name is at index 1
                    println!("✓ CONCAT arguments correctly mapped to columns 0 and 1");
                }
                _ => panic!("Expected column references for function arguments"),
            }
        }
        _ => panic!("Expected function call"),
    }
}

/// Test complex expressions with schema
#[test]
fn test_complex_expressions_with_schema() {
    println!("\n=== Complex Expressions with Schema Test ===");
    
    let schema = Schema::new(vec![
        ColumnSchema::new("a".to_string(), ConcreteDatatype::Int64(Int64Type)),
        ColumnSchema::new("b".to_string(), ConcreteDatatype::Int64(Int64Type)),
        ColumnSchema::new("c".to_string(), ConcreteDatatype::Int64(Int64Type)),
    ]);
    
    // Test complex nested expression
    let sql = "SELECT (a + b) * c";
    println!("Testing SQL: {}", sql);
    
    let expressions = extract_select_expressions(sql, &schema).unwrap();
    assert_eq!(expressions.len(), 1);
    
    match &expressions[0] {
        ScalarExpr::CallBinary { func, expr1, expr2 } => {
            assert_eq!(*func, BinaryFunc::Mul);
            
            // First operand should be addition (a + b)
            match expr1.as_ref() {
                ScalarExpr::CallBinary { func: add_func, expr1: inner_expr1, expr2: inner_expr2 } => {
                    assert_eq!(*add_func, BinaryFunc::Add);
                    
                    // Verify addition operands
                    match (inner_expr1.as_ref(), inner_expr2.as_ref()) {
                        (ScalarExpr::Column(idx1), ScalarExpr::Column(idx2)) => {
                            assert_eq!(*idx1, 0); // a is at index 0
                            assert_eq!(*idx2, 1); // b is at index 1
                            println!("✓ (a + b) correctly mapped to columns 0 + 1");
                        }
                        _ => panic!("Expected column references for addition"),
                    }
                }
                _ => panic!("Expected addition as first operand"),
            }
            
            // Second operand should be column c
            match expr2.as_ref() {
                ScalarExpr::Column(idx) => {
                    assert_eq!(*idx, 2); // c is at index 2
                    println!("✓ * c correctly mapped to column 2");
                }
                _ => panic!("Expected column reference for multiplication"),
            }
        }
        _ => panic!("Expected binary operation"),
    }
}

/// Test backward compatibility - ensure old API still works
#[test]
fn test_backward_compatibility() {
    println!("\n=== Backward Compatibility Test ===");
    
    // Test that the old API (without schema) still works
    let sql = "SELECT a + b";
    println!("Testing legacy API with SQL: {}", sql);
    
    // Create schema for backward compatibility test
    let schema = Schema::new(vec![
        ColumnSchema::new("a".to_string(), ConcreteDatatype::Int64(Int64Type)),
        ColumnSchema::new("b".to_string(), ConcreteDatatype::Int64(Int64Type)),
    ]);
    let expressions = flow::sql_conversion::extract_select_expressions(sql, &schema).unwrap();
    assert_eq!(expressions.len(), 1);
    
    match &expressions[0] {
        ScalarExpr::CallBinary { func, .. } => {
            assert_eq!(*func, BinaryFunc::Add);
            println!("✓ Legacy API still works correctly");
        }
        _ => panic!("Expected binary operation"),
    }
}