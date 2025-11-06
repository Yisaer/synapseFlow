//! Simple tests for schema-aware expression conversion

use flow::sql_conversion::{extract_select_expressions, convert_expr_to_scalar, ConversionError};
use flow::expr::{ScalarExpr, BinaryFunc};
use datatypes::{Schema, ColumnSchema, ConcreteDatatype, Int64Type, StringType};

/// Test basic schema-aware column mapping
#[test]
fn test_basic_schema_mapping() {
    println!("\n=== Basic Schema Mapping Test ===");
    
    // Create a simple schema
    let schema = Schema::new(vec![
        ColumnSchema::new("id".to_string(), ConcreteDatatype::Int64(Int64Type)),
        ColumnSchema::new("name".to_string(), ConcreteDatatype::String(StringType)),
        ColumnSchema::new("age".to_string(), ConcreteDatatype::Int64(Int64Type)),
    ]);
    
    // Test simple column references
    let sql = "SELECT name, age";
    println!("Testing SQL: {}", sql);
    
    let expressions = extract_select_expressions(sql, &schema).unwrap();
    assert_eq!(expressions.len(), 2);
    
    // Verify mapping - name should be index 1, age should be index 2
    if let ScalarExpr::Column(idx) = &expressions[0] {
        assert_eq!(*idx, 1); // name is at index 1
        println!("✓ 'name' correctly mapped to column index 1");
    }
    
    if let ScalarExpr::Column(idx) = &expressions[1] {
        assert_eq!(*idx, 2); // age is at index 2
        println!("✓ 'age' correctly mapped to column index 2");
    }
}

/// Test error handling - column not found
#[test]
fn test_column_not_found() {
    println!("\n=== Column Not Found Test ===");
    
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

/// Test binary operations with correct column mapping
#[test]
fn test_binary_operations_with_schema() {
    println!("\n=== Binary Operations with Schema Test ===");
    
    let schema = Schema::new(vec![
        ColumnSchema::new("salary".to_string(), ConcreteDatatype::Int64(Int64Type)),
        ColumnSchema::new("bonus".to_string(), ConcreteDatatype::Int64(Int64Type)),
    ]);
    
    let sql = "SELECT salary + bonus";
    println!("Testing SQL: {}", sql);
    
    let expressions = extract_select_expressions(sql, &schema).unwrap();
    assert_eq!(expressions.len(), 1);
    
    // Should be a binary operation
    match &expressions[0] {
        ScalarExpr::CallBinary { func, expr1, expr2 } => {
            assert_eq!(*func, BinaryFunc::Add);
            
            // Verify operands are correctly mapped to columns
            if let (ScalarExpr::Column(idx1), ScalarExpr::Column(idx2)) = (expr1.as_ref(), expr2.as_ref()) {
                assert_eq!(*idx1, 0); // salary is at index 0
                assert_eq!(*idx2, 1); // bonus is at index 1
                println!("✓ 'salary + bonus' correctly mapped to columns 0 + 1");
            }
        }
        _ => panic!("Expected binary operation"),
    }
}

/// Test compound identifier (simplified)
#[test]
fn test_compound_identifier() {
    println!("\n=== Compound Identifier Test ===");
    
    let schema = Schema::new(vec![
        ColumnSchema::new("id".to_string(), ConcreteDatatype::Int64(Int64Type)),
        ColumnSchema::new("name".to_string(), ConcreteDatatype::String(StringType)),
    ]);
    
    let sql = "SELECT users.name FROM users";
    println!("Testing SQL: {}", sql);
    
    let expressions = extract_select_expressions(sql, &schema).unwrap();
    assert_eq!(expressions.len(), 1);
    
    // Should map to the correct column (name is at index 1)
    if let ScalarExpr::Column(idx) = &expressions[0] {
        assert_eq!(*idx, 1); // name is at index 1
        println!("✓ 'users.name' correctly mapped to column index 1");
    }
}