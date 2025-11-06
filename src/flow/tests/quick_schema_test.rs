//! Quick test to verify schema-aware conversion works

use flow::sql_conversion::extract_select_expressions;
use flow::expr::{ScalarExpr, BinaryFunc};
use datatypes::{Schema, ColumnSchema, ConcreteDatatype, Int64Type, StringType};

#[test]
fn test_quick_schema_conversion() {
    println!("\n=== Quick Schema Conversion Test ===");
    
    // Create a simple schema
    let schema = Schema::new(vec![
        ColumnSchema::new("a".to_string(), ConcreteDatatype::Int64(Int64Type)),
        ColumnSchema::new("b".to_string(), ConcreteDatatype::Int64(Int64Type)),
    ]);
    
    // Test simple expression
    let sql = "SELECT a + b";
    println!("Testing SQL: {}", sql);
    
    let expressions = extract_select_expressions(sql, &schema).unwrap();
    assert_eq!(expressions.len(), 1);
    
    // Should be a binary operation with correct column mapping
    match &expressions[0] {
        ScalarExpr::CallBinary { func, expr1, expr2 } => {
            assert_eq!(*func, BinaryFunc::Add);
            
            // Verify operands are correctly mapped to columns 0 and 1
            if let (ScalarExpr::Column(idx1), ScalarExpr::Column(idx2)) = (expr1.as_ref(), expr2.as_ref()) {
                assert_eq!(*idx1, 0); // a is at index 0
                assert_eq!(*idx2, 1); // b is at index 1
                println!("✓ 'a + b' correctly mapped to columns 0 + 1");
            }
        }
        _ => panic!("Expected binary operation"),
    }
}

#[test]
fn test_column_mapping() {
    println!("\n=== Column Mapping Test ===");
    
    let schema = Schema::new(vec![
        ColumnSchema::new("id".to_string(), ConcreteDatatype::Int64(Int64Type)),
        ColumnSchema::new("name".to_string(), ConcreteDatatype::String(StringType)),
        ColumnSchema::new("age".to_string(), ConcreteDatatype::Int64(Int64Type)),
    ]);
    
    let sql = "SELECT name, age";
    println!("Testing SQL: {}", sql);
    
    let expressions = extract_select_expressions(sql, &schema).unwrap();
    assert_eq!(expressions.len(), 2);
    
    // Verify correct column mapping
    if let ScalarExpr::Column(idx1) = &expressions[0] {
        assert_eq!(*idx1, 1); // name is at index 1
        println!("✓ 'name' correctly mapped to column index 1");
    }
    
    if let ScalarExpr::Column(idx2) = &expressions[1] {
        assert_eq!(*idx2, 2); // age is at index 2
        println!("✓ 'age' correctly mapped to column index 2");
    }
}