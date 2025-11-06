//! Final schema-aware tests - clean implementation

use flow::sql_conversion::extract_select_expressions;
use flow::expr::{ScalarExpr, BinaryFunc};
use datatypes::{Schema, ColumnSchema, ConcreteDatatype, Int64Type};

#[test]
fn test_schema_aware_basic() {
    println!("\n=== Schema-Aware Basic Test ===");
    
    // Create schema with proper column names
    let schema = Schema::new(vec![
        ColumnSchema::new("id".to_string(), ConcreteDatatype::Int64(Int64Type)),
        ColumnSchema::new("name".to_string(), ConcreteDatatype::Int64(Int64Type)),
        ColumnSchema::new("age".to_string(), ConcreteDatatype::Int64(Int64Type)),
    ]);
    
    // Test simple column references
    let sql = "SELECT name, age";
    println!("Testing SQL: {}", sql);
    
    let expressions = extract_select_expressions(sql, &schema).unwrap();
    assert_eq!(expressions.len(), 2);
    
    // Verify correct mapping
    if let ScalarExpr::Column(idx) = &expressions[0] {
        assert_eq!(*idx, 1); // name is at index 1
        println!("✓ 'name' mapped to column index 1");
    }
    
    if let ScalarExpr::Column(idx) = &expressions[1] {
        assert_eq!(*idx, 2); // age is at index 2
        println!("✓ 'age' mapped to column index 2");
    }
}

#[test]
fn test_schema_aware_binary_ops() {
    println!("\n=== Schema-Aware Binary Operations Test ===");
    
    let schema = Schema::new(vec![
        ColumnSchema::new("salary".to_string(), ConcreteDatatype::Int64(Int64Type)),
        ColumnSchema::new("bonus".to_string(), ConcreteDatatype::Int64(Int64Type)),
    ]);
    
    let sql = "SELECT salary + bonus";
    println!("Testing SQL: {}", sql);
    
    let expressions = extract_select_expressions(sql, &schema).unwrap();
    assert_eq!(expressions.len(), 1);
    
    match &expressions[0] {
        ScalarExpr::CallBinary { func, expr1, expr2 } => {
            assert_eq!(*func, BinaryFunc::Add);
            
            // Verify operands are correctly mapped
            if let (ScalarExpr::Column(idx1), ScalarExpr::Column(idx2)) = (expr1.as_ref(), expr2.as_ref()) {
                assert_eq!(*idx1, 0); // salary is at index 0
                assert_eq!(*idx2, 1); // bonus is at index 1
                println!("✓ 'salary + bonus' mapped to columns 0 + 1");
            }
        }
        _ => panic!("Expected binary operation"),
    }
}

#[test]
fn test_schema_aware_complex() {
    println!("\n=== Schema-Aware Complex Expression Test ===");
    
    let schema = Schema::new(vec![
        ColumnSchema::new("a".to_string(), ConcreteDatatype::Int64(Int64Type)),
        ColumnSchema::new("b".to_string(), ConcreteDatatype::Int64(Int64Type)),
        ColumnSchema::new("c".to_string(), ConcreteDatatype::Int64(Int64Type)),
    ]);
    
    let sql = "SELECT (a + b) * c";
    println!("Testing SQL: {}", sql);
    
    let expressions = extract_select_expressions(sql, &schema).unwrap();
    assert_eq!(expressions.len(), 1);
    
    // Should be multiplication with addition as first operand
    match &expressions[0] {
        ScalarExpr::CallBinary { func, expr1, .. } => {
            assert_eq!(*func, BinaryFunc::Mul);
            
            // First operand should be addition (a + b)
            if let ScalarExpr::CallBinary { func: add_func, expr1: inner_expr1, expr2: inner_expr2 } = expr1.as_ref() {
                assert_eq!(*add_func, BinaryFunc::Add);
                
                // Verify addition operands
                if let (ScalarExpr::Column(idx1), ScalarExpr::Column(idx2)) = (inner_expr1.as_ref(), inner_expr2.as_ref()) {
                    assert_eq!(*idx1, 0); // a is at index 0
                    assert_eq!(*idx2, 1); // b is at index 1
                    println!("✓ '(a + b)' correctly mapped to columns 0 + 1");
                }
            }
        }
        _ => panic!("Expected binary operation"),
    }
}