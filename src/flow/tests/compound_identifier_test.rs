//! Tests for compound identifier functionality (a.b) with source_name support

use flow::ScalarExpr;
use datatypes::{Schema, ColumnSchema, ConcreteDatatype, Int64Type, StringType};
use flow::sql_conversion::extract_select_expressions;

#[test]
fn test_compound_identifier_with_source_name() {
    println!("\n=== Testing Compound Identifier with Source Name ===");
    
    // Create schema with columns from different tables
    let schema = Schema::new(vec![
        ColumnSchema::new("id".to_string(), "users".to_string(), ConcreteDatatype::Int64(Int64Type)),
        ColumnSchema::new("name".to_string(), "users".to_string(), ConcreteDatatype::String(StringType)),
        ColumnSchema::new("id".to_string(), "orders".to_string(), ConcreteDatatype::Int64(Int64Type)),
        ColumnSchema::new("total".to_string(), "orders".to_string(), ConcreteDatatype::Int64(Int64Type)),
        ColumnSchema::new("user_id".to_string(), "orders".to_string(), ConcreteDatatype::Int64(Int64Type)),
    ]);
    
    // Test 1: users.id (should find users.id, not orders.id)
    println!("Test 1: SELECT users.id");
    let sql1 = "SELECT users.id";
    match extract_select_expressions(sql1, &schema) {
        Ok(expressions) => {
            assert_eq!(expressions.len(), 1);
            match &expressions[0] {
                ScalarExpr::Column(idx) => {
                    assert_eq!(*idx, 0, "Should find users.id at index 0");
                    println!("✓ Successfully resolved users.id to column index 0");
                }
                _ => panic!("Expected Column expression"),
            }
        }
        Err(e) => panic!("Failed to resolve users.id: {}", e),
    }
    
    // Test 2: orders.id (should find orders.id, not users.id)
    println!("Test 2: SELECT orders.id");
    let sql2 = "SELECT orders.id";
    match extract_select_expressions(sql2, &schema) {
        Ok(expressions) => {
            assert_eq!(expressions.len(), 1);
            match &expressions[0] {
                ScalarExpr::Column(idx) => {
                    assert_eq!(*idx, 2, "Should find orders.id at index 2");
                    println!("✓ Successfully resolved orders.id to column index 2");
                }
                _ => panic!("Expected Column expression"),
            }
        }
        Err(e) => panic!("Failed to resolve orders.id: {}", e),
    }
    
    // Test 3: users.name (should find users.name)
    println!("Test 3: SELECT users.name");
    let sql3 = "SELECT users.name";
    match extract_select_expressions(sql3, &schema) {
        Ok(expressions) => {
            assert_eq!(expressions.len(), 1);
            match &expressions[0] {
                ScalarExpr::Column(idx) => {
                    assert_eq!(*idx, 1, "Should find users.name at index 1");
                    println!("✓ Successfully resolved users.name to column index 1");
                }
                _ => panic!("Expected Column expression"),
            }
        }
        Err(e) => panic!("Failed to resolve users.name: {}", e),
    }
    
    // Test 4: orders.total (should find orders.total)
    println!("Test 4: SELECT orders.total");
    let sql4 = "SELECT orders.total";
    match extract_select_expressions(sql4, &schema) {
        Ok(expressions) => {
            assert_eq!(expressions.len(), 1);
            match &expressions[0] {
                ScalarExpr::Column(idx) => {
                    assert_eq!(*idx, 3, "Should find orders.total at index 3");
                    println!("✓ Successfully resolved orders.total to column index 3");
                }
                _ => panic!("Expected Column expression"),
            }
        }
        Err(e) => panic!("Failed to resolve orders.total: {}", e),
    }
}

#[test]
fn test_compound_identifier_not_found() {
    println!("\n=== Testing Compound Identifier Not Found ===");
    
    let schema = Schema::new(vec![
        ColumnSchema::new("id".to_string(), "users".to_string(), ConcreteDatatype::Int64(Int64Type)),
        ColumnSchema::new("name".to_string(), "users".to_string(), ConcreteDatatype::String(StringType)),
    ]);
    
    // Test: users.email (should fail - users table doesn't have email column)
    println!("Test: SELECT users.email (should fail)");
    let sql = "SELECT users.email";
    match extract_select_expressions(sql, &schema) {
        Ok(_) => panic!("Expected failure for users.email"),
        Err(e) => {
            println!("✓ Correctly failed for users.email: {}", e);
            assert!(e.to_string().contains("users.email"));
        }
    }
}

#[test]
fn test_simple_identifier_still_works() {
    println!("\n=== Testing Simple Identifier Still Works ===");
    
    let schema = Schema::new(vec![
        ColumnSchema::new("id".to_string(), "users".to_string(), ConcreteDatatype::Int64(Int64Type)),
        ColumnSchema::new("name".to_string(), "users".to_string(), ConcreteDatatype::String(StringType)),
    ]);
    
    // Test: simple identifier (should still work)
    println!("Test: SELECT id (simple identifier)");
    let sql = "SELECT id";
    match extract_select_expressions(sql, &schema) {
        Ok(expressions) => {
            assert_eq!(expressions.len(), 1);
            match &expressions[0] {
                ScalarExpr::Column(idx) => {
                    // Should find the first id column (users.id at index 0)
                    assert_eq!(*idx, 0);
                    println!("✓ Simple identifier id correctly resolved to column index 0");
                }
                _ => panic!("Expected Column expression"),
            }
        }
        Err(e) => panic!("Simple identifier should work: {}", e),
    }
}

#[test]
fn test_unsupported_compound_identifier_length() {
    println!("\n=== Testing Unsupported Compound Identifier Length ===");
    
    let schema = Schema::new(vec![
        ColumnSchema::new("id".to_string(), "users".to_string(), ConcreteDatatype::Int64(Int64Type)),
    ]);
    
    // Test: 3-part identifier (should fail - only 1 and 2 parts supported)
    println!("Test: SELECT db.users.id (should fail - 3 parts not supported)");
    let sql = "SELECT db.users.id";
    match extract_select_expressions(sql, &schema) {
        Ok(_) => panic!("Expected failure for 3-part identifier"),
        Err(e) => {
            println!("✓ Correctly failed for 3-part identifier: {}", e);
            assert!(e.to_string().contains("3 parts"));
        }
    }
}