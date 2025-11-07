//! Tests for ColumnSchema with source_name functionality

use datatypes::{ColumnSchema, ConcreteDatatype, Int64Type, StringType};

#[test]
fn test_column_schema_basic() {
    // Test basic ColumnSchema with all required parameters
    let column = ColumnSchema::new(
        "id".to_string(), 
        "users".to_string(),
        ConcreteDatatype::Int64(Int64Type)
    );
    
    assert_eq!(column.name, "id");
    assert_eq!(column.source_name(), "users");
    assert!(column.belongs_to("users"));
    assert!(!column.belongs_to("orders"));
}

#[test]
fn test_column_schema_different_sources() {
    // Test multiple columns from different tables
    let user_id = ColumnSchema::new(
        "id".to_string(), 
        "users".to_string(),
        ConcreteDatatype::Int64(Int64Type)
    );
    
    let user_name = ColumnSchema::new(
        "name".to_string(), 
        "users".to_string(),
        ConcreteDatatype::String(StringType)
    );
    
    let order_id = ColumnSchema::new(
        "id".to_string(), 
        "orders".to_string(),
        ConcreteDatatype::Int64(Int64Type)
    );
    
    // Verify source associations
    assert!(user_id.belongs_to("users"));
    assert!(user_name.belongs_to("users"));
    assert!(order_id.belongs_to("orders"));
    
    assert!(!user_id.belongs_to("orders"));
    assert!(!order_id.belongs_to("users"));
}

#[test]
fn test_column_schema_comparison() {
    // Test that columns with same name but different sources are different
    let user_id = ColumnSchema::new(
        "id".to_string(), 
        "users".to_string(),
        ConcreteDatatype::Int64(Int64Type)
    );
    
    let order_id = ColumnSchema::new(
        "id".to_string(), 
        "orders".to_string(),
        ConcreteDatatype::Int64(Int64Type)
    );
    
    // Same name but different sources - should be different
    assert_ne!(user_id, order_id);
    assert!(user_id.belongs_to("users"));
    assert!(order_id.belongs_to("orders"));
}

#[test]
fn test_source_name_access() {
    // Test that source_name is always accessible
    let column = ColumnSchema::new(
        "test".to_string(),
        "my_table".to_string(), 
        ConcreteDatatype::Int64(Int64Type)
    );
    
    // Should always return the source name, never None
    let source = column.source_name();
    assert_eq!(source, "my_table");
}

#[test]
fn test_belongs_to_functionality() {
    // Test the belongs_to method thoroughly
    let column = ColumnSchema::new(
        "user_id".to_string(),
        "users".to_string(),
        ConcreteDatatype::Int64(Int64Type)
    );
    
    assert!(column.belongs_to("users"));
    assert!(!column.belongs_to("orders"));
    assert!(!column.belongs_to("USERs")); // Case sensitive
    assert!(!column.belongs_to("")); // Empty string
}